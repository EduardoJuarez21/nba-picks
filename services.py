import json
import os
import shlex
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
if (BASE_DIR / "scripts").is_dir():
    ROOT_DIR = BASE_DIR
elif (BASE_DIR.parent / "scripts").is_dir():
    ROOT_DIR = BASE_DIR.parent
else:
    ROOT_DIR = BASE_DIR
SCRIPTS_DIR = ROOT_DIR / "scripts"
DATA_DIR = os.getenv("DATA_DIR", "data")

_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()


def _log(scope: str, message: str) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now}] [{scope}] {message}")


def today_utc_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _parse_day(day_raw: str | None) -> str | None:
    if day_raw is None:
        return None
    raw = str(day_raw).strip()
    if not raw:
        return None
    parts = raw.split("-")
    if len(parts) == 2 and all(p.isdigit() for p in parts):
        dd, mm = parts
        if len(dd) == 2 and len(mm) == 2:
            yyyy = datetime.now(timezone.utc).year
            return f"{yyyy}-{mm}-{dd}"
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        yyyy, mm, dd = parts
        if len(yyyy) == 4 and len(mm) == 2 and len(dd) == 2:
            return f"{yyyy}-{mm}-{dd}"
    return None


def normalize_day(day_raw: str | None) -> str | None:
    return _parse_day(day_raw)


def _set_versioned_paths_nba(day: str) -> None:
    safe_day = day.replace("/", "-")

    def out(subdir: str, name: str, ext: str) -> str:
        return str(Path(DATA_DIR) / "fuente" / "nba" / subdir / f"{name}_{safe_day}.{ext}")

    os.environ["LEAGUE"] = "nba"
    os.environ["DAY"] = day
    os.environ["FIXTURES_PATH"] = out("fixtures", "fixtures", "json")
    os.environ["OUT_PATH_TEAM_STRENGTH"] = out("team_strength", "team_strength", "json")
    os.environ["OUT_PATH_INJURIES"] = out("injuries", "injuries", "json")
    os.environ["OUT_PATH_FEATURES"] = out("features", "ready_context", "json")
    os.environ["OUT_PATH_ODDS"] = out("odds", "odds", "json")
    os.environ["OUT_PATH_PRED"] = out("predictions", "predictions", "json")
    os.environ["OUT_TSV"] = str(Path("out") / "nba" / f"picks_{safe_day}.tsv")


def _run_step(label: str, args: list[str], output_path: str | None, skip_if_exists: bool) -> None:
    if skip_if_exists and output_path and Path(ROOT_DIR / output_path).is_file():
        _log("pipeline", f"skip {label}: {output_path} exists")
        return
    cmd_str = " ".join(shlex.quote(str(a)) for a in args)
    _log("pipeline", f"run {label}: {cmd_str}")
    started = time.monotonic()
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    proc = subprocess.Popen(
        args,
        cwd=ROOT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    if proc.stdout is not None:
        for raw_line in proc.stdout:
            line = raw_line.rstrip("\r\n")
            if line:
                _log(label, line)
    return_code = proc.wait()
    elapsed = time.monotonic() - started
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, args)
    _log("pipeline", f"ok {label} elapsed={elapsed:.1f}s")


def _fixtures_empty(fixtures_path: str | None) -> bool:
    if not fixtures_path:
        return True
    p = ROOT_DIR / fixtures_path
    if not p.is_file():
        return True
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return True
    return not payload


def _run_nba_pipeline(day: str) -> str:
    _set_versioned_paths_nba(day)
    _log("pipeline", f"start league=nba day={day}")
    skip_if_exists = os.getenv("SKIP_IF_EXISTS", "false").strip().lower() == "true"
    py = sys.executable or "python"
    steps = [
        ("fixtures", [py, str(SCRIPTS_DIR / "nba_fetch_fixtures.py")], os.getenv("FIXTURES_PATH"), skip_if_exists),
        ("odds", [py, str(SCRIPTS_DIR / "nba_fetch_odds.py")], os.getenv("OUT_PATH_ODDS"), skip_if_exists),
        (
            "team_strength",
            [py, str(SCRIPTS_DIR / "nba_team_strength.py")],
            os.getenv("OUT_PATH_TEAM_STRENGTH"),
            skip_if_exists,
        ),
        ("injuries", [py, str(SCRIPTS_DIR / "nba_injuries_pdf.py")], os.getenv("OUT_PATH_INJURIES"), skip_if_exists),
        ("features", [py, str(SCRIPTS_DIR / "nba_build_ready_context.py")], os.getenv("OUT_PATH_FEATURES"), skip_if_exists),
        ("pred", [py, str(SCRIPTS_DIR / "predicciones_nba.py")], os.getenv("OUT_TSV"), skip_if_exists),
    ]
    for label, cmd, out_path, skip in steps:
        _run_step(label, cmd, out_path, skip)
        if label == "fixtures" and _fixtures_empty(os.getenv("FIXTURES_PATH")):
            _log("pipeline", "done no_fixtures")
            return "no_fixtures"
    _log("pipeline", "done ok")
    return "ok"


def _picks_path(day: str) -> Path:
    return ROOT_DIR / "out" / "nba" / f"picks_{day}.tsv"


def _read_tsv_rows(path: Path) -> tuple[list[str], list[dict]]:
    if not path.is_file():
        return [], []
    text = path.read_text(encoding="utf-8")
    lines = [ln.rstrip("\n") for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return [], []
    headers = lines[0].split("\t")
    rows = []
    for line in lines[1:]:
        parts = line.split("\t")
        rows.append({headers[i]: parts[i] if i < len(parts) else "" for i in range(len(headers))})
    return headers, rows


def _telegram_config() -> tuple[str, str] | None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return None
    return token, chat_id


def _send_telegram_message(text: str) -> bool:
    cfg = _telegram_config()
    if not cfg:
        _log("telegram/sendMessage", "skipped missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return False
    token, chat_id = cfg
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
        resp.raise_for_status()
        _log("telegram/sendMessage", f"ok status_code={resp.status_code}")
        return True
    except Exception as exc:
        body = ""
        if "resp" in locals():
            body = (resp.text or "").strip().replace("\n", " ")[:200]
        _log("telegram/sendMessage", f"failed error={exc} body={body!r}")
        return False


def _build_match_lines(rows: list[dict]) -> dict[str, dict]:
    matches: dict[str, dict] = {}
    for row in rows:
        decision = str(row.get("decision") or "").strip().upper()
        if not decision.startswith("BET"):
            continue
        match = row.get("match") or f"{row.get('home', '')} vs {row.get('away', '')}".strip()
        market = (row.get("pick_market") or row.get("market") or "").upper()
        pick = (row.get("pick") or "").upper()
        line_val = row.get("line") or ""
        odds = row.get("odds") or ""
        kickoff = row.get("kickoff_utc") or ""
        entry = matches.setdefault(match, {"kickoff": kickoff, "rows": []})
        entry["rows"].append({"market": market, "pick": pick, "line": line_val, "odds": odds})
    return matches


def _send_telegram_picks_from_rows(rows: list[dict], day: str, single_message: bool = True, max_chars: int = 3500) -> int:
    matches = _build_match_lines(rows)
    if not matches:
        _log("telegram/picks", "skip no BET rows")
        return 0
    lines: list[str] = [f"NBA picks {day}", ""]
    for match in sorted(matches.keys()):
        lines.append(match)
        by_market = {item["market"]: item for item in matches[match]["rows"]}
        for market in ("ML", "SPREAD", "TOTAL"):
            item = by_market.get(market)
            if not item:
                continue
            if market in ("SPREAD", "TOTAL") and item["line"]:
                lines.append(f"- {market} {item['pick']} {item['line']} @ {item['odds']}")
            else:
                lines.append(f"- {market} {item['pick']} @ {item['odds']}")
        lines.append("")
    if lines and lines[-1] == "":
        lines.pop()
    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[: max_chars - 20].rstrip() + "\n...(truncado)"
    if single_message:
        return 1 if _send_telegram_message(text) else 0
    return 1 if _send_telegram_message(text) else 0


def _db_url() -> str | None:
    return os.getenv("DATABASE_URL") or None


_NUMERIC_FIELDS = {
    "match_id", "line", "odds", "books_count", "lambda_home", "lambda_away",
    "p_home", "p_draw", "p_away", "p_hat", "p_implied", "edge", "ev",
    "hh_games", "aa_games",
}


def _coerce_num(val: object) -> object:
    """Convierte cadena vacía a None para columnas numéricas."""
    if val is None or str(val).strip() == "":
        return None
    try:
        return float(str(val).strip())
    except ValueError:
        return None


def _db_save_picks_rows(day: str, rows: list[dict]) -> int:
    """Guarda los picks NBA en picks_rows. Devuelve el número de filas insertadas."""
    url = _db_url()
    if not url or not rows:
        return 0
    try:
        import psycopg2
    except ImportError:
        _log("db", "psycopg2 no disponible, saltando guardado en BD")
        return 0

    INSERT_SQL = """
        INSERT INTO picks_rows (
            league, day, match_id, run_id, created_at_utc, kickoff_utc,
            home, away, market, pick, line, odds, book, books_count,
            lambda_home, lambda_away, p_home, p_draw, p_away, p_hat,
            p_implied, edge, ev, hh_games, aa_games, decision, flags,
            result, bet
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (league, day, kickoff_utc, home, away, market, pick, line)
        DO UPDATE SET
            decision = excluded.decision,
            odds = excluded.odds,
            edge = excluded.edge,
            ev = excluded.ev,
            flags = excluded.flags,
            bet = excluded.bet
    """

    def _v(r: dict, key: str) -> object:
        val = r.get(key, "")
        return _coerce_num(val) if key in _NUMERIC_FIELDS else (str(val) if val is not None else "")

    try:
        conn = psycopg2.connect(url)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM picks_rows WHERE league = %s AND day = %s", ("nba", day))
            inserted = 0
            for r in rows:
                try:
                    cur.execute(
                        INSERT_SQL,
                        (
                            "nba", day,
                            _v(r, "match_id"), _v(r, "run_id"), _v(r, "created_at_utc"), _v(r, "kickoff_utc"),
                            _v(r, "home"), _v(r, "away"), _v(r, "market"), _v(r, "pick"),
                            _v(r, "line"), _v(r, "odds"), _v(r, "book"), _v(r, "books_count"),
                            _v(r, "lambda_home"), _v(r, "lambda_away"),
                            _v(r, "p_home"), _v(r, "p_draw"), _v(r, "p_away"), _v(r, "p_hat"),
                            _v(r, "p_implied"), _v(r, "edge"), _v(r, "ev"),
                            _v(r, "hh_games"), _v(r, "aa_games"),
                            _v(r, "decision"), _v(r, "flags"), _v(r, "result"), _v(r, "bet"),
                        ),
                    )
                    inserted += 1
                except Exception as exc:
                    _log("db", f"error insertando fila match_id={r.get('match_id')}: {exc}")
        finally:
            cur.close()
            conn.close()
        _log("db", f"picks_rows insertadas={inserted} league=nba day={day}")
        return inserted
    except Exception as exc:
        _log("db", f"error guardando en BD: {exc}")
        return 0


def _run_job(job_id: str, day: str, force: bool) -> None:
    with _jobs_lock:
        _jobs[job_id] = {"status": "running", "league": "nba", "day": day, "force": force}
    try:
        result = _run_nba_pipeline(day)
        picks_path = _picks_path(day)
        _, rows = _read_tsv_rows(picks_path)
        db_saved = _db_save_picks_rows(day, rows) if rows else 0
        sent = _send_telegram_picks_from_rows(rows, day, single_message=True, max_chars=3500) if rows else 0
        with _jobs_lock:
            _jobs[job_id] = {
                "status": "ok" if result == "ok" else result,
                "league": "nba",
                "day": day,
                "telegram_messages": sent,
                "db_rows_saved": db_saved,
                "rows": len(rows),
            }
    except Exception as exc:
        with _jobs_lock:
            _jobs[job_id] = {"status": "error", "league": "nba", "day": day, "error": str(exc)}


def queue_job(payload: dict | None) -> tuple[dict, int]:
    day = _parse_day((payload or {}).get("day")) if isinstance(payload, dict) else None
    if payload and isinstance(payload, dict) and payload.get("day") and not day:
        return {"status": "error", "error": "Formato invalido. Usa DD-MM o YYYY-MM-DD."}, 400
    if not day:
        day = today_utc_iso()
    force = False
    if isinstance(payload, dict):
        force = str(payload.get("force") or "").strip().lower() in {"1", "true", "yes", "y", "si", "on"}
    job_id = uuid.uuid4().hex
    t = threading.Thread(target=_run_job, args=(job_id, day, force), daemon=True)
    t.start()
    return {"status": "queued", "job_id": job_id, "league": "nba", "day": day, "force": force}, 202


def get_status(job_id: str) -> tuple[dict, int]:
    with _jobs_lock:
        info = _jobs.get(job_id)
    if not info:
        return {"status": "not_found", "job_id": job_id}, 404
    payload = {"job_id": job_id}
    payload.update(info)
    return payload, 200


def get_picks_result(day: str) -> tuple[dict, int]:
    path = _picks_path(day)
    _, rows = _read_tsv_rows(path)
    if not rows:
        return {"status": "empty", "league": "nba", "day": day, "items": []}, 200
    return {"status": "ok", "league": "nba", "day": day, "items": rows}, 200


def _coerce_match_id(raw: object) -> str:
    return str(raw or "").strip()


def _find_fixture_day_by_match_id(match_id: int) -> str | None:
    look_dirs = [
        ROOT_DIR / DATA_DIR / "fuente" / "nba" / "fixtures",
        ROOT_DIR / "fuente" / "nba" / "fixtures",
    ]
    target = str(match_id)
    for folder in look_dirs:
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("fixtures_*.json"), reverse=True):
            try:
                items = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            for item in items:
                game_id = _coerce_match_id(item.get("game_id") or item.get("match_id"))
                if game_id == target:
                    stem = path.stem
                    day = stem.replace("fixtures_", "")
                    if _parse_day(day):
                        return day
    return None


def _filter_rows_by_match_id(rows: list[dict], match_id: int) -> list[dict]:
    target = str(match_id)
    return [row for row in rows if _coerce_match_id(row.get("match_id")) == target]


def picks_for_match_id(match_id: int, day_raw: str | None = None) -> tuple[dict, int]:
    day = _parse_day(day_raw) if day_raw else None
    if day_raw and not day:
        return {"status": "error", "error": "Formato invalido. Usa DD-MM o YYYY-MM-DD."}, 400
    if not day:
        day = _find_fixture_day_by_match_id(match_id) or today_utc_iso()
    path = _picks_path(day)
    if not path.is_file():
        result = _run_nba_pipeline(day)
        if result == "no_fixtures":
            return {"status": "empty", "league": "nba", "day": day, "match_id": match_id, "items": []}, 200
    _, rows = _read_tsv_rows(path)
    matched = _filter_rows_by_match_id(rows, match_id)
    if matched:
        sent = _send_telegram_picks_from_rows(matched, day, single_message=True, max_chars=3500)
        if sent == 0:
            _send_telegram_message(f"NBA {day}\nmatch_id={match_id}\nSin picks BET para este partido.")
    return {
        "status": "ok" if matched else "empty",
        "league": "nba",
        "day": day,
        "match_id": match_id,
        "items": matched,
    }, 200
