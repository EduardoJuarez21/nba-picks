"""nba_injuries_pdf_fixed_v2.py

Parser robusto del PDF oficial de Injury Report (NBA) -> JSON compatible con nba_build_ready_context.py.

✅ Fixes principales:
- `teams` ahora es un dict: {"Lakers": [ {player,status,reason,...}, ... ], ...}
- Output por defecto va a `fuente/nba/features/injuries_YYYY-MM-DD.json` (lo que espera nba_build_ready_context.py)
- También escribe copias en rutas legacy para que nada se rompa si aún usas scripts viejos.

Uso:
  DAY=2026-01-30 PDF_PATH=./Injury-Report_2026-01-30_12_00AM.pdf python nba_injuries_pdf_fixed_v2.py
  # Si no pasas PDF_PATH, intenta descargar el reporte desde official.nba.com

Vars:
  DAY (YYYY-MM-DD)                 requerido (o --day)
  PDF_PATH                         opcional (o --pdf); si falta se descarga
  LEAGUE                           default: nba
  OUT_INJ / OUT_PATH_INJURIES      override del output principal
  ALSO_WRITE_LEGACY=1              default: 1 (escribe copias legacy)
  NBA_INJURY_PAGE_URL              override de la pagina oficial
  NBA_INJURY_CACHE_DIR             directorio cache para PDFs descargados
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# PDF parsing backends are optional.
# Prefer pdfplumber when available (it preserves tabular layout better),
# but allow running without it.

from nba_utils import ensure_parent_dir, versioned_path


INJURY_PAGE_URL = os.getenv(
    "NBA_INJURY_PAGE_URL",
    "https://official.nba.com/nba-injury-report-2025-26-season/",
)
INJURY_CACHE_DIR = Path(
    os.getenv("NBA_INJURY_CACHE_DIR", str(Path("data") / "cache" / "nba_injuries"))
)


STATUS_SET = {
    "OUT",
    "DOUBTFUL",
    "QUESTIONABLE",
    "PROBABLE",
    "AVAILABLE",
    "NOTYETSUBMITTED",
}

# Razones a excluir del conteo de severidad (G-League / no impactantes)
EXCLUDE_REASON_TOKENS = {
    "G-LEAGUE",
    "GLEAGUE",
    "TWO-WAY",
    "TWOWAY",
    "ONASSIGNMENT",
    "ON-ASSIGNMENT",
    "NOTWITHTEAM",
    "NOT-WITH-TEAM",
}


# ---- Team aliases ----
# Normalizamos quitando todo lo que no sea letra, y lower.

def _norm_team_key(s: str) -> str:
    return re.sub(r"[^a-z]", "", (s or "").lower())


# (city, nickname)
NBA_TEAMS: List[Tuple[str, str]] = [
    ("Atlanta", "Hawks"),
    ("Boston", "Celtics"),
    ("Brooklyn", "Nets"),
    ("Charlotte", "Hornets"),
    ("Chicago", "Bulls"),
    ("Cleveland", "Cavaliers"),
    ("Dallas", "Mavericks"),
    ("Denver", "Nuggets"),
    ("Detroit", "Pistons"),
    ("Golden State", "Warriors"),
    ("Houston", "Rockets"),
    ("Indiana", "Pacers"),
    ("LA", "Clippers"),
    ("Los Angeles", "Lakers"),
    ("Memphis", "Grizzlies"),
    ("Miami", "Heat"),
    ("Milwaukee", "Bucks"),
    ("Minnesota", "Timberwolves"),
    ("New Orleans", "Pelicans"),
    ("New York", "Knicks"),
    ("Oklahoma City", "Thunder"),
    ("Orlando", "Magic"),
    ("Philadelphia", "76ers"),
    ("Phoenix", "Suns"),
    ("Portland", "Trail Blazers"),
    ("Sacramento", "Kings"),
    ("San Antonio", "Spurs"),
    ("Toronto", "Raptors"),
    ("Utah", "Jazz"),
    ("Washington", "Wizards"),
]

TEAM_ALIAS_TO_NICK: Dict[str, str] = {}
TEAM_NICK_TO_FULL: Dict[str, str] = {}

for city, nick in NBA_TEAMS:
    full = f"{city} {nick}".replace("  ", " ").strip()
    TEAM_NICK_TO_FULL[nick] = full

    # alias por nickname
    TEAM_ALIAS_TO_NICK[_norm_team_key(nick)] = nick

    # alias por full name con espacios
    TEAM_ALIAS_TO_NICK[_norm_team_key(full)] = nick

    # alias camelcase típico del PDF (sin espacios)
    TEAM_ALIAS_TO_NICK[_norm_team_key(full.replace(" ", ""))] = nick

# aliases comunes
TEAM_ALIAS_TO_NICK[_norm_team_key("LAClippers")] = "Clippers"
TEAM_ALIAS_TO_NICK[_norm_team_key("LosAngelesClippers")] = "Clippers"
TEAM_ALIAS_TO_NICK[_norm_team_key("GoldenStateWarriors")] = "Warriors"
TEAM_ALIAS_TO_NICK[_norm_team_key("PortlandTrailBlazers")] = "Trail Blazers"
TEAM_ALIAS_TO_NICK[_norm_team_key("NewOrleansPelicans")] = "Pelicans"
TEAM_ALIAS_TO_NICK[_norm_team_key("NewYorkKnicks")] = "Knicks"
TEAM_ALIAS_TO_NICK[_norm_team_key("OklahomaCityThunder")] = "Thunder"

# ---- Abbreviations (for matchup like NOP@WAS) ----
ABBR_TO_NICK: Dict[str, str] = {
    "ATL": "Hawks",
    "BOS": "Celtics",
    "BKN": "Nets",
    "CHA": "Hornets",
    "CHI": "Bulls",
    "CLE": "Cavaliers",
    "DAL": "Mavericks",
    "DEN": "Nuggets",
    "DET": "Pistons",
    "GSW": "Warriors",
    "HOU": "Rockets",
    "IND": "Pacers",
    "LAC": "Clippers",
    "LAL": "Lakers",
    "MEM": "Grizzlies",
    "MIA": "Heat",
    "MIL": "Bucks",
    "MIN": "Timberwolves",
    "NOP": "Pelicans",
    "NYK": "Knicks",
    "OKC": "Thunder",
    "ORL": "Magic",
    "PHI": "76ers",
    "PHX": "Suns",
    "POR": "Trail Blazers",
    "SAC": "Kings",
    "SAS": "Spurs",
    "TOR": "Raptors",
    "UTA": "Jazz",
    "WAS": "Wizards",
}


def canonical_team_name(team_raw: str) -> Optional[str]:
    if not team_raw:
        return None
    key = _norm_team_key(team_raw)
    return TEAM_ALIAS_TO_NICK.get(key)


def matchup_full_name(matchup: Optional[str]) -> Optional[str]:
    if not matchup or "@" not in matchup:
        return None
    away_abbr, home_abbr = matchup.split("@", 1)
    away_nick = ABBR_TO_NICK.get(away_abbr.strip().upper())
    home_nick = ABBR_TO_NICK.get(home_abbr.strip().upper())
    if not away_nick or not home_nick:
        return None
    away_full = TEAM_NICK_TO_FULL.get(away_nick, away_nick)
    home_full = TEAM_NICK_TO_FULL.get(home_nick, home_nick)
    return f"{away_full}@{home_full}"


@dataclass
class Ctx:
    game_date: Optional[str] = None  # MM/DD/YYYY
    game_time: Optional[str] = None  # HH:MM(ET)
    matchup: Optional[str] = None    # AAA@BBB
    team_raw: Optional[str] = None


def _is_date(tok: str) -> bool:
    return bool(re.fullmatch(r"\d{2}/\d{2}/\d{4}", tok))


def _is_time(tok: str) -> bool:
    return bool(re.fullmatch(r"\d{1,2}:\d{2}\(ET\)", tok))


def _is_matchup(tok: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]{2,4}@[A-Z]{2,4}", tok))


def _looks_like_team(tok: str) -> bool:
    # En el PDF viene como CamelCase sin espacios, sin coma.
    if not tok or "," in tok:
        return False
    if tok.upper() in STATUS_SET:
        return False
    if _is_date(tok) or _is_time(tok) or _is_matchup(tok):
        return False
    # Debe tener al menos una mayúscula interna: LosAngelesLakers
    return bool(re.search(r"[A-Z][a-z]+[A-Z]", tok))


def _status_from_token(tok: str) -> Optional[str]:
    if not tok:
        return None
    up = tok.upper()
    if up in STATUS_SET:
        return up
    return None


def _reason_excluded(reason: Optional[str]) -> bool:
    if not reason:
        return False
    up = re.sub(r"[^A-Z-]", "", reason.upper())
    for tok in EXCLUDE_REASON_TOKENS:
        if tok in up:
            return True
    return False


def _http_get(url: str, timeout: int = 25) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="ignore")


def _find_pdf_links(html: str) -> List[str]:
    links = re.findall(r'href=["\']([^"\']+\.pdf[^"\']*)["\']', html, re.IGNORECASE)
    if not links:
        return []
    # Prefer links that look like the injury report
    preferred = [l for l in links if "Injury-Report" in l or "injury-report" in l]
    out = preferred or links
    # de-dupe, preserve order
    seen = set()
    dedup: List[str] = []
    for l in out:
        if l in seen:
            continue
        seen.add(l)
        dedup.append(l)
    return dedup


def _pdf_dt_from_name(name: str) -> Optional[datetime]:
    m = re.search(
        r"Injury-Report_(\d{4}-\d{2}-\d{2})_(\d{2})_(\d{2})(AM|PM)",
        name,
        re.IGNORECASE,
    )
    if not m:
        return None
    date_str, hh, mm, ampm = m.group(1), m.group(2), m.group(3), m.group(4).upper()
    hour = int(hh)
    minute = int(mm)
    if ampm == "PM" and hour != 12:
        hour += 12
    if ampm == "AM" and hour == 12:
        hour = 0
    try:
        return datetime.fromisoformat(f"{date_str}T{hour:02d}:{minute:02d}:00").replace(
            tzinfo=timezone.utc
        )
    except Exception:
        return None


def _select_pdf_url(links: List[str], day: Optional[str]) -> Optional[str]:
    best_url = None
    best_dt: Optional[datetime] = None
    for url in links:
        name = url.split("/")[-1].split("?")[0]
        dt = _pdf_dt_from_name(name)
        if dt is None:
            continue
        if day and dt.date().isoformat() != day:
            continue
        if best_dt is None or dt > best_dt:
            best_dt = dt
            best_url = url
    if best_url:
        return best_url
    if day:
        for url in links:
            if day in url:
                return url
    return links[0] if links else None


def _download_pdf(url: str, cache_dir: Path) -> Optional[Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    name = url.split("/")[-1].split("?")[0]
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    dest = cache_dir / name
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=35) as resp:
        data = resp.read()
    dest.write_bytes(data)
    return dest


def _download_latest_pdf(day: Optional[str]) -> Optional[Path]:
    try:
        html = _http_get(INJURY_PAGE_URL, timeout=25)
    except Exception:
        return None
    links = _find_pdf_links(html)
    if not links:
        return None
    url = _select_pdf_url(links, day)
    if not url:
        return None
    pdf_url = urllib.parse.urljoin(INJURY_PAGE_URL, url)
    try:
        return _download_pdf(pdf_url, INJURY_CACHE_DIR)
    except Exception:
        return None


def parse_pdf_lines(pdf_path: Path) -> List[str]:
    """Extrae texto del PDF y lo divide en líneas.

    NOTA: intentamos usar pdfplumber (mejor para tablas), pero si no está instalado,
    caemos a pypdf/PyPDF2 o PyMuPDF si están disponibles.
    """

    def _extract_with_pdfplumber(p: Path) -> Optional[str]:
        try:
            import pdfplumber  # type: ignore
        except Exception:
            return None
        try:
            out: List[str] = []
            with pdfplumber.open(str(p)) as pdf:
                for page in pdf.pages:
                    out.append(page.extract_text() or "")
            return "\n".join(out)
        except Exception:
            return None

    def _extract_with_pypdf(p: Path) -> Optional[str]:
        # pypdf (preferred) or PyPDF2 fallback
        Reader = None
        try:
            from pypdf import PdfReader as Reader  # type: ignore
        except Exception:
            try:
                from PyPDF2 import PdfReader as Reader  # type: ignore
            except Exception:
                Reader = None
        if Reader is None:
            return None
        try:
            r = Reader(str(p))
            parts: List[str] = []
            for page in r.pages:
                try:
                    parts.append(page.extract_text() or "")
                except Exception:
                    parts.append("")
            return "\n".join(parts)
        except Exception:
            return None

    def _extract_with_pymupdf(p: Path) -> Optional[str]:
        try:
            import fitz  # PyMuPDF
        except Exception:
            return None
        try:
            doc = fitz.open(str(p))
            parts = [doc.load_page(i).get_text("text") for i in range(doc.page_count)]
            doc.close()
            return "\n".join(parts)
        except Exception:
            return None

    # Prefer engines in this order
    engine = None
    text = _extract_with_pdfplumber(pdf_path)
    if text is not None:
        engine = "pdfplumber"
    else:
        text = _extract_with_pypdf(pdf_path)
        if text is not None:
            engine = "pypdf"
        else:
            text = _extract_with_pymupdf(pdf_path)
            if text is not None:
                engine = "pymupdf"

    if text is None:
        raise ModuleNotFoundError(
            "No hay backend para leer PDF. Instala UNO de estos paquetes: "
            "'pdfplumber' (recomendado), o 'pypdf', o 'PyPDF2', o 'PyMuPDF'."
        )

    # log backend used
    if engine:
        print(f"[injuries] PDF backend: {engine}", file=sys.stderr)

    lines: List[str] = []
    for ln in text.splitlines():
        ln = ln.strip()
        if ln:
            lines.append(ln)
    return lines


def _auto_find_pdf(search_dirs: List[Path]) -> Optional[Path]:
    candidates: List[Path] = []
    for d in search_dirs:
        if not d.exists():
            continue
        # Primero: nombre típico del reporte
        candidates.extend(sorted(d.glob("Injury-Report*.pdf")))
    if not candidates:
        for d in search_dirs:
            if d.exists():
                candidates.extend(sorted(d.glob("*.pdf")))
    if not candidates:
        return None
    # Elegimos el más reciente por modified time
    return max(candidates, key=lambda p: p.stat().st_mtime)


def parse_injuries_from_lines(lines: List[str]) -> Tuple[List[dict], List[dict]]:
    """Devuelve (injuries_rows, unmatched_rows)."""
    ctx = Ctx()
    rows: List[dict] = []
    unmatched: List[dict] = []

    for ln in lines:
        # Skip headers/footers
        if "Injury Report" in ln:
            continue
        if ln.startswith("Page "):
            continue
        if ln.startswith("GameDate") or ln.startswith("This report"):
            continue

        toks = ln.split()
        if not toks:
            continue

        i = 0
        # Date
        if i < len(toks) and _is_date(toks[i]):
            ctx.game_date = toks[i]
            i += 1
        # Time
        if i < len(toks) and _is_time(toks[i]):
            ctx.game_time = toks[i]
            i += 1
        # Matchup
        if i < len(toks) and _is_matchup(toks[i]):
            ctx.matchup = toks[i]
            i += 1

        # Team
        if i < len(toks) and _looks_like_team(toks[i]):
            ctx.team_raw = toks[i]
            i += 1

        if i >= len(toks):
            continue

        # NOTYETSUBMITTED puede venir como único token después del team
        st = _status_from_token(toks[i])
        if st == "NOTYETSUBMITTED":
            rows.append(
                {
                    "game_date": ctx.game_date,
                    "game_time": ctx.game_time,
                    "matchup": ctx.matchup,
                    "team_raw": ctx.team_raw,
                    "player": None,
                    "status": "NOTYETSUBMITTED",
                    "reason": None,
                    "raw_line": ln,
                }
            )
            continue

        # Player puede tener coma; si no, intentamos juntar hasta encontrar status
        player_parts: List[str] = []
        status: Optional[str] = None

        # armamos player_parts hasta que encontremos status
        while i < len(toks):
            maybe = _status_from_token(toks[i])
            if maybe and maybe != "NOTYETSUBMITTED":
                status = maybe
                i += 1
                break
            player_parts.append(toks[i])
            i += 1

        if not status:
            # No encontramos status; registramos como unmatched
            unmatched.append({"raw_line": ln, "ctx": ctx.__dict__.copy()})
            continue

        player = " ".join(player_parts).strip() if player_parts else None
        reason = " ".join(toks[i:]).strip() if i < len(toks) else None

        if not ctx.team_raw:
            unmatched.append({"raw_line": ln, "ctx": ctx.__dict__.copy()})
            continue

        rows.append(
            {
                "game_date": ctx.game_date,
                "game_time": ctx.game_time,
                "matchup": ctx.matchup,
                "team_raw": ctx.team_raw,
                "player": player,
                "status": status,
                "reason": reason,
                "raw_line": ln,
            }
        )

    return rows, unmatched


def build_output(rows: List[dict], unmatched: List[dict]) -> dict:
    teams: Dict[str, List[dict]] = {}
    injuries_unknown: List[dict] = []

    for r in rows:
        team_raw = r.get("team_raw")
        team_nick = canonical_team_name(team_raw or "")

        if r.get("status") == "NOTYETSUBMITTED":
            injuries_unknown.append(
                {
                    "team_raw": team_raw,
                    "team": team_nick,
                    "matchup": r.get("matchup"),
                    "matchup_full": matchup_full_name(r.get("matchup")),
                    "note": "NOTYETSUBMITTED",
                    "raw_line": r.get("raw_line"),
                }
            )
            continue

        if not team_nick:
            injuries_unknown.append(
                {
                    "team_raw": team_raw,
                    "team": None,
                    "matchup": r.get("matchup"),
                    "matchup_full": matchup_full_name(r.get("matchup")),
                    "note": "UNMAPPED_TEAM",
                    "raw_line": r.get("raw_line"),
                }
            )
            continue

        entry = {
            "player": r.get("player"),
            "status": (r.get("status") or "").upper(),
            "reason": r.get("reason"),
            "game_date": r.get("game_date"),
            "game_time": r.get("game_time"),
            "matchup": r.get("matchup"),
            "matchup_full": matchup_full_name(r.get("matchup")),
            "source": "pdf",
            "exclude_from_counts": _reason_excluded(r.get("reason")),
        }

        # Guardamos por nickname (lo más común en tu pipeline)
        teams.setdefault(team_nick, []).append(entry)

        # También guardamos por full-name para compatibilidad si tus fixtures traen "Los Angeles Lakers"
        full = TEAM_NICK_TO_FULL.get(team_nick)
        if full:
            teams.setdefault(full, []).append(entry)

    out = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": {
            "type": "pdf",
            "note": "NBA official injury report",
        },
        # build_ready_context solo evalúa bool() de esto, pero guardamos detalle útil
        "injuries_unknown": injuries_unknown,
        "teams": teams,
        "unmatched": unmatched,
    }
    return out


def write_json(path: Path, data: dict) -> None:
    ensure_parent_dir(path)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default=os.getenv("DAY", "").strip(), help="YYYY-MM-DD")
    ap.add_argument("--pdf", default=os.getenv("PDF_PATH", "").strip(), help="Ruta al Injury Report PDF")
    args = ap.parse_args()
    requested_day = (args.day or os.getenv("DAY", "")).strip() or None

    def _infer_day_from_filename(p: Path) -> Optional[str]:
        m = re.search(r"(20\d{2}-\d{2}-\d{2})", p.name)
        if m:
            return m.group(1)
        m = re.search(r"(20\d{2})(\d{2})(\d{2})", p.name)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        return None

    pdf_in = (args.pdf or "").strip()
    pdf_path = Path(pdf_in) if pdf_in else None
    if not pdf_path or not pdf_path.exists():
        render_candidates = [
            Path("/opt/render/project/src"),
            Path("/opt/render/project/src/data/cache/nba_injuries"),
            Path("/tmp"),
        ]
        pdf_path = _auto_find_pdf(
            [
                Path.cwd(),
                Path(__file__).resolve().parent,
                INJURY_CACHE_DIR,
                Path.cwd() / "data" / "cache" / "nba_injuries",
                *render_candidates,
            ]
        )
        if pdf_path and requested_day:
            inferred = _infer_day_from_filename(pdf_path)
            if inferred and inferred != requested_day:
                print(
                    f"[injuries] found PDF for {inferred}, but DAY={requested_day}; ignoring auto-detected file",
                    file=sys.stderr,
                )
                pdf_path = None
        if pdf_path:
            print(f"[injuries] auto-detected PDF: {pdf_path}", file=sys.stderr)
        if not pdf_path:
            download_day = requested_day
            pdf_path = _download_latest_pdf(download_day)
            if pdf_path:
                print(f"[injuries] downloaded PDF: {pdf_path}", file=sys.stderr)
        if not pdf_path:
            if os.getenv("ALLOW_MISSING_PDF", "0").lower() in {"1", "true", "yes"}:
                # Modo tolerante (Render): genera salida vacía pero válida
                day = (args.day or "").strip() or datetime.now().strftime("%Y-%m-%d")
                league = os.getenv("LEAGUE", "nba")
                out_main = os.getenv(
                    "OUT_INJ",
                    os.getenv(
                        "OUT_PATH_INJURIES",
                        versioned_path(league, "features", "injuries", day),
                    ),
                )
                out_legacy_1 = versioned_path(league, "injuries", "injuries", day)
                out_legacy_2 = f"./out/{league}/injuries_{day}.json"
                out = {
                    "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "source": {"type": "pdf", "note": "NBA official injury report (missing)"},
                    "injuries_unknown": [],
                    "teams": {},
                    "unmatched": [],
                }
                write_json(Path(out_main), out)
                if os.getenv("ALSO_WRITE_LEGACY", "1").strip() not in {"0", "false", "False"}:
                    for p in {out_legacy_1, out_legacy_2}:
                        try:
                            write_json(Path(p), out)
                        except Exception:
                            pass
                print(f"[injuries] PDF missing; wrote empty injuries to {out_main}", file=sys.stderr)
                return
            raise SystemExit(
                "No se encontró el Injury Report PDF. "
                "Pasa --pdf 'RUTA\\Injury-Report_YYYY-MM-DD_12_00AM.pdf' "
                "o exporta PDF_PATH. "
                "Para no fallar, usa ALLOW_MISSING_PDF=1."
            )

    day = (args.day or "").strip()
    if not day:
        day = _infer_day_from_filename(pdf_path) or datetime.now().strftime("%Y-%m-%d")
        print(f"[injuries] DAY no fue provisto; usando {day}", file=sys.stderr)

    league = os.getenv("LEAGUE", "nba")

    # Output principal (lo que espera nba_build_ready_context.py)
    out_main = os.getenv(
        "OUT_INJ",
        os.getenv(
            "OUT_PATH_INJURIES",
            versioned_path(league, "features", "injuries", day),
        ),
    )

    # Outputs legacy (por compatibilidad)
    also_legacy = os.getenv("ALSO_WRITE_LEGACY", "1").strip() not in {"0", "false", "False"}
    out_legacy_1 = versioned_path(league, "injuries", "injuries", day)
    out_legacy_2 = f"./out/{league}/injuries_{day}.json"

    lines = parse_pdf_lines(pdf_path)
    rows, unmatched = parse_injuries_from_lines(lines)
    out = build_output(rows, unmatched)

    # escribe principal
    write_json(Path(out_main), out)

    # escribe legacy
    if also_legacy:
        for p in {out_legacy_1, out_legacy_2}:
            try:
                write_json(Path(p), out)
            except Exception:
                # legacy es best-effort
                pass

    # Log rápido (para CI)
    n_teams = len(out.get("teams", {}))
    n_unknown = len(out.get("injuries_unknown", []))
    n_unmatched = len(out.get("unmatched", []))
    print(f"[injuries] wrote: {out_main}")
    if also_legacy:
        print(f"[injuries] also wrote legacy: {out_legacy_1} and {out_legacy_2}")
    print(f"[injuries] teams_keys={n_teams} unknown={n_unknown} unmatched={n_unmatched}")


if __name__ == "__main__":
    main()
