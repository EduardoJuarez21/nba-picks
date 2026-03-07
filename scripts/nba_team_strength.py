"""nba_team_strength.py

Obtiene estadisticas de equipos NBA desde Tank01 Fantasy Stats (RapidAPI).
- Season averages (ppg/oppg):  getNBATeams
- Home/Away splits + last-10:  getNBATeamSchedule  (1 call por equipo del dia)

Produce el mismo formato JSON que consume nba_build_ready_context.py.

Vars de entorno:
  RAPIDAPI_KEY              requerida
  DAY                       YYYY-MM-DD  requerida
  LEAGUE                    default: nba
  FIXTURES_PATH             para leer equipos del dia
  OUT_PATH_TEAM_STRENGTH    override ruta de salida
  NBA_SPLITS_LAST_N         cuantos juegos para last-N (default: 10)
"""

import json
import os
from datetime import date
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional, Tuple

import requests

from nba_utils import versioned_path

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "").strip()
RAPIDAPI_HOST = "tank01-fantasy-stats.p.rapidapi.com"

LEAGUE = os.getenv("LEAGUE", "nba")
DAY = os.getenv("DAY", "").strip()
if not DAY:
    raise SystemExit("Falta DAY en .env (YYYY-MM-DD)")
if not RAPIDAPI_KEY:
    raise SystemExit("Falta RAPIDAPI_KEY en .env")

FIXTURES_PATH = os.getenv("FIXTURES_PATH", versioned_path(LEAGUE, "fixtures", "fixtures", DAY))
OUT_PATH = os.getenv("OUT_PATH_TEAM_STRENGTH", versioned_path(LEAGUE, "team_strength", "team_strength", DAY))
LAST_N = int(os.getenv("NBA_SPLITS_LAST_N", "10"))

# Año de temporada para getNBATeamSchedule (2025-26 -> "2026")
_DAY_DATE = date.fromisoformat(DAY)
SEASON_YEAR = str(_DAY_DATE.year if _DAY_DATE.month >= 7 else _DAY_DATE.year)
DAY_INT = int(DAY.replace("-", ""))   # Para comparar con gameDate YYYYMMDD


def _api_get(url: str, params: Dict[str, Any] = {}) -> Any:
    headers = {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
    }
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Tank01 API error {r.status_code}: {r.text[:400]}")
    data = r.json()
    return data.get("body", data) if isinstance(data, dict) else data


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or str(v).strip() in ("", "—"):
            return None
        return float(str(v).strip())
    except Exception:
        return None


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None or str(v).strip() == "":
            return None
        return int(str(v).strip())
    except Exception:
        return None


def _avg(vals: List[float]) -> Optional[float]:
    return round(mean(vals), 2) if vals else None


def _read_fixtures(path: str) -> List[Dict[str, Any]]:
    p = Path(path)
    return json.loads(p.read_text(encoding="utf-8")) if p.is_file() else []


def _fetch_all_teams() -> Dict[int, Dict[str, Any]]:
    """Devuelve {teamID: team_record} con ppg, oppg, teamAbv, etc."""
    body = _api_get(f"https://{RAPIDAPI_HOST}/getNBATeams")
    if not isinstance(body, list):
        return {}
    return {int(t["teamID"]): t for t in body if t.get("teamID")}


def _fetch_schedule(team_id: int) -> List[Dict[str, Any]]:
    """Devuelve lista de partidos del equipo en la temporada actual."""
    body = _api_get(
        f"https://{RAPIDAPI_HOST}/getNBATeamSchedule",
        params={"teamID": str(team_id), "season": SEASON_YEAR},
    )
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        return body.get("schedule", body.get("games", []))
    return []


def _compute_splits(
    schedule: List[Dict[str, Any]], team_abbr: str
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    Calcula splits de temporada y last-N para un equipo.
    Devuelve (season_splits, last_n_splits, recent_games).

    season_splits / last_n_splits:
      {"home_pf": [...], "home_pa": [...], "away_pf": [...], "away_pa": [...]}
    """
    completed = [
        g for g in schedule
        if g.get("gameStatus") == "Completed"
        and g.get("homePts")
        and g.get("awayPts")
        and _safe_int(g.get("gameDate", "99999999")) <= DAY_INT
    ]
    # Ordenar por fecha asc para tomar los ultimos N
    completed.sort(key=lambda g: g.get("gameDate", "0"))

    home_pf, home_pa = [], []
    away_pf, away_pa = [], []
    recent_games = []

    for g in completed:
        h_pts = _safe_float(g.get("homePts"))
        a_pts = _safe_float(g.get("awayPts"))
        if h_pts is None or a_pts is None:
            continue
        is_home = g.get("home", "").upper() == team_abbr.upper()
        if is_home:
            home_pf.append(h_pts)
            home_pa.append(a_pts)
        else:
            away_pf.append(a_pts)
            away_pa.append(h_pts)

        recent_games.append({
            "date": g.get("gameDate"),
            "opponent": g.get("away") if is_home else g.get("home"),
            "home": is_home,
            "pf": h_pts if is_home else a_pts,
            "pa": a_pts if is_home else h_pts,
            "result": g.get("homeResult") if is_home else g.get("awayResult"),
        })

    # Last-N: ultimos LAST_N partidos (home+away mezclados)
    last_n = completed[-LAST_N:]
    ln_pf, ln_pa = [], []
    for g in last_n:
        h_pts = _safe_float(g.get("homePts"))
        a_pts = _safe_float(g.get("awayPts"))
        if h_pts is None or a_pts is None:
            continue
        is_home = g.get("home", "").upper() == team_abbr.upper()
        ln_pf.append(h_pts if is_home else a_pts)
        ln_pa.append(a_pts if is_home else h_pts)

    season_splits = {
        "home_pf": home_pf, "home_pa": home_pa,
        "away_pf": away_pf, "away_pa": away_pa,
    }
    last_n_splits = {"pf": ln_pf, "pa": ln_pa, "games": len(last_n)}

    return season_splits, last_n_splits, recent_games[-5:]   # ultimos 5 como referencia


def main() -> None:
    fixtures = _read_fixtures(FIXTURES_PATH)

    # Equipos del dia: {tank01_team_id: full_name}
    team_ids_wanted: Dict[int, str] = {}
    for fx in fixtures:
        for id_key, name_key in (("home_team_id", "home"), ("away_team_id", "away")):
            tid = fx.get(id_key)
            if tid is not None:
                team_ids_wanted[int(tid)] = fx.get(name_key) or ""

    all_teams = _fetch_all_teams()

    out: List[Dict[str, Any]] = []
    for tid, team_name in sorted(team_ids_wanted.items()):
        t = all_teams.get(tid, {})
        team_abbr = t.get("teamAbv", "")
        ppg = _safe_float(t.get("ppg"))
        oppg = _safe_float(t.get("oppg"))
        wins = _safe_int(t.get("wins"))
        losses = _safe_int(t.get("loss"))
        games_total = (wins + losses) if (wins is not None and losses is not None) else None

        # Splits desde el historial de partidos
        splits: Dict[str, Any] = {}
        last_n: Dict[str, Any] = {}
        recent: List[Any] = []
        if team_abbr:
            try:
                schedule = _fetch_schedule(tid)
                splits, last_n, recent = _compute_splits(schedule, team_abbr)
                print(f"[team_strength] {team_name}: {len(splits['home_pf'])}H / {len(splits['away_pf'])}A games")
            except Exception as exc:
                print(f"[team_strength] [warn] schedule error para {team_name}: {exc}")

        # ORTG/DRTG aproximados desde ppg/oppg con PACE baseline = 100
        # (NBA pace real ≈ 98-102; usando ppg directamente como proxy de ORTG)
        NBA_PACE = 100.0
        home_pf_avg = _avg(splits.get("home_pf", []))
        home_pa_avg = _avg(splits.get("home_pa", []))
        away_pf_avg = _avg(splits.get("away_pf", []))
        away_pa_avg = _avg(splits.get("away_pa", []))
        last10_pf_avg = _avg(last_n.get("pf", []))
        last10_pa_avg = _avg(last_n.get("pa", []))

        out.append(
            {
                "team_id": tid,
                "team": team_name or f"{t.get('teamCity','')} {t.get('teamName','')}".strip(),
                "games": games_total,
                "pf_pg": ppg,
                "pa_pg": oppg,
                "ortg": ppg,
                "drtg": oppg,
                "pace": NBA_PACE,
                # Splits home/away calculados desde schedule
                "home_pf_pg": home_pf_avg,
                "home_pa_pg": home_pa_avg,
                "home_ortg": home_pf_avg,
                "home_drtg": home_pa_avg,
                "home_pace": NBA_PACE,
                "away_pf_pg": away_pf_avg,
                "away_pa_pg": away_pa_avg,
                "away_ortg": away_pf_avg,
                "away_drtg": away_pa_avg,
                "away_pace": NBA_PACE,
                # Last-N
                "last10_games": last_n.get("games"),
                "last10_pf_pg": last10_pf_avg,
                "last10_pa_pg": last10_pa_avg,
                "last10_ortg": last10_pf_avg,
                "last10_drtg": last10_pa_avg,
                "last10_pace": NBA_PACE,
                "recent_games": recent,
                "stats_unknown": not bool(t),
                "as_of": DAY,
            }
        )

    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(OUT_PATH).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK -> {OUT_PATH} (items={len(out)})")


if __name__ == "__main__":
    main()
