"""nba_fetch_advanced_stats.py

Obtiene ORTG, DRTG y PACE por equipo desde stats.nba.com usando
requests directamente (sin nba_api).

Produce: {"Team Name": {"ortg": x, "drtg": x, "pace": x}, ...}
Guarda en fuente/nba/advanced/advanced_stats_{DAY}.json

Vars de entorno:
  DAY          YYYY-MM-DD  requerida
  LEAGUE       default: nba
  OUT_PATH_ADVANCED  override ruta de salida
  NBA_STATS_TIMEOUT  default: 30
"""

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from nba_utils import nba_stats_headers, resolve_nba_season, versioned_path

DAY = os.getenv("DAY", "").strip()
if not DAY:
    raise SystemExit("Falta DAY en .env (YYYY-MM-DD)")

LEAGUE = os.getenv("LEAGUE", "nba")
TIMEOUT = int(os.getenv("NBA_STATS_TIMEOUT", "30"))
SEASON = resolve_nba_season(DAY)   # e.g. "2025-26"

DEFAULT_PATH = versioned_path(LEAGUE, "advanced", "advanced_stats", DAY)
OUT_PATH = os.getenv("OUT_PATH_ADVANCED", DEFAULT_PATH)

NBA_STATS_URL = "https://stats.nba.com/stats/leaguedashteamstats"

PARAMS = {
    "Season": SEASON,
    "SeasonType": "Regular Season",
    "MeasureType": "Advanced",
    "PerMode": "PerGame",
    "LeagueID": "00",
    "DateFrom": "",
    "DateTo": "",
    "GameScope": "",
    "GameSegment": "",
    "LastNGames": "0",
    "Location": "",
    "Month": "0",
    "OpponentTeamID": "0",
    "Outcome": "",
    "PORound": "0",
    "PaceAdjust": "N",
    "PlusMinus": "N",
    "Rank": "N",
    "ShotClockRange": "",
    "VsConference": "",
    "VsDivision": "",
    "Period": "0",
    "Conference": "",
    "Division": "",
    "TwoWay": "0",
}


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or str(v).strip() in ("", "—", "-"):
            return None
        return float(str(v).strip())
    except Exception:
        return None


def fetch_advanced_stats() -> Dict[str, Dict[str, Any]]:
    headers = nba_stats_headers()
    for attempt in range(1, 4):
        try:
            r = requests.get(NBA_STATS_URL, headers=headers, params=PARAMS, timeout=TIMEOUT)
            if r.status_code == 429:
                wait = 15 * attempt
                print(f"[advanced] rate-limited (429), esperando {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code} desde stats.nba.com")
            data = r.json()
            break
        except Exception as exc:
            if attempt < 3:
                time.sleep(5 * attempt)
                continue
            print(f"[advanced] no se pudo descargar stats.nba.com: {exc}")
            return {}

    result_set = data.get("resultSets", [{}])[0]
    col_names = [c.lower() for c in result_set.get("headers", [])]
    rows = result_set.get("rowSet", [])

    if not col_names or not rows:
        print("[advanced] respuesta vacia de stats.nba.com")
        return {}

    idx: Dict[str, int] = {}
    for i, col in enumerate(col_names):
        if col == "team_name":
            idx["team"] = i
        elif col in ("e_off_rating", "off_rating"):
            idx.setdefault("ortg", i)
        elif col in ("e_def_rating", "def_rating"):
            idx.setdefault("drtg", i)
        elif col == "pace":
            idx["pace"] = i

    if "team" not in idx:
        print(f"[advanced] columna team_name no encontrada. Columnas: {col_names[:10]}")
        return {}

    result: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        team_name = row[idx["team"]] if idx.get("team") is not None else None
        if not team_name:
            continue
        result[team_name] = {
            "ortg": _safe_float(row[idx["ortg"]]) if "ortg" in idx else None,
            "drtg": _safe_float(row[idx["drtg"]]) if "drtg" in idx else None,
            "pace": _safe_float(row[idx["pace"]]) if "pace" in idx else None,
        }

    print(f"[advanced] {len(result)} equipos obtenidos desde stats.nba.com (season={SEASON})")
    return result


def main() -> None:
    data = fetch_advanced_stats()

    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(OUT_PATH).write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"OK -> {OUT_PATH} (teams={len(data)})")


if __name__ == "__main__":
    main()
