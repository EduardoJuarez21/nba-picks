"""nba_fetch_fixtures.py

Obtiene los partidos NBA del dia desde Tank01 Fantasy Stats (RapidAPI)
endpoint: getNBAGamesForDate  host: tank01-fantasy-stats.p.rapidapi.com

Produce el mismo formato JSON que consumia la version anterior:
[
  {
    "game_id": "...",
    "utc_kickoff": "...",
    "status": "...",
    "league": "NBA",
    "season": "...",
    "home_team_id": ...,
    "away_team_id": ...,
    "home": "...",
    "away": "..."
  }
]

Vars de entorno:
  RAPIDAPI_KEY     requerida
  DAY              YYYY-MM-DD  requerida
  LEAGUE           default: nba
  FIXTURES_PATH    override ruta de salida
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from nba_utils import versioned_path

# Abreviatura Tank01 -> nombre completo que usa The Odds API
ABBR_TO_FULL: Dict[str, str] = {
    "ATL": "Atlanta Hawks", "BOS": "Boston Celtics", "BKN": "Brooklyn Nets",
    "CHA": "Charlotte Hornets", "CHI": "Chicago Bulls", "CLE": "Cleveland Cavaliers",
    "DAL": "Dallas Mavericks", "DEN": "Denver Nuggets", "DET": "Detroit Pistons",
    "GSW": "Golden State Warriors", "GS": "Golden State Warriors",
    "HOU": "Houston Rockets", "IND": "Indiana Pacers",
    "LAC": "Los Angeles Clippers", "LAL": "Los Angeles Lakers",
    "MEM": "Memphis Grizzlies", "MIA": "Miami Heat", "MIL": "Milwaukee Bucks",
    "MIN": "Minnesota Timberwolves", "NOP": "New Orleans Pelicans",
    "NO": "New Orleans Pelicans", "NYK": "New York Knicks", "NY": "New York Knicks",
    "OKC": "Oklahoma City Thunder", "ORL": "Orlando Magic",
    "PHI": "Philadelphia 76ers", "PHX": "Phoenix Suns", "PHO": "Phoenix Suns",
    "POR": "Portland Trail Blazers", "SAC": "Sacramento Kings",
    "SAS": "San Antonio Spurs", "SA": "San Antonio Spurs",
    "TOR": "Toronto Raptors", "UTA": "Utah Jazz", "WAS": "Washington Wizards",
}

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "").strip()
RAPIDAPI_HOST = "tank01-fantasy-stats.p.rapidapi.com"
RAPIDAPI_URL = f"https://{RAPIDAPI_HOST}/getNBAGamesForDate"

LEAGUE = os.getenv("LEAGUE", "nba")
DAY = os.getenv("DAY", "").strip()
if not DAY:
    raise SystemExit("Falta DAY en .env (YYYY-MM-DD)")
if not RAPIDAPI_KEY:
    raise SystemExit("Falta RAPIDAPI_KEY en .env")

DEFAULT_PATH = versioned_path(LEAGUE, "fixtures", "fixtures", DAY)
OUT_PATH = os.getenv("FIXTURES_PATH", DEFAULT_PATH)


def _game_date_param(day: str) -> str:
    """Convierte YYYY-MM-DD a YYYYMMDD que espera Tank01."""
    return day.replace("-", "")


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(str(value))
    except Exception:
        return None


def _kickoff_utc(epoch: Any, game_date: str) -> str:
    """Construye ISO UTC. Usa epoch si viene, si no devuelve fecha con hora aproximada."""
    if epoch:
        try:
            from datetime import datetime, timezone
            ts = float(epoch)
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0)
            return dt.isoformat().replace("+00:00", "Z")
        except Exception:
            pass
    # Fallback: solo fecha
    safe = (game_date or DAY).replace("/", "-")
    if len(safe) == 8 and safe.isdigit():
        safe = f"{safe[:4]}-{safe[4:6]}-{safe[6:]}"
    return f"{safe}T00:00:00Z"


def _parse_games(data: Any) -> List[Dict[str, Any]]:
    """Extrae la lista de partidos del payload de Tank01."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        body = data.get("body")
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            # {"body": {"gameDate": ..., "games": [...]}}
            games = body.get("games")
            if isinstance(games, list):
                return games
    return []


def _fetch_games() -> List[Dict[str, Any]]:
    headers = {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
    }
    params = {"gameDate": _game_date_param(DAY)}
    r = requests.get(RAPIDAPI_URL, headers=headers, params=params, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Tank01 API error {r.status_code}: {r.text[:400]}")
    return _parse_games(r.json())


def _resolve_name(abbr: str) -> str:
    """Convierte abreviatura Tank01 a nombre completo."""
    return ABBR_TO_FULL.get(abbr.upper(), abbr)


def _build_fixtures(games: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    fixtures = []
    for g in games:
        home_abbr = g.get("home") or ""
        away_abbr = g.get("away") or ""
        home_name = g.get("homeTeam") or _resolve_name(home_abbr)
        away_name = g.get("awayTeam") or _resolve_name(away_abbr)

        epoch = g.get("gameTime_epoch") or g.get("gameDateTimeEpoch")
        game_date = g.get("gameDate") or _game_date_param(DAY)
        # Tank01 puede dar la temporada dentro del gameID (ej. "20260306_LAL@BOS")
        season = g.get("season") or g.get("seasonType") or ""

        fixtures.append(
            {
                "game_id": g.get("gameID") or g.get("id"),
                "utc_kickoff": _kickoff_utc(epoch, game_date),
                "status": g.get("gameStatus") or g.get("gameStatusText") or "Scheduled",
                "league": "NBA",
                "season": str(season),
                "home_team_id": _safe_int(g.get("teamIDHome") or g.get("homeTeamID")),
                "away_team_id": _safe_int(g.get("teamIDAway") or g.get("awayTeamID")),
                "home": home_name,
                "away": away_name,
            }
        )
    return fixtures


def main() -> None:
    games = _fetch_games()
    fixtures = _build_fixtures(games)

    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(OUT_PATH).write_text(
        json.dumps(fixtures, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"OK -> {OUT_PATH} (items={len(fixtures)}, source=tank01)")


if __name__ == "__main__":
    main()
