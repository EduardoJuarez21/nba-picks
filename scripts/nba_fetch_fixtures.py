import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List

import requests
from nba_api.stats.endpoints import scoreboardv3

from nba_utils import nba_stats_headers, versioned_path

LEAGUE = os.getenv("LEAGUE", "nba")
DAY = os.getenv("DAY", "").strip()
if not DAY:
    raise SystemExit("Falta DAY en .env (YYYY-MM-DD)")

DEFAULT_PATH = versioned_path(LEAGUE, "fixtures", "fixtures", DAY)
OUT_PATH = os.getenv("FIXTURES_PATH", DEFAULT_PATH)
NBA_STATS_TIMEOUT = int(os.getenv("NBA_STATS_TIMEOUT", "60"))
NBA_STATS_RETRIES = max(1, int(os.getenv("NBA_STATS_RETRIES", "3")))
NBA_STATS_RETRY_SLEEP = float(os.getenv("NBA_STATS_RETRY_SLEEP", "2.0"))
NBA_FIXTURES_ESPN_FALLBACK = os.getenv("NBA_FIXTURES_ESPN_FALLBACK", "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
NBA_STATS_HEADERS = nba_stats_headers()


def _team_name(team: Dict[str, Any]) -> str:
    city = team.get("TEAM_CITY_NAME") or team.get("teamCity") or ""
    name = team.get("TEAM_NICKNAME") or team.get("teamName") or ""
    if city and name:
        return f"{city} {name}".strip()
    return (
        team.get("TEAM_NAME")
        or team.get("teamName")
        or team.get("TEAM_ABBREVIATION")
        or team.get("teamTricode")
        or ""
    )

def _kickoff_utc(game_date: str | None) -> str:
    if not game_date:
        return ""
    raw = str(game_date)
    if "T" in raw:
        if raw.endswith("Z"):
            return raw
        return f"{raw}Z"
    return f"{raw}T00:00:00Z"


def _row_to_map(headers: List[Any], row: List[Any]) -> Dict[str, Any]:
    size = min(len(headers), len(row))
    return {str(headers[i]): row[i] for i in range(size)}


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(str(value))
    except Exception:
        return None


def _fetch_stats_scoreboard() -> Dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, NBA_STATS_RETRIES + 1):
        try:
            return scoreboardv3.ScoreboardV3(
                game_date=DAY,
                headers=NBA_STATS_HEADERS,
                timeout=NBA_STATS_TIMEOUT,
            ).get_dict()
        except Exception as exc:
            last_error = exc
            if attempt < NBA_STATS_RETRIES:
                wait_s = NBA_STATS_RETRY_SLEEP * attempt
                print(
                    f"[warn] nba stats timeout/error attempt {attempt}/{NBA_STATS_RETRIES}: "
                    f"{exc}. retry in {wait_s:.1f}s"
                )
                time.sleep(wait_s)
    if last_error:
        raise last_error
    raise RuntimeError("No se pudo consultar NBA Stats")


def _fixtures_from_stats(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    # ScoreboardV3 raw payload has homeTeam/awayTeam directly under scoreboard.games.
    scoreboard = data.get("scoreboard") or {}
    games = scoreboard.get("games") or []
    if games:
        fixtures: List[Dict[str, Any]] = []
        for g in games:
            home = g.get("homeTeam") or {}
            away = g.get("awayTeam") or {}
            game_date = g.get("gameTimeUTC") or DAY
            fixtures.append(
                {
                    "game_id": g.get("gameId"),
                    "utc_kickoff": _kickoff_utc(game_date),
                    "status": g.get("gameStatusText") or g.get("gameStatus"),
                    "league": "NBA",
                    "season": "",
                    "home_team_id": _safe_int(home.get("teamId")),
                    "away_team_id": _safe_int(away.get("teamId")),
                    "home": _team_name(home),
                    "away": _team_name(away),
                }
            )
        return fixtures

    # Backward-compatible parser for tabular/legacy payload shapes.
    result_sets = data.get("resultSets") or []
    sets_by_name = {rs.get("name"): rs for rs in result_sets if isinstance(rs, dict)}
    game_header = sets_by_name.get("GameHeader") or {}
    line_score = sets_by_name.get("LineScore") or {}

    gh_headers = game_header.get("headers") or []
    gh_rows = game_header.get("rowSet") or []
    ls_headers = line_score.get("headers") or []
    ls_rows = line_score.get("rowSet") or []

    teams_by_id: Dict[int, Dict[str, Any]] = {}
    for row in ls_rows:
        row_map = _row_to_map(ls_headers, row)
        tid = row_map.get("TEAM_ID")
        if tid is not None:
            teams_by_id[int(tid)] = row_map

    fixtures: List[Dict[str, Any]] = []
    for row in gh_rows:
        g = _row_to_map(gh_headers, row)
        home_id = _safe_int(g.get("HOME_TEAM_ID"))
        away_id = _safe_int(g.get("VISITOR_TEAM_ID"))
        home = teams_by_id.get(home_id) if home_id is not None else {}
        away = teams_by_id.get(away_id) if away_id is not None else {}
        game_date = g.get("GAME_DATE_EST") or DAY
        fixtures.append(
            {
                "game_id": g.get("GAME_ID"),
                "utc_kickoff": _kickoff_utc(game_date),
                "status": g.get("GAME_STATUS_TEXT") or g.get("GAME_STATUS_ID"),
                "league": "NBA",
                "season": g.get("SEASON") or "",
                "home_team_id": home_id,
                "away_team_id": away_id,
                "home": _team_name(home),
                "away": _team_name(away),
            }
        )
    return fixtures


def _fixtures_from_espn() -> List[Dict[str, Any]]:
    dates = DAY.replace("-", "")
    r = requests.get(ESPN_SCOREBOARD, params={"dates": dates}, timeout=NBA_STATS_TIMEOUT)
    if r.status_code >= 400:
        raise RuntimeError(f"Error ESPN scoreboard: {r.status_code} {r.text[:200]}")
    data = r.json()

    fixtures: List[Dict[str, Any]] = []
    for ev in data.get("events") or []:
        comps = ev.get("competitions") or []
        if not comps:
            continue
        comp = comps[0]
        competitors = comp.get("competitors") or []
        home = next((c for c in competitors if c.get("homeAway") == "home"), {}) or {}
        away = next((c for c in competitors if c.get("homeAway") == "away"), {}) or {}
        home_team = (home.get("team") or {}).get("displayName") or ""
        away_team = (away.get("team") or {}).get("displayName") or ""
        if not home_team or not away_team:
            continue

        status_obj = (comp.get("status") or {}).get("type") or {}
        status = status_obj.get("shortDetail") or status_obj.get("description") or ""
        season = (ev.get("season") or {}).get("year") or ""
        fixtures.append(
            {
                "game_id": ev.get("id"),
                "utc_kickoff": _kickoff_utc(ev.get("date") or DAY),
                "status": status,
                "league": "NBA",
                "season": season,
                "home_team_id": _safe_int((home.get("team") or {}).get("id")),
                "away_team_id": _safe_int((away.get("team") or {}).get("id")),
                "home": home_team,
                "away": away_team,
            }
        )
    return fixtures

def main() -> None:
    source = "nba_stats"
    try:
        stats_data = _fetch_stats_scoreboard()
        fixtures = _fixtures_from_stats(stats_data)
        del stats_data  # Free memory
    except Exception as stats_error:
        if not NBA_FIXTURES_ESPN_FALLBACK:
            raise SystemExit(f"Fallo NBA stats: {stats_error}") from stats_error
        print(f"[warn] nba stats no disponible para {DAY}: {stats_error}")
        fixtures = _fixtures_from_espn()
        source = "espn"

    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(OUT_PATH).write_text(json.dumps(fixtures, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK -> {OUT_PATH} (items={len(fixtures)}, source={source})")


if __name__ == "__main__":
    main()
