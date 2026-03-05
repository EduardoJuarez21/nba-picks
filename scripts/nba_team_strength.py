import json
import os
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from nba_api.stats.endpoints import leaguedashteamstats, teamdashboardbygeneralsplits

from nba_utils import nba_stats_headers, resolve_nba_season, versioned_path

LEAGUE = os.getenv("LEAGUE", "nba")
DAY = os.getenv("DAY", "").strip()
if not DAY:
    raise SystemExit("Falta DAY en .env (YYYY-MM-DD)")

SEASON = resolve_nba_season(DAY)

FIXTURES_PATH = os.getenv("FIXTURES_PATH", versioned_path(LEAGUE, "fixtures", "fixtures", DAY))
DEFAULT_PATH = versioned_path(LEAGUE, "team_strength", "team_strength", DAY)
OUT_PATH = os.getenv("OUT_PATH_TEAM_STRENGTH", DEFAULT_PATH)
NBA_STATS_HEADERS = nba_stats_headers()


def _read_fixtures(path: str) -> List[Dict[str, Any]]:
    if not Path(path).is_file():
        return []
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _fetch_team_stats(
    location: str | None = None,
    last_n_games: int | None = None,
    measure_type: str | None = None,
) -> Dict[int, Dict[str, Any]]:
    params: Dict[str, Any] = {
        "season": SEASON,
        "per_mode_detailed": "PerGame",
    }
    if measure_type:
        params["measure_type_detailed_defense"] = measure_type
    if location:
        params["location_nullable"] = location
    if last_n_games is not None:
        params["last_n_games"] = str(last_n_games)
    try:
        stats = leaguedashteamstats.LeagueDashTeamStats(
            headers=NBA_STATS_HEADERS,
            **params,
        ).get_dict()
    except Exception:
        return {}

    if not stats:
        return {}

    result_sets = stats.get("resultSets")
    if isinstance(result_sets, list) and result_sets:
        result = result_sets[0]
    elif isinstance(stats.get("resultSet"), dict):
        result = stats.get("resultSet") or {}
    else:
        return {}
    headers = result.get("headers") or []
    rows = result.get("rowSet") or []
    by_team: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        row_map = {headers[i]: row[i] for i in range(len(headers))}
        team_id = row_map.get("TEAM_ID")
        if team_id is None:
            continue
        by_team[int(team_id)] = row_map
    return by_team


def _fetch_team_splits(
    team_id: int, last_n_games: int | None = None, timeout_s: int | None = None
) -> Dict[str, Dict[str, Any]]:
    if os.getenv("NBA_SPLITS_ENABLED", "1") != "1":
        return {}
    if timeout_s is None:
        timeout_s = int(os.getenv("NBA_SPLITS_TIMEOUT", "6"))
    params: Dict[str, Any] = {
        "team_id": team_id,
        "season": SEASON,
        "per_mode_detailed": "PerGame",
    }
    if last_n_games is not None:
        params["last_n_games"] = str(last_n_games)
    try:
        stats = teamdashboardbygeneralsplits.TeamDashboardByGeneralSplits(
            headers=NBA_STATS_HEADERS,
            **params,
            timeout=timeout_s,
        ).get_dict()
    except Exception:
        return {}
    if not stats:
        return {}
    result_sets = stats.get("resultSets") or []
    splits = {}
    for rs in result_sets:
        # OverallTeamDashboard: GROUP_VALUE = "Overall"
        # LocationTeamDashboard: GROUP_VALUE = "Home" | "Road"  (no "Away")
        if rs.get("name") not in ("OverallTeamDashboard", "LocationTeamDashboard"):
            continue
        headers = rs.get("headers") or []
        rows = rs.get("rowSet") or []
        for row in rows:
            row_map = {headers[i]: row[i] for i in range(len(headers))}
            loc = (row_map.get("GROUP_VALUE") or "").upper()
            if loc == "ROAD":
                loc = "AWAY"
            splits[loc] = row_map
    return splits


def _opp_pts(row: Dict[str, Any]) -> float | None:
    if not row:
        return None
    opp = row.get("OPP_PTS")
    if opp is not None:
        return opp
    pts = row.get("PTS")
    plus_minus = row.get("PLUS_MINUS")
    if pts is None or plus_minus is None:
        return None
    try:
        return float(pts) - float(plus_minus)
    except Exception:
        return None


def main() -> None:
    fixtures = _read_fixtures(FIXTURES_PATH)
    team_ids = set()
    team_names: Dict[int, str] = {}
    for fx in fixtures:
        hid = fx.get("home_team_id")
        aid = fx.get("away_team_id")
        if hid:
            team_ids.add(int(hid))
            team_names[int(hid)] = fx.get("home") or ""
        if aid:
            team_ids.add(int(aid))
            team_names[int(aid)] = fx.get("away") or ""

    try:
        stats = leaguedashteamstats.LeagueDashTeamStats(
            headers=NBA_STATS_HEADERS,
            season=SEASON,
            per_mode_detailed="PerGame",
            measure_type_detailed_defense="Base",
        ).get_dict()
    except Exception as e:
        print(f"[team_strength] [warn] nba stats no disponible: {e}")
        stats = None
    result = (stats.get("resultSets") or [])[0] if stats else {}
    headers = result.get("headers") or []
    rows = result.get("rowSet") or []

    by_team: Dict[int, Dict[str, Any]] = {}
    row_maps: List[Dict[str, Any]] = []
    for row in rows:
        row_map = {headers[i]: row[i] for i in range(len(headers))}
        row_maps.append(row_map)
        team_id = row_map.get("TEAM_ID")
        if team_id is None:
            continue
        by_team[int(team_id)] = row_map

    # Stats avanzados (ORTG/DRTG/PACE) y últimos 10 (base + advanced).
    by_team_adv = _fetch_team_stats(measure_type="Advanced")
    by_team_last10 = _fetch_team_stats(last_n_games=10, measure_type="Base")
    # Advanced para last10: ORtg/DRtg/PACE recientes (Base no los incluye)
    by_team_last10_adv = _fetch_team_stats(last_n_games=10, measure_type="Advanced")

    # Debug opcional: imprime headers y filas completas.
    if os.getenv("NBA_DEBUG_TEAM_STATS") == "1":
        print(json.dumps({"headers": headers, "rows": row_maps}, ensure_ascii=False, indent=2))
    
    # Free memory
    del stats
    del result
    del row_maps
    del rows

    out: List[Dict[str, Any]] = []
    for tid in sorted(team_ids):
        row = by_team.get(tid, {})
        row_adv = by_team_adv.get(tid, {})
        row_last10 = by_team_last10.get(tid, {})
        row_last10_adv = by_team_last10_adv.get(tid, {})
        splits = _fetch_team_splits(tid)

        split_home = splits.get("HOME", {}) or row
        split_away = splits.get("AWAY", {}) or row
        split_l10 = row_last10 or row

        pts = row.get("PTS")
        opp_pts = _opp_pts(row)
        home_pts = split_home.get("PTS")
        home_opp_pts = _opp_pts(split_home)
        away_pts = split_away.get("PTS")
        away_opp_pts = _opp_pts(split_away)
        l10_pts = split_l10.get("PTS")
        l10_opp_pts = _opp_pts(split_l10)
        out.append(
            {
                "team_id": tid,
                "team": team_names.get(tid) or row.get("TEAM_NAME", ""),
                "games": row.get("GP"),
                "pf_pg": pts,
                "pa_pg": opp_pts,
                "ortg": row_adv.get("OFF_RATING"),
                "drtg": row_adv.get("DEF_RATING"),
                "pace": row_adv.get("PACE"),
                "home_pf_pg": home_pts,
                "home_pa_pg": home_opp_pts,
                "home_ortg": split_home.get("OFF_RATING") or row_adv.get("OFF_RATING"),
                "home_drtg": split_home.get("DEF_RATING") or row_adv.get("DEF_RATING"),
                "home_pace": split_home.get("PACE") or row_adv.get("PACE"),
                "away_pf_pg": away_pts,
                "away_pa_pg": away_opp_pts,
                "away_ortg": split_away.get("OFF_RATING") or row_adv.get("OFF_RATING"),
                "away_drtg": split_away.get("DEF_RATING") or row_adv.get("DEF_RATING"),
                "away_pace": split_away.get("PACE") or row_adv.get("PACE"),
                "last10_games": split_l10.get("GP"),
                "last10_pf_pg": l10_pts,
                "last10_pa_pg": l10_opp_pts,
                "last10_ortg": row_last10_adv.get("OFF_RATING"),
                "last10_drtg": row_last10_adv.get("DEF_RATING"),
                "last10_pace": row_last10_adv.get("PACE"),
                "recent_games": [],
                "stats_unknown": not bool(row),
                "as_of": date.fromisoformat(DAY).isoformat(),
            }
        )
        del splits  # Free inside loop

    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(OUT_PATH).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK -> {OUT_PATH} (items={len(out)})")


if __name__ == "__main__":
    main()
