"""nba_build_ready_context_fixed_v2.py

Versión tolerante a diferentes salidas de injuries.

Qué mejora vs tu `nba_build_ready_context.py` original:
- Busca el archivo de lesiones en varias rutas (features/, injuries/ legacy, ./out/)
- Si `injuries['teams']` viene como lista (formato viejo), lo convierte a dict.
- Hace lookup por alias de equipo ("Los Angeles Lakers" -> "Lakers", "Portland Trail Blazers" -> "Trail Blazers", etc.)

No cambia la estructura de salida de ready_context; sólo hace más robusta la carga de lesiones.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from nba_utils import versioned_path, ensure_parent_dir

LEAGUE = os.getenv("LEAGUE", "nba")
DAY = os.getenv("DAY", "").strip()
if not DAY:
    raise SystemExit("Falta DAY en .env (YYYY-MM-DD)")

FIXTURES_PATH = os.getenv("FIXTURES_PATH", versioned_path(LEAGUE, "features", "fixtures", DAY))
ODDS_PATH = os.getenv("OUT_PATH_ODDS", versioned_path(LEAGUE, "features", "odds", DAY))
TEAM_STRENGTH_PATH = os.getenv(
    "OUT_PATH_TEAM_STRENGTH",
    versioned_path(LEAGUE, "team_strength", "team_strength", DAY),
)

# Si INJ_PATH no existe, haremos fallback a rutas alternativas.
INJ_PATH_ENV = os.getenv("INJ_PATH", "").strip() or None
OUT_PATH = os.getenv("OUT_PATH_FEATURES", versioned_path(LEAGUE, "features", "ready_context", DAY))

# ---------------------------------
# Aliases de equipos (full/camelcase -> nickname)
# ---------------------------------
# Mantener en sync con el parser de injuries.
_TEAM_ALIASES = {
    "atlantahawks": "Hawks",
    "bostonceltics": "Celtics",
    "brooklynnets": "Nets",
    "charlottehornets": "Hornets",
    "chicagobulls": "Bulls",
    "clevelandcavaliers": "Cavaliers",
    "dallasmavericks": "Mavericks",
    "denvernuggets": "Nuggets",
    "detroitpistons": "Pistons",
    "goldenstatewarriors": "Warriors",
    "houstonrockets": "Rockets",
    "indianapacers": "Pacers",
    "losangelesclippers": "Clippers",
    "losangeleslakers": "Lakers",
    "memphisgrizzlies": "Grizzlies",
    "miamiheat": "Heat",
    "milwaukeebucks": "Bucks",
    "minnesotatimberwolves": "Timberwolves",
    "neworleanspelicans": "Pelicans",
    "newyorkknicks": "Knicks",
    "oklahomacitythunder": "Thunder",
    "orlandomagic": "Magic",
    "philadelphia76ers": "76ers",
    "phoenixsuns": "Suns",
    "portlandtrailblazers": "Trail Blazers",
    "sacramentokings": "Kings",
    "sanantoniospurs": "Spurs",
    "torontoraptors": "Raptors",
    "utahjazz": "Jazz",
    "washingtonwizards": "Wizards",
}

# Para nombres ya en nickname ("Lakers", "Trail Blazers")
for nick in list(_TEAM_ALIASES.values()):
    _TEAM_ALIASES["".join(ch for ch in nick.lower() if ch.isalnum())] = nick

# Abreviaturas NBA comunes (para matchups tipo NOP@WAS)
ABBR_TO_NICK = {
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
NICK_TO_ABBR = {v: k for k, v in ABBR_TO_NICK.items()}

# ---------------------------------
# Rest computation (B2B / 3-in-4)
# ---------------------------------
_ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
_REST_LOOKBACK_DAYS = int(os.getenv("NBA_REST_LOOKBACK_DAYS", "3"))
_REST_FETCH_ENABLED = os.getenv("NBA_REST_FETCH_ENABLED", "1") not in ("0", "false", "no")
_REST_FETCH_TIMEOUT = int(os.getenv("NBA_REST_FETCH_TIMEOUT", "10"))


def _fetch_teams_played_on(date_str: str) -> List[str]:
    """Devuelve los displayNames de todos los equipos que jugaron en esa fecha (ESPN)."""
    dates_param = date_str.replace("-", "")
    try:
        r = requests.get(_ESPN_SCOREBOARD, params={"dates": dates_param}, timeout=_REST_FETCH_TIMEOUT)
        if r.status_code >= 400:
            return []
        data = r.json()
    except Exception:
        return []

    teams: List[str] = []
    for ev in data.get("events") or []:
        comps = ev.get("competitions") or []
        if not comps:
            continue
        for competitor in comps[0].get("competitors") or []:
            name = (competitor.get("team") or {}).get("displayName") or ""
            if name:
                teams.append(name)
    return teams


def _build_recent_games_map(day: str) -> Dict[str, List[str]]:
    """
    Retorna {norm_key: [fechas_jugadas]} para los últimos _REST_LOOKBACK_DAYS días.
    Cada equipo puede aparecer con múltiples keys (norm del full name y del nickname)
    para maximizar el match con los nombres de fixtures.
    """
    if not _REST_FETCH_ENABLED:
        return {}

    d = datetime.strptime(day, "%Y-%m-%d")
    result: Dict[str, List[str]] = {}

    for offset in range(1, _REST_LOOKBACK_DAYS + 1):
        past_day = (d - timedelta(days=offset)).strftime("%Y-%m-%d")
        for team_name in _fetch_teams_played_on(past_day):
            # Indexamos por norm del nombre completo Y por norm del nickname
            for key in (_norm_team(team_name), _norm_team(_team_nickname(team_name))):
                if key:
                    result.setdefault(key, []).append(past_day)

    return result


def _compute_rest_flags(team_name: str, recent_map: Dict[str, List[str]], day: str) -> Dict[str, bool]:
    """
    Calcula flags de descanso para un equipo dado el mapa de partidos recientes.
      b2b          → jugó ayer (DAY-1)
      three_in_four → jugó ayer Y (anteayer o hace 3 días)
    """
    if not recent_map:
        return {}

    d = datetime.strptime(day, "%Y-%m-%d")
    d1 = (d - timedelta(days=1)).strftime("%Y-%m-%d")
    d2 = (d - timedelta(days=2)).strftime("%Y-%m-%d")
    d3 = (d - timedelta(days=3)).strftime("%Y-%m-%d")

    # Buscar por varias keys posibles
    dates_played: List[str] = []
    for key in (_norm_team(team_name), _norm_team(_team_nickname(team_name))):
        if key and key in recent_map:
            dates_played = recent_map[key]
            break

    played_d1 = d1 in dates_played
    played_d2 = d2 in dates_played
    played_d3 = d3 in dates_played

    return {
        "b2b": played_d1,
        "three_in_four": played_d1 and (played_d2 or played_d3),
    }


def _norm_team(s: str) -> str:
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _team_nickname(name: str) -> str:
    key = _norm_team(name)
    return _TEAM_ALIASES.get(key, name)


def _load_json(path: str) -> Any:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _first_existing(paths: List[str]) -> Optional[str]:
    for p in paths:
        if not p:
            continue
        try:
            if Path(p).exists():
                return p
        except Exception:
            continue
    return None


def _find_fixtures_path(day: str) -> Optional[str]:
    candidates: List[str] = []
    # Env or default
    if FIXTURES_PATH:
        candidates.append(FIXTURES_PATH)
    # Common locations
    candidates.append(versioned_path(LEAGUE, "features", "fixtures", day))
    candidates.append(versioned_path(LEAGUE, "fixtures", "fixtures", day))
    # data/ prefijo
    candidates.append(str(Path("data") / versioned_path(LEAGUE, "features", "fixtures", day)))
    candidates.append(str(Path("data") / versioned_path(LEAGUE, "fixtures", "fixtures", day)))
    return _first_existing(candidates)


def _find_odds_path(day: str) -> Optional[str]:
    candidates: List[str] = []
    if ODDS_PATH:
        candidates.append(ODDS_PATH)
    candidates.append(versioned_path(LEAGUE, "features", "odds", day))
    candidates.append(versioned_path(LEAGUE, "odds", "odds", day))
    candidates.append(str(Path("data") / versioned_path(LEAGUE, "features", "odds", day)))
    candidates.append(str(Path("data") / versioned_path(LEAGUE, "odds", "odds", day)))
    return _first_existing(candidates)


def _find_team_strength_path(day: str) -> Optional[str]:
    candidates: List[str] = []
    if TEAM_STRENGTH_PATH:
        candidates.append(TEAM_STRENGTH_PATH)
    candidates.append(versioned_path(LEAGUE, "team_strength", "team_strength", day))
    candidates.append(str(Path("data") / versioned_path(LEAGUE, "team_strength", "team_strength", day)))
    return _first_existing(candidates)


def _find_injury_path(day: str) -> Optional[str]:
    candidates: List[str] = []
    if INJ_PATH_ENV:
        candidates.append(INJ_PATH_ENV)

    # Ruta esperada (features)
    candidates.append(versioned_path(LEAGUE, "features", "injuries", day))
    candidates.append(str(Path("data") / versioned_path(LEAGUE, "features", "injuries", day)))

    # Rutas legacy
    candidates.append(versioned_path(LEAGUE, "injuries", "injuries", day))
    candidates.append(str(Path("data") / versioned_path(LEAGUE, "injuries", "injuries", day)))

    return _first_existing(candidates)


def _coerce_teams_to_dict(teams_val: Any) -> Dict[str, List[Dict[str, Any]]]:
    """Convierte `teams` a dict si viene en formatos viejos."""
    if isinstance(teams_val, dict):
        return teams_val

    out: Dict[str, List[Dict[str, Any]]] = {}

    if isinstance(teams_val, list):
        # Formato 1: [{team: "Lakers", injuries: [...]}, ...]
        for item in teams_val:
            if not isinstance(item, dict):
                continue
            if "team" in item and "injuries" in item and isinstance(item["injuries"], list):
                out[str(item["team"])] = item["injuries"]
                continue

            # Formato 2: [{team: "Lakers", player:..., status:...}, ...]
            if "team" in item and "player" in item:
                team = str(item.get("team") or "")
                if not team:
                    continue
                out.setdefault(team, []).append(item)

    return out


def _inj_list_for(team_name: str, injuries_by_team: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not injuries_by_team:
        return []

    # 1) directo
    v = injuries_by_team.get(team_name)
    if isinstance(v, list):
        return v

    # 2) nickname
    nick = _team_nickname(team_name)
    if nick != team_name:
        v = injuries_by_team.get(nick)
        if isinstance(v, list):
            return v

    # 2b) abreviatura estándar (si injuries viene con keys tipo "NOP")
    abbr = NICK_TO_ABBR.get(nick) or NICK_TO_ABBR.get(team_name)
    if abbr:
        v = injuries_by_team.get(abbr)
        if isinstance(v, list):
            return v

    # 3) intentos comunes (si fixture trae "Los Angeles Lakers" pero injuries trae "LosAngelesLakers")
    camel = "".join(team_name.split())
    v = injuries_by_team.get(camel)
    if isinstance(v, list):
        return v

    # 4) normalizado
    key = _norm_team(team_name)
    # algunas salidas guardan keys normalizadas
    v = injuries_by_team.get(key)
    if isinstance(v, list):
        return v

    return []


def _count_statuses(inj_list: List[Dict[str, Any]]) -> Dict[str, int]:
    c = {"OUT": 0, "DOUBTFUL": 0, "QUESTIONABLE": 0}
    for it in inj_list or []:
        if bool(it.get("exclude_from_counts")):
            continue
        status_raw = str(it.get("status") or it.get("CurrentStatus") or "").strip().upper()
        if status_raw in c:
            c[status_raw] += 1
    return c


def _normalize_fixtures(fixtures_raw: Any) -> List[Dict[str, Any]]:
    if isinstance(fixtures_raw, dict):
        return fixtures_raw.get("fixtures", []) or []
    if isinstance(fixtures_raw, list):
        return fixtures_raw
    return []


def _normalize_odds(odds_raw: Any) -> Dict[str, Any]:
    if isinstance(odds_raw, dict):
        return odds_raw.get("odds", {}) or {}
    if isinstance(odds_raw, list):
        out: Dict[str, Any] = {}
        for item in odds_raw:
            if not isinstance(item, dict):
                continue
            game_id = item.get("game_id") or item.get("match_id") or item.get("id")
            if game_id is None:
                continue
            out[str(game_id)] = item
        return out
    return {}


def _normalize_strength(strength_raw: Any) -> List[Dict[str, Any]]:
    if isinstance(strength_raw, dict):
        return strength_raw.get("teams", []) or []
    if isinstance(strength_raw, list):
        return strength_raw
    return []


def _strength_maps(strength_list: List[Dict[str, Any]]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    by_team: Dict[str, Any] = {}
    by_id: Dict[str, Any] = {}
    for item in strength_list:
        if not isinstance(item, dict):
            continue
        team = str(item.get("team") or "")
        if team:
            by_team[team] = item
            by_team[_team_nickname(team)] = item
            by_team[_norm_team(team)] = item
        team_id = item.get("team_id")
        if team_id is not None:
            by_id[str(team_id)] = item
    return by_team, by_id


def main() -> None:
    fixtures_path = _find_fixtures_path(DAY) or FIXTURES_PATH
    odds_path = _find_odds_path(DAY) or ODDS_PATH
    strength_path = _find_team_strength_path(DAY) or TEAM_STRENGTH_PATH

    fixtures_raw = _load_json(fixtures_path) if fixtures_path else []
    odds_raw = _load_json(odds_path) if odds_path else {}
    strength_raw = _load_json(strength_path) if strength_path else []

    fixtures = _normalize_fixtures(fixtures_raw)
    odds = _normalize_odds(odds_raw)
    strength_list = _normalize_strength(strength_raw)
    strength_by_team, strength_by_id = _strength_maps(strength_list)

    # --- Rest data (B2B / 3-in-4) ---
    recent_games_map = _build_recent_games_map(DAY)
    if recent_games_map:
        print(f"[rest] mapa de partidos recientes cargado ({len(recent_games_map)} equipos indexados)")
    else:
        print("[rest] sin datos de descanso (ESPN no disponible o NBA_REST_FETCH_ENABLED=0)")

    inj_path = _find_injury_path(DAY)
    injuries = _load_json(inj_path) if inj_path else {}

    injuries_by_team_raw = injuries.get("teams") if isinstance(injuries, dict) else {}
    injuries_by_team = _coerce_teams_to_dict(injuries_by_team_raw)
    injuries_unknown = True
    if isinstance(injuries, dict):
        # Solo respetamos booleano explícito; lista "injuries_unknown" no implica desconocido.
        if isinstance(injuries.get("injuries_unknown"), bool):
            injuries_unknown = injuries.get("injuries_unknown")
        else:
            injuries_unknown = not bool(injuries_by_team)

    rows: List[Dict[str, Any]] = []

    for fx in fixtures:
        home = fx.get("home") or ""
        away = fx.get("away") or ""

        match = dict(fx)
        if match.get("match_id") is None and match.get("game_id") is not None:
            match["match_id"] = match.get("game_id")
        if match.get("kickoff_utc") is None and match.get("utc_kickoff") is not None:
            match["kickoff_utc"] = match.get("utc_kickoff")

        match_id = match.get("game_id") or match.get("match_id") or match.get("id")

        home_inj = _inj_list_for(str(home), injuries_by_team)
        away_inj = _inj_list_for(str(away), injuries_by_team)

        home_counts = _count_statuses(home_inj)
        away_counts = _count_statuses(away_inj)

        home_id = match.get("home_team_id")
        away_id = match.get("away_team_id")

        home_strength = {}
        away_strength = {}
        if home_id is not None:
            home_strength = strength_by_id.get(str(home_id), {})
        if not home_strength and home:
            home_strength = strength_by_team.get(home, {}) or strength_by_team.get(_team_nickname(home), {})

        if away_id is not None:
            away_strength = strength_by_id.get(str(away_id), {})
        if not away_strength and away:
            away_strength = strength_by_team.get(away, {}) or strength_by_team.get(_team_nickname(away), {})

        home_rest = _compute_rest_flags(str(home), recent_games_map, DAY)
        away_rest = _compute_rest_flags(str(away), recent_games_map, DAY)

        context = {
            "home_strength": home_strength,
            "away_strength": away_strength,
            "home_rest": home_rest,
            "away_rest": away_rest,
            "injuries_unknown": injuries_unknown,
            "injuries_parsed": bool(injuries_by_team),
            "home_injuries": home_inj,
            "away_injuries": away_inj,
            "home_injury_flags": {
                "counts": home_counts,
                "has_out_or_doubtful": bool(home_counts["OUT"] + home_counts["DOUBTFUL"] > 0),
            },
            "away_injury_flags": {
                "counts": away_counts,
                "has_out_or_doubtful": bool(away_counts["OUT"] + away_counts["DOUBTFUL"] > 0),
            },
        }

        row = {
            "match": match,
            "odds": odds.get(str(match_id), {}) if match_id is not None else {},
            "context": context,
        }
        rows.append(row)

    out = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "league": LEAGUE,
        "day": DAY,
        "fixtures_path": fixtures_path,
        "odds_path": odds_path,
        "team_strength_path": strength_path,
        "injuries_path": inj_path,
        "rows": rows,
    }

    ensure_parent_dir(OUT_PATH)
    Path(OUT_PATH).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"ready_context generado: {OUT_PATH} (injuries: {inj_path or 'none'})")


if __name__ == "__main__":
    main()
