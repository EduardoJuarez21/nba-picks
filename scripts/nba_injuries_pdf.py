"""nba_injuries_api.py

Obtiene las lesiones NBA desde Tank01 Fantasy Stats (RapidAPI)
endpoint: getNBAInjuries  host: tank01-fantasy-stats.p.rapidapi.com

Produce el mismo formato JSON que consumia el parser de PDF,
compatible con nba_build_ready_context.py:
{
  "generated_at_utc": "...",
  "source": {"type": "api", ...},
  "injuries_unknown": [],
  "teams": {
    "Lakers": [{player, status, reason, source, exclude_from_counts}, ...],
    "Los Angeles Lakers": [...same...],
    ...
  },
  "unmatched": []
}

Vars de entorno:
  RAPIDAPI_KEY        requerida
  DAY                 YYYY-MM-DD  (requerida para la ruta de salida)
  LEAGUE              default: nba
  OUT_INJ / OUT_PATH_INJURIES   override ruta de salida
  ALSO_WRITE_LEGACY   default: 1
  ALLOW_MISSING_INJ   default: 0  (si 1, genera salida vacia en lugar de fallar)
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from nba_utils import ensure_parent_dir, versioned_path


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "").strip()
RAPIDAPI_HOST = "tank01-fantasy-stats.p.rapidapi.com"
RAPIDAPI_URL_INJURIES = f"https://{RAPIDAPI_HOST}/getNBAInjuryList"
RAPIDAPI_URL_PLAYERS = f"https://{RAPIDAPI_HOST}/getNBAPlayerList"

DAY = os.getenv("DAY", "").strip()
LEAGUE = os.getenv("LEAGUE", "nba")

OUT_MAIN = os.getenv(
    "OUT_INJ",
    os.getenv(
        "OUT_PATH_INJURIES",
        versioned_path(LEAGUE, "features", "injuries", DAY) if DAY else "",
    ),
)
ALSO_WRITE_LEGACY = os.getenv("ALSO_WRITE_LEGACY", "1").strip() not in {"0", "false", "False"}
ALLOW_MISSING_INJ = os.getenv("ALLOW_MISSING_INJ", "0").lower() in {"1", "true", "yes"}

# ---------------------------------------------------------------------------
# Team tables (mismas que nba_injuries_pdf.py para compatibilidad)
# ---------------------------------------------------------------------------

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

ABBR_TO_NICK: Dict[str, str] = {
    "ATL": "Hawks", "BOS": "Celtics", "BKN": "Nets", "CHA": "Hornets",
    "CHI": "Bulls", "CLE": "Cavaliers", "DAL": "Mavericks", "DEN": "Nuggets",
    "DET": "Pistons", "GSW": "Warriors", "HOU": "Rockets", "IND": "Pacers",
    "LAC": "Clippers", "LAL": "Lakers", "MEM": "Grizzlies", "MIA": "Heat",
    "MIL": "Bucks", "MIN": "Timberwolves", "NOP": "Pelicans", "NYK": "Knicks",
    "OKC": "Thunder", "ORL": "Magic", "PHI": "76ers", "PHX": "Suns",
    "POR": "Trail Blazers", "SAC": "Kings", "SAS": "Spurs", "TOR": "Raptors",
    "UTA": "Jazz", "WAS": "Wizards",
}

def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


# Construir alias: normalizado -> nickname
_ALIAS: Dict[str, str] = {}
_NICK_TO_FULL: Dict[str, str] = {}

for _city, _nick in NBA_TEAMS:
    _full = f"{_city} {_nick}".strip()
    _NICK_TO_FULL[_nick] = _full
    for _key in (_norm_key(_nick), _norm_key(_full), _norm_key(_full.replace(" ", ""))):
        _ALIAS[_key] = _nick

for _abbr, _nick in ABBR_TO_NICK.items():
    _ALIAS[_norm_key(_abbr)] = _nick

# Abreviaturas alternativas que usa Tank01
_ALIAS["ny"] = "Knicks"
_ALIAS["gs"] = "Warriors"
_ALIAS["sa"] = "Spurs"
_ALIAS["no"] = "Pelicans"

# aliases especiales
for _pair in [
    ("LAClippers", "Clippers"), ("LosAngelesClippers", "Clippers"),
    ("GoldenStateWarriors", "Warriors"), ("PortlandTrailBlazers", "Trail Blazers"),
    ("NewOrleansPelicans", "Pelicans"), ("NewYorkKnicks", "Knicks"),
    ("OklahomaCityThunder", "Thunder"),
]:
    _ALIAS[_norm_key(_pair[0])] = _pair[1]


def _resolve_nick(team_raw: str) -> Optional[str]:
    if not team_raw:
        return None
    return _ALIAS.get(_norm_key(team_raw))


# ---------------------------------------------------------------------------
# Status mapping
# ---------------------------------------------------------------------------

_STATUS_MAP: Dict[str, str] = {
    "out": "OUT",
    "doubtful": "DOUBTFUL",
    "questionable": "QUESTIONABLE",
    "probable": "PROBABLE",
    "available": "AVAILABLE",
    "day-to-day": "QUESTIONABLE",
    "day to day": "QUESTIONABLE",
    "game time decision": "QUESTIONABLE",
    "gtd": "QUESTIONABLE",
    "injured reserve": "OUT",
    "ir": "OUT",
    "ten-day il": "OUT",
    "two-way": "AVAILABLE",
}

EXCLUDE_REASON_TOKENS = {"G-LEAGUE", "GLEAGUE", "TWO-WAY", "TWOWAY", "ONASSIGNMENT", "NOTWITHTEAM"}


def _map_status(raw: str) -> str:
    return _STATUS_MAP.get((raw or "").lower().strip(), "QUESTIONABLE")


def _reason_excluded(reason: Optional[str]) -> bool:
    if not reason:
        return False
    up = re.sub(r"[^A-Z-]", "", reason.upper())
    return any(tok in up for tok in EXCLUDE_REASON_TOKENS)


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def _api_get(url: str) -> Any:
    headers = {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
    }
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Tank01 API error {r.status_code}: {r.text[:400]}")
    data = r.json()
    if isinstance(data, dict):
        return data.get("body", data)
    return data


def _fetch_player_map() -> Dict[str, Dict[str, Any]]:
    """Devuelve {playerID: {longName, team (abbr), teamID}}."""
    body = _api_get(RAPIDAPI_URL_PLAYERS)
    if not isinstance(body, list):
        return {}
    return {p["playerID"]: p for p in body if p.get("playerID")}


def _fetch_injuries() -> List[Dict[str, Any]]:
    body = _api_get(RAPIDAPI_URL_INJURIES)
    if isinstance(body, list):
        return body
    return []


# ---------------------------------------------------------------------------
# Build output
# ---------------------------------------------------------------------------

def _build_output(records: List[Dict[str, Any]], player_map: Dict[str, Any]) -> dict:
    teams: Dict[str, List[dict]] = {}
    unmatched: List[dict] = []

    for rec in records:
        pid = rec.get("playerID", "")
        player_info = player_map.get(pid, {})

        # Team viene del player map (abreviatura: "MIA", "LAL", etc.)
        team_raw = player_info.get("team") or ""
        nick = _resolve_nick(team_raw)

        player = player_info.get("longName") or pid or None
        designation = rec.get("designation") or "Questionable"
        reason = rec.get("description") or None

        status = _map_status(str(designation))

        if not nick:
            unmatched.append({"team_raw": team_raw, "player": player, "raw": rec})
            continue

        entry = {
            "player": player,
            "status": status,
            "reason": reason,
            "source": "api",
            "exclude_from_counts": _reason_excluded(reason),
        }

        # Guardar por nickname Y full name (igual que el PDF parser)
        teams.setdefault(nick, []).append(entry)
        full = _NICK_TO_FULL.get(nick)
        if full:
            teams.setdefault(full, []).append(entry)

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": {
            "type": "api",
            "provider": "tank01-fantasy-stats",
            "endpoint": "getNBAInjuries",
        },
        "injuries_unknown": [],
        "teams": teams,
        "unmatched": unmatched,
    }


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def _write_json(path: str, data: dict) -> None:
    ensure_parent_dir(path)
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _empty_output() -> dict:
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": {"type": "api", "provider": "tank01-fantasy-stats", "note": "empty/error"},
        "injuries_unknown": [],
        "teams": {},
        "unmatched": [],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not DAY:
        raise SystemExit("Falta DAY en .env (YYYY-MM-DD)")
    if not OUT_MAIN:
        raise SystemExit("No se pudo determinar OUT_INJ / OUT_PATH_INJURIES")

    out_legacy_1 = versioned_path(LEAGUE, "injuries", "injuries", DAY)
    out_legacy_2 = f"./out/{LEAGUE}/injuries_{DAY}.json"

    if not RAPIDAPI_KEY:
        if ALLOW_MISSING_INJ:
            out = _empty_output()
            _write_json(OUT_MAIN, out)
            print(f"[injuries] RAPIDAPI_KEY ausente; escribiendo salida vacia en {OUT_MAIN}", file=sys.stderr)
            return
        raise SystemExit("Falta RAPIDAPI_KEY en .env")

    try:
        player_map = _fetch_player_map()
        records = _fetch_injuries()
    except Exception as exc:
        if ALLOW_MISSING_INJ:
            out = _empty_output()
            _write_json(OUT_MAIN, out)
            print(f"[injuries] API error: {exc}; escribiendo salida vacia", file=sys.stderr)
            return
        raise SystemExit(f"Error consultando Tank01 injuries: {exc}") from exc

    out = _build_output(records, player_map)

    _write_json(OUT_MAIN, out)

    if ALSO_WRITE_LEGACY:
        for p in {out_legacy_1, out_legacy_2}:
            try:
                _write_json(p, out)
            except Exception:
                pass

    n_teams = len(out["teams"])
    n_players = sum(len(v) for v in out["teams"].values()) // 2  # dividido 2 x nick+full
    n_unmatched = len(out["unmatched"])
    print(f"[injuries] wrote: {OUT_MAIN}")
    if ALSO_WRITE_LEGACY:
        print(f"[injuries] also wrote legacy: {out_legacy_1} and {out_legacy_2}")
    print(f"[injuries] team_keys={n_teams} players~={n_players} unmatched={n_unmatched}")


if __name__ == "__main__":
    main()
