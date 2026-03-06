import hashlib
import json
import os
import re
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

from nba_utils import versioned_path

ENV_PATH = Path(os.getenv("ENV_PATH", ".env"))
if ENV_PATH.is_file():
    load_dotenv(ENV_PATH, override=False)

API_KEY = os.getenv("ODDS_API_KEY")
if not API_KEY:
    raise SystemExit("Falta ODDS_API_KEY en .env")

LEAGUE = os.getenv("LEAGUE", "nba")
DATA_DIR = os.getenv("DATA_DIR", "data")
DAY = os.getenv("DAY", "").strip()
if not DAY:
    raise SystemExit("Falta DAY en .env (YYYY-MM-DD)")

SPORT = os.getenv("NBA_ODDS_SPORT", "basketball_nba")
# Pinnacle por default: sus líneas son la referencia sharp del mercado.
# Si se vacía NBA_ODDS_BOOKMAKERS, cae a REGIONS como fallback.
BOOKMAKERS = os.getenv("NBA_ODDS_BOOKMAKERS", "pinnacle").strip()
REGIONS = os.getenv("NBA_ODDS_REGIONS", os.getenv("ODDS_REGIONS", "us"))
ODDS_FORMAT = os.getenv("ODDS_FORMAT", "decimal").lower()
MARKETS = os.getenv("NBA_ODDS_MARKETS", "h2h,totals,spreads")

FIXTURES_PATH = os.getenv("FIXTURES_PATH", versioned_path(LEAGUE, "fixtures", "fixtures", DAY))
DEFAULT_PATH = versioned_path(LEAGUE, "odds", "odds", DAY)
OUT_PATH = os.getenv("OUT_PATH_ODDS", DEFAULT_PATH)

CACHE_DIR = Path(os.getenv("CACHE_DIR", f"./{DATA_DIR}/cache/oddsapi"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "43200"))
FORCE_REFRESH = os.getenv("FORCE_REFRESH", "false").lower() == "true"

TOTAL_MIN = float(os.getenv("NBA_TOTAL_MIN", "100"))
TOTAL_MAX = float(os.getenv("NBA_TOTAL_MAX", "300"))
SPREAD_MAX = float(os.getenv("NBA_SPREAD_MAX", "40"))
LOG_UNMATCHED = os.getenv("NBA_LOG_UNMATCHED", "true").lower() == "true"

NBA_ALIASES = {
    "la clippers": "los angeles clippers",
    "los angeles clippers": "los angeles clippers",
    "clippers": "los angeles clippers",
    "la lakers": "los angeles lakers",
    "los angeles lakers": "los angeles lakers",
    "lakers": "los angeles lakers",
    "gs warriors": "golden state warriors",
    "golden state warriors": "golden state warriors",
    "warriors": "golden state warriors",
    "okc thunder": "oklahoma city thunder",
    "oklahoma city thunder": "oklahoma city thunder",
    "thunder": "oklahoma city thunder",
    "ny knicks": "new york knicks",
    "new york knicks": "new york knicks",
    "knicks": "new york knicks",
    "bk nets": "brooklyn nets",
    "brooklyn nets": "brooklyn nets",
    "nets": "brooklyn nets",
    "no pelicans": "new orleans pelicans",
    "new orleans pelicans": "new orleans pelicans",
    "pelicans": "new orleans pelicans",
    "sa spurs": "san antonio spurs",
    "san antonio spurs": "san antonio spurs",
    "spurs": "san antonio spurs",
    "phi 76ers": "philadelphia 76ers",
    "philadelphia 76ers": "philadelphia 76ers",
    "76ers": "philadelphia 76ers",
    "utah jazz": "utah jazz",
    "jazz": "utah jazz",
    "boston celtics": "boston celtics",
    "celtics": "boston celtics",
    "miami heat": "miami heat",
    "heat": "miami heat",
    "minnesota timberwolves": "minnesota timberwolves",
    "timberwolves": "minnesota timberwolves",
    "toronto raptors": "toronto raptors",
    "raptors": "toronto raptors",
    "atlanta hawks": "atlanta hawks",
    "hawks": "atlanta hawks",
    "chicago bulls": "chicago bulls",
    "bulls": "chicago bulls",
    "charlotte hornets": "charlotte hornets",
    "hornets": "charlotte hornets",
    "portland trail blazers": "portland trail blazers",
    "trail blazers": "portland trail blazers",
    "blazers": "portland trail blazers",
    "dallas mavericks": "dallas mavericks",
    "mavericks": "dallas mavericks",
    "houston rockets": "houston rockets",
    "rockets": "houston rockets",
    "sacramento kings": "sacramento kings",
    "kings": "sacramento kings",
    "phoenix suns": "phoenix suns",
    "suns": "phoenix suns",
    "milwaukee bucks": "milwaukee bucks",
    "bucks": "milwaukee bucks",
    "cleveland cavaliers": "cleveland cavaliers",
    "cavaliers": "cleveland cavaliers",
    "denver nuggets": "denver nuggets",
    "nuggets": "denver nuggets",
    "indiana pacers": "indiana pacers",
    "pacers": "indiana pacers",
    "washington wizards": "washington wizards",
    "wizards": "washington wizards",
    "orlando magic": "orlando magic",
    "magic": "orlando magic",
    "memphis grizzlies": "memphis grizzlies",
    "grizzlies": "memphis grizzlies",
    "detroit pistons": "detroit pistons",
    "pistons": "detroit pistons",
    "cleveland cavaliers": "cleveland cavaliers",
    "new orleans pelicans": "new orleans pelicans",
    "new york knicks": "new york knicks",
    "philadelphia 76ers": "philadelphia 76ers",
    "los angeles clippers": "los angeles clippers",
    "los angeles lakers": "los angeles lakers",
    "san antonio spurs": "san antonio spurs",
    "golden state warriors": "golden state warriors",
    "oklahoma city thunder": "oklahoma city thunder",
    "minnesota timberwolves": "minnesota timberwolves",
    "miami heat": "miami heat",
}


def _cache_key(url: str, params: Dict[str, Any], salt: str = "") -> str:
    items = "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))
    raw = f"{url}?{items}|{salt}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def get_json_cached(url: str, params: Dict[str, Any], cache_salt: str = "") -> Dict[str, Any]:
    key = _cache_key(url, params, cache_salt)
    cache_file = CACHE_DIR / f"{key}.json"

    if not FORCE_REFRESH and cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < CACHE_TTL_SECONDS:
            print(f"[odds] cache hit ({int(age)}s old) -> {cache_file.name}")
            return json.loads(cache_file.read_text(encoding="utf-8"))

    r = requests.get(url, params=params, timeout=30)
    remaining = r.headers.get("x-requests-remaining", "?")
    used = r.headers.get("x-requests-used", "?")
    print(f"[odds] API call -> used={used} remaining={remaining}")
    if r.status_code >= 400:
        raise SystemExit(f"Error consultando Odds API: {r.status_code} {r.text[:800]}")
    data = r.json()
    cache_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return data


def _read_fixtures(path: str) -> List[Dict[str, Any]]:
    if not Path(path).is_file():
        return []
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _norm_team(name: str) -> str:
    s = (name or "").lower().strip()
    s = s.replace("&", "and")
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return NBA_ALIASES.get(s, s)


def _to_utc_iso(dt_str: str) -> str:
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_decimal(price: float) -> float:
    if ODDS_FORMAT == "american":
        if price > 0:
            return 1.0 + (price / 100.0)
        return 1.0 + (100.0 / abs(price))
    return float(price)


def _best_price_h2h(bookmakers: List[Dict[str, Any]], home: str, away: str) -> Dict[str, Any]:
    home_n = _norm_team(home)
    away_n = _norm_team(away)
    best = {"HOME": None, "AWAY": None}

    for b in bookmakers or []:
        bkey = b.get("key")
        for m in b.get("markets") or []:
            if m.get("key") != "h2h":
                continue
            for o in m.get("outcomes") or []:
                name = _norm_team(o.get("name", ""))
                price = o.get("price")
                if not isinstance(price, (int, float)):
                    continue
                dec_price = _to_decimal(float(price))
                if name == home_n:
                    cur = best["HOME"]
                    if cur is None or dec_price > cur["odds"]:
                        best["HOME"] = {"odds": dec_price, "book": bkey}
                elif name == away_n:
                    cur = best["AWAY"]
                    if cur is None or dec_price > cur["odds"]:
                        best["AWAY"] = {"odds": dec_price, "book": bkey}
    return best


def _best_price_totals(bookmakers: List[Dict[str, Any]]) -> Dict[str, Any]:
    lines: Dict[str, Dict[str, Any]] = {}
    for b in bookmakers or []:
        bkey = b.get("key")
        for m in b.get("markets") or []:
            if m.get("key") != "totals":
                continue
            for o in m.get("outcomes") or []:
                side = _norm_team(o.get("name", ""))  # over/under
                point = o.get("point")
                price = o.get("price")
                if side not in ("over", "under"):
                    continue
                if not isinstance(point, (int, float)) or not isinstance(price, (int, float)):
                    continue
                if point < TOTAL_MIN or point > TOTAL_MAX:
                    continue
                dec_price = _to_decimal(float(price))
                line_key = str(float(point))
                if line_key not in lines:
                    lines[line_key] = {"over": None, "under": None}
                cur = lines[line_key][side]
                if cur is None or dec_price > cur["odds"]:
                    lines[line_key][side] = {"odds": dec_price, "book": bkey}
    return lines


def _best_price_spreads(bookmakers: List[Dict[str, Any]], home: str, away: str) -> Dict[str, Any]:
    lines: Dict[str, Dict[str, Any]] = {}
    home_n = _norm_team(home)
    away_n = _norm_team(away)
    for b in bookmakers or []:
        bkey = b.get("key")
        for m in b.get("markets") or []:
            if m.get("key") != "spreads":
                continue
            for o in m.get("outcomes") or []:
                name = _norm_team(o.get("name", ""))
                point = o.get("point")
                price = o.get("price")
                if not isinstance(point, (int, float)) or not isinstance(price, (int, float)):
                    continue
                if abs(point) > SPREAD_MAX:
                    continue
                dec_price = _to_decimal(float(price))
                if name == home_n:
                    side = "home"
                elif name == away_n:
                    side = "away"
                else:
                    continue
                line_key = str(abs(float(point)))
                if line_key not in lines:
                    lines[line_key] = {"home": None, "away": None}
                cur = lines[line_key][side]
                if cur is None or dec_price > cur["odds"]:
                    lines[line_key][side] = {"odds": dec_price, "book": bkey, "line": float(point)}
    return lines


def main() -> None:
    fixtures = _read_fixtures(FIXTURES_PATH)
    if not fixtures:
        Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
        Path(OUT_PATH).write_text(json.dumps([], ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"OK -> {OUT_PATH} (items=0)")
        return

    bulk_url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds"
    params: Dict[str, Any] = {
        "apiKey": API_KEY,
        "markets": MARKETS,
        "oddsFormat": ODDS_FORMAT,
        "dateFormat": "iso",
    }
    # bookmakers= y regions= son mutuamente excluyentes en la Odds API.
    # Si BOOKMAKERS está definido (default: "pinnacle"), usarlo para obtener
    # exactamente las líneas del book donde apuestas. Si se vacía, usar regions.
    if BOOKMAKERS:
        params["bookmakers"] = BOOKMAKERS
    else:
        params["regions"] = REGIONS
    data = get_json_cached(bulk_url, params, cache_salt=f"DAY={DAY}")

    odds_events: List[Dict[str, Any]] = []
    for ev in data or []:
        ko = ev.get("commence_time")
        if not ko:
            continue
        kickoff_utc = _to_utc_iso(ko)
        odds_events.append(
            {
                "event_id": ev.get("id"),
                "kickoff_utc": kickoff_utc,
                "home": ev.get("home_team"),
                "away": ev.get("away_team"),
                "bookmakers": ev.get("bookmakers") or [],
            }
        )

    out: List[Dict[str, Any]] = []
    unmatched = []
    for fx in fixtures:
        ko = fx.get("utc_kickoff") or ""
        day = ko[:10]
        home = fx.get("home") or ""
        away = fx.get("away") or ""

        best_event = None
        swapped = False
        for ev in odds_events:
            ev_home = _norm_team(ev.get("home"))
            ev_away = _norm_team(ev.get("away"))
            fx_home = _norm_team(home)
            fx_away = _norm_team(away)

            direct = (ev_home == fx_home and ev_away == fx_away)
            invert = (ev_home == fx_away and ev_away == fx_home)
            if not direct and not invert:
                continue

            swapped = invert
            if not day:
                best_event = ev
                break
            ev_day = ev.get("kickoff_utc", "")[:10]
            if not ev_day:
                best_event = ev
                break
            try:
                fx_day = datetime.fromisoformat(day).date()
                odds_day = datetime.fromisoformat(ev_day).date()
            except Exception:
                best_event = ev
                break
            if abs((odds_day - fx_day).days) <= 1:
                best_event = ev
                break

        if not best_event:
            if LOG_UNMATCHED:
                unmatched.append(f"{home} vs {away} ({day})")
            continue

        bookmakers = best_event.get("bookmakers") or []
        if swapped:
            best_ml = _best_price_h2h(bookmakers, away, home)
            best_spreads = _best_price_spreads(bookmakers, away, home)
        else:
            best_ml = _best_price_h2h(bookmakers, home, away)
            best_spreads = _best_price_spreads(bookmakers, home, away)
        best_totals = _best_price_totals(bookmakers)

        out.append(
            {
                "game_id": fx.get("game_id"),
                "kickoff_utc": ko,
                "home": home,
                "away": away,
                "home_team_id": fx.get("home_team_id"),
                "away_team_id": fx.get("away_team_id"),
                "best": {
                    "ml": best_ml,
                    "totals_by_line": best_totals,
                    "spreads_by_line": best_spreads,
                },
            }
        )

    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(OUT_PATH).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK -> {OUT_PATH} (items={len(out)})")
    if LOG_UNMATCHED and unmatched:
        print(f"[nba] odds unmatched fixtures={len(unmatched)}")
        for item in unmatched[:10]:
            print(f"[nba] unmatched: {item}")


if __name__ == "__main__":
    main()
