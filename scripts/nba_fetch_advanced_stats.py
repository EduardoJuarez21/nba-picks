"""nba_fetch_advanced_stats.py

Fetches ORTG, DRTG and PACE by team.
Primary source: ESPN Hollinger Team Stats (pd.read_html).
Fallback: Basketball Reference.

Output format:
  {"Team Name": {"ortg": x, "drtg": x, "pace": x}, ...}
"""

import json
import os
import time
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment

from nba_utils import resolve_nba_season, versioned_path

DAY = os.getenv("DAY", "").strip()
if not DAY:
    raise SystemExit("Falta DAY en .env (YYYY-MM-DD)")

LEAGUE = os.getenv("LEAGUE", "nba")
TIMEOUT = int(os.getenv("NBA_STATS_TIMEOUT", "30"))
SEASON = resolve_nba_season(DAY)  # e.g. 2025-26
USE_NBA_API_FALLBACK = os.getenv("NBA_ADVANCED_USE_NBA_API_FALLBACK", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}

DEFAULT_PATH = versioned_path(LEAGUE, "advanced", "advanced_stats", DAY)
OUT_PATH = os.getenv("OUT_PATH_ADVANCED", DEFAULT_PATH)

ESPN_HOLLINGER_URL = "https://www.espn.com/nba/hollinger/teamstats"


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or str(v).strip() in ("", "-", "--"):
            return None
        return float(str(v).strip())
    except Exception:
        return None


def _norm_col_name(name: object) -> str:
    return "".join(ch for ch in str(name).lower() if ch.isalnum())


def _season_end_year_from_season(season: str) -> int:
    # "2025-26" -> 2026
    if "-" not in season:
        return int(season)
    start, end = season.split("-", 1)
    if len(end) == 2:
        return int(start[:2] + end)
    return int(end)


def _extract_all_tables(html: str) -> list[pd.DataFrame]:
    soup = BeautifulSoup(html, "html.parser")
    html_parts = [html]
    for comment in soup.find_all(string=lambda s: isinstance(s, Comment)):
        if "<table" in comment:
            html_parts.append(str(comment))

    tables: list[pd.DataFrame] = []
    for part in html_parts:
        try:
            tables.extend(pd.read_html(StringIO(part)))
        except ValueError:
            continue
    return tables


def _find_target_table(tables: list[pd.DataFrame]) -> pd.DataFrame | None:
    for df in tables:
        local = df.copy()
        if isinstance(local.columns, pd.MultiIndex):
            local.columns = [str(c[-1]) for c in local.columns]

        col_map = {_norm_col_name(c): str(c) for c in local.columns}
        required = ("team", "ortg", "drtg", "pace")
        if all(req in col_map for req in required):
            team_col = col_map["team"]
            ortg_col = col_map["ortg"]
            drtg_col = col_map["drtg"]
            pace_col = col_map["pace"]
            return local[[team_col, ortg_col, drtg_col, pace_col]].rename(
                columns={
                    team_col: "team",
                    ortg_col: "ortg",
                    drtg_col: "drtg",
                    pace_col: "pace",
                }
            )
    return None


def _clean_table(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["team"] = out["team"].astype(str).str.strip()
    out = out[out["team"] != ""]
    out = out[~out["team"].isin(["Team", "League Average"])]
    out = out[~out["team"].str.contains("Division", case=False, na=False)]

    out["ortg"] = pd.to_numeric(out["ortg"], errors="coerce")
    out["drtg"] = pd.to_numeric(out["drtg"], errors="coerce")
    out["pace"] = pd.to_numeric(out["pace"], errors="coerce")
    out = out.dropna(subset=["team", "ortg", "drtg", "pace"]).reset_index(drop=True)
    return out


def _fetch_from_espn_hollinger() -> Dict[str, Dict[str, Any]]:
    """Scrapes ESPN Hollinger team stats: OFF EFF → ortg, DEF EFF → drtg, PACE → pace."""
    tables = pd.read_html(ESPN_HOLLINGER_URL, header=1)
    if not tables:
        raise RuntimeError("No se encontraron tablas en ESPN Hollinger")

    df = tables[0]
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df["TEAM"].notna() & (df["TEAM"] != "TEAM")]

    col_map = {c.upper(): c for c in df.columns}
    off_col = col_map.get("OFF EFF") or col_map.get("OFFEFF") or col_map.get("OFF.EFF")
    def_col = col_map.get("DEF EFF") or col_map.get("DEFEFF") or col_map.get("DEF.EFF")
    pace_col = col_map.get("PACE")

    if not off_col or not def_col or not pace_col:
        raise RuntimeError(f"Columnas ESPN no encontradas. Disponibles: {list(df.columns)}")

    result: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        team = str(row["TEAM"]).strip()
        if not team:
            continue
        result[team] = {
            "ortg": _safe_float(row.get(off_col)),
            "drtg": _safe_float(row.get(def_col)),
            "pace": _safe_float(row.get(pace_col)),
        }

    print(f"[advanced/espn] {len(result)} equipos")
    return result


def _fetch_from_basketball_reference() -> Dict[str, Dict[str, Any]]:
    season_end_year = _season_end_year_from_season(SEASON)
    url = f"https://www.basketball-reference.com/leagues/NBA_{season_end_year}.html"
    headers = {
        "User-Agent": os.getenv("NBA_BR_USER_AGENT", "Mozilla/5.0").strip(),
    }

    html = ""
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, headers=headers, timeout=TIMEOUT)
            if resp.status_code == 429 and attempt < 3:
                wait = 10 * attempt
                print(f"[advanced/br] rate-limited, esperando {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            html = resp.text
            break
        except Exception as exc:
            if attempt < 3:
                wait = 5 * attempt
                print(f"[advanced/br] intento={attempt} fallo: {exc}. reintentando en {wait}s")
                time.sleep(wait)
                continue
            raise RuntimeError(f"Basketball Reference no disponible: {exc}") from exc

    tables = _extract_all_tables(html)
    if not tables:
        raise RuntimeError("No se encontraron tablas en la pagina de Basketball Reference.")

    target = _find_target_table(tables)
    if target is None:
        raise RuntimeError("No se encontro tabla con Team/ORtg/DRtg/Pace.")

    cleaned = _clean_table(target)
    result: Dict[str, Dict[str, Any]] = {}
    for _, row in cleaned.iterrows():
        team = str(row["team"]).strip()
        if not team:
            continue
        result[team] = {
            "ortg": _safe_float(row["ortg"]),
            "drtg": _safe_float(row["drtg"]),
            "pace": _safe_float(row["pace"]),
        }

    print(f"[advanced/br] {len(result)} equipos season={SEASON} url={url}")
    return result


def _fetch_from_nba_api() -> Dict[str, Dict[str, Any]]:
    try:
        from nba_api.stats.endpoints import leaguedashteamstats
    except Exception as exc:
        raise RuntimeError(f"nba_api no disponible: {exc}") from exc

    df = None
    for attempt in range(1, 4):
        try:
            endpoint = leaguedashteamstats.LeagueDashTeamStats(
                measure_type_detailed_defense="Advanced",
                season=SEASON,
                season_type_all_star="Regular Season",
                per_mode_detailed="PerGame",
                timeout=TIMEOUT,
            )
            frames = endpoint.get_data_frames()
            if not frames:
                return {}
            df = frames[0]
            break
        except Exception as exc:
            if attempt < 3:
                time.sleep(5 * attempt)
                continue
            raise RuntimeError(f"nba_api fallo: {exc}") from exc

    if df is None or df.empty:
        return {}

    col_map = {_norm_col_name(c): str(c) for c in df.columns}
    team_col = col_map.get("teamname")
    ortg_col = col_map.get("offrating") or col_map.get("eoffrating")
    drtg_col = col_map.get("defrating") or col_map.get("edefrating")
    pace_col = col_map.get("pace")
    if not team_col:
        return {}

    result: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        team = str(row.get(team_col) or "").strip()
        if not team:
            continue
        result[team] = {
            "ortg": _safe_float(row.get(ortg_col)) if ortg_col else None,
            "drtg": _safe_float(row.get(drtg_col)) if drtg_col else None,
            "pace": _safe_float(row.get(pace_col)) if pace_col else None,
        }
    print(f"[advanced/nba_api] {len(result)} equipos season={SEASON}")
    return result


def fetch_advanced_stats() -> Dict[str, Dict[str, Any]]:
    # 1. ESPN Hollinger (fuente principal)
    try:
        return _fetch_from_espn_hollinger()
    except Exception as exc:
        print(f"[advanced/espn] error: {exc}")

    # 2. Basketball Reference (fallback)
    try:
        return _fetch_from_basketball_reference()
    except Exception as exc:
        print(f"[advanced/br] error: {exc}")

    # 3. nba_api (fallback opcional)
    if USE_NBA_API_FALLBACK:
        print("[advanced] intentando fallback nba_api")
        try:
            return _fetch_from_nba_api()
        except Exception as exc2:
            print(f"[advanced/nba_api] error: {exc2}")

    return {}


def main() -> None:
    data = fetch_advanced_stats()
    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(OUT_PATH).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK -> {OUT_PATH} (teams={len(data)})")


if __name__ == "__main__":
    main()
