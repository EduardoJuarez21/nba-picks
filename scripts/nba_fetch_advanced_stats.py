"""nba_fetch_advanced_stats.py

Scrapea Basketball Reference para obtener ORTG, DRTG y PACE por equipo.
URL: https://www.basketball-reference.com/leagues/NBA_{YEAR}.html

Produce un JSON: {"Team Name": {"ortg": x, "drtg": x, "pace": x}, ...}
Guarda en fuente/nba/advanced/advanced_stats_{DAY}.json

Vars de entorno:
  DAY          YYYY-MM-DD  requerida
  LEAGUE       default: nba
  BBREF_YEAR   override del año (default: auto desde DAY)
  OUT_PATH_ADVANCED  override ruta de salida
  BBREF_TIMEOUT      default: 20
"""

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from nba_utils import resolve_nba_season, versioned_path

DAY = os.getenv("DAY", "").strip()
if not DAY:
    raise SystemExit("Falta DAY en .env (YYYY-MM-DD)")

LEAGUE = os.getenv("LEAGUE", "nba")
TIMEOUT = int(os.getenv("BBREF_TIMEOUT", "20"))

# Año de la temporada (ej. 2025-26 -> 2026)
def _season_year(day: str) -> int:
    season = resolve_nba_season(day)          # "2025-26"
    return int(season.split("-")[0]) + 1      # 2026

YEAR = int(os.getenv("BBREF_YEAR", str(_season_year(DAY))))
BBREF_URL = f"https://www.basketball-reference.com/leagues/NBA_{YEAR}.html"

DEFAULT_PATH = versioned_path(LEAGUE, "advanced", "advanced_stats", DAY)
OUT_PATH = os.getenv("OUT_PATH_ADVANCED", DEFAULT_PATH)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
    "Referer": "https://www.google.com/",
}

# Aliases BBRef -> nombre canónico del pipeline
BBREF_ALIASES: Dict[str, str] = {
    "Los Angeles Lakers": "Los Angeles Lakers",
    "Los Angeles Clippers": "Los Angeles Clippers",
    "Golden State Warriors": "Golden State Warriors",
    "Oklahoma City Thunder": "Oklahoma City Thunder",
    "New Orleans Pelicans": "New Orleans Pelicans",
    "New York Knicks": "New York Knicks",
    "San Antonio Spurs": "San Antonio Spurs",
    "Portland Trail Blazers": "Portland Trail Blazers",
    "Minnesota Timberwolves": "Minnesota Timberwolves",
    "Philadelphia 76ers": "Philadelphia 76ers",
    "Toronto Raptors": "Toronto Raptors",
    "Charlotte Hornets": "Charlotte Hornets",
    "Miami Heat": "Miami Heat",
    "Boston Celtics": "Boston Celtics",
    "Atlanta Hawks": "Atlanta Hawks",
    "Brooklyn Nets": "Brooklyn Nets",
    "Chicago Bulls": "Chicago Bulls",
    "Cleveland Cavaliers": "Cleveland Cavaliers",
    "Dallas Mavericks": "Dallas Mavericks",
    "Denver Nuggets": "Denver Nuggets",
    "Detroit Pistons": "Detroit Pistons",
    "Houston Rockets": "Houston Rockets",
    "Indiana Pacers": "Indiana Pacers",
    "Memphis Grizzlies": "Memphis Grizzlies",
    "Milwaukee Bucks": "Milwaukee Bucks",
    "Orlando Magic": "Orlando Magic",
    "Phoenix Suns": "Phoenix Suns",
    "Sacramento Kings": "Sacramento Kings",
    "Utah Jazz": "Utah Jazz",
    "Washington Wizards": "Washington Wizards",
}


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or str(v).strip() in ("", "—", "-"):
            return None
        return float(str(v).strip())
    except Exception:
        return None


def _fetch_html(url: str, retries: int = 3) -> str:
    session = requests.Session()
    session.headers.update(HEADERS)
    last_err: Exception = RuntimeError("no attempt")
    for attempt in range(1, retries + 1):
        try:
            # Primera peticion a la home para obtener cookies
            if attempt == 1:
                try:
                    session.get("https://www.basketball-reference.com/", timeout=TIMEOUT)
                    time.sleep(1.5)
                except Exception:
                    pass
            r = session.get(url, timeout=TIMEOUT)
            if r.status_code == 429:
                wait = 15 * attempt
                print(f"[advanced] rate-limited (429), esperando {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code} desde {url}")
            return r.text
        except Exception as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(4 * attempt)
    raise last_err


def _find_advanced_table(soup: BeautifulSoup):
    """Busca la tabla de stats avanzadas por equipo."""
    # BBRef puede comentar tablas con <!-- -->; descomenta si hace falta
    candidates = [
        "advanced-team",
        "advanced_team",
        "div_advanced-team",
        "div_advanced_team",
    ]
    for cid in candidates:
        tbl = soup.find("table", {"id": cid})
        if tbl:
            return tbl

    # Fallback: buscar en comentarios HTML
    for comment in soup.find_all(string=lambda t: isinstance(t, str) and "advanced" in t.lower() and "<table" in t):
        inner = BeautifulSoup(comment, "html.parser")
        for cid in candidates:
            tbl = inner.find("table", {"id": cid})
            if tbl:
                return tbl

    return None


def _parse_advanced_table(table) -> Dict[str, Dict[str, Any]]:
    """Extrae ORTG, DRTG y PACE de la tabla avanzada."""
    thead = table.find("thead")
    if not thead:
        return {}

    # Obtener nombres de columnas (puede haber filas de encabezado múltiples)
    header_rows = thead.find_all("tr")
    col_names: List[str] = []
    for th in header_rows[-1].find_all(["th", "td"]):
        col_names.append(th.get("data-stat", th.get_text(strip=True)).lower())

    # Mapear columnas relevantes
    col_idx: Dict[str, int] = {}
    for i, col in enumerate(col_names):
        if col in ("team", "team_name"):
            col_idx["team"] = i
        elif col in ("off_rtg", "ortg", "o_rtg"):
            col_idx["ortg"] = i
        elif col in ("def_rtg", "drtg", "d_rtg"):
            col_idx["drtg"] = i
        elif col == "pace":
            col_idx["pace"] = i

    if "team" not in col_idx:
        return {}

    result: Dict[str, Dict[str, Any]] = {}
    tbody = table.find("tbody")
    if not tbody:
        return {}

    for row in tbody.find_all("tr"):
        if "thead" in row.get("class", []) or row.get("class") == ["thead"]:
            continue
        cells = row.find_all(["td", "th"])
        if not cells:
            continue

        def _cell(idx: int) -> str:
            if idx >= len(cells):
                return ""
            return cells[idx].get_text(strip=True)

        team_raw = _cell(col_idx["team"])
        if not team_raw or team_raw in ("", "League Average", "Lg Avg"):
            continue

        # Limpiar asteriscos (equipos en playoffs)
        team_clean = re.sub(r"\*$", "", team_raw).strip()
        team_name = BBREF_ALIASES.get(team_clean, team_clean)

        result[team_name] = {
            "ortg": _safe_float(_cell(col_idx["ortg"])) if "ortg" in col_idx else None,
            "drtg": _safe_float(_cell(col_idx["drtg"])) if "drtg" in col_idx else None,
            "pace": _safe_float(_cell(col_idx["pace"])) if "pace" in col_idx else None,
        }

    return result


def fetch_advanced_stats() -> Dict[str, Dict[str, Any]]:
    """Devuelve {team_name: {ortg, drtg, pace}} o {} si no disponible."""
    try:
        html = _fetch_html(BBREF_URL)
    except Exception as exc:
        print(f"[advanced] no se pudo descargar BBRef: {exc}")
        return {}

    soup = BeautifulSoup(html, "html.parser")
    table = _find_advanced_table(soup)
    if not table:
        print(f"[advanced] tabla avanzada no encontrada en {BBREF_URL}")
        return {}

    data = _parse_advanced_table(table)
    print(f"[advanced] {len(data)} equipos parseados desde BBRef ({YEAR})")
    return data


def main() -> None:
    data = fetch_advanced_stats()

    Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(OUT_PATH).write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"OK -> {OUT_PATH} (teams={len(data)})")


if __name__ == "__main__":
    main()
