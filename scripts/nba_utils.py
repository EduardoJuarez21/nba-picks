import os
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ENV_PATH = Path(os.getenv("ENV_PATH", ".env"))
if ENV_PATH.is_file():
    load_dotenv(ENV_PATH, override=False)

def resolve_nba_season(day_str: str) -> str:
    season_override = os.getenv("NBA_SEASON")
    if season_override:
        return season_override

    try:
        day = date.fromisoformat(day_str)
    except Exception:
        raise SystemExit("DAY invalido, usa YYYY-MM-DD")

    year = day.year
    season_start = year if day.month >= 7 else year - 1
    return f"{season_start}-{str(season_start + 1)[-2:]}"


def iso_utc_from_timestamp(ts: int | float | None) -> str:
    if not ts:
        return ""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def ensure_parent_dir(path: Path | str) -> None:
    target = Path(path)
    parent = target.parent
    if parent and not parent.exists():
        parent.mkdir(parents=True, exist_ok=True)


def versioned_path(league: str, subdir: str, name: str, day: str) -> str:
    safe_day = day.replace("/", "-")
    return str(Path("fuente") / league / subdir / f"{name}_{safe_day}.json")


def nba_stats_headers() -> dict[str, str]:
    """Headers compatibles con stats.nba.com para evitar bloqueos por bot/WAF."""
    user_agent = os.getenv(
        "NBA_STATS_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    ).strip()
    referer = os.getenv("NBA_STATS_REFERER", "https://www.nba.com/").strip()
    origin = os.getenv("NBA_STATS_ORIGIN", "https://www.nba.com").strip()
    return {
        "Host": "stats.nba.com",
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "x-nba-stats-origin": "stats",
        "x-nba-stats-token": "true",
        "Connection": "keep-alive",
        "Referer": referer,
        "Origin": origin,
    }
