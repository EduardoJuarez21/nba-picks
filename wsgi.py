try:
    from app import app
except ModuleNotFoundError:
    from nba_service.app import app  # type: ignore

