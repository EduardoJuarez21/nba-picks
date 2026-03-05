import hmac
import os
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS

try:
    from nba_service import services  # type: ignore
except ModuleNotFoundError:
    import services  # type: ignore

BASE_DIR = Path(__file__).resolve().parent
if (BASE_DIR / "services.py").is_file():
    ROOT_DIR = BASE_DIR
else:
    ROOT_DIR = BASE_DIR.parent
ENV_PATH = Path(os.getenv("ENV_PATH", ROOT_DIR / ".env"))
if ENV_PATH.is_file():
    load_dotenv(ENV_PATH, override=False)

app = Flask(__name__)

API_AUTH_HEADER = (os.getenv("API_AUTH_HEADER", "X-API-Token") or "X-API-Token").strip()
API_AUTH_TOKEN = (os.getenv("API_AUTH_TOKEN", "") or "").strip()
API_AUTH_LOG_TOKEN = (os.getenv("API_AUTH_LOG_TOKEN", "false") or "false").strip().lower() == "true"

CORS(
    app,
    resources={r"/*": {"origins": os.getenv("CORS_ORIGINS", "*").split(",")}},
    supports_credentials=True,
)


def _extract_token() -> str:
    token = (request.headers.get(API_AUTH_HEADER) or "").strip()
    if token:
        return token
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""


@app.before_request
def _require_api_token():
    if request.method == "OPTIONS":
        return None
    if not API_AUTH_TOKEN:
        return jsonify({"status": "error", "error": "API auth token not configured"}), 500
    provided = _extract_token()
    token_match = bool(provided) and hmac.compare_digest(provided, API_AUTH_TOKEN)
    if API_AUTH_LOG_TOKEN:
        print(
            f"[auth-debug] method={request.method} path={request.path} "
            f"header={API_AUTH_HEADER} provided_present={bool(provided)} token_match={token_match}"
        )
    if not token_match:
        return jsonify({"status": "error", "error": "unauthorized"}), 401
    return None


@app.after_request
def _add_cors_headers(resp):
    origin = os.getenv("CORS_ALLOW_ORIGIN", "*")
    resp.headers.setdefault("Access-Control-Allow-Origin", origin)
    allow_headers = f"Content-Type, Authorization, {API_AUTH_HEADER}"
    resp.headers.setdefault("Access-Control-Allow-Headers", allow_headers)
    resp.headers.setdefault("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    return resp


@app.get("/")
def root():
    return {"ok": True, "service": "nba-picks-api"}


@app.get("/nba/picks")
def run_nba_picks():
    payload = {}
    day = request.args.get("day")
    if day:
        payload["day"] = day
    force = request.args.get("force")
    if force is not None:
        payload["force"] = force
    body, code = services.queue_job(payload or None)
    return jsonify(body), code


@app.get("/nba/status/<job_id>")
def get_nba_status(job_id: str):
    body, code = services.get_status(job_id)
    return jsonify(body), code


@app.get("/nba/picks/result")
def get_nba_picks_for_day():
    day = services.normalize_day(request.args.get("day"))
    if request.args.get("day") and not day:
        return jsonify({"status": "error", "error": "Formato invalido. Usa DD-MM o YYYY-MM-DD."}), 400
    if not day:
        day = services.today_utc_iso()
    body, code = services.get_picks_result(day)
    return jsonify(body), code


@app.post("/picks/match")
def picks_for_match():
    payload = request.get_json(silent=True) or {}
    league = str(payload.get("league") or "nba").strip().lower()
    if league != "nba":
        return jsonify({"status": "error", "error": "Este servicio solo soporta NBA"}), 400
    match_id = payload.get("match_id")
    if match_id in (None, ""):
        return jsonify({"status": "error", "error": "Falta match_id"}), 400
    try:
        match_id = int(match_id)
    except Exception:
        return jsonify({"status": "error", "error": "match_id invalido"}), 400
    body, code = services.picks_for_match_id(match_id, day_raw=payload.get("day"))
    return jsonify(body), code


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5001"))
    app.run(host="0.0.0.0", port=port)
