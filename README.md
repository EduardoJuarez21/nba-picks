# nba_service

Microservicio Flask para separar el flujo NBA del proyecto principal.

## Endpoints

- `GET /nba/picks?day=DD-MM|YYYY-MM-DD&force=true|false`
- `GET /nba/status/<job_id>`
- `GET /nba/picks/result?day=DD-MM|YYYY-MM-DD`
- `POST /picks/match` con body JSON: `{"league":"nba","match_id":12345,"day":"YYYY-MM-DD"}`

## Variables de entorno clave

- `API_AUTH_TOKEN` (requerida)
- `API_AUTH_HEADER` (default: `X-API-Token`)
- `API_AUTH_PUBLIC_PATHS` (default: `/,/nba/picks/result`)
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `DATA_DIR` (default: `data`)
- `NBA_STATS_USER_AGENT` (opcional, para `stats.nba.com`)

## Ejecutar local

Desde la raiz del repo:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

Por defecto corre en `http://localhost:5001`.

## Despliegue

Con Render, puedes usar `render.yaml` o configurar manualmente:

- Build command: `pip install -r requirements.txt`
- Start command: `gunicorn wsgi:app --bind 0.0.0.0:$PORT --timeout 600`
