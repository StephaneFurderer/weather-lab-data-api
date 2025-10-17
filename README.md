# WeatherLab Data API

FastAPI service exposing hurricane track data for n8n.

## Run locally

```
cd weather-lab-data-api
PYTHONPATH="$(pwd)/.." uvicorn main:app --host 127.0.0.1 --port 8010 --reload
```

Endpoints:
- GET /health
- GET /available-dates
- GET /data?date=YYYY-MM-DD

## Deploy (Railway)

This directory includes a Dockerfile and railway.toml. Set the service root to `weather-lab-data-api` and deploy. Railway provides PORT; healthcheck is `/health`.
