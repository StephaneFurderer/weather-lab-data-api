<!-- 12b602ce-36e3-440a-a4a5-4a60733715a7 920c8b84-c705-4707-94e1-4ae36e457c98 -->
# WeatherLab Data API (FastAPI)

## Overview

Build a standalone FastAPI service that wraps `WeatherImpact/data_fetcher.py` to provide JSON endpoints consumable by n8n. Service supports caching and persistence of fetched CSVs via a pluggable storage layer (Postgres by default), and is deployable to Railway.

## Goals

- JSON-first responses (records + metadata)
- Modular folder: `api/` (service), `api/storage/` (DB + files), `api/schemas/` (Pydantic)
- Store fetched CSVs + fetch metadata in Postgres (Railway) with simple memoization
- CORS enabled for n8n
- Health and observability endpoints

## Folder Structure

```
/Users/sf/Applications/ai-cookbook/WeatherImpact/
├── api/
│   ├── main.py                 # FastAPI app (routers, startup)
│   ├── routers/
│   │   └── data.py             # /health, /available-dates, /data, /data-range, /summary
│   ├── schemas/
│   │   ├── base.py             # Common models
│   │   └── data.py             # Request/response models
│   ├── storage/
│   │   ├── base.py             # Storage interface
│   │   ├── postgres.py         # Postgres implementation (Railway)
│   │   └── filesystem.py       # Local dev storage
│   ├── services/
│   │   └── fetch_service.py    # Wraps HurricaneDataFetcher, caching, serialization
│   ├── utils/
│   │   ├── serialize.py        # Datetime→ISO, dataframe→records
│   │   └── cache.py            # LRU cache helper
│   └── settings.py             # Pydantic Settings (env)
├── WeatherImpact/
│   └── data_fetcher.py         # existing fetcher (imported by service)
├── requirements.txt            # + fastapi, uvicorn, pydantic, psycopg[binary]
├── Dockerfile                  # Railway deploy
└── railway.toml                # Service + Postgres
```

## Environment Settings

- `API_HOST` (default 0.0.0.0)
- `API_PORT` (default 8000)
- `CORS_ORIGINS` (csv; default `*`)
- `DB_URL` (Railway Postgres URL)
- `STORAGE_BACKEND` (postgres|filesystem; default postgres)
- `DATA_DIR` (for filesystem storage)

## Data Model (Postgres)

- `files` (id, date, filename, bytes_size, sha256, created_at)
- `fetches` (id, date, source_url, file_id FK, record_count, created_at)
- Index on `date` for fast lookup; upsert by date

## Endpoints

- GET `/health` → `{ status: "ok", time }`
- GET `/available-dates` → `{"dates": [YYYY-MM-DD, ...]}`
- GET `/data` params: `date` (YYYY-MM-DD, required), `force` (bool, default false)
  - Returns: `{ meta: {date, record_count, source, cached}, records: [...] }`
- POST `/data-range` body: `{ start: YYYY-MM-DD, end?: YYYY-MM-DD, days?: int, force?: bool }`
  - Returns: `{ meta: {...}, data: { "YYYY-MM-DD": {record_count, records: [...]}, ... } }`
- GET `/summary` params: `date`
  - Returns: summary from `HurricaneDataFetcher.get_hurricane_summary`

## Serialization Rules

- Datetime fields → ISO8601 strings
- Timedelta (lead_time) → seconds
- NaN → null

## Caching & Storage Flow

1) Request `/data?date=YYYY-MM-DD`

2) Check `fetches` by date (unless `force=true`)

3) If present: return stored records; else:

   - Use `HurricaneDataFetcher.download_hurricane_data(date)`
   - Serialize and store CSV (bytes or text) + create `files` + `fetches` rows
   - Return JSON `{meta, records}`

## Security

- Optional API key via header `X-API-Key` (enabled when `API_KEY` is set)
- CORS open by default; configurable

## Deployment (Railway)

- Dockerfile runs `uvicorn api.main:app --host 0.0.0.0 --port ${PORT}`
- `railway.toml` defines service and a Postgres plugin; `DB_URL` injected

## n8n Usage

- HTTP Request node: GET `https://<railway-app>.up.railway.app/data?date=2024-08-13`
- Response → use `{{ $json.records }}` in subsequent nodes
- For range: POST `/data-range` with JSON body

## Rollout Plan

1) Add deps; scaffold `api/` modules

2) Implement storage (postgres, filesystem)

3) Implement routers + service integration

4) Local run + Postman tests

5) Add Dockerfile + railway.toml; deploy to Railway

## Assumptions

- JSON is primary output; CSV downloads not required initially
- Postgres is available in Railway project

## To-dos

- [ ] add-deps: fastapi, uvicorn[standard], pydantic>=2, psycopg[binary], python-dotenv
- [ ] create-schemas: Pydantic models for meta, record, responses
- [ ] create-utils: serializers, cache
- [ ] create-storage: postgres + filesystem implementations
- [ ] create-api-main: FastAPI app + routers + CORS + health
- [ ] wire-fetcher: integrate HurricaneDataFetcher with storage + cache
- [ ] railway-deploy: Dockerfile + railway.toml + README with n8n examples

### To-dos

- [ ] Add FastAPI/uvicorn deps to requirements.txt
- [ ] Create Pydantic models for records, meta, and responses
- [ ] Add serializers and CSV/ZIP streaming helpers
- [ ] Implement FastAPI app with /health, /available-dates, /data, /data-range, /summary
- [ ] Integrate HurricaneDataFetcher into API with caching and conversions
- [ ] Add README section with n8n examples and endpoint docs
- [ ] Add Dockerfile and container run instructions (optional)