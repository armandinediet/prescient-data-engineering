Beleza. Aqui vai um **README pronto** (copiar e colar) e depois um jeito **simples e não caótico** de pôr cron.

---

## README.md (cola na raiz)

````md
# Weather Platform (OpenWeather → Postgres → dbt)

Small, production-minded data pipeline that ingests OpenWeather 5-day / 3-hour forecast data and loads it into Postgres.
The pipeline is split into 3 layers:

1) **Raw (bronze)**: store the API response as JSONB (schema-safe, resilient to changes)
2) **Stage (silver)**: flatten the JSON into a relational table using pandas
3) **Curated (gold)**: final models built with dbt

## Architecture

- **Ingest**: `OpenWeatherMap API` → `raw_api_events (jsonb)`
- **Stage**: `raw_api_events` → `ext_openweather_forecast` (flattened)
- **Transform**: dbt models → curated marts / analytics tables

## Requirements

- Docker (for Postgres)
- Python 3.12+
- Poetry

## Setup

### 1) Create and load environment variables

Create a `.env` in the repository root:

```bash
OPENWEATHER_API_KEY=your_key_here
DATABASE_URL=postgresql://postgres:postgres@localhost:5433/weather
````

Load it into your shell:

```bash
set -a
source .env
set +a
```

### 2) Start Postgres

```bash
docker compose up -d
```

### 3) Install Python dependencies

```bash
poetry install
```
### Search cities
```bash
gunzip -c city.list.json.gz \
| jq '.[] | select(.name=="São Paulo" and .country=="BR")'
```


## Run the pipeline

### A) Ingest (raw JSONB)

```bash
poetry run python -m cli ingest --job openweather_forecast
```

Check:

```sql
select job_name, status_code, error, requested_at
from raw_api_events
order by requested_at desc
limit 20;
```

### B) Stage / Flatten (pandas)

```bash
poetry run python -m cli stage --job openweather_forecast_flatten
```

Check:

```sql
select city_id, forecast_dt, temp, humidity
from ext_openweather_forecast
order by forecast_dt desc
limit 20;
```

### C) Transform (dbt)

Run from repository root:

```bash
poetry run dbt run --project-dir dbt --profiles-dir dbt
poetry run dbt test --project-dir dbt --profiles-dir dbt
```

### scheduling
```bash
 poetry run python -m cli server
```
### remove all cron
```bash
crontab -r
```

## Configuration

Ingestion jobs are declared in `ingests/registry.yaml`.
Each job references a config file (example: `ingests/openweather.yaml`) where cities and API parameters are defined.

This makes it easy to add new ingests without changing orchestration code: add a new entry to the registry + a new config file.

## Notes

* Raw ingestion is designed to be resilient to schema changes by keeping the original payload in JSONB.
* Flattening happens before dbt to avoid complex JSON parsing in SQL and to keep dbt models clean and portable.
* This pipeline is intentionally simple and can be deployed as a container + cron schedule in production.

````

