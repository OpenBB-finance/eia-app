# EIA Bulk Data Service

FastAPI backend that ingests EIA bulk data and WPSR weekly reports into DuckDB and serves them as an [OpenBB Workspace](https://pro.openbb.co) backend.

**Bulk datasets:** STEO (Short-Term Energy Outlook), TOTAL (Total Energy), INTL (International Energy Data), SEDS (State Energy Data System), IEO (International Energy Outlook).

**Weekly data:** WPSR (Weekly Petroleum Status Report) — downloaded directly from EIA XLS files.

The database is built at Docker build time. A background task refreshes data every Wednesday at 11:00 AM ET (aligned with the WPSR release schedule). Only datasets with a newer `last_updated` timestamp are re-downloaded.

## Local development

```bash
# create env
conda create -n eia python=3.12 -y
conda activate eia

# install
pip install -e .

# ingest data (builds the DuckDB database)
python -m src.ingest

# run the server
uvicorn src.main:app --host 0.0.0.0 --port 7779 --reload
```

The API will be available at `http://localhost:7779`. The ingest step downloads ~50 MB of bulk ZIPs plus WPSR XLS files and builds the DuckDB database (~200K series, ~10M observations). Subsequent ingests skip unchanged datasets.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `EIA_DATASETS` | `STEO,TOTAL,INTL,SEDS,IEO` | Comma-separated dataset codes, or `ALL` |
| `EIA_DB_PATH` | `data/eia_bulk.duckdb` | Path to the DuckDB file |
| `EIA_DATA_DIR` | `data` | Directory for downloaded ZIPs and DB |
| `EIA_FORCE_RELOAD` | `false` | Force re-download even if unchanged |
| `PORT` | `7779` | Server port |

## Docker

```bash
docker compose up --build
```

The Docker build runs `python -m src.ingest` to bake the database into the image. At runtime, the API opens the database read-only and schedules a background refresh every Wednesday at 11:00 AM ET. An nginx sidecar handles caching and CORS.

Data persists in the `eia_data` volume.

## Dokku deployment

```bash
# create app
dokku apps:create eia

# set env
dokku config:set eia EIA_DATASETS=STEO,TOTAL,INTL,SEDS,IEO

# persistent storage for the DuckDB database and downloaded ZIPs
dokku storage:ensure-directory eia-data
dokku storage:mount eia /var/lib/dokku/data/storage/eia-data:/app/data

# custom nginx config (caching + CORS)
cp nginx.conf.sigil /home/dokku/eia/nginx.conf.sigil

# deploy
git push dokku main
```

The `nginx.conf.sigil` template provides the same caching/CORS behavior as the Docker nginx config, using Dokku's template variables for dynamic container routing.

## API endpoints

### Config & health

| Endpoint | Description |
|---|---|
| `GET /health` | Health check with dataset count |
| `GET /widgets.json` | OpenBB widget definitions |
| `GET /apps.json` | OpenBB app layout |

### Browse & search

| Endpoint | Description |
|---|---|
| `GET /dataset_overview` | All datasets with series counts |
| `GET /dataset_metrics` | Summary KPIs (datasets, series, observations, WPSR) |
| `GET /series_search?q=&dataset_id=&fuel_type=&measure_type=&frequency=&limit=` | Full-text + filtered search (BM25 or ILIKE fallback) |
| `GET /series_detail?series_id=` | Markdown detail card for a series |
| `GET /category_breakdown?dataset_id=` | Category tree with series counts |

### Time series

| Endpoint | Description |
|---|---|
| `GET /time_series_chart?series_id=&start_date=&end_date=` | Single series observations (chart view) |
| `GET /time_series_table?series_id=&start_date=&end_date=` | Single series observations (table view) |
| `GET /multi_series_chart?series_ids=A,B&start_date=&end_date=` | Multiple series pivoted by date (max 10) |

### STEO

| Endpoint | Description |
|---|---|
| `GET /steo_table_options` | Available STEO table IDs with names |
| `GET /steo_table?table_id=&start_date=&end_date=` | STEO data pivoted by date, columns include units |

### WPSR

| Endpoint | Description |
|---|---|
| `GET /wpsr_category_options` | WPSR categories (Balance Sheet, Imports, Stocks, etc.) |
| `GET /wpsr_table_options?category=` | Tables within a WPSR category |
| `GET /wpsr_data?category=&table=&start_date=&end_date=` | WPSR data pivoted by date |

Multi-series and pivoted endpoints return rows indexed by date with one column per series. Column names are shortened by stripping shared prefixes/suffixes.
