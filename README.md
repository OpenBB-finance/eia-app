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

The API will be available at `http://localhost:7779`. The ingest step downloads bulk ZIPs plus WPSR XLS files and builds the DuckDB database (~200K series, ~10M observations). Subsequent ingests skip unchanged datasets.

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

Docker runs `python -m src.ingest` at entry. At runtime, the API schedules a background refresh every Wednesday at 11:00 AM ET for new WPSR releases, and the bulk manifest file is read nightly to check if any individual datasets have been updated.

Data persists in the `eia_data` volume.

## Deployment

Deployment is automated via GitHub Actions using [Dokku](https://dokku.com). Pushing to `main` triggers a deploy.
