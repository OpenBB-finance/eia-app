#!/bin/sh
set -e

DB_PATH="${EIA_DB_PATH:-/app/data/eia_bulk.duckdb}"

if [ ! -f "$DB_PATH" ]; then
    echo "No database found at $DB_PATH, running initial ingestion..."
    python -m src.ingest
fi

case "$1" in
    ingest)
        shift
        exec python -m src.ingest "$@"
        ;;
    serve)
        shift
        exec uvicorn src.main:app --host 0.0.0.0 --port 7779 "$@"
        ;;
    *)
        echo "Usage: entrypoint.sh {ingest|serve}"
        echo "  ingest  - Download EIA bulk data and load into DuckDB"
        echo "  serve   - Start the FastAPI/OpenBB backend on port 7779"
        exit 1
        ;;
esac
