#!/bin/sh
set -e

if [ ! -f "${EIA_DB_PATH:-/app/data/eia_bulk.duckdb}" ] && [ -f /app/_built_data/eia_bulk.duckdb ]; then
    echo "Seeding database from build-time snapshot..."
    cp /app/_built_data/eia_bulk.duckdb "${EIA_DB_PATH:-/app/data/eia_bulk.duckdb}"
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
