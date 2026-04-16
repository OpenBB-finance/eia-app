FROM python:3.12-slim

RUN groupadd -r eia && useradd -r -g eia -m eia

WORKDIR /app

COPY pyproject.toml ./
COPY src/ src/
RUN pip install --no-cache-dir .

COPY widgets.json apps.json entrypoint.sh ./

RUN chmod +x entrypoint.sh && \
    mkdir -p data _built_data && chown -R eia:eia /app

USER eia

RUN EIA_DB_PATH=/app/_built_data/eia_bulk.duckdb EIA_DATA_DIR=/app/_built_data python -m src.ingest

EXPOSE 7779

ENTRYPOINT ["./entrypoint.sh"]
CMD ["serve"]
