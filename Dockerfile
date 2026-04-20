FROM python:3.12-slim

RUN groupadd -r eia && useradd -r -g eia -m eia

WORKDIR /app

COPY pyproject.toml ./
COPY src/ src/
RUN pip install --no-cache-dir .

COPY widgets.json apps.json entrypoint.sh ./

RUN chmod +x entrypoint.sh && \
    mkdir -p data && chown -R eia:eia /app

USER eia

EXPOSE 7779

ENTRYPOINT ["./entrypoint.sh"]
CMD ["serve"]
