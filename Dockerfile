FROM python:3.12-slim

RUN groupadd -g 32767 eia && useradd -u 32767 -g 32767 -m eia

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
