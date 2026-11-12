# Une seule image pour les trois services Dagster (webserver, démon, code) :
# même version de Dagster partout, un seul build.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    GAZETTE_DATA_DIR=/data \
    PYTHONPATH=/opt/gazette \
    TZ=Europe/Paris

WORKDIR /opt/gazette

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dagster.yaml workspace.yaml ${DAGSTER_HOME}/
COPY pyproject.toml ./
COPY docs/sources.csv ./docs/sources.csv
COPY migrations ./migrations
COPY gazette ./gazette
COPY tests ./tests

RUN mkdir -p /data /opt/dagster/storage
