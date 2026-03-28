#!/bin/bash

# 에러 발생 시 즉시 중단 및 파이프 에러 체크
set -eo pipefail

CONNECTION_FILE="connections.json"
TEMP_DIR="/tmp"

# Airflow 컨테이너 자동 감지 (AF3: apiserver, AF2: webserver)
if docker ps --format '{{.Names}}' | grep -q '^airflow-apiserver$'; then
  CONTAINER="airflow-apiserver"
elif docker ps --format '{{.Names}}' | grep -q '^airflow-webserver$'; then
  CONTAINER="airflow-webserver"
else
  echo "Error: No running Airflow container found (airflow-apiserver or airflow-webserver)." >&2
  exit 1
fi

echo "Starting Airflow backup process... (container: $CONTAINER)"

# ================================
# Backup Airflow Connections
# ================================
echo "Backup Airflow Connections..."
docker exec "$CONTAINER" airflow connections export "$TEMP_DIR/$CONNECTION_FILE" --format json
docker cp "$CONTAINER:$TEMP_DIR/$CONNECTION_FILE" ./"$CONNECTION_FILE"
docker exec "$CONTAINER" rm "$TEMP_DIR/$CONNECTION_FILE"
echo "Connections backup completed: $CONNECTION_FILE"
