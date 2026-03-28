#!/bin/bash

# 에러 발생 시 즉시 중단 및 파이프 에러 체크
set -eo pipefail

VARIABLE_FILE="variables.json"
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
# Backup Airflow Variables
# ================================
echo "Backup Airflow variables..."
docker exec "$CONTAINER" airflow variables export "$TEMP_DIR/$VARIABLE_FILE"
docker cp "$CONTAINER:$TEMP_DIR/$VARIABLE_FILE" ./"$VARIABLE_FILE"
docker exec "$CONTAINER" rm "$TEMP_DIR/$VARIABLE_FILE"
echo "Variables backup completed: $VARIABLE_FILE"
