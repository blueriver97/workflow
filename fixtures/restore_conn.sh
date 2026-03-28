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

echo "Starting Airflow recovery process... (container: $CONTAINER)"

# ================================
# Restore Airflow Connections
# ================================
if [ -f "$CONNECTION_FILE" ]; then
  echo "Restoring Airflow connections from $CONNECTION_FILE..."
  docker cp "$CONNECTION_FILE" "$CONTAINER:$TEMP_DIR/restore_connections.json"
  docker exec -u root "$CONTAINER" chown airflow "$TEMP_DIR/restore_connections.json"
  docker exec "$CONTAINER" airflow connections import "$TEMP_DIR/restore_connections.json"
  docker exec "$CONTAINER" rm "$TEMP_DIR/restore_connections.json"
  echo "Connections restoration completed."
else
  echo "Warning: $CONNECTION_FILE not found. Skipping connections restoration."
fi
