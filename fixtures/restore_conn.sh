#!/bin/bash

# 에러 발생 시 즉시 중단 및 파이프 에러 체크
set -eo pipefail

CONNECTION_FILE="connections.json"
TEMP_DIR="/tmp"

echo "Starting Airflow recovery process..."

# ================================
# Restore Airflow Connections
# ================================
if [ -f "$CONNECTION_FILE" ]; then
  echo "Restoring Airflow connections from $CONNECTION_FILE..."
  docker cp "$CONNECTION_FILE" airflow-apiserver:$TEMP_DIR/restore_connections.json
  docker exec -u root airflow-apiserver chown airflow $TEMP_DIR/restore_connections.json
  docker exec airflow-apiserver airflow connections import $TEMP_DIR/restore_connections.json
  docker exec airflow-apiserver rm $TEMP_DIR/restore_connections.json
  echo "Connections restoration completed."
else
  echo "Warning: $CONNECTION_FILE not found. Skipping connections restoration."
fi
