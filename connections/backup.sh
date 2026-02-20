#!/bin/bash

# 에러 발생 시 즉시 중단 및 파이프 에러 체크
set -eo pipefail

CONNECTION_FILE="connections.json"
TEMP_DIR="/tmp"

echo "Starting Airflow backup process..."

# ================================
# Backup Airflow Connections
# ================================
echo "Backup Airflow Connections..."
docker exec airflow-apiserver airflow connections export $TEMP_DIR/$CONNECTION_FILE --format json
docker cp airflow-apiserver:$TEMP_DIR/$CONNECTION_FILE ./"$CONNECTION_FILE"
docker exec airflow-apiserver rm $TEMP_DIR/$CONNECTION_FILE
echo "Connections backup completed: $CONNECTION_FILE"
