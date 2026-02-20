#!/bin/bash

# 에러 발생 시 즉시 중단 및 파이프 에러 체크
set -eo pipefail

VARIABLE_FILE="variables.json"
TEMP_DIR="/tmp"

echo "Starting Airflow backup process..."

# ================================
# Backup Airflow Variables
# ================================
echo "Backup Airflow variables..."
docker exec airflow-apiserver airflow variables export $TEMP_DIR/$VARIABLE_FILE
docker cp airflow-apiserver:$TEMP_DIR/$VARIABLE_FILE ./"$VARIABLE_FILE"
docker exec airflow-apiserver rm $TEMP_DIR/$VARIABLE_FILE
echo "Variables backup completed: $VARIABLE_FILE"
