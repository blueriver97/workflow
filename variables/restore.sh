#!/bin/bash

# 에러 발생 시 즉시 중단 및 파이프 에러 체크
set -eo pipefail

VARIABLE_FILE="variables.json"
TEMP_DIR="/tmp"

echo "Starting Airflow recovery process..."

# ================================
# Restore Airflow Variables
# ================================
if [ -f "$VARIABLE_FILE" ]; then
  echo "Restoring Airflow variables from $VARIABLE_FILE..."
  docker cp "$VARIABLE_FILE" airflow-apiserver:$TEMP_DIR/restore_variables.json
  docker exec -u root airflow-apiserver chown airflow $TEMP_DIR/restore_variables.json
  docker exec airflow-apiserver airflow variables import $TEMP_DIR/restore_variables.json
  docker exec airflow-apiserver rm $TEMP_DIR/restore_variables.json
  echo "Variables restoration completed."
else
  echo "Warning: $VARIABLE_FILE not found. Skipping variables restoration."
fi
