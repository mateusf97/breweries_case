#!/bin/bash

# Nome do container (ajuste conforme necessário)
CONTAINER_NAME=airflow_scheduler

# Caminho local e no container
LOCAL_SCRIPT="test_pipeline.py.py"
CONTAINER_PATH="/opt/airflow/tests/$LOCAL_SCRIPT"

echo "📦 Copiando script para o container..."
docker cp "$LOCAL_SCRIPT" "$CONTAINER_NAME":"$CONTAINER_PATH"

echo "🚀 Executando script dentro do container..."
docker exec -it "$CONTAINER_NAME" python "$CONTAINER_PATH"

# Descomente se quiser apagar o script depois:
# echo "🧹 Limpando script do container..."
# docker exec -it "$CONTAINER_NAME" rm "$CONTAINER_PATH"
