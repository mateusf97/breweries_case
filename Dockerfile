FROM apache/airflow:2.7.2-python3.10

COPY requirements.txt .

USER root

# Criar as pastas e ajustar permissões (logs e data)
RUN mkdir -p /opt/airflow/logs /opt/airflow/data \
    && chown -R airflow: /opt/airflow/logs /opt/airflow/data \
    && chmod -R 777 /opt/airflow/logs /opt/airflow/data

USER airflow

RUN pip install --no-cache-dir -r requirements.txt
