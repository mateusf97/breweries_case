FROM apache/airflow:2.7.2-python3.10

COPY requirements.txt .

USER airflow

RUN mkdir -p /opt/airflow/data/bronze /opt/airflow/data/silver /opt/airflow/data/gold /opt/airflow/logs \
    && chown -R airflow: /opt/airflow/data /opt/airflow/logs \
    && chmod -R 775 /opt/airflow/data /opt/airflow/logs

RUN pip install --no-cache-dir -r requirements.txt
