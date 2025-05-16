FROM apache/airflow:2.7.2-python3.10

COPY requirements.txt .

USER airflow

RUN pip install --no-cache-dir -r requirements.txt
