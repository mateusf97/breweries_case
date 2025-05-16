from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import json

# Argumentos padrão da DAG
default_args = {
    'owner': 'mateus',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5),
}

# Função para extrair dados da API e salvar na camada Bronze
def extract_bronze_data(**kwargs):
    # URL da API pública de cervejarias
    url = "https://api.openbrewerydb.org/v1/breweries?per_page=50"
    
    # Requisição dos dados da API
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Caminho onde o arquivo será salvo dentro do container
    output_path = "/opt/airflow/data/bronze/breweries_raw.json"

    # Garante que o diretório existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Salva os dados crus no formato JSON
    with open(output_path, 'w') as f:
        json.dump(data, f)

    print(f"✅ Dados salvos com sucesso em: {output_path}")

# Definição da DAG
with DAG(
    dag_id='brewery_pipeline_bronze',
    default_args=default_args,
    description='Pipeline para ingestão de dados da API Open Brewery - Camada Bronze',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 5, 16),
    catchup=False,
) as dag:

    # Tarefa de extração de dados da API para camada Bronze
    task_extract_bronze = PythonOperator(
        task_id='extract_bronze_data',
        python_callable=extract_bronze_data,
    )

    task_extract_bronze  # Define a tarefa na DAG
