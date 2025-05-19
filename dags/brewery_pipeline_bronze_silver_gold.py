from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import json
import pandas as pd
import glob
import logging
import os
import json
import time
import logging
import requests
from math import ceil

# Argumentos padrão da DAG
default_args = {
    'owner': 'mateus',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5),
}



def extract_bronze_data():
    """
        Coleta todos os dados da API Open Brewery (paginada), respeita o limite de 200 por página.
        Consulta quantas páginas existem com /meta, percorre tudo com 1s de delay,
        se alguma falhar tenta de novo no final. Junta tudo num único JSON e salva na camada bronze.
    """

    base_url = "https://api.openbrewerydb.org/v1/breweries"
    meta_url = f"{base_url}/meta"
    per_page = 200
    all_data = []
    failed_urls = []

    try:
        # Obtém o total de registros para calcular o número de páginas
        meta_response = requests.get(meta_url)
        meta_response.raise_for_status()
        total_breweries = meta_response.json().get("total", 0)
        total_pages = ceil(total_breweries / per_page)
        logging.info(f"🔢 Total de registros: {total_breweries} | Total de páginas: {total_pages}")
    except Exception as e:
        logging.error("❌ Erro ao obter os metadados da API:", e)
        return

    # Loop pelas páginas com delay de 1 segundo
    for page in range(1, total_pages + 1):
        url = f"{base_url}?page={page}&per_page={per_page}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            all_data.extend(data)
            logging.info(f"✅ Página {page} coletada com sucesso. Total acumulado: {len(all_data)} registros.")
        except Exception as e:
            logging.error(f"⚠️ Erro na página {page}: {url}")
            failed_urls.append(url)
        time.sleep(1)  # Aguarda 1 segundo entre chamadas

    # Tenta novamente as URLs que falharam
    for url in failed_urls:
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            all_data.extend(data)
            logging.info(f"🔁 Retry bem-sucedido para: {url}")
        except Exception as e:
            logging.error(f"❌ Falha permanente em: {url}")

    # Salva os dados em JSON
    output_path = "/opt/airflow/data/bronze/breweries_raw.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(all_data, f)

    logging.info(f"📁 Dados salvos com sucesso em: {output_path}")


# Transformação para Silver
def transform_to_silver():
    input_path = "/opt/airflow/data/bronze/breweries_raw.json"
    output_dir = "/opt/airflow/data/silver/breweries"

    os.makedirs(output_dir, exist_ok=True)

    with open(input_path, 'r') as f:
        data = json.load(f)

    df = pd.json_normalize(data)

    cols = [
        "id", "name", "brewery_type", "address_1", "address_2", "address_3",
        "city", "state_province", "postal_code", "country", "longitude", "latitude",
        "phone", "website_url", "state", "street"
    ]
    df = df[cols]

    df = df.astype("string")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df[["address_2", "address_3"]] = df[["address_2", "address_3"]].fillna("")

    for state, group in df.groupby("state"):
        state_dir = os.path.join(output_dir, f"state={state.replace(' ', '_')}")
        os.makedirs(state_dir, exist_ok=True)
        group.to_parquet(os.path.join(state_dir, "breweries.parquet"), index=False)

    logging.info("✅ Dados transformados e salvos na camada Silver")


# Agregação para Gold
def aggregate_gold():
    silver_path = "/opt/airflow/data/silver/breweries"
    output_path = "/opt/airflow/data/gold/breweries_agg.csv"

    all_files = glob.glob(os.path.join(silver_path, "state=*/breweries.parquet"))
    dfs = [pd.read_parquet(f) for f in all_files]
    full_df = pd.concat(dfs, ignore_index=True)

    agg_df = full_df.groupby(["brewery_type", "state"]).size().reset_index(name="brewery_count")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    agg_df.to_csv(output_path, index=False)

    logging.info("✅ Agregação concluída e dados salvos na camada Gold")


# DAG principal
with DAG(
    dag_id="brewery_pipeline_bronze_silver_gold",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description="Pipeline Bronze -> Silver -> Gold",
    tags=["brewery"],
) as dag:

    task_extract_bronze = PythonOperator(
        task_id='extract_bronze_data',
        python_callable=extract_bronze_data,
    )

    transform_task = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_to_silver,
    )

    aggregate_task = PythonOperator(
        task_id="aggregate_breweries",
        python_callable=aggregate_gold,
    )

    task_extract_bronze >> transform_task >> aggregate_task
