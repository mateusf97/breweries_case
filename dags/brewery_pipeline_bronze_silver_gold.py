from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import glob
import time
import logging
import traceback
import requests
import pandas as pd
from math import ceil


# Argumentos padrão da DAG
default_args = {
    'owner': 'mateus',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5),
}


#Extração de dados para a BRonze
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


def transform_to_silver():
    """
    Transforma os dados crus da camada Bronze em formato colunar (Parquet),
    com partição por estado (`state`). A função valida e define os tipos
    de dados esperados, trata campos ausentes e garante a estrutura final.
    """
    input_path = "/opt/airflow/data/bronze/breweries_raw.json"
    output_dir = "/opt/airflow/data/silver/breweries"

    try:
        os.makedirs(output_dir, exist_ok=True)

        # Lê os dados do JSON
        with open(input_path, 'r') as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise ValueError("Formato inválido: o JSON deve conter uma lista de registros")

        df = pd.json_normalize(data)

        # Colunas esperadas
        expected_cols = [
            "id", "name", "brewery_type", "address_1", "address_2", "address_3",
            "city", "state_province", "postal_code", "country", "longitude", "latitude",
            "phone", "website_url", "state", "street"
        ]

        # Verifica colunas faltantes
        missing_cols = [col for col in expected_cols if col not in df.columns]
        for col in missing_cols:
            df[col] = None  # adiciona coluna ausente com valor None

        df = df[expected_cols]  # reordena/filtra as colunas

        # Tipagem padrão: string + float para coordenadas
        df = df.astype("string")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df[["address_2", "address_3"]] = df[["address_2", "address_3"]].fillna("")

        # Salva em formato Parquet particionado por estado
        for state, group in df.groupby("state"):
            try:
                if not isinstance(state, str):
                    state = "unknown"
                safe_state = state.replace(" ", "_").replace("/", "_")
                state_dir = os.path.join(output_dir, f"state={safe_state}")
                os.makedirs(state_dir, exist_ok=True)

                group.to_parquet(
                    os.path.join(state_dir, "breweries.parquet"),
                    index=False,
                    engine="pyarrow"
                )
            except Exception as e:
                logging.error(f"Erro ao salvar Parquet para o estado: {state}")
                logging.error(traceback.format_exc())

        logging.info("✅ Dados transformados e salvos na camada Silver")

    except Exception as e:
        logging.error("❌ Erro durante a transformação da camada Silver")
        logging.error(traceback.format_exc())

import os
import glob
import logging
import traceback
import pandas as pd

def aggregate_gold():
    """
    Realiza agregações a partir da camada Silver e gera 3 CSVs:
    - Contagem de cervejarias por tipo
    - Contagem por estado
    - Contagem por tipo e estado

    Os resultados são salvos na camada Gold.
    """
    silver_path = "/opt/airflow/data/silver/breweries"
    gold_path = "/opt/airflow/data/gold"

    try:
        os.makedirs(gold_path, exist_ok=True)

        # Encontra todos os arquivos Parquet particionados por estado
        all_files = glob.glob(os.path.join(silver_path, "state=*/breweries.parquet"))

        if not all_files:
            logging.warning("⚠️ Nenhum arquivo encontrado na camada Silver para agregação.")
            return

        # Lê todos os arquivos Parquet em um único DataFrame
        dfs = []
        for f in all_files:
            try:
                dfs.append(pd.read_parquet(f))
            except Exception as e:
                logging.error(f"❌ Erro ao ler arquivo: {f}")
                logging.error(traceback.format_exc())

        if not dfs:
            logging.warning("⚠️ Nenhum dado válido carregado para agregação.")
            return

        full_df = pd.concat(dfs, ignore_index=True)

        # Verifica se colunas necessárias estão presentes
        required_cols = ["brewery_type", "state"]
        if not all(col in full_df.columns for col in required_cols):
            logging.error("❌ Colunas necessárias para agregação ausentes nos dados.")
            return

        # --- Agregação 1: por tipo de cervejaria ---
        agg_by_type = full_df.groupby("brewery_type").size().reset_index(name="brewery_count")
        agg_by_type.to_csv(os.path.join(gold_path, "brewery_count_by_type.csv"), index=False)

        # --- Agregação 2: por estado ---
        agg_by_state = full_df.groupby("state").size().reset_index(name="brewery_count")
        agg_by_state.to_csv(os.path.join(gold_path, "brewery_count_by_state.csv"), index=False)

        # --- Agregação 3: por tipo e estado ---
        agg_by_type_state = full_df.groupby(["brewery_type", "state"]).size().reset_index(name="brewery_count")
        agg_by_type_state.to_csv(os.path.join(gold_path, "brewery_count_by_type_and_state.csv"), index=False)

        logging.info("✅ Agregações concluídas e dados salvos na camada Gold")

    except Exception as e:
        logging.error("❌ Erro na agregação da camada Gold")
        logging.error(traceback.format_exc())


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