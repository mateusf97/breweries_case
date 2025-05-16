from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import os
import pandas as pd

def transform_to_silver():
    input_path = "/opt/airflow/data/bronze/breweries_raw.json"
    output_dir = "/opt/airflow/data/silver/breweries"

    os.makedirs(output_dir, exist_ok=True)

    with open(input_path, 'r') as f:
        data = json.load(f)

    df = pd.json_normalize(data)

    # Mantendo todas as colunas pedidas
    cols = [
        "id", "name", "brewery_type", "address_1", "address_2", "address_3",
        "city", "state_province", "postal_code", "country", "longitude", "latitude",
        "phone", "website_url", "state", "street"
    ]
    df = df[cols]

    # Tipagem explícita
    df = df.astype({
        "id": "string",
        "name": "string",
        "brewery_type": "string",
        "address_1": "string",
        "address_2": "string",
        "address_3": "string",
        "city": "string",
        "state_province": "string",
        "postal_code": "string",
        "country": "string",
        "phone": "string",
        "website_url": "string",
        "state": "string",
        "street": "string"
    })

    # longitude e latitude: float com coercion (transforma erros em NaN)
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")

    # Exemplo de transformação: preencher endereços nulos com string vazia
    df[["address_2", "address_3"]] = df[["address_2", "address_3"]].fillna("")

    # Salva como Parquet particionado por estado (state)
    for state, group in df.groupby("state"):
        state_dir = os.path.join(output_dir, f"state={state.replace(' ', '_')}")
        os.makedirs(state_dir, exist_ok=True)
        group.to_parquet(os.path.join(state_dir, "breweries.parquet"), index=False)


default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="brewery_pipeline_silver",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description="Camada Silver - Transformação para Parquet",
    tags=["silver", "brewery"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_to_silver,
    )

    transform_task
