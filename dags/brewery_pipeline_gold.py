from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import glob

def aggregate_gold():
    silver_path = "/opt/airflow/data/silver/breweries"
    output_path = "/opt/airflow/data/gold/breweries_agg.csv"

    all_files = glob.glob(os.path.join(silver_path, "state=*/breweries.parquet"))

    dfs = []
    for file in all_files:
        df = pd.read_parquet(file)
        dfs.append(df)
    
    full_df = pd.concat(dfs, ignore_index=True)

    # Agregação: contar por brewery_type e state
    agg_df = full_df.groupby(["brewery_type", "state"]).size().reset_index(name="brewery_count")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    agg_df.to_csv(output_path, index=False)

default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="brewery_pipeline_gold",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description="Camada Gold - Agregação para visualização",
    tags=["gold", "brewery"],
) as dag:

    aggregate_task = PythonOperator(
        task_id="aggregate_breweries",
        python_callable=aggregate_gold,
    )

    aggregate_task
