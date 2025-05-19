import os
import json
import shutil
import pytest
import requests
import pandas as pd
import sys

from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from brewery_pipeline_bronze_silver_gold import extract_bronze_data, transform_to_silver, aggregate_gold

# Use caminhos temporários
BRONZE_PATH = "/data/bronze/breweries_raw.json"
SILVER_PATH = "/data/silver/breweries"
GOLD_PATH = "/data/gold"


@pytest.fixture(scope="module", autouse=True)
def clean_dirs():
    # Limpa diretórios antes e depois dos testes
    for path in ["/tmp/test_pipeline"]:
        if os.path.exists(path):
            shutil.rmtree(path)
    yield
    for path in ["/tmp/test_pipeline"]:
        if os.path.exists(path):
            shutil.rmtree(path)


def test_api_status_code_200():
    url = "https://api.openbrewerydb.org/v1/breweries/b54b16e1-ac3b-4bff-a11f-f7ae9ddc27e0"
    resp = requests.get(url)
    assert resp.status_code == 200


def test_api_content():
    url = "https://api.openbrewerydb.org/v1/breweries/b54b16e1-ac3b-4bff-a11f-f7ae9ddc27e0"
    expected = {
        "id": "b54b16e1-ac3b-4bff-a11f-f7ae9ddc27e0",
        "name": "MadTree Brewing 2.0",
        "brewery_type": "regional",
        "address_1": "5164 Kennedy Ave",
        "city": "Cincinnati",
        "state": "Ohio",
    }
    data = requests.get(url).json()
    for key, value in expected.items():
        assert data[key] == value


def test_extract_creates_bronze():
    print('Iniciando teste Bronze')
    extract_bronze_data()  # chama sem argumento
    output_path = "/opt/airflow/data/bronze/breweries_raw.json"
    assert os.path.exists(output_path)
    with open(output_path, 'r') as f:
        data = json.load(f)
        assert isinstance(data, list)
        assert len(data) > 0  # json não pode estar vazio


def test_transform_creates_silver():
    transform_to_silver()  # chama sem argumento
    output_dir = "/opt/airflow/data/silver/breweries"
    files = list(Path(output_dir).rglob("*.parquet"))
    assert len(files) > 0
    df = pd.read_parquet(files[0])
    # verifica colunas importantes da transformação
    for col in ["state", "brewery_type", "name"]:
        assert col in df.columns
    assert not df.empty


def test_aggregate_creates_gold():
    aggregate_gold()  # chama sem argumento
    gold_path = "/opt/airflow/data/gold"
    expected_files = [
        "brewery_count_by_type.csv",
        "brewery_count_by_state.csv",
        "brewery_count_by_type_and_state.csv"
    ]
    for f in expected_files:
        full_path = os.path.join(gold_path, f)
        assert os.path.exists(full_path)
        df = pd.read_csv(full_path)
        assert not df.empty
