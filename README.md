# 📘 Documentação Técnica - Projeto Breweries Case

## 🚀 Como Iniciar o Projeto
1. Clone o repositório:
```
git clone git@github.com:mateusf97/breweries_case.git
cd breweries_case
```

2. Dê permissão de leitura e escrita para as pastas essenciais:
```
chmod -R 777 ./logs ./data ./dags
```

3. Suba o projeto com Docker Compose (no Linux):
```
docker compose up --build
```

4. Acesse a página web em:
```
http://localhost:8088/
```

**Login padrão:**
```
Usuário: Admin
Senha: admin
```

## 📂 Arquitetura do Pipeline
### 1️⃣ Bronze Layer
- Coleta os dados da API Open Brewery com paginação.
- Armazena os dados brutos em JSON.
- Se falhar, tenta novamente ao final.

Trecho de código:
```python
for url in failed_urls:
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        all_data.extend(data)
        logging.info(f"🔁 Retry bem-sucedido para: {url}")
    except Exception as e:
        logging.error(f"❌ Falha permanente em: {url}")
```

### 2️⃣ Silver Layer
- Converte o JSON em Parquet.
- Particiona por estado.
- Tipagem e limpeza de dados.

### 3️⃣ Gold Layer
- Agrega os dados da Silver.
- Gera 3 arquivos CSV:
  - brewery_count_by_type.csv
  - brewery_count_by_state.csv
  - brewery_count_by_type_and_state.csv

## 📊 Página Web
Fiz o processamento dos arquivos da GOLD, e gerei um mapa HTML+JS para visualizar as informações, basta acessar o arquivo .html

**Acesso:**

```
html_view/brewery_dashboard_final_mateus.html
```

## 📬 Sistema de Monitoramento
Airflow gera logs e envia e-mails de falha.

Exemplo de log:
```bash
❌ Erro ao ler arquivo: /opt/airflow/data/silver/breweries/state=XX/breweries.parquet
```

## 🧠 Tecnologias Utilizadas
- Airflow
- Python
- PySpark
- Parquet
- Docker
- Git

## 📈 Camadas Adicionais no GOLD
Além das agregações principais, temos também:
- brewery_count_by_type.csv
- brewery_count_by_state.csv
- brewery_count_by_type_and_state.csv

## 🗂 Estrutura de Diretórios

```
.
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── dags/
├── html_view/
└── docker-compose.yml
```


# ✅ Testes Automatizados - Brewery Pipeline

Este projeto inclui uma suíte de testes para validar o funcionamento do pipeline de dados que extrai, transforma e agrega informações da Open Brewery API.

## 🔧 Pré-requisitos

- Projeto já rodando com Docker e Airflow.
- A DAG `brewery_pipeline_bronze_silver_gold` deve ter sido executada pelo menos uma vez.

## 🚀 Executando os Testes

### 1. Acesse o container do Airflow:

```bash
docker exec -it airflow_scheduler bash
```

### 2. Execute os testes com Pytest:

```bash
pytest dags/test/test_pipeline.py --disable-warnings
```

## 🧪 O que está sendo testado?

| Teste                           | Objetivo                                               |
|-------------------------------|--------------------------------------------------------|
| `test_api_status_code_200`     | Verifica se a API responde com status 200             |
| `test_api_content`             | Valida o conteúdo da resposta da API                  |
| `test_extract_creates_bronze`  | Verifica a criação correta do arquivo bronze JSON     |
| `test_transform_creates_silver`| Verifica a transformação para Parquet na camada Silver|
| `test_aggregate_creates_gold`  | Confirma a geração dos arquivos CSV agregados         |

Todos os arquivos de output são esperados em `/opt/airflow/data`.


