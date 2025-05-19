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
Lê os arquivos da GOLD, permite filtros por estado e tipo.

**Acesso:**
```
http://localhost:8088/
Usuário: Admin
Senha: admin
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
- PySpark / Pandas
- Parquet
- Docker
- Streamlit
- Git

## 📈 Camadas Adicionais no GOLD
Além das agregações principais, temos também:
- brewery_count_by_type.csv
- brewery_count_by_state.csv
- brewery_count_by_type_and_state.csv

## 🗂 Estrutura de Diretórios
.
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── dags/
├── web/
└── docker-compose.yml

