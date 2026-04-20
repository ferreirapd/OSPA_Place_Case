# OSPA Place Case - Pedro Ferreira

Pipeline de dados e plataforma de visualização para orientar decisões de investimento em Belo Horizonte, construída sobre dados públicos do [Portal de Dados Abertos da PBH](https://dados.pbh.gov.br).

---

## O que o projeto faz

O pipeline cruza oito bases públicas da PBH e da BHTRANS para produzir um score de atratividade por bairro em três dimensões: atividade econômica, acessibilidade e qualidade urbana. O app Streamlit é a camada de visualização em cima desse pipeline.

| Dimensão | Peso | Fontes |
|---|---|---|
| Atividade Econômica | 40% | Atividade Econômica (CNAE/PBH) |
| Acessibilidade Multimodal | 35% | Pontos de Ônibus, Embarques por Ponto, Acidentes, Matriz O-D |
| Qualidade Urbana | 25% | Parques Municipais, Equipamentos Esportivos |

---

## Estrutura do repositório

```
OSPA_Place_Case/
├── data/
│   ├── raw/                        # CSVs brutos — não versionados
│   └── processed/                  # Parquets de saída
│       ├── empresas_por_bairro.parquet
│       ├── acessibilidade_por_bairro.parquet
│       ├── qualidade_urbana_por_bairro.parquet
│       ├── matriz_od_agregada.parquet
│       ├── score_final.parquet
│       └── bairros_excluidos.csv   # auditoria de bairros descartados no ETL
│
├── etl/
│   ├── extract.py                  # download via API CKAN
│   ├── pipeline.py                 # orquestrador
│   └── transform/
│       ├── _io.py                  # helpers: leitura de CSV, normalização, fuzzy match
│       ├── _spatial.py             # helpers: spatial join GeoPandas reutilizável
│       ├── economico.py            # Pandas
│       ├── acessibilidade.py       # Pandas + GeoPandas
│       ├── qualidade_urbana.py     # Pandas
│       ├── matriz_od.py            # PySpark
│       └── score.py                # composição final ponderada
│
├── app/
│   ├── main.py                     # entry point Streamlit
│   ├── components/
│   │   ├── graficos.py             # componentes Plotly reutilizáveis
│   │   └── mapas.py                # componentes Folium reutilizáveis
│   └── pages/
│       ├── 01_panorama_economico.py
│       ├── 02_infraestrutura_mobilidade.py
│       ├── 03_oportunidades.py
│       └── 04_visao_tecnica.py
│
├── notebooks/
│   └── exploratory_analysis.ipynb
│
├── validate_etl.py                 # validação dos parquets gerados pelo pipeline
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado e rodando
- Git

---

## Como rodar

### 1. Clone o repositório

```bash
git clone https://github.com/ferreirapd/OSPA_Place_Case.git
cd OSPA_Place_Case
```

### 2. Build da imagem

```bash
docker-compose build
```

A primeira build demora ~3–5 minutos (instala Java + dependências Python).
Builds subsequentes usam cache e levam ~30 segundos.

### 3. Execute o pipeline ETL

```bash
docker-compose run app python -m etl.pipeline
```

Para pular o download se os dados já estiverem em `data/raw/`:

```bash
docker-compose run app python -m etl.pipeline --skip-extract
```

### 4. Valide os dados gerados

```bash
docker-compose run app python validate_etl.py
```

### 5. Suba o app

```bash
docker-compose up
```

Acesse: **http://localhost:8501**

---

## Navegação do app

| Página | Conteúdo |
|---|---|
| Panorama Econômico | KPIs, ranking de bairros, distribuição setorial |
| Infraestrutura e Mobilidade | Transporte público, fluxo de passageiros, parques, equipamentos |
| Mapa de Oportunidades | Score final, quadrante estratégico, perfil detalhado por bairro |
| Visão Técnica | Pipeline ETL, código-fonte, arquitetura AWS, próximos passos |

---

## Stack técnica

| Camada | Tecnologia |
|---|---|
| ETL (geral) | Python 3.11 · Pandas 2.2 · GeoPandas · rapidfuzz |
| ETL (Matriz O-D) | PySpark 3.5 |
| Extração | Requests (API CKAN) |
| Visualização | Plotly · Folium · streamlit-folium |
| App | Streamlit 1.35 |
| Formato intermediário | Parquet (PyArrow) |
| Container | Docker — python:3.11-slim + OpenJDK 21 JRE |

---

## Dados utilizados

| Dataset | Organização | Periodicidade |
|---|---|---|
| Atividade Econômica | PBH | Mensal |
| Bairro Oficial | PBH | Mensal |
| Ponto de Ônibus | BHTRANS | Mensal |
| Estimativa de Embarque por Ponto | BHTRANS | Mensal |
| Acidentes de Trânsito com Vítima | BHTRANS | Anual |
| Parques Municipais | PBH/FPZ | Mensal |
| Equipamentos Esportivos | PBH | Mensal |
| Matriz Origem-Destino | BHTRANS | Mensal (amostra: 1 mês) |

---

## Autor

Pedro Ferreira — [@ferreirapd](https://github.com/ferreirapd)