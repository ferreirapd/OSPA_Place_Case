# OSPA Place Data Engineer Case - Pedro Ferreira

Pipeline de dados e plataforma de visualização para orientar decisões de
investimento em Belo Horizonte, construída sobre dados públicos do
[Portal de Dados Abertos da PBH](https://dados.pbh.gov.br).

---

## O que o projeto faz

O pipeline cruza oito bases públicas da PBH e da BHTRANS para produzir um score
de atratividade por bairro em três dimensões: atividade econômica, acessibilidade
e qualidade urbana. O app Streamlit é a camada de visualização em cima desse
pipeline.

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
│   ├── raw/                        # CSVs brutos, não versionados
│   └── processed/
│       ├── empresas_por_bairro.parquet
│       ├── acessibilidade_por_bairro.parquet
│       ├── qualidade_urbana_por_bairro.parquet
│       ├── matriz_od_agregada.parquet
│       ├── score_final.parquet
│       └── bairros_excluidos.csv   # auditoria de exclusões do ETL
│
├── etl/
│   ├── extract.py
│   ├── pipeline.py
│   └── transform/
│       ├── _io.py          # leitura de CSV, normalização, fuzzy match para nomes de bairros
│       ├── _spatial.py     # spatial join GeoPandas reutilizável
│       ├── economico.py
│       ├── acessibilidade.py
│       ├── qualidade_urbana.py
│       ├── matriz_od.py    # PySpark
│       └── score.py
│
├── app/
│   ├── main.py
│   ├── components/
│   │   ├── graficos.py
│   │   ├── mapas.py
│   │   └── footer.py
│   └── pages/
│
├── notebooks/
│   └── exploratory_analysis.ipynb
├── validate_etl.py
├── Dockerfile
├── docker-compose.yml
├── runtime.txt
├── requirements-etl.txt
└── requirements.txt
```

---

## Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado e rodando
- Git

---

## Como rodar

```bash
git clone https://github.com/ferreirapd/OSPA_Place_Case.git
cd OSPA_Place_Case
docker-compose build
docker-compose run app python -m etl.pipeline               # pipeline completo
docker-compose run app python -m etl.pipeline --skip-extract  # se já tem data/raw/
docker-compose run app python validate_etl.py               # validação dos parquets
docker-compose up                                            # sobe o app em :8501
```

Acesse: **http://localhost:8501**

---

## Navegação do app

| Página | Conteúdo |
|---|---|
| Início | Contexto do projeto e fontes |
| Panorama Econômico | KPIs, ranking de bairros, distribuição setorial |
| Infraestrutura e Mobilidade | Transporte público, fluxo de passageiros, parques, equipamentos |
| Mapa de Oportunidades | Score final, quadrante estratégico, perfil por bairro |
| Pipeline e Arquitetura | Pipeline ETL, código-fonte, arquitetura AWS, próximos passos |

A navegação é controlada via `st.navigation()` em `app/main.py`, o nome no
menu lateral e a ordem das páginas são definidos lá, independentes dos nomes
de arquivo.

---

## Stack técnica

| Camada | Tecnologia |
|---|---|
| ETL geral | Python 3.11 · Pandas 2.2 · GeoPandas · rapidfuzz |
| ETL Matriz O-D | PySpark 3.5 |
| Extração | Requests (API CKAN) |
| Visualização | Plotly · Folium · streamlit-folium |
| App | Streamlit |
| Container | Docker - python:3.11-slim + OpenJDK 21 JRE |
| Formato intermediário | Parquet (PyArrow) |

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

Pedro Ferreira - [@ferreirapd](https://github.com/ferreirapd)