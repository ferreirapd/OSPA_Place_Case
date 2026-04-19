# 🏙️ OSPA Place Data Engineer Case - Pedro Ferreira

Plataforma de dados para orientar decisões de investimento em Belo Horizonte,
construída sobre dados públicos do [Portal de Dados Abertos da PBH](https://dados.pbh.gov.br).

---

## Sobre o Projeto

O projeto cruza **8 bases de dados públicas** para construir um índice de atratividade
por bairro com três dimensões:

| Dimensão | Peso | Fontes |
|---|---|---|
| 📊 Atividade Econômica | 40% | Atividade Econômica (CNAE/PBH) |
| 🚌 Acessibilidade Multimodal | 35% | Pontos de Ônibus, Embarques por Ponto, Acidentes de Trânsito, Matriz O-D |
| 🌳 Qualidade Urbana | 25% | Parques Municipais, Equipamentos Esportivos |

---

## Estrutura do Repositório

```
OSPA_Place_Case/
│
├── data/
│   ├── raw/            # Dados brutos (não versionados)
│   └── processed/      # Saída do ETL (.parquet)
│
├── etl/
│   ├── extract.py      # Download via API CKAN
│   ├── pipeline.py     # Orquestrador
│   └── transform/
│       ├── economico.py
│       ├── acessibilidade.py
│       ├── qualidade_urbana.py
│       ├── matriz_od.py    # PySpark
│       └── score.py
│
├── app/
│   ├── main.py         # Entry point Streamlit
│   ├── components/     # Folium + Plotly reutilizáveis
│   └── pages/          # 9 páginas (5 investidores + 4 técnicas)
│
├── notebooks/
│   └── exploratory.ipynb
│
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado e rodando
- Git

---

## Início Rápido

### 1. Clone o repositório

```bash
git clone https://github.com/ferreirapd/OSPA_Place_Case.git
cd OSPA_Place_Case
```

### 2. Build da imagem Docker

```bash
docker-compose build
```

> A primeira build demora ~3–5 minutos (instala Java + dependências Python).
> Builds subsequentes usam cache e levam ~30 segundos.

### 3. Execute o pipeline ETL

```bash
docker-compose run app python -m etl.pipeline
```

Isso irá:
1. Baixar todas as fontes do portal da PBH para `data/raw/`
2. Processar cada dimensão e salvar em `data/processed/`
3. Calcular o score final por bairro

> Para pular o download (se já tiver os dados em `data/raw/`):
> ```bash
> docker-compose run app python -m etl.pipeline --skip-extract
> ```

### 4. Suba o app

```bash
docker-compose up
```

Acesse: **http://localhost:8501**

---

## Navegação do App

### Para Investidores
| Página | Descrição |
|---|---|
| Visão Geral | KPIs macro + mapa de densidade econômica |
| Análise Setorial | Distribuição de CNAEs por bairro com filtros |
| Acessibilidade | Índice multimodal + scatter infraestrutura × demanda |
| Qualidade Urbana | Parques e equipamentos + cruzamento com atividade econômica |
| Mapa de Oportunidades | Score final + ranking + perfil radar por bairro |

### Visão Técnica
| Página | Descrição |
|---|---|
| Arquitetura Atual | Fluxo de dados local + decisões de design |
| Pipeline ETL | Diagrama por etapa + código-fonte expansível |
| Arquitetura AWS | Como escalaria em produção com estimativa de custo |
| Visão de Futuro | Roadmap de novas fontes + parceiros privados + ML |

---

## Stack Técnica

| Camada | Tecnologia |
|---|---|
| Linguagem | Python 3.11 |
| ETL (geral) | Pandas 2.2, PyArrow |
| ETL (Matriz O-D) | PySpark 3.5 |
| Extração | Requests (API CKAN) |
| Visualização | Plotly, Folium, streamlit-folium |
| App | Streamlit 1.35 |
| Container | Docker (python:3.11-slim + OpenJDK 17 JRE) |
| Formato intermediário | Parquet (Snappy) |

---

## Dados Utilizados

Todas as fontes são públicas e disponíveis em [dados.pbh.gov.br](https://dados.pbh.gov.br):

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

## Observações sobre os Dados

- Os nomes de colunas nos CSVs do portal podem variar entre versões.
  Os módulos de transformação incluem fallbacks de encoding e normalização de nomes.
- A Matriz O-D usa hexágonos H3 como unidade espacial — o módulo PySpark
  realiza a conversão para bairros via join aproximado por centroide.
- O GeoJSON de polígonos de bairros para os mapas coropléticos deve ser
  obtido separadamente via [BHMAP](https://bhmap.pbh.gov.br) e salvo
  em `data/raw/bairros/bairros.geojson`.

---

## Autor

Pedro Ferreira — [@ferreirapd](https://github.com/ferreirapd)