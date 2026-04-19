"""
Página Técnica: Arquitetura Atual — como o projeto está estruturado hoje.
"""

import streamlit as st

st.set_page_config(
    page_title="Arquitetura Atual · Visão Técnica",
    layout="wide"
)
st.title("⚙️ Arquitetura Atual")
st.caption("Como o projeto está estruturado na entrega")
st.markdown("---")

st.markdown(
    """
    A arquitetura atual foi desenhada para ser **reproduzível em qualquer máquina**
    via Docker, sem dependências de infraestrutura externa.
    """
)

# ---------------------------------------------------------------------------
# Diagrama textual da arquitetura atual
# ---------------------------------------------------------------------------
st.subheader("Fluxo de Dados")

st.code(
    """
┌─────────────────────────────────────────────────────────┐
│                   FONTES EXTERNAS                        │
│                                                          │
│   Portal de Dados Abertos da PBH (CKAN API)             │
│   dados.pbh.gov.br                                       │
│                                                          │
│   ├── Atividade Econômica    (CSV — mensal)              │
│   ├── Bairros Oficiais       (CSV — mensal)              │
│   ├── Pontos de Ônibus       (CSV — mensal)              │
│   ├── Embarques por Ponto    (CSV — mensal)              │
│   ├── Acidentes de Trânsito  (CSV — anual)               │
│   ├── Parques Municipais     (CSV — mensal)              │
│   ├── Equipamentos Esportivos(CSV — mensal)              │
│   └── Matriz O-D             (CSV — mensal, 1 mês)      │
└──────────────────────────┬──────────────────────────────┘
                           │  HTTP GET (requests)
                           ▼
┌─────────────────────────────────────────────────────────┐
│                  EXTRACT (etl/extract.py)                │
│                                                          │
│   • Consulta API CKAN para resolver URL mais recente    │
│   • Download com streaming (sem carregar tudo na RAM)   │
│   • Idempotente: pula se arquivo já existe              │
│   • Saída: data/raw/<fonte>/<arquivo>.csv               │
└──────────────────────────┬──────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│               TRANSFORM (etl/transform/)                 │
│                                                          │
│   economico.py      → Pandas                            │
│   ├── Filtra empresas ativas                            │
│   ├── Normaliza nomes de bairro                         │
│   ├── Extrai divisão CNAE (2 dígitos)                   │
│   └── Agrega: total_empresas, diversidade_setores       │
│                                                          │
│   acessibilidade.py → Pandas                            │
│   ├── Join pontos × embarques por CODIGO_PONTO          │
│   ├── Agrega embarques/dia por bairro                   │
│   ├── Conta acidentes por bairro                        │
│   └── Compõe índice ponderado (0–1)                     │
│                                                          │
│   qualidade_urbana.py → Pandas                          │
│   ├── Conta parques por bairro                          │
│   ├── Conta equipamentos esportivos por bairro          │
│   └── Compõe índice ponderado (0–1)                     │
│                                                          │
│   matriz_od.py      → PySpark  ◄── único módulo Spark  │
│   ├── Lê CSV esparso N×N hexágonos H3                   │
│   ├── UDF: converte H3 → lat/lng                        │
│   ├── Broadcast join: H3 → bairro mais próximo          │
│   └── Agrega viagens originadas por bairro              │
│                                                          │
│   score.py          → Pandas                            │
│   ├── Merge das 4 dimensões por bairro                  │
│   ├── Min-max normalização por componente               │
│   └── Score final ponderado (0–100)                     │
└──────────────────────────┬──────────────────────────────┘
                           │  .parquet (PyArrow)
                           ▼
┌─────────────────────────────────────────────────────────┐
│                  data/processed/                         │
│                                                          │
│   empresas_por_bairro.parquet                           │
│   acessibilidade_por_bairro.parquet                     │
│   qualidade_urbana_por_bairro.parquet                   │
│   matriz_od_agregada.parquet                            │
│   score_final.parquet                                   │
└──────────────────────────┬──────────────────────────────┘
                           │  pd.read_parquet()
                           ▼
┌─────────────────────────────────────────────────────────┐
│              APP STREAMLIT (app/)                        │
│                                                          │
│   main.py          → Home + navegação                   │
│   pages/           → 9 páginas (investidores + técnica) │
│   components/      → Folium maps + Plotly charts        │
│                                                          │
│   Executa em: http://localhost:8501                      │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│                     DOCKER                               │
│                                                          │
│   Base: python:3.11-slim + openjdk-17-jre-headless      │
│   JVM heap: máx 1GB (JAVA_TOOL_OPTIONS=-Xmx1g)          │
│   data/ montado como volume (persiste entre runs)       │
│                                                          │
│   docker-compose up    → sobe tudo                      │
│   docker-compose run app python -m etl.pipeline         │
│                        → executa ETL                    │
└─────────────────────────────────────────────────────────┘
""",
    language="text",
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Decisões técnicas relevantes
# ---------------------------------------------------------------------------
st.subheader("Decisões de Design")

col1, col2 = st.columns(2)

with col1:
    st.markdown(
        """
        **Por que Parquet como formato intermediário?**

        Os dados processados são gravados em `.parquet` (via PyArrow) em vez de CSV por três razões:
        - Leitura ~10× mais rápida pelo Streamlit (colunar vs. linha a linha)
        - Tipagem preservada sem necessidade de reparse
        - Compressão nativa reduz uso de disco

        **Por que PySpark apenas na Matriz O-D?**

        O overhead de inicializar uma JVM e uma SparkSession (~15s) só
        se justifica quando o dado é grande o suficiente. Para as demais
        bases (~milhares de linhas), Pandas vetorizado é mais rápido e
        muito mais simples de debugar.
        """
    )

with col2:
    st.markdown(
        """
        **Por que `python:3.11-slim` como base Docker?**

        A imagem `slim` remove documentação, locales e pacotes desnecessários,
        reduzindo de ~900MB (full) para ~130MB de base. O único acréscimo
        necessário foi o `openjdk-17-jre-headless` para o PySpark — e apenas
        o JRE (sem JDK), pois não precisamos compilar código Java.

        **Por que idempotência no Extract?**

        O `extract.py` pula downloads já existentes. Isso permite rodar
        `pipeline.py --skip-extract` durante o desenvolvimento sem rebaixar
        todas as fontes a cada mudança no Transform.
        """
    )

st.markdown("---")

# ---------------------------------------------------------------------------
# Estrutura de pastas
# ---------------------------------------------------------------------------
st.subheader("Estrutura do Repositório")

st.code(
    """
bh-investment-insights/
│
├── data/
│   ├── raw/                    # Dados brutos (não versionados no git)
│   └── processed/              # Saída do ETL (.parquet)
│
├── etl/
│   ├── extract.py              # Download das fontes via CKAN API
│   ├── pipeline.py             # Orquestrador — chama tudo em ordem
│   └── transform/
│       ├── economico.py        # Pandas
│       ├── acessibilidade.py   # Pandas
│       ├── qualidade_urbana.py # Pandas
│       ├── matriz_od.py        # PySpark
│       └── score.py            # Pandas — composição final
│
├── app/
│   ├── main.py                 # Entry point Streamlit
│   ├── components/
│   │   ├── mapas.py            # Folium
│   │   └── graficos.py         # Plotly
│   └── pages/                  # 9 páginas numeradas
│
├── notebooks/
│   └── exploratory.ipynb       # EDA inicial
│
├── Dockerfile                  # python:3.11-slim + JRE
├── docker-compose.yml
├── requirements.txt
└── README.md
""",
    language="text",
)