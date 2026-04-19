"""
Página: Visão Geral de BH — métricas macro do ecossistema econômico.
"""

from pathlib import Path
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium
from app.components.graficos import bar_ranking
from app.components.mapas import choropleth_map

PROCESSED = Path(__file__).resolve().parents[2] / "data" / "processed"

st.set_page_config(
    page_title="Visão Geral · BH Investment Insights",
    layout="wide"
)
st.title("📊 Visão Geral de Belo Horizonte")
st.caption("Panorama econômico consolidado por bairro")
st.markdown("---")


@st.cache_data
def load_data() -> pd.DataFrame | None:
    """
    Carrega o dataset de empresas por bairro do cache ou disco.

    :return: DataFrame ou None se o arquivo não existir
    """
    path = PROCESSED / "empresas_por_bairro.parquet"
    if not path.exists():
        return None
    return pd.read_parquet(path)


df = load_data()

if df is None:
    st.warning(
        "⚠️ Dados ainda não processados. Execute o pipeline ETL primeiro:\n\n"
        "```bash\ndocker-compose run app python -m etl.pipeline\n```"
    )
    st.stop()

# ---------------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

col1.metric("Total de Empresas Ativas", f"{df['total_empresas'].sum():,.0f}")
col2.metric("Bairros com Atividade", f"{len(df):,}")
col3.metric("Setores Econômicos (CNAE)", f"{df['diversidade_setores'].max():,}")
col4.metric(
    "Bairro Mais Denso",
    df.loc[df["total_empresas"].idxmax(), "bairro"].title(),
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Gráfico + mapa lado a lado
# ---------------------------------------------------------------------------
col_left, col_right = st.columns([1, 1])

with col_left:
    st.subheader("Top 15 Bairros por Empresas Ativas")
    fig = bar_ranking(df, col_x="total_empresas", col_y="bairro", titulo="")
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Distribuição Geográfica")
    st.caption("Mapa coroplético disponível após integração do GeoJSON de bairros")
    m = choropleth_map(df, col_valor="total_empresas", titulo="Empresas por Bairro")
    st_folium(m, width="100%", height=420)