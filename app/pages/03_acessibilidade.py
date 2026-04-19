"""
Página: Acessibilidade e Mobilidade — índice multimodal por bairro.
"""

from pathlib import Path
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium
from app.components.graficos import bar_ranking, scatter_dimensoes
from app.components.mapas import choropleth_map

PROCESSED = Path(__file__).resolve().parents[2] / "data" / "processed"

st.set_page_config(
    page_title="Acessibilidade · BH Investment Insights",
    layout="wide"
)
st.title("🚌 Acessibilidade e Mobilidade")
st.caption(
    "Análise multimodal combinando transporte público (volume real de passageiros), "
    "fluxo de tráfego geral e cobertura por bairro"
)
st.markdown("---")


@st.cache_data
def load_data() -> pd.DataFrame | None:
    """
    Carrega dados de acessibilidade por bairro.

    :return: DataFrame ou None se não existir
    """
    path = PROCESSED / "acessibilidade_por_bairro.parquet"
    if not path.exists():
        return None
    return pd.read_parquet(path)


@st.cache_data
def load_od() -> pd.DataFrame | None:
    """
    Carrega dados agregados da Matriz Origem-Destino.

    :return: DataFrame ou None se não existir
    """
    path = PROCESSED / "matriz_od_agregada.parquet"
    if not path.exists():
        return None
    return pd.read_parquet(path)


df = load_data()
df_od = load_od()

if df is None:
    st.warning("⚠️ Execute o pipeline ETL antes de acessar esta página.")
    st.stop()

df["bairro_display"] = df["bairro"].str.title()

# ---------------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)
col1.metric("Total de Pontos de Ônibus", f"{int(df['total_pontos_onibus'].sum()):,}")
col2.metric(
    "Embarques/dia (total)",
    f"{int(df['total_embarques_dia'].sum()):,}",
    help="Soma de todos os embarques estimados em dia útil típico",
)
col3.metric(
    "Bairro com Maior Fluxo",
    df.loc[df["total_embarques_dia"].idxmax(), "bairro_display"],
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Mapa de acessibilidade
# ---------------------------------------------------------------------------
st.subheader("Índice de Acessibilidade por Bairro")
st.caption(
    "Combinação ponderada de: volume de embarques (50%), "
    "pontos de ônibus (35%) e intensidade de tráfego por acidentes (15%)"
)

col_mapa, col_rank = st.columns([3, 2])

with col_mapa:
    m = choropleth_map(
        df,
        col_valor="indice_acessibilidade",
        titulo="Índice de Acessibilidade (0–1)",
        cor="Blues",
    )
    st_folium(m, width="100%", height=450)

with col_rank:
    st.subheader("Top 15 Bairros")
    fig = bar_ranking(
        df.assign(bairro=df["bairro_display"]),
        col_x="indice_acessibilidade",
        col_y="bairro",
        cor="#1a6faf",
    )
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Scatter: pontos de ônibus × embarques (identifica infraestrutura subutilizada)
# ---------------------------------------------------------------------------
st.subheader("Infraestrutura vs. Demanda Real")
st.caption(
    "Bairros à **direita e abaixo** têm muitos pontos mas pouco uso — "
    "infraestrutura subutilizada. Bairros à **esquerda e acima** têm alta demanda "
    "com poucos pontos — gargalo de oferta."
)

fig_scatter = scatter_dimensoes(
    df.assign(bairro=df["bairro_display"]),
    col_x="total_pontos_onibus",
    col_y="total_embarques_dia",
    col_size="total_acidentes",
    col_label="bairro",
    titulo="Pontos de Ônibus × Embarques Diários (tamanho = acidentes)",
)
st.plotly_chart(fig_scatter, use_container_width=True)

# ---------------------------------------------------------------------------
# Matriz O-D (se disponível)
# ---------------------------------------------------------------------------
if df_od is not None:
    st.markdown("---")
    st.subheader("🗺️ Fluxo de Origem — Matriz O-D (amostra 1 mês)")
    st.caption(
        "Total de viagens originadas por bairro segundo a bilhetagem eletrônica. "
        "Bairros com alto fluxo de origem indicam forte demanda de deslocamento — "
        "fator relevante para comércio e serviços."
    )

    df_od["bairro_display"] = df_od["bairro"].str.title()
    fig_od = bar_ranking(
        df_od.assign(bairro=df_od["bairro_display"]),
        col_x="total_viagens_originadas",
        col_y="bairro",
        titulo="",
        cor="#0e4d8c",
    )
    st.plotly_chart(fig_od, use_container_width=True)

# ---------------------------------------------------------------------------
# Tabela detalhada
# ---------------------------------------------------------------------------
with st.expander("📋 Ver tabela completa"):
    cols_display = {
        "bairro_display": "Bairro",
        "total_pontos_onibus": "Pontos de Ônibus",
        "total_embarques_dia": "Embarques/dia",
        "total_acidentes": "Acidentes (proxy tráfego)",
        "indice_acessibilidade": "Índice Acessibilidade",
    }
    st.dataframe(
        df[cols_display.keys()].rename(columns=cols_display)
        .sort_values("Índice Acessibilidade", ascending=False),
        use_container_width=True,
        hide_index=True,
    )