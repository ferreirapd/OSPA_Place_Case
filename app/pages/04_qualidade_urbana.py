"""
Página: Qualidade Urbana — parques e equipamentos esportivos por bairro.
"""

from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

from app.components.graficos import bar_ranking, scatter_dimensoes
from app.components.mapas import choropleth_map

PROCESSED = Path(__file__).resolve().parents[2] / "data" / "processed"

st.set_page_config(page_title="Qualidade Urbana · BH Investment Insights", layout="wide")
st.title("🌳 Qualidade Urbana")
st.caption(
    "Distribuição de amenidades públicas por bairro — fator de atração de "
    "trabalhadores, residentes e negócios de alto valor agregado"
)
st.markdown("---")


@st.cache_data
def load_data() -> pd.DataFrame | None:
    """
    Carrega dados de qualidade urbana por bairro.

    :return: DataFrame ou None se não existir
    """
    path = PROCESSED / "qualidade_urbana_por_bairro.parquet"
    if not path.exists():
        return None
    return pd.read_parquet(path)


@st.cache_data
def load_economico() -> pd.DataFrame | None:
    """
    Carrega dados econômicos para cruzamento.

    :return: DataFrame ou None se não existir
    """
    path = PROCESSED / "empresas_por_bairro.parquet"
    if not path.exists():
        return None
    return pd.read_parquet(path)


df = load_data()
df_eco = load_economico()

if df is None:
    st.warning("⚠️ Execute o pipeline ETL antes de acessar esta página.")
    st.stop()

df["bairro_display"] = df["bairro"].str.title()

# ---------------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)
col1.metric("Total de Parques Municipais", f"{int(df['total_parques'].sum()):,}")
col2.metric(
    "Total de Equipamentos Esportivos",
    f"{int(df['total_equipamentos_esportivos'].sum()):,}",
)
col3.metric(
    "Bairro com Melhor Índice",
    df.loc[df["indice_qualidade_urbana"].idxmax(), "bairro_display"],
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Mapa + ranking
# ---------------------------------------------------------------------------
st.subheader("Índice de Qualidade Urbana por Bairro")
st.caption("Média ponderada de parques (50%) e equipamentos esportivos (50%) — ambos normalizados por rank percentual")

col_mapa, col_rank = st.columns([3, 2])

with col_mapa:
    m = choropleth_map(
        df,
        col_valor="indice_qualidade_urbana",
        titulo="Índice de Qualidade Urbana (0–1)",
        cor="Greens",
    )
    st_folium(m, width="100%", height=450)

with col_rank:
    st.subheader("Top 15 Bairros")
    fig = bar_ranking(
        df.assign(bairro=df["bairro_display"]),
        col_x="indice_qualidade_urbana",
        col_y="bairro",
        cor="#2d8a4e",
    )
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Cruzamento: qualidade urbana × atividade econômica
# ---------------------------------------------------------------------------
if df_eco is not None:
    st.subheader("Qualidade Urbana × Atividade Econômica")
    st.caption(
        "Bairros no **quadrante superior direito** combinam alto desempenho econômico "
        "e boa qualidade urbana — os mais consolidados. "
        "Bairros no **quadrante superior esquerdo** têm boa qualidade urbana mas "
        "baixa atividade — potencial latente para novos negócios."
    )

    df_cross = df.merge(
        df_eco[["bairro", "total_empresas", "diversidade_setores"]],
        on="bairro",
        how="inner",
    )
    df_cross["bairro_display"] = df_cross["bairro"].str.title()

    fig_cross = scatter_dimensoes(
        df_cross.assign(bairro=df_cross["bairro_display"]),
        col_x="total_empresas",
        col_y="indice_qualidade_urbana",
        col_size="diversidade_setores",
        col_label="bairro",
        titulo="Empresas Ativas × Índice de Qualidade Urbana (tamanho = diversidade setorial)",
    )
    st.plotly_chart(fig_cross, use_container_width=True)

# ---------------------------------------------------------------------------
# Tabela
# ---------------------------------------------------------------------------
with st.expander("📋 Ver tabela completa"):
    cols_display = {
        "bairro_display": "Bairro",
        "total_parques": "Parques",
        "total_equipamentos_esportivos": "Equipamentos Esportivos",
        "indice_qualidade_urbana": "Índice Qualidade Urbana",
    }
    st.dataframe(
        df[cols_display.keys()].rename(columns=cols_display)
        .sort_values("Índice Qualidade Urbana", ascending=False),
        use_container_width=True,
        hide_index=True,
    )
