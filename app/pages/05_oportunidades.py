"""
Página: Mapa de Oportunidades — score final de atratividade por bairro.
"""

from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

from app.components.graficos import bar_ranking, radar_bairro, scatter_dimensoes
from app.components.mapas import score_map

PROCESSED = Path(__file__).resolve().parents[2] / "data" / "processed"

st.set_page_config(page_title="Oportunidades · BH Investment Insights", layout="wide")
st.title("🎯 Mapa de Oportunidades")
st.caption(
    "Score de atratividade para investimento por bairro — "
    "combinação ponderada de atividade econômica, acessibilidade e qualidade urbana"
)
st.markdown("---")


@st.cache_data
def load_data() -> pd.DataFrame | None:
    """
    Carrega o score final por bairro.

    :return: DataFrame ou None se não existir
    """
    path = PROCESSED / "score_final.parquet"
    if not path.exists():
        return None
    return pd.read_parquet(path)


df = load_data()

if df is None:
    st.warning("⚠️ Execute o pipeline ETL antes de acessar esta página.")
    st.stop()

df["bairro_display"] = df["bairro"].str.title()

# ---------------------------------------------------------------------------
# Metodologia em destaque
# ---------------------------------------------------------------------------
with st.expander("ℹ️ Como o score é calculado", expanded=False):
    st.markdown(
        """
        O **Score de Atratividade** (0–100) é composto por três dimensões:

        | Dimensão | Peso | Componentes |
        |---|---|---|
        | 📊 Atividade Econômica | **40%** | Densidade de empresas (60%) + Diversidade setorial (40%) |
        | 🚌 Acessibilidade | **35%** | Embarques diários (50%) + Pontos de ônibus (35%) + Tráfego (15%) |
        | 🌳 Qualidade Urbana | **25%** | Parques (50%) + Equipamentos esportivos (50%) |

        Cada componente é normalizado via **rank percentual** antes da composição,
        garantindo que diferenças de escala entre as fontes não distorçam o resultado.

        A Matriz O-D (quando disponível) enriquece o componente de acessibilidade
        com um peso adicional de 30% sobre o fluxo real de viagens originadas.
        """
    )

st.markdown("---")

# ---------------------------------------------------------------------------
# KPIs do top 5
# ---------------------------------------------------------------------------
st.subheader("🏆 Top 5 Bairros para Investimento")

top5 = df.head(5)
cols = st.columns(5)
for i, (_, row) in enumerate(top5.iterrows()):
    cols[i].metric(
        label=f"#{int(row['ranking'])} {row['bairro_display']}",
        value=f"{row['score_final']:.1f}",
        help="Score 0–100",
    )

st.markdown("---")

# ---------------------------------------------------------------------------
# Mapa + ranking lado a lado
# ---------------------------------------------------------------------------
col_mapa, col_rank = st.columns([3, 2])

with col_mapa:
    st.subheader("Score por Bairro")
    m = score_map(df)
    st_folium(m, width="100%", height=480)

with col_rank:
    st.subheader("Ranking Geral")
    fig_rank = bar_ranking(
        df.assign(bairro=df["bairro_display"]),
        col_x="score_final",
        col_y="bairro",
        titulo="",
        top_n=20,
    )
    st.plotly_chart(fig_rank, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Scatter: econômico × acessibilidade (identifica oportunidades)
# ---------------------------------------------------------------------------
st.subheader("Quadrante de Oportunidades")
st.caption(
    "**Superior direito** = consolidados (alta atividade + alta acessibilidade). "
    "**Superior esquerdo** = potencial inexplorado (boa acessibilidade, baixa ocupação econômica). "
    "**Inferior direito** = alta atividade com gargalo de mobilidade."
)

score_cols = [c for c in ("score_eco", "score_ace", "score_qua") if c in df.columns]
if len(score_cols) >= 2:
    fig_quad = scatter_dimensoes(
        df.assign(bairro=df["bairro_display"]),
        col_x="score_eco",
        col_y="score_ace",
        col_size="score_final",
        col_label="bairro",
        titulo="Score Econômico × Score Acessibilidade (tamanho = score final)",
    )
    st.plotly_chart(fig_quad, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Perfil detalhado de um bairro específico
# ---------------------------------------------------------------------------
st.subheader("🔍 Perfil Detalhado por Bairro")

bairro_sel = st.selectbox(
    "Selecione um bairro",
    df["bairro_display"].tolist(),
    index=0,
)

row = df[df["bairro_display"] == bairro_sel].iloc[0]

col_radar, col_metricas = st.columns([1, 2])

with col_radar:
    scores_radar = {}
    if "score_eco" in row:
        scores_radar["Econômico"] = round(row["score_eco"], 3)
    if "score_ace" in row:
        scores_radar["Acessibilidade"] = round(row["score_ace"], 3)
    if "score_qua" in row:
        scores_radar["Qualidade Urbana"] = round(row["score_qua"], 3)

    if scores_radar:
        fig_radar = radar_bairro(scores_radar, bairro_sel)
        st.plotly_chart(fig_radar, use_container_width=True)

with col_metricas:
    st.markdown(f"### {bairro_sel}")
    st.markdown(f"**Ranking geral:** #{int(row['ranking'])} de {len(df)} bairros")
    st.markdown(f"**Score Final:** {row['score_final']:.1f} / 100")
    st.markdown("---")

    m1, m2, m3 = st.columns(3)
    m1.metric("Score Econômico", f"{row.get('score_eco', 0):.3f}")
    m2.metric("Score Acessibilidade", f"{row.get('score_ace', 0):.3f}")
    m3.metric("Score Qualidade Urbana", f"{row.get('score_qua', 0):.3f}")

    st.markdown("---")
    m4, m5, m6 = st.columns(3)
    m4.metric("Empresas Ativas", f"{int(row.get('total_empresas', 0)):,}")
    m5.metric("Embarques/dia", f"{int(row.get('total_embarques_dia', 0)):,}")
    m6.metric("Parques", f"{int(row.get('total_parques', 0)):,}")
