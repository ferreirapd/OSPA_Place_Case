"""
Página 3: Oportunidades — score de atratividade para investimento por bairro.
"""

from pathlib import Path
import sys
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.components.graficos import bar_ranking, radar_bairro, scatter_dimensoes

PROCESSED = Path(__file__).resolve().parents[2] / "data" / "processed"

st.set_page_config(page_title="Oportunidades · BH", layout="wide")
st.title("Mapa de Oportunidades")
st.caption(
    "Score de atratividade por bairro: combinação ponderada de "
    "atividade econômica (40%), acessibilidade (35%) e qualidade urbana (25%)"
)
st.markdown("---")


@st.cache_data
def load_score() -> pd.DataFrame | None:
    path = PROCESSED / "score_final.parquet"
    return pd.read_parquet(path) if path.exists() else None


df = load_score()

if df is None:
    st.warning("Execute o pipeline ETL antes de acessar esta página.")
    st.stop()

df["bairro_display"] = df["bairro"].str.title()

# ── Metodologia ───────────────────────────────────────────────────────────────
with st.expander("Como o score é calculado"):
    st.markdown(
        """
        O score de atratividade (0–100) é a composição ponderada de três dimensões,
        cada uma calculada a partir de rank percentual para eliminar distorções de escala:

        | Dimensão | Peso | Composição |
        |---|---|---|
        | Atividade Econômica | 40% | 60% densidade de empresas + 40% diversidade setorial |
        | Acessibilidade | 35% | 50% embarques/dia + 30% pontos de ônibus + 20% inverso de acidentes |
        | Qualidade Urbana | 25% | 50% parques + 50% equipamentos esportivos |

        Quando a Matriz O-D está disponível, ela enriquece o componente de
        acessibilidade com um bônus de 30% sobre o fluxo real de viagens.
        Bairros sem dado em alguma dimensão recebem a média da dimensão.
        """
    )

st.markdown("---")

# ── Top 5 ─────────────────────────────────────────────────────────────────────
st.subheader("Top 5 bairros para investimento")
top5 = df.head(5)
cols_top = st.columns(5)
for i, (_, row) in enumerate(top5.iterrows()):
    cols_top[i].metric(
        label=f"#{int(row['ranking'])} {row['bairro_display']}",
        value=f"{row['score_final']:.1f}",
        help="Score 0–100",
    )

st.markdown("---")

# ── Ranking + quadrante ───────────────────────────────────────────────────────
col_rank, col_quad = st.columns([1, 2])

with col_rank:
    st.subheader("Ranking geral")
    fig_rank = bar_ranking(
        df.assign(bairro=df["bairro_display"]),
        col_x="score_final", col_y="bairro",
        titulo="", top_n=20,
    )
    st.plotly_chart(fig_rank, use_container_width=True)

with col_quad:
    st.subheader("Quadrante de oportunidades")
    st.caption(
        "Superior direito: alta atividade e boa acessibilidade — consolidados. "
        "Superior esquerdo: boa acessibilidade, baixa ocupação — potencial latente."
    )
    score_cols_ok = all(c in df.columns for c in ("score_eco", "score_ace", "score_final"))
    if score_cols_ok:
        fig_quad = scatter_dimensoes(
            df.assign(bairro=df["bairro_display"]),
            col_x="score_eco",
            col_y="score_ace",
            col_size="score_final",
            col_label="bairro",
            titulo="",
        )
        st.plotly_chart(fig_quad, use_container_width=True)

st.markdown("---")

# ── Perfil individual ─────────────────────────────────────────────────────────
st.subheader("Perfil detalhado por bairro")

bairro_sel = st.selectbox(
    "Selecione um bairro",
    sorted(df["bairro_display"].tolist()),
    index=sorted(df["bairro_display"].tolist()).index("Estoril")
    if "Estoril" in df["bairro_display"].tolist() else 0,
)

row = df[df["bairro_display"] == bairro_sel].iloc[0]

col_radar, col_metricas = st.columns([1, 2])

with col_radar:
    scores_radar = {}
    for col, label in (
        ("score_eco", "Econômico"),
        ("score_ace", "Acessibilidade"),
        ("score_qua", "Qualidade Urbana"),
    ):
        if col in row and pd.notna(row[col]):
            # Radar espera valores 0-1; score está em 0-100
            scores_radar[label] = round(row[col] / 100, 3)

    if scores_radar:
        fig_radar = radar_bairro(scores_radar, bairro_sel)
        st.plotly_chart(fig_radar, use_container_width=True)

with col_metricas:
    st.markdown(f"### {bairro_sel}")
    st.markdown(
        f"**Ranking:** #{int(row['ranking'])} de {len(df)} bairros &nbsp;|&nbsp; "
        f"**Score final:** {row['score_final']:.1f} / 100"
    )
    st.markdown("---")

    m1, m2, m3 = st.columns(3)
    m1.metric("Score econômico", f"{row.get('score_eco', 0):.1f}")
    m2.metric("Score acessibilidade", f"{row.get('score_ace', 0):.1f}")
    m3.metric("Score qualidade urbana", f"{row.get('score_qua', 0):.1f}")

    st.markdown("---")
    m4, m5, m6 = st.columns(3)

    total_emp = row.get("total_empresas")
    total_emb = row.get("total_embarques_dia")
    total_parq = row.get("total_parques")

    m4.metric("Empresas ativas", f"{int(total_emp):,}" if pd.notna(total_emp) else "—")
    m5.metric("Embarques/dia", f"{int(total_emb):,}" if pd.notna(total_emb) else "—")
    m6.metric("Parques", f"{int(total_parq)}" if pd.notna(total_parq) else "—")

    if "setor_dominante_nome" in row and pd.notna(row["setor_dominante_nome"]):
        st.markdown(f"**Setor dominante:** {row['setor_dominante_nome']}")

st.markdown("---")

# ── Tabela ────────────────────────────────────────────────────────────────────
with st.expander("Ver ranking completo"):
    cols_disp = {
        "ranking": "Ranking",
        "bairro_display": "Bairro",
        "score_final": "Score Final",
        "score_eco": "Econômico",
        "score_ace": "Acessibilidade",
        "score_qua": "Qualidade Urbana",
    }
    available = {k: v for k, v in cols_disp.items() if k in df.columns}
    st.dataframe(
        df[available.keys()].rename(columns=available),
        use_container_width=True, hide_index=True,
    )