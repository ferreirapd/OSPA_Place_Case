"""
Mapa de Oportunidades: score de atratividade para investimento por bairro.
"""

from pathlib import Path
import sys
import pandas as pd
import streamlit as st
from app.components.graficos import bar_ranking, radar_bairro, scatter_dimensoes
from app.components.footer import render_footer


sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

PROCESSED = Path(__file__).resolve().parents[2]/"data"/"processed"


@st.cache_data
def load_score() -> pd.DataFrame | None:
    """
    Carrega o score final de atratividade por bairro, resultado do pipeline de
    ETL. O score é uma combinação ponderada de atividade econômica, acessibilidade
    e qualidade urbana, e é a principal métrica para identificar oportunidades de
    investimento.
    
    :return: DataFrame com colunas mínimas 'bairro', 'score_final' e 'ranking', ou None se o arquivo não existir
    """
    path = PROCESSED / "score_final.parquet"
    return pd.read_parquet(path) if path.exists() else None


st.title("Mapa de Oportunidades")
st.caption(
    "Score de atratividade por bairro: combinação ponderada de "
    "atividade econômica (40%), acessibilidade (35%) e qualidade urbana (25%)"
)
st.markdown("---")

df = load_score()

if df is None:
    st.warning("Execute o pipeline ETL antes de acessar esta página.")
    st.stop()

df["bairro_display"] = df["bairro"].str.title()

with st.expander("Como o score é calculado"):
    st.markdown(
        """
        O score (0–100) é a composição ponderada de três dimensões, cada uma
        calculada a partir de rank percentual para eliminar distorções de escala:

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

col_rank, col_quad = st.columns([1, 2])

with col_rank:
    st.subheader("Ranking geral")
    st.plotly_chart(
        bar_ranking(
            df.assign(bairro=df["bairro_display"]),
            col_x="score_final", col_y="bairro", titulo="", top_n=20,
        ),
        use_container_width=True,
    )

with col_quad:
    st.subheader("Quadrante de oportunidades")
    st.caption(
        "Superior direito: alta atividade e boa acessibilidade, consolidados. "
        "Superior esquerdo: boa acessibilidade, baixa ocupação, potencial latente."
    )
    if all(c in df.columns for c in ("score_eco", "score_ace", "score_final")):
        st.plotly_chart(
            scatter_dimensoes(
                df.assign(bairro=df["bairro_display"]),
                col_x="score_eco", col_y="score_ace",
                col_size="score_final", col_label="bairro", titulo="",
            ),
            use_container_width=True,
        )

st.markdown("---")

st.subheader("Perfil detalhado por bairro")

bairros_sorted = sorted(df["bairro_display"].tolist())
default_idx = bairros_sorted.index("Estoril") if "Estoril" in bairros_sorted else 0

bairro_sel = st.selectbox("Selecione um bairro", bairros_sorted, index=default_idx)
row = df[df["bairro_display"] == bairro_sel].iloc[0]

col_radar, col_metricas = st.columns([1, 2])

with col_radar:
    scores_radar: dict[str, float] = {}
    for col, label in (
        ("score_eco", "Econômico"),
        ("score_ace", "Acessibilidade"),
        ("score_qua", "Qualidade Urbana"),
    ):
        if col in row and pd.notna(row[col]):
            scores_radar[label] = round(float(row[col]) / 100, 3)

    if scores_radar:
        st.plotly_chart(radar_bairro(scores_radar, bairro_sel), use_container_width=True)

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
    v_emp  = row.get("total_empresas")
    v_emb  = row.get("total_embarques_dia")
    v_parq = row.get("total_parques")
    m4.metric("Empresas ativas", f"{int(v_emp):,}" if pd.notna(v_emp) else "-")
    m5.metric("Embarques/dia", f"{int(v_emb):,}" if pd.notna(v_emb) else "-")
    m6.metric("Parques", f"{int(v_parq)}" if pd.notna(v_parq) else "-")

    if "setor_dominante_nome" in row and pd.notna(row["setor_dominante_nome"]):
        st.markdown(f"**Setor dominante:** {row['setor_dominante_nome']}")

st.markdown("---")

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

render_footer()