"""
Página 2: Infraestrutura e Mobilidade — acessibilidade e qualidade urbana por bairro.
"""

from pathlib import Path
import sys
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.components.graficos import bar_ranking, scatter_dimensoes

PROCESSED = Path(__file__).resolve().parents[2] / "data" / "processed"

st.set_page_config(page_title="Infraestrutura · BH", layout="wide")
st.title("Infraestrutura e Mobilidade")
st.caption(
    "Transporte público, fluxo de passageiros, acidentes de trânsito "
    "e equipamentos urbanos por bairro"
)
st.markdown("---")


@st.cache_data
def load_ace() -> pd.DataFrame | None:
    path = PROCESSED / "acessibilidade_por_bairro.parquet"
    return pd.read_parquet(path) if path.exists() else None


@st.cache_data
def load_qua() -> pd.DataFrame | None:
    path = PROCESSED / "qualidade_urbana_por_bairro.parquet"
    return pd.read_parquet(path) if path.exists() else None


@st.cache_data
def load_od() -> pd.DataFrame | None:
    path = PROCESSED / "matriz_od_agregada.parquet"
    return pd.read_parquet(path) if path.exists() else None


df_ace = load_ace()
df_qua = load_qua()
df_od  = load_od()

if df_ace is None:
    st.warning("Execute o pipeline ETL antes de acessar esta página.")
    st.stop()

df_ace["bairro_display"] = df_ace["bairro"].str.title()

# ── Tabs: Acessibilidade | Qualidade Urbana ────────────────────────────────────
tab_ace, tab_qua = st.tabs(["Transporte Público", "Equipamentos Urbanos"])

# ───────────────────────── TAB ACESSIBILIDADE ─────────────────────────────────
with tab_ace:

    col1, col2, col3 = st.columns(3)
    col1.metric("Total de pontos de ônibus", f"{int(df_ace['total_pontos_onibus'].sum()):,}")
    col2.metric(
        "Embarques/dia (total)",
        f"{int(df_ace['total_embarques_dia'].sum()):,}",
    )
    col3.metric(
        "Maior fluxo de passageiros",
        df_ace.loc[df_ace["total_embarques_dia"].idxmax(), "bairro_display"],
    )

    st.markdown("---")

    col_rank, col_scatter = st.columns([1, 2])

    with col_rank:
        st.subheader("Índice de acessibilidade")
        st.caption("50% embarques + 30% pontos + 20% inverso de acidentes")
        fig_ace = bar_ranking(
            df_ace.assign(bairro=df_ace["bairro_display"]),
            col_x="indice_acessibilidade", col_y="bairro",
            titulo="", top_n=15, cor="#1a6faf",
        )
        st.plotly_chart(fig_ace, use_container_width=True)

    with col_scatter:
        st.subheader("Pontos de ônibus × embarques diários")
        st.caption(
            "Bairros à direita e abaixo têm muita infraestrutura mas pouco uso. "
            "À esquerda e acima, a demanda supera a oferta."
        )
        fig_sc = scatter_dimensoes(
            df_ace.assign(bairro=df_ace["bairro_display"]),
            col_x="total_pontos_onibus",
            col_y="total_embarques_dia",
            col_size="total_acidentes",
            col_label="bairro",
            titulo="",
        )
        st.plotly_chart(fig_sc, use_container_width=True)

    # Matriz O-D
    if df_od is not None and not df_od.empty:
        st.markdown("---")
        st.subheader("Viagens originadas por bairro — Matriz O-D")
        st.caption(
            "Volume de viagens de transporte público com origem em cada bairro, "
            "baseado na bilhetagem eletrônica (amostra de 1 mês)."
        )
        df_od["bairro_display"] = df_od["bairro"].str.title()
        fig_od = bar_ranking(
            df_od.assign(bairro=df_od["bairro_display"]),
            col_x="total_viagens_originadas", col_y="bairro",
            titulo="", cor="#0e4d8c",
        )
        st.plotly_chart(fig_od, use_container_width=True)

    with st.expander("Ver tabela completa — acessibilidade"):
        cols = {
            "bairro_display": "Bairro",
            "total_pontos_onibus": "Pontos de Ônibus",
            "total_embarques_dia": "Embarques/dia",
            "total_acidentes": "Acidentes",
            "indice_acessibilidade": "Índice",
        }
        st.dataframe(
            df_ace[cols.keys()].rename(columns=cols)
            .sort_values("Índice", ascending=False),
            use_container_width=True, hide_index=True,
        )

# ─────────────────────── TAB QUALIDADE URBANA ─────────────────────────────────
with tab_qua:

    if df_qua is None:
        st.warning("Dados de qualidade urbana não disponíveis.")
    else:
        df_qua["bairro_display"] = df_qua["bairro"].str.title()

        col1, col2, col3 = st.columns(3)
        col1.metric("Total de parques municipais", f"{int(df_qua['total_parques'].sum()):,}")
        col2.metric(
            "Total de equipamentos esportivos",
            f"{int(df_qua['total_equipamentos_esportivos'].sum()):,}",
        )
        col3.metric(
            "Melhor índice urbano",
            df_qua.loc[df_qua["indice_qualidade_urbana"].idxmax(), "bairro_display"],
        )

        st.markdown("---")

        col_rank2, col_rank3 = st.columns(2)

        with col_rank2:
            st.subheader("Parques por bairro")
            fig_parq = bar_ranking(
                df_qua.assign(bairro=df_qua["bairro_display"]),
                col_x="total_parques", col_y="bairro",
                titulo="", cor="#2d8a4e",
            )
            st.plotly_chart(fig_parq, use_container_width=True)

        with col_rank3:
            st.subheader("Equipamentos esportivos por bairro")
            fig_equip = bar_ranking(
                df_qua.assign(bairro=df_qua["bairro_display"]),
                col_x="total_equipamentos_esportivos", col_y="bairro",
                titulo="", cor="#4c72b0",
            )
            st.plotly_chart(fig_equip, use_container_width=True)

        with st.expander("Ver tabela completa — qualidade urbana"):
            cols = {
                "bairro_display": "Bairro",
                "total_parques": "Parques",
                "total_equipamentos_esportivos": "Equip. Esportivos",
                "indice_qualidade_urbana": "Índice",
            }
            st.dataframe(
                df_qua[cols.keys()].rename(columns=cols)
                .sort_values("Índice", ascending=False),
                use_container_width=True, hide_index=True,
            )