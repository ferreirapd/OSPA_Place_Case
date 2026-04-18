"""
Página: Análise Setorial — distribuição de CNAEs por bairro.
"""

from pathlib import Path

import pandas as pd
import streamlit as st

from app.components.graficos import bar_ranking, pie_setores

PROCESSED = Path(__file__).resolve().parents[2] / "data" / "processed"
RAW = Path(__file__).resolve().parents[2] / "data" / "raw"

st.set_page_config(page_title="Análise Setorial · BH Investment Insights", layout="wide")
st.title("🏭 Análise Setorial")
st.caption("Distribuição de atividades econômicas por setor e bairro")
st.markdown("---")

# Mapeamento CNAE divisão (2 dígitos) → nome do setor macro
CNAE_LABELS: dict[str, str] = {
    "01": "Agricultura", "05": "Mineração", "10": "Alimentos",
    "13": "Têxtil", "19": "Combustíveis", "22": "Borracha/Plástico",
    "25": "Metal", "26": "Eletrônicos", "28": "Máquinas",
    "33": "Manutenção", "35": "Energia", "36": "Saneamento",
    "41": "Construção", "45": "Veículos", "46": "Comércio Atacado",
    "47": "Comércio Varejo", "49": "Transporte Terrestre",
    "52": "Armazenagem", "55": "Hospedagem", "56": "Alimentação",
    "58": "Editoras", "61": "Telecomunicações", "62": "TI/Software",
    "64": "Financeiro", "68": "Imobiliário", "69": "Jurídico",
    "70": "Consultoria", "71": "Arquitetura/Eng.", "72": "P&D",
    "73": "Publicidade", "74": "Design", "75": "Veterinário",
    "77": "Locação", "78": "RH/Seleção", "79": "Turismo",
    "80": "Segurança", "81": "Facilities", "82": "Apoio Administrativo",
    "84": "Administração Pública", "85": "Educação",
    "86": "Saúde", "87": "Assistência Social", "90": "Artes/Cultura",
    "93": "Esporte/Lazer", "95": "Reparação", "96": "Serviços Pessoais",
}


@st.cache_data
def load_empresas() -> pd.DataFrame | None:
    """
    Carrega dados de empresas por bairro.

    :return: DataFrame ou None se não existir
    """
    path = PROCESSED / "empresas_por_bairro.parquet"
    if not path.exists():
        return None
    return pd.read_parquet(path)


@st.cache_data
def load_raw_economico() -> pd.DataFrame | None:
    """
    Carrega dados brutos de atividade econômica para análise granular.

    :return: DataFrame ou None se não existir
    """
    path = RAW / "atividade_economica" / "atividade_economica.csv"
    if not path.exists():
        return None
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(path, dtype=str, encoding=enc, low_memory=False)
            df.columns = df.columns.str.strip().str.upper()
            return df
        except UnicodeDecodeError:
            continue
    return None


df_agg = load_empresas()
df_raw = load_raw_economico()

if df_agg is None:
    st.warning("⚠️ Execute o pipeline ETL antes de acessar esta página.")
    st.stop()

# ---------------------------------------------------------------------------
# Filtros
# ---------------------------------------------------------------------------
col_f1, col_f2 = st.columns([2, 1])

with col_f1:
    bairros_disponiveis = ["Todos"] + sorted(df_agg["bairro"].str.title().tolist())
    bairro_sel = st.selectbox("Filtrar por bairro", bairros_disponiveis)

with col_f2:
    top_n = st.slider("Top N bairros no ranking", min_value=5, max_value=30, value=15)

st.markdown("---")

# ---------------------------------------------------------------------------
# Visão geral de setores (dados brutos)
# ---------------------------------------------------------------------------
if df_raw is not None and "CNAE_DIVISAO" not in df_raw.columns:
    if "CNAE_PRINCIPAL" in df_raw.columns:
        df_raw["CNAE_DIVISAO"] = df_raw["CNAE_PRINCIPAL"].str[:2]

if df_raw is not None and "CNAE_DIVISAO" in df_raw.columns:
    # Aplica filtro de bairro se selecionado
    df_filtrado = df_raw.copy()
    if bairro_sel != "Todos":
        bairro_norm = (
            bairro_sel.upper()
            .encode("ascii", errors="ignore")
            .decode("ascii")
        )
        if "BAIRRO" in df_filtrado.columns:
            df_filtrado = df_filtrado[
                df_filtrado["BAIRRO"]
                .str.strip().str.upper()
                .str.normalize("NFD")
                .str.encode("ascii", errors="ignore")
                .str.decode("ascii")
                == bairro_norm
            ]

    contagem_setor = (
        df_filtrado["CNAE_DIVISAO"]
        .value_counts()
        .reset_index()
        .rename(columns={"CNAE_DIVISAO": "setor", "count": "total"})
    )
    contagem_setor["setor_nome"] = contagem_setor["setor"].map(CNAE_LABELS).fillna(
        "Setor " + contagem_setor["setor"]
    )

    col_l, col_r = st.columns([1, 1])

    with col_l:
        st.subheader(
            f"Distribuição de Setores — {'BH' if bairro_sel == 'Todos' else bairro_sel}"
        )
        fig_pie = pie_setores(contagem_setor, col_setor="setor_nome", col_valor="total")
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_r:
        st.subheader("Ranking de Setores por Volume")
        fig_bar = bar_ranking(
            contagem_setor,
            col_x="total",
            col_y="setor_nome",
            top_n=10,
        )
        st.plotly_chart(fig_bar, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Ranking de bairros por diversidade de setores
# ---------------------------------------------------------------------------
st.subheader(f"Top {top_n} Bairros por Diversidade de Setores")
st.caption(
    "Maior diversidade indica ecossistema econômico mais resiliente — "
    "menos dependente de um único setor."
)

df_div = df_agg[["bairro", "diversidade_setores", "total_empresas"]].copy()
df_div["bairro"] = df_div["bairro"].str.title()

fig_div = bar_ranking(
    df_div,
    col_x="diversidade_setores",
    col_y="bairro",
    titulo="",
    top_n=top_n,
)
st.plotly_chart(fig_div, use_container_width=True)

# ---------------------------------------------------------------------------
# Tabela detalhada
# ---------------------------------------------------------------------------
with st.expander("📋 Ver tabela completa"):
    df_display = df_agg.copy()
    df_display["bairro"] = df_display["bairro"].str.title()
    if "setor_dominante" in df_display.columns:
        df_display["setor_dominante"] = df_display["setor_dominante"].map(CNAE_LABELS).fillna(
            df_display["setor_dominante"]
        )
    st.dataframe(
        df_display.rename(columns={
            "bairro": "Bairro",
            "total_empresas": "Empresas Ativas",
            "diversidade_setores": "Setores Distintos",
            "setor_dominante": "Setor Dominante",
        }),
        use_container_width=True,
        hide_index=True,
    )
