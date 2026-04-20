"""
Panorama Econômico: distribuição de empresas, setores e densidade por bairro.
"""

from pathlib import Path
import sys
import pandas as pd
import streamlit as st
from app.components.graficos import bar_ranking, pie_setores
from app.components.footer import render_footer


sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

PROCESSED = Path(__file__).resolve().parents[2]/"data"/"processed"
RAW = Path(__file__).resolve().parents[2]/"data"/"raw"
CNAE_LABELS: dict[str, str] = {
    "01": "Agricultura",
    "05": "Mineração",
    "10": "Alimentos",
    "13": "Têxtil",
    "19": "Combustíveis",
    "22": "Borracha/Plástico",
    "25": "Metal",
    "26": "Eletrônicos",
    "28": "Máquinas",
    "33": "Manutenção",
    "35": "Energia",
    "36": "Saneamento",
    "41": "Construção",
    "45": "Veículos",
    "46": "Com. Atacado",
    "47": "Com. Varejo",
    "49": "Transp. Terrestre",
    "52": "Armazenagem",
    "55": "Hospedagem",
    "56": "Alimentação",
    "58": "Editoras",
    "61": "Telecom",
    "62": "TI/Software",
    "64": "Financeiro",
    "68": "Imobiliário",
    "69": "Jurídico",
    "70": "Consultoria",
    "71": "Arq./Eng.",
    "72": "P&D",
    "73": "Publicidade",
    "74": "Design",
    "75": "Veterinário",
    "77": "Locação",
    "78": "RH/Seleção",
    "79": "Turismo",
    "80": "Segurança",
    "81": "Facilities",
    "82": "Adm. Apoio",
    "84": "Adm. Pública",
    "85": "Educação",
    "86": "Saúde",
    "87": "Assist. Social",
    "90": "Artes/Cultura",
    "93": "Esporte/Lazer",
    "95": "Reparação",
    "96": "Serv. Pessoais",
}


@st.cache_data
def load_empresas() -> pd.DataFrame | None:
    """
    Carrega dados de empresas por bairro, já processados e agregados.

    :return: DataFrame com colunas 'bairro', 'total_empresas', 'diversidade_setores', etc
    """
    path = PROCESSED/"empresas_por_bairro.parquet"
    return pd.read_parquet(path) if path.exists() else None


@st.cache_data
def load_raw_eco() -> pd.DataFrame | None:
    """
    Carrega dados brutos de atividade econômica, sem processamento.

    :return: DataFrame com dados brutos, ou None se o arquivo não existir
    """ 
    path = RAW/"atividade_economica"/"atividade_economica.csv"
    if not path.exists():
        return None
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(path, dtype=str, encoding=enc, sep=";", low_memory=False)
            df.columns = df.columns.str.strip().str.upper()
            return df
        except UnicodeDecodeError:
            continue
    return None


st.title("Panorama Econômico")
st.caption("Distribuição de empresas ativas, setores e densidade por bairro")
st.markdown("---")

df = load_empresas()
df_raw = load_raw_eco()

if df is None:
    st.warning("Execute o pipeline ETL antes de acessar esta página.")
    st.stop()

df["bairro_display"] = df["bairro"].str.title()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Empresas com alvará ativo", f"{df['total_empresas'].sum():,.0f}")
col2.metric("Bairros com atividade", f"{len(df):,}")
col3.metric("Diversidade máxima de setores", f"{df['diversidade_setores'].max():,}")
col4.metric(
    "Bairro mais denso",
    df.loc[df["total_empresas"].idxmax(), "bairro_display"],
)

st.markdown("---")

col_left, col_right = st.columns(2)

with col_left:
    top_n = st.slider("Top N bairros", 5, 30, 15, key="top_n_eco")
    st.subheader("Bairros por volume de empresas")
    st.plotly_chart(
        bar_ranking(
            df.assign(bairro=df["bairro_display"]),
            col_x="total_empresas", col_y="bairro", titulo="", top_n=top_n,
        ),
        use_container_width=True,
    )

with col_right:
    st.subheader("Distribuição por setor - BH")
    if df_raw is not None:
        if "CNAE_DIVISAO" not in df_raw.columns and "CNAE_PRINCIPAL" in df_raw.columns:
            df_raw["CNAE_DIVISAO"] = df_raw["CNAE_PRINCIPAL"].str[:2]
        if "CNAE_DIVISAO" in df_raw.columns:
            contagem = (
                df_raw["CNAE_DIVISAO"]
                .value_counts()
                .reset_index()
                .rename(columns={"CNAE_DIVISAO": "setor", "count": "total"})
            )
            contagem["setor_nome"] = (
                contagem["setor"].map(CNAE_LABELS)
                .fillna("Setor " + contagem["setor"].astype(str))
            )
            st.plotly_chart(
                pie_setores(contagem, col_setor="setor_nome", col_valor="total"),
                use_container_width=True,
            )
    else:
        st.info("Dados brutos não disponíveis para distribuição setorial.")

st.markdown("---")

st.subheader("Diversidade setorial por bairro")
st.caption(
    "Bairros com alta diversidade dependem menos de um único setor: "
    "um indicador de resiliência econômica."
)
st.plotly_chart(
    bar_ranking(
        df.assign(bairro=df["bairro_display"]),
        col_x="diversidade_setores", col_y="bairro", titulo="", top_n=top_n,
    ),
    use_container_width=True,
)

st.markdown("---")

with st.expander("Ver tabela completa"):
    cols_disp = {
        "bairro_display": "Bairro",
        "total_empresas": "Empresas Ativas",
        "diversidade_setores": "Setores Distintos",
    }
    if "setor_dominante_nome" in df.columns:
        cols_disp["setor_dominante_nome"] = "Setor Dominante"
    st.dataframe(
        df[cols_disp.keys()].rename(columns=cols_disp)
        .sort_values("Empresas Ativas", ascending=False),
        use_container_width=True, hide_index=True,
    )

render_footer()