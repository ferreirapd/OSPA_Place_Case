"""
Entry point do app Streamlit — BH Investment Insights.

Configura a navegação entre as seções de stakeholders e visão técnica.
"""

import streamlit as st

st.set_page_config(
    page_title="BH Investment Insights",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Sidebar — navegação principal
# ---------------------------------------------------------------------------
st.sidebar.title("🏙️ BH Investment Insights")
st.sidebar.markdown("---")

st.sidebar.markdown("### 📌 Navegação")
st.sidebar.markdown(
    """
**Para Investidores**
- Visão Geral de BH
- Análise Setorial
- Acessibilidade e Mobilidade
- Qualidade Urbana
- Mapa de Oportunidades

**Visão Técnica**
- Arquitetura Atual
- Pipeline ETL
- Arquitetura AWS
- Visão de Futuro
"""
)

st.sidebar.markdown("---")
st.sidebar.caption("Dados: Portal de Dados Abertos da PBH · 2024")

# ---------------------------------------------------------------------------
# Home page
# ---------------------------------------------------------------------------
st.title("🏙️ BH Investment Insights")
st.subheader("Uma plataforma de dados para orientar investimentos em Belo Horizonte")

st.markdown("---")

col1, col2 = st.columns([3, 2])

with col1:
    st.markdown(
        """
        ### O Desafio

        Belo Horizonte concentra oportunidades econômicas distribuídas de forma
        desigual pelo território. Investidores enfrentam dificuldade em identificar
        **onde investir** — e por quê — sem uma visão integrada do contexto urbano.

        ### A Proposta

        Esta plataforma cruza **dados públicos abertos da PBH** para construir um
        índice de atratividade por bairro, combinando três dimensões:

        - 📊 **Atividade Econômica** — densidade e diversidade de empresas ativas
        - 🚌 **Acessibilidade Multimodal** — fluxo real de pessoas e infraestrutura
        - 🌳 **Qualidade Urbana** — amenidades e equipamentos públicos

        ### Como Navegar

        Use o menu lateral para acessar as análises para **investidores** ou
        explorar os detalhes técnicos do pipeline de dados na **Visão Técnica**.
        """
    )

with col2:
    st.info(
        """
        **📂 Fontes de Dados**

        - Atividade Econômica (PBH)
        - Bairros Oficiais (PBH)
        - Pontos de Ônibus (BHTRANS)
        - Embarque por Ponto (BHTRANS)
        - Acidentes de Trânsito (BHTRANS)
        - Parques Municipais (PBH)
        - Equipamentos Esportivos (PBH)
        - Matriz O-D — amostra (BHTRANS)

        Todos disponíveis em
        [dados.pbh.gov.br](https://dados.pbh.gov.br)
        """,
        icon="🗂️",
    )

    st.success(
        """
        **⚙️ Stack Técnica**

        Python · Pandas · PySpark
        Streamlit · Plotly · Folium
        Docker · Parquet
        """,
        icon="🛠️",
    )
