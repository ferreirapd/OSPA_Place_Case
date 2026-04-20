"""
Página inicial — contexto do projeto.
"""

import streamlit as st

st.title("BH Investment Insights")
st.markdown("---")

col1, col2 = st.columns([3, 2])

with col1:
    st.markdown(
        """
        ### O desafio

        Belo Horizonte tem 489 bairros, oito bases de dados públicas abertas e
        nenhuma ferramenta que junte tudo num lugar só para quem quer entender
        onde investir na cidade.

        O case proposto pelo OSPA Place pedia exatamente isso: um pipeline de ETL
        sobre os dados do portal da PBH e uma visualização que transforme esses
        dados em algo útil para stakeholders.

        ### O que foi construído

        O pipeline cruza três dimensões para produzir um score de atratividade
        por bairro: a atividade econômica registrada pela prefeitura, a
        infraestrutura e o fluxo real de transporte público, e os equipamentos
        urbanos de cada região. O app aqui é a camada de visualização em cima
        desse pipeline.

        ### Como navegar

        As três páginas para investidores mostram os dados de ângulos diferentes —
        de um panorama geral até o perfil detalhado de cada bairro. A página
        técnica explica as decisões de engenharia por trás do pipeline e como a
        arquitetura escalaria em produção.
        """
    )

with col2:
    st.info(
        """
        **Fontes de dados**

        - Atividade Econômica (PBH)
        - Bairros Oficiais (PBH)
        - Pontos de Ônibus (BHTRANS)
        - Embarque por Ponto (BHTRANS)
        - Acidentes de Trânsito (BHTRANS)
        - Parques Municipais (PBH/FPZ)
        - Equipamentos Esportivos (PBH)
        - Matriz Origem-Destino (BHTRANS)

        Todas em [dados.pbh.gov.br](https://dados.pbh.gov.br)
        """,
        icon="🗂️",
    )

    st.success(
        """
        **Stack**

        Python · Pandas · PySpark · rapidfuzz
        Streamlit · Plotly · Folium
        Docker · Parquet
        """,
        icon="⚙️",
    )