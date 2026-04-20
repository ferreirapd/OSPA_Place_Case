"""
Entry point do app Streamlit — OSPA Place Case.

Toda a navegação é definida aqui via st.navigation(). Os títulos e a ordem
no menu lateral vêm do title= de cada st.Page, não dos nomes de arquivo.
"""

import streamlit as st

st.set_page_config(
    page_title="OSPA Place Case Data Engineer - Pedro Ferreira",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

nav = st.navigation(
    {
        "Para Investidores": [
            st.Page(
                "pages/page_home.py",
                title="Início",
                icon=":material/home:",
                default=True,
            ),
            st.Page(
                "pages/page_panorama_economico.py",
                title="Panorama Econômico",
                icon=":material/bar_chart:",
            ),
            st.Page(
                "pages/page_infraestrutura.py",
                title="Infraestrutura e Mobilidade",
                icon=":material/directions_bus:",
            ),
            st.Page(
                "pages/page_oportunidades.py",
                title="Mapa de Oportunidades",
                icon=":material/map:",
            ),
        ],
        "Visão Técnica": [
            st.Page(
                "pages/page_tecnica.py",
                title="Pipeline e Arquitetura",
                icon=":material/settings:",
            ),
        ],
    }
)

nav.run()