"""
Entry point do app Streamlit: OSPA Place Data Engineer Case.

Toda a navegação é definida aqui via st.navigation(). Os títulos e a ordem
no menu lateral vêm do title= de cada st.Page, não dos nomes de arquivo.
"""

from pathlib import Path
import streamlit as st


LOGO = Path(__file__).parent/"assets"/"logo_ospa_place.png"

st.set_page_config(
    page_title="OSPA Place Data Engineer Case - Pedro Ferreira",
    page_icon=str(LOGO),
    layout="wide",
    initial_sidebar_state="expanded",
)

st.logo(
    image=str(LOGO),
    icon_image=str(LOGO),
)

nav = st.navigation(
    {
        "": [
            st.Page(
                "pages/page_home.py",
                title="Início",
                icon=":material/home:",
                default=True,
            ),
        ],
        "Para Investidores": [
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