"""
Footer reutilizável com créditos e links de perfil do autor.
"""

import streamlit as st


def render_footer() -> None:
    """
    Renderiza footer com nome do autor, GitHub e LinkedIn.
    Deve ser chamado no final do corpo de cada página, após todo o conteúdo.
    Não é fixo, aparece somente após o scroll completo.
    """
    st.markdown("---")
    st.markdown(
        """
        <div style="text-align: center; color: #888; font-size: 0.85rem; padding: 8px 0 4px 0;">
            Desenvolvido por <strong>Pedro Ferreira</strong>
            &nbsp;·&nbsp;
            <a href="https://github.com/ferreirapd" target="_blank" style="color: #888; text-decoration: none;">
                <img src="https://cdn.simpleicons.org/github/888" width="16" style="vertical-align: middle; margin-right: 3px;"/>
                ferreirapd
            </a>
            &nbsp;·&nbsp;
            <a href="https://linkedin.com/in/ferreirapd" target="_blank" style="color: #888; text-decoration: none;">
                <img src="https://cdn.simpleicons.org/linkedin/888" width="16" style="vertical-align: middle; margin-right: 3px;"/>
                ferreirapd
            </a>
        </div>
        """,
        unsafe_allow_html=True,
    )