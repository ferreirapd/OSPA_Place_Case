"""
Componentes reutilizáveis de gráficos Plotly para o app Streamlit.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

CORES_BH = px.colors.sequential.Oranges
COR_DESTAQUE = "#e05c00"


def bar_ranking(
    df: pd.DataFrame,
    col_x: str,
    col_y: str,
    titulo: str = "",
    top_n: int = 15,
    cor: str = COR_DESTAQUE,
) -> go.Figure:
    """
    Gráfico de barras horizontais para ranking de bairros.

    :param df: DataFrame com dados
    :param col_x: Coluna do valor (eixo x)
    :param col_y: Coluna do label (eixo y)
    :param titulo: Título do gráfico
    :param top_n: Número de itens a exibir
    :param cor: Cor das barras
    :return: Figura Plotly
    """
    df_plot = df.nlargest(top_n, col_x).sort_values(col_x)
    fig = px.bar(
        df_plot,
        x=col_x,
        y=col_y,
        orientation="h",
        title=titulo,
        color_discrete_sequence=[cor],
    )
    fig.update_layout(
        yaxis_title="",
        xaxis_title="",
        margin=dict(l=0, r=20, t=40, b=0),
        plot_bgcolor="white",
    )
    return fig


def scatter_dimensoes(
    df: pd.DataFrame,
    col_x: str,
    col_y: str,
    col_size: str | None = None,
    col_label: str = "bairro",
    titulo: str = "",
) -> go.Figure:
    """
    Scatter plot para cruzar duas dimensões do score, com bairros como pontos.

    :param df: DataFrame com métricas por bairro
    :param col_x: Dimensão do eixo X
    :param col_y: Dimensão do eixo Y
    :param col_size: Coluna para tamanho do ponto (opcional)
    :param col_label: Coluna de rótulo no hover
    :param titulo: Título do gráfico
    :return: Figura Plotly
    """
    fig = px.scatter(
        df,
        x=col_x,
        y=col_y,
        size=col_size,
        hover_name=col_label,
        title=titulo,
        color=col_size or col_y,
        color_continuous_scale="Oranges",
    )
    fig.update_layout(
        plot_bgcolor="white",
        margin=dict(l=0, r=0, t=40, b=0),
    )
    fig.update_xaxes(showgrid=True, gridcolor="#f0f0f0")
    fig.update_yaxes(showgrid=True, gridcolor="#f0f0f0")
    return fig


def pie_setores(
    df: pd.DataFrame,
    col_setor: str,
    col_valor: str,
    titulo: str = "Distribuição por Setor",
    top_n: int = 8,
) -> go.Figure:
    """
    Gráfico de pizza para distribuição de setores econômicos.

    Agrupa os menores setores em 'Outros' para manter legibilidade.

    :param df: DataFrame com setor e contagem
    :param col_setor: Coluna de nome do setor
    :param col_valor: Coluna de contagem/valor
    :param titulo: Título do gráfico
    :param top_n: Número de setores exibidos individualmente
    :return: Figura Plotly
    """
    df_sorted = df.nlargest(top_n, col_valor)
    outros = df[col_valor].sum() - df_sorted[col_valor].sum()
    if outros > 0:
        outros_row = pd.DataFrame([{col_setor: "Outros", col_valor: outros}])
        df_sorted = pd.concat([df_sorted, outros_row], ignore_index=True)

    fig = px.pie(
        df_sorted,
        names=col_setor,
        values=col_valor,
        title=titulo,
        color_discrete_sequence=CORES_BH,
        hole=0.35,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    return fig


def radar_bairro(
    scores: dict[str, float],
    bairro: str,
) -> go.Figure:
    """
    Gráfico radar com os três componentes do score de um bairro.

    :param scores: Dicionário {dimensão: valor 0-1}
    :param bairro: Nome do bairro para o título
    :return: Figura Plotly
    """
    categorias = list(scores.keys())
    valores = list(scores.values())
    valores_fechado = valores + [valores[0]]
    categorias_fechado = categorias + [categorias[0]]

    fig = go.Figure(
        go.Scatterpolar(
            r=valores_fechado,
            theta=categorias_fechado,
            fill="toself",
            line_color=COR_DESTAQUE,
            fillcolor=f"rgba(224, 92, 0, 0.2)",
        )
    )
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
        title=f"Perfil: {bairro}",
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig
