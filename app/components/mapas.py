"""
Componentes reutilizáveis de mapa para o app Streamlit.

Todas as funções retornam objetos Folium prontos para exibição
via streamlit-folium.
"""

from typing import Any
import folium
import pandas as pd
from folium.plugins import MarkerCluster

BH_CENTER = [-19.9167, -43.9345]
DEFAULT_ZOOM = 12


def choropleth_map(
    df: pd.DataFrame,
    col_valor: str,
    col_bairro: str = "bairro",
    geojson: dict[str, Any] | None = None,
    titulo: str = "",
    cor: str = "YlOrRd",
) -> folium.Map:
    """
    Cria mapa coroplético colorindo bairros por valor numérico.

    :param df: DataFrame com coluna de bairro e valor
    :param col_valor: Nome da coluna numérica a visualizar
    :param col_bairro: Nome da coluna de bairro (chave de join com GeoJSON)
    :param geojson: GeoJSON dos polígonos de bairros (opcional)
    :param titulo: Título exibido na legenda
    :param cor: Esquema de cores Brewer (ex: 'YlOrRd', 'Blues', 'RdYlGn')
    :return: Objeto folium.Map configurado
    """
    m = folium.Map(location=BH_CENTER, zoom_start=DEFAULT_ZOOM, tiles="CartoDB positron")

    if geojson is not None:
        folium.Choropleth(
            geo_data=geojson,
            data=df,
            columns=[col_bairro, col_valor],
            key_on="feature.properties.NOME",
            fill_color=cor,
            fill_opacity=0.7,
            line_opacity=0.3,
            legend_name=titulo,
            nan_fill_color="lightgray",
        ).add_to(m)

        # Tooltip com nome do bairro e valor
        folium.GeoJson(
            geojson,
            style_function=lambda x: {"fillOpacity": 0, "weight": 0},
            tooltip=folium.GeoJsonTooltip(
                fields=["NOME"],
                aliases=["Bairro:"],
                localize=True,
            ),
        ).add_to(m)
    else:
        # Fallback: pontos com tamanho proporcional ao valor
        point_map(df, col_valor=col_valor, col_bairro=col_bairro).add_to(m)

    return m


def point_map(
    df: pd.DataFrame,
    col_lat: str = "latitude",
    col_lon: str = "longitude",
    col_valor: str | None = None,
    col_bairro: str = "bairro",
    cluster: bool = True,
) -> folium.Map:
    """
    Cria mapa de pontos com clustering opcional.

    :param df: DataFrame com colunas de coordenadas
    :param col_lat: Nome da coluna de latitude
    :param col_lon: Nome da coluna de longitude
    :param col_valor: Coluna de valor para popup (opcional)
    :param col_bairro: Coluna de nome para popup
    :param cluster: Se True, agrupa pontos próximos com MarkerCluster
    :return: Objeto folium.Map configurado
    """
    m = folium.Map(location=BH_CENTER, zoom_start=DEFAULT_ZOOM, tiles="CartoDB positron")

    layer = MarkerCluster() if cluster else m

    for _, row in df.dropna(subset=[col_lat, col_lon]).iterrows():
        popup_text = str(row.get(col_bairro, ""))
        if col_valor and col_valor in row:
            popup_text += f"<br><b>{col_valor}:</b> {row[col_valor]:.2f}"

        folium.CircleMarker(
            location=[row[col_lat], row[col_lon]],
            radius=5,
            color="#e05c00",
            fill=True,
            fill_opacity=0.7,
            popup=folium.Popup(popup_text, max_width=200),
        ).add_to(layer)

    if cluster:
        layer.add_to(m)

    return m


def score_map(
    df: pd.DataFrame,
    col_score: str = "score_final",
    col_bairro: str = "bairro",
    geojson: dict[str, Any] | None = None,
) -> folium.Map:
    """
    Mapa de calor do score de oportunidade por bairro.

    Usa escala divergente verde (alto score) → vermelho (baixo score).

    :param df: DataFrame com score por bairro
    :param col_score: Coluna do score final
    :param col_bairro: Coluna de nome do bairro
    :param geojson: GeoJSON dos polígonos de bairros
    :return: Objeto folium.Map configurado
    """
    return choropleth_map(
        df=df,
        col_valor=col_score,
        col_bairro=col_bairro,
        geojson=geojson,
        titulo="Score de Atratividade (0–100)",
        cor="RdYlGn",
    )