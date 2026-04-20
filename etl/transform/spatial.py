"""
Helpers de spatial join reutilizados pelos módulos de transformação.

Consolida a lógica repetida que estava duplicada em acessibilidade.py:
- Parse de WKT em polígonos de bairros (EPSG:31983)
- Atribuição de bairro a cada ponto por contenção
- Padronização canônica do bairro resultante

Há duas variantes de entrada: pontos em formato WKT (colunas GEOMETRIA)
ou pontos em formato coordenadas X/Y (UTM). Ambas produzem a mesma saída:
um DataFrame com coluna `bairro` canônica.
"""

import logging
from typing import TYPE_CHECKING
import pandas as pd
from etl.transform._io import EPSG_PBH, match_bairro_canonico

if TYPE_CHECKING:
    import geopandas as gpd

log = logging.getLogger(__name__)


def _load_gdf_bairros(
    df_bairros: pd.DataFrame,
    col_geom: str,
    col_nome: str,
) -> "gpd.GeoDataFrame":
    """
    Converte DataFrame de bairros em GeoDataFrame com polígonos parseados.

    :param df_bairros: DataFrame bruto com coluna de geometria WKT
    :param col_geom: Nome da coluna de geometria
    :param col_nome: Nome da coluna de nome do bairro
    :return: GeoDataFrame com colunas [col_nome, geometry] em EPSG:31983
    """
    import geopandas as gpd
    from shapely import wkt

    df = df_bairros[[col_nome, col_geom]].copy()
    df["geometry"] = df[col_geom].apply(
        lambda s: wkt.loads(s) if isinstance(s, str) and s.strip() else None
    )
    return gpd.GeoDataFrame(
        df.dropna(subset=["geometry"]),
        geometry="geometry",
        crs=f"EPSG:{EPSG_PBH}",
    )[[col_nome, "geometry"]]


def sjoin_pontos_wkt(
    df_pontos: pd.DataFrame,
    df_bairros: pd.DataFrame,
    col_geom_pontos: str,
    col_geom_bairros: str,
    col_nome_bairro: str,
) -> pd.DataFrame:
    """
    Atribui nome de bairro a cada ponto via spatial join (pontos em WKT).

    :param df_pontos: DataFrame com coluna de geometria WKT
    :param df_bairros: DataFrame de bairros com geometria WKT e nome
    :param col_geom_pontos: Nome da coluna WKT nos pontos
    :param col_geom_bairros: Nome da coluna WKT nos bairros
    :param col_nome_bairro: Nome da coluna de nome de bairro
    :return: df_pontos enriquecido com coluna BAIRRO_SJOIN
    """
    import geopandas as gpd
    from shapely import wkt

    df = df_pontos.copy()
    df["_geom"] = df[col_geom_pontos].apply(
        lambda s: wkt.loads(s) if isinstance(s, str) and s.strip() else None
    )
    gdf_pontos = gpd.GeoDataFrame(
        df.dropna(subset=["_geom"]),
        geometry="_geom",
        crs=f"EPSG:{EPSG_PBH}",
    )
    gdf_bairros = _load_gdf_bairros(df_bairros, col_geom_bairros, col_nome_bairro)

    joined = gpd.sjoin(gdf_pontos, gdf_bairros, how="left", predicate="within")
    result = pd.DataFrame(
        joined.drop(columns=["geometry", "_geom", "index_right"], errors="ignore")
    ).rename(columns={col_nome_bairro: "BAIRRO_SJOIN"})

    n_matched = result["BAIRRO_SJOIN"].notna().sum()
    log.info("Spatial join (WKT): %d/%d pontos com bairro", n_matched, len(result))
    return result


def sjoin_pontos_xy(
    df_pontos: pd.DataFrame,
    df_bairros: pd.DataFrame,
    col_x: str,
    col_y: str,
    col_geom_bairros: str,
    col_nome_bairro: str,
) -> pd.DataFrame:
    """
    Atribui nome de bairro a cada ponto via spatial join (pontos em X/Y UTM).

    :param df_pontos: DataFrame com colunas de coordenadas X e Y em UTM
    :param df_bairros: DataFrame de bairros com geometria WKT e nome
    :param col_x: Nome da coluna X (coordenada UTM)
    :param col_y: Nome da coluna Y (coordenada UTM)
    :param col_geom_bairros: Nome da coluna WKT nos bairros
    :param col_nome_bairro: Nome da coluna de nome de bairro
    :return: df_pontos enriquecido com coluna BAIRRO_SJOIN
    """
    import geopandas as gpd

    df = df_pontos.copy()
    for col in (col_x, col_y):
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", ".", regex=False),
            errors="coerce",
        )
    df = df.dropna(subset=[col_x, col_y])
    if df.empty:
        log.warning("Nenhum ponto com coordenadas válidas")
        return pd.DataFrame(columns=[*df_pontos.columns, "BAIRRO_SJOIN"])

    gdf_pontos = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[col_x], df[col_y]),
        crs=f"EPSG:{EPSG_PBH}",
    )
    gdf_bairros = _load_gdf_bairros(df_bairros, col_geom_bairros, col_nome_bairro)

    joined = gpd.sjoin(gdf_pontos, gdf_bairros, how="left", predicate="within")
    result = pd.DataFrame(
        joined.drop(columns=["geometry", "index_right"], errors="ignore")
    ).rename(columns={col_nome_bairro: "BAIRRO_SJOIN"})

    n_matched = result["BAIRRO_SJOIN"].notna().sum()
    log.info("Spatial join (XY): %d/%d pontos com bairro", n_matched, len(result))
    return result


def aggregate_por_bairro_canonico(
    df_com_sjoin: pd.DataFrame,
    canonicos: list[str],
    etapa: str,
    fonte: str,
    col_metrica: str = "total",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Converte BAIRRO_SJOIN em bairro canônico e agrega por contagem.

    :param df_com_sjoin: DataFrame com coluna BAIRRO_SJOIN do spatial join
    :param canonicos: Lista de bairros canônicos
    :param etapa: Nome da etapa (para auditoria em EXCLUSOES)
    :param fonte: Nome do dataset de origem (para auditoria em EXCLUSOES)
    :param col_metrica: Nome da coluna de contagem no output
    :return: Tupla (agregação por bairro, df enriquecido com BAIRRO_CANON)
    """
    df = df_com_sjoin[df_com_sjoin["BAIRRO_SJOIN"].notna()].copy()
    df["BAIRRO_CANON"] = match_bairro_canonico(
        df["BAIRRO_SJOIN"], canonicos, etapa=etapa, fonte=fonte,
    )
    df = df[df["BAIRRO_CANON"].notna()]

    agg = (
        df.groupby("BAIRRO_CANON")
        .size()
        .reset_index(name=col_metrica)
        .rename(columns={"BAIRRO_CANON": "bairro"})
    )
    return agg, df