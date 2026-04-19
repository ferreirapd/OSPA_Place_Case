"""
Transformação da camada de acessibilidade multimodal.

Combina três fontes:
- Pontos de ônibus (localização via WKT — spatial join com bairros)
- Embarque por ponto (volume de passageiros, join por ID)
- Acidentes de trânsito (COORDENADA_X/Y em UTM EPSG:31983 — spatial join)

Todos os nomes de bairro são padronizados contra a tabela canônica
extraída de atividade_economica.csv (nomes populares).
"""

import logging
from pathlib import Path
import geopandas as gpd
import pandas as pd
from etl.transform._io import (
    find_column,
    load_bairros_canonicos,
    load_csv,
    match_bairro_canonico,
    spatial_join_to_bairros,
)

log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[2] / "data"
RAW = BASE / "raw"
PROCESSED = BASE / "processed"

PATHS = {
    "pontos":    RAW / "pontos_onibus"      / "pontos_onibus.csv",
    "embarques": RAW / "embarque_por_ponto" / "embarque_por_ponto.csv",
    "acidentes": RAW / "acidentes_transito" / "acidentes_transito.csv",
    "bairros":   RAW / "bairros"            / "bairros.csv",
    # Fonte canônica de nomes populares de bairro
    "eco":       RAW / "atividade_economica" / "atividade_economica.csv",
}
OUT_PATH = PROCESSED / "acessibilidade_por_bairro.parquet"

CRS_UTM = "EPSG:31983"

COL_CANDIDATES_BAIRRO   = ("NOME_BAIRRO_POPULAR", "NOME_BAIRRO", "NOME", "BAIRRO")
COL_CANDIDATES_ID_PONTO = (
    "IDENTIFICADOR_PONTO_ONIBUS", "ID_PONTO_ONIBUS_LINHA",
    "SIU", "CODIGO_PONTO", "COD_PONTO", "ID_PONTO", "COD_PONTO_ONIBUS", "PONTO",
)
COL_CANDIDATES_EMBARQUES = (
    "TOTAL GERAL", "QTD_EMBARQUES_DU", "QTD_EMBARQUES", "EMBARQUES_DU",
    "EMBARQUES", "TOTAL_EMBARQUES", "NUM_EMBARQUES", "PASSAGEIROS",
)
COL_CANDIDATES_GEOM   = ("GEOMETRIA", "GEOM", "WKT")
COL_CANDIDATES_COORD_X = ("COORDENADA_X", "X", "COORD_X", "LON", "LONGITUDE")
COL_CANDIDATES_COORD_Y = ("COORDENADA_Y", "Y", "COORD_Y", "LAT", "LATITUDE")


def _agg_pontos_via_spatial_join(
    df_pontos: pd.DataFrame,
    df_bairros: pd.DataFrame,
    canonicos: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    """
    Atribui bairro canônico a cada ponto via spatial join e agrega por bairro.

    :param df_pontos: DataFrame de pontos com coluna GEOMETRIA
    :param df_bairros: DataFrame de bairros com GEOMETRIA e nome
    :param canonicos: Lista de bairros canônicos para o fuzzy match
    :return: (agregação por bairro, df enriquecido, nome da coluna de ID)
    """
    col_geom_pt = find_column(df_pontos,  *COL_CANDIDATES_GEOM)
    col_geom_br = find_column(df_bairros, *COL_CANDIDATES_GEOM)
    col_nome_br = find_column(df_bairros, *COL_CANDIDATES_BAIRRO)
    col_id      = find_column(df_pontos,  *COL_CANDIDATES_ID_PONTO)

    if not all([col_geom_pt, col_geom_br, col_nome_br]):
        log.warning(
            "Spatial join impossível — geom_pt=%s, geom_br=%s, nome_br=%s",
            col_geom_pt, col_geom_br, col_nome_br,
        )
        return pd.DataFrame(columns=["bairro", "total_pontos_onibus"]), df_pontos, col_id

    df_enriched = spatial_join_to_bairros(
        df_pontos, df_bairros,
        col_wkt_pontos=col_geom_pt,
        col_wkt_bairros=col_geom_br,
        col_nome_bairro=col_nome_br,
    )

    df_enriched = df_enriched[df_enriched["BAIRRO_SJOIN"].notna()].copy()
    df_enriched["BAIRRO_CANON"] = match_bairro_canonico(
        df_enriched["BAIRRO_SJOIN"], canonicos
    )
    df_enriched = df_enriched[df_enriched["BAIRRO_CANON"].notna()]

    agg = (
        df_enriched.groupby("BAIRRO_CANON")
        .size()
        .reset_index(name="total_pontos_onibus")
        .rename(columns={"BAIRRO_CANON": "bairro"})
    )
    return agg, df_enriched, col_id


def _agg_embarques(
    df_pontos_enriched: pd.DataFrame,
    df_embarques: pd.DataFrame,
    col_id_pontos: str | None,
) -> pd.DataFrame:
    """
    Soma embarques diários por bairro via join com a tabela de pontos enriquecida.

    :param df_pontos_enriched: Pontos com coluna BAIRRO_CANON do spatial join
    :param df_embarques: DataFrame bruto de embarques
    :param col_id_pontos: Nome da coluna de ID no DataFrame de pontos
    :return: DataFrame com total_embarques_dia por bairro canônico
    """
    col_id_emb = find_column(df_embarques, *COL_CANDIDATES_ID_PONTO)
    col_qtd    = find_column(df_embarques, *COL_CANDIDATES_EMBARQUES)

    if not all([col_id_pontos, col_id_emb, col_qtd]):
        log.warning(
            "Join embarques: colunas insuficientes (id_pontos=%s, id_emb=%s, qtd=%s)",
            col_id_pontos, col_id_emb, col_qtd,
        )
        return pd.DataFrame(columns=["bairro", "total_embarques_dia"])

    df_embarques = df_embarques.copy()
    df_embarques[col_qtd] = pd.to_numeric(
        df_embarques[col_qtd], errors="coerce"
    ).fillna(0)

    pontos_lookup = (
        df_pontos_enriched[[col_id_pontos, "BAIRRO_CANON"]]
        .rename(columns={col_id_pontos: col_id_emb})
        .drop_duplicates(subset=col_id_emb)
    )

    merged = df_embarques.merge(pontos_lookup, on=col_id_emb, how="left")

    return (
        merged.dropna(subset=["BAIRRO_CANON"])
        .groupby("BAIRRO_CANON")[col_qtd]
        .sum()
        .reset_index(name="total_embarques_dia")
        .rename(columns={"BAIRRO_CANON": "bairro"})
    )


def _agg_acidentes_por_bairro(
    df_acidentes: pd.DataFrame,
    df_bairros: pd.DataFrame,
    canonicos: list[str],
) -> pd.DataFrame:
    """
    Distribui acidentes por bairro canônico via spatial join COORDENADA_X/Y (UTM).

    :param df_acidentes: DataFrame com COORDENADA_X/Y em UTM EPSG:31983
    :param df_bairros: DataFrame de bairros com GEOMETRIA e nome
    :param canonicos: Lista de bairros canônicos para o fuzzy match
    :return: DataFrame com total_acidentes por bairro canônico
    """
    col_x       = find_column(df_acidentes, *COL_CANDIDATES_COORD_X)
    col_y       = find_column(df_acidentes, *COL_CANDIDATES_COORD_Y)
    col_geom_br = find_column(df_bairros,   *COL_CANDIDATES_GEOM)
    col_nome_br = find_column(df_bairros,   *COL_CANDIDATES_BAIRRO)

    if not all([col_x, col_y, col_geom_br, col_nome_br]):
        log.warning(
            "Spatial join de acidentes impossível: x=%s, y=%s, geom_br=%s, nome_br=%s",
            col_x, col_y, col_geom_br, col_nome_br,
        )
        return pd.DataFrame(columns=["bairro", "total_acidentes"])

    df_acid = df_acidentes.copy()
    for col in (col_x, col_y):
        df_acid[col] = pd.to_numeric(
            df_acid[col].astype(str).str.replace(",", ".", regex=False),
            errors="coerce",
        )

    df_acid = df_acid.dropna(subset=[col_x, col_y])
    if df_acid.empty:
        log.warning("Nenhum acidente com coordenadas válidas")
        return pd.DataFrame(columns=["bairro", "total_acidentes"])

    gdf_acid = gpd.GeoDataFrame(
        df_acid,
        geometry=gpd.points_from_xy(df_acid[col_x], df_acid[col_y]),
        crs=CRS_UTM,
    )

    from shapely import wkt as shapely_wkt

    df_br = df_bairros.copy()
    df_br.columns = df_br.columns.str.strip().str.upper()
    df_br = df_br.dropna(subset=[col_geom_br])
    df_br["geometry"] = df_br[col_geom_br].map(
        lambda w: shapely_wkt.loads(w) if isinstance(w, str) else None
    )
    gdf_bairros = gpd.GeoDataFrame(
        df_br[[col_nome_br, "geometry"]].rename(columns={col_nome_br: "bairro_raw"}),
        crs=CRS_UTM,
    )

    gdf_joined = gpd.sjoin(
        gdf_acid[["geometry"]],
        gdf_bairros[["bairro_raw", "geometry"]],
        how="left", predicate="within",
    )

    sem_bairro = gdf_joined["bairro_raw"].isna().sum()
    log.info(
        "Acidentes: %d total | %d sem bairro (%.1f%%)",
        len(gdf_joined), sem_bairro,
        sem_bairro / len(gdf_joined) * 100 if len(gdf_joined) else 0,
    )

    gdf_joined = gdf_joined.dropna(subset=["bairro_raw"])
    gdf_joined["bairro"] = match_bairro_canonico(gdf_joined["bairro_raw"], canonicos)
    gdf_joined = gdf_joined.dropna(subset=["bairro"])

    agg = (
        gdf_joined.groupby("bairro")
        .size()
        .reset_index(name="total_acidentes")
    )
    log.info("Acidentes em %d bairros canônicos", len(agg))
    return agg


def _compute_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula índice de acessibilidade normalizado (0-1).

    Pesos: 50% embarques + 30% pontos + 20% inverso de acidentes.

    :param df: DataFrame com métricas brutas por bairro
    :return: DataFrame com coluna indice_acessibilidade adicionada
    """
    for col in ("total_pontos_onibus", "total_embarques_dia", "total_acidentes"):
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["rank_pontos"]        = df["total_pontos_onibus"].rank(pct=True)
    df["rank_embarques"]     = df["total_embarques_dia"].rank(pct=True)
    df["rank_acidentes_inv"] = 1 - df["total_acidentes"].rank(pct=True)

    df["indice_acessibilidade"] = (
        0.50 * df["rank_embarques"]
        + 0.30 * df["rank_pontos"]
        + 0.20 * df["rank_acidentes_inv"]
    ).round(4)

    return df.drop(columns=["rank_pontos", "rank_embarques", "rank_acidentes_inv"])


def run() -> pd.DataFrame:
    """
    Executa a transformação completa da camada de acessibilidade.

    :return: DataFrame agregado por bairro com índice de acessibilidade
    """
    log.info("Iniciando transformação: acessibilidade")

    canonicos    = load_bairros_canonicos(PATHS["eco"])
    df_pontos    = load_csv(PATHS["pontos"],    "pontos_onibus")
    df_embarques = load_csv(PATHS["embarques"], "embarques")
    df_acidentes = load_csv(PATHS["acidentes"], "acidentes")
    df_bairros   = load_csv(PATHS["bairros"],   "bairros")

    agg_pontos, df_pontos_enriched, col_id = _agg_pontos_via_spatial_join(
        df_pontos, df_bairros, canonicos,
    )
    agg_embarques = _agg_embarques(df_pontos_enriched, df_embarques, col_id)
    agg_acidentes = _agg_acidentes_por_bairro(df_acidentes, df_bairros, canonicos)

    df = (
        agg_pontos
        .merge(agg_embarques, on="bairro", how="outer")
        .merge(agg_acidentes, on="bairro", how="outer")
        .fillna(0)
    )
    df = _compute_index(df)

    PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    log.info("✓ acessibilidade_por_bairro.parquet salvo: %d bairros", len(df))
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run().sort_values("indice_acessibilidade", ascending=False).head(10).to_string())