"""
Transformação da camada de acessibilidade multimodal.

Combina três fontes:
- Pontos de ônibus (localização via WKT — spatial join com bairros)
- Estimativa de embarque por ponto (volume de passageiros, join por ID do ponto)
- Acidentes de trânsito (COORDENADA_X/Y em UTM EPSG:31983 — spatial join com bairros)

Agrega por bairro e produz índice de acessibilidade normalizado (0–1).
"""

import logging
from pathlib import Path
import geopandas as gpd
import pandas as pd
from etl.transform._io import (
    find_column,
    load_csv,
    normalize_bairro,
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
}
OUT_PATH = PROCESSED / "acessibilidade_por_bairro.parquet"

CRS_UTM = "EPSG:31983"

# Candidatos a nome de coluna
COL_CANDIDATES_BAIRRO     = ("NOME_BAIRRO_POPULAR", "NOME_BAIRRO", "NOME", "BAIRRO")
COL_CANDIDATES_ID_PONTO   = (
    "IDENTIFICADOR_PONTO_ONIBUS",
    "ID_PONTO_ONIBUS_LINHA",
    "SIU",
    "CODIGO_PONTO",
    "COD_PONTO",
    "ID_PONTO",
    "COD_PONTO_ONIBUS",
    "PONTO",
)
COL_CANDIDATES_EMBARQUES  = (
    "TOTAL GERAL",
    "QTD_EMBARQUES_DU",
    "QTD_EMBARQUES",
    "EMBARQUES_DU",
    "EMBARQUES",
    "TOTAL_EMBARQUES",
    "NUM_EMBARQUES",
    "PASSAGEIROS",
)
COL_CANDIDATES_GEOM       = ("GEOMETRIA", "GEOM", "WKT")
COL_CANDIDATES_COORD_X    = ("COORDENADA_X", "X", "COORD_X", "LON", "LONGITUDE")
COL_CANDIDATES_COORD_Y    = ("COORDENADA_Y", "Y", "COORD_Y", "LAT", "LATITUDE")


def _agg_pontos_via_spatial_join(
    df_pontos: pd.DataFrame,
    df_bairros: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    """
    Atribui bairro a cada ponto via spatial join e conta por bairro.

    :param df_pontos: DataFrame bruto de pontos com coluna GEOMETRIA
    :param df_bairros: DataFrame bruto de bairros com GEOMETRIA e NOME
    :return: Tupla (agregação por bairro, df_pontos enriquecido com BAIRRO_SJOIN, nome da coluna de ID)
    """
    col_geom_pt = find_column(df_pontos,  *COL_CANDIDATES_GEOM)
    col_geom_br = find_column(df_bairros, *COL_CANDIDATES_GEOM)
    col_nome_br = find_column(df_bairros, *COL_CANDIDATES_BAIRRO)
    col_id      = find_column(df_pontos,  *COL_CANDIDATES_ID_PONTO)

    if not all([col_geom_pt, col_geom_br, col_nome_br]):
        log.warning(
            "Spatial join impossível — colunas: geom_pt=%s, geom_br=%s, nome_br=%s",
            col_geom_pt, col_geom_br, col_nome_br,
        )
        return pd.DataFrame(columns=["bairro", "total_pontos_onibus"]), df_pontos, col_id

    log.info("Executando spatial join: pontos × polígonos de bairros...")
    df_enriched = spatial_join_to_bairros(
        df_pontos, df_bairros,
        col_wkt_pontos=col_geom_pt,
        col_wkt_bairros=col_geom_br,
        col_nome_bairro=col_nome_br,
    )

    df_enriched = df_enriched[df_enriched["BAIRRO_SJOIN"].notna()].copy()
    df_enriched["BAIRRO_NORM"] = normalize_bairro(df_enriched["BAIRRO_SJOIN"])

    agg = (
        df_enriched.groupby("BAIRRO_NORM")
        .size()
        .reset_index(name="total_pontos_onibus")
        .rename(columns={"BAIRRO_NORM": "bairro"})
    )
    return agg, df_enriched, col_id


def _agg_embarques(
    df_pontos_enriched: pd.DataFrame,
    df_embarques: pd.DataFrame,
    col_id_pontos: str | None,
) -> pd.DataFrame:
    """
    Soma embarques diários por bairro, via join com a tabela de pontos enriquecida.

    :param df_pontos_enriched: Pontos já com coluna BAIRRO_NORM do spatial join
    :param df_embarques: DataFrame bruto de embarques
    :param col_id_pontos: Nome da coluna de ID no DataFrame de pontos
    :return: DataFrame com total_embarques_dia por bairro
    """
    col_id_emb = find_column(df_embarques, *COL_CANDIDATES_ID_PONTO)
    col_qtd    = find_column(df_embarques, *COL_CANDIDATES_EMBARQUES)

    if not all([col_id_pontos, col_id_emb, col_qtd]):
        log.warning(
            "Join embarques: colunas insuficientes (id_pontos=%s, id_emb=%s, qtd=%s) — pulando",
            col_id_pontos, col_id_emb, col_qtd,
        )
        log.warning("Colunas disponíveis em embarques: %s", list(df_embarques.columns))
        return pd.DataFrame(columns=["bairro", "total_embarques_dia"])

    df_embarques = df_embarques.copy()
    df_embarques[col_qtd] = pd.to_numeric(df_embarques[col_qtd], errors="coerce").fillna(0)

    pontos_lookup = (
        df_pontos_enriched[[col_id_pontos, "BAIRRO_NORM"]]
        .rename(columns={col_id_pontos: col_id_emb})
        .drop_duplicates(subset=col_id_emb)
    )

    merged = df_embarques.merge(pontos_lookup, on=col_id_emb, how="left")

    agg = (
        merged.dropna(subset=["BAIRRO_NORM"])
        .groupby("BAIRRO_NORM")[col_qtd]
        .sum()
        .reset_index(name="total_embarques_dia")
        .rename(columns={"BAIRRO_NORM": "bairro"})
    )
    return agg


def _agg_acidentes_por_bairro(
    df_acidentes: pd.DataFrame,
    df_bairros: pd.DataFrame,
) -> pd.DataFrame:
    """
    Distribui acidentes por bairro via spatial join usando COORDENADA_X/Y (UTM EPSG:31983).

    Substitui a abordagem anterior de contagem global uniforme.

    :param df_acidentes: DataFrame bruto de acidentes com colunas COORDENADA_X e COORDENADA_Y
    :param df_bairros: DataFrame bruto de bairros com GEOMETRIA e NOME
    :return: DataFrame com total_acidentes por bairro
    """
    col_x       = find_column(df_acidentes, *COL_CANDIDATES_COORD_X)
    col_y       = find_column(df_acidentes, *COL_CANDIDATES_COORD_Y)
    col_geom_br = find_column(df_bairros,   *COL_CANDIDATES_GEOM)
    col_nome_br = find_column(df_bairros,   *COL_CANDIDATES_BAIRRO)

    if not all([col_x, col_y, col_geom_br, col_nome_br]):
        log.warning(
            "Spatial join de acidentes impossível — colunas: x=%s, y=%s, geom_br=%s, nome_br=%s",
            col_x, col_y, col_geom_br, col_nome_br,
        )
        return pd.DataFrame(columns=["bairro", "total_acidentes"])

    # Converte coordenadas — PBH usa vírgula como decimal em alguns exports
    df_acid = df_acidentes.copy()
    df_acid[col_x] = pd.to_numeric(
        df_acid[col_x].astype(str).str.replace(",", "."), errors="coerce"
    )
    df_acid[col_y] = pd.to_numeric(
        df_acid[col_y].astype(str).str.replace(",", "."), errors="coerce"
    )

    sem_coords = df_acid[[col_x, col_y]].isna().any(axis=1).sum()
    if sem_coords > 0:
        log.warning("Acidentes sem coordenadas válidas: %d — descartados", sem_coords)

    df_acid = df_acid.dropna(subset=[col_x, col_y])

    if df_acid.empty:
        log.warning("Nenhum acidente com coordenadas válidas — retornando vazio")
        return pd.DataFrame(columns=["bairro", "total_acidentes"])

    # Cria GeoDataFrame de pontos em UTM
    gdf_acid = gpd.GeoDataFrame(
        df_acid,
        geometry=gpd.points_from_xy(df_acid[col_x], df_acid[col_y]),
        crs=CRS_UTM,
    )

    # Carrega polígonos de bairros
    from shapely import wkt as shapely_wkt

    df_bairros_copy = df_bairros.copy()
    df_bairros_copy.columns = df_bairros_copy.columns.str.strip().str.upper()

    df_bairros_copy = df_bairros_copy.dropna(subset=[col_geom_br])
    df_bairros_copy["geometry"] = df_bairros_copy[col_geom_br].map(
        lambda w: shapely_wkt.loads(w) if isinstance(w, str) else None
    )
    df_bairros_copy = df_bairros_copy.dropna(subset=["geometry"])

    gdf_bairros = gpd.GeoDataFrame(
        df_bairros_copy[[col_nome_br, "geometry"]].rename(columns={col_nome_br: "bairro"}),
        crs=CRS_UTM,
    )

    # Spatial join: ponto dentro do polígono
    log.info("Executando spatial join: acidentes × polígonos de bairros...")
    gdf_joined = gpd.sjoin(
        gdf_acid[["geometry"]],
        gdf_bairros[["bairro", "geometry"]],
        how="left",
        predicate="within",
    )

    sem_bairro = gdf_joined["bairro"].isna().sum()
    log.info(
        "Acidentes: %d total | %d sem bairro após spatial join (%.1f%%)",
        len(gdf_joined), sem_bairro, sem_bairro / len(gdf_joined) * 100,
    )

    agg = (
        gdf_joined
        .dropna(subset=["bairro"])
        .groupby("bairro")
        .size()
        .reset_index(name="total_acidentes")
    )

    # Normaliza nome para o mesmo padrão dos outros DataFrames
    agg["bairro"] = normalize_bairro(agg["bairro"])

    log.info("Acidentes distribuídos por %d bairros", len(agg))
    return agg


def _compute_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula índice de acessibilidade normalizado (0–1) por bairro.

    Pesos: 50% embarques + 30% pontos de ônibus + 20% inverso de acidentes.
    Acidentes são invertidos: mais acidentes = menor acessibilidade percebida.

    :param df: DataFrame com métricas brutas por bairro
    :return: DataFrame com coluna indice_acessibilidade adicionada
    """
    for col in ("total_pontos_onibus", "total_embarques_dia", "total_acidentes"):
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["rank_pontos"]    = df["total_pontos_onibus"].rank(pct=True)
    df["rank_embarques"] = df["total_embarques_dia"].rank(pct=True)

    # Inverte ranking de acidentes: bairro com menos acidentes recebe rank mais alto
    df["rank_acidentes_inv"] = (1 - df["total_acidentes"].rank(pct=True))

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

    df_pontos    = load_csv(PATHS["pontos"],    "pontos_onibus")
    df_embarques = load_csv(PATHS["embarques"], "embarques")
    df_acidentes = load_csv(PATHS["acidentes"], "acidentes")
    df_bairros   = load_csv(PATHS["bairros"],   "bairros")

    agg_pontos, df_pontos_enriched, col_id_pontos = _agg_pontos_via_spatial_join(
        df_pontos, df_bairros,
    )
    agg_embarques = _agg_embarques(df_pontos_enriched, df_embarques, col_id_pontos)
    agg_acidentes = _agg_acidentes_por_bairro(df_acidentes, df_bairros)

    # Merge das três dimensões
    df = (
        agg_pontos
        .merge(agg_embarques, on="bairro", how="outer")
        .merge(agg_acidentes,  on="bairro", how="outer")
        .fillna(0)
    )

    df = _compute_index(df)

    PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    log.info("✓ acessibilidade_por_bairro.parquet salvo: %d bairros", len(df))

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = run()
    print(df.sort_values("indice_acessibilidade", ascending=False).head(10).to_string())