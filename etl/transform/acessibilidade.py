"""
Transformação da camada de acessibilidade multimodal.

Combina três fontes:
- Pontos de ônibus (WKT -> spatial join com bairros)
- Embarque por ponto (join por ID de ponto)
- Acidentes de trânsito (COORDENADA_X/Y em UTM -> spatial join com bairros)

Nomes são padronizados contra a tabela canônica de NOME_BAIRRO.
"""

import logging
from pathlib import Path
import pandas as pd
from etl.transform._io import find_column, load_bairros_canonicos, load_csv
from etl.transform._spatial import aggregate_por_bairro_canonico, sjoin_pontos_wkt, sjoin_pontos_xy


log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[2]/"data"
RAW = BASE/"raw"
PROCESSED = BASE/"processed"
PATHS = {
    "pontos": RAW/"pontos_onibus"/"pontos_onibus.csv",
    "embarques": RAW/"embarque_por_ponto"/"embarque_por_ponto.csv",
    "acidentes": RAW/"acidentes_transito"/"acidentes_transito.csv",
    "bairros": RAW/"bairros"/"bairros.csv",
    "eco": RAW/"atividade_economica"/"atividade_economica.csv",
}
OUT_PATH = PROCESSED/"acessibilidade_por_bairro.parquet"
ETAPA = "acessibilidade"
COL_CANDIDATES_BAIRRO = ("NOME_BAIRRO_POPULAR", "NOME_BAIRRO", "NOME", "BAIRRO")
COL_CANDIDATES_ID_PONTO = (
    "IDENTIFICADOR_PONTO_ONIBUS", "ID_PONTO_ONIBUS_LINHA",
    "SIU", "CODIGO_PONTO", "COD_PONTO", "ID_PONTO", "COD_PONTO_ONIBUS", "PONTO",
)
COL_CANDIDATES_EMBARQUES = (
    "TOTAL GERAL", "QTD_EMBARQUES_DU", "QTD_EMBARQUES", "EMBARQUES_DU",
    "EMBARQUES", "TOTAL_EMBARQUES", "NUM_EMBARQUES", "PASSAGEIROS",
)
COL_CANDIDATES_GEOM = ("GEOMETRIA", "GEOM", "WKT")
COL_CANDIDATES_COORD_X = ("COORDENADA_X", "X", "COORD_X", "LON", "LONGITUDE")
COL_CANDIDATES_COORD_Y = ("COORDENADA_Y", "Y", "COORD_Y", "LAT", "LATITUDE")


def _agg_pontos_onibus(
    df_pontos: pd.DataFrame,
    df_bairros: pd.DataFrame,
    canonicos: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    """
    Spatial join pontos de ônibus -> bairro canônico e contagem por bairro.

    :param df_pontos: DataFrame de pontos com coluna de geometria WKT
    :param df_bairros: DataFrame de bairros com geometria WKT e nome
    :param canonicos: Lista de bairros canônicos
    :return: (agregação por bairro, df enriquecido, nome da coluna de ID de ponto)
    """
    col_geom_pt = find_column(df_pontos, *COL_CANDIDATES_GEOM)
    col_geom_br = find_column(df_bairros, *COL_CANDIDATES_GEOM)
    col_nome_br = find_column(df_bairros, *COL_CANDIDATES_BAIRRO)
    col_id = find_column(df_pontos, *COL_CANDIDATES_ID_PONTO)

    if not all([col_geom_pt, col_geom_br, col_nome_br]):
        log.warning("Spatial join de pontos impossível - colunas faltando")
        return pd.DataFrame(columns=["bairro", "total_pontos_onibus"]), df_pontos, col_id

    df_sjoin = sjoin_pontos_wkt(
        df_pontos, df_bairros,
        col_geom_pontos=col_geom_pt,
        col_geom_bairros=col_geom_br,
        col_nome_bairro=col_nome_br,
    )
    agg, df_enriched = aggregate_por_bairro_canonico(
        df_sjoin, canonicos,
        etapa=ETAPA, fonte="pontos_onibus",
        col_metrica="total_pontos_onibus",
    )
    return agg, df_enriched, col_id


def _agg_embarques(
    df_pontos_enriched: pd.DataFrame,
    df_embarques: pd.DataFrame,
    col_id_pontos: str | None,
) -> pd.DataFrame:
    """
    Soma embarques diários por bairro via join com pontos já enriquecidos.

    :param df_pontos_enriched: Pontos com coluna BAIRRO_CANON
    :param df_embarques: DataFrame bruto de embarques
    :param col_id_pontos: Nome da coluna de ID no DataFrame de pontos
    :return: DataFrame com total_embarques_dia por bairro
    """
    col_id_emb = find_column(df_embarques, *COL_CANDIDATES_ID_PONTO)
    col_qtd = find_column(df_embarques, *COL_CANDIDATES_EMBARQUES)

    if not all([col_id_pontos, col_id_emb, col_qtd]):
        log.warning(
            "Join embarques abortado - ids=%s/%s, qtd=%s",
            col_id_pontos, col_id_emb, col_qtd,
        )
        return pd.DataFrame(columns=["bairro", "total_embarques_dia"])

    df_emb = df_embarques.copy()
    df_emb[col_qtd] = pd.to_numeric(df_emb[col_qtd], errors="coerce").fillna(0)

    pontos_lookup = (
        df_pontos_enriched[[col_id_pontos, "BAIRRO_CANON"]]
        .rename(columns={col_id_pontos: col_id_emb})
        .drop_duplicates(subset=col_id_emb)
    )
    merged = df_emb.merge(pontos_lookup, on=col_id_emb, how="left")

    return (
        merged.dropna(subset=["BAIRRO_CANON"])
        .groupby("BAIRRO_CANON")[col_qtd]
        .sum()
        .reset_index(name="total_embarques_dia")
        .rename(columns={"BAIRRO_CANON": "bairro"})
    )


def _agg_acidentes(
    df_acidentes: pd.DataFrame,
    df_bairros: pd.DataFrame,
    canonicos: list[str],
) -> pd.DataFrame:
    """
    Spatial join acidentes (X/Y UTM) -> bairro canônico e contagem por bairro.

    :param df_acidentes: DataFrame com COORDENADA_X/Y em UTM
    :param df_bairros: DataFrame de bairros com geometria WKT e nome
    :param canonicos: Lista de bairros canônicos
    :return: DataFrame com total_acidentes por bairro canônico
    """
    col_x = find_column(df_acidentes, *COL_CANDIDATES_COORD_X)
    col_y = find_column(df_acidentes, *COL_CANDIDATES_COORD_Y)
    col_geom_br = find_column(df_bairros, *COL_CANDIDATES_GEOM)
    col_nome_br = find_column(df_bairros, *COL_CANDIDATES_BAIRRO)

    if not all([col_x, col_y, col_geom_br, col_nome_br]):
        log.warning("Spatial join de acidentes impossível - colunas faltando")
        return pd.DataFrame(columns=["bairro", "total_acidentes"])

    df_sjoin = sjoin_pontos_xy(
        df_acidentes, df_bairros,
        col_x=col_x, col_y=col_y,
        col_geom_bairros=col_geom_br,
        col_nome_bairro=col_nome_br,
    )
    agg, _ = aggregate_por_bairro_canonico(
        df_sjoin, canonicos,
        etapa=ETAPA, fonte="acidentes",
        col_metrica="total_acidentes",
    )
    return agg


def _compute_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula índice de acessibilidade (0-1) por bairro.
    Pesos: 50% embarques + 30% pontos + 20% inverso de acidentes.
    Acidentes invertidos: mais acidentes -> menor acessibilidade percebida.

    :param df: DataFrame com métricas brutas por bairro
    :return: DataFrame com coluna indice_acessibilidade adicionada
    """
    for col in ("total_pontos_onibus", "total_embarques_dia", "total_acidentes"):
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    rank_pontos = df["total_pontos_onibus"].rank(pct=True)
    rank_embarques = df["total_embarques_dia"].rank(pct=True)
    rank_acidentes_inv = 1 - df["total_acidentes"].rank(pct=True)

    df["indice_acessibilidade"] = (
        0.50 * rank_embarques + 0.30 * rank_pontos + 0.20 * rank_acidentes_inv
    ).round(4)
    return df


def run() -> pd.DataFrame:
    """
    Executa a transformação completa da camada de acessibilidade.

    :return: DataFrame agregado por bairro com índice de acessibilidade
    """
    log.info("Iniciando transformação: %s", ETAPA)

    canonicos = load_bairros_canonicos(PATHS["eco"])
    df_pontos = load_csv(PATHS["pontos"], "pontos_onibus")
    df_embarques = load_csv(PATHS["embarques"], "embarques")
    df_acidentes = load_csv(PATHS["acidentes"], "acidentes")
    df_bairros = load_csv(PATHS["bairros"], "bairros")

    agg_pontos, df_enriched, col_id = _agg_pontos_onibus(df_pontos, df_bairros, canonicos)
    agg_embarques = _agg_embarques(df_enriched, df_embarques, col_id)
    agg_acidentes = _agg_acidentes(df_acidentes, df_bairros, canonicos)

    df = (
        agg_pontos
        .merge(agg_embarques, on="bairro", how="outer")
        .merge(agg_acidentes, on="bairro", how="outer")
        .fillna(0)
    )
    df = _compute_index(df)

    PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    log.info("Salvo: %s (%d bairros)", OUT_PATH.name, len(df))
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run().sort_values("indice_acessibilidade", ascending=False).head(10).to_string())