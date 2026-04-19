"""
Transformação da base de Atividade Econômica da PBH.

Lê o CSV bruto, limpa, normaliza CNAEs e agrega métricas por bairro:
- total de empresas
- diversidade de setores (CNAE divisão)
- distribuição dos top setores
"""

import logging
from pathlib import Path
import pandas as pd
from etl.transform._io import find_column, load_csv, normalize_bairro

log = logging.getLogger(__name__)

RAW_PATH = Path(__file__).resolve().parents[2] / "data" / "raw" / "atividade_economica" / "atividade_economica.csv"
OUT_PATH = Path(__file__).resolve().parents[2] / "data" / "processed" / "empresas_por_bairro.parquet"

# Nomes possíveis das colunas — a primeira que existir é usada.
# Isso torna o módulo robusto a variações entre versões do dataset.
COL_CANDIDATES_BAIRRO = ("NOME_BAIRRO_POPULAR", "NOME_BAIRRO", "BAIRRO")
COL_CANDIDATES_CNAE = ("CNAE_PRINCIPAL", "CNAE", "COD_CNAE", "CNAE_FISCAL")
COL_CANDIDATES_SITUACAO = ("SITUACAO", "SITUACAO_CADASTRAL", "STATUS")
COL_CANDIDATES_DATA = ("DATA_INICIO_ATIVIDADE", "DATA_ABERTURA", "INICIO_ATIVIDADE")

SITUACAO_ATIVA_VALUES = ("ATIVA", "ATIVO", "1")


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa e normaliza o DataFrame bruto de atividade econômica.

    :param df: DataFrame bruto com colunas normalizadas para maiúsculas
    :return: DataFrame limpo com coluna 'BAIRRO_NORM' e 'CNAE_DIVISAO'
    """
    col_bairro = find_column(df, *COL_CANDIDATES_BAIRRO)
    col_cnae = find_column(df, *COL_CANDIDATES_CNAE)
    col_situacao = find_column(df, *COL_CANDIDATES_SITUACAO)
    col_data = find_column(df, *COL_CANDIDATES_DATA)

    if col_bairro is None:
        raise ValueError(
            f"Nenhuma coluna de bairro encontrada. Esperava alguma de: {COL_CANDIDATES_BAIRRO}. "
            f"Colunas disponíveis: {list(df.columns)[:20]}"
        )

    log.info(
        "Colunas detectadas — bairro: '%s', cnae: '%s', situacao: '%s', data: '%s'",
        col_bairro, col_cnae, col_situacao, col_data,
    )

    # Filtra empresas ativas (se a coluna existir)
    if col_situacao:
        mask_ativa = (
            df[col_situacao].fillna("").astype(str).str.strip().str.upper()
            .isin(SITUACAO_ATIVA_VALUES)
        )
        df = df[mask_ativa].copy()
        log.info("Após filtro de situação ativa: %d linhas", len(df))

    df = df.copy()
    df["BAIRRO_NORM"] = normalize_bairro(df[col_bairro])

    if col_cnae:
        df["CNAE_DIVISAO"] = df[col_cnae].fillna("").astype(str).str[:2]

    if col_data:
        df["DATA_NORM"] = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True)

    return df[df["BAIRRO_NORM"] != ""]


def _aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega métricas de atividade econômica por bairro.

    :param df: DataFrame limpo com 'BAIRRO_NORM'
    :return: DataFrame agregado com uma linha por bairro
    """
    agg_kwargs: dict[str, tuple[str, str]] = {"total_empresas": ("BAIRRO_NORM", "count")}
    if "CNAE_DIVISAO" in df.columns:
        agg_kwargs["diversidade_setores"] = ("CNAE_DIVISAO", "nunique")

    agg = (
        df.groupby("BAIRRO_NORM")
        .agg(**agg_kwargs)
        .reset_index()
        .rename(columns={"BAIRRO_NORM": "bairro"})
    )

    # Setor dominante por bairro (apenas se CNAE disponível)
    if "CNAE_DIVISAO" in df.columns:
        setor_dominante = (
            df.groupby(["BAIRRO_NORM", "CNAE_DIVISAO"])
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
            .drop_duplicates(subset="BAIRRO_NORM")
            .rename(columns={"BAIRRO_NORM": "bairro", "CNAE_DIVISAO": "setor_dominante"})
            [["bairro", "setor_dominante"]]
        )
        agg = agg.merge(setor_dominante, on="bairro", how="left")

    return agg.sort_values("total_empresas", ascending=False)


def run() -> pd.DataFrame:
    """
    Executa a transformação completa da base de atividade econômica.

    :return: DataFrame agregado por bairro, salvo em processed/
    """
    log.info("Iniciando transformação: atividade econômica")
    df_raw = load_csv(RAW_PATH, "atividade_economica")
    df_clean = _clean(df_raw)
    df_agg = _aggregate(df_clean)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_agg.to_parquet(OUT_PATH, index=False)
    log.info("✓ empresas_por_bairro.parquet salvo: %d bairros", len(df_agg))

    return df_agg


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = run()
    print(df.head(10).to_string())