"""
Transformação da camada de acessibilidade multimodal.

Combina três fontes:
- Pontos de ônibus (localização)
- Estimativa de embarque por ponto (volume de passageiros)
- Acidentes de trânsito (proxy de intensidade de tráfego geral)

Agrega por bairro e produz índice de acessibilidade normalizado (0–1).
"""

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[2] / "data"
RAW = BASE / "raw"
PROCESSED = BASE / "processed"

PATHS = {
    "pontos": RAW / "pontos_onibus" / "pontos_onibus.csv",
    "embarques": RAW / "embarque_por_ponto" / "embarque_por_ponto.csv",
    "acidentes": RAW / "acidentes_transito" / "acidentes_transito.csv",
}
OUT_PATH = PROCESSED / "acessibilidade_por_bairro.parquet"

# Colunas esperadas — ajustar conforme dicionário de dados do portal
COL_BAIRRO_PONTO = "BAIRRO"
COL_ID_PONTO = "CODIGO_PONTO"
COL_EMBARQUES = "QTD_EMBARQUES_DU"   # embarques dia útil típico
COL_BAIRRO_ACIDENTE = "BAIRRO"


def _load_csv(path: Path, label: str) -> pd.DataFrame:
    """
    Lê um CSV bruto com fallback de encoding.

    :param path: Caminho para o arquivo CSV
    :param label: Nome da fonte para logging
    :return: DataFrame carregado
    """
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(path, encoding=enc, dtype=str, low_memory=False)
            df.columns = df.columns.str.strip().str.upper()
            log.info("'%s' carregado: %d linhas", label, len(df))
            return df
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Não foi possível decodificar {path}")


def _normalize_bairro(series: pd.Series) -> pd.Series:
    """
    Normaliza nomes de bairro: strip, upper, remove acentos.

    :param series: Série com nomes de bairro
    :return: Série normalizada
    """
    return (
        series.str.strip()
        .str.upper()
        .str.normalize("NFD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
    )


def _agg_pontos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Conta pontos de ônibus por bairro.

    :param df: DataFrame bruto de pontos
    :return: DataFrame com total_pontos_onibus por bairro
    """
    df[COL_BAIRRO_PONTO] = _normalize_bairro(df[COL_BAIRRO_PONTO])
    return (
        df.groupby(COL_BAIRRO_PONTO)
        .size()
        .reset_index(name="total_pontos_onibus")
        .rename(columns={COL_BAIRRO_PONTO: "bairro"})
    )


def _agg_embarques(df_pontos: pd.DataFrame, df_embarques: pd.DataFrame) -> pd.DataFrame:
    """
    Soma embarques diários por bairro, via join com a tabela de pontos.

    :param df_pontos: DataFrame bruto de pontos (com BAIRRO e CODIGO_PONTO)
    :param df_embarques: DataFrame bruto de embarques (com CODIGO_PONTO e QTD)
    :return: DataFrame com total_embarques_dia por bairro
    """
    if COL_ID_PONTO not in df_pontos.columns or COL_EMBARQUES not in df_embarques.columns:
        log.warning("Colunas de join embarques não encontradas — pulando")
        return pd.DataFrame(columns=["bairro", "total_embarques_dia"])

    df_pontos[COL_BAIRRO_PONTO] = _normalize_bairro(df_pontos[COL_BAIRRO_PONTO])
    df_embarques[COL_EMBARQUES] = pd.to_numeric(df_embarques[COL_EMBARQUES], errors="coerce").fillna(0)

    merged = df_embarques.merge(
        df_pontos[[COL_ID_PONTO, COL_BAIRRO_PONTO]],
        on=COL_ID_PONTO,
        how="left",
    )
    return (
        merged.groupby(COL_BAIRRO_PONTO)[COL_EMBARQUES]
        .sum()
        .reset_index(name="total_embarques_dia")
        .rename(columns={COL_BAIRRO_PONTO: "bairro"})
    )


def _agg_acidentes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Conta acidentes de trânsito por bairro.

    :param df: DataFrame bruto de acidentes
    :return: DataFrame com total_acidentes por bairro
    """
    df[COL_BAIRRO_ACIDENTE] = _normalize_bairro(df[COL_BAIRRO_ACIDENTE])
    return (
        df.groupby(COL_BAIRRO_ACIDENTE)
        .size()
        .reset_index(name="total_acidentes")
        .rename(columns={COL_BAIRRO_ACIDENTE: "bairro"})
    )


def _compute_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula índice de acessibilidade normalizado (0–1) por bairro.

    Fórmula: média dos ranks percentuais de embarques e pontos de ônibus.
    Acidentes entram como bônus de intensidade viária (não penaliza).

    :param df: DataFrame com métricas brutas por bairro
    :return: DataFrame com coluna indice_acessibilidade adicionada
    """
    for col in ("total_pontos_onibus", "total_embarques_dia", "total_acidentes"):
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Rank percentual (0–1) para cada dimensão
    df["rank_pontos"] = df["total_pontos_onibus"].rank(pct=True)
    df["rank_embarques"] = df["total_embarques_dia"].rank(pct=True)
    df["rank_acidentes"] = df["total_acidentes"].rank(pct=True)

    # Pesos: embarques > pontos > acidentes
    df["indice_acessibilidade"] = (
        0.50 * df["rank_embarques"]
        + 0.35 * df["rank_pontos"]
        + 0.15 * df["rank_acidentes"]
    ).round(4)

    return df.drop(columns=["rank_pontos", "rank_embarques", "rank_acidentes"])


def run() -> pd.DataFrame:
    """
    Executa a transformação completa da camada de acessibilidade.

    :return: DataFrame agregado por bairro com índice de acessibilidade
    """
    log.info("Iniciando transformação: acessibilidade")

    df_pontos = _load_csv(PATHS["pontos"], "pontos_onibus")
    df_embarques = _load_csv(PATHS["embarques"], "embarques")
    df_acidentes = _load_csv(PATHS["acidentes"], "acidentes")

    agg_pontos = _agg_pontos(df_pontos)
    agg_embarques = _agg_embarques(df_pontos, df_embarques)
    agg_acidentes = _agg_acidentes(df_acidentes)

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
    df = run()
    print(df.sort_values("indice_acessibilidade", ascending=False).head(10).to_string())
