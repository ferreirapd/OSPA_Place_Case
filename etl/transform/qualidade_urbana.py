"""
Transformação da camada de qualidade urbana.

Combina:
- Parques municipais (localização)
- Equipamentos esportivos públicos (localização)

Agrega por bairro e compõe índice de qualidade urbana normalizado (0–1).
"""

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[2] / "data"
RAW = BASE / "raw"
PROCESSED = BASE / "processed"

PATHS = {
    "parques": RAW / "parques" / "parques.csv",
    "equipamentos": RAW / "equipamentos_esportivos" / "equipamentos_esportivos.csv",
}
OUT_PATH = PROCESSED / "qualidade_urbana_por_bairro.parquet"

COL_BAIRRO = "BAIRRO"


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


def _count_por_bairro(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Conta registros por bairro e renomeia a coluna de contagem.

    :param df: DataFrame com coluna BAIRRO
    :param col_name: Nome da coluna de contagem no resultado
    :return: DataFrame com bairro e contagem
    """
    df[COL_BAIRRO] = _normalize_bairro(df[COL_BAIRRO])
    return (
        df.groupby(COL_BAIRRO)
        .size()
        .reset_index(name=col_name)
        .rename(columns={COL_BAIRRO: "bairro"})
    )


def _compute_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula índice de qualidade urbana normalizado (0–1).

    Média ponderada dos ranks percentuais de parques e equipamentos.

    :param df: DataFrame com métricas brutas
    :return: DataFrame com coluna indice_qualidade_urbana
    """
    for col in ("total_parques", "total_equipamentos_esportivos"):
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)

    df["rank_parques"] = df["total_parques"].rank(pct=True)
    df["rank_equipamentos"] = df["total_equipamentos_esportivos"].rank(pct=True)

    df["indice_qualidade_urbana"] = (
        0.50 * df["rank_parques"]
        + 0.50 * df["rank_equipamentos"]
    ).round(4)

    return df.drop(columns=["rank_parques", "rank_equipamentos"])


def run() -> pd.DataFrame:
    """
    Executa a transformação completa da camada de qualidade urbana.

    :return: DataFrame agregado por bairro com índice de qualidade urbana
    """
    log.info("Iniciando transformação: qualidade urbana")

    df_parques = _load_csv(PATHS["parques"], "parques")
    df_equip = _load_csv(PATHS["equipamentos"], "equipamentos_esportivos")

    agg_parques = _count_por_bairro(df_parques, "total_parques")
    agg_equip = _count_por_bairro(df_equip, "total_equipamentos_esportivos")

    df = agg_parques.merge(agg_equip, on="bairro", how="outer").fillna(0)
    df = _compute_index(df)

    PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    log.info("✓ qualidade_urbana_por_bairro.parquet salvo: %d bairros", len(df))

    return df


if __name__ == "__main__":
    df = run()
    print(df.sort_values("indice_qualidade_urbana", ascending=False).head(10).to_string())
