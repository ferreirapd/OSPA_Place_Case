"""
Transformação da camada de qualidade urbana.

Combina:
- Parques municipais (localização + bairro)
- Equipamentos esportivos públicos (localização + bairro)

Agrega por bairro e compõe índice de qualidade urbana normalizado (0–1).
"""

import logging
from pathlib import Path
import pandas as pd
from etl.transform._io import find_column, load_csv, normalize_bairro

log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[2] / "data"
RAW = BASE / "raw"
PROCESSED = BASE / "processed"

PATHS = {
    "parques": RAW / "parques" / "parques.csv",
    "equipamentos": RAW / "equipamentos_esportivos" / "equipamentos_esportivos.csv",
}
OUT_PATH = PROCESSED / "qualidade_urbana_por_bairro.parquet"

COL_CANDIDATES_BAIRRO = ("NOME_BAIRRO_POPULAR", "NOME_BAIRRO", "BAIRRO")


def _count_por_bairro(
    df: pd.DataFrame,
    col_name: str
) -> pd.DataFrame:
    """
    Conta registros por bairro, com detecção flexível de coluna.

    :param df: DataFrame bruto
    :param col_name: Nome da coluna de contagem no resultado
    :return: DataFrame com bairro e contagem
    """
    col_bairro = find_column(df, *COL_CANDIDATES_BAIRRO)
    if col_bairro is None:
        log.warning("Coluna de bairro não encontrada (cols=%s) — retornando vazio", list(df.columns)[:10])
        return pd.DataFrame(columns=["bairro", col_name])

    df = df.copy()
    df["BAIRRO_NORM"] = normalize_bairro(df[col_bairro])

    return (
        df[df["BAIRRO_NORM"] != ""]
        .groupby("BAIRRO_NORM")
        .size()
        .reset_index(name=col_name)
        .rename(columns={"BAIRRO_NORM": "bairro"})
    )


def _compute_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula índice de qualidade urbana normalizado (0–1).

    Média dos ranks percentuais de parques e equipamentos esportivos.

    :param df: DataFrame com métricas brutas
    :return: DataFrame com coluna indice_qualidade_urbana
    """
    for col in ("total_parques", "total_equipamentos_esportivos"):
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["rank_parques"] = df["total_parques"].rank(pct=True)
    df["rank_equipamentos"] = df["total_equipamentos_esportivos"].rank(pct=True)

    df["indice_qualidade_urbana"] = (
        0.50 * df["rank_parques"] + 0.50 * df["rank_equipamentos"]
    ).round(4)

    return df.drop(columns=["rank_parques", "rank_equipamentos"])


def run() -> pd.DataFrame:
    """
    Executa a transformação completa da camada de qualidade urbana.

    :return: DataFrame agregado por bairro com índice de qualidade urbana
    """
    log.info("Iniciando transformação: qualidade urbana")

    df_parques = load_csv(PATHS["parques"], "parques")
    df_equip = load_csv(PATHS["equipamentos"], "equipamentos_esportivos")

    agg_parques = _count_por_bairro(df_parques, "total_parques")
    agg_equip = _count_por_bairro(df_equip, "total_equipamentos_esportivos")

    df = agg_parques.merge(agg_equip, on="bairro", how="outer").fillna(0)
    df = _compute_index(df)

    PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    log.info("✓ qualidade_urbana_por_bairro.parquet salvo: %d bairros", len(df))

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = run()
    print(df.sort_values("indice_qualidade_urbana", ascending=False).head(10).to_string())