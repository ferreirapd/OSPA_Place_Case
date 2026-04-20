"""
Transformação da camada de qualidade urbana.

Combina:
- Parques municipais (nome do bairro + localização)
- Equipamentos esportivos públicos (nome do bairro + localização)

Nomes de bairro são padronizados contra a tabela canônica de NOME_BAIRRO.
"""

import logging
from pathlib import Path
import pandas as pd
from etl.transform._io import find_column, load_bairros_canonicos, load_csv, match_bairro_canonico


log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[2]/"data"
RAW = BASE/"raw"
PROCESSED = BASE/"processed"
PATHS = {
    "parques": RAW/"parques"/"parques.csv",
    "equipamentos": RAW/"equipamentos_esportivos"/"equipamentos_esportivos.csv",
    "eco": RAW/"atividade_economica"/"atividade_economica.csv",
}
OUT_PATH = PROCESSED/"qualidade_urbana_por_bairro.parquet"
ETAPA = "qualidade_urbana"
COL_CANDIDATES_BAIRRO = ("NOME_BAIRRO_POPULAR", "NOME_BAIRRO", "BAIRRO")


def _count_por_bairro(
    df: pd.DataFrame,
    col_name: str,
    canonicos: list[str],
    fonte: str,
) -> pd.DataFrame:
    """
    Conta registros por bairro canônico via fuzzy match.

    :param df: DataFrame bruto
    :param col_name: Nome da coluna de contagem no resultado
    :param canonicos: Lista de bairros canônicos
    :param fonte: Nome do dataset (para auditoria em EXCLUSOES)
    :return: DataFrame com bairro canônico e contagem
    """
    col_bairro = find_column(df, *COL_CANDIDATES_BAIRRO)
    if col_bairro is None:
        log.warning(
            "Coluna de bairro não encontrada em %s. Candidatos: %s",
            fonte, COL_CANDIDATES_BAIRRO,
        )
        return pd.DataFrame(columns=["bairro", col_name])

    df = df.copy()
    df["BAIRRO_CANON"] = match_bairro_canonico(
        df[col_bairro], canonicos, etapa=ETAPA, fonte=fonte,
    )

    n_sem = df["BAIRRO_CANON"].isna().sum()
    if n_sem:
        log.warning("%d registros de %s sem bairro canônico - descartados", n_sem, fonte)

    return (
        df[df["BAIRRO_CANON"].notna()]
        .groupby("BAIRRO_CANON")
        .size()
        .reset_index(name=col_name)
        .rename(columns={"BAIRRO_CANON": "bairro"})
    )


def _compute_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula índice de qualidade urbana (0-1) como média dos ranks percentuais.

    :param df: DataFrame com métricas brutas por bairro
    :return: DataFrame com coluna indice_qualidade_urbana adicionada
    """
    for col in ("total_parques", "total_equipamentos_esportivos"):
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    rank_parques = df["total_parques"].rank(pct=True)
    rank_equipamentos = df["total_equipamentos_esportivos"].rank(pct=True)
    df["indice_qualidade_urbana"] = (
        0.50 * rank_parques + 0.50 * rank_equipamentos
    ).round(4)
    return df


def run() -> pd.DataFrame:
    """
    Executa a transformação completa da camada de qualidade urbana.

    :return: DataFrame agregado por bairro com índice de qualidade urbana
    """
    log.info("Iniciando transformação: %s", ETAPA)

    canonicos = load_bairros_canonicos(PATHS["eco"])
    df_parques = load_csv(PATHS["parques"], "parques")
    df_equip = load_csv(PATHS["equipamentos"], "equipamentos_esportivos")

    agg_parques = _count_por_bairro(df_parques, "total_parques", canonicos, "parques")
    agg_equip = _count_por_bairro(df_equip, "total_equipamentos_esportivos", canonicos, "equipamentos_esportivos")

    df = agg_parques.merge(agg_equip, on="bairro", how="outer").fillna(0)
    df = _compute_index(df)

    PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    log.info("Salvo: %s (%d bairros)", OUT_PATH.name, len(df))
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run().sort_values("indice_qualidade_urbana", ascending=False).head(10).to_string())