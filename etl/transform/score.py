"""
Composição do score final de atratividade por bairro.

Fórmula (0-100):
    score_final = 40% score_eco + 35% score_ace + 25% score_qua

Base canônica: todos os nomes populares extraídos de atividade_economica.csv.
Bairros sem dado em alguma dimensão recebem a média dessa dimensão.
"""

import logging
from pathlib import Path
import pandas as pd
from etl.transform._io import load_bairros_canonicos


log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[2]/"data"
PROCESSED = BASE/"processed"
ECO_PATH = BASE/"raw"/"atividade_economica"/"atividade_economica.csv"
INPUTS = {
    "economico": PROCESSED/"empresas_por_bairro.parquet",
    "acessibilidade": PROCESSED/"acessibilidade_por_bairro.parquet",
    "qualidade": PROCESSED/"qualidade_urbana_por_bairro.parquet",
    "od": PROCESSED/"matriz_od_agregada.parquet",
}
OUT_PATH = PROCESSED/"score_final.parquet"
PESOS = {"economico": 0.40, "acessibilidade": 0.35, "qualidade": 0.25}


def _load(
    path: Path,
    label: str
) -> pd.DataFrame | None:
    """
    Carrega Parquet processado, retornando None se não existir.

    :param path: Caminho do arquivo Parquet
    :param label: Nome da fonte para logging
    :return: DataFrame ou None
    """
    if not path.exists():
        log.warning("%s não encontrado - dimensão ausente no score", label)
        return None
    df = pd.read_parquet(path)
    log.info("%s carregado: %d bairros", label, len(df))
    return df


def _min_max(series: pd.Series) -> pd.Series:
    """
    Normaliza série para [0, 100] via min-max.

    :param series: Série numérica
    :return: Série normalizada em [0, 100]
    """
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series(50.0, index=series.index)
    return (series - mn) / (mx - mn) * 100


def _merge_economico(
    score: pd.DataFrame,
    df_eco: pd.DataFrame | None
) -> pd.DataFrame:
    """
    Incorpora dimensão econômica ao score.

    :param score: DataFrame base com coluna bairro
    :param df_eco: DataFrame de empresas por bairro, ou None
    :return: score enriquecido com score_eco e colunas de detalhe
    """
    if df_eco is None:
        score["score_eco"] = 0.0
        return score

    df_eco = df_eco.copy()
    df_eco["score_eco"] = (
        _min_max(df_eco["total_empresas"]) * 0.6
        + _min_max(df_eco["diversidade_setores"]) * 0.4
    )
    cols = ["bairro", "score_eco", "total_empresas", "diversidade_setores"]
    cols += [c for c in ("setor_dominante", "setor_dominante_nome") if c in df_eco.columns]
    return score.merge(df_eco[cols], on="bairro", how="left")


def _merge_acessibilidade(
    score: pd.DataFrame,
    df_ace: pd.DataFrame | None
) -> pd.DataFrame:
    """
    Incorpora dimensão acessibilidade ao score.

    :param score: DataFrame base
    :param df_ace: DataFrame de acessibilidade por bairro, ou None
    :return: score enriquecido com score_ace e colunas de detalhe
    """
    if df_ace is None:
        score["score_ace"] = 0.0
        return score

    df_ace = df_ace.copy()
    # indice_acessibilidade vem em [0,1]; converte para [0,100]
    df_ace["score_ace"] = df_ace["indice_acessibilidade"] * 100
    cols = [
        "bairro", "score_ace",
        "total_pontos_onibus", "total_embarques_dia", "total_acidentes",
    ]
    return score.merge(df_ace[cols], on="bairro", how="left")


def _merge_qualidade(
    score: pd.DataFrame,
    df_qua: pd.DataFrame | None
) -> pd.DataFrame:
    """
    Incorpora dimensão qualidade urbana ao score.

    :param score: DataFrame base
    :param df_qua: DataFrame de qualidade urbana por bairro, ou None
    :return: score enriquecido com score_qua e colunas de detalhe
    """
    if df_qua is None:
        score["score_qua"] = 0.0
        return score

    df_qua = df_qua.copy()
    df_qua["score_qua"] = df_qua["indice_qualidade_urbana"] * 100
    cols = ["bairro", "score_qua", "total_parques", "total_equipamentos_esportivos"]
    return score.merge(df_qua[cols], on="bairro", how="left")


def _aplicar_bonus_od(
    score: pd.DataFrame,
    df_od: pd.DataFrame | None
) -> pd.DataFrame:
    """
    Aplica fluxo de matriz O-D como bônus (30%) no componente de acessibilidade.

    :param score: DataFrame com score_ace já populado
    :param df_od: DataFrame de matriz O-D agregada, ou None
    :return: score com score_ace ajustado e total_viagens_originadas adicionado
    """
    if df_od is None or df_od.empty:
        return score

    score = score.merge(
        df_od[["bairro", "total_viagens_originadas"]], on="bairro", how="left",
    )
    score["score_ace"] = (
        score["score_ace"].fillna(0) * 0.7
        + _min_max(score["total_viagens_originadas"].fillna(0)) * 0.3
    )
    return score


def run() -> pd.DataFrame:
    """
    Calcula e salva o score final de atratividade por bairro canônico.

    :return: DataFrame com score_final (0-100) e componentes por bairro
    """
    log.info("Iniciando composição do score final")

    bairros_canon = load_bairros_canonicos(ECO_PATH)
    score = pd.DataFrame({"bairro": sorted(bairros_canon)})
    log.info("Base canônica: %d bairros", len(score))

    score = _merge_economico(score, _load(INPUTS["economico"], "economico"))
    score = _merge_acessibilidade(score, _load(INPUTS["acessibilidade"], "acessibilidade"))
    score = _merge_qualidade(score, _load(INPUTS["qualidade"], "qualidade"))
    score = _aplicar_bonus_od(score, _load(INPUTS["od"], "matriz_od"))

    # Imputa média nos bairros sem dado em cada dimensão
    for col in ("score_eco", "score_ace", "score_qua"):
        score[col] = score[col].fillna(score[col].mean())

    # score_eco/ace/qua já em [0,100]; pesos somam 1.0 => mantém range
    score["score_final"] = (
        PESOS["economico"] * score["score_eco"]
        + PESOS["acessibilidade"] * score["score_ace"]
        + PESOS["qualidade"] * score["score_qua"]
    ).round(2)

    score = score.sort_values("score_final", ascending=False).reset_index(drop=True)
    score["ranking"] = score.index + 1

    PROCESSED.mkdir(parents=True, exist_ok=True)
    score.to_parquet(OUT_PATH, index=False)
    log.info("Salvo: %s (%d bairros)", OUT_PATH.name, len(score))
    return score


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = run()
    print(
        df[["ranking", "bairro", "score_final", "score_eco", "score_ace", "score_qua"]]
        .head(15).to_string()
    )