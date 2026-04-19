"""
Composição do score final de atratividade para investimento por bairro.

Fórmula do score (0-100):
    score_final = 40 × score_eco + 35 × score_ace + 25 × score_qua

onde cada score_x já está em escala 0-100 (resultado de _min_max()).
O multiplicador final é apenas pelos pesos (0 a 1) — NÃO multiplica por
100 de novo, senão o resultado ultrapassa 100.

A base da tabela final é a lista canônica extraída de atividade_economica.csv.
"""

import logging
from pathlib import Path
import pandas as pd
from etl.transform._io import load_bairros_canonicos

log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[2] / "data"
PROCESSED = BASE / "processed"
ECO_PATH  = BASE / "raw" / "atividade_economica" / "atividade_economica.csv"

INPUTS = {
    "economico":     PROCESSED / "empresas_por_bairro.parquet",
    "acessibilidade": PROCESSED / "acessibilidade_por_bairro.parquet",
    "qualidade":     PROCESSED / "qualidade_urbana_por_bairro.parquet",
    "od":            PROCESSED / "matriz_od_agregada.parquet",
}
OUT_PATH = PROCESSED / "score_final.parquet"

PESOS = {"economico": 0.40, "acessibilidade": 0.35, "qualidade": 0.25}


def _load(
    path: Path,
    label: str
) -> pd.DataFrame | None:
    """
    Carrega um Parquet processado, retornando None se não existir.

    :param path: Caminho do arquivo Parquet
    :param label: Nome da fonte para logging
    :return: DataFrame carregado ou None
    """
    if not path.exists():
        log.warning("'%s' não encontrado — dimensão ausente no score", label)
        return None
    df = pd.read_parquet(path)
    log.info("'%s' carregado: %d bairros", label, len(df))
    return df


def _min_max(series: pd.Series) -> pd.Series:
    """
    Normaliza uma série para o intervalo [0, 100] via min-max.

    :param series: Série numérica
    :return: Série normalizada no intervalo [0, 100]
    """
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series(50.0, index=series.index)
    return (series - mn) / (mx - mn) * 100


def run() -> pd.DataFrame:
    """
    Calcula e salva o score final de atratividade por bairro canônico.

    :return: DataFrame com score_final (0-100) e componentes por bairro
    """
    log.info("Iniciando composição do score final")

    # Base canônica — nomes populares de bairro extraídos da atividade econômica
    bairros_canon = load_bairros_canonicos(ECO_PATH)
    score = pd.DataFrame({"bairro": sorted(bairros_canon)})
    log.info("Base canônica: %d bairros", len(score))

    df_eco = _load(INPUTS["economico"],      "economico")
    df_ace = _load(INPUTS["acessibilidade"], "acessibilidade")
    df_qua = _load(INPUTS["qualidade"],      "qualidade")
    df_od  = _load(INPUTS["od"],             "matriz_od")

    # --- Dimensão econômica (score já em 0-100) ---
    if df_eco is not None:
        df_eco["score_eco"] = (
            _min_max(df_eco["total_empresas"]) * 0.6
            + _min_max(df_eco["diversidade_setores"]) * 0.4
        )
        cols_eco = ["bairro", "score_eco", "total_empresas", "diversidade_setores"]
        for c in ("setor_dominante", "setor_dominante_nome"):
            if c in df_eco.columns:
                cols_eco.append(c)
        score = score.merge(df_eco[cols_eco], on="bairro", how="left")
    else:
        score["score_eco"] = 0.0

    # --- Dimensão acessibilidade (indice_acessibilidade já em 0-1 → 0-100) ---
    if df_ace is not None:
        df_ace = df_ace.copy()
        # indice_acessibilidade está em 0-1 (rank percentual); converte para 0-100
        df_ace["score_ace"] = df_ace["indice_acessibilidade"] * 100
        score = score.merge(
            df_ace[[
                "bairro", "score_ace",
                "total_pontos_onibus", "total_embarques_dia", "total_acidentes",
            ]],
            on="bairro", how="left",
        )
    else:
        score["score_ace"] = 0.0

    # --- Dimensão qualidade urbana (indice em 0-1 → 0-100) ---
    if df_qua is not None:
        df_qua = df_qua.copy()
        df_qua["score_qua"] = df_qua["indice_qualidade_urbana"] * 100
        score = score.merge(
            df_qua[[
                "bairro", "score_qua",
                "total_parques", "total_equipamentos_esportivos",
            ]],
            on="bairro", how="left",
        )
    else:
        score["score_qua"] = 0.0

    # --- Enriquecimento opcional: O-D como bônus no componente acessibilidade ---
    if df_od is not None and not df_od.empty:
        score = score.merge(
            df_od[["bairro", "total_viagens_originadas"]],
            on="bairro", how="left",
        )
        score["score_ace"] = (
            score["score_ace"].fillna(0) * 0.7
            + _min_max(score["total_viagens_originadas"].fillna(0)) * 0.3
        )

    # --- Imputa média nos bairros sem dados em cada dimensão ---
    for col in ("score_eco", "score_ace", "score_qua"):
        media = score[col].mean()
        score[col] = score[col].fillna(media)

    # --- Score final ponderado (0-100) ---
    # score_eco, score_ace, score_qua já estão em 0-100.
    # Multiplicar pelos pesos (somam 1.0) mantém o resultado em 0-100.
    score["score_final"] = (
        PESOS["economico"]      * score["score_eco"]
        + PESOS["acessibilidade"] * score["score_ace"]
        + PESOS["qualidade"]      * score["score_qua"]
    ).round(2)

    score = score.sort_values("score_final", ascending=False).reset_index(drop=True)
    score["ranking"] = score.index + 1

    PROCESSED.mkdir(parents=True, exist_ok=True)
    score.to_parquet(OUT_PATH, index=False)
    log.info("✓ score_final.parquet salvo: %d bairros", len(score))
    return score


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = run()
    print(
        df[["ranking", "bairro", "score_final", "score_eco", "score_ace", "score_qua"]]
        .head(15).to_string()
    )