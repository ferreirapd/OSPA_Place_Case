"""
Composição do score final de atratividade para investimento por bairro.

Combina as três dimensões processadas:
- Atividade Econômica  → densidade e diversidade de empresas
- Acessibilidade       → mobilidade multimodal
- Qualidade Urbana     → amenidades e infraestrutura pública

Fórmula do score (0–100):
    score = 40 * norm(economico) + 35 * norm(acessibilidade) + 25 * norm(qualidade_urbana)

Os pesos refletem a lógica de negócio:
- Atividade econômica é o sinal mais direto de viabilidade
- Acessibilidade determina o alcance do negócio
- Qualidade urbana é diferencial de médio/longo prazo
"""

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

PROCESSED = Path(__file__).resolve().parents[2] / "data" / "processed"

INPUTS = {
    "economico": PROCESSED / "empresas_por_bairro.parquet",
    "acessibilidade": PROCESSED / "acessibilidade_por_bairro.parquet",
    "qualidade": PROCESSED / "qualidade_urbana_por_bairro.parquet",
    "od": PROCESSED / "matriz_od_agregada.parquet",   # opcional
}
OUT_PATH = PROCESSED / "score_final.parquet"

PESOS = {
    "economico": 0.40,
    "acessibilidade": 0.35,
    "qualidade": 0.25,
}


def _load(path: Path, label: str) -> pd.DataFrame | None:
    """
    Carrega um Parquet processado, retorna None se não existir.

    :param path: Caminho do arquivo Parquet
    :param label: Nome da fonte para logging
    :return: DataFrame ou None
    """
    if not path.exists():
        log.warning("'%s' não encontrado — dimensão ausente no score", label)
        return None
    df = pd.read_parquet(path)
    log.info("'%s' carregado: %d bairros", label, len(df))
    return df


def _min_max(series: pd.Series) -> pd.Series:
    """
    Normaliza uma série para o intervalo [0, 1] via min-max.

    :param series: Série numérica
    :return: Série normalizada
    """
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series(0.5, index=series.index)
    return (series - mn) / (mx - mn)


def run() -> pd.DataFrame:
    """
    Calcula e salva o score final de atratividade por bairro.

    :return: DataFrame com score_final (0–100) e componentes por bairro
    """
    log.info("Iniciando composição do score final")

    df_eco = _load(INPUTS["economico"], "economico")
    df_ace = _load(INPUTS["acessibilidade"], "acessibilidade")
    df_qua = _load(INPUTS["qualidade"], "qualidade")
    df_od = _load(INPUTS["od"], "matriz_od")   # pode ser None

    # Base: todos os bairros presentes em qualquer dimensão
    bairros = set()
    for df in (df_eco, df_ace, df_qua, df_od):
        if df is not None:
            bairros.update(df["bairro"].unique())

    score = pd.DataFrame({"bairro": sorted(bairros)})

    # ------------------------------------------------------------------
    # Dimensão econômica
    # ------------------------------------------------------------------
    if df_eco is not None:
        df_eco["score_eco"] = (
            _min_max(df_eco["total_empresas"]) * 0.6
            + _min_max(df_eco["diversidade_setores"]) * 0.4
        )
        score = score.merge(
            df_eco[["bairro", "score_eco", "total_empresas", "diversidade_setores",
                     *([c] if (c := "setor_dominante") in df_eco.columns else [])]],
            on="bairro", how="left",
        )
    else:
        score["score_eco"] = 0.0

    # ------------------------------------------------------------------
    # Dimensão acessibilidade
    # ------------------------------------------------------------------
    if df_ace is not None:
        score = score.merge(
            df_ace[["bairro", "indice_acessibilidade",
                     "total_pontos_onibus", "total_embarques_dia", "total_acidentes"]],
            on="bairro", how="left",
        )
        score.rename(columns={"indice_acessibilidade": "score_ace"}, inplace=True)
    else:
        score["score_ace"] = 0.0

    # ------------------------------------------------------------------
    # Dimensão qualidade urbana
    # ------------------------------------------------------------------
    if df_qua is not None:
        score = score.merge(
            df_qua[["bairro", "indice_qualidade_urbana",
                     "total_parques", "total_equipamentos_esportivos"]],
            on="bairro", how="left",
        )
        score.rename(columns={"indice_qualidade_urbana": "score_qua"}, inplace=True)
    else:
        score["score_qua"] = 0.0

    # ------------------------------------------------------------------
    # Enriquecimento opcional: fluxo O-D
    # ------------------------------------------------------------------
    if df_od is not None:
        score = score.merge(
            df_od[["bairro", "total_viagens_originadas"]],
            on="bairro", how="left",
        )
        # Incorpora fluxo O-D como bônus no componente de acessibilidade
        score["score_ace"] = (
            score["score_ace"].fillna(0) * 0.7
            + _min_max(score["total_viagens_originadas"].fillna(0)) * 0.3
        )

    # ------------------------------------------------------------------
    # Score final ponderado (0–100)
    # ------------------------------------------------------------------
    for col in ("score_eco", "score_ace", "score_qua"):
        score[col] = score[col].fillna(0)

    score["score_final"] = (
        PESOS["economico"] * score["score_eco"]
        + PESOS["acessibilidade"] * score["score_ace"]
        + PESOS["qualidade"] * score["score_qua"]
    ) * 100

    score["score_final"] = score["score_final"].round(2)
    score = score.sort_values("score_final", ascending=False).reset_index(drop=True)
    score["ranking"] = score.index + 1

    PROCESSED.mkdir(parents=True, exist_ok=True)
    score.to_parquet(OUT_PATH, index=False)
    log.info("✓ score_final.parquet salvo: %d bairros", len(score))

    return score


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = run()
    print(df[["ranking", "bairro", "score_final", "score_eco", "score_ace", "score_qua"]].head(15).to_string())
