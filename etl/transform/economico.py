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

log = logging.getLogger(__name__)

RAW_PATH = Path(__file__).resolve().parents[2] / "data" / "raw" / "atividade_economica" / "atividade_economica.csv"
OUT_PATH = Path(__file__).resolve().parents[2] / "data" / "processed" / "empresas_por_bairro.parquet"

# Colunas esperadas no CSV bruto (podem variar — ajustar conforme dicionário de dados)
COL_BAIRRO = "BAIRRO"
COL_CNAE = "CNAE_PRINCIPAL"
COL_SITUACAO = "SITUACAO"
COL_DATA_ABERTURA = "DATA_INICIO_ATIVIDADE"

SITUACAO_ATIVA = "ATIVA"


def _load_raw(path: Path) -> pd.DataFrame:
    """
    Lê o CSV bruto de atividade econômica com tratamento de encoding.

    :param path: Caminho para o arquivo CSV bruto
    :return: DataFrame com os dados brutos carregados
    """
    for encoding in ("utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(path, encoding=encoding, dtype=str, low_memory=False)
            log.info("CSV carregado com encoding '%s': %d linhas", encoding, len(df))
            return df
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Não foi possível decodificar {path}")


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa e normaliza o DataFrame bruto de atividade econômica.

    :param df: DataFrame bruto
    :return: DataFrame limpo com colunas padronizadas
    """
    df.columns = df.columns.str.strip().str.upper()

    # Mantém apenas empresas ativas
    if COL_SITUACAO in df.columns:
        df = df[df[COL_SITUACAO].str.upper().str.strip() == SITUACAO_ATIVA].copy()

    # Normaliza bairro
    df[COL_BAIRRO] = (
        df[COL_BAIRRO]
        .str.strip()
        .str.upper()
        .str.normalize("NFD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
    )

    # Extrai divisão CNAE (2 primeiros dígitos = setor macro)
    if COL_CNAE in df.columns:
        df["CNAE_DIVISAO"] = df[COL_CNAE].str[:2]

    # Converte data de abertura
    if COL_DATA_ABERTURA in df.columns:
        df[COL_DATA_ABERTURA] = pd.to_datetime(df[COL_DATA_ABERTURA], errors="coerce", dayfirst=True)

    return df.dropna(subset=[COL_BAIRRO])


def _aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega métricas de atividade econômica por bairro.

    :param df: DataFrame limpo
    :return: DataFrame agregado com uma linha por bairro
    """
    agg = (
        df.groupby(COL_BAIRRO)
        .agg(
            total_empresas=(COL_BAIRRO, "count"),
            diversidade_setores=("CNAE_DIVISAO", "nunique"),
        )
        .reset_index()
        .rename(columns={COL_BAIRRO: "bairro"})
    )

    # Setor dominante por bairro
    if "CNAE_DIVISAO" in df.columns:
        setor_dominante = (
            df.groupby([COL_BAIRRO, "CNAE_DIVISAO"])
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
            .drop_duplicates(subset=COL_BAIRRO)
            .rename(columns={COL_BAIRRO: "bairro", "CNAE_DIVISAO": "setor_dominante"})
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
    df_raw = _load_raw(RAW_PATH)
    df_clean = _clean(df_raw)
    df_agg = _aggregate(df_clean)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_agg.to_parquet(OUT_PATH, index=False)
    log.info("✓ empresas_por_bairro.parquet salvo: %d bairros", len(df_agg))

    return df_agg


if __name__ == "__main__":
    df = run()
    print(df.head(10).to_string())
