"""
Transformação da base de Atividade Econômica da PBH.

Filtra empresas com alvará ativo, extrai divisão CNAE e agrega por bairro:
- total_empresas
- diversidade_setores
- setor_dominante (código) + setor_dominante_nome (descrição)
"""

import logging
from pathlib import Path
import pandas as pd
from etl.transform._io import EXCLUSOES, find_column, load_bairros_canonicos, load_csv, match_bairro_canonico


log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[2]/"data"
RAW_PATH = BASE/"raw"/"atividade_economica"/"atividade_economica.csv"
OUT_PATH = BASE/"processed"/"empresas_por_bairro.parquet"
ETAPA = "economico"
FONTE = "atividade_economica"
COL_CANDIDATES_BAIRRO = ("NOME_BAIRRO_POPULAR", "NOME_BAIRRO", "BAIRRO")
COL_CANDIDATES_CNAE = ("CNAE_PRINCIPAL", "CNAE", "COD_CNAE", "CNAE_FISCAL")
COL_CANDIDATES_ALVARA = ("IND_POSSUI_ALVARA", "POSSUI_ALVARA", "ALVARA")
COL_CANDIDATES_DATA = ("DATA_INICIO_ATIVIDADE", "DATA_ABERTURA", "INICIO_ATIVIDADE")
ALVARA_ATIVO_VALUES = ("S", "SIM", "1", "Y", "YES")

# Mapeamento CNAE divisão (2 dígitos) → nome macro do setor. Fonte: IBGE CNAE 2.3.
CNAE_LABELS: dict[str, str] = {
    "01": "Agricultura", "05": "Mineração", "10": "Alimentos",
    "13": "Têxtil", "19": "Combustíveis", "22": "Borracha/Plástico",
    "25": "Metal", "26": "Eletrônicos", "28": "Máquinas",
    "33": "Manutenção", "35": "Energia", "36": "Saneamento",
    "41": "Construção", "45": "Veículos", "46": "Comércio Atacado",
    "47": "Comércio Varejo", "49": "Transporte Terrestre",
    "52": "Armazenagem", "55": "Hospedagem", "56": "Alimentação",
    "58": "Editoras", "61": "Telecomunicações", "62": "TI/Software",
    "64": "Financeiro", "68": "Imobiliário", "69": "Jurídico",
    "70": "Consultoria", "71": "Arquitetura/Eng.", "72": "P&D",
    "73": "Publicidade", "74": "Design", "75": "Veterinário",
    "77": "Locação", "78": "RH/Seleção", "79": "Turismo",
    "80": "Segurança", "81": "Facilities", "82": "Apoio Administrativo",
    "84": "Administração Pública", "85": "Educação",
    "86": "Saúde", "87": "Assistência Social", "90": "Artes/Cultura",
    "93": "Esporte/Lazer", "95": "Reparação", "96": "Serviços Pessoais",
}


def _filtrar_ativos(
    df: pd.DataFrame,
    col_alvara: str | None
) -> pd.DataFrame:
    """
    Filtra apenas empresas com alvará ativo.

    :param df: DataFrame bruto
    :param col_alvara: Nome da coluna de alvará (None se ausente)
    :return: DataFrame filtrado; retorna df original se coluna ausente
    """
    if col_alvara is None:
        log.warning("Coluna de alvará ausente - total inclui empresas inativas")
        return df

    n_antes = len(df)
    mask = (
        df[col_alvara].fillna("").astype(str).str.strip().str.upper()
        .isin(ALVARA_ATIVO_VALUES)
    )
    df = df[mask].copy()
    log.info(
        "Filtro alvará ativo: %d -> %d linhas (%.1f%% ativas)",
        n_antes, len(df), len(df) / n_antes * 100 if n_antes else 0,
    )
    return df


def _clean(
    df: pd.DataFrame,
    canonicos: list[str]
) -> pd.DataFrame:
    """
    Filtra, normaliza e padroniza o DataFrame bruto de atividade econômica.

    :param df: DataFrame bruto com colunas em MAIÚSCULAS
    :param canonicos: Lista de bairros canônicos
    :return: DataFrame com BAIRRO_CANON e CNAE_DIVISAO
    :raises ValueError: Se coluna de bairro não for encontrada
    """
    col_bairro = find_column(df, *COL_CANDIDATES_BAIRRO)
    col_cnae = find_column(df, *COL_CANDIDATES_CNAE)
    col_alvara = find_column(df, *COL_CANDIDATES_ALVARA)
    col_data = find_column(df, *COL_CANDIDATES_DATA)

    if col_bairro is None:
        raise ValueError(
            f"Coluna de bairro não encontrada. Esperado: {COL_CANDIDATES_BAIRRO}. "
            f"Disponíveis: {list(df.columns)[:20]}"
        )
    log.info(
        "Colunas detectadas: bairro=%s, cnae=%s, alvara=%s, data=%s",
        col_bairro, col_cnae, col_alvara, col_data,
    )

    df = _filtrar_ativos(df, col_alvara)
    df = df.copy()
    df["BAIRRO_CANON"] = match_bairro_canonico(
        df[col_bairro], canonicos, etapa=ETAPA, fonte=FONTE,
    )

    n_sem = df["BAIRRO_CANON"].isna().sum()
    if n_sem:
        log.warning("%d empresas sem bairro canônico - descartadas", n_sem)
    df = df[df["BAIRRO_CANON"].notna()].copy()

    if col_cnae:
        df["CNAE_DIVISAO"] = df[col_cnae].fillna("").astype(str).str[:2]
    if col_data:
        df["DATA_NORM"] = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True)

    return df


def _aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega métricas de atividade econômica por bairro canônico.

    :param df: DataFrame limpo com BAIRRO_CANON e opcionalmente CNAE_DIVISAO
    :return: DataFrame agregado ordenado por total_empresas decrescente
    """
    agg_kwargs: dict[str, tuple[str, str]] = {
        "total_empresas": ("BAIRRO_CANON", "count"),
    }
    if "CNAE_DIVISAO" in df.columns:
        agg_kwargs["diversidade_setores"] = ("CNAE_DIVISAO", "nunique")

    agg = (
        df.groupby("BAIRRO_CANON")
        .agg(**agg_kwargs)
        .reset_index()
        .rename(columns={"BAIRRO_CANON": "bairro"})
    )

    if "CNAE_DIVISAO" in df.columns:
        setor_dom = (
            df.groupby(["BAIRRO_CANON", "CNAE_DIVISAO"])
            .size()
            .reset_index(name="cnt")
            .sort_values("cnt", ascending=False)
            .drop_duplicates(subset="BAIRRO_CANON")
            .rename(columns={"BAIRRO_CANON": "bairro", "CNAE_DIVISAO": "setor_dominante"})
            [["bairro", "setor_dominante"]]
        )
        setor_dom["setor_dominante_nome"] = (
            setor_dom["setor_dominante"]
            .map(CNAE_LABELS)
            .fillna("Setor " + setor_dom["setor_dominante"].astype(str))
        )
        agg = agg.merge(setor_dom, on="bairro", how="left")

    return agg.sort_values("total_empresas", ascending=False).reset_index(drop=True)


def run() -> pd.DataFrame:
    """
    Executa a transformação completa da base de atividade econômica.

    :return: DataFrame agregado por bairro, persistido em data/processed/
    """
    log.info("Iniciando transformação: %s", ETAPA)
    canonicos = load_bairros_canonicos(RAW_PATH)
    df_clean = _clean(load_csv(RAW_PATH, FONTE), canonicos)
    df_agg = _aggregate(df_clean)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_agg.to_parquet(OUT_PATH, index=False)
    log.info("Salvo: %s (%d bairros)", OUT_PATH.name, len(df_agg))
    return df_agg


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run().head(10).to_string())