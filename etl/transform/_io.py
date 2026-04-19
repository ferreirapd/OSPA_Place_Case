"""
Utilitários compartilhados pelos módulos de transformação.

Centraliza:
- Leitura de CSVs brutos do portal da PBH (encoding e separador variáveis)
- Normalização textual de nomes de bairro (upper, sem acento)
- Padronização contra a tabela canônica de nomes populares
- Spatial join genérico pontos × polígonos de bairros

Sobre a fonte canônica:
  O bairros.csv da PBH tem colunas ID_BAC, CODIGO, TIPO, NOME, AREA_KM2,
  PERIMETR_M, GEOMETRIA. A coluna NOME contém subdivisões administrativas
  internas ("Primeira", "Sexta", "Do Castelo") — não nomes populares.
  Usar ela como canônico faz "SAVASSI", "CENTRO" e "LOURDES" desaparecerem.

  A fonte correta é atividade_economica.csv / coluna NOME_BAIRRO, que
  contém os nomes populares usados no cotidiano de BH. A função
  load_bairros_canonicos() recebe o path desse arquivo.
"""

import csv
import logging
from pathlib import Path
import pandas as pd

log = logging.getLogger(__name__)

ENCODINGS = ("utf-8", "utf-8-sig", "latin-1", "cp1252")
SEPARATORS = (",", ";", "\t", "|")

EPSG_PBH = 31983

# Score mínimo (0-100) aceito no fuzzy match.
# 88 rejeita falsos-positivos tipo SANTA EFIGENIA × SANTA MONICA
# mas aceita variações legítimas tipo SAO JUDAS × SAO JUDAS TADEU.
FUZZY_SCORE_CUTOFF = 88

# Candidatos a coluna de nome popular de bairro, em ordem de preferência.
# NOME_BAIRRO (atividade econômica) tem nomes como "SAVASSI", "CENTRO".
# NÃO usar bairros.csv/NOME — contém subdivisões internas, não nomes populares.
BAIRROS_CANONICOS_CANDIDATOS = ("NOME_BAIRRO", "NOME_BAIRRO_POPULAR", "BAIRRO")


def _detect_separator(
    path: Path,
    encoding: str
) -> str:
    """
    Detecta o separador do CSV lendo a primeira linha com Sniffer.

    :param path: Caminho para o arquivo CSV
    :param encoding: Encoding já validado do arquivo
    :return: Caractere separador detectado
    """
    with open(path, "r", encoding=encoding) as f:
        sample = f.read(8192)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters="".join(SEPARATORS))
        return dialect.delimiter
    except csv.Error:
        first_line = sample.splitlines()[0] if sample else ""
        counts = {sep: first_line.count(sep) for sep in SEPARATORS}
        return max(counts, key=counts.get) if max(counts.values()) > 0 else ","


def load_csv(
    path: Path,
    label: str = ""
) -> pd.DataFrame:
    """
    Carrega um CSV detectando encoding e separador automaticamente.

    Normaliza nomes de colunas para MAIÚSCULAS sem espaços nas bordas.

    :param path: Caminho para o arquivo CSV
    :param label: Rótulo opcional para logging
    :return: DataFrame com colunas normalizadas e valores como string
    :raises ValueError: Se não conseguir decodificar ou parsear o arquivo
    """
    last_error: Exception | None = None
    for encoding in ENCODINGS:
        try:
            sep = _detect_separator(path, encoding)
            df = pd.read_csv(
                path, encoding=encoding, sep=sep,
                dtype=str, low_memory=False, on_bad_lines="skip",
            )
            df.columns = df.columns.str.strip().str.upper()
            log.info(
                "'%s' carregado: %d linhas × %d colunas (encoding='%s', sep='%s')",
                label or path.name, len(df), len(df.columns), encoding, sep,
            )
            return df
        except (UnicodeDecodeError, pd.errors.ParserError) as exc:
            last_error = exc
            continue
    raise ValueError(f"Falha ao ler CSV '{path}': {last_error}")


def normalize_bairro(series: pd.Series) -> pd.Series:
    """
    Normaliza nomes de bairro: strip, upper, remove acentos via NFD.

    :param series: Série com nomes de bairro em qualquer formato
    :return: Série normalizada (MAIÚSCULAS ASCII sem acento)
    """
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
        .str.normalize("NFD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
    )


def find_column(
    df: pd.DataFrame,
    *candidates: str
) -> str | None:
    """
    Procura a primeira coluna do DataFrame que case com algum candidato.

    :param df: DataFrame com colunas já normalizadas
    :param candidates: Nomes possíveis em ordem de preferência
    :return: Nome da primeira coluna encontrada ou None
    """
    cols_upper = {c.upper(): c for c in df.columns}
    for candidate in candidates:
        if candidate.upper() in cols_upper:
            return cols_upper[candidate.upper()]
    return None


def load_bairros_canonicos(eco_csv_path: Path) -> list[str]:
    """
    Carrega a lista canônica de nomes populares de bairros de BH.

    Extrai os nomes únicos da coluna NOME_BAIRRO do CSV de atividade
    econômica — a fonte mais completa e consistente com os outros datasets.
    Os nomes retornados já estão normalizados (MAIÚSCULAS, sem acento),
    no mesmo formato produzido por normalize_bairro().

    Por que NÃO usar bairros.csv como canônico:
    Esse arquivo contém subdivisões administrativas internas (NOME =
    "Primeira", "Sexta", "Do Castelo"…). Savassi, Centro e Lourdes
    simplesmente não existem nele.

    :param eco_csv_path: Caminho para CSV com coluna NOME_BAIRRO
    :return: Lista ordenada de nomes canônicos únicos já normalizados
    :raises FileNotFoundError: Se o arquivo não existir
    :raises ValueError: Se nenhuma coluna de nome for encontrada
    """
    if not eco_csv_path.exists():
        raise FileNotFoundError(
            f"Arquivo de fonte canônica não encontrado: {eco_csv_path}. "
            "Rode o pipeline de extract primeiro."
        )

    df = load_csv(eco_csv_path, "fonte_canonica")
    col_nome = find_column(df, *BAIRROS_CANONICOS_CANDIDATOS)

    if col_nome is None:
        raise ValueError(
            f"Nenhuma coluna de nome de bairro encontrada em {eco_csv_path}. "
            f"Esperado: {BAIRROS_CANONICOS_CANDIDATOS}. "
            f"Disponíveis: {df.columns.tolist()[:15]}"
        )

    nomes_norm = normalize_bairro(df[col_nome])
    canonicos = sorted(set(nomes_norm[nomes_norm != ""].tolist()))

    log.info(
        "Tabela canônica: %d nomes únicos (arquivo='%s', coluna='%s')",
        len(canonicos), eco_csv_path.name, col_nome,
    )
    return canonicos


def match_bairro_canonico(
    series: pd.Series,
    canonicos: list[str],
    score_cutoff: int = FUZZY_SCORE_CUTOFF,
) -> pd.Series:
    """
    Mapeia nomes de bairros contra a tabela canônica via fuzzy match.

    Estratégia vetorizada:
    1. Normaliza a entrada (upper + sem acento)
    2. Para cada nome único: match exato → se não encontrar, rapidfuzz.WRatio
    3. Aplica o mapa com Series.map — um único sweep no DataFrame inteiro

    Nomes com score < score_cutoff retornam NaN (decisão de descarte fica
    no chamador).

    :param series: Série com nomes de bairro em qualquer formato
    :param canonicos: Lista de nomes canônicos já normalizados
    :param score_cutoff: Score mínimo aceito (0-100). Default 88
    :return: Série com nomes canônicos. NaN para os não identificáveis
    """
    from rapidfuzz import fuzz, process

    norm = normalize_bairro(series)
    canonicos_set = set(canonicos)
    unique_norm = [x for x in norm.dropna().unique() if x != ""]

    mapping: dict[str, str | None] = {}
    n_exatos, n_fuzzy, n_sem_match = 0, 0, 0

    for nome in unique_norm:
        if nome in canonicos_set:
            mapping[nome] = nome
            n_exatos += 1
            continue
        melhor = process.extractOne(
            nome, canonicos,
            scorer=fuzz.WRatio,
            score_cutoff=score_cutoff,
        )
        if melhor is not None:
            mapping[nome] = melhor[0]
            n_fuzzy += 1
        else:
            mapping[nome] = None
            n_sem_match += 1

    log.info(
        "Fuzzy match: %d exatos | %d aproximados | %d sem match "
        "(cutoff=%d, total únicos=%d)",
        n_exatos, n_fuzzy, n_sem_match, score_cutoff, len(unique_norm),
    )
    return norm.map(mapping)


def spatial_join_to_bairros(
    df_pontos: pd.DataFrame,
    df_bairros: pd.DataFrame,
    col_wkt_pontos: str = "GEOMETRIA",
    col_wkt_bairros: str = "GEOMETRIA",
    col_nome_bairro: str = "NOME",
) -> pd.DataFrame:
    """
    Atribui nome de bairro a cada ponto via spatial join GeoPandas.

    O nome retornado em 'BAIRRO_SJOIN' vem da coluna col_nome_bairro do
    bairros.csv — que é o nome administrativo interno. Chamar
    match_bairro_canonico() no resultado é obrigatório para converter
    para o nome canônico popular antes de salvar.

    :param df_pontos: DataFrame com geometria WKT
    :param df_bairros: DataFrame com polígonos WKT e nome do bairro
    :param col_wkt_pontos: Coluna WKT nos pontos
    :param col_wkt_bairros: Coluna WKT nos polígonos
    :param col_nome_bairro: Coluna com o nome do bairro nos polígonos
    :return: df_pontos enriquecido com coluna 'BAIRRO_SJOIN'
    """
    import geopandas as gpd
    from shapely import wkt

    gdf_bairros = df_bairros[[col_nome_bairro, col_wkt_bairros]].copy()
    gdf_bairros["geometry"] = gdf_bairros[col_wkt_bairros].apply(
        lambda s: wkt.loads(s) if isinstance(s, str) and s.strip() else None
    )
    gdf_bairros = gpd.GeoDataFrame(
        gdf_bairros.dropna(subset=["geometry"]),
        geometry="geometry",
        crs=f"EPSG:{EPSG_PBH}",
    )[[col_nome_bairro, "geometry"]]

    df_pontos = df_pontos.copy()
    df_pontos["_geom"] = df_pontos[col_wkt_pontos].apply(
        lambda s: wkt.loads(s) if isinstance(s, str) and s.strip() else None
    )
    gdf_pontos = gpd.GeoDataFrame(
        df_pontos.dropna(subset=["_geom"]),
        geometry="_geom",
        crs=f"EPSG:{EPSG_PBH}",
    )

    joined = gpd.sjoin(
        gdf_pontos, gdf_bairros,
        how="left", predicate="within",
    )

    result = pd.DataFrame(
        joined.drop(columns=["geometry", "_geom", "index_right"], errors="ignore")
    )
    result = result.rename(columns={col_nome_bairro: "BAIRRO_SJOIN"})

    n_matched = result["BAIRRO_SJOIN"].notna().sum()
    log.info("Spatial join: %d/%d pontos atribuídos a um bairro", n_matched, len(result))
    return result