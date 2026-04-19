"""
Utilitários compartilhados pelos módulos de transformação.

Centraliza a leitura de CSVs brutos do portal da PBH, que apresentam
inconsistências comuns em datasets governamentais brasileiros:
- Separador variado (vírgula, ponto-e-vírgula)
- Encoding variado (UTF-8, Latin-1, CP1252)
- Nomes de colunas com grafias inconsistentes entre datasets
"""

import csv
import logging
from pathlib import Path
import pandas as pd

log = logging.getLogger(__name__)

ENCODINGS = ("utf-8", "utf-8-sig", "latin-1", "cp1252")
SEPARATORS = (",", ";", "\t", "|")

# EPSG:31983 é o sistema de referência oficial dos dados geoespaciais da PBH
EPSG_PBH = 31983


def _detect_separator(
    path: Path,
    encoding: str
) -> str:
    """
    Detecta o separador do CSV lendo a primeira linha com Sniffer.

    Cai para heurística por contagem se o Sniffer falhar.

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
        # Fallback: conta ocorrências de cada separador na primeira linha
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
    :return: DataFrame com colunas normalizadas e todos os valores como string
    :raises ValueError: Se não conseguir decodificar ou parsear o arquivo
    """
    last_error: Exception | None = None

    for encoding in ENCODINGS:
        try:
            sep = _detect_separator(path, encoding)
            df = pd.read_csv(
                path,
                encoding=encoding,
                sep=sep,
                dtype=str,
                low_memory=False,
                on_bad_lines="skip",   # tolerante a linhas malformadas
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
    Normaliza nomes de bairro: strip, upper, remove acentos.

    Garante que o mesmo bairro escrito de formas diferentes entre datasets
    seja reconhecido como idêntico no join.

    :param series: Série com nomes de bairro
    :return: Série normalizada
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

    Útil quando o nome exato varia entre versões do dataset
    (ex: 'BAIRRO' vs 'NOME_BAIRRO' vs 'NOME_BAIRRO_POPULAR').

    :param df: DataFrame com colunas já normalizadas
    :param candidates: Nomes possíveis em ordem de preferência
    :return: Nome da primeira coluna encontrada ou None
    """
    cols_upper = {c.upper(): c for c in df.columns}
    for candidate in candidates:
        if candidate.upper() in cols_upper:
            return cols_upper[candidate.upper()]
    return None


def spatial_join_to_bairros(
    df_pontos: pd.DataFrame,
    df_bairros: pd.DataFrame,
    col_wkt_pontos: str = "GEOMETRIA",
    col_wkt_bairros: str = "GEOMETRIA",
    col_nome_bairro: str = "NOME",
) -> pd.DataFrame:
    """
    Atribui nome de bairro a cada ponto via spatial join.

    Usa GeoPandas com geometrias em EPSG:31983 (SRS oficial da PBH).
    Pontos fora de todos os polígonos recebem NaN na coluna de bairro.

    :param df_pontos: DataFrame com geometria WKT em coluna col_wkt_pontos
    :param df_bairros: DataFrame com polígonos WKT em col_wkt_bairros e nome em col_nome_bairro
    :param col_wkt_pontos: Nome da coluna WKT no DataFrame de pontos
    :param col_wkt_bairros: Nome da coluna WKT no DataFrame de bairros
    :param col_nome_bairro: Nome da coluna com o nome do bairro no DataFrame de bairros
    :return: DataFrame original com coluna 'BAIRRO_SJOIN' adicionada
    """
    import geopandas as gpd
    from shapely import wkt

    # Parse WKT dos bairros (polígonos)
    gdf_bairros = df_bairros[[col_nome_bairro, col_wkt_bairros]].copy()
    gdf_bairros["geometry"] = gdf_bairros[col_wkt_bairros].apply(
        lambda s: wkt.loads(s) if isinstance(s, str) and s.strip() else None
    )
    gdf_bairros = gpd.GeoDataFrame(
        gdf_bairros.dropna(subset=["geometry"]),
        geometry="geometry",
        crs=f"EPSG:{EPSG_PBH}",
    )[[col_nome_bairro, "geometry"]]

    # Parse WKT dos pontos
    df_pontos = df_pontos.copy()
    df_pontos["_geom"] = df_pontos[col_wkt_pontos].apply(
        lambda s: wkt.loads(s) if isinstance(s, str) and s.strip() else None
    )
    gdf_pontos = gpd.GeoDataFrame(
        df_pontos.dropna(subset=["_geom"]),
        geometry="_geom",
        crs=f"EPSG:{EPSG_PBH}",
    )

    # Spatial join: cada ponto recebe o nome do bairro que o contém
    joined = gpd.sjoin(
        gdf_pontos,
        gdf_bairros,
        how="left",
        predicate="within",
    )

    result = pd.DataFrame(
        joined.drop(columns=["geometry", "_geom", "index_right"], errors="ignore")
    )
    result = result.rename(columns={col_nome_bairro: "BAIRRO_SJOIN"})

    n_matched = result["BAIRRO_SJOIN"].notna().sum()
    log.info("Spatial join: %d/%d pontos atribuídos a um bairro", n_matched, len(result))

    return result