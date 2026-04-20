"""
Transformação da Matriz Origem-Destino com PySpark.

Pipeline:
1. Lê CSV (~188k registros) com PySpark
2. Agrega por hexágono H3 de origem
3. Extrai centroide da GEOMETRIA_ORIGEM (WKT EPSG:31983)
4. Converte UTM -> WGS84
5. Nearest-neighbor com centroides dos bairros (NumPy vetorizado)
6. Padroniza nomes contra a tabela canônica
"""

import logging
from pathlib import Path

log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[2] / "data"
RAW_OD      = BASE / "raw" / "matriz_od"            / "matriz_od.csv"
RAW_BAIRROS = BASE / "raw" / "bairros"              / "bairros.csv"
RAW_ECO     = BASE / "raw" / "atividade_economica"  / "atividade_economica.csv"
OUT_PATH    = BASE / "processed" / "matriz_od_agregada.parquet"

ETAPA = "matriz_od"

COL_ORIGEM_H3   = "H3_ORIGEM"
COL_DESTINO_H3  = "H3_DESTINO"
COL_GEOM_ORIGEM = "GEOMETRIA_ORIGEM"
COL_BAIRRO_GEOM = "GEOMETRIA"
COL_BAIRRO_NOME_CANDIDATOS = ("NOME_BAIRRO_POPULAR", "NOME_BAIRRO", "NOME")

CRS_UTM   = "EPSG:31983"
CRS_WGS84 = "EPSG:4326"


def _get_spark():
    """
    Inicializa SparkSession para ambiente local com baixo uso de memória.

    :return: SparkSession ativa
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName("bh-matriz-od")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _bairro_centroids_dict(bairros_path: Path) -> dict[str, tuple[float, float]]:
    """
    Lê polígonos de bairros e retorna dict {nome_upper: (lat, lng)} em WGS84.

    Usa o nome administrativo do bairros.csv apenas como chave interna;
    match_bairro_canonico() converte depois para nomes populares.

    :param bairros_path: Caminho para o CSV de bairros
    :return: Dict com nome em MAIÚSCULAS -> (latitude, longitude) WGS84
    """
    import geopandas as gpd
    import pandas as pd
    from shapely import wkt

    df = pd.read_csv(bairros_path, dtype=str, encoding="utf-8", sep=",")
    df.columns = df.columns.str.strip().str.upper()

    col_nome = next((c for c in COL_BAIRRO_NOME_CANDIDATOS if c in df.columns), None)
    if col_nome is None:
        log.warning("Nenhuma coluna de nome em bairros.csv")
        return {}

    centroids: dict[str, tuple[float, float]] = {}
    for _, row in df.iterrows():
        nome     = str(row.get(col_nome, "")).strip()
        geom_wkt = str(row.get(COL_BAIRRO_GEOM, "")).strip()
        if not nome or not geom_wkt or geom_wkt == "nan":
            continue
        try:
            geom = wkt.loads(geom_wkt)
            gdf = gpd.GeoDataFrame(geometry=[geom], crs=CRS_UTM).to_crs(CRS_WGS84)
            c = gdf.geometry[0].centroid
            centroids[nome.upper()] = (c.y, c.x)
        except Exception as exc:
            log.debug("Centroide falhou para %r: %s", nome, exc)

    log.info("Centroides computados: %d bairros", len(centroids))
    return centroids


def _wkt_to_centroid_wgs84(wkt_utm: str) -> tuple[float, float] | None:
    """
    Extrai centroide de WKT em UTM e converte para WGS84.

    :param wkt_utm: String WKT em coordenadas UTM EPSG:31983
    :return: (latitude, longitude) em WGS84, ou None se inválido
    """
    try:
        import geopandas as gpd
        from shapely import wkt

        geom = wkt.loads(wkt_utm)
        c = gpd.GeoDataFrame(geometry=[geom], crs=CRS_UTM).to_crs(CRS_WGS84).geometry[0].centroid
        return (c.y, c.x)
    except Exception:
        return None


def _extrair_h3_com_centroides(spark, raw_od: Path):
    """
    Agrega O-D por H3 em Spark e retorna Pandas com centroides WGS84.

    :param spark: SparkSession ativa
    :param raw_od: Caminho para o CSV da matriz O-D
    :return: DataFrame Pandas com H3, total_viagens, destinos_unicos, lat, lng
    """
    import pandas as pd
    from pyspark.sql import functions as F

    df_od = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .csv(str(raw_od))
    )
    log.info("Matriz O-D: %d registros", df_od.count())

    df_h3 = (
        df_od
        .groupBy(COL_ORIGEM_H3)
        .agg(
            F.count("*").alias("total_viagens"),
            F.countDistinct(COL_DESTINO_H3).alias("destinos_unicos"),
            F.first(COL_GEOM_ORIGEM).alias("geom_wkt"),
        )
        .filter(F.col("geom_wkt").isNotNull())
        .toPandas()
    )
    log.info("Hexágonos H3 únicos: %d", len(df_h3))

    if df_h3.empty:
        return df_h3

    coords = df_h3["geom_wkt"].map(_wkt_to_centroid_wgs84)
    df_h3["lat"] = coords.map(lambda c: c[0] if c else None)
    df_h3["lng"] = coords.map(lambda c: c[1] if c else None)
    df_h3 = df_h3.dropna(subset=["lat", "lng"])
    log.info("Hexágonos com coordenadas válidas: %d", len(df_h3))
    return df_h3


def _nearest_bairro(
    df_h3,
    centroids: dict[str, tuple[float, float]],
):
    """
    Atribui a cada hexágono o bairro do centroide mais próximo.

    :param df_h3: DataFrame com colunas lat, lng
    :param centroids: Dict bairro -> (lat, lng)
    :return: df_h3 com coluna 'bairro_raw' adicionada
    """
    import numpy as np

    nomes = list(centroids.keys())
    lats = np.array([centroids[b][0] for b in nomes])
    lngs = np.array([centroids[b][1] for b in nomes])

    orig_lats = df_h3["lat"].to_numpy()[:, None]
    orig_lngs = df_h3["lng"].to_numpy()[:, None]
    dists_sq  = (orig_lats - lats) ** 2 + (orig_lngs - lngs) ** 2
    idx       = np.argmin(dists_sq, axis=1)

    df_h3 = df_h3.copy()
    df_h3["bairro_raw"] = [nomes[i] for i in idx]
    return df_h3


def run() -> None:
    """
    Executa a transformação da Matriz O-D com PySpark.

    Agrega viagens originadas por bairro canônico e salva em Parquet.
    """
    from etl.transform._io import load_bairros_canonicos, match_bairro_canonico

    log.info("Iniciando transformação: %s", ETAPA)

    if not RAW_OD.exists():
        log.warning("Matriz O-D não encontrada em %s — pulando", RAW_OD)
        return

    canonicos = load_bairros_canonicos(RAW_ECO)
    spark = _get_spark()
    try:
        df_h3 = _extrair_h3_com_centroides(spark, RAW_OD)
        if df_h3.empty:
            log.warning("Sem hexágonos válidos — abortando")
            return

        centroids = _bairro_centroids_dict(RAW_BAIRROS)
        if not centroids:
            log.warning("Sem centroides de bairros — abortando")
            return

        df_h3 = _nearest_bairro(df_h3, centroids)
        df_h3["bairro"] = match_bairro_canonico(
            df_h3["bairro_raw"], canonicos, etapa=ETAPA, fonte="matriz_od",
        )
        df_h3 = df_h3.dropna(subset=["bairro"])

        df_fluxo = (
            df_h3
            .groupby("bairro", as_index=False)
            .agg(
                total_viagens_originadas=("total_viagens", "sum"),
                destinos_unicos=("destinos_unicos", "sum"),
            )
        )

        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        df_fluxo.to_parquet(OUT_PATH, index=False)
        log.info("Salvo: %s (%d bairros)", OUT_PATH.name, len(df_fluxo))
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()