"""
Transformação da Matriz Origem-Destino com PySpark.

Pipeline:
1. Lê o CSV (~188k registros, sep=';') com PySpark
2. Agrega contagem de viagens por hexágono H3 de origem
3. Extrai centroide da GEOMETRIA_ORIGEM (WKT EPSG:31983)
4. Converte UTM → WGS84 via GeoPandas
5. Nearest-neighbor com centroides dos bairros (NumPy)
6. Padroniza nomes contra a tabela canônica (atividade econômica)
7. Salva em Parquet
"""

import logging
from pathlib import Path

log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[2] / "data"
RAW_OD      = BASE / "raw" / "matriz_od"           / "matriz_od.csv"
RAW_BAIRROS = BASE / "raw" / "bairros"              / "bairros.csv"
RAW_ECO     = BASE / "raw" / "atividade_economica"  / "atividade_economica.csv"
OUT_PATH    = BASE / "processed" / "matriz_od_agregada.parquet"

COL_ORIGEM_H3  = "H3_ORIGEM"
COL_DESTINO_H3 = "H3_DESTINO"
COL_GEOM_ORIGEM = "GEOMETRIA_ORIGEM"

# bairros.csv — geometria vem daqui, nome NÃO vem daqui
COL_BAIRRO_GEOM = "GEOMETRIA"
COL_BAIRRO_NOME = "NOME"   # nome administrativo — usado só pros centroides

CRS_UTM   = "EPSG:31983"
CRS_WGS84 = "EPSG:4326"


def _get_spark():
    """
    Inicializa SparkSession para ambiente local com uso mínimo de memória.

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
    Lê os polígonos de bairros e retorna {nome_upper: (lat, lng)} em WGS84.

    Usa o nome administrativo (NOME) apenas para indexação interna dos
    centroides — o nearest-neighbor mapeia para esses nomes, e depois
    match_bairro_canonico() converte para o nome popular canônico.

    :param bairros_path: Caminho para o CSV de bairros
    :return: Dict nome_upper → (latitude, longitude) do centroide em WGS84
    """
    import geopandas as gpd
    import pandas as pd
    from shapely import wkt

    df = pd.read_csv(bairros_path, dtype=str, encoding="utf-8", sep=",")
    df.columns = df.columns.str.strip().str.upper()

    centroids: dict[str, tuple[float, float]] = {}
    for _, row in df.iterrows():
        nome     = str(row.get(COL_BAIRRO_NOME, "")).strip()
        geom_wkt = str(row.get(COL_BAIRRO_GEOM, "")).strip()
        if not nome or not geom_wkt or geom_wkt == "nan":
            continue
        try:
            geom = wkt.loads(geom_wkt)
            gdf = gpd.GeoDataFrame(geometry=[geom], crs=CRS_UTM)
            gdf_wgs = gdf.to_crs(CRS_WGS84)
            c = gdf_wgs.geometry[0].centroid
            centroids[nome.upper()] = (c.y, c.x)
        except Exception as exc:
            log.debug("Bairro '%s' — erro ao calcular centroide: %s", nome, exc)

    log.info("Centroides de bairros computados: %d", len(centroids))
    return centroids


def _wkt_to_centroid_wgs84(wkt_utm: str) -> tuple[float, float] | None:
    """
    Extrai o centroide de um WKT em EPSG:31983 e converte para WGS84.

    :param wkt_utm: String WKT em coordenadas UTM EPSG:31983
    :return: Tupla (latitude, longitude) em WGS84, ou None se inválido
    """
    try:
        import geopandas as gpd
        from shapely import wkt

        geom = wkt.loads(wkt_utm)
        gdf = gpd.GeoDataFrame(geometry=[geom], crs=CRS_UTM)
        c = gdf.to_crs(CRS_WGS84).geometry[0].centroid
        return (c.y, c.x)
    except Exception:
        return None


def run() -> None:
    """
    Executa a transformação da Matriz O-D com PySpark.

    Agrega viagens originadas por bairro canônico e salva em Parquet.
    """
    import pandas as pd
    from etl.transform._io import load_bairros_canonicos, match_bairro_canonico

    log.info("Iniciando transformação PySpark: Matriz O-D")

    if not RAW_OD.exists():
        log.warning("Arquivo de matriz O-D não encontrado em %s — pulando", RAW_OD)
        return

    canonicos = load_bairros_canonicos(RAW_ECO)
    spark = _get_spark()

    from pyspark.sql import functions as F

    # 1. Leitura
    df_od = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .csv(str(RAW_OD))
    )
    log.info("Matriz O-D: %d registros | colunas: %s", df_od.count(), df_od.columns)

    # 2. Agrega por H3 de origem (reduz problema de escala antes de sair do Spark)
    df_h3_agg = (
        df_od
        .groupBy(COL_ORIGEM_H3)
        .agg(
            F.count("*").alias("total_viagens"),
            F.countDistinct(COL_DESTINO_H3).alias("destinos_unicos"),
            F.first(COL_GEOM_ORIGEM).alias("geom_wkt"),
        )
        .filter(F.col("geom_wkt").isNotNull())
    )

    df_h3_pd = df_h3_agg.toPandas()
    log.info("Hexágonos H3 únicos com geometria: %d", len(df_h3_pd))

    if df_h3_pd.empty:
        log.warning("Nenhum hexágono com geometria válida — abortando")
        spark.stop()
        return

    # 3. Extrai centroide WGS84
    coords = df_h3_pd["geom_wkt"].map(_wkt_to_centroid_wgs84)
    df_h3_pd["lat"] = coords.map(lambda c: c[0] if c else None)
    df_h3_pd["lng"] = coords.map(lambda c: c[1] if c else None)
    df_h3_pd = df_h3_pd.dropna(subset=["lat", "lng"])
    log.info("Hexágonos com coordenadas válidas: %d", len(df_h3_pd))

    if df_h3_pd.empty:
        log.warning("Nenhuma coordenada válida após conversão — abortando")
        spark.stop()
        return

    # 4. Nearest-neighbor vetorizado em NumPy
    centroids = _bairro_centroids_dict(RAW_BAIRROS)
    if not centroids:
        log.warning("Nenhum centroide disponível — abortando")
        spark.stop()
        return

    import numpy as np

    bairro_nomes = list(centroids.keys())
    bairro_lats  = np.array([centroids[b][0] for b in bairro_nomes])
    bairro_lngs  = np.array([centroids[b][1] for b in bairro_nomes])

    orig_lats = df_h3_pd["lat"].to_numpy()[:, None]
    orig_lngs = df_h3_pd["lng"].to_numpy()[:, None]
    dists_sq  = (orig_lats - bairro_lats) ** 2 + (orig_lngs - bairro_lngs) ** 2
    idx       = np.argmin(dists_sq, axis=1)

    df_h3_pd["bairro_raw"] = [bairro_nomes[i] for i in idx]

    # 5. Padroniza nome administrativo → canônico popular
    df_h3_pd["bairro"] = match_bairro_canonico(df_h3_pd["bairro_raw"], canonicos)
    df_h3_pd = df_h3_pd.dropna(subset=["bairro"])

    # 6. Agrega por bairro canônico
    df_fluxo = (
        df_h3_pd
        .groupby("bairro", as_index=False)
        .agg(
            total_viagens_originadas=("total_viagens", "sum"),
            destinos_unicos=("destinos_unicos", "sum"),
        )
    )

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_fluxo.to_parquet(OUT_PATH, index=False)
    log.info("✓ matriz_od_agregada.parquet salvo: %d bairros", len(df_fluxo))

    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()