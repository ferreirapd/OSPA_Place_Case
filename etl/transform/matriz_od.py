"""
Transformação da Matriz Origem-Destino com PySpark.

Este módulo demonstra o uso de PySpark para processar dados esparsos
de alta dimensionalidade — cenário onde Pandas seria ineficiente.

Pipeline:
1. Lê o CSV da matriz O-D com PySpark (188k registros, sep=;)
2. Agrega contagem de viagens por hexágono H3 de origem
3. Extrai centroide da coluna GEOMETRIA_ORIGEM (WKT EPSG:31983) via UDF
4. Converte UTM → WGS84 via pyproj
5. Faz nearest-neighbor com centroides dos bairros em Pandas/NumPy
6. Agrega total de viagens por bairro e salva como Parquet

Colunas reais confirmadas no dataset (abril/2026):
  O-D: ID_USUARIO, TIPO_CARTAO, DATA, FAIXA_HORARIA_ORIGEM,
       H3_ORIGEM, H3_DESTINO, GEOMETRIA_ORIGEM, GEOMETRIA_DESTINO
  Bairros: ID_BAC, CODIGO, TIPO, NOME, AREA_KM2, PERIMETR_M, GEOMETRIA

Por que PySpark aqui?
- 188k registros × múltiplos meses = escala que justifica o overhead
- Agregação por H3 antes do spatial join reduz o problema de O(n×m)
  para O(hexágonos_únicos × bairros), que cabe em Pandas
"""

import logging
from pathlib import Path

log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[2] / "data"
RAW_OD = BASE / "raw" / "matriz_od" / "matriz_od.csv"
RAW_BAIRROS = BASE / "raw" / "bairros" / "bairros.csv"
OUT_PATH = BASE / "processed" / "matriz_od_agregada.parquet"

# Colunas reais confirmadas no CSV da PBH
COL_ORIGEM_H3 = "H3_ORIGEM"
COL_DESTINO_H3 = "H3_DESTINO"
COL_GEOM_ORIGEM = "GEOMETRIA_ORIGEM"

# Bairros
COL_BAIRRO_NOME = "NOME"
COL_BAIRRO_GEOM = "GEOMETRIA"

# Projeção dos dados brutos da PBH (UTM zona 23S)
CRS_UTM = "EPSG:31983"
CRS_WGS84 = "EPSG:4326"


def _get_spark():
    """
    Inicializa SparkSession configurada para ambiente local com uso mínimo de memória.

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
    Lê os polígonos de bairros e retorna dicionário {nome_upper: (lat, lng)}.

    Usa GeoPandas — bairros são poucos (322) e cabem em memória.

    :param bairros_path: Caminho para o CSV de bairros
    :return: Dicionário NOME_BAIRRO → (latitude, longitude) do centroide em WGS84
    """
    import geopandas as gpd
    import pandas as pd
    from shapely import wkt

    df = pd.read_csv(bairros_path, dtype=str, encoding="utf-8", sep=",")
    df.columns = df.columns.str.strip().str.upper()

    centroids: dict[str, tuple[float, float]] = {}
    for _, row in df.iterrows():
        nome = str(row.get(COL_BAIRRO_NOME, "")).strip()
        geom_wkt = str(row.get(COL_BAIRRO_GEOM, "")).strip()
        if not nome or not geom_wkt or geom_wkt == "nan":
            continue
        try:
            geom = wkt.loads(geom_wkt)
            gdf = gpd.GeoDataFrame(geometry=[geom], crs=CRS_UTM)
            gdf_wgs = gdf.to_crs(CRS_WGS84)
            centroid = gdf_wgs.geometry[0].centroid
            centroids[nome.upper()] = (centroid.y, centroid.x)  # (lat, lng)
        except Exception as exc:
            log.debug("Erro ao processar bairro '%s': %s", nome, exc)
            continue

    log.info("Centroides de bairros computados: %d bairros", len(centroids))
    return centroids


def _wkt_to_centroid_wgs84(wkt_utm: str) -> tuple[float, float] | None:
    """
    Extrai o centroide de um WKT em EPSG:31983 e converte para WGS84.

    Usado como UDF no Spark via pandas_udf ou chamado após toPandas().

    :param wkt_utm: String WKT em coordenadas UTM EPSG:31983
    :return: Tupla (latitude, longitude) em WGS84, ou None se inválido
    """
    try:
        import geopandas as gpd
        from shapely import wkt

        geom = wkt.loads(wkt_utm)
        gdf = gpd.GeoDataFrame(geometry=[geom], crs=CRS_UTM)
        gdf_wgs = gdf.to_crs(CRS_WGS84)
        c = gdf_wgs.geometry[0].centroid
        return (c.y, c.x)
    except Exception:
        return None


def run() -> None:
    """
    Executa a transformação da Matriz O-D com PySpark.

    Agrega viagens originadas por bairro e salva em Parquet.
    """
    log.info("Iniciando transformação PySpark: Matriz O-D")

    if not RAW_OD.exists():
        log.warning("Arquivo de matriz O-D não encontrado em %s — pulando", RAW_OD)
        return

    spark = _get_spark()

    from pyspark.sql import functions as F

    # ------------------------------------------------------------------
    # 1. Leitura da matriz O-D
    # ------------------------------------------------------------------
    df_od = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .csv(str(RAW_OD))
    )
    total = df_od.count()
    log.info("Matriz O-D carregada: %d registros | colunas: %s", total, df_od.columns)

    # ------------------------------------------------------------------
    # 2. Agrega por H3 de origem no Spark
    #    Mantém uma amostra da geometria para extrair coordenadas depois
    #    Isso reduz o problema de ~188k linhas para poucos hexágonos únicos
    # ------------------------------------------------------------------
    df_h3_agg = (
        df_od
        .groupBy(COL_ORIGEM_H3)
        .agg(
            F.count("*").alias("total_viagens"),
            F.countDistinct(COL_DESTINO_H3).alias("destinos_unicos"),
            # first() pega qualquer geometria do hexágono — todas são iguais
            # pois H3 é uma célula fixa no espaço
            F.first(COL_GEOM_ORIGEM).alias("geom_wkt"),
        )
        .filter(F.col("geom_wkt").isNotNull())
    )

    # Converte para Pandas — após agregação, são poucos hexágonos únicos
    df_h3_pd = df_h3_agg.toPandas()
    log.info("Hexágonos H3 únicos com geometria: %d", len(df_h3_pd))

    if df_h3_pd.empty:
        log.warning("Nenhum hexágono com geometria válida — abortando")
        spark.stop()
        return

    # ------------------------------------------------------------------
    # 3. Extrai centroide WGS84 da coluna GEOMETRIA_ORIGEM (WKT UTM)
    #    Feito em Pandas após toPandas() — são poucos hexágonos únicos
    # ------------------------------------------------------------------
    log.info("Extraindo centroides WGS84 dos hexágonos...")

    coords = df_h3_pd["geom_wkt"].map(_wkt_to_centroid_wgs84)
    df_h3_pd["lat"] = coords.map(lambda c: c[0] if c else None)
    df_h3_pd["lng"] = coords.map(lambda c: c[1] if c else None)

    df_h3_pd = df_h3_pd.dropna(subset=["lat", "lng"])
    log.info("Hexágonos com coordenadas válidas após conversão: %d", len(df_h3_pd))

    if df_h3_pd.empty:
        log.warning("Nenhuma coordenada válida após conversão UTM→WGS84 — abortando")
        spark.stop()
        return

    # ------------------------------------------------------------------
    # 4. Nearest-neighbor vetorizado em NumPy
    #    Atribui cada hexágono ao bairro cujo centroide é mais próximo
    # ------------------------------------------------------------------
    centroids = _bairro_centroids_dict(RAW_BAIRROS)

    if not centroids:
        log.warning("Nenhum centroide de bairro disponível — abortando")
        spark.stop()
        return

    import numpy as np

    bairros_list = list(centroids.items())       # [(nome, (lat, lng)), ...]
    bairro_nomes = [b[0] for b in bairros_list]
    bairro_lats = np.array([b[1][0] for b in bairros_list])
    bairro_lngs = np.array([b[1][1] for b in bairros_list])

    orig_lats = df_h3_pd["lat"].to_numpy()[:, None]   # (n, 1)
    orig_lngs = df_h3_pd["lng"].to_numpy()[:, None]   # (n, 1)

    # Distância euclidiana ao quadrado — suficiente para argmin
    # Shape: (n_hexagonos, n_bairros)
    dists_sq = (orig_lats - bairro_lats) ** 2 + (orig_lngs - bairro_lngs) ** 2
    idx_mais_proximo = np.argmin(dists_sq, axis=1)    # (n,)

    df_h3_pd["bairro"] = [bairro_nomes[i] for i in idx_mais_proximo]

    # ------------------------------------------------------------------
    # 5. Agrega por bairro e salva
    # ------------------------------------------------------------------
    df_fluxo = (
        df_h3_pd
        .groupby("bairro", as_index=False)
        .agg(
            total_viagens_originadas=("total_viagens", "sum"),
            destinos_unicos=("destinos_unicos", "sum"),
        )
    )

    # Normaliza nome para lowercase — padrão dos outros Parquets
    df_fluxo["bairro"] = df_fluxo["bairro"].str.title()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_fluxo.to_parquet(OUT_PATH, index=False)
    log.info("✓ matriz_od_agregada.parquet salvo: %d bairros", len(df_fluxo))

    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()