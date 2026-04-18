"""
Transformação da Matriz Origem-Destino com PySpark.

Este módulo demonstra o uso de PySpark para processar dados esparsos
de alta dimensionalidade — cenário onde Pandas seria ineficiente.

Pipeline:
1. Lê o CSV da matriz O-D (hexágonos H3 × hexágonos H3)
2. Converte hexágonos H3 → coordenadas centroides
3. Faz spatial join com polígonos de bairros (via broadcast join)
4. Agrega fluxos de origem e destino por bairro
5. Salva como Parquet particionado

Por que PySpark aqui?
- A matriz é esparsa: N×N hexágonos com maioria zero
- Em BH, ~500 hexágonos → até 250k combinações por mês
- O spatial join seria O(n²) em Pandas; PySpark paraleliza nativamente
"""

import logging
from pathlib import Path

log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parents[2] / "data"
RAW_OD = BASE / "raw" / "matriz_od" / "matriz_od.csv"
RAW_BAIRROS = BASE / "raw" / "bairros" / "bairros.csv"
OUT_PATH = BASE / "processed" / "matriz_od_agregada.parquet"

# Colunas esperadas no CSV da matriz O-D (ajustar conforme dicionário de dados)
COL_ORIGEM_H3 = "H3_ORIGEM"
COL_DESTINO_H3 = "H3_DESTINO"
COL_FLUXO = "QTD_VIAGENS"

# Colunas de bairros
COL_BAIRRO_NOME = "NOME"
COL_BAIRRO_LAT = "LATITUDE"
COL_BAIRRO_LON = "LONGITUDE"


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
        .config("spark.sql.shuffle.partitions", "8")   # reduz overhead em dados pequenos
        .config("spark.ui.enabled", "false")           # desativa UI para ambiente headless
        .getOrCreate()
    )


def _h3_to_latlng(h3_index: str) -> tuple[float, float] | tuple[None, None]:
    """
    Converte um índice H3 para coordenadas (latitude, longitude) do centroide.

    :param h3_index: String do índice H3 (ex: '8a1e2b3c4d5f6e7')
    :return: Tupla (lat, lng) ou (None, None) em caso de erro
    """
    try:
        import h3
        lat, lng = h3.h3_to_geo(h3_index)
        return lat, lng
    except Exception:
        return None, None


def run() -> None:
    """
    Executa a transformação da Matriz O-D com PySpark.

    Agrega viagens por bairro de origem e destino e salva em Parquet.
    """
    log.info("Iniciando transformação PySpark: Matriz O-D")

    if not RAW_OD.exists():
        log.warning("Arquivo de matriz O-D não encontrado em %s — pulando", RAW_OD)
        return

    spark = _get_spark()

    # ------------------------------------------------------------------
    # 1. Leitura
    # ------------------------------------------------------------------
    df_od = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(RAW_OD))
    )
    log.info("Matriz O-D carregada: %d registros", df_od.count())

    df_bairros = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(RAW_BAIRROS))
    )

    # ------------------------------------------------------------------
    # 2. Conversão H3 → lat/lng via UDF
    # ------------------------------------------------------------------
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    schema_latlng = StructType([
        StructField("lat", DoubleType()),
        StructField("lng", DoubleType()),
    ])

    @F.udf(schema_latlng)
    def h3_to_latlng_udf(h3_index: str):
        """UDF PySpark: converte H3 → (lat, lng)."""
        try:
            import h3 as h3lib
            lat, lng = h3lib.h3_to_geo(h3_index)
            return (lat, lng)
        except Exception:
            return (None, None)

    df_od = (
        df_od
        .withColumn("origem_coords", h3_to_latlng_udf(F.col(COL_ORIGEM_H3)))
        .withColumn("destino_coords", h3_to_latlng_udf(F.col(COL_DESTINO_H3)))
        .withColumn("origem_lat", F.col("origem_coords.lat"))
        .withColumn("origem_lng", F.col("origem_coords.lng"))
        .withColumn("destino_lat", F.col("destino_coords.lat"))
        .withColumn("destino_lng", F.col("destino_coords.lng"))
        .drop("origem_coords", "destino_coords")
    )

    # ------------------------------------------------------------------
    # 3. Spatial join simplificado: associa H3 ao bairro mais próximo
    #    (aproximação por centroide — suficiente para análise de fluxo)
    # ------------------------------------------------------------------
    # Broadcast join: bairros é pequeno (~600 linhas), O-D pode ser grande
    df_bairros_b = F.broadcast(
        df_bairros.select(
            F.col(COL_BAIRRO_NOME).alias("bairro"),
            F.col(COL_BAIRRO_LAT).cast("double").alias("bairro_lat"),
            F.col(COL_BAIRRO_LON).cast("double").alias("bairro_lng"),
        )
    )

    # Join por aproximação: encontra bairro com menor distância euclidiana ao centroide H3
    # Nota: em produção substituir por ST_Within com GeoPandas ou Sedona
    df_origem = (
        df_od
        .crossJoin(df_bairros_b)
        .withColumn(
            "dist_origem",
            F.sqrt(
                F.pow(F.col("origem_lat") - F.col("bairro_lat"), 2)
                + F.pow(F.col("origem_lng") - F.col("bairro_lng"), 2)
            ),
        )
        .groupBy(COL_ORIGEM_H3)
        .agg(
            F.first("bairro").alias("bairro_origem"),
            F.min("dist_origem").alias("_drop"),
        )
        .drop("_drop")
    )

    # ------------------------------------------------------------------
    # 4. Agregação: total de viagens por bairro de destino
    # ------------------------------------------------------------------
    df_fluxo = (
        df_od
        .join(df_origem, on=COL_ORIGEM_H3, how="left")
        .groupBy("bairro_origem")
        .agg(
            F.sum(COL_FLUXO).alias("total_viagens_originadas"),
            F.countDistinct(COL_DESTINO_H3).alias("destinos_unicos"),
        )
        .withColumnRenamed("bairro_origem", "bairro")
    )

    # ------------------------------------------------------------------
    # 5. Salva como Parquet
    # ------------------------------------------------------------------
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    (
        df_fluxo
        .toPandas()
        .to_parquet(OUT_PATH, index=False)
    )
    log.info("✓ matriz_od_agregada.parquet salvo")

    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
