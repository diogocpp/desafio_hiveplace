import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("hiveplace.pipeline")


# ---------------------------------------------------------------------------
# Schema explícito
# ---------------------------------------------------------------------------
RAW_SCHEMA = StructType([
    StructField("id",                 StringType(),    True),
    StructField("vendor_id",          IntegerType(),   True),
    StructField("pickup_datetime",    TimestampType(), True),
    StructField("dropoff_datetime",   TimestampType(), True),
    StructField("passenger_count",    IntegerType(),   True),
    StructField("pickup_longitude",   DoubleType(),    True),
    StructField("pickup_latitude",    DoubleType(),    True),
    StructField("dropoff_longitude",  DoubleType(),    True),
    StructField("dropoff_latitude",   DoubleType(),    True),
    StructField("store_and_fwd_flag", StringType(),    True),
    StructField("trip_duration",      IntegerType(),   True),
])


# ---------------------------------------------------------------------------
# Argumentos CLI
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="HivePlace — Data Pipeline")
    parser.add_argument("--input",        required=True, help="Caminho do CSV bruto")
    parser.add_argument("--output-clean", required=True, help="Diretório saída tratada (Parquet)")
    parser.add_argument("--output-agg",   required=True, help="Diretório saída agregada (Parquet)")
    parser.add_argument("--output-report",default="reports", help="Diretório do relatório JSON")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("hiveplace-pipeline")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------
def read_raw(spark: SparkSession, path: str):
    log.info(f"Lendo dados brutos: {path}")
    return (
        spark.read
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(RAW_SCHEMA)
        .csv(path)
    )


# ---------------------------------------------------------------------------
# Padronização de nomes
# ---------------------------------------------------------------------------
def standardize_columns(df):
    rename_map = {
        "id":                 "id_corrida",
        "vendor_id":          "cd_fornecedor",
        "pickup_datetime":    "ts_embarque",
        "dropoff_datetime":   "ts_desembarque",
        "passenger_count":    "qt_passageiro",
        "pickup_longitude":   "lg_embarque",
        "pickup_latitude":    "lt_embarque",
        "dropoff_longitude":  "lg_desembarque",
        "dropoff_latitude":   "lt_desembarque",
        "store_and_fwd_flag": "in_armazenamento_envio",
        "trip_duration":      "te_duracao_segundo",
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


# ---------------------------------------------------------------------------
# Regras de qualidade
# ---------------------------------------------------------------------------
def get_discard_rules():
    return {
        "ts_embarque_nulo":           F.col("ts_embarque").isNull(),
        "ts_desembarque_nulo":        F.col("ts_desembarque").isNull(),
        "duracao_nula_ou_negativa":   (F.col("te_duracao_segundo") <= 0) | F.col("te_duracao_segundo").isNull(),
        "passageiros_invalidos":      (F.col("qt_passageiro") <= 0) | F.col("qt_passageiro").isNull(),
        "desembarque_antes_embarque": F.col("ts_desembarque") <= F.col("ts_embarque"),
    }


def tag_records(df):
    expr = F.lit(None).cast(StringType())
    for reason, condition in get_discard_rules().items():
        expr = F.when(condition, F.lit(reason)).otherwise(expr)
    return df.withColumn("_motivo_descarte", expr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args  = parse_args()
    spark = create_spark()

    # 1. Leitura
    raw_df = read_raw(spark, args.input)
    total  = raw_df.cache().count()
    log.info(f"Total lido: {total:,} registros")

    # 2. Padronização
    df = standardize_columns(raw_df)

    # 3. Remoção de duplicados
    df = df.dropDuplicates([
        "ts_embarque", "ts_desembarque",
        "lg_embarque", "lt_embarque",
        "lg_desembarque", "lt_desembarque",
    ])

    # 4. Classificação de qualidade
    df = tag_records(df).cache()

    # 5. Separação válidos x descartados
    valid_df   = df.filter(F.col("_motivo_descarte").isNull()).drop("_motivo_descarte")
    invalid_df = df.filter(F.col("_motivo_descarte").isNotNull())

    validos = valid_df.count()
    log.info(f"Válidos: {validos:,} | Descartados: {total - validos:,}")
    log.info(f"Amostra de descartados:")
    invalid_df.show(5, truncate=False)

    spark.stop()
    log.info("Pipeline finalizado.")


if __name__ == "__main__":
    main()