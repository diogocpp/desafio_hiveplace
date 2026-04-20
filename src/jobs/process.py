import argparse
import logging

from pyspark.sql import SparkSession
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
# Main
# ---------------------------------------------------------------------------
def main():
    args  = parse_args()
    spark = create_spark()

    raw_df = read_raw(spark, args.input)
    total  = raw_df.cache().count()
    log.info(f"Total lido: {total:,} registros")

    spark.stop()
    log.info("Pipeline finalizado.")


if __name__ == "__main__":
    main()