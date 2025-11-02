import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, year, month
from scripts.utils import ensure_dir, logger

PROJECT_ROOT = Path(r"D:\BDA\projects\so-pipeline")
RAW_JSON = Path(sys.argv[1]) if len(sys.argv) > 1 else PROJECT_ROOT / "stackoverflow_questions.json"
OUT_PARQUET = Path(sys.argv[2]) if len(sys.argv) > 2 else PROJECT_ROOT / "data" / "parquet" / "raw_parquet"

ensure_dir(OUT_PARQUET)

def main():
    spark = SparkSession.builder \
        .appName("so-ingest") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    logger.info(f"Reading JSON from {RAW_JSON}")
    df = spark.read.json(str(RAW_JSON))

    logger.info("Schema:")
    df.printSchema()
    cnt = df.count()
    logger.info(f"Rows read: {cnt}")

    if 'creation_date' in df.columns:
        df = df.withColumn("created_date", to_date(from_unixtime(df.creation_date)))
        df = df.withColumn("year", year("created_date")).withColumn("month", month("created_date"))
        df.write.mode("overwrite").partitionBy("year", "month").parquet(str(OUT_PARQUET))
    else:
        df.write.mode("overwrite").parquet(str(OUT_PARQUET))

    spark.stop()
    logger.info(f"Wrote parquet to {OUT_PARQUET}")

if __name__ == "__main__":
    main()
