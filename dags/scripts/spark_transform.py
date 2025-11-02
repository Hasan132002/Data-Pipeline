import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, when, length, split
from scripts.utils import ensure_dir, logger

PROJECT_ROOT = Path(r"D:\BDA\projects\so-pipeline")
IN_PARQUET = Path(sys.argv[1]) if len(sys.argv) > 1 else PROJECT_ROOT / "data" / "parquet" / "raw_parquet"
OUT_PARQUET = Path(sys.argv[2]) if len(sys.argv) > 2 else PROJECT_ROOT / "data" / "parquet" / "processed"

ensure_dir(OUT_PARQUET)

def clean_text(df, colname):
    df = df.withColumn(colname, when(col(colname).isNull(), "").otherwise(col(colname)))
    df = df.withColumn(colname, lower(col(colname)))
    df = df.withColumn(colname, regexp_replace(col(colname), r"[\r\n]+", " "))
    df = df.withColumn(colname, regexp_replace(col(colname), r"<[^>]+>", ""))  
    return df

def main():
    spark = SparkSession.builder.appName("so-transform").getOrCreate()
    logger.info(f"Loading parquet from {IN_PARQUET}")
    df = spark.read.parquet(str(IN_PARQUET))

    if 'body' in df.columns:
        df = clean_text(df, 'body')
    if 'title' in df.columns:
        df = clean_text(df, 'title')


    df = df.withColumn("body_length", length(col("body")))
    df = df.withColumn("title_length", length(col("title")))
    df = df.withColumn("tags_count", when(col("tags").isNull(), 0).otherwise(length(split(col("tags"), ","))))

    analytics_df = df.select(
        col("question_id").alias("question_id"),
        col("title").alias("title"),
        col("body").alias("body"),
        col("body_length"),
        col("title_length"),
        col("tags_count"),
        col("owner")
    )

    analytics_df.write.mode("overwrite").parquet(str(OUT_PARQUET))
    spark.stop()
    logger.info(f"Wrote processed parquet to {OUT_PARQUET}")

if __name__ == "__main__":
    main()
