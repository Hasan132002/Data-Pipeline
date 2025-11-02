import os
import sys
import pandas as pd
import sqlalchemy
from pathlib import Path
from scripts.utils import logger

PROJECT_ROOT = Path(r"D:\BDA\projects\so-pipeline")
IN_PARQUET = Path(sys.argv[1]) if len(sys.argv) > 1 else PROJECT_ROOT / "data" / "parquet" / "with_inference.parquet"

DB_URL = os.environ.get("SO_DB_URL", "postgresql://so_user:so_pass@localhost:5432/so_db")  

def main():
    logger.info(f"Reading parquet from {IN_PARQUET}")
    df = pd.read_parquet(str(IN_PARQUET))
    logger.info(f"Rows to load: {len(df)}")
    engine = sqlalchemy.create_engine(DB_URL)

    out_df = df[['question_id','title','body_length','title_length','tags_count','sent_label','sent_score']].copy()
    out_df.to_sql('so_sentiment', engine, if_exists='replace', index=False)
    logger.info("Loaded data into Postgres table so_sentiment")

if __name__ == "__main__":
    main()
