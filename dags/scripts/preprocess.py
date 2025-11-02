import os
import json
import re
import pandas as pd
from pathlib import Path

from dags.scripts.utils import ensure_dir, logger


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_FILE = Path(os.environ.get("RAW_FILE", str(PROJECT_ROOT / "data" / "raw.jsonl")))
OUT_DIR = Path(os.environ.get("OUT_DIR", str(PROJECT_ROOT / "data" / "processed")))
ensure_dir(OUT_DIR)


def clean_text(s):
    """Remove HTML, escape chars, URLs, and non-ASCII."""
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)             
    s = re.sub(r"\\n|\\r|\\t", " ", s)        
    s = re.sub(r"https?://\S+", " ", s)        
    s = re.sub(r"[^\x00-\x7F]+", " ", s)       
    s = re.sub(r"\s+", " ", s).strip()        
    return s.lower()


def main():
    rows = []
    invalid_count = 0

    with open(RAW_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                invalid_count += 1
                continue

            if not isinstance(obj, dict):
                invalid_count += 1
                continue

            qid = obj.get("question_id") or obj.get("id") or None
            title = clean_text(obj.get("title", ""))
            body = clean_text(obj.get("body", ""))
            tags = ",".join(obj.get("tags", [])) if obj.get("tags") else ""

            rows.append({
                "question_id": qid,
                "title": title,
                "body": body,
                "tags": tags
            })

    if not rows:
        logger.info(" No valid data found after preprocessing.")
        return

    df = pd.DataFrame(rows)
    df["title_length"] = df["title"].str.len()
    df["body_length"] = df["body"].str.len()

    processed_path = OUT_DIR / "processed.parquet"
    df.to_parquet(processed_path, index=False)

    logger.info(f" Wrote processed parquet to: {processed_path}")
    logger.info(f" Skipped {invalid_count} invalid/non-dict lines")


if __name__ == "__main__":
    main()
