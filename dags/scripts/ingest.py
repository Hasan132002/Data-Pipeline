import os, json, time
from pathlib import Path
# from scripts.utils import ensure_dir, logger, read_json_lines
from .utils import ensure_dir, logger, read_json_lines

PROJECT_ROOT = Path.cwd()
RAW_JSON = Path(os.environ.get("RAW_JSON", str(PROJECT_ROOT / "stackoverflow_questions.json")))
OUT_DIR = Path(os.environ.get("OUT_DIR", str(PROJECT_ROOT / "data")))
ensure_dir(OUT_DIR)

def main(limit=None):
    logger.info(f"Reading raw JSON from {RAW_JSON}")
    items = read_json_lines(RAW_JSON, limit=limit)
    logger.info(f"Read {len(items)} records")
    out_file = OUT_DIR / "raw.jsonl"
    with open(out_file, "w", encoding="utf-8") as f:
        for obj in items:
            f.write(json.dumps(obj) + "\n")
    logger.info(f"Wrote raw JSONL to {out_file}")

if __name__ == "__main__":
    main()
