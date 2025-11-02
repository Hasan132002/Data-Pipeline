import os, json, logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("so_pipeline")

def ensure_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)
    return str(Path(path).resolve())

def read_json_lines(path, limit=None):
    data = []
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            data.append(obj)
            if limit and i+1 >= limit:
                break
    return data