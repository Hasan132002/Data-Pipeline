import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1] 
input_path = PROJECT_ROOT / "data" / "raw.jsonl"
output_path = PROJECT_ROOT / "data" / "raw_clean.jsonl"

print(f" Input: {input_path}")
print(f" Output: {output_path}")

if not input_path.exists():
    raise FileNotFoundError(f"Input file not found: {input_path}")

with open(input_path, "r", encoding="utf-8") as fin, open(output_path, "w", encoding="utf-8") as fout:
    count = 0
    for line in fin:
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                obj = json.loads(line)
                json.dump(obj, fout)
                fout.write("\n")
                count += 1
            except json.JSONDecodeError:
                continue
print(f" Cleaned data written to {output_path} ({count} valid records)")
