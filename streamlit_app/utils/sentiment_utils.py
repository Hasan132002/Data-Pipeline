from transformers import pipeline
from typing import List

_pipeline = None

def get_pipeline():
    global _pipeline
    if _pipeline is None:
        _pipeline = pipeline("sentiment-analysis")
    return _pipeline


def analyze_sentiments(texts: List[str]) -> List[dict]:
    pipe = get_pipeline()
    results = pipe(texts, truncation=True)
    return results