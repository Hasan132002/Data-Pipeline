from typing import List
from sentence_transformers import SentenceTransformer
import numpy as np

_model_cache = {}

def get_model(model_name: str = 'sentence-transformers/all-mpnet-base-v2', hf_token: str = None):
    key = f"{model_name}"
    if key not in _model_cache:
        _model_cache[key] = SentenceTransformer(model_name, use_auth_token=hf_token)
    return _model_cache[key]


def embed_texts(texts: List[str], batch_size: int = 16, hf_token: str = None) -> List[List[float]]:
    model = get_model(hf_token=hf_token)
    embeddings = model.encode(texts, batch_size=batch_size, show_progress_bar=True, convert_to_numpy=True)
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    embeddings = embeddings / norms
    return embeddings.tolist()