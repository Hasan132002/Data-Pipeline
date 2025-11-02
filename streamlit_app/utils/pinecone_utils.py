from pinecone import Pinecone, ServerlessSpec
from typing import List


class PineconeClient:
    def __init__(self, api_key: str, environment: str, index_name: str, dim: int):
        self.api_key = api_key
        self.environment = environment
        self.index_name = index_name
        self.dim = dim
        self.pc = Pinecone(api_key=self.api_key)
        self._index = None

    def create_index_if_not_exists(self):
        indexes = [i["name"] for i in self.pc.list_indexes()]
        if self.index_name not in indexes:
            self.pc.create_index(
                name=self.index_name,
                dimension=self.dim,
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region=self.environment),
            )
        self._index = self.pc.Index(self.index_name)

    def upsert(self, ids: List[str], embeddings: List[List[float]]):
        if self._index is None:
            self.create_index_if_not_exists()
        vectors = [{"id": ids[i], "values": embeddings[i]} for i in range(len(ids))]
        batch_size = 100
        for i in range(0, len(vectors), batch_size):
            self._index.upsert(vectors=vectors[i:i + batch_size])

    def query(self, vector, top_k=5):
        if self._index is None:
            self.create_index_if_not_exists()
        res = self._index.query(vector=vector, top_k=top_k)
        results = [{"id": m["id"], "score": m["score"]} for m in res["matches"]]
        return results

    def describe_index(self):
        return self.pc.describe_index(self.index_name)
