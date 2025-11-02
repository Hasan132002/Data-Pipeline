from pymongo import MongoClient
from typing import List, Dict, Any

class MongoClientWrapper:
    def __init__(self, uri: str, db_name: str, collection_name: str):
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self._init()

    def _init(self):
        self.client = MongoClient(self.uri)
        self.db = self.client[self.db_name]
        self.col = self.db[self.collection_name]

    def insert_many(self, docs: List[Dict[str, Any]]):
        if not docs:
            return
        self.col.insert_many(docs)

    def insert_one(self, doc: Dict[str, Any]):
        return self.col.insert_one(doc)

    def find(self, filter: Dict = {}, limit: int = 100):
        cursor = self.col.find(filter).limit(limit)
        return list(cursor)