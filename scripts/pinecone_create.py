from dotenv import load_dotenv
import os
from pinecone import Pinecone, ServerlessSpec
from pymongo import MongoClient

load_dotenv()
    
pine_key = os.environ.get("PINECONE_API_KEY")
idx = os.environ.get("PINECONE_INDEX_NAME", "so-embeddings")
dim = int(os.environ.get("PINECONE_DIM", 768))

pc = Pinecone(api_key=pine_key)

existing_indexes = [index.name for index in pc.list_indexes()]

if idx not in existing_indexes:
    pc.create_index(
        name=idx,
        dimension=dim,
        metric="cosine",
        spec=ServerlessSpec(cloud="aws", region="us-east-1") 
    )
    print(f" Created Pinecone index: {idx}")
else:
    print(f"Pinecone index already exists: {idx}")

index = pc.Index(idx)
print(f" Connected to index: {idx}")

client = MongoClient("mongodb://localhost:27017")
db = client["so_db"]

if "so_sentiment" not in db.list_collection_names():
    db.create_collection("so_sentiment")
    print(" Created MongoDB collection: so_sentiment")
else:
    print("ℹ MongoDB collection already exists: so_sentiment")
