import os, pandas as pd
from pathlib import Path
from transformers import pipeline
from sentence_transformers import SentenceTransformer
import pinecone
from pymongo import MongoClient
from scripts.utils import logger

PROJECT_ROOT = Path.cwd()
PROCESSED_PATH = Path(os.environ.get("PROCESSED_PATH", str(PROJECT_ROOT / "data" / "processed" / "processed.parquet")))

HF_TOKEN = os.environ.get("HUGGINGFACE_TOKEN")
HF_MODEL = os.environ.get("HF_MODEL_NAME", "cardiffnlp/twitter-roberta-base-sentiment")
PINE_KEY = os.environ.get("PINECONE_API_KEY")
PINE_ENV = os.environ.get("PINECONE_ENVIRONMENT")
PINE_IDX = os.environ.get("PINECONE_INDEX_NAME","so-embeddings")
PINE_DIM = int(os.environ.get("PINECONE_DIM",768))

MONGO_URI = os.environ.get("MONGODB_URI","mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGODB_DB","so_db")
MONGO_COL = os.environ.get("MONGODB_COLLECTION","so_sentiment")

def run():
    logger.info("Loading processed parquet")
    df = pd.read_parquet(PROCESSED_PATH)
    texts = (df["title"].fillna("") + "\n" + df["body"].fillna("")).tolist()
    logger.info(f"Running HF sentiment on {len(texts)} rows (truncated to 512 chars)")
    classifier = pipeline("sentiment-analysis", model=HF_MODEL, tokenizer=HF_MODEL, use_auth_token=HF_TOKEN)
    results = []
    for t in texts:
        try:
            r = classifier(t[:512])[0]
        except Exception as e:
            logger.exception("HF inference failed on text: %s", e)
            r = {"label":"NEU","score":0.0}
        results.append(r)
    df["sent_label"] = [r["label"] for r in results]
    df["sent_score"] = [float(r.get("score",0.0)) for r in results]

    logger.info("Creating embeddings with sentence-transformers")
    embed_model = SentenceTransformer("all-mpnet-base-v2")
    embeddings = embed_model.encode(texts, show_progress_bar=True)

    if PINE_KEY:
        pinecone.init(api_key=PINE_KEY, environment=PINE_ENV)
        if PINE_IDX not in pinecone.list_indexes():
            pinecone.create_index(PINE_IDX, dimension=len(embeddings[0]))
        idx = pinecone.Index(PINE_IDX)
        batch = []
        for i, vec in enumerate(embeddings):
            meta = {
                "question_id": str(df.iloc[i].get("question_id")),
                "title": df.iloc[i].get("title"),
                "sent_label": df.iloc[i].get("sent_label")
            }
            batch.append((str(i), vec.tolist(), meta))
            if len(batch) >= 100:
                idx.upsert(vectors=batch)
                batch = []
        if batch:
            idx.upsert(vectors=batch)
        logger.info("Upserted embeddings to Pinecone index: %s", PINE_IDX)
    else:
        logger.warning("PINECONE_API_KEY not set — skipping upsert")

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COL]
    records = df.to_dict(orient="records")
    for r in records:
        qid = r.get("question_id")
        if qid is None:
            col.insert_one(r)
        else:
            col.update_one({"question_id": qid}, {"$set": r}, upsert=True)
    logger.info("Stored inference results to MongoDB collection %s.%s", MONGO_DB, MONGO_COL)

if __name__ == "__main__":
    run()
