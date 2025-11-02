import os
import json
import time
from pathlib import Path
from typing import List, Dict

import streamlit as st
import pandas as pd
from dotenv import load_dotenv

from utils.embedding_utils import embed_texts
from utils.pinecone_utils import PineconeClient
from utils.mongo_utils import MongoClientWrapper
from utils.sentiment_utils import analyze_sentiments
from utils.visualization_utils import plot_sentiment_bar, plot_wordcloud

load_dotenv()

BASE_DIR = Path(__file__).parent
DATA_PATH = BASE_DIR / "data" / "stackoverflow_questions.json"

PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY")
PINECONE_ENV = os.environ.get("PINECONE_ENVIRONMENT")
PINECONE_INDEX = os.environ.get("PINECONE_INDEX_NAME")
PINECONE_DIM = int(os.environ.get("PINECONE_DIM", 768))

MONGO_URI = os.environ.get("MONGODB_URI")
MONGO_DB = os.environ.get("MONGODB_DB")
MONGO_COLLECTION = os.environ.get("MONGODB_COLLECTION")

HF_TOKEN = os.environ.get("HUGGINGFACE_TOKEN")

st.set_page_config(page_title="SO Pipeline Dashboard", layout="wide")

@st.cache_data
def load_json(path: Path) -> List[Dict]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

@st.cache_data(show_spinner=False)
def to_dataframe(items: List[Dict]) -> pd.DataFrame:
    rows = []
    for it in items:
        rows.append({
            "question_id": it.get("question_id"),
            "title": it.get("title"),
            "body": it.get("body"),
            "tags": ",".join(it.get("tags", [])),
            "link": it.get("link"),
            "owner": it.get("owner", {}).get("display_name"),
            "creation_date": it.get("creation_date"),
            "is_answered": it.get("is_answered"),
        })
    df = pd.DataFrame(rows)
    return df

pine_client = PineconeClient(api_key=PINECONE_API_KEY, environment=PINECONE_ENV, index_name=PINECONE_INDEX, dim=PINECONE_DIM)
mongo = MongoClientWrapper(MONGO_URI, MONGO_DB, MONGO_COLLECTION)

st.title("StackOverflow Visualization Dashboard")

col1, col2 = st.columns([2, 1])

with col1:
    st.header("Dataset & Controls")
    st.write("**Source file:**", str(DATA_PATH))

    if DATA_PATH.exists():
        raw = load_json(DATA_PATH)
        st.success(f"Loaded {len(raw)} records from JSON file")
        if st.checkbox("Preview dataset"):
            df = to_dataframe(raw)
            st.dataframe(df.head(200))
    else:
        st.error("Data file not found: place `stackoverflow_questions.json` in the `data/` folder")
        st.stop()

    run_mode = st.radio("Process mode", ["Button-driven", "Auto on startup"], index=0)

    st.markdown("---")
    st.subheader("Embedding & Indexing")
    embed_button = st.button("Compute embeddings & push to Pinecone")

    if run_mode == "Auto on startup":
        embed_button = True

    if embed_button:
        with st.spinner("Computing embeddings — this can take a few minutes for large datasets"):
            texts = [ (it.get('title') or '') + '\n' + (it.get('body') or '') for it in raw]
            ids = [str(it.get('question_id')) for it in raw]

            embeddings = embed_texts(texts, batch_size=16, hf_token=HF_TOKEN)

            st.info(f"Computed {len(embeddings)} embeddings")

            pine_client.create_index_if_not_exists()
            pine_client.upsert(ids, embeddings)
            st.success("Upserted embeddings to Pinecone")

            st.info("Running sentiment analysis...")
            sentiments = analyze_sentiments(texts)

            records = []
            for i, it in enumerate(raw):
                rec = {
                    "question_id": it.get('question_id'),
                    "title": it.get('title'),
                    "body": it.get('body'),
                    "tags": it.get('tags', []),
                    "link": it.get('link'),
                    "owner": it.get('owner', {}).get('display_name'),
                    "embedding_id": ids[i],
                    "sentiment": sentiments[i],
                    "created_at": int(time.time())
                }
                records.append(rec)

            mongo.insert_many(records)
            st.success(f"Inserted {len(records)} records to MongoDB collection `{MONGO_COLLECTION}`")

with col2:
    st.header("Quick Controls")
    st.write("Pinecone index: ", PINECONE_INDEX)
    st.write("MongoDB collection: ", MONGO_COLLECTION)

    if st.button("Pinecone: index info"):
        info = pine_client.describe_index()
        st.json(info)

    if st.button("Mongo: show top 10 docs"):
        docs = mongo.find({}, limit=10)
        st.write(docs)

st.markdown("---")

st.header("Analytics & Search")
cols = st.columns(2)

stored = mongo.find({}, limit=10000)
if stored:
    df_stored = pd.DataFrame(stored)
else:
    df_stored = pd.DataFrame()

with cols[0]:
    st.subheader("Sentiment distribution")
    if not df_stored.empty:
        plot_sentiment_bar(df_stored)
    else:
        st.info("No stored documents in MongoDB yet. Run embedding/upsert step first.")

with cols[1]:
    st.subheader("Wordcloud for titles")
    if not df_stored.empty:
        plot_wordcloud(df_stored["title"].astype(str).tolist())
    else:
        st.info("No stored documents in MongoDB yet.")

st.markdown("---")
st.header("Semantic Search / Q&A")
query = st.text_input("Enter a question or search query")
num_results = st.slider("Number of results", 1, 10, 5)
if st.button("Search") and query:
    q_emb = embed_texts([query], batch_size=1, hf_token=HF_TOKEN)[0]
    results = pine_client.query(q_emb, top_k=num_results)
    hits = []
    for r in results:
        meta = mongo.find({"question_id": int(r['id'])}, limit=1)
        meta = meta[0] if meta else {}
        hits.append({"id": r['id'], "score": r['score'], "title": meta.get('title'), "link": meta.get('link')})
    st.write(hits)

    st.subheader("Rudimentary answer (concat + summarize)")
    concat = "\n\n".join([mongo.find({"question_id": int(h['id'])}, limit=1)[0].get('body', '') for h in hits if mongo.find({"question_id": int(h['id'])}, limit=1)])
    if concat:
        st.text_area("Retrieved context (top docs)", concat[:10000], height=300)
    else:
        st.info("No context found for top hits")

st.markdown("---")
st.caption("Built for: D:\\BDA\\projects\\so-pipeline\\streamlit_app")