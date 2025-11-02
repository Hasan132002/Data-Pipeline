import streamlit as st
import pandas as pd
import sqlalchemy
import plotly.express as px
import os

DB_URL = os.environ.get("SO_DB_URL", "postgresql://so_user:so_pass@localhost:5432/so_db")

st.title("StackOverflow - Sentiment Dashboard")

engine = sqlalchemy.create_engine(DB_URL)
query = "SELECT sent_label, COUNT(*) as cnt FROM so_sentiment GROUP BY sent_label"
df = pd.read_sql(query, engine)

fig = px.pie(df, names="sent_label", values="cnt", title="Sentiment distribution")
st.plotly_chart(fig)

st.subheader("Sample rows")
sample = pd.read_sql("SELECT question_id, title, sent_label, sent_score FROM so_sentiment LIMIT 50", engine)
st.write(sample)