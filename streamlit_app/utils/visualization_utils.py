import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud


def plot_sentiment_bar(df: pd.DataFrame):
    counts = df['sentiment'].apply(lambda s: s.get('label') if isinstance(s, dict) else s).value_counts()
    st.bar_chart(counts)


def plot_wordcloud(texts: list):
    joined = " ".join(texts)
    wc = WordCloud(width=800, height=400, collocations=False).generate(joined)
    fig, ax = plt.subplots(figsize=(10,4))
    ax.imshow(wc, interpolation='bilinear')
    ax.axis('off')
    st.pyplot(fig)