import os
import requests
import pandas as pd
import sqlite3
from datetime import datetime

API_KEY = os.getenv("NEWS_API_KEY")

def fetch_news():
    if not API_KEY:
        raise ValueError("NEWS_API_KEY is not set.")

    url = "https://newsapi.org/v2/everything"

    params = {
        "q": "technology OR AI OR business OR health",
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 100
    }

    headers = {"X-Api-Key": API_KEY}

    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()

    if data.get("status") != "ok":
        raise ValueError(f"API error: {data}")

    articles = data.get("articles", [])
    fetch_time = datetime.utcnow().isoformat()

    rows = []
    for article in articles:
        rows.append({
            "title": article.get("title"),
            "description": article.get("description"),
            "content": article.get("content"),
            "published_at": article.get("publishedAt"),
            "source": article.get("source", {}).get("name"),
            "author": article.get("author"),
            "url": article.get("url"),
            "fetched_at": fetch_time
        })

    return pd.DataFrame(rows)

def save_to_db(new_df):
    conn = sqlite3.connect("news_data.db")

    try:
        old_df = pd.read_sql("SELECT * FROM news_table", conn)
        combined = pd.concat([old_df, new_df], ignore_index=True)
    except Exception:
        combined = new_df

    combined = combined.drop_duplicates(subset=["url"])
    combined.to_sql("news_table", conn, if_exists="replace", index=False)
    conn.close()

    return combined

def main():
    new_data = fetch_news()
    print("Fetched:", len(new_data))

    all_data = save_to_db(new_data)
    print("Total stored:", len(all_data))

    print(all_data.head())

if __name__ == "__main__":
    main()
