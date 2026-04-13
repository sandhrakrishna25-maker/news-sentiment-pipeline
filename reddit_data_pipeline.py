import requests
import feedparser
import pandas as pd
import sqlite3
from datetime import datetime

RSS_FEEDS = {
    "news": "https://www.reddit.com/r/news/new/.rss",
    "worldnews": "https://www.reddit.com/r/worldnews/new/.rss",
    "technology": "https://www.reddit.com/r/technology/new/.rss",
    "MachineLearning": "https://www.reddit.com/r/MachineLearning/new/.rss",
    "artificial": "https://www.reddit.com/r/artificial/new/.rss",
    "programming": "https://www.reddit.com/r/programming/.rss",
    "datascience": "https://www.reddit.com/r/datascience/.rss",
    "business": "https://www.reddit.com/r/business/.rss",
    "stocks": "https://www.reddit.com/r/stocks/.rss",
    "economics": "https://www.reddit.com/r/economics/.rss",
    "startups": "https://www.reddit.com/r/startups/.rss",
    "science": "https://www.reddit.com/r/science/.rss",
    "futurology": "https://www.reddit.com/r/Futurology/.rss",
    "privacy": "https://www.reddit.com/r/privacy/.rss",
    "cybersecurity": "https://www.reddit.com/r/cybersecurity/.rss"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RedditRSSCollector/1.0)"
}

def fetch_reddit_rss():
    rows = []
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for subreddit, url in RSS_FEEDS.items():
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)

            if resp.status_code != 200:
                print(f"Failed: {subreddit}")
                continue

            feed = feedparser.parse(resp.text)

            for entry in feed.entries:
                rows.append({
                    "subreddit": subreddit,
                    "title": entry.get("title", ""),
                    "link": entry.get("link") or entry.get("id") or "",
                    "published": entry.get("published", ""),
                    "summary": entry.get("summary", ""),
                    "fetched_at": fetched_at
                })

        except Exception as e:
            print("Error:", e)

    return pd.DataFrame(rows)


def save_to_db(df):
    conn = sqlite3.connect("/home/ubuntu/news-sentiment-pipeline/reddit_pipeline.db")

    try:
        old_df = pd.read_sql("SELECT * FROM reddit_raw_data", conn)
        combined = pd.concat([old_df, df], ignore_index=True)
    except:
        combined = df.copy()

    
    df.to_sql("reddit_raw_data", conn, if_exists="append", index=False)

    conn.close()


def main():
    df = fetch_reddit_rss()
    print("Fetched:", len(df))
    save_to_db(df)


if __name__ == "__main__":
    main()
    ##Switched from News API to Reddit RSS for live interval-based data fetching
