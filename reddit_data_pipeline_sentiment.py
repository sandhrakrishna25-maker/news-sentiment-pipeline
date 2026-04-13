
import requests
import feedparser
import pandas as pd
import sqlite3
from datetime import datetime
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

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

# Download VADER lexicon
try:
    nltk.data.find("sentiment/vader_lexicon.zip")
except LookupError:
    nltk.download("vader_lexicon")

sia = SentimentIntensityAnalyzer()


def get_sentiment(text):
    score = sia.polarity_scores(text)["compound"]

    if score >= 0.05:
        category = "positive"
    elif score <= -0.05:
        category = "negative"
    else:
        category = "neutral"

    return score, category


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
    # Connect to DB, saved in current folder
    conn = sqlite3.connect("reddit_pipeline_Sentmnt.db")

    try:
        old_df = pd.read_sql("SELECT * FROM reddit_pipeline_Sentmnt_Table", conn)
        combined = pd.concat([old_df, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["title", "link"])
    except:
        combined = df.copy()

    # Save back to same DB table name
    combined.to_sql("reddit_pipeline_Sentmnt_Table", conn, if_exists="replace", index=False)

    # Save CSV file in same location
    combined.to_csv("reddit_pipeline_stmnt.csv", index=False)

    conn.close()


def main():
    df = fetch_reddit_rss()
    print("Fetched:", len(df))

    # Remove duplicates from data frame
    df = df.drop_duplicates(subset=["title", "link"])

    # Sentiment analysis using title + summary fields
    df["text_for_sentiment"] = df["title"].fillna("") + " " + df["summary"].fillna("")
    df[["sentiment_score", "sentiment_category"]] = df["text_for_sentiment"].apply(
        lambda x: pd.Series(get_sentiment(x))
    )
    df.drop(columns=["text_for_sentiment"], inplace=True)

    save_to_db(df)


if __name__ == "__main__":
    main()
    ##Switched from News API to Reddit RSS for live interval-based data fetching