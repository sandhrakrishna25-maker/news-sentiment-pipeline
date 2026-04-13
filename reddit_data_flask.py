from flask import Flask, render_template_string
import sqlite3
import pandas as pd

app = Flask(__name__)

DB_PATH = "reddit_pipeline_Sentmnt.db"
TABLE_NAME = "reddit_pipeline_Sentmnt_Table"


def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
    conn.close()

    if "sentiment_score" in df.columns:
        df["sentiment_score"] = pd.to_numeric(df["sentiment_score"], errors="coerce")

    return df


@app.route("/")
def dashboard():
    df = load_data()

    if df.empty:
        return "<h2>No data found in database.</h2>"

    total_rows = len(df)

    sentiment_counts = (
        df["sentiment_category"]
        .value_counts()
        .reset_index()
    )
    sentiment_counts.columns = ["sentiment_category", "count"]

    avg_score = round(df["sentiment_score"].mean(), 3)

    top_positive = df.sort_values(by="sentiment_score", ascending=False).head(5)
    top_negative = df.sort_values(by="sentiment_score", ascending=True).head(5)

    subreddit_summary = (
        df.groupby("subreddit")["sentiment_score"]
        .agg(["count", "mean"])
        .reset_index()
        .sort_values(by="mean", ascending=False)
    )
    subreddit_summary["mean"] = subreddit_summary["mean"].round(3)

    recent_posts = df.sort_values(by="fetched_at", ascending=False).head(20)

    high_positive = df[df["sentiment_score"] >= 0.5].sort_values(by="sentiment_score", ascending=False).head(10)
    high_negative = df[df["sentiment_score"] <= -0.5].sort_values(by="sentiment_score", ascending=True).head(10)

    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Reddit Sentiment Dashboard</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 30px;
                background-color: #f4f6f8;
                color: #222;
            }
            h1, h2 {
                color: #1f3b5b;
            }
            .cards {
                display: flex;
                gap: 20px;
                flex-wrap: wrap;
                margin-bottom: 30px;
            }
            .card {
                background: white;
                padding: 20px;
                border-radius: 12px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.08);
                min-width: 220px;
            }
            .highlight-positive {
                color: green;
                font-weight: bold;
            }
            .highlight-negative {
                color: red;
                font-weight: bold;
            }
            .highlight-neutral {
                color: #666;
                font-weight: bold;
            }
            table {
                border-collapse: collapse;
                width: 100%;
                background: white;
                margin-bottom: 30px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            }
            th, td {
                border: 1px solid #ddd;
                padding: 10px;
                text-align: left;
                vertical-align: top;
            }
            th {
                background-color: #1f3b5b;
                color: white;
            }
            tr:nth-child(even) {
                background-color: #f9f9f9;
            }
            .section {
                margin-bottom: 40px;
            }
            a {
                color: #1a73e8;
                text-decoration: none;
            }
            a:hover {
                text-decoration: underline;
            }
            .small {
                font-size: 13px;
                color: #555;
            }
        </style>
    </head>
    <body>
        <h1>Reddit Sentiment Analysis Dashboard</h1>

        <div class="cards">
            <div class="card">
                <h3>Total Records</h3>
                <p>{{ total_rows }}</p>
            </div>
            <div class="card">
                <h3>Average Sentiment Score</h3>
                <p>{{ avg_score }}</p>
            </div>
            <div class="card">
                <h3>Positive Posts</h3>
                <p>{{ positive_count }}</p>
            </div>
            <div class="card">
                <h3>Negative Posts</h3>
                <p>{{ negative_count }}</p>
            </div>
            <div class="card">
                <h3>Neutral Posts</h3>
                <p>{{ neutral_count }}</p>
            </div>
        </div>

        <div class="section">
            <h2>1. Sentiment Analysis with Titles</h2>
            <table>
                <tr>
                    <th>Subreddit</th>
                    <th>Title</th>
                    <th>Sentiment Score</th>
                    <th>Sentiment Category</th>
                    <th>Published</th>
                </tr>
                {% for _, row in recent_posts.iterrows() %}
                <tr>
                    <td>{{ row['subreddit'] }}</td>
                    <td>
                        <a href="{{ row['link'] }}" target="_blank">{{ row['title'] }}</a>
                    </td>
                    <td>{{ row['sentiment_score'] }}</td>
                    <td>
                        {% if row['sentiment_category'] == 'positive' %}
                            <span class="highlight-positive">{{ row['sentiment_category'] }}</span>
                        {% elif row['sentiment_category'] == 'negative' %}
                            <span class="highlight-negative">{{ row['sentiment_category'] }}</span>
                        {% else %}
                            <span class="highlight-neutral">{{ row['sentiment_category'] }}</span>
                        {% endif %}
                    </td>
                    <td>{{ row['published'] }}</td>
                </tr>
                {% endfor %}
            </table>
        </div>

        <div class="section">
            <h2>2. Sentiment Score Highlights</h2>

            <h3>Top Positive Titles</h3>
            <table>
                <tr>
                    <th>Title</th>
                    <th>Subreddit</th>
                    <th>Score</th>
                </tr>
                {% for _, row in top_positive.iterrows() %}
                <tr>
                    <td><a href="{{ row['link'] }}" target="_blank">{{ row['title'] }}</a></td>
                    <td>{{ row['subreddit'] }}</td>
                    <td class="highlight-positive">{{ row['sentiment_score'] }}</td>
                </tr>
                {% endfor %}
            </table>

            <h3>Top Negative Titles</h3>
            <table>
                <tr>
                    <th>Title</th>
                    <th>Subreddit</th>
                    <th>Score</th>
                </tr>
                {% for _, row in top_negative.iterrows() %}
                <tr>
                    <td><a href="{{ row['link'] }}" target="_blank">{{ row['title'] }}</a></td>
                    <td>{{ row['subreddit'] }}</td>
                    <td class="highlight-negative">{{ row['sentiment_score'] }}</td>
                </tr>
                {% endfor %}
            </table>
        </div>

        <div class="section">
            <h2>3. Sentiment Distribution Summary</h2>
            <table>
                <tr>
                    <th>Sentiment Category</th>
                    <th>Count</th>
                </tr>
                {% for _, row in sentiment_counts.iterrows() %}
                <tr>
                    <td>{{ row['sentiment_category'] }}</td>
                    <td>{{ row['count'] }}</td>
                </tr>
                {% endfor %}
            </table>
        </div>

        <div class="section">
            <h2>4. Average Sentiment by Subreddit</h2>
            <table>
                <tr>
                    <th>Subreddit</th>
                    <th>Total Posts</th>
                    <th>Average Sentiment Score</th>
                </tr>
                {% for _, row in subreddit_summary.iterrows() %}
                <tr>
                    <td>{{ row['subreddit'] }}</td>
                    <td>{{ row['count'] }}</td>
                    <td>{{ row['mean'] }}</td>
                </tr>
                {% endfor %}
            </table>
        </div>

        <div class="section">
            <h2>5. Strong Positive Highlights</h2>
            <table>
                <tr>
                    <th>Title</th>
                    <th>Subreddit</th>
                    <th>Score</th>
                </tr>
                {% for _, row in high_positive.iterrows() %}
                <tr>
                    <td><a href="{{ row['link'] }}" target="_blank">{{ row['title'] }}</a></td>
                    <td>{{ row['subreddit'] }}</td>
                    <td class="highlight-positive">{{ row['sentiment_score'] }}</td>
                </tr>
                {% endfor %}
            </table>
        </div>

        <div class="section">
            <h2>6. Strong Negative Highlights</h2>
            <table>
                <tr>
                    <th>Title</th>
                    <th>Subreddit</th>
                    <th>Score</th>
                </tr>
                {% for _, row in high_negative.iterrows() %}
                <tr>
                    <td><a href="{{ row['link'] }}" target="_blank">{{ row['title'] }}</a></td>
                    <td>{{ row['subreddit'] }}</td>
                    <td class="highlight-negative">{{ row['sentiment_score'] }}</td>
                </tr>
                {% endfor %}
            </table>
        </div>

    </body>
    </html>
    """

    positive_count = int((df["sentiment_category"] == "positive").sum())
    negative_count = int((df["sentiment_category"] == "negative").sum())
    neutral_count = int((df["sentiment_category"] == "neutral").sum())

    return render_template_string(
        html,
        total_rows=total_rows,
        avg_score=avg_score,
        positive_count=positive_count,
        negative_count=negative_count,
        neutral_count=neutral_count,
        sentiment_counts=sentiment_counts,
        subreddit_summary=subreddit_summary,
        recent_posts=recent_posts,
        top_positive=top_positive,
        top_negative=top_negative,
        high_positive=high_positive,
        high_negative=high_negative
    )


if __name__ == "__main__":
    app.run(debug=True)