import praw
from praw.models import MoreComments
from datetime import datetime, UTC
import csv
import os
from dotenv import load_dotenv
import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import matplotlib.pyplot as plt
from wordcloud import WordCloud

nltk.download("punkt")
nltk.download("stopwords")
nltk.download("punkt_tab")
# Load the environment variables
cwd = os.getcwd()
load_dotenv(str(cwd)+"\\secrets.env")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
PASSWORD = os.getenv("PASSWORD")
USERNAME = "Huraira7777"
USER_AGENT = os.getenv("USER_AGENT")


def fetch_data(subreddit, limit):
    # Create a Reddit instance
    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        password=PASSWORD,
        username=USERNAME,
        user_agent=USER_AGENT,
    )

    data = reddit.subreddit(subreddit).top(limit=limit)
    json_data = []
    for submission in data:
        title = submission.title
        upvotes = submission.score
        author = submission.author
        subreddit = submission.subreddit.display_name
        post_text = submission.selftext if submission.selftext else "No text"
        # convert to utc
        date = datetime.fromtimestamp(submission.created_utc, UTC).strftime("%Y-%m-%d %H:%M:%S")
        json_data.append({
            "Title": title,
            "Post Text": post_text,
            "Author": author,
            "Date": date,
            "Upvotes": upvotes,
            "Subreddit": subreddit})


    return json_data

def save_to_csv(data, filename):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        headers = list(data[0].keys())
        writer.writerow(headers)
        for row in data:
            writer.writerow(list(row.values()))

def clean_data(input_file, output_file):
    data = pd.read_csv(input_file)
    # Replace empty text column with "No text"
    data["Post Text"] = data["Post Text"].replace("", "No text")
    data.to_csv(output_file)


def summarize_data(input_file):
    data = pd.read_csv(input_file)
    # Get the average number of upvotes
    avg_upvotes = data["Upvotes"].mean()
    print(f"Average number of upvotes: {avg_upvotes}")
    # Get the average number of upvotes per subreddit
    avg_upvotes_per_subreddit = data.groupby("Subreddit")["Upvotes"].mean()
    print(f"Average number of upvotes per subreddit:")
    print(avg_upvotes_per_subreddit)
    num_posts_per_subreddit = data["Subreddit"].value_counts()
    print(f"Number of posts per subreddit:")
    print(num_posts_per_subreddit)

    # Prepare data for word text frequency analysis
    # Remove special characters
    data["Post Text"] = data["Post Text"].str.replace("[^a-zA-Z#]", " ")
    # Tokenize and remove stopwords
    stop_words = set(stopwords.words("english"))
    data["Post Text"] = data["Post Text"].apply(lambda x: " ".join([word for word in word_tokenize(x) if word.lower() not in stop_words]))
    # Remove any special characters
    data["Post Text"] = data["Post Text"].apply(lambda x: " ".join([word for word in x.split(" ") if word.isalnum()]))
    # Get the most common words in the text
    all_words_text = " ".join(data["Post Text"]).split()
    freq = nltk.FreqDist(all_words_text)
    plt.figure(figsize=(12, 6))  # Increase figure size
    freq.plot(20, cumulative=False, label="Word Frequency")
    plt.xticks(rotation=45)
    plt.xlabel('Word')
    plt.ylabel('Frequency')
    plt.legend()
    plt.tight_layout()
    # Save the plot
    plt.savefig("datasets/figures/word_frequency.png")

    # Get the most common words in the title
    data["Title"] = data["Title"].str.replace("[^a-zA-Z#]", " ")
    data["Title"] = data["Title"].apply(lambda x: " ".join([word for word in word_tokenize(x) if word.lower() not in stop_words]))
    # Remove any special characters
    data["Title"] = data["Title"].apply(lambda x: " ".join([word for word in x.split(" ") if word.isalnum()]))
    all_words = " ".join(data["Title"]).split()
    freq = nltk.FreqDist(all_words)
    plt.figure(figsize=(12, 6))  # Increase figure size
    freq.plot(20, cumulative=False, label="Word Frequency")
    plt.xticks(rotation=45)
    plt.xlabel('Word')
    plt.ylabel('Frequency')
    plt.legend()
    plt.tight_layout()
    # Save the plot
    plt.savefig("datasets/figures/title_word_frequency.png")

    # Make a word cloud of the most common words in the text
    word_cloud = WordCloud(width=800, height=400, max_words=150, background_color="white").generate(" ".join(all_words_text))
    plt.figure(figsize=(10, 10), facecolor=None)
    plt.imshow(word_cloud)
    plt.axis("off")
    plt.xlabel('Word')
    plt.ylabel('Frequency')
    plt.legend()
    # Save the plot
    plt.savefig("datasets/figures/word_cloud.png")


if __name__ == "__main__":
    data = fetch_data("remotework+workfromhome", 200)
    save_to_csv(data, "datasets/raw/reddit_posts.csv")
    clean_data("datasets/raw/reddit_posts.csv", "datasets/processed/cleaned_reddit_posts.csv")
    summarize_data("datasets/processed/cleaned_reddit_posts.csv")
