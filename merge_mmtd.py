
import pandas as pd
import re

def normalize_text(s):
    if pd.isna(s):
        return ""
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s]", "", s)
    return re.sub(r"\s+", " ", s)

tweets = pd.read_csv("tweets_with_names_sample.csv")
spotify = pd.read_csv("Spotify_cleaned.csv")
hot100 = pd.read_csv("hot100_cleaned.csv")
artists = pd.read_csv("top_10000_artists.csv")

for df in [tweets, spotify, hot100]:
    df["artist_name"] = df["artist_name"].apply(normalize_text)
    df["track_name"] = df["track_name"].apply(normalize_text)

artists["artist_name"] = artists["artist_name"].apply(normalize_text)

merged = tweets.merge(spotify, on=["artist_name", "track_name"], how="left")
merged = merged.merge(hot100, on=["artist_name", "track_name"], how="left")
merged = merged.merge(artists, on="artist_name", how="left")

merged.to_csv("MMTD_MERGED_FINAL.csv", index=False)
