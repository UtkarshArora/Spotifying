
import pandas as pd

df = pd.read_csv("MMTD_MERGED_FINAL.csv")

song_counts = (
    df.groupby(["artist_name", "track_name"])
      .size()
      .reset_index(name="tweet_count")
)

top_tracks = song_counts.sort_values("tweet_count", ascending=False).head(10)
corr = df[["tweet_count", "spotify_popularity"]].corr()

print("Top tracks by tweet volume:")
print(top_tracks)
print("\nCorrelation between tweet count and Spotify popularity:")
print(corr)
