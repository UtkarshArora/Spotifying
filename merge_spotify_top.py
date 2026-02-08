import pandas as pd


spotify = pd.read_csv("Spotify_cleaned.csv")



def extract_primary_artist(s):
    if pd.isna(s):
        return None
    s = str(s).strip()
    
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    parts = [p.strip(" '\"") for p in s.split(",") if p.strip(" '\"")]
    return parts[0] if parts else None


spotify["primary_artist"] = spotify["artists"].apply(extract_primary_artist)

top_artists = pd.read_csv(
    "top_10000_artists.csv", header=None, names=["artist_name", "artist_listeners"]
)

top_songs = pd.read_csv(
    "top_10000_songs.csv",
    header=None,
    names=["track_name", "artist_name", "song_release_date", "song_listens"],
)


merged_tracks = spotify.merge(
    top_songs,
    left_on=["name", "primary_artist"],
    right_on=["track_name", "artist_name"],
    how="left",  
)

merged_all = merged_tracks.merge(
    top_artists, left_on="primary_artist", right_on="artist_name", how="left"
)

merged_all = merged_all.drop(
    columns=["track_name", "artist_name_x", "artist_name_y"], errors="ignore"
)

merged_all.to_csv("merged_spotify_top.csv", index=False)

print("Saved merged_spotify_top.csv with shape:", merged_all.shape)
