
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 6:
        continue

    tweetId, userId, artistId, trackId, timestamp, weekday = parts
    print("STAT\tTOTAL_TWEETS\t1")
    print(f"USER\t{userId}\t1")
    print(f"TRACK\t{trackId}\t1")
    print(f"ARTIST\t{artistId}\t1")
