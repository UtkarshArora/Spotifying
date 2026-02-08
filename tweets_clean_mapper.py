
#!/usr/bin/env python3
import sys
from datetime import datetime

for line in sys.stdin:
    parts = line.strip().split()
    if len(parts) < 5:
        continue

    tweetId, userId, artistId, trackId = parts[0], parts[1], parts[2], parts[3]
    timestamp = " ".join(parts[4:])

    if not (tweetId.isdigit() and userId.isdigit() and artistId.isdigit() and trackId.isdigit()):
        continue

    try:
        dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    except:
        continue

    weekday = dt.weekday()
    print(f"{tweetId}\t{userId}\t{artistId}\t{trackId}\t{timestamp}\t{weekday}")
