
#!/usr/bin/env python3
import sys

current_key = None
total = 0

for line in sys.stdin:
    key, value, count = line.strip().split("\t")
    count = int(count)
    composite = f"{key}|{value}"

    if composite != current_key:
        if current_key:
            k, v = current_key.split("|")
            print(f"{k}\t{v}\t{total}")
        current_key = composite
        total = 0

    total += count

if current_key:
    k, v = current_key.split("|")
    print(f"{k}\t{v}\t{total}")
