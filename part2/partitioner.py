#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 2  |  Task 2.1
  Custom Partitioner: Secondary Sort Pattern
  E-Commerce Clickstream Dataset
=============================================================================
  Partitions by user_id ONLY (first field before TAB).
  This ensures ALL events for the same user go to the SAME reducer,
  regardless of timestamp — the key requirement of Secondary Sort.

  Without this, events for the same user could end up on different
  reducers (because the full composite key user_id+timestamp is used
  for partitioning by default), breaking session reconstruction.

  Usage in Hadoop Streaming:
    -partitioner partitioner.py
=============================================================================
"""

import sys
import hashlib

def main():
    # Read number of reducers from first line (Hadoop passes it)
    num_reducers_line = sys.stdin.readline().strip()
    try:
        num_reducers = int(num_reducers_line)
    except ValueError:
        num_reducers = 2  # default

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        # Extract user_id (first field before TAB)
        parts   = line.split('\t')
        user_id = parts[0] if parts else ''

        # Hash user_id to determine reducer
        # All events for same user → same reducer
        hash_val   = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
        reducer_id = hash_val % num_reducers

        print(f"{reducer_id}\t{line}")

if __name__ == "__main__":
    main()
