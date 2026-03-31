#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 2  |  Task 2.1
  Mapper: User Session Reconstruction (Secondary Sort)
  E-Commerce Clickstream Dataset
=============================================================================
  Secondary Sort Pattern:
    Composite key = user_id TAB timestamp
    - Partitioner uses ONLY user_id → all events for same user
      go to same reducer
    - Comparator sorts by user_id THEN timestamp ascending
    - Guarantees chronological order per user at reducer

  Output:
    user_id TAB timestamp TAB event_type TAB price TAB device TAB source
=============================================================================
"""

import sys
import csv

# Schema definition — matches clickstream CSV header
SCHEMA = [
    'event_id', 'user_id', 'timestamp', 'session_id', 'event_type',
    'product_id', 'category', 'price', 'device_type', 'traffic_source'
]

def is_header(row):
    return len(row) > 0 and row[0].strip().lower() == 'event_id'

def get_indices(header):
    return {col.strip(): idx for idx, col in enumerate(header)}

def main():
    reader   = csv.reader(sys.stdin)
    first    = next(reader, None)
    if first is None:
        return

    if is_header(first):
        indices   = get_indices(first)
        rows      = reader
    else:
        indices   = get_indices(SCHEMA)
        rows      = [first] + list(reader)

    try:
        u  = indices['user_id']
        t  = indices['timestamp']
        et = indices['event_type']
        pr = indices['price']
        dv = indices['device_type']
        sr = indices['traffic_source']
    except KeyError as e:
        sys.stderr.write(f"Schema error: {e}\n")
        return

    max_idx = max(u, t, et, pr, dv, sr)

    for row in rows:
        try:
            if len(row) <= max_idx:
                continue
            user_id   = row[u].strip()
            timestamp = row[t].strip()
            etype     = row[et].strip()
            price     = row[pr].strip()
            device    = row[dv].strip()
            source    = row[sr].strip()

            if not user_id or not timestamp:
                continue

            price_val = float(price) if price else 0.0

            # Composite key: user_id TAB timestamp
            # Hadoop will sort by BOTH fields
            # Partitioner will use ONLY user_id (field 1)
            print(f"{user_id}\t{timestamp}\t{etype}\t"
                  f"{price_val:.2f}\t{device}\t{source}")
        except Exception:
            continue

if __name__ == "__main__":
    main()
