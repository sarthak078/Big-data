#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 2  |  Task 2.2
  RFM Mapper: Compute per-user RFM features
  E-Commerce Clickstream Dataset
=============================================================================
  For each event, emit:
    user_id TAB timestamp TAB event_type TAB price TAB session_id
=============================================================================
"""

import sys
import csv

SCHEMA = [
    'event_id','user_id','timestamp','session_id','event_type',
    'product_id','category','price','device_type','traffic_source'
]

def is_header(row):
    return len(row) > 0 and row[0].strip().lower() == 'event_id'

def get_indices(header):
    return {col.strip(): idx for idx, col in enumerate(header)}

def main():
    reader = csv.reader(sys.stdin)
    first  = next(reader, None)
    if first is None:
        return

    if is_header(first):
        indices = get_indices(first)
        rows    = reader
    else:
        indices = get_indices(SCHEMA)
        rows    = [first] + list(reader)

    try:
        u  = indices['user_id']
        t  = indices['timestamp']
        et = indices['event_type']
        pr = indices['price']
        si = indices['session_id']
    except KeyError as e:
        sys.stderr.write(f"Schema error: {e}\n")
        return

    max_idx = max(u, t, et, pr, si)

    for row in rows:
        try:
            if len(row) <= max_idx:
                continue
            user_id    = row[u].strip()
            timestamp  = row[t].strip()
            event_type = row[et].strip()
            price      = row[pr].strip()
            session_id = row[si].strip()

            if not user_id or not timestamp:
                continue

            price_val = float(price) if price else 0.0

            print(f"{user_id}\t{timestamp}\t{event_type}\t"
                  f"{price_val:.2f}\t{session_id}")
        except Exception:
            continue

if __name__ == "__main__":
    main()
