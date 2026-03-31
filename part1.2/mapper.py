#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 1B  |  Task 1.2
  Mapper: Peak Enforcement Time Analysis
  NYC Parking Violations Dataset
=============================================================================
  Reads the cleaned CSV (output of Task 1.1).
  Emits:  hour_of_day  →  1
  (one line per valid record that has a parseable hour)
=============================================================================
"""

import sys
import csv

def main():
    reader = csv.reader(sys.stdin)
    header = next(reader, None)
    if not header:
        return

    # Resolve column index for hour_of_day (last column added by Task 1.1)
    try:
        idx_hour = header.index('hour_of_day')
    except ValueError:
        idx_hour = len(header) - 1   # fallback: last column

    for row in reader:
        if len(row) <= idx_hour:
            continue
        hour_str = row[idx_hour].strip()
        try:
            hour = int(hour_str)
        except ValueError:
            continue
        if 0 <= hour <= 23:
            print(f"{hour:02d}\t1")

if __name__ == "__main__":
    main()
