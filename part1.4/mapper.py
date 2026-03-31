#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 1C  |  Task 1.4
  Mapper: Vehicle Risk Profiling
  NYC Parking Violations Dataset
=============================================================================
  Emits two record types:
    C  TAB  color    TAB  1   (for color risk analysis)
    P  TAB  plate_id TAB  1   (for repeat offender analysis)
=============================================================================
"""

import sys
import csv

INVALID_PLATES = {'BLANKPLATE', 'BLANK', 'NOPLATE', 'MISSING', 'UNKNOWN', '0', '99', '00'}

def main():
    reader = csv.reader(sys.stdin)
    header = next(reader, None)
    if not header:
        return

    try:
        idx_plate  = header.index('plate_id')
        idx_color  = header.index('vehicle_color')
        idx_state  = header.index('registration_state')
    except ValueError:
        idx_plate  = 1
        idx_color  = 33
        idx_state  = 2

    max_idx = max(idx_plate, idx_color, idx_state)

    for row in reader:
        if len(row) <= max_idx:
            continue

        plate_id = row[idx_plate].strip()
        color    = row[idx_color].strip()
        state    = row[idx_state].strip()

        # Skip invalid plates
        if not plate_id or plate_id.upper() in INVALID_PLATES:
            continue

        # Emit color record (colors already standardized by Task 1.1)
        if color and color != 'UNKNOWN':
            print(f"C\t{color}\t1")

        # Emit plate record
        print(f"P\t{plate_id}\t1")

if __name__ == "__main__":
    main()
