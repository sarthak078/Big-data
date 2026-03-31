#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 1C  |  Task 1.5
  Mapper: Revenue Analysis by Violation Type (Map-Side Join)
  NYC Parking Violations Dataset
=============================================================================
  Pattern: Map-Side Join
    The fine lookup table is loaded INSIDE the mapper (not from HDFS).
    This avoids a reduce-side join and is more efficient for small
    lookup tables joined against large datasets.

  Output format:
    violation_code TAB 1 TAB fine_amount TAB violation_description
=============================================================================
"""

import sys
import csv

# ---------------------------------------------------------------------------
# Fine lookup table — loaded in mapper (this IS the Map-Side Join)
# Small lookup table hardcoded / loaded at map time
# In production this would be loaded from Distributed Cache
# ---------------------------------------------------------------------------

FINE_LOOKUP = {
    '5':  (150, 'Bus Lane Violation'),
    '7':  (150, 'Failure to Display Muni Meter Receipt'),
    '9':  (138, 'Obstructing Traffic/Intersection'),
    '10': (115, 'No Standing - Day/Time Limits'),
    '12': (150, 'No Standing - Snow Emergency'),
    '14': (115, 'No Standing - Except Trucks'),
    '16': (115, 'No Standing - Except Authorized'),
    '17': (115, 'No Standing - Bus Stop'),
    '18': (115, 'No Standing - Hotel Loading'),
    '19': (115, 'No Standing - Fire Zone'),
    '20': (65,  'No Parking - Day/Time Limits'),
    '21': (45,  'No Parking - Street Cleaning'),
    '22': (115, 'No Standing - Except Taxi'),
    '31': (115, 'No Standing - Commercial Meter Zone'),
    '36': (150, 'Exceeding Posted Speed Limit'),
    '37': (35,  'Parking Meter Violation - Overtime'),
    '38': (35,  'Failing to Display Muni Meter Receipt'),
    '40': (115, 'No Standing - Except Authorized Vehicle'),
    '42': (35,  'Expired Muni Meter'),
    '45': (115, 'No Standing - Taxi Stand'),
    '46': (115, 'Double Parking'),
    '47': (115, 'Double Parking - Street Cleaning'),
    '48': (115, 'Bike Lane Violation'),
    '50': (115, 'No Standing - Crane/Derrick'),
    '51': (115, 'No Standing - Restricted Highway'),
    '53': (115, 'No Standing - Safety Zone'),
    '61': (115, 'No Parking - Night Time'),
    '62': (115, 'No Parking - Day/Time Limits (Specific)'),
    '66': (115, 'Detached Trailer'),
    '67': (115, 'Blocking Pedestrian Ramp'),
    '68': (115, 'No Standing - Driveway'),
    '69': (35,  'Failure to Pay Meter'),
    '70': (50,  'Parking on Sidewalk'),
    '71': (50,  'Overnight Parking Prohibited'),
    '74': (50,  'Crosswalk Violation'),
    '75': (50,  'No Parking - Arterial Highway'),
    '76': (50,  'No Parking - All Times'),
    '78': (50,  'No Parking - Except Authorized'),
    '80': (115, 'No Standing - Opposite Street Cleaning'),
    '82': (115, 'Blocking Hydrant'),
    '84': (115, 'No Standing - Opposite Stop Sign'),
    '87': (115, 'Blocking Driveway'),
    '98': (115, 'No Standing - Off-Street Parking'),
}

DEFAULT_FINE        = 50
DEFAULT_DESCRIPTION = 'Other Violation'

def get_fine_info(violation_code):
    """Map-Side Join: look up fine amount and description for violation code."""
    if not violation_code:
        return DEFAULT_FINE, DEFAULT_DESCRIPTION
    return FINE_LOOKUP.get(str(violation_code).strip(),
                           (DEFAULT_FINE, DEFAULT_DESCRIPTION))

def main():
    reader = csv.reader(sys.stdin)
    header = next(reader, None)
    if not header:
        return

    try:
        idx_violation_code = header.index('violation_code')
    except ValueError:
        idx_violation_code = 5

    for row in reader:
        if len(row) <= idx_violation_code:
            continue

        violation_code = row[idx_violation_code].strip()
        if not violation_code:
            continue

        # MAP-SIDE JOIN: join violation code with fine lookup table
        fine, description = get_fine_info(violation_code)

        # Emit: violation_code TAB count TAB fine TAB description
        print(f"{violation_code}\t1\t{fine}\t{description}")

if __name__ == "__main__":
    main()
