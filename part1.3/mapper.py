#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 1B  |  Task 1.3
  Mapper: Geographic Hotspot Identification (Top-K Pattern)
  NYC Parking Violations Dataset
=============================================================================
"""

import sys
import csv
import heapq
from collections import defaultdict

K = 20

FINE_MAP = {
    '5': 150, '7': 150, '9': 138, '10': 115, '12': 150,
    '14': 115, '16': 115, '17': 115, '18': 115, '19': 115,
    '20': 65, '21': 45, '22': 115, '31': 115, '36': 150,
    '37': 35, '38': 35, '40': 115, '42': 35, '45': 115,
    '46': 115, '47': 115, '48': 115, '50': 115, '51': 115,
    '53': 115, '61': 115, '62': 115, '66': 115, '67': 115,
    '68': 115, '69': 35, '70': 50, '71': 50, '74': 50,
    '75': 50, '76': 50, '78': 50, '80': 115, '82': 115,
    '84': 115, '87': 115, '98': 115
}

AREA_MAP = {
    '10010': 'Gramercy Park Area',     '10110': 'Chelsea Area',
    '10210': 'Greenwich Village Area', '10310': 'SoHo Area',
    '10410': 'Midtown East Area',      '10510': 'Midtown West Area',
    '10610': 'Upper West Side Area',   '10710': 'Upper East Side Area',
    '10810': 'Harlem Area',            '10910': 'Financial District Area',
    '11010': 'Brooklyn Heights Area',  '11110': 'Williamsburg Area',
    '11210': 'Park Slope Area',        '11310': 'Astoria Area',
    '11410': 'Long Island City Area',  '11510': 'Flushing Area',
    '11610': 'Jackson Heights Area',   '11710': 'Forest Hills Area',
    '11810': 'Coney Island Area',      '11910': 'Staten Island Area',
    '13495': 'Flatbush Avenue Area',   '13665': 'Queens Boulevard Area',
    '25645': 'Atlantic Avenue Area',   '10350': 'Eastern Parkway Area',
    '13610': 'Broadway Area',          '13550': 'Nostrand Avenue Area',
    '35710': 'Fulton Street Area',     '30640': 'Jamaica Avenue Area',
    '25390': 'Madison Avenue Area',    '22690': 'Park Avenue Area',
    '34570': 'Lexington Avenue Area',  '34310': 'Amsterdam Avenue Area',
    '17210': 'St. Nicholas Avenue Area','12410': 'Convent Avenue Area',
    '34110': '7th Avenue Area',        '40404': 'School Zone Camera Area',
}

def get_fine(violation_code):
    if not violation_code:
        return 50
    return FINE_MAP.get(str(violation_code).strip(), 50)

def clean_street_name(street_name):
    if not street_name or street_name == '' or street_name == '0':
        return None
    street_name = street_name.strip().strip('"')
    words = street_name.split()
    street_name = ' '.join(words).title()
    return street_name

def main():
    local_counts  = defaultdict(int)
    local_revenue = defaultdict(float)

    reader = csv.reader(sys.stdin)
    header = next(reader, None)
    if not header:
        return

    try:
        idx_street_code    = header.index('street_code1')
        idx_street_name    = header.index('street_name')
        idx_violation_code = header.index('violation_code')
    except ValueError:
        idx_street_code    = 9
        idx_street_name    = 24
        idx_violation_code = 5

    max_idx = max(idx_street_code, idx_street_name, idx_violation_code)

    for row in reader:
        if len(row) <= max_idx:
            continue

        street_code    = row[idx_street_code].strip()
        street_name    = row[idx_street_name].strip().strip('"')
        violation_code = row[idx_violation_code].strip()

        if not street_code or street_code in ('0', ''):
            continue

        clean_name = clean_street_name(street_name)
        if clean_name is None:
            clean_name = AREA_MAP.get(street_code, f'Area {street_code}')

        location_key = f"{street_code}|{clean_name}"
        local_counts[location_key]  += 1
        local_revenue[location_key] += get_fine(violation_code)

    # Local Top-K using min-heap
    top_k = []
    for location, count in local_counts.items():
        revenue = local_revenue[location]
        heapq.heappush(top_k, (count, location, revenue))
        if len(top_k) > K:
            heapq.heappop(top_k)

    for count, location, revenue in top_k:
        print(f"{location}\t{count}\t{revenue}")

if __name__ == "__main__":
    main()
