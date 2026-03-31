#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 1C  |  Task 1.4
  Reducer: Vehicle Risk Profiling
  NYC Parking Violations Dataset
=============================================================================
"""

import sys
from collections import defaultdict

# Known standard colors — anything else goes into OTHER
STANDARD_COLORS = {
    'BLACK', 'WHITE', 'GRAY', 'RED', 'BLUE', 'GREEN',
    'YELLOW', 'TAN', 'BROWN', 'SILVER', 'ORANGE', 'PURPLE',
    'GOLD', 'BEIGE', 'MAROON', 'NAVY', 'CREAM'
}

def main():
    color_counts = defaultdict(int)
    plate_counts = defaultdict(int)

    # ── Aggregate ────────────────────────────────────────────────────────────
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split('\t')
        if len(parts) != 3:
            continue
        record_type = parts[0]
        key         = parts[1]
        try:
            count = int(parts[2])
        except ValueError:
            continue

        if record_type == 'C':
            color = key if key in STANDARD_COLORS else 'OTHER'
            color_counts[color] += count
        elif record_type == 'P':
            plate_counts[key] += count

    # =========================================================================
    # COLOR RISK ANALYSIS
    # =========================================================================
    total_violations = sum(color_counts.values())
    num_colors       = len([c for c in color_counts if c != 'OTHER'])
    expected_pct     = 100.0 / num_colors if num_colors else 1.0
    sorted_colors    = sorted(color_counts.items(), key=lambda x: x[1], reverse=True)

    print("\n" + "="*60)
    print("TASK 1.4 — VEHICLE RISK PROFILING")
    print("="*60)

    print("\n" + "="*60)
    print("COLOR RISK ANALYSIS")
    print("="*60)

    for color, count in sorted_colors:
        if color == 'OTHER':
            continue
        pct        = count / total_violations * 100 if total_violations else 0
        risk_score = pct / expected_pct
        if count >= 1_000_000:
            tickets_str = f"{count/1_000_000:.1f}M"
        else:
            tickets_str = f"{count:,}"
        print(f"{color} vehicles: {tickets_str} tickets "
              f"({pct:.1f}% of total) - Risk Score: {risk_score:.2f}")

    # Print OTHER at the end
    if 'OTHER' in color_counts:
        other_count = color_counts['OTHER']
        other_pct   = other_count / total_violations * 100 if total_violations else 0
        print(f"OTHER vehicles: {other_count:,} tickets "
              f"({other_pct:.1f}% of total) - unrecognized color values")

    print(f"\nTotal violations analyzed: {total_violations:,}")
    print("\nRisk Score Explanation:")
    print("  > 1.0 = Higher violation rate than average")
    print("  = 1.0 = Average violation rate")
    print("  < 1.0 = Lower violation rate than average")

    # =========================================================================
    # REPEAT OFFENDER PATTERNS
    # =========================================================================
    range_5_10    = 0
    range_11_20   = 0
    range_21_plus = 0
    top_plate     = None
    top_count     = 0

    for plate, count in plate_counts.items():
        if   5 <= count <= 10:  range_5_10    += 1
        elif 11 <= count <= 20: range_11_20   += 1
        elif count >= 21:       range_21_plus += 1
        if count > top_count:
            top_count = count
            top_plate = plate

    total_vehicles   = len(plate_counts)
    repeat_offenders = range_5_10 + range_11_20 + range_21_plus

    print("\n" + "="*60)
    print("REPEAT OFFENDER PATTERNS")
    print("="*60)
    print(f"5-10 violations:  {range_5_10:,} vehicles")
    print(f"11-20 violations: {range_11_20:,} vehicles")
    print(f"21+ violations:   {range_21_plus:,} vehicles (chronic offenders)")
    print(f"Top offender: Plate {top_plate} with {top_count:,} violations")

    print("\n" + "="*60)
    print("SUMMARY STATISTICS")
    print("="*60)
    print(f"Total unique vehicles            : {total_vehicles:,}")
    print(f"Total violations analyzed        : {total_violations:,}")
    print(f"Repeat offenders (5+ violations) : {repeat_offenders:,} "
          f"({repeat_offenders/total_vehicles*100:.2f}%)")

    print("\n" + "="*60)
    print("ENFORCEMENT RECOMMENDATION")
    print("="*60)
    top_color     = sorted_colors[0][0] if sorted_colors else 'N/A'
    top_color_pct = sorted_colors[0][1] / total_violations * 100 if sorted_colors else 0
    print(f"  -> Target {range_21_plus:,} chronic offenders (21+ violations)")
    print(f"  -> Top color: {top_color} ({top_color_pct:.1f}% of all violations)")
    print(f"  -> Plate {top_plate} is highest-risk with {top_count:,} violations")
    print("="*60)

if __name__ == "__main__":
    main()
