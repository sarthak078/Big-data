#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 1B  |  Task 1.3
  Reducer: Geographic Hotspot Identification (Global Top-K Aggregation)
  NYC Parking Violations Dataset
=============================================================================
"""

import sys
import heapq

K            = 20
DAYS_IN_DATA = 365

def format_revenue(revenue):
    if revenue >= 1000000:
        return f"${revenue/1000000:.1f}M"
    elif revenue >= 1000:
        return f"${revenue/1000:.0f}K"
    else:
        return f"${revenue:.0f}"

def main():
    location_data = {}

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t')
        if len(parts) != 3:
            continue

        location_key = parts[0]
        try:
            count   = int(parts[1])
            revenue = float(parts[2])
        except ValueError:
            continue

        if '|' in location_key:
            street_code, street_name = location_key.split('|', 1)
        else:
            street_code = location_key
            street_name = location_key

        if location_key not in location_data:
            location_data[location_key] = {
                'street_code': street_code,
                'street_name': street_name,
                'count':       0,
                'revenue':     0.0,
            }
        location_data[location_key]['count']   += count
        location_data[location_key]['revenue'] += revenue

    # Global Top-K
    top_k = []
    for location_key, data in location_data.items():
        heapq.heappush(top_k, (data['count'], location_key, data['revenue'],
                                data['street_code'], data['street_name']))
        if len(top_k) > K:
            heapq.heappop(top_k)

    top_k_sorted = sorted(top_k, key=lambda x: x[0], reverse=True)

    total_violations = sum(d['count']   for d in location_data.values())
    total_revenue    = sum(d['revenue'] for d in location_data.values())

    # ── Top-20 Table ──────────────────────────────────────────────────────────
    print("\n" + "="*105)
    print("TASK 1.3 — TOP 20 VIOLATION LOCATIONS (Geographic Hotspot Identification)")
    print("="*105)
    print(f"{'Rank':<6}{'Street Code':<14}{'Location Name':<42}{'Tickets':>10}  {'Est. Revenue':>13}  {'Tickets/Day':>11}  {'% Total':>8}")
    print("-"*105)

    for rank, (count, key, revenue, street_code, street_name) in enumerate(top_k_sorted, 1):
        name_disp = street_name[:40] if len(street_name) > 40 else street_name
        tpd = count / DAYS_IN_DATA
        pct = count / total_violations * 100 if total_violations else 0
        marker = " <-- TOP 5" if rank <= 5 else ""
        print(f"{rank:<6}{street_code:<14}{name_disp:<42}{count:>10,}  "
              f"{format_revenue(revenue):>13}  {tpd:>11.1f}  {pct:>7.2f}%{marker}")

    print("-"*105)
    print(f"{'TOTAL':<20}{'All Locations':<42}{total_violations:>10,}  "
          f"{format_revenue(total_revenue):>13}  "
          f"{total_violations/DAYS_IN_DATA:>11.1f}  {'100.00%':>8}")
    print("="*105)

    # ── Resource Allocation ───────────────────────────────────────────────────
    print("\n" + "="*105)
    print("RESOURCE ALLOCATION STRATEGY")
    print("="*105)

    tiers = [
        ("TIER 1 - CRITICAL  (Rank  1-5 )", top_k_sorted[:5],   "40%"),
        ("TIER 2 - HIGH      (Rank  6-10)", top_k_sorted[5:10], "25%"),
        ("TIER 3 - MODERATE  (Rank 11-20)", top_k_sorted[10:],  "20%"),
    ]

    for tier_label, locations, officer_pct in tiers:
        tier_count   = sum(x[0] for x in locations)
        tier_revenue = sum(x[2] for x in locations)
        tier_pct_v   = tier_count   / total_violations * 100 if total_violations else 0
        tier_pct_r   = tier_revenue / total_revenue    * 100 if total_revenue    else 0

        print(f"\n{tier_label}  |  Officers: {officer_pct}  |  "
              f"Violations: {tier_pct_v:.1f}%  |  Revenue: {tier_pct_r:.1f}%")
        print("-"*105)

        for i, (count, key, revenue, street_code, street_name) in enumerate(locations, 1):
            name_disp = street_name[:40] if len(street_name) > 40 else street_name
            print(f"  {i:>2}. {street_code:<12}  {name_disp:<42}"
                  f"{count:>10,} tickets  {format_revenue(revenue):>10}")

        print(f"  Subtotal: {tier_count:,} tickets  |  "
              f"{format_revenue(tier_revenue)}  |  {tier_pct_v:.1f}% of all violations")

    # ── Final Recommendation ──────────────────────────────────────────────────
    top10_count   = sum(x[0] for x in top_k_sorted[:10])
    top10_revenue = sum(x[2] for x in top_k_sorted[:10])
    top10_pct_v   = top10_count   / total_violations * 100 if total_violations else 0
    top10_pct_r   = top10_revenue / total_revenue    * 100 if total_revenue    else 0

    print("\n" + "="*105)
    print("FINAL STRATEGIC RECOMMENDATION")
    print("="*105)
    print(f"  Deploy 65% of enforcement officers to the TOP 10 locations:")
    print(f"  -> Captures {top10_pct_v:.1f}% of ALL violations ({top10_count:,} tickets)")
    print(f"  -> Captures {top10_pct_r:.1f}% of total estimated revenue ({format_revenue(top10_revenue)})")
    print(f"  -> Remaining 35% patrol Ranks 11-20 for full city coverage")
    print(f"  -> Prioritize Tier 1 locations during peak hours 08:00-12:00 (Task 1.2 finding)")
    print("="*105)

if __name__ == "__main__":
    main()
