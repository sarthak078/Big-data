#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 1C  |  Task 1.5
  Reducer: Revenue Analysis by Violation Type
  NYC Parking Violations Dataset
=============================================================================
  Aggregates ticket counts and revenue per violation code.
  Outputs ranked revenue table + business insights.
=============================================================================
"""

import sys
from collections import defaultdict

def fmt_rev(amount):
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.1f}M"
    if amount >= 1_000:
        return f"${amount/1_000:.0f}K"
    return f"${amount:.0f}"

def fmt_tickets(count):
    if count >= 1_000_000:
        return f"{count/1_000_000:.1f}M"
    if count >= 1_000:
        return f"{count/1_000:.0f}K"
    return f"{count:,}"

def main():
    violation_data = defaultdict(lambda: {'count': 0, 'revenue': 0.0, 'description': ''})

    # ── Aggregate ────────────────────────────────────────────────────────────
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split('\t')
        if len(parts) != 4:
            continue

        violation_code = parts[0]
        try:
            count   = int(parts[1])
            fine    = float(parts[2])
        except ValueError:
            continue
        description = parts[3]

        violation_data[violation_code]['count']       += count
        violation_data[violation_code]['revenue']     += fine * count
        violation_data[violation_code]['description']  = description

    # Sort by revenue descending
    sorted_violations = sorted(
        violation_data.items(),
        key=lambda x: x[1]['revenue'],
        reverse=True
    )

    total_tickets = sum(d['count']   for d in violation_data.values())
    total_revenue = sum(d['revenue'] for d in violation_data.values())

    # =========================================================================
    # REVENUE BY VIOLATION TYPE TABLE
    # =========================================================================
    print("\n" + "="*105)
    print("TASK 1.5 — REVENUE ANALYSIS BY VIOLATION TYPE (Map-Side Join)")
    print("="*105)
    print(f"{'Code':<6}{'Description':<40}{'Tickets':>10}  {'Fine':>6}  "
          f"{'Total Revenue':>14}  {'% Revenue':>10}  {'% Tickets':>10}")
    print("-"*105)

    for code, data in sorted_violations:
        count       = data['count']
        revenue     = data['revenue']
        description = data['description']
        fine        = revenue / count if count else 0
        pct_rev     = revenue / total_revenue * 100 if total_revenue else 0
        pct_tick    = count   / total_tickets * 100 if total_tickets else 0

        desc_display = description[:38] if len(description) > 38 else description
        print(f"{code:<6}{desc_display:<40}{fmt_tickets(count):>10}  "
              f"${fine:>5.0f}  {fmt_rev(revenue):>14}  "
              f"{pct_rev:>9.1f}%  {pct_tick:>9.1f}%")

    print("-"*105)
    print(f"{'TOTAL':<46}{fmt_tickets(total_tickets):>10}  "
          f"{'':>6}  {fmt_rev(total_revenue):>14}  {'100.0%':>10}  {'100.0%':>10}")
    print("="*105)

    # =========================================================================
    # TOP 5 MOST PROFITABLE
    # =========================================================================
    print("\n" + "="*105)
    print("TOP 5 MOST PROFITABLE VIOLATION TYPES")
    print("="*105)

    for rank, (code, data) in enumerate(sorted_violations[:5], 1):
        count    = data['count']
        revenue  = data['revenue']
        desc     = data['description']
        fine     = revenue / count if count else 0
        pct_rev  = revenue / total_revenue * 100 if total_revenue else 0
        pct_tick = count   / total_tickets * 100 if total_tickets else 0
        print(f"\n#{rank} Code {code} ({desc})")
        print(f"   {fmt_tickets(count)} tickets x ${fine:.0f} = "
              f"{fmt_rev(revenue)} ({pct_rev:.1f}% of revenue)")
        print(f"   Generates {pct_rev:.1f}% of revenue from only {pct_tick:.1f}% of tickets")

    print("\n" + "="*105)

    # =========================================================================
    # BUSINESS INSIGHTS
    # =========================================================================
    top1        = sorted_violations[0]
    top1_code   = top1[0]
    top1_data   = top1[1]
    top1_pct_r  = top1_data['revenue'] / total_revenue * 100 if total_revenue else 0
    top1_pct_t  = top1_data['count']   / total_tickets * 100 if total_tickets else 0

    # Most tickets (not necessarily most revenue)
    most_tickets     = max(violation_data.items(), key=lambda x: x[1]['count'])
    most_tick_code   = most_tickets[0]
    most_tick_data   = most_tickets[1]
    most_tick_pct_r  = most_tick_data['revenue'] / total_revenue * 100 if total_revenue else 0
    most_tick_pct_t  = most_tick_data['count']   / total_tickets * 100 if total_tickets else 0

    print("\n" + "="*105)
    print("BUSINESS INSIGHTS")
    print("="*105)
    print(f"  Total Revenue Generated  : {fmt_rev(total_revenue)}")
    print(f"  Total Tickets Issued     : {fmt_tickets(total_tickets)}")
    print(f"  Unique Violation Types   : {len(violation_data)}")
    print(f"\n  Most Profitable Type     : Code {top1_code} ({top1_data['description']})")
    print(f"    -> {top1_pct_r:.1f}% of revenue from {top1_pct_t:.1f}% of tickets")
    print(f"\n  Highest Volume Type      : Code {most_tick_code} ({most_tick_data['description']})")
    print(f"    -> {most_tick_pct_t:.1f}% of tickets, {most_tick_pct_r:.1f}% of revenue")
    print(f"\n  RECOMMENDATION:")
    print(f"    -> Prioritize enforcement of Code {top1_code} violations")
    print(f"       Maximum revenue per officer deployment")
    print(f"    -> Code {most_tick_code} generates volume but lower revenue per ticket")
    print("="*105)

if __name__ == "__main__":
    main()
