#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 1B  |  Task 1.2
  Reducer: Peak Enforcement Time Analysis
  NYC Parking Violations Dataset
=============================================================================
  Aggregates per-hour counts, ranks hours, prints:
    • Full 24-hour distribution table
    • Optimal 4-hour enforcement window recommendation
=============================================================================
"""

import sys
from collections import defaultdict

# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

W = 72   # table width

def bar_chart(count, max_count, width=30):
    """Simple ASCII bar scaled to max_count."""
    filled = int(count / max_count * width) if max_count else 0
    return '█' * filled + '░' * (width - filled)

def center(text, width=W):
    return f"│{text:^{width - 2}}│"

def divider(left='├', right='┤', ch='─'):
    return f"{left}{ch * (W - 2)}{right}"

# ---------------------------------------------------------------------------
# Main reducer logic
# ---------------------------------------------------------------------------

def main():
    hourly_counts = defaultdict(int)

    # ── Aggregate ────────────────────────────────────────────────────────────
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split('\t')
        if len(parts) != 2:
            continue
        try:
            hour  = int(parts[0])
            count = int(parts[1])
        except ValueError:
            continue
        if 0 <= hour <= 23:
            hourly_counts[hour] += count

    if not hourly_counts:
        print("No data received.")
        return

    total     = sum(hourly_counts.values())
    max_count = max(hourly_counts.values())

    # Rank hours by count (descending)
    ranked = sorted(hourly_counts.items(), key=lambda x: x[1], reverse=True)
    rank_map = {hour: rank + 1 for rank, (hour, _) in enumerate(ranked)}

    # ── 24-Hour Distribution Table ───────────────────────────────────────────
    print()
    print(f"┌{'─' * (W - 2)}┐")
    print(f"┌{'─' * (W - 2)}┐")
    print(center("  TASK 1.2 — PEAK ENFORCEMENT TIME ANALYSIS  "))
    print(center("  NYC Parking Violations  ·  Hourly Distribution  "))
    print(divider())
    print(f"│  {'Hour':<6}{'Period':<10}{'Tickets':>10}{'Percentage':>12}"
          f"{'Rank':>6}  │")
    print(divider())

    # Time period labels
    def period(h):
        if  0 <= h <  6: return "Night"
        if  6 <= h < 12: return "Morning"
        if 12 <= h < 17: return "Afternoon"
        if 17 <= h < 21: return "Evening"
        return "Night"

    # Period color markers (text only — terminal will color these)
    def period_marker(h):
        if  0 <= h <  6: return "🌙"
        if  6 <= h < 12: return "🌅"
        if 12 <= h < 17: return "☀️ "
        if 17 <= h < 21: return "🌆"
        return "🌙"

    for hour in range(24):
        count  = hourly_counts.get(hour, 0)
        pct    = count / total * 100 if total else 0
        rank   = rank_map.get(hour, 24)
        barchart = bar_chart(count, max_count)
        marker = "◀ TOP" if rank <= 4 else "     "
        print(f"│  {hour:02d}:00 {period(hour):<10}{count:>10,}{pct:>11.1f}%"
              f"{rank:>6}  {marker}│")

        # Separator between time periods
        if hour in (5, 11, 16, 20):
            print(divider('├', '┤', '·'))

    print(divider())
    print(f"│  {'TOTAL':<16}{total:>10,}{'100.0%':>12}{'':>6}  "
          f"{'':28} {'     '}│")
    print(f"└{'─' * (W - 2)}┘")

    # ── Find optimal 4-hour window ───────────────────────────────────────────
    best_window_start = 0
    best_window_count = 0

    for start in range(24):
        window_count = sum(hourly_counts.get((start + i) % 24, 0) for i in range(4))
        if window_count > best_window_count:
            best_window_count = window_count
            best_window_start = start

    window_hours = [(best_window_start + i) % 24 for i in range(4)]
    window_pct   = best_window_count / total * 100 if total else 0

    # Top 4 individual hours
    top4 = ranked[:4]
    top4_count = sum(c for _, c in top4)
    top4_pct   = top4_count / total * 100 if total else 0

    # ── Recommendation Box ───────────────────────────────────────────────────
    print()
    print(f"┌{'─' * (W - 2)}┐")
    print(center("  ENFORCEMENT RECOMMENDATION  "))
    print(divider())
    print(center("  Optimal Consecutive 4-Hour Window  "))
    print(divider('├', '┤', '·'))

    for i, h in enumerate(window_hours):
        count = hourly_counts.get(h, 0)
        pct   = count / total * 100 if total else 0
        rank  = rank_map.get(h, 24)
        print(f"│    {i+1}. {h:02d}:00 – {(h+1):02d}:00"
              f"   {count:>10,} tickets   {pct:5.1f}%   Rank #{rank:<3}        │")

    print(divider('├', '┤', '·'))
    print(f"│    Window Total:  {best_window_count:>10,} tickets   {window_pct:5.1f}% of all violations  │")
    print(divider())
    print(center("  Top 4 Individual Peak Hours (non-consecutive)  "))
    print(divider('├', '┤', '·'))

    for rank_pos, (hour, count) in enumerate(top4, 1):
        pct = count / total * 100 if total else 0
        print(f"│    #{rank_pos}  {hour:02d}:00   {count:>10,} tickets   {pct:5.1f}%"
              f"   │")

    print(divider('├', '┤', '·'))
    print(f"│    Top-4 Total:   {top4_count:>10,} tickets   {top4_pct:5.1f}% of all violations  │")
    print(divider())
    print(f"│  DEPLOY maximum officers during:                                     │")
    h_start = best_window_start
    h_end   = (best_window_start + 4) % 24
    print(f"│    ▶  {h_start:02d}:00 – {h_end:02d}:00  ({window_pct:.1f}% of daily violations captured)      │")
    print(f"│    ▶  Focus on Top-4 hours for non-consecutive peak deployment        │")
    print(f"└{'─' * (W - 2)}┘")
    print()

if __name__ == "__main__":
    main()
