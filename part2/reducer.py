#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 2  |  Task 2.1
  Reducer: User Session Reconstruction (Secondary Sort)
  E-Commerce Clickstream Dataset
=============================================================================
  Receives events:
    - Grouped by user_id (custom partitioner ensures this)
    - Sorted by user_id + timestamp (KeyFieldBasedComparator)
    - Processes each user's events in chronological order
    - Reconstructs sessions using 30-minute timeout rule
=============================================================================
"""

import sys
from datetime import datetime
from collections import defaultdict

SESSION_TIMEOUT = 30   # minutes
OUTPUT_HEADER   = ("session_id,user_id,start_time,end_time,duration_minutes,"
                   "event_count,converted,total_revenue,device_type,traffic_source")

def parse_ts(ts_str):
    try:
        return datetime.strptime(ts_str.strip(), '%Y-%m-%d %H:%M:%S')
    except Exception:
        return None

def main():
    print(OUTPUT_HEADER)

    current_user   = None
    current_events = []   # (ts, etype, price, device, source)

    # Metrics
    total_sessions     = 0
    total_duration     = 0.0
    converted_sessions = 0
    total_events_all   = 0
    total_conv_revenue = 0.0
    session_counter    = 0
    pattern_data       = defaultdict(lambda: {
        'sessions': 0, 'converted': 0,
        'total_duration': 0.0, 'total_events': 0
    })

    def flush_user(user_id, events):
        nonlocal total_sessions, total_duration, converted_sessions
        nonlocal total_events_all, total_conv_revenue, session_counter

        if not events:
            return

        # ── Split into sessions ───────────────────────────────────────────
        sessions     = []
        current_sess = [events[0]]

        for i in range(1, len(events)):
            prev_ts  = current_sess[-1][0]
            curr_ts  = events[i][0]

            if prev_ts is None or curr_ts is None:
                current_sess.append(events[i])
                continue

            diff_min  = (curr_ts - prev_ts).total_seconds() / 60
            midnight  = curr_ts.date() != prev_ts.date()

            if diff_min > SESSION_TIMEOUT or midnight:
                sessions.append(current_sess)
                current_sess = [events[i]]
            else:
                current_sess.append(events[i])

        sessions.append(current_sess)

        # ── Process each session ──────────────────────────────────────────
        for sess in sessions:
            session_counter += 1
            sid = f"SESS_{session_counter:08d}"

            timestamps = [e[0] for e in sess if e[0] is not None]
            if not timestamps:
                continue

            start_time = min(timestamps)
            end_time   = max(timestamps)
            duration   = (end_time - start_time).total_seconds() / 60

            etypes    = [e[1] for e in sess]
            prices    = [e[2] for e in sess]
            devices   = [e[3] for e in sess]
            sources   = [e[4] for e in sess]

            converted = 1 if 'purchase' in etypes else 0
            revenue   = sum(p for p, et in zip(prices, etypes)
                            if et == 'purchase')
            device    = devices[0] if devices else 'unknown'
            source    = sources[0] if sources else 'unknown'
            evt_count = len(sess)

            print(f"{sid},{user_id},"
                  f"{start_time.strftime('%Y-%m-%d %H:%M:%S')},"
                  f"{end_time.strftime('%Y-%m-%d %H:%M:%S')},"
                  f"{duration:.1f},{evt_count},{converted},"
                  f"{revenue:.2f},{device},{source}")

            total_sessions     += 1
            total_duration     += duration
            total_events_all   += evt_count
            if converted:
                converted_sessions += 1
                total_conv_revenue += revenue

            pattern_data[(device, source)]['sessions']       += 1
            pattern_data[(device, source)]['converted']      += converted
            pattern_data[(device, source)]['total_duration'] += duration
            pattern_data[(device, source)]['total_events']   += evt_count

    # ── Main processing loop ──────────────────────────────────────────────────
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t')
        if len(parts) < 6:
            continue

        try:
            user_id = parts[0]
            ts      = parse_ts(parts[1])
            etype   = parts[2]
            price   = float(parts[3])
            device  = parts[4]
            source  = parts[5]
        except Exception:
            continue

        # New user detected → flush previous user's events
        if user_id != current_user:
            flush_user(current_user, current_events)
            current_user   = user_id
            current_events = []

        current_events.append((ts, etype, price, device, source))

    # Flush last user
    flush_user(current_user, current_events)

    # =========================================================================
    # SUMMARY METRICS
    # =========================================================================
    avg_dur    = total_duration     / total_sessions    if total_sessions    else 0
    conv_rate  = converted_sessions / total_sessions * 100 if total_sessions else 0
    avg_events = total_events_all   / total_sessions    if total_sessions    else 0
    avg_rev    = total_conv_revenue / converted_sessions if converted_sessions else 0

    print("\n" + "="*70)
    print("TASK 2.1 — USER SESSION RECONSTRUCTION SUMMARY")
    print("="*70)
    print(f"  Total sessions reconstructed  : {total_sessions:,}")
    print(f"  Average session duration      : {avg_dur:.1f} minutes")
    print(f"  Conversion rate               : {conv_rate:.1f}%")
    print(f"  Average events per session    : {avg_events:.1f}")
    print(f"  Average revenue/converted sess: ${avg_rev:.2f}")
    print(f"  Total converted sessions      : {converted_sessions:,}")
    print(f"  Total revenue                 : ${total_conv_revenue:,.2f}")

    # =========================================================================
    # GOLDEN SESSION PATTERN
    # =========================================================================
    print("\n" + "="*70)
    print("GOLDEN SESSION PATTERN ANALYSIS")
    print("="*70)
    print(f"  {'Device':<12}{'Source':<12}{'Sessions':>10}{'Conv Rate':>12}"
          f"{'Avg Duration':>14}{'Avg Events':>12}")
    print("-"*70)

    results = []
    for (device, source), d in pattern_data.items():
        s   = d['sessions']
        cr  = d['converted'] / s * 100 if s else 0
        dur = d['total_duration'] / s   if s else 0
        ev  = d['total_events']   / s   if s else 0
        results.append((cr, device, source, s, d['converted'], dur, ev))

    results.sort(reverse=True)

    for cr, device, source, s, conv, dur, ev in results:
        print(f"  {device:<12}{source:<12}{s:>10,}{cr:>11.1f}%"
              f"{dur:>13.1f}m{ev:>12.1f}")

    golden = next(
        (r for r in results if r[3] >= max(10, total_sessions * 0.01)),
        results[0] if results else None
    )

    if golden:
        cr, device, source, s, conv, dur, ev = golden
        print("\n" + "="*70)
        print("GOLDEN SESSION PATTERN (Highest Conversion Rate)")
        print("="*70)
        print(f"  Device         : {device}")
        print(f"  Traffic Source : {source}")
        print(f"  Conversion Rate: {cr:.1f}%")
        print(f"  Avg Duration   : {dur:.1f} minutes")
        print(f"  Avg Events     : {ev:.1f} per session")
        print(f"\n  RECOMMENDATIONS:")
        print(f"  -> Optimize {device} experience for {source} traffic")
        print(f"  -> Target {source} campaigns to drive {device} users")
        print(f"  -> Design sessions around {dur:.0f}-minute engagement window")
        print(f"  -> Aim for {ev:.0f} events per session to maximize conversion")
    print("="*70)

if __name__ == "__main__":
    main()
