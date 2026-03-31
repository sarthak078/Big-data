#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 2  |  Task 2.2
  RFM Reducer: Aggregate per-user RFM features
=============================================================================
  Input:  user_id TAB timestamp TAB event_type TAB price TAB session_id
  Output: user_id TAB R TAB F TAB M  (normalized 0-1)

  R (Recency)  = days since last activity / 90, capped at 1.0
  F (Frequency)= distinct sessions / 50, capped at 1.0
  M (Monetary) = total purchase value / 5000, capped at 1.0
=============================================================================
"""

import sys
from datetime import datetime

# Reference date for recency calculation
REFERENCE_DATE = datetime(2026, 3, 31)

# Normalization parameters per assignment spec
R_NORM = 90.0
F_NORM = 50.0
M_NORM = 5000.0

def main():
    current_user  = None
    timestamps    = []
    sessions      = set()
    total_revenue = 0.0

    def emit_user(user_id, timestamps, sessions, revenue):
        if not timestamps:
            return

        # R: days since last activity
        latest = max(timestamps)
        days_since = (REFERENCE_DATE - latest).days
        R = min(days_since / R_NORM, 1.0)

        # F: distinct sessions
        F = min(len(sessions) / F_NORM, 1.0)

        # M: total purchase value
        M = min(revenue / M_NORM, 1.0)

        print(f"{user_id}\t{R:.6f}\t{F:.6f}\t{M:.6f}")

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t')
        if len(parts) < 5:
            continue

        try:
            user_id    = parts[0]
            ts_str     = parts[1]
            event_type = parts[2]
            price      = float(parts[3])
            session_id = parts[4]

            ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
        except Exception:
            continue

        if user_id != current_user:
            if current_user is not None:
                emit_user(current_user, timestamps, sessions, total_revenue)
            current_user  = user_id
            timestamps    = []
            sessions      = set()
            total_revenue = 0.0

        timestamps.append(ts)
        sessions.add(session_id)
        if event_type == 'purchase':
            total_revenue += price

    if current_user is not None:
        emit_user(current_user, timestamps, sessions, total_revenue)

if __name__ == "__main__":
    main()
