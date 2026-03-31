#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 2  |  Task 2.2
  K-Means Reducer: Compute new centroids
=============================================================================
  Input:  cluster_id TAB R TAB F TAB M TAB user_id
  Output: cluster_id TAB new_R TAB new_F TAB new_M TAB count
          (mean of all assigned users)
=============================================================================
"""

import sys

def main():
    current_cluster = None
    sum_R = 0.0
    sum_F = 0.0
    sum_M = 0.0
    count = 0

    def emit_centroid(cluster_id, sum_R, sum_F, sum_M, count):
        if count == 0:
            return
        new_R = sum_R / count
        new_F = sum_F / count
        new_M = sum_M / count
        print(f"{cluster_id}\t{new_R:.6f}\t{new_F:.6f}\t{new_M:.6f}\t{count}")

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t')
        if len(parts) < 4:
            continue

        try:
            cluster_id = int(parts[0])
            R          = float(parts[1])
            F          = float(parts[2])
            M          = float(parts[3])
        except Exception:
            continue

        if cluster_id != current_cluster:
            if current_cluster is not None:
                emit_centroid(current_cluster, sum_R, sum_F, sum_M, count)
            current_cluster = cluster_id
            sum_R = 0.0
            sum_F = 0.0
            sum_M = 0.0
            count = 0

        sum_R += R
        sum_F += F
        sum_M += M
        count += 1

    if current_cluster is not None:
        emit_centroid(current_cluster, sum_R, sum_F, sum_M, count)

if __name__ == "__main__":
    main()
