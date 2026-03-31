#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 2  |  Task 2.2
  K-Means Mapper: Assign users to nearest centroid
=============================================================================
  Loads centroids from local file (distributed via -file flag).
  For each user, computes Euclidean distance to all centroids.
  Assigns user to nearest centroid.

  Input:  user_id TAB R TAB F TAB M
  Output: cluster_id TAB R TAB F TAB M TAB user_id
=============================================================================
"""

import sys
import math
import os

CENTROID_FILE = 'centroids.txt'

def load_centroids():
    """Load current centroids from local file."""
    centroids = []
    try:
        with open(CENTROID_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) >= 4:
                    cluster_id = int(parts[0])
                    R = float(parts[1])
                    F = float(parts[2])
                    M = float(parts[3])
                    centroids.append((cluster_id, R, F, M))
    except Exception as e:
        sys.stderr.write(f"Error loading centroids: {e}\n")
    return centroids

def euclidean_distance(r1, f1, m1, r2, f2, m2):
    """Euclidean distance in 3D RFM space."""
    return math.sqrt((r1-r2)**2 + (f1-f2)**2 + (m1-m2)**2)

def main():
    centroids = load_centroids()
    if not centroids:
        sys.stderr.write("No centroids loaded!\n")
        return

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t')
        if len(parts) < 4:
            continue

        try:
            user_id = parts[0]
            R       = float(parts[1])
            F       = float(parts[2])
            M       = float(parts[3])
        except Exception:
            continue

        # Find nearest centroid
        nearest_cluster = 0
        min_dist        = float('inf')

        for cluster_id, cR, cF, cM in centroids:
            dist = euclidean_distance(R, F, M, cR, cF, cM)
            if dist < min_dist:
                min_dist        = dist
                nearest_cluster = cluster_id

        # Emit: cluster_id TAB R TAB F TAB M TAB user_id
        print(f"{nearest_cluster}\t{R:.6f}\t{F:.6f}\t{M:.6f}\t{user_id}")

if __name__ == "__main__":
    main()
