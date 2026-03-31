#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 2  |  Task 2.2
  Analysis: Final cluster profiles and marketing strategy
=============================================================================
"""

from collections import defaultdict

LABEL_NAMES = ['Champions', 'Potential Loyalists', 'At Risk', 'New/Casual']
STRATEGIES  = [
    'VIP programs, loyalty rewards, early access to new products',
    'Personalized recommendations, upgrade incentives, loyalty enrollment',
    'Win-back campaigns, special discount offers, re-engagement emails',
    'Welcome series, educational content, first purchase discounts',
]

def load_assignments(path='final_assignments.txt'):
    clusters = defaultdict(lambda: {'count':0,'R':0.0,'F':0.0,'M':0.0})
    with open(path) as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) < 4:
                continue
            try:
                c = int(parts[0])
                R = float(parts[1])
                F = float(parts[2])
                M = float(parts[3])
                clusters[c]['count'] += 1
                clusters[c]['R']     += R
                clusters[c]['F']     += F
                clusters[c]['M']     += M
            except:
                continue
    return clusters

def main():
    clusters = load_assignments()
    total    = sum(d['count'] for d in clusters.values())

    # Sort by avg R ascending (lowest R = most recent = Champions)
    sorted_c = sorted(
        clusters.items(),
        key=lambda x: x[1]['R']/x[1]['count'] if x[1]['count'] else 1
    )

    label_map = {cid: i for i, (cid, _) in enumerate(sorted_c)}

    print("\n" + "="*70)
    print("TASK 2.2 — K-MEANS CUSTOMER SEGMENTATION RESULTS")
    print("="*70)
    print(f"  K=4 clusters | Euclidean distance | RFM normalized features")
    print(f"  R=days_since_last/90  F=sessions/50  M=revenue/5000")

    print("\n" + "="*70)
    print("CLUSTER ANALYSIS")
    print("="*70)

    for cid, data in sorted_c:
        count = data['count']
        if count == 0:
            continue
        li   = label_map[cid]
        name = LABEL_NAMES[li]
        pct  = count / total * 100
        avgR = data['R'] / count
        avgF = data['F'] / count
        avgM = data['M'] / count

        profiles = [
            'Recent, frequent, high-value customers',
            'Growing engagement and spending patterns',
            'Previously valuable, now becoming inactive',
            'New or occasional shoppers with low activity',
        ]

        print(f"\nCluster {li} - \"{name}\" ({pct:.1f}% of customers):")
        print(f"  Centroid: R={avgR:.2f}, F={avgF:.2f}, M={avgM:.2f}")
        print(f"  Size    : {count:,} customers")
        print(f"  Profile : {profiles[li]}")
        print(f"  Strategy: {STRATEGIES[li]}")

    print("\n" + "="*70)
    print("CONVERGENCE REPORT")
    print("="*70)
    try:
        with open('centroids.txt') as f:
            lines = f.readlines()
        print(f"  Final centroids ({len(lines)} clusters):")
        print(f"  {'Cluster':<10}{'R':>10}{'F':>10}{'M':>10}{'Count':>10}")
        print("  " + "-"*45)
        for line in lines:
            parts = line.strip().split('\t')
            if len(parts) >= 5:
                print(f"  {parts[0]:<10}{float(parts[1]):>10.4f}"
                      f"{float(parts[2]):>10.4f}{float(parts[3]):>10.4f}"
                      f"{int(parts[4]):>10,}")
    except:
        pass

    print("\n" + "="*70)
    print("MARKETING STRATEGY & REVENUE IMPACT")
    print("="*70)

    for cid, data in sorted_c:
        count = data['count']
        if count == 0:
            continue
        li     = label_map[cid]
        name   = LABEL_NAMES[li]
        pct    = count / total * 100
        avgM   = data['M'] / count
        est_rev = avgM * 5000 * count

        print(f"\n  {name} — {count:,} customers ({pct:.1f}%)")
        print(f"  Est. Revenue Potential: ${est_rev:,.0f}")
        print(f"  Action: {STRATEGIES[li]}")

    print("\n" + "="*70)

if __name__ == '__main__':
    main()
