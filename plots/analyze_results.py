"""
Statistical analysis for AdaptRoute experiment results.

Cliff's delta convention:
  delta = P(baseline_latency > AR_latency) - P(AR_latency > baseline_latency)
  Positive delta  → AR has lower (better) latency  → AR wins
  Negative delta  → baseline has lower latency → baseline wins

This matches the paper's §4.4 statement:
  "higher rank corresponds to lower (better) latency;
   positive values indicate that AdaptRoute achieves lower (better) latency."
"""

import os
import itertools
import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu, shapiro

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
CSV_IN  = os.path.join(RESULTS_DIR, "experiment_results.csv")
CSV_OUT = os.path.join(RESULTS_DIR, "statistical_tests.csv")
SUMMARY = os.path.join(RESULTS_DIR, "summary_stats.csv")

ALPHA      = 0.05
N_COMP     = 3          # AdaptRoute vs each of 3 baselines
ALPHA_ADJ  = ALPHA / N_COMP   # Bonferroni-corrected threshold ≈ 0.0167

METRIC     = "latency_p95"   # primary metric for pairwise tests
BASELINES  = ["kafka_only", "rabbitmq_only", "static_split"]
AR         = "adaptroute"


def cliffs_delta(baseline_vals, ar_vals):
    """
    Cliff's delta = P(baseline > AR) - P(AR > baseline).
    Positive → AR has lower (better) latency.
    Uses the Mann-Whitney U statistic where U counts baseline > AR pairs.
    """
    n_b = len(baseline_vals)
    n_a = len(ar_vals)
    # mannwhitneyu(x, y) returns U1 = count of pairs where x[i] > y[j]
    u1, _ = mannwhitneyu(baseline_vals, ar_vals, alternative="two-sided")
    delta = (2 * u1 / (n_b * n_a)) - 1
    return delta


def effect_size_label(d):
    a = abs(d)
    if a < 0.147:  return "negligible"
    if a < 0.330:  return "small"
    if a < 0.474:  return "medium"
    return "large"


def main():
    df = pd.read_csv(CSV_IN)
    workloads = sorted(df["workload"].unique())

    # ── Summary statistics ────────────────────────────────────────────────────
    summary_rows = []
    for (wl, strat), g in df.groupby(["workload", "strategy"]):
        row = {
            "workload":  wl,
            "strategy":  strat,
            "n":         len(g),
        }
        for col in ["latency_p50", "latency_p95", "latency_p99",
                    "throughput", "delivery_rate", "routing_overhead_ms"]:
            row[f"{col}_mean"] = g[col].mean()
            row[f"{col}_sd"]   = g[col].std(ddof=1)
        summary_rows.append(row)

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(SUMMARY, index=False, float_format="%.4f")
    print(f"Summary written to {SUMMARY}")

    # ── Pairwise hypothesis tests ─────────────────────────────────────────────
    test_rows = []
    for wl in workloads:
        ar_vals = df[(df["workload"] == wl) & (df["strategy"] == AR)][METRIC].values
        for baseline in BASELINES:
            bl_vals = df[(df["workload"] == wl) & (df["strategy"] == baseline)][METRIC].values

            # Normality
            _, sw_p_ar = shapiro(ar_vals)
            _, sw_p_bl = shapiro(bl_vals)

            # Mann-Whitney U (two-sided)
            u_stat, p_val = mannwhitneyu(bl_vals, ar_vals, alternative="two-sided")

            # Cliff's delta (positive = AR wins)
            delta = cliffs_delta(bl_vals, ar_vals)
            sig   = p_val < ALPHA_ADJ

            test_rows.append({
                "workload":         wl,
                "comparison":       f"AdaptRoute vs {baseline.replace('_', '-').title()}",
                "test":             "Mann-Whitney U",
                "statistic":        round(u_stat, 1),
                "p_value":          round(p_val, 6),
                "cliffs_delta":     round(delta, 4),
                "significant":      "Yes" if sig else "No",
                "shapiro_ar_p":     round(sw_p_ar, 4),
                "shapiro_bl_p":     round(sw_p_bl, 4),
                "effect_size":      effect_size_label(delta),
                "ar_wins":          "Yes" if delta > 0 else "No",
            })

    tests_df = pd.DataFrame(test_rows)
    tests_df.to_csv(CSV_OUT, index=False)
    print(f"Statistical tests written to {CSV_OUT}")

    # ── Console summary ───────────────────────────────────────────────────────
    print(f"\nAlpha_adj = {ALPHA_ADJ:.4f} (Bonferroni over {N_COMP} comparisons)\n")
    print(f"{'Workload':<5} {'Comparison':<35} {'p':>8} {'delta':>7} {'Sig':>4} {'AR wins':>8}")
    print("-" * 72)
    for _, r in tests_df.iterrows():
        comp = r["comparison"].replace("AdaptRoute vs ", "vs ")
        print(f"{r['workload']:<5} {comp:<35} {r['p_value']:>8.4f} "
              f"{r['cliffs_delta']:>7.4f} {r['significant']:>4} {r['ar_wins']:>8}")

    # ── P50 / P95 / P99 per workload × strategy ───────────────────────────────
    print("\n── Summary Statistics (P95 latency, ms) ──")
    for wl in workloads:
        print(f"\n  {wl}:")
        for strat in [AR] + BASELINES:
            g = df[(df["workload"] == wl) & (df["strategy"] == strat)]
            print(f"    {strat:<20} P50={g['latency_p50'].mean():.3f} "
                  f"P95={g['latency_p95'].mean():.3f}±{g['latency_p95'].std(ddof=1):.2f} "
                  f"P99={g['latency_p99'].mean():.3f}  "
                  f"tput={g['throughput'].mean():.1f}")


if __name__ == "__main__":
    main()
