from __future__ import annotations

import pandas as pd


def compare_runs(df_a: pd.DataFrame, df_b: pd.DataFrame) -> dict:
    """
    Compare two runs:
      A = baseline (older)
      B = candidate (newer)

    Returns:
      new_failures, fixed, still_failing, flaky, metric_regressions
    """
    key = "test_name"

    a = df_a.set_index(key)
    b = df_b.set_index(key)

    common = a.index.intersection(b.index)
    only_b = b.index.difference(a.index)
    only_a = a.index.difference(b.index)

    a_c = a.loc[common].copy()
    b_c = b.loc[common].copy()

    a_status = a_c["status"].astype(str)
    b_status = b_c["status"].astype(str)

    new_fail = common[(a_status != "FAIL") & (b_status == "FAIL")]
    fixed = common[(a_status == "FAIL") & (b_status != "FAIL")]
    still_failing = common[(a_status == "FAIL") & (b_status == "FAIL")]

    # Simple "flaky" heuristic: changed status between PASS and FAIL
    flaky = common[((a_status == "PASS") & (b_status == "FAIL")) | ((a_status == "FAIL") & (b_status == "PASS"))]

    # Metric regressions (tunable thresholds)
    metric_regressions = []

    def metric_delta(col: str, threshold: float, higher_is_worse: bool = True):
        if col not in a_c.columns or col not in b_c.columns:
            return
        aa = pd.to_numeric(a_c[col], errors="coerce")
        bb = pd.to_numeric(b_c[col], errors="coerce")
        d = bb - aa
        if higher_is_worse:
            bad = d > threshold
        else:
            bad = d < -threshold
        idx = common[bad.fillna(False)]
        if len(idx) > 0:
            metric_regressions.append((col, d.loc[idx].sort_values(ascending=False)))

    metric_delta("metric_latency_ms", threshold=5.0, higher_is_worse=True)
    metric_delta("metric_cpu_pct", threshold=3.0, higher_is_worse=True)
    metric_delta("metric_mem_mb", threshold=10.0, higher_is_worse=True)
    metric_delta("duration_s", threshold=1.0, higher_is_worse=True)

    return {
        "only_in_newer": b.loc[only_b].reset_index() if len(only_b) else pd.DataFrame(),
        "only_in_older": a.loc[only_a].reset_index() if len(only_a) else pd.DataFrame(),
        "new_failures": b.loc[new_fail].reset_index() if len(new_fail) else pd.DataFrame(),
        "fixed": b.loc[fixed].reset_index() if len(fixed) else pd.DataFrame(),
        "still_failing": b.loc[still_failing].reset_index() if len(still_failing) else pd.DataFrame(),
        "flaky": b.loc[flaky].reset_index() if len(flaky) else pd.DataFrame(),
        "metric_regressions": metric_regressions,
    }
