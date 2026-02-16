from __future__ import annotations

import pandas as pd


def summarize_run(df: pd.DataFrame) -> dict:
    total = len(df)
    pass_n = int((df["status"] == "PASS").sum()) if "status" in df.columns else 0
    fail_n = int((df["status"] == "FAIL").sum()) if "status" in df.columns else 0
    skip_n = int((df["status"] == "SKIP").sum()) if "status" in df.columns else 0
    return {
        "total": total,
        "pass": pass_n,
        "fail": fail_n,
        "skip": skip_n,
        "fail_rate": (fail_n / total) if total else 0.0,
    }


def top_failure_modules(df: pd.DataFrame, n: int = 8) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["module", "fail_count"])
    fails = df[df["status"] == "FAIL"]
    if fails.empty:
        return pd.DataFrame(columns=["module", "fail_count"])
    return (
        fails.groupby("module", dropna=False)
        .size()
        .reset_index(name="fail_count")
        .sort_values("fail_count", ascending=False)
        .head(n)
    )


def top_flaky_tests(history: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """
    history: rows of (run_id, test_name, status)
    Flaky heuristic: status changes frequency across runs.
    """
    if history.empty:
        return pd.DataFrame(columns=["test_name", "flips"])
    pivot = history.sort_values(["test_name", "run_id"])
    pivot["prev"] = pivot.groupby("test_name")["status"].shift(1)
    pivot["flip"] = (pivot["prev"].notna()) & (pivot["status"] != pivot["prev"])
    out = (
        pivot.groupby("test_name")["flip"]
        .sum()
        .reset_index(name="flips")
        .sort_values("flips", ascending=False)
        .head(n)
    )
    return out
