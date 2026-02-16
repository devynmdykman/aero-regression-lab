from __future__ import annotations

import argparse
from pathlib import Path
import streamlit as st

from reglab.db.repo import DB
from reglab.analysis.deltas import compare_runs
from reglab.analysis.trends import summarize_run, top_failure_modules

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db", type=str, default="reglab.duckdb")
    return p.parse_args()

def main():
    args = parse_args()
    db_path = Path(args.db)

    st.title("Reglab — Regression & Failure Analysis")
    st.caption(f"DB: {db_path}")

    dbi = DB(db_path)
    con = dbi.connect()

    runs = dbi.list_runs(con)
    if runs.empty:
        st.warning("No runs found. Use `reglab ingest ...` first.")
        return

    run_ids = list(runs["run_id"].tolist())
    run_ids_rev = list(reversed(run_ids))  # newest first

    col1, col2 = st.columns(2)
    with col1:
        run_b = st.selectbox("Newer run (candidate)", run_ids_rev, index=0)
    with col2:
        run_a = st.selectbox("Baseline run (older)", run_ids_rev, index=min(1, len(run_ids_rev)-1))

    if run_a == run_b:
        st.info("Pick two different runs to compare.")
        return

    df_a = dbi.fetch_results(con, run_a)
    df_b = dbi.fetch_results(con, run_b)

    s_a = summarize_run(df_a)
    s_b = summarize_run(df_b)

    st.subheader("Run Summary")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Baseline fail rate", f"{s_a['fail_rate']*100:.1f}%")
    c2.metric("Newer fail rate", f"{s_b['fail_rate']*100:.1f}%")
    c3.metric("Baseline fails", s_a["fail"])
    c4.metric("Newer fails", s_b["fail"])

    st.subheader("Top Failure Modules (Newer)")
    st.dataframe(top_failure_modules(df_b), use_container_width=True)

    st.subheader("Delta Analysis")
    d = compare_runs(df_a, df_b)

    st.markdown(f"**New failures:** {len(d['new_failures'])}  |  **Fixed:** {len(d['fixed'])}  |  **Still failing:** {len(d['still_failing'])}")

    with st.expander("New Failures", expanded=True):
        st.dataframe(d["new_failures"], use_container_width=True)
    with st.expander("Fixed"):
        st.dataframe(d["fixed"], use_container_width=True)
    with st.expander("Still Failing"):
        st.dataframe(d["still_failing"], use_container_width=True)
    with st.expander("Flaky"):
        st.dataframe(d["flaky"], use_container_width=True)

    st.subheader("Metric Regressions")
    if d["metric_regressions"]:
        for col, series in d["metric_regressions"]:
            st.markdown(f"**{col}**")
            st.dataframe(series.head(25).to_frame("delta"), use_container_width=True)
    else:
        st.success("No metric regressions over thresholds.")

if __name__ == "__main__":
    main()
