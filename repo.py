from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import duckdb
import pandas as pd

from .schema import SCHEMA_SQL


@dataclass
class DB:
    path: Path

    def connect(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(str(self.path))
        con.execute(SCHEMA_SQL)
        return con

    def upsert_run(self, con: duckdb.DuckDBPyConnection, run_row: dict) -> None:
        con.execute(
            """
            INSERT INTO runs(run_id, started_at, suite, vehicle, build, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
              started_at=excluded.started_at,
              suite=excluded.suite,
              vehicle=excluded.vehicle,
              build=excluded.build,
              notes=excluded.notes
            """,
            [
                run_row["run_id"],
                run_row["started_at"],
                run_row["suite"],
                run_row["vehicle"],
                run_row["build"],
                run_row.get("notes", ""),
            ],
        )

    def upsert_test_results(self, con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
        # Ensure columns exist
        expected = [
            "run_id",
            "test_name",
            "module",
            "status",
            "duration_s",
            "metric_latency_ms",
            "metric_cpu_pct",
            "metric_mem_mb",
            "failure_code",
            "failure_log",
        ]
        for c in expected:
            if c not in df.columns:
                df[c] = None

        con.register("incoming", df[expected])

        con.execute(
            """
            INSERT INTO test_results
            SELECT * FROM incoming
            ON CONFLICT(run_id, test_name) DO UPDATE SET
              module=excluded.module,
              status=excluded.status,
              duration_s=excluded.duration_s,
              metric_latency_ms=excluded.metric_latency_ms,
              metric_cpu_pct=excluded.metric_cpu_pct,
              metric_mem_mb=excluded.metric_mem_mb,
              failure_code=excluded.failure_code,
              failure_log=excluded.failure_log
            """
        )
        con.unregister("incoming")

    def list_runs(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        return con.execute(
            "SELECT run_id, started_at, suite, vehicle, build FROM runs ORDER BY started_at"
        ).df()

    def latest_runs(self, con: duckdb.DuckDBPyConnection, n: int = 2) -> list[str]:
        rows = con.execute(
            "SELECT run_id FROM runs ORDER BY started_at DESC LIMIT ?", [n]
        ).fetchall()
        return [r[0] for r in rows]

    def fetch_results(self, con: duckdb.DuckDBPyConnection, run_id: str) -> pd.DataFrame:
        return con.execute(
            """
            SELECT *
            FROM test_results
            WHERE run_id = ?
            """,
            [run_id],
        ).df()
