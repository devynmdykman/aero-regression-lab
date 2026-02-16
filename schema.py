from __future__ import annotations

SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS runs (
  run_id VARCHAR PRIMARY KEY,
  started_at TIMESTAMP,
  suite VARCHAR,
  vehicle VARCHAR,
  build VARCHAR,
  notes VARCHAR
);

CREATE TABLE IF NOT EXISTS test_results (
  run_id VARCHAR,
  test_name VARCHAR,
  module VARCHAR,
  status VARCHAR,           -- PASS / FAIL / SKIP
  duration_s DOUBLE,
  metric_latency_ms DOUBLE,
  metric_cpu_pct DOUBLE,
  metric_mem_mb DOUBLE,
  failure_code VARCHAR,
  failure_log VARCHAR,
  PRIMARY KEY (run_id, test_name)
);

CREATE INDEX IF NOT EXISTS idx_test_results_run ON test_results(run_id);
CREATE INDEX IF NOT EXISTS idx_test_results_module ON test_results(module);
"""
