from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from jinja2 import Template
import pandas as pd

HTML_TEMPLATE = Template(
    """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Reglab Report</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 24px; }
    h1,h2 { margin-bottom: 6px; }
    .meta { color: #444; margin-bottom: 18px; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
    .card { border: 1px solid #ddd; border-radius: 12px; padding: 14px; }
    table { border-collapse: collapse; width: 100%; font-size: 13px; }
    th, td { border-bottom: 1px solid #eee; padding: 6px 8px; text-align: left; vertical-align: top; }
    th { background: #fafafa; }
    code { background: #f6f6f6; padding: 2px 6px; border-radius: 6px; }
  </style>
</head>
<body>
  <h1>Regression Delta Report</h1>
  <div class="meta">
    Baseline: <code>{{ run_a }}</code> → Newer: <code>{{ run_b }}</code>
  </div>

  <div class="grid">
    <div class="card">
      <h2>New Failures ({{ new_failures_count }})</h2>
      {{ new_failures_table | safe }}
    </div>
    <div class="card">
      <h2>Fixed ({{ fixed_count }})</h2>
      {{ fixed_table | safe }}
    </div>
  </div>

  <div class="grid" style="margin-top: 14px;">
    <div class="card">
      <h2>Still Failing ({{ still_failing_count }})</h2>
      {{ still_failing_table | safe }}
    </div>
    <div class="card">
      <h2>Flaky ({{ flaky_count }})</h2>
      {{ flaky_table | safe }}
    </div>
  </div>

  <div class="card" style="margin-top: 14px;">
    <h2>Metric Regressions</h2>
    {{ metric_block | safe }}
  </div>

</body>
</html>
"""
)

def _df_to_html(df: pd.DataFrame, cols: list[str]) -> str:
    if df is None or df.empty:
        return "<div><em>None</em></div>"
    show = [c for c in cols if c in df.columns]
    if not show:
        show = list(df.columns)[:6]
    return df[show].to_html(index=False, escape=True)

@dataclass
class ReportInputs:
    run_a: str
    run_b: str
    deltas: dict

def render_html(inputs: ReportInputs) -> str:
    d = inputs.deltas
    metric_lines = []
    for (col, series) in d.get("metric_regressions", []):
        metric_lines.append(f"<h3>{col}</h3>")
        metric_lines.append(series.head(15).to_frame("delta").to_html(escape=True))
    metric_block = "\n".join(metric_lines) if metric_lines else "<em>None</em>"

    html = HTML_TEMPLATE.render(
        run_a=inputs.run_a,
        run_b=inputs.run_b,
        new_failures_count=len(d.get("new_failures", [])),
        fixed_count=len(d.get("fixed", [])),
        still_failing_count=len(d.get("still_failing", [])),
        flaky_count=len(d.get("flaky", [])),
        new_failures_table=_df_to_html(d.get("new_failures"), ["test_name", "module", "failure_code"]),
        fixed_table=_df_to_html(d.get("fixed"), ["test_name", "module"]),
        still_failing_table=_df_to_html(d.get("still_failing"), ["test_name", "module", "failure_code"]),
        flaky_table=_df_to_html(d.get("flaky"), ["test_name", "module", "status"]),
        metric_block=metric_block,
    )
    return html

def write_html(path: Path, html: str) -> None:
    path.write_text(html, encoding="utf-8")

