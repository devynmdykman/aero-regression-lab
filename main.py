from __future__ import annotations

from pathlib import Path
import typer
from rich import print
from rich.table import Table

from reglab.config import Settings
from reglab.db.repo import DB
from reglab.ingest.parsers import find_run_files, load_run_file
from reglab.analysis.deltas import compare_runs
from reglab.report.render import ReportInputs, render_html, write_html
from reglab.utils.sample_data import generate_runs

app = typer.Typer(add_completion=False)

def _table_from_df(df, title: str, cols: list[str], max_rows: int = 12):
    t = Table(title=title)
    use = [c for c in cols if c in df.columns]
    for c in use:
        t.add_column(c)
    for _, row in df.head(max_rows).iterrows():
        t.add_row(*[str(row[c]) for c in use])
    return t

@app.command("gen-sample")
def gen_sample(out: Path = typer.Option(Path("data/sample_runs"), help="Output folder"),
               runs: int = typer.Option(12, help="Number of runs"),
               tests: int = typer.Option(80, help="Tests per run")):
    paths = generate_runs(out=out, runs=runs, tests_per_run=tests)
    print(f"[green]Generated {len(paths)} runs in {out}[/green]")

@app.command("ingest")
def ingest(path: Path = typer.Argument(..., help="Folder or single .json run file"),
           db: Path = typer.Option(Settings().db_path, help="DuckDB path")):
    dbi = DB(db)
    con = dbi.connect()

    files = find_run_files(path)
    if not files:
        raise typer.BadParameter(f"No .json run files found at: {path}")

    for f in files:
        run_row, df = load_run_file(f)
        dbi.upsert_run(con, run_row)
        dbi.upsert_test_results(con, df)
        print(f"[cyan]Ingested[/cyan] {f.name} ({len(df)} tests)")

    runs_df = dbi.list_runs(con)
    print(f"[green]DB now has {len(runs_df)} runs[/green] at {db}")

@app.command("list-runs")
def list_runs(db: Path = typer.Option(Settings().db_path, help="DuckDB path")):
    dbi = DB(db)
    con = dbi.connect()
    df = dbi.list_runs(con)
    print(df)

@app.command("compare")
def compare(db: Path = typer.Option(Settings().db_path, help="DuckDB path"),
            run_a: str | None = typer.Option(None, help="Baseline run_id (older)"),
            run_b: str | None = typer.Option(None, help="Newer run_id")):
    dbi = DB(db)
    con = dbi.connect()

    if run_a is None or run_b is None:
        latest = dbi.latest_runs(con, 2)
        if len(latest) < 2:
            raise typer.BadParameter("Need at least 2 runs in DB to compare.")
        run_b, run_a = latest[0], latest[1]  # latest[0] is newest
    df_a = dbi.fetch_results(con, run_a)
    df_b = dbi.fetch_results(con, run_b)

    d = compare_runs(df_a, df_b)

    print(f"[bold]Compare[/bold] {run_a} → {run_b}")
    if not d["new_failures"].empty:
        print(_table_from_df(d["new_failures"], "New Failures", ["test_name", "module", "failure_code"]))
    else:
        print("[green]No new failures[/green]")

    if not d["fixed"].empty:
        print(_table_from_df(d["fixed"], "Fixed", ["test_name", "module"]))
    else:
        print("[yellow]No fixed tests[/yellow]")

    if d["metric_regressions"]:
        print("[bold red]Metric regressions detected[/bold red]")
        for col, series in d["metric_regressions"]:
            print(f"[red]{col}[/red]")
            print(series.head(8).to_string())
    else:
        print("[green]No metric regressions over thresholds[/green]")

@app.command("report")
def report(db: Path = typer.Option(Settings().db_path, help="DuckDB path"),
           out: Path = typer.Option(Path("report.html"), help="Output HTML report"),
           run_a: str | None = typer.Option(None, help="Baseline run_id (older)"),
           run_b: str | None = typer.Option(None, help="Newer run_id")):
    dbi = DB(db)
    con = dbi.connect()

    if run_a is None or run_b is None:
        latest = dbi.latest_runs(con, 2)
        if len(latest) < 2:
            raise typer.BadParameter("Need at least 2 runs in DB to report.")
        run_b, run_a = latest[0], latest[1]

    df_a = dbi.fetch_results(con, run_a)
    df_b = dbi.fetch_results(con, run_b)
    d = compare_runs(df_a, df_b)

    html = render_html(ReportInputs(run_a=run_a, run_b=run_b, deltas=d))
    write_html(out, html)
    print(f"[green]Wrote report[/green] {out}")

if __name__ == "__main__":
    app()
