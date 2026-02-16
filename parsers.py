from __future__ import annotations

from pathlib import Path
import json
import pandas as pd


def load_run_file(path: Path) -> tuple[dict, pd.DataFrame]:
    """
    Expected JSON format:
    {
      "run": { "run_id": "...", "started_at": "...", "suite": "...", "vehicle": "...", "build": "...", "notes": "" },
      "results": [ { "test_name": "...", "module": "...", "status": "PASS|FAIL|SKIP", ... }, ... ]
    }
    """
    raw = json.loads(path.read_text())
    run_row = raw["run"]
    df = pd.DataFrame(raw["results"])
    df["run_id"] = run_row["run_id"]
    return run_row, df


def find_run_files(folder: Path) -> list[Path]:
    if folder.is_file() and folder.suffix.lower() == ".json":
        return [folder]
    if folder.is_dir():
        return sorted(folder.glob("*.json"))
    return []
