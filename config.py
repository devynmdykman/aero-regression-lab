from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

DEFAULT_DB_PATH = Path("reglab.duckdb")


@dataclass(frozen=True)
class Settings:
    db_path: Path = DEFAULT_DB_PATH

    @staticmethod
    def ensure_parent(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
