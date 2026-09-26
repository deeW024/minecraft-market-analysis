"""Command-line runner for YEE-75 within-category direction discovery."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from .category_directions import CategoryDirectionError, build_category_direction_discovery


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build YEE-75 deterministic category-local direction evidence from accepted YEE-73 SQLite"
    )
    parser.add_argument("--input-db", required=True, type=Path, help="Read-only accepted YEE-73 category_signal_layer.sqlite")
    parser.add_argument("--output-dir", required=True, type=Path, help="Output folder containing only the pre-written GOAL_ALIGNMENT.md")
    parser.add_argument("--code-commit", default=None, help="Exact source commit recorded in immutable run metadata")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        qa = build_category_direction_discovery(args.input_db, args.output_dir, code_commit=args.code_commit)
    except (CategoryDirectionError, OSError, sqlite3.Error) as exc:
        parser.exit(2, f"YEE-75 category direction build failed: {exc}\n")
    print(json.dumps({
        "status": qa["status"],
        "delivery_status": qa["delivery_status"],
        "run_id": qa["run_id"],
        "row_counts": qa["row_counts"],
        "passing_checks": qa["passing_checks"],
        "check_count": qa["check_count"],
        "failed_checks": qa["failed_checks"],
        "output_dir": str(args.output_dir.resolve()),
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
