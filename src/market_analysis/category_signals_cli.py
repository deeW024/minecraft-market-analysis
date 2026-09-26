"""Command-line runner for the YEE-73 Category Signal Layer."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from .category_signals import CategorySignalError, build_category_signal_layer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build YEE-73 source-aware category signals from accepted YEE-61 SQLite"
    )
    parser.add_argument("--input-db", required=True, type=Path, help="Read-only accepted YEE-61 category_first_foundation.sqlite")
    parser.add_argument("--output-dir", required=True, type=Path, help="Empty local output directory for YEE-73 artifacts")
    parser.add_argument("--code-commit", default=None, help="Exact source commit recorded in immutable run metadata")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        qa = build_category_signal_layer(args.input_db, args.output_dir, code_commit=args.code_commit)
    except (CategorySignalError, OSError, sqlite3.Error) as exc:
        parser.exit(2, f"YEE-73 category signal build failed: {exc}\n")
    print(json.dumps({
        "status": qa["status"],
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
