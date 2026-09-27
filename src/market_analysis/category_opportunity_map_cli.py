"""Command line interface for the deterministic YEE-76 build."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from .category_opportunity_map import CategoryOpportunityError, build_category_opportunity_map


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the offline YEE-76 category opportunity map")
    parser.add_argument("--input-db", required=True, type=Path, help="accepted YEE-75 category-direction SQLite")
    parser.add_argument("--output-dir", required=True, type=Path, help="new or empty YEE-76 artifact directory")
    parser.add_argument("--code-commit", required=True, help="YEE-76 source commit SHA")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        qa = build_category_opportunity_map(
            args.input_db,
            args.output_dir,
            code_commit=args.code_commit,
        )
    except (CategoryOpportunityError, OSError, sqlite3.Error) as exc:
        print(f"YEE-76 build failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({
        "work_order": qa["work_order"],
        "status": qa["status"],
        "delivery_status": qa["delivery_status"],
        "run_id": qa["run_id"],
        "code_commit": qa["code_commit"],
        "category_opportunity_state_counts": qa["category_opportunity_state_counts"],
        "failed_checks": qa["failed_checks"],
        "output_dir": str(args.output_dir.resolve()),
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
