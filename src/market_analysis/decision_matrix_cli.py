"""Command-line runner for the YEE-59 decision-matrix build."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .decision_matrix import DecisionMatrixError, build_decision_set


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Build the deterministic YEE-59 concept decision set"
    )
    parser.add_argument(
        "--yee57-input-dir",
        required=True,
        type=Path,
        help="directory containing the three accepted YEE-57 inputs",
    )
    parser.add_argument(
        "--yee55-input-dir",
        required=True,
        type=Path,
        help="directory containing the accepted YEE-55 alternatives JSONL and SQLite",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="new or empty output directory",
    )
    args = parser.parse_args()
    try:
        qa = build_decision_set(
            args.yee57_input_dir,
            args.yee55_input_dir,
            args.output_dir,
        )
    except (DecisionMatrixError, OSError, sqlite3.Error) as exc:
        parser.exit(2, f"YEE-59 decision matrix failed: {exc}\n")
    print(json.dumps({
        "status": qa["status"],
        "matrix_rows": qa["row_counts"]["concept_decision_matrix"],
        "pareto_rows": qa["row_counts"]["pareto_dominance"],
        "decision_set_rows": qa["row_counts"]["supervisor_decision_set"],
        "frontier_orders": qa["frontier_orders"],
        "dominated_orders": qa["dominated_orders"],
        "output_dir": str(args.output_dir.resolve()),
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
