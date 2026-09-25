"""Command-line runner for the YEE-60 plugin-only eligibility gate."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .plugin_eligibility import PluginEligibilityError, build_plugin_universe


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Build the YEE-60 plugin-only universe from accepted YEE-29 artifacts"
    )
    parser.add_argument("--input-db", required=True, type=Path)
    parser.add_argument("--input-jsonl", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    try:
        qa = build_plugin_universe(args.input_db, args.input_jsonl, args.output_dir)
    except (PluginEligibilityError, OSError, sqlite3.Error) as exc:
        parser.exit(2, f"YEE-60 plugin eligibility failed: {exc}\n")
    print(json.dumps({
        "status": qa["status"],
        "row_counts": qa["row_counts"],
        "status_counts": qa["status_counts"],
        "failed_checks": qa["failed_checks"],
        "output_dir": str(args.output_dir.resolve()),
    }, ensure_ascii=False, sort_keys=True))
    return 0 if qa["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
