"""Command-line runner for the YEE-61 category-first foundation."""

from __future__ import annotations

import argparse
import json

from .category_foundation import CategoryFoundationError, build_category_foundation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build YEE-61 scope/category foundation from accepted YEE-60 artifacts"
    )
    parser.add_argument("--input-db", required=True, help="Read-only accepted YEE-60 plugin_eligibility.sqlite")
    parser.add_argument("--input-jsonl", required=True, help="Accepted YEE-60 plugin_only_resource_features.jsonl")
    parser.add_argument("--output-dir", required=True, help="New local directory for YEE-61 production artifacts")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        qa = build_category_foundation(args.input_db, args.input_jsonl, args.output_dir)
    except (CategoryFoundationError, OSError) as exc:
        build_parser().exit(2, f"YEE-61 category foundation failed: {exc}\n")
    print(json.dumps({
        "status": qa["status"],
        "row_counts": qa["row_counts"],
        "failed_checks": qa["failed_checks"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
