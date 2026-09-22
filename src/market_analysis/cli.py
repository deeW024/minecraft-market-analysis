from __future__ import annotations

import argparse
from pathlib import Path

from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the YEE-29 deterministic feature layer")
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--input-db", required=True, type=Path)
    build.add_argument("--output-dir", required=True, type=Path)
    build.add_argument("--analysis-as-of")
    build.add_argument("--code-version", default="working-tree")
    args = parser.parse_args()
    if args.command == "build":
        result = run_pipeline(args.input_db, args.output_dir, args.analysis_as_of, args.code_version)
        print(f"status={result['qa']['status']} run_id={result['run_id']} analysis_as_of={result['analysis_as_of']}")


if __name__ == "__main__":
    main()


