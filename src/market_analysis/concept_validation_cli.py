from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .concept_validation import ConceptValidationError, build_bundle


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build deterministic YEE-83 concept-validation artifacts")
    parser.add_argument("--yee81-db", required=True, type=Path)
    parser.add_argument("--yee81-capture", required=True, type=Path)
    parser.add_argument("--yee79-db", required=True, type=Path)
    parser.add_argument("--yee77-db", required=True, type=Path)
    parser.add_argument("--capture", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--repo-root", required=True, type=Path)
    parser.add_argument("--execution-commit", required=True)
    args = parser.parse_args(argv)
    inputs = {
        "yee81_sqlite": args.yee81_db,
        "yee81_capture": args.yee81_capture,
        "yee79_sqlite": args.yee79_db,
        "yee77_sqlite": args.yee77_db,
    }
    try:
        qa = build_bundle(inputs, args.capture, args.output_dir, args.execution_commit, args.repo_root)
    except (ConceptValidationError, OSError, ValueError) as exc:
        print(f"YEE-83 build failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"status": qa["status"], "concept_count": qa["concept_count"], "mandatory_query_count": qa["mandatory_query_count"], "failed_checks": qa["failed_checks"], "output_dir": str(args.output_dir.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
