"""Command-line runner for the authorized YEE-57 synthesis pilot."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from .concept_synthesis import ConceptSynthesisError, build_pilot


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the authorized YEE-57 concept-synthesis pilot")
    parser.add_argument("--input-dir", required=True, type=Path, help="accepted YEE-55 production artifacts")
    parser.add_argument("--capture", required=True, type=Path, help="authorized CONCEPT_CAPTURE.json")
    parser.add_argument("--output-dir", required=True, type=Path, help="pilot output directory")
    args = parser.parse_args()
    try:
        qa = build_pilot(args.input_dir, args.capture, args.output_dir)
    except (ConceptSynthesisError, OSError, sqlite3.Error) as exc:
        parser.exit(2, f"YEE-57 pilot failed: {exc}\n")
    print(json.dumps({
        "status": qa["status"],
        "readiness_rows": qa["row_counts"]["concept_readiness_matrix"],
        "concept_cards": qa["row_counts"]["opportunity_concept_cards"],
        "ready_orders": qa["concept_ready_orders"],
        "output_dir": str(args.output_dir.resolve()),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
