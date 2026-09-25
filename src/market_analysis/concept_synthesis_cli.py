"""Command-line runner for the authorized YEE-57 synthesis stage."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from .concept_synthesis import ConceptSynthesisError, build_final, build_pilot


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an authorized YEE-57 concept-synthesis stage")
    parser.add_argument("--input-dir", required=True, type=Path, help="accepted YEE-55 production artifacts")
    parser.add_argument("--capture", required=True, type=Path, help="authorized CONCEPT_CAPTURE.json")
    parser.add_argument("--output-dir", required=True, type=Path, help="output directory")
    parser.add_argument("--stage", choices=("pilot", "final"), default="pilot")
    parser.add_argument("--accepted-pilot-dir", type=Path, help="immutable accepted pilot directory required by final stage")
    args = parser.parse_args()
    try:
        if args.stage == "final":
            if args.accepted_pilot_dir is None:
                parser.error("--accepted-pilot-dir is required with --stage final")
            qa = build_final(args.input_dir, args.accepted_pilot_dir, args.capture, args.output_dir)
        else:
            qa = build_pilot(args.input_dir, args.capture, args.output_dir)
    except (ConceptSynthesisError, OSError, sqlite3.Error) as exc:
        parser.exit(2, f"YEE-57 synthesis failed: {exc}\n")
    print(json.dumps({
        "status": qa["status"],
        "readiness_rows": qa["row_counts"]["concept_readiness_matrix"],
        "concept_cards": qa["row_counts"]["opportunity_concept_cards"],
        "ready_orders": qa["concept_ready_orders"],
        "concept_card_orders": qa["concept_card_orders"],
        "stage": args.stage,
        "output_dir": str(args.output_dir.resolve()),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
