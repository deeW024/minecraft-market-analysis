"""CLI for the deterministic YEE-43 family signal layer."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .family_signals import build_family_signal_layer


def main() -> int:
    parser = argparse.ArgumentParser(description="Build YEE-43 family-level source-aware market signals")
    parser.add_argument("--yee37-db", required=True, type=Path, help="accepted YEE-37 concept_retrieval.sqlite")
    parser.add_argument("--yee30-db", required=True, type=Path, help="accepted YEE-30 retrieval.db (read-only)")
    parser.add_argument("--yee29-db", required=True, type=Path, help="accepted YEE-29 analysis.db (read-only)")
    parser.add_argument("--output-dir", required=True, type=Path, help="new or empty output directory")
    args = parser.parse_args()
    result = build_family_signal_layer(args.yee37_db, args.yee30_db, args.yee29_db, args.output_dir)
    print(json.dumps({"status": result["status"], "output_dir": str(result["output_dir"]), "checks": result["qa"]["checks"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
