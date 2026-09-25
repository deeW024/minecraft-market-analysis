from __future__ import annotations

import argparse
import json
from pathlib import Path

from .commercial_validation import build_pilot


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the authorized YEE-55 deep-commercial-validation pilot")
    parser.add_argument("--cohort", required=True, type=Path, help="accepted YEE-54 next_validation_cohort.jsonl")
    parser.add_argument("--synthesis-db", required=True, type=Path, help="accepted read-only YEE-54 opportunity_synthesis.sqlite")
    parser.add_argument("--yee47-db", required=True, type=Path, help="accepted read-only YEE-47 external_market_research.sqlite")
    parser.add_argument("--capture", required=True, type=Path, help="bounded public-research capture JSON")
    parser.add_argument("--output-dir", required=True, type=Path, help="empty production output directory")
    args = parser.parse_args()
    result = build_pilot(args.cohort, args.synthesis_db, args.yee47_db, args.capture, args.output_dir)
    print(json.dumps({
        "status": result["status"],
        "output_dir": str(result["output_dir"]),
        "row_counts": result["qa"]["details"]["row_counts"],
        "qa": str(result["output_dir"] / "QA_RESULT.json"),
        "manifest": str(result["output_dir"] / "DATASET_MANIFEST.json"),
    }, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
