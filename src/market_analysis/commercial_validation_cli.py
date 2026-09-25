from __future__ import annotations

import argparse
import json
from pathlib import Path

from .commercial_validation import build_full, build_pilot


def main() -> int:
    parser = argparse.ArgumentParser(description="Build authorized YEE-55 deep-commercial-validation artifacts")
    parser.add_argument("--stage", choices=("pilot", "full"), default="pilot", help="pilot or authorized full 15-family final dataset")
    parser.add_argument("--cohort", required=True, type=Path, help="accepted YEE-54 next_validation_cohort.jsonl")
    parser.add_argument("--synthesis-db", required=True, type=Path, help="accepted read-only YEE-54 opportunity_synthesis.sqlite")
    parser.add_argument("--yee47-db", required=True, type=Path, help="accepted read-only YEE-47 external_market_research.sqlite")
    parser.add_argument("--capture", required=True, type=Path, help="bounded public-research capture JSON")
    parser.add_argument("--pilot-capture", type=Path, help="accepted order-1..5 capture, required for --stage full")
    parser.add_argument("--accepted-pilot-output", type=Path, help="accepted pilot artifact directory, required for --stage full")
    parser.add_argument("--output-dir", required=True, type=Path, help="empty production output directory")
    args = parser.parse_args()
    if args.stage == "full":
        if args.pilot_capture is None or args.accepted_pilot_output is None:
            parser.error("--stage full requires --pilot-capture and --accepted-pilot-output")
        result = build_full(args.cohort, args.synthesis_db, args.yee47_db, args.pilot_capture,
                            args.capture, args.accepted_pilot_output, args.output_dir)
    else:
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
