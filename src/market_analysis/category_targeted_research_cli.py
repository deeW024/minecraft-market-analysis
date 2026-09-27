from __future__ import annotations

import argparse
import json
from pathlib import Path

from market_analysis.category_targeted_research import build_bundle


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the deterministic YEE-77 external research bundle")
    parser.add_argument("--input-db", type=Path, required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    result = build_bundle(args.input_db, args.capture, args.output_dir)
    print(json.dumps({"overall_status": result["overall_status"], "failed_checks": result["failed_checks"]}, sort_keys=True))
    if result["overall_status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
