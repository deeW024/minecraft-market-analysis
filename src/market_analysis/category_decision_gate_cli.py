from __future__ import annotations

import argparse
import json
from pathlib import Path

from market_analysis.category_decision_gate import build_bundle


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the deterministic YEE-79 category decision dossier")
    parser.add_argument("--input-db", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pull-request-url")
    parser.add_argument("--verification-json", type=Path)
    args = parser.parse_args()
    verification = None
    if args.verification_json:
        verification = json.loads(args.verification_json.read_text(encoding="utf-8"))
    try:
        result = build_bundle(
            args.input_db,
            args.output_dir,
            pull_request_url=args.pull_request_url,
            verification=verification,
        )
    except Exception as exc:
        print(json.dumps({"overall_status": "FAIL", "error": f"{type(exc).__name__}: {exc}"}, sort_keys=True))
        raise SystemExit(1) from exc
    print(json.dumps({
        "overall_status": result["overall_status"],
        "failed_checks": result["failed_checks"],
        "check_count": result["qa"]["check_count"],
        "passing_checks": result["qa"]["passing_checks"],
    }, sort_keys=True))
    if result["overall_status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
