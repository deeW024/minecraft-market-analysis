from __future__ import annotations

import argparse
import json

from .opportunity_triage import build_opportunity_triage


def main() -> None:
    parser = argparse.ArgumentParser(description="Build deterministic YEE-46 opportunity triage outputs")
    parser.add_argument("--input-db", required=True, help="accepted YEE-43 family_signal_analysis.sqlite")
    parser.add_argument("--output-dir", required=True, help="new or empty YEE-46 artifact directory")
    args = parser.parse_args()
    result = build_opportunity_triage(args.input_db, args.output_dir)
    print(json.dumps({
        "status": result["status"],
        "output_dir": str(result["output_dir"]),
        "output_counts": result["qa"]["details"]["output_counts"],
        "triage_bucket_counts": result["qa"]["details"]["triage_bucket_counts"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
