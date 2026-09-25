from __future__ import annotations

import argparse
import json
from pathlib import Path

from .external_research import _load_accepted_inputs, build_final, build_pilot


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an authorized YEE-47 external research dataset")
    parser.add_argument("--shortlist", required=True, type=Path, help="accepted YEE-46 opportunity_shortlist.jsonl")
    parser.add_argument("--analysis-db", required=True, type=Path, help="accepted YEE-46 opportunity_triage_analysis.sqlite")
    parser.add_argument("--preflight", action="store_true", help="verify accepted inputs and print pilot identities")
    parser.add_argument("--capture", type=Path, help="opened-source research capture JSON")
    parser.add_argument("--final", action="store_true", help="build the authorized final ranks 1..100 dataset")
    parser.add_argument("--accepted-pilot-capture", type=Path, help="accepted immutable pilot ranks 1..10 capture (required with --final)")
    parser.add_argument("--output-dir", type=Path, help="new/empty production artifact directory")
    args = parser.parse_args()

    if args.preflight:
        families, hashes = _load_accepted_inputs(args.shortlist, args.analysis_db)
        print(json.dumps({
            "status": "PASS",
            "inputs": hashes,
            "pilot_families": [
                {key: row.get(key) for key in (
                    "consensus_rank", "family_id", "canonical_topic_key", "member_topic_keys", "aliases",
                    "candidate_classes", "consensus_score", "balanced_rank", "demand_first_rank",
                    "whitespace_first_rank",
                )}
                for row in families[:10]
            ],
        }, ensure_ascii=False, sort_keys=True))
        return 0

    if args.capture is None or args.output_dir is None:
        parser.error("--capture and --output-dir are required unless --preflight is used")
    if args.final:
        if args.accepted_pilot_capture is None:
            parser.error("--accepted-pilot-capture is required with --final")
        result = build_final(args.shortlist, args.analysis_db, args.capture, args.accepted_pilot_capture, args.output_dir)
    else:
        result = build_pilot(args.shortlist, args.analysis_db, args.capture, args.output_dir)
    print(json.dumps({
        "status": result["status"],
        "output_dir": str(result["output_dir"]),
        "row_counts": result["qa"]["details"]["row_counts"],
        "manifest": str(result["output_dir"] / "DATASET_MANIFEST.json"),
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
