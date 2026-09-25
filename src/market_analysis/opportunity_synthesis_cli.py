from __future__ import annotations

import argparse
import json
from pathlib import Path

from .opportunity_synthesis import build_synthesis


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the deterministic YEE-54 opportunity synthesis")
    parser.add_argument("--shortlist", required=True, type=Path, help="accepted YEE-46 opportunity_shortlist.jsonl")
    parser.add_argument("--analysis-db", required=True, type=Path, help="accepted YEE-46 opportunity_triage_analysis.sqlite")
    parser.add_argument("--research-db", required=True, type=Path, help="accepted YEE-47 external_market_research.sqlite")
    parser.add_argument("--family-packs", required=True, type=Path, help="accepted YEE-47 family_research_packs.jsonl")
    parser.add_argument("--evidence", required=True, type=Path, help="accepted YEE-47 external_evidence.jsonl")
    parser.add_argument("--competitors", required=True, type=Path, help="accepted YEE-47 competitor_entities.jsonl")
    parser.add_argument("--queries", required=True, type=Path, help="accepted YEE-47 research_queries.jsonl")
    parser.add_argument("--output-dir", required=True, type=Path, help="new or empty YEE-54 artifacts directory")
    args = parser.parse_args()
    result = build_synthesis({
        "yee46_shortlist": args.shortlist,
        "yee46_analysis_db": args.analysis_db,
        "yee47_research_db": args.research_db,
        "yee47_family_packs": args.family_packs,
        "yee47_evidence": args.evidence,
        "yee47_competitors": args.competitors,
        "yee47_queries": args.queries,
    }, args.output_dir)
    print(json.dumps({
        "status": result["status"],
        "output_dir": str(result["output_dir"]),
        "row_counts": result["qa"]["details"]["row_counts"],
        "manifest": str(result["output_dir"] / "DATASET_MANIFEST.json"),
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
