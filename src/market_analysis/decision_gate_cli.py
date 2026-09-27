from __future__ import annotations

import argparse
import json
from pathlib import Path

from market_analysis.decision_gate import _current_execution_commit, build_decision_gate


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the deterministic YEE-78 Supervisor/User decision gate from accepted YEE-77 SQLite"
    )
    parser.add_argument("--input-db", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    commit = _current_execution_commit()
    qa = build_decision_gate(args.input_db, args.output_dir, commit)
    print(json.dumps({
        "status": qa["status"],
        "failed_checks": qa["failed_checks"],
        "execution_code_commit": commit,
        "readiness_counts": qa["reconciliation"]["readiness_counts"],
    }, sort_keys=True))
    if qa["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
