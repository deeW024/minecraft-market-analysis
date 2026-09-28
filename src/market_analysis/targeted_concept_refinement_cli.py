from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from .targeted_concept_refinement import RefinementError, build_bundle


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build deterministic YEE-95 targeted refinement artifacts")
    parser.add_argument("--yee83-db", required=True, type=Path)
    parser.add_argument("--yee83-capture", required=True, type=Path)
    parser.add_argument("--yee81-db", required=True, type=Path)
    parser.add_argument("--yee81-capture", required=True, type=Path)
    parser.add_argument("--yee79-db", required=True, type=Path)
    parser.add_argument("--yee77-db", required=True, type=Path)
    parser.add_argument("--capture", required=True, type=Path)
    parser.add_argument("--goal-alignment", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--repo-root", required=True, type=Path)
    parser.add_argument("--pull-request-url", required=True)
    parser.add_argument("--drive-folder-url", required=True)
    parser.add_argument("--execution-commit")
    args = parser.parse_args(argv)
    inputs = {
        "yee83_sqlite": args.yee83_db, "yee83_capture": args.yee83_capture,
        "yee81_sqlite": args.yee81_db, "yee81_capture": args.yee81_capture,
        "yee79_sqlite": args.yee79_db, "yee77_sqlite": args.yee77_db,
    }
    try:
        commit = args.execution_commit or subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=args.repo_root, text=True
        ).strip()
        qa = build_bundle(inputs, args.capture, args.goal_alignment, args.output_dir, commit, args.pull_request_url, args.drive_folder_url)
    except (RefinementError, OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"YEE-95 build failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"status": qa["status"], "concept_count": qa["authorized_concept_count"], "mandatory_queries": qa["mandatory_query_count"], "failed_checks": qa["failed_checks"], "output_dir": str(args.output_dir.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
