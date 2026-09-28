from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from .deep_commercial_validation import BASELINE_COMMIT, build_dataset


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the YEE-81 Stage G commercial validation dataset")
    parser.add_argument("--yee79-db", required=True, type=Path)
    parser.add_argument("--yee77-db", required=True, type=Path)
    parser.add_argument("--capture", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    execution_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    subprocess.run(
        ["git", "merge-base", "--is-ancestor", BASELINE_COMMIT, execution_commit], check=True
    )
    result = build_dataset(
        args.yee79_db, args.yee77_db, args.capture, args.output_dir, execution_commit
    )
    print(f"status={result['status']}")
    print(f"output_dir={result['output_dir']}")
    print(f"execution_code_commit={execution_commit}")
    print(f"qa_checks={sum(result['qa']['checks'].values())}/{len(result['qa']['checks'])}")
    if result["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
