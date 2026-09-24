"""CLI for the YEE-37 candidate-pair pilot and its fixed stability audit."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import subprocess
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping

from .concept_pairs import (
    EXPECTED_MODEL_ALIAS,
    EXPECTED_MODEL_VERSION,
    PAIR_GENERATION_VERSION,
    PAIR_NORMALIZATION_VERSION,
    PAIR_OUTPUT_VERSION,
    PAIR_POLICY_VERSION,
    PAIR_QUESTION_SET_VERSION,
    PAIR_MAX_EXAMPLES_PER_TOPIC,
    PAIR_STATE_MAX_BYTES,
    PAIR_STATE_VERSION,
    PILOT_SIZE,
    RESOLUTION_POLICY_VERSION,
    STABILITY_REPETITIONS,
    STABILITY_SIZE,
    YEE30_ACCEPTED_DB_SHA256,
    YEE31_ACCEPTED_INPUT_SHA256,
    YEE31_RUN_ID,
    PairAdjudicationRunner,
    apply_resolution_policy_v03,
    build_pair_state,
    build_pilot_report,
    build_stability_report,
    deterministic_manual_audit_sample,
    generate_candidate_pairs,
    load_accepted_advance_topics,
    load_pair_questions,
    select_stratified_pairs,
)
from .jev_triage import (
    DIRECT_BASE_URL,
    canonical_json,
    jev_provider_from_env,
    sha256_bytes,
    sha256_file,
)

V02_ACCEPTED_RUN_ID = "3471b3fe8ece9cb122b336c9a700a7a3a393953fb719dbe47e8b999fb4cc65ee"
V02_ACCEPTED_CODE_COMMIT = "9bd3a09062d49eafc78a154bc389d6ae732d4b0a"
V02_CANDIDATE_UNIVERSE_SHA256 = "c9701db8fc7d75b7d96cb70126344093ca839574ade8773bd29d1898444f23db"
V01_MANUAL_LABELS_SHA256 = "a4d563e402506d49e57e4d18c2c227e9f10cab8857016616ac357a77d87e0996"


def _jsonl_bytes(rows: list[Mapping[str, Any]]) -> bytes:
    return b"".join((canonical_json(row) + "\n").encode("utf-8") for row in rows)


def _write_generated(path: Path, content: bytes) -> None:
    """Create an immutable pre-dispatch artifact, or verify its exact replay."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != content:
            raise RuntimeError(f"refusing to replace existing non-identical artifact: {path.name}")
        return
    path.write_bytes(content)


def _write_rebuilt(path: Path, content: bytes) -> None:
    """Atomically replace outputs derived from resumable persisted attempts."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(content)
    temporary.replace(path)


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    _write_generated(path, (canonical_json(value) + "\n").encode("utf-8"))


def _write_rebuilt_json(path: Path, value: Mapping[str, Any]) -> None:
    _write_rebuilt(path, (canonical_json(value) + "\n").encode("utf-8"))


def _reuse_persisted_run_metadata(
    path: Path,
    current_run_basis: Mapping[str, Any],
) -> dict[str, Any]:
    if not path.is_file():
        raise RuntimeError("offline finalize requires persisted run metadata")
    stored = json.loads(path.read_text(encoding="utf-8"))
    if any(key not in stored for key in current_run_basis):
        raise RuntimeError("persisted run metadata is incomplete")
    stored_basis = {key: stored[key] for key in current_run_basis}
    stored_run_id = sha256_bytes(canonical_json(stored_basis).encode("utf-8"))
    if stored.get("run_id") != stored_run_id:
        raise RuntimeError("persisted run metadata identity is invalid")
    comparable_stored = {key: value for key, value in stored_basis.items() if key != "code_commit"}
    comparable_current = {
        key: value for key, value in current_run_basis.items() if key != "code_commit"
    }
    if comparable_stored != comparable_current:
        raise RuntimeError("offline finalize inputs differ from persisted run identity")
    return stored


def _write_jsonl(path: Path, rows: list[Mapping[str, Any]], *, rebuild: bool = False) -> None:
    writer = _write_rebuilt if rebuild else _write_generated
    writer(path, _jsonl_bytes(rows))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _write_raw_evidence(path: Path, rows: list[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("wb") as raw_file:
        with gzip.GzipFile(fileobj=raw_file, mode="wb", filename="", mtime=0) as compressed:
            for row in rows:
                compressed.write((canonical_json(row) + "\n").encode("utf-8"))
    temporary.replace(path)


def _write_outcome_csv(path: Path, rows: list[Mapping[str, Any]]) -> None:
    fields = [
        "pair_id", "left_topic_key", "right_topic_key", "replicate_id",
        "status", "attempt_count", "concept_relation", "merge_disposition", "merge_safe",
        "lexical_alias", "evidence_alignment", "relation_direction",
        "policy_decision", "returned_model", "probability_sum_deviations", "cache_key",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            normalized = row.get("normalized") or {}
            item = {key: row.get(key) for key in fields if key in row}
            for key in (
                "concept_relation", "merge_disposition", "merge_safe", "lexical_alias", "evidence_alignment",
                "relation_direction", "policy_decision", "returned_model",
                "probability_sum_deviations", "cache_key",
            ):
                value = normalized.get(key)
                if isinstance(value, Mapping):
                    if "choice" in value:
                        value = value["choice"]
                    elif "noul" in value:
                        value = value["noul"]
                    elif "score" in value:
                        value = value["score"]
                item[key] = value
            writer.writerow(item)
    temporary.replace(path)


def _pair_outcomes(
    selected: list[Mapping[str, Any]],
    results: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_id: dict[str, list[Mapping[str, Any]]] = {}
    for result in results:
        by_id.setdefault(str(result["pair_id"]), []).append(result)
    return [
        {
            "pair_id": pair["pair_id"],
            "left_topic_key": pair["left_topic_key"],
            "right_topic_key": pair["right_topic_key"],
            "blocking_reasons": pair["blocking_reasons"],
            "normalized_edit_similarity": pair["normalized_edit_similarity"],
            "token_jaccard": pair["token_jaccard"],
            "evidence_overlap_count": pair["evidence_overlap_count"],
            "sentinel_expectations": pair["sentinel_expectations"],
            **result,
        }
        for pair in selected
        for result in sorted(
            by_id.get(str(pair["pair_id"]), []),
            key=lambda item: (str(item.get("replicate_id") or ""), str(item.get("cache_key") or "")),
        )
    ]


def _manual_audit_packet(
    worklist: list[Mapping[str, Any]],
    pairs: list[Mapping[str, Any]],
    topics_by_key: Mapping[str, Mapping[str, Any]],
    outcomes: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    pair_by_id = {str(pair["pair_id"]): pair for pair in pairs}
    outcome_by_id = {str(row["pair_id"]): row for row in outcomes}

    def member(topic_key: str) -> dict[str, Any]:
        topic = topics_by_key[topic_key]
        pack = topic["evidence_pack"]
        return {
            "topic_key": topic_key,
            "candidate_class": topic["candidate"]["candidate_class"],
            "source_presence": topic["candidate"]["source_presence"],
            "topic_source_facts": topic["topic_source_facts"],
            "evidence_examples_by_source": {
                source: [
                    {
                        key: value
                        for key, value in example.items()
                        if key != "source_url"
                    }
                    for example in source_pack["examples"]
                ]
                for source, source_pack in sorted(pack["sources"].items())
            },
        }

    packet = []
    for row in worklist:
        pair_id = str(row["pair_id"])
        pair = pair_by_id[pair_id]
        outcome = outcome_by_id[pair_id]
        normalized = outcome["normalized"]
        packet.append({
            **row,
            "blocking_features": {
                "normalized_edit_similarity": pair["normalized_edit_similarity"],
                "token_jaccard": pair["token_jaccard"],
                "evidence_overlap_count": pair["evidence_overlap_count"],
            },
            "left_evidence": member(pair["left_topic_key"]),
            "right_evidence": member(pair["right_topic_key"]),
            "model_typed_answers": normalized["typed_answers"],
        })
    return packet


def _combine_manual_audit(
    worklist: list[Mapping[str, Any]],
    results_path: Path,
) -> list[dict[str, Any]]:
    results = {str(row.get("pair_id")): row for row in _read_jsonl(results_path)}
    combined = []
    for row in worklist:
        result = results.get(str(row["pair_id"]))
        if result is None:
            continue
        if result.get("manual_policy_decision") not in {"MERGE", "KEEP_SEPARATE", "REVIEW"}:
            raise ValueError(f"manual audit policy is invalid for {row['pair_id']}")
        if result.get("manual_relation") not in {
            "SAME_CONCEPT", "BROADER_NARROWER", "RELATED_DISTINCT", "UNRELATED", "INSUFFICIENT"
        }:
            raise ValueError(f"manual audit relation is invalid for {row['pair_id']}")
        combined.append({**row, **result})
    return combined


def _assert_terminal_outcomes(results: list[Mapping[str, Any]], label: str) -> None:
    nonterminal = sorted(
        {str(row.get("status")) for row in results}
        - {"completed", "failed"}
    )
    if nonterminal:
        raise RuntimeError(
            f"offline finalize found nonterminal {label} outcomes: {', '.join(nonterminal)}"
        )


def _acceptance_gate_status(
    pilot_report: Mapping[str, Any],
    stability_report: Mapping[str, Any],
) -> str:
    return (
        "PASS"
        if pilot_report.get("status") == "PASS"
        and all((stability_report.get("gates") or {}).values())
        else "FAIL"
    )


def _probability_qa(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    by_question: dict[str, list[float]] = {}
    for row in rows:
        normalized = row.get("normalized") or {}
        for question_id, deviation in normalized.get("probability_sum_deviations", {}).items():
            by_question.setdefault(question_id, []).append(abs(deviation["deviation_from_one"]))
    return {
        "non_unit_probability_sums": {
            question_id: {
                "count": len(values),
                "maximum_absolute_deviation": round(max(values), 12),
            }
            for question_id, values in sorted(by_question.items())
        },
        "raw_probability_values_preserved": True,
    }


def _attempt_metrics(
    outcomes: list[Mapping[str, Any]],
    attempts: list[Mapping[str, Any]],
) -> dict[str, Any]:
    stage_counts = Counter(str(row["stage"]) for row in attempts)
    dispatched = sum(bool(row.get("wire_request_captured")) for row in attempts)
    retries = sum(int(row["attempt_no"]) > 1 for row in attempts)
    token_input = 0
    token_output = 0
    model_counts: Counter[str] = Counter()
    successful = []
    for row in outcomes:
        normalized = row.get("normalized") or {}
        if normalized:
            model_counts[str(normalized["returned_model"])] += 1
            token_input += int(normalized.get("usage", {}).get("input_tokens", 0))
            token_output += int(normalized.get("usage", {}).get("output_tokens", 0))
            successful.append(row)
    return {
        "http_attempts": dispatched,
        "application_retry_attempts": retries,
        "attempt_stage_counts": dict(sorted(stage_counts.items())),
        "returned_model_distribution": dict(sorted(model_counts.items())),
        "usage_tokens": {"input_tokens": token_input, "output_tokens": token_output},
        "probability_qa": _probability_qa(successful),
    }


def _universe_qa(pairs: list[Mapping[str, Any]]) -> dict[str, Any]:
    reason_counts = Counter()
    class_counts = Counter()
    source_counts = Counter()
    band_counts = Counter()
    sentinel_counts = Counter()
    for pair in pairs:
        reason_counts.update(pair["blocking_reasons"])
        class_counts[">".join(pair["candidate_classes"])] += 1
        source_counts[">".join("+".join(items) or "none" for items in pair["source_presence"])] += 1
        score = max(pair["normalized_edit_similarity"], pair["token_jaccard"])
        band_counts["high" if score >= 0.92 else "mid" if score >= 0.70 else "low"] += 1
        for sentinel in pair["sentinel_expectations"]:
            sentinel_counts[sentinel["sentinel_id"]] += 1
    return {
        "candidate_pair_count": len(pairs),
        "unique_pair_ids": len({pair["pair_id"] for pair in pairs}),
        "fuzzy_only_pair_count": sum(bool(pair["fuzzy_only"]) for pair in pairs),
        "blocking_reason_counts": dict(sorted(reason_counts.items())),
        "candidate_class_pair_counts": dict(sorted(class_counts.items())),
        "source_presence_pair_counts": dict(sorted(source_counts.items())),
        "lexical_similarity_bands": dict(sorted(band_counts.items())),
        "sentinel_counts": dict(sorted(sentinel_counts.items())),
    }


def _write_report(
    path: Path,
    metadata: Mapping[str, Any],
    universe: Mapping[str, Any],
    pilot: Mapping[str, Any],
    stability: Mapping[str, Any],
    execution: Mapping[str, Any],
) -> None:
    lines = [
        "# YEE-37 Candidate-Pair Pilot",
        "",
        f"Status: {metadata['status']}",
        f"Acceptance gates: {metadata['acceptance_gate_status']}",
        "",
        f"Run ID: {metadata['run_id']}",
        f"Code commit: {metadata['code_commit']}",
        f"Artifact recalculation code commit: {metadata.get('artifact_finalization_code_commit', metadata['code_commit'])}",
        f"YEE-31 ADVANCE input SHA-256: {metadata['input_hashes']['yee31_advance_review_set_sha256']}",
        f"YEE-30 read-only DB SHA-256: {metadata['input_hashes']['yee30_retrieval_db_sha256_before']}",
        f"Question set {metadata['question_set_version']} / {metadata['question_set_sha256']}",
        f"Pair state {metadata['pair_state_version']}; policy {metadata['pair_policy_version']}; output {metadata['pair_output_version']}",
        f"Model alias {metadata['requested_model_alias']} -> expected {metadata['expected_model_version']}",
        f"Transport {metadata['transport_id']} at {metadata['endpoint']}",
        "",
        "## Deterministic pair universe",
        "",
        f"- Candidate pairs: {universe['candidate_pair_count']}",
        f"- Fuzzy-only pairs after the per-topic top-{metadata['fuzzy_neighbor_limit']} cap: {universe['fuzzy_only_pair_count']}",
        f"- Blocking reasons: {canonical_json(universe['blocking_reason_counts'])}",
        f"- Candidate class pairs: {canonical_json(universe['candidate_class_pair_counts'])}",
        f"- Source-presence combinations: {canonical_json(universe['source_presence_pair_counts'])}",
        f"- Sentinels present: {len(universe['sentinel_counts'])}",
        "",
        "Lexical metrics only block and stratify candidate pairs. They never determine MERGE.",
        "",
        "## 120-pair pilot",
        "",
        f"- Status: {pilot['status']}; valid typed responses {pilot['valid_typed_responses']}/{pilot['pair_count']}",
        f"- Policy decisions: {canonical_json(pilot['policy_decision_counts'])}",
        f"- Native relation answers: {canonical_json(pilot['native_relation_counts'])}",
        f"- HTTP attempts including retries: {execution['pilot']['http_attempts']}",
        f"- Usage tokens returned: {canonical_json(execution['pilot']['usage_tokens'])}",
        f"- Manual audit sample: {pilot['manual_audit_count']} pairs",
        f"- Gates: {canonical_json(pilot['gates'])}",
        "",
        "## 40-pair x 3 stability audit",
        "",
        f"- Status: {stability['status']}; complete pairs {stability['complete_candidate_count']}/{stability['candidate_count']}",
        f"- Authoritative policy exact agreement: {stability['authoritative_policy_exact_agreement_count']}/{stability['candidate_count']} ({stability['authoritative_policy_exact_agreement_rate']:.3f})",
        f"- Policy transitions: {canonical_json(stability['authoritative_policy_transition_patterns'])}",
        f"- Relation transitions: {canonical_json(stability['relation_transition_patterns'])}",
        f"- Sentinel MERGE/non-MERGE flips: {stability['sentinel_merge_flip_count']}",
        f"- HTTP attempts including retries: {execution['stability']['http_attempts']}",
        f"- Usage tokens returned: {canonical_json(execution['stability']['usage_tokens'])}",
        f"- Gates: {canonical_json(stability['gates'])}",
        "",
        "Per-question value, probability and confidence deltas are in STABILITY_QA.json. All replicate responses are retained; no majority vote is applied.",
        "",
        "## Scope boundary",
        "",
        "Only local candidate-pair generation, the authorized Jev pilot and fixed stability audit were run. No full pair adjudication, family build, marketplace/API request, external research, ranking, recommendation or later work order was run.",
        "",
        "PR remains open/draft/unmerged for supervisor review.",
        "",
    ]
    _write_rebuilt(path, "\n".join(lines).encode("utf-8"))


def _artifact_manifest(output_dir: Path, metadata: Mapping[str, Any]) -> dict[str, Any]:
    artifacts = []
    for path in sorted(output_dir.iterdir(), key=lambda item: item.name):
        if not path.is_file() or path.name in {"MANIFEST.json", "MANIFEST.json.tmp"}:
            continue
        artifacts.append({
            "path": path.name,
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        })
    return {
        "work_order": "YEE-37",
        "status": metadata["status"],
        "acceptance_gate_status": metadata["acceptance_gate_status"],
        "run_id": metadata["run_id"],
        "code_commit": metadata["code_commit"],
        "input_hashes": metadata["input_hashes"],
        "question_set_version": metadata["question_set_version"],
        "question_set_sha256": metadata["question_set_sha256"],
        "requested_model_alias": metadata["requested_model_alias"],
        "expected_model_version": metadata["expected_model_version"],
        "transport_id": metadata["transport_id"],
        "endpoint": metadata["endpoint"],
        "pair_generation_version": PAIR_GENERATION_VERSION,
        "pair_normalization_version": PAIR_NORMALIZATION_VERSION,
        "pair_state_version": PAIR_STATE_VERSION,
        "pair_question_set_version": PAIR_QUESTION_SET_VERSION,
        "pair_policy_version": PAIR_POLICY_VERSION,
        "pair_output_version": PAIR_OUTPUT_VERSION,
        "artifact_files": artifacts,
    }


def _load_cached_results(
    runner: PairAdjudicationRunner,
    pilot: list[Mapping[str, Any]],
    stability: list[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    pilot_all = runner.export_results({None})
    stability_all = runner.export_results(
        {f"stability-{rep}" for rep in range(1, STABILITY_REPETITIONS + 1)}
    )
    pilot_ids = {str(pair["pair_id"]) for pair in pilot}
    stability_ids = {str(pair["pair_id"]) for pair in stability}
    return (
        [row for row in pilot_all if row["pair_id"] in pilot_ids],
        [row for row in stability_all if row["pair_id"] in stability_ids],
    )


def _directory_hashes(directory: Path) -> list[dict[str, Any]]:
    if not directory.is_dir():
        raise RuntimeError(f"v0.1 evidence directory is missing: {directory}")
    return [
        {
            "path": path.relative_to(directory).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in sorted(directory.rglob("*"), key=lambda item: item.relative_to(directory).as_posix())
        if path.is_file()
    ]


def _required_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise RuntimeError(f"offline v0.3 replay is missing required evidence: {path.name}")
    return _read_jsonl(path)


def _v03_outcome(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = row.get("normalized")
    if not isinstance(normalized, Mapping):
        raise RuntimeError(f"offline v0.3 replay found an outcome without normalized Jev evidence: {row.get('pair_id')}")
    derived = dict(row)
    derived.update(apply_resolution_policy_v03(row, normalized))
    return derived


def _decision_counts(rows: list[Mapping[str, Any]], field: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row[field]) for row in rows).items()))


def _v03_sentinel_results(
    selected_pairs: list[Mapping[str, Any]],
    outcomes: list[Mapping[str, Any]],
    *,
    enforce_expectation: bool = True,
) -> list[dict[str, Any]]:
    by_id: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in outcomes:
        by_id[str(row["pair_id"])].append(row)
    results = []
    for pair in selected_pairs:
        for expectation in pair.get("sentinel_expectations", []):
            sentinel_id = str(expectation["sentinel_id"])
            observed = [
                str(row.get("resolution_decision"))
                for row in by_id.get(str(pair["pair_id"]), [])
            ]
            if sentinel_id.casefold() == "announce / announcer":
                results.append({
                    "pair_id": pair["pair_id"],
                    "sentinel_id": sentinel_id,
                    "v03_qa_role": "DIAGNOSTIC_ONLY",
                    "historical_v02_expected_policy": expectation["expected_policy"],
                    "resolution_decisions": observed,
                    "hard_gate": False,
                })
                continue
            expected = expectation["expected_policy"]
            if not enforce_expectation:
                results.append({
                    "pair_id": pair["pair_id"],
                    "sentinel_id": sentinel_id,
                    "pilot_expected_policy": expected,
                    "resolution_decisions": observed,
                    "hard_gate": True,
                    "acceptance_scope": "stability_replicate_diagnostics",
                    "replicates_agree": bool(observed) and len(set(observed)) == 1,
                })
                continue
            passed = bool(observed) and (
                all(decision == "MERGE" for decision in observed)
                if expected == "MERGE"
                else all(decision in {"KEEP_SEPARATE", "REVIEW"} for decision in observed)
            )
            results.append({
                "pair_id": pair["pair_id"],
                "sentinel_id": sentinel_id,
                "expected_policy": expected,
                "resolution_decisions": observed,
                "hard_gate": True,
                "pass": passed,
            })
    return results


def _write_resolution_csv(path: Path, rows: list[Mapping[str, Any]]) -> None:
    fields = [
        "pair_id", "left_topic_key", "right_topic_key", "replicate_id", "status",
        "attempt_count", "jev_policy_decision_v02", "resolution_decision",
        "resolution_source", "resolution_policy_version", "returned_model",
        "concept_relation", "merge_disposition", "cache_key",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            normalized = row.get("normalized") or {}
            item = {key: row.get(key) for key in fields}
            item["returned_model"] = normalized.get("returned_model")
            item["concept_relation"] = (normalized.get("concept_relation") or {}).get("choice")
            item["merge_disposition"] = (normalized.get("merge_disposition") or {}).get("choice")
            item["cache_key"] = normalized.get("cache_key")
            writer.writerow(item)
    temporary.replace(path)


def _offline_v03_replay(
    output_dir: Path,
    v02_evidence_dir: Path,
    v01_evidence_dir: Path,
    advance_path: Path,
    retrieval_db: Path,
    code_commit: str,
) -> dict[str, Any]:
    required = (
        "MANIFEST.json",
        "RUN_METADATA.json",
        "CANDIDATE_PAIRS.jsonl",
        "PILOT_SELECTION.jsonl",
        "STABILITY_SELECTION.jsonl",
        "PILOT_OUTCOMES.jsonl",
        "STABILITY_OUTCOMES.jsonl",
        "STABILITY_QA.json",
        "RAW_REQUEST_RESPONSE_EVIDENCE.jsonl.gz",
        "YEE37_PAIR_PILOT.sqlite",
    )
    if not v02_evidence_dir.is_dir() or not v01_evidence_dir.is_dir():
        raise RuntimeError("offline v0.3 replay requires existing v0.2 and v0.1 evidence directories")
    missing = [name for name in required if not (v02_evidence_dir / name).is_file()]
    if missing:
        raise RuntimeError(f"offline v0.3 replay is missing v0.2 evidence files: {', '.join(missing)}")

    v02_before = _directory_hashes(v02_evidence_dir)
    v01_before = _directory_hashes(v01_evidence_dir)
    v02_hashes = {item["path"]: item for item in v02_before}
    v02_manifest = json.loads((v02_evidence_dir / "MANIFEST.json").read_text(encoding="utf-8"))
    for declared in v02_manifest.get("artifact_files", []):
        actual = v02_hashes.get(str(declared.get("path")))
        if actual is None or any(
            actual.get(key) != declared.get(key) for key in ("bytes", "sha256")
        ):
            raise RuntimeError(f"v0.2 evidence does not match its manifest: {declared.get('path')}")

    advance_before = sha256_file(advance_path).lower()
    y30_before = sha256_file(retrieval_db).lower()
    if advance_before != YEE31_ACCEPTED_INPUT_SHA256 or y30_before != YEE30_ACCEPTED_DB_SHA256:
        raise RuntimeError("offline v0.3 replay inputs differ from the accepted YEE-31/YEE-30 hashes")

    run_metadata = json.loads((v02_evidence_dir / "RUN_METADATA.json").read_text(encoding="utf-8"))
    if (
        run_metadata.get("run_id") != V02_ACCEPTED_RUN_ID
        or run_metadata.get("code_commit") != V02_ACCEPTED_CODE_COMMIT
        or run_metadata.get("requested_model_alias") != EXPECTED_MODEL_ALIAS
        or run_metadata.get("pair_state_version") != PAIR_STATE_VERSION
        or run_metadata.get("pair_question_set_version") != PAIR_QUESTION_SET_VERSION
        or run_metadata.get("pair_policy_version") != PAIR_POLICY_VERSION
        or run_metadata.get("pair_output_version") != PAIR_OUTPUT_VERSION
        or run_metadata.get("expected_model_version") != EXPECTED_MODEL_VERSION
        or run_metadata.get("question_set_sha256") != "643e66605bf93c88875258b766e70364dd3b9fdda7d96ff85e813609e1c3a8af"
        or run_metadata.get("transport_id") != "typesafe-systemone-http-v1"
    ):
        raise RuntimeError("v0.2 run metadata does not match the accepted inference contract")

    candidate_pairs = _required_jsonl(v02_evidence_dir / "CANDIDATE_PAIRS.jsonl")
    pilot_pairs = _required_jsonl(v02_evidence_dir / "PILOT_SELECTION.jsonl")
    stability_pairs = _required_jsonl(v02_evidence_dir / "STABILITY_SELECTION.jsonl")
    pilot_source = _required_jsonl(v02_evidence_dir / "PILOT_OUTCOMES.jsonl")
    stability_source = _required_jsonl(v02_evidence_dir / "STABILITY_OUTCOMES.jsonl")
    if len(candidate_pairs) != 1_190 or len({str(row["pair_id"]) for row in candidate_pairs}) != 1_190:
        raise RuntimeError("v0.2 candidate universe must remain exactly 1,190 unique pairs")
    if sha256_file(v02_evidence_dir / "CANDIDATE_PAIRS.jsonl").lower() != V02_CANDIDATE_UNIVERSE_SHA256:
        raise RuntimeError("v0.2 candidate universe differs from its accepted SHA-256")
    if len(pilot_pairs) != PILOT_SIZE or len(stability_pairs) != STABILITY_SIZE:
        raise RuntimeError("v0.2 pilot/stability selection counts differ from the accepted identities")
    pilot_ids = [str(row["pair_id"]) for row in pilot_pairs]
    stability_ids = [str(row["pair_id"]) for row in stability_pairs]
    v01_hashes = {item["path"]: item["sha256"] for item in v01_before}
    selection_names = ("CANDIDATE_PAIRS.jsonl", "PILOT_SELECTION.jsonl", "STABILITY_SELECTION.jsonl")
    for name in selection_names:
        if v01_hashes.get(name) != v02_hashes.get(name, {}).get("sha256"):
            raise RuntimeError(f"v0.2 candidate universe/selection differs from immutable v0.1: {name}")
    if len(set(pilot_ids)) != PILOT_SIZE or len(set(stability_ids)) != STABILITY_SIZE:
        raise RuntimeError("v0.2 pilot/stability selections contain duplicate identities")
    if not set(pilot_ids + stability_ids).issubset({str(row["pair_id"]) for row in candidate_pairs}):
        raise RuntimeError("v0.2 pilot/stability selections are outside the accepted pair universe")
    if Counter(str(row["pair_id"]) for row in pilot_source) != Counter(pilot_ids):
        raise RuntimeError("v0.2 pilot outcomes do not reconcile to the exact 120 selected identities")
    stability_counts = Counter(str(row["pair_id"]) for row in stability_source)
    if len(stability_source) != STABILITY_SIZE * STABILITY_REPETITIONS or any(
        stability_counts[pair_id] != STABILITY_REPETITIONS for pair_id in stability_ids
    ) or set(stability_counts) != set(stability_ids):
        raise RuntimeError("v0.2 stability outcomes do not reconcile to the exact 40 × 3 identities")
    if any(
        row.get("status") != "completed"
        or not (row.get("normalized") or {}).get("typed_answers")
        or (row.get("normalized") or {}).get("returned_model") != EXPECTED_MODEL_VERSION
        for row in pilot_source + stability_source
    ):
        raise RuntimeError("v0.2 outcomes contain incomplete typed evidence or a non-pinned model identity")

    raw_evidence_path = v02_evidence_dir / "RAW_REQUEST_RESPONSE_EVIDENCE.jsonl.gz"
    with gzip.open(raw_evidence_path, "rt", encoding="utf-8") as evidence_file:
        raw_evidence_count = sum(1 for line in evidence_file if line.strip())
    if raw_evidence_count != PILOT_SIZE + STABILITY_SIZE * STABILITY_REPETITIONS:
        raise RuntimeError("v0.2 raw request/response evidence count does not reconcile to 240 attempts")

    pilot_outcomes = [_v03_outcome(row) for row in pilot_source]
    stability_outcomes = [_v03_outcome(row) for row in stability_source]
    if any(
        canonical_json(source.get("normalized")) != canonical_json(derived.get("normalized"))
        or derived["jev_policy_decision_v02"] != source["normalized"]["policy_decision"]
        for source, derived in zip(pilot_source + stability_source, pilot_outcomes + stability_outcomes)
    ):
        raise RuntimeError("v0.3 replay altered existing v0.2 typed/model evidence")

    pilot_by_id = {str(row["pair_id"]): row for row in pilot_outcomes}
    stability_by_id: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in stability_outcomes:
        stability_by_id[str(row["pair_id"])].append(row)
    pilot_sentinels = _v03_sentinel_results(pilot_pairs, pilot_outcomes)
    stability_sentinels = _v03_sentinel_results(
        stability_pairs,
        stability_outcomes,
        enforce_expectation=False,
    )
    pilot_hard_sentinels = [row for row in pilot_sentinels if row["hard_gate"]]
    stability_hard_sentinels = [row for row in stability_sentinels if row["hard_gate"]]
    pilot_merge_sentinels = [row for row in pilot_hard_sentinels if row["expected_policy"] == "MERGE"]
    pilot_nonmerge_sentinels = [row for row in pilot_hard_sentinels if row["expected_policy"] == "NOT_MERGE"]
    stability_rows = []
    resolution_transition_patterns: Counter[str] = Counter()
    native_policy_transition_patterns: Counter[str] = Counter()
    stable_count = 0
    native_stable_count = 0
    for pair in stability_pairs:
        pair_rows = sorted(stability_by_id[str(pair["pair_id"])], key=lambda row: str(row["replicate_id"]))
        decisions = [str(row["resolution_decision"]) for row in pair_rows]
        native_decisions = [str(row["jev_policy_decision_v02"]) for row in pair_rows]
        stable_count += len(decisions) == STABILITY_REPETITIONS and len(set(decisions)) == 1
        native_stable_count += len(native_decisions) == STABILITY_REPETITIONS and len(set(native_decisions)) == 1
        for first, second in zip(decisions, decisions[1:]):
            resolution_transition_patterns[f"{first}->{second}"] += 1
        for first, second in zip(native_decisions, native_decisions[1:]):
            native_policy_transition_patterns[f"{first}->{second}"] += 1
        stability_rows.append({
            "pair_id": pair["pair_id"],
            "replicate_count": len(pair_rows),
            "resolution_decisions": decisions,
            "resolution_sources": [str(row["resolution_source"]) for row in pair_rows],
            "jev_policy_decisions_v02": native_decisions,
            "resolution_exact_agreement": len(decisions) == STABILITY_REPETITIONS and len(set(decisions)) == 1,
            "jev_policy_v02_exact_agreement_diagnostic": len(native_decisions) == STABILITY_REPETITIONS and len(set(native_decisions)) == 1,
        })

    sentinel_merge_flips = []
    hard_sentinel_by_id = {
        str(pair["pair_id"]): expectation
        for pair in stability_pairs
        for expectation in pair.get("sentinel_expectations", [])
        if str(expectation["sentinel_id"]).casefold() != "announce / announcer"
    }
    for pair_id, expectation in hard_sentinel_by_id.items():
        decisions = [str(row["resolution_decision"]) for row in stability_by_id[pair_id]]
        if "MERGE" in decisions and any(decision != "MERGE" for decision in decisions):
            sentinel_merge_flips.append({
                "pair_id": pair_id,
                "sentinel_id": expectation["sentinel_id"],
                "decisions": decisions,
            })
    cross_phase_sentinel_variances = []
    for pair_id, expectation in hard_sentinel_by_id.items():
        pilot_decision = str(pilot_by_id[pair_id]["resolution_decision"])
        stability_decisions = [str(row["resolution_decision"]) for row in stability_by_id[pair_id]]
        if any((decision == "MERGE") != (pilot_decision == "MERGE") for decision in stability_decisions):
            cross_phase_sentinel_variances.append({
                "pair_id": pair_id,
                "sentinel_id": expectation["sentinel_id"],
                "pilot_resolution_decision": pilot_decision,
                "stability_resolution_decisions": stability_decisions,
                "acceptance_scope": "diagnostic_only_no_v02_evidence_rewritten",
            })

    v01_manual_path = v01_evidence_dir / "MANUAL_AUDIT_RESULTS.jsonl"
    manual_labels = _required_jsonl(v01_manual_path)
    if sha256_file(v01_manual_path).lower() != V01_MANUAL_LABELS_SHA256:
        raise RuntimeError("immutable v0.1 manual-audit labels differ from the accepted SHA-256")
    manual_ids = [str(row["pair_id"]) for row in manual_labels]
    if len(manual_labels) != 40 or len(set(manual_ids)) != 40 or not set(manual_ids).issubset(set(pilot_by_id)):
        raise RuntimeError("the immutable v0.1 40-label manual audit does not reconcile to the saved v0.2 pilot")
    policy_agreements = sum(
        str(label.get("manual_policy_decision")) == str(pilot_by_id[str(label["pair_id"])]["resolution_decision"])
        for label in manual_labels
    )
    source_pilot_by_id = {str(row["pair_id"]): row for row in pilot_source}
    v02_policy_agreements = sum(
        str(label.get("manual_policy_decision")) == str(
            (source_pilot_by_id[str(label["pair_id"])].get("normalized") or {}).get("policy_decision")
        )
        for label in manual_labels
    )
    relation_agreements = sum(
        str(label.get("manual_relation")) == str(
            (pilot_by_id[str(label["pair_id"])].get("normalized") or {}).get("concept_relation", {}).get("choice")
        )
        for label in manual_labels
    )
    historical_audit = {
        "authority": "diagnostic_only",
        "labels_modified": False,
        "source_file": "MANUAL_AUDIT_RESULTS.jsonl",
        "source_file_sha256": sha256_file(v01_manual_path),
        "label_count": len(manual_labels),
        "jev_policy_v02_agreement_count": v02_policy_agreements,
        "jev_policy_v02_agreement_rate": round(v02_policy_agreements / len(manual_labels), 6),
        "policy_agreement_count": policy_agreements,
        "policy_agreement_rate": round(policy_agreements / len(manual_labels), 6),
        "relation_agreement_count": relation_agreements,
        "relation_agreement_rate": round(relation_agreements / len(manual_labels), 6),
    }

    pilot_valid = sum(row.get("status") == "completed" for row in pilot_outcomes)
    pilot_gates = {
        "120_of_120_saved_v02_outcomes_replayed": len(pilot_outcomes) == PILOT_SIZE and pilot_valid == PILOT_SIZE,
        "all_7_hard_expected_merge_sentinels_resolve_merge": len(pilot_merge_sentinels) == 7 and all(row["pass"] for row in pilot_merge_sentinels),
        "all_4_hard_expected_not_merge_sentinels_resolve_separate_or_review": len(pilot_nonmerge_sentinels) == 4 and all(row["pass"] for row in pilot_nonmerge_sentinels),
        "announce_announcer_is_diagnostic_only": sum(row.get("v03_qa_role") == "DIAGNOSTIC_ONLY" for row in pilot_sentinels) == 1,
    }
    stability_gates = {
        "40_of_40_pairs_have_three_replicates_and_exact_resolution_agreement": len(stability_rows) == STABILITY_SIZE and stable_count == STABILITY_SIZE and all(row["replicate_count"] == STABILITY_REPETITIONS for row in stability_rows),
        "all_11_hard_sentinel_pairs_reconcile_to_three_replicates": len(stability_hard_sentinels) == 11 and all(len(row["resolution_decisions"]) == STABILITY_REPETITIONS for row in stability_hard_sentinels),
        "zero_hard_sentinel_merge_nonmerge_flips": not sentinel_merge_flips,
    }

    native_jev_v02_stability = {
        "exact_agreement_count": native_stable_count,
        "candidate_count": len(stability_rows),
        "exact_agreement_rate": round(native_stable_count / len(stability_rows), 6) if stability_rows else 0.0,
        "transition_patterns": dict(sorted(native_policy_transition_patterns.items())),
        "source_v02_qa_sha256": sha256_file(v02_evidence_dir / "STABILITY_QA.json"),
    }
    source_stability_qa = json.loads((v02_evidence_dir / "STABILITY_QA.json").read_text(encoding="utf-8"))
    for key in (
        "per_question_value_deltas",
        "per_question_probability_deltas",
        "per_question_confidence_deltas",
        "native_decision_exact_agreement_count",
        "native_decision_transition_patterns",
    ):
        if key in source_stability_qa:
            native_jev_v02_stability[key] = source_stability_qa[key]

    source_files_to_check = (
        "CANDIDATE_PAIRS.jsonl",
        "PILOT_SELECTION.jsonl",
        "STABILITY_SELECTION.jsonl",
        "PILOT_OUTCOMES.jsonl",
        "STABILITY_OUTCOMES.jsonl",
        "RAW_REQUEST_RESPONSE_EVIDENCE.jsonl.gz",
        "YEE37_PAIR_PILOT.sqlite",
    )
    source_file_hashes = {
        name: v02_hashes[name]["sha256"] for name in source_files_to_check
    }
    input_hashes_before_after = {
        "yee31_advance_review_set": {"before": advance_before, "after": None},
        "yee30_retrieval_db": {"before": y30_before, "after": None},
    }
    source_outcome_evidence_sha = sha256_bytes(canonical_json([
        {"pair_id": row["pair_id"], "replicate_id": row.get("replicate_id"), "normalized": row["normalized"]}
        for row in pilot_source + stability_source
    ]).encode("utf-8"))
    derived_outcome_evidence_sha = sha256_bytes(canonical_json([
        {"pair_id": row["pair_id"], "replicate_id": row.get("replicate_id"), "normalized": row["normalized"]}
        for row in pilot_outcomes + stability_outcomes
    ]).encode("utf-8"))
    all_gates = {**pilot_gates, **stability_gates}
    status = "PASS" if all(all_gates.values()) else "FAIL"
    def replay_metrics(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
        normalized_rows = [row["normalized"] for row in rows if row.get("normalized")]
        return {
            "outcome_count": len(rows),
            "failed_outcome_count": sum(row.get("status") != "completed" for row in rows),
            "http_attempt_count": sum(int(row.get("attempt_count", 0)) for row in rows),
            "retry_attempt_count": sum(max(0, int(row.get("attempt_count", 0)) - 1) for row in rows),
            "returned_model_distribution": dict(sorted(Counter(
                str(row.get("returned_model")) for row in normalized_rows
            ).items())),
            "usage_tokens": {
                "input_tokens": sum(int((row.get("usage") or {}).get("input_tokens", 0)) for row in normalized_rows),
                "output_tokens": sum(int((row.get("usage") or {}).get("output_tokens", 0)) for row in normalized_rows),
            },
            "probability_qa": _probability_qa([{"normalized": row} for row in normalized_rows]),
        }
    qa = {
        "work_order": "YEE-37",
        "status": "PAIR_RESOLUTION_V03_READY_FOR_SUPERVISOR_REVIEW",
        "resolution_gate_status": status,
        "execution_mode": "offline_v0.3_post_inference_replay",
        "jev_or_network_requests": 0,
        "code_commit": code_commit,
        "resolution_policy_version": RESOLUTION_POLICY_VERSION,
        "inference_contract_versions_unchanged": {
            "pair_state_version": PAIR_STATE_VERSION,
            "question_set_version": PAIR_QUESTION_SET_VERSION,
            "pair_policy_version": PAIR_POLICY_VERSION,
            "pair_output_version": PAIR_OUTPUT_VERSION,
            "requested_model_alias": run_metadata["requested_model_alias"],
            "expected_model_version": EXPECTED_MODEL_VERSION,
        },
        "run_id": run_metadata["run_id"],
        "accepted_input_hashes_before_after": input_hashes_before_after,
        "candidate_universe": {
            "pair_count": len(candidate_pairs),
            "unique_pair_id_count": len({str(row["pair_id"]) for row in candidate_pairs}),
            "sha256": source_file_hashes["CANDIDATE_PAIRS.jsonl"],
            "accepted_sha256_preserved": source_file_hashes["CANDIDATE_PAIRS.jsonl"] == V02_CANDIDATE_UNIVERSE_SHA256,
            "byte_identical_to_v01": v01_hashes["CANDIDATE_PAIRS.jsonl"] == source_file_hashes["CANDIDATE_PAIRS.jsonl"],
            "pilot_selection_sha256": source_file_hashes["PILOT_SELECTION.jsonl"],
            "pilot_selection_byte_identical_to_v01": v01_hashes["PILOT_SELECTION.jsonl"] == source_file_hashes["PILOT_SELECTION.jsonl"],
            "stability_selection_sha256": source_file_hashes["STABILITY_SELECTION.jsonl"],
            "stability_selection_byte_identical_to_v01": v01_hashes["STABILITY_SELECTION.jsonl"] == source_file_hashes["STABILITY_SELECTION.jsonl"],
        },
        "pilot": {
            "selected_identity_count": len(pilot_pairs),
            "replayed_outcome_count": len(pilot_outcomes),
            "valid_typed_responses": pilot_valid,
            "jev_policy_decision_v02_counts": _decision_counts(pilot_outcomes, "jev_policy_decision_v02"),
            "resolution_decision_counts": _decision_counts(pilot_outcomes, "resolution_decision"),
            "resolution_source_counts": _decision_counts(pilot_outcomes, "resolution_source"),
            "hard_sentinel_results": pilot_hard_sentinels,
            "diagnostic_sentinel_results": [row for row in pilot_sentinels if not row["hard_gate"]],
            "gates": pilot_gates,
        },
        "stability": {
            "selected_pair_count": len(stability_pairs),
            "replayed_replicate_count": len(stability_outcomes),
            "runs_per_pair": STABILITY_REPETITIONS,
            "resolution_exact_agreement_count": stable_count,
            "resolution_exact_agreement_rate": round(stable_count / len(stability_rows), 6) if stability_rows else 0.0,
            "resolution_transition_patterns": dict(sorted(resolution_transition_patterns.items())),
            "resolution_decision_counts": _decision_counts(stability_outcomes, "resolution_decision"),
            "resolution_source_counts": _decision_counts(stability_outcomes, "resolution_source"),
            "jev_policy_decision_v02_counts": _decision_counts(stability_outcomes, "jev_policy_decision_v02"),
            "jev_policy_v02_stability_diagnostic": native_jev_v02_stability,
            "pair_reconciliation": stability_rows,
            "hard_sentinel_results": stability_hard_sentinels,
            "diagnostic_sentinel_results": [row for row in stability_sentinels if not row["hard_gate"]],
            "hard_sentinel_merge_nonmerge_flips": sentinel_merge_flips,
            "pilot_vs_stability_hard_sentinel_variances": cross_phase_sentinel_variances,
            "gates": stability_gates,
        },
        "historical_v01_manual_audit": historical_audit,
        "source_v02_execution_metrics": {
            "pilot": replay_metrics(pilot_outcomes),
            "stability": replay_metrics(stability_outcomes),
            "all": replay_metrics(pilot_outcomes + stability_outcomes),
        },
        "evidence_integrity": {
            "source_v02_file_inventory_before": v02_before,
            "source_v01_file_inventory_before": v01_before,
            "source_v02_selected_file_hashes_before_after": {name: {"before": digest, "after": None} for name, digest in source_file_hashes.items()},
            "raw_evidence_record_count": raw_evidence_count,
            "raw_response_and_model_evidence_unchanged": source_outcome_evidence_sha == derived_outcome_evidence_sha,
            "normalized_v02_evidence_sha256_before": source_outcome_evidence_sha,
            "normalized_v02_evidence_sha256_after": derived_outcome_evidence_sha,
        },
        "all_gates": all_gates,
        "full_pair_adjudication_and_family_build": "NOT_RUN_BLOCKED_PENDING_SUPERVISOR_REVIEW",
    }

    if output_dir.exists():
        unexpected = [path.name for path in output_dir.iterdir() if path.is_file() and path.name not in {
            "PILOT_OUTCOMES_V03.jsonl", "PILOT_OUTCOMES_V03.csv",
            "STABILITY_OUTCOMES_V03.jsonl", "STABILITY_OUTCOMES_V03.csv",
            "RESOLUTION_QA_V03.json", "FINAL_REPORT_V03.md", "MANIFEST_V03.json",
        }]
        if unexpected:
            raise RuntimeError(f"v0.3 output directory contains unrelated files: {', '.join(sorted(unexpected))}")
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_jsonl(output_dir / "PILOT_OUTCOMES_V03.jsonl", pilot_outcomes, rebuild=True)
    _write_resolution_csv(output_dir / "PILOT_OUTCOMES_V03.csv", pilot_outcomes)
    _write_jsonl(output_dir / "STABILITY_OUTCOMES_V03.jsonl", stability_outcomes, rebuild=True)
    _write_resolution_csv(output_dir / "STABILITY_OUTCOMES_V03.csv", stability_outcomes)

    advance_after = sha256_file(advance_path).lower()
    y30_after = sha256_file(retrieval_db).lower()
    v02_after = _directory_hashes(v02_evidence_dir)
    v01_after = _directory_hashes(v01_evidence_dir)
    if advance_before != advance_after or y30_before != y30_after:
        raise RuntimeError("accepted YEE-31/YEE-30 canonical input changed during v0.3 offline replay")
    if v02_before != v02_after or v01_before != v01_after:
        raise RuntimeError("v0.1/v0.2 immutable source evidence changed during v0.3 offline replay")
    input_hashes_before_after["yee31_advance_review_set"]["after"] = advance_after
    input_hashes_before_after["yee30_retrieval_db"]["after"] = y30_after
    for name, digest in source_file_hashes.items():
        qa["evidence_integrity"]["source_v02_selected_file_hashes_before_after"][name]["after"] = digest
    qa["evidence_integrity"]["source_v02_file_inventory_after"] = v02_after
    qa["evidence_integrity"]["source_v01_file_inventory_after"] = v01_after
    qa["evidence_integrity"]["source_file_inventories_unchanged"] = True

    _write_rebuilt_json(output_dir / "RESOLUTION_QA_V03.json", qa)
    report_lines = [
        "# YEE-37 Hybrid Resolution Policy v0.3 — Offline Replay",
        "",
        f"Status: `{qa['status']}`",
        f"Replay gates: **{status}**",
        f"Code commit: `{code_commit}`",
        f"Source v0.2 run: `{run_metadata['run_id']}`",
        f"Resolution policy: `{RESOLUTION_POLICY_VERSION}`",
        "",
        "No Jev/provider/network calls were made. No v0.2 inference outcomes, typed answers, SQLite, raw request/response evidence, or v0.1 manual labels were modified.",
        "",
        "## Resolution policy",
        "",
        "1. Explicit trailing version qualifier → `KEEP_SEPARATE` / `VERSION_QUALIFIER`.",
        "2. Existing exact `separator_insensitive_alphanumeric_equality` blocker → `MERGE` / `EXACT_FORM_ALIAS`.",
        "3. Otherwise preserve v0.2 Jev policy → `JEV_V02`.",
        "",
        "Jev v0.2 policy decisions and typed answers remain separate, unchanged evidence beside `resolution_decision` and `resolution_source`.",
        "",
        "## Pilot and stability replay",
        "",
        f"- Pilot outcomes reconciled: {qa['pilot']['replayed_outcome_count']}/{PILOT_SIZE}.",
        f"- Pilot Jev v0.2 decisions: `{canonical_json(qa['pilot']['jev_policy_decision_v02_counts'])}`.",
        f"- Pilot v0.3 resolution decisions: `{canonical_json(qa['pilot']['resolution_decision_counts'])}`.",
        f"- Hard sentinels: MERGE {sum(row['pass'] for row in pilot_merge_sentinels)}/{len(pilot_merge_sentinels)}; NOT_MERGE {sum(row['pass'] for row in pilot_nonmerge_sentinels)}/{len(pilot_nonmerge_sentinels)}.",
        f"- Stability exact resolution agreement: {stable_count}/{STABILITY_SIZE}.",
        f"- Stability resolution transitions: `{canonical_json(qa['stability']['resolution_transition_patterns'])}`.",
        f"- Native Jev v0.2 exact agreement (diagnostic): {native_stable_count}/{STABILITY_SIZE}.",
        f"- Hard-sentinel MERGE/non-MERGE flips: {len(sentinel_merge_flips)}.",
        "- `announce / announcer`: diagnostic-only per supervisor addendum; retained historical v0.2 expectation/result is not a hard-gate failure.",
        f"- Pilot-vs-stability hard-sentinel class differences (diagnostic): `{canonical_json(cross_phase_sentinel_variances)}`.",
        "",
        "## Evidence and audit",
        "",
        f"- Raw request/response archive: {raw_evidence_count} records; source SHA-256 `{v02_hashes['RAW_REQUEST_RESPONSE_EVIDENCE.jsonl.gz']['sha256']}`.",
        f"- v0.2 SQLite SHA-256 unchanged: `{v02_hashes['YEE37_PAIR_PILOT.sqlite']['sha256']}`.",
        f"- Preserved v0.2 execution metrics: `{canonical_json(qa['source_v02_execution_metrics']['all'])}`.",
        f"- v0.2 normalized/model evidence SHA-256 before/after: `{source_outcome_evidence_sha}` / `{derived_outcome_evidence_sha}`.",
        f"- Historical immutable v0.1 audit agreement (diagnostic only): Jev v0.2 policy {v02_policy_agreements}/40 → v0.3 resolution {policy_agreements}/40; relation {relation_agreements}/40.",
        f"- Accepted YEE-31 SHA-256 before/after: `{advance_before}` / `{advance_after}`.",
        f"- Accepted YEE-30 SHA-256 before/after: `{y30_before}` / `{y30_after}`.",
        "",
        "Source v0.2 evidence folder: https://drive.google.com/drive/folders/1sTMBOSMGu_jFV-wUTOEOQmIbmok5qXbS",
        "Source v0.1 manual-label folder: https://drive.google.com/drive/folders/15ocxVaXqXdNLLy-CXjqyBcjLmMFXV-wp",
        "",
        "No live inference, full 1,190-pair adjudication, or family build was run. PR #4 remains open/draft/unmerged pending supervisor review.",
        "",
    ]
    _write_rebuilt(output_dir / "FINAL_REPORT_V03.md", "\n".join(report_lines).encode("utf-8"))
    artifact_files = []
    for path in sorted(output_dir.iterdir(), key=lambda item: item.name):
        if path.is_file() and path.name != "MANIFEST_V03.json":
            artifact_files.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path)})
    manifest = {
        "work_order": "YEE-37",
        "status": qa["status"],
        "resolution_gate_status": status,
        "code_commit": code_commit,
        "resolution_policy_version": RESOLUTION_POLICY_VERSION,
        "source_v02_run_id": run_metadata["run_id"],
        "inference_contract_versions_unchanged": qa["inference_contract_versions_unchanged"],
        "accepted_input_hashes_before_after": input_hashes_before_after,
        "candidate_universe_and_selection_sha256": {
            name: v02_hashes[name]["sha256"]
            for name in ("CANDIDATE_PAIRS.jsonl", "PILOT_SELECTION.jsonl", "STABILITY_SELECTION.jsonl")
        },
        "source_v02_evidence_inventory": v02_after,
        "source_v01_manual_audit_inventory": v01_after,
        "source_v02_drive_folder": "https://drive.google.com/drive/folders/1sTMBOSMGu_jFV-wUTOEOQmIbmok5qXbS",
        "source_v01_drive_folder": "https://drive.google.com/drive/folders/15ocxVaXqXdNLLy-CXjqyBcjLmMFXV-wp",
        "artifact_files": artifact_files,
        "jev_or_network_requests": 0,
        "full_adjudication_or_family_build": False,
    }
    _write_rebuilt_json(output_dir / "MANIFEST_V03.json", manifest)
    return {
        "status": qa["status"],
        "resolution_gate_status": status,
        "pilot_replayed": len(pilot_outcomes),
        "stability_replicates_replayed": len(stability_outcomes),
        "resolution_stability": f"{stable_count}/{STABILITY_SIZE}",
        "network_requests": 0,
        "output_dir": str(output_dir),
    }


def _pair_state_size_rows(
    pairs: list[Mapping[str, Any]],
    topics_by_key: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    oversized = []
    for pair in pairs:
        state = build_pair_state(pair, topics_by_key)
        state_bytes = canonical_json(state).encode("utf-8")
        members = {side: state[side] for side in ("left", "right")}
        if any(
            len(member["examples"]) > PAIR_MAX_EXAMPLES_PER_TOPIC
            for member in members.values()
        ):
            raise RuntimeError(f"pair_state has more than 8 examples/topic: {pair['pair_id']}")
        row = {
            "pair_id": pair["pair_id"],
            "left_topic_key": pair["left_topic_key"],
            "right_topic_key": pair["right_topic_key"],
            "state_bytes": len(state_bytes),
            "state_sha256": sha256_bytes(state_bytes),
            "left_example_count": len(members["left"]["examples"]),
            "right_example_count": len(members["right"]["examples"]),
            "left_source_example_counts": dict(sorted(Counter(
                example["source"] for example in members["left"]["examples"]
            ).items())),
            "right_source_example_counts": dict(sorted(Counter(
                example["source"] for example in members["right"]["examples"]
            ).items())),
        }
        rows.append(row)
        if len(state_bytes) > PAIR_STATE_MAX_BYTES:
            oversized.append((pair["pair_id"], len(state_bytes)))
    if oversized:
        raise RuntimeError(
            "pair_state v0.2 preflight exceeded 16 KiB; no inference is allowed: "
            + canonical_json(oversized)
        )
    return rows


def _offline_v02_preflight(
    output_dir: Path,
    v01_evidence_dir: Path,
    advance_path: Path,
    retrieval_db: Path,
    input_hashes: Mapping[str, str],
    question_sha: str,
    code_commit: str,
    pairs: list[Mapping[str, Any]],
    pair_bytes: bytes,
    pilot_pairs: list[Mapping[str, Any]],
    stability_pairs: list[Mapping[str, Any]],
    topics_by_key: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    prior_files_before = _directory_hashes(v01_evidence_dir)
    prior_file_hashes = {item["path"]: item["sha256"] for item in prior_files_before}
    expected = {
        "CANDIDATE_PAIRS.jsonl": pair_bytes,
        "PILOT_SELECTION.jsonl": _jsonl_bytes(pilot_pairs),
        "STABILITY_SELECTION.jsonl": _jsonl_bytes(stability_pairs),
    }
    for name, current in expected.items():
        path = v01_evidence_dir / name
        if not path.is_file() or path.read_bytes() != current:
            raise RuntimeError(f"v0.2 changed the accepted v0.1 universe/selection artifact: {name}")

    prior_pilot = _read_jsonl(v01_evidence_dir / "PILOT_OUTCOMES.jsonl")
    oversized_v01 = [
        row for row in prior_pilot
        if row.get("status") == "failed" and "max_tokens_exceeded" in str(row.get("last_error") or "")
    ]
    if len(oversized_v01) != 4:
        raise RuntimeError(f"expected the four accepted v0.1 max_tokens pairs, found {len(oversized_v01)}")

    state_rows = _pair_state_size_rows(pairs, topics_by_key)
    state_by_pair = {row["pair_id"]: row for row in state_rows}
    missing_old = sorted(row["pair_id"] for row in oversized_v01 if row["pair_id"] not in state_by_pair)
    if missing_old:
        raise RuntimeError(f"historically oversized pilot pair is missing from the v0.2 universe: {missing_old}")
    old_pair_sizes = {
        row["pair_id"]: state_by_pair[row["pair_id"]]["state_bytes"]
        for row in oversized_v01
    }

    advance_after = sha256_file(advance_path)
    y30_after = sha256_file(retrieval_db)
    if (
        advance_after.lower() != YEE31_ACCEPTED_INPUT_SHA256
        or y30_after.lower() != YEE30_ACCEPTED_DB_SHA256
    ):
        raise RuntimeError("accepted YEE-31/YEE-30 canonical input changed during offline v0.2 preflight")
    prior_files_after = _directory_hashes(v01_evidence_dir)
    if prior_files_before != prior_files_after:
        raise RuntimeError("accepted v0.1 pilot/stability evidence changed during v0.2 preflight")

    pilot_identity_sha = sha256_bytes(canonical_json([row["pair_id"] for row in pilot_pairs]).encode())
    stability_identity_sha = sha256_bytes(canonical_json([row["pair_id"] for row in stability_pairs]).encode())
    qa = {
        "work_order": "YEE-37",
        "status": "PAIR_GATE_V02_READY_FOR_SUPERVISOR_REVIEW",
        "execution_mode": "offline_only",
        "network_or_jev_requests": 0,
        "code_commit": code_commit,
        "input_hashes_before_after": {
            "yee31_advance_review_set": {"before": input_hashes["yee31_advance_review_set_sha256"], "after": advance_after},
            "yee30_retrieval_db": {"before": input_hashes["yee30_retrieval_db_sha256_before"], "after": y30_after},
        },
        "pair_generation_version": PAIR_GENERATION_VERSION,
        "pair_state_version": PAIR_STATE_VERSION,
        "question_set_version": PAIR_QUESTION_SET_VERSION,
        "question_set_sha256": question_sha,
        "policy_version": PAIR_POLICY_VERSION,
        "output_version": PAIR_OUTPUT_VERSION,
        "candidate_pair_count": len(pairs),
        "candidate_pair_unique_id_count": len({row["pair_id"] for row in pairs}),
        "candidate_pairs_sha256": sha256_bytes(pair_bytes),
        "candidate_pair_artifact_byte_identical_to_v01": True,
        "pilot_count": len(pilot_pairs),
        "pilot_selection_sha256": sha256_bytes(expected["PILOT_SELECTION.jsonl"]),
        "pilot_selection_identity_sha256": pilot_identity_sha,
        "pilot_selection_artifact_byte_identical_to_v01": True,
        "stability_pair_count": len(stability_pairs),
        "stability_repetitions": STABILITY_REPETITIONS,
        "stability_selection_sha256": sha256_bytes(expected["STABILITY_SELECTION.jsonl"]),
        "stability_selection_identity_sha256": stability_identity_sha,
        "stability_selection_artifact_byte_identical_to_v01": True,
        "pair_state_max_bytes": PAIR_STATE_MAX_BYTES,
        "pair_states_checked": len(state_rows),
        "pair_states_within_byte_bound": len(state_rows),
        "max_pair_state_bytes": max(row["state_bytes"] for row in state_rows),
        "mean_pair_state_bytes": round(sum(row["state_bytes"] for row in state_rows) / len(state_rows), 2),
        "max_examples_per_topic": PAIR_MAX_EXAMPLES_PER_TOPIC,
        "observed_max_examples_per_topic": max(
            max(row["left_example_count"], row["right_example_count"]) for row in state_rows
        ),
        "historically_oversized_v01_pairs": old_pair_sizes,
        "v01_evidence_unchanged": True,
        "v01_evidence_files": prior_files_before,
    }
    _write_jsonl(output_dir / "PAIR_STATE_SIZE_QA_V02.jsonl", state_rows)
    _write_json(output_dir / "OFFLINE_V02_QA.json", qa)
    report = "\n".join([
        "# YEE-37 Pair Gate v0.2 — Offline Review",
        "",
        "Status: `PAIR_GATE_V02_READY_FOR_SUPERVISOR_REVIEW`",
        "",
        "No Jev/provider/network calls were made. The v0.1 pilot/stability database and artifacts were read-only and their complete directory hash inventory matched before and after.",
        "",
        f"- Pair universe: {len(pairs)} pairs; byte-identical to accepted v0.1 `CANDIDATE_PAIRS.jsonl` (SHA-256 `{qa['candidate_pairs_sha256']}`).",
        f"- Pilot/stability selections: {len(pilot_pairs)} and {len(stability_pairs)} identities; both files byte-identical to accepted v0.1 selections.",
        f"- State bound: {len(state_rows)}/{len(pairs)} pair states are <= {PAIR_STATE_MAX_BYTES} bytes; max {qa['max_pair_state_bytes']} bytes; max examples/topic {qa['observed_max_examples_per_topic']}.",
        f"- Previously oversized pairs: {canonical_json(old_pair_sizes)} bytes after compaction.",
        f"- Contract versions: state `{PAIR_STATE_VERSION}`, questions `{PAIR_QUESTION_SET_VERSION}`, policy `{PAIR_POLICY_VERSION}`, output `{PAIR_OUTPUT_VERSION}`.",
        f"- YEE-31 input SHA-256 before/after: `{advance_after}`.",
        f"- YEE-30 read-only DB SHA-256 before/after: `{y30_after}`.",
        "",
        "The 120-pair pilot, 40 × 3 stability audit, full pair adjudication and family build were not run. v0.1 evidence remains immutable.",
        "",
        f"Code commit: `{code_commit}`. Pair-state size evidence is `PAIR_STATE_SIZE_QA_V02.jsonl`; QA and hashes are in `OFFLINE_V02_QA.json`.",
        "",
        "PR #4 remains draft/open/unmerged pending supervisor review.",
        "",
    ])
    _write_generated(output_dir / "OFFLINE_V02_REPORT.md", report.encode("utf-8"))
    manifest = {
        "work_order": "YEE-37",
        "status": qa["status"],
        "code_commit": code_commit,
        "input_hashes": qa["input_hashes_before_after"],
        "pair_generation_version": PAIR_GENERATION_VERSION,
        "pair_state_version": PAIR_STATE_VERSION,
        "question_set_version": PAIR_QUESTION_SET_VERSION,
        "question_set_sha256": question_sha,
        "policy_version": PAIR_POLICY_VERSION,
        "output_version": PAIR_OUTPUT_VERSION,
        "v01_evidence_unchanged": True,
        "files": [],
    }
    for path in sorted(output_dir.iterdir(), key=lambda item: item.name):
        if path.is_file() and path.name != "OFFLINE_V02_MANIFEST.json":
            manifest["files"].append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path)})
    _write_json(output_dir / "OFFLINE_V02_MANIFEST.json", manifest)
    return {
        "status": qa["status"],
        "network_or_jev_requests": 0,
        "candidate_pair_count": qa["candidate_pair_count"],
        "max_pair_state_bytes": qa["max_pair_state_bytes"],
        "output_dir": str(output_dir),
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    advance_path = Path(args.advance_set).resolve()
    retrieval_db = Path(args.input_db).resolve()
    output_dir = Path(args.output_dir).resolve()
    code_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=Path(__file__).resolve().parents[2],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    if args.offline_v03_replay:
        if not args.v02_evidence_dir or not args.v01_evidence_dir:
            raise RuntimeError("offline v0.3 replay requires --v02-evidence-dir and --v01-evidence-dir")
        return _offline_v03_replay(
            output_dir,
            Path(args.v02_evidence_dir).resolve(),
            Path(args.v01_evidence_dir).resolve(),
            advance_path,
            retrieval_db,
            code_commit,
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    questions, question_sha = load_pair_questions()
    y30_metadata, topics, input_hashes = load_accepted_advance_topics(advance_path, retrieval_db)
    topic_by_key = {str(topic["topic_key"]): topic for topic in topics}
    pairs = generate_candidate_pairs(topics, input_hashes)
    pair_bytes = _jsonl_bytes(pairs)
    _write_generated(output_dir / "CANDIDATE_PAIRS.jsonl", pair_bytes)
    if _jsonl_bytes(generate_candidate_pairs(topics, input_hashes)) != pair_bytes:
        raise RuntimeError("candidate-pair rebuild was not byte-identical")

    available_sentinels = {pair["pair_id"] for pair in pairs if pair.get("sentinel_expectations")}
    if len(available_sentinels) != 12:
        raise RuntimeError(
            f"expected all 12 corpus sentinel pairs in the 1,807-topic input, found {len(available_sentinels)}"
        )
    pilot_pairs = select_stratified_pairs(pairs, PILOT_SIZE)
    stability_pairs = select_stratified_pairs(pilot_pairs, STABILITY_SIZE)
    _write_generated(output_dir / "PILOT_SELECTION.jsonl", _jsonl_bytes(pilot_pairs))
    _write_generated(output_dir / "STABILITY_SELECTION.jsonl", _jsonl_bytes(stability_pairs))
    _write_generated(
        output_dir / "JEV_PAIR_QUESTIONS.json",
        (canonical_json({
            "version": PAIR_QUESTION_SET_VERSION,
            "questions": questions,
        }) + "\n").encode("utf-8"),
    )
    if args.offline_v02_preflight:
        if not args.v01_evidence_dir:
            raise RuntimeError("offline v0.2 preflight requires --v01-evidence-dir")
        return _offline_v02_preflight(
            output_dir,
            Path(args.v01_evidence_dir).resolve(),
            advance_path,
            retrieval_db,
            input_hashes,
            question_sha,
            code_commit,
            pairs,
            pair_bytes,
            pilot_pairs,
            stability_pairs,
            topic_by_key,
        )
    state_size_rows = _pair_state_size_rows(pairs, topic_by_key)
    _write_jsonl(output_dir / "PAIR_STATE_SIZE_QA_V02.jsonl", state_size_rows)
    if args.preflight_only:
        universe_preflight = {
            "work_order": "YEE-37",
            "code_commit": code_commit,
            "input_hashes": input_hashes,
            "accepted_advance_topic_count": len(topics),
            "accepted_advance_unique_topic_count": len(topic_by_key),
            "candidate_pair_count": len(pairs),
            "candidate_pairs_sha256": sha256_bytes(pair_bytes),
            "candidate_pair_rebuild_byte_identical": True,
            "pilot_count": len(pilot_pairs),
            "stability_pair_count": len(stability_pairs),
            "stability_repetitions": STABILITY_REPETITIONS,
            "sentinel_pair_count": len(available_sentinels),
            "candidate_universe_qa": _universe_qa(pairs),
            "question_set_version": PAIR_QUESTION_SET_VERSION,
            "question_set_sha256": question_sha,
            "pair_state_version": PAIR_STATE_VERSION,
            "pair_state_count": len(state_size_rows),
            "pair_state_max_bytes": max(row["state_bytes"] for row in state_size_rows),
            "network_requests": 0,
            "yee30_read_only": True,
        }
        _write_json(output_dir / "UNIVERSE_PREFLIGHT.json", universe_preflight)
        return {
            "status": "UNIVERSE_READY_NO_NETWORK",
            "candidate_pair_count": len(pairs),
            "candidate_pairs_sha256": sha256_bytes(pair_bytes),
            "sentinel_pair_count": len(available_sentinels),
            "pilot_count": len(pilot_pairs),
            "stability_pair_count": len(stability_pairs),
            "output_dir": str(output_dir),
        }

    transport_kind = os.environ.get("JEV_TRANSPORT", "")
    credential_present = bool(os.environ.get("TYPESAFE_API_KEY"))
    if transport_kind != "typesafe":
        raise RuntimeError("preflight requires JEV_TRANSPORT=typesafe")
    if not credential_present:
        raise RuntimeError("preflight requires TYPESAFE_API_KEY present")
    custom_base_url = os.environ.get("TYPESAFE_BASE_URL", DIRECT_BASE_URL).rstrip("/")
    if custom_base_url != DIRECT_BASE_URL:
        raise RuntimeError("preflight endpoint differs from the approved direct TypeSafe API")

    run_basis = {
        "work_order": "YEE-37",
        "yee31_run_id": YEE31_RUN_ID,
        "input_hashes": input_hashes,
        "question_set_sha256": question_sha,
        "code_commit": code_commit,
        "requested_model_alias": EXPECTED_MODEL_ALIAS,
        "expected_model_version": EXPECTED_MODEL_VERSION,
        "transport_id": "typesafe-systemone-http-v1",
        "endpoint": DIRECT_BASE_URL + "/v1/systemone",
        "inference_parameters": {},
        "pair_generation_version": PAIR_GENERATION_VERSION,
        "pair_normalization_version": PAIR_NORMALIZATION_VERSION,
        "pair_state_version": PAIR_STATE_VERSION,
        "pair_question_set_version": PAIR_QUESTION_SET_VERSION,
        "pair_policy_version": PAIR_POLICY_VERSION,
        "pair_output_version": PAIR_OUTPUT_VERSION,
        "candidate_pair_count": len(pairs),
        "pilot_count": len(pilot_pairs),
        "stability_count": len(stability_pairs),
        "stability_repetitions": STABILITY_REPETITIONS,
        "retry_max_attempts": args.max_attempts,
    }
    run_id = sha256_bytes(canonical_json(run_basis).encode("utf-8"))
    run_metadata = {
        **run_basis,
        "run_id": run_id,
        "y30_build_metadata": {
            key: y30_metadata[key]
            for key in ("work_order", "topic_schema_version", "normalization_version", "analysis_as_of")
            if key in y30_metadata
        },
        "question_set_version": PAIR_QUESTION_SET_VERSION,
        "pair_state_version": PAIR_STATE_VERSION,
        "pair_generation_version": PAIR_GENERATION_VERSION,
        "pair_normalization_version": PAIR_NORMALIZATION_VERSION,
        "pair_policy_version": PAIR_POLICY_VERSION,
        "pair_output_version": PAIR_OUTPUT_VERSION,
    }
    if args.finalize_only:
        run_metadata = _reuse_persisted_run_metadata(
            output_dir / "RUN_METADATA.json",
            run_basis,
        )
        run_id = str(run_metadata["run_id"])
    _write_json(output_dir / "RUN_METADATA.json", run_metadata)
    provider = jev_provider_from_env()
    if (
        provider.model_identifier != EXPECTED_MODEL_ALIAS
        or provider.model_version != EXPECTED_MODEL_VERSION
        or provider.transport_id != "typesafe-systemone-http-v1"
        or provider.transport_config.get("base_url") != DIRECT_BASE_URL
    ):
        provider.close()
        raise RuntimeError("Jev provider identity/endpoint failed closed before any live request")

    metadata = {
        **run_metadata,
        "transport_config": dict(provider.transport_config),
        "credential_present": credential_present,
        "fuzzy_neighbor_limit": 8,
    }
    _write_json(output_dir / "RUN_PREFLIGHT.json", {
        **metadata,
        "advance_topic_count": len(topics),
        "advance_unique_topic_count": len(topic_by_key),
        "advance_input_sha256": input_hashes["yee31_advance_review_set_sha256"],
        "yee30_read_only": True,
        "candidate_pair_count": len(pairs),
        "candidate_pairs_sha256": sha256_bytes(pair_bytes),
        "candidate_pair_rebuild_byte_identical": True,
        "pilot_count": len(pilot_pairs),
        "stability_count": len(stability_pairs),
        "stability_repetitions": STABILITY_REPETITIONS,
        "all_12_sentinels_in_universe": True,
        "network_requests_before_preflight": 0,
    })

    database = output_dir / "YEE37_PAIR_PILOT.sqlite"
    manual_results_path = output_dir / "MANUAL_AUDIT_RESULTS.jsonl"
    try:
        with PairAdjudicationRunner(
            database,
            provider,
            questions,
            question_sha,
            metadata,
            max_attempts=args.max_attempts,
        ) as runner:
            runner.register_candidate_pairs(pairs)
            if args.finalize_only:
                pilot_results, stability_results = _load_cached_results(
                    runner, pilot_pairs, stability_pairs
                )
                _assert_terminal_outcomes(pilot_results, "pilot")
                _assert_terminal_outcomes(stability_results, "stability")
            else:
                pilot_results = runner.run_pairs(pilot_pairs, topic_by_key)
                pilot_outcomes = _pair_outcomes(pilot_pairs, pilot_results)
                worklist = deterministic_manual_audit_sample(pilot_pairs, pilot_outcomes, limit=40)
                _write_jsonl(
                    output_dir / "MANUAL_AUDIT_WORKLIST.jsonl",
                    _manual_audit_packet(worklist, pilot_pairs, topic_by_key, pilot_outcomes),
                    rebuild=True,
                )
                stability_results = []
                for pair in stability_pairs:
                    state = build_pair_state(pair, topic_by_key)
                    for repetition in range(1, STABILITY_REPETITIONS + 1):
                        stability_results.append(
                            runner.run_pair(pair, state, replicate_id=f"stability-{repetition}")
                        )
                pilot_results, stability_results = _load_cached_results(
                    runner, pilot_pairs, stability_pairs
                )
            all_attempts = runner.raw_attempts()
            pilot_outcomes = _pair_outcomes(pilot_pairs, pilot_results)
            stability_outcomes = _pair_outcomes(stability_pairs, stability_results)
            pilot_ids = {str(pair["pair_id"]) for pair in pilot_pairs}
            stability_ids = {str(pair["pair_id"]) for pair in stability_pairs}
            pilot_attempts = [
                row for row in all_attempts
                if row["pair_id"] in pilot_ids and row["replicate_id"] is None
            ]
            stability_attempts = [
                row for row in all_attempts
                if row["pair_id"] in stability_ids and row["replicate_id"] is not None
            ]
            pilot_metrics = _attempt_metrics(pilot_outcomes, pilot_attempts)
            stability_metrics = _attempt_metrics(stability_outcomes, stability_attempts)
            attempt_metrics = _attempt_metrics(
                pilot_outcomes + stability_outcomes, all_attempts
            )
            manual_worklist = deterministic_manual_audit_sample(pilot_pairs, pilot_outcomes, limit=40)
            manual_audit = _combine_manual_audit(manual_worklist, manual_results_path)
            pilot_report = build_pilot_report(pilot_pairs, pilot_outcomes, manual_audit)
            stability_report = build_stability_report(stability_pairs, stability_outcomes)
            status = "PAIR_PILOT_READY_FOR_SUPERVISOR_REVIEW"
            acceptance_gate_status = _acceptance_gate_status(pilot_report, stability_report)
            report_metadata = {
                **metadata,
                "artifact_finalization_code_commit": code_commit,
                "acceptance_gate_status": acceptance_gate_status,
                "status": status,
            }
            _write_jsonl(output_dir / "PILOT_OUTCOMES.jsonl", pilot_outcomes, rebuild=True)
            _write_outcome_csv(output_dir / "PILOT_OUTCOMES.csv", pilot_outcomes)
            _write_jsonl(output_dir / "STABILITY_OUTCOMES.jsonl", stability_outcomes, rebuild=True)
            _write_rebuilt_json(output_dir / "PILOT_QA.json", pilot_report)
            _write_rebuilt_json(output_dir / "STABILITY_QA.json", stability_report)
            _write_raw_evidence(output_dir / "RAW_REQUEST_RESPONSE_EVIDENCE.jsonl.gz", all_attempts)
            _write_report(
                output_dir / "FINAL_REPORT.md",
                report_metadata,
                _universe_qa(pairs),
                pilot_report,
                stability_report,
                {"pilot": pilot_metrics, "stability": stability_metrics, "all": attempt_metrics},
            )
            input_after = sha256_file(advance_path)
            y30_after = sha256_file(retrieval_db)
            if input_after.lower() != YEE31_ACCEPTED_INPUT_SHA256 or y30_after.lower() != YEE30_ACCEPTED_DB_SHA256:
                raise RuntimeError("accepted YEE-31/YEE-30 input changed during YEE-37 execution")
            qa_summary = {
                "status": status,
                "acceptance_gate_status": acceptance_gate_status,
                "input_hashes_before_after": {
                    "yee31_advance_review_set": {
                        "before": input_hashes["yee31_advance_review_set_sha256"],
                        "after": input_after,
                    },
                    "yee30_retrieval_db": {
                        "before": input_hashes["yee30_retrieval_db_sha256_before"],
                        "after": y30_after.lower(),
                    },
                },
                "candidate_universe": _universe_qa(pairs),
                "pilot": pilot_report,
                "stability": stability_report,
                "execution_metrics": {
                    "pilot": pilot_metrics,
                    "stability": stability_metrics,
                    "all": attempt_metrics,
                },
            }
            _write_rebuilt_json(output_dir / "PAIR_PILOT_QA.json", qa_summary)
        _write_rebuilt_json(
            output_dir / "MANIFEST.json",
            _artifact_manifest(output_dir, report_metadata),
        )
    finally:
        provider.close()
    return {
        "status": status,
        "acceptance_gate_status": acceptance_gate_status,
        "run_id": run_id,
        "candidate_pair_count": len(pairs),
        "pilot_status": pilot_report["status"],
        "pilot_valid": pilot_report["valid_typed_responses"],
        "stability_status": stability_report["status"],
        "stability_policy_agreement": stability_report["authoritative_policy_exact_agreement_rate"],
        "manual_audit_count": pilot_report["manual_audit_count"],
        "output_dir": str(output_dir),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--advance-set", required=True)
    parser.add_argument("--input-db", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--max-attempts", type=int, default=3)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--finalize-only",
        action="store_true",
        help="Regenerate QA/exports from persisted pilot evidence without model/network calls.",
    )
    mode.add_argument(
        "--preflight-only",
        action="store_true",
        help="Build and byte-check the complete candidate-pair universe without Jev/network calls.",
    )
    mode.add_argument(
        "--offline-v02-preflight",
        action="store_true",
        help="Verify v0.2 state sizes and exact v0.1 pair/selection identities without network calls.",
    )
    mode.add_argument(
        "--offline-v03-replay",
        action="store_true",
        help="Replay saved v0.2 pilot/stability outcomes through resolution policy v0.3 with zero network calls.",
    )
    parser.add_argument(
        "--v02-evidence-dir",
        help="Accepted immutable v0.2 evidence directory, required for --offline-v03-replay.",
    )
    parser.add_argument(
        "--v01-evidence-dir",
        help="Accepted immutable v0.1 evidence directory, required for v0.2 preflight or v0.3 audit comparison.",
    )
    args = parser.parse_args()
    print(json.dumps(run(args), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
