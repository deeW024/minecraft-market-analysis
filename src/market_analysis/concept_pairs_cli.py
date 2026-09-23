"""CLI for the YEE-37 candidate-pair pilot and its fixed stability audit."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import subprocess
from collections import Counter
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
    PILOT_SIZE,
    STABILITY_REPETITIONS,
    STABILITY_SIZE,
    YEE30_ACCEPTED_DB_SHA256,
    YEE31_ACCEPTED_INPUT_SHA256,
    YEE31_RUN_ID,
    PairAdjudicationRunner,
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
        "status", "attempt_count", "concept_relation", "merge_safe",
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
                "concept_relation", "merge_safe", "lexical_alias", "evidence_alignment",
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
        "",
        f"Run ID: {metadata['run_id']}",
        f"Code commit: {metadata['code_commit']}",
        f"Artifact recalculation code commit: {metadata.get('artifact_finalization_code_commit', metadata['code_commit'])}",
        f"YEE-31 ADVANCE input SHA-256: {metadata['input_hashes']['yee31_advance_review_set_sha256']}",
        f"YEE-30 read-only DB SHA-256: {metadata['input_hashes']['yee30_retrieval_db_sha256_before']}",
        f"Question set {metadata['question_set_version']} / {metadata['question_set_sha256']}",
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


def run(args: argparse.Namespace) -> dict[str, Any]:
    advance_path = Path(args.advance_set).resolve()
    retrieval_db = Path(args.input_db).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    questions, question_sha = load_pair_questions()
    code_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=Path(__file__).resolve().parents[2],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
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
            status = (
                "PAIR_PILOT_READY_FOR_SUPERVISOR_REVIEW"
                if pilot_report["status"] == "PASS"
                and all(stability_report["gates"].values())
                else "PILOT_GATES_FAILED_REVIEW_REQUIRED"
            )
            report_metadata = {
                **metadata,
                "artifact_finalization_code_commit": code_commit,
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
    args = parser.parse_args()
    print(json.dumps(run(args), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
