"""YEE-37 offline production preflight and explicitly authorized full runner."""

from __future__ import annotations

import argparse
import base64
import json
import re
import sqlite3
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Mapping

from .concept_pairs import (
    EXPECTED_MODEL_ALIAS,
    EXPECTED_MODEL_VERSION,
    PAIR_GENERATION_VERSION,
    PAIR_OUTPUT_VERSION,
    PAIR_POLICY_VERSION,
    PAIR_QUESTION_SET_VERSION,
    PAIR_STATE_VERSION,
    RESOLUTION_POLICY_VERSION,
    STABILITY_REPETITIONS,
    STABILITY_SIZE,
    PILOT_SIZE,
    PairAdjudicationRunner,
    apply_resolution_policy_v03,
    build_pair_state,
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
from .production_pipeline import (
    COHERENCE_EDGE_VERSION,
    FAMILY_BUILD_VERSION,
    FAMILY_ID_VERSION,
    PRODUCTION_PIPELINE_VERSION,
    ProductionPreflightError,
    artifact_manifest,
    build_concept_retrieval_db,
    build_family_artifacts,
    create_preflight_state,
    csv_bytes,
    deterministic_raw_archive,
    jsonl_bytes,
    plan_representative_coherence_edges,
)

ACCEPTED_PREFLIGHT_CODE_COMMIT = "acc70a0f544fa4214c0387ff69c5f47cfdc0441a"


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ProductionPreflightError(f"expected JSON object: {path.name}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    result = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for number, line in enumerate(handle, start=1):
            if not line.strip():
                raise ProductionPreflightError(f"blank JSONL line {number} in {path.name}")
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ProductionPreflightError(f"non-object JSONL row {number} in {path.name}")
            result.append(row)
    return result


def _inventory(directory: Path) -> list[dict[str, Any]]:
    return [
        {"path": path.relative_to(directory).as_posix(), "bytes": path.stat().st_size, "sha256": sha256_file(path)}
        for path in sorted(directory.rglob("*"), key=lambda item: item.as_posix())
        if path.is_file()
    ]


def _assert_source_manifest(directory: Path, manifest: Mapping[str, Any]) -> None:
    file_entries = manifest.get("artifact_files")
    if not isinstance(file_entries, list):
        raise ProductionPreflightError("accepted v0.2 manifest lacks artifact_files")
    for item in file_entries:
        path = directory / str(item["path"])
        if not path.is_file() or path.stat().st_size != int(item["bytes"]) or sha256_file(path) != item["sha256"]:
            raise ProductionPreflightError(f"accepted v0.2 artifact differs from its manifest: {item['path']}")


def _db_metadata(database: Path) -> dict[str, Any]:
    connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return {
            row["key"]: json.loads(row["value_json"])
            for row in connection.execute("SELECT key,value_json FROM run_metadata")
        }
    finally:
        connection.close()


def _current_git_head() -> str:
    repo_root = Path(__file__).resolve().parents[2]
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _resolve_execution_code_commit(authorized_execution_commit: str | None) -> str:
    if not isinstance(authorized_execution_commit, str) or not re.fullmatch(r"[0-9a-f]{40}", authorized_execution_commit):
        raise ProductionPreflightError("a full authorized execution commit SHA is required")
    current_head = _current_git_head()
    if current_head != authorized_execution_commit:
        raise ProductionPreflightError("current Git HEAD does not match the authorized execution commit")
    return current_head


def _record_execution_provenance(
    database: Path,
    run_id: str,
    preflight_code_commit: str,
    execution_code_commit: str,
) -> None:
    if preflight_code_commit != ACCEPTED_PREFLIGHT_CODE_COMMIT:
        raise ProductionPreflightError("accepted preflight code commit does not match the authorized baseline")
    if not re.fullmatch(r"[0-9a-f]{40}", execution_code_commit):
        raise ProductionPreflightError("execution code commit is not a full Git SHA")
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    try:
        production_row = connection.execute(
            "SELECT metadata_json FROM production_run_metadata WHERE run_id=?", (run_id,)
        ).fetchone()
        if production_row is None:
            raise ProductionPreflightError("accepted production metadata is missing for execution provenance")
        production_metadata = json.loads(production_row["metadata_json"])
        if production_metadata.get("code_commit") != preflight_code_commit:
            raise ProductionPreflightError("production run identity no longer matches its preflight code commit")
        cached_code_row = connection.execute(
            "SELECT value_json FROM run_metadata WHERE key='code_commit'"
        ).fetchone()
        if cached_code_row is None or json.loads(cached_code_row["value_json"]) != preflight_code_commit:
            raise ProductionPreflightError("pair cache metadata no longer matches its preflight code commit")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS production_execution_provenance ("
            "run_id TEXT NOT NULL,preflight_code_commit TEXT NOT NULL,execution_code_commit TEXT NOT NULL,"
            "PRIMARY KEY(run_id,execution_code_commit))"
        )
        existing = connection.execute(
            "SELECT preflight_code_commit FROM production_execution_provenance "
            "WHERE run_id=? AND execution_code_commit=?",
            (run_id, execution_code_commit),
        ).fetchone()
        if existing and existing["preflight_code_commit"] != preflight_code_commit:
            raise ProductionPreflightError("execution provenance conflicts with an existing append-only record")
        connection.execute(
            "INSERT OR IGNORE INTO production_execution_provenance "
            "(run_id,preflight_code_commit,execution_code_commit) VALUES (?,?,?)",
            (run_id, preflight_code_commit, execution_code_commit),
        )
        connection.commit()
    finally:
        connection.close()


def _resolution_map(database: Path) -> dict[str, dict[str, Any]]:
    connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.row_factory = sqlite3.Row
    try:
        return {
            row["pair_id"]: json.loads(row["derived_json"])
            for row in connection.execute("SELECT pair_id,derived_json FROM pair_resolutions")
        }
    finally:
        connection.close()


def _registry_snapshot(database: Path, table: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if table not in {"candidate_pairs", "coherence_edges"}:
        raise ValueError("unknown pair registry")
    connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        pairs = [json.loads(row[0]) for row in connection.execute(f"SELECT pair_json FROM {table} ORDER BY left_topic_key,right_topic_key,pair_id")]
        rows = connection.execute(
            f"SELECT p.pair_id,p.left_topic_key,p.right_topic_key,p.pair_json,r.status,r.normalized_json,r.last_error,r.cache_key,r.replicate_id "
            f"FROM {table} p LEFT JOIN pair_runs r ON r.pair_id=p.pair_id AND r.replicate_id IS NULL "
            "ORDER BY p.left_topic_key,p.right_topic_key,p.pair_id"
        ).fetchall()
        runs = []
        for row in rows:
            runs.append({
                "pair_id": row["pair_id"],
                "left_topic_key": row["left_topic_key"],
                "right_topic_key": row["right_topic_key"],
                "pair": json.loads(row["pair_json"]),
                "status": row["status"] or "pending",
                "normalized": json.loads(row["normalized_json"]) if row["normalized_json"] else None,
                "last_error": row["last_error"],
                "cache_key": row["cache_key"],
                "replicate_id": row["replicate_id"],
                "attempt_count": connection.execute(
                    "SELECT count(*) FROM pair_attempts WHERE cache_key=?", (row["cache_key"],)
                ).fetchone()[0] if row["cache_key"] else 0,
            })
        return pairs, runs
    finally:
        connection.close()


def _raw_attempt_export(database: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            "SELECT a.*,r.pair_id,r.replicate_id,p.left_topic_key,p.right_topic_key,"
            "CASE WHEN b.pair_id IS NULL THEN 'COHERENCE_EDGE' ELSE 'BASE_PAIR' END AS pair_kind "
            "FROM pair_attempts a JOIN pair_runs r USING(cache_key) "
            "JOIN pair_registry p USING(pair_id) LEFT JOIN candidate_pairs b USING(pair_id) "
            "ORDER BY pair_kind,p.left_topic_key,p.right_topic_key,COALESCE(r.replicate_id,''),a.attempt_no"
        ).fetchall()
        result = []
        for row in rows:
            result.append({
                "pair_id": row["pair_id"],
                "pair_kind": row["pair_kind"],
                "left_topic_key": row["left_topic_key"],
                "right_topic_key": row["right_topic_key"],
                "replicate_id": row["replicate_id"],
                "cache_key": row["cache_key"],
                "attempt_no": row["attempt_no"],
                "stage": row["stage"],
                "wire_request_captured": bool(row["wire_request_captured"]),
                "request_sha256": row["request_sha256"],
                "request_body_base64": base64.b64encode(row["request_body"]).decode("ascii") if row["request_body"] else None,
                "raw_response_sha256": row["raw_response_sha256"],
                "raw_response_base64": base64.b64encode(row["raw_response"]).decode("ascii") if row["raw_response"] else None,
                "request_id": row["request_id"],
                "error": row["error"],
            })
        return result
    finally:
        connection.close()


def _replay_outputs(
    topics: Mapping[str, Mapping[str, Any]],
    pairs: list[Mapping[str, Any]],
    base_runs: list[Mapping[str, Any]],
    base_resolutions: Mapping[str, Mapping[str, Any]],
    edges: list[Mapping[str, Any]],
    edge_runs: list[Mapping[str, Any]],
    edge_resolutions: Mapping[str, Mapping[str, Any]],
    provenance: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    first = build_family_artifacts(
        topics, pairs, base_runs, base_resolutions, edges, edge_runs, edge_resolutions, provenance
    )
    second = build_family_artifacts(
        topics, pairs, base_runs, base_resolutions, edges, edge_runs, edge_resolutions, provenance
    )
    output_names = (
        "concept_families", "concept_members", "pair_relations", "review_pairs",
        "broader_narrower_edges", "family_evidence_packs", "coherence_edges",
    )
    if any(jsonl_bytes(first[name]) != jsonl_bytes(second[name]) for name in output_names):
        raise ProductionPreflightError("deterministic family JSONL replay differs")
    if any(csv_bytes(first[name]) != csv_bytes(second[name]) for name in (
        "concept_families", "concept_members", "pair_relations", "review_pairs",
        "broader_narrower_edges", "coherence_edges",
    )):
        raise ProductionPreflightError("deterministic family CSV replay differs")
    first_for_db = {**first, "metadata": dict(metadata)}
    second_for_db = {**second, "metadata": dict(metadata)}
    with tempfile.TemporaryDirectory(prefix="yee37-replay-a-") as first_dir, tempfile.TemporaryDirectory(prefix="yee37-replay-b-") as second_dir:
        first_db = Path(first_dir) / "concept_retrieval.sqlite"
        second_db = Path(second_dir) / "concept_retrieval.sqlite"
        first_mode = build_concept_retrieval_db(first_db, first_for_db)
        second_mode = build_concept_retrieval_db(second_db, second_for_db)
        first_sha, second_sha = sha256_file(first_db), sha256_file(second_db)
    if first_sha != second_sha or first_mode != second_mode:
        raise ProductionPreflightError("concept_retrieval.sqlite replay was not byte-identical")
    return first, first_mode


def _write_file(directory: Path, name: str, payload: bytes) -> None:
    path = directory / name
    if path.exists():
        raise ProductionPreflightError(f"offline output must be fresh; file already exists: {name}")
    path.write_bytes(payload)


def _replace_file(directory: Path, name: str, payload: bytes) -> None:
    (directory / name).write_bytes(payload)


def _persist_resolutions(
    connection: sqlite3.Connection,
    pairs: list[Mapping[str, Any]],
    results: list[Mapping[str, Any]],
) -> None:
    pair_by_id = {str(pair["pair_id"]): pair for pair in pairs}
    for result in results:
        if result.get("status") != "completed" or not isinstance(result.get("normalized"), Mapping):
            continue
        pair_id = str(result["pair_id"])
        normalized = result["normalized"]
        resolution = apply_resolution_policy_v03(pair_by_id[pair_id], normalized)
        connection.execute(
            "INSERT INTO pair_resolutions(pair_id,resolution_policy_version,jev_policy_decision_v02,"
            "resolution_decision,resolution_source,derived_json,source_normalized_sha256) "
            "VALUES (?,?,?,?,?,?,?) ON CONFLICT(pair_id) DO UPDATE SET "
            "resolution_policy_version=excluded.resolution_policy_version,"
            "jev_policy_decision_v02=excluded.jev_policy_decision_v02,"
            "resolution_decision=excluded.resolution_decision,"
            "resolution_source=excluded.resolution_source,derived_json=excluded.derived_json,"
            "source_normalized_sha256=excluded.source_normalized_sha256",
            (
                pair_id,
                resolution["resolution_policy_version"],
                resolution["jev_policy_decision_v02"],
                resolution["resolution_decision"],
                resolution["resolution_source"],
                canonical_json(resolution),
                sha256_bytes(canonical_json(normalized).encode("utf-8")),
            ),
        )
    connection.commit()


def run_authorized_production(args: argparse.Namespace) -> dict[str, Any]:
    """Resume all base pairs, then separately adjudicate required coherence edges."""
    execution_code_commit = _resolve_execution_code_commit(args.authorized_execution_commit)
    advance_path = Path(args.advance_set).resolve()
    y30_db = Path(args.input_db).resolve()
    output_dir = Path(args.output_dir).resolve()
    database = output_dir / "production.sqlite"
    if not database.is_file():
        raise ProductionPreflightError("production SQLite is missing; create it with --offline-preflight first")
    accepted_qa = _read_json(output_dir / "PRODUCTION_PREFLIGHT_QA.json")
    if accepted_qa.get("status") != "PRODUCTION_PIPELINE_READY_FOR_SUPERVISOR_REVIEW":
        raise ProductionPreflightError("production state did not pass its offline supervisor preflight")
    before = {
        "yee31_advance_review_set_sha256": sha256_file(advance_path),
        "yee30_retrieval_db_sha256": sha256_file(y30_db),
    }
    _y30_metadata, topics, input_hashes = load_accepted_advance_topics(advance_path, y30_db)
    if before["yee31_advance_review_set_sha256"] != input_hashes["yee31_advance_review_set_sha256"]:
        raise ProductionPreflightError("YEE-31 accepted input hash mismatch before live dispatch")
    pairs = generate_candidate_pairs(topics, input_hashes)
    stored_pairs, _ = _registry_snapshot(database, "candidate_pairs")
    if jsonl_bytes(pairs) != jsonl_bytes(stored_pairs):
        raise ProductionPreflightError("production base pair universe differs from accepted input reconstruction")
    questions, question_sha = load_pair_questions()
    runner_metadata = _db_metadata(database)
    preflight_code_commit = runner_metadata.get("code_commit")
    if preflight_code_commit != ACCEPTED_PREFLIGHT_CODE_COMMIT:
        raise ProductionPreflightError("accepted production state has an unexpected preflight code commit")
    if runner_metadata.get("question_set_sha256") != question_sha:
        raise ProductionPreflightError("production DB question identity differs from checked-in contract")
    provider = jev_provider_from_env()
    if (
        provider.model_identifier != EXPECTED_MODEL_ALIAS
        or provider.model_version != EXPECTED_MODEL_VERSION
        or provider.transport_id != "typesafe-systemone-http-v1"
        or provider.transport_config.get("base_url") != DIRECT_BASE_URL
    ):
        provider.close()
        raise ProductionPreflightError("live provider identity failed closed before dispatch")
    topics_by_key = {str(topic["topic_key"]): topic for topic in topics}
    try:
        with PairAdjudicationRunner(
            database,
            provider,
            questions,
            question_sha,
            runner_metadata,
            max_attempts=args.max_attempts,
        ) as runner:
            runner.register_candidate_pairs(pairs)
            _record_execution_provenance(
                database,
                str(runner_metadata["run_id"]),
                preflight_code_commit,
                execution_code_commit,
            )
            base_results = runner.run_pairs(pairs, topics_by_key)
            _persist_resolutions(runner.connection, pairs, base_results)
            for _iteration in range(len(topics_by_key) + 1):
                base_pairs, base_runs = _registry_snapshot(database, "candidate_pairs")
                edges, edge_runs = _registry_snapshot(database, "coherence_edges")
                base_resolutions = _resolution_map(database)
                edge_resolutions = _resolution_map_for_registry(database, "coherence_edges")
                additions = plan_representative_coherence_edges(
                    topics_by_key, base_pairs, base_runs, base_resolutions, edges, edge_resolutions
                )
                if additions:
                    runner.register_coherence_edges(additions)
                    for edge in additions:
                        request, cache_key = runner._request(edge, build_pair_state(edge, topics_by_key), None)
                        runner._ensure_run(edge, request, cache_key, None)
                edges, _edge_runs = _registry_snapshot(database, "coherence_edges")
                if edges:
                    coherence_results = runner.run_pairs(edges, topics_by_key)
                    _persist_resolutions(runner.connection, edges, coherence_results)
                if not additions:
                    refreshed_base_pairs, refreshed_base_runs = _registry_snapshot(database, "candidate_pairs")
                    refreshed_edges, refreshed_edge_runs = _registry_snapshot(database, "coherence_edges")
                    refreshed_base_resolutions = _resolution_map(database)
                    refreshed_edge_resolutions = _resolution_map_for_registry(database, "coherence_edges")
                    more = plan_representative_coherence_edges(
                        topics_by_key, refreshed_base_pairs, refreshed_base_runs,
                        refreshed_base_resolutions, refreshed_edges, refreshed_edge_resolutions,
                    )
                    if not more:
                        break
                    continue
                # Newly discovered representative edges can merge components,
                # changing the representative and requiring another proof pass.
                continue
            else:
                raise ProductionPreflightError("coherence family construction did not converge")
    finally:
        provider.close()

    after = {
        "yee31_advance_review_set_sha256": sha256_file(advance_path),
        "yee30_retrieval_db_sha256": sha256_file(y30_db),
    }
    if before != after:
        raise ProductionPreflightError("accepted YEE-30/YEE-31 inputs changed during production run")
    return _finalize_production_exports(
        database, output_dir, topics_by_key, pairs, before, args, execution_code_commit
    )


def _resolution_map_for_registry(database: Path, registry: str) -> dict[str, dict[str, Any]]:
    pairs, _runs = _registry_snapshot(database, registry)
    all_resolutions = _resolution_map(database)
    return {str(pair["pair_id"]): all_resolutions[str(pair["pair_id"])] for pair in pairs if str(pair["pair_id"]) in all_resolutions}


def _finalize_production_exports(
    database: Path,
    output_dir: Path,
    topics_by_key: Mapping[str, Mapping[str, Any]],
    expected_pairs: list[Mapping[str, Any]],
    input_hashes: Mapping[str, str],
    args: argparse.Namespace,
    execution_code_commit: str,
) -> dict[str, Any]:
    base_pairs, base_runs = _registry_snapshot(database, "candidate_pairs")
    coherence_edges, coherence_runs = _registry_snapshot(database, "coherence_edges")
    resolutions = _resolution_map(database)
    edge_resolutions = _resolution_map_for_registry(database, "coherence_edges")
    production_meta_connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    production_meta_connection.row_factory = sqlite3.Row
    try:
        metadata_row = production_meta_connection.execute(
            "SELECT run_id,metadata_json FROM production_run_metadata ORDER BY run_id LIMIT 1"
        ).fetchone()
        production_metadata = json.loads(metadata_row["metadata_json"])
        preflight_code_commit = production_metadata.get("code_commit")
        execution_row = production_meta_connection.execute(
            "SELECT preflight_code_commit FROM production_execution_provenance "
            "WHERE run_id=? AND execution_code_commit=?",
            (metadata_row["run_id"], execution_code_commit),
        ).fetchone()
        if execution_row is None or execution_row["preflight_code_commit"] != preflight_code_commit:
            raise ProductionPreflightError("finalization is missing matching append-only execution provenance")
        base_status_counts = dict(production_meta_connection.execute(
            "SELECT status,count(*) FROM pair_runs WHERE pair_id IN (SELECT pair_id FROM candidate_pairs) AND replicate_id IS NULL GROUP BY status"
        ).fetchall())
        raw_attempt_count = production_meta_connection.execute("SELECT count(*) FROM pair_attempts").fetchone()[0]
        replicate_count = production_meta_connection.execute("SELECT count(*) FROM pair_runs WHERE replicate_id IS NOT NULL").fetchone()[0]
        seed_count = production_meta_connection.execute("SELECT count(*) FROM seed_provenance").fetchone()[0]
        seed_attempt_count = production_meta_connection.execute("SELECT COALESCE(sum(source_attempt_count),0) FROM seed_provenance").fetchone()[0]
    finally:
        production_meta_connection.close()
    provenance = {
        "yee31_run_id": production_metadata.get("yee31_run_id"),
        "input_hashes": dict(input_hashes),
        "production_run_id": metadata_row["run_id"],
        "source_v02_run_id": production_metadata.get("source_run_id"),
        "source_v02_database_sha256": production_metadata.get("source_database_sha256"),
    }
    family_metadata = {
        "run_id": metadata_row["run_id"],
        "work_order": "YEE-37",
        "execution_mode": "production_resume",
        "pair_generation_version": PAIR_GENERATION_VERSION,
        "pair_state_version": PAIR_STATE_VERSION,
        "pair_question_set_version": PAIR_QUESTION_SET_VERSION,
        "pair_policy_version": PAIR_POLICY_VERSION,
        "pair_output_version": PAIR_OUTPUT_VERSION,
        "resolution_policy_version": RESOLUTION_POLICY_VERSION,
        "family_build_version": FAMILY_BUILD_VERSION,
        "family_id_version": FAMILY_ID_VERSION,
        "coherence_edge_version": COHERENCE_EDGE_VERSION,
        "preflight_code_commit": preflight_code_commit,
        "execution_code_commit": execution_code_commit,
        "requested_model_alias": EXPECTED_MODEL_ALIAS,
        "expected_model_version": EXPECTED_MODEL_VERSION,
        "source_input_hashes": dict(input_hashes),
        "network_requests": raw_attempt_count - seed_attempt_count,
    }
    artifacts = build_family_artifacts(
        topics_by_key, base_pairs, base_runs, resolutions, coherence_edges, coherence_runs,
        edge_resolutions, provenance,
    )
    artifacts["metadata"] = family_metadata
    for table in ("concept_families", "concept_members", "pair_relations", "review_pairs", "broader_narrower_edges", "coherence_edges"):
        _replace_file(output_dir, f"{table}.jsonl", jsonl_bytes(artifacts[table]))
        _replace_file(output_dir, f"{table}.csv", csv_bytes(artifacts[table]))
    _replace_file(output_dir, "family_evidence_packs.jsonl", jsonl_bytes(artifacts["family_evidence_packs"]))
    _replace_file(output_dir, "BASE_PAIR_UNIVERSE.jsonl", jsonl_bytes(expected_pairs))
    _replace_file(output_dir, "RAW_REQUEST_RESPONSE_EVIDENCE.jsonl.gz", deterministic_raw_archive(_raw_attempt_export(database)))
    build_concept_retrieval_db(output_dir / "concept_retrieval.sqlite", artifacts)
    execution = {
        "status": artifacts["qa"]["status"],
        "run_id": metadata_row["run_id"],
        "preflight_code_commit": preflight_code_commit,
        "execution_code_commit": execution_code_commit,
        "base_pair_status_counts": base_status_counts,
        "completed_seed_count": seed_count,
        "pending_base_pairs": base_status_counts.get("pending", 0),
        "failed_base_pairs": base_status_counts.get("failed", 0),
        "coherence_edge_count": len(coherence_edges),
        "coherence_edge_status_counts": artifacts["qa"]["coherence_edge_status_counts"],
        "raw_attempt_count": raw_attempt_count,
        "attempt_records_new_since_seed": raw_attempt_count - seed_attempt_count,
        "stability_replicates_imported": replicate_count,
        "input_hashes_before_after": {key: {"before": val, "after": val} for key, val in input_hashes.items()},
        "family_qa": artifacts["qa"],
        "network_or_jev_calls_since_seed": raw_attempt_count - seed_attempt_count,
    }
    _replace_file(output_dir, "PRODUCTION_FINAL_QA.json", (canonical_json(execution) + "\n").encode("utf-8"))
    _replace_file(output_dir, "FINAL_REPORT.md", ("\n".join([
        "# YEE-37 Production Pair Adjudication + Families",
        "",
        f"Status: `{artifacts['qa']['status']}`",
        "",
        f"Preflight code commit: `{preflight_code_commit}`.",
        f"Execution code commit: `{execution_code_commit}`.",
        f"Base pairs: {len(base_pairs):,}; outcomes: {canonical_json(base_status_counts)}.",
        f"Separate representative-coherence edges: {len(coherence_edges):,}; statuses: {canonical_json(artifacts['qa']['coherence_edge_status_counts'])}.",
        f"Families: {len(artifacts['concept_families']):,}; topics reconciled: {len(artifacts['concept_members']):,}/{len(topics_by_key):,}.",
        f"Seeded canonical pilot outcomes: {seed_count}; stability replicates imported: {replicate_count}.",
        f"Input hashes unchanged: `{canonical_json(input_hashes)}`.",
        "",
    ])).encode("utf-8"))
    manifest = artifact_manifest(output_dir, {**family_metadata, **execution})
    _replace_file(output_dir, "DATASET_MANIFEST.json", (canonical_json(manifest) + "\n").encode("utf-8"))
    return execution


def run_offline_preflight(args: argparse.Namespace) -> dict[str, Any]:
    advance_path = Path(args.advance_set).resolve()
    y30_db = Path(args.input_db).resolve()
    v02_dir = Path(args.v02_evidence_dir).resolve()
    v03_dir = Path(args.v03_evidence_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ProductionPreflightError("preflight output directory must be empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    v02_inventory_before = _inventory(v02_dir)
    source_manifest = _read_json(v02_dir / "MANIFEST.json")
    _assert_source_manifest(v02_dir, source_manifest)
    v02_meta = _db_metadata(v02_dir / "YEE37_PAIR_PILOT.sqlite")
    if source_manifest.get("run_id") != v02_meta.get("run_id"):
        raise ProductionPreflightError("accepted v0.2 run metadata and manifest disagree")
    if source_manifest.get("status") != "PAIR_PILOT_READY_FOR_SUPERVISOR_REVIEW":
        raise ProductionPreflightError("v0.2 source run is not the accepted pilot evidence")

    v03_manifest = _read_json(v03_dir / "MANIFEST_V03.json")
    v03_files = {row["path"]: row for row in v03_manifest.get("artifact_files", [])}
    for name in ("PILOT_OUTCOMES_V03.jsonl",):
        file_path = v03_dir / name
        entry = v03_files.get(name)
        if not entry or not file_path.is_file() or sha256_file(file_path) != entry["sha256"]:
            raise ProductionPreflightError(f"accepted v0.3 replay artifact failed manifest validation: {name}")

    input_hash_before = {
        "yee31_advance_review_set_sha256": sha256_file(advance_path),
        "yee30_retrieval_db_sha256": sha256_file(y30_db),
    }
    y30_metadata, topics, loaded_hashes = load_accepted_advance_topics(advance_path, y30_db)
    if input_hash_before["yee31_advance_review_set_sha256"] != loaded_hashes["yee31_advance_review_set_sha256"]:
        raise ProductionPreflightError("YEE-31 input hash changed during load")
    pairs = generate_candidate_pairs(topics, loaded_hashes)
    pair_path = v02_dir / "CANDIDATE_PAIRS.jsonl"
    expected_pair_bytes = jsonl_bytes(pairs)
    if pair_path.read_bytes() != expected_pair_bytes:
        raise ProductionPreflightError("reconstructed base pair universe differs from accepted v0.2 artifact")
    pilot = select_stratified_pairs(pairs, PILOT_SIZE)
    stability = select_stratified_pairs(pilot, STABILITY_SIZE)
    if (v02_dir / "PILOT_SELECTION.jsonl").read_bytes() != jsonl_bytes(pilot):
        raise ProductionPreflightError("reconstructed pilot selection differs from accepted v0.2 identities")
    if (v02_dir / "STABILITY_SELECTION.jsonl").read_bytes() != jsonl_bytes(stability):
        raise ProductionPreflightError("reconstructed stability selection differs from accepted v0.2 identities")
    if len(stability) != STABILITY_SIZE:
        raise ProductionPreflightError("accepted stability selection is not 40 pairs")
    questions, question_sha = load_pair_questions()
    if question_sha != v02_meta.get("question_set_sha256"):
        raise ProductionPreflightError("checked-in question contract differs from accepted v0.2 cache identity")
    if v02_meta.get("expected_model_version") != EXPECTED_MODEL_VERSION or v02_meta.get("requested_model_alias") != EXPECTED_MODEL_ALIAS:
        raise ProductionPreflightError("accepted pair inference model pin is inconsistent")
    if v02_meta.get("transport_id") != "typesafe-systemone-http-v1" or v02_meta.get("endpoint") != DIRECT_BASE_URL + "/v1/systemone":
        raise ProductionPreflightError("accepted pair transport identity is inconsistent")

    v02_outcomes = _read_jsonl(v02_dir / "PILOT_OUTCOMES.jsonl")
    v03_outcomes = _read_jsonl(v03_dir / "PILOT_OUTCOMES_V03.jsonl")
    if len(v02_outcomes) != 120 or len(v03_outcomes) != 120:
        raise ProductionPreflightError("accepted pilot outcomes must reconcile 120/120")
    source_db = v02_dir / "YEE37_PAIR_PILOT.sqlite"
    source_raw = v02_dir / "RAW_REQUEST_RESPONSE_EVIDENCE.jsonl.gz"
    source_database_sha = sha256_file(source_db)
    source_raw_sha = sha256_file(source_raw)
    source_outcomes_sha = sha256_file(v02_dir / "PILOT_OUTCOMES.jsonl")
    manifest_items = {row["path"]: row for row in source_manifest["artifact_files"]}
    for name, actual in (
        ("YEE37_PAIR_PILOT.sqlite", source_database_sha),
        ("RAW_REQUEST_RESPONSE_EVIDENCE.jsonl.gz", source_raw_sha),
        ("PILOT_OUTCOMES.jsonl", source_outcomes_sha),
    ):
        if manifest_items.get(name, {}).get("sha256") != actual:
            raise ProductionPreflightError(f"accepted v0.2 seed source does not match manifest: {name}")

    output_dir.mkdir(parents=True, exist_ok=True)
    database = output_dir / "production.sqlite"
    source_pair_ids = {str(row["pair_id"]) for row in pilot}
    provenance = {
        "yee31_run_id": v02_meta.get("yee31_run_id"),
        "yee31_advance_review_set_sha256": loaded_hashes["yee31_advance_review_set_sha256"],
        "yee30_retrieval_db_sha256": loaded_hashes["yee30_retrieval_db_sha256_before"],
        "source_v02_run_id": v02_meta["run_id"],
        "source_v02_code_commit": v02_meta.get("code_commit"),
        "source_v02_database_sha256": source_database_sha,
        "source_v02_raw_archive_sha256": source_raw_sha,
        "input_hashes": {
            "yee31_advance_review_set_sha256": loaded_hashes["yee31_advance_review_set_sha256"],
            "yee30_retrieval_db_sha256": loaded_hashes["yee30_retrieval_db_sha256_before"],
        },
    }
    code_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=Path(__file__).resolve().parents[2],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    seed_summary = create_preflight_state(
        database,
        pairs,
        {str(topic["topic_key"]): topic for topic in topics},
        source_pair_ids,
        v02_outcomes,
        v03_outcomes,
        source_db,
        source_raw,
        v02_dir / "PILOT_OUTCOMES.jsonl",
        str(v02_meta["run_id"]),
        source_database_sha,
        source_raw_sha,
        loaded_hashes,
        code_commit,
    )
    base_pairs, base_runs = _registry_snapshot(database, "candidate_pairs")
    edges, edge_runs = _registry_snapshot(database, "coherence_edges")
    base_resolutions = _resolution_map(database)
    edge_resolutions: dict[str, dict[str, Any]] = {}
    family_metadata = {
        "run_id": seed_summary["run_id"],
        "work_order": "YEE-37",
        "execution_mode": "offline_preflight",
        "code_commit": code_commit,
        "production_pipeline_version": PRODUCTION_PIPELINE_VERSION,
        "pair_generation_version": PAIR_GENERATION_VERSION,
        "pair_state_version": PAIR_STATE_VERSION,
        "pair_question_set_version": PAIR_QUESTION_SET_VERSION,
        "pair_policy_version": PAIR_POLICY_VERSION,
        "pair_output_version": PAIR_OUTPUT_VERSION,
        "resolution_policy_version": RESOLUTION_POLICY_VERSION,
        "family_build_version": FAMILY_BUILD_VERSION,
        "family_id_version": FAMILY_ID_VERSION,
        "coherence_edge_version": COHERENCE_EDGE_VERSION,
        "source_input_hashes": provenance["input_hashes"],
        "network_requests": 0,
        "pending_full_adjudication_not_dispatched": True,
    }
    artifacts, index_mode = _replay_outputs(
        {str(topic["topic_key"]): topic for topic in topics},
        base_pairs,
        base_runs,
        base_resolutions,
        edges,
        edge_runs,
        edge_resolutions,
        provenance,
        family_metadata,
    )
    artifacts["metadata"] = {**family_metadata, "search_index_mode": index_mode}

    # A read-only seed export makes it auditable that only canonical v0.2 pilot
    # outcomes entered production; the normalized inference objects are unchanged.
    seeded_rows = []
    for row in sorted(v02_outcomes, key=lambda item: str(item["pair_id"])):
        derived = next(item for item in v03_outcomes if item["pair_id"] == row["pair_id"])
        seeded_rows.append({
            **row,
            "seed_provenance": {
                "source_run_id": v02_meta["run_id"],
                "source_database_sha256": source_database_sha,
                "source_outcomes_sha256": source_outcomes_sha,
                "replicate_id": None,
            },
            "resolution": {
                key: derived[key]
                for key in (
                    "jev_policy_decision_v02", "resolution_decision", "resolution_source",
                    "resolution_policy_version",
                )
            },
        })
    raw_rows = _raw_attempt_export(database)
    if len(raw_rows) != seed_summary["seed_attempt_count"] or any(row["replicate_id"] is not None for row in raw_rows):
        raise ProductionPreflightError("production raw evidence contains non-pilot or stability attempts")
    for row in raw_rows:
        body = json.loads(base64.b64decode(row["request_body_base64"]))
        if set(body) != {"model", "state", "questions"}:
            raise ProductionPreflightError("seeded wire request contains fields outside the accepted request contract")

    input_hash_after = {
        "yee31_advance_review_set_sha256": sha256_file(advance_path),
        "yee30_retrieval_db_sha256": sha256_file(y30_db),
    }
    if input_hash_after != input_hash_before or input_hash_after != {
        "yee31_advance_review_set_sha256": loaded_hashes["yee31_advance_review_set_sha256"],
        "yee30_retrieval_db_sha256": loaded_hashes["yee30_retrieval_db_sha256_before"],
    }:
        raise ProductionPreflightError("accepted YEE-31/YEE-30 inputs changed during preflight")
    v02_inventory_after = _inventory(v02_dir)
    if v02_inventory_before != v02_inventory_after:
        raise ProductionPreflightError("immutable v0.2 source evidence changed during offline preflight")

    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    try:
        base_counts = dict(connection.execute(
            "SELECT status,count(*) FROM pair_runs WHERE pair_id IN (SELECT pair_id FROM candidate_pairs) AND replicate_id IS NULL GROUP BY status"
        ).fetchall())
        replicate_count = connection.execute("SELECT count(*) FROM pair_runs WHERE replicate_id IS NOT NULL").fetchone()[0]
        seed_count = connection.execute("SELECT count(*) FROM seed_provenance").fetchone()[0]
        raw_attempt_count = connection.execute("SELECT count(*) FROM pair_attempts").fetchone()[0]
    finally:
        connection.close()
    if base_counts != {"completed": 120, "pending": 1_070} or seed_count != 120 or replicate_count != 0:
        raise ProductionPreflightError("final preflight count reconciliation failed")

    replay_output_names = (
        "concept_families", "concept_members", "pair_relations", "review_pairs",
        "broader_narrower_edges", "family_evidence_packs", "coherence_edges",
    )
    for name in ("concept_families", "concept_members", "pair_relations", "review_pairs", "broader_narrower_edges", "coherence_edges"):
        _write_file(output_dir, f"{name}.jsonl", jsonl_bytes(artifacts[name]))
        _write_file(output_dir, f"{name}.csv", csv_bytes(artifacts[name]))
    _write_file(output_dir, "family_evidence_packs.jsonl", jsonl_bytes(artifacts["family_evidence_packs"]))
    _write_file(output_dir, "BASE_PAIR_UNIVERSE.jsonl", jsonl_bytes(base_pairs))
    _write_file(output_dir, "SEEDED_V02_PILOT_OUTCOMES.jsonl", jsonl_bytes(seeded_rows))
    _write_file(output_dir, "SEEDED_RAW_REQUEST_RESPONSE_EVIDENCE.jsonl.gz", deterministic_raw_archive(raw_rows))
    retrieval_path = output_dir / "concept_retrieval.sqlite"
    build_concept_retrieval_db(retrieval_path, artifacts)

    preflight = {
        **family_metadata,
        "status": "PRODUCTION_PIPELINE_READY_FOR_SUPERVISOR_REVIEW",
        "accepted_source_run_id": v02_meta["run_id"],
        "accepted_source_database_sha256": source_database_sha,
        "accepted_source_outcomes_sha256": source_outcomes_sha,
        "accepted_source_raw_archive_sha256": source_raw_sha,
        "accepted_seed_pair_count": len(seeded_rows),
        "base_pair_count": len(base_pairs),
        "base_pair_status_counts": base_counts,
        "completed_from_seed": seed_count,
        "pending": 1_070,
        "seeded_raw_attempt_count": raw_attempt_count,
        "stability_replicates_imported": replicate_count,
        "coherence_edges_separate_from_base_universe": len(edges),
        "candidate_pair_universe_sha256": sha256_bytes(expected_pair_bytes),
        "pilot_selection_count": len(pilot),
        "stability_selection_count": len(stability),
        "stability_repetitions": STABILITY_REPETITIONS,
        "source_stability_rows_observed_read_only": seed_summary["source_stability_run_count"],
        "source_stability_attempts_read_only": seed_summary["source_stability_attempt_count"],
        "family_qa": artifacts["qa"],
        "retrieval_index_mode": index_mode,
        "deterministic_replay": "PASS_BYTE_IDENTICAL_JSONL_CSV_AND_RETRIEVAL_DB",
        "input_hashes_before_after": {key: {"before": value, "after": input_hash_after[key]} for key, value in input_hash_before.items()},
        "source_v02_inventory_unchanged": True,
        "network_or_jev_requests": 0,
        "pending_base_pairs_dispatched": 0,
        "coherence_edges_dispatched": 0,
        "full_adjudication_started": False,
        "family_build_finalized": False,
    }
    _write_file(output_dir, "PRODUCTION_PREFLIGHT.json", (canonical_json(preflight) + "\n").encode("utf-8"))
    _write_file(output_dir, "PRODUCTION_PREFLIGHT_QA.json", (canonical_json({
        "status": preflight["status"],
        "production_state": {
            "base_pairs": preflight["base_pair_count"],
            "completed": preflight["completed_from_seed"],
            "pending": preflight["pending"],
            "failed": base_counts.get("failed", 0),
            "stability_replicates_imported": replicate_count,
            "raw_attempts_seeded": raw_attempt_count,
            "coherence_edges": len(edges),
            "coherence_edges_are_separate_registry": True,
        },
        "family_build": artifacts["qa"],
        "seed_provenance": {
            "source_run_id": v02_meta["run_id"],
            "source_database_sha256": source_database_sha,
            "source_outcomes_sha256": source_outcomes_sha,
            "source_raw_archive_sha256": source_raw_sha,
            "seed_count": seed_count,
            "all_replicate_id_null": True,
            "source_v02_inventory_unchanged": True,
        },
        "deterministic_replay": preflight["deterministic_replay"],
        "input_hashes_before_after": preflight["input_hashes_before_after"],
        "network_or_jev_requests": 0,
    }) + "\n").encode("utf-8"))
    report = "\n".join([
        "# YEE-37 Production Pipeline — Offline Preflight",
        "",
        "Status: `PRODUCTION_PIPELINE_READY_FOR_SUPERVISOR_REVIEW`",
        "",
        "No Jev/provider/network calls were made. The preflight created a fresh production SQLite state from the accepted v0.2 pilot only; accepted YEE-30/YEE-31 inputs and all v0.2 evidence remained read-only.",
        "",
        f"- Base universe: {len(base_pairs):,} pairs; `{base_counts.get('completed', 0):,}` completed from the accepted pilot seed and `{base_counts.get('pending', 0):,}` pending.",
        f"- Seed provenance: {seed_count} outcomes (`replicate_id=None`), {raw_attempt_count} raw request/response attempts; source run `{v02_meta['run_id']}`.",
        f"- Stability: {STABILITY_SIZE} × {STABILITY_REPETITIONS} was inspected read-only for reconciliation but **zero stability outcomes/replicates were imported**.",
        f"- Representative coherence: {len(edges)} separate `COHERENCE_EDGE` records; none were dispatched. They do not alter the {len(base_pairs)} base-pair universe.",
        f"- Family preview: {len(artifacts['concept_families']):,} provisional rows covering exactly {len(artifacts['concept_members']):,}/{len(topics):,} input topics; finalization is blocked by pending base pairs/coherence edges.",
        f"- Evidence dedupe: {artifacts['qa']['canonical_resource_example_occurrences']:,} example occurrences → {artifacts['qa']['unique_family_evidence_identities']:,} unique identities.",
        f"- Deterministic replay: byte-identical JSONL/CSV exports and `concept_retrieval.sqlite` ({index_mode}).",
        f"- YEE-31 input SHA-256: `{input_hash_after['yee31_advance_review_set_sha256']}` before/after.",
        f"- YEE-30 read-only DB SHA-256: `{input_hash_after['yee30_retrieval_db_sha256']}` before/after.",
        "",
        "The remaining 1,070 base pairs and every coherence edge remain undispatched. No family dataset is represented as final. PR #4 remains draft/open/unmerged.",
        "",
        f"Code commit: `{code_commit}`. Production state, QA, exports, raw seed evidence, retrieval DB and hashes are in `DATASET_MANIFEST.json`.",
        "",
    ])
    _write_file(output_dir, "FINAL_REPORT.md", report.encode("utf-8"))
    manifest = artifact_manifest(output_dir, preflight)
    _write_file(output_dir, "DATASET_MANIFEST.json", (canonical_json(manifest) + "\n").encode("utf-8"))
    return {
        "status": preflight["status"],
        "run_id": preflight["run_id"],
        "base_pair_count": len(base_pairs),
        "completed": seed_count,
        "pending": base_counts.get("pending", 0),
        "coherence_edges": len(edges),
        "network_or_jev_requests": 0,
        "output_dir": str(output_dir),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--advance-set", required=True)
    parser.add_argument("--input-db", required=True)
    parser.add_argument("--v02-evidence-dir", required=True)
    parser.add_argument("--v03-evidence-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--authorized-execution-commit")
    parser.add_argument(
        "--offline-preflight",
        action="store_true",
        help="Create fresh production SQLite, seed only accepted pilot outcomes, and stop before inference.",
    )
    parser.add_argument(
        "--run-authorized-production",
        action="store_true",
        help="Dispatch pending production pairs/coherence edges; requires separate supervisor authorization.",
    )
    args = parser.parse_args()
    if args.offline_preflight == args.run_authorized_production:
        parser.error("select exactly one of --offline-preflight or --run-authorized-production")
    if args.offline_preflight and args.authorized_execution_commit:
        parser.error("--authorized-execution-commit is only valid for authorized production runs")
    if args.run_authorized_production and not args.authorized_execution_commit:
        parser.error("--run-authorized-production requires --authorized-execution-commit")
    if args.offline_preflight:
        result = run_offline_preflight(args)
    else:
        result = run_authorized_production(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
