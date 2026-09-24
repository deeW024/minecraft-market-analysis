"""Resumable YEE-37 production pair state and conservative family construction."""

from __future__ import annotations

import base64
import csv
import gzip
import hashlib
import io
import json
import sqlite3
from collections import Counter, defaultdict
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
    PairAdjudicationRunner,
    apply_resolution_policy_v03,
    build_pair_state,
    load_pair_questions,
)
from .jev_triage import canonical_json, sha256_bytes, sha256_file
from .topic_layer import normalize_tokens


PRODUCTION_PIPELINE_VERSION = "yee-37-production-pipeline-v0.1"
FAMILY_BUILD_VERSION = "yee-37-concept-families-v0.1"
FAMILY_ID_VERSION = "yee-37-family-id-v0.1"
COHERENCE_EDGE_VERSION = "yee-37-representative-coherence-v0.1"


class ProductionPreflightError(RuntimeError):
    pass


class _OfflineProvider:
    """Identity-only provider used to create resumable DB rows without dispatch."""

    provider_id = "JEV"
    model_identifier = EXPECTED_MODEL_ALIAS
    model_version = EXPECTED_MODEL_VERSION
    transport_id = "typesafe-systemone-http-v1"

    def __init__(self, transport_config: Mapping[str, Any]) -> None:
        self.transport_config = dict(transport_config)

    def complete(self, *_: Any, **__: Any) -> Any:
        raise AssertionError("offline preflight must never dispatch a Jev request")


def _write_metadata(connection: sqlite3.Connection, run_id: str, metadata: Mapping[str, Any]) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS production_run_metadata (
            run_id TEXT PRIMARY KEY,
            metadata_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS seed_provenance (
            cache_key TEXT PRIMARY KEY,
            pair_id TEXT NOT NULL,
            source_run_id TEXT NOT NULL,
            source_database_sha256 TEXT NOT NULL,
            source_outcomes_sha256 TEXT NOT NULL,
            source_raw_archive_sha256 TEXT NOT NULL,
            normalized_sha256 TEXT NOT NULL,
            source_attempt_count INTEGER NOT NULL,
            replicate_id TEXT
        );
        CREATE TABLE IF NOT EXISTS pair_resolutions (
            pair_id TEXT PRIMARY KEY,
            resolution_policy_version TEXT NOT NULL,
            jev_policy_decision_v02 TEXT NOT NULL,
            resolution_decision TEXT NOT NULL,
            resolution_source TEXT NOT NULL,
            derived_json TEXT NOT NULL,
            source_normalized_sha256 TEXT NOT NULL
        );
        """
    )
    encoded = canonical_json(metadata)
    existing = connection.execute(
        "SELECT metadata_json FROM production_run_metadata WHERE run_id=?", (run_id,)
    ).fetchone()
    if existing and existing[0] != encoded:
        raise ProductionPreflightError("production run metadata conflicts with existing state")
    connection.execute(
        "INSERT OR IGNORE INTO production_run_metadata(run_id,metadata_json) VALUES (?,?)",
        (run_id, encoded),
    )


def _raw_archive_attempts(path: Path) -> dict[tuple[str, int], dict[str, Any]]:
    result: dict[tuple[str, int], dict[str, Any]] = {}
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            row = json.loads(line)
            key = (str(row["cache_key"]), int(row["attempt_no"]))
            if key in result:
                raise ProductionPreflightError(f"duplicate v0.2 raw attempt in archive at line {line_no}")
            result[key] = row
    return result


def create_preflight_state(
    database: str | Path,
    pairs: list[Mapping[str, Any]],
    topics_by_key: Mapping[str, Mapping[str, Any]],
    pilot_pair_ids: set[str],
    v02_outcomes: list[Mapping[str, Any]],
    v03_outcomes: list[Mapping[str, Any]],
    source_database: str | Path,
    source_raw_archive: str | Path,
    source_outcomes_file: str | Path,
    source_run_id: str,
    source_database_sha256: str,
    source_raw_archive_sha256: str,
    input_hashes: Mapping[str, str],
    code_commit: str,
    expected_pair_count: int = 1_190,
    expected_seed_count: int = 120,
    expected_stability_run_count: int | None = STABILITY_SIZE * STABILITY_REPETITIONS,
) -> dict[str, Any]:
    """Create a fresh state with exact base universe and provenance-verified pilot seed."""
    database = Path(database).resolve()
    source_database = Path(source_database).resolve()
    source_raw_archive = Path(source_raw_archive).resolve()
    source_outcomes_file = Path(source_outcomes_file).resolve()
    if database.exists():
        raise ProductionPreflightError("production SQLite must be fresh; destination already exists")
    if len(pairs) != expected_pair_count or len({str(row["pair_id"]) for row in pairs}) != expected_pair_count:
        raise ProductionPreflightError("base pair universe count or pair_id uniqueness is invalid")
    if len(pilot_pair_ids) != expected_seed_count or len(v02_outcomes) != expected_seed_count:
        raise ProductionPreflightError("accepted v0.2 pilot seed must contain exactly 120 identities")
    if {str(row["pair_id"]) for row in v02_outcomes} != pilot_pair_ids:
        raise ProductionPreflightError("v0.2 outcome identities do not equal the accepted pilot selection")
    if {str(row["pair_id"]) for row in v03_outcomes} != pilot_pair_ids:
        raise ProductionPreflightError("v0.3 derived outcomes do not equal the accepted pilot selection")
    if sha256_file(source_database).lower() != source_database_sha256.lower():
        raise ProductionPreflightError("accepted v0.2 SQLite SHA-256 mismatch")
    if sha256_file(source_raw_archive).lower() != source_raw_archive_sha256.lower():
        raise ProductionPreflightError("accepted v0.2 raw archive SHA-256 mismatch")
    outcomes_sha = sha256_file(source_outcomes_file)
    raw_rows = _raw_archive_attempts(source_raw_archive)
    source_stability_attempt_count = sum(row.get("replicate_id") is not None for row in raw_rows.values())
    if expected_stability_run_count is not None and source_stability_attempt_count < expected_stability_run_count:
        raise ProductionPreflightError("accepted v0.2 raw archive is missing saved stability attempts")

    read_uri = f"file:{source_database.as_posix()}?mode=ro"
    source = sqlite3.connect(read_uri, uri=True)
    source.row_factory = sqlite3.Row
    try:
        source_metadata = {
            row["key"]: json.loads(row["value_json"])
            for row in source.execute("SELECT key,value_json FROM run_metadata")
        }
        if source_metadata.get("run_id") != source_run_id:
            raise ProductionPreflightError("v0.2 source run id does not match accepted provenance")
        if source_metadata.get("expected_model_version") != EXPECTED_MODEL_VERSION:
            raise ProductionPreflightError("v0.2 source does not use the accepted concrete model")
        if source_metadata.get("pair_output_version") != PAIR_OUTPUT_VERSION:
            raise ProductionPreflightError("v0.2 source output contract is not the accepted contract")
        source_stability = source.execute(
            "SELECT pair_id,replicate_id FROM pair_runs WHERE replicate_id IS NOT NULL ORDER BY pair_id,replicate_id"
        ).fetchall()
        if expected_stability_run_count is not None:
            replicate_counts = Counter(str(row["replicate_id"]) for row in source_stability)
            distinct_stability_pairs = {str(row["pair_id"]) for row in source_stability}
            if (
                len(source_stability) != expected_stability_run_count
                or len(distinct_stability_pairs) != STABILITY_SIZE
                or len(replicate_counts) != STABILITY_REPETITIONS
                or any(count != STABILITY_SIZE for count in replicate_counts.values())
            ):
                raise ProductionPreflightError("accepted source stability run set is not exactly 40 pairs × 3")
        questions, local_question_sha = load_pair_questions()
        if local_question_sha != source_metadata.get("question_set_sha256"):
            raise ProductionPreflightError("checked-in question contract differs from accepted v0.2 cache identity")
        pilot_rows = {str(row["pair_id"]): row for row in v02_outcomes}
        derived_rows = {str(row["pair_id"]): row for row in v03_outcomes}
        pair_by_id = {str(row["pair_id"]): row for row in pairs}
        if not pilot_pair_ids.issubset(pair_by_id):
            raise ProductionPreflightError("pilot seed contains an identity outside the base universe")
        for pair_id in sorted(pilot_pair_ids):
            row = pilot_rows[pair_id]
            if row.get("status") != "completed" or row.get("replicate_id") is not None:
                raise ProductionPreflightError("only completed replicate_id=None v0.2 pilot outcomes may seed production")
            normalized = row.get("normalized")
            derived = derived_rows[pair_id]
            if not isinstance(normalized, dict) or canonical_json(derived.get("normalized")) != canonical_json(normalized):
                raise ProductionPreflightError("v0.3 replay changed accepted v0.2 normalized model evidence")
            expected_resolution = apply_resolution_policy_v03(pair_by_id[pair_id], normalized)
            actual_resolution = {
                key: derived.get(key)
                for key in (
                    "jev_policy_decision_v02", "resolution_decision", "resolution_source",
                    "resolution_policy_version",
                )
            }
            if expected_resolution != actual_resolution:
                raise ProductionPreflightError("saved v0.3 policy outcome differs from deterministic replay")

        source_run_rows = {
            str(row["pair_id"]): row
            for row in source.execute(
                "SELECT * FROM pair_runs WHERE replicate_id IS NULL AND pair_id IN (%s)" %
                ",".join("?" for _ in pilot_pair_ids),
                tuple(sorted(pilot_pair_ids)),
            )
        }
        if set(source_run_rows) != pilot_pair_ids:
            raise ProductionPreflightError("v0.2 SQLite does not have exactly one canonical run per pilot pair")
        source_attempts: dict[str, list[sqlite3.Row]] = {}
        for pair_id in sorted(pilot_pair_ids):
            source_run = source_run_rows[pair_id]
            if source_run["status"] != "completed" or source_run["normalized_json"] is None:
                raise ProductionPreflightError("v0.2 pilot source contains a non-completed canonical outcome")
            normalized = json.loads(source_run["normalized_json"])
            if canonical_json(normalized) != canonical_json(pilot_rows[pair_id]["normalized"]):
                raise ProductionPreflightError("v0.2 SQLite normalized outcome differs from accepted JSONL")
            attempts = source.execute(
                "SELECT * FROM pair_attempts WHERE cache_key=? ORDER BY attempt_no",
                (source_run["cache_key"],),
            ).fetchall()
            if not attempts:
                raise ProductionPreflightError("accepted pilot outcome has no persisted raw attempts")
            for attempt in attempts:
                if not attempt["wire_request_captured"] or not attempt["request_body"] or not attempt["raw_response"]:
                    raise ProductionPreflightError("accepted pilot attempt is missing raw request/response bytes")
                if sha256_bytes(attempt["request_body"]) != attempt["request_sha256"]:
                    raise ProductionPreflightError("accepted pilot request bytes fail their stored hash")
                if sha256_bytes(attempt["raw_response"]) != attempt["raw_response_sha256"]:
                    raise ProductionPreflightError("accepted pilot response bytes fail their stored hash")
                archive_row = raw_rows.get((str(source_run["cache_key"]), int(attempt["attempt_no"])))
                if archive_row is None or archive_row.get("replicate_id") is not None:
                    raise ProductionPreflightError("raw archive does not contain the canonical pilot attempt")
                if base64.b64decode(archive_row["request_body_base64"]) != attempt["request_body"]:
                    raise ProductionPreflightError("raw archive request differs from accepted SQLite evidence")
                if base64.b64decode(archive_row["raw_response_base64"]) != attempt["raw_response"]:
                    raise ProductionPreflightError("raw archive response differs from accepted SQLite evidence")
            source_attempts[pair_id] = attempts

        universe_sha = sha256_bytes(canonical_json([
            {key: row[key] for key in sorted(row)} for row in pairs
        ]).encode("utf-8"))
        production_run_id = sha256_bytes(canonical_json({
            "pipeline_version": PRODUCTION_PIPELINE_VERSION,
            "family_build_version": FAMILY_BUILD_VERSION,
            "family_id_version": FAMILY_ID_VERSION,
            "coherence_edge_version": COHERENCE_EDGE_VERSION,
            "resolution_policy_version": RESOLUTION_POLICY_VERSION,
            "code_commit": code_commit,
            "input_hashes": dict(input_hashes),
            "question_set_sha256": source_metadata["question_set_sha256"],
            "source_run_id": source_run_id,
            "source_database_sha256": source_database_sha256,
            "source_outcomes_sha256": outcomes_sha,
            "source_raw_archive_sha256": source_raw_archive_sha256,
            "base_pair_universe_sha256": universe_sha,
            "base_pair_count": expected_pair_count,
            "seed_pair_ids": sorted(pilot_pair_ids),
        }).encode("utf-8"))
        runner_metadata = {
            "work_order": "YEE-37",
            "run_id": production_run_id,
            "code_commit": code_commit,
            "yee31_run_id": source_metadata.get("yee31_run_id"),
            "input_hashes": dict(input_hashes),
            "y30_build_metadata": source_metadata.get("y30_build_metadata", {}),
            "requested_model_alias": EXPECTED_MODEL_ALIAS,
            "expected_model_version": EXPECTED_MODEL_VERSION,
            "transport_id": source_metadata["transport_id"],
            "transport_config": source_metadata.get("transport_config", {}),
            "endpoint": source_metadata.get("endpoint"),
            "inference_parameters": source_metadata.get("inference_parameters", {}),
            "question_set_sha256": source_metadata["question_set_sha256"],
            "question_set_version": source_metadata["question_set_version"],
            "pair_generation_version": PAIR_GENERATION_VERSION,
            "pair_state_version": PAIR_STATE_VERSION,
            "pair_question_set_version": PAIR_QUESTION_SET_VERSION,
            "pair_policy_version": PAIR_POLICY_VERSION,
            "pair_output_version": PAIR_OUTPUT_VERSION,
            "candidate_pair_count": expected_pair_count,
            "pilot_seed_count": expected_seed_count,
            "stability_replicates_seeded": 0,
            "retry_max_attempts": 3,
            "resolution_policy_version": RESOLUTION_POLICY_VERSION,
            "production_pipeline_version": PRODUCTION_PIPELINE_VERSION,
        }
        production_basis = {
            "run_id": runner_metadata["run_id"],
            "work_order": "YEE-37",
            "status": "OFFLINE_PREFLIGHT",
            "code_commit": code_commit,
            "source_run_id": source_run_id,
            "source_v02_code_commit": source_metadata.get("code_commit"),
            "yee31_run_id": source_metadata.get("yee31_run_id"),
            "source_database_sha256": source_database_sha256,
            "source_raw_archive_sha256": source_raw_archive_sha256,
            "source_outcomes_sha256": outcomes_sha,
            "input_hashes": dict(input_hashes),
            "base_pair_universe_sha256": universe_sha,
            "base_pair_count": expected_pair_count,
            "seed_count": expected_seed_count,
            "replicate_id": None,
            "pair_generation_version": PAIR_GENERATION_VERSION,
            "pair_policy_version": PAIR_POLICY_VERSION,
            "resolution_policy_version": RESOLUTION_POLICY_VERSION,
            "pair_output_version": PAIR_OUTPUT_VERSION,
            "family_build_version": FAMILY_BUILD_VERSION,
            "family_id_version": FAMILY_ID_VERSION,
            "coherence_edge_version": COHERENCE_EDGE_VERSION,
            "requested_model_alias": EXPECTED_MODEL_ALIAS,
            "expected_model_version": EXPECTED_MODEL_VERSION,
            "transport_id": source_metadata.get("transport_id"),
            "inference_parameters": source_metadata.get("inference_parameters", {}),
            "network_requests": 0,
        }
        database.parent.mkdir(parents=True, exist_ok=True)
        offline_provider = _OfflineProvider(source_metadata.get("transport_config", {}))
        with PairAdjudicationRunner(
            database,
            offline_provider,
            questions,
            str(source_metadata["question_set_sha256"]),
            runner_metadata,
        ) as runner:
            runner.register_candidate_pairs([dict(pair) for pair in pairs])
            for pair in pairs:
                state = build_pair_state(pair, topics_by_key)
                request, cache_key = runner._request(pair, state, None)
                run = runner._ensure_run(pair, request, cache_key, None)
                if run["pair_id"] != pair["pair_id"]:
                    raise ProductionPreflightError("base pair cache registration mismatch")

            connection = runner.connection
            _write_metadata(connection, runner_metadata["run_id"], production_basis)
            for pair_id in sorted(pilot_pair_ids):
                pair = pair_by_id[pair_id]
                request, cache_key = runner._request(pair, build_pair_state(pair, topics_by_key), None)
                source_run = source_run_rows[pair_id]
                if source_run["cache_key"] != cache_key:
                    raise ProductionPreflightError("seed cache identity does not match production request identity")
                dest_run = connection.execute(
                    "SELECT cache_key FROM pair_runs WHERE cache_key=? AND pair_id=? AND replicate_id IS NULL",
                    (cache_key, pair_id),
                ).fetchone()
                if not dest_run:
                    raise ProductionPreflightError("seed run was not registered in the fresh production DB")
                connection.execute(
                    "UPDATE pair_runs SET status='completed',normalized_json=?,last_error=NULL WHERE cache_key=?",
                    (source_run["normalized_json"], cache_key),
                )
                for attempt in source_attempts[pair_id]:
                    connection.execute(
                        "INSERT INTO pair_attempts(cache_key,attempt_no,stage,request_body,request_sha256,"
                        "wire_request_captured,raw_response,raw_response_sha256,request_id,error,usage_json,parsed_at_ns) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                        tuple(attempt[key] for key in (
                            "cache_key", "attempt_no", "stage", "request_body", "request_sha256",
                            "wire_request_captured", "raw_response", "raw_response_sha256", "request_id",
                            "error", "usage_json", "parsed_at_ns",
                        )),
                    )
                derived = apply_resolution_policy_v03(pair, pilot_rows[pair_id]["normalized"])
                connection.execute(
                    "INSERT INTO pair_resolutions(pair_id,resolution_policy_version,jev_policy_decision_v02,"
                    "resolution_decision,resolution_source,derived_json,source_normalized_sha256) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (
                        pair_id,
                        derived["resolution_policy_version"],
                        derived["jev_policy_decision_v02"],
                        derived["resolution_decision"],
                        derived["resolution_source"],
                        canonical_json(derived),
                        sha256_bytes(canonical_json(pilot_rows[pair_id]["normalized"]).encode("utf-8")),
                    ),
                )
                connection.execute(
                    "INSERT INTO seed_provenance(cache_key,pair_id,source_run_id,source_database_sha256,"
                    "source_outcomes_sha256,source_raw_archive_sha256,normalized_sha256,source_attempt_count,replicate_id) "
                    "VALUES (?,?,?,?,?,?,?,?,NULL)",
                    (
                        cache_key, pair_id, source_run_id, source_database_sha256,
                        outcomes_sha, source_raw_archive_sha256,
                        sha256_bytes(canonical_json(pilot_rows[pair_id]["normalized"]).encode("utf-8")),
                        len(source_attempts[pair_id]),
                    ),
                )
            connection.commit()
            if connection.execute("SELECT count(*) FROM candidate_pairs").fetchone()[0] != expected_pair_count:
                raise ProductionPreflightError("production DB base universe count changed during seed")
            if connection.execute("SELECT count(*) FROM pair_runs WHERE pair_id IN (SELECT pair_id FROM candidate_pairs) AND replicate_id IS NULL AND status='completed'").fetchone()[0] != expected_seed_count:
                raise ProductionPreflightError("production seed completion count is not exactly 120")
            if connection.execute("SELECT count(*) FROM pair_runs WHERE pair_id IN (SELECT pair_id FROM candidate_pairs) AND replicate_id IS NULL AND status='pending'").fetchone()[0] != expected_pair_count - expected_seed_count:
                raise ProductionPreflightError("production pending count is not exactly 1,070")
            if connection.execute("SELECT count(*) FROM pair_runs WHERE replicate_id IS NOT NULL").fetchone()[0] != 0:
                raise ProductionPreflightError("stability replicates must not be imported into production state")
            if connection.execute("SELECT count(*) FROM pair_attempts").fetchone()[0] != sum(len(source_attempts[x]) for x in pilot_pair_ids):
                raise ProductionPreflightError("production raw-attempt import count mismatch")
            connection.commit()
            base_rows = _load_registry_runs(connection, "candidate_pairs", resolutions=True)
            base_resolutions = {
                row["pair_id"]: row["resolution"] for row in base_rows if row.get("resolution")
            }
            planned_edges = plan_representative_coherence_edges(
                topics_by_key, pairs, base_rows, base_resolutions, [], {}
            )
            runner.register_coherence_edges(planned_edges)
            for edge in planned_edges:
                request, _ = runner._request(edge, build_pair_state(edge, topics_by_key), None)
                runner._ensure_run(edge, request, request.cache_key, None)
            connection.commit()
            return {
                "run_id": runner_metadata["run_id"],
                "metadata": production_basis,
                "base_pair_count": expected_pair_count,
                "completed_seed_count": expected_seed_count,
                "pending_base_pair_count": expected_pair_count - expected_seed_count,
                "seed_attempt_count": connection.execute("SELECT count(*) FROM pair_attempts").fetchone()[0],
                "coherence_edge_count": connection.execute("SELECT count(*) FROM coherence_edges").fetchone()[0],
                "replicate_run_count": connection.execute("SELECT count(*) FROM pair_runs WHERE replicate_id IS NOT NULL").fetchone()[0],
                "source_stability_run_count": len(source_stability),
                "source_stability_attempt_count": source_stability_attempt_count,
            }
    finally:
        source.close()


def _load_registry_runs(
    connection: sqlite3.Connection,
    registry: str,
    *,
    resolutions: bool = False,
) -> list[dict[str, Any]]:
    if registry not in {"candidate_pairs", "coherence_edges"}:
        raise ValueError("unknown pair registry")
    rows = connection.execute(
        f"SELECT p.pair_id,p.left_topic_key,p.right_topic_key,p.pair_json,r.status,r.normalized_json,"
        f"r.last_error,r.cache_key,r.replicate_id FROM {registry} p "
        "LEFT JOIN pair_runs r ON r.pair_id=p.pair_id AND r.replicate_id IS NULL "
        "ORDER BY p.left_topic_key,p.right_topic_key,p.pair_id"
    ).fetchall()
    result = []
    for row in rows:
        record = {
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
        }
        if resolutions:
            saved = connection.execute(
                "SELECT derived_json FROM pair_resolutions WHERE pair_id=?", (row["pair_id"],)
            ).fetchone()
            record["resolution"] = json.loads(saved[0]) if saved else None
        result.append(record)
    return result


def _disjoint_components(topic_keys: list[str], merge_pairs: list[tuple[str, str]]) -> list[list[str]]:
    parent = {key: key for key in topic_keys}

    def find(item: str) -> str:
        while parent[item] != item:
            parent[item] = parent[parent[item]]
            item = parent[item]
        return item

    for left, right in sorted(merge_pairs):
        a, b = find(left), find(right)
        if a != b:
            parent[max(a, b)] = min(a, b)
    groups: dict[str, list[str]] = defaultdict(list)
    for key in topic_keys:
        groups[find(key)].append(key)
    return [sorted(group) for _root, group in sorted(groups.items())]


def _topic_rank(topic: Mapping[str, Any]) -> tuple[int, int, int, str]:
    count = sum(int(row.get("resource_count") or 0) for row in topic.get("topic_source_facts", []))
    key = str(topic["topic_key"])
    token_count = len(normalize_tokens(key, frozenset()))
    return (-count, token_count, len(key), key)


def _coherence_edge_id(left: str, right: str) -> str:
    low, high = sorted((left, right))
    digest = sha256_bytes(f"{COHERENCE_EDGE_VERSION}\n{low}\n{high}".encode("utf-8"))
    return f"coh_{digest}"


def _make_coherence_edge(
    left: str,
    right: str,
    base_pair_by_keys: Mapping[tuple[str, str], Mapping[str, Any]],
) -> dict[str, Any]:
    key = tuple(sorted((left, right)))
    base = base_pair_by_keys.get(key)
    edge = dict(base or {
        "left_topic_key": key[0],
        "right_topic_key": key[1],
        "blocking_reasons": ["representative_coherence_required"],
        "normalized_edit_similarity": 0.0,
        "token_jaccard": 0.0,
        "evidence_overlap_count": 0,
        "shared_canonical_identities": [],
        "sentinel_expectations": [],
    })
    edge["pair_id"] = _coherence_edge_id(*key)
    edge["edge_type"] = "COHERENCE_EDGE"
    edge["coherence_edge_version"] = COHERENCE_EDGE_VERSION
    edge["related_base_pair_id"] = str(base["pair_id"]) if base else None
    edge["left_topic_key"], edge["right_topic_key"] = key
    edge["blocking_reasons"] = sorted(set(edge.get("blocking_reasons", [])) | {"representative_coherence_required"})
    return edge


def plan_representative_coherence_edges(
    topics_by_key: Mapping[str, Mapping[str, Any]],
    base_pairs: list[Mapping[str, Any]],
    base_runs: list[Mapping[str, Any]],
    base_resolutions: Mapping[str, Mapping[str, Any]],
    coherence_edges: list[Mapping[str, Any]],
    coherence_resolutions: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Plan explicit direct representative proofs without expanding the base universe."""
    base_by_id = {str(pair["pair_id"]): pair for pair in base_pairs}
    base_by_keys = {
        tuple(sorted((str(pair["left_topic_key"]), str(pair["right_topic_key"])))): pair
        for pair in base_pairs
    }
    base_status = {str(row["pair_id"]): str(row.get("status")) for row in base_runs}
    coherence_by_keys = {
        tuple(sorted((str(edge["left_topic_key"]), str(edge["right_topic_key"])))): edge
        for edge in coherence_edges
    }
    merge_links: list[tuple[str, str]] = []
    for pair_id, resolution in base_resolutions.items():
        pair = base_by_id[pair_id]
        if resolution.get("resolution_decision") == "MERGE":
            merge_links.append((str(pair["left_topic_key"]), str(pair["right_topic_key"])))
    for edge_id, resolution in coherence_resolutions.items():
        edge = next((row for row in coherence_edges if row["pair_id"] == edge_id), None)
        if edge and resolution.get("resolution_decision") == "MERGE":
            merge_links.append((str(edge["left_topic_key"]), str(edge["right_topic_key"])))
    components = _disjoint_components(sorted(topics_by_key), merge_links)
    existing_ids = {str(edge["pair_id"]) for edge in coherence_edges}
    planned: dict[str, dict[str, Any]] = {}
    for component in components:
        if len(component) < 2:
            continue
        representative = min((topics_by_key[key] for key in component), key=_topic_rank)
        representative_key = str(representative["topic_key"])
        for member in component:
            if member == representative_key:
                continue
            key = tuple(sorted((representative_key, member)))
            base = base_by_keys.get(key)
            direct_base = next(
                (
                    row for row in base_runs
                    if row["pair_id"] == (base["pair_id"] if base else None)
                ),
                None,
            )
            if direct_base and direct_base.get("status") == "completed":
                direct_resolution = base_resolutions.get(str(base["pair_id"]), {})
                if direct_resolution.get("resolution_decision") == "MERGE":
                    continue
                # An explicit KEEP/REVIEW/failed direct relation blocks this component.
                continue
            existing_edge = coherence_by_keys.get(key)
            if existing_edge:
                if existing_edge.get("pair_id") in coherence_resolutions:
                    continue
                edge = dict(existing_edge)
            else:
                edge = _make_coherence_edge(*key, base_by_keys)
            if edge["pair_id"] not in existing_ids:
                planned[edge["pair_id"]] = edge
    return [planned[key] for key in sorted(planned)]


def _family_id(member_keys: list[str]) -> str:
    payload = FAMILY_ID_VERSION + "\n" + "\n".join(sorted(member_keys))
    return "family_" + sha256_bytes(payload.encode("utf-8"))


def _dedup_family_evidence(
    member_keys: list[str],
    topics_by_key: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], int, int]:
    occurrences: list[tuple[str, dict[str, Any]]] = []
    for topic_key in sorted(member_keys):
        pack = topics_by_key[topic_key].get("evidence_pack", {})
        for source, source_pack in sorted(pack.get("sources", {}).items()):
            for example in source_pack.get("examples", []):
                identity = str(example.get("canonical_identity") or "")
                if not identity:
                    raise ProductionPreflightError(f"evidence example has no canonical identity in {topic_key}")
                allowed = set(topics_by_key[topic_key].get("accepted_y31", {}).get("canonical_identity_references", []))
                if identity not in allowed:
                    raise ProductionPreflightError(f"evidence identity does not resolve to accepted YEE-31 for {topic_key}")
                occurrences.append((topic_key, dict(example)))
    grouped: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)
    for topic_key, example in occurrences:
        grouped[str(example["canonical_identity"])].append((topic_key, example))
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for identity, entries in sorted(grouped.items()):
        sources = {str(example.get("source")) for _topic, example in entries}
        if len(sources) != 1:
            raise ProductionPreflightError(f"canonical evidence identity has conflicting source values: {identity}")
        canonical_variants = sorted({canonical_json(example) for _topic, example in entries})
        representative = json.loads(canonical_variants[0])
        by_source[next(iter(sources))].append({
            "canonical_identity": identity,
            "source": next(iter(sources)),
            "source_record": representative,
            "member_topic_keys": sorted({topic_key for topic_key, _example in entries}),
            "selection_roles": sorted({
                str(role)
                for _topic, example in entries
                for role in example.get("selection_roles", [])
            }),
            "evidence_variants": [json.loads(item) for item in canonical_variants],
        })
    for source in by_source:
        by_source[source].sort(key=lambda row: row["canonical_identity"])
    return dict(sorted(by_source.items())), len(occurrences), len(grouped)


def build_family_artifacts(
    topics_by_key: Mapping[str, Mapping[str, Any]],
    base_pairs: list[Mapping[str, Any]],
    base_runs: list[Mapping[str, Any]],
    base_resolutions: Mapping[str, Mapping[str, Any]],
    coherence_edges: list[Mapping[str, Any]],
    coherence_runs: list[Mapping[str, Any]],
    coherence_resolutions: Mapping[str, Mapping[str, Any]],
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    """Build deterministic provisional or final families using direct representative proof."""
    base_by_id = {str(pair["pair_id"]): pair for pair in base_pairs}
    edge_by_id = {str(edge["pair_id"]): edge for edge in coherence_edges}
    base_run_by_id = {str(row["pair_id"]): row for row in base_runs}
    edge_run_by_id = {str(row["pair_id"]): row for row in coherence_runs}
    base_by_keys = {
        tuple(sorted((str(pair["left_topic_key"]), str(pair["right_topic_key"])))): pair
        for pair in base_pairs
    }
    proposed_edges = plan_representative_coherence_edges(
        topics_by_key,
        base_pairs,
        base_runs,
        base_resolutions,
        coherence_edges,
        coherence_resolutions,
    )
    all_edges = {**edge_by_id, **{str(row["pair_id"]): row for row in proposed_edges}}
    has_pending_base = any(row.get("status") != "completed" for row in base_runs)
    has_failed_base = any(row.get("status") == "failed" for row in base_runs)
    all_coherence_rows = [edge_run_by_id.get(edge_id, {}) for edge_id in all_edges]
    has_pending_coherence = any(
        not row or row.get("status") != "completed" for row in all_coherence_rows
    )
    has_failed_coherence = any(row.get("status") == "failed" for row in all_coherence_rows)
    final = not (has_pending_base or has_failed_base or has_pending_coherence or has_failed_coherence)

    merge_links: list[tuple[str, str]] = []
    base_resolution_by_keys: dict[tuple[str, str], Mapping[str, Any]] = {}
    for pair_id, resolution in base_resolutions.items():
        pair = base_by_id[pair_id]
        key = tuple(sorted((str(pair["left_topic_key"]), str(pair["right_topic_key"]))))
        base_resolution_by_keys[key] = resolution
        if resolution.get("resolution_decision") == "MERGE":
            merge_links.append(key)
    coherence_resolution_by_keys: dict[tuple[str, str], Mapping[str, Any]] = {}
    for edge_id, resolution in coherence_resolutions.items():
        edge = all_edges.get(edge_id)
        if edge:
            key = tuple(sorted((str(edge["left_topic_key"]), str(edge["right_topic_key"]))))
            coherence_resolution_by_keys[key] = resolution
            if resolution.get("resolution_decision") == "MERGE":
                merge_links.append(key)
    components = _disjoint_components(sorted(topics_by_key), merge_links)

    planned_by_keys = {
        tuple(sorted((str(edge["left_topic_key"]), str(edge["right_topic_key"])))): edge
        for edge in proposed_edges
    }
    existing_coherence_by_keys = {
        tuple(sorted((str(edge["left_topic_key"]), str(edge["right_topic_key"])))): edge
        for edge in all_edges.values()
    }

    accepted_components: list[tuple[list[str], str]] = []
    reviewed_singletons: set[str] = set()
    for component in components:
        if len(component) == 1:
            accepted_components.append((component, "SINGLETON" if final else "PROVISIONAL_SINGLETON"))
            continue
        representative = min((topics_by_key[key] for key in component), key=_topic_rank)
        rep_key = str(representative["topic_key"])
        missing_or_blocked = False
        for member in component:
            if member == rep_key:
                continue
            key = tuple(sorted((rep_key, member)))
            base_pair = base_by_keys.get(key)
            direct_base = base_resolution_by_keys.get(key)
            if direct_base is not None:
                if direct_base.get("resolution_decision") != "MERGE":
                    missing_or_blocked = True
                    break
                continue
            direct_coherence = coherence_resolution_by_keys.get(key)
            if direct_coherence is not None:
                if direct_coherence.get("resolution_decision") != "MERGE":
                    missing_or_blocked = True
                    break
                continue
            if key in planned_by_keys or key in existing_coherence_by_keys or (base_pair and base_run_by_id.get(str(base_pair["pair_id"]), {}).get("status") != "completed"):
                missing_or_blocked = True
                continue
            missing_or_blocked = True
        if missing_or_blocked:
            reviewed_singletons.update(component)
            accepted_components.extend(([member], "PROVISIONAL_REVIEW_SINGLETON" if not final else "REVIEW_SINGLETON") for member in component)
        else:
            accepted_components.append((component, "ACCEPTED" if final else "PROVISIONAL_COHERENT"))

    family_rows: list[dict[str, Any]] = []
    member_rows: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []
    duplicate_occurrences = unique_examples = 0
    for members, status in sorted(accepted_components, key=lambda item: tuple(item[0])):
        members = sorted(members)
        representative = min((topics_by_key[key] for key in members), key=_topic_rank)
        canonical_key = str(representative["topic_key"])
        family_id = _family_id(members)
        evidence, occurrences, unique_count = _dedup_family_evidence(members, topics_by_key)
        duplicate_occurrences += occurrences - unique_count
        unique_examples += unique_count
        source_union = sorted({
            str(row.get("source"))
            for key in members
            for row in topics_by_key[key].get("topic_source_facts", [])
        })
        classes = sorted({
            str(topics_by_key[key].get("candidate", {}).get("candidate_class", "unknown"))
            for key in members
        })
        family_rows.append({
            "family_id": family_id,
            "canonical_topic_key": canonical_key,
            "member_count": len(members),
            "aliases": [key for key in members if key != canonical_key],
            "member_topic_keys": members,
            "source_presence_union": source_union,
            "candidate_classes": classes,
            "family_status": status,
            "family_build_version": FAMILY_BUILD_VERSION,
            "family_id_version": FAMILY_ID_VERSION,
            "resolution_policy_version": RESOLUTION_POLICY_VERSION,
            "provenance": dict(provenance),
        })
        for key in members:
            topic = topics_by_key[key]
            member_rows.append({
                "family_id": family_id,
                "topic_key": key,
                "canonical_topic_key": canonical_key,
                "is_representative": key == canonical_key,
                "member_status": status,
                "yee31_triage_reference": {
                    "run_id": topic.get("accepted_y31", {}).get("run_id"),
                    "policy_decision": "ADVANCE",
                    "state_sha256": topic.get("accepted_y31", {}).get("state_sha256"),
                    "question_set_sha256": topic.get("accepted_y31", {}).get("question_set_sha256"),
                    "evidence_pack_ref": topic.get("accepted_y31", {}).get("evidence_pack_ref"),
                },
                "yee30_evidence_reference": {
                    "retrieval_db_sha256": topic.get("accepted_y31", {}).get("evidence_pack_ref", {}).get("retrieval_db_sha256"),
                    "topic_key": key,
                    "canonical_identity_count": len(topic.get("accepted_y31", {}).get("canonical_identity_references", [])),
                },
                "provenance": dict(provenance),
            })
        evidence_rows.append({
            "family_id": family_id,
            "canonical_topic_key": canonical_key,
            "member_topic_keys": members,
            "source_presence_union": source_union,
            "member_source_facts": [
                {"topic_key": key, "source_facts": topics_by_key[key].get("topic_source_facts", [])}
                for key in members
            ],
            "sources": evidence,
            "evidence_occurrence_count_before_deduplication": occurrences,
            "unique_canonical_identity_count": unique_count,
            "aggregation_policy": "Preserve member/source-native facts; do not sum raw downloads or sales across sources.",
            "provenance": dict(provenance),
        })

    member_rows.sort(key=lambda row: row["topic_key"])
    family_rows.sort(key=lambda row: row["family_id"])
    evidence_rows.sort(key=lambda row: row["family_id"])
    pair_relations: list[dict[str, Any]] = []
    review_pairs: list[dict[str, Any]] = []
    broader_edges: list[dict[str, Any]] = []

    def consume_registry(
        pair_records: list[Mapping[str, Any]],
        run_records: list[Mapping[str, Any]],
        resolutions: Mapping[str, Mapping[str, Any]],
        pair_kind: str,
    ) -> None:
        run_by_id = {str(row["pair_id"]): row for row in run_records}
        for pair in sorted(pair_records, key=lambda row: (str(row["left_topic_key"]), str(row["right_topic_key"]), str(row["pair_id"]))):
            pair_id = str(pair["pair_id"])
            run = run_by_id.get(pair_id, {})
            normalized = run.get("normalized")
            resolution = resolutions.get(pair_id)
            common = {
                "pair_id": pair_id,
                "pair_kind": pair_kind,
                "left_topic_key": pair["left_topic_key"],
                "right_topic_key": pair["right_topic_key"],
                "status": run.get("status", "pending"),
                "blocking_reasons": pair.get("blocking_reasons", []),
                "normalized_edit_similarity": pair.get("normalized_edit_similarity"),
                "token_jaccard": pair.get("token_jaccard"),
                "evidence_overlap_count": pair.get("evidence_overlap_count", 0),
                "input_hashes": dict(provenance.get("input_hashes", {})),
                "resolution_policy_version": RESOLUTION_POLICY_VERSION,
                "provenance": {
                    **dict(provenance),
                    "cache_key": run.get("cache_key"),
                    "raw_attempt_count": run.get("attempt_count", 0),
                },
            }
            if normalized is not None and run.get("status") == "completed":
                output = {
                    **common,
                    "native_typed_answers": normalized.get("typed_answers", {}),
                    "native_policy_decision_v02": normalized.get("policy_decision"),
                    "resolution": dict(resolution or {}),
                    "returned_model": normalized.get("returned_model"),
                    "state_sha256": normalized.get("state_sha256"),
                    "question_set_sha256": normalized.get("question_set_sha256"),
                    "request_sha256": normalized.get("request_sha256"),
                    "response_sha256": normalized.get("response_sha256"),
                    "native_probabilities_and_confidence_preserved": True,
                }
                pair_relations.append(output)
                relation = normalized.get("concept_relation", {}).get("choice")
                if relation == "BROADER_NARROWER":
                    broader_edges.append({
                        "edge_id": f"broader_{pair_id}",
                        "pair_id": pair_id,
                        "pair_kind": pair_kind,
                        "left_topic_key": pair["left_topic_key"],
                        "right_topic_key": pair["right_topic_key"],
                        "relation_direction": normalized.get("relation_direction", {}).get("choice"),
                        "resolution_decision": (resolution or {}).get("resolution_decision"),
                        "provenance": dict(provenance),
                    })
                if (resolution or {}).get("resolution_decision") == "REVIEW":
                    review_pairs.append({**common, "resolution": dict(resolution or {}), "review_reason": "policy_review"})
            else:
                review_pairs.append({
                    **common,
                    "resolution": dict(resolution or {}),
                    "review_reason": "failed_or_pending_adjudication",
                    "last_error": run.get("last_error"),
                })

    consume_registry(base_pairs, base_runs, base_resolutions, "BASE_PAIR")
    consume_registry(list(all_edges.values()), coherence_runs, coherence_resolutions, "COHERENCE_EDGE")
    pair_relations.sort(key=lambda row: (row["pair_kind"], row["left_topic_key"], row["right_topic_key"], row["pair_id"]))
    review_pairs.sort(key=lambda row: (row["pair_kind"], row["left_topic_key"], row["right_topic_key"], row["pair_id"]))
    broader_edges.sort(key=lambda row: (row["left_topic_key"], row["right_topic_key"], row["edge_id"]))
    coherence_output = []
    for edge_id, edge in sorted(all_edges.items()):
        run = edge_run_by_id.get(edge_id, {})
        coherence_output.append({
            **dict(edge),
            "status": run.get("status", "pending"),
            "attempt_count": run.get("attempt_count", 0),
            "resolution": dict(coherence_resolutions.get(edge_id, {})),
            "separate_from_base_pair_universe": True,
        })

    assignment = {row["topic_key"]: row["family_id"] for row in member_rows}
    family_sizes = Counter(row["member_count"] for row in family_rows)
    if len(member_rows) != len(topics_by_key) or set(assignment) != set(topics_by_key) or len(assignment) != len(member_rows):
        raise ProductionPreflightError("family build failed exact one-family-per-topic reconciliation")
    if any(row["member_count"] != sum(1 for member in member_rows if member["family_id"] == row["family_id"]) for row in family_rows):
        raise ProductionPreflightError("family member counts do not reconcile")
    qa = {
        "status": "PASS" if final else "PROVISIONAL_OFFLINE_PREFLIGHT",
        "family_build_version": FAMILY_BUILD_VERSION,
        "family_id_version": FAMILY_ID_VERSION,
        "resolution_policy_version": RESOLUTION_POLICY_VERSION,
        "base_pair_count": len(base_pairs),
        "base_pair_status_counts": dict(sorted(Counter(str(row.get("status")) for row in base_runs).items())),
        "base_pair_resolution_counts": dict(sorted(Counter(
            str(row.get("resolution_decision")) for row in base_resolutions.values()
        ).items())),
        "coherence_edge_count": len(all_edges),
        "coherence_edge_status_counts": dict(sorted(Counter(str(row.get("status", "pending")) for row in coherence_output).items())),
        "adjudicated_pair_relation_count": len(pair_relations),
        "review_pair_count": len(review_pairs),
        "broader_narrower_edge_count": len(broader_edges),
        "input_topic_count": len(topics_by_key),
        "assigned_topic_count": len(member_rows),
        "unique_assigned_topic_count": len(assignment),
        "one_family_or_singleton_per_topic": True,
        "family_count": len(family_rows),
        "family_size_distribution": {str(key): family_sizes[key] for key in sorted(family_sizes)},
        "singleton_count": family_sizes.get(1, 0),
        "multi_member_family_count": sum(count for size, count in family_sizes.items() if size > 1),
        "topics_reduced": len(topics_by_key) - len(family_rows),
        "families_finalized": final,
        "pending_base_pairs_block_finalization": has_pending_base,
        "failed_base_pairs_block_finalization": has_failed_base,
        "pending_coherence_edges_block_finalization": has_pending_coherence,
        "failed_coherence_edges_block_finalization": has_failed_coherence,
        "canonical_resource_example_occurrences": sum(row["evidence_occurrence_count_before_deduplication"] for row in evidence_rows),
        "unique_family_evidence_identities": unique_examples,
        "deduplicated_evidence_occurrences": duplicate_occurrences,
        "family_source_metrics_are_member_level_not_cross_source_sums": True,
        "input_hashes": dict(provenance.get("input_hashes", {})),
    }
    return {
        "concept_families": family_rows,
        "concept_members": member_rows,
        "pair_relations": pair_relations,
        "review_pairs": review_pairs,
        "broader_narrower_edges": broader_edges,
        "family_evidence_packs": evidence_rows,
        "coherence_edges": coherence_output,
        "qa": qa,
        "planned_coherence_edges": proposed_edges,
    }


def jsonl_bytes(rows: list[Mapping[str, Any]]) -> bytes:
    return b"".join((canonical_json(row) + "\n").encode("utf-8") for row in rows)


def csv_bytes(rows: list[Mapping[str, Any]]) -> bytes:
    columns = sorted({key for row in rows for key in row})
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow([
            "" if row.get(key) is None
            else canonical_json(row[key]) if isinstance(row.get(key), (dict, list))
            else row[key]
            for key in columns
        ])
    return buffer.getvalue().encode("utf-8")


def deterministic_raw_archive(rows: list[Mapping[str, Any]]) -> bytes:
    payload = jsonl_bytes(rows)
    return gzip.compress(payload, compresslevel=9, mtime=0)


def artifact_manifest(directory: str | Path, metadata: Mapping[str, Any], exclude: set[str] | None = None) -> dict[str, Any]:
    directory = Path(directory)
    excluded = exclude or {"DATASET_MANIFEST.json"}
    files = []
    for path in sorted(directory.iterdir(), key=lambda item: item.name):
        if path.is_file() and path.name not in excluded:
            files.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path)})
    return {**dict(metadata), "artifacts": files}


def build_concept_retrieval_db(path: str | Path, artifacts: Mapping[str, Any]) -> str:
    """Create a deterministic family retrieval DB with FTS5 or a token index fallback."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=DELETE")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE concept_families(family_id TEXT PRIMARY KEY,canonical_topic_key TEXT NOT NULL,member_count INTEGER NOT NULL,family_status TEXT NOT NULL,row_json TEXT NOT NULL);
        CREATE TABLE concept_members(topic_key TEXT PRIMARY KEY,family_id TEXT NOT NULL,row_json TEXT NOT NULL);
        CREATE TABLE pair_relations(pair_id TEXT NOT NULL,pair_kind TEXT NOT NULL,row_json TEXT NOT NULL,PRIMARY KEY(pair_id,pair_kind));
        CREATE TABLE review_pairs(pair_id TEXT NOT NULL,pair_kind TEXT NOT NULL,row_json TEXT NOT NULL,PRIMARY KEY(pair_id,pair_kind));
        CREATE TABLE broader_narrower_edges(edge_id TEXT PRIMARY KEY,row_json TEXT NOT NULL);
        CREATE TABLE family_evidence_packs(family_id TEXT PRIMARY KEY,row_json TEXT NOT NULL);
        CREATE TABLE coherence_edges(edge_id TEXT PRIMARY KEY,status TEXT NOT NULL,related_base_pair_id TEXT,row_json TEXT NOT NULL);
        CREATE TABLE metadata(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);
        CREATE INDEX concept_members_family_idx ON concept_members(family_id,topic_key);
        CREATE INDEX pair_relations_kind_idx ON pair_relations(pair_kind,pair_id);
        """
    )
    for row in artifacts["concept_families"]:
        connection.execute(
            "INSERT INTO concept_families VALUES (?,?,?,?,?)",
            (row["family_id"], row["canonical_topic_key"], row["member_count"], row["family_status"], canonical_json(row)),
        )
    for row in artifacts["concept_members"]:
        connection.execute("INSERT INTO concept_members VALUES (?,?,?)", (row["topic_key"], row["family_id"], canonical_json(row)))
    for table in ("pair_relations", "review_pairs"):
        for row in artifacts[table]:
            connection.execute(f"INSERT INTO {table} VALUES (?,?,?)", (row["pair_id"], row["pair_kind"], canonical_json(row)))
    for row in artifacts["broader_narrower_edges"]:
        connection.execute("INSERT INTO broader_narrower_edges VALUES (?,?)", (row["edge_id"], canonical_json(row)))
    for row in artifacts["family_evidence_packs"]:
        connection.execute("INSERT INTO family_evidence_packs VALUES (?,?)", (row["family_id"], canonical_json(row)))
    for row in artifacts["coherence_edges"]:
        connection.execute(
            "INSERT INTO coherence_edges VALUES (?,?,?,?)",
            (row["pair_id"], row["status"], row.get("related_base_pair_id"), canonical_json(row)),
        )
    connection.executemany(
        "INSERT INTO metadata VALUES (?,?)",
        [(key, canonical_json(value)) for key, value in sorted(artifacts["metadata"].items())],
    )
    search_rows = []
    by_family = {row["family_id"]: row for row in artifacts["family_evidence_packs"]}
    for family in artifacts["concept_families"]:
        pack = by_family[family["family_id"]]
        evidence_text = " ".join(
            str(example.get("source_record", {}).get(key, ""))
            for source_rows in pack["sources"].values()
            for example in source_rows
            for key in ("title", "summary")
        )
        for key in family["member_topic_keys"]:
            search_rows.append((
                family["family_id"], key, family["canonical_topic_key"],
                " ".join(family["member_topic_keys"]), evidence_text,
            ))
    fts_mode = "fts5"
    try:
        connection.execute(
            "CREATE VIRTUAL TABLE concept_fts USING fts5(family_id UNINDEXED,topic_key,canonical_topic_key,aliases,evidence)"
        )
        connection.executemany("INSERT INTO concept_fts VALUES (?,?,?,?,?)", search_rows)
    except sqlite3.OperationalError:
        fts_mode = "deterministic_inverted_index"
        connection.execute("CREATE TABLE concept_search_tokens(token TEXT NOT NULL,family_id TEXT NOT NULL,topic_key TEXT NOT NULL,PRIMARY KEY(token,family_id,topic_key))")
        index_rows = set()
        for family_id, topic_key, canonical_key, aliases, evidence_text in search_rows:
            text = " ".join((topic_key, canonical_key, aliases, evidence_text)).casefold()
            for token in normalize_tokens(text, frozenset()):
                index_rows.add((token, family_id, topic_key))
        connection.executemany("INSERT INTO concept_search_tokens VALUES (?,?,?)", sorted(index_rows))
        connection.execute("CREATE INDEX concept_search_token_idx ON concept_search_tokens(token,family_id)")
    connection.execute(
        "INSERT INTO metadata VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
        ("search_index_mode", canonical_json(fts_mode)),
    )
    connection.execute("PRAGMA user_version=1")
    connection.commit()
    connection.execute("VACUUM")
    connection.close()
    return fts_mode
