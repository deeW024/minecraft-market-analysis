import base64
import json
import sqlite3

import pytest

from market_analysis import production_cli
from market_analysis.concept_pairs import (
    EXPECTED_MODEL_ALIAS,
    EXPECTED_MODEL_VERSION,
    PAIR_OUTPUT_VERSION,
    PAIR_QUESTION_SET_VERSION,
    PAIR_STATE_VERSION,
    RESOLUTION_POLICY_VERSION,
    PairAdjudicationRunner,
    apply_resolution_policy_v03,
    build_pair_state,
    load_pair_questions,
)
from market_analysis.jev_triage import canonical_json, sha256_bytes, sha256_file
from market_analysis.production_pipeline import (
    COHERENCE_EDGE_VERSION,
    FAMILY_ID_VERSION,
    ProductionPreflightError,
    build_concept_retrieval_db,
    build_family_artifacts,
    create_preflight_state,
    deterministic_raw_archive,
    jsonl_bytes,
    plan_representative_coherence_edges,
)


def test_authorized_cli_has_pair_runner_and_coherence_state_builder():
    assert production_cli.PairAdjudicationRunner is PairAdjudicationRunner
    assert production_cli.build_pair_state is build_pair_state


class _Provider:
    provider_id = "JEV"
    model_identifier = EXPECTED_MODEL_ALIAS
    model_version = EXPECTED_MODEL_VERSION
    transport_id = "typesafe-systemone-http-v1"
    transport_config = {"base_url": "https://api.typesafe.ai", "timeout_seconds": 30.0}

    def complete(self, *_args, **_kwargs):
        raise AssertionError("test must not make a provider call")


def _topic(key, count, identity=None, title=None):
    identities = [identity] if identity else []
    examples = [] if not identity else [{
        "canonical_identity": identity,
        "source": "modrinth",
        "source_resource_id": identity.split(":", 1)[-1],
        "title": title or key,
        "summary": f"evidence for {key}",
        "downloads_total": str(count),
        "selection_roles": ["representative"],
    }]
    return {
        "topic_key": key,
        "candidate": {"topic_display": key, "candidate_class": "overlap", "source_presence": {"modrinth": count}},
        "topic_source_facts": [{"topic_key": key, "source": "modrinth", "resource_count": count, "downloads_total_p50": count * 100}],
        "evidence_pack": {
            "topic_key": key,
            "candidate_class": "overlap",
            "sources": {"modrinth": {"examples": examples}} if examples else {},
        },
        "accepted_y31": {
            "run_id": "accepted-y31-run",
            "policy_decision": "ADVANCE",
            "state_sha256": "1" * 64,
            "question_set_sha256": "2" * 64,
            "evidence_pack_ref": {"dataset": "YEE-30", "topic_key": key, "retrieval_db_sha256": "3" * 64},
            "canonical_identity_references": identities,
        },
    }


def _pair(pair_id, left, right, reasons=()):
    return {
        "pair_id": pair_id,
        "left_topic_key": left,
        "right_topic_key": right,
        "blocking_reasons": list(reasons),
        "normalized_edit_similarity": 0.9,
        "token_jaccard": 0.8,
        "evidence_overlap_count": 0,
        "shared_canonical_identities": [],
        "sentinel_expectations": [],
    }


def _resolution(decision="MERGE", source="JEV_V02"):
    return {
        "jev_policy_decision_v02": decision,
        "resolution_decision": decision,
        "resolution_source": source,
        "resolution_policy_version": RESOLUTION_POLICY_VERSION,
    }


def _completed_run(pair, normalized=None):
    return {
        "pair_id": pair["pair_id"],
        "left_topic_key": pair["left_topic_key"],
        "right_topic_key": pair["right_topic_key"],
        "pair": pair,
        "status": "completed",
        "normalized": normalized or {
            "policy_decision": "MERGE",
            "typed_answers": {"concept_relation": {"choice": "SAME_CONCEPT", "type": "choice"}},
            "returned_model": EXPECTED_MODEL_VERSION,
            "state_sha256": "a" * 64,
            "question_set_sha256": "b" * 64,
            "request_sha256": "c" * 64,
            "response_sha256": "d" * 64,
        },
        "attempt_count": 1,
        "cache_key": f"cache-{pair['pair_id']}",
        "last_error": None,
    }


def _family_artifacts(topics, pairs, runs, resolutions, edges=(), edge_runs=(), edge_resolutions=None):
    return build_family_artifacts(
        topics,
        pairs,
        runs,
        resolutions,
        list(edges),
        list(edge_runs),
        edge_resolutions or {},
        {"input_hashes": {"yee31": "a" * 64, "yee30": "b" * 64}},
    )


def test_transitive_merge_bridge_requires_separate_representative_coherence_edge():
    topics = {
        "alpha": _topic("alpha", 30),
        "beta": _topic("beta", 20),
        "gamma": _topic("gamma", 10),
    }
    ab, bc = _pair("ab", "alpha", "beta"), _pair("bc", "beta", "gamma")
    pairs = [ab, bc]
    runs = [_completed_run(ab), _completed_run(bc)]
    resolutions = {"ab": _resolution(), "bc": _resolution()}
    planned = plan_representative_coherence_edges(topics, pairs, runs, resolutions, [], {})
    artifacts = _family_artifacts(topics, pairs, runs, resolutions)

    assert len(pairs) == 2  # Base universe is untouched.
    assert len(planned) == 1
    assert planned[0]["edge_type"] == "COHERENCE_EDGE"
    assert planned[0]["left_topic_key"] == "alpha"
    assert planned[0]["right_topic_key"] == "gamma"
    assert planned[0]["pair_id"].startswith("coh_")
    assert artifacts["qa"]["coherence_edge_count"] == 1
    assert artifacts["qa"]["assigned_topic_count"] == 3
    assert all(row["member_count"] == 1 for row in artifacts["concept_families"])
    assert {row["family_status"] for row in artifacts["concept_families"]} == {"PROVISIONAL_REVIEW_SINGLETON"}


def test_coherence_registry_stays_outside_base_pair_table_with_foreign_keys_enabled(tmp_path):
    questions, question_sha = load_pair_questions()
    topics = {"alpha": _topic("alpha", 10), "beta": _topic("beta", 20)}
    base = _pair("base-ab", "alpha", "beta")
    edge = {
        **base,
        "pair_id": "coherence-ab",
        "edge_type": "COHERENCE_EDGE",
        "coherence_edge_version": COHERENCE_EDGE_VERSION,
        "related_base_pair_id": base["pair_id"],
    }
    metadata = {
        "run_id": "production-fixture",
        "work_order": "YEE-37",
        "inference_parameters": {},
    }
    with PairAdjudicationRunner(
        tmp_path / "registry.sqlite", _Provider(), questions, question_sha, metadata
    ) as runner:
        runner.connection.execute("PRAGMA foreign_keys=ON")
        runner.register_candidate_pairs([base])
        runner.register_coherence_edges([edge])
        request, cache_key = runner._request(edge, build_pair_state(edge, topics), None)
        runner._ensure_run(edge, request, cache_key, None)
        runner.connection.commit()
        assert runner.connection.execute("SELECT count(*) FROM candidate_pairs").fetchone()[0] == 1
        assert runner.connection.execute("SELECT count(*) FROM coherence_edges").fetchone()[0] == 1
        assert runner.connection.execute("SELECT count(*) FROM pair_runs WHERE pair_id='coherence-ab'").fetchone()[0] == 1


def test_representative_coherence_merge_completes_component_without_polluting_base_pairs():
    topics = {
        "alpha": _topic("alpha", 30),
        "beta": _topic("beta", 20),
        "gamma": _topic("gamma", 10),
    }
    ab, bc = _pair("ab", "alpha", "beta"), _pair("bc", "beta", "gamma")
    base_pairs = [ab, bc]
    base_runs = [_completed_run(ab), _completed_run(bc)]
    base_resolutions = {"ab": _resolution(), "bc": _resolution()}
    planned = plan_representative_coherence_edges(topics, base_pairs, base_runs, base_resolutions, [], {})
    edge = planned[0]
    edge_run = _completed_run(edge)
    artifacts = _family_artifacts(
        topics,
        base_pairs,
        base_runs,
        base_resolutions,
        planned,
        [edge_run],
        {edge["pair_id"]: _resolution()},
    )

    assert len(base_pairs) == 2
    assert len(artifacts["coherence_edges"]) == 1
    assert artifacts["coherence_edges"][0]["separate_from_base_pair_universe"]
    assert artifacts["concept_families"][0]["member_topic_keys"] == ["alpha", "beta", "gamma"]
    assert artifacts["concept_families"][0]["family_status"] == "ACCEPTED"


def test_direct_representative_merge_accepts_family_with_deterministic_representative_and_id():
    topics = {
        "alpha": _topic("alpha", 10),
        "beta": _topic("beta", 40),
    }
    pair = _pair("ab", "alpha", "beta")
    artifacts = _family_artifacts(topics, [pair], [_completed_run(pair)], {"ab": _resolution()})

    assert len(artifacts["concept_families"]) == 1
    family = artifacts["concept_families"][0]
    assert family["canonical_topic_key"] == "beta"
    assert family["family_id"] == "family_" + sha256_bytes(
        f"{FAMILY_ID_VERSION}\nalpha\nbeta".encode("utf-8")
    )
    assert family["family_status"] == "ACCEPTED"
    assert len(artifacts["concept_members"]) == 2


def test_family_evidence_deduplicates_canonical_identities_without_cross_source_sums():
    topics = {
        "alpha": _topic("alpha", 10, "modrinth:same", "same title"),
        "beta": _topic("beta", 40, "modrinth:same", "same title"),
    }
    pair = _pair("ab", "alpha", "beta")
    artifacts = _family_artifacts(topics, [pair], [_completed_run(pair)], {"ab": _resolution()})
    pack = artifacts["family_evidence_packs"][0]

    assert pack["evidence_occurrence_count_before_deduplication"] == 2
    assert pack["unique_canonical_identity_count"] == 1
    evidence = pack["sources"]["modrinth"]
    assert len(evidence) == 1
    assert evidence[0]["member_topic_keys"] == ["alpha", "beta"]
    assert len(pack["member_source_facts"]) == 2
    assert "sum" not in pack
    assert "do not sum raw downloads" in pack["aggregation_policy"]


def test_keep_separate_edge_does_not_merge_topics_and_review_is_explicit():
    topics = {"alpha": _topic("alpha", 10), "beta": _topic("beta", 20)}
    pair = _pair("ab", "alpha", "beta")
    run = _completed_run(pair)
    artifacts = _family_artifacts(topics, [pair], [run], {"ab": _resolution("KEEP_SEPARATE")})

    assert artifacts["qa"]["family_count"] == 2
    assert artifacts["qa"]["one_family_or_singleton_per_topic"]
    assert artifacts["pair_relations"][0]["native_typed_answers"] == run["normalized"]["typed_answers"]


def test_family_jsonl_csv_and_retrieval_db_replay_are_byte_identical(tmp_path):
    topics = {"alpha": _topic("alpha", 10), "beta": _topic("beta", 20)}
    pair = _pair("ab", "alpha", "beta")
    args = (topics, [pair], [_completed_run(pair)], {"ab": _resolution()})
    first = _family_artifacts(*args)
    replay = _family_artifacts(*args)
    assert jsonl_bytes(first["concept_families"]) == jsonl_bytes(replay["concept_families"])
    assert jsonl_bytes(first["family_evidence_packs"]) == jsonl_bytes(replay["family_evidence_packs"])
    assert jsonl_bytes(first["pair_relations"]) == jsonl_bytes(replay["pair_relations"])
    metadata = {"run_id": "offline-fixture", "policy": RESOLUTION_POLICY_VERSION}
    first_db, second_db = tmp_path / "a.sqlite", tmp_path / "b.sqlite"
    mode_a = build_concept_retrieval_db(first_db, {**first, "metadata": metadata})
    mode_b = build_concept_retrieval_db(second_db, {**replay, "metadata": metadata})
    assert mode_a == mode_b
    assert sha256_file(first_db) == sha256_file(second_db)


def _write_seed_source(tmp_path, pair, topic_map, normalized, replicate_id=None):
    questions, question_sha = load_pair_questions()
    source_db = tmp_path / "source.sqlite"
    metadata = {
        "work_order": "YEE-37",
        "run_id": "accepted-v02-source-run",
        "expected_model_version": EXPECTED_MODEL_VERSION,
        "requested_model_alias": EXPECTED_MODEL_ALIAS,
        "transport_id": _Provider.transport_id,
        "transport_config": _Provider.transport_config,
        "endpoint": "https://api.typesafe.ai/v1/systemone",
        "inference_parameters": {},
        "question_set_sha256": question_sha,
        "question_set_version": PAIR_QUESTION_SET_VERSION,
        "pair_output_version": PAIR_OUTPUT_VERSION,
        "pair_state_version": PAIR_STATE_VERSION,
        "pair_policy_version": "yee-37-pair-policy-v0.2",
        "candidate_pair_count": 2,
    }
    provider = _Provider()
    with PairAdjudicationRunner(source_db, provider, questions, question_sha, metadata) as runner:
        runner.register_candidate_pairs([pair, _pair("other", "beta", "gamma", ["another_reason"])])
        state = build_pair_state(pair, topic_map)
        request, cache_key = runner._request(pair, state, replicate_id)
        runner._ensure_run(pair, request, cache_key, replicate_id)
        response = b'{"model":"jev-1.13.0","answers":{}}'
        runner.connection.execute(
            "UPDATE pair_runs SET status='completed',normalized_json=? WHERE cache_key=?",
            (canonical_json(normalized), cache_key),
        )
        runner.connection.execute(
            "INSERT INTO pair_attempts(cache_key,attempt_no,stage,request_body,request_sha256,wire_request_captured,"
            "raw_response,raw_response_sha256,request_id,usage_json,parsed_at_ns) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (cache_key, 1, "completed", request.body, request.request_sha256, 1, response,
             sha256_bytes(response), "fixture-request", "{}", 1),
        )
        runner.connection.commit()
    attempt = {
        "cache_key": cache_key,
        "attempt_no": 1,
        "replicate_id": replicate_id,
        "request_body_base64": base64.b64encode(request.body).decode("ascii"),
        "raw_response_base64": base64.b64encode(response).decode("ascii"),
    }
    raw_path = tmp_path / "source-raw.jsonl.gz"
    raw_path.write_bytes(deterministic_raw_archive([attempt]))
    outcome = {
        "pair_id": pair["pair_id"],
        "left_topic_key": pair["left_topic_key"],
        "right_topic_key": pair["right_topic_key"],
        "replicate_id": replicate_id,
        "status": "completed",
        "normalized": normalized,
    }
    outcomes_path = tmp_path / "pilot-outcomes.jsonl"
    outcomes_path.write_bytes(jsonl_bytes([outcome]))
    return source_db, raw_path, outcomes_path, outcome, cache_key


def test_fresh_preflight_seeds_only_null_replicate_and_reconciles_completed_pending(tmp_path):
    topics = {"alpha": _topic("alpha", 10), "beta": _topic("beta", 20), "gamma": _topic("gamma", 5)}
    pair = _pair("ab", "alpha", "beta", ["separator_insensitive_alphanumeric_equality"])
    topic_map = topics
    normalized = {"policy_decision": "KEEP_SEPARATE", "typed_answers": {"concept_relation": {"type": "choice", "choice": "SAME_CONCEPT"}}}
    source_db, raw_path, outcome_path, outcome, _cache_key = _write_seed_source(tmp_path, pair, topic_map, normalized)
    derived = {**outcome, **apply_resolution_policy_v03(pair, normalized)}
    result_db = tmp_path / "production.sqlite"
    input_hashes = {"yee31_advance_review_set_sha256": "a" * 64, "yee30_retrieval_db_sha256_before": "b" * 64}
    summary = create_preflight_state(
        result_db,
        [pair, _pair("other", "beta", "gamma", ["another_reason"])],
        topic_map,
        {"ab"},
        [outcome],
        [derived],
        source_db,
        raw_path,
        outcome_path,
        "accepted-v02-source-run",
        sha256_file(source_db),
        sha256_file(raw_path),
        input_hashes,
        "code-commit",
        expected_pair_count=2,
        expected_seed_count=1,
        expected_stability_run_count=None,
    )

    assert summary["base_pair_count"] == 2
    assert summary["completed_seed_count"] == 1
    assert summary["pending_base_pair_count"] == 1
    connection = sqlite3.connect(result_db)
    try:
        assert connection.execute("SELECT count(*) FROM candidate_pairs").fetchone()[0] == 2
        assert connection.execute("SELECT count(*) FROM coherence_edges").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM seed_provenance WHERE replicate_id IS NULL").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM pair_runs WHERE replicate_id IS NOT NULL").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM pair_attempts").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM pair_runs WHERE status='pending'").fetchone()[0] == 1
        saved = connection.execute("SELECT normalized_json FROM pair_runs WHERE status='completed'").fetchone()[0]
        assert canonical_json(json.loads(saved)) == canonical_json(normalized)
    finally:
        connection.close()


def test_preflight_rejects_stability_replicate_as_production_seed(tmp_path):
    topics = {"alpha": _topic("alpha", 10), "beta": _topic("beta", 20)}
    pair = _pair("ab", "alpha", "beta")
    normalized = {"policy_decision": "MERGE", "typed_answers": {}}
    source_db, raw_path, outcome_path, outcome, _cache_key = _write_seed_source(
        tmp_path, pair, topics, normalized, replicate_id="stability-1"
    )
    derived = {**outcome, **apply_resolution_policy_v03(pair, normalized)}
    with pytest.raises(ProductionPreflightError, match="replicate_id=None"):
        create_preflight_state(
            tmp_path / "production.sqlite",
            [pair],
            topics,
            {"ab"},
            [outcome],
            [derived],
            source_db,
            raw_path,
            outcome_path,
            "accepted-v02-source-run",
            sha256_file(source_db),
            sha256_file(raw_path),
            {"yee31": "a" * 64, "yee30": "b" * 64},
            "code-commit",
            expected_pair_count=1,
            expected_seed_count=1,
            expected_stability_run_count=None,
        )
