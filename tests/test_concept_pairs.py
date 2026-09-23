import json
from collections import Counter

import pytest

import market_analysis.concept_pairs as pairs_module
from market_analysis.concept_pairs_cli import _assert_terminal_outcomes
from market_analysis.concept_pairs import (
    EXPECTED_MODEL_ALIAS,
    EXPECTED_MODEL_VERSION,
    PairAdjudicationRunner,
    build_pair_state,
    build_stability_report,
    deterministic_manual_audit_sample,
    generate_candidate_pairs,
    load_pair_questions,
    normalize_pair_response,
    pair_policy,
    select_stratified_pairs,
)
from market_analysis.jev_triage import (
    ModelVersionMismatchError,
    ProviderResponse,
    RawEvidenceCapture,
    TriageValidationError,
    canonical_json,
    parse_response,
    sha256_bytes,
)


def test_offline_finalize_accepts_terminal_failures_but_rejects_running_rows():
    _assert_terminal_outcomes(
        [{"status": "completed"}, {"status": "failed"}],
        "pilot",
    )
    with pytest.raises(RuntimeError, match="nonterminal pilot outcomes"):
        _assert_terminal_outcomes([{"status": "running"}], "pilot")


def _topic(topic_key, identities=(), candidate_class="overlap", sources=None):
    return {
        "topic_key": topic_key,
        "candidate": {
            "topic_display": topic_key,
            "candidate_class": candidate_class,
            "source_presence": sources or {"modrinth": 10},
            "source_fact_keys": [],
            "demand_gate_sources": ["modrinth"],
            "eligibility_reasons": [],
        },
        "topic_source_facts": [],
        "evidence_pack": {
            "topic_key": topic_key,
            "candidate_class": candidate_class,
            "research_eligible": True,
            "sources": {},
            "topic_schema_version": "yee-30-topic-layer-v0.1",
        },
        "accepted_y31": {
            "run_id": "accepted-run",
            "policy_decision": "ADVANCE",
            "state_sha256": "a" * 64,
            "question_set_sha256": "b" * 64,
            "evidence_pack_ref": {
                "dataset": "YEE-30",
                "topic_key": topic_key,
                "retrieval_db_sha256": "c" * 64,
            },
            "canonical_identity_references": sorted(identities),
        },
    }


def _pair_record(left, right, *, sentinel=None, reasons=None):
    left, right = sorted((left, right))
    return {
        "pair_id": sha256_bytes(f"fixture\n{left}\n{right}".encode()),
        "left_topic_key": left,
        "right_topic_key": right,
        "blocking_reasons": reasons or ["normalized_edit_similarity_ge_0_86"],
        "normalized_edit_similarity": 0.9,
        "token_jaccard": 0.5,
        "fuzzy_neighbor_score": 0.9,
        "evidence_overlap_count": 0,
        "shared_canonical_identities": [],
        "fuzzy_only": True,
        "fuzzy_top8_selected_by": [left, right],
        "sentinel_expectations": [sentinel] if sentinel else [],
        "candidate_classes": ["free_demand_only", "overlap"],
        "source_presence": [["modrinth"], ["modrinth", "voxel"]],
        "input_hashes": {
            "yee31_advance_review_set_sha256": "d" * 64,
            "yee30_retrieval_db_sha256_before": "c" * 64,
        },
        "pair_generation_version": "fixture",
        "pair_normalization_version": "fixture",
    }


def _native_response(relation="SAME_CONCEPT", merge_safe=0.9, model="jev-1.13.0"):
    questions, _ = load_pair_questions()
    answers = {}
    for question_id, question in questions.items():
        kind = question["type"]
        if kind == "choice":
            choices = list(question["criteria"])
            choice = relation if question_id == "concept_relation" else "NOT_APPLICABLE"
            answers[question_id] = {
                "type": "choice",
                "choice": choice,
                "confidence": 0.9,
                "probabilities": {
                    label: (0.9 if label == choice else 0.1 / (len(choices) - 1))
                    for label in choices
                },
            }
        elif kind == "score":
            legend = {str(index): value for index, value in enumerate(question["criteria"])}
            answers[question_id] = {
                "type": "score",
                "score": 4,
                "confidence": 0.85,
                "legend": legend,
                "probabilities": {"0": 0.02, "1": 0.03, "2": 0.05, "3": 0.2, "4": 0.7},
            }
        else:
            answers[question_id] = {
                "type": "noul",
                "noul": merge_safe if question_id == "merge_safe" else 0.8,
            }
    return {"model": model, "answers": answers, "usage": {"input_tokens": 80, "output_tokens": 40}}


class FixtureReasoner:
    provider_id = "JEV"
    model_identifier = EXPECTED_MODEL_ALIAS
    model_version = EXPECTED_MODEL_VERSION
    transport_id = "fixture-typesafe"
    transport_config = {"base_url": "https://api.typesafe.ai"}

    def __init__(self, responses, on_dispatch=None):
        self.responses = list(responses)
        self.requests = []
        self.on_dispatch = on_dispatch

    def complete(self, request, capture=None):
        self.requests.append(request)
        if capture is not None:
            capture.before_dispatch(request.body)
        if self.on_dispatch is not None:
            self.on_dispatch(request)
        response = self.responses.pop(0)
        raw = canonical_json(response).encode("utf-8")
        if capture is not None:
            capture.before_parse(200, raw, f"fixture-{len(self.requests)}")
        return ProviderResponse(raw, f"fixture-{len(self.requests)}")


def _runner(path, reasoner, max_attempts=2):
    questions, question_hash = load_pair_questions()
    return PairAdjudicationRunner(
        path,
        reasoner,
        questions,
        question_hash,
        {
            "input_hashes": {"yee31": "a" * 64, "yee30": "b" * 64},
            "requested_model_alias": EXPECTED_MODEL_ALIAS,
            "expected_model_version": EXPECTED_MODEL_VERSION,
            "inference_parameters": {},
        },
        max_attempts=max_attempts,
        retry_delays=(),
    )


def test_pair_generation_includes_rules_evidence_and_all_available_sentinels(monkeypatch):
    keys = [
        "anti cheat", "anticheat", "archaeology", "archeology",
        "advancement", "advancements", "animation", "animations",
        "armor", "armour", "name tag", "nametag", "one block", "oneblock",
        "village", "villager", "create ore", "create more", "announce",
        "announcer", "armor trim", "illager", "alpha topic", "beta topic",
    ]
    monkeypatch.setattr(pairs_module, "EXPECTED_ADVANCE_TOPICS", len(keys))
    topics = [_topic(key) for key in keys]
    topics[keys.index("alpha topic")]["accepted_y31"]["canonical_identity_references"] = [
        "shared-a", "shared-b",
    ]
    topics[keys.index("beta topic")]["accepted_y31"]["canonical_identity_references"] = [
        "shared-a", "shared-b",
    ]
    hashes = {"yee31_advance_review_set_sha256": "d" * 64, "yee30_retrieval_db_sha256_before": "c" * 64}

    first = generate_candidate_pairs(topics, hashes)
    second = generate_candidate_pairs(list(reversed(topics)), hashes)
    by_names = {(row["left_topic_key"], row["right_topic_key"]): row for row in first}

    assert canonical_json(first) == canonical_json(second)
    assert len(first) == len({row["pair_id"] for row in first})
    assert by_names[("anti cheat", "anticheat")]["blocking_reasons"].count("sentinel_guard") == 1
    assert "whole_phrase_singular_plural_equivalence" in by_names[("advancement", "advancements")]["blocking_reasons"]
    overlap = by_names[("alpha topic", "beta topic")]
    assert overlap["evidence_overlap_count"] == 2
    assert "retained_evidence_identity_overlap_ge_2" in overlap["blocking_reasons"]
    assert len([row for row in first if row["sentinel_expectations"]]) == 12
    assert by_names[("village", "villager")]["sentinel_expectations"][0]["expected_policy"] == "NOT_MERGE"


def test_fuzzy_only_neighbor_cap_is_per_topic_and_rule_pairs_bypass_it(monkeypatch):
    keys = [f"magic item {chr(97 + index)}" for index in range(12)]
    keys += ["armor", "armor trim"]
    monkeypatch.setattr(pairs_module, "EXPECTED_ADVANCE_TOPICS", len(keys))
    topics = [_topic(key) for key in keys]
    pairs = generate_candidate_pairs(topics, {"input": "d" * 64})

    outgoing = Counter()
    final_degree = Counter()
    for row in pairs:
        for topic_key in row["fuzzy_top8_selected_by"]:
            outgoing[topic_key] += 1
        if row["fuzzy_only"]:
            final_degree[row["left_topic_key"]] += 1
            final_degree[row["right_topic_key"]] += 1
    assert all(count <= 8 for count in outgoing.values())
    assert all(count <= 8 for count in final_degree.values())
    armor_pair = next(row for row in pairs if {row["left_topic_key"], row["right_topic_key"]} == {"armor", "armor trim"})
    assert armor_pair["sentinel_expectations"]
    assert not armor_pair["fuzzy_only"]


def test_pair_policy_uses_only_typed_relation_and_merge_safe_value():
    assert pair_policy({"concept_relation": {"choice": "SAME_CONCEPT"}, "merge_safe": {"noul": 0.5}}) == "MERGE"
    assert pair_policy({"concept_relation": {"choice": "SAME_CONCEPT"}, "merge_safe": {"noul": 0.499}}) == "REVIEW"
    assert pair_policy({"concept_relation": {"choice": "INSUFFICIENT"}, "merge_safe": {"noul": 1.0}}) == "REVIEW"
    for relation in ("BROADER_NARROWER", "RELATED_DISTINCT", "UNRELATED"):
        assert pair_policy({"concept_relation": {"choice": relation}, "merge_safe": {"noul": 1.0}}) == "KEEP_SEPARATE"
    with pytest.raises(TriageValidationError):
        pair_policy({"concept_relation": {"choice": "FUZZY_SIMILAR"}, "merge_safe": {"noul": 1.0}})


def test_stratified_pilot_and_stability_force_sentinels_deterministically():
    pairs = [
        _pair_record(
            f"topic-{index:02}",
            f"topic-{index + 1:02}",
            sentinel={"sentinel_id": f"s-{index:02}", "expected_policy": "MERGE"} if index < 12 else None,
            reasons=[f"reason-{index % 3}"],
        )
        for index in range(50)
    ]
    pilot = select_stratified_pairs(list(reversed(pairs)), 45)
    repeated = select_stratified_pairs(pairs, 45)
    stability = select_stratified_pairs(pilot, 40)

    assert [row["pair_id"] for row in pilot] == [row["pair_id"] for row in repeated]
    assert len(pilot) == 45
    assert len(stability) == 40
    assert len([row for row in pilot if row["sentinel_expectations"]]) == 12
    assert {row["pair_id"] for row in stability if row["sentinel_expectations"]} == {
        row["pair_id"] for row in pilot if row["sentinel_expectations"]
    }


def test_pair_state_contains_accepted_evidence_but_not_prior_triage_prose():
    left = _topic("left", ["shared"])
    right = _topic("right", ["shared"])
    left["evidence_pack"]["sources"] = {
        "modrinth": {
            "resource_count": 2,
            "demand_percentile_ge90_count": 1,
            "examples": [{
                "canonical_identity": "shared",
                "source_resource_id": "r-1",
                "title": "Title",
                "summary": "Summary",
                "source_url": "https://example.invalid/never-follow",
            }],
        }
    }
    pair = _pair_record("left", "right", reasons=["retained_evidence_identity_overlap_ge_2"])
    state = build_pair_state(pair, {"left": left, "right": right})
    rendered = canonical_json(state)

    assert "Title" in rendered and "Summary" in rendered
    assert "never-follow" not in rendered
    assert "rationale" not in rendered
    assert "external_research_questions" not in rendered


def test_pair_response_contract_preserves_typed_values_and_policy():
    questions, _ = load_pair_questions()
    envelope = _native_response("SAME_CONCEPT", 0.5)
    raw = canonical_json(envelope).encode()
    parsed = parse_response(raw, questions, EXPECTED_MODEL_VERSION)
    pair = _pair_record("alpha", "beta")
    normalized = normalize_pair_response(
        parsed, pair, "a" * 64, "b" * 64, "c" * 64, sha256_bytes(raw), "d" * 64, None
    )
    assert normalized["returned_model"] == EXPECTED_MODEL_VERSION
    assert normalized["policy_decision"] == "MERGE"
    assert normalized["typed_answers"]["concept_relation"]["choice"] == "SAME_CONCEPT"
    assert normalized["typed_answers"]["evidence_alignment"]["score"] == 4


def test_request_and_response_are_persisted_before_dispatch_and_parse(tmp_path):
    pair = _pair_record("alpha", "beta")
    questions, _ = load_pair_questions()
    sequence = []
    holder = {}

    def check_dispatch(request):
        runner = holder["runner"]
        attempt = runner.connection.execute(
            "SELECT stage,wire_request_captured,request_body FROM pair_attempts WHERE cache_key=?",
            (request.cache_key,),
        ).fetchone()
        assert attempt["stage"] == "calling"
        assert attempt["wire_request_captured"] == 1
        assert bytes(attempt["request_body"]) == request.body
        sequence.append("dispatch")

    reasoner = FixtureReasoner([_native_response()], check_dispatch)
    runner = _runner(tmp_path / "pair.db", reasoner)
    holder["runner"] = runner
    runner.register_candidate_pairs([pair])
    state = {"pair_id": pair["pair_id"], "left": {"topic_key": "alpha"}, "right": {"topic_key": "beta"}}
    try:
        result = runner.run_pair(pair, state)
        attempt = runner.connection.execute(
            "SELECT stage,raw_response,request_body,request_id FROM pair_attempts"
        ).fetchone()
        assert result["status"] == "completed"
        assert attempt["stage"] == "completed"
        assert bytes(attempt["raw_response"]) == canonical_json(_native_response()).encode()
        assert bytes(attempt["request_body"]) == reasoner.requests[0].body
        assert attempt["request_id"] == "fixture-1"
        assert sequence == ["dispatch"]
    finally:
        runner.close()


def test_cache_resume_is_idempotent_and_replicate_identity_is_separate(tmp_path):
    pair = _pair_record("alpha", "beta")
    reasoner = FixtureReasoner([_native_response(), _native_response()])
    runner = _runner(tmp_path / "resume.db", reasoner)
    runner.register_candidate_pairs([pair])
    state = {"pair_id": pair["pair_id"], "left": {"topic_key": "alpha"}, "right": {"topic_key": "beta"}}
    try:
        first = runner.run_pair(pair, state)
        replay = runner.run_pair(pair, state)
        repeated = runner.run_pair(pair, state, replicate_id="stability-1")
    finally:
        runner.close()
    assert first["cache_key"] == replay["cache_key"]
    assert replay["status"] == "completed"
    assert repeated["cache_key"] != first["cache_key"]
    assert len(reasoner.requests) == 2


def test_model_version_mismatch_saves_raw_and_aborts_without_retry_or_next_pair(tmp_path):
    first = _pair_record("alpha", "beta")
    second = _pair_record("gamma", "delta")
    mismatch = _native_response(model="jev-1.14.0")
    reasoner = FixtureReasoner([mismatch, _native_response()])
    runner = _runner(tmp_path / "mismatch.db", reasoner, max_attempts=3)
    runner.register_candidate_pairs([first, second])
    try:
        with pytest.raises(ModelVersionMismatchError, match="expected 'jev-1.13.0', received 'jev-1.14.0'"):
            runner.run_pairs(
                [first, second],
                {
                    "alpha": _topic("alpha"),
                    "beta": _topic("beta"),
                    "gamma": _topic("gamma"),
                    "delta": _topic("delta"),
                },
            )
        saved = runner.connection.execute(
            "SELECT stage,raw_response FROM pair_attempts WHERE cache_key IN "
            "(SELECT cache_key FROM pair_runs WHERE pair_id=?)",
            (first["pair_id"],),
        ).fetchone()
    finally:
        runner.close()
    assert len(reasoner.requests) == 1
    assert saved["stage"] == "model_version_mismatch"
    assert bytes(saved["raw_response"]) == canonical_json(mismatch).encode()


def test_bounded_parse_retry_uses_first_contract_valid_response(tmp_path):
    pair = _pair_record("alpha", "beta")
    invalid = {"model": EXPECTED_MODEL_VERSION, "answers": {}}
    reasoner = FixtureReasoner([invalid, _native_response("RELATED_DISTINCT", 0.9)])
    runner = _runner(tmp_path / "retry.db", reasoner, max_attempts=2)
    runner.register_candidate_pairs([pair])
    state = {"pair_id": pair["pair_id"], "left": {"topic_key": "alpha"}, "right": {"topic_key": "beta"}}
    try:
        result = runner.run_pair(pair, state)
        attempts = runner.raw_attempts()
    finally:
        runner.close()
    assert result["status"] == "completed"
    assert result["normalized"]["policy_decision"] == "KEEP_SEPARATE"
    assert [row["stage"] for row in attempts] == ["parse_error", "completed"]
    assert len(reasoner.requests) == 2


def test_manual_audit_selection_is_deterministic_and_covers_observed_decisions():
    pairs = [_pair_record(f"left-{index:03}", f"right-{index:03}") for index in range(45)]
    outcomes = []
    decisions = ("MERGE", "KEEP_SEPARATE", "REVIEW")
    for index, pair in enumerate(pairs):
        outcomes.append({
            **pair,
            "status": "completed",
            "normalized": {
                "policy_decision": decisions[index % 3],
                "concept_relation": {"choice": "SAME_CONCEPT"},
                "merge_safe": {"noul": 0.8},
            },
        })
    audit = deterministic_manual_audit_sample(pairs, outcomes, 40)
    again = deterministic_manual_audit_sample(list(reversed(pairs)), list(reversed(outcomes)), 40)
    assert [row["pair_id"] for row in audit] == [row["pair_id"] for row in again]
    assert len(audit) == 40
    assert {row["model_policy_decision"] for row in audit} == set(decisions)


def test_stability_report_uses_pair_policy_and_keeps_all_three_replicates():
    pairs = [_pair_record(f"left-{index:03}", f"right-{index:03}") for index in range(40)]
    pairs[0]["sentinel_expectations"] = [{"sentinel_id": "anti cheat / anticheat", "expected_policy": "MERGE"}]
    outcomes = []
    for index, pair in enumerate(pairs):
        for repetition in range(1, 4):
            relation = "SAME_CONCEPT" if not (index == 0 and repetition == 2) else "RELATED_DISTINCT"
            envelope = _native_response(relation, 0.9)
            normalized = {
                "policy_decision": "MERGE" if relation == "SAME_CONCEPT" else "KEEP_SEPARATE",
                "concept_relation": envelope["answers"]["concept_relation"],
                "typed_answers": envelope["answers"],
                "merge_safe": envelope["answers"]["merge_safe"],
                "cache_key": f"{pair['pair_id']}-{repetition}",
            }
            outcomes.append({
                "pair_id": pair["pair_id"],
                "replicate_id": f"stability-{repetition}",
                "cache_key": normalized["cache_key"],
                "status": "completed",
                "normalized": normalized,
            })
    report = build_stability_report(pairs, outcomes)
    assert report["candidate_count"] == 40
    assert report["runs_per_candidate"] == 3
    assert report["complete_candidate_count"] == 40
    assert report["authoritative_policy_exact_agreement_count"] == 39
    assert report["authoritative_policy_exact_agreement_rate"] == pytest.approx(0.975)
    assert report["sentinel_merge_flip_count"] == 1
    assert report["per_question_value_deltas"]["concept_relation"]["adjacent_changed_pairs"] == 2


def test_stability_deltas_compare_only_typed_values():
    pairs = [_pair_record("alpha", "beta")]
    outcomes = []
    for repetition, confidence in enumerate((0.9, 0.8, 0.7), start=1):
        envelope = _native_response("SAME_CONCEPT", 0.9)
        answer = envelope["answers"]["concept_relation"]
        answer["confidence"] = confidence
        answer["probabilities"] = {
            "SAME_CONCEPT": 0.7 + repetition / 100,
            "BROADER_NARROWER": 0.1,
            "RELATED_DISTINCT": 0.1,
            "UNRELATED": 0.05,
            "INSUFFICIENT": 0.05 - repetition / 100,
        }
        outcomes.append({
            "pair_id": pairs[0]["pair_id"],
            "replicate_id": f"stability-{repetition}",
            "status": "completed",
            "normalized": {
                "policy_decision": "MERGE",
                "concept_relation": answer,
                "typed_answers": envelope["answers"],
                "merge_safe": envelope["answers"]["merge_safe"],
                "cache_key": str(repetition),
            },
        })
    report = build_stability_report(pairs, outcomes)
    assert report["per_question_value_deltas"]["concept_relation"] == {
        "adjacent_changed_pairs": 0,
        "adjacent_compared_pairs": 2,
    }
    assert report["per_question_probability_deltas"]["concept_relation"] == {
        "adjacent_changed_pairs": 2,
        "adjacent_compared_pairs": 2,
    }
    assert report["per_question_confidence_deltas"]["concept_relation"] == {
        "adjacent_changed_pairs": 2,
        "adjacent_compared_pairs": 2,
    }
