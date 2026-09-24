import json
from collections import Counter

import pytest

import market_analysis.concept_pairs as pairs_module
from market_analysis.concept_pairs_cli import (
    _acceptance_gate_status,
    _assert_terminal_outcomes,
    _reuse_persisted_run_metadata,
    _v03_outcome,
    _v03_sentinel_results,
)
from market_analysis.concept_pairs import (
    EXPECTED_MODEL_ALIAS,
    EXPECTED_MODEL_VERSION,
    PAIR_MAX_EXAMPLES_PER_TOPIC,
    PAIR_STATE_MAX_BYTES,
    PAIR_STATE_VERSION,
    RESOLUTION_POLICY_VERSION,
    PairAdjudicationRunner,
    apply_resolution_policy_v03,
    build_pair_state,
    build_stability_report,
    deterministic_manual_audit_sample,
    generate_candidate_pairs,
    has_version_qualifier_pair,
    is_retryable_jev_transport_error,
    load_pair_questions,
    normalize_pair_response,
    pair_policy,
    select_stratified_pairs,
)
from market_analysis.jev_triage import (
    JevTransportError,
    InputIntegrityError,
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


def test_supervisor_review_status_keeps_failed_acceptance_gates_explicit():
    assert _acceptance_gate_status(
        {"status": "FAIL"},
        {"gates": {"stability": False}},
    ) == "FAIL"
    assert _acceptance_gate_status(
        {"status": "PASS"},
        {"gates": {"stability": True, "sentinels": True}},
    ) == "PASS"


def test_offline_finalize_reuses_verified_run_identity_across_finalizer_commits(tmp_path):
    persisted_basis = {
        "code_commit": "inference-commit",
        "accepted_input_sha256": "a" * 64,
        "model_version": "jev-1.13.0",
    }
    metadata = {
        **persisted_basis,
        "run_id": sha256_bytes(canonical_json(persisted_basis).encode("utf-8")),
    }
    path = tmp_path / "RUN_METADATA.json"
    path.write_text(canonical_json(metadata) + "\n", encoding="utf-8")

    replayed = _reuse_persisted_run_metadata(
        path,
        {**persisted_basis, "code_commit": "finalizer-commit"},
    )
    assert replayed == metadata
    with pytest.raises(RuntimeError, match="inputs differ"):
        _reuse_persisted_run_metadata(
            path,
            {**persisted_basis, "code_commit": "finalizer-commit", "accepted_input_sha256": "b" * 64},
        )


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
            if question_id == "concept_relation":
                choice = relation
            elif question_id == "merge_disposition":
                choice = (
                    "MERGE" if relation == "SAME_CONCEPT"
                    else "KEEP_SEPARATE" if relation in {"BROADER_NARROWER", "RELATED_DISTINCT", "UNRELATED"}
                    else "REVIEW"
                )
            else:
                choice = "NOT_APPLICABLE"
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


def test_pair_policy_v02_uses_disposition_and_keeps_merge_safe_diagnostic():
    for merge_safe in (0.1, 0.499, 0.5, 0.9):
        answers = {
            "concept_relation": {"choice": "SAME_CONCEPT"},
            "merge_disposition": {"choice": "MERGE"},
            "merge_safe": {"noul": merge_safe},
        }
        assert pair_policy(answers) == "MERGE"
    assert pair_policy({
        "concept_relation": {"choice": "INSUFFICIENT"},
        "merge_disposition": {"choice": "MERGE"},
    }) == "REVIEW"
    for relation in ("BROADER_NARROWER", "RELATED_DISTINCT", "UNRELATED"):
        assert pair_policy({
            "concept_relation": {"choice": relation},
            "merge_disposition": {"choice": "KEEP_SEPARATE"},
        }) == "KEEP_SEPARATE"
    assert pair_policy({
        "concept_relation": {"choice": "SAME_CONCEPT"},
        "merge_disposition": {"choice": "KEEP_SEPARATE"},
    }) == "REVIEW"
    assert pair_policy({
        "concept_relation": {"choice": "RELATED_DISTINCT"},
        "merge_disposition": {"choice": "MERGE"},
    }) == "REVIEW"
    assert pair_policy({
        "concept_relation": {"choice": "UNRELATED"},
        "merge_disposition": {"choice": "REVIEW"},
    }) == "REVIEW"
    with pytest.raises(TriageValidationError):
        pair_policy({
            "concept_relation": {"choice": "FUZZY_SIMILAR"},
            "merge_disposition": {"choice": "MERGE"},
        })


def test_sentinel_policy_fixtures_cover_merge_and_not_merge():
    expected_merge = {
        "concept_relation": {"choice": "SAME_CONCEPT"},
        "merge_disposition": {"choice": "MERGE"},
        "merge_safe": {"noul": 0.2},
    }
    expected_not_merge = {
        "concept_relation": {"choice": "RELATED_DISTINCT"},
        "merge_disposition": {"choice": "KEEP_SEPARATE"},
        "merge_safe": {"noul": 0.99},
    }
    assert pair_policy(expected_merge) == "MERGE"
    assert pair_policy(expected_not_merge) == "KEEP_SEPARATE"


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
                "source": "modrinth",
                "title": "Title",
                "summary": "Summary",
                "selection_roles": ["representative"],
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
    assert "topic_source_facts" not in rendered
    assert "source_resource_id" not in rendered
    assert "source_url" not in rendered
    assert state["pair_state_version"] == PAIR_STATE_VERSION
    assert set(state["left"]) == {
        "topic_key", "topic_display", "candidate_class", "source_presence", "examples", "provenance"
    }


def test_pair_state_v02_selects_shared_roles_source_balanced_and_only_allowlisted_fields():
    left = _topic("left")
    right = _topic("right")
    roles = ("representative", "highest_demand_percentile", "freshest")
    for topic in (left, right):
        topic["candidate"]["source_presence"] = {"modrinth": 20, "voxel": 12, "hangar": 3}
        for source in ("modrinth", "voxel", "hangar"):
            topic["evidence_pack"]["sources"][source] = {
                "examples": [
                    {
                        "source": source,
                        "canonical_identity": f"{source}-{index}",
                        "source_resource_id": f"raw-id-{index}",
                        "title": f"{source} title {index}",
                        "summary": f"{source} summary {index}",
                        "project_type_norm": "plugin",
                        "category_facets_json": '["utility","world"]',
                        "selection_roles": [roles[index % len(roles)]],
                        "downloads_total": "987654321",
                        "price_amount": 4.5,
                        "version_facets_json": '["1.21"]',
                    }
                    for index in range(12)
                ]
            }
    pair = _pair_record("left", "right")
    pair["shared_canonical_identities"] = ["modrinth-0", "voxel-0"]
    state = build_pair_state(pair, {"left": left, "right": right})
    member = state["left"]
    rendered = canonical_json(state)
    assert len(member["examples"]) == PAIR_MAX_EXAMPLES_PER_TOPIC
    assert [row["canonical_identity"] for row in member["examples"][:2]] == [
        "modrinth-0", "voxel-0"
    ]
    assert set(member["source_presence"]) == {"modrinth", "voxel", "hangar"}
    source_counts = Counter(row["source"] for row in member["examples"])
    assert max(source_counts.values()) - min(source_counts.values()) <= 1
    assert set(member["examples"][0]) == {
        "source", "canonical_identity", "title", "summary", "project_type_norm",
        "category_facets", "selection_roles",
    }
    assert member["examples"][0]["category_facets"] == ["utility", "world"]
    assert "987654321" not in rendered
    assert "price_amount" not in rendered and "version_facets" not in rendered
    assert "source_resource_id" not in rendered
    assert len(rendered.encode("utf-8")) <= PAIR_STATE_MAX_BYTES


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
    assert normalized["merge_disposition"]["choice"] == "MERGE"
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
    state = {"pair_state_version": PAIR_STATE_VERSION, "pair_id": pair["pair_id"], "left": {"topic_key": "alpha"}, "right": {"topic_key": "beta"}}
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
    state = {"pair_state_version": PAIR_STATE_VERSION, "pair_id": pair["pair_id"], "left": {"topic_key": "alpha"}, "right": {"topic_key": "beta"}}
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


def test_cache_identity_includes_all_v02_contract_versions(tmp_path, monkeypatch):
    pair = _pair_record("alpha", "beta")
    reasoner = FixtureReasoner([])
    runner = _runner(tmp_path / "versions.db", reasoner)
    state = {
        "pair_state_version": PAIR_STATE_VERSION,
        "pair_id": pair["pair_id"],
        "left": {"topic_key": "alpha"},
        "right": {"topic_key": "beta"},
    }
    try:
        _, baseline = runner._request(pair, state, None)
        for name in (
            "PAIR_STATE_VERSION",
            "PAIR_QUESTION_SET_VERSION",
            "PAIR_POLICY_VERSION",
            "PAIR_OUTPUT_VERSION",
        ):
            original = getattr(pairs_module, name)
            changed = original + "-fixture-change"
            monkeypatch.setattr(pairs_module, name, changed)
            changed_state = dict(state)
            if name == "PAIR_STATE_VERSION":
                changed_state["pair_state_version"] = changed
            _, changed_key = runner._request(pair, changed_state, None)
            assert changed_key != baseline
            monkeypatch.setattr(pairs_module, name, original)
    finally:
        runner.close()


def test_oversized_pair_state_fails_closed_before_provider_dispatch(tmp_path):
    pair = _pair_record("alpha", "beta")
    reasoner = FixtureReasoner([_native_response()])
    runner = _runner(tmp_path / "oversized.db", reasoner)
    runner.register_candidate_pairs([pair])
    oversized = {
        "pair_state_version": PAIR_STATE_VERSION,
        "pair_id": pair["pair_id"],
        "padding": "x" * PAIR_STATE_MAX_BYTES,
    }
    try:
        with pytest.raises(InputIntegrityError, match="16 KiB"):
            runner.run_pair(pair, oversized)
    finally:
        runner.close()
    assert reasoner.requests == []


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


def test_resolution_policy_v03_exact_form_alias_overrides_jev_and_preserves_diagnostics():
    pair = _pair_record(
        "name tag",
        "nametag",
        reasons=["separator_insensitive_alphanumeric_equality"],
    )
    normalized = {
        "policy_decision": "KEEP_SEPARATE",
        "concept_relation": {"choice": "SAME_CONCEPT"},
    }

    result = apply_resolution_policy_v03(pair, normalized)

    assert result == {
        "jev_policy_decision_v02": "KEEP_SEPARATE",
        "resolution_decision": "MERGE",
        "resolution_source": "EXACT_FORM_ALIAS",
        "resolution_policy_version": RESOLUTION_POLICY_VERSION,
    }
    assert normalized["policy_decision"] == "KEEP_SEPARATE"


@pytest.mark.parametrize(
    ("base", "qualified"),
    [
        ("prominence", "prominence II"),
        ("project", "project 3"),
        ("plugin", "plugin 1.2.3"),
        ("plugin", "plugin v2.0"),
        ("plugin", "plugin mk3"),
        ("plugin", "plugin mk 3"),
        ("resource", "resource X"),
        ("Farmer's", "Farmer’s II"),
    ],
)
def test_resolution_policy_v03_version_qualifier_precedes_exact_alias(base, qualified):
    pair = _pair_record(
        base,
        qualified,
        reasons=["separator_insensitive_alphanumeric_equality"],
    )
    normalized = {"policy_decision": "MERGE"}

    assert has_version_qualifier_pair(base, qualified)
    result = apply_resolution_policy_v03(pair, normalized)
    assert result["jev_policy_decision_v02"] == "MERGE"
    assert result["resolution_decision"] == "KEEP_SEPARATE"
    assert result["resolution_source"] == "VERSION_QUALIFIER"


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("prominence", "prominence xi"),
        ("project", "project 2 beta"),
        ("plugin", "plugin mk beta"),
        ("plugin", "plugin v.2"),
    ],
)
def test_version_qualifier_guard_requires_one_explicit_trailing_qualifier(left, right):
    assert not has_version_qualifier_pair(left, right)


def test_resolution_policy_v03_falls_back_to_jev_without_fuzzy_merge():
    pair = _pair_record("alpha plugin", "alpha plugins")
    normalized = {"policy_decision": "REVIEW"}

    result = apply_resolution_policy_v03(pair, normalized)

    assert result["jev_policy_decision_v02"] == "REVIEW"
    assert result["resolution_decision"] == "REVIEW"
    assert result["resolution_source"] == "JEV_V02"


def test_v03_replay_adds_resolution_without_mutating_saved_typed_outcome():
    pair = _pair_record(
        "name tag",
        "nametag",
        reasons=["separator_insensitive_alphanumeric_equality"],
    )
    normalized = {
        "policy_decision": "KEEP_SEPARATE",
        "concept_relation": {"choice": "SAME_CONCEPT", "confidence": 0.51},
        "returned_model": EXPECTED_MODEL_VERSION,
        "response_sha256": "e" * 64,
    }
    source = {
        **pair,
        "status": "completed",
        "replicate_id": "stability-1",
        "normalized": normalized,
    }
    before = canonical_json(source)

    first = _v03_outcome(source)
    second = _v03_outcome(source)

    assert canonical_json(source) == before
    assert first == second
    assert first["normalized"] == normalized
    assert first["jev_policy_decision_v02"] == "KEEP_SEPARATE"
    assert first["resolution_decision"] == "MERGE"
    assert first["resolution_source"] == "EXACT_FORM_ALIAS"


def test_announce_announcer_is_reclassified_as_diagnostic_without_rewriting_v02():
    pair = _pair_record(
        "announce",
        "announcer",
        sentinel={"sentinel_id": "announce / announcer", "expected_policy": "NOT_MERGE"},
    )
    outcome = {
        "pair_id": pair["pair_id"],
        "resolution_decision": "MERGE",
        "jev_policy_decision_v02": "MERGE",
    }

    result = _v03_sentinel_results([pair], [outcome])

    assert pair["sentinel_expectations"][0]["expected_policy"] == "NOT_MERGE"
    assert result == [{
        "pair_id": pair["pair_id"],
        "sentinel_id": "announce / announcer",
        "v03_qa_role": "DIAGNOSTIC_ONLY",
        "historical_v02_expected_policy": "NOT_MERGE",
        "resolution_decisions": ["MERGE"],
        "hard_gate": False,
    }]


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (JevTransportError("connection timeout", connection_timeout_failure=True), True),
        *[
            (JevTransportError("transient HTTP", status_code=status), True)
            for status in (408, 425, 429, 500, 502, 503, 504, 529)
        ],
        *[
            (JevTransportError("permanent HTTP", status_code=status), False)
            for status in (400, 401, 403, 404, 422)
        ],
        (JevTransportError("unclassified transport failure"), False),
        (JevTransportError("evidence persistence failed"), False),
        (TriageValidationError("parse contract failure"), False),
        (RuntimeError("arbitrary exception"), False),
    ],
)
def test_retry_classifier_is_explicitly_allowlisted(error, expected):
    assert is_retryable_jev_transport_error(error) is expected


def test_allowlisted_transient_http_failure_gets_one_bounded_retry(tmp_path):
    pair = _pair_record("alpha", "beta")

    class RetryThenSuccessReasoner(FixtureReasoner):
        def complete(self, request, capture=None):
            self.requests.append(request)
            if capture is not None:
                capture.before_dispatch(request.body)
            if len(self.requests) == 1:
                raw = b'{"error":"temporary"}'
                if capture is not None:
                    capture.before_parse(529, raw, "fixture-transient")
                raise JevTransportError(
                    "temporary provider failure",
                    status_code=529,
                    request_id="fixture-transient",
                    error_body=raw.decode(),
                )
            response = self.responses.pop(0)
            raw = canonical_json(response).encode()
            if capture is not None:
                capture.before_parse(200, raw, f"fixture-{len(self.requests)}")
            return ProviderResponse(raw, f"fixture-{len(self.requests)}")

    reasoner = RetryThenSuccessReasoner([_native_response("RELATED_DISTINCT")])
    runner = _runner(tmp_path / "transient_retry.db", reasoner, max_attempts=2)
    runner.register_candidate_pairs([pair])
    state = {
        "pair_state_version": PAIR_STATE_VERSION,
        "pair_id": pair["pair_id"],
        "left": {"topic_key": "alpha"},
        "right": {"topic_key": "beta"},
    }
    try:
        result = runner.run_pair(pair, state)
        attempts = runner.raw_attempts()
    finally:
        runner.close()

    assert result["status"] == "completed"
    assert result["normalized"]["policy_decision"] == "KEEP_SEPARATE"
    assert [row["stage"] for row in attempts] == ["retryable_transport_error", "completed"]
    assert len(reasoner.requests) == 2


@pytest.mark.parametrize(
    "failure",
    [
        JevTransportError("permanent HTTP failure", status_code=403),
        RuntimeError("arbitrary provider exception"),
    ],
)
def test_nonallowlisted_provider_failure_is_terminal_and_resume_safe(tmp_path, failure):
    pair = _pair_record("alpha", "beta")

    class FailingReasoner(FixtureReasoner):
        def complete(self, request, capture=None):
            self.requests.append(request)
            if capture is not None:
                capture.before_dispatch(request.body)
                if isinstance(failure, JevTransportError):
                    capture.before_parse(403, b'{"error":"forbidden"}', "fixture-forbidden")
            raise failure

    reasoner = FailingReasoner([])
    runner = _runner(tmp_path / "terminal_failure.db", reasoner, max_attempts=3)
    runner.register_candidate_pairs([pair])
    state = {
        "pair_state_version": PAIR_STATE_VERSION,
        "pair_id": pair["pair_id"],
        "left": {"topic_key": "alpha"},
        "right": {"topic_key": "beta"},
    }
    try:
        first = runner.run_pair(pair, state)
        resumed = runner.run_pair(pair, state)
        attempts = runner.raw_attempts()
    finally:
        runner.close()

    assert first["status"] == resumed["status"] == "failed"
    assert [row["stage"] for row in attempts] == ["terminal_nonretryable_error"]
    assert len(reasoner.requests) == 1


def test_parse_contract_failure_is_terminal_and_resume_safe(tmp_path):
    pair = _pair_record("alpha", "beta")
    invalid = {"model": EXPECTED_MODEL_VERSION, "answers": {}}
    reasoner = FixtureReasoner([invalid, _native_response("RELATED_DISTINCT", 0.9)])
    runner = _runner(tmp_path / "retry.db", reasoner, max_attempts=2)
    runner.register_candidate_pairs([pair])
    state = {"pair_state_version": PAIR_STATE_VERSION, "pair_id": pair["pair_id"], "left": {"topic_key": "alpha"}, "right": {"topic_key": "beta"}}
    try:
        result = runner.run_pair(pair, state)
        resumed = runner.run_pair(pair, state)
        attempts = runner.raw_attempts()
    finally:
        runner.close()
    assert result["status"] == resumed["status"] == "failed"
    assert [row["stage"] for row in attempts] == ["parse_error"]
    assert len(reasoner.requests) == 1


def test_http_400_max_tokens_exceeded_is_terminal_raw_preserved_and_idempotent(tmp_path):
    pair = _pair_record("alpha", "beta")
    error_body = canonical_json({"detail": {"error_type": "max_tokens_exceeded"}}).encode()

    class MaxTokensReasoner(FixtureReasoner):
        def complete(self, request, capture=None):
            self.requests.append(request)
            if capture is not None:
                capture.before_dispatch(request.body)
                capture.before_parse(400, error_body, "fixture-max-tokens")
            raise JevTransportError(
                "Jev HTTP request failed",
                status_code=400,
                request_id="fixture-max-tokens",
                error_body=error_body.decode(),
            )

    reasoner = MaxTokensReasoner([])
    runner = _runner(tmp_path / "max_tokens.db", reasoner, max_attempts=3)
    runner.register_candidate_pairs([pair])
    state = {
        "pair_state_version": PAIR_STATE_VERSION,
        "pair_id": pair["pair_id"],
        "left": {"topic_key": "alpha"},
        "right": {"topic_key": "beta"},
    }
    try:
        first = runner.run_pair(pair, state)
        resumed = runner.run_pair(pair, state)
        attempt = runner.connection.execute(
            "SELECT stage,error,raw_response,request_id FROM pair_attempts"
        ).fetchone()
        count = runner.connection.execute("SELECT COUNT(*) FROM pair_attempts").fetchone()[0]
    finally:
        runner.close()
    assert first["status"] == resumed["status"] == "failed"
    assert first["cache_key"] == resumed["cache_key"]
    assert len(reasoner.requests) == count == 1
    assert attempt["stage"] == "terminal_max_tokens_exceeded"
    assert bytes(attempt["raw_response"]) == error_body
    assert "max_tokens_exceeded" in attempt["error"]
    assert attempt["request_id"] == "fixture-max-tokens"


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
                "merge_disposition": {"choice": "MERGE"},
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
                "merge_disposition": envelope["answers"]["merge_disposition"],
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
                "merge_disposition": envelope["answers"]["merge_disposition"],
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
