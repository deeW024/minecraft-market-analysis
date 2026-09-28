from __future__ import annotations

import hashlib

import pytest

from market_analysis import targeted_concept_refinement as refinement


def _case(case_id: str, *, group: str | None = None, product: str = "Jobs Reborn", env: str | None = None,
          cause: str = "UNKNOWN", applicability: str = "CURRENT_OR_RECENT"):
    return {
        "case_id": case_id,
        "concept_id": "yee83-jobs-02-progress-recovery",
        "source_url": f"https://example.org/{case_id}",
        "independence_group_id": group or case_id,
        "environment_key": env or case_id,
        "affected_product": product,
        "failure_class": "JOB_PROGRESS_DURABILITY_LOSS",
        "supports_failure_class": True,
        "root_cause_state": cause,
        "current_applicability_state": applicability,
    }


def test_authorized_cohort_is_exactly_the_two_job_concepts():
    assert refinement.cohort_sha(list(refinement.CONCEPTS)) == refinement.COHORT_SHA256
    assert list(sorted(refinement.CONCEPTS)) == [
        "yee83-jobs-01-economy-consistency",
        "yee83-jobs-02-progress-recovery",
    ]


def test_recurrence_supported_requires_independent_groups_urls_and_environments():
    rows = [
        _case("a", product="Jobs Reborn"),
        _case("b", product="VJobs"),
        _case("c", product="Jobs Plus"),
    ]
    assert refinement.derive_recurrence(rows, "yee83-jobs-02-progress-recovery")["recurrence_state"] == "SUPPORTED"


def test_same_product_cases_are_mixed_not_cross_product_recurrence():
    rows = [_case("a"), _case("b"), _case("c")]
    result = refinement.derive_recurrence(rows, "yee83-jobs-02-progress-recovery")
    assert result["recurrence_state"] == "MIXED"
    assert result["cross_product_status"] == "NOT_ESTABLISHED"
    assert result["independent_case_count"] == 3


def test_copied_or_duplicated_incident_group_cannot_inflate_recurrence():
    rows = [_case("a", group="same-event"), _case("b", group="same-event")]
    with pytest.raises(refinement.RefinementError, match="Duplicate incident"):
        refinement.derive_recurrence(rows, "yee83-jobs-02-progress-recovery")


def test_single_operator_case_is_weak_and_empty_is_insufficient():
    one = refinement.derive_recurrence([_case("a")], "yee83-jobs-02-progress-recovery")
    empty = refinement.derive_recurrence([], "yee83-jobs-02-progress-recovery")
    assert one["recurrence_state"] == "WEAK"
    assert empty["recurrence_state"] == "INSUFFICIENT_EVIDENCE"


def test_historical_case_retains_historical_applicability_instead_of_becoming_current():
    row = _case("historic", applicability="HISTORICAL_ONLY")
    result = refinement.derive_recurrence([row], row["concept_id"])
    assert result["recurrence_state"] == "WEAK"
    assert row["current_applicability_state"] == "HISTORICAL_ONLY"


def test_overall_state_uses_core_dimension_hard_rules():
    dimensions = [{"dimension": name, "state": "MIXED", "evidence_ids": ["e"]} for name in refinement.DIMENSIONS]
    assert refinement.derive_overall(dimensions) == "REFINEMENT_SIGNAL_MIXED"
    dimensions[0]["state"] = "INSUFFICIENT_EVIDENCE"
    assert refinement.derive_overall(dimensions) == "REFINEMENT_INSUFFICIENT_EVIDENCE"
    dimensions[0]["state"] = "WEAK"
    dimensions[4]["state"] = "WEAK"
    assert refinement.derive_overall(dimensions) == "REFINEMENT_SIGNAL_WEAK"


def test_mixed_core_precedes_weak_core_when_both_are_present():
    dimensions = [{"dimension": name, "state": "SUPPORTED", "evidence_ids": ["e"]} for name in refinement.DIMENSIONS]
    dimensions[0]["state"] = "WEAK"
    dimensions[4]["state"] = "MIXED"
    assert refinement.derive_overall(dimensions) == "REFINEMENT_SIGNAL_MIXED"


def test_mixed_without_core_support_does_not_pass_as_mixed():
    dimensions = [{"dimension": name, "state": "SUPPORTED", "evidence_ids": []} for name in refinement.DIMENSIONS]
    dimensions[0]["state"] = "MIXED"
    assert refinement.derive_overall(dimensions) == "REFINEMENT_SIGNAL_WEAK"


def test_overall_requires_exactly_seven_dimensions():
    with pytest.raises(refinement.RefinementError, match="exactly the seven"):
        refinement.derive_overall([{"dimension": "PROBLEM_RECURRENCE", "state": "WEAK"}])


def test_csv_null_is_literal_backslash_n_and_jsonl_is_stable():
    assert refinement._csv_cell(None) == r"\N"
    assert refinement._json({"z": None, "a": "ą"}) == '{"a":"ą","z":null}'


def test_canonical_url_dedupes_tracking_and_fragments():
    assert refinement.canonical_url("HTTPS://EXAMPLE.org/a/?utm_source=x#section") == "https://example.org/a"


def test_full_captured_cohort_has_no_unsupported_drop_or_product_decision_fields():
    path = refinement.Path(__file__).parents[1] / "YEE95_GOAL_ALIGNMENT.md"
    text = path.read_text(encoding="utf-8")
    assert "YEE-95 Goal Alignment" in text
    assert "No Product Spec, prototype, MVP, implementation" in text


def test_wrong_yee83_pin_fails_closed_without_mutating_input(tmp_path, monkeypatch):
    source = tmp_path / "pinned-input.sqlite"
    source.write_bytes(b"accepted read-only input")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    monkeypatch.setattr(refinement, "INPUT_HASHES", {name: digest for name in refinement.INPUT_HASHES})
    paths = {name: source for name in refinement.INPUT_HASHES}
    before = source.read_bytes()
    assert set(refinement.validate_input_hashes(paths).values()) == {digest}
    monkeypatch.setitem(refinement.INPUT_HASHES, "yee83_sqlite", "0" * 64)
    with pytest.raises(refinement.RefinementError, match="input hash mismatch"):
        refinement.validate_input_hashes(paths)
    assert source.read_bytes() == before


def test_wrong_cohort_hash_and_dropped_concept_leakage_fail_closed():
    authorized = sorted(refinement.CONCEPTS)
    refinement._validate_cohort(authorized, ["not-in-cohort"])
    with pytest.raises(refinement.RefinementError, match="authorized two-concept"):
        refinement._validate_cohort([authorized[0], "yee83-jobs-03-authorization-integrity"], [])
    with pytest.raises(refinement.RefinementError, match="dropped YEE-83 concept"):
        refinement._validate_cohort([authorized[0], "yee83-jobs-03-authorization-integrity"], ["yee83-jobs-03-authorization-integrity"])


@pytest.mark.parametrize("concept_id,query", [
    ("yee83-jobs-01-economy-consistency", "Minecraft economy problems"),
    ("yee83-jobs-02-progress-recovery", "Minecraft data loss"),
    ("yee83-jobs-02-progress-recovery", "Jobs plugin is popular on Minecraft"),
])
def test_generic_market_queries_do_not_satisfy_concept_targeting(concept_id, query):
    assert not refinement._query_targets_concept(concept_id, query)
    assert not refinement._qa_query_targets_concept(concept_id, query)


def test_concept_query_guards_accept_the_exact_operational_boundaries():
    assert refinement._query_targets_concept("yee83-jobs-01-economy-consistency", "Jobs Reborn reward payout Vault provider transaction")
    api_query = "VaultAPI EconomyResponse transaction depositPlayer result"
    assert not refinement._qa_query_targets_concept("yee83-jobs-01-economy-consistency", api_query)
    rationale = "Official Vault provider transaction result for Jobs Reborn reward payout consistency."
    assert refinement._query_targets_concept("yee83-jobs-01-economy-consistency", api_query, rationale)
    assert refinement._qa_query_targets_concept("yee83-jobs-01-economy-consistency", api_query, rationale)
    assert refinement._query_targets_concept("yee83-jobs-02-progress-recovery", "Jobs Reborn job progress lost after disconnect restart MySQL")
    assert refinement._qa_query_targets_concept("yee83-jobs-02-progress-recovery", "Jobs Reborn save lifecycle SQLite multi-server progress")


def test_recurrence_followups_are_required_per_failed_primary_purpose():
    queries = [
        {"concept_id": "yee83-jobs-02-progress-recovery", "query_kind": "MANDATORY", "purpose": "OPERATOR_RECURRENCE_A", "execution_order": 1, "new_independent_case_found": False},
        {"concept_id": "yee83-jobs-02-progress-recovery", "query_kind": "MANDATORY", "purpose": "OPERATOR_RECURRENCE_B", "execution_order": 2, "new_independent_case_found": True},
        {"concept_id": "yee83-jobs-02-progress-recovery", "query_kind": "FOLLOWUP", "purpose": "OPERATOR_RECURRENCE_B", "execution_order": 3, "query_text": "different B formulation one"},
        {"concept_id": "yee83-jobs-02-progress-recovery", "query_kind": "FOLLOWUP", "purpose": "OPERATOR_RECURRENCE_B", "execution_order": 4, "query_text": "different B formulation two"},
    ]
    with pytest.raises(refinement.RefinementError, match="same-purpose recurrence follow-ups"):
        refinement._validate_recurrence_followups(queries)
    queries[2]["purpose"] = queries[3]["purpose"] = "OPERATOR_RECURRENCE_A"
    refinement._validate_recurrence_followups(queries)


def test_first_discovery_provenance_keeps_parent_case_and_separate_thread_incident_distinct():
    concept = "yee83-jobs-02-progress-recovery"
    thread_url = "https://example.org/jobs-reset-thread"
    accepted = {
        "sources": [{"source_id": "UPSTREAM_THREAD", "url": thread_url}],
        "evidence": [{"concept_id": concept, "source_id": "UPSTREAM_THREAD", "evidence_type": "HISTORICAL_USER_REPORT", "observation": "SQLite reset issue changed to MySQL"}],
    }
    queries = [
        {"query_id": "Q-A", "concept_id": concept, "query_kind": "MANDATORY", "purpose": "OPERATOR_RECURRENCE_A", "execution_order": 1, "opened_source_ids": ["S-GH"], "new_independent_case_found": True},
        {"query_id": "Q-B", "concept_id": concept, "query_kind": "MANDATORY", "purpose": "OPERATOR_RECURRENCE_B", "execution_order": 2, "opened_source_ids": ["S-THREAD"], "new_independent_case_found": False},
        {"query_id": "Q-B-FU", "concept_id": concept, "query_kind": "FOLLOWUP", "purpose": "OPERATOR_RECURRENCE_B", "execution_order": 3, "opened_source_ids": ["S-THREAD"], "new_independent_case_found": True},
    ]
    cases = [
        {"case_id": "parent-kaspar", "concept_id": concept, "source_id": "S-THREAD", "source_url": thread_url,
         "operator_context": "SQLite created a new user ID after restart", "triggering_event": "restart", "observed_consequence": "duplicate jobs",
         "reported_workaround_or_resolution": "moved to MySQL", "incident_locator": "Kaspar59|sqlite|mysql|2021", "supports_failure_class": True},
        {"case_id": "new-jimmy", "concept_id": concept, "source_id": "S-THREAD", "source_url": thread_url,
         "operator_context": "Jobs disappeared after plugin upgrade", "triggering_event": "reboot after upgrade", "observed_consequence": "jobs reset",
         "reported_workaround_or_resolution": "not stated", "incident_locator": "Jimmy_FPE|upgrade|2018", "supports_failure_class": True},
        {"case_id": "new-gh", "concept_id": concept, "source_id": "S-GH", "source_url": "https://example.org/jobs-issue",
         "operator_context": "current Jobs progress report", "triggering_event": "restart", "observed_consequence": "progress missing",
         "reported_workaround_or_resolution": "unknown", "incident_locator": "operator-a|issue", "supports_failure_class": True},
    ]
    capture = {"queries": queries, "sources": [{"source_id": "S-THREAD", "canonical_url": thread_url}, {"source_id": "S-GH", "canonical_url": "https://example.org/jobs-issue"}], "operator_cases": cases}
    refinement._normalize_case_discovery(capture, accepted)
    by_case = {row["case_id"]: row for row in cases}
    assert by_case["parent-kaspar"]["case_origin"] == "ACCEPTED_UPSTREAM"
    assert by_case["new-jimmy"]["case_origin"] == "TARGETED_RESEARCH"
    assert by_case["new-jimmy"]["discovered_by_query_id"] == "Q-B"
    assert by_case["new-gh"]["discovered_by_query_id"] == "Q-A"
    assert queries[1]["new_independent_case_found"] is True
    assert queries[2]["new_independent_case_found"] is False
    refinement._validate_case_discovery(queries, cases)


def test_same_url_copied_operator_report_cannot_be_counted_as_a_distinct_case():
    copied = [
        {"source_url": "https://example.org/thread", "operator_identity": "same-user", "incident_locator": "same-event", "independence_group_id": "event-a"},
        {"source_url": "https://example.org/thread?utm_source=copy", "operator_identity": "same-user", "incident_locator": "same-event", "independence_group_id": "event-b"},
    ]
    with pytest.raises(refinement.RefinementError, match="distinct identifiable operators/incidents"):
        refinement._validate_same_url_case_independence(copied)


def test_root_cause_conflict_and_configuration_counterevidence_are_preserved():
    concept = "yee83-jobs-02-progress-recovery"
    rows = [
        _case("config", cause="CONFIGURATION_OR_OPERATOR_ERROR"),
        _case("conflict", cause="CONFLICTING"),
    ]
    result = refinement.derive_recurrence(rows, concept)
    assert result["recurrence_state"] == "MIXED"
    assert {row["root_cause_state"] for row in rows} == {"CONFIGURATION_OR_OPERATOR_ERROR", "CONFLICTING"}


def test_no_wedge_and_valid_refined_wedge_are_both_allowed():
    no_wedge = {"wedge_state": "NO_DEFENSIBLE_WEDGE", "refined_wedge": None, "basis": "Evidence did not establish a separate paid gap.", "scope_boundary": "Stay within job progress.", "evidence_ids": ["e1"], "counterevidence_ids": ["e2"]}
    positive = {**no_wedge, "wedge_state": "REFINED_WEDGE", "refined_wedge": "Job-progress integrity diagnostics at restart/disconnect boundaries."}
    assert refinement._validate_wedge_record(no_wedge, {"e1", "e2"}) is None
    assert refinement._validate_wedge_record(positive, {"e1", "e2"}) is None
    with pytest.raises(refinement.RefinementError, match="bounded statement"):
        refinement._validate_wedge_record({**positive, "refined_wedge": None}, {"e1", "e2"})
    with pytest.raises(refinement.RefinementError, match="null refined_wedge"):
        refinement._validate_wedge_record({**no_wedge, "refined_wedge": "hidden forced wedge"}, {"e1", "e2"})


def test_unsupported_differentiation_claim_is_downgraded_and_independently_checked():
    assessment = {"state": "SUPPORTED", "source_claimed_state": "SUPPORTED", "evidence_ids": ["op", "inc"], "counterevidence_ids": ["inc"]}
    evidence = {"op": {"source_id": "operator", "evidence_type": "OPERATOR_CASE"}, "inc": {"source_id": "incumbent", "evidence_type": "INCUMBENT_CAPABILITY"}}
    sources = {"operator": {"canonical_url": "https://example.org/same"}, "incumbent": {"canonical_url": "https://example.org/same/"}}
    normalized = {**assessment, "state": refinement._differentiation_state(assessment, evidence, sources)}
    assert normalized["state"] == "MIXED"
    assert refinement._qa_differentiation_state(normalized, evidence, sources) == "MIXED"
    sources["incumbent"]["canonical_url"] = "https://official.example.org/docs"
    supported = refinement._differentiation_state(assessment, evidence, sources)
    assert supported == "SUPPORTED"


def test_paid_competitor_price_stays_source_native_and_is_not_wtp():
    row = {"evidence_type": "PAID_COMPETITOR_PRECEDENT", "is_direct_wtp": False, "amount": 17.99, "currency": "EUR"}
    refinement._validate_paid_value_observations([row])
    with pytest.raises(refinement.RefinementError, match="currency conversion"):
        refinement._validate_paid_value_observations([{**row, "converted_amount": 19.2, "converted_currency": "USD"}])
    with pytest.raises(refinement.RefinementError, match="cannot be converted to WTP"):
        refinement._validate_paid_value_observations([{**row, "is_direct_wtp": True}])


def test_decision_template_remains_undecided_and_forbidden_fields_are_recursive():
    valid = {"decision_status": "UNDECIDED", "advance_to_product_spec_concept_ids": [], "request_further_refinement_concept_ids": [], "held_concept_ids": [], "dropped_concept_ids": [], "build_none": False, "rationale": None, "decided_at": None}
    refinement._validate_decision_template(valid)
    with pytest.raises(refinement.RefinementError, match="undecided"):
        refinement._validate_decision_template({**valid, "decision_status": "ADVANCE_TO_PRODUCT_SPEC"})
    assert refinement._forbidden_fields({"nested": [{"rank": 1}]}) == {"rank"}


def test_deterministic_exports_and_pinned_input_validation_are_read_only(tmp_path, monkeypatch):
    path = tmp_path / "source.sqlite"
    path.write_bytes(b"immutable canonical input")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    monkeypatch.setattr(refinement, "INPUT_HASHES", {name: digest for name in refinement.INPUT_HASHES})
    paths = {name: path for name in refinement.INPUT_HASHES}
    before = path.read_bytes()
    refinement.validate_input_hashes(paths)
    rows = [{"id": "a", "evidence": ["e2", "e1"], "optional": None}]
    assert refinement._table_jsonl(rows) == refinement._table_jsonl(rows)
    assert refinement._table_csv(rows) == refinement._table_csv(rows)
    assert path.read_bytes() == before
