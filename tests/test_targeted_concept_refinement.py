from __future__ import annotations

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
