from __future__ import annotations

import copy

import pytest

from market_analysis.category_opportunity_map import (
    DIRECTION_TIER_BY_STATE,
    _baseline_evidence_classification,
    _build_outputs,
    _category_opportunity_state,
    _lead_watch_counts_by_direction_type,
    _positive_support_shape,
    _qa_positive_support_shape,
    _risk_flags,
)


def _baseline(candidate_state: str, classification: str) -> dict[str, str]:
    return {"direction_id": "baseline", "candidate_state": candidate_state, "classification": classification}


@pytest.mark.parametrize(
    ("members", "baseline", "states", "expected"),
    [
        (0, [_baseline("NO_CLEAR_PATTERN", "EVIDENCE_AGAINST_WHITESPACE")], ["ADVANCE_TO_STAGE_D"], "INSUFFICIENT_EVIDENCE"),
        (10, [_baseline("INSUFFICIENT_EVIDENCE", "INSUFFICIENT_EVIDENCE")], ["ADVANCE_TO_STAGE_D"], "INSUFFICIENT_EVIDENCE"),
        (10, [_baseline("NO_CLEAR_PATTERN", "EVIDENCE_AGAINST_WHITESPACE")], ["ADVANCE_TO_STAGE_D"], "PROMISING"),
        (10, [_baseline("NO_CLEAR_PATTERN", "EVIDENCE_AGAINST_WHITESPACE")], ["WATCH_COVERAGE_LIMITED"], "MIXED_OPPORTUNITY"),
        (10, [
            _baseline("NO_CLEAR_PATTERN", "EVIDENCE_AGAINST_WHITESPACE"),
            _baseline("NO_CLEAR_PATTERN", "EVIDENCE_AGAINST_WHITESPACE"),
        ], ["NO_CLEAR_PATTERN"], "LOW_OPPORTUNITY"),
        (10, [
            _baseline("NO_CLEAR_PATTERN", "EVIDENCE_AGAINST_WHITESPACE"),
            _baseline("NO_CLEAR_PATTERN", "NOT_EVIDENCE_AGAINST_WHITESPACE"),
        ], ["NO_CLEAR_PATTERN"], "NO_CLEAR_OPPORTUNITY"),
        (10, [_baseline("NO_CLEAR_PATTERN", "NOT_EVIDENCE_AGAINST_WHITESPACE")], ["NO_CLEAR_PATTERN"], "NO_CLEAR_OPPORTUNITY"),
    ],
)
def test_category_state_precedence_and_all_boundaries(members, baseline, states, expected):
    state, _ = _category_opportunity_state(members, baseline, states)
    assert state == expected


def test_baseline_negative_classification_ignores_insufficient_patterns_but_not_mixed_patterns():
    assert _baseline_evidence_classification(
        "NO_CLEAR_PATTERN", ["INSUFFICIENT_EVIDENCE", "LOW_DEMAND_PATTERN"],
    )[0] == "EVIDENCE_AGAINST_WHITESPACE"
    assert _baseline_evidence_classification(
        "NO_CLEAR_PATTERN", ["INSUFFICIENT_EVIDENCE", "INSUFFICIENT_EVIDENCE"],
    )[0] == "NOT_EVIDENCE_AGAINST_WHITESPACE"
    assert _baseline_evidence_classification(
        "NO_CLEAR_PATTERN", ["LOW_DEMAND_PATTERN", "MIXED_PATTERN"],
    )[0] == "NOT_EVIDENCE_AGAINST_WHITESPACE"
    assert _baseline_evidence_classification(
        "INSUFFICIENT_EVIDENCE", ["LOW_DEMAND_PATTERN", "LOW_DEMAND_PATTERN"],
    )[0] == "INSUFFICIENT_EVIDENCE"


def test_lexical_absence_is_not_negative_evidence():
    baselines = [
        _baseline("NO_CLEAR_PATTERN", "EVIDENCE_AGAINST_WHITESPACE"),
        _baseline("NO_CLEAR_PATTERN", "NOT_EVIDENCE_AGAINST_WHITESPACE"),
    ]
    assert _category_opportunity_state(10, baselines, ["NO_CLEAR_PATTERN"])[0] == "NO_CLEAR_OPPORTUNITY"


def test_direction_tiers_and_positive_support_shape_are_state_mappings_only():
    assert DIRECTION_TIER_BY_STATE == {
        "ADVANCE_TO_STAGE_D": "LEAD_DIRECTION",
        "WATCH_COVERAGE_LIMITED": "WATCH_DIRECTION",
        "NO_CLEAR_PATTERN": "CONTEXT_DIRECTION",
        "INSUFFICIENT_EVIDENCE": "INSUFFICIENT_DIRECTION",
    }
    directions = [
        {"direction_id": "b-lead", "direction_type": "SUBCATEGORY_BASELINE"},
        {"direction_id": "b-watch", "direction_type": "SUBCATEGORY_BASELINE"},
        {"direction_id": "l-lead", "direction_type": "LEXICAL_SUBNICHE"},
        {"direction_id": "context", "direction_type": "LEXICAL_SUBNICHE"},
    ]
    evaluations = {
        "b-lead": {"candidate_state": "ADVANCE_TO_STAGE_D"},
        "b-watch": {"candidate_state": "WATCH_COVERAGE_LIMITED"},
        "l-lead": {"candidate_state": "ADVANCE_TO_STAGE_D"},
        "context": {"candidate_state": "NO_CLEAR_PATTERN"},
    }
    assert _positive_support_shape(directions, evaluations) == "BASELINE_AND_LEXICAL"
    assert _qa_positive_support_shape(directions, evaluations) == "BASELINE_AND_LEXICAL"
    assert _lead_watch_counts_by_direction_type(directions, evaluations) == {
        "SUBCATEGORY_BASELINE": {"LEAD_DIRECTION": 1, "WATCH_DIRECTION": 1},
        "LEXICAL_SUBNICHE": {"LEAD_DIRECTION": 1, "WATCH_DIRECTION": 0},
    }


def test_risk_flags_are_explicit_and_do_not_change_category_state():
    directions = [
        {"direction_id": "base", "direction_type": "SUBCATEGORY_BASELINE"},
        {"direction_id": "lex", "direction_type": "LEXICAL_SUBNICHE"},
    ]
    evaluations = {
        "base": {"candidate_state": "NO_CLEAR_PATTERN"},
        "lex": {"candidate_state": "ADVANCE_TO_STAGE_D"},
    }
    baseline_evidence = [{"direction_id": "base", "candidate_state": "NO_CLEAR_PATTERN"}]
    state, _ = _category_opportunity_state(3, baseline_evidence, ["NO_CLEAR_PATTERN", "ADVANCE_TO_STAGE_D"])
    flags = _risk_flags(
        "uncategorized", state, {"hangar": 2, "voxel": 0},
        {"hangar": "MATERIAL", "voxel": "VERY_HIGH"}, baseline_evidence,
        directions, evaluations,
    )
    assert state == "PROMISING"
    assert flags == [
        "HANGAR_SUPPLY_INFERENCE_RISK_MATERIAL",
        "LEXICAL_ONLY_LEAD_SUPPORT",
        "NO_VOXEL_CONFIRMED_MEMBERS",
        "UNCATEGORIZED_BUCKET",
        "VOXEL_SUPPLY_INFERENCE_RISK_VERY_HIGH",
    ]
    assert _category_opportunity_state(3, baseline_evidence, ["NO_CLEAR_PATTERN", "ADVANCE_TO_STAGE_D"])[0] == state


def _synthetic_loaded() -> dict:
    taxonomy = [
        {"category_id": "category_a", "category_order": 0, "category_name": "Category A", "definition": "A", "subcategories": []},
        {"category_id": "category_b", "category_order": 1, "category_name": "Category B", "definition": "B", "subcategories": []},
    ]
    specifications = [
        ("a-base-1", "category_a", 0, "SUBCATEGORY_BASELINE", "base_one", "NO_CLEAR_PATTERN", "LOW_DEMAND_PATTERN"),
        ("a-base-2", "category_a", 0, "SUBCATEGORY_BASELINE", "base_two", "NO_CLEAR_PATTERN", "DEMAND_WITH_BROAD_SUPPLY"),
        ("a-lex", "category_a", 0, "LEXICAL_SUBNICHE", "lexical_a", "ADVANCE_TO_STAGE_D", "OBSERVED_SUPPORTED_PATTERN"),
        ("b-base-1", "category_b", 1, "SUBCATEGORY_BASELINE", "base_three", "NO_CLEAR_PATTERN", "MIXED_PATTERN"),
        ("b-base-2", "category_b", 1, "SUBCATEGORY_BASELINE", "base_four", "NO_CLEAR_PATTERN", "MIXED_PATTERN"),
    ]
    directions = []
    evaluations = []
    source_facts = []
    for direction_id, category_id, category_order, direction_type, key, state, hangar_pattern in specifications:
        directions.append({
            "direction_id": direction_id,
            "primary_category_id": category_id,
            "primary_category_order": category_order,
            "direction_type": direction_type,
            "canonical_direction_key": key,
            "baseline_subcategory_id": key if direction_type == "SUBCATEGORY_BASELINE" else None,
        })
        patterns = {"hangar": hangar_pattern, "voxel": "INSUFFICIENT_EVIDENCE"}
        evaluations.append({
            "direction_id": direction_id,
            "primary_category_id": category_id,
            "candidate_state": state,
            "reason_codes": ["FIXTURE"],
            "observed_member_count_by_source": {"hangar": 1, "voxel": 0},
            "source_pattern_state_by_source": patterns,
            "source_supply_inference_risk_by_source": {"hangar": "MATERIAL", "voxel": "VERY_HIGH"},
        })
        for source in ("hangar", "voxel"):
            source_facts.append({
                "direction_id": direction_id,
                "source": source,
                "observed_whitespace_pattern_state": patterns[source],
                "demand_strength": "NONE" if patterns[source] == "INSUFFICIENT_EVIDENCE" else "LOW",
                "observed_supply_band": "UNKNOWN",
                "observed_confirmed_member_count": 0 if source == "voxel" else 1,
                "source_supply_inference_risk": "VERY_HIGH" if source == "voxel" else "MATERIAL",
                "paid_evidence_scope": "SOURCE_AVAILABLE" if source == "voxel" else "NOT_AVAILABLE_FOR_SOURCE",
                "paid_state_counts": {"PAID": 1} if source == "voxel" else None,
                "paid_evidence_observed_count": 1 if source == "voxel" else None,
                "price_band_counts": {">0-5": 1} if source == "voxel" else None,
                "price_available_count": 1 if source == "voxel" else None,
            })
    context = {
        "hangar": {"paid_evidence_scope": "NOT_AVAILABLE_FOR_SOURCE"},
        "voxel": {"paid_evidence_scope": "SOURCE_AVAILABLE"},
    }
    return {
        "taxonomy": taxonomy,
        "directions": directions,
        "evaluations": evaluations,
        "source_facts": source_facts,
        "category_summaries": [
            {"category_id": "category_a", "confirmed_member_count_by_source": {"hangar": 4, "voxel": 0}, "source_coverage_context": context},
            {"category_id": "category_b", "confirmed_member_count_by_source": {"hangar": 3, "voxel": 0}, "source_coverage_context": context},
        ],
        "source_coverage_by_source": {"hangar": {}, "voxel": {}},
        "candidates": [{"direction_id": "a-lex"}],
        "evidence_packs": [{"direction_id": "a-lex", "example_identity_ids": ["a1"]}],
    }


def test_category_outputs_are_isolated_and_candidate_options_are_exact_subset():
    loaded = _synthetic_loaded()
    original = _build_outputs(loaded)
    changed = copy.deepcopy(loaded)
    changed["category_summaries"][1]["confirmed_member_count_by_source"] = {"hangar": 0, "voxel": 0}
    updated = _build_outputs(changed)
    original_a = original["category_opportunity_profiles"][0]
    updated_a = updated["category_opportunity_profiles"][0]
    assert original_a == updated_a
    assert original["category_opportunity_profiles"][1]["category_opportunity_state"] == "NO_CLEAR_OPPORTUNITY"
    assert updated["category_opportunity_profiles"][1]["category_opportunity_state"] == "INSUFFICIENT_EVIDENCE"
    assert [row["direction_id"] for row in original["category_research_options"]] == ["a-lex"]
    assert updated_a["lead_watch_counts_by_direction_type"] == {
        "SUBCATEGORY_BASELINE": {"LEAD_DIRECTION": 0, "WATCH_DIRECTION": 0},
        "LEXICAL_SUBNICHE": {"LEAD_DIRECTION": 1, "WATCH_DIRECTION": 0},
    }


def test_baseline_advance_remains_a_lead_and_is_not_reclassified():
    loaded = _synthetic_loaded()
    loaded["evaluations"][0]["candidate_state"] = "ADVANCE_TO_STAGE_D"
    loaded["candidates"].append({"direction_id": "a-base-1"})
    loaded["evidence_packs"].append({"direction_id": "a-base-1", "example_identity_ids": ["a2"]})
    profile = _build_outputs(loaded)["category_opportunity_profiles"][0]
    assert profile["category_opportunity_state"] == "PROMISING"
    assert profile["positive_support_shape"] == "BASELINE_AND_LEXICAL"
    assert profile["lead_watch_counts_by_direction_type"]["SUBCATEGORY_BASELINE"]["LEAD_DIRECTION"] == 1
    assert profile["lead_watch_counts_by_direction_type"]["LEXICAL_SUBNICHE"]["LEAD_DIRECTION"] == 1
