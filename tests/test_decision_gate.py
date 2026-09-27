from __future__ import annotations

import csv
import hashlib
import json
import socket
import urllib.request
from dataclasses import replace

import pytest

from market_analysis import decision_gate as gate


DIRECTIONS = [
    ("administration", "dir_0480c15eace353f0426f1db5", "auth", "PROMISING", "RESOLVED", "SUFFICIENT"),
    ("communication", "dir_142f5946b84de25a3a07dd76", "cross_platform_communication", "PROMISING", "RESOLVED", "SUFFICIENT"),
    ("communication", "dir_7457adf52d5db6bc11d5973c", "death message", "PROMISING", "RESOLVED", "SUFFICIENT"),
    ("developer_tools", "dir_d36aa111cd7e8f7191d5ac47", "developer_automation", "MIXED_OPPORTUNITY", "RESOLVED", "SUFFICIENT"),
    ("economy", "dir_4f0954dab063d842029902d3", "shops_and_trading", "MIXED_OPPORTUNITY", "RESOLVED", "SUFFICIENT"),
    ("economy", "dir_78e16d4e251e9350b653a2de", "jobs_and_rewards", "MIXED_OPPORTUNITY", "RESOLVED", "SUFFICIENT"),
    ("gameplay", "dir_0ad55a06d421187e8a8f44dd", "progression", "PROMISING", "AMBIGUOUS", "AMBIGUOUS"),
    ("gameplay", "dir_3165035eb0f2780a59880e64", "teleport", "PROMISING", "RESOLVED", "SUFFICIENT"),
    ("gameplay", "dir_617529293c4b381179ea326e", "custom recipes", "PROMISING", "RESOLVED", "SUFFICIENT"),
    ("gameplay", "dir_6416ab7146c319984671650c", "item_frames", "PROMISING", "AMBIGUOUS", "AMBIGUOUS"),
    ("gameplay", "dir_671b1de8435d83ce44203a66", "skip_the_night", "PROMISING", "RESOLVED", "SUFFICIENT"),
    ("gameplay", "dir_8c9b343c9d5bcbf146ca6e67", "entities_and_combat", "PROMISING", "AMBIGUOUS", "AMBIGUOUS"),
    ("gameplay", "dir_a75cae79739ce1d81d996871", "sleep", "PROMISING", "RESOLVED", "SUFFICIENT"),
    ("gameplay", "dir_b0df585042a3d44c0bc1bdf0", "home", "PROMISING", "RESOLVED", "SUFFICIENT"),
    ("gameplay", "dir_c8153fe3dcf8970596e83e04", "homes", "PROMISING", "RESOLVED", "SUFFICIENT"),
    ("protection", "dir_31aa5e6de836e19810ab626c", "abuse_and_spam_controls", "MIXED_OPPORTUNITY", "AMBIGUOUS", "AMBIGUOUS"),
]


def make_snapshot() -> gate.InputSnapshot:
    categories = [
        {
            "category_id": category_id,
            "category_order": order,
            "category_name": category_id.replace("_", " ").title(),
            "known_ambiguity_risk_notes": [f"{category_id} ambiguity context"],
            "taxonomy_version": gate.ACCEPTED_TAXONOMY_VERSION,
        }
        for order, category_id in enumerate(gate.CATEGORY_ORDER)
    ]
    directions_by_id = {row[1]: row for row in DIRECTIONS}
    options = []
    for category_id, direction_id, key, category_state, status, coverage in DIRECTIONS:
        options.append({
            "direction_id": direction_id,
            "category_id": category_id,
            "category_order": gate.CATEGORY_ORDER.index(category_id),
            "category_opportunity_state": category_state,
            "direction_type": "LEXICAL_SUBNICHE",
            "canonical_direction_key": key,
            "candidate_state": "ADVANCE_TO_STAGE_D",
            "direction_tier": "LEAD_DIRECTION",
            "positive_support_shape": "LEXICAL_ONLY",
            "risk_flags": ["VOXEL_SUPPLY_INFERENCE_RISK_VERY_HIGH"],
            "reason_codes": ["TEST_FIXTURE"],
            "source_facts_by_source": {},
            "direction_evidence_pack": {
                "source_facts_side_by_side": {
                    "hangar": {
                        "source": "hangar",
                        "demand_strength": "MEDIUM",
                        "source_native_engagement": {"watcher_count": {"p50": 4, "p90": 8}},
                    },
                    "voxel": {
                        "source": "voxel",
                        "demand_strength": "INSUFFICIENT",
                        "source_native_engagement": {"voxel_review_count": {"p50": None, "p90": None}},
                    },
                }
            },
        })

    claim_cycle = ("FEATURE", "MAINTENANCE", "OPERATOR_PAIN", "POPULARITY_PROXY", "SEMANTIC_IDENTITY")
    evidence = []
    evidence_by_direction: dict[str, list[str]] = {direction_id: [] for _, direction_id, *_ in DIRECTIONS}
    for index in range(135):
        direction_id = DIRECTIONS[index % len(DIRECTIONS)][1]
        category_id = directions_by_id[direction_id][0]
        claim_type = claim_cycle[index % len(claim_cycle)]
        evidence_id = f"ev-{index:03d}"
        source_id = f"src-{index % 53:03d}"
        evidence_by_direction[direction_id].append(evidence_id)
        evidence.append({
            "evidence_id": evidence_id,
            "direction_id": direction_id,
            "category_id": category_id,
            "source_id": source_id,
            "claim_type": claim_type,
            "currency": None,
            "numeric_value": 100 + index if claim_type == "POPULARITY_PROXY" else None,
            "numeric_unit": "fixture-native counter" if claim_type == "POPULARITY_PROXY" else None,
            "entity_name": "Fixture plugin" if claim_type == "POPULARITY_PROXY" else None,
            "feature_theme": f"feature_{index % 4}" if claim_type == "FEATURE" else None,
            "observation": f"Accepted fixture observation {index}?",
            "optional_excerpt": None,
            "notes": "",
            "retrieved_at": "2026-09-27T14:05:19Z",
        })
    evidence_by_id = {row["evidence_id"]: row for row in evidence}

    sources = []
    for index in range(53):
        source_type = (
            "COMMUNITY" if index % 7 == 0 else
            "MARKETPLACE_LISTING" if index % 5 == 0 else
            "PRIMARY_PRODUCT"
        )
        sources.append({
            "source_id": f"src-{index:03d}",
            "canonical_url": f"https://domain-{index % 13}.example/item-{index}",
            "source_domain": f"domain-{index % 13}.example",
            "source_type": source_type,
            "access_status": "OPENED",
            "source_title": f"Fixture source {index}",
            "retrieved_at": "2026-09-27T14:05:19Z",
        })
    source_by_id = {row["source_id"]: row for row in sources}

    queries = []
    query_sources_by_direction: dict[str, set[str]] = {}
    for index in range(71):
        direction_id = DIRECTIONS[index % len(DIRECTIONS)][1]
        evidence_ids = evidence_by_direction[direction_id]
        evidence_id = evidence_ids[index % len(evidence_ids)]
        source_id = evidence_by_id[evidence_id]["source_id"]
        query_sources_by_direction.setdefault(direction_id, set()).add(source_id)
        queries.append({
            "query_id": f"q-{index:03d}",
            "direction_id": direction_id,
            "category_id": directions_by_id[direction_id][0],
            "purpose": ("semantic resolution", "competitor discovery", "operator pain-point discovery")[index % 3],
            "query_text": f"fixture question {index}?",
            "query_sequence": index + 1,
            "issued_at": "2026-09-27T14:05:19Z",
            "issued_at_basis": "capture_retrieved_at",
            "result_action": "OPENED",
            "result_source_ids": [source_id],
            "result_evidence_ids": [evidence_id],
            "lifecycle_evidence_ids": [],
        })

    resolved_ids = [direction_id for _, direction_id, _, _, status, _ in DIRECTIONS if status == "RESOLVED"]
    entities = []
    for index in range(33):
        relation_type = "DIRECT" if index < 24 else "ADJACENT" if index < 31 else "SUBSTITUTE"
        direction_id = resolved_ids[index % len(resolved_ids)]
        evidence_ids = evidence_by_direction[direction_id]
        evidence_id = evidence_ids[index % len(evidence_ids)]
        lifecycle_ids = [
            item for item in evidence_ids
            if evidence_by_id[item]["claim_type"] == "MAINTENANCE"
        ][:1]
        entities.append({
            "competitor_id": f"cmp-{index:03d}",
            "direction_id": direction_id,
            "category_id": directions_by_id[direction_id][0],
            "entity_name": f"Fixture entity {index}",
            "relation_type": relation_type,
            "product_type": "Minecraft plugin",
            "platform_or_ecosystem": "Paper",
            "canonical_url": f"https://competitor-{index}.example/",
            "feature_summary": f"Source-backed feature {index}",
            "maintenance_status": "CURRENT_LISTING_VISIBLE",
            "price_amount": None,
            "currency": None,
            "pricing_model": None,
            "evidence_ids": [evidence_id],
            "lifecycle_evidence_ids": lifecycle_ids,
            "normalization_note": None,
            "plugin_scope_status": "PLUGIN_PRODUCT_CONFIRMED",
        })

    home_id = "dir_b0df585042a3d44c0bc1bdf0"
    homes_id = "dir_c8153fe3dcf8970596e83e04"
    skip_id = "dir_671b1de8435d83ce44203a66"
    sleep_id = "dir_a75cae79739ce1d81d996871"
    relations = [
        {
            "relation_id": "rel-home-homes",
            "direction_id": home_id,
            "related_direction_id": homes_id,
            "category_id": "gameplay",
            "relation_type": "SUBSTANTIAL_OVERLAP",
            "rationale": "Frozen fixture rationale for home/home-count overlap.",
            "evidence_ids": [evidence_by_direction[home_id][0], evidence_by_direction[homes_id][0]],
        },
        {
            "relation_id": "rel-skip-sleep",
            "direction_id": skip_id,
            "related_direction_id": sleep_id,
            "category_id": "gameplay",
            "relation_type": "SUBSTANTIAL_OVERLAP",
            "rationale": "Frozen fixture rationale for night-skip/sleep overlap.",
            "evidence_ids": [evidence_by_direction[skip_id][0], evidence_by_direction[sleep_id][0]],
        },
    ]
    packs = []
    for category_id, direction_id, key, category_state, status, coverage in DIRECTIONS:
        direction_evidence = [row for row in evidence if row["direction_id"] == direction_id]
        direction_queries = [row for row in queries if row["direction_id"] == direction_id]
        direction_entities = [row for row in entities if row["direction_id"] == direction_id]
        relation_ids = sorted(
            row["relation_id"] for row in relations
            if direction_id in {row["direction_id"], row["related_direction_id"]}
        )
        source_ids = sorted(
            {row["source_id"] for row in direction_evidence}
            | {source_id for query in direction_queries for source_id in query["result_source_ids"]}
        )
        domains = {
            source_by_id[row["source_id"]]["source_domain"]
            for row in direction_evidence
        }
        feature_themes = sorted({
            row["feature_theme"] for row in direction_evidence
            if row["claim_type"] == "FEATURE"
        })
        direct_ids = sorted(row["competitor_id"] for row in direction_entities if row["relation_type"] == "DIRECT")
        substitute_ids = sorted(row["competitor_id"] for row in direction_entities if row["relation_type"] == "SUBSTITUTE")
        packs.append({
            "direction_id": direction_id,
            "category_id": category_id,
            "canonical_direction_key": key,
            "category_opportunity_state": category_state,
            "direction_type": "LEXICAL_SUBNICHE",
            "candidate_state": "ADVANCE_TO_STAGE_D",
            "direction_tier": "LEAD_DIRECTION",
            "positive_support_shape": "LEXICAL_ONLY",
            "risk_flags": ["VOXEL_SUPPLY_INFERENCE_RISK_VERY_HIGH"],
            "reason_codes": ["TEST_FIXTURE"],
            "research_status": status,
            "research_coverage_status": coverage,
            "resolved_market_job": None if status == "AMBIGUOUS" else f"Fixture job for {key}",
            "market_job_summary": f"Fixture summary for {key}",
            "direct_competitor_count": len(direct_ids),
            "direct_competitor_ids": direct_ids,
            "substitute_competitor_ids": substitute_ids,
            "feature_themes": feature_themes,
            "pricing_observations_by_currency": {},
            "maintenance_summary": f"Fixture maintenance summary for {key}",
            "popularity_proxy_summary": [
                row for row in direction_evidence if row["claim_type"] == "POPULARITY_PROXY"
            ],
            "operator_pain_observations": [
                row for row in direction_evidence if row["claim_type"] == "OPERATOR_PAIN"
            ],
            "differentiation_hypotheses": [],
            "semantic_relation_ids": relation_ids,
            "evidence_ids": sorted(row["evidence_id"] for row in direction_evidence),
            "source_ids": source_ids,
            "evidence_count": len(direction_evidence),
            "primary_or_marketplace_evidence_count": sum(
                source_by_id[row["source_id"]]["source_type"] in {
                    "PRIMARY_PRODUCT", "PRIMARY_DOCS", "PRIMARY_REPOSITORY", "MARKETPLACE_LISTING", "PRIMARY_SUPPORT",
                }
                for row in direction_evidence
            ),
            "community_evidence_count": sum(
                source_by_id[row["source_id"]]["source_type"] == "COMMUNITY"
                for row in direction_evidence
            ),
            "distinct_domain_count": len(domains),
            "query_count": len(direction_queries),
            "research_notes": "Fixture notes; no prevalence or commercial conclusion.",
        })

    coverage_rows = []
    for order, category_id in enumerate(gate.CATEGORY_ORDER):
        option_rows = [row for row in options if row["category_id"] == category_id]
        coverage_rows.append({
            "category_id": category_id,
            "category_name": category_id.replace("_", " ").title(),
            "category_order": order,
            "category_research_status": (
                "RESEARCHED_OPTIONS" if option_rows else
                "INSUFFICIENT_STAGE_D_EVIDENCE" if category_id == "server_utilities" else
                "NO_STAGE_D_RESEARCH_OPTIONS"
            ),
            "option_count": len(option_rows),
            "option_ids": sorted(row["direction_id"] for row in option_rows),
            "research_status_counts": {},
            "coverage_status_counts": {},
            "coverage_notes": [f"Fixture coverage context for {category_id}."],
            "risk_notes": [],
            "evidence_count": 0,
            "query_count": 0,
            "source_ids": [],
            "competitor_ids": [],
            "direct_competitor_ids": [],
            "semantic_relation_ids": [],
        })

    metadata = {
        "schema_version": gate.ACCEPTED_YEE77_SCHEMA,
        "execution_code_commit": gate.ACCEPTED_YEE77_CODE_COMMIT,
        "research_capture_sha256": gate.ACCEPTED_CAPTURE_SHA256,
        "input_sha256_before": gate.ACCEPTED_YEE76_SHA256,
        "input_sha256_after": gate.ACCEPTED_YEE76_SHA256,
        "input_run_id": gate.ACCEPTED_YEE76_RUN_ID,
        "input_code_commit": gate.ACCEPTED_YEE76_CODE_COMMIT,
        "input_merge_commit": gate.ACCEPTED_YEE76_MERGE_COMMIT,
        "input_schema_version": gate.ACCEPTED_YEE76_SCHEMA,
        "input_taxonomy_version": gate.ACCEPTED_TAXONOMY_VERSION,
        "input_taxonomy_sha256": gate.ACCEPTED_TAXONOMY_SHA256,
        "input_read_mode": "read-only",
        "input_work_order": "YEE-76",
        "input_option_count": "16",
        "input_category_profile_count": "11",
        "input_direction_tier_count": "47",
        "direction_ids_sha256": gate.ACCEPTED_DIRECTION_IDS_SHA256,
    }
    row_counts = dict(gate.ACCEPTED_INPUT_COUNTS)
    return gate.InputSnapshot(
        input_sha256=gate.ACCEPTED_YEE77_SHA256,
        categories=categories,
        options=options,
        packs=packs,
        category_coverage=coverage_rows,
        sources=sources,
        queries=queries,
        evidence=evidence,
        entities=entities,
        relations=relations,
        metadata=metadata,
        provenance={"input_read_only": "true"},
        row_counts=row_counts,
        sqlite_integrity_ok=True,
        sqlite_foreign_keys_ok=True,
        query_only_enabled=True,
    )


def test_accepted_direction_identity_hash_and_counts_are_frozen():
    snapshot = make_snapshot()
    assert gate._direction_id_sha256(snapshot.options) == gate.ACCEPTED_DIRECTION_IDS_SHA256
    assert gate._input_gate_checks(snapshot).values()
    assert len({row["direction_id"] for row in snapshot.options}) == 16


@pytest.mark.parametrize(
    ("status", "coverage", "expected"),
    [
        ("RESOLVED", "SUFFICIENT", "READY_FOR_SUPERVISOR_DECISION"),
        ("AMBIGUOUS", "AMBIGUOUS", "REQUIRES_SCOPE_REFINEMENT"),
        ("UNRESOLVED", "PARTIAL", "REQUIRES_MORE_EVIDENCE"),
        ("RESOLVED", "PARTIAL", "REQUIRES_MORE_EVIDENCE"),
    ],
)
def test_readiness_boundaries(status, coverage, expected):
    assert gate._readiness(status, coverage) == expected


def test_ambiguous_source_rows_never_become_ready():
    snapshot = make_snapshot()
    rows = gate._build_decision_rows(snapshot)
    ambiguous = [row for row in rows["cards"] if row["research_status"] == "AMBIGUOUS"]
    assert len(ambiguous) == 4
    assert all(row["decision_readiness"] == "REQUIRES_SCOPE_REFINEMENT" for row in ambiguous)


@pytest.mark.parametrize(
    ("field", "value", "failed_check"),
    [
        ("input_sha256", "0" * 64, "accepted_input_sha256"),
        ("metadata", {"schema_version": "wrong"}, "accepted_yee77_schema_and_execution_commit"),
        ("metadata", {"input_run_id": "wrong"}, "accepted_capture_and_upstream_pins"),
        ("row_counts", {**gate.ACCEPTED_INPUT_COUNTS, "frozen_option_snapshot": 15}, "accepted_row_counts"),
    ],
)
def test_input_hash_schema_and_count_mismatch_fail_closed(field, value, failed_check):
    snapshot = make_snapshot()
    bad_snapshot = replace(snapshot, **{field: value})
    checks = gate._input_gate_checks(bad_snapshot)
    assert checks[failed_check] is False
    with pytest.raises(gate.DecisionGateError, match="BLOCKED_INPUT_MISMATCH"):
        gate._validate_input_snapshot(bad_snapshot)


def test_wrong_input_file_hash_fails_before_database_read(tmp_path):
    source = tmp_path / "not-the-accepted-input.sqlite"
    source.write_bytes(b"not the accepted database")
    with pytest.raises(gate.DecisionGateError, match="SHA-256"):
        gate._read_snapshot(source)


def test_zero_option_categories_stay_visible_with_explicit_null_state():
    rows = gate._build_decision_rows(make_snapshot())
    empty = [row for row in rows["categories"] if row["option_count"] == 0]
    assert len(rows["categories"]) == 11
    assert len(empty) == 5
    assert all(row["category_opportunity_state"] is None for row in empty)
    assert all(
        row["category_opportunity_state_basis"] == "NOT_PRESENT_IN_YEE77_FOR_ZERO_OPTION_CATEGORY"
        for row in empty
    )
    assert all(row["evidence_caveats"]["no_options_means_no_stage_e_option_not_no_market"] for row in empty)


def test_both_overlap_relations_are_preserved_without_merging():
    snapshot = make_snapshot()
    groups = gate._build_overlap_groups(snapshot)
    assert len(groups) == 2
    assert {frozenset(row["member_direction_keys"]) for row in groups} == gate.EXPECTED_OVERLAP_KEY_PAIRS
    assert all(row["relation_type"] == "SUBSTANTIAL_OVERLAP" for row in groups)
    rows = gate._build_decision_rows(snapshot)
    assert len(rows["cards"]) == 16
    members = {
        direction_id
        for group in rows["overlap_groups"]
        for direction_id in group["member_direction_ids"]
    }
    assert len(members) == 4
    assert all(rows["cards"][index]["direction_id"] for index in range(16))
    assert all(group["double_counting_note"] for group in groups)


def test_duplicate_overlap_relation_is_rejected():
    snapshot = make_snapshot()
    duplicate = {**snapshot.relations[0], "relation_id": "rel-duplicate"}
    invalid = replace(snapshot, relations=[*snapshot.relations, duplicate])
    with pytest.raises(gate.DecisionGateError, match="Duplicate or self-referential"):
        gate._build_overlap_groups(invalid)


def test_question_catalog_and_gap_question_mapping_are_deterministic():
    snapshot = make_snapshot()
    rows = gate._build_decision_rows(snapshot)
    assert [row["question_id"] for row in rows["question_catalog"]] == [
        "Q_PAIN_PREVALENCE", "Q_BUYER_SEGMENT", "Q_WILLINGNESS_TO_PAY",
        "Q_PAID_ALTERNATIVES", "Q_DIFFERENTIATION", "Q_PURCHASE_TRIGGER",
        "Q_SUPPORT_BURDEN", "Q_CHANNEL_FIT",
    ]
    cards = {row["canonical_direction_key"]: row for row in rows["cards"]}
    ambiguous = cards["progression"]
    assert "SCOPE_REFINEMENT_REQUIRED" in ambiguous["validation_gap_codes"]
    assert "Q_BUYER_SEGMENT" in ambiguous["validation_question_ids"]
    assert "Q_CHANNEL_FIT" in ambiguous["validation_question_ids"]
    mapped = [row for row in rows["validation_questions"] if row["direction_id"] == ambiguous["direction_id"]]
    assert all(row["question_status"] == "FUTURE_QUESTION_ONLY_NOT_ANSWERED" for row in mapped)
    assert all(row["question_text"].endswith("?") for row in rows["question_catalog"])


def test_pricing_missing_is_not_free_and_does_not_emit_zero():
    context = gate._pricing_context(
        {"pricing_observations_by_currency": {}},
        [],
        [{"competitor_id": "cmp-x", "price_amount": None, "pricing_model": None, "evidence_ids": []}],
    )
    assert context["evidence_state"] == "NOT_OBSERVED"
    assert context["pricing_observations_by_currency"] == {}
    assert context["source_backed_pricing_entities"] == []
    assert "not mean free or zero" in context["not_observed_semantics"]
    assert all(value != 0 for value in context.values() if isinstance(value, (int, float)))


def test_source_native_counters_and_currency_observations_stay_separate():
    price_rows = {
        "USD": [{"amount": 12, "currency": "USD"}],
        "EUR": [{"amount": 9, "currency": "EUR"}],
    }
    source_context = gate._source_fact_dimensions(make_snapshot().options[0])
    assert set(source_context) == {"hangar", "voxel"}
    assert source_context["hangar"]["source_native_engagement"] != source_context["voxel"]["source_native_engagement"]
    context = gate._pricing_context(
        {"pricing_observations_by_currency": price_rows},
        [],
        [],
    )
    assert context["evidence_state"] == "OBSERVED"
    assert context["pricing_observations_by_currency"] == price_rows
    assert set(context["pricing_observations_by_currency"]) == {"USD", "EUR"}


def test_validation_gap_rules_include_overlap_scope_and_source_coverage():
    snapshot = make_snapshot()
    rows = gate._build_decision_rows(snapshot)
    cards = {row["canonical_direction_key"]: row for row in rows["cards"]}
    assert "OVERLAP_REQUIRES_DECISION_CONTEXT" in cards["home"]["validation_gap_codes"]
    assert "SCOPE_REFINEMENT_REQUIRED" in cards["progression"]["validation_gap_codes"]
    assert "SOURCE_COVERAGE_RISK_PRESENT" in cards["auth"]["validation_gap_codes"]
    assert "PRICING_EVIDENCE_NOT_OBSERVED" in cards["auth"]["validation_gap_codes"]
    assert gate._qa_expected_gap_codes(
        cards["auth"],
        set(),
        ["VOXEL_SUPPLY_INFERENCE_RISK_VERY_HIGH"],
        next(row for row in snapshot.packs if row["direction_id"] == cards["auth"]["direction_id"]),
        [row for row in snapshot.evidence if row["direction_id"] == cards["auth"]["direction_id"]],
        [],
    ) == cards["auth"]["validation_gap_codes"]


def test_empty_decision_template_allows_build_none_and_no_default_selection():
    template = gate._template()
    assert gate._template_is_empty_and_complete(template, {row[1] for row in DIRECTIONS})
    assert template["decision_status"] == "UNDECIDED"
    assert template["selected_for_deep_commercial_validation_direction_ids"] == []
    assert template["requested_scope_refinement_direction_ids"] == []
    assert template["held_direction_ids"] == []
    assert template["dropped_direction_ids"] == []
    assert template["build_none_selected"] is False
    assert any("build-none" in instruction for instruction in template["instructions"])


def test_decision_rows_contain_no_rank_score_winner_shortlist_or_selection_fields():
    rows = gate._build_decision_rows(make_snapshot())
    forbidden = {
        "rank", "ranking", "score", "opportunity_score", "winner", "recommendation",
        "recommended_direction_id", "shortlist", "preferred_direction", "default_selected",
        "auto_selected", "preselected_direction_ids",
    }
    keys = gate._recursive_keys(rows)
    assert keys.isdisjoint(forbidden)
    assert len(rows["cards"]) == 16


def test_all_card_provenance_refs_are_from_accepted_snapshot():
    snapshot = make_snapshot()
    rows = gate._build_decision_rows(snapshot)
    source_ids = {row["source_id"] for row in snapshot.sources}
    evidence_ids = {row["evidence_id"] for row in snapshot.evidence}
    entity_ids = {row["competitor_id"] for row in snapshot.entities}
    relation_ids = {row["relation_id"] for row in snapshot.relations}
    for card in rows["cards"]:
        assert set(card["source_ids"]) <= source_ids
        assert set(card["evidence_ids"]) <= evidence_ids
        assert set(card["competitor_ids"]) <= entity_ids
        assert set(card["semantic_relation_ids"]) <= relation_ids


def test_production_path_has_no_network_dependency(monkeypatch, tmp_path):
    def forbidden(*args, **kwargs):
        raise AssertionError("unexpected network attempt")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(urllib.request, "urlopen", forbidden)
    snapshot = make_snapshot()
    rows = gate._build_decision_rows(snapshot)
    core1 = tmp_path / "core-a"
    core2 = tmp_path / "core-b"
    metadata = gate._run_metadata(snapshot, "a" * 40, gate._source_field_summary(rows["cards"]))
    provenance = gate._input_provenance(snapshot)
    gate._write_core(core1, snapshot, rows, metadata, provenance)
    gate._write_core(core2, snapshot, rows, metadata, provenance)
    assert gate._export_reconciliation(core1, rows)
    assert all((core1 / name).read_bytes() == (core2 / name).read_bytes() for name in gate.CORE_ARTIFACTS)


def test_jsonl_csv_sqlite_reconciliation_and_null_encoding(tmp_path):
    snapshot = make_snapshot()
    rows = gate._build_decision_rows(snapshot)
    metadata = gate._run_metadata(snapshot, "a" * 40, gate._source_field_summary(rows["cards"]))
    provenance = gate._input_provenance(snapshot)
    output = tmp_path / "bundle"
    gate._write_core(output, snapshot, rows, metadata, provenance)
    assert gate._export_reconciliation(output, rows)
    assert gate._sqlite_output_integrity(output / "decision_gate.sqlite")
    json_rows = gate._read_jsonl(output / "category_decision_context.jsonl")
    assert any(row["category_opportunity_state"] is None for row in json_rows)
    with (output / "category_decision_context.csv").open(encoding="utf-8", newline="") as stream:
        csv_rows = list(csv.DictReader(stream))
    assert any(row["category_opportunity_state"] == r"\N" for row in csv_rows)
    with (output / "decision_gate.sqlite").open("rb") as stream:
        assert stream.read(16) == b"SQLite format 3\x00"


def test_full_bundle_replay_immutability_manifest_and_qa_pass(monkeypatch, tmp_path):
    snapshot = make_snapshot()
    input_path = tmp_path / "accepted-input-copy.sqlite"
    input_path.write_bytes(b"immutable frozen input fixture")
    pinned_sha = hashlib.sha256(input_path.read_bytes()).hexdigest()
    snapshot = replace(snapshot, input_sha256=pinned_sha)
    monkeypatch.setattr(gate, "ACCEPTED_YEE77_SHA256", pinned_sha)
    monkeypatch.setattr(gate, "_read_snapshot", lambda path: snapshot)
    original_hash = hashlib.sha256(input_path.read_bytes()).hexdigest()
    output_a = tmp_path / "output-a"
    output_b = tmp_path / "output-b"

    qa_a = gate.build_decision_gate(input_path, output_a, "a" * 40)
    qa_b = gate.build_decision_gate(input_path, output_b, "a" * 40)

    assert qa_a["status"] == "PASS", qa_a["failed_checks"]
    assert qa_a["check_count"] == qa_a["passing_check_count"] + qa_a["failed_check_count"]
    assert qa_a["reconciliation"]["readiness_counts"] == {
        "READY_FOR_SUPERVISOR_DECISION": 12,
        "REQUIRES_SCOPE_REFINEMENT": 4,
        "REQUIRES_MORE_EVIDENCE": 0,
    }
    assert qa_a["reconciliation"]["category_opportunity_state_explicit_null_count"] == 5
    assert qa_a["deterministic_replay"]["byte_identical"] is True
    assert qa_a["deterministic_replay"]["artifact_count"] == len(gate.FINAL_ARTIFACTS)
    assert qa_b["status"] == "PASS"
    assert all(
        (output_a / filename).read_bytes() == (output_b / filename).read_bytes()
        for filename in gate.FINAL_ARTIFACTS
    )
    assert hashlib.sha256(input_path.read_bytes()).hexdigest() == original_hash
    manifest = json.loads((output_a / "DATASET_MANIFEST.json").read_text(encoding="utf-8"))
    assert manifest["input_sha256"] == original_hash
    assert len(manifest["artifacts"]) == len(gate.FINAL_ARTIFACTS) - 1
    for item in manifest["artifacts"]:
        artifact = output_a / item["path"]
        assert artifact.stat().st_size == item["size_bytes"]
        assert gate.sha256_file(artifact) == item["sha256"]


def test_missing_resolved_job_stays_null_and_ambiguous_has_explicit_scope_text():
    snapshot = make_snapshot()
    rows = gate._build_decision_rows(snapshot)
    cards = {row["canonical_direction_key"]: row for row in rows["cards"]}
    assert cards["progression"]["resolved_market_job"] is None
    assert cards["progression"]["uncertainty_context"]["research_ambiguity_statement"]
    assert cards["auth"]["resolved_market_job"] is not None
