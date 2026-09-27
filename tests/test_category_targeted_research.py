from __future__ import annotations

from market_analysis.category_targeted_research import (
    _build_coverage,
    _normalise_capture,
    _qa,
    _qa_artifacts,
    _write_csv,
    _write_jsonl,
    _write_sqlite,
)


def _fixture() -> tuple[list[dict], list[dict], dict]:
    direction_id = "dir_fixture"
    category_id = "administration"
    option = {
        "direction_id": direction_id,
        "category_id": category_id,
        "category_opportunity_state": "PROMISING",
        "direction_type": "LEXICAL_SUBNICHE",
        "canonical_direction_key": "fixture_direction",
        "candidate_state": "ADVANCE_TO_STAGE_D",
        "direction_tier": "LEAD_DIRECTION",
        "positive_support_shape": "LEXICAL_ONLY",
        "risk_flags": [],
        "reason_codes": ["FIXTURE"],
        "direction_evidence_pack": {"baseline": "unchanged"},
    }
    category_ids = [
        "administration", "communication", "developer_tools", "economy", "gameplay",
        "protection", "minigames", "roleplay", "world_management", "server_utilities", "uncategorized",
    ]
    categories = [
        {"category_id": value, "category_order": index, "category_name": value.replace("_", " ").title()}
        for index, value in enumerate(category_ids)
    ]
    sources = [
        {"source_id": "s-market", "canonical_url": "https://market.example/plugin", "title": "Market listing", "source_type": "MARKETPLACE_LISTING", "access_status": "OPENED"},
        {"source_id": "s-docs", "canonical_url": "https://docs.example/plugin", "title": "Official docs", "source_type": "PRIMARY_DOCS", "access_status": "OPENED"},
        {"source_id": "s-community", "canonical_url": "https://community.example/post", "title": "Operator report", "source_type": "COMMUNITY", "access_status": "OPENED"},
        {"source_id": "s-repo", "canonical_url": "https://github.com/example/plugin", "title": "Repository", "source_type": "PRIMARY_REPOSITORY", "access_status": "OPENED"},
    ]
    evidence = [
        ("e-semantic", "s-market", "SEMANTIC_IDENTITY"),
        ("e-relation", "s-market", "COMPETITOR_RELATION"),
        ("e-feature", "s-docs", "FEATURE"),
        ("e-maintenance", "s-market", "MAINTENANCE"),
        ("e-pain", "s-community", "OPERATOR_PAIN"),
        ("e-popularity", "s-repo", "POPULARITY_PROXY"),
    ]
    evidence_rows = [
        {
            "evidence_id": evidence_id,
            "direction_id": direction_id,
            "category_id": category_id,
            "source_id": source_id,
            "claim_type": claim_type,
            "observation": f"Fixture {claim_type.lower()} observation.",
            "feature_theme": "fixture_theme" if claim_type == "FEATURE" else None,
        }
        for evidence_id, source_id, claim_type in evidence
    ]
    queries = [
        {
            "query_id": f"q-{index}",
            "direction_id": direction_id,
            "query_purpose": purpose,
            "query": f"fixture {purpose}",
            "result_source_ids": ["s-market", "s-docs"],
        }
        for index, purpose in enumerate(("semantic resolution", "competitor discovery", "operator pain-point discovery"))
    ]
    entity = {
        "competitor_id": "c-fixture",
        "direction_id": direction_id,
        "category_id": category_id,
        "name": "Fixture Plugin",
        "canonical_url": "https://market.example/plugin",
        "relation_type": "DIRECT",
        "maintenance_status": "CURRENT_LISTING_CHECKED",
        "evidence_ids": ["e-semantic", "e-relation", "e-feature"],
        "lifecycle_evidence_ids": ["e-maintenance"],
        "price": None,
        "currency": None,
    }
    capture = {
        "retrieved_at": "2026-09-27T14:05:19Z",
        "source_documents": sources,
        "research_queries": queries,
        "external_evidence": evidence_rows,
        "competitor_entities": [entity],
        "direction_semantic_relations": [],
        "pack_notes": {
            direction_id: {
                "research_status": "RESOLVED",
                "resolved_market_job": "Fixture job",
                "market_job_summary": "Fixture summary.",
            }
        },
    }
    return [option], categories, capture


def _normalized_fixture() -> tuple[list[dict], list[dict], dict]:
    options, categories, capture = _fixture()
    data = _normalise_capture(options, capture)
    data["category_research_coverage"] = _build_coverage(categories, data["direction_research_packs"])
    return options, categories, data


def test_resolved_fixture_preserves_frozen_identity_and_evidence_contract():
    options, categories, data = _normalized_fixture()
    qa = _qa(options, categories, data, "700c22ad7bdd2ba9502e5adf933b3994e4ad84a852caef26d400094a9e8734bb")

    assert data["direction_research_packs"][0]["direction_evidence_pack"] == {"baseline": "unchanged"}
    assert qa["checks"]["immutable_stage_d_option_fields_preserved"]
    assert qa["checks"]["required_distinct_query_purposes_for_all_options"]
    assert qa["checks"]["resolved_research_coverage_gates"]
    assert qa["checks"]["direct_competitors_have_relation_semantic_and_current_lifecycle_evidence"]
    assert qa["checks"]["retained_competitor_relations_link_to_current_entity"]


def test_orphaned_competitor_relation_and_non_lifecycle_reference_fail_qa():
    options, categories, data = _normalized_fixture()
    data["competitor_entities"][0]["evidence_ids"].remove("e-relation")
    data["competitor_entities"][0]["lifecycle_evidence_ids"] = ["e-feature"]

    qa = _qa(options, categories, data, "700c22ad7bdd2ba9502e5adf933b3994e4ad84a852caef26d400094a9e8734bb")

    assert not qa["checks"]["direct_competitors_have_relation_semantic_and_current_lifecycle_evidence"]
    assert not qa["checks"]["retained_competitor_relations_link_to_current_entity"]


def test_sqlite_integrity_foreign_keys_and_all_csv_jsonl_exports_reconcile(tmp_path):
    options, categories, data = _normalized_fixture()
    for table in (
        "source_documents", "external_evidence", "competitor_entities", "research_queries",
        "direction_semantic_relations", "direction_research_packs", "category_research_coverage",
    ):
        _write_jsonl(tmp_path / f"{table}.jsonl", data[table])
        _write_csv(tmp_path / f"{table}.csv", data[table])
    _write_sqlite(tmp_path / "category_targeted_external_research.sqlite", "fixture", "0" * 40, categories, options, data)
    qa = {"checks": {}, "failed_checks": [], "overall_status": "PASS"}

    _qa_artifacts(tmp_path, options, categories, data, qa)

    assert qa["checks"]["sqlite_integrity_check"]
    assert qa["checks"]["sqlite_foreign_key_check"]
    assert qa["checks"]["sqlite_table_counts_reconcile"]
    assert qa["checks"]["sqlite_frozen_options_match_accepted_input"]
    assert qa["checks"]["jsonl_csv_export_counts_reconcile"]
    assert qa["overall_status"] == "PASS"
