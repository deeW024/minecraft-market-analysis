from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from market_analysis import category_targeted_research as research


def _fixture(direction_id: str = "dir_fixture", category_id: str = "administration") -> tuple[list[dict], list[dict], dict]:
    option = {
        "direction_id": direction_id,
        "category_id": category_id,
        "category_opportunity_state": "PROMISING",
        "direction_type": "LEXICAL_SUBNICHE",
        "canonical_direction_key": "fixture_direction",
        "candidate_state": "ADVANCE_TO_STAGE_D",
        "direction_tier": "LEAD_DIRECTION",
        "positive_support_shape": "LEXICAL_ONLY",
        "risk_flags": ["FIXTURE_RISK"],
        "reason_codes": ["FIXTURE"],
        "direction_evidence_pack": {"baseline": "unchanged"},
    }
    categories = [
        {"category_id": value, "category_order": index, "category_name": value.replace("_", " ").title()}
        for index, value in enumerate(research.EXPECTED_CATEGORY_ORDER)
    ]
    sources = [
        {"source_id": "s-market", "canonical_url": "https://market.example/plugin", "title": "Market listing", "source_type": "MARKETPLACE_LISTING", "access_status": "OPENED"},
        {"source_id": "s-docs", "canonical_url": "https://docs.example/plugin", "title": "Official docs", "source_type": "PRIMARY_DOCS", "access_status": "OPENED"},
        {"source_id": "s-community", "canonical_url": "https://community.example/post", "title": "Operator report", "source_type": "COMMUNITY", "access_status": "OPENED"},
        {"source_id": "s-repo", "canonical_url": "https://github.com/example/plugin", "title": "Repository", "source_type": "PRIMARY_REPOSITORY", "access_status": "OPENED"},
    ]
    evidence_specs = [
        ("e-semantic", "s-market", "SEMANTIC_IDENTITY"),
        ("e-relation", "s-market", "COMPETITOR_RELATION"),
        ("e-feature", "s-docs", "FEATURE"),
        ("e-maintenance", "s-docs", "MAINTENANCE"),
        ("e-pain", "s-community", "OPERATOR_PAIN"),
        ("e-popularity", "s-repo", "POPULARITY_PROXY"),
    ]
    evidence = [
        {
            "evidence_id": evidence_id,
            "direction_id": direction_id,
            "category_id": category_id,
            "source_id": source_id,
            "claim_type": claim_type,
            "observation": f"Fixture {claim_type.lower()} observation.",
            "feature_theme": "fixture_theme" if claim_type == "FEATURE" else None,
            "numeric_value": 27 if claim_type == "POPULARITY_PROXY" else None,
            "unit": "public stars" if claim_type == "POPULARITY_PROXY" else None,
        }
        for evidence_id, source_id, claim_type in evidence_specs
    ]
    queries = [
        {
            "query_id": f"q-{index}",
            "direction_id": direction_id,
            "query_purpose": purpose,
            "query": f"fixture {purpose} query {index}",
            "result_source_ids": source_ids,
        }
        for index, (purpose, source_ids) in enumerate((
            ("semantic resolution", ["s-market", "s-docs"]),
            ("competitor discovery", ["s-market", "s-docs"]),
            ("operator pain-point discovery", ["s-community"]),
        ), start=1)
    ]
    entity = {
        "competitor_id": "c-fixture",
        "direction_id": direction_id,
        "category_id": category_id,
        "name": "Fixture Plugin",
        "canonical_url": "https://market.example/plugin",
        "relation_type": "DIRECT",
        "product_type": "Minecraft server plugin",
        "platform_or_ecosystem": "Paper",
        "plugin_scope_status": "CONFIRMED",
        "pricing_model": "UNKNOWN",
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
        "external_evidence": evidence,
        "competitor_entities": [entity],
        "direction_semantic_relations": [],
        "pack_notes": {
            direction_id: {
                "research_status": "RESOLVED",
                "resolved_market_job": "Fixture job",
                "market_job_summary": "Fixture summary.",
                "maintenance_summary": "Current activity was checked.",
            }
        },
    }
    return [option], categories, capture


def _normalized_fixture(direction_id: str = "dir_fixture", category_id: str = "administration"):
    options, categories, capture = _fixture(direction_id, category_id)
    data = research._normalise_capture(options, capture)
    data["category_research_coverage"] = research._build_coverage(
        categories, data["direction_research_packs"], data["competitor_entities"],
        data["direction_semantic_relations"], data["external_evidence"], data["research_queries"],
    )
    return options, categories, capture, data


def test_resolved_fixture_preserves_frozen_identity_and_evidence_contract():
    options, categories, _, data = _normalized_fixture()
    qa = research._qa(options, categories, data, research.INPUT_SHA256)
    pack = data["direction_research_packs"][0]

    assert pack["direction_evidence_pack"] == {"baseline": "unchanged"}
    assert pack["research_coverage_status"] == "SUFFICIENT"
    assert qa["checks"]["immutable_stage_d_option_fields_preserved"]
    assert qa["checks"]["required_distinct_query_purposes_for_all_options"]
    assert qa["checks"]["resolved_research_coverage_gates"]
    assert qa["checks"]["direct_competitors_have_relation_semantic_and_current_lifecycle_evidence"]
    assert qa["checks"]["retained_competitor_relations_link_to_current_entity"]


def test_url_canonicalization_and_duplicate_source_ids_are_deduplicated_and_remapped():
    options, _, capture = _fixture()
    capture["source_documents"][0]["canonical_url"] = "https://market.example/plugin?b=2"
    duplicate = dict(capture["source_documents"][0])
    duplicate.update({
        "source_id": "s-market-alias",
        "canonical_url": "HTTPS://Market.Example:443/plugin/?utm_source=fixture&b=2#section",
    })
    capture["source_documents"].append(duplicate)
    evidence = dict(capture["external_evidence"][0])
    evidence.update({"evidence_id": "e-semantic-alias", "source_id": "s-market-alias"})
    capture["external_evidence"].append(evidence)
    capture["research_queries"][0]["result_source_ids"].append("s-market-alias")

    data = research._normalise_capture(options, capture)

    assert research._canonicalize_url("HTTPS://Market.Example:443/plugin/?utm_source=fixture&b=2#section") == "https://market.example/plugin?b=2"
    assert len(data["source_documents"]) == 4
    market = next(row for row in data["source_documents"] if row["source_id"] == "s-market")
    assert market["source_id_aliases"] == ["s-market", "s-market-alias"]
    assert {row["source_id"] for row in data["external_evidence"] if row["evidence_id"].startswith("e-semantic")} == {"s-market"}
    assert data["research_queries"][0]["result_source_ids"].count("s-market") == 1


def test_reused_source_id_for_distinct_canonical_urls_is_rejected():
    options, _, capture = _fixture()
    duplicate = dict(capture["source_documents"][0])
    duplicate["canonical_url"] = "https://different.example/other"
    capture["source_documents"].append(duplicate)

    with pytest.raises(ValueError, match="reused for different canonical URLs"):
        research._normalise_capture(options, capture)


def test_query_timestamp_fallback_is_explicit_and_matches_capture_time():
    options, categories, capture = _fixture()
    capture["research_queries"][0]["issued_at"] = "2026-09-27T13:00:00Z"
    data = research._normalise_capture(options, capture)
    data["category_research_coverage"] = research._build_coverage(
        categories, data["direction_research_packs"], data["competitor_entities"],
        data["direction_semantic_relations"], data["external_evidence"], data["research_queries"],
    )
    qa = research._qa(options, categories, data, research.INPUT_SHA256)

    assert data["research_queries"][0]["issued_at_basis"] == "captured_individual_query_timestamp"
    assert data["research_queries"][1]["issued_at"] == capture["retrieved_at"]
    assert data["research_queries"][1]["issued_at_basis"].startswith("frozen_capture_retrieved_at")
    assert data["normalization_summary"]["timestamp_fallback_query_count"] == 2
    assert qa["checks"]["query_issued_timestamps_have_explicit_or_disclosed_fallback_basis"]

    data["research_queries"][1]["issued_at"] = "2026-09-27T12:00:00Z"
    qa = research._qa(options, categories, data, research.INPUT_SHA256)
    assert not qa["checks"]["query_issued_timestamps_have_explicit_or_disclosed_fallback_basis"]


def test_unavailable_source_cannot_support_retained_evidence():
    options, _, capture = _fixture()
    capture["source_documents"][0]["access_status"] = "UNAVAILABLE_AFTER_ATTEMPT"

    with pytest.raises(ValueError, match="Unavailable source cannot support evidence"):
        research._normalise_capture(options, capture)


@pytest.mark.parametrize("reference", ["query_source", "entity_evidence"])
def test_unknown_source_and_evidence_references_fail_closed(reference: str):
    options, _, capture = _fixture()
    if reference == "query_source":
        capture["research_queries"][0]["result_source_ids"].append("missing-source")
        expected = "references unknown sources"
    else:
        capture["competitor_entities"][0]["evidence_ids"].append("missing-evidence")
        expected = "unknown evidence"

    with pytest.raises(ValueError, match=expected):
        research._normalise_capture(options, capture)


def test_direct_competitor_requires_positive_plugin_scope_and_product_identity():
    options, _, capture = _fixture()
    capture["competitor_entities"][0]["plugin_scope_status"] = "AMBIGUOUS"

    with pytest.raises(ValueError, match="positive plugin-scope evidence"):
        research._normalise_capture(options, capture)

    options, _, capture = _fixture()
    capture["competitor_entities"][0]["platform_or_ecosystem"] = None
    with pytest.raises(ValueError, match="product/platform identity evidence"):
        research._normalise_capture(options, capture)


def test_non_plugin_adjacent_context_never_enters_direct_competitor_counts():
    options, categories, capture = _fixture()
    capture["external_evidence"].append({
        "evidence_id": "e-adjacent", "direction_id": "dir_fixture", "category_id": "administration",
        "source_id": "s-docs", "claim_type": "ADJACENT_CONTEXT", "observation": "Non-plugin adjacent context.",
    })
    capture["competitor_entities"].append({
        "competitor_id": "c-adjacent", "direction_id": "dir_fixture", "category_id": "administration",
        "name": "Adjacent product", "canonical_url": "https://docs.example/plugin", "relation_type": "ADJACENT",
        "plugin_scope_status": "NOT_PLUGIN", "product_type": "Web service", "platform_or_ecosystem": None,
        "pricing_model": "UNKNOWN", "price": None, "currency": None, "maintenance_status": None,
        "feature_summary": None, "evidence_ids": ["e-adjacent"], "lifecycle_evidence_ids": [],
    })

    data = research._normalise_capture(options, capture)
    data["category_research_coverage"] = research._build_coverage(
        categories, data["direction_research_packs"], data["competitor_entities"],
        data["direction_semantic_relations"], data["external_evidence"], data["research_queries"],
    )
    qa = research._qa(options, categories, data, research.INPUT_SHA256)
    pack = data["direction_research_packs"][0]

    assert pack["direct_competitor_count"] == 1
    assert pack["direct_competitor_ids"] == ["c-fixture"]
    assert qa["checks"]["non_plugin_entities_are_adjacent_context_only"]


@pytest.mark.parametrize("status", ["AMBIGUOUS", "UNRESOLVED"])
def test_ambiguous_or_unresolved_direction_does_not_retain_direction_level_direct(status: str):
    options, categories, capture = _fixture()
    capture["pack_notes"]["dir_fixture"]["research_status"] = status

    data = research._normalise_capture(options, capture)
    data["category_research_coverage"] = research._build_coverage(
        categories, data["direction_research_packs"], data["competitor_entities"],
        data["direction_semantic_relations"], data["external_evidence"], data["research_queries"],
    )
    qa = research._qa(options, categories, data, research.INPUT_SHA256)

    assert data["competitor_entities"][0]["relation_type"] == "ADJACENT"
    assert data["direction_research_packs"][0]["research_coverage_status"] == status
    assert data["direction_research_packs"][0]["direct_competitor_count"] == 0
    assert qa["checks"]["direct_entities_only_for_resolved_options"]
    assert qa["checks"]["resolved_research_coverage_gates"]


def test_resolved_coverage_boundary_is_partial_below_five_evidence_items():
    options, categories, capture = _fixture()
    capture["external_evidence"] = [
        row for row in capture["external_evidence"] if row["evidence_id"] not in {"e-pain", "e-popularity"}
    ]
    capture["competitor_entities"][0]["evidence_ids"].remove("e-feature")

    data = research._normalise_capture(options, capture)
    data["category_research_coverage"] = research._build_coverage(
        categories, data["direction_research_packs"], data["competitor_entities"],
        data["direction_semantic_relations"], data["external_evidence"], data["research_queries"],
    )
    qa = research._qa(options, categories, data, research.INPUT_SHA256)

    assert data["direction_research_packs"][0]["research_coverage_status"] == "PARTIAL"
    assert qa["checks"]["resolved_research_coverage_gates"]


def test_each_option_requires_three_distinct_query_purposes():
    options, categories, capture = _fixture()
    capture["research_queries"] = [row for row in capture["research_queries"] if row["query_purpose"] != "operator pain-point discovery"]
    data = research._normalise_capture(options, capture)
    data["category_research_coverage"] = research._build_coverage(
        categories, data["direction_research_packs"], data["competitor_entities"],
        data["direction_semantic_relations"], data["external_evidence"], data["research_queries"],
    )
    qa = research._qa(options, categories, data, research.INPUT_SHA256)

    assert not qa["checks"]["required_distinct_query_purposes_for_all_options"]
    assert data["direction_research_packs"][0]["research_coverage_status"] == "PARTIAL"


def test_direct_lifecycle_check_requires_maintenance_evidence_and_query_provenance():
    options, categories, capture = _fixture()
    capture["competitor_entities"][0]["lifecycle_evidence_ids"] = []
    data = research._normalise_capture(options, capture)
    data["category_research_coverage"] = research._build_coverage(
        categories, data["direction_research_packs"], data["competitor_entities"],
        data["direction_semantic_relations"], data["external_evidence"], data["research_queries"],
    )
    qa = research._qa(options, categories, data, research.INPUT_SHA256)

    assert data["direction_research_packs"][0]["research_coverage_status"] == "PARTIAL"
    assert not qa["checks"]["query_lifecycle_checks_are_provenanced"]
    assert not qa["checks"]["direct_competitors_have_relation_semantic_and_current_lifecycle_evidence"]


def test_missing_price_is_null_not_free_or_zero():
    _, _, _, data = _normalized_fixture()
    entity = data["competitor_entities"][0]
    assert entity["pricing_model"] is None
    assert entity["price_amount"] is None
    assert entity["currency"] is None

    options, _, capture = _fixture()
    capture["competitor_entities"][0]["price"] = 0
    with pytest.raises(ValueError, match="Pricing fields require PRICING evidence"):
        research._normalise_capture(options, capture)


def test_pricing_observations_keep_currencies_separate():
    options, categories, capture = _fixture()
    for evidence_id, currency, value in (("e-price-usd", "USD", 4.99), ("e-price-eur", "EUR", 4.49)):
        capture["external_evidence"].append({
            "evidence_id": evidence_id, "direction_id": "dir_fixture", "category_id": "administration",
            "source_id": "s-market", "claim_type": "PRICING", "observation": f"Listed price {value} {currency}.",
            "numeric_value": value, "numeric_unit": "monthly", "currency": currency,
        })
    data = research._normalise_capture(options, capture)
    data["category_research_coverage"] = research._build_coverage(
        categories, data["direction_research_packs"], data["competitor_entities"],
        data["direction_semantic_relations"], data["external_evidence"], data["research_queries"],
    )
    qa = research._qa(options, categories, data, research.INPUT_SHA256)
    pricing = data["direction_research_packs"][0]["pricing_observations_by_currency"]

    assert set(pricing) == {"EUR", "USD"}
    assert pricing["USD"][0]["numeric_value"] == 4.99
    assert pricing["EUR"][0]["numeric_value"] == 4.49
    assert qa["checks"]["pricing_summaries_keep_currencies_separate"]


def test_hypotheses_need_two_canonical_urls_and_feature_or_pain_basis():
    options, _, capture = _fixture()
    capture["differentiation_hypotheses"] = [{
        "direction_id": "dir_fixture", "label": "HYPOTHESIS", "hypothesis": "Possible gap.",
        "observed_evidence": "Observed fixture evidence.", "evidence_ids": ["e-semantic", "e-relation"],
        "purely_semantic_or_overlap": True,
    }]
    with pytest.raises(ValueError, match="two canonical source URLs"):
        research._normalise_capture(options, capture)

    capture["differentiation_hypotheses"][0].update({
        "evidence_ids": ["e-feature", "e-pain"], "purely_semantic_or_overlap": False,
    })
    normalized = research._normalise_capture(options, capture)
    hypothesis = normalized["direction_research_packs"][0]["differentiation_hypotheses"][0]
    assert hypothesis["label"] == "HYPOTHESIS"
    assert len(hypothesis["evidence_ids"]) == 2


def test_semantic_relation_enum_is_category_local_and_does_not_mutate_option_ids():
    options, _, capture = _fixture()
    second = dict(options[0], direction_id="dir_fixture_2", canonical_direction_key="fixture_direction_2")
    options.append(second)
    capture["pack_notes"][second["direction_id"]] = {"research_status": "AMBIGUOUS"}
    capture["external_evidence"].append({
        "evidence_id": "e-overlap", "direction_id": second["direction_id"], "category_id": "administration",
        "source_id": "s-market", "claim_type": "OVERLAP_SIGNAL", "observation": "The jobs overlap.",
    })
    capture["direction_semantic_relations"] = [{
        "relation_id": "rel-fixture", "direction_id": "dir_fixture", "related_direction_id": "dir_fixture_2",
        "category_id": "administration", "relation_type": "OVERLAP_SIGNAL", "evidence_ids": ["e-overlap"],
        "rationale": "Same category, partially overlapping server jobs.",
    }]

    data = research._normalise_capture(options, capture)

    assert {row["direction_id"] for row in data["direction_research_packs"]} == {"dir_fixture", "dir_fixture_2"}
    assert data["direction_semantic_relations"][0]["relation_type"] == "SUBSTANTIAL_OVERLAP"
    assert next(row for row in data["external_evidence"] if row["evidence_id"] == "e-overlap")["claim_type"] == "OVERLAP_SIGNAL"

    capture["direction_semantic_relations"][0]["relation_type"] = "NOT_ALLOWED"
    with pytest.raises(ValueError, match="Unknown semantic relation type"):
        research._normalise_capture(options, capture)

    capture["direction_semantic_relations"][0]["relation_type"] = "OVERLAP_SIGNAL"
    other = dict(second, direction_id="dir_other_category", category_id="gameplay")
    options.append(other)
    capture["pack_notes"][other["direction_id"]] = {"research_status": "AMBIGUOUS"}
    capture["direction_semantic_relations"][0]["related_direction_id"] = other["direction_id"]
    with pytest.raises(ValueError, match="mismatched category/endpoints"):
        research._normalise_capture(options, capture)


def _full_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    categories = [
        {"category_id": category_id, "category_order": index, "category_name": category_id.replace("_", " ").title()}
        for index, category_id in enumerate(research.EXPECTED_CATEGORY_ORDER)
    ]
    options = []
    for category_id, count in research.EXPECTED_OPTION_COUNTS.items():
        for index in range(count):
            direction_id = f"dir-fixture-{len(options):02d}"
            options.append({
                "direction_id": direction_id,
                "category_id": category_id,
                "category_opportunity_state": "PROMISING",
                "direction_type": "LEXICAL_SUBNICHE",
                "canonical_direction_key": f"fixture_{len(options):02d}",
                "candidate_state": "ADVANCE_TO_STAGE_D",
                "direction_tier": "LEAD_DIRECTION" if index == 0 else "WATCH_DIRECTION",
                "positive_support_shape": "LEXICAL_ONLY",
                "risk_flags": ["FIXTURE_RISK"],
                "reason_codes": ["FIXTURE"],
                "direction_evidence_pack": {"fixture": "immutable"},
            })
    option_ids_sha = hashlib.sha256("".join(f"{row['direction_id']}\n" for row in sorted(options, key=lambda row: row["direction_id"])).encode("utf-8")).hexdigest()
    monkeypatch.setattr(research, "DIRECTION_IDS_SHA256", option_ids_sha)

    input_path = tmp_path / "accepted_input.sqlite"
    connection = sqlite3.connect(input_path)
    connection.executescript("""
        CREATE TABLE category_opportunity_profiles(category_id TEXT);
        CREATE TABLE category_direction_tiers(direction_id TEXT);
        CREATE TABLE category_research_options(category_order INTEGER, direction_id TEXT, record_json TEXT);
        CREATE TABLE frozen_category_snapshot(category_id TEXT, category_order INTEGER, category_json TEXT);
        CREATE TABLE run_metadata(key TEXT, value TEXT);
        CREATE TABLE input_provenance(key TEXT, value TEXT);
    """)
    connection.executemany("INSERT INTO category_opportunity_profiles VALUES (?)", [(row["category_id"],) for row in categories])
    connection.executemany("INSERT INTO category_direction_tiers VALUES (?)", [(f"tier-{index:02d}",) for index in range(47)])
    connection.executemany(
        "INSERT INTO category_research_options VALUES (?,?,?)",
        [(research.EXPECTED_CATEGORY_ORDER.index(row["category_id"]), row["direction_id"], json.dumps(row)) for row in options],
    )
    connection.executemany(
        "INSERT INTO frozen_category_snapshot VALUES (?,?,?)",
        [(row["category_id"], row["category_order"], json.dumps(row)) for row in categories],
    )
    metadata = {
        "run_id": research.INPUT_RUN_ID,
        "code_commit": research.INPUT_CODE_COMMIT,
        "work_order": "YEE-76",
        "schema_version": research.INPUT_SCHEMA_VERSION,
        "taxonomy_version": research.INPUT_TAXONOMY_VERSION,
        "taxonomy_sha256": research.INPUT_TAXONOMY_SHA256,
    }
    provenance = {
        "taxonomy_version": research.INPUT_TAXONOMY_VERSION,
        "taxonomy_sha256": research.INPUT_TAXONOMY_SHA256,
        "input_read_only": "true",
    }
    connection.executemany("INSERT INTO run_metadata VALUES (?,?)", metadata.items())
    connection.executemany("INSERT INTO input_provenance VALUES (?,?)", provenance.items())
    connection.commit()
    connection.close()
    input_sha = research.sha256_file(input_path)
    monkeypatch.setattr(research, "INPUT_SHA256", input_sha)

    capture = {
        "input_work_order": "YEE-76", "input_run_id": research.INPUT_RUN_ID, "input_sha256": input_sha,
        "schema_version": research.SCHEMA_VERSION, "retrieved_at": "2026-09-27T14:05:19Z",
        "source_documents": [], "research_queries": [], "external_evidence": [],
        "competitor_entities": [], "direction_semantic_relations": [], "pack_notes": {},
    }
    for index, option in enumerate(options):
        direction_id = option["direction_id"]
        category_id = option["category_id"]
        market_id, docs_id, community_id = (f"s-{index}-{kind}" for kind in ("market", "docs", "community"))
        capture["source_documents"].extend([
            {"source_id": market_id, "canonical_url": f"https://market{index}.example/item", "title": f"Market {index}", "source_type": "MARKETPLACE_LISTING", "access_status": "OPENED"},
            {"source_id": docs_id, "canonical_url": f"https://docs{index}.example/plugin", "title": f"Docs {index}", "source_type": "PRIMARY_DOCS", "access_status": "OPENED"},
            {"source_id": community_id, "canonical_url": f"https://community{index}.example/post", "title": f"Community {index}", "source_type": "COMMUNITY", "access_status": "OPENED"},
        ])
        evidence_ids = {
            "semantic": f"e-{index}-semantic", "relation": f"e-{index}-relation",
            "feature": f"e-{index}-feature", "maintenance": f"e-{index}-maintenance",
            "pain": f"e-{index}-pain",
        }
        for kind, source_id, claim_type in (
            ("semantic", market_id, "SEMANTIC_IDENTITY"),
            ("relation", market_id, "COMPETITOR_RELATION"),
            ("feature", docs_id, "FEATURE"),
            ("maintenance", docs_id, "MAINTENANCE"),
            ("pain", community_id, "OPERATOR_PAIN"),
        ):
            capture["external_evidence"].append({
                "evidence_id": evidence_ids[kind], "direction_id": direction_id, "category_id": category_id,
                "source_id": source_id, "claim_type": claim_type, "observation": f"Fixture {kind} for {direction_id}.",
                "feature_theme": "fixture_capability" if kind == "feature" else None,
            })
        capture["competitor_entities"].append({
            "competitor_id": f"c-{index}", "direction_id": direction_id, "category_id": category_id,
            "name": f"Fixture plugin {index}", "canonical_url": f"https://market{index}.example/item",
            "relation_type": "DIRECT", "product_type": "Minecraft server plugin", "platform_or_ecosystem": "Paper",
            "plugin_scope_status": "CONFIRMED", "pricing_model": "UNKNOWN", "price": None, "currency": None,
            "maintenance_status": "CURRENT_LISTING_CHECKED", "feature_summary": "Fixture feature.",
            "evidence_ids": [evidence_ids["semantic"], evidence_ids["relation"], evidence_ids["feature"]],
            "lifecycle_evidence_ids": [evidence_ids["maintenance"]],
        })
        capture["research_queries"].extend([
            {"query_id": f"q-{index}-semantic", "direction_id": direction_id, "query_purpose": "semantic resolution", "query": f"semantic query {index}", "result_source_ids": [market_id, docs_id]},
            {"query_id": f"q-{index}-competitor", "direction_id": direction_id, "query_purpose": "competitor discovery", "query": f"competitor query {index}", "result_source_ids": [market_id, docs_id]},
            {"query_id": f"q-{index}-pain", "direction_id": direction_id, "query_purpose": "operator pain-point discovery", "query": f"pain query {index}", "result_source_ids": [community_id]},
        ])
        capture["pack_notes"][direction_id] = {
            "research_status": "RESOLVED", "resolved_market_job": f"Fixture job {index}",
            "market_job_summary": f"Fixture market summary {index}.",
            "maintenance_summary": "Current activity was checked.", "research_notes": "Synthetic offline fixture.",
        }
    capture_path = tmp_path / "frozen_capture.json"
    capture_path.write_text(json.dumps(capture, sort_keys=True), encoding="utf-8", newline="\n")
    monkeypatch.setattr(research, "CAPTURE_SHA256", research.sha256_file(capture_path))
    return input_path, capture_path, options


def test_exact_16_options_all_11_categories_replay_exports_and_input_immutability(tmp_path, monkeypatch):
    input_path, capture_path, options = _full_fixture(tmp_path, monkeypatch)
    original_sha = research.sha256_file(input_path)
    monkeypatch.setattr(research, "_current_execution_commit", lambda: "a" * 40)
    output = tmp_path / "production_bundle"

    qa = research.build_bundle(input_path, capture_path, output)
    assert qa["overall_status"] == "PASS", (qa["failed_checks"], qa["replay"])
    assert qa["option_count"] == 16
    assert qa["category_count"] == 11
    assert qa["checks"]["exact_16_option_identity_reconciliation"]
    assert qa["checks"]["all_11_taxonomy_category_coverage_rows"]
    assert qa["checks"]["exact_researched_and_zero_option_category_statuses"]
    assert qa["checks"]["sqlite_foreign_key_reference_tables_match_normalized_rows"]
    assert qa["replay"]["byte_identical"]
    assert qa["replay"]["artifact_count"] == 17
    assert qa["input_sha256_before"] == original_sha == qa["input_sha256_after"]
    packs = [json.loads(line) for line in (output / "direction_research_packs.jsonl").read_text(encoding="utf-8").splitlines()]
    coverage = [json.loads(line) for line in (output / "category_research_coverage.jsonl").read_text(encoding="utf-8").splitlines()]
    assert {row["direction_id"] for row in packs} == {row["direction_id"] for row in options}
    assert len(coverage) == 11
    assert [row["category_id"] for row in coverage] == list(research.EXPECTED_CATEGORY_ORDER)

    first_hashes = {path.name: research.sha256_file(path) for path in output.iterdir() if path.is_file()}
    replay_qa = research.build_bundle(input_path, capture_path, output)
    second_hashes = {path.name: research.sha256_file(path) for path in output.iterdir() if path.is_file()}
    assert replay_qa["overall_status"] == "PASS"
    assert first_hashes == second_hashes
    assert research.sha256_file(input_path) == original_sha


def test_sqlite_integrity_foreign_keys_and_csv_jsonl_reconciliation(tmp_path):
    options, categories, _, data = _normalized_fixture()
    data["input_provenance"] = {}
    for table in (
        "source_documents", "external_evidence", "competitor_entities", "research_queries",
        "direction_semantic_relations", "direction_research_packs", "category_research_coverage",
    ):
        research._write_jsonl(tmp_path / f"{table}.jsonl", data[table])
        research._write_csv(tmp_path / f"{table}.csv", data[table])
    research._write_sqlite(tmp_path / "category_targeted_external_research.sqlite", "fixture", "0" * 40, categories, options, data)
    qa = {
        "checks": {}, "failed_checks": [], "overall_status": "PASS",
        "input_sha256_before": "fixture", "input_sha256_after": "fixture", "execution_code_commit": "0" * 40,
        "capture_sha256": research.CAPTURE_SHA256,
    }

    research._qa_artifacts(tmp_path, options, categories, data, qa)

    assert qa["checks"]["sqlite_integrity_check"]
    assert qa["checks"]["sqlite_foreign_key_check"]
    assert qa["checks"]["sqlite_table_counts_reconcile"]
    assert qa["checks"]["sqlite_frozen_options_match_accepted_input"]
    assert qa["checks"]["sqlite_foreign_key_reference_tables_match_normalized_rows"]
    assert qa["checks"]["jsonl_csv_values_and_row_counts_reconcile"]
    assert qa["overall_status"] == "PASS"
