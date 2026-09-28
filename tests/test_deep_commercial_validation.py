from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from market_analysis import deep_commercial_validation as validation


def _canonical_rows():
    return [
        {"direction_id": direction_id, "direction_key": key, "category_id": category,
         "market_job": f"Canonical job for {key}"}
        for direction_id, (key, category) in sorted(validation.AUTHORIZED_DIRECTIONS.items())
    ]


def _capture():
    ids = sorted(validation.AUTHORIZED_DIRECTIONS)
    sources = []
    queries = []
    evidence = []
    directions = []
    for index, direction_id in enumerate(ids, 1):
        source_id = f"src-{index}"
        evidence_id = f"ev-{index}"
        source_url = f"https://public.example.test/source-{index}"
        sources.append({
            "source_id": source_id, "url": source_url, "title": f"Opened source {index}",
            "domain": "public.example.test", "source_type": "OFFICIAL_PRODUCT_OR_DOCUMENTATION", "access_status": "OPENED",
            "retrieved_at": "2026-09-28T02:00:00Z", "published_or_updated_at": None,
            "date_note": "Publication date not stated on opened page.",
        })
        query_ids = {}
        for sequence, purpose in enumerate(validation.QUERY_PURPOSES, 1):
            query_id = f"q-{index}-{sequence}"
            query_ids[purpose] = query_id
            queries.append({
                "query_id": query_id, "direction_id": direction_id, "sequence": sequence,
                "purpose": purpose, "query_text": f"Fixture query {purpose} for direction {index}",
                "issued_at": "2026-09-28T01:00:00Z", "query_kind": "MANDATORY_SEARCH",
                "result_note": "Search discovery only; opened canonical page recorded separately.",
                "opened_source_ids": [source_id] if sequence == 1 else [],
            })
        evidence.append({
            "evidence_id": evidence_id, "direction_id": direction_id, "source_id": source_id,
            "query_ids": [query_ids[validation.QUERY_PURPOSES[0]]], "evidence_type": "PRODUCT_FACT",
            "observation": f"The opened source documents the fixture job for direction {index}.",
            "source_locator": "Overview section", "limitations": "Fixture only.",
            "source_basis": "OPENED_CANONICAL_SOURCE",
        })
        dimensions = []
        for name in validation.DIMENSIONS:
            dimensions.append({
                "dimension": name,
                "state": "SUPPORTED" if name == validation.DIMENSIONS[0] else "INSUFFICIENT_EVIDENCE",
                "basis": "One same-direction fixture fact." if name == validation.DIMENSIONS[0] else "No fixture evidence found.",
                "evidence_ids": [evidence_id] if name == validation.DIMENSIONS[0] else [],
            })
        directions.append({
            "direction_id": direction_id, "coverage_status": "PARTIAL",
            "dimension_assessments": dimensions,
            "direct_wtp_summary": "No direct-WTP assertion in this fixture.",
            "paid_market_precedent_summary": "No paid precedent in this fixture.",
            "other_proxy_summary": "Fixture only; no demand inference.",
            "unknowns": ["direct WTP"], "risk_flags": [],
        })
    return {
        "capture_version": validation.CAPTURE_VERSION,
        "canonical_input_hashes": {"yee79_decision_gate": "a" * 64, "yee77_research": "b" * 64},
        "cohort_id_set_sha256": validation.COHORT_SHA256,
        "product_scope_branch": "PLUGIN_ONLY",
        "authorized_direction_ids": ids, "sources": sources, "queries": queries,
        "evidence": evidence, "buyer_problems": [], "competitor_offerings": [],
        "pricing_observations": [], "direct_wtp_observations": [],
        "paid_market_precedents": [], "other_market_proxies": [],
        "differentiation_observations": [], "feasibility_observations": [],
        "support_burden_observations": [], "channel_observations": [],
        "directions": directions,
    }


def _add_fixture_competitor(capture, name="Fixture plugin", relation="DIRECT_OR_NEAR_SUBSTITUTE", product_type="Minecraft server plugin"):
    evidence = capture["evidence"][0]
    capture["competitor_offerings"].append({
        "offering_id": f"co-{name.casefold().replace(' ', '-')}",
        "direction_id": evidence["direction_id"], "entity_name": name,
        "canonical_url": capture["sources"][0]["url"], "relation_type": relation,
        "product_type": product_type, "buyer_job": "Fixture job",
        "monetization_status": "UNKNOWN", "evidence_ids": [evidence["evidence_id"]],
        "limitations": "Fixture only.",
    })


def _sqlite_fixture(path: Path, kind: str) -> None:
    con = sqlite3.connect(path)
    try:
        if kind == "yee79":
            con.execute("CREATE TABLE direction_decision_cards(direction_id TEXT,record_json TEXT)")
            con.execute("CREATE TABLE frozen_direction_identity(direction_id TEXT,record_json TEXT)")
            for direction_id, (key, category) in validation.AUTHORIZED_DIRECTIONS.items():
                card = {"research_status": "RESOLVED", "research_coverage_status": "SUFFICIENT",
                        "category_id": category, "resolved_market_job": f"Canonical job for {key}"}
                con.execute("INSERT INTO direction_decision_cards VALUES(?,?)", (direction_id, json.dumps(card)))
                con.execute("INSERT INTO frozen_direction_identity VALUES(?,?)", (direction_id, json.dumps({"direction_id": direction_id})))
        else:
            con.execute("CREATE TABLE direction_research_packs(direction_id TEXT,category_id TEXT,research_status TEXT,research_coverage_status TEXT)")
            for direction_id, (_, category) in validation.AUTHORIZED_DIRECTIONS.items():
                con.execute("INSERT INTO direction_research_packs VALUES(?,?,?,?)", (direction_id, category, "RESOLVED", "SUFFICIENT"))
        con.commit()
    finally:
        con.close()


def _pinned_fixture(tmp_path):
    yee79 = tmp_path / "yee79.sqlite"
    yee77 = tmp_path / "yee77.sqlite"
    _sqlite_fixture(yee79, "yee79")
    _sqlite_fixture(yee77, "yee77")
    hashes = {
        "yee79_decision_gate": hashlib.sha256(yee79.read_bytes()).hexdigest(),
        "yee77_research": hashlib.sha256(yee77.read_bytes()).hexdigest(),
    }
    return yee79, yee77, hashes


def test_exact_cohort_and_one_mandatory_query_per_purpose():
    rows, hashes = _canonical_rows(), {"yee79_decision_gate": "a" * 64, "yee77_research": "b" * 64}
    payload = validation.normalize_capture(_capture(), rows, hashes)
    assert len(payload["direction_commercial_validation"]) == 5
    assert len(payload["commercial_research_queries"]) == 50


def test_query_coverage_rejects_duplicate_mandatory_purpose():
    capture = _capture()
    direction_id = sorted(validation.AUTHORIZED_DIRECTIONS)[0]
    row = next(q for q in capture["queries"] if q["direction_id"] == direction_id and q["sequence"] == 10)
    row["purpose"] = validation.QUERY_PURPOSES[0]
    with pytest.raises(validation.CommercialValidationError, match="exactly one mandatory search"):
        validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])


def test_search_snippet_cannot_be_used_as_factual_evidence():
    capture = _capture()
    capture["evidence"][0]["source_basis"] = "SEARCH_RESULT_SNIPPET"
    with pytest.raises(validation.CommercialValidationError, match="opened source"):
        validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])


def test_evidence_must_have_same_direction_opened_source_and_query():
    capture = _capture()
    capture["evidence"][0]["query_ids"] = [capture["queries"][-1]["query_id"]]
    with pytest.raises(validation.CommercialValidationError, match="same-direction"):
        validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])


def test_direct_wtp_cannot_be_inferred_from_competitor_price():
    capture = _capture()
    direction_id = sorted(validation.AUTHORIZED_DIRECTIONS)[0]
    capture["direct_wtp_observations"].append({
        "wtp_id": "w-inferred", "direction_id": direction_id,
        "wtp_class": "INFERRED_FROM_COMPETITOR_PRICE", "statement": "Fixture price",
        "amount": 10, "currency_code": "EUR", "currency_symbol": "€",
        "evidence_ids": [capture["evidence"][0]["evidence_id"]], "limitations": "Invalid.",
    })
    with pytest.raises(validation.CommercialValidationError, match="direct WTP"):
        validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])


def test_missing_price_is_not_free():
    capture = _capture()
    direction_id = sorted(validation.AUTHORIZED_DIRECTIONS)[0]
    _add_fixture_competitor(capture, "Unknown price")
    capture["pricing_observations"].append({
        "pricing_id": "p-unknown", "direction_id": direction_id,
        "entity_name": "Unknown price", "amount": None, "currency_code": None,
        "currency_symbol": None, "billing_model": None, "period": None,
        "tier_or_scope": None, "price_context": "Price unavailable on opened page",
        "monetization_status": "FREE_VERIFIED",
        "evidence_ids": [capture["evidence"][0]["evidence_id"]], "limitations": "Invalid.",
    })
    with pytest.raises(validation.CommercialValidationError, match="unknown amount cannot be normalized as free"):
        validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])


def test_null_price_with_unknown_status_remains_null_not_free():
    capture = _capture()
    direction_id = sorted(validation.AUTHORIZED_DIRECTIONS)[0]
    _add_fixture_competitor(capture, "Unknown price")
    capture["pricing_observations"].append({
        "pricing_id": "p-null", "direction_id": direction_id, "entity_name": "Unknown price",
        "amount": None, "currency_code": None, "currency_symbol": None,
        "billing_model": None, "period": None, "tier_or_scope": "Not stated",
        "price_context": "No price disclosed on opened page", "monetization_status": "UNKNOWN",
        "evidence_ids": [capture["evidence"][0]["evidence_id"]], "limitations": "Not evidence of free access.",
    })
    payload = validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])
    assert payload["pricing_observations"][0]["price_amount"] is None
    assert payload["pricing_observations"][0]["monetization_status"] == "UNKNOWN"


def test_sqlite_jsonl_csv_replay_is_deterministic_and_inputs_read_only(tmp_path):
    yee79, yee77, hashes = _pinned_fixture(tmp_path)
    before = (yee79.read_bytes(), yee77.read_bytes())
    capture_path = tmp_path / "capture.json"
    capture = _capture()
    capture["canonical_input_hashes"] = hashes
    capture_bytes = (json.dumps(capture, sort_keys=True, ensure_ascii=False, indent=2) + "\n").encode()
    capture_path.write_bytes(capture_bytes)
    output = tmp_path / "output"
    result = validation.build_dataset(
        yee79, yee77, capture_path, output, validation.BASELINE_COMMIT, expected_hashes=hashes
    )
    assert result["status"] == "PASS", result["qa"]["checks"]
    assert result["qa"]["checks"]["deterministic_replay_byte_identical"] is True
    assert (output / "COMMERCIAL_RESEARCH_CAPTURE.json").read_bytes() == capture_bytes
    assert (yee79.read_bytes(), yee77.read_bytes()) == before
    assert validation._check_exports(output, validation.normalize_capture(capture, _canonical_rows(), hashes))["row_counts_reconcile"]


def test_database_capture_rejects_wrong_input_hashes(tmp_path):
    yee79, yee77, _ = _pinned_fixture(tmp_path)
    with pytest.raises(validation.CommercialValidationError, match="canonical input hash mismatch"):
        validation.load_inputs(yee79, yee77)


def test_wrong_cohort_identity_and_cohort_sha_fail_closed():
    capture = _capture()
    capture["authorized_direction_ids"] = capture["authorized_direction_ids"][:-1]
    with pytest.raises(validation.CommercialValidationError, match="exact authorized direction set"):
        validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])
    capture = _capture()
    capture["cohort_id_set_sha256"] = "0" * 64
    with pytest.raises(validation.CommercialValidationError, match="cohort-id-set SHA"):
        validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])


def test_unauthorized_direction_leakage_fails_closed():
    capture = _capture()
    capture["evidence"][0]["direction_id"] = "dir-unauthorized"
    with pytest.raises(validation.CommercialValidationError, match="invalid/duplicate evidence"):
        validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])


def test_unavailable_source_cannot_back_evidence():
    capture = _capture()
    capture["sources"][0]["access_status"] = "UNAVAILABLE"
    with pytest.raises(validation.CommercialValidationError, match="OPENED access status"):
        validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])


def test_canonical_url_duplicate_is_rejected():
    capture = _capture()
    duplicate = dict(capture["sources"][0], source_id="src-duplicate", url="https://PUBLIC.EXAMPLE.TEST/source-1/?utm_source=x")
    capture["sources"].append(duplicate)
    with pytest.raises(validation.CommercialValidationError, match="duplicate canonical source URL"):
        validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])


def test_direct_competitor_requires_server_plugin_scope():
    capture = _capture()
    _add_fixture_competitor(capture, relation="DIRECT", product_type="Recipe datapack")
    with pytest.raises(validation.CommercialValidationError, match="DIRECT competitor is not evidenced as a plugin"):
        validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])


def test_non_plugin_context_is_adjacent_not_direct():
    capture = _capture()
    _add_fixture_competitor(capture, "Recipe datapack", relation="ADJACENT_SUBSTITUTE", product_type="Datapack")
    payload = validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])
    row = payload["commercial_competitor_offerings"][0]
    assert row["relation_type"] == "ADJACENT"
    assert row["competitor_id"] not in payload["direction_commercial_validation"][0]["direct_competitor_ids"]
    assert row["competitor_id"] in payload["direction_commercial_validation"][0]["adjacent_context_ids"]


def test_source_native_currencies_are_not_collapsed():
    capture = _capture()
    _add_fixture_competitor(capture, "Fixture paid plugin")
    evidence_id = capture["evidence"][0]["evidence_id"]
    for pricing_id, amount, currency in (("p-eur", 4.5, "EUR"), ("p-usd", 5.0, "USD")):
        capture["pricing_observations"].append({
            "pricing_id": pricing_id, "direction_id": capture["evidence"][0]["direction_id"],
            "entity_name": "Fixture paid plugin", "amount": amount, "currency_code": currency,
            "currency_symbol": None, "billing_model": "LISTED_PRICE", "period": None,
            "tier_or_scope": "Public listing", "price_context": "Current displayed price",
            "monetization_status": "PAID_VERIFIED", "evidence_ids": [evidence_id], "limitations": "No conversion.",
        })
    rows = validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])["pricing_observations"]
    assert {(row["price_amount"], row["currency"]) for row in rows} == {(4.5, "EUR"), (5.0, "USD")}


def test_direct_wtp_and_paid_precedent_remain_distinct_types():
    capture = _capture()
    ev = capture["evidence"][0]
    ev["evidence_type"] = "EXPLICIT_PURCHASE_INTENT"
    capture["direct_wtp_observations"].append({
        "wtp_id": "wtp-explicit", "direction_id": ev["direction_id"], "wtp_class": "EXPLICIT_WTP_INTENT",
        "statement": "The operator explicitly said they would pay.", "amount": None,
        "currency_code": None, "currency_symbol": None, "evidence_ids": [ev["evidence_id"]], "limitations": "Fixture only.",
    })
    capture["paid_market_precedents"].append({
        "precedent_id": "paid-listed", "direction_id": ev["direction_id"], "entity_name": "Fixture paid product",
        "precedent_type": "CURRENT_LISTED_PAID_PRICE", "amount": 8.0, "currency_code": "EUR",
        "currency_symbol": "€", "billing_model": "ONE_TIME", "evidence_ids": [ev["evidence_id"]], "limitations": "Not target WTP.",
    })
    payload = validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])
    assert payload["direct_wtp_observations"][0]["wtp_type"] == "DIRECT_WTP_STATEMENT"
    assert payload["paid_market_precedents"][0]["wtp_type"] == "PAID_COMPETITOR_PRECEDENT"


def test_buyer_observations_are_source_specific_and_not_prevalence():
    capture = _capture()
    direction_id = capture["evidence"][0]["direction_id"]
    capture["buyer_problems"].append({
        "observation_id": "bp-one", "direction_id": direction_id, "actor": "Server operator",
        "situation_problem": "Fixture context", "desired_outcome": "Fixture outcome",
        "evidence_ids": [capture["evidence"][0]["evidence_id"]], "limitations": "One source only.",
    })
    payload = validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])
    buyer = payload["buyer_problem_observations"][0]
    assert buyer["source_id"] == capture["evidence"][0]["source_id"]
    assert buyer["evidence_ids"] == [capture["evidence"][0]["evidence_id"]]
    assert buyer["evidence_strength"] in validation.EVIDENCE_STRENGTHS
    assert buyer["observation_type"] in validation.OBSERVATION_TYPES
    assert all("%" not in row["observation"] for row in payload["buyer_problem_observations"])


def test_differentiation_requires_two_urls_pain_and_competitor_fact():
    capture = _capture()
    direction_id = capture["evidence"][0]["direction_id"]
    pain = capture["evidence"][0]
    pain["evidence_type"] = "BUYER_NEED"
    second_source = dict(capture["sources"][0], source_id="src-competitor", url="https://public.example.test/competitor")
    capture["sources"].append(second_source)
    query = next(row for row in capture["queries"] if row["direction_id"] == direction_id and row["sequence"] == 2)
    query["opened_source_ids"] = [second_source["source_id"]]
    query["discovered_result_urls"] = [second_source["url"]]
    competitor_evidence = dict(pain, evidence_id="ev-competitor", source_id=second_source["source_id"],
                               query_ids=[query["query_id"]], evidence_type="COMPETITOR_FEATURES",
                               observation="A current plugin listing documents the comparable feature.")
    capture["evidence"].append(competitor_evidence)
    _add_fixture_competitor(capture, "Fixture plugin")
    capture["differentiation_observations"].append({
        "observation_id": "diff-aligned", "direction_id": direction_id, "comparison_target": "Fixture plugin",
        "feature_axis": "shared workflow", "observation": "Aligned pain and competitor evidence for fixture gate.",
        "status": "NOT_ESTABLISHED", "evidence_ids": [pain["evidence_id"], competitor_evidence["evidence_id"]],
        "limitations": "Synthetic threshold fixture.", "gap_alignment_verified": True,
    })
    payload = validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])
    row = next(row for row in payload["differentiation_observations"] if row["differentiation_id"] == "diff-aligned")
    assert row["evidence_state"] == "EVIDENCE_BACKED"


def test_unaligned_differentiation_remains_insufficient():
    capture = _capture()
    evidence_id = capture["evidence"][0]["evidence_id"]
    capture["differentiation_observations"].append({
        "observation_id": "diff-weak", "direction_id": capture["evidence"][0]["direction_id"],
        "comparison_target": "Unverified", "feature_axis": "potential feature",
        "observation": "No aligned buyer-pain and competitor evidence.", "status": "NOT_ESTABLISHED",
        "evidence_ids": [evidence_id], "limitations": "Insufficient same-gap evidence.",
    })
    payload = validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])
    assert payload["differentiation_observations"][0]["evidence_state"] == "INSUFFICIENT"


def test_dimension_enum_and_exactly_seven_per_direction():
    capture = _capture()
    capture["directions"][0]["dimension_assessments"][0]["state"] = "HIGH"
    with pytest.raises(validation.CommercialValidationError, match="invalid dimension assessment"):
        validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])
    capture = _capture()
    capture["directions"][0]["dimension_assessments"].pop()
    with pytest.raises(validation.CommercialValidationError, match="exactly seven commercial dimensions"):
        validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])


def test_direction_packs_have_no_score_ranking_or_concept_fields():
    payload = validation.normalize_capture(_capture(), _canonical_rows(), _capture()["canonical_input_hashes"])
    forbidden = {"score", "rank", "winner", "recommended_build", "product_concept", "roadmap"}
    for pack in payload["direction_commercial_validation"]:
        assert not forbidden.intersection(pack)
        assert pack["research_coverage_status"] == "PARTIAL"
        assert set(pack["evidence_gap_codes"]).issubset(validation.EVIDENCE_GAP_CODES)
