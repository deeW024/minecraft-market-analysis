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
        "authorized_direction_ids": ids, "sources": sources, "queries": queries,
        "evidence": evidence, "buyer_problems": [], "competitor_offerings": [],
        "pricing_observations": [], "direct_wtp_observations": [],
        "paid_market_precedents": [], "other_market_proxies": [],
        "differentiation_observations": [], "feasibility_observations": [],
        "support_burden_observations": [], "channel_observations": [],
        "directions": directions,
    }


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
    capture["pricing_observations"].append({
        "pricing_id": "p-null", "direction_id": direction_id, "entity_name": "Unknown price",
        "amount": None, "currency_code": None, "currency_symbol": None,
        "billing_model": None, "period": None, "tier_or_scope": "Not stated",
        "price_context": "No price disclosed on opened page", "monetization_status": "UNKNOWN",
        "evidence_ids": [capture["evidence"][0]["evidence_id"]], "limitations": "Not evidence of free access.",
    })
    payload = validation.normalize_capture(capture, _canonical_rows(), capture["canonical_input_hashes"])
    assert payload["pricing_observations"][0]["amount"] is None
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
