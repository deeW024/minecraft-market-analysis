from __future__ import annotations

import copy
import hashlib
import json
import sqlite3

import pytest

from market_analysis import commercial_validation as validation


def _fixture_capture():
    capture = {
        "capture_version": validation.CAPTURE_VERSION,
        "canonical_input_hashes": dict(validation.EXPECTED_HASHES),
        "queries": [], "opened_pages": [], "evidence": [], "families": [],
    }
    for rank, order, family_id in validation.PILOT_FAMILIES:
        prefix = f"f{order}"
        purpose_list = [
            "buyer/job validation", "buyer/job validation",
            "pain prevalence discovery", "pain prevalence discovery", "pain prevalence discovery",
            "paid alternatives/pricing", "paid alternatives/pricing", "paid alternatives/pricing",
            "feasibility support",
        ]
        for sequence, purpose in enumerate(purpose_list, 1):
            capture["queries"].append({
                "capture_id": f"{prefix}-q{sequence}", "family_id": family_id,
                "sequence": sequence, "query_text": f"fixture family {order} query {sequence}",
                "issued_at": "2026-09-25T09:00:00Z", "purpose": purpose,
                "result_action": "Fixture opened one public page or search discovery only.",
            })
        source_types = ["PRIMARY_PRODUCT", "PRIMARY_DOCS", "COMMUNITY", "COMMUNITY", "COMMUNITY",
                        "MARKETPLACE_LISTING", "PRIMARY_REPOSITORY", "PRIMARY_REPOSITORY"]
        evidence_specs = [
            ("buyer_job", "BUYER_JOB", "A documented user job."),
            ("buyer_job", "BUYER_JOB", "A documented adopter context."),
            ("pain_point", "PAIN_POINT", "An individual support observation."),
            ("pain_point", "PAIN_POINT", "A second independent support observation."),
            ("pain_point", "PAIN_POINT", "A third independent support observation."),
            ("paid_alternative", "PRICING", "A paid alternative has a source-stated price."),
            ("feasibility", "FEASIBILITY", "Supported platform context."),
            ("feasibility", "FEASIBILITY", "Documented dependency/source integration."),
        ]
        page_refs = []
        evidence_refs = []
        for number, ((dimension, claim_type, observation), source_type) in enumerate(zip(evidence_specs, source_types), 1):
            page_ref = f"{prefix}-p{number}"
            evidence_ref = f"{prefix}-e{number}"
            page_refs.append(page_ref)
            evidence_refs.append(evidence_ref)
            query_sequence = 1 if number == 1 else 2 if number == 2 else number if 3 <= number <= 5 else 6 if number == 6 else 9
            capture["opened_pages"].append({
                "capture_id": page_ref, "family_id": family_id,
                "query_ref": f"{prefix}-q{query_sequence}",
                "source_url": f"https://example.org/family-{order}/page-{number}",
                "source_title": f"Fixture source {order}-{number}", "source_type": source_type,
                "retrieved_at": "2026-09-25T09:00:00Z",
            })
            evidence = {
                "capture_id": evidence_ref, "family_id": family_id, "page_ref": page_ref,
                "dimension": dimension, "claim_type": claim_type, "observation": observation,
            }
            if number == 6:
                evidence.update({"numeric_value": 1.0, "currency": "USD", "numeric_unit": "one-time license"})
            capture["evidence"].append(evidence)
        capture["families"].append({
            "family_id": family_id, "validation_status": "COMPLETE_WITH_UNKNOWNS",
            "primary_user_role": "Minecraft player (fixture)", "buyer_or_payer_role": "UNKNOWN",
            "buyer_job_hypotheses": [
                {"actor": "player", "situation_problem": "Needs a feature.", "desired_outcome": "Use the feature.", "evidence_refs": [evidence_refs[0]]},
                {"actor": "operator", "situation_problem": "Needs deployment.", "desired_outcome": "Deploy the feature.", "evidence_refs": [evidence_refs[1]]},
            ],
            "pain_clusters": [{
                "pain_theme": "fixture issue", "evidence_refs": evidence_refs[2:5],
                "sample_status": "REPEATED_SAMPLE",
            }],
            "pain_signal_summary": "Repeated sample only; not a population estimate.",
            "paid_alternative_search_status": "ALTERNATIVES_LOCATED",
            "alternatives": [{
                "entity_name": f"Alternative {order}", "canonical_url": f"https://example.org/alternative-{order}",
                "relation_type": "SUBSTITUTE", "product_type": "Fixture alternative", "buyer_job": "Fixture job",
                "monetization_status": "PAID_PRICE_VERIFIED", "price_amount": 1.0, "currency": "USD",
                "price_unit": "one-time license", "evidence_refs": [evidence_refs[5]],
            }],
            "differentiation_hypotheses": [{
                "comparison_target": f"Alternative {order}", "feature_axis": "architecture",
                "target_cell": "EVIDENCED_PRESENT", "comparison_cell": "NOT_ESTABLISHED",
                "candidate_hypothesis": "Candidate hypothesis, not an outcome claim.",
                "target_evidence_refs": [evidence_refs[0]], "comparison_evidence_refs": [evidence_refs[5]],
                "evidence_refs": [evidence_refs[0], evidence_refs[5]],
            }],
            "feasibility_evidence_status": "COMPLETE",
            "feasibility_facts": [
                {"kind": "platform", "fact": "Fixture platform.", "evidence_refs": [evidence_refs[6]]},
                {"kind": "dependency_integration", "fact": "Fixture integration.", "evidence_refs": [evidence_refs[7]]},
                {"kind": "implementation_source", "fact": "Fixture source.", "evidence_refs": [evidence_refs[7]]},
            ],
            "research_notes": "Fixture sample only.",
        })
    return capture


def _canonical_rows():
    rows = []
    for rank, order, family_id in validation.PILOT_FAMILIES:
        rows.append({
            "family_id": family_id, "consensus_rank": rank, "deep_validation_order": order,
            "validation_cohort": "COHORT_A", "canonical_topic_key": f"topic-{rank}",
            "aliases": [], "baseline_marker": {"accepted": True, "rank": rank},
        })
    return rows


def _normalized(capture=None):
    capture = copy.deepcopy(capture or _fixture_capture())
    inherited = {family_id: [f"yee47-{number}"] for number, family_id in enumerate(validation.PILOT_IDS, 1)}
    return capture, validation._normalize_capture(capture, _canonical_rows(), inherited), inherited


def _validate_fixture_payload(capture, payload):
    output_checks = {
        "row_counts_reconcile": True, "schemas_reconcile": True,
        "sqlite_integrity_ok": True, "foreign_key_check_ok": True,
    }
    return validation._validate_payload(
        payload, _canonical_rows(), output_checks,
        validation.EXPECTED_HASHES, validation.EXPECTED_HASHES, True, capture,
    )


def test_exact_five_family_membership_and_every_upstream_field_is_preserved():
    _, payload, _ = _normalized()
    packs = payload["commercial_validation_packs"]
    assert [row["family_id"] for row in packs] == list(validation.PILOT_IDS)
    for source, pack in zip(_canonical_rows(), packs):
        assert {key: pack[key] for key in source} == source
    assert len(packs) == 5


def test_identity_conflict_does_not_emit_product_specific_commercial_inferences():
    capture = _fixture_capture()
    family = capture["families"][0]
    family.update({
        "validation_status": "IDENTITY_CONFLICT", "buyer_job_hypotheses": [],
        "pain_clusters": [], "alternatives": [], "differentiation_hypotheses": [],
        "paid_alternative_search_status": "NONE_LOCATED_IN_BOUNDED_SEARCH",
        "feasibility_evidence_status": "PARTIAL", "feasibility_facts": [],
    })
    _, payload, _ = _normalized(capture)
    row = payload["commercial_validation_packs"][0]
    assert row["validation_status"] == "IDENTITY_CONFLICT"
    assert row["buyer_job_hypotheses"] == []
    assert row["retained_alternative_ids"] == []
    assert row["differentiation_hypotheses"] == []


def test_unknown_buyer_and_missing_price_remain_explicit_unknown_not_free():
    capture = _fixture_capture()
    family = capture["families"][0]
    family["buyer_or_payer_role"] = "UNKNOWN"
    family["alternatives"][0].update({
        "monetization_status": "UNKNOWN", "price_amount": None, "currency": None, "price_unit": None,
    })
    family["paid_alternative_search_status"] = "NONE_LOCATED_IN_BOUNDED_SEARCH"
    _, payload, _ = _normalized(capture)
    pack = payload["commercial_validation_packs"][0]
    alternative = payload["market_alternatives"][0]
    assert pack["buyer_or_payer_role"] == "UNKNOWN"
    assert pack["paid_alternative_search_status"] == "NONE_LOCATED_IN_BOUNDED_SEARCH"
    assert alternative["monetization_status"] == "UNKNOWN"
    assert alternative["price_amount"] is None
    assert alternative["currency"] is None


def test_paid_price_status_requires_a_source_native_amount():
    capture = _fixture_capture()
    capture["families"][0]["alternatives"][0]["price_amount"] = None
    with pytest.raises(validation.CommercialValidationError, match="verified paid pricing"):
        _normalized(capture)


def test_repeated_pain_sample_threshold_requires_three_distinct_observations_and_pages():
    capture = _fixture_capture()
    capture["families"][0]["pain_clusters"][0]["evidence_refs"] = ["f1-e3", "f1-e4"]
    with pytest.raises(validation.CommercialValidationError, match="same-theme observation evidence"):
        _normalized(capture)


def test_family_pain_status_does_not_pool_distinct_themes():
    capture = _fixture_capture()
    family = capture["families"][0]
    family["pain_clusters"] = [
        {"pain_theme": "fixture theme one", "evidence_refs": ["f1-e3", "f1-e4"], "sample_status": "LIMITED_SAMPLE"},
        {"pain_theme": "fixture theme two", "evidence_refs": ["f1-e5"], "sample_status": "LIMITED_SAMPLE"},
    ]
    family["pain_signal_summary"] = "Two themes have limited samples; observations are not pooled across themes."
    _, payload, _ = _normalized(capture)
    pack = payload["commercial_validation_packs"][0]
    assert [row["sample_status"] for row in payload["pain_clusters"] if row["family_id"] == pack["family_id"]] == [
        "LIMITED_SAMPLE", "LIMITED_SAMPLE",
    ]
    assert pack["dimension_statuses"]["pain_prevalence_sample"] == "LIMITED_SAMPLE"


def test_sparse_bounded_search_results_pass_with_explicit_unknown_statuses():
    capture = _fixture_capture()
    family = capture["families"][0]
    family["primary_user_role"] = "Unsupported user assertion"
    family["buyer_or_payer_role"] = "Unsupported payer assertion"
    family["buyer_job_hypotheses"] = []
    family["pain_clusters"] = []
    family["pain_signal_summary"] = "NOT_ESTABLISHED after required bounded searches; no defensible observations retained."
    family["feasibility_facts"] = []
    family["feasibility_evidence_status"] = "NOT_ESTABLISHED"
    capture["evidence"] = [
        row for row in capture["evidence"]
        if row["capture_id"] not in {"f1-e1", "f1-e2", "f1-e3", "f1-e4", "f1-e5", "f1-e7", "f1-e8"}
    ]
    family["differentiation_hypotheses"] = []
    _, payload, _ = _normalized(capture)
    pack = payload["commercial_validation_packs"][0]
    assert pack["primary_user_role"] == "UNKNOWN"
    assert pack["buyer_or_payer_role"] == "UNKNOWN"
    assert pack["dimension_statuses"]["buyer_job"] == "NOT_ESTABLISHED"
    assert pack["dimension_statuses"]["pain_prevalence_sample"] == "NOT_ESTABLISHED"
    assert pack["feasibility_evidence_status"] == "NOT_ESTABLISHED"
    assert not pack["buyer_job_hypotheses"]
    assert _validate_fixture_payload(capture, payload)["status"] == "PASS"


@pytest.mark.parametrize("dimension,expected_status", [("buyer_job", "LIMITED_SAMPLE"), ("feasibility", "PARTIAL")])
def test_one_retained_buyer_or_feasibility_item_is_valid(dimension, expected_status):
    capture = _fixture_capture()
    family = capture["families"][0]
    if dimension == "buyer_job":
        capture["evidence"] = [row for row in capture["evidence"] if row["capture_id"] != "f1-e2"]
        family["buyer_job_hypotheses"] = family["buyer_job_hypotheses"][:1]
    else:
        capture["evidence"] = [row for row in capture["evidence"] if row["capture_id"] != "f1-e8"]
        family["feasibility_facts"] = family["feasibility_facts"][:1]
        family["feasibility_evidence_status"] = "PARTIAL"
    _, payload, _ = _normalized(capture)
    pack = payload["commercial_validation_packs"][0]
    field = "buyer_job" if dimension == "buyer_job" else "feasibility"
    value = pack["dimension_statuses"][field] if dimension == "buyer_job" else pack["feasibility_evidence_status"]
    assert value == expected_status
    assert _validate_fixture_payload(capture, payload)["status"] == "PASS"


def test_hosting_starting_price_and_recommended_plan_are_distinct_evidence():
    capture = _fixture_capture()
    family = capture["families"][0]
    family["alternatives"][0]["pricing_notes"] = "Advertised from $1.00/month; recommended 8 GB plan separately costs $2.00/month."
    family["alternatives"][0]["evidence_refs"].append("f1-e9")
    capture["opened_pages"].append({
        "capture_id": "f1-p9", "family_id": validation.PILOT_IDS[0], "query_ref": "f1-q6",
        "source_url": "https://example.org/hosting", "source_title": "Hosting plans",
        "source_type": "MARKETPLACE_LISTING", "retrieved_at": "2026-09-25T09:00:00Z",
    })
    capture["evidence"].append({
        "capture_id": "f1-e9", "family_id": validation.PILOT_IDS[0], "page_ref": "f1-p9",
        "dimension": "paid_alternative", "claim_type": "PRICING", "numeric_value": 2.0,
        "currency": "USD", "numeric_unit": "USD/month",
        "observation": "A separately recommended 8 GB hosting plan costs $2.00/month.",
    })
    _, payload, _ = _normalized(capture)
    alternative = payload["market_alternatives"][0]
    evidence = {row["evidence_id"]: row for row in payload["validation_evidence"]}
    assert alternative["price_amount"] == 1.0
    assert "$2.00/month" in alternative["pricing_notes"]
    assert len([eid for eid in alternative["evidence_ids"] if evidence[eid]["claim_type"] == "PRICING"]) == 2


def test_not_established_differentiation_cell_is_not_rewritten_as_unsupported():
    _, payload, _ = _normalized()
    assert payload["differentiation_hypotheses"][0]["comparison_cell"] == "NOT_ESTABLISHED"


def test_complete_feasibility_requires_platform_dependency_and_source():
    capture = _fixture_capture()
    capture["families"][0]["feasibility_facts"] = [
        item for item in capture["families"][0]["feasibility_facts"] if item["kind"] != "implementation_source"
    ]
    with pytest.raises(validation.CommercialValidationError, match="COMPLETE feasibility requires"):
        _normalized(capture)


def test_orphan_or_cross_family_evidence_reference_is_rejected():
    capture = _fixture_capture()
    capture["families"][0]["buyer_job_hypotheses"][0]["evidence_refs"] = ["not-an-evidence-id"]
    with pytest.raises(validation.CommercialValidationError, match="unknown evidence reference"):
        _normalized(capture)


def test_query_and_opened_page_budgets_are_enforced():
    capture = _fixture_capture()
    family_id = validation.PILOT_IDS[0]
    for sequence in range(10, 22):
        capture["queries"].append({
            "capture_id": f"over-budget-{sequence}", "family_id": family_id, "sequence": sequence,
            "query_text": f"budget test {sequence}", "issued_at": "2026-09-25T09:00:00Z",
            "purpose": "feasibility support", "result_action": "fixture",
        })
    with pytest.raises(validation.CommercialValidationError, match="query budget exceeded"):
        _normalized(capture)


def test_opened_page_budget_is_enforced_independently_of_query_budget():
    capture = _fixture_capture()
    family_id = validation.PILOT_IDS[0]
    for number in range(19):
        capture["opened_pages"].append({
            "capture_id": f"extra-page-{number}", "family_id": family_id,
            "query_ref": "f1-q1", "source_url": f"https://example.org/extra/{number}",
            "source_title": f"Extra page {number}", "source_type": "COMMUNITY",
            "retrieved_at": "2026-09-25T09:00:00Z",
        })
    with pytest.raises(validation.CommercialValidationError, match="opened-page budget"):
        _normalized(capture)


def test_pain_observation_cap_is_enforced():
    capture = _fixture_capture()
    family_id = validation.PILOT_IDS[0]
    for number in range(8):
        page_ref = f"f1-extra-p{number}"
        capture["opened_pages"].append({
            "capture_id": page_ref, "family_id": family_id, "query_ref": "f1-q3",
            "source_url": f"https://example.org/pain/{number}", "source_title": f"Pain report {number}",
            "source_type": "COMMUNITY", "retrieved_at": "2026-09-25T09:00:00Z",
        })
        capture["evidence"].append({
            "capture_id": f"f1-extra-e{number}", "family_id": family_id, "page_ref": page_ref,
            "dimension": "pain_point", "claim_type": "PAIN_POINT",
            "observation": f"Independent pain report {number}.",
        })
    with pytest.raises(validation.CommercialValidationError, match="ten-observation pain evidence cap"):
        _normalized(capture)


def test_deterministic_jsonl_csv_sqlite_exports_replay_byte_identically(tmp_path):
    capture, payload, _ = _normalized()
    left = tmp_path / "left"
    right = tmp_path / "right"
    hashes = dict(validation.EXPECTED_HASHES)
    left_files = validation._write_core(left, payload, capture, hashes)
    right_files = validation._write_core(right, validation._normalize_capture(capture, _canonical_rows(), {
        fid: [f"yee47-{i}"] for i, fid in enumerate(validation.PILOT_IDS, 1)
    }), capture, hashes)
    assert {path.name: validation._sha256(path) for path in left_files} == {
        path.name: validation._sha256(path) for path in right_files
    }
    checks = validation._check_outputs(left, payload)
    assert checks["sqlite_integrity_ok"] is True
    assert checks["foreign_key_check_ok"] is True
    assert checks["row_counts_reconcile"] is True
    assert checks["schemas_reconcile"] is True
    assert all(value for key, value in checks.items() if key.endswith("_reconcile"))


def test_read_only_canonical_input_hashes_remain_unchanged_and_build_finishes_on_five_rows(tmp_path, monkeypatch):
    cohort = tmp_path / "next_validation_cohort.jsonl"
    synthesis = tmp_path / "opportunity_synthesis.sqlite"
    yee47 = tmp_path / "external_market_research.sqlite"
    output = tmp_path / "artifacts"
    rows = []
    canonical = {order: (rank, family_id) for rank, order, family_id in validation.PILOT_FAMILIES}
    for order in range(1, 16):
        rank, family_id = canonical.get(order, (order, f"family-nonpilot-{order}"))
        rows.append({"family_id": family_id, "consensus_rank": rank, "deep_validation_order": order,
                     "validation_cohort": "COHORT_A", "baseline_marker": {"order": order}})
    cohort.write_text("".join(validation._canonical_json(row) + "\n" for row in rows), encoding="utf-8")
    with sqlite3.connect(synthesis) as connection:
        connection.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT)")
        connection.execute("CREATE TABLE next_validation_cohort (family_id TEXT, consensus_rank INTEGER, deep_validation_order INTEGER, validation_cohort TEXT)")
        connection.executemany("INSERT INTO next_validation_cohort VALUES (?,?,?,?)", [
            (row["family_id"], row["consensus_rank"], row["deep_validation_order"], row["validation_cohort"])
            for row in rows
        ])
    with sqlite3.connect(yee47) as connection:
        connection.execute("CREATE TABLE external_evidence (evidence_id TEXT, family_id TEXT)")
        connection.executemany("INSERT INTO external_evidence VALUES (?,?)", [
            (f"inherited-{index}", family_id) for index, family_id in enumerate(validation.PILOT_IDS)
        ])
    actual_hashes = {
        "next_validation_cohort": validation._sha256(cohort),
        "opportunity_synthesis_db": validation._sha256(synthesis),
        "yee47_external_research_db": validation._sha256(yee47),
    }
    monkeypatch.setattr(validation, "EXPECTED_HASHES", actual_hashes)
    capture = _fixture_capture()
    capture["canonical_input_hashes"] = actual_hashes
    capture_path = tmp_path / "capture.json"
    capture_path.write_text(validation._canonical_json(capture), encoding="utf-8")
    before = {path.name: validation._sha256(path) for path in (cohort, synthesis, yee47)}

    result = validation.build_pilot(cohort, synthesis, yee47, capture_path, output)

    after = {path.name: validation._sha256(path) for path in (cohort, synthesis, yee47)}
    assert result["status"] == "PASS"
    assert before == after
    assert result["qa"]["checks"]["exactly_five_authorized_families"] is True
    assert result["qa"]["checks"]["accepted_cohort_fields_preserved"] is True
    assert result["qa"]["checks"]["deterministic_replay_byte_identical"] is True
    assert result["qa"]["checks"]["all_deliverables_byte_identical_on_replay"] is True
    assert result["qa"]["checks"]["bounded_query_and_page_gates_respected"] is True
    assert len(result["qa"]["details"]["family_checks"]) == 5
    assert result["qa"]["details"]["opened_page_count_by_family"][validation.PILOT_IDS[0]] == 8
    assert result["qa"]["details"]["row_counts"]["commercial_validation_packs"] == 5
    assert json.loads((output / "QA_RESULT.json").read_text(encoding="utf-8"))["status"] == "PASS"
    manifest = json.loads((output / "DATASET_MANIFEST.json").read_text(encoding="utf-8"))
    assert {entry["file"] for entry in manifest["files"]} >= {"deep_commercial_validation.sqlite", "PILOT_REPORT.md", "QA_RESULT.json"}
