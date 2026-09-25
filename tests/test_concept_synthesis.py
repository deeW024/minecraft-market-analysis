from __future__ import annotations

import json
import sqlite3

import pytest

import market_analysis.concept_synthesis as synthesis
from market_analysis.concept_synthesis import (
    CAPTURE_VERSION,
    CONCEPT_COLUMNS,
    EXPECTED_READY_ORDERS,
    FINAL_CONCEPT_ORDERS,
    PILOT_CONCEPT_ORDERS,
    READINESS_COLUMNS,
    ConceptSynthesisError,
    _csv_bytes,
    _normalize_cards,
    _output_checks,
    _readonly_connection,
    _validate_capture,
    _write_sqlite,
    _uncertainty_and_questions,
    commercial_signal_state,
    derive_readiness_matrix,
    build_final,
    build_pilot,
)


RANK_BY_ORDER = {1: 1, 2: 3, 3: 4, 4: 7, 5: 8, 6: 9, 7: 16, 8: 18, 9: 19, 10: 22, 11: 27, 12: 28, 13: 31, 14: 32, 15: 34}


def _fixture_payload():
    ready = set(EXPECTED_READY_ORDERS)
    packs = []
    evidence = []
    clusters = []
    alternatives = []
    differentiations = []
    for order in range(1, 16):
        family_id = f"family-{order:02d}"
        if order in ready:
            buyer = "LIMITED_SAMPLE" if order == 8 else "EVIDENCED"
            pain = "REPEATED_SAMPLE" if order in {3, 5, 8, 10, 11} else "LIMITED_SAMPLE"
            feasibility = "PARTIAL" if order in {2, 4, 10} else "COMPLETE"
        elif order == 6:
            buyer, pain, feasibility = "NOT_ESTABLISHED", "NOT_ESTABLISHED", "NOT_ESTABLISHED"
        elif order == 7:
            buyer, pain, feasibility = "EVIDENCED", "NOT_ESTABLISHED", "PARTIAL"
        elif order == 9:
            buyer, pain, feasibility = "NOT_ESTABLISHED", "LIMITED_SAMPLE", "COMPLETE"
        elif order == 12:
            buyer, pain, feasibility = "EVIDENCED", "LIMITED_SAMPLE", "NOT_ESTABLISHED"
        elif order == 13:
            buyer, pain, feasibility = "EVIDENCED", "LIMITED_SAMPLE", "PARTIAL"
        elif order == 14:
            buyer, pain, feasibility = "EVIDENCED", "NOT_ESTABLISHED", "COMPLETE"
        else:
            buyer, pain, feasibility = "EVIDENCED", "LIMITED_SAMPLE", "NOT_ESTABLISHED"
        job_evidence_id = f"job-evidence-{order:02d}"
        pain_evidence_id = f"pain-evidence-{order:02d}"
        hypothesis = {
            "actor": "Minecraft player",
            "desired_outcome": f"Perform fixture job {order}.",
            "evidence_ids": [job_evidence_id],
            "situation_problem": f"Fixture situation {order}.",
            "status": "HYPOTHESIS_NOT_POPULATION_CLAIM",
        }
        evidence.extend([
            {"evidence_id": job_evidence_id, "family_id": family_id, "claim_type": "BUYER_JOB"},
            {"evidence_id": pain_evidence_id, "family_id": family_id, "claim_type": "PAIN_POINT"},
        ])
        pain_cluster = {
            "cluster_id": f"pain-cluster-{order:02d}", "family_id": family_id,
            "deep_validation_order": order, "pain_theme": "fixture pain theme",
            "sample_status": pain, "observation_count": 1, "source_page_count": 1,
            "evidence_ids": [pain_evidence_id], "sample_caveat": "Fixture only; not a population estimate.",
        }
        clusters.append(pain_cluster)
        alt_rows = []
        if order in {1, 2, 4, 5, 8, 9, 10, 12, 14}:
            monetization = {
                1: "PAID_PRICE_VERIFIED", 2: "UNKNOWN", 4: "FREE_VERIFIED",
                5: "MONETIZED_PRICE_NOT_VISIBLE", 8: "UNKNOWN", 9: "UNKNOWN",
                10: "FREEMIUM_PRICE_VERIFIED", 12: "UNKNOWN", 14: "UNKNOWN",
            }[order]
            alt = {
                "alternative_id": f"alternative-{order:02d}", "family_id": family_id,
                "deep_validation_order": order, "monetization_status": monetization,
                "evidence_ids": [job_evidence_id],
            }
            alt_rows.append(alt)
            alternatives.append(alt)
        diff_rows = []
        if order in {1, 2, 3, 4, 5, 8, 10, 11}:
            diff = {
                "hypothesis_id": f"diff-{order:02d}", "family_id": family_id,
                "deep_validation_order": order, "candidate_hypothesis": f"Fixture comparison {order}.",
                "evidence_ids": [job_evidence_id],
            }
            diff_rows.append(diff)
            differentiations.append(diff)
        packs.append({
            "family_id": family_id,
            "deep_validation_order": order,
            "consensus_rank": RANK_BY_ORDER[order],
            "canonical_topic_key": f"topic-{order:02d}",
            "validation_cohort": "COHORT_A",
            "dimension_statuses": {
                "buyer_job": buyer,
                "pain_prevalence_sample": pain,
            },
            "feasibility_evidence_status": feasibility,
            "primary_user_role": f"User role {order}.",
            "buyer_or_payer_role": "UNKNOWN — payer is not established.",
            "buyer_job_hypotheses": [hypothesis],
            "pain_signal_summary": f"{pain}: fixture pain remains bounded.",
            "pain_clusters": [pain_cluster],
            "feasibility_facts": [{"fact": f"Fixture fact {order}.", "evidence_ids": [job_evidence_id]}],
            "retained_alternative_ids": [row["alternative_id"] for row in alt_rows],
            "differentiation_hypotheses": [row["candidate_hypothesis"] for row in diff_rows],
        })
    return {
        "commercial_validation_packs": packs,
        "validation_evidence": evidence,
        "pain_clusters": clusters,
        "market_alternatives": alternatives,
        "differentiation_hypotheses": differentiations,
        "validation_queries": [],
    }


def _capture(payload, authorized_orders=PILOT_CONCEPT_ORDERS):
    families = []
    for order in authorized_orders:
        family_id = f"family-{order:02d}"
        families.append({
            "family_id": family_id,
            "concept_statement": "Candidate solution hypothesis: a family-specific flow could support the recorded player job while keeping evidence limitations visible.",
            "solution_primitives": [
                {
                    "primitive_text": "Organize the concept around the recorded player job.",
                    "basis_type": "JOB_ENABLEMENT",
                    "basis_refs": [f"job-evidence-{order:02d}"],
                },
                {
                    "primitive_text": "Treat the bounded same-theme pain reports as a validation target.",
                    "basis_type": "PAIN_RESPONSE",
                    "basis_refs": [f"pain-cluster-{order:02d}", f"pain-evidence-{order:02d}"],
                },
            ],
            "synthesis_notes": "This candidate remains a hypothesis; the bounded evidence does not establish population prevalence or a market conclusion.",
        })
    return {"capture_version": CAPTURE_VERSION, "families": families}


def test_readiness_rule_produces_exact_fifteen_family_gate_and_all_reasons():
    matrix = derive_readiness_matrix(_fixture_payload())
    assert len(matrix) == 15
    assert [row["deep_validation_order"] for row in matrix if row["concept_readiness"] == "CONCEPT_READY"] == list(EXPECTED_READY_ORDERS)
    assert [row["concept_readiness"] for row in matrix].count("NEEDS_EVIDENCE") == 7
    by_order = {row["deep_validation_order"]: row for row in matrix}
    assert by_order[6]["readiness_reasons"] == [
        "BUYER_JOB_NOT_ESTABLISHED", "PAIN_NOT_ESTABLISHED",
        "COMPARATIVE_CONTEXT_NOT_ESTABLISHED", "FEASIBILITY_NOT_ESTABLISHED",
    ]
    assert by_order[7]["readiness_reasons"] == ["PAIN_NOT_ESTABLISHED", "COMPARATIVE_CONTEXT_NOT_ESTABLISHED"]
    assert by_order[9]["readiness_reasons"] == ["BUYER_JOB_NOT_ESTABLISHED"]
    assert by_order[12]["readiness_reasons"] == ["FEASIBILITY_NOT_ESTABLISHED"]
    assert by_order[13]["readiness_reasons"] == ["COMPARATIVE_CONTEXT_NOT_ESTABLISHED"]
    assert by_order[14]["readiness_reasons"] == ["PAIN_NOT_ESTABLISHED"]
    assert by_order[15]["readiness_reasons"] == ["COMPARATIVE_CONTEXT_NOT_ESTABLISHED", "FEASIBILITY_NOT_ESTABLISHED"]


@pytest.mark.parametrize(
    ("buyer", "pain", "feasibility", "has_alternative", "has_differentiation", "expected"),
    [
        ("EVIDENCED", "LIMITED_SAMPLE", "COMPLETE", True, False, "CONCEPT_READY"),
        ("LIMITED_SAMPLE", "REPEATED_SAMPLE", "PARTIAL", False, True, "CONCEPT_READY"),
        ("NOT_ESTABLISHED", "REPEATED_SAMPLE", "COMPLETE", True, False, "NEEDS_EVIDENCE"),
        ("EVIDENCED", "NOT_ESTABLISHED", "COMPLETE", True, False, "NEEDS_EVIDENCE"),
        ("EVIDENCED", "LIMITED_SAMPLE", "NOT_ESTABLISHED", True, False, "NEEDS_EVIDENCE"),
        ("EVIDENCED", "LIMITED_SAMPLE", "PARTIAL", False, False, "NEEDS_EVIDENCE"),
    ],
)
def test_readiness_gate_truth_table(buyer, pain, feasibility, has_alternative, has_differentiation, expected):
    pack = {
        "family_id": "f", "deep_validation_order": 1, "consensus_rank": 1,
        "canonical_topic_key": "t", "validation_cohort": "COHORT_A",
        "dimension_statuses": {"buyer_job": buyer, "pain_prevalence_sample": pain},
        "feasibility_evidence_status": feasibility,
    }
    alternatives = [{"alternative_id": "a", "family_id": "f"}] if has_alternative else []
    diffs = [{"hypothesis_id": "d", "family_id": "f"}] if has_differentiation else []
    assert derive_readiness_matrix({
        "commercial_validation_packs": [pack],
        "market_alternatives": alternatives,
        "differentiation_hypotheses": diffs,
    })[0]["concept_readiness"] == expected


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("PAID_PRICE_VERIFIED", "VERIFIED_PAID_SIGNAL"),
        ("FREEMIUM_PRICE_VERIFIED", "VERIFIED_PAID_SIGNAL"),
        ("MONETIZED_PRICE_NOT_VISIBLE", "VERIFIED_PAID_SIGNAL"),
        ("UNKNOWN", "COMPARATIVE_SIGNAL_ONLY"),
        ("FREE_VERIFIED", "COMPARATIVE_SIGNAL_ONLY"),
    ],
)
def test_commercial_signal_state_classification(status, expected):
    assert commercial_signal_state([{"monetization_status": status}]) == expected


def test_no_alternative_is_no_retained_comparative_signal():
    assert commercial_signal_state([]) == "NO_RETAINED_COMPARATIVE_SIGNAL"


def test_capture_normalizes_exact_authorized_cards_and_preserves_upstream_semantics():
    payload = _fixture_payload()
    matrix = derive_readiness_matrix(payload)
    capture = _capture(payload)
    rows = _validate_capture(capture, payload, matrix)
    cards = _normalize_cards(payload, matrix, rows)
    assert [row["deep_validation_order"] for row in cards] == [1, 2, 3, 4, 5]
    by_order = {row["deep_validation_order"]: row for row in cards}
    source = payload["commercial_validation_packs"][0]
    assert by_order[1]["buyer_job_basis"] == source["buyer_job_hypotheses"]
    assert by_order[1]["target_user_role"] == source["primary_user_role"]
    assert by_order[1]["payer_role"] == source["buyer_or_payer_role"]
    assert by_order[1]["pain_basis"]["family_pain_status"] == "LIMITED_SAMPLE"
    assert by_order[1]["pain_basis"]["clusters"][0]["sample_status"] == "LIMITED_SAMPLE"
    assert by_order[1]["concept_statement"] == capture["families"][0]["concept_statement"]
    assert by_order[3]["commercial_signal_state"] == "NO_RETAINED_COMPARATIVE_SIGNAL"
    assert all("PAIN_LIMITED" in row["uncertainty_flags"] for row in cards if row["pain_basis"]["family_pain_status"] == "LIMITED_SAMPLE")


def test_capture_rejects_unauthorized_order_eight_even_when_ready():
    payload = _fixture_payload()
    capture = _capture(payload)
    extra = dict(capture["families"][0])
    extra["family_id"] = "family-08"
    capture["families"].append(extra)
    with pytest.raises(ConceptSynthesisError, match="exactly the five authorized"):
        _validate_capture(capture, payload, derive_readiness_matrix(payload))


def test_final_capture_authorizes_only_exact_ready_orders_and_preserves_pilot_entries():
    payload = _fixture_payload()
    matrix = derive_readiness_matrix(payload)
    pilot_capture = _capture(payload)
    final_capture = _capture(payload, FINAL_CONCEPT_ORDERS)
    assert final_capture["families"][:5] == pilot_capture["families"]

    capture_rows = _validate_capture(final_capture, payload, matrix, FINAL_CONCEPT_ORDERS)
    cards = _normalize_cards(payload, matrix, capture_rows, FINAL_CONCEPT_ORDERS)
    assert [row["deep_validation_order"] for row in cards] == list(FINAL_CONCEPT_ORDERS)

    unauthorized = dict(final_capture)
    unauthorized["families"] = list(final_capture["families"])
    extra = dict(final_capture["families"][0])
    extra["family_id"] = "family-06"
    unauthorized["families"].append(extra)
    with pytest.raises(ConceptSynthesisError, match="exactly the eight authorized"):
        _validate_capture(unauthorized, payload, matrix, FINAL_CONCEPT_ORDERS)


def test_final_build_replay_keeps_accepted_pilot_bytes_and_adds_only_authorized_cards(tmp_path, monkeypatch):
    payload = _fixture_payload()
    input_dir = tmp_path / "accepted-input"
    input_dir.mkdir()
    for name in synthesis.INPUT_HASHES:
        (input_dir / name).write_bytes(b"fixture-input")
    hashes = {name: synthesis._sha256(input_dir / name) for name in synthesis.INPUT_HASHES}
    checks = {
        "pinned_hashes_match": True,
        "input_sqlite_integrity_ok": True,
        "input_sqlite_foreign_keys_ok": True,
        "all_jsonl_exports_match_sqlite": True,
        "canonical_input_hashes_stable": True,
    }
    monkeypatch.setattr(synthesis, "_load_inputs", lambda _path: (payload, hashes, checks))

    pilot_dir = tmp_path / "accepted-pilot"
    pilot_capture_path = tmp_path / "pilot-capture.json"
    pilot_capture_path.write_text(synthesis.canonical_json(_capture(payload)) + "\n", encoding="utf-8")
    pilot_qa = build_pilot(input_dir, pilot_capture_path, pilot_dir)
    accepted_pilot_bytes = {
        name: (pilot_dir / name).read_bytes()
        for name in synthesis.OUTPUT_FILES
    }

    final_capture_path = tmp_path / "final-capture.json"
    final_capture_path.write_text(
        synthesis.canonical_json(_capture(payload, FINAL_CONCEPT_ORDERS)) + "\n", encoding="utf-8",
    )
    final_dir = tmp_path / "final-output"
    final_qa = build_final(input_dir, pilot_dir, final_capture_path, final_dir)

    assert pilot_qa["status"] == final_qa["status"] == "PASS"
    assert final_qa["row_counts"]["concept_readiness_matrix"] == 15
    assert final_qa["row_counts"]["opportunity_concept_cards"] == 8
    assert final_qa["concept_card_orders"] == list(FINAL_CONCEPT_ORDERS)
    assert final_qa["checks"]["accepted_pilot_orders_1_to_5_unchanged"] is True
    assert final_qa["checks"]["accepted_pilot_report_unchanged"] is True
    assert (final_dir / "PILOT_REPORT.md").read_bytes() == accepted_pilot_bytes["PILOT_REPORT.md"]
    assert all((pilot_dir / name).read_bytes() == data for name, data in accepted_pilot_bytes.items())
    assert (final_dir / "FINAL_REPORT.md").is_file()


def test_capture_rejects_cross_family_and_unknown_basis_refs():
    payload = _fixture_payload()
    matrix = derive_readiness_matrix(payload)
    capture = _capture(payload)
    capture["families"][0]["solution_primitives"][0]["basis_refs"] = ["job-evidence-08"]
    with pytest.raises(ConceptSynthesisError, match="cross-family basis ref"):
        _validate_capture(capture, payload, matrix)
    capture = _capture(payload)
    capture["families"][0]["solution_primitives"][0]["basis_refs"] = ["not-a-source-ref"]
    with pytest.raises(ConceptSynthesisError, match="unknown basis ref"):
        _validate_capture(capture, payload, matrix)


@pytest.mark.parametrize(
    ("index", "basis_type", "refs", "message"),
    [
        (0, "JOB_ENABLEMENT", ["pain-cluster-01"], "buyer/job evidence"),
        (1, "PAIN_RESPONSE", ["job-evidence-01"], "pain evidence/cluster"),
    ],
)
def test_capture_enforces_job_and_pain_basis_semantics(index, basis_type, refs, message):
    payload = _fixture_payload()
    capture = _capture(payload)
    capture["families"][0]["solution_primitives"][index]["basis_type"] = basis_type
    capture["families"][0]["solution_primitives"][index]["basis_refs"] = refs
    with pytest.raises(ConceptSynthesisError, match=message):
        _validate_capture(capture, payload, derive_readiness_matrix(payload))


def test_differentiation_direction_requires_hypothesis_and_its_evidence():
    payload = _fixture_payload()
    capture = _capture(payload)
    family = capture["families"][0]
    family["solution_primitives"][1] = {
        "primitive_text": "Use an evidenced comparison as a design direction.",
        "basis_type": "DIFFERENTIATION_DIRECTION",
        "basis_refs": ["diff-01"],
    }
    with pytest.raises(ConceptSynthesisError, match="must cite hypothesis evidence"):
        _validate_capture(capture, payload, derive_readiness_matrix(payload))


@pytest.mark.parametrize("text", [
    "Candidate solution hypothesis: this is the best option.",
    "Candidate solution hypothesis: this is a market gap.",
    "Candidate solution hypothesis: try a $9.99 price.",
])
def test_capture_rejects_forbidden_conclusions_and_numeric_pricing(text):
    payload = _fixture_payload()
    capture = _capture(payload)
    capture["families"][0]["concept_statement"] = text
    with pytest.raises(ConceptSynthesisError):
        _validate_capture(capture, payload, derive_readiness_matrix(payload))


def test_uncertainty_flags_and_validation_questions_are_deterministic_and_bounded():
    flags, questions = _uncertainty_and_questions(
        "UNKNOWN payer", "LIMITED_SAMPLE", "COMPARATIVE_SIGNAL_ONLY", "PARTIAL",
    )
    assert flags == [
        "PAYER_UNKNOWN", "PAIN_LIMITED", "PAID_SIGNAL_NOT_ESTABLISHED",
        "FEASIBILITY_PARTIAL", "COMPARATIVE_ONLY",
    ]
    assert len(questions) == 4
    assert questions == _uncertainty_and_questions(
        "UNKNOWN payer", "LIMITED_SAMPLE", "COMPARATIVE_SIGNAL_ONLY", "PARTIAL",
    )[1]


def test_accepted_sqlite_input_connection_is_read_only(tmp_path):
    path = tmp_path / "input.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE source (value TEXT)")
        connection.execute("INSERT INTO source VALUES ('accepted')")
    connection = _readonly_connection(path)
    try:
        assert connection.execute("PRAGMA query_only").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("INSERT INTO source VALUES ('mutation')")
    finally:
        connection.close()


def test_jsonl_csv_sqlite_exports_reconcile_and_replay_byte_identically(tmp_path):
    payload = _fixture_payload()
    readiness = derive_readiness_matrix(payload)
    cards = _normalize_cards(payload, readiness, _validate_capture(_capture(payload), payload, readiness))
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    for output in (first, second):
        (output / "concept_readiness_matrix.jsonl").write_bytes(
            b"".join((json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode() for row in readiness)
        )
        (output / "concept_readiness_matrix.csv").write_bytes(_csv_bytes(readiness, READINESS_COLUMNS))
        (output / "opportunity_concept_cards.jsonl").write_bytes(
            b"".join((json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode() for row in cards)
        )
        (output / "opportunity_concept_cards.csv").write_bytes(_csv_bytes(cards, CONCEPT_COLUMNS))
        _write_sqlite(output / "concept_synthesis.sqlite", readiness, cards, {"work_order": "YEE-57"})
    checks = _output_checks(first, readiness, cards)
    assert all(checks.values())
    for name in (
        "concept_readiness_matrix.jsonl", "concept_readiness_matrix.csv",
        "opportunity_concept_cards.jsonl", "opportunity_concept_cards.csv", "concept_synthesis.sqlite",
    ):
        assert (first / name).read_bytes() == (second / name).read_bytes()
