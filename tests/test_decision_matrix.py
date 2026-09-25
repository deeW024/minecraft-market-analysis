from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from market_analysis import concept_synthesis as synthesis
from market_analysis import decision_matrix as decision


ORDERS = (1, 2, 3, 4, 5, 8, 10, 11)
RANKS = {1: 1, 2: 3, 3: 4, 4: 7, 5: 8, 8: 18, 10: 22, 11: 27}
PROFILE = {
    1: ("EVIDENCED", "LIMITED_SAMPLE", "COMPLETE", "UNKNOWN — payer not identified", "DIRECT", "PAID_PRICE_VERIFIED"),
    2: ("EVIDENCED", "LIMITED_SAMPLE", "PARTIAL", "unknown payer", "ADJACENT", "PAID_PRICE_VERIFIED"),
    3: ("LIMITED_SAMPLE", "REPEATED_SAMPLE", "COMPLETE", "UNKNOWN", "DIRECT", "FREE_VERIFIED"),
    4: ("EVIDENCED", "LIMITED_SAMPLE", "PARTIAL", "Server operator explicitly identified", "SUBSTITUTE", "MONETIZED_PRICE_NOT_VISIBLE"),
    5: ("LIMITED_SAMPLE", "REPEATED_SAMPLE", "COMPLETE", "Minecraft buyer role explicitly evidenced", "ADJACENT", "FREEMIUM_PRICE_VERIFIED"),
    8: ("EVIDENCED", "LIMITED_SAMPLE", "PARTIAL", "UNKNOWN", "ADJACENT", "PAID_PRICE_VERIFIED"),
    10: ("LIMITED_SAMPLE", "LIMITED_SAMPLE", "PARTIAL", "UNKNOWN", "DIRECT", "UNKNOWN"),
    11: ("LIMITED_SAMPLE", "NOT_ESTABLISHED", "NOT_ESTABLISHED", "UNKNOWN", "DIRECT", "FREE_VERIFIED"),
}


def _canonical_json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _write_jsonl(path, rows):
    path.write_text(
        "".join(_canonical_json(row) + "\n" for row in rows),
        encoding="utf-8",
        newline="\n",
    )


def _column_type(column, json_fields, integer_fields):
    if column in integer_fields:
        return "INTEGER"
    if column in json_fields:
        return "TEXT"
    return "TEXT"


def _write_input_db(path, tables):
    with sqlite3.connect(path) as db:
        for table, columns, rows, json_fields, integer_fields in tables:
            definitions = ", ".join(
                f'"{column}" {_column_type(column, json_fields, integer_fields)}'
                for column in columns
            )
            db.execute(f'CREATE TABLE "{table}" ({definitions})')
            placeholders = ", ".join("?" for _ in columns)
            values = [
                tuple(
                    _canonical_json(row[column])
                    if isinstance(row[column], (list, dict))
                    else row[column]
                    for column in columns
                )
                for row in rows
            ]
            db.executemany(
                f'INSERT INTO "{table}" VALUES ({placeholders})',
                values,
            )


def _fixture_inputs(tmp_path, monkeypatch):
    yee57_dir = tmp_path / "yee57"
    yee55_dir = tmp_path / "yee55"
    yee57_dir.mkdir()
    yee55_dir.mkdir()
    cards = []
    readiness = []
    alternatives = []

    for order in range(1, 16):
        family_id = f"family-{order}"
        ready = order in ORDERS
        if ready:
            buyer, pain, feasibility, payer, relation, monetization = PROFILE[order]
            alt_id = f"alternative-{order}"
            card = {
                "concept_id": f"concept-{order}",
                "family_id": family_id,
                "deep_validation_order": order,
                "consensus_rank": RANKS[order],
                "canonical_topic_key": f"fixture topic {order}",
                "concept_readiness": "CONCEPT_READY",
                "target_user_role": f"Fixture user {order}",
                "payer_role": payer,
                "buyer_job_basis": [{"status": "HYPOTHESIS_NOT_POPULATION_CLAIM"}],
                "pain_basis": {
                    "family_pain_status": pain,
                    "pain_signal_summary": f"Bounded fixture status: {pain}.",
                    "clusters": [],
                },
                "commercial_signal_state": (
                    "VERIFIED_PAID_SIGNAL"
                    if monetization in decision.VERIFIED_MONETIZATION
                    else "COMPARATIVE_SIGNAL_ONLY"
                ),
                "retained_alternative_ids": [alt_id],
                "differentiation_basis": [{"hypothesis_id": f"diff-{order}"}],
                "feasibility_status": feasibility,
                "feasibility_basis": [],
                "concept_statement": f"Candidate solution hypothesis: fixture concept {order}.",
                "solution_primitives": [],
                "uncertainty_flags": ["PAYER_UNKNOWN"] if "UNKNOWN" in payer.upper() else [],
                "next_validation_questions": [],
                "evidence_refs": [],
                "synthesis_notes": "Fixture only.",
            }
            cards.append(card)
            readiness_row = {
                "family_id": family_id,
                "deep_validation_order": order,
                "consensus_rank": RANKS[order],
                "canonical_topic_key": card["canonical_topic_key"],
                "validation_cohort": "COHORT_A",
                "buyer_job_status": buyer,
                "pain_status": pain,
                "feasibility_status": feasibility,
                "retained_alternative_count": 1,
                "differentiation_count": 1,
                "commercial_signal_state": card["commercial_signal_state"],
                "concept_readiness": "CONCEPT_READY",
                "readiness_reasons": [],
            }
            alternatives.append({
                "alternative_id": alt_id,
                "family_id": family_id,
                "deep_validation_order": order,
                "entity_name": f"Fixture alternative {order}",
                "canonical_url": f"https://example.invalid/{order}",
                "relation_type": relation,
                "product_type": "fixture",
                "buyer_job": "fixture",
                "monetization_status": monetization,
                "price_amount": None,
                "price_min": None,
                "price_max": None,
                "currency": None,
                "price_unit": None,
                "pricing_notes": None,
                "evidence_ids": [f"evidence-{order}"],
            })
        else:
            rank = 40 + order
            card = None
            readiness_row = {
                "family_id": family_id,
                "deep_validation_order": order,
                "consensus_rank": rank,
                "canonical_topic_key": f"fixture topic {order}",
                "validation_cohort": "COHORT_A",
                "buyer_job_status": "NOT_ESTABLISHED",
                "pain_status": "NOT_ESTABLISHED",
                "feasibility_status": "NOT_ESTABLISHED",
                "retained_alternative_count": 0,
                "differentiation_count": 0,
                "commercial_signal_state": "NO_RETAINED_COMPARATIVE_SIGNAL",
                "concept_readiness": "NEEDS_EVIDENCE",
                "readiness_reasons": ["FIXTURE_NOT_READY"],
            }
        readiness.append(readiness_row)

    yee57_db = yee57_dir / "concept_synthesis.sqlite"
    _write_input_db(yee57_db, [
        (
            "concept_readiness_matrix",
            synthesis.READINESS_COLUMNS,
            readiness,
            synthesis.READINESS_JSON_FIELDS,
            {"deep_validation_order", "consensus_rank", "retained_alternative_count", "differentiation_count"},
        ),
        (
            "opportunity_concept_cards",
            synthesis.CONCEPT_COLUMNS,
            cards,
            synthesis.CONCEPT_JSON_FIELDS,
            {"deep_validation_order", "consensus_rank"},
        ),
    ])
    cards_path = yee57_dir / "opportunity_concept_cards.jsonl"
    readiness_path = yee57_dir / "concept_readiness_matrix.jsonl"
    _write_jsonl(cards_path, cards)
    _write_jsonl(readiness_path, readiness)

    alternatives_path = yee55_dir / "market_alternatives.jsonl"
    _write_jsonl(alternatives_path, alternatives)
    yee55_db = yee55_dir / "deep_commercial_validation.sqlite"
    alt_json_fields = {"evidence_ids"}
    alt_integer_fields = {"deep_validation_order"}
    _write_input_db(yee55_db, [
        (
            "market_alternatives",
            tuple(alternatives[0]),
            alternatives,
            alt_json_fields,
            alt_integer_fields,
        ),
    ])
    monkeypatch.setattr(decision, "YEE57_INPUT_HASHES", {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (yee57_db, cards_path, readiness_path)
    })
    monkeypatch.setattr(decision, "YEE55_INPUT_HASHES", {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (yee55_db, alternatives_path)
    })
    return yee57_dir, yee55_dir, cards, readiness, alternatives


@pytest.mark.parametrize(
    ("status", "expected"),
    [("EVIDENCED", 2), ("LIMITED_SAMPLE", 1), ("NOT_ESTABLISHED", 0), ("UNKNOWN", 0)],
)
def test_buyer_job_ordinal_mapping(status, expected):
    assert decision.buyer_job_strength(status) == expected


@pytest.mark.parametrize(
    ("status", "expected"),
    [("REPEATED_SAMPLE", 2), ("LIMITED_SAMPLE", 1), ("NOT_ESTABLISHED", 0)],
)
def test_pain_ordinal_mapping(status, expected):
    assert decision.pain_evidence_strength(status) == expected


@pytest.mark.parametrize(
    ("status", "expected"),
    [("COMPLETE", 2), ("PARTIAL", 1), ("NOT_ESTABLISHED", 0)],
)
def test_feasibility_ordinal_mapping(status, expected):
    assert decision.feasibility_evidence_strength(status) == expected


@pytest.mark.parametrize(
    ("payer_role", "expected"),
    [
        ("UNKNOWN", ("UNKNOWN", 0)),
        ("Payer unknown at this time", ("UNKNOWN", 0)),
        ("unknown buyer", ("UNKNOWN", 0)),
        ("A paying server operator is explicitly named", ("SIGNAL_PRESENT", 1)),
        ("UNKNOWNish payer", ("SIGNAL_PRESENT", 1)),
    ],
)
def test_payer_token_semantics(payer_role, expected):
    assert decision.payer_or_buyer_signal(payer_role) == expected


def _alternative(relation, monetization, alternative_id="a"):
    return {
        "alternative_id": alternative_id,
        "entity_name": "fixture",
        "canonical_url": "https://example.invalid/",
        "relation_type": relation,
        "monetization_status": monetization,
    }


@pytest.mark.parametrize(
    ("relation", "monetization", "expected_state", "expected_strength"),
    [
        ("DIRECT", "PAID_PRICE_VERIFIED", "VERIFIED_PAID_DIRECT_OR_SUBSTITUTE", 2),
        ("SUBSTITUTE", "FREEMIUM_PRICE_VERIFIED", "VERIFIED_PAID_DIRECT_OR_SUBSTITUTE", 2),
        ("ADJACENT", "MONETIZED_PRICE_NOT_VISIBLE", "VERIFIED_PAID_ADJACENT_ONLY", 1),
        ("ADJACENT", "UNKNOWN", "NO_VERIFIED_PAID_ALTERNATIVE", 0),
        ("DIRECT", "FREE_VERIFIED", "NO_VERIFIED_PAID_ALTERNATIVE", 0),
    ],
)
def test_paid_market_relation_and_monetization_semantics(
    relation, monetization, expected_state, expected_strength
):
    state, strength, evidence = decision.paid_market_proximity(
        [_alternative(relation, monetization)]
    )
    assert (state, strength) == (expected_state, expected_strength)
    assert len(evidence) == (1 if expected_strength else 0)


def _dominance_row(order, rank, values):
    return {
        "concept_id": f"c-{order}",
        "family_id": f"f-{order}",
        "deep_validation_order": order,
        "consensus_rank": rank,
        **dict(zip(decision.STRENGTH_FIELDS, values, strict=True)),
    }


def test_pareto_dominance_requires_no_worse_on_all_six_and_one_strict():
    stronger = _dominance_row(1, 1, (2, 2, 2, 2, 2))
    weaker = _dominance_row(2, 2, (1, 1, 1, 1, 1))
    assert decision.dominates(stronger, weaker)
    assert not decision.dominates(weaker, stronger)
    assert not decision.dominates(stronger, dict(stronger))

    for index in range(6):
        one_dimension_weaker = dict(stronger)
        if index == 0:
            one_dimension_weaker["consensus_rank"] = 3
        else:
            field = decision.STRENGTH_FIELDS[index - 1]
            one_dimension_weaker[field] = 0
        assert not decision.dominates(one_dimension_weaker, weaker)


def test_current_frontier_and_complete_dominator_sets_derive_from_rule(
    tmp_path, monkeypatch
):
    yee57, yee55, _, _, _ = _fixture_inputs(tmp_path, monkeypatch)
    cards = synthesis._read_jsonl(yee57 / "opportunity_concept_cards.jsonl")
    readiness = synthesis._read_jsonl(yee57 / "concept_readiness_matrix.jsonl")
    alternatives = synthesis._read_jsonl(yee55 / "market_alternatives.jsonl")
    matrix = decision._decision_matrix(cards, readiness, alternatives)
    pareto = decision.derive_pareto_dominance(matrix)
    assert [
        row["deep_validation_order"]
        for row in pareto if row["pareto_state"] == "PARETO_FRONTIER"
    ] == [1, 3, 4, 5]
    by_order = {row["deep_validation_order"]: row for row in pareto}
    assert by_order[1]["all_dominator_orders"] == []
    assert by_order[2]["all_dominator_orders"] == [1]
    assert by_order[2]["primary_dominance_witness"] == 1
    assert by_order[8]["all_dominator_orders"] == [1, 2, 4]
    assert by_order[10]["all_dominator_orders"] == [1, 2, 3, 4, 5, 8]
    assert by_order[11]["all_dominator_orders"] == [1, 2, 3, 4, 5, 8, 10]
    assert by_order[3]["primary_dominance_witness"] is None


def test_score_and_weight_fields_are_absent_from_schema():
    fields = set(decision.MATRIX_COLUMNS) | set(decision.PARETO_COLUMNS)
    assert decision._no_overall_score_fields()
    assert not fields.intersection({
        "score", "overall_score", "composite_score", "weighted_score",
        "normalized_score", "opportunity_score", "decision_score",
        "weight", "weights", "composite_weight", "normalized_aggregate",
    })
    assert Path("DECISION_SCHEMA.md").read_text(encoding="utf-8") == decision._schema_text()


def test_build_preserves_inputs_reconciles_exports_and_replays_byte_identically(
    tmp_path, monkeypatch
):
    yee57, yee55, cards, readiness, _ = _fixture_inputs(tmp_path, monkeypatch)
    source_paths = [
        *(yee57 / name for name in decision.YEE57_INPUT_HASHES),
        *(yee55 / name for name in decision.YEE55_INPUT_HASHES),
    ]
    before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in source_paths}
    out_a = tmp_path / "build-a"
    out_b = tmp_path / "build-b"
    qa = decision.build_decision_set(yee57, yee55, out_a, code_commit="fixture-commit")
    qa_b = decision.build_decision_set(yee57, yee55, out_b, code_commit="fixture-commit")

    assert qa["status"] == "PASS"
    assert qa["row_counts"] == {
        "concept_decision_matrix": 8,
        "pareto_dominance": 8,
        "supervisor_decision_set": 4,
    }
    assert qa["frontier_orders"] == [1, 3, 4, 5]
    assert qa["dominated_orders"] == [2, 8, 10, 11]
    assert all(qa["checks"].values())
    assert all(qa_b["checks"].values())
    assert [json.loads(line)["deep_validation_order"] for line in (out_a / "concept_decision_matrix.jsonl").read_text(encoding="utf-8").splitlines()] == list(ORDERS)

    matrix_rows = [
        json.loads(line)
        for line in (out_a / "concept_decision_matrix.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    card_by_id = {row["concept_id"]: row for row in cards}
    readiness_by_family = {row["family_id"]: row for row in readiness}
    for row in matrix_rows:
        assert {key: row[key] for key in card_by_id[row["concept_id"]]} == card_by_id[row["concept_id"]]
        assert all(row[key] == value for key, value in readiness_by_family[row["family_id"]].items())

    db = sqlite3.connect(out_a / "concept_decision.sqlite")
    assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert db.execute("PRAGMA foreign_key_check").fetchall() == []
    assert db.execute("SELECT count(*) FROM concept_decision_matrix").fetchone()[0] == 8
    assert db.execute("SELECT count(*) FROM pareto_dominance").fetchone()[0] == 8
    assert db.execute("SELECT count(*) FROM supervisor_decision_set").fetchone()[0] == 4
    db.close()

    assert {
        name: hashlib.sha256((out_a / name).read_bytes()).hexdigest()
        for name in decision.OUTPUT_FILES
    } == {
        name: hashlib.sha256((out_b / name).read_bytes()).hexdigest()
        for name in decision.OUTPUT_FILES
    }
    after = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in source_paths}
    assert before == after
