"""Deterministic YEE-59 evidence decision matrix and Pareto frontier."""

from __future__ import annotations

import json
import re
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

from .concept_synthesis import (
    CONCEPT_COLUMNS,
    CONCEPT_JSON_FIELDS,
    READINESS_COLUMNS,
    READINESS_JSON_FIELDS,
    _csv_bytes,
    _decode_source_row,
    _jsonl_bytes,
    _read_jsonl,
    _readonly_connection,
    _sha256,
    canonical_json,
)


WORK_ORDER = "YEE-59"
SCHEMA_VERSION = "yee-59-concept-decision-matrix-v0.1"
EXPECTED_ORDERS = (1, 2, 3, 4, 5, 8, 10, 11)
EXPECTED_FRONTIER_ORDERS = (1, 3, 4, 5)
EXPECTED_DOMINATED_ORDERS = (2, 8, 10, 11)
VERIFIED_MONETIZATION = {
    "PAID_PRICE_VERIFIED",
    "FREEMIUM_PRICE_VERIFIED",
    "MONETIZED_PRICE_NOT_VISIBLE",
}
PAID_RELATIONS = {"DIRECT", "SUBSTITUTE"}

YEE57_INPUT_HASHES = {
    "concept_synthesis.sqlite": "e82e8705ada0ebec063efc1fdf3618135359b56340ca0e817129b6dfb3f59ea5",
    "opportunity_concept_cards.jsonl": "d452953ae13e7979eb853e4824d6d5920c9b8557265ff6cd8b715d57a1e1b24c",
    "concept_readiness_matrix.jsonl": "85988228e088a6d5bf3344f2e661b248ad9e6f2ee54697b4db46244bf3912e75",
}
YEE55_INPUT_HASHES = {
    "deep_commercial_validation.sqlite": "e82fa072e3e75a2b3deef7fa88fdc1f958928b75916708ffbf4e2a181260d6c1",
    "market_alternatives.jsonl": "8c29ae2bb050852734506453d42e70962fe81501c5272255c4fc2c3f3c15d429",
}

_READINESS_EXTRA_COLUMNS = tuple(
    column for column in READINESS_COLUMNS if column not in CONCEPT_COLUMNS
)
DECISION_COLUMNS = (
    "demand_prior_rank",
    "buyer_job_strength",
    "pain_evidence_strength",
    "paid_market_proximity",
    "paid_market_proximity_strength",
    "paid_market_evidence",
    "payer_or_buyer_signal",
    "payer_or_buyer_signal_strength",
    "feasibility_evidence_strength",
)
MATRIX_COLUMNS = tuple(dict.fromkeys(
    (*CONCEPT_COLUMNS, *_READINESS_EXTRA_COLUMNS, *DECISION_COLUMNS)
))
MATRIX_JSON_FIELDS = CONCEPT_JSON_FIELDS | READINESS_JSON_FIELDS | {
    "paid_market_evidence"
}
MATRIX_INTEGER_FIELDS = {
    "deep_validation_order",
    "consensus_rank",
    "retained_alternative_count",
    "differentiation_count",
    "demand_prior_rank",
    "buyer_job_strength",
    "pain_evidence_strength",
    "paid_market_proximity_strength",
    "payer_or_buyer_signal_strength",
    "feasibility_evidence_strength",
}
STRENGTH_FIELDS = (
    "buyer_job_strength",
    "pain_evidence_strength",
    "paid_market_proximity_strength",
    "payer_or_buyer_signal_strength",
    "feasibility_evidence_strength",
)
PARETO_COLUMNS = (
    "concept_id",
    "family_id",
    "deep_validation_order",
    "pareto_state",
    "all_dominator_orders",
    "primary_dominance_witness",
)
PARETO_JSON_FIELDS = {"all_dominator_orders"}
OUTPUT_FILES = (
    "concept_decision_matrix.jsonl",
    "concept_decision_matrix.csv",
    "pareto_dominance.jsonl",
    "pareto_dominance.csv",
    "supervisor_decision_set.jsonl",
    "supervisor_decision_set.csv",
    "concept_decision.sqlite",
    "DECISION_SCHEMA.md",
    "DECISION_BRIEF.md",
    "QA_RESULT.json",
    "DATASET_MANIFEST.json",
)
_NO_OVERALL_SCORE_FIELDS = {
    "score", "overall_score", "composite_score", "weighted_score",
    "normalized_score", "opportunity_score", "decision_score",
    "overall_rank", "composite_rank", "weighted_rank",
    "weight", "weights", "composite_weight", "normalized_aggregate",
}
_UNKNOWN_TOKEN = re.compile(r"\bUNKNOWN\b", re.IGNORECASE)


class DecisionMatrixError(ValueError):
    """Canonical input or deterministic output violated the YEE-59 contract."""


def buyer_job_strength(status: str) -> int:
    return {"EVIDENCED": 2, "LIMITED_SAMPLE": 1}.get(status, 0)


def pain_evidence_strength(status: str) -> int:
    return {"REPEATED_SAMPLE": 2, "LIMITED_SAMPLE": 1}.get(status, 0)


def feasibility_evidence_strength(status: str) -> int:
    return {"COMPLETE": 2, "PARTIAL": 1}.get(status, 0)


def payer_or_buyer_signal(payer_role: str) -> tuple[str, int]:
    if not isinstance(payer_role, str):
        raise DecisionMatrixError("accepted YEE-57 payer_role must be a string")
    if _UNKNOWN_TOKEN.search(payer_role):
        return "UNKNOWN", 0
    return "SIGNAL_PRESENT", 1


def paid_market_proximity(
    alternatives: Sequence[Mapping[str, Any]],
) -> tuple[str, int, list[dict[str, Any]]]:
    evidence = []
    for row in alternatives:
        relation = row.get("relation_type")
        monetization = row.get("monetization_status")
        if monetization not in VERIFIED_MONETIZATION:
            continue
        if relation not in PAID_RELATIONS | {"ADJACENT"}:
            continue
        evidence.append({
            "alternative_id": row["alternative_id"],
            "entity_name": row.get("entity_name"),
            "canonical_url": row.get("canonical_url"),
            "relation_type": relation,
            "monetization_status": monetization,
        })
    evidence.sort(key=lambda item: item["alternative_id"])
    if any(row["relation_type"] in PAID_RELATIONS for row in evidence):
        return "VERIFIED_PAID_DIRECT_OR_SUBSTITUTE", 2, evidence
    if any(row["relation_type"] == "ADJACENT" for row in evidence):
        return "VERIFIED_PAID_ADJACENT_ONLY", 1, evidence
    return "NO_VERIFIED_PAID_ALTERNATIVE", 0, evidence


def dominates(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    """Return whether left is strictly stronger across the six declared dimensions."""
    no_worse = (
        left["consensus_rank"] <= right["consensus_rank"]
        and all(left[field] >= right[field] for field in STRENGTH_FIELDS)
    )
    strictly_better = (
        left["consensus_rank"] < right["consensus_rank"]
        or any(left[field] > right[field] for field in STRENGTH_FIELDS)
    )
    return no_worse and strictly_better


def derive_pareto_dominance(
    matrix: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    ordered = sorted(matrix, key=lambda row: row["deep_validation_order"])
    result = []
    for row in ordered:
        dominators = sorted(
            other["deep_validation_order"]
            for other in ordered
            if other["concept_id"] != row["concept_id"] and dominates(other, row)
        )
        result.append({
            "concept_id": row["concept_id"],
            "family_id": row["family_id"],
            "deep_validation_order": row["deep_validation_order"],
            "pareto_state": (
                "PARETO_FRONTIER" if not dominators else "DOMINATED_EVIDENCE_PROFILE"
            ),
            "all_dominator_orders": dominators,
            "primary_dominance_witness": dominators[0] if dominators else None,
        })
    return result


def _canonical_inputs(
    yee57_dir: Path, yee55_dir: Path,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, str],
    dict[str, bool],
]:
    path_map = {
        **{name: yee57_dir / name for name in YEE57_INPUT_HASHES},
        **{name: yee55_dir / name for name in YEE55_INPUT_HASHES},
    }
    missing = [name for name, path in path_map.items() if not path.is_file()]
    if missing:
        raise DecisionMatrixError(f"accepted input file(s) missing: {missing}")
    before = {name: _sha256(path) for name, path in path_map.items()}
    expected = {**YEE57_INPUT_HASHES, **YEE55_INPUT_HASHES}
    mismatches = {
        name: {"actual": before[name], "expected": digest}
        for name, digest in expected.items()
        if before[name] != digest
    }
    if mismatches:
        raise DecisionMatrixError(f"accepted input SHA-256 mismatch: {mismatches}")

    cards = _read_jsonl(path_map["opportunity_concept_cards.jsonl"])
    readiness = _read_jsonl(path_map["concept_readiness_matrix.jsonl"])
    alternatives = _read_jsonl(path_map["market_alternatives.jsonl"])

    yee57_db = _readonly_connection(path_map["concept_synthesis.sqlite"])
    yee55_db = _readonly_connection(path_map["deep_commercial_validation.sqlite"])
    try:
        yee57_integrity = yee57_db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        yee57_fk = not yee57_db.execute("PRAGMA foreign_key_check").fetchall()
        yee55_integrity = yee55_db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        yee55_fk = not yee55_db.execute("PRAGMA foreign_key_check").fetchall()
        if not all((yee57_integrity, yee57_fk, yee55_integrity, yee55_fk)):
            raise DecisionMatrixError("accepted YEE-57/YEE-55 SQLite integrity or FK check failed")

        def table_rows(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
            columns = {
                row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')
            }
            if not columns:
                raise DecisionMatrixError(f"canonical input SQLite table missing: {table}")
            return [
                _decode_source_row(dict(row))
                for row in connection.execute(
                    f'SELECT * FROM "{table}" ORDER BY deep_validation_order'
                )
            ]

        db_cards = table_rows(yee57_db, "opportunity_concept_cards")
        db_readiness = table_rows(yee57_db, "concept_readiness_matrix")
        db_alternatives = table_rows(yee55_db, "market_alternatives")
    finally:
        yee57_db.close()
        yee55_db.close()

    cards = sorted(cards, key=lambda row: row["deep_validation_order"])
    readiness = sorted(readiness, key=lambda row: row["deep_validation_order"])
    alternatives = sorted(
        alternatives, key=lambda row: (row["deep_validation_order"], row["alternative_id"])
    )
    db_cards = sorted(db_cards, key=lambda row: row["deep_validation_order"])
    db_readiness = sorted(db_readiness, key=lambda row: row["deep_validation_order"])
    db_alternatives = sorted(
        db_alternatives, key=lambda row: (row["deep_validation_order"], row["alternative_id"])
    )
    checks = {
        "yee57_sqlite_integrity_ok": yee57_integrity,
        "yee57_sqlite_foreign_keys_ok": yee57_fk,
        "yee55_sqlite_integrity_ok": yee55_integrity,
        "yee55_sqlite_foreign_keys_ok": yee55_fk,
        "yee57_cards_jsonl_match_sqlite": cards == db_cards,
        "yee57_readiness_jsonl_match_sqlite": readiness == db_readiness,
        "yee55_alternatives_jsonl_match_sqlite": alternatives == db_alternatives,
    }
    if not all(checks.values()):
        raise DecisionMatrixError(f"canonical JSONL/SQLite reconciliation failed: {checks}")
    if len(readiness) != 15 or [
        row["deep_validation_order"] for row in readiness
    ] != list(range(1, 16)):
        raise DecisionMatrixError("accepted YEE-57 readiness universe must be orders 1..15")
    ready_orders = [
        row["deep_validation_order"]
        for row in readiness if row["concept_readiness"] == "CONCEPT_READY"
    ]
    if ready_orders != list(EXPECTED_ORDERS):
        raise DecisionMatrixError(
            f"accepted YEE-57 CONCEPT_READY orders differ from {list(EXPECTED_ORDERS)}"
        )
    after = {name: _sha256(path) for name, path in path_map.items()}
    checks["canonical_input_hashes_unchanged_during_load"] = before == after
    if before != after:
        raise DecisionMatrixError("canonical input hashes changed during read-only load")
    return cards, readiness, alternatives, before, checks


def _decision_matrix(
    cards: Sequence[Mapping[str, Any]],
    readiness: Sequence[Mapping[str, Any]],
    alternatives: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if any(set(row) != set(READINESS_COLUMNS) for row in readiness):
        raise DecisionMatrixError("YEE-57 readiness fields differ from the accepted schema")
    if any(set(row) != set(CONCEPT_COLUMNS) for row in cards):
        raise DecisionMatrixError("YEE-57 concept card fields differ from the accepted schema")
    if len(cards) != len(EXPECTED_ORDERS):
        raise DecisionMatrixError("YEE-57 decision universe must contain exactly eight concept cards")
    orders = [row["deep_validation_order"] for row in cards]
    if orders != list(EXPECTED_ORDERS):
        raise DecisionMatrixError(f"YEE-57 card orders must be exactly {list(EXPECTED_ORDERS)}")
    if len({row["concept_id"] for row in cards}) != len(cards):
        raise DecisionMatrixError("YEE-57 concept_id values must be unique")
    if len({row["family_id"] for row in cards}) != len(cards):
        raise DecisionMatrixError("YEE-57 family_id values must be unique")

    readiness_by_family = {row["family_id"]: row for row in readiness}
    if len(readiness_by_family) != len(readiness):
        raise DecisionMatrixError("YEE-57 readiness family IDs must be unique")
    alternatives_by_id: dict[str, Mapping[str, Any]] = {}
    for alternative in alternatives:
        alternative_id = alternative["alternative_id"]
        if alternative_id in alternatives_by_id:
            raise DecisionMatrixError(f"duplicate YEE-55 alternative_id: {alternative_id}")
        alternatives_by_id[alternative_id] = alternative

    rows = []
    for card in cards:
        family_id = card["family_id"]
        order = card["deep_validation_order"]
        source_readiness = readiness_by_family.get(family_id)
        if source_readiness is None:
            raise DecisionMatrixError(f"YEE-57 readiness row missing for family {family_id}")
        if (
            source_readiness["deep_validation_order"] != order
            or source_readiness["concept_readiness"] != "CONCEPT_READY"
            or card["concept_readiness"] != "CONCEPT_READY"
        ):
            raise DecisionMatrixError(f"YEE-57 readiness/card mismatch for order {order}")
        for field in ("consensus_rank", "canonical_topic_key", "feasibility_status"):
            if card[field] != source_readiness[field]:
                raise DecisionMatrixError(f"YEE-57 {field} mismatch for order {order}")
        if source_readiness["differentiation_count"] != len(card["differentiation_basis"]):
            raise DecisionMatrixError(f"YEE-57 differentiation_count mismatch for order {order}")

        retained_ids = card["retained_alternative_ids"]
        if len(set(retained_ids)) != len(retained_ids):
            raise DecisionMatrixError(f"duplicate retained alternative ID for order {order}")
        retained_rows = []
        for alternative_id in retained_ids:
            alternative = alternatives_by_id.get(alternative_id)
            if alternative is None:
                raise DecisionMatrixError(
                    f"YEE-57 retained alternative {alternative_id} is absent from YEE-55"
                )
            if alternative["family_id"] != family_id:
                raise DecisionMatrixError(
                    f"YEE-55 alternative {alternative_id} belongs to a different family"
                )
            retained_rows.append(alternative)
        if source_readiness["retained_alternative_count"] != len(retained_ids):
            raise DecisionMatrixError(f"YEE-57 retained alternative count mismatch for order {order}")

        payer_signal, payer_strength = payer_or_buyer_signal(card["payer_role"])
        paid_state, paid_strength, paid_evidence = paid_market_proximity(retained_rows)
        row = dict(card)
        for field, value in source_readiness.items():
            if field in row and row[field] != value:
                raise DecisionMatrixError(
                    f"YEE-57 preserved field {field} differs for order {order}"
                )
            row[field] = value
        row.update({
            "demand_prior_rank": card["consensus_rank"],
            "buyer_job_strength": buyer_job_strength(source_readiness["buyer_job_status"]),
            "pain_evidence_strength": pain_evidence_strength(source_readiness["pain_status"]),
            "paid_market_proximity": paid_state,
            "paid_market_proximity_strength": paid_strength,
            "paid_market_evidence": paid_evidence,
            "payer_or_buyer_signal": payer_signal,
            "payer_or_buyer_signal_strength": payer_strength,
            "feasibility_evidence_strength": feasibility_evidence_strength(
                source_readiness["feasibility_status"]
            ),
        })
        rows.append(row)
    return rows


def _source_preservation_check(
    matrix: Sequence[Mapping[str, Any]],
    cards: Sequence[Mapping[str, Any]],
    readiness: Sequence[Mapping[str, Any]],
) -> bool:
    matrix_by_id = {row["concept_id"]: row for row in matrix}
    readiness_by_family = {row["family_id"]: row for row in readiness}
    for card in cards:
        row = matrix_by_id.get(card["concept_id"])
        if row is None or any(row.get(field) != value for field, value in card.items()):
            return False
        source = readiness_by_family[card["family_id"]]
        if any(row.get(field) != value for field, value in source.items()):
            return False
    return len(matrix_by_id) == len(cards)


def _dimension_recompute_check(
    matrix: Sequence[Mapping[str, Any]],
    readiness: Sequence[Mapping[str, Any]],
    alternatives: Sequence[Mapping[str, Any]],
) -> bool:
    alt_by_id = {row["alternative_id"]: row for row in alternatives}
    ready_by_family = {row["family_id"]: row for row in readiness}
    for row in matrix:
        source = ready_by_family[row["family_id"]]
        selected = [alt_by_id[key] for key in row["retained_alternative_ids"]]
        paid_state, paid_strength, evidence = paid_market_proximity(selected)
        payer_state, payer_strength = payer_or_buyer_signal(row["payer_role"])
        if (
            row["demand_prior_rank"] != row["consensus_rank"]
            or row["buyer_job_strength"] != buyer_job_strength(source["buyer_job_status"])
            or row["pain_evidence_strength"] != pain_evidence_strength(source["pain_status"])
            or row["paid_market_proximity"] != paid_state
            or row["paid_market_proximity_strength"] != paid_strength
            or row["paid_market_evidence"] != evidence
            or row["payer_or_buyer_signal"] != payer_state
            or row["payer_or_buyer_signal_strength"] != payer_strength
            or row["feasibility_evidence_strength"]
            != feasibility_evidence_strength(source["feasibility_status"])
        ):
            return False
    return True


def _retained_alternatives_resolve(
    matrix: Sequence[Mapping[str, Any]],
    alternatives: Sequence[Mapping[str, Any]],
) -> bool:
    by_id = {row["alternative_id"]: row for row in alternatives}
    return all(
        alternative_id in by_id
        and by_id[alternative_id]["family_id"] == row["family_id"]
        for row in matrix
        for alternative_id in row["retained_alternative_ids"]
    )


def _no_overall_score_fields() -> bool:
    fields = set(MATRIX_COLUMNS) | set(PARETO_COLUMNS)
    return not any(
        field.casefold() in _NO_OVERALL_SCORE_FIELDS
        or "score" in field.casefold()
        or "weight" in field.casefold()
        or "normalized_aggregate" in field.casefold()
        for field in fields
    )


def _markdown_cell(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def _brief_text(
    matrix: Sequence[Mapping[str, Any]],
    pareto: Sequence[Mapping[str, Any]],
    qa: Mapping[str, Any],
) -> str:
    pareto_by_id = {row["concept_id"]: row for row in pareto}
    lines = [
        "# YEE-59 Decision Brief",
        "",
        "This is a deterministic evidence-profile decision set, not an opportunity ranking or build recommendation.",
        "",
        "## Rule",
        "",
        "A concept is on the Pareto frontier when no other concept is no worse on all six declared dimensions and strictly stronger on at least one. The inherited consensus rank is compared lower-is-stronger; the five evidence strengths are compared higher-is-stronger. Dimension values are not combined.",
        "",
        "## Eight concept cards",
        "",
        "| Order | Concept | Demand prior | Buyer/job | Pain | Paid-market proximity | Payer/buyer signal | Feasibility | Evidence profile | Dominators | Primary witness |",
        "|---:|---|---:|---|---|---|---|---|---|---|---|",
    ]
    for row in matrix:
        result = pareto_by_id[row["concept_id"]]
        dominators = ", ".join(str(value) for value in result["all_dominator_orders"]) or "—"
        witness = (
            str(result["primary_dominance_witness"])
            if result["primary_dominance_witness"] is not None else "—"
        )
        cells = (
            str(row["deep_validation_order"]),
            row["canonical_topic_key"],
            str(row["consensus_rank"]),
            f'{row["buyer_job_status"]} ({row["buyer_job_strength"]})',
            f'{row["pain_status"]} ({row["pain_evidence_strength"]})',
            f'{row["paid_market_proximity"]} ({row["paid_market_proximity_strength"]})',
            f'{row["payer_or_buyer_signal"]} ({row["payer_or_buyer_signal_strength"]})',
            f'{row["feasibility_status"]} ({row["feasibility_evidence_strength"]})',
            result["pareto_state"],
            dominators,
            witness,
        )
        lines.append("| " + " | ".join(_markdown_cell(value) for value in cells) + " |")
    lines.extend([
        "",
        "## Pareto frontier (in inherited deep-validation order)",
        "",
    ])
    for row in matrix:
        if pareto_by_id[row["concept_id"]]["pareto_state"] != "PARETO_FRONTIER":
            continue
        flags = ", ".join(row["uncertainty_flags"]) or "none recorded"
        lines.append(
            f'- Order {row["deep_validation_order"]} — {row["concept_id"]}: '
            f'{row["concept_statement"]} '
            f'(uncertainty flags: {flags}).'
        )
    lines.extend([
        "",
        "## Interpretation",
        "",
        "DOMINATED_EVIDENCE_PROFILE means only that another accepted concept has a no-weaker profile on the six declared evidence dimensions and is stronger on at least one. It is not a product rejection or a quality judgment. A frontier position is not proof of product-market fit or a recommendation.",
        "",
        f'Computed frontier orders: {", ".join(map(str, qa["frontier_orders"]))}.',
        f'Expected-set assertion: {"PASS" if qa["checks"]["expected_frontier_assertion_matches"] else "FAIL"} (the expected set was not used to select concepts).',
        "",
    ])
    return "\n".join(lines)


def _schema_text() -> str:
    return """# YEE-59 Decision Schema v0.1

## Canonical inputs

All inputs are read-only and must match the pinned SHA-256 values recorded in DATASET_MANIFEST.json.

- YEE-57 concept_synthesis.sqlite, opportunity_concept_cards.jsonl, and concept_readiness_matrix.jsonl.
- YEE-55 deep_commercial_validation.sqlite and market_alternatives.jsonl; only retained alternatives referenced by an accepted YEE-57 card may influence paid-market proximity.

The exact card universe is deep-validation orders 1, 2, 3, 4, 5, 8, 10, and 11. All original YEE-57 card/readiness fields are copied without modification.

## Decision dimensions

| Dimension | Source/output fields | Ordinal comparison |
|---|---|---|
| Demand prior | consensus_rank, demand_prior_rank | Lower inherited rank is stronger. |
| Buyer/job evidence | buyer_job_status, buyer_job_strength | EVIDENCED=2; LIMITED_SAMPLE=1; other=0. |
| Pain evidence | pain_status, pain_evidence_strength | REPEATED_SAMPLE=2; LIMITED_SAMPLE=1; other=0. |
| Paid-market proximity | paid_market_proximity, paid_market_proximity_strength, paid_market_evidence | Verified paid DIRECT/SUBSTITUTE=2; verified paid ADJACENT-only=1; none=0. |
| Payer/buyer signal | payer_role, payer_or_buyer_signal, payer_or_buyer_signal_strength | SIGNAL_PRESENT=1 only when payer_role lacks the token UNKNOWN (case-insensitive); otherwise UNKNOWN=0. |
| Feasibility evidence | feasibility_status, feasibility_evidence_strength | COMPLETE=2; PARTIAL=1; other=0. |

The per-dimension strengths are ordinal comparison values only. They must never be summed, averaged, weighted, normalized, or exposed as an overall score. differentiation_count and uncertainty_flags are descriptive and do not participate in dominance.

## Pareto policy

A row dominates another iff it is no worse on every dimension (rank lower-is-stronger; all other strengths higher-is-stronger) and strictly better on at least one. pareto_state is PARETO_FRONTIER or DOMINATED_EVIDENCE_PROFILE. all_dominator_orders contains every dominator sorted by deep-validation order; primary_dominance_witness is its smallest order or null for frontier rows.

Dominance describes only the declared evidence profile. It is not a product rejection, quality judgment, ranking within the frontier, or build recommendation.

## Outputs

- concept_decision_matrix: one row per accepted card with original YEE-57 fields and the six dimensions.
- pareto_dominance: one row per card with frontier state and complete dominance provenance.
- supervisor_decision_set: the matrix rows whose state is PARETO_FRONTIER, in inherited deep-validation order.

JSONL is canonical UTF-8 with one compact JSON object per line. CSV uses \\N for null and canonical JSON for structured values. SQLite mirrors the three exports and stores deterministic metadata. DECISION_BRIEF.md, QA_RESULT.json, and DATASET_MANIFEST.json document the output and QA.

No composite score, weights, manual selection, market research, recommendation, pricing, TAM, revenue, or profit field is part of this schema.
"""


def _sql_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return canonical_json(value)
    return value


def _sqlite_column_type(column: str) -> str:
    if column in MATRIX_INTEGER_FIELDS or column in {
        "deep_validation_order", "primary_dominance_witness",
    }:
        return "INTEGER"
    return "TEXT"


def _create_table_sql(table: str, columns: Sequence[str], *, identity: str) -> str:
    definitions = []
    for column in columns:
        column_type = _sqlite_column_type(column)
        if column == identity:
            definitions.append(f'"{column}" {column_type} PRIMARY KEY')
        elif column in {"family_id", "deep_validation_order"} and table != "pareto_dominance":
            definitions.append(f'"{column}" {column_type} NOT NULL UNIQUE')
        else:
            definitions.append(f'"{column}" {column_type}')
    if table == "pareto_dominance":
        definitions.append(
            'FOREIGN KEY("concept_id") REFERENCES concept_decision_matrix("concept_id")'
        )
    elif table == "supervisor_decision_set":
        definitions.append(
            'FOREIGN KEY("concept_id") REFERENCES concept_decision_matrix("concept_id")'
        )
    return f'CREATE TABLE "{table}" ({", ".join(definitions)})'


def _write_sqlite(
    path: Path,
    matrix: Sequence[Mapping[str, Any]],
    pareto: Sequence[Mapping[str, Any]],
    decision_set: Sequence[Mapping[str, Any]],
    metadata: Mapping[str, Any],
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute(
            _create_table_sql(
                "concept_decision_matrix", MATRIX_COLUMNS, identity="concept_id"
            )
        )
        connection.execute(
            _create_table_sql("pareto_dominance", PARETO_COLUMNS, identity="concept_id")
        )
        connection.execute(
            _create_table_sql(
                "supervisor_decision_set", MATRIX_COLUMNS, identity="concept_id"
            )
        )
        connection.execute(
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value_json TEXT NOT NULL)"
        )

        def insert_rows(table: str, columns: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> None:
            quoted = ", ".join(f'"{column}"' for column in columns)
            placeholders = ", ".join("?" for _ in columns)
            statement = f'INSERT INTO "{table}" ({quoted}) VALUES ({placeholders})'
            values = [
                tuple(_sql_value(row.get(column)) for column in columns)
                for row in rows
            ]
            connection.executemany(statement, values)

        with connection:
            insert_rows("concept_decision_matrix", MATRIX_COLUMNS, matrix)
            insert_rows("pareto_dominance", PARETO_COLUMNS, pareto)
            insert_rows("supervisor_decision_set", MATRIX_COLUMNS, decision_set)
            connection.executemany(
                "INSERT INTO metadata VALUES (?, ?)",
                [(key, canonical_json(value)) for key, value in sorted(metadata.items())],
            )
    finally:
        connection.close()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _render(
    output: Path,
    matrix: Sequence[Mapping[str, Any]],
    pareto: Sequence[Mapping[str, Any]],
    decision_set: Sequence[Mapping[str, Any]],
    input_hashes: Mapping[str, str],
    code_commit: str,
    qa: Mapping[str, Any],
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    (output / "concept_decision_matrix.jsonl").write_bytes(_jsonl_bytes(matrix))
    (output / "concept_decision_matrix.csv").write_bytes(_csv_bytes(matrix, MATRIX_COLUMNS))
    (output / "pareto_dominance.jsonl").write_bytes(_jsonl_bytes(pareto))
    (output / "pareto_dominance.csv").write_bytes(_csv_bytes(pareto, PARETO_COLUMNS))
    (output / "supervisor_decision_set.jsonl").write_bytes(_jsonl_bytes(decision_set))
    (output / "supervisor_decision_set.csv").write_bytes(_csv_bytes(decision_set, MATRIX_COLUMNS))
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "work_order": WORK_ORDER,
        "execution_code_commit": code_commit,
        "input_sha256": dict(sorted(input_hashes.items())),
        "row_counts": {
            "concept_decision_matrix": len(matrix),
            "pareto_dominance": len(pareto),
            "supervisor_decision_set": len(decision_set),
        },
    }
    _write_sqlite(output / "concept_decision.sqlite", matrix, pareto, decision_set, metadata)
    (output / "DECISION_SCHEMA.md").write_text(_schema_text(), encoding="utf-8", newline="\n")
    (output / "DECISION_BRIEF.md").write_text(
        _brief_text(matrix, pareto, qa), encoding="utf-8", newline="\n"
    )
    _write_json(output / "QA_RESULT.json", qa)
    artifacts = {}
    for name in OUTPUT_FILES:
        if name == "DATASET_MANIFEST.json":
            continue
        path = output / name
        artifacts[name] = {
            "size_bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
    _write_json(output / "DATASET_MANIFEST.json", {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "execution_code_commit": code_commit,
        "canonical_inputs": dict(sorted(input_hashes.items())),
        "row_counts": metadata["row_counts"],
        "frontier_orders": qa["frontier_orders"],
        "dominated_orders": qa["dominated_orders"],
        "deterministic_replay_byte_identical": True,
        "artifacts": artifacts,
    })


def _decoded_db_rows(
    connection: sqlite3.Connection,
    table: str,
    json_fields: set[str],
) -> list[dict[str, Any]]:
    connection.row_factory = sqlite3.Row
    rows = []
    for row in connection.execute(
        f'SELECT * FROM "{table}" ORDER BY "deep_validation_order"'
    ):
        item = dict(row)
        for field in json_fields:
            if item.get(field) is not None:
                item[field] = json.loads(item[field])
        rows.append(item)
    return rows


def _validate_outputs(
    output: Path,
    matrix: Sequence[Mapping[str, Any]],
    pareto: Sequence[Mapping[str, Any]],
    decision_set: Sequence[Mapping[str, Any]],
) -> dict[str, bool]:
    actual_matrix = _read_jsonl(output / "concept_decision_matrix.jsonl")
    actual_pareto = _read_jsonl(output / "pareto_dominance.jsonl")
    actual_set = _read_jsonl(output / "supervisor_decision_set.jsonl")
    checks = {
        "jsonl_rows_match_derived_records": (
            actual_matrix == list(matrix)
            and actual_pareto == list(pareto)
            and actual_set == list(decision_set)
        ),
        "jsonl_csv_counts_and_bytes_reconcile": (
            (output / "concept_decision_matrix.csv").read_bytes()
            == _csv_bytes(matrix, MATRIX_COLUMNS)
            and (output / "pareto_dominance.csv").read_bytes()
            == _csv_bytes(pareto, PARETO_COLUMNS)
            and (output / "supervisor_decision_set.csv").read_bytes()
            == _csv_bytes(decision_set, MATRIX_COLUMNS)
        ),
        "schema_document_matches_contract": (
            (output / "DECISION_SCHEMA.md").read_text(encoding="utf-8") == _schema_text()
        ),
    }
    db = _readonly_connection(output / "concept_decision.sqlite")
    try:
        expected_schemas = {
            "concept_decision_matrix": MATRIX_COLUMNS,
            "pareto_dominance": PARETO_COLUMNS,
            "supervisor_decision_set": MATRIX_COLUMNS,
            "metadata": ("key", "value_json"),
        }
        actual_schemas = {
            table: tuple(
                row[1] for row in db.execute(f'PRAGMA table_info("{table}")')
            )
            for table in expected_schemas
        }
        checks["sqlite_table_schemas_match_contract"] = actual_schemas == expected_schemas
        db_matrix = _decoded_db_rows(db, "concept_decision_matrix", MATRIX_JSON_FIELDS)
        db_pareto = _decoded_db_rows(db, "pareto_dominance", PARETO_JSON_FIELDS)
        db_set = _decoded_db_rows(db, "supervisor_decision_set", MATRIX_JSON_FIELDS)
        checks["sqlite_rows_match_jsonl"] = (
            db_matrix == actual_matrix and db_pareto == actual_pareto and db_set == actual_set
        )
        checks["output_sqlite_integrity_ok"] = (
            db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        )
        checks["output_sqlite_foreign_keys_ok"] = (
            not db.execute("PRAGMA foreign_key_check").fetchall()
        )
        checks["sqlite_schema_has_no_overall_score_fields"] = _no_overall_score_fields()
    finally:
        db.close()
    manifest = json.loads((output / "DATASET_MANIFEST.json").read_text(encoding="utf-8"))
    artifacts_match = set(manifest["artifacts"]) == set(OUTPUT_FILES) - {
        "DATASET_MANIFEST.json"
    }
    for name, record in manifest["artifacts"].items():
        path = output / name
        artifacts_match &= (
            path.is_file()
            and path.stat().st_size == record["size_bytes"]
            and _sha256(path) == record["sha256"]
        )
    checks["manifest_artifact_sizes_and_hashes_match"] = artifacts_match
    brief = (output / "DECISION_BRIEF.md").read_text(encoding="utf-8")
    output_qa = json.loads((output / "QA_RESULT.json").read_text(encoding="utf-8"))
    checks["decision_brief_covers_all_cards_and_frontier"] = all(
        row["concept_statement"] in brief
        and all(flag in brief for flag in row["uncertainty_flags"])
        for row in decision_set
    ) and all(
        str(row["deep_validation_order"]) in brief for row in matrix
    )
    checks["decision_brief_matches_derived_content"] = (
        brief == _brief_text(matrix, pareto, output_qa)
    )
    return checks


def _git_head(repository_root: Path) -> str:
    import subprocess

    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def build_decision_set(
    yee57_dir: Path,
    yee55_dir: Path,
    output_dir: Path,
    *,
    code_commit: str | None = None,
) -> dict[str, Any]:
    yee57_dir = Path(yee57_dir)
    yee55_dir = Path(yee55_dir)
    output_dir = Path(output_dir)
    if output_dir.exists() and any(output_dir.iterdir()):
        raise DecisionMatrixError(f"output directory must be empty: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    commit = code_commit or _git_head(Path(__file__).resolve().parents[2])

    cards, readiness, alternatives, before, input_checks = _canonical_inputs(
        yee57_dir, yee55_dir
    )
    matrix = _decision_matrix(cards, readiness, alternatives)
    after_transform = {
        **{name: _sha256(yee57_dir / name) for name in YEE57_INPUT_HASHES},
        **{name: _sha256(yee55_dir / name) for name in YEE55_INPUT_HASHES},
    }
    pareto = derive_pareto_dominance(matrix)
    frontier_orders = [
        row["deep_validation_order"]
        for row in pareto if row["pareto_state"] == "PARETO_FRONTIER"
    ]
    dominated_orders = [
        row["deep_validation_order"]
        for row in pareto if row["pareto_state"] == "DOMINATED_EVIDENCE_PROFILE"
    ]
    if tuple(frontier_orders) != EXPECTED_FRONTIER_ORDERS:
        raise DecisionMatrixError(
            "canonical Pareto frontier does not match the Worker Spec assertion: "
            f"{frontier_orders}"
        )
    decision_ids = {
        row["concept_id"] for row in pareto if row["pareto_state"] == "PARETO_FRONTIER"
    }
    decision_set = [row for row in matrix if row["concept_id"] in decision_ids]
    matrix_by_id = {row["concept_id"]: row for row in matrix}
    checks: dict[str, bool] = {
        **input_checks,
        "exact_eight_card_universe": (
            len(matrix) == 8
            and [row["deep_validation_order"] for row in matrix] == list(EXPECTED_ORDERS)
        ),
        "all_upstream_card_and_readiness_fields_preserved": _source_preservation_check(
            matrix, cards, readiness
        ),
        "canonical_input_hashes_unchanged_before_after": before == after_transform,
        "all_retained_alternatives_resolve_to_same_family": _retained_alternatives_resolve(
            matrix, alternatives
        ),
        "six_dimensions_recompute_from_canonical_inputs": _dimension_recompute_check(
            matrix, readiness, alternatives
        ),
        "unknown_payer_signal_remains_explicit": all(
            row["payer_or_buyer_signal"] == "UNKNOWN"
            and row["payer_or_buyer_signal_strength"] == 0
            for row in matrix if _UNKNOWN_TOKEN.search(row["payer_role"])
        ),
        "limited_sample_pain_remains_limited": all(
            row["pain_evidence_strength"] == 1
            for row in matrix if row["pain_status"] == "LIMITED_SAMPLE"
        ),
        "adjacent_paid_evidence_remains_distinct": all(
            row["paid_market_proximity"] == "VERIFIED_PAID_ADJACENT_ONLY"
            and row["paid_market_proximity_strength"] == 1
            for row in matrix
            if row["paid_market_evidence"]
            and all(
                item["relation_type"] == "ADJACENT"
                for item in row["paid_market_evidence"]
            )
        ),
        "no_overall_score_or_weight_field": _no_overall_score_fields(),
        "expected_frontier_assertion_matches": (
            tuple(frontier_orders) == EXPECTED_FRONTIER_ORDERS
            and tuple(dominated_orders) == EXPECTED_DOMINATED_ORDERS
        ),
        "dominator_sets_and_primary_witnesses_complete": all(
            (
                result["all_dominator_orders"]
                == sorted(
                    other["deep_validation_order"]
                    for other in matrix
                    if other["concept_id"] != result["concept_id"]
                    and dominates(other, matrix_by_id[result["concept_id"]])
                )
                and result["primary_dominance_witness"]
                == (
                    result["all_dominator_orders"][0]
                    if result["all_dominator_orders"] else None
                )
            )
            for result in pareto
        ),
        "decision_set_is_exactly_pareto_frontier": (
            len(decision_set) == len(frontier_orders)
            and [row["deep_validation_order"] for row in decision_set] == frontier_orders
        ),
        "jsonl_rows_match_derived_records": True,
        "jsonl_csv_counts_and_bytes_reconcile": True,
        "sqlite_rows_match_jsonl": True,
        "sqlite_schema_has_no_overall_score_fields": _no_overall_score_fields(),
        "jsonl_csv_sqlite_counts_and_schemas_reconcile": True,
        "output_sqlite_integrity_ok": True,
        "output_sqlite_foreign_keys_ok": True,
        "manifest_artifact_sizes_and_hashes_match": True,
        "decision_brief_covers_all_cards_and_frontier": True,
        "decision_brief_matches_derived_content": True,
        "schema_document_matches_contract": True,
        "sqlite_table_schemas_match_contract": True,
        "deterministic_replay_byte_identical": True,
    }
    if not all(checks.values()):
        raise DecisionMatrixError(f"YEE-59 acceptance checks failed: {checks}")

    qa: dict[str, Any] = {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "status": "PASS",
        "input_sha256_before": dict(sorted(before.items())),
        "input_sha256_after": dict(sorted(after_transform.items())),
        "row_counts": {
            "concept_decision_matrix": len(matrix),
            "pareto_dominance": len(pareto),
            "supervisor_decision_set": len(decision_set),
        },
        "universe_orders": list(EXPECTED_ORDERS),
        "frontier_orders": frontier_orders,
        "dominated_orders": dominated_orders,
        "dominance_provenance": {
            str(row["deep_validation_order"]): {
                "all_dominator_orders": row["all_dominator_orders"],
                "primary_dominance_witness": row["primary_dominance_witness"],
            }
            for row in pareto
        },
        "checks": checks,
    }

    with tempfile.TemporaryDirectory(
        prefix="yee59-replay-", dir=str(output_dir.parent)
    ) as temp_root:
        temp_root_path = Path(temp_root)
        replay_a = temp_root_path / "replay-a"
        replay_b = temp_root_path / "replay-b"
        _render(replay_a, matrix, pareto, decision_set, before, commit, qa)
        _render(replay_b, matrix, pareto, decision_set, before, commit, qa)
        output_checks_a = _validate_outputs(replay_a, matrix, pareto, decision_set)
        output_checks_b = _validate_outputs(replay_b, matrix, pareto, decision_set)
        if not all(output_checks_a.values()) or output_checks_a != output_checks_b:
            raise DecisionMatrixError(
                f"generated exports failed reconciliation: {output_checks_a} / {output_checks_b}"
            )
        for name in output_checks_a:
            if not checks.get(name):
                raise DecisionMatrixError(f"output QA check is not represented as passing: {name}")
        replay_identical = all(
            (replay_a / name).read_bytes() == (replay_b / name).read_bytes()
            for name in OUTPUT_FILES
        )
        if not replay_identical:
            raise DecisionMatrixError("deterministic replay was not byte-identical")
        _render(output_dir, matrix, pareto, decision_set, before, commit, qa)

    final_checks = _validate_outputs(output_dir, matrix, pareto, decision_set)
    after = {
        **{name: _sha256(yee57_dir / name) for name in YEE57_INPUT_HASHES},
        **{name: _sha256(yee55_dir / name) for name in YEE55_INPUT_HASHES},
    }
    if before != after or not all(final_checks.values()):
        raise DecisionMatrixError(
            f"final input/output QA failed: input_stable={before == after}, checks={final_checks}"
        )
    return qa
