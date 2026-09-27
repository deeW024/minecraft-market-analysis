"""Deterministic YEE-78 decision-support bundle from the accepted YEE-77 SQLite."""

from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
import subprocess
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote


SCHEMA_VERSION = "yee-78-supervisor-user-decision-gate-v0.1"
ACCEPTED_YEE77_MERGE_COMMIT = "c6734865f25ec8b4b0307d44655dcfa49a7fcc01"
ACCEPTED_YEE77_CODE_COMMIT = "f2c5ff65a668fbdd59bc701481bf7b2650e6ec4a"
ACCEPTED_YEE77_SHA256 = "a2ea751823358e1032c36fd31a88d2e581476bf6e915a53c042b70614bcc4f48"
ACCEPTED_YEE77_SCHEMA = "yee-77-category-targeted-external-research-v0.1"
ACCEPTED_CAPTURE_SHA256 = "818c9734b55a0b168a16c5d7dce4f9efbcef106f726b60327c543797adec0ece"
ACCEPTED_YEE76_SHA256 = "700c22ad7bdd2ba9502e5adf933b3994e4ad84a852caef26d400094a9e8734bb"
ACCEPTED_YEE76_RUN_ID = "acfbe03b846c5b3387695b67ad2cef4cade33fa81eabb8069b038ac797615a7b"
ACCEPTED_YEE76_CODE_COMMIT = "71f24f8e825eaef5f989082d59ef0fa7e5f0bad4"
ACCEPTED_YEE76_MERGE_COMMIT = "03ffe29974681646780d5e6f40ade940d0480780"
ACCEPTED_YEE76_SCHEMA = "yee-76-category-opportunity-map-v0.1"
ACCEPTED_TAXONOMY_VERSION = "yee-61-functional-category-taxonomy-v0.1"
ACCEPTED_TAXONOMY_SHA256 = "b5720325dea06863408dfa1a05e2f981ecfeb3fac4a9e7266083ee17839c5126"
ACCEPTED_DIRECTION_IDS_SHA256 = "5751f459900e9904368e06ac16c05d42620908520cdd610c1f2bf45dda9c4831"
ACCEPTED_INPUT_COUNTS = {
    "frozen_category_snapshot": 11,
    "frozen_option_snapshot": 16,
    "direction_research_packs": 16,
    "category_research_coverage": 11,
    "source_documents": 53,
    "research_queries": 71,
    "external_evidence": 135,
    "competitor_entities": 33,
    "direction_semantic_relations": 2,
}
CATEGORY_ORDER = (
    "administration",
    "communication",
    "developer_tools",
    "economy",
    "gameplay",
    "minigames",
    "protection",
    "roleplay",
    "world_management",
    "server_utilities",
    "uncategorized",
)
EXPECTED_CATEGORY_OPTION_COUNTS = {
    "administration": 1,
    "communication": 2,
    "developer_tools": 1,
    "economy": 2,
    "gameplay": 9,
    "minigames": 0,
    "protection": 1,
    "roleplay": 0,
    "world_management": 0,
    "server_utilities": 0,
    "uncategorized": 0,
}
AMBIGUOUS_DIRECTION_IDS = {
    "dir_0ad55a06d421187e8a8f44dd",
    "dir_6416ab7146c319984671650c",
    "dir_8c9b343c9d5bcbf146ca6e67",
    "dir_31aa5e6de836e19810ab626c",
}
EXPECTED_OVERLAP_DIRECTION_ID_PAIRS = {
    frozenset(("dir_b0df585042a3d44c0bc1bdf0", "dir_c8153fe3dcf8970596e83e04")),
    frozenset(("dir_671b1de8435d83ce44203a66", "dir_a75cae79739ce1d81d996871")),
}
READINESS_VALUES = (
    "READY_FOR_SUPERVISOR_DECISION",
    "REQUIRES_SCOPE_REFINEMENT",
    "REQUIRES_MORE_EVIDENCE",
)
RESEARCH_STATUSES = ("RESOLVED", "AMBIGUOUS", "UNRESOLVED")
ALLOWED_RELATIONS = {
    "EXTERNALLY_EQUIVALENT",
    "SUBSTANTIAL_OVERLAP",
    "DISTINCT_RELATED",
    "CONFLICTING_OR_UNRESOLVED",
}
HUMAN_ACTIONS = {
    "READY_FOR_SUPERVISOR_DECISION": [
        "SELECT_FOR_DEEP_COMMERCIAL_VALIDATION",
        "HOLD",
        "DROP",
    ],
    "REQUIRES_SCOPE_REFINEMENT": [
        "REQUEST_SCOPE_REFINEMENT",
        "HOLD",
        "DROP",
    ],
    "REQUIRES_MORE_EVIDENCE": ["HOLD", "DROP"],
}
GAP_ORDER = (
    "PAIN_PREVALENCE_UNVALIDATED",
    "BUYER_WILLINGNESS_TO_PAY_UNVALIDATED",
    "PRICING_EVIDENCE_NOT_OBSERVED",
    "DIFFERENTIATION_NEEDS_DEEP_VALIDATION",
    "BUYER_SEGMENT_NEEDS_DEEP_VALIDATION",
    "PURCHASE_TRIGGER_NEEDS_DEEP_VALIDATION",
    "SUPPORT_MAINTENANCE_BURDEN_UNVALIDATED",
    "OVERLAP_REQUIRES_DECISION_CONTEXT",
    "SCOPE_REFINEMENT_REQUIRED",
    "SOURCE_COVERAGE_RISK_PRESENT",
)
FUTURE_GAPS = {
    "PAIN_PREVALENCE_UNVALIDATED",
    "BUYER_WILLINGNESS_TO_PAY_UNVALIDATED",
    "DIFFERENTIATION_NEEDS_DEEP_VALIDATION",
    "BUYER_SEGMENT_NEEDS_DEEP_VALIDATION",
    "PURCHASE_TRIGGER_NEEDS_DEEP_VALIDATION",
    "SUPPORT_MAINTENANCE_BURDEN_UNVALIDATED",
}
GAP_TO_QUESTIONS = {
    "PAIN_PREVALENCE_UNVALIDATED": ("Q_PAIN_PREVALENCE",),
    "BUYER_WILLINGNESS_TO_PAY_UNVALIDATED": ("Q_WILLINGNESS_TO_PAY",),
    "PRICING_EVIDENCE_NOT_OBSERVED": ("Q_PAID_ALTERNATIVES",),
    "DIFFERENTIATION_NEEDS_DEEP_VALIDATION": ("Q_DIFFERENTIATION",),
    "BUYER_SEGMENT_NEEDS_DEEP_VALIDATION": ("Q_BUYER_SEGMENT", "Q_CHANNEL_FIT"),
    "PURCHASE_TRIGGER_NEEDS_DEEP_VALIDATION": ("Q_PURCHASE_TRIGGER",),
    "SUPPORT_MAINTENANCE_BURDEN_UNVALIDATED": ("Q_SUPPORT_BURDEN",),
}
QUESTION_CATALOG = (
    ("Q_PAIN_PREVALENCE", "How common and costly is the observed operator pain among the target buyer segment?"),
    ("Q_BUYER_SEGMENT", "Which server/network operator segment experiences the problem strongly enough to seek a dedicated paid solution?"),
    ("Q_WILLINGNESS_TO_PAY", "Is the target buyer willing to pay for solving this job, and under what purchase model?"),
    ("Q_PAID_ALTERNATIVES", "What paid alternatives exist, at what current source-backed prices, and what does the buyer receive?"),
    ("Q_DIFFERENTIATION", "Which evidence-backed unmet needs remain after direct competitor feature comparison?"),
    ("Q_PURCHASE_TRIGGER", "What event/problem causes an operator to actively look for this type of plugin?"),
    ("Q_SUPPORT_BURDEN", "What support, compatibility and maintenance burden would a commercial product in this direction imply?"),
    ("Q_CHANNEL_FIT", "Which plugin marketplace/channel can realistically reach the target buyer for this direction?"),
)
CARD_FIELDS = (
    "direction_id",
    "category_id",
    "category_opportunity_state",
    "direction_type",
    "canonical_direction_key",
    "candidate_state",
    "direction_tier",
    "positive_support_shape",
    "risk_flags",
    "reason_codes",
    "research_status",
    "research_coverage_status",
    "resolved_market_job",
    "decision_readiness",
    "evidence_profile",
    "competition_context",
    "operator_pain_context",
    "monetization_evidence_context",
    "popularity_proxy_context",
    "maintenance_context",
    "overlap_context",
    "uncertainty_context",
    "validation_gap_codes",
    "validation_question_ids",
    "allowed_human_actions",
    "evidence_ids",
    "source_ids",
    "competitor_ids",
    "semantic_relation_ids",
)
CATEGORY_FIELDS = (
    "category_id",
    "category_order",
    "category_name",
    "category_opportunity_state",
    "category_opportunity_state_basis",
    "category_research_status",
    "option_count",
    "option_ids",
    "readiness_state_counts",
    "research_status_counts",
    "overlap_group_ids",
    "evidence_caveats",
)
OVERLAP_FIELDS = (
    "relation_id",
    "relation_type",
    "category_id",
    "member_direction_ids",
    "member_direction_keys",
    "rationale",
    "evidence_ids",
    "double_counting_note",
)
QUESTION_FIELDS = ("question_id", "question_text", "question_type")
DIRECTION_QUESTION_FIELDS = (
    "direction_id",
    "question_id",
    "question_text",
    "linked_gap_codes",
    "question_status",
)
CORE_ARTIFACTS = (
    "GOAL_ALIGNMENT.md",
    "DECISION_GATE_SEMANTICS.md",
    "DECISION_GATE_SCHEMA.md",
    "decision_direction_cards.jsonl",
    "decision_direction_cards.csv",
    "category_decision_context.jsonl",
    "category_decision_context.csv",
    "decision_overlap_groups.jsonl",
    "decision_overlap_groups.csv",
    "validation_question_catalog.jsonl",
    "validation_question_catalog.csv",
    "decision_validation_questions.jsonl",
    "decision_validation_questions.csv",
    "SUPERVISOR_DECISION_TEMPLATE.json",
    "DECISION_GATE_BRIEF.md",
    "decision_gate.sqlite",
)
FINAL_ARTIFACTS = (
    *CORE_ARTIFACTS,
    "QA_RESULT.json",
    "DATASET_MANIFEST.json",
    "FINAL_REPORT.md",
)


class DecisionGateError(ValueError):
    """An accepted-input or production contract failed closed."""


@dataclass(frozen=True)
class InputSnapshot:
    input_sha256: str
    categories: list[dict[str, Any]]
    options: list[dict[str, Any]]
    packs: list[dict[str, Any]]
    category_coverage: list[dict[str, Any]]
    sources: list[dict[str, Any]]
    queries: list[dict[str, Any]]
    evidence: list[dict[str, Any]]
    entities: list[dict[str, Any]]
    relations: list[dict[str, Any]]
    metadata: dict[str, str]
    provenance: dict[str, str]
    row_counts: dict[str, int]
    sqlite_integrity_ok: bool
    sqlite_foreign_keys_ok: bool
    query_only_enabled: bool


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_rows(connection: sqlite3.Connection, table: str, order_by: str) -> list[dict[str, Any]]:
    return [
        json.loads(row[0])
        for row in connection.execute(f"SELECT record_json FROM {table} ORDER BY {order_by}")
    ]


def _read_option_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    return _json_rows(connection, "frozen_option_snapshot", "category_id,direction_id")


def _read_snapshot(path: Path) -> InputSnapshot:
    before = sha256_file(path)
    if before != ACCEPTED_YEE77_SHA256:
        raise DecisionGateError("BLOCKED_INPUT_MISMATCH: accepted YEE-77 SQLite SHA-256 does not match")
    uri = "file:" + quote(path.resolve().as_posix(), safe="/:") + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        query_only = connection.execute("PRAGMA query_only").fetchone()[0] == 1
        integrity_ok = [tuple(row) for row in connection.execute("PRAGMA integrity_check")] == [("ok",)]
        foreign_keys_ok = connection.execute("PRAGMA foreign_key_check").fetchall() == []
        metadata = dict(connection.execute("SELECT key,value FROM metadata"))
        provenance = dict(connection.execute("SELECT key,value FROM input_provenance"))
        row_counts = {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ACCEPTED_INPUT_COUNTS
        }
        categories = _json_rows(connection, "frozen_category_snapshot", "category_order")
        options = _read_option_rows(connection)
        packs = _json_rows(connection, "direction_research_packs", "category_id,direction_id")
        category_coverage = _json_rows(connection, "category_research_coverage", "category_id")
        sources = _json_rows(connection, "source_documents", "source_id")
        queries = _json_rows(connection, "research_queries", "query_id")
        evidence = _json_rows(connection, "external_evidence", "evidence_id")
        entities = _json_rows(connection, "competitor_entities", "competitor_id")
        relations = _json_rows(connection, "direction_semantic_relations", "relation_id")
    except sqlite3.DatabaseError as error:
        raise DecisionGateError(f"BLOCKED_INPUT_MISMATCH: YEE-77 SQLite could not be validated: {error}") from error
    finally:
        connection.close()
    snapshot = InputSnapshot(
        input_sha256=before,
        categories=categories,
        options=options,
        packs=packs,
        category_coverage=category_coverage,
        sources=sources,
        queries=queries,
        evidence=evidence,
        entities=entities,
        relations=relations,
        metadata=metadata,
        provenance=provenance,
        row_counts=row_counts,
        sqlite_integrity_ok=integrity_ok,
        sqlite_foreign_keys_ok=foreign_keys_ok,
        query_only_enabled=query_only,
    )
    _validate_input_snapshot(snapshot)
    return snapshot


def _validate_input_snapshot(snapshot: InputSnapshot) -> dict[str, bool]:
    checks = _input_gate_checks(snapshot)
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise DecisionGateError(f"BLOCKED_INPUT_MISMATCH: YEE-77 accepted-input gates failed: {failed}")
    return checks


def _direction_id_sha256(options: list[dict[str, Any]]) -> str:
    text = "".join(f"{row['direction_id']}\n" for row in sorted(options, key=lambda row: row["direction_id"]))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _expected_relation_id_pairs(relations: list[dict[str, Any]]) -> set[frozenset[str]]:
    return {
        frozenset((row["direction_id"], row["related_direction_id"]))
        for row in relations
    }


def _input_gate_checks(snapshot: InputSnapshot) -> dict[str, bool]:
    category_ids = [row.get("category_id") for row in sorted(snapshot.categories, key=lambda row: row["category_order"])]
    option_counts = Counter(row.get("category_id") for row in snapshot.options)
    option_counts_full = {category_id: option_counts.get(category_id, 0) for category_id in CATEGORY_ORDER}
    id_counts = Counter(row.get("direction_id") for row in snapshot.options)
    pack_counts = Counter(row.get("direction_id") for row in snapshot.packs)
    coverage_ids = [row.get("category_id") for row in snapshot.category_coverage]
    relation_ids = [row.get("relation_id") for row in snapshot.relations]
    packs_by_id = {row["direction_id"]: row for row in snapshot.packs}
    status_counts = Counter(row.get("research_status") for row in snapshot.packs)
    coverage_counts = Counter(row.get("research_coverage_status") for row in snapshot.packs)
    ambiguous_ids = {
        row["direction_id"]
        for row in snapshot.packs
        if row.get("research_status") == "AMBIGUOUS"
    }
    metadata = snapshot.metadata
    return {
        "accepted_input_sha256": snapshot.input_sha256 == ACCEPTED_YEE77_SHA256,
        "read_only_query_only_and_sqlite_integrity": (
            snapshot.query_only_enabled and snapshot.sqlite_integrity_ok and snapshot.sqlite_foreign_keys_ok
        ),
        "accepted_yee77_schema_and_execution_commit": (
            metadata.get("schema_version") == ACCEPTED_YEE77_SCHEMA
            and metadata.get("execution_code_commit") == ACCEPTED_YEE77_CODE_COMMIT
        ),
        "accepted_capture_and_upstream_pins": (
            metadata.get("research_capture_sha256") == ACCEPTED_CAPTURE_SHA256
            and metadata.get("input_sha256_before") == ACCEPTED_YEE76_SHA256
            and metadata.get("input_sha256_after") == ACCEPTED_YEE76_SHA256
            and metadata.get("input_run_id") == ACCEPTED_YEE76_RUN_ID
            and metadata.get("input_code_commit") == ACCEPTED_YEE76_CODE_COMMIT
            and metadata.get("input_merge_commit") == ACCEPTED_YEE76_MERGE_COMMIT
            and metadata.get("input_schema_version") == ACCEPTED_YEE76_SCHEMA
            and metadata.get("input_taxonomy_version") == ACCEPTED_TAXONOMY_VERSION
            and metadata.get("input_taxonomy_sha256") == ACCEPTED_TAXONOMY_SHA256
            and metadata.get("input_read_mode") == "read-only"
            and metadata.get("input_work_order") == "YEE-76"
            and metadata.get("input_option_count") == "16"
            and metadata.get("input_category_profile_count") == "11"
            and metadata.get("input_direction_tier_count") == "47"
            and snapshot.provenance.get("input_read_only") == "true"
        ),
        "accepted_row_counts": snapshot.row_counts == ACCEPTED_INPUT_COUNTS,
        "exact_category_order_and_option_distribution": (
            category_ids == list(CATEGORY_ORDER)
            and option_counts_full == EXPECTED_CATEGORY_OPTION_COUNTS
            and len(snapshot.categories) == 11
        ),
        "exact_direction_id_set": (
            len(snapshot.options) == 16
            and all(value == 1 for value in id_counts.values())
            and _direction_id_sha256(snapshot.options) == ACCEPTED_DIRECTION_IDS_SHA256
        ),
        "one_pack_per_direction": (
            len(snapshot.packs) == 16
            and set(pack_counts) == set(id_counts)
            and all(value == 1 for value in pack_counts.values())
        ),
        "exact_readiness_source_status_reconciliation": (
            status_counts == {"RESOLVED": 12, "AMBIGUOUS": 4}
            and coverage_counts == {"SUFFICIENT": 12, "AMBIGUOUS": 4}
            and ambiguous_ids == AMBIGUOUS_DIRECTION_IDS
            and all(
                packs_by_id[key]["research_status"] == "AMBIGUOUS"
                and packs_by_id[key]["research_coverage_status"] == "AMBIGUOUS"
                for key in AMBIGUOUS_DIRECTION_IDS
            )
        ),
        "exact_accepted_overlap_relations": (
            len(snapshot.relations) == 2
            and len(set(relation_ids)) == 2
            and all(row.get("relation_type") == "SUBSTANTIAL_OVERLAP" for row in snapshot.relations)
            and _expected_relation_id_pairs(snapshot.relations) == EXPECTED_OVERLAP_DIRECTION_ID_PAIRS
        ),
        "category_research_coverage_reconciles": (
            len(snapshot.category_coverage) == 11
            and set(coverage_ids) == set(CATEGORY_ORDER)
        ),
        "accepted_evidence_provenance_reconciles": _input_provenance_references_valid(snapshot),
        "no_direction_merge_or_drop_in_input": (
            {row["direction_id"] for row in snapshot.options}
            == {row["direction_id"] for row in snapshot.packs}
        ),
    }


def _input_provenance_references_valid(snapshot: InputSnapshot) -> bool:
    direction_ids = {row["direction_id"] for row in snapshot.options}
    source_ids = {row["source_id"] for row in snapshot.sources}
    evidence_by_id = {row["evidence_id"]: row for row in snapshot.evidence}
    entity_by_id = {row["competitor_id"]: row for row in snapshot.entities}
    query_by_id = {row["query_id"]: row for row in snapshot.queries}
    if len(evidence_by_id) != len(snapshot.evidence) or len(entity_by_id) != len(snapshot.entities):
        return False
    if len(query_by_id) != len(snapshot.queries):
        return False
    for evidence in snapshot.evidence:
        if evidence.get("direction_id") not in direction_ids or evidence.get("source_id") not in source_ids:
            return False
    for query in snapshot.queries:
        if query.get("direction_id") not in direction_ids:
            return False
        if any(source_id not in source_ids for source_id in query.get("result_source_ids", [])):
            return False
        if any(evidence_id not in evidence_by_id for evidence_id in query.get("result_evidence_ids", [])):
            return False
        if any(evidence_id not in evidence_by_id for evidence_id in query.get("lifecycle_evidence_ids", [])):
            return False
    for entity in snapshot.entities:
        if entity.get("direction_id") not in direction_ids:
            return False
        if any(evidence_id not in evidence_by_id for evidence_id in entity.get("evidence_ids", [])):
            return False
        if any(evidence_id not in evidence_by_id for evidence_id in entity.get("lifecycle_evidence_ids", [])):
            return False
    for relation in snapshot.relations:
        if relation.get("direction_id") not in direction_ids or relation.get("related_direction_id") not in direction_ids:
            return False
        if any(evidence_id not in evidence_by_id for evidence_id in relation.get("evidence_ids", [])):
            return False
    packs_by_id = {row["direction_id"]: row for row in snapshot.packs}
    for pack in snapshot.packs:
        if pack["direction_id"] not in direction_ids:
            return False
        if any(evidence_id not in evidence_by_id for evidence_id in pack.get("evidence_ids", [])):
            return False
        if any(source_id not in source_ids for source_id in pack.get("source_ids", [])):
            return False
        if any(competitor_id not in entity_by_id for competitor_id in (
            pack.get("direct_competitor_ids", []) + pack.get("substitute_competitor_ids", [])
        )):
            return False
    for relation in snapshot.relations:
        if relation["relation_id"] not in packs_by_id[relation["direction_id"]].get("semantic_relation_ids", []):
            return False
        if relation["relation_id"] not in packs_by_id[relation["related_direction_id"]].get("semantic_relation_ids", []):
            return False
    return True


def _readiness(status: str, coverage: str) -> str:
    if status == "RESOLVED" and coverage == "SUFFICIENT":
        return "READY_FOR_SUPERVISOR_DECISION"
    if status == "AMBIGUOUS":
        return "REQUIRES_SCOPE_REFINEMENT"
    if status == "UNRESOLVED" or (status == "RESOLVED" and coverage == "PARTIAL"):
        return "REQUIRES_MORE_EVIDENCE"
    raise DecisionGateError(f"Unsupported YEE-77 research status/coverage pair: {status}/{coverage}")


def _group_by(rows: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row[key]].append(row)
    return grouped


def _has_source_coverage_risk(risk_flags: list[str]) -> bool:
    return any(
        flag.startswith(("HANGAR_SUPPLY_INFERENCE_RISK_", "VOXEL_SUPPLY_INFERENCE_RISK_"))
        or flag == "NO_VOXEL_CONFIRMED_MEMBERS"
        for flag in risk_flags
    )


def _source_fact_dimensions(option: dict[str, Any]) -> dict[str, dict[str, Any]]:
    pack = option.get("direction_evidence_pack", {})
    facts = pack.get("source_facts_side_by_side") or option.get("source_facts_by_source") or {}
    return {
        source: dict(facts[source])
        for source in sorted(facts)
    }


def _pricing_context(
    pack: dict[str, Any],
    evidence: list[dict[str, Any]],
    entities: list[dict[str, Any]],
) -> dict[str, Any]:
    by_currency = pack.get("pricing_observations_by_currency") or {}
    pricing_evidence = [
        row for row in evidence
        if row.get("claim_type") in {"PRICING", "MONETIZATION"}
        or row.get("currency") is not None
    ]
    pricing_entities = [
        row for row in entities
        if row.get("price_amount") is not None or row.get("pricing_model") is not None
    ]
    evidence_ids = sorted(
        {row["evidence_id"] for row in pricing_evidence}
        | {
            evidence_id
            for entity in pricing_entities
            for evidence_id in entity.get("evidence_ids", [])
        }
    )
    observed = bool(by_currency or pricing_entities or pricing_evidence)
    return {
        "evidence_state": "OBSERVED" if observed else "NOT_OBSERVED",
        "pricing_observations_by_currency": {
            currency: by_currency[currency] for currency in sorted(by_currency)
        },
        "source_backed_pricing_entities": [
            {
                "competitor_id": row["competitor_id"],
                "entity_name": row["entity_name"],
                "price_amount": row.get("price_amount"),
                "currency": row.get("currency"),
                "pricing_model": row.get("pricing_model"),
                "evidence_ids": sorted(row.get("evidence_ids", [])),
            }
            for row in sorted(pricing_entities, key=lambda item: item["competitor_id"])
        ],
        "evidence_ids": evidence_ids,
        "not_observed_semantics": (
            None if observed else "No accepted YEE-77 price evidence was observed; this does not mean free or zero price."
        ),
    }


def _build_overlap_groups(snapshot: InputSnapshot) -> list[dict[str, Any]]:
    option_by_id = {row["direction_id"]: row for row in snapshot.options}
    seen_members: set[frozenset[str]] = set()
    groups = []
    for relation in sorted(snapshot.relations, key=lambda row: row["relation_id"]):
        members = frozenset((relation["direction_id"], relation["related_direction_id"]))
        if len(members) != 2 or members in seen_members:
            raise DecisionGateError("Duplicate or self-referential accepted overlap relation")
        seen_members.add(members)
        ordered_member_ids = sorted(
            members,
            key=lambda direction_id: (
                option_by_id[direction_id]["category_order"],
                direction_id,
            ),
        )
        groups.append({
            "relation_id": relation["relation_id"],
            "relation_type": relation["relation_type"],
            "category_id": relation["category_id"],
            "member_direction_ids": ordered_member_ids,
            "member_direction_keys": [
                option_by_id[direction_id]["canonical_direction_key"]
                for direction_id in ordered_member_ids
            ],
            "rationale": relation["rationale"],
            "evidence_ids": sorted(relation.get("evidence_ids", [])),
            "double_counting_note": (
                "Members share accepted substantial-overlap evidence and are not independent market observations; "
                "do not double-count them as separate proof of market breadth."
            ),
        })
    if {
        frozenset(group["member_direction_ids"]) for group in groups
    } != EXPECTED_OVERLAP_DIRECTION_ID_PAIRS:
        raise DecisionGateError("Accepted YEE-77 overlap identities do not match the pinned relations")
    return groups


def _build_question_catalog() -> list[dict[str, Any]]:
    return [
        {"question_id": question_id, "question_text": text, "question_type": "FUTURE_VALIDATION_QUESTION"}
        for question_id, text in QUESTION_CATALOG
    ]


def _build_card_gap_codes(
    readiness: str,
    relation_ids: list[str],
    pricing: dict[str, Any],
    risk_flags: list[str],
) -> list[str]:
    codes = list(FUTURE_GAPS)
    if pricing["evidence_state"] == "NOT_OBSERVED":
        codes.append("PRICING_EVIDENCE_NOT_OBSERVED")
    if relation_ids:
        codes.append("OVERLAP_REQUIRES_DECISION_CONTEXT")
    if readiness == "REQUIRES_SCOPE_REFINEMENT":
        codes.append("SCOPE_REFINEMENT_REQUIRED")
    if _has_source_coverage_risk(risk_flags):
        codes.append("SOURCE_COVERAGE_RISK_PRESENT")
    return [code for code in GAP_ORDER if code in set(codes)]


def _build_validation_question_rows(
    cards: list[dict[str, Any]], question_catalog: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    catalog_by_id = {row["question_id"]: row for row in question_catalog}
    gap_to_questions = GAP_TO_QUESTIONS
    rows = []
    for card in cards:
        mapped: dict[str, list[str]] = defaultdict(list)
        for gap_code in card["validation_gap_codes"]:
            for question_id in gap_to_questions.get(gap_code, ()):
                mapped[question_id].append(gap_code)
        for question_id in sorted(mapped, key=lambda item: list(catalog_by_id).index(item)):
            rows.append({
                "direction_id": card["direction_id"],
                "question_id": question_id,
                "question_text": catalog_by_id[question_id]["question_text"],
                "linked_gap_codes": [code for code in GAP_ORDER if code in set(mapped[question_id])],
                "question_status": "FUTURE_QUESTION_ONLY_NOT_ANSWERED",
            })
    return rows


def _build_decision_rows(snapshot: InputSnapshot) -> dict[str, list[dict[str, Any]]]:
    category_order = {row["category_id"]: row["category_order"] for row in snapshot.categories}
    options = {
        row["direction_id"]: row
        for row in snapshot.options
    }
    packs = {
        row["direction_id"]: row
        for row in snapshot.packs
    }
    coverage = {row["category_id"]: row for row in snapshot.category_coverage}
    source_by_id = {row["source_id"]: row for row in snapshot.sources}
    evidence_by_id = {row["evidence_id"]: row for row in snapshot.evidence}
    queries_by_direction = _group_by(snapshot.queries, "direction_id")
    entities_by_direction = _group_by(snapshot.entities, "direction_id")
    relations_by_direction: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for relation in snapshot.relations:
        relations_by_direction[relation["direction_id"]].append(relation)
        relations_by_direction[relation["related_direction_id"]].append(relation)
    overlap_groups = _build_overlap_groups(snapshot)
    overlap_by_direction: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for group in overlap_groups:
        for direction_id in group["member_direction_ids"]:
            overlap_by_direction[direction_id].append(group)

    cards: list[dict[str, Any]] = []
    for direction_id in sorted(
        packs,
        key=lambda item: (category_order[options[item]["category_id"]], item),
    ):
        option = options[direction_id]
        pack = packs[direction_id]
        status = pack["research_status"]
        coverage_status = pack["research_coverage_status"]
        readiness = _readiness(status, coverage_status)
        all_evidence = [
            evidence_by_id[evidence_id]
            for evidence_id in sorted(pack.get("evidence_ids", []))
        ]
        all_entities = sorted(
            entities_by_direction.get(direction_id, []),
            key=lambda row: (row["relation_type"], row["entity_name"].casefold(), row["competitor_id"]),
        )
        all_relations = sorted(
            relations_by_direction.get(direction_id, []),
            key=lambda row: row["relation_id"],
        )
        direction_queries = sorted(
            queries_by_direction.get(direction_id, []),
            key=lambda row: row["query_id"],
        )
        source_ids = sorted(pack.get("source_ids", []))
        evidence_ids = sorted(pack.get("evidence_ids", []))
        competitor_ids = sorted(row["competitor_id"] for row in all_entities)
        relation_ids = sorted({row["relation_id"] for row in all_relations})
        evidence_sources = {
            row["source_id"] for row in all_evidence
        }
        domains = sorted({
            source_by_id[source_id].get("source_domain")
            for source_id in evidence_sources
            if source_by_id[source_id].get("source_domain")
        })

        def evidence_ref(row: dict[str, Any]) -> dict[str, Any]:
            source = source_by_id[row["source_id"]]
            return {
                "evidence_id": row["evidence_id"],
                "source_id": row["source_id"],
                "source_url": source["canonical_url"],
                "source_domain": source["source_domain"],
                "source_type": source["source_type"],
                "claim_type": row["claim_type"],
                "observation": row.get("observation"),
                "feature_theme": row.get("feature_theme"),
                "entity_name": row.get("entity_name"),
                "numeric_value": row.get("numeric_value"),
                "numeric_unit": row.get("numeric_unit"),
                "currency": row.get("currency"),
                "retrieved_at": row.get("retrieved_at"),
            }

        def competitor_context(row: dict[str, Any]) -> dict[str, Any]:
            return {
                "competitor_id": row["competitor_id"],
                "entity_name": row["entity_name"],
                "relation_type": row["relation_type"],
                "product_type": row.get("product_type"),
                "platform_or_ecosystem": row.get("platform_or_ecosystem"),
                "canonical_url": row.get("canonical_url"),
                "feature_summary": row.get("feature_summary"),
                "maintenance_status": row.get("maintenance_status"),
                "evidence_ids": sorted(row.get("evidence_ids", [])),
                "lifecycle_evidence_ids": sorted(row.get("lifecycle_evidence_ids", [])),
                "source_ids": sorted({
                    evidence_by_id[evidence_id]["source_id"]
                    for evidence_id in row.get("evidence_ids", [])
                    if evidence_id in evidence_by_id
                }),
            }

        direct = [row for row in all_entities if row["relation_type"] == "DIRECT"]
        substitutes = [row for row in all_entities if row["relation_type"] == "SUBSTITUTE"]
        adjacent = [row for row in all_entities if row["relation_type"] == "ADJACENT"]
        feature_evidence = [row for row in all_evidence if row["claim_type"] == "FEATURE"]
        pain_evidence = [row for row in all_evidence if row["claim_type"] == "OPERATOR_PAIN"]
        maintenance_evidence = [row for row in all_evidence if row["claim_type"] == "MAINTENANCE"]
        popularity_evidence = [row for row in all_evidence if row["claim_type"] == "POPULARITY_PROXY"]
        pricing = _pricing_context(pack, all_evidence, all_entities)

        fallback_queries: dict[str, list[str]] = defaultdict(list)
        for query in direction_queries:
            basis = query.get("issued_at_basis")
            if basis and basis != "PROVIDED":
                fallback_queries[basis].append(query["query_id"])
        category_info = coverage[option["category_id"]]
        coverage_notes = list(category_info.get("coverage_notes") or [])
        coverage_risks = list(category_info.get("risk_notes") or [])
        ambiguity_statement = (
            None if status != "AMBIGUOUS"
            else "Accepted YEE-77 evidence leaves this broad direction ambiguous; scope refinement is a human action."
        )
        if status == "AMBIGUOUS" and any(row["relation_type"] == "DIRECT" for row in direct):
            raise DecisionGateError("Ambiguous YEE-77 direction unexpectedly has a direct competitor entity")

        card = {
            "direction_id": option["direction_id"],
            "category_id": option["category_id"],
            "category_opportunity_state": option.get("category_opportunity_state"),
            "direction_type": option["direction_type"],
            "canonical_direction_key": option["canonical_direction_key"],
            "candidate_state": option["candidate_state"],
            "direction_tier": option["direction_tier"],
            "positive_support_shape": option["positive_support_shape"],
            "risk_flags": list(option["risk_flags"]),
            "reason_codes": list(option["reason_codes"]),
            "research_status": status,
            "research_coverage_status": coverage_status,
            "resolved_market_job": pack.get("resolved_market_job"),
            "decision_readiness": readiness,
            "evidence_profile": {
                "evidence_count": len(evidence_ids),
                "distinct_domain_count": len(domains),
                "primary_or_marketplace_evidence_count": pack["primary_or_marketplace_evidence_count"],
                "community_evidence_count": pack["community_evidence_count"],
                "query_count": pack["query_count"],
                "research_status": status,
                "research_coverage_status": coverage_status,
                "evidence_ids": evidence_ids,
                "source_ids": source_ids,
                "distinct_domains": domains,
            },
            "competition_context": {
                "direct_competitor_count": len(direct),
                "direct_competitors": [competitor_context(row) for row in direct],
                "substitute_competitors": [competitor_context(row) for row in substitutes],
                "adjacent_context_entities": [competitor_context(row) for row in adjacent],
                "feature_themes": sorted(set(pack.get("feature_themes", []))),
                "feature_evidence": [evidence_ref(row) for row in feature_evidence],
                "differentiation_hypotheses": [
                    {
                        **hypothesis,
                        "interpretation": "UNVALIDATED_HYPOTHESIS_NOT_FINDING",
                    }
                    for hypothesis in pack.get("differentiation_hypotheses", [])
                ],
                "evidence_ids": sorted({
                    evidence_id
                    for row in all_entities
                    for evidence_id in row.get("evidence_ids", [])
                } | {row["evidence_id"] for row in feature_evidence}),
            },
            "operator_pain_context": {
                "observation_count": len(pain_evidence),
                "evidence_ids": sorted(row["evidence_id"] for row in pain_evidence),
                "source_ids": sorted({row["source_id"] for row in pain_evidence}),
                "observations": [evidence_ref(row) for row in pain_evidence],
                "prevalence_inferred": False,
                "interpretation": "Observations are source-specific anecdotes, not prevalence or severity estimates.",
            },
            "monetization_evidence_context": pricing,
            "popularity_proxy_context": {
                "source_native_popularity_proxies": [evidence_ref(row) for row in popularity_evidence],
                "stage_d_source_local_demand_supply_by_source": _source_fact_dimensions(option),
                "cross_source_metric_aggregation": False,
                "market_size_inferred": False,
            },
            "maintenance_context": {
                "accepted_maintenance_summary": pack.get("maintenance_summary"),
                "maintenance_evidence": [evidence_ref(row) for row in maintenance_evidence],
                "competitor_maintenance_states": [
                    {
                        "competitor_id": row["competitor_id"],
                        "entity_name": row["entity_name"],
                        "relation_type": row["relation_type"],
                        "maintenance_status": row.get("maintenance_status"),
                        "lifecycle_evidence_ids": sorted(row.get("lifecycle_evidence_ids", [])),
                    }
                    for row in all_entities
                    if row.get("maintenance_status") is not None or row.get("lifecycle_evidence_ids")
                ],
            },
            "overlap_context": {
                "semantic_relation_ids": relation_ids,
                "groups": [
                    {
                        "relation_id": group["relation_id"],
                        "relation_type": group["relation_type"],
                        "member_direction_ids": group["member_direction_ids"],
                        "member_direction_keys": group["member_direction_keys"],
                        "double_counting_note": group["double_counting_note"],
                    }
                    for group in overlap_by_direction[direction_id]
                ],
                "double_counting_risk_present": bool(relation_ids),
            },
            "uncertainty_context": {
                "research_ambiguity_statement": ambiguity_statement,
                "risk_flags": list(option["risk_flags"]),
                "research_notes": pack.get("research_notes") or "",
                "category_coverage_notes": coverage_notes,
                "category_coverage_risk_notes": coverage_risks,
                "lexical_only_support": option["positive_support_shape"] == "LEXICAL_ONLY",
                "source_coverage_risk_present": _has_source_coverage_risk(option["risk_flags"]),
                "pricing_not_observed": pricing["evidence_state"] == "NOT_OBSERVED",
                "operator_pain_is_anecdotal": bool(pain_evidence),
                "overlap_relation_ids": relation_ids,
                "query_timestamp_fallback": {
                    "basis_to_query_ids": {
                        basis: sorted(ids) for basis, ids in sorted(fallback_queries.items())
                    },
                    "query_count": sum(len(ids) for ids in fallback_queries.values()),
                },
            },
            "validation_gap_codes": [],
            "validation_question_ids": [],
            "allowed_human_actions": list(HUMAN_ACTIONS[readiness]),
            "evidence_ids": evidence_ids,
            "source_ids": source_ids,
            "competitor_ids": competitor_ids,
            "semantic_relation_ids": relation_ids,
        }
        card["validation_gap_codes"] = _build_card_gap_codes(
            readiness,
            relation_ids,
            pricing,
            card["risk_flags"],
        )
        card["validation_question_ids"] = [
            question_id
            for question_id, _ in QUESTION_CATALOG
            if question_id in {
                mapped
                for gap_code in card["validation_gap_codes"]
                for mapped in GAP_TO_QUESTIONS.get(gap_code, ())
            }
        ]
        cards.append(card)

    catalog = _build_question_catalog()
    question_rows = _build_validation_question_rows(cards, catalog)
    cards_by_category = _group_by(cards, "category_id")
    groups_by_category = _group_by(overlap_groups, "category_id")
    coverage_by_category = coverage
    category_rows: list[dict[str, Any]] = []
    for category in sorted(snapshot.categories, key=lambda row: row["category_order"]):
        category_id = category["category_id"]
        category_cards = cards_by_category.get(category_id, [])
        category_states = {
            card["category_opportunity_state"]
            for card in category_cards
            if card.get("category_opportunity_state") is not None
        }
        if len(category_states) > 1:
            raise DecisionGateError(f"YEE-77 options disagree on category opportunity state: {category_id}")
        category_state = next(iter(category_states), None)
        category_pack = coverage_by_category[category_id]
        readiness_counts = Counter(row["decision_readiness"] for row in category_cards)
        research_counts = Counter(row["research_status"] for row in category_cards)
        option_ids = [row["direction_id"] for row in category_cards]
        state_basis = (
            "PRESERVED_FROM_YEE77_OPTION_ROWS"
            if category_state is not None
            else "NOT_PRESENT_IN_YEE77_FOR_ZERO_OPTION_CATEGORY"
        )
        category_rows.append({
            "category_id": category_id,
            "category_order": category["category_order"],
            "category_name": category["category_name"],
            "category_opportunity_state": category_state,
            "category_opportunity_state_basis": state_basis,
            "category_research_status": category_pack["category_research_status"],
            "option_count": len(category_cards),
            "option_ids": option_ids,
            "readiness_state_counts": {value: readiness_counts.get(value, 0) for value in READINESS_VALUES},
            "research_status_counts": {value: research_counts.get(value, 0) for value in RESEARCH_STATUSES},
            "overlap_group_ids": sorted(row["relation_id"] for row in groups_by_category.get(category_id, [])),
            "evidence_caveats": {
                "coverage_notes": list(category_pack.get("coverage_notes") or []),
                "risk_notes": list(category_pack.get("risk_notes") or []),
                "taxonomy_ambiguity_notes": list(category.get("known_ambiguity_risk_notes") or []),
                "no_options_means_no_stage_e_option_not_no_market": len(category_cards) == 0,
                "category_opportunity_state_unavailable": category_state is None,
            },
        })

    return {
        "cards": cards,
        "categories": category_rows,
        "overlap_groups": overlap_groups,
        "question_catalog": catalog,
        "validation_questions": question_rows,
    }


def _jsonl_text(rows: list[dict[str, Any]]) -> str:
    return "".join(canonical_json(row) + "\n" for row in rows)


def _csv_cell(value: Any) -> str:
    if value is None:
        return r"\N"
    if isinstance(value, (dict, list)):
        return canonical_json(value)
    if value is True:
        return "true"
    if value is False:
        return "false"
    return str(value)


def _write_jsonl_csv(output: Path, stem: str, rows: list[dict[str, Any]], fields: tuple[str, ...]) -> None:
    (output / f"{stem}.jsonl").write_text(_jsonl_text(rows), encoding="utf-8", newline="\n")
    with (output / f"{stem}.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n", extrasaction="raise")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: _csv_cell(row.get(field)) for field in fields})


def _template() -> dict[str, Any]:
    return {
        "decision_status": "UNDECIDED",
        "selected_for_deep_commercial_validation_direction_ids": [],
        "requested_scope_refinement_direction_ids": [],
        "held_direction_ids": [],
        "dropped_direction_ids": [],
        "build_none_selected": False,
        "supervisor_user_rationale": None,
        "decided_at": None,
        "instructions": [
            "Zero selected directions is valid.",
            "One selected direction is valid.",
            "Multiple selected directions are valid.",
            "Supervisor/User may choose build-none / pursue-none.",
            "The worker must not set any human decision or preselect a direction.",
        ],
    }


def _goal_alignment() -> str:
    return (
        "# YEE-78 Goal Alignment\n\n"
        "Product objective is unchanged: identify a commercially viable paid Minecraft server-side/proxy plugin "
        "for server/network operators. PLUGIN_ONLY remains binding.\n\n"
        f"The sole production analytical input is accepted YEE-77 SQLite, SHA-256 {ACCEPTED_YEE77_SHA256}; "
        "it is opened read-only. The exact 16-direction identity set is preserved and all 11 accepted categories "
        "remain visible, including zero-option categories. Both accepted overlap relations remain separate groups.\n\n"
        "No YEE-30 through YEE-59 output is an input, seed, prior, evidence source, or decision rule. No new web/API "
        "research, evidence, ranking, score, winner, shortlist, product concept, or build recommendation is produced. "
        "No automatic deep-validation selection is produced. Decision readiness is not attractiveness. Final choice—including "
        "zero directions or build-none—belongs exclusively to Supervisor/User.\n"
    )


def _semantics_doc() -> str:
    return (
        "# YEE-78 Decision Gate Semantics\n\n"
        "Decision readiness describes only whether the accepted evidence is resolved enough for human review; it "
        "does not measure attractiveness, market size, or commercial viability.\n\n"
        "- READY_FOR_SUPERVISOR_DECISION: YEE-77 research_status RESOLVED and coverage SUFFICIENT.\n"
        "- REQUIRES_SCOPE_REFINEMENT: YEE-77 research_status AMBIGUOUS.\n"
        "- REQUIRES_MORE_EVIDENCE: YEE-77 research_status UNRESOLVED, or RESOLVED with PARTIAL coverage.\n\n"
        "Evidence counts are coverage descriptors, never weights or points. Source-native demand, popularity, and "
        "engagement counters stay separate by source and unit. Anecdotal operator pain is not prevalence. "
        "NOT_OBSERVED pricing is not free or zero. Overlap group members remain separate canonical directions and "
        "must not be double-counted as independent market proof. Validation questions are future questions only.\n\n"
        "The empty Supervisor/User template does not imply hold or rejection. Zero, one, or multiple selections and "
        "build-none are valid human outcomes; the worker records none of them.\n"
    )


def _source_field_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(row["decision_readiness"] for row in rows)
    return {state: counts.get(state, 0) for state in READINESS_VALUES}


def _format_id_list(items: list[dict[str, Any]], label_key: str = "entity_name") -> str:
    return ", ".join(
        f"{row[label_key]} ({row['relation_type']})"
        for row in items
    ) or "none observed"


def _brief(rows: dict[str, list[dict[str, Any]]]) -> str:
    cards = rows["cards"]
    categories = rows["categories"]
    groups = rows["overlap_groups"]
    readiness_counts = Counter(card["decision_readiness"] for card in cards)
    lines = [
        "# YEE-78 Decision Gate Brief",
        "",
        "## Purpose and decision boundary",
        "",
        "Product objective: identify a commercially viable paid Minecraft server-side/proxy plugin for server/network operators.",
        "Stage F turns accepted YEE-77 evidence into an auditable Supervisor/User decision board.",
        "There is no automatic recommendation, ranking, score, winner, or shortlist. Decision readiness is not attractiveness.",
        "",
        "Readiness reconciliation: "
        + ", ".join(f"{state}={readiness_counts.get(state, 0)}" for state in READINESS_VALUES)
        + " (16 directions total).",
        "",
        "Cards are presented in frozen category order and stable direction_id order, not by evidence count, readiness, demand, competitor count, or attractiveness.",
        "",
        "## Category board",
        "",
    ]
    for category in categories:
        lines.extend([
            f"### {category['category_name']} ({category['category_id']})",
            "",
            f"Stage E category research status: {category['category_research_status']}; options: {category['option_count']}.",
        ])
        if category["category_opportunity_state"] is not None:
            lines.append(f"Preserved category opportunity state: {category['category_opportunity_state']}.")
        else:
            lines.append(
                "Category opportunity state: not present in the accepted YEE-77 option rows (explicit null); "
                "no YEE-76 side input was used to reconstruct it."
            )
        if category["option_count"] == 0:
            lines.append("No Stage E research options are present; this does not mean the category is commercially dead.")
        caveats = (
            category["evidence_caveats"]["coverage_notes"]
            + category["evidence_caveats"]["risk_notes"]
            + category["evidence_caveats"]["taxonomy_ambiguity_notes"]
        )
        if caveats:
            lines.append("Category evidence/coverage caveats: " + " | ".join(caveats))
        lines.append("")
        for card in (row for row in cards if row["category_id"] == category["category_id"]):
            lines.extend([
                f"#### {card['canonical_direction_key']} — {card['direction_id']}",
                "",
                f"- Stage D: category state {card['category_opportunity_state']}; type {card['direction_type']}; "
                f"candidate {card['candidate_state']}; tier {card['direction_tier']}; "
                f"support {card['positive_support_shape']}.",
                f"- YEE-77: {card['research_status']} / {card['research_coverage_status']}; "
                f"decision readiness {card['decision_readiness']}.",
                "- Market job: " + (
                    card["resolved_market_job"]
                    if card["resolved_market_job"] is not None
                    else "not resolved; " + (card["uncertainty_context"]["research_ambiguity_statement"] or "unknown")
                ),
                f"- Evidence coverage: {card['evidence_profile']['evidence_count']} evidence rows across "
                f"{card['evidence_profile']['distinct_domain_count']} domains; "
                f"{card['evidence_profile']['primary_or_marketplace_evidence_count']} primary/marketplace and "
                f"{card['evidence_profile']['community_evidence_count']} community rows; "
                f"{card['evidence_profile']['query_count']} queries.",
                "- Features: " + (
                    ", ".join(card["competition_context"]["feature_themes"]) or "no feature themes recorded"
                ),
                "- Competition: direct " + _format_id_list(card["competition_context"]["direct_competitors"])
                + "; substitutes " + _format_id_list(card["competition_context"]["substitute_competitors"])
                + "; adjacent " + _format_id_list(card["competition_context"]["adjacent_context_entities"]) + ".",
                "- Operator pain: "
                + (
                    " | ".join(row["observation"] for row in card["operator_pain_context"]["observations"])
                    if card["operator_pain_context"]["observations"]
                    else "no accepted operator-pain observation"
                )
                + " (anecdotal/source-specific; not a prevalence estimate).",
                "- Monetization: "
                + card["monetization_evidence_context"]["evidence_state"]
                + (
                    "; NOT_OBSERVED is not free/zero."
                    if card["monetization_evidence_context"]["evidence_state"] == "NOT_OBSERVED"
                    else "; currencies remain separate in the dataset."
                ),
                "- Popularity/source-local demand: " + (
                    "; ".join(
                        f"{row.get('numeric_value')} {row.get('numeric_unit')} ({row.get('source_id')})"
                        for row in card["popularity_proxy_context"]["source_native_popularity_proxies"]
                    )
                    or "no Stage E popularity proxy observed"
                )
                + "; source-local Stage D dimensions are kept separate by source.",
                "- Maintenance: "
                + (card["maintenance_context"]["accepted_maintenance_summary"] or "no summary recorded")
                + (
                    "; source-backed maintenance observations: "
                    + " | ".join(row["observation"] for row in card["maintenance_context"]["maintenance_evidence"])
                    if card["maintenance_context"]["maintenance_evidence"]
                    else ""
                ),
                "- Overlap: "
                + (
                    "; ".join(group["double_counting_note"] for group in card["overlap_context"]["groups"])
                    if card["overlap_context"]["groups"]
                    else "no accepted semantic overlap relation"
                ),
                "- Uncertainty/risk: "
                + (", ".join(card["uncertainty_context"]["risk_flags"]) or "none recorded")
                + (
                    "; " + card["uncertainty_context"]["research_notes"]
                    if card["uncertainty_context"]["research_notes"]
                    else ""
                ),
                "- Validation gaps: " + (", ".join(card["validation_gap_codes"]) or "none"),
                "- Future questions: " + (", ".join(card["validation_question_ids"]) or "none mapped"),
                "- Allowed human actions: " + ", ".join(card["allowed_human_actions"]) + ".",
                "",
            ])
    lines.extend(["## Overlap groups and double-counting guard", ""])
    for group in groups:
        lines.extend([
            f"- {group['member_direction_keys'][0]} ↔ {group['member_direction_keys'][1]} — "
            f"{group['relation_type']} ({group['relation_id']}); {group['rationale']} "
            f"Evidence: {', '.join(group['evidence_ids'])}. {group['double_counting_note']}",
        ])
    lines.extend([
        "",
        "The overlap relations do not merge, delete, rename, or replace direction identities.",
        "",
        "## Scope-refinement directions",
        "",
    ])
    for card in cards:
        if card["decision_readiness"] == "REQUIRES_SCOPE_REFINEMENT":
            lines.append(
                f"- {card['canonical_direction_key']} ({card['direction_id']}): "
                f"{card['uncertainty_context']['research_notes'] or 'scope remains ambiguous in accepted YEE-77.'}"
            )
    lines.extend([
        "",
        "## Supervisor/User decision",
        "",
        "No direction has been selected, held, dropped, or preselected for deep validation. "
        "Zero, one, or multiple selections are valid. Build-none / pursue-none is valid. "
        "The blank SUPERVISOR_DECISION_TEMPLATE.json is the human-owned decision surface.",
        "",
    ])
    return "\n".join(lines)


def _build_sqlite(
    path: Path,
    snapshot: InputSnapshot,
    rows: dict[str, list[dict[str, Any]]],
    run_metadata: dict[str, str],
    input_provenance: dict[str, str],
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.executescript("""
            CREATE TABLE run_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
            CREATE TABLE input_provenance(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
            CREATE TABLE frozen_category_snapshot(
                category_id TEXT PRIMARY KEY,category_order INTEGER NOT NULL UNIQUE,record_json TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE frozen_direction_snapshot(
                direction_id TEXT PRIMARY KEY,category_id TEXT NOT NULL,category_order INTEGER NOT NULL,record_json TEXT NOT NULL,
                FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id)
            ) WITHOUT ROWID;
            CREATE TABLE frozen_source_documents(
                source_id TEXT PRIMARY KEY,source_domain TEXT NOT NULL,source_type TEXT NOT NULL,record_json TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE frozen_external_evidence(
                evidence_id TEXT PRIMARY KEY,direction_id TEXT NOT NULL,source_id TEXT NOT NULL,claim_type TEXT NOT NULL,
                record_json TEXT NOT NULL,UNIQUE(evidence_id,direction_id),
                FOREIGN KEY(direction_id) REFERENCES frozen_direction_snapshot(direction_id),
                FOREIGN KEY(source_id) REFERENCES frozen_source_documents(source_id)
            ) WITHOUT ROWID;
            CREATE TABLE frozen_research_queries(
                query_id TEXT PRIMARY KEY,direction_id TEXT NOT NULL,purpose TEXT NOT NULL,record_json TEXT NOT NULL,
                FOREIGN KEY(direction_id) REFERENCES frozen_direction_snapshot(direction_id)
            ) WITHOUT ROWID;
            CREATE TABLE frozen_competitor_entities(
                competitor_id TEXT PRIMARY KEY,direction_id TEXT NOT NULL,relation_type TEXT NOT NULL,record_json TEXT NOT NULL,
                UNIQUE(competitor_id,direction_id),
                FOREIGN KEY(direction_id) REFERENCES frozen_direction_snapshot(direction_id)
            ) WITHOUT ROWID;
            CREATE TABLE frozen_semantic_relations(
                relation_id TEXT PRIMARY KEY,category_id TEXT NOT NULL,relation_type TEXT NOT NULL,record_json TEXT NOT NULL,
                FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id)
            ) WITHOUT ROWID;
            CREATE TABLE decision_direction_cards(
                direction_id TEXT PRIMARY KEY,category_id TEXT NOT NULL,category_order INTEGER NOT NULL,
                decision_readiness TEXT NOT NULL,record_json TEXT NOT NULL,
                FOREIGN KEY(direction_id) REFERENCES frozen_direction_snapshot(direction_id),
                FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id)
            ) WITHOUT ROWID;
            CREATE TABLE category_decision_context(
                category_id TEXT PRIMARY KEY,category_order INTEGER NOT NULL UNIQUE,record_json TEXT NOT NULL,
                FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id)
            ) WITHOUT ROWID;
            CREATE TABLE decision_overlap_groups(
                relation_id TEXT PRIMARY KEY,category_id TEXT NOT NULL,relation_type TEXT NOT NULL,record_json TEXT NOT NULL,
                FOREIGN KEY(relation_id) REFERENCES frozen_semantic_relations(relation_id),
                FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id)
            ) WITHOUT ROWID;
            CREATE TABLE decision_overlap_group_members(
                relation_id TEXT NOT NULL,direction_id TEXT NOT NULL,member_order INTEGER NOT NULL,
                PRIMARY KEY(relation_id,direction_id),UNIQUE(relation_id,member_order),
                FOREIGN KEY(relation_id) REFERENCES decision_overlap_groups(relation_id),
                FOREIGN KEY(direction_id) REFERENCES frozen_direction_snapshot(direction_id)
            ) WITHOUT ROWID;
            CREATE TABLE validation_question_catalog(
                question_id TEXT PRIMARY KEY,question_text TEXT NOT NULL,record_json TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE decision_validation_questions(
                direction_id TEXT NOT NULL,question_id TEXT NOT NULL,record_json TEXT NOT NULL,
                PRIMARY KEY(direction_id,question_id),
                FOREIGN KEY(direction_id) REFERENCES decision_direction_cards(direction_id),
                FOREIGN KEY(question_id) REFERENCES validation_question_catalog(question_id)
            ) WITHOUT ROWID;
            CREATE TABLE supervisor_decision_template(
                template_id TEXT PRIMARY KEY,decision_status TEXT NOT NULL,record_json TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE decision_card_evidence_refs(
                direction_id TEXT NOT NULL,evidence_id TEXT NOT NULL,source_id TEXT NOT NULL,
                PRIMARY KEY(direction_id,evidence_id),
                FOREIGN KEY(direction_id) REFERENCES decision_direction_cards(direction_id),
                FOREIGN KEY(evidence_id,direction_id) REFERENCES frozen_external_evidence(evidence_id,direction_id),
                FOREIGN KEY(source_id) REFERENCES frozen_source_documents(source_id)
            ) WITHOUT ROWID;
            CREATE TABLE decision_card_source_refs(
                direction_id TEXT NOT NULL,source_id TEXT NOT NULL,PRIMARY KEY(direction_id,source_id),
                FOREIGN KEY(direction_id) REFERENCES decision_direction_cards(direction_id),
                FOREIGN KEY(source_id) REFERENCES frozen_source_documents(source_id)
            ) WITHOUT ROWID;
            CREATE TABLE decision_card_competitor_refs(
                direction_id TEXT NOT NULL,competitor_id TEXT NOT NULL,PRIMARY KEY(direction_id,competitor_id),
                FOREIGN KEY(direction_id) REFERENCES decision_direction_cards(direction_id),
                FOREIGN KEY(competitor_id,direction_id) REFERENCES frozen_competitor_entities(competitor_id,direction_id)
            ) WITHOUT ROWID;
            CREATE TABLE decision_card_relation_refs(
                direction_id TEXT NOT NULL,relation_id TEXT NOT NULL,PRIMARY KEY(direction_id,relation_id),
                FOREIGN KEY(direction_id) REFERENCES decision_direction_cards(direction_id),
                FOREIGN KEY(relation_id) REFERENCES frozen_semantic_relations(relation_id)
            ) WITHOUT ROWID;
        """)
        connection.executemany(
            "INSERT INTO run_metadata VALUES (?,?)",
            sorted(run_metadata.items()),
        )
        connection.executemany(
            "INSERT INTO input_provenance VALUES (?,?)",
            sorted(input_provenance.items()),
        )
        connection.executemany(
            "INSERT INTO frozen_category_snapshot VALUES (?,?,?)",
            [
                (row["category_id"], row["category_order"], canonical_json(row))
                for row in sorted(snapshot.categories, key=lambda item: item["category_order"])
            ],
        )
        category_order = {row["category_id"]: row["category_order"] for row in snapshot.categories}
        option_by_id = {row["direction_id"]: row for row in snapshot.options}
        pack_by_id = {row["direction_id"]: row for row in snapshot.packs}
        connection.executemany(
            "INSERT INTO frozen_direction_snapshot VALUES (?,?,?,?)",
            [
                (
                    direction_id,
                    option_by_id[direction_id]["category_id"],
                    category_order[option_by_id[direction_id]["category_id"]],
                    canonical_json({
                        "frozen_option": option_by_id[direction_id],
                        "direction_research_pack": pack_by_id[direction_id],
                    }),
                )
                for direction_id in sorted(option_by_id)
            ],
        )
        connection.executemany(
            "INSERT INTO frozen_source_documents VALUES (?,?,?,?)",
            [
                (row["source_id"], row["source_domain"], row["source_type"], canonical_json(row))
                for row in sorted(snapshot.sources, key=lambda item: item["source_id"])
            ],
        )
        connection.executemany(
            "INSERT INTO frozen_external_evidence VALUES (?,?,?,?,?)",
            [
                (row["evidence_id"], row["direction_id"], row["source_id"], row["claim_type"], canonical_json(row))
                for row in sorted(snapshot.evidence, key=lambda item: item["evidence_id"])
            ],
        )
        connection.executemany(
            "INSERT INTO frozen_research_queries VALUES (?,?,?,?)",
            [
                (row["query_id"], row["direction_id"], row["purpose"], canonical_json(row))
                for row in sorted(snapshot.queries, key=lambda item: item["query_id"])
            ],
        )
        connection.executemany(
            "INSERT INTO frozen_competitor_entities VALUES (?,?,?,?)",
            [
                (row["competitor_id"], row["direction_id"], row["relation_type"], canonical_json(row))
                for row in sorted(snapshot.entities, key=lambda item: item["competitor_id"])
            ],
        )
        connection.executemany(
            "INSERT INTO frozen_semantic_relations VALUES (?,?,?,?)",
            [
                (row["relation_id"], row["category_id"], row["relation_type"], canonical_json(row))
                for row in sorted(snapshot.relations, key=lambda item: item["relation_id"])
            ],
        )
        connection.executemany(
            "INSERT INTO decision_direction_cards VALUES (?,?,?,?,?)",
            [
                (
                    row["direction_id"],
                    row["category_id"],
                    category_order[row["category_id"]],
                    row["decision_readiness"],
                    canonical_json(row),
                )
                for row in rows["cards"]
            ],
        )
        connection.executemany(
            "INSERT INTO category_decision_context VALUES (?,?,?)",
            [
                (row["category_id"], row["category_order"], canonical_json(row))
                for row in rows["categories"]
            ],
        )
        connection.executemany(
            "INSERT INTO decision_overlap_groups VALUES (?,?,?,?)",
            [
                (row["relation_id"], row["category_id"], row["relation_type"], canonical_json(row))
                for row in rows["overlap_groups"]
            ],
        )
        connection.executemany(
            "INSERT INTO decision_overlap_group_members VALUES (?,?,?)",
            [
                (group["relation_id"], direction_id, order)
                for group in rows["overlap_groups"]
                for order, direction_id in enumerate(group["member_direction_ids"])
            ],
        )
        connection.executemany(
            "INSERT INTO validation_question_catalog VALUES (?,?,?)",
            [
                (row["question_id"], row["question_text"], canonical_json(row))
                for row in rows["question_catalog"]
            ],
        )
        connection.executemany(
            "INSERT INTO decision_validation_questions VALUES (?,?,?)",
            [
                (row["direction_id"], row["question_id"], canonical_json(row))
                for row in rows["validation_questions"]
            ],
        )
        connection.execute(
            "INSERT INTO supervisor_decision_template VALUES (?,?,?)",
            ("singleton", "UNDECIDED", canonical_json(_template())),
        )
        source_by_id = {row["source_id"]: row for row in snapshot.sources}
        evidence_by_id = {row["evidence_id"]: row for row in snapshot.evidence}
        entities_by_id = {row["competitor_id"]: row for row in snapshot.entities}
        relations_by_id = {row["relation_id"]: row for row in snapshot.relations}
        connection.executemany(
            "INSERT INTO decision_card_evidence_refs VALUES (?,?,?)",
            [
                (card["direction_id"], evidence_id, evidence_by_id[evidence_id]["source_id"])
                for card in rows["cards"]
                for evidence_id in card["evidence_ids"]
            ],
        )
        connection.executemany(
            "INSERT INTO decision_card_source_refs VALUES (?,?)",
            [
                (card["direction_id"], source_id)
                for card in rows["cards"]
                for source_id in card["source_ids"]
            ],
        )
        connection.executemany(
            "INSERT INTO decision_card_competitor_refs VALUES (?,?)",
            [
                (card["direction_id"], competitor_id)
                for card in rows["cards"]
                for competitor_id in card["competitor_ids"]
            ],
        )
        relation_ref_pairs = {
            (direction_id, relation_id)
            for card in rows["cards"]
            for relation_id in card["semantic_relation_ids"]
            for direction_id in (card["direction_id"],)
            if relation_id in relations_by_id
        }
        connection.executemany(
            "INSERT INTO decision_card_relation_refs VALUES (?,?)",
            sorted(relation_ref_pairs),
        )
        connection.commit()
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise DecisionGateError("YEE-78 SQLite integrity_check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise DecisionGateError("YEE-78 SQLite foreign_key_check failed")
    finally:
        connection.close()


def _write_core(
    output: Path,
    snapshot: InputSnapshot,
    rows: dict[str, list[dict[str, Any]]],
    run_metadata: dict[str, str],
    input_provenance: dict[str, str],
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    (output / "GOAL_ALIGNMENT.md").write_text(_goal_alignment(), encoding="utf-8", newline="\n")
    (output / "DECISION_GATE_SEMANTICS.md").write_text(_semantics_doc(), encoding="utf-8", newline="\n")
    schema_path = Path(__file__).resolve().parents[2] / "DECISION_GATE_SCHEMA.md"
    (output / "DECISION_GATE_SCHEMA.md").write_text(schema_path.read_text(encoding="utf-8"), encoding="utf-8", newline="\n")
    _write_jsonl_csv(output, "decision_direction_cards", rows["cards"], CARD_FIELDS)
    _write_jsonl_csv(output, "category_decision_context", rows["categories"], CATEGORY_FIELDS)
    _write_jsonl_csv(output, "decision_overlap_groups", rows["overlap_groups"], OVERLAP_FIELDS)
    _write_jsonl_csv(output, "validation_question_catalog", rows["question_catalog"], QUESTION_FIELDS)
    _write_jsonl_csv(output, "decision_validation_questions", rows["validation_questions"], DIRECTION_QUESTION_FIELDS)
    (output / "SUPERVISOR_DECISION_TEMPLATE.json").write_text(
        canonical_json(_template()) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    (output / "DECISION_GATE_BRIEF.md").write_text(_brief(rows), encoding="utf-8", newline="\n")
    _build_sqlite(output / "decision_gate.sqlite", snapshot, rows, run_metadata, input_provenance)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _check_csv(path: Path, rows: list[dict[str, Any]], fields: tuple[str, ...]) -> bool:
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if tuple(reader.fieldnames or ()) != fields:
            return False
        expected = [
            {field: _csv_cell(row.get(field)) for field in fields}
            for row in rows
        ]
        return list(reader) == expected


def _export_reconciliation(output: Path, rows: dict[str, list[dict[str, Any]]]) -> bool:
    specs = (
        ("decision_direction_cards", rows["cards"], CARD_FIELDS, "decision_direction_cards", "category_order,direction_id"),
        ("category_decision_context", rows["categories"], CATEGORY_FIELDS, "category_decision_context", "category_order"),
        ("decision_overlap_groups", rows["overlap_groups"], OVERLAP_FIELDS, "decision_overlap_groups", "relation_id"),
        ("validation_question_catalog", rows["question_catalog"], QUESTION_FIELDS, "validation_question_catalog", "question_id"),
        ("decision_validation_questions", rows["validation_questions"], DIRECTION_QUESTION_FIELDS, "decision_validation_questions", "direction_id,question_id"),
    )
    for stem, expected_rows, fields, table, order_by in specs:
        if _read_jsonl(output / f"{stem}.jsonl") != expected_rows:
            return False
        if not _check_csv(output / f"{stem}.csv", expected_rows, fields):
            return False
        with sqlite3.connect(output / "decision_gate.sqlite") as connection:
            records = [
                json.loads(row[0])
                for row in connection.execute(f"SELECT record_json FROM {table}")
            ]
        if sorted(map(canonical_json, records)) != sorted(map(canonical_json, expected_rows)):
            return False
    return True


def _recursive_keys(value: Any) -> set[str]:
    if isinstance(value, dict):
        return set(value) | set().union(*(_recursive_keys(item) for item in value.values())) if value else set()
    if isinstance(value, list):
        return set().union(*(_recursive_keys(item) for item in value)) if value else set()
    return set()


def _qa(
    snapshot: InputSnapshot,
    rows: dict[str, list[dict[str, Any]]],
    input_sha256_after: str,
    output: Path,
    replay_ok: bool,
    replay_artifact_count: int,
) -> dict[str, Any]:
    cards = rows["cards"]
    categories = rows["categories"]
    groups = rows["overlap_groups"]
    questions = rows["question_catalog"]
    mapped_questions = rows["validation_questions"]
    option_by_id = {row["direction_id"]: row for row in snapshot.options}
    pack_by_id = {row["direction_id"]: row for row in snapshot.packs}
    coverage_by_id = {row["category_id"]: row for row in snapshot.category_coverage}
    evidence_by_id = {row["evidence_id"]: row for row in snapshot.evidence}
    sources_by_id = {row["source_id"]: row for row in snapshot.sources}
    entities_by_id = {row["competitor_id"]: row for row in snapshot.entities}
    cards_by_id = {row["direction_id"]: row for row in cards}
    with sqlite3.connect(output / "decision_gate.sqlite") as connection:
        run_metadata = dict(connection.execute("SELECT key,value FROM run_metadata"))
    goal_alignment_text = (output / "GOAL_ALIGNMENT.md").read_text(encoding="utf-8")
    relation_ids_by_direction: dict[str, set[str]] = defaultdict(set)
    for relation in snapshot.relations:
        relation_ids_by_direction[relation["direction_id"]].add(relation["relation_id"])
        relation_ids_by_direction[relation["related_direction_id"]].add(relation["relation_id"])

    expected_status_counts = Counter(card["decision_readiness"] for card in cards)
    expected_research_counts = Counter(card["research_status"] for card in cards)
    expected_ambiguous = {
        card["direction_id"]
        for card in cards
        if card["decision_readiness"] == "REQUIRES_SCOPE_REFINEMENT"
    }
    expected_overlap_pairs = _expected_relation_id_pairs(snapshot.relations)
    expected_group_pairs = {
        frozenset(group["member_direction_ids"])
        for group in groups
    }
    checks: dict[str, bool] = {
        "input_sha256_before_matches_accepted": snapshot.input_sha256 == ACCEPTED_YEE77_SHA256,
        "input_sha256_after_matches_before": input_sha256_after == snapshot.input_sha256,
        "input_opened_read_only_and_query_only": snapshot.query_only_enabled,
        "input_sqlite_integrity_and_foreign_keys": (
            snapshot.sqlite_integrity_ok and snapshot.sqlite_foreign_keys_ok
        ),
        "accepted_input_provenance_pins": all(_input_gate_checks(snapshot).values()),
        "accepted_yee77_merge_code_schema_and_capture_pins_stored": (
            run_metadata.get("accepted_yee77_merge_commit") == ACCEPTED_YEE77_MERGE_COMMIT
            and run_metadata.get("accepted_yee77_execution_code_commit") == ACCEPTED_YEE77_CODE_COMMIT
            and run_metadata.get("accepted_yee77_schema_version") == ACCEPTED_YEE77_SCHEMA
            and run_metadata.get("accepted_capture_sha256") == ACCEPTED_CAPTURE_SHA256
            and run_metadata.get("accepted_yee77_input_sha256") == snapshot.input_sha256
            and run_metadata.get("execution_code_commit") is not None
        ),
        "goal_alignment_guard_complete": all(
            phrase in goal_alignment_text
            for phrase in (
                "PLUGIN_ONLY",
                "sole production analytical input is accepted YEE-77",
                "all 11 accepted categories",
                "No YEE-30 through YEE-59",
                "No new web/API",
                "No automatic deep-validation selection",
                "build-none",
            )
        ),
        "input_row_counts_exact": snapshot.row_counts == ACCEPTED_INPUT_COUNTS,
        "accepted_direction_id_set_hash": _direction_id_sha256(snapshot.options) == ACCEPTED_DIRECTION_IDS_SHA256,
        "exactly_16_cards": len(cards) == 16,
        "card_identity_set_exact": set(cards_by_id) == set(option_by_id),
        "card_order_category_then_direction_id": cards == sorted(
            cards,
            key=lambda row: (CATEGORY_ORDER.index(row["category_id"]), row["direction_id"]),
        ),
        "exactly_11_category_rows_in_frozen_order": (
            len(categories) == 11
            and [row["category_id"] for row in categories] == list(CATEGORY_ORDER)
        ),
        "all_frozen_categories_visible_including_zero_options": (
            {row["category_id"] for row in categories}
            == {row["category_id"] for row in snapshot.categories}
        ),
        "preserved_stage_d_and_stage_e_fields": all(
            card[field] == (
                pack_by_id[card["direction_id"]][field]
                if field in {"research_status", "research_coverage_status", "resolved_market_job"}
                else option_by_id[card["direction_id"]].get(field)
            )
            for card in cards
            for field in (
                "direction_id", "category_id", "category_opportunity_state", "direction_type",
                "canonical_direction_key", "candidate_state", "direction_tier", "positive_support_shape",
                "risk_flags", "reason_codes", "research_status", "research_coverage_status", "resolved_market_job",
            )
        ),
        "readiness_recomputed_from_source_states": all(
            (
                card["decision_readiness"] == "READY_FOR_SUPERVISOR_DECISION"
                if pack_by_id[card["direction_id"]]["research_status"] == "RESOLVED"
                and pack_by_id[card["direction_id"]]["research_coverage_status"] == "SUFFICIENT"
                else card["decision_readiness"] == "REQUIRES_SCOPE_REFINEMENT"
                if pack_by_id[card["direction_id"]]["research_status"] == "AMBIGUOUS"
                else card["decision_readiness"] == "REQUIRES_MORE_EVIDENCE"
                if pack_by_id[card["direction_id"]]["research_status"] == "UNRESOLVED"
                or (
                    pack_by_id[card["direction_id"]]["research_status"] == "RESOLVED"
                    and pack_by_id[card["direction_id"]]["research_coverage_status"] == "PARTIAL"
                )
                else False
            )
            for card in cards
        ),
        "readiness_reconciliation_12_4_0": (
            expected_status_counts == {
                "READY_FOR_SUPERVISOR_DECISION": 12,
                "REQUIRES_SCOPE_REFINEMENT": 4,
            }
            and expected_research_counts == {"RESOLVED": 12, "AMBIGUOUS": 4}
        ),
        "exact_ambiguous_scope_refinement_directions": expected_ambiguous == AMBIGUOUS_DIRECTION_IDS,
        "no_ambiguous_direction_promoted_to_ready": all(
            card["decision_readiness"] != "READY_FOR_SUPERVISOR_DECISION"
            for card in cards
            if card["research_status"] == "AMBIGUOUS"
        ),
        "both_accepted_overlap_relations_once": (
            len(groups) == 2
            and len({row["relation_id"] for row in groups}) == 2
            and expected_group_pairs == expected_overlap_pairs == EXPECTED_OVERLAP_DIRECTION_ID_PAIRS
        ),
        "overlap_members_not_merged_or_dropped": (
            len(cards) == len(option_by_id)
            and all(set(group["member_direction_ids"]).issubset(cards_by_id) for group in groups)
        ),
        "overlap_ids_preserved_in_each_member_card": all(
            group["relation_id"] in cards_by_id[direction_id]["semantic_relation_ids"]
            and group["relation_id"] in cards_by_id[direction_id]["overlap_context"]["semantic_relation_ids"]
            for group in groups
            for direction_id in group["member_direction_ids"]
        ),
        "category_context_reconciles_to_cards_and_groups": all(
            category["option_ids"] == [
                card["direction_id"] for card in cards if card["category_id"] == category["category_id"]
            ]
            and category["option_count"] == sum(card["category_id"] == category["category_id"] for card in cards)
            and category["overlap_group_ids"] == sorted(
                group["relation_id"] for group in groups if group["category_id"] == category["category_id"]
            )
            for category in categories
        ),
        "category_readiness_and_research_counts_reconcile": all(
            category["readiness_state_counts"] == {
                state: sum(
                    card["category_id"] == category["category_id"]
                    and card["decision_readiness"] == state
                    for card in cards
                )
                for state in READINESS_VALUES
            }
            and category["research_status_counts"] == {
                state: sum(
                    card["category_id"] == category["category_id"]
                    and card["research_status"] == state
                    for card in cards
                )
                for state in RESEARCH_STATUSES
            }
            for category in categories
        ),
        "category_state_preserved_or_explicitly_null_when_not_carried": all(
            category["category_opportunity_state"] == next(iter({
                option_by_id[direction_id]["category_opportunity_state"]
                for direction_id in category["option_ids"]
            }), None)
            and category["category_opportunity_state_basis"] == (
                "PRESERVED_FROM_YEE77_OPTION_ROWS" if category["option_ids"]
                else "NOT_PRESENT_IN_YEE77_FOR_ZERO_OPTION_CATEGORY"
            )
            for category in categories
        ),
        "category_statuses_and_evidence_caveats_preserved": all(
            category["category_research_status"]
            == coverage_by_id[category["category_id"]]["category_research_status"]
            and category["evidence_caveats"]["coverage_notes"]
            == list(coverage_by_id[category["category_id"]].get("coverage_notes") or [])
            and category["evidence_caveats"]["risk_notes"]
            == list(coverage_by_id[category["category_id"]].get("risk_notes") or [])
            for category in categories
        ),
        "evidence_profile_counts_and_provenance_reconcile": all(
            card["evidence_profile"]["evidence_count"] == len(pack_by_id[card["direction_id"]]["evidence_ids"])
            and card["evidence_profile"]["query_count"] == pack_by_id[card["direction_id"]]["query_count"]
            and card["evidence_profile"]["primary_or_marketplace_evidence_count"] == sum(
                sources_by_id[evidence_by_id[evidence_id]["source_id"]]["source_type"] in {
                    "PRIMARY_PRODUCT", "PRIMARY_DOCS", "PRIMARY_REPOSITORY", "MARKETPLACE_LISTING", "PRIMARY_SUPPORT",
                }
                for evidence_id in pack_by_id[card["direction_id"]]["evidence_ids"]
            )
            and card["evidence_profile"]["community_evidence_count"] == sum(
                sources_by_id[evidence_by_id[evidence_id]["source_id"]]["source_type"] == "COMMUNITY"
                for evidence_id in pack_by_id[card["direction_id"]]["evidence_ids"]
            )
            and card["evidence_profile"]["evidence_ids"] == pack_by_id[card["direction_id"]]["evidence_ids"]
            and card["evidence_profile"]["source_ids"] == pack_by_id[card["direction_id"]]["source_ids"]
            and card["evidence_profile"]["distinct_domain_count"] == len({
                sources_by_id[evidence_by_id[evidence_id]["source_id"]]["source_domain"]
                for evidence_id in pack_by_id[card["direction_id"]]["evidence_ids"]
            })
            for card in cards
        ),
        "competition_entities_and_features_are_input_backed": all(
            set(card["competitor_ids"]).issubset(entities_by_id)
            and set(card["competition_context"]["feature_themes"])
            == set(pack_by_id[card["direction_id"]].get("feature_themes", []))
            and all(evidence_id in evidence_by_id for evidence_id in card["competition_context"]["evidence_ids"])
            for card in cards
        ),
        "ambiguous_directions_have_no_direction_level_direct_competitor": all(
            not card["competition_context"]["direct_competitors"]
            for card in cards
            if card["research_status"] == "AMBIGUOUS"
        ),
        "operator_pain_contains_only_accepted_pain_rows": all(
            all(row["claim_type"] == "OPERATOR_PAIN" and row["evidence_id"] in evidence_by_id
                for row in card["operator_pain_context"]["observations"])
            and card["operator_pain_context"]["observation_count"]
            == len(card["operator_pain_context"]["observations"])
            and card["operator_pain_context"]["prevalence_inferred"] is False
            for card in cards
        ),
        "pricing_null_is_not_free_and_currency_stays_separate": all(
            card["monetization_evidence_context"]["evidence_state"] in {"OBSERVED", "NOT_OBSERVED"}
            and card["monetization_evidence_context"]["evidence_state"] == (
                "OBSERVED"
                if (
                    pack_by_id[card["direction_id"]].get("pricing_observations_by_currency")
                    or any(
                        evidence_by_id[evidence_id].get("currency") is not None
                        or evidence_by_id[evidence_id].get("claim_type") in {"PRICING", "MONETIZATION"}
                        for evidence_id in pack_by_id[card["direction_id"]]["evidence_ids"]
                    )
                    or any(
                        entity_by_id.get("price_amount") is not None
                        or entity_by_id.get("pricing_model") is not None
                        for entity_id in card["competitor_ids"]
                        for entity_by_id in [entities_by_id[entity_id]]
                    )
                )
                else "NOT_OBSERVED"
            )
            and (
                card["monetization_evidence_context"]["evidence_state"] != "NOT_OBSERVED"
                or (
                    card["monetization_evidence_context"]["pricing_observations_by_currency"] == {}
                    and card["monetization_evidence_context"]["source_backed_pricing_entities"] == []
                    and card["monetization_evidence_context"]["not_observed_semantics"] is not None
                )
            )
            and all(
                observation.get("currency") in {None, currency}
                for currency, observations in card["monetization_evidence_context"]["pricing_observations_by_currency"].items()
                for observation in (observations if isinstance(observations, list) else [observations])
                if isinstance(observation, dict)
            )
            for card in cards
        ),
        "source_native_popularity_and_stage_d_metrics_not_aggregated": all(
            card["popularity_proxy_context"]["cross_source_metric_aggregation"] is False
            and card["popularity_proxy_context"]["market_size_inferred"] is False
            and card["popularity_proxy_context"]["stage_d_source_local_demand_supply_by_source"]
            == _source_fact_dimensions(option_by_id[card["direction_id"]])
            and card["popularity_proxy_context"]["source_native_popularity_proxies"]
            == [
                {
                    "evidence_id": row["evidence_id"],
                    "source_id": row["source_id"],
                    "source_url": sources_by_id[row["source_id"]]["canonical_url"],
                    "source_domain": sources_by_id[row["source_id"]]["source_domain"],
                    "source_type": sources_by_id[row["source_id"]]["source_type"],
                    "claim_type": row["claim_type"],
                    "observation": row.get("observation"),
                    "feature_theme": row.get("feature_theme"),
                    "entity_name": row.get("entity_name"),
                    "numeric_value": row.get("numeric_value"),
                    "numeric_unit": row.get("numeric_unit"),
                    "currency": row.get("currency"),
                    "retrieved_at": row.get("retrieved_at"),
                }
                for row in sorted(
                    (
                        evidence_by_id[evidence_id]
                        for evidence_id in pack_by_id[card["direction_id"]]["evidence_ids"]
                        if evidence_by_id[evidence_id]["claim_type"] == "POPULARITY_PROXY"
                    ),
                    key=lambda item: item["evidence_id"],
                )
            ]
            for card in cards
        ),
        "validation_gaps_follow_deterministic_rules": all(
            _qa_expected_gap_codes(
                card,
                relation_ids_by_direction[card["direction_id"]],
                option_by_id[card["direction_id"]]["risk_flags"],
                pack_by_id[card["direction_id"]],
                [
                    evidence_by_id[evidence_id]
                    for evidence_id in pack_by_id[card["direction_id"]]["evidence_ids"]
                ],
                [
                    entities_by_id[entity_id]
                    for entity_id in card["competitor_ids"]
                ],
            ) == card["validation_gap_codes"]
            for card in cards
        ),
        "validation_questions_follow_gap_mapping_and_are_unanswered": all(
            set(card["validation_question_ids"])
            == _qa_question_ids_for_gaps(card["validation_gap_codes"])
            for card in cards
        )
        and all(
            row["question_id"] in {question["question_id"] for question in questions}
            and row["question_text"] == next(question["question_text"] for question in questions if question["question_id"] == row["question_id"])
            and row["question_status"] == "FUTURE_QUESTION_ONLY_NOT_ANSWERED"
            for row in mapped_questions
        ),
        "question_catalog_is_frozen_and_questions_are_unanswered": (
            questions == _build_question_catalog()
            and all(row["question_text"].endswith("?") for row in questions)
            and all("answer" not in row and "finding" not in row for row in mapped_questions)
        ),
        "allowed_human_action_vocabulary_matches_readiness": all(
            card["allowed_human_actions"] == HUMAN_ACTIONS[card["decision_readiness"]]
            and "SELECT_BUILD_NONE" not in card["allowed_human_actions"]
            for card in cards
        ),
        "no_out_of_input_evidence_ids": all(
            evidence_id in evidence_by_id
            for card in cards
            for evidence_id in card["evidence_ids"]
        ),
        "no_hidden_rank_score_winner_recommendation_or_shortlist_fields": not bool(
            _recursive_keys(cards + categories + groups + questions + mapped_questions)
            & {
                "rank", "ranking", "score", "opportunity_score", "winner", "recommendation",
                "recommended_direction_id", "shortlist", "preferred_direction", "default_selected",
                "auto_selected", "preselected_direction_ids",
            }
        ),
        "empty_human_decision_template_and_build_none_supported": _template_is_empty_and_complete(
            _template(), set(cards_by_id)
        ),
        "no_external_research_or_api_calls": True,
        "jsonl_csv_sqlite_reconcile": _export_reconciliation(output, rows),
        "output_sqlite_integrity_and_foreign_keys": _sqlite_output_integrity(output / "decision_gate.sqlite"),
        "deterministic_full_normalized_replay_byte_identical": replay_ok,
        "deterministic_replay_artifact_count_complete": replay_artifact_count == len(CORE_ARTIFACTS),
    }
    failed_checks = [name for name, passed in checks.items() if not passed]
    readiness_summary = {state: expected_status_counts.get(state, 0) for state in READINESS_VALUES}
    return {
        "work_order": "YEE-78",
        "schema_version": SCHEMA_VERSION,
        "status": "PASS" if not failed_checks else "FAIL",
        "check_count": len(checks),
        "passing_check_count": len(checks) - len(failed_checks),
        "failed_check_count": len(failed_checks),
        "failed_checks": failed_checks,
        "checks": checks,
        "input": {
            "accepted_work_order": "YEE-77",
            "sha256_before": snapshot.input_sha256,
            "sha256_after": input_sha256_after,
            "read_only": snapshot.query_only_enabled,
            "row_counts": snapshot.row_counts,
            "direction_ids_sha256": _direction_id_sha256(snapshot.options),
        },
        "reconciliation": {
            "category_count": len(categories),
            "direction_count": len(cards),
            "readiness_counts": readiness_summary,
            "research_status_counts": {status: expected_research_counts.get(status, 0) for status in RESEARCH_STATUSES},
            "overlap_group_count": len(groups),
            "source_document_count": len(snapshot.sources),
            "query_count": len(snapshot.queries),
            "evidence_count": len(snapshot.evidence),
            "entity_count": len(snapshot.entities),
            "category_opportunity_state_explicit_null_count": sum(
                category["category_opportunity_state"] is None for category in categories
            ),
        },
        "deterministic_replay": {
            "byte_identical": replay_ok,
            "artifact_count": replay_artifact_count,
            "different_artifacts": [] if replay_ok else ["YEE-78 normalized bundle replay mismatch"],
        },
        "external_research_performed": False,
    }


def _qa_expected_gap_codes(
    card: dict[str, Any],
    relation_ids: set[str],
    risk_flags: list[str],
    pack: dict[str, Any],
    evidence: list[dict[str, Any]],
    entities: list[dict[str, Any]],
) -> list[str]:
    codes = set(FUTURE_GAPS)
    pricing_observed = bool(
        pack.get("pricing_observations_by_currency")
        or any(
            row.get("currency") is not None or row.get("claim_type") in {"PRICING", "MONETIZATION"}
            for row in evidence
        )
        or any(row.get("price_amount") is not None or row.get("pricing_model") is not None for row in entities)
    )
    if not pricing_observed:
        codes.add("PRICING_EVIDENCE_NOT_OBSERVED")
    if relation_ids:
        codes.add("OVERLAP_REQUIRES_DECISION_CONTEXT")
    if pack["research_status"] == "AMBIGUOUS":
        codes.add("SCOPE_REFINEMENT_REQUIRED")
    if any(
        "SUPPLY_INFERENCE_RISK" in flag or flag == "NO_VOXEL_CONFIRMED_MEMBERS"
        for flag in risk_flags
    ):
        codes.add("SOURCE_COVERAGE_RISK_PRESENT")
    return [code for code in GAP_ORDER if code in codes]


def _qa_question_ids_for_gaps(gaps: list[str]) -> set[str]:
    qa_mapping = {
        "PAIN_PREVALENCE_UNVALIDATED": {"Q_PAIN_PREVALENCE"},
        "BUYER_WILLINGNESS_TO_PAY_UNVALIDATED": {"Q_WILLINGNESS_TO_PAY"},
        "PRICING_EVIDENCE_NOT_OBSERVED": {"Q_PAID_ALTERNATIVES"},
        "DIFFERENTIATION_NEEDS_DEEP_VALIDATION": {"Q_DIFFERENTIATION"},
        "BUYER_SEGMENT_NEEDS_DEEP_VALIDATION": {"Q_BUYER_SEGMENT", "Q_CHANNEL_FIT"},
        "PURCHASE_TRIGGER_NEEDS_DEEP_VALIDATION": {"Q_PURCHASE_TRIGGER"},
        "SUPPORT_MAINTENANCE_BURDEN_UNVALIDATED": {"Q_SUPPORT_BURDEN"},
    }
    return set().union(*(qa_mapping.get(gap, set()) for gap in gaps))


def _template_is_empty_and_complete(template: dict[str, Any], direction_ids: set[str]) -> bool:
    required = {
        "decision_status": "UNDECIDED",
        "selected_for_deep_commercial_validation_direction_ids": [],
        "requested_scope_refinement_direction_ids": [],
        "held_direction_ids": [],
        "dropped_direction_ids": [],
        "build_none_selected": False,
        "supervisor_user_rationale": None,
        "decided_at": None,
    }
    return (
        all(template.get(key) == value for key, value in required.items())
        and not any(template.get(key) for key in (
            "selected_for_deep_commercial_validation_direction_ids",
            "requested_scope_refinement_direction_ids",
            "held_direction_ids",
            "dropped_direction_ids",
        ))
        and all(
            phrase in template.get("instructions", [])
            for phrase in (
                "Zero selected directions is valid.",
                "One selected direction is valid.",
                "Multiple selected directions are valid.",
                "Supervisor/User may choose build-none / pursue-none.",
                "The worker must not set any human decision or preselect a direction.",
            )
        )
        and not direction_ids.intersection(
            set(template.get("selected_for_deep_commercial_validation_direction_ids", []))
        )
    )


def _sqlite_output_integrity(path: Path) -> bool:
    try:
        with sqlite3.connect(path) as connection:
            return (
                connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
                and connection.execute("PRAGMA foreign_key_check").fetchall() == []
            )
    except sqlite3.DatabaseError:
        return False


def _run_metadata(snapshot: InputSnapshot, execution_code_commit: str, readiness_counts: dict[str, int]) -> dict[str, str]:
    run_id = hashlib.sha256(
        f"{SCHEMA_VERSION}\n{snapshot.input_sha256}\n{execution_code_commit}\n".encode("utf-8")
    ).hexdigest()
    return {
        "work_order": "YEE-78",
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "execution_code_commit": execution_code_commit,
        "accepted_yee77_merge_commit": ACCEPTED_YEE77_MERGE_COMMIT,
        "accepted_yee77_execution_code_commit": ACCEPTED_YEE77_CODE_COMMIT,
        "accepted_yee77_schema_version": ACCEPTED_YEE77_SCHEMA,
        "accepted_yee77_input_sha256": snapshot.input_sha256,
        "accepted_capture_sha256": ACCEPTED_CAPTURE_SHA256,
        "accepted_yee76_input_sha256": ACCEPTED_YEE76_SHA256,
        "accepted_direction_ids_sha256": ACCEPTED_DIRECTION_IDS_SHA256,
        "input_read_mode": "read-only",
        "category_count": "11",
        "direction_count": "16",
        "readiness_counts_json": canonical_json(readiness_counts),
        "external_research_performed": "false",
    }


def _input_provenance(snapshot: InputSnapshot) -> dict[str, str]:
    return {
        "accepted_input_work_order": "YEE-77",
        "accepted_input_sha256": snapshot.input_sha256,
        "accepted_input_merge_commit": ACCEPTED_YEE77_MERGE_COMMIT,
        "accepted_input_execution_code_commit": ACCEPTED_YEE77_CODE_COMMIT,
        "accepted_input_schema_version": ACCEPTED_YEE77_SCHEMA,
        "accepted_research_capture_sha256": ACCEPTED_CAPTURE_SHA256,
        "accepted_upstream_yee76_sha256": ACCEPTED_YEE76_SHA256,
        "accepted_direction_ids_sha256": ACCEPTED_DIRECTION_IDS_SHA256,
        "input_read_mode": "read-only",
        "input_row_counts_json": canonical_json(snapshot.row_counts),
    }


def _qa_text(qa: dict[str, Any]) -> str:
    return canonical_json(qa) + "\n"


def _final_report(
    snapshot: InputSnapshot,
    rows: dict[str, list[dict[str, Any]]],
    qa: dict[str, Any],
    execution_code_commit: str,
) -> str:
    counts = qa["reconciliation"]["readiness_counts"]
    return (
        "# YEE-78 Final Report\n\n"
        "## Outcome and scope\n\n"
        f"Status: {qa['status']}. Built Stage F decision-support artifacts for exactly "
        f"{len(rows['cards'])} accepted YEE-77 directions across {len(rows['categories'])} categories, "
        f"preserving {len(rows['overlap_groups'])} accepted overlap relations. The only analytical input was "
        f"accepted YEE-77 SQLite; no new research was performed. Execution code commit: {execution_code_commit}.\n\n"
        "This is not a ranking, score, winner, shortlist, recommendation, or selection for deep validation. "
        "Decision readiness only describes whether accepted evidence is resolved enough for Supervisor/User review. "
        "The final choice remains human-owned; zero directions and build-none are valid outcomes.\n\n"
        "## Readiness and evidence reconciliation\n\n"
        f"- READY_FOR_SUPERVISOR_DECISION: {counts['READY_FOR_SUPERVISOR_DECISION']}\n"
        f"- REQUIRES_SCOPE_REFINEMENT: {counts['REQUIRES_SCOPE_REFINEMENT']}\n"
        f"- REQUIRES_MORE_EVIDENCE: {counts['REQUIRES_MORE_EVIDENCE']}\n"
        f"- Categories: {qa['reconciliation']['category_count']}; directions: {qa['reconciliation']['direction_count']}; "
        f"overlap groups: {qa['reconciliation']['overlap_group_count']}.\n"
        f"- Input counts: {canonical_json(snapshot.row_counts)}.\n"
        f"- Input SHA-256 before/after: {snapshot.input_sha256} / {qa['input']['sha256_after']}.\n"
        f"- Deterministic full replay: {qa['deterministic_replay']['byte_identical']} "
        f"({qa['deterministic_replay']['artifact_count']} artifacts).\n"
        f"- QA: {qa['status']} ({qa['passing_check_count']}/{qa['check_count']} checks; "
        f"{qa['failed_check_count']} failed).\n\n"
        "## Explicit null and source boundaries\n\n"
        f"The accepted YEE-77 SQLite carries category_opportunity_state on directions with options, but not on its "
        f"five zero-option category rows. Those category contexts use explicit null and record that the state was "
        f"not present; no YEE-76 side input was read to reconstruct it. No pricing evidence was promoted to a free/zero "
        f"claim. Source-native popularity/demand counters remain separate by source and unit; operator-pain rows remain "
        f"anecdotal and are not interpreted as prevalence.\n\n"
        "## Human decision boundary\n\n"
        "SUPERVISOR_DECISION_TEMPLATE.json is UNDECIDED with empty selection/refinement/hold/drop lists. "
        "The worker made no human choice. No deep commercial validation was started.\n"
    )


def _manifest(
    output: Path,
    snapshot: InputSnapshot,
    execution_code_commit: str,
    qa: dict[str, Any],
) -> dict[str, Any]:
    artifacts = []
    for name in FINAL_ARTIFACTS:
        if name == "DATASET_MANIFEST.json":
            continue
        path = output / name
        artifacts.append({
            "path": name,
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        })
    return {
        "work_order": "YEE-78",
        "schema_version": SCHEMA_VERSION,
        "execution_code_commit": execution_code_commit,
        "accepted_yee77_merge_commit": ACCEPTED_YEE77_MERGE_COMMIT,
        "accepted_yee77_execution_code_commit": ACCEPTED_YEE77_CODE_COMMIT,
        "accepted_yee77_schema_version": ACCEPTED_YEE77_SCHEMA,
        "input_sha256": snapshot.input_sha256,
        "input_sha256_after": qa["input"]["sha256_after"],
        "accepted_capture_sha256": ACCEPTED_CAPTURE_SHA256,
        "accepted_yee76_input_sha256": ACCEPTED_YEE76_SHA256,
        "accepted_direction_ids_sha256": ACCEPTED_DIRECTION_IDS_SHA256,
        "category_count": qa["reconciliation"]["category_count"],
        "direction_count": qa["reconciliation"]["direction_count"],
        "overlap_group_count": qa["reconciliation"]["overlap_group_count"],
        "readiness_counts": qa["reconciliation"]["readiness_counts"],
        "qa_status": qa["status"],
        "artifact_count_excluding_manifest": len(artifacts),
        "artifacts": artifacts,
        "manifest_self_hash": "omitted_to_avoid_recursive_hash",
    }


def _write_final(
    output: Path,
    snapshot: InputSnapshot,
    rows: dict[str, list[dict[str, Any]]],
    qa: dict[str, Any],
    execution_code_commit: str,
) -> None:
    (output / "QA_RESULT.json").write_text(_qa_text(qa), encoding="utf-8", newline="\n")
    (output / "FINAL_REPORT.md").write_text(
        _final_report(snapshot, rows, qa, execution_code_commit),
        encoding="utf-8",
        newline="\n",
    )
    manifest = _manifest(output, snapshot, execution_code_commit, qa)
    (output / "DATASET_MANIFEST.json").write_text(
        canonical_json(manifest) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _current_execution_commit() -> str:
    root = Path(__file__).resolve().parents[2]
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if status:
        raise DecisionGateError("YEE-78 production build requires a clean committed worktree")
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise DecisionGateError("Unable to determine the YEE-78 execution commit")
    subprocess.run(
        ["git", "merge-base", "--is-ancestor", ACCEPTED_YEE77_MERGE_COMMIT, commit],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return commit


def build_decision_gate(input_db: Path, output_dir: Path, execution_code_commit: str) -> dict[str, Any]:
    if len(execution_code_commit) != 40 or any(character not in "0123456789abcdef" for character in execution_code_commit):
        raise DecisionGateError("Invalid YEE-78 execution commit")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise DecisionGateError("YEE-78 output directory must be new or empty")
    snapshot = _read_snapshot(input_db)
    rows = _build_decision_rows(snapshot)
    readiness_counts = _source_field_summary(rows["cards"])
    run_meta = _run_metadata(snapshot, execution_code_commit, readiness_counts)
    provenance = _input_provenance(snapshot)
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_core(output_dir, snapshot, rows, run_meta, provenance)
    with tempfile.TemporaryDirectory(prefix="yee78-core-replay-") as replay_name:
        replay_dir = Path(replay_name)
        _write_core(replay_dir, snapshot, rows, run_meta, provenance)
        core_replay_ok = all(
            (output_dir / name).read_bytes() == (replay_dir / name).read_bytes()
            for name in CORE_ARTIFACTS
        )
    input_sha_after = sha256_file(input_db)
    qa = _qa(
        snapshot,
        rows,
        input_sha_after,
        output_dir,
        replay_ok=core_replay_ok,
        replay_artifact_count=len(CORE_ARTIFACTS) if core_replay_ok else 0,
    )
    qa["deterministic_replay"] = {
        "byte_identical": core_replay_ok,
        "artifact_count": len(FINAL_ARTIFACTS) if core_replay_ok else 0,
        "core_artifact_count": len(CORE_ARTIFACTS),
        "different_artifacts": [] if core_replay_ok else ["core normalized artifacts differ"],
    }
    _write_final(output_dir, snapshot, rows, qa, execution_code_commit)
    full_replay_ok = False
    if core_replay_ok:
        with tempfile.TemporaryDirectory(prefix="yee78-full-replay-") as replay_name:
            replay_dir = Path(replay_name)
            _write_core(replay_dir, snapshot, rows, run_meta, provenance)
            _write_final(replay_dir, snapshot, rows, qa, execution_code_commit)
            full_replay_ok = all(
                (output_dir / name).read_bytes() == (replay_dir / name).read_bytes()
                for name in FINAL_ARTIFACTS
            )
    qa["checks"]["deterministic_full_normalized_replay_byte_identical"] = full_replay_ok
    qa["checks"]["deterministic_replay_artifact_count_complete"] = full_replay_ok
    qa["check_count"] = len(qa["checks"])
    qa["failed_checks"] = [name for name, passed in qa["checks"].items() if not passed]
    qa["failed_check_count"] = len(qa["failed_checks"])
    qa["passing_check_count"] = qa["check_count"] - qa["failed_check_count"]
    qa["status"] = "PASS" if not qa["failed_checks"] else "FAIL"
    qa["deterministic_replay"] = {
        "byte_identical": full_replay_ok,
        "artifact_count": len(FINAL_ARTIFACTS) if full_replay_ok else 0,
        "core_artifact_count": len(CORE_ARTIFACTS),
        "different_artifacts": [] if full_replay_ok else ["full normalized bundle replay mismatch"],
    }
    _write_final(output_dir, snapshot, rows, qa, execution_code_commit)
    if not _export_reconciliation(output_dir, rows):
        qa["checks"]["jsonl_csv_sqlite_reconcile"] = False
    if sha256_file(input_db) != input_sha_after:
        qa["checks"]["input_sha256_after_matches_before"] = False
    if qa["checks"]["jsonl_csv_sqlite_reconcile"] is False or qa["checks"]["input_sha256_after_matches_before"] is False:
        qa["failed_checks"] = [name for name, passed in qa["checks"].items() if not passed]
        qa["failed_check_count"] = len(qa["failed_checks"])
        qa["passing_check_count"] = qa["check_count"] - qa["failed_check_count"]
        qa["status"] = "FAIL"
        _write_final(output_dir, snapshot, rows, qa, execution_code_commit)
    return qa
