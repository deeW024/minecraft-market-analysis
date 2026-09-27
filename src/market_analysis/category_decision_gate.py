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


SCHEMA_VERSION = "yee-79-category-direction-decision-gate-v0.1"
YEE77_SQLITE_SHA256 = "a2ea751823358e1032c36fd31a88d2e581476bf6e915a53c042b70614bcc4f48"
YEE77_REVIEWED_HEAD = "f2c5ff65a668fbdd59bc701481bf7b2650e6ec4a"
YEE77_MERGE_COMMIT = "c6734865f25ec8b4b0307d44655dcfa49a7fcc01"
YEE77_SCHEMA_VERSION = "yee-77-category-targeted-external-research-v0.1"
YEE76_SQLITE_SHA256 = "700c22ad7bdd2ba9502e5adf933b3994e4ad84a852caef26d400094a9e8734bb"
YEE76_RUN_ID = "acfbe03b846c5b3387695b67ad2cef4cade33fa81eabb8069b038ac797615a7b"
YEE76_SCHEMA_VERSION = "yee-76-category-opportunity-map-v0.1"
YEE76_CODE_COMMIT = "71f24f8e825eaef5f989082d59ef0fa7e5f0bad4"
YEE76_MERGE_COMMIT = "03ffe29974681646780d5e6f40ade940d0480780"
YEE76_TAXONOMY_SHA256 = "b5720325dea06863408dfa1a05e2f981ecfeb3fac4a9e7266083ee17839c5126"
DIRECTION_IDS_SHA256 = "5751f459900e9904368e06ac16c05d42620908520cdd610c1f2bf45dda9c4831"
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
STAGE_D_FIELDS = (
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
    "direction_evidence_pack",
)
STAGE_E_FIELDS = (
    "research_status",
    "research_coverage_status",
    "resolved_market_job",
    "market_job_summary",
)
GAP_CODE_ORDER = (
    "RESEARCH_AMBIGUOUS",
    "RESEARCH_UNRESOLVED",
    "RESEARCH_PARTIAL",
    "NO_EXPLICIT_PRICING_EVIDENCE",
    "NO_OPERATOR_PAIN_EVIDENCE",
    "NO_POPULARITY_PROXY",
    "NO_DIFFERENTIATION_HYPOTHESIS",
    "NO_DIRECT_COMPETITOR",
    "SINGLE_EVIDENCE_DOMAIN",
    "SEMANTIC_OVERLAP_PRESENT",
)
PRIMARY_OR_MARKETPLACE_SOURCE_TYPES = {
    "PRIMARY_DOCS",
    "PRIMARY_PRODUCT",
    "PRIMARY_REPOSITORY",
    "PRIMARY_SUPPORT",
    "MARKETPLACE_LISTING",
}
WORKSHEET_CHOICES = (
    "ADVANCE_FOR_DEEP_VALIDATION",
    "HOLD",
    "REJECT_FROM_CURRENT_COHORT",
)
CORE_ARTIFACTS = (
    "GOAL_ALIGNMENT.md",
    "DECISION_GATE_PROTOCOL.md",
    "DECISION_GATE_SCHEMA.md",
    "direction_decision_cards.jsonl",
    "direction_decision_cards.csv",
    "category_decision_context.jsonl",
    "category_decision_context.csv",
    "SUPERVISOR_DECISION_BRIEF.md",
    "decision_gate.sqlite",
)
REQUIRED_ARTIFACTS = (
    *CORE_ARTIFACTS,
    "QA_RESULT.json",
    "FINAL_REPORT.md",
    "DATASET_MANIFEST.json",
)
FORBIDDEN_OUTPUT_FIELDS = {
    "score",
    "weighted_score",
    "rank",
    "winner",
    "recommendation",
    "build_recommendation",
    "concept",
    "revenue_estimate",
    "tam_estimate",
    "profit_estimate",
    "pareto_frontier",
    "attractiveness_score",
}


class InputMismatch(ValueError):
    """The accepted Stage E input does not match its pinned contract."""


@dataclass(frozen=True)
class InputPins:
    sqlite_sha256: str = YEE77_SQLITE_SHA256
    schema_version: str = YEE77_SCHEMA_VERSION
    execution_commit: str = YEE77_REVIEWED_HEAD
    yee76_sha256: str = YEE76_SQLITE_SHA256
    yee76_run_id: str = YEE76_RUN_ID
    yee76_schema_version: str = YEE76_SCHEMA_VERSION
    yee76_code_commit: str = YEE76_CODE_COMMIT
    yee76_merge_commit: str = YEE76_MERGE_COMMIT
    yee76_taxonomy_sha256: str = YEE76_TAXONOMY_SHA256
    direction_ids_sha256: str = DIRECTION_IDS_SHA256
    category_order: tuple[str, ...] = CATEGORY_ORDER
    option_count: int = 16
    category_count: int = 11
    table_counts: tuple[tuple[str, int], ...] = (
        ("frozen_option_snapshot", 16),
        ("frozen_category_snapshot", 11),
        ("direction_research_packs", 16),
        ("category_research_coverage", 11),
        ("source_documents", 53),
        ("research_queries", 71),
        ("external_evidence", 135),
        ("competitor_entities", 33),
        ("direction_semantic_relations", 2),
    )
    expected_state_counts: tuple[tuple[str, str, int], ...] = (
        ("AMBIGUOUS", "AMBIGUOUS", 4),
        ("RESOLVED", "SUFFICIENT", 12),
    )


CANONICAL_INPUT_PINS = InputPins()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _direction_ids_sha256(direction_ids: list[str]) -> str:
    return _sha256_bytes(("".join(f"{value}\n" for value in sorted(direction_ids))).encode("utf-8"))


def _execution_commit() -> str:
    repo_root = Path(__file__).resolve().parents[2]
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=repo_root, text=True, encoding="utf-8"
    ).strip()


def _required_table_rows(connection: sqlite3.Connection, table: str, query: str) -> list[tuple]:
    try:
        return connection.execute(query).fetchall()
    except sqlite3.Error as exc:
        raise InputMismatch(f"Required YEE-77 table {table} is not readable: {exc}") from exc


def _record_rows(
    connection: sqlite3.Connection,
    table: str,
    query: str,
    columns: tuple[str, ...],
) -> list[dict[str, Any]]:
    rows = _required_table_rows(connection, table, query)
    result = []
    for row in rows:
        item = dict(zip(columns, row, strict=True))
        raw = item.pop("record_json")
        try:
            record = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise InputMismatch(f"Invalid record_json in YEE-77 table {table}") from exc
        item["record"] = record
        result.append(item)
    return result


def _read_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
    metadata = dict(_required_table_rows(connection, "metadata", "SELECT key,value FROM metadata"))
    input_provenance = dict(
        _required_table_rows(connection, "input_provenance", "SELECT key,value FROM input_provenance")
    )
    return {
        "metadata": metadata,
        "input_provenance": input_provenance,
        "options": _record_rows(
            connection,
            "frozen_option_snapshot",
            "SELECT direction_id,category_id,record_json FROM frozen_option_snapshot ORDER BY direction_id",
            ("direction_id", "category_id", "record_json"),
        ),
        "categories": _record_rows(
            connection,
            "frozen_category_snapshot",
            "SELECT category_id,category_order,record_json FROM frozen_category_snapshot ORDER BY category_order",
            ("category_id", "category_order", "record_json"),
        ),
        "packs": _record_rows(
            connection,
            "direction_research_packs",
            "SELECT direction_id,category_id,research_status,research_coverage_status,record_json "
            "FROM direction_research_packs ORDER BY direction_id",
            ("direction_id", "category_id", "research_status", "research_coverage_status", "record_json"),
        ),
        "coverage": _record_rows(
            connection,
            "category_research_coverage",
            "SELECT category_id,option_count,category_research_status,record_json "
            "FROM category_research_coverage ORDER BY category_id",
            ("category_id", "option_count", "category_research_status", "record_json"),
        ),
        "sources": _record_rows(
            connection,
            "source_documents",
            "SELECT source_id,source_domain,canonical_url,record_json FROM source_documents ORDER BY source_id",
            ("source_id", "source_domain", "canonical_url", "record_json"),
        ),
        "queries": _record_rows(
            connection,
            "research_queries",
            "SELECT query_id,direction_id,category_id,record_json FROM research_queries ORDER BY query_id",
            ("query_id", "direction_id", "category_id", "record_json"),
        ),
        "evidence": _record_rows(
            connection,
            "external_evidence",
            "SELECT evidence_id,direction_id,category_id,source_id,claim_type,record_json "
            "FROM external_evidence ORDER BY evidence_id",
            ("evidence_id", "direction_id", "category_id", "source_id", "claim_type", "record_json"),
        ),
        "competitors": _record_rows(
            connection,
            "competitor_entities",
            "SELECT competitor_id,direction_id,category_id,record_json "
            "FROM competitor_entities ORDER BY competitor_id",
            ("competitor_id", "direction_id", "category_id", "record_json"),
        ),
        "relations": _record_rows(
            connection,
            "direction_semantic_relations",
            "SELECT relation_id,direction_id,related_direction_id,category_id,record_json "
            "FROM direction_semantic_relations ORDER BY relation_id",
            ("relation_id", "direction_id", "related_direction_id", "category_id", "record_json"),
        ),
        "table_counts": {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "frozen_option_snapshot",
                "frozen_category_snapshot",
                "direction_research_packs",
                "category_research_coverage",
                "source_documents",
                "research_queries",
                "external_evidence",
                "competitor_entities",
                "direction_semantic_relations",
            )
        },
    }


def _assert_input_pins(
    snapshot: dict[str, Any],
    input_sha256: str,
    integrity: str,
    foreign_key_errors: list[tuple],
    pins: InputPins = CANONICAL_INPUT_PINS,
) -> dict[str, bool]:
    metadata = snapshot["metadata"]
    provenance = snapshot["input_provenance"]
    options = snapshot["options"]
    categories = snapshot["categories"]
    packs = snapshot["packs"]
    checks: dict[str, bool] = {}

    def require(name: str, condition: bool, message: str) -> None:
        checks[name] = bool(condition)
        if not condition:
            raise InputMismatch(message)

    require("accepted_yee77_sqlite_sha256", input_sha256 == pins.sqlite_sha256, "Accepted YEE-77 SQLite SHA-256 mismatch")
    require("yee77_schema_version", metadata.get("schema_version") == pins.schema_version, "YEE-77 schema version mismatch")
    require("yee77_execution_commit", metadata.get("execution_code_commit") == pins.execution_commit, "YEE-77 execution commit mismatch")
    require("yee76_sha256_before", metadata.get("input_sha256_before") == pins.yee76_sha256, "Inherited YEE-76 input SHA-256 mismatch")
    require("yee76_sha256_after", metadata.get("input_sha256_after") == pins.yee76_sha256, "Inherited YEE-76 before/after SHA-256 mismatch")
    require("yee76_run_id", metadata.get("input_run_id") == pins.yee76_run_id, "Inherited YEE-76 run_id mismatch")
    require("yee76_schema_version", metadata.get("input_schema_version") == pins.yee76_schema_version, "Inherited YEE-76 schema mismatch")
    require("yee76_code_commit", metadata.get("input_code_commit") == pins.yee76_code_commit, "Inherited YEE-76 code commit mismatch")
    require("yee76_merge_commit", metadata.get("input_merge_commit") == pins.yee76_merge_commit, "Inherited YEE-76 merge commit mismatch")
    require("yee76_taxonomy_sha256", metadata.get("input_taxonomy_sha256") == pins.yee76_taxonomy_sha256, "Inherited taxonomy SHA-256 mismatch")
    require("yee76_taxonomy_version", metadata.get("input_taxonomy_version") == "yee-61-functional-category-taxonomy-v0.1", "Inherited taxonomy version mismatch")
    require("input_work_order", metadata.get("input_work_order") == "YEE-76", "Unexpected YEE-77 upstream work order")
    require("input_opened_read_only", metadata.get("input_read_mode") == "read-only", "YEE-76 provenance does not declare read-only input")
    require("integrity_check", integrity == "ok", "Accepted YEE-77 SQLite integrity check failed")
    require("foreign_key_check", not foreign_key_errors, "Accepted YEE-77 SQLite contains foreign key errors")

    counts = snapshot["table_counts"]
    require("exact_option_rows", counts["frozen_option_snapshot"] == pins.option_count, "YEE-77 frozen option count mismatch")
    require("exact_category_rows", counts["frozen_category_snapshot"] == pins.category_count, "YEE-77 frozen category count mismatch")
    require("pack_count_matches_options", counts["direction_research_packs"] == pins.option_count, "YEE-77 pack count mismatch")
    require("coverage_count_matches_categories", counts["category_research_coverage"] == pins.category_count, "YEE-77 coverage count mismatch")
    for table, expected in pins.table_counts:
        require(f"{table}_count", counts[table] == expected, f"YEE-77 {table} count mismatch")

    direction_ids = [row["direction_id"] for row in options]
    direction_hash = _direction_ids_sha256(direction_ids)
    require("direction_id_set_sha256", direction_hash == pins.direction_ids_sha256, "YEE-77 direction-id set SHA-256 mismatch")
    require("metadata_direction_id_set_sha256", metadata.get("direction_ids_sha256") == pins.direction_ids_sha256, "YEE-77 metadata direction-id SHA-256 mismatch")
    category_ids = [row["category_id"] for row in categories]
    require("frozen_category_order", tuple(category_ids) == pins.category_order, "YEE-77 category order mismatch")
    require("frozen_category_orders_contiguous", [row["category_order"] for row in categories] == list(range(pins.category_count)), "YEE-77 category_order values are not contiguous")
    require("option_record_ids_match", all(row["record"].get("direction_id") == row["direction_id"] for row in options), "Frozen option identity columns disagree with their records")
    require("option_categories_valid", all(row["category_id"] in category_ids and row["record"].get("category_id") == row["category_id"] for row in options), "Frozen option references an unknown or inconsistent category")
    require("pack_identity_matches", all(row["record"].get("direction_id") == row["direction_id"] and row["record"].get("category_id") == row["category_id"] for row in packs), "YEE-77 pack identity columns disagree with records")
    require("pack_coverage_columns_match", all(row["record"].get("research_status") == row["research_status"] and row["record"].get("research_coverage_status") == row["research_coverage_status"] for row in packs), "YEE-77 pack state columns disagree with records")

    state_counts = Counter((row["research_status"], row["research_coverage_status"]) for row in packs)
    actual_state_counts = tuple(sorted((status, coverage, count) for (status, coverage), count in state_counts.items()))
    require("frozen_readiness_shape", actual_state_counts == pins.expected_state_counts, "YEE-77 frozen readiness state shape mismatch")
    require("expected_status_totals", actual_state_counts == pins.expected_state_counts, "YEE-77 expected research states mismatch")

    for name, value in snapshot["input_provenance"].items():
        if name == "input_read_only":
            require("yee77_upstream_read_only", value == "true", "YEE-77 upstream provenance is not read-only")
    return checks


def _load_input(path: Path, pins: InputPins = CANONICAL_INPUT_PINS) -> dict[str, Any]:
    if not path.is_file():
        raise InputMismatch(f"Accepted YEE-77 SQLite does not exist: {path}")
    input_sha_before = _sha256_file(path)
    uri = path.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        if connection.execute("PRAGMA query_only").fetchone()[0] != 1:
            raise InputMismatch("Accepted YEE-77 SQLite could not be opened query-only")
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_key_errors = connection.execute("PRAGMA foreign_key_check").fetchall()
        snapshot = _read_snapshot(connection)
    finally:
        connection.close()
    input_sha_after_read = _sha256_file(path)
    if input_sha_after_read != input_sha_before:
        raise InputMismatch("Accepted YEE-77 SQLite changed while being read")
    input_checks = _assert_input_pins(snapshot, input_sha_before, integrity, foreign_key_errors, pins)
    snapshot.update({
        "input_path": str(path.resolve()),
        "input_sha256_before": input_sha_before,
        "input_sha256_after_read": input_sha_after_read,
        "preflight_integrity": integrity,
        "preflight_foreign_key_errors": foreign_key_errors,
        "input_checks": input_checks,
    })
    return snapshot


def _readiness(research_status: str, coverage_status: str) -> tuple[str, list[str]]:
    if research_status == "AMBIGUOUS":
        return "HOLD_AMBIGUOUS", ["RESEARCH_AMBIGUOUS"]
    if research_status == "UNRESOLVED":
        return "HOLD_UNRESOLVED", ["RESEARCH_UNRESOLVED"]
    if research_status == "RESOLVED" and coverage_status == "SUFFICIENT":
        return "DECISION_READY", []
    if research_status == "RESOLVED" and coverage_status == "PARTIAL":
        return "HOLD_PARTIAL", ["RESEARCH_PARTIAL"]
    if research_status == "RESOLVED" and coverage_status == "AMBIGUOUS":
        return "HOLD_AMBIGUOUS", ["RESEARCH_AMBIGUOUS"]
    raise ValueError(f"Unsupported YEE-77 readiness input: {research_status}/{coverage_status}")


def _group_by(rows: list[dict[str, Any]], field: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row[field]].append(row)
    return dict(grouped)


def _entity_relation(row: dict[str, Any]) -> str:
    return str(row["record"].get("relation_type") or "")


def _build_pricing_summary(
    pricing_evidence: list[dict[str, Any]],
    entities: list[dict[str, Any]],
) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    evidence_by_id = {row["evidence_id"]: row for row in pricing_evidence}
    linked_by_entity: dict[str, set[str]] = {}
    for entity in entities:
        record = entity["record"]
        linked: set[str] = set(record.get("evidence_ids") or [])
        entity_name = record.get("entity_name")
        for evidence in pricing_evidence:
            if evidence["evidence_id"] in linked or (
                entity_name is not None and evidence["record"].get("entity_name") == entity_name
            ):
                linked.add(evidence["evidence_id"])
        linked_by_entity[entity["competitor_id"]] = linked.intersection(evidence_by_id)

    observations = []
    paid_count = 0
    free_count = 0
    for row in pricing_evidence:
        record = row["record"]
        currency = record.get("currency") or "UNKNOWN_CURRENCY"
        observation = {
            "evidence_id": row["evidence_id"],
            "source_id": row["source_id"],
            "entity_name": record.get("entity_name"),
            "numeric_value": record.get("numeric_value"),
            "numeric_unit": record.get("numeric_unit"),
            "currency": record.get("currency"),
            "observation": record.get("observation"),
        }
        grouped[currency].append(observation)
        observations.append(observation)
        value = record.get("numeric_value")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if value > 0:
                paid_count += 1
            elif value == 0:
                free_count += 1
    unknown_price_entities = sum(not linked_by_entity[row["competitor_id"]] for row in entities)
    return {
        "pricing_evidence_count": len(pricing_evidence),
        "pricing_observations_by_currency": {
            currency: sorted(values, key=lambda item: item["evidence_id"])
            for currency, values in sorted(grouped.items())
        },
        "explicit_paid_price_point_count": paid_count,
        "explicit_free_price_point_count": free_count,
        "unknown_price_entity_count": unknown_price_entities,
        "pricing_evidence_ids": sorted(row["evidence_id"] for row in pricing_evidence),
        "pricing_evidence_by_entity": {
            competitor_id: sorted(evidence_ids)
            for competitor_id, evidence_ids in sorted(linked_by_entity.items())
        },
    }


def _gap_codes(
    research_status: str,
    coverage_status: str,
    pricing_count: int,
    pain_count: int,
    popularity_count: int,
    hypothesis_count: int,
    direct_count: int,
    domain_count: int,
    relation_count: int,
) -> list[str]:
    candidates = set()
    if research_status == "AMBIGUOUS":
        candidates.add("RESEARCH_AMBIGUOUS")
    if research_status == "UNRESOLVED":
        candidates.add("RESEARCH_UNRESOLVED")
    if coverage_status == "PARTIAL":
        candidates.add("RESEARCH_PARTIAL")
    if pricing_count == 0:
        candidates.add("NO_EXPLICIT_PRICING_EVIDENCE")
    if pain_count == 0:
        candidates.add("NO_OPERATOR_PAIN_EVIDENCE")
    if popularity_count == 0:
        candidates.add("NO_POPULARITY_PROXY")
    if hypothesis_count == 0:
        candidates.add("NO_DIFFERENTIATION_HYPOTHESIS")
    if direct_count == 0:
        candidates.add("NO_DIRECT_COMPETITOR")
    if domain_count == 1:
        candidates.add("SINGLE_EVIDENCE_DOMAIN")
    if relation_count:
        candidates.add("SEMANTIC_OVERLAP_PRESENT")
    return [code for code in GAP_CODE_ORDER if code in candidates]


def _build_cards(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    options = {row["direction_id"]: row["record"] for row in snapshot["options"]}
    packs = {row["direction_id"]: row["record"] for row in snapshot["packs"]}
    categories = {row["category_id"]: row["record"] for row in snapshot["categories"]}
    sources = {row["source_id"]: row["record"] for row in snapshot["sources"]}
    evidence_by_direction = _group_by(snapshot["evidence"], "direction_id")
    entities_by_direction = _group_by(snapshot["competitors"], "direction_id")
    queries_by_direction = _group_by(snapshot["queries"], "direction_id")
    relations_by_direction: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in snapshot["relations"]:
        relations_by_direction[row["direction_id"]].append(row)
        if row["related_direction_id"] != row["direction_id"]:
            relations_by_direction[row["related_direction_id"]].append(row)

    cards = []
    for option_row in sorted(
        snapshot["options"],
        key=lambda row: (int(categories[row["category_id"]]["category_order"]), row["direction_id"]),
    ):
        direction_id = option_row["direction_id"]
        option = options[direction_id]
        pack = packs[direction_id]
        evidence = evidence_by_direction.get(direction_id, [])
        entities = sorted(entities_by_direction.get(direction_id, []), key=lambda row: row["competitor_id"])
        queries = queries_by_direction.get(direction_id, [])
        relations = sorted(
            {row["relation_id"]: row for row in relations_by_direction.get(direction_id, [])}.values(),
            key=lambda row: row["relation_id"],
        )
        status = pack["research_status"]
        coverage_status = pack["research_coverage_status"]
        readiness, hold_reason_codes = _readiness(status, coverage_status)
        direct = [row for row in entities if _entity_relation(row) == "DIRECT"]
        substitutes = [row for row in entities if _entity_relation(row) == "SUBSTITUTE"]
        adjacent = [row for row in entities if _entity_relation(row) == "ADJACENT"]
        pricing_evidence = [row for row in evidence if row["claim_type"] == "PRICING"]
        pain_evidence = [row for row in evidence if row["claim_type"] == "OPERATOR_PAIN"]
        popularity_evidence = [row for row in evidence if row["claim_type"] == "POPULARITY_PROXY"]
        feature_evidence = [row for row in evidence if row["claim_type"] == "FEATURE"]
        evidence_source_ids = sorted({row["source_id"] for row in evidence})
        domains = sorted({sources[source_id]["source_domain"] for source_id in evidence_source_ids})
        primary_or_marketplace_count = sum(
            sources[row["source_id"]].get("source_type") in PRIMARY_OR_MARKETPLACE_SOURCE_TYPES
            for row in evidence
        )
        community_count = sum(
            sources[row["source_id"]].get("source_type") == "COMMUNITY"
            for row in evidence
        )
        pain_source_ids = {row["source_id"] for row in pain_evidence}
        pain_domains = {sources[source_id]["source_domain"] for source_id in pain_source_ids}
        price = _build_pricing_summary(pricing_evidence, entities)
        hypothesis_rows = list(pack.get("differentiation_hypotheses") or [])
        hypothesis_evidence_ids = sorted({
            evidence_id
            for hypothesis in hypothesis_rows
            for evidence_id in (hypothesis.get("evidence_ids") or [])
        })
        relation_ids = list(pack.get("semantic_relation_ids") or [])
        relation_records = [row["record"] for row in relations if row["relation_id"] in relation_ids]
        evidence_ids = sorted(row["evidence_id"] for row in evidence)
        competitor_ids = [row["competitor_id"] for row in entities]
        source_ids = sorted(set(pack.get("source_ids") or []) | set(evidence_source_ids))
        all_relation_ids = sorted(set(relation_ids) | {row["relation_id"] for row in relations})
        gaps = _gap_codes(
            status,
            coverage_status,
            len(pricing_evidence),
            len(pain_evidence),
            len(popularity_evidence),
            len(hypothesis_rows),
            len(direct),
            len(domains),
            len(all_relation_ids),
        )
        pricing_linked = price["pricing_evidence_by_entity"]
        lifecycle = [
            {
                "competitor_id": row["competitor_id"],
                "entity_name": row["record"].get("entity_name"),
                "maintenance_status": row["record"].get("maintenance_status"),
                "lifecycle_evidence_ids": list(row["record"].get("lifecycle_evidence_ids") or []),
            }
            for row in direct
        ]
        popularity_metrics = [
            {
                "evidence_id": row["evidence_id"],
                "source_id": row["source_id"],
                "source_domain": sources[row["source_id"]]["source_domain"],
                "metric_label": row["record"].get("notes") or row["record"].get("numeric_unit"),
                "numeric_value": row["record"].get("numeric_value"),
                "numeric_unit": row["record"].get("numeric_unit"),
                "observation": row["record"].get("observation"),
            }
            for row in popularity_evidence
        ]
        feature_themes = list(pack.get("feature_themes") or [])
        card = {
            "direction_id": direction_id,
            "category_id": option["category_id"],
            "category_order": option.get("category_order", categories[option["category_id"]]["category_order"]),
            "category_opportunity_state": option["category_opportunity_state"],
            "direction_type": option["direction_type"],
            "canonical_direction_key": option["canonical_direction_key"],
            "candidate_state": option["candidate_state"],
            "direction_tier": option["direction_tier"],
            "positive_support_shape": option["positive_support_shape"],
            "risk_flags": list(option.get("risk_flags") or []),
            "reason_codes": list(option.get("reason_codes") or []),
            "direction_evidence_pack": option["direction_evidence_pack"],
            "research_status": pack["research_status"],
            "research_coverage_status": pack["research_coverage_status"],
            "resolved_market_job": pack.get("resolved_market_job"),
            "market_job_summary": pack.get("market_job_summary"),
            "decision_readiness_state": readiness,
            "hold_reason_codes": hold_reason_codes,
            "semantic_relation_ids": all_relation_ids,
            "semantic_relations": relation_records,
            "evidence_count": len(evidence),
            "distinct_domain_count": len(domains),
            "distinct_domains": domains,
            "primary_or_marketplace_evidence_count": primary_or_marketplace_count,
            "community_evidence_count": community_count,
            "query_count": len(queries),
            "direct_competitor_count": len(direct),
            "direct_competitor_ids": [row["competitor_id"] for row in direct],
            "substitute_competitor_count": len(substitutes),
            "substitute_competitor_ids": [row["competitor_id"] for row in substitutes],
            "adjacent_context_count": len(adjacent),
            "adjacent_context_ids": [row["competitor_id"] for row in adjacent],
            "competitor_ids": competitor_ids,
            "direct_competitor_lifecycle_statuses": lifecycle,
            "pricing_evidence_count": price["pricing_evidence_count"],
            "pricing_observations_by_currency": price["pricing_observations_by_currency"],
            "explicit_paid_price_point_count": price["explicit_paid_price_point_count"],
            "explicit_free_price_point_count": price["explicit_free_price_point_count"],
            "unknown_price_entity_count": price["unknown_price_entity_count"],
            "pricing_evidence_ids": price["pricing_evidence_ids"],
            "operator_pain_evidence_count": len(pain_evidence),
            "operator_pain_distinct_source_count": len(pain_source_ids),
            "operator_pain_distinct_domain_count": len(pain_domains),
            "popularity_proxy_count": len(popularity_evidence),
            "popularity_proxy_native_metrics": popularity_metrics,
            "feature_evidence_count": len(feature_evidence),
            "feature_themes": feature_themes,
            "differentiation_hypothesis_count": len(hypothesis_rows),
            "differentiation_hypotheses": hypothesis_rows,
            "evidence_ids": evidence_ids,
            "source_ids": source_ids,
            "evidence_gap_codes": gaps,
            "decision_fact_provenance": {
                "frozen_option_snapshot_direction_id": direction_id,
                "direction_research_pack_direction_id": direction_id,
                "readiness": {
                    "direction_research_pack_direction_id": direction_id,
                    "fields": ["research_status", "research_coverage_status"],
                },
                "evidence": {"evidence_ids": evidence_ids},
                "queries": {"query_ids": sorted(row["query_id"] for row in queries)},
                "pricing": {"evidence_ids": price["pricing_evidence_ids"]},
                "operator_pain": {"evidence_ids": sorted(row["evidence_id"] for row in pain_evidence)},
                "popularity": {"evidence_ids": sorted(row["evidence_id"] for row in popularity_evidence)},
                "features": {"evidence_ids": sorted(row["evidence_id"] for row in feature_evidence)},
                "hypotheses": {
                    "direction_research_pack_direction_id": direction_id,
                    "evidence_ids": hypothesis_evidence_ids,
                },
                "competitors": {"competitor_ids": competitor_ids},
                "sources": {"source_ids": source_ids},
                "relations": {"relation_ids": all_relation_ids},
            },
            "decision_notes": {
                "research_notes": pack.get("research_notes"),
                "maintenance_summary": pack.get("maintenance_summary"),
                "popularity_proxy_summary": pack.get("popularity_proxy_summary"),
                "pricing_observations_by_currency_in_stage_e": pack.get("pricing_observations_by_currency"),
            },
        }
        for entity in entities:
            record = entity["record"]
            if record.get("price_amount") is not None and not pricing_linked[entity["competitor_id"]]:
                raise InputMismatch(
                    f"Competitor price lacks linked PRICING evidence for {direction_id}/{entity['competitor_id']}"
                )
        cards.append(card)
    return cards


def _build_category_context(snapshot: dict[str, Any], cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cards_by_category = _group_by(cards, "category_id")
    options_by_category = _group_by(snapshot["options"], "category_id")
    coverage = {row["category_id"]: row["record"] for row in snapshot["coverage"]}
    contexts = []
    for category_row in sorted(snapshot["categories"], key=lambda row: row["category_order"]):
        category_id = category_row["category_id"]
        category = category_row["record"]
        category_cards = cards_by_category.get(category_id, [])
        category_options = options_by_category.get(category_id, [])
        ready_ids = sorted(
            row["direction_id"] for row in category_cards
            if row["decision_readiness_state"] == "DECISION_READY"
        )
        hold_ids = sorted(
            row["direction_id"] for row in category_cards
            if row["decision_readiness_state"].startswith("HOLD_")
        )
        option_ids = sorted(row["direction_id"] for row in category_options)
        states_by_direction = {
            row["direction_id"]: row["record"].get("category_opportunity_state")
            for row in category_options
        }
        distinct_states = sorted({value for value in states_by_direction.values() if value is not None})
        category_state = distinct_states[0] if len(distinct_states) == 1 else None
        evidence_ids = sorted({value for row in category_cards for value in row["evidence_ids"]})
        source_ids = sorted({value for row in category_cards for value in row["source_ids"]})
        all_competitor_ids = sorted({value for row in category_cards for value in row["competitor_ids"]})
        direct_ids = sorted({value for row in category_cards for value in row["direct_competitor_ids"]})
        relation_ids = sorted({value for row in category_cards for value in row["semantic_relation_ids"]})
        stage_e = coverage[category_id]
        notes = [stage_e.get("coverage_notes")] if stage_e.get("coverage_notes") else []
        notes.extend(stage_e.get("risk_notes") or [])
        if not option_ids:
            notes.append("Zero accepted options in this category; retained as context, not negative external evidence.")
        if not distinct_states:
            notes.append("Category-level opportunity state is absent from the frozen YEE-77 category snapshot; left null.")
        elif len(distinct_states) > 1:
            notes.append("Stage D category opportunity states differ by direction; source values are retained by direction without reduction.")
        contexts.append({
            "category_id": category_id,
            "category_order": category_row["category_order"],
            "category_name": category.get("category_name"),
            "category_definition": category.get("definition"),
            "category_opportunity_state": category_state,
            "category_opportunity_state_by_direction": states_by_direction,
            "stage_e_category_research_status": stage_e.get("category_research_status"),
            "option_ids": option_ids,
            "decision_ready_ids": ready_ids,
            "hold_ids": hold_ids,
            "direct_competitor_count": len(direct_ids),
            "direct_competitor_ids": direct_ids,
            "competitor_ids": all_competitor_ids,
            "evidence_count": len(evidence_ids),
            "evidence_ids": evidence_ids,
            "source_count": len(source_ids),
            "source_ids": source_ids,
            "semantic_relation_ids": relation_ids,
            "descriptive_notes": notes,
            "decision_fact_provenance": {
                "frozen_category_snapshot_category_id": category_id,
                "category_research_coverage_category_id": category_id,
                "direction_research_pack_ids": option_ids,
                "evidence_ids": evidence_ids,
                "source_ids": source_ids,
                "competitor_ids": all_competitor_ids,
                "relation_ids": relation_ids,
            },
        })
    return contexts


def _build_dataset(snapshot: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cards = _build_cards(snapshot)
    categories = _build_category_context(snapshot, cards)
    return cards, categories


def _cell(value: Any) -> str:
    if value is None:
        return r"\N"
    if isinstance(value, (dict, list)):
        return canonical_json(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    payload = "".join(canonical_json(row) + "\n" for row in rows)
    path.write_text(payload, encoding="utf-8", newline="\n")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"Cannot export an empty required table: {path.name}")
    fields = list(rows[0])
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n", extrasaction="raise")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _cell(row[key]) for key in fields})


def _markdown_cell(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, list):
        rendered = "; ".join(str(item) for item in value)
    elif isinstance(value, dict):
        rendered = canonical_json(value)
    else:
        rendered = str(value)
    return rendered.replace("|", r"\|").replace("\n", " ")


def _format_ids(values: list[str]) -> str:
    return ", ".join(values) if values else "—"


def _render_goal_alignment(snapshot: dict[str, Any]) -> str:
    counts = Counter((row["research_status"], row["research_coverage_status"]) for row in snapshot["packs"])
    return (
        "# YEE-79 Goal Alignment\n\n"
        "## Product objective and PLUGIN_ONLY\n\n"
        "The unchanged project objective is a commercially viable paid Minecraft server-side/proxy plugin for server owners, operators, administrators, or network operators. "
        "PLUGIN_ONLY remains a hard constraint and is inherited from the linked Product Objective & Scope Guard. This Stage F dossier does not add, broaden, or reclassify the opportunity universe.\n\n"
        "## Accepted input gate\n\n"
        f"- Sole analytical input: accepted YEE-77 SQLite, SHA-256 {snapshot['input_sha256_before']}.\n"
        f"- YEE-77 schema / execution commit: {YEE77_SCHEMA_VERSION} / {YEE77_REVIEWED_HEAD}.\n"
        f"- Inherited YEE-76 SHA-256 / run: {YEE76_SQLITE_SHA256} / {YEE76_RUN_ID}.\n"
        f"- Direction-id-set SHA-256: {_direction_ids_sha256([row['direction_id'] for row in snapshot['options']])}; {len(snapshot['options'])} options and {len(snapshot['categories'])} frozen categories.\n"
        f"- Frozen research state: {counts[('RESOLVED', 'SUFFICIENT')]} RESOLVED/SUFFICIENT, "
        f"{counts[('AMBIGUOUS', 'AMBIGUOUS')]} AMBIGUOUS/AMBIGUOUS.\n"
        "- Input opened query-only; the builder verifies its SHA before and after processing and never writes to it.\n\n"
        "## Boundaries\n\n"
        "- Every accepted direction remains represented; no Stage D/E identity, state, tier, risk, reason, or evidence-pack field is edited.\n"
        "- Historical YEE-30 through YEE-59 output is excluded as evidence, priors, ranks, or decision seeds.\n"
        "- No live research, scalar score, weighting, rank, Pareto frontier, winner, build recommendation, concept, revenue/TAM/profit estimate, or Stage G action is produced.\n"
        "- Missing evidence stays missing; price null is not free and source-native popularity metrics are not combined.\n"
        "- Supervisor/User owns every advancement choice. All worksheet choices remain UNSET.\n"
    )


def _render_protocol(execution_commit: str) -> str:
    return (
        "# Decision Gate Protocol — YEE-79\n\n"
        f"Schema: {SCHEMA_VERSION}. Builder commit: {execution_commit}.\n\n"
        "1. Open the hash-pinned YEE-77 SQLite query-only and verify file SHA, SQLite integrity/FK, schema, execution commit, inherited YEE-76 provenance, 16 direction IDs, taxonomy order, and frozen 12/4 readiness shape.\n"
        "2. Copy the Stage D identity/context and Stage E status/job fields from the structured YEE-77 records. The SQLite rows are authoritative; report prose is not input.\n"
        "3. Derive readiness only from research completeness: RESOLVED+SUFFICIENT → DECISION_READY; AMBIGUOUS → HOLD_AMBIGUOUS; UNRESOLVED → HOLD_UNRESOLVED; RESOLVED+PARTIAL → HOLD_PARTIAL. Any unsupported pair fails closed.\n"
        "4. Derive evidence, source-domain, query, competitor, price, pain, feature, popularity, hypothesis, and relation facts from referenced YEE-77 rows. DIRECT, SUBSTITUTE, and ADJACENT remain separate. Pricing is sourced only from PRICING evidence; currencies stay separate; popularity retains source-native units.\n"
        "5. Emit stable cards ordered by frozen taxonomy category_order then direction_id, and category context in frozen taxonomy order, including categories with zero accepted options.\n"
        "6. Emit the brief as descriptive comparison only. No score, weight, attractiveness ranking, Pareto frontier, winner, recommendation, concept, or Stage G field is created. Worksheet values remain UNSET.\n"
        "7. Serialize JSONL as canonical UTF-8/LF JSON with JSON null; CSV as UTF-8 without BOM/LF with nested values canonicalized and null encoded as \\\\N. Store source rows and provenance links in SQLite.\n"
        "8. Rebuild core outputs twice from the frozen input and require byte-identical artifacts. Verify output hashes, SQLite integrity/FK, exports, and the unchanged input SHA.\n\n"
        "No network research is performed by this workflow.\n"
    )


def _render_schema() -> str:
    return (
        "# Decision Gate Schema — YEE-79\n\n"
        f"Version: {SCHEMA_VERSION}.\n\n"
        "## direction_decision_cards\n\n"
        "One row per accepted direction. Fields preserve Stage D identity/context and Stage E status/job, then add readiness, evidence and domain/query coverage, DIRECT/SUBSTITUTE/ADJACENT competitor IDs and counts, lifecycle evidence, PRICING-only observations grouped by currency, unknown price entity count, operator-pain counts, source-native popularity observations, copied feature themes/hypotheses/semantic relations, provenance IDs, gap codes, and descriptive notes.\n\n"
        "Readiness enum: DECISION_READY, HOLD_AMBIGUOUS, HOLD_UNRESOLVED, HOLD_PARTIAL. It is a research-completeness state, not attractiveness.\n\n"
        "## category_decision_context\n\n"
        "Exactly 11 rows in frozen taxonomy order. Includes category identity/definition, available Stage D category opportunity state (null where absent from accepted YEE-77 category rows), Stage E status, option/ready/hold IDs, direct competitor/evidence/source totals, semantic relation IDs, provenance, and non-evaluative notes. Zero-option categories remain explicit and are not treated as negative evidence.\n\n"
        "## Evidence gap codes\n\n"
        + "\n".join(f"- {code}: descriptive missingness or context flag; never a negative score." for code in GAP_CODE_ORDER)
        + "\n\nMissing price is null/unknown, not free. Free is counted only when a PRICING evidence row explicitly records numeric value zero. Currency buckets are never merged. Popularity metrics are copied per source-native metric and unit.\n\n"
        "Supervisor/User worksheet values are stored as UNSET. The permitted later choices are ADVANCE_FOR_DEEP_VALIDATION, HOLD, and REJECT_FROM_CURRENT_COHORT.\n"
    )


def _render_brief(
    snapshot: dict[str, Any],
    cards: list[dict[str, Any]],
    categories: list[dict[str, Any]],
    execution_commit: str,
) -> str:
    ready = [row for row in cards if row["decision_readiness_state"] == "DECISION_READY"]
    holds = [row for row in cards if row["decision_readiness_state"].startswith("HOLD_")]
    ready.sort(key=lambda row: (row["category_order"], row["direction_id"]))
    holds.sort(key=lambda row: (row["category_order"], row["direction_id"]))
    card_by_id = {row["direction_id"]: row for row in cards}
    competitor_by_id = {row["competitor_id"]: row["record"] for row in snapshot["competitors"]}
    state_counts = Counter(row["decision_readiness_state"] for row in cards)
    all_ids = [row["direction_id"] for row in cards]
    lines = [
        "# YEE-79 Supervisor Decision Brief",
        "",
        "## Input and audit header",
        "",
        f"- Accepted YEE-77 SQLite SHA-256: {snapshot['input_sha256_before']}",
        f"- YEE-77 schema / reviewed execution commit: {YEE77_SCHEMA_VERSION} / {YEE77_REVIEWED_HEAD}",
        f"- YEE-77 merge commit / YEE-76 merge commit: {YEE77_MERGE_COMMIT} / {YEE76_MERGE_COMMIT}",
        f"- Inherited YEE-76 SHA-256 / run_id / taxonomy SHA-256: {YEE76_SQLITE_SHA256} / {YEE76_RUN_ID} / {YEE76_TAXONOMY_SHA256}",
        f"- YEE-79 builder commit: {execution_commit}",
        f"- Direction-id-set SHA-256: {_direction_ids_sha256([row['direction_id'] for row in snapshot['options']])}",
        f"- Scope: evidence-readiness comparison for exactly {len(cards)} accepted directions; stop for Supervisor/User decision. No Stage G.",
        "",
        "## Readiness summary",
        "",
        f"- DECISION_READY: {state_counts['DECISION_READY']}",
        f"- HOLD_AMBIGUOUS: {state_counts['HOLD_AMBIGUOUS']}",
        f"- HOLD_UNRESOLVED: {state_counts['HOLD_UNRESOLVED']}",
        f"- HOLD_PARTIAL: {state_counts['HOLD_PARTIAL']}",
        f"- No hidden removals: {len(cards)} cards cover the exact frozen direction set ({_format_ids(all_ids)}).",
        "",
        "Readiness describes research completeness only; it is not a commercial attractiveness judgment.",
        "",
        "## Decision-ready directions",
        "",
        "Stable taxonomy category order, then direction_id. Rows are not ranked.",
        "",
        "| Direction ID | Category | Direction key | Resolved market job | Stage D tier/state | Direct competitors | Pricing evidence / currencies | Pain evidence | Feature themes | Hypotheses | Domains | Evidence gaps | Semantic relations |",
        "| --- | --- | --- | --- | --- | --- | --- | ---: | --- | ---: | ---: | --- | --- |",
    ]
    for row in ready:
        direct_names = [
            competitor_by_id[value].get("entity_name", value)
            for value in row["direct_competitor_ids"]
        ]
        price_currencies = ", ".join(row["pricing_observations_by_currency"]) or "none observed"
        lines.append(
            "| " + " | ".join([
                row["direction_id"],
                row["category_id"],
                _markdown_cell(row["canonical_direction_key"]),
                _markdown_cell(row["resolved_market_job"]),
                f"{row['direction_tier']} / {_markdown_cell(row['category_opportunity_state'])}",
                _markdown_cell(direct_names),
                f"{row['pricing_evidence_count']} / {price_currencies}",
                str(row["operator_pain_evidence_count"]),
                _markdown_cell(row["feature_themes"]),
                str(row["differentiation_hypothesis_count"]),
                str(row["distinct_domain_count"]),
                _markdown_cell(row["evidence_gap_codes"]),
                _markdown_cell(row["semantic_relation_ids"]),
            ]) + " |"
        )
    lines.extend([
        "",
        "## Hold directions",
        "",
        "| Direction ID | Hold state | Category | Research status / coverage | Research notes | Adjacent context | Evidence gaps | Unknown / not resolved |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ])
    for row in holds:
        notes = row["decision_notes"].get("research_notes") or row["market_job_summary"]
        adjacent = [
            competitor_by_id[value].get("entity_name", value)
            for value in row["adjacent_context_ids"]
        ]
        lines.append(
            "| " + " | ".join([
                row["direction_id"],
                row["decision_readiness_state"],
                row["category_id"],
                f"{row['research_status']} / {row['research_coverage_status']}",
                _markdown_cell(notes),
                _markdown_cell(adjacent),
                _markdown_cell(row["evidence_gap_codes"]),
                "Market interpretation remains ambiguous, unresolved, or partially covered; no forced resolution.",
            ]) + " |"
        )
    lines.extend(["", "## Descriptive tradeoff groupings", ""])

    def group(title: str, predicate) -> None:
        selected = [row["direction_id"] for row in cards if predicate(row)]
        lines.append(f"- {title}: {len(selected)} — {_format_ids(selected)}")

    group("Directions with explicit pricing evidence", lambda row: row["pricing_evidence_count"] > 0)
    group("Directions with no explicit pricing evidence", lambda row: row["pricing_evidence_count"] == 0)
    group("0 direct competitors", lambda row: row["direct_competitor_count"] == 0)
    group("1–2 direct competitors", lambda row: 1 <= row["direct_competitor_count"] <= 2)
    group("3–5 direct competitors", lambda row: 3 <= row["direct_competitor_count"] <= 5)
    group("6+ direct competitors", lambda row: row["direct_competitor_count"] >= 6)
    group("Directions with operator-pain evidence", lambda row: row["operator_pain_evidence_count"] > 0)
    group("Directions with retained differentiation hypotheses", lambda row: row["differentiation_hypothesis_count"] > 0)
    group("Directions with semantic relations", lambda row: bool(row["semantic_relation_ids"]))
    lines.extend([
        "",
        "### Decision-ready category distribution",
        "",
        "| Category | Accepted options | Decision-ready | Hold |",
        "| --- | ---: | ---: | ---: |",
    ])
    for row in categories:
        lines.append(
            f"| {row['category_id']} | {len(row['option_ids'])} | {len(row['decision_ready_ids'])} | {len(row['hold_ids'])} |"
        )
    lines.extend([
        "",
        "### Interpretation caveats",
        "",
        "- These groups expose the accepted evidence; they do not imply that more competitors are good or bad.",
        "- No explicit price means price evidence is absent. It is not a free-price observation.",
        "- Price points remain grouped by their recorded currency; no currency conversion or combined total is produced.",
        "- Popularity metrics retain source-native names and units and are not compared as a unified metric.",
        "- Operator-pain rows are source-backed observations, not prevalence estimates.",
        "- Zero-option categories are preserved context, not evidence of negative opportunity.",
        "- Differentiation hypotheses are copied from YEE-77 and remain hypotheses, not findings.",
        "",
        "## Supervisor/User decision worksheet",
        "",
        "The worker leaves every decision UNSET. The permitted later values are ADVANCE_FOR_DEEP_VALIDATION, HOLD, or REJECT_FROM_CURRENT_COHORT.",
        "",
        "| Direction ID | Supervisor/User decision |",
        "| --- | --- |",
    ])
    for row in ready:
        lines.append(f"| {row['direction_id']} | UNSET |")
    lines.extend([
        "",
        "No decision has been filled. This dossier stops before Stage G and does not choose a direction, winner, build target, or concept.",
        "",
    ])
    return "\n".join(lines)


def _write_core_docs(
    output: Path,
    snapshot: dict[str, Any],
    cards: list[dict[str, Any]],
    categories: list[dict[str, Any]],
    execution_commit: str,
) -> None:
    (output / "GOAL_ALIGNMENT.md").write_text(_render_goal_alignment(snapshot), encoding="utf-8", newline="\n")
    (output / "DECISION_GATE_PROTOCOL.md").write_text(_render_protocol(execution_commit), encoding="utf-8", newline="\n")
    (output / "DECISION_GATE_SCHEMA.md").write_text(_render_schema(), encoding="utf-8", newline="\n")
    (output / "SUPERVISOR_DECISION_BRIEF.md").write_text(
        _render_brief(snapshot, cards, categories, execution_commit), encoding="utf-8", newline="\n"
    )


def _write_sqlite(
    path: Path,
    snapshot: dict[str, Any],
    cards: list[dict[str, Any]],
    categories: list[dict[str, Any]],
    core_hashes: dict[str, str],
    execution_commit: str,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.executescript("""
            CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE input_provenance (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE frozen_direction_identity (
                direction_id TEXT PRIMARY KEY, category_id TEXT NOT NULL,
                record_json TEXT NOT NULL,
                FOREIGN KEY(category_id) REFERENCES frozen_category_identity(category_id)
            );
            CREATE TABLE frozen_category_identity (
                category_id TEXT PRIMARY KEY, category_order INTEGER NOT NULL UNIQUE,
                record_json TEXT NOT NULL
            );
            CREATE TABLE frozen_direction_research_pack (
                direction_id TEXT PRIMARY KEY, category_id TEXT NOT NULL,
                research_status TEXT, research_coverage_status TEXT, record_json TEXT NOT NULL,
                FOREIGN KEY(direction_id) REFERENCES frozen_direction_identity(direction_id),
                FOREIGN KEY(category_id) REFERENCES frozen_category_identity(category_id)
            );
            CREATE TABLE frozen_category_research_coverage (
                category_id TEXT PRIMARY KEY, option_count INTEGER NOT NULL,
                category_research_status TEXT, record_json TEXT NOT NULL,
                FOREIGN KEY(category_id) REFERENCES frozen_category_identity(category_id)
            );
            CREATE TABLE source_documents (
                source_id TEXT PRIMARY KEY, source_domain TEXT, canonical_url TEXT, record_json TEXT NOT NULL
            );
            CREATE TABLE research_queries (
                query_id TEXT PRIMARY KEY, direction_id TEXT NOT NULL, category_id TEXT NOT NULL,
                record_json TEXT NOT NULL,
                FOREIGN KEY(direction_id) REFERENCES frozen_direction_identity(direction_id),
                FOREIGN KEY(category_id) REFERENCES frozen_category_identity(category_id)
            );
            CREATE TABLE external_evidence (
                evidence_id TEXT PRIMARY KEY, direction_id TEXT NOT NULL, category_id TEXT NOT NULL,
                source_id TEXT NOT NULL, claim_type TEXT, record_json TEXT NOT NULL,
                FOREIGN KEY(direction_id) REFERENCES frozen_direction_identity(direction_id),
                FOREIGN KEY(category_id) REFERENCES frozen_category_identity(category_id),
                FOREIGN KEY(source_id) REFERENCES source_documents(source_id)
            );
            CREATE TABLE competitor_entities (
                competitor_id TEXT PRIMARY KEY, direction_id TEXT NOT NULL, category_id TEXT NOT NULL,
                record_json TEXT NOT NULL,
                FOREIGN KEY(direction_id) REFERENCES frozen_direction_identity(direction_id),
                FOREIGN KEY(category_id) REFERENCES frozen_category_identity(category_id)
            );
            CREATE TABLE direction_semantic_relations (
                relation_id TEXT PRIMARY KEY, direction_id TEXT NOT NULL, related_direction_id TEXT NOT NULL,
                category_id TEXT NOT NULL, record_json TEXT NOT NULL,
                FOREIGN KEY(direction_id) REFERENCES frozen_direction_identity(direction_id),
                FOREIGN KEY(related_direction_id) REFERENCES frozen_direction_identity(direction_id),
                FOREIGN KEY(category_id) REFERENCES frozen_category_identity(category_id)
            );
            CREATE TABLE direction_decision_cards (
                direction_id TEXT PRIMARY KEY, category_id TEXT NOT NULL,
                decision_readiness_state TEXT NOT NULL, record_json TEXT NOT NULL,
                FOREIGN KEY(direction_id) REFERENCES frozen_direction_identity(direction_id),
                FOREIGN KEY(category_id) REFERENCES frozen_category_identity(category_id)
            );
            CREATE TABLE category_decision_context (
                category_id TEXT PRIMARY KEY, category_order INTEGER NOT NULL UNIQUE,
                record_json TEXT NOT NULL,
                FOREIGN KEY(category_id) REFERENCES frozen_category_identity(category_id)
            );
            CREATE TABLE decision_fact_provenance (
                record_type TEXT NOT NULL, record_id TEXT NOT NULL, fact_area TEXT NOT NULL,
                record_json TEXT NOT NULL, PRIMARY KEY(record_type, record_id, fact_area)
            );
            CREATE TABLE competitor_evidence (
                competitor_id TEXT NOT NULL, evidence_id TEXT NOT NULL,
                PRIMARY KEY(competitor_id, evidence_id),
                FOREIGN KEY(competitor_id) REFERENCES competitor_entities(competitor_id),
                FOREIGN KEY(evidence_id) REFERENCES external_evidence(evidence_id)
            );
            CREATE TABLE relation_evidence (
                relation_id TEXT NOT NULL, evidence_id TEXT NOT NULL,
                PRIMARY KEY(relation_id, evidence_id),
                FOREIGN KEY(relation_id) REFERENCES direction_semantic_relations(relation_id),
                FOREIGN KEY(evidence_id) REFERENCES external_evidence(evidence_id)
            );
            CREATE TABLE supervisor_decision_worksheet (
                direction_id TEXT PRIMARY KEY, decision TEXT NOT NULL,
                permitted_choices_json TEXT NOT NULL,
                CHECK(decision='UNSET'),
                FOREIGN KEY(direction_id) REFERENCES direction_decision_cards(direction_id)
            );
        """)
        metadata = {
            "schema_version": SCHEMA_VERSION,
            "execution_commit": execution_commit,
            "input_schema_version": snapshot["metadata"]["schema_version"],
            "input_execution_commit": snapshot["metadata"]["execution_code_commit"],
            "input_sha256": snapshot["input_sha256_before"],
            "output_sha256_scope": "all core files except this SQLite database; its exact digest is in QA_RESULT.json and DATASET_MANIFEST.json",
            "direction_ids_sha256": _direction_ids_sha256([row["direction_id"] for row in snapshot["options"]]),
            "direction_count": str(len(cards)),
            "category_count": str(len(categories)),
        }
        metadata.update({f"output_sha256:{name}": digest for name, digest in sorted(core_hashes.items())})
        input_provenance = dict(snapshot["input_provenance"])
        input_provenance.update({
            "input_work_order": "YEE-77",
            "input_filename": "category_targeted_external_research.sqlite",
            "input_sha256": snapshot["input_sha256_before"],
            "input_sha256_after": snapshot["input_sha256_after_read"],
            "input_read_only": "true",
            "yee77_reviewed_head": YEE77_REVIEWED_HEAD,
            "yee77_merge_commit": YEE77_MERGE_COMMIT,
            "yee76_accepted_sha256": YEE76_SQLITE_SHA256,
            "yee76_run_id": YEE76_RUN_ID,
            "yee76_merge_commit": YEE76_MERGE_COMMIT,
            "yee76_taxonomy_sha256": YEE76_TAXONOMY_SHA256,
        })
        connection.executemany("INSERT INTO metadata VALUES(?,?)", sorted(metadata.items()))
        connection.executemany("INSERT INTO input_provenance VALUES(?,?)", sorted(input_provenance.items()))
        connection.executemany(
            "INSERT INTO frozen_category_identity VALUES(?,?,?)",
            [(row["category_id"], row["category_order"], canonical_json(row["record"])) for row in snapshot["categories"]],
        )
        connection.executemany(
            "INSERT INTO frozen_direction_identity VALUES(?,?,?)",
            [(row["direction_id"], row["category_id"], canonical_json(row["record"])) for row in snapshot["options"]],
        )
        connection.executemany(
            "INSERT INTO frozen_direction_research_pack VALUES(?,?,?,?,?)",
            [(row["direction_id"], row["category_id"], row["research_status"], row["research_coverage_status"], canonical_json(row["record"])) for row in snapshot["packs"]],
        )
        connection.executemany(
            "INSERT INTO frozen_category_research_coverage VALUES(?,?,?,?)",
            [(row["category_id"], row["option_count"], row["category_research_status"], canonical_json(row["record"])) for row in snapshot["coverage"]],
        )
        connection.executemany(
            "INSERT INTO source_documents VALUES(?,?,?,?)",
            [(row["source_id"], row["source_domain"], row["canonical_url"], canonical_json(row["record"])) for row in snapshot["sources"]],
        )
        connection.executemany(
            "INSERT INTO research_queries VALUES(?,?,?,?)",
            [(row["query_id"], row["direction_id"], row["category_id"], canonical_json(row["record"])) for row in snapshot["queries"]],
        )
        connection.executemany(
            "INSERT INTO external_evidence VALUES(?,?,?,?,?,?)",
            [(row["evidence_id"], row["direction_id"], row["category_id"], row["source_id"], row["claim_type"], canonical_json(row["record"])) for row in snapshot["evidence"]],
        )
        connection.executemany(
            "INSERT INTO competitor_entities VALUES(?,?,?,?)",
            [(row["competitor_id"], row["direction_id"], row["category_id"], canonical_json(row["record"])) for row in snapshot["competitors"]],
        )
        connection.executemany(
            "INSERT INTO direction_semantic_relations VALUES(?,?,?,?,?)",
            [(row["relation_id"], row["direction_id"], row["related_direction_id"], row["category_id"], canonical_json(row["record"])) for row in snapshot["relations"]],
        )
        connection.executemany(
            "INSERT INTO direction_decision_cards VALUES(?,?,?,?)",
            [(row["direction_id"], row["category_id"], row["decision_readiness_state"], canonical_json(row)) for row in cards],
        )
        connection.executemany(
            "INSERT INTO category_decision_context VALUES(?,?,?)",
            [(row["category_id"], row["category_order"], canonical_json(row)) for row in categories],
        )
        provenance_rows = []
        for card in cards:
            for area, record in card["decision_fact_provenance"].items():
                provenance_rows.append(("direction", card["direction_id"], area, canonical_json(record)))
        for category in categories:
            provenance_rows.append(("category", category["category_id"], "decision_fact_provenance", canonical_json(category["decision_fact_provenance"])))
        connection.executemany("INSERT INTO decision_fact_provenance VALUES(?,?,?,?)", provenance_rows)
        competitor_evidence = sorted({
            (row["competitor_id"], evidence_id)
            for row in snapshot["competitors"]
            for evidence_id in (row["record"].get("evidence_ids") or []) + (row["record"].get("lifecycle_evidence_ids") or [])
        })
        connection.executemany("INSERT INTO competitor_evidence VALUES(?,?)", competitor_evidence)
        relation_evidence = sorted({
            (row["relation_id"], evidence_id)
            for row in snapshot["relations"]
            for evidence_id in (row["record"].get("evidence_ids") or [])
        })
        connection.executemany("INSERT INTO relation_evidence VALUES(?,?)", relation_evidence)
        worksheet_rows = [
            (row["direction_id"], "UNSET", canonical_json(list(WORKSHEET_CHOICES)))
            for row in cards if row["decision_readiness_state"] == "DECISION_READY"
        ]
        connection.executemany("INSERT INTO supervisor_decision_worksheet VALUES(?,?,?)", worksheet_rows)
        connection.commit()
    finally:
        connection.close()


def _count_table(connection: sqlite3.Connection, table: str) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _verify_export(output: Path, cards: list[dict[str, Any]], categories: list[dict[str, Any]]) -> dict[str, Any]:
    expected = {
        "direction_decision_cards": cards,
        "category_decision_context": categories,
    }
    counts: dict[str, int] = {}
    for stem, rows in expected.items():
        jsonl_path = output / f"{stem}.jsonl"
        csv_path = output / f"{stem}.csv"
        jsonl_bytes = jsonl_path.read_bytes()
        csv_bytes = csv_path.read_bytes()
        if jsonl_bytes.startswith(b"\xef\xbb\xbf") or b"\r" in jsonl_bytes or not jsonl_bytes.endswith(b"\n"):
            raise ValueError(f"Non-canonical JSONL encoding: {jsonl_path.name}")
        if jsonl_bytes.decode("utf-8").splitlines() != [canonical_json(row) for row in rows]:
            raise ValueError(f"JSONL rows do not reconcile: {jsonl_path.name}")
        if csv_bytes.startswith(b"\xef\xbb\xbf") or b"\r" in csv_bytes or not csv_bytes.endswith(b"\n"):
            raise ValueError(f"Non-canonical CSV encoding: {csv_path.name}")
        with csv_path.open("r", encoding="utf-8-sig", newline="") as stream:
            parsed = list(csv.DictReader(stream))
        if len(parsed) != len(rows) or list(parsed[0]) != list(rows[0]):
            raise ValueError(f"CSV shape does not reconcile: {csv_path.name}")
        for parsed_row, expected_row in zip(parsed, rows, strict=True):
            if parsed_row != {key: _cell(value) for key, value in expected_row.items()}:
                raise ValueError(f"CSV values do not reconcile: {csv_path.name}")
        counts[f"{stem}_jsonl"] = len(_read_jsonl(jsonl_path))
        counts[f"{stem}_csv"] = len(parsed)
    return counts


def _forbidden_keys(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key.casefold() in FORBIDDEN_OUTPUT_FIELDS:
                found.add(key)
            found.update(_forbidden_keys(child))
    elif isinstance(value, list):
        for child in value:
            found.update(_forbidden_keys(child))
    return found


def _qa_checks(
    snapshot: dict[str, Any],
    cards: list[dict[str, Any]],
    categories: list[dict[str, Any]],
    output: Path,
    input_sha_after: str,
    replay_ok: bool,
    pins: InputPins,
) -> tuple[dict[str, bool], dict[str, Any]]:
    db = sqlite3.connect((output / "decision_gate.sqlite").resolve().as_uri() + "?mode=ro", uri=True)
    try:
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        fk_errors = list(db.execute("PRAGMA foreign_key_check"))
        expected_tables = {
            "frozen_direction_research_pack": len(snapshot["packs"]),
            "frozen_category_research_coverage": len(snapshot["coverage"]),
            "direction_decision_cards": len(cards),
            "category_decision_context": len(categories),
            "source_documents": len(snapshot["sources"]),
            "research_queries": len(snapshot["queries"]),
            "external_evidence": len(snapshot["evidence"]),
            "competitor_entities": len(snapshot["competitors"]),
            "direction_semantic_relations": len(snapshot["relations"]),
        }
        sqlite_counts = {table: _count_table(db, table) for table in expected_tables}
        db_card_rows = [json.loads(row[0]) for row in db.execute("SELECT record_json FROM direction_decision_cards ORDER BY rowid")]
        db_category_rows = [json.loads(row[0]) for row in db.execute("SELECT record_json FROM category_decision_context ORDER BY category_order")]
        worksheet = list(db.execute("SELECT direction_id,decision,permitted_choices_json FROM supervisor_decision_worksheet ORDER BY direction_id"))
        db_metadata = dict(db.execute("SELECT key,value FROM metadata"))
        frozen_reconciles = (
            [tuple(row) for row in db.execute("SELECT direction_id,category_id,record_json FROM frozen_direction_identity ORDER BY direction_id")]
            == [(row["direction_id"], row["category_id"], canonical_json(row["record"])) for row in sorted(snapshot["options"], key=lambda item: item["direction_id"])]
            and [tuple(row) for row in db.execute("SELECT category_id,category_order,record_json FROM frozen_category_identity ORDER BY category_order")]
            == [(row["category_id"], row["category_order"], canonical_json(row["record"])) for row in snapshot["categories"]]
            and [tuple(row) for row in db.execute("SELECT direction_id,category_id,research_status,research_coverage_status,record_json FROM frozen_direction_research_pack ORDER BY direction_id")]
            == [(row["direction_id"], row["category_id"], row["research_status"], row["research_coverage_status"], canonical_json(row["record"])) for row in sorted(snapshot["packs"], key=lambda item: item["direction_id"])]
            and [tuple(row) for row in db.execute("SELECT category_id,option_count,category_research_status,record_json FROM frozen_category_research_coverage ORDER BY category_id")]
            == [(row["category_id"], row["option_count"], row["category_research_status"], canonical_json(row["record"])) for row in sorted(snapshot["coverage"], key=lambda item: item["category_id"])]
            and [tuple(row) for row in db.execute("SELECT source_id,source_domain,canonical_url,record_json FROM source_documents ORDER BY source_id")]
            == [(row["source_id"], row["source_domain"], row["canonical_url"], canonical_json(row["record"])) for row in sorted(snapshot["sources"], key=lambda item: item["source_id"])]
            and [tuple(row) for row in db.execute("SELECT query_id,direction_id,category_id,record_json FROM research_queries ORDER BY query_id")]
            == [(row["query_id"], row["direction_id"], row["category_id"], canonical_json(row["record"])) for row in sorted(snapshot["queries"], key=lambda item: item["query_id"])]
            and [tuple(row) for row in db.execute("SELECT evidence_id,direction_id,category_id,source_id,claim_type,record_json FROM external_evidence ORDER BY evidence_id")]
            == [(row["evidence_id"], row["direction_id"], row["category_id"], row["source_id"], row["claim_type"], canonical_json(row["record"])) for row in sorted(snapshot["evidence"], key=lambda item: item["evidence_id"])]
            and [tuple(row) for row in db.execute("SELECT competitor_id,direction_id,category_id,record_json FROM competitor_entities ORDER BY competitor_id")]
            == [(row["competitor_id"], row["direction_id"], row["category_id"], canonical_json(row["record"])) for row in sorted(snapshot["competitors"], key=lambda item: item["competitor_id"])]
            and [tuple(row) for row in db.execute("SELECT relation_id,direction_id,related_direction_id,category_id,record_json FROM direction_semantic_relations ORDER BY relation_id")]
            == [(row["relation_id"], row["direction_id"], row["related_direction_id"], row["category_id"], canonical_json(row["record"])) for row in sorted(snapshot["relations"], key=lambda item: item["relation_id"])]
        )
    finally:
        db.close()

    input_options = {row["direction_id"]: row for row in snapshot["options"]}
    input_packs = {row["direction_id"]: row for row in snapshot["packs"]}
    card_ids = [row["direction_id"] for row in cards]
    id_set_expected = {row["direction_id"] for row in snapshot["options"]}
    readiness_counts = Counter(row["decision_readiness_state"] for row in cards)
    stage_d_preserved = all(
        all(card[field] == input_options[card["direction_id"]]["record"].get(field) for field in STAGE_D_FIELDS)
        for card in cards
    )
    stage_e_preserved = all(
        all(card[field] == input_packs[card["direction_id"]]["record"].get(field) for field in STAGE_E_FIELDS)
        for card in cards
    )
    exact_readiness = all(
        (card["decision_readiness_state"], card["hold_reason_codes"])
        == _readiness(input_packs[card["direction_id"]]["research_status"], input_packs[card["direction_id"]]["research_coverage_status"])
        for card in cards
    )
    raw_entities = _group_by(snapshot["competitors"], "direction_id")
    competitor_reconciles = all(
        card["direct_competitor_count"] == sum(_entity_relation(row) == "DIRECT" for row in raw_entities.get(card["direction_id"], []))
        and card["substitute_competitor_count"] == sum(_entity_relation(row) == "SUBSTITUTE" for row in raw_entities.get(card["direction_id"], []))
        and card["adjacent_context_count"] == sum(_entity_relation(row) == "ADJACENT" for row in raw_entities.get(card["direction_id"], []))
        and not (set(card["direct_competitor_ids"]) & set(card["adjacent_context_ids"]))
        for card in cards
    )
    raw_evidence = _group_by(snapshot["evidence"], "direction_id")
    evidence_reconciles = all(
        card["evidence_count"] == len(raw_evidence.get(card["direction_id"], []))
        and card["pricing_evidence_ids"] == sorted(row["evidence_id"] for row in raw_evidence.get(card["direction_id"], []) if row["claim_type"] == "PRICING")
        and card["operator_pain_evidence_count"] == sum(row["claim_type"] == "OPERATOR_PAIN" for row in raw_evidence.get(card["direction_id"], []))
        and card["feature_evidence_count"] == sum(row["claim_type"] == "FEATURE" for row in raw_evidence.get(card["direction_id"], []))
        and card["popularity_proxy_count"] == sum(row["claim_type"] == "POPULARITY_PROXY" for row in raw_evidence.get(card["direction_id"], []))
        for card in cards
    )
    source_rows = {row["source_id"]: row["record"] for row in snapshot["sources"]}
    query_rows = _group_by(snapshot["queries"], "direction_id")
    pack_rows = {row["direction_id"]: row["record"] for row in snapshot["packs"]}
    domain_counts = {
        card["direction_id"]: len({source_rows[row["source_id"]]["source_domain"] for row in raw_evidence.get(card["direction_id"], [])})
        for card in cards
    }
    source_query_facts_reconcile = all(
        card["query_count"] == len(query_rows.get(card["direction_id"], []))
        and card["distinct_domains"] == sorted({source_rows[row["source_id"]]["source_domain"] for row in raw_evidence.get(card["direction_id"], [])})
        and card["distinct_domain_count"] == domain_counts[card["direction_id"]]
        and card["primary_or_marketplace_evidence_count"] == sum(source_rows[row["source_id"]].get("source_type") in PRIMARY_OR_MARKETPLACE_SOURCE_TYPES for row in raw_evidence.get(card["direction_id"], []))
        and card["community_evidence_count"] == sum(source_rows[row["source_id"]].get("source_type") == "COMMUNITY" for row in raw_evidence.get(card["direction_id"], []))
        and card["source_ids"] == sorted(set(pack_rows[card["direction_id"]].get("source_ids") or []) | {row["source_id"] for row in raw_evidence.get(card["direction_id"], [])})
        for card in cards
    )
    pricing_reconciles = all(
        (lambda price: (
            card["pricing_evidence_count"] == price["pricing_evidence_count"]
            and card["pricing_observations_by_currency"] == price["pricing_observations_by_currency"]
            and card["explicit_paid_price_point_count"] == price["explicit_paid_price_point_count"]
            and card["explicit_free_price_point_count"] == price["explicit_free_price_point_count"]
            and card["unknown_price_entity_count"] == price["unknown_price_entity_count"]
            and card["pricing_evidence_ids"] == price["pricing_evidence_ids"]
        ))(_build_pricing_summary(
            [row for row in raw_evidence.get(card["direction_id"], []) if row["claim_type"] == "PRICING"],
            raw_entities.get(card["direction_id"], []),
        ))
        for card in cards
    )
    source_native_and_themes_reconcile = all(
        card["feature_themes"] == (pack_rows[card["direction_id"]].get("feature_themes") or [])
        and card["popularity_proxy_native_metrics"] == [
            {
                "evidence_id": row["evidence_id"],
                "source_id": row["source_id"],
                "source_domain": source_rows[row["source_id"]]["source_domain"],
                "metric_label": row["record"].get("notes") or row["record"].get("numeric_unit"),
                "numeric_value": row["record"].get("numeric_value"),
                "numeric_unit": row["record"].get("numeric_unit"),
                "observation": row["record"].get("observation"),
            }
            for row in raw_evidence.get(card["direction_id"], []) if row["claim_type"] == "POPULARITY_PROXY"
        ]
        for card in cards
    )
    gap_reconciles = all(
        card["evidence_gap_codes"] == _gap_codes(
            card["research_status"], card["research_coverage_status"], card["pricing_evidence_count"],
            card["operator_pain_evidence_count"], card["popularity_proxy_count"],
            card["differentiation_hypothesis_count"], card["direct_competitor_count"],
            domain_counts[card["direction_id"]], len(card["semantic_relation_ids"]),
        )
        for card in cards
    )
    relation_copy = all(
        relation in [row["record"] for row in snapshot["relations"]]
        and relation["category_id"] == next(card["category_id"] for card in cards if card["direction_id"] == relation["direction_id"])
        and relation["category_id"] == next(card["category_id"] for card in cards if card["direction_id"] == relation["related_direction_id"])
        for card in cards for relation in card["semantic_relations"]
    )
    category_options = _group_by(snapshot["options"], "category_id")
    cards_by_category = _group_by(cards, "category_id")
    category_context_reconciles = all(
        row["option_ids"] == sorted(option["direction_id"] for option in category_options.get(row["category_id"], []))
        and row["decision_ready_ids"] == sorted(card["direction_id"] for card in cards_by_category.get(row["category_id"], []) if card["decision_readiness_state"] == "DECISION_READY")
        and row["hold_ids"] == sorted(card["direction_id"] for card in cards_by_category.get(row["category_id"], []) if card["decision_readiness_state"].startswith("HOLD_"))
        and row["direct_competitor_ids"] == sorted({competitor["competitor_id"] for card in cards_by_category.get(row["category_id"], []) for competitor in raw_entities.get(card["direction_id"], []) if _entity_relation(competitor) == "DIRECT"})
        and row["semantic_relation_ids"] == sorted({relation_id for card in cards_by_category.get(row["category_id"], []) for relation_id in card["semantic_relation_ids"]})
        and row["evidence_ids"] == sorted({evidence_id for card in cards_by_category.get(row["category_id"], []) for evidence_id in card["evidence_ids"]})
        and row["source_ids"] == sorted({source_id for card in cards_by_category.get(row["category_id"], []) for source_id in card["source_ids"]})
        and row["stage_e_category_research_status"] == next(coverage["record"]["category_research_status"] for coverage in snapshot["coverage"] if coverage["category_id"] == row["category_id"])
        for row in categories
    )
    category_order_ok = [row["category_id"] for row in categories] == list(CATEGORY_ORDER)
    ready_ids = [row["direction_id"] for row in cards if row["decision_readiness_state"] == "DECISION_READY"]
    worksheet_ok = len(worksheet) == len(ready_ids) and all(
        decision == "UNSET" and json.loads(choices) == list(WORKSHEET_CHOICES)
        for _, decision, choices in worksheet
    )
    export_counts = _verify_export(output, cards, categories)
    no_forbidden = not _forbidden_keys(cards) and not _forbidden_keys(categories)
    provenance_reconciles = all(
        card["decision_fact_provenance"]["direction_research_pack_direction_id"] == card["direction_id"]
        and card["decision_fact_provenance"]["evidence"]["evidence_ids"] == card["evidence_ids"]
        and card["decision_fact_provenance"]["queries"]["query_ids"] == sorted(row["query_id"] for row in query_rows.get(card["direction_id"], []))
        and card["decision_fact_provenance"]["competitors"]["competitor_ids"] == card["competitor_ids"]
        and card["decision_fact_provenance"]["sources"]["source_ids"] == card["source_ids"]
        and card["decision_fact_provenance"]["relations"]["relation_ids"] == card["semantic_relation_ids"]
        for card in cards
    )
    checks = {
        "input_sha256_unchanged": input_sha_after == snapshot["input_sha256_before"],
        "input_pin_and_sqlite_preflight": all(snapshot["input_checks"].values()),
        "exact_16_direction_id_set": len(cards) == 16 and set(card_ids) == id_set_expected and len(set(card_ids)) == len(card_ids),
        "direction_id_set_sha256": _direction_ids_sha256(card_ids) == pins.direction_ids_sha256,
        "stage_d_fields_unchanged": stage_d_preserved,
        "stage_e_fields_unchanged": stage_e_preserved,
        "readiness_derived_only_from_research_state": exact_readiness,
        "expected_readiness_totals": readiness_counts == Counter({
            "DECISION_READY": sum(count for status, coverage, count in pins.expected_state_counts if status == "RESOLVED" and coverage == "SUFFICIENT"),
            "HOLD_AMBIGUOUS": sum(count for status, coverage, count in pins.expected_state_counts if status == "AMBIGUOUS" or coverage == "AMBIGUOUS"),
            "HOLD_UNRESOLVED": sum(count for status, coverage, count in pins.expected_state_counts if status == "UNRESOLVED"),
            "HOLD_PARTIAL": sum(count for status, coverage, count in pins.expected_state_counts if status == "RESOLVED" and coverage == "PARTIAL"),
        }),
        "no_hidden_direction_removal": len(cards) == len(snapshot["options"]),
        "competitor_relation_counts_reconcile": competitor_reconciles,
        "pricing_pain_feature_popularity_counts_reconcile": evidence_reconciles,
        "pricing_only_currency_and_unknown_semantics_reconcile": pricing_reconciles,
        "source_domain_query_and_source_type_facts_reconcile": source_query_facts_reconcile,
        "source_native_popularity_and_feature_themes_reconcile": source_native_and_themes_reconcile,
        "hypotheses_copied_exactly": all(card["differentiation_hypotheses"] == (input_packs[card["direction_id"]]["record"].get("differentiation_hypotheses") or []) for card in cards),
        "semantic_relations_copied_category_local": relation_copy,
        "decision_fact_provenance_reconciles": provenance_reconciles,
        "evidence_gap_codes_reconcile": gap_reconciles,
        "exact_11_category_context_order": len(categories) == 11 and category_order_ok,
        "category_context_counts_and_ids_reconcile": category_context_reconciles,
        "zero_option_categories_retained": all(row["option_ids"] or any("Zero accepted options" in note for note in row["descriptive_notes"]) for row in categories),
        "stable_category_then_direction_order": cards == sorted(cards, key=lambda row: (row["category_order"], row["direction_id"])),
        "no_rank_score_winner_or_concept_fields": no_forbidden,
        "worksheet_decisions_unset": worksheet_ok,
        "jsonl_csv_and_sqlite_reconcile": db_card_rows == cards and db_category_rows == categories and export_counts == {
            "direction_decision_cards_jsonl": len(cards), "direction_decision_cards_csv": len(cards),
            "category_decision_context_jsonl": len(categories), "category_decision_context_csv": len(categories),
        },
        "sqlite_integrity_check_ok": integrity == "ok",
        "sqlite_foreign_key_check_zero": not fk_errors,
        "sqlite_row_counts_reconcile": sqlite_counts == expected_tables,
        "sqlite_frozen_inputs_reconcile_exactly": frozen_reconciles,
        "sqlite_metadata_pins_and_output_hashes_present": db_metadata.get("execution_commit") is not None and db_metadata.get("input_sha256") == snapshot["input_sha256_before"] and all(db_metadata.get(f"output_sha256:{name}") for name in CORE_ARTIFACTS if name != "decision_gate.sqlite"),
        "goal_alignment_scope_gate": all(
            text in (output / "GOAL_ALIGNMENT.md").read_text(encoding="utf-8").casefold()
            for text in ("plugin_only", "historical yee-30 through yee-59", "no live research", "stage g", "16 options")
        ),
        "deterministic_complete_replay": replay_ok,
    }
    return checks, {
        "sqlite_integrity": integrity,
        "sqlite_foreign_key_violation_count": len(fk_errors),
        "sqlite_row_counts": sqlite_counts,
        "export_row_counts": export_counts,
        "readiness_counts": dict(sorted(readiness_counts.items())),
    }


def _render_final_report(
    execution_commit: str,
    snapshot: dict[str, Any],
    qa: dict[str, Any],
    pull_request_url: str | None,
    verification: dict[str, Any] | None,
) -> str:
    readiness = qa["readiness_counts"]
    lines = [
        "# YEE-79 Final Report — Category Direction Decision Gate v0",
        "",
        f"- Execution HEAD: `{execution_commit}`",
        f"- PR: {pull_request_url or 'pending / not supplied to deterministic builder'}",
        f"- Accepted YEE-77 SQLite SHA-256: `{snapshot['input_sha256_before']}`",
        f"- Accepted YEE-77 execution commit: `{YEE77_REVIEWED_HEAD}`",
        f"- Inherited YEE-76 SHA-256: `{YEE76_SQLITE_SHA256}`",
        f"- Direction identity SHA-256: `{_direction_ids_sha256([row['direction_id'] for row in snapshot['options']])}`",
        f"- QA: **{qa['status']}** ({qa['passing_checks']}/{qa['check_count']} checks; {len(qa['failed_checks'])} failed)",
        "",
        "## Output shape",
        "",
        f"- Exact frozen option identities retained: {len(snapshot['options'])}.",
        f"- Readiness: {readiness.get('DECISION_READY', 0)} DECISION_READY; {readiness.get('HOLD_AMBIGUOUS', 0)} HOLD_AMBIGUOUS; {readiness.get('HOLD_UNRESOLVED', 0)} HOLD_UNRESOLVED; {readiness.get('HOLD_PARTIAL', 0)} HOLD_PARTIAL.",
        f"- Frozen category context rows: {len(snapshot['categories'])}; zero-option categories remain present.",
        "- All decision worksheet rows remain UNSET.",
        "- No scoring, weighting, ranking, winner, recommendation, product concept, or Stage G work is produced.",
        "- The build performs no live research and treats the accepted YEE-77 SQLite as the sole analytical input.",
        "",
        "## Verification",
        "",
        f"- Input SHA before/after: `{snapshot['input_sha256_before']}` / `{snapshot['input_sha256_after_read']}`.",
        f"- SQLite integrity: `{qa['sqlite_integrity']}`; foreign-key violations: {qa['sqlite_foreign_key_violation_count']}.",
        f"- Deterministic full-artifact replay: `{qa['checks']['deterministic_complete_replay']}`.",
        "- Check details and exact row counts are in `QA_RESULT.json`; per-file sizes and SHA-256 are in `DATASET_MANIFEST.json`.",
    ]
    if verification:
        lines.extend(["", "## Repository verification", ""])
        lines.extend(f"- {key}: {value}" for key, value in sorted(verification.items()))
    lines.extend([
        "",
        "## Human decision boundary",
        "",
        "The dossier exposes descriptive evidence and tradeoffs only. Supervisor/User choices remain UNSET; this is not the Stage F decision and Stage G must not start until explicitly authorized.",
        "",
    ])
    return "\n".join(lines)


def _write_manifest(output: Path, snapshot: dict[str, Any], execution_commit: str) -> dict[str, Any]:
    artifacts = []
    for name in REQUIRED_ARTIFACTS:
        if name == "DATASET_MANIFEST.json":
            continue
        path = output / name
        artifacts.append({"file_name": name, "size_bytes": path.stat().st_size, "sha256": _sha256_file(path)})
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "execution_commit": execution_commit,
        "input": {
            "work_order": "YEE-77",
            "file_name": "category_targeted_external_research.sqlite",
            "sha256": snapshot["input_sha256_before"],
            "read_only": True,
        },
        "direction_ids_sha256": _direction_ids_sha256([row["direction_id"] for row in snapshot["options"]]),
        "artifacts": artifacts,
        "manifest_self_hash_note": "Manifest does not hash itself; exact manifest bytes can be independently SHA-256 hashed after creation.",
    }
    (output / "DATASET_MANIFEST.json").write_text(canonical_json(manifest) + "\n", encoding="utf-8", newline="\n")
    return manifest


def _build_once(
    input_path: Path,
    output: Path,
    execution_commit: str,
    pull_request_url: str | None,
    verification: dict[str, Any] | None,
    pins: InputPins,
    replay_verified: bool,
) -> dict[str, Any]:
    if output.exists() and any(output.iterdir()):
        raise ValueError("YEE-79 output directory must be empty before a production build")
    output.mkdir(parents=True, exist_ok=True)
    snapshot = _load_input(input_path, pins)
    goal_alignment = _render_goal_alignment(snapshot).casefold()
    required_alignment = ("plugin_only", "historical yee-30 through yee-59", "no live research", "stage g", "supervisor/user")
    if not all(text in goal_alignment for text in required_alignment):
        raise InputMismatch("BLOCKED_SCOPE_MISMATCH: GOAL_ALIGNMENT contract failed")
    cards, categories = _build_dataset(snapshot)
    _write_core_docs(output, snapshot, cards, categories, execution_commit)
    _write_jsonl(output / "direction_decision_cards.jsonl", cards)
    _write_csv(output / "direction_decision_cards.csv", cards)
    _write_jsonl(output / "category_decision_context.jsonl", categories)
    _write_csv(output / "category_decision_context.csv", categories)
    core_hashes = {name: _sha256_file(output / name) for name in CORE_ARTIFACTS if name != "decision_gate.sqlite"}
    _write_sqlite(output / "decision_gate.sqlite", snapshot, cards, categories, core_hashes, execution_commit)
    input_sha_after = _sha256_file(input_path)
    checks, detail = _qa_checks(snapshot, cards, categories, output, input_sha_after, replay_ok=replay_verified, pins=pins)
    qa = {
        "work_order": "YEE-79",
        "schema_version": SCHEMA_VERSION,
        "execution_commit": execution_commit,
        "status": "PASS" if all(checks.values()) else "FAIL",
        "input": {
            "file_name": "category_targeted_external_research.sqlite",
            "sha256_before": snapshot["input_sha256_before"],
            "sha256_after": input_sha_after,
            "read_only": True,
            "schema_version": snapshot["metadata"].get("schema_version"),
            "execution_commit": snapshot["metadata"].get("execution_code_commit"),
            "inherited_yee76_sha256": snapshot["metadata"].get("input_sha256_before"),
            "inherited_yee76_run_id": snapshot["metadata"].get("input_run_id"),
            "inherited_taxonomy_sha256": snapshot["metadata"].get("input_taxonomy_sha256"),
            "direction_ids_sha256": _direction_ids_sha256([row["direction_id"] for row in snapshot["options"]]),
        },
        "input_table_counts": snapshot["table_counts"],
        "output_row_counts": {
            "direction_decision_cards": len(cards),
            "category_decision_context": len(categories),
            **detail["export_row_counts"],
            **detail["sqlite_row_counts"],
        },
        "readiness_counts": detail["readiness_counts"],
        "sqlite_integrity": detail["sqlite_integrity"],
        "sqlite_foreign_key_violation_count": detail["sqlite_foreign_key_violation_count"],
        "checks": checks,
        "check_count": len(checks),
        "passing_checks": sum(checks.values()),
        "failed_checks": sorted(name for name, value in checks.items() if not value),
        "output_sha256": {name: _sha256_file(output / name) for name in CORE_ARTIFACTS},
        "verification": verification or {},
        "deterministic_replay": {"byte_identical": replay_verified, "artifact_count": len(REQUIRED_ARTIFACTS)},
    }
    (output / "QA_RESULT.json").write_text(canonical_json(qa) + "\n", encoding="utf-8", newline="\n")
    (output / "FINAL_REPORT.md").write_text(
        _render_final_report(execution_commit, snapshot, qa, pull_request_url, verification),
        encoding="utf-8", newline="\n",
    )
    _write_manifest(output, snapshot, execution_commit)
    return {"snapshot": snapshot, "cards": cards, "categories": categories, "qa": qa}


def build_bundle(
    input_db: Path | str,
    output_dir: Path | str,
    *,
    code_commit: str | None = None,
    pull_request_url: str | None = None,
    verification: dict[str, Any] | None = None,
    pins: InputPins = CANONICAL_INPUT_PINS,
) -> dict[str, Any]:
    input_path = Path(input_db).resolve()
    output = Path(output_dir).resolve()
    if not input_path.is_file():
        raise InputMismatch(f"Accepted YEE-77 SQLite does not exist: {input_path}")
    execution_commit = code_commit or _execution_commit()
    first = _build_once(input_path, output, execution_commit, pull_request_url, verification, pins, replay_verified=True)
    with tempfile.TemporaryDirectory(prefix="yee79-replay-") as replay_name:
        replay_dir = Path(replay_name) / "bundle"
        _build_once(input_path, replay_dir, execution_commit, pull_request_url, verification, pins, replay_verified=True)
        replay_ok = all((output / name).read_bytes() == (replay_dir / name).read_bytes() for name in REQUIRED_ARTIFACTS)
    qa_path = output / "QA_RESULT.json"
    qa = json.loads(qa_path.read_text(encoding="utf-8"))
    # The claim in QA is only handed off after all final output bytes have reconciled.
    if not replay_ok:
        qa["checks"]["deterministic_complete_replay"] = False
        qa["status"] = "FAIL"
        qa["passing_checks"] = sum(qa["checks"].values())
        qa["failed_checks"] = sorted(name for name, value in qa["checks"].items() if not value)
        qa_path.write_text(canonical_json(qa) + "\n", encoding="utf-8", newline="\n")
        (output / "FINAL_REPORT.md").write_text(
            _render_final_report(execution_commit, first["snapshot"], qa, pull_request_url, verification),
            encoding="utf-8", newline="\n",
        )
        _write_manifest(output, first["snapshot"], execution_commit)
    manifest = json.loads((output / "DATASET_MANIFEST.json").read_text(encoding="utf-8"))
    if qa["failed_checks"]:
        raise InputMismatch(f"YEE-79 production QA failed: {qa['failed_checks']}")
    return {"overall_status": qa["status"], "failed_checks": qa["failed_checks"], "qa": qa, "manifest": manifest}
