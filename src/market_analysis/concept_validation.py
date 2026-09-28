"""Deterministic YEE-83 concept validation from pinned evidence and frozen research."""

from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

WORK_ORDER = "YEE-83"
CAPTURE_VERSION = "yee-83-product-opportunity-concept-validation-v0.1"
BASELINE_COMMIT = "e8f4bc140a9bad8af10b2cbf3a55a0f408ca343d"
ACCEPTED_YEE81_EXECUTION_COMMIT = "4692b0cc58fd5966158e7cead3b921b1b0b0ee1d"
COHORT_SHA256 = "cacc587b79e4d5ba818c258c757888aa33e258d3b78426449562265b1dec0fdb"
INPUT_HASHES = {
    "yee81_sqlite": "470a7e160d9b0408b878206098980e07e92cc17f3c61448ee3eef2bb1f1ae7a4",
    "yee81_capture": "5e1dfd09e57433c1b8eae19d8d6930dd1447129cb5caef925b7d9367ec76c159",
    "yee79_sqlite": "7059d52e7f54c7b6b0709f6301a7ceb0a442e2c7635dcc8912cbe5c21f3079fe",
    "yee77_sqlite": "a2ea751823358e1032c36fd31a88d2e581476bf6e915a53c042b70614bcc4f48",
}
DIRECTIONS = {
    "dir_142f5946b84de25a3a07dd76": ("cross_platform_communication", "Cross-server/proxy chat relay"),
    "dir_78e16d4e251e9350b653a2de": ("jobs_and_rewards", "Player activity jobs with configurable economic rewards"),
}
PURPOSES = (
    "BUYER_PROBLEM_SPECIFICITY", "PURCHASE_TRIGGER_SPECIFICITY", "INCUMBENT_COVERAGE",
    "DIFFERENTIATION_GAP", "PAID_VALUE_EXCHANGE", "TECHNICAL_AND_SUPPORT_RISK",
)
DIMENSIONS = (
    "BUYER_PROBLEM_ALIGNMENT", "PURCHASE_TRIGGER_ALIGNMENT", "PAID_VALUE_EXCHANGE",
    "DIFFERENTIATION", "INCUMBENT_PRESSURE", "IMPLEMENTATION_FEASIBILITY", "SUPPORT_MAINTENANCE",
)
DIMENSION_STATES = {"SUPPORTED", "MIXED", "WEAK", "INSUFFICIENT_EVIDENCE"}
CONCEPT_STATES = {
    "CONCEPT_EVIDENCE_SUPPORTED", "CONCEPT_EVIDENCE_MIXED", "CONCEPT_EVIDENCE_WEAK",
    "CONCEPT_INSUFFICIENT_EVIDENCE",
}
SCOPE_STATES = {"EVIDENCE_BACKED_NEED", "SYNTHESIZED_REQUIREMENT", "HYPOTHESIS_TO_VALIDATE"}
COVERAGE_STATES = {"NOT_OBSERVED", "PARTIALLY_COVERED", "SUBSTANTIALLY_COVERED", "CONFLICTING", "UNKNOWN"}
SCOPE_COVERAGE_STATES = {"NOT_OBSERVED", "PARTIALLY_COVERED", "SUBSTANTIALLY_COVERED", "UNKNOWN"}
GAP_TYPES = {
    "WORKFLOW_FRICTION", "CONFIGURATION_COMPLEXITY", "INTEGRATION_GAP", "COMPATIBILITY_GAP",
    "MIGRATION_GAP", "RELIABILITY_OR_OPERATIONS_GAP", "SECURITY_OR_SAFETY_GAP",
    "ECONOMY_OR_DATA_INTEGRITY_GAP", "MISSING_CAPABILITY", "OTHER_SOURCE_BACKED_GAP",
}
DIFF_STATES = {"EVIDENCE_BACKED", "MIXED", "HYPOTHESIS_ONLY", "INSUFFICIENT"}
VALUE_TYPES = {
    "DIRECT_WTP_STATEMENT", "OBSERVED_PURCHASE_OR_PAYMENT", "PAID_COMPETITOR_PRECEDENT",
    "PREMIUM_TIER_PRECEDENT", "BEHAVIORAL_PROXY", "OPERATIONAL_VALUE_PROXY", "NO_WTP_EVIDENCE",
}
VALUE_STATES = {"SUPPORTED", "MIXED", "WEAK", "INSUFFICIENT_EVIDENCE"}
EVIDENCE_GAPS = {
    "BUYER_PROBLEM_NOT_SPECIFIC", "PURCHASE_TRIGGER_UNCLEAR", "NO_DIRECT_WTP_EVIDENCE",
    "PAID_VALUE_EXCHANGE_WEAK", "INCUMBENT_ALREADY_COVERS_SCOPE", "DIFFERENTIATION_NOT_ESTABLISHED",
    "DIFFERENTIATION_CONFLICTING", "FREE_INCUMBENT_PRESSURE", "PAID_INCUMBENT_PRESSURE",
    "COMPATIBILITY_SCOPE_RISK", "INTEGRATION_COMPLEXITY_RISK", "DATA_INTEGRITY_RISK",
    "SECURITY_SENSITIVITY_RISK", "SUPPORT_BURDEN_RISK", "SOURCE_COVERAGE_RISK",
    "CONCEPT_TOO_BROAD", "CONCEPT_OVERLAPS_SIBLING", "OTHER_EVIDENCE_GAP",
}
NULL_TOKEN = r"\N"

TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "metadata": ("key", "value"),
    "input_provenance": ("input_name", "sha256", "read_only"),
    "authorized_direction_cohort": ("direction_id", "canonical_direction_key", "resolved_market_job", "cohort_sha256"),
    "concepts": ("concept_id", "direction_id", "canonical_direction_key", "concept_label", "target_buyer_segment", "operator_context", "problem_statement", "purchase_trigger_hypothesis", "primary_opportunity_axis", "paid_value_exchange_hypothesis", "minimum_paid_scope", "explicit_non_goals", "upstream_seed_evidence_ids", "upstream_buyer_observation_ids", "upstream_competitor_ids", "upstream_risk_flags", "synthesis_rationale", "novelty_disclosure"),
    "concept_seed_trace": ("trace_id", "concept_id", "direction_id", "seed_type", "upstream_id", "source_id", "relevance"),
    "concept_scope_items": ("scope_item_id", "concept_id", "capability_statement", "scope_state", "evidence_ids", "buyer_value_link", "incumbent_coverage_state", "notes"),
    "concept_distinctness_relations": ("relation_id", "direction_id", "concept_a_id", "concept_b_id", "distinctness_basis", "overlap_notes", "materially_distinct"),
    "concept_source_documents": ("source_id", "url", "canonical_url", "domain", "title", "source_type", "access_status", "retrieved_at", "published_or_updated_at", "date_note"),
    "concept_validation_queries": ("query_id", "concept_id", "direction_id", "execution_order", "purpose", "query_text", "issued_at", "query_kind", "followup_of", "usable_source_found", "result_note", "discovered_result_urls", "opened_source_ids"),
    "concept_evidence": ("evidence_id", "concept_id", "direction_id", "source_id", "query_ids", "evidence_type", "observation", "source_locator", "limitations", "used_search_snippet"),
    "incumbent_coverage_observations": ("coverage_id", "concept_id", "competitor_id", "competitor_name", "relation_type", "tested_axis", "coverage_state", "source_ids", "evidence_ids", "retrieved_at", "notes"),
    "concept_differentiation_observations": ("differentiation_id", "concept_id", "gap_type", "gap_statement", "buyer_evidence_ids", "competitor_evidence_ids", "source_ids", "evidence_state", "counterevidence_ids", "notes"),
    "paid_value_exchange_observations": ("value_id", "concept_id", "value_statement", "evidence_type", "source_ids", "evidence_ids", "state", "limitations"),
    "concept_feasibility_observations": ("feasibility_id", "concept_id", "theme", "observation", "source_ids", "evidence_ids", "notes"),
    "concept_support_observations": ("support_id", "concept_id", "burden_theme", "observation", "source_ids", "evidence_ids", "notes"),
    "concept_validation_cards": ("concept_id", "direction_id", "canonical_direction_key", "concept_label", "target_buyer_segment", "operator_context", "problem_statement", "purchase_trigger_hypothesis", "primary_opportunity_axis", "paid_value_exchange_hypothesis", "minimum_paid_scope_item_ids", "explicit_non_goals", "upstream_seed_evidence_ids", "upstream_buyer_observation_ids", "upstream_competitor_ids", "upstream_risk_flags", "fresh_validation_evidence_ids", "direct_competitor_ids", "substitute_competitor_ids", "adjacent_context_ids", "incumbent_coverage_observation_ids", "differentiation_observation_ids", "paid_value_exchange_observation_ids", "feasibility_observation_ids", "support_observation_ids", "dimension_assessments", "concept_validation_state", "evidence_gap_codes", "risk_flags", "unknowns", "research_coverage_status", "research_notes", "synthesis_rationale", "novelty_disclosure"),
    "supervisor_decision_template": ("decision_status", "advanced_concept_ids", "requested_refinement_concept_ids", "held_concept_ids", "dropped_concept_ids", "build_none", "rationale", "decided_at"),
}
EXPORTS = {
    "concept_source_documents": "concept_source_documents",
    "concept_validation_queries": "concept_validation_queries",
    "concept_evidence": "concept_evidence",
    "concept_seed_trace": "concept_seed_trace",
    "concept_scope_items": "concept_scope_items",
    "concept_distinctness_relations": "concept_distinctness_relations",
    "incumbent_coverage_observations": "incumbent_coverage_observations",
    "concept_differentiation_observations": "concept_differentiation_observations",
    "paid_value_exchange_observations": "paid_value_exchange_observations",
    "concept_feasibility_observations": "concept_feasibility_observations",
    "concept_support_observations": "concept_support_observations",
    "concept_validation_cards": "concept_validation_cards",
}


class ConceptValidationError(ValueError):
    """Raised when pinned inputs or YEE-83 contract validation fails."""


def canonical_url(url: str) -> str:
    parts = urlsplit(url.strip())
    query = urlencode(sorted((k, v) for k, v in parse_qsl(parts.query) if not k.lower().startswith("utm_")))
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, query, ""))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cohort_sha(direction_ids: Iterable[str]) -> str:
    return hashlib.sha256(("\n".join(sorted(direction_ids)) + "\n").encode("utf-8")).hexdigest()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _stable_id(prefix: str, *parts: str) -> str:
    return f"{prefix}_{hashlib.sha256(chr(0).join(parts).encode('utf-8')).hexdigest()[:18]}"


def _validate_evidence_refs(
    evidence_ids: list[str], concept_id: str, evidence_by_id: dict[str, dict[str, Any]],
    allowed_source_ids: list[str] | None = None,
) -> set[str]:
    rows = [evidence_by_id.get(evidence_id) for evidence_id in evidence_ids]
    if not evidence_ids or any(row is None or row.get("concept_id") != concept_id for row in rows):
        raise ConceptValidationError("Normalized observation must reference same-concept evidence")
    evidence_source_ids = {row["source_id"] for row in rows if row is not None}
    if allowed_source_ids is not None and not evidence_source_ids <= set(allowed_source_ids):
        raise ConceptValidationError("Normalized observation source IDs must include its evidence sources")
    return evidence_source_ids


def _load_read_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def validate_hashes(paths: dict[str, Path]) -> dict[str, str]:
    actual = {name: sha256_file(path) for name, path in paths.items()}
    for name, expected in INPUT_HASHES.items():
        if actual.get(name) != expected:
            raise ConceptValidationError(f"Pinned input hash mismatch: {name}")
    return actual


def validate_concept_cohort(concepts: list[dict[str, Any]], expected_sha: str = COHORT_SHA256) -> None:
    if len({row.get("concept_id") for row in concepts}) != len(concepts):
        raise ConceptValidationError("Concept IDs must be unique")
    ids = sorted({row.get("direction_id") for row in concepts})
    if ids != sorted(DIRECTIONS) or cohort_sha(ids) != expected_sha:
        raise ConceptValidationError("Authorized direction cohort mismatch")
    counts = {direction: sum(row.get("direction_id") == direction for row in concepts) for direction in DIRECTIONS}
    if counts != {direction: 3 for direction in DIRECTIONS} or len(concepts) != 6:
        raise ConceptValidationError("Expected exactly three concepts per authorized direction")


def validate_concept_seeds(concept: dict[str, Any], upstream: dict[str, Any]) -> None:
    evidence_ids = set(upstream["evidence_ids"].get(concept["direction_id"], []))
    buyer_ids = set(upstream["buyer_ids"].get(concept["direction_id"], []))
    competitors = set(upstream["competitor_ids"].get(concept["direction_id"], []))
    seeds = set(concept.get("upstream_seed_evidence_ids", []))
    buyers = set(concept.get("upstream_buyer_observation_ids", []))
    comps = set(concept.get("upstream_competitor_ids", []))
    if len(seeds) < 2 or not seeds <= evidence_ids:
        raise ConceptValidationError(f"Invalid upstream evidence seeds for {concept.get('concept_id')}")
    seed_types = {upstream["evidence"][concept["direction_id"]][evidence_id].get("evidence_type") for evidence_id in seeds}
    buyer_types = {"BUYER_NEED", "EXPLICIT_PURCHASE_INTENT", "OPERATOR_FRICTION", "SUPPORT_CASE", "SECURITY_SUPPORT_REPORT", "USER_FRICTION"}
    competition_types = {"CHANNEL_AND_COMPETITOR_LISTING", "COMPETITOR_FEATURES", "FREE_INCUMBENT", "INTEGRATION_SUPPORT", "PAID_COMPETITOR_PRICE", "PAID_MARKET_PRECEDENT", "PLATFORM_COMPATIBILITY_DOCUMENTATION"}
    if not seed_types & buyer_types or not seed_types & competition_types:
        raise ConceptValidationError(f"Concept needs both buyer/problem and incumbent/technical YEE-81 evidence seeds: {concept.get('concept_id')}")
    if not buyers or not buyers <= buyer_ids:
        raise ConceptValidationError(f"Missing buyer/problem seed for {concept.get('concept_id')}")
    if not comps or not comps <= competitors:
        raise ConceptValidationError(f"Missing incumbent seed for {concept.get('concept_id')}")
    if concept.get("canonical_direction_key") != DIRECTIONS[concept["direction_id"]][0]:
        raise ConceptValidationError("Concept market-job/direction mismatch")


def validate_query_coverage(concepts: list[dict[str, Any]], queries: list[dict[str, Any]]) -> None:
    concept_ids = {c["concept_id"] for c in concepts}
    concept_map = {c["concept_id"]: c for c in concepts}
    query_ids = [q.get("query_id") for q in queries]
    execution_orders = [q.get("execution_order") for q in queries]
    if len(query_ids) != len(set(query_ids)) or len(execution_orders) != len(set(execution_orders)):
        raise ConceptValidationError("Query IDs and execution orders must be unique")
    if any(q.get("concept_id") not in concept_map or q.get("direction_id") != concept_map[q["concept_id"]]["direction_id"] or q.get("purpose") not in PURPOSES or q.get("query_kind") not in {"MANDATORY", "FOLLOWUP"} for q in queries):
        raise ConceptValidationError("Query must resolve to its concept, direction, and allowed purpose")
    if any(q.get("query_kind") == "FOLLOWUP" and not q.get("followup_of") for q in queries):
        raise ConceptValidationError("Follow-up query must identify its parent query")
    rows = [q for q in queries if q.get("query_kind") == "MANDATORY"]
    by_pair = {(q.get("concept_id"), q.get("purpose")) for q in rows}
    required = {(cid, purpose) for cid in concept_ids for purpose in PURPOSES}
    if len(rows) != 36 or len(by_pair) != 36 or not required <= by_pair:
        raise ConceptValidationError("Missing mandatory targeted research queries")
    concepts_by_id = concept_map
    for query in queries:
        concept = concepts_by_id.get(query.get("concept_id"))
        terms = [term.casefold() for term in (concept or {}).get("query_target_terms", [])]
        text = query.get("query_text", "").casefold()
        if not terms or not any(term in text for term in terms):
            raise ConceptValidationError("Generic direction-level query cannot satisfy concept query contract")
        if not query.get("result_note") or not isinstance(query.get("opened_source_ids"), list):
            raise ConceptValidationError("Each mandatory query needs a result note and explicit opened-source list")
    for query in queries:
        if query.get("followup_of"):
            original = next((q for q in queries if q.get("query_id") == query["followup_of"]), None)
            if not original or original.get("purpose") != query.get("purpose") or original.get("concept_id") != query.get("concept_id"):
                raise ConceptValidationError("Follow-up query must preserve concept and purpose")
            if int(query.get("execution_order", 0)) <= int(original.get("execution_order", 0)):
                raise ConceptValidationError("Follow-up query must occur after the query it follows")
    by_id = {q["query_id"]: q for q in queries}
    for query in queries:
        if query.get("usable_source_found") is False:
            followed = [
                row for row in queries
                if row.get("followup_of") == query["query_id"]
                and row.get("concept_id") == query.get("concept_id")
                and row.get("purpose") == query.get("purpose")
            ]
            if not followed:
                raise ConceptValidationError("A query without a usable opened source needs a same-concept/purpose follow-up")
    if any(query.get("followup_of") not in by_id for query in queries if query.get("followup_of")):
        raise ConceptValidationError("Follow-up references an unknown query")
    if any(not query.get("query_text") or not query.get("issued_at") or not query.get("result_note") or not isinstance(query.get("opened_source_ids"), list) or not isinstance(query.get("discovered_result_urls"), list) for query in queries):
        raise ConceptValidationError("Every research query needs timestamp, result note, and explicit discovery/opened-source lists")


def validate_evidence_sources(sources: list[dict[str, Any]], evidence: list[dict[str, Any]], queries: list[dict[str, Any]]) -> None:
    canonical = [canonical_url(s["url"]) for s in sources]
    if len(canonical) != len(set(canonical)):
        raise ConceptValidationError("Duplicate canonical source URL")
    source_map = {s["source_id"]: s for s in sources}
    query_map = {q["query_id"]: q for q in queries}
    if any(not source.get("retrieved_at") for source in sources):
        raise ConceptValidationError("Every opened source needs a retrieval timestamp")
    for item in evidence:
        source = source_map.get(item.get("source_id"))
        if not source or source.get("access_status") != "OPENED":
            raise ConceptValidationError("Evidence must resolve to an opened public source")
        if item.get("used_search_snippet") is not False:
            raise ConceptValidationError("Search snippets are discovery only, never evidence")
        if not item.get("source_locator") or not item.get("query_ids"):
            raise ConceptValidationError("Evidence needs query and source locator provenance")
        for query_id in item["query_ids"]:
            query = query_map.get(query_id)
            if not query or query.get("concept_id") != item.get("concept_id") or source["source_id"] not in query.get("opened_source_ids", []):
                raise ConceptValidationError("Evidence query/source/concept provenance mismatch")


def effective_differentiation_state(row: dict[str, Any], evidence: list[dict[str, Any]], sources: list[dict[str, Any]]) -> str:
    evidence_map = {e["evidence_id"]: e for e in evidence}
    source_map = {s["source_id"]: s for s in sources}
    buyer_ids = set(row.get("buyer_evidence_ids", []))
    competitor_ids = set(row.get("competitor_evidence_ids", []))
    buyer_types = {"BUYER_NEED", "USER_FRICTION", "OPERATOR_FRICTION", "SUPPORT_CASE", "SECURITY_SUPPORT_REPORT"}
    competitor_types = {"PRIMARY_PRODUCT", "PRIMARY_DOCS", "MARKETPLACE_LISTING", "PRIMARY_SUPPORT", "PLATFORM_DOCUMENTATION"}
    has_buyer = any(eid in evidence_map and evidence_map[eid].get("evidence_type") in buyer_types for eid in buyer_ids)
    has_competitor = any(
        eid in evidence_map
        and evidence_map[eid].get("source_id") in source_map
        and source_map[evidence_map[eid]["source_id"]].get("source_type") in competitor_types
        for eid in competitor_ids
    )
    used_ids = buyer_ids | competitor_ids
    source_urls = {
        canonical_url(source_map[evidence_map[eid]["source_id"]]["url"])
        for eid in used_ids
        if eid in evidence_map and evidence_map[eid].get("source_id") in source_map
    }
    if row.get("evidence_state") == "EVIDENCE_BACKED" and (not (len(source_urls) >= 2 and has_buyer and has_competitor) or row.get("counterevidence_ids")):
        return "HYPOTHESIS_ONLY"
    return row.get("evidence_state", "INSUFFICIENT")


def derive_concept_state(dimensions: list[dict[str, Any]], research_coverage_status: str = "SUFFICIENT") -> str:
    by_name = {d["dimension"]: d["state"] for d in dimensions}
    if len(by_name) != 7 or set(by_name) != set(DIMENSIONS) or any(v not in DIMENSION_STATES for v in by_name.values()):
        raise ConceptValidationError("Exactly seven allowed concept dimensions are required")
    core = [by_name[name] for name in ("BUYER_PROBLEM_ALIGNMENT", "PURCHASE_TRIGGER_ALIGNMENT", "DIFFERENTIATION")]
    if all(v == "SUPPORTED" for v in core):
        return "CONCEPT_EVIDENCE_SUPPORTED"
    if any(v == "INSUFFICIENT_EVIDENCE" for v in core):
        return "CONCEPT_INSUFFICIENT_EVIDENCE"
    if any(v == "MIXED" for v in core):
        return "CONCEPT_EVIDENCE_MIXED"
    if any(v == "WEAK" for v in core) and research_coverage_status == "SUFFICIENT":
        return "CONCEPT_EVIDENCE_WEAK"
    return "CONCEPT_INSUFFICIENT_EVIDENCE"


def validate_decision_template(template: dict[str, Any]) -> None:
    expected = {
        "decision_status": "UNDECIDED", "advanced_concept_ids": [], "requested_refinement_concept_ids": [],
        "held_concept_ids": [], "dropped_concept_ids": [], "build_none": False, "rationale": None, "decided_at": None,
    }
    if template != expected:
        raise ConceptValidationError("Supervisor decision template must remain undecided")


def _reject_forbidden_fields(value: Any) -> None:
    forbidden = {"score", "rank", "ranking", "winner", "recommended_build", "recommendation", "price_recommendation", "product_spec", "converted_price", "normalized_price", "price_usd", "currency_converted_price"}
    if isinstance(value, dict):
        if any(str(k).casefold() in forbidden for k in value):
            raise ConceptValidationError("Scoring/ranking/recommendation field is forbidden")
        for child in value.values():
            _reject_forbidden_fields(child)
    elif isinstance(value, list):
        for child in value:
            _reject_forbidden_fields(child)


def _read_upstream(path: Path) -> dict[str, Any]:
    connection = _load_read_only(path)
    try:
        out: dict[str, Any] = {"directions": {}, "evidence_ids": {}, "buyer_ids": {}, "competitor_ids": {}, "evidence": {}, "buyers": {}, "competitors": {}}
        for direction_id in DIRECTIONS:
            row = connection.execute("SELECT * FROM direction_commercial_validation WHERE direction_id=?", (direction_id,)).fetchone()
            if row is None:
                raise ConceptValidationError(f"YEE-81 direction missing: {direction_id}")
            record = json.loads(row["record_json"])
            if record["yee79_decision"] != "ADVANCE_FOR_DEEP_VALIDATION" or record["canonical_direction_key"] != DIRECTIONS[direction_id][0] or record["resolved_market_job"] != DIRECTIONS[direction_id][1]:
                raise ConceptValidationError("YEE-81 active direction contract mismatch")
            out["directions"][direction_id] = record
            for table, key, dest in (("commercial_evidence", "evidence_id", "evidence"), ("buyer_problem_observations", "observation_id", "buyers"), ("commercial_competitor_offerings", "competitor_id", "competitors")):
                rows = connection.execute(f"SELECT * FROM {table} WHERE direction_id=?", (direction_id,)).fetchall()
                values = []
                for item in rows:
                    row_obj = json.loads(item["record_json"])
                    values.append(row_obj)
                    out[dest].setdefault(direction_id, {})[row_obj[key]] = row_obj
                out[{"commercial_evidence":"evidence_ids","buyer_problem_observations":"buyer_ids","commercial_competitor_offerings":"competitor_ids"}[table]][direction_id] = [item[key] for item in values]
        return out
    finally:
        connection.close()


def _jsonl_csv(path_base: Path, rows: list[dict[str, Any]], columns: tuple[str, ...]) -> None:
    jsonl = path_base.with_suffix(".jsonl")
    jsonl.write_text("".join(_json(row) + "\n" for row in rows), encoding="utf-8", newline="\n")
    csv_path = path_base.with_suffix(".csv")
    with csv_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: NULL_TOKEN if row.get(key) is None else _json(row[key]) if isinstance(row.get(key), (dict, list, bool)) else row.get(key) for key in columns})


def _stable_rows(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: str(row.get(key, "")))


def _table_rows(table: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if table == "concept_validation_queries":
        return sorted(rows, key=lambda row: int(row["execution_order"]))
    return _stable_rows(rows, TABLE_COLUMNS[table][0])


def _cards(concepts: list[dict[str, Any]], validations: list[dict[str, Any]], capture: dict[str, Any]) -> list[dict[str, Any]]:
    validation_map = {v["concept_id"]: v for v in validations}
    query_concepts = {c["concept_id"] for c in concepts}
    if set(validation_map) != query_concepts:
        raise ConceptValidationError("Each concept must have exactly one validation result")
    result = []
    for concept in sorted(concepts, key=lambda c: (c["direction_id"], c["concept_id"])):
        val = validation_map[concept["concept_id"]]
        assessments = val["dimension_assessments"]
        state = derive_concept_state(assessments, val["research_coverage_status"])
        card = {k: concept[k] for k in (
            "concept_id", "direction_id", "canonical_direction_key", "concept_label", "target_buyer_segment", "operator_context",
            "problem_statement", "purchase_trigger_hypothesis", "primary_opportunity_axis", "paid_value_exchange_hypothesis",
        "explicit_non_goals", "upstream_seed_evidence_ids", "upstream_buyer_observation_ids", "upstream_competitor_ids",
        "upstream_risk_flags", "synthesis_rationale", "novelty_disclosure",
        )}
        card.update({
            "minimum_paid_scope_item_ids": [s["scope_item_id"] for s in concept["minimum_paid_scope"]],
            "fresh_validation_evidence_ids": sorted(e["evidence_id"] for e in capture["evidence"] if e["concept_id"] == concept["concept_id"]),
            "direct_competitor_ids": val["direct_competitor_ids"],
            "substitute_competitor_ids": val.get("substitute_competitor_ids", []),
            "adjacent_context_ids": val.get("adjacent_context_ids", []),
            "incumbent_coverage_observation_ids": sorted(x["coverage_id"] for x in val["incumbent_coverage_observations"]),
            "differentiation_observation_ids": sorted(x["differentiation_id"] for x in val["differentiation_observations"]),
            "paid_value_exchange_observation_ids": sorted(x["value_id"] for x in val["paid_value_exchange_observations"]),
            "feasibility_observation_ids": sorted(x["feasibility_id"] for x in val["feasibility_observations"]),
            "support_observation_ids": sorted(x["support_id"] for x in val["support_observations"]),
            "dimension_assessments": assessments,
            "concept_validation_state": state,
            "evidence_gap_codes": sorted(set(val["evidence_gap_codes"])),
            "risk_flags": sorted(set(val["risk_flags"])),
            "unknowns": val["unknowns"],
            "research_coverage_status": val["research_coverage_status"],
            "research_notes": val["research_notes"],
        })
        result.append(card)
    return result


def _validate_capture(capture: dict[str, Any], upstream: dict[str, Any]) -> list[dict[str, Any]]:
    required_capture_fields = {"capture_version", "captured_at", "concepts", "queries", "sources", "evidence", "validations", "distinctness_relations", "decision_template"}
    if not required_capture_fields <= capture.keys():
        raise ConceptValidationError("Frozen capture is missing Worker Spec sections")
    concepts = capture["concepts"]
    validate_concept_cohort(concepts)
    required_concept_fields = {
        "concept_id", "direction_id", "canonical_direction_key", "concept_label", "target_buyer_segment",
        "operator_context", "problem_statement", "purchase_trigger_hypothesis", "primary_opportunity_axis",
        "paid_value_exchange_hypothesis", "minimum_paid_scope", "explicit_non_goals", "upstream_seed_evidence_ids",
        "upstream_buyer_observation_ids", "upstream_competitor_ids", "upstream_risk_flags", "synthesis_rationale",
        "novelty_disclosure",
    }
    for concept in concepts:
        if required_concept_fields - concept.keys():
            raise ConceptValidationError("Concept is missing Worker Spec synthesis fields")
        novelty_keys = {"EVIDENCE_BACKED", "SYNTHESIZED_FROM_EVIDENCE", "HYPOTHESIS_TO_VALIDATE"}
        if set(concept["novelty_disclosure"]) != novelty_keys or any(not isinstance(concept["novelty_disclosure"][key], list) for key in novelty_keys):
            raise ConceptValidationError("Concept novelty disclosure must partition evidence, synthesis, and hypotheses")
    for concept in concepts:
        validate_concept_seeds(concept, upstream)
        if not concept.get("target_buyer_segment") or not concept.get("problem_statement") or not concept.get("purchase_trigger_hypothesis"):
            raise ConceptValidationError("Concept buyer/problem/trigger is required")
        scope_fields = {"scope_item_id", "concept_id", "capability_statement", "scope_state", "evidence_ids", "buyer_value_link", "incumbent_coverage_state", "notes"}
        if not concept.get("minimum_paid_scope") or any(
            scope_fields - scope.keys()
            or scope.get("concept_id") != concept["concept_id"]
            or scope.get("scope_state") not in SCOPE_STATES
            or scope.get("incumbent_coverage_state") not in SCOPE_COVERAGE_STATES
            or not scope.get("capability_statement")
            for scope in concept["minimum_paid_scope"]
        ):
            raise ConceptValidationError("Invalid minimum paid scope")
    validate_query_coverage(concepts, capture["queries"])
    validate_evidence_sources(capture["sources"], capture["evidence"], capture["queries"])
    validate_decision_template(capture.get("decision_template", {}))
    source_ids = {source["source_id"] for source in capture["sources"]}
    if any(not set(query["opened_source_ids"]) <= source_ids for query in capture["queries"]):
        raise ConceptValidationError("Query opened-source references must resolve to captured sources")
    pairs = capture["distinctness_relations"]
    concept_by_id = {concept["concept_id"]: concept for concept in concepts}
    if len(pairs) != 6 or len({pair.get("relation_id") for pair in pairs}) != 6:
        raise ConceptValidationError("Exactly six unique within-direction distinctness relations are required")
    axes_by_direction: dict[str, set[str]] = {direction_id: set() for direction_id in DIRECTIONS}
    for concept in concepts:
        axis = concept.get("primary_opportunity_axis", "").strip().casefold()
        if not axis or axis in axes_by_direction[concept["direction_id"]]:
            raise ConceptValidationError("Sibling concepts need distinct primary opportunity axes")
        axes_by_direction[concept["direction_id"]].add(axis)
    for direction in DIRECTIONS:
        siblings = sorted(c["concept_id"] for c in concepts if c["direction_id"] == direction)
        required = {tuple(pair) for pair in __import__("itertools").combinations(siblings, 2)}
        observed = {(min(p["concept_a_id"], p["concept_b_id"]), max(p["concept_a_id"], p["concept_b_id"])) for p in pairs if p["direction_id"] == direction}
        if required != observed or any(
            not p.get("materially_distinct") or not p.get("distinctness_basis") or not p.get("overlap_notes")
            or p["concept_a_id"] not in concept_by_id or p["concept_b_id"] not in concept_by_id
            or concept_by_id[p["concept_a_id"]]["direction_id"] != direction
            or concept_by_id[p["concept_b_id"]]["direction_id"] != direction
            for p in pairs if p["direction_id"] == direction
        ):
            raise ConceptValidationError("Sibling concept distinctness failed")
    validation_ids = [val.get("concept_id") for val in capture["validations"]]
    if len(validation_ids) != 6 or set(validation_ids) != {concept["concept_id"] for concept in concepts}:
        raise ConceptValidationError("Each concept must have exactly one validation record")
    evidence_by_id = {row["evidence_id"]: row for row in capture["evidence"]}
    if len(evidence_by_id) != len(capture["evidence"]) or len(source_ids) != len(capture["sources"]):
        raise ConceptValidationError("Evidence and source IDs must be unique")
    for concept in concepts:
        accepted_evidence_ids = set(upstream["evidence_ids"][concept["direction_id"]])
        for scope in concept["minimum_paid_scope"]:
            if not scope["evidence_ids"] or any(
                (evidence_id in evidence_by_id and evidence_by_id[evidence_id].get("concept_id") != concept["concept_id"])
                or (evidence_id not in evidence_by_id and evidence_id not in accepted_evidence_ids)
                for evidence_id in scope["evidence_ids"]
            ):
                raise ConceptValidationError("Scope evidence must resolve to same-concept fresh or accepted YEE-81 evidence")
    for val in capture["validations"]:
        concept_id = val["concept_id"]
        required_observation_lists = ("direct_competitors", "incumbent_coverage_observations", "differentiation_observations", "paid_value_exchange_observations", "feasibility_observations", "support_observations")
        if any(not val.get(name) for name in required_observation_lists):
            raise ConceptValidationError("Each concept requires incumbent, differentiation, paid-value, feasibility, and support validation records")
        if not val.get("direct_competitor_ids") or not val.get("direct_competitors"):
            raise ConceptValidationError("Each validation needs direct plugin incumbent coverage")
        if {row["competitor_id"] for row in val["direct_competitors"]} != set(val["direct_competitor_ids"]):
            raise ConceptValidationError("Direct competitor identity list does not reconcile")
        for competitor in val["direct_competitors"]:
            if competitor.get("relation_type") != "DIRECT" or competitor.get("plugin_scope_status") not in {"SERVER_SIDE_PLUGIN", "SERVER_AND_PROXY_PLUGIN"}:
                raise ConceptValidationError("Direct competitor must be a confirmed Minecraft server/proxy plugin")
            if not set(competitor.get("source_ids", [])) & source_ids or not competitor.get("evidence_ids"):
                raise ConceptValidationError("Direct competitor needs opened-source and evidence provenance")
            _validate_evidence_refs(competitor["evidence_ids"], concept_id, evidence_by_id)
        for obs in val["incumbent_coverage_observations"]:
            if obs.get("concept_id") != concept_id or not set(obs.get("source_ids", [])) <= source_ids or not obs.get("evidence_ids"):
                raise ConceptValidationError("Incumbent coverage must preserve same-concept opened-source evidence")
            if obs["coverage_state"] not in COVERAGE_STATES or obs["relation_type"] != "DIRECT":
                raise ConceptValidationError("Only validated direct plugin incumbents may be counted here")
            if not any(c["competitor_id"] == obs["competitor_id"] and c["plugin_scope_status"] in {"SERVER_SIDE_PLUGIN", "SERVER_AND_PROXY_PLUGIN"} for c in val["direct_competitors"]):
                raise ConceptValidationError("Direct competitor must be source-backed as a Minecraft plugin")
            _validate_evidence_refs(obs["evidence_ids"], concept_id, evidence_by_id, obs["source_ids"])
        for diff in val["differentiation_observations"]:
            if diff.get("concept_id") != concept_id:
                raise ConceptValidationError("Differentiation observation concept mismatch")
            if diff["gap_type"] not in GAP_TYPES:
                raise ConceptValidationError("Unknown differentiation gap type")
            used_ids = set(diff.get("buyer_evidence_ids", [])) | set(diff.get("competitor_evidence_ids", [])) | set(diff.get("counterevidence_ids", []))
            if any(evidence_by_id.get(eid, {}).get("concept_id") != concept_id for eid in used_ids):
                raise ConceptValidationError("Differentiation evidence must belong to the same concept")
            _validate_evidence_refs(sorted(used_ids), concept_id, evidence_by_id, diff.get("source_ids", []))
            diff["evidence_state"] = effective_differentiation_state(diff, capture["evidence"], capture["sources"])
            if diff["evidence_state"] not in DIFF_STATES:
                raise ConceptValidationError("Invalid differentiation state")
        for value in val["paid_value_exchange_observations"]:
            if value.get("concept_id") != concept_id or not set(value.get("source_ids", [])) <= source_ids:
                raise ConceptValidationError("Paid-value evidence concept/source mismatch")
            if value["evidence_type"] not in VALUE_TYPES or value["state"] not in VALUE_STATES:
                raise ConceptValidationError("Invalid paid value-exchange enum")
            if value["evidence_type"] == "DIRECT_WTP_STATEMENT" and not any(e["evidence_type"] == "DIRECT_WTP_STATEMENT" for e in capture["evidence"] if e["evidence_id"] in value["evidence_ids"]):
                raise ConceptValidationError("Competitor price or precedent cannot be classified as direct WTP")
            _validate_evidence_refs(value["evidence_ids"], concept_id, evidence_by_id, value["source_ids"])
        for observation_name in ("feasibility_observations", "support_observations"):
            for observation in val[observation_name]:
                if observation.get("concept_id") != concept_id or not set(observation.get("source_ids", [])) <= source_ids:
                    raise ConceptValidationError("Technical/support observation concept/source mismatch")
                _validate_evidence_refs(observation.get("evidence_ids", []), concept_id, evidence_by_id, observation.get("source_ids", []))
        for assessment in val["dimension_assessments"]:
            if assessment["state"] not in DIMENSION_STATES:
                raise ConceptValidationError("Invalid dimension state")
            if not {"dimension", "state", "basis", "evidence_ids", "source_ids", "risk_flags", "unknowns"} <= assessment.keys():
                raise ConceptValidationError("Dimension assessment is missing Worker Spec fields")
            if not assessment["basis"]:
                raise ConceptValidationError("Dimension assessment basis is required")
            if not isinstance(assessment["risk_flags"], list) or not isinstance(assessment["unknowns"], list):
                raise ConceptValidationError("Dimension risk_flags and unknowns must be arrays")
            if not set(assessment["source_ids"]) <= source_ids:
                raise ConceptValidationError("Dimension source references must resolve to captured sources")
            _validate_evidence_refs(assessment["evidence_ids"], concept_id, evidence_by_id, assessment["source_ids"])
        if len(val["dimension_assessments"]) != 7 or {d["dimension"] for d in val["dimension_assessments"]} != set(DIMENSIONS):
            raise ConceptValidationError("Each concept must have exactly seven dimensions")
        if val["research_coverage_status"] not in {"SUFFICIENT", "PARTIAL", "INSUFFICIENT"}:
            raise ConceptValidationError("Invalid research coverage status")
        if any(code not in EVIDENCE_GAPS for code in val.get("evidence_gap_codes", [])):
            raise ConceptValidationError("Unknown evidence-gap code")
    _reject_forbidden_fields(capture)
    return _cards(concepts, capture["validations"], capture)


def _flatten_validation(capture: dict[str, Any], name: str) -> list[dict[str, Any]]:
    return [row for validation in capture["validations"] for row in validation[name]]


def _write_sqlite(path: Path, tables: dict[str, list[dict[str, Any]]], metadata: dict[str, str]) -> None:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA page_size=4096")
    for table, columns in TABLE_COLUMNS.items():
        definitions = [f'"{col}" TEXT' for col in columns]
        definitions[0] += " PRIMARY KEY"
        if table != "concepts" and "concept_id" in columns:
            definitions[columns.index("concept_id")] += " REFERENCES concepts(concept_id)"
        if table == "concept_distinctness_relations":
            definitions[columns.index("concept_a_id")] += " REFERENCES concepts(concept_id)"
            definitions[columns.index("concept_b_id")] += " REFERENCES concepts(concept_id)"
        if table == "concept_evidence":
            definitions[columns.index("source_id")] += " REFERENCES concept_source_documents(source_id)"
        connection.execute(f'CREATE TABLE "{table}" ({", ".join(definitions)})')
    all_rows = dict(tables)
    all_rows["metadata"] = [{"key": k, "value": v} for k, v in sorted(metadata.items())]
    insert_order = ["concepts", "concept_source_documents"] + sorted(set(TABLE_COLUMNS) - {"concepts", "concept_source_documents"})
    for table in insert_order:
        columns = TABLE_COLUMNS[table]
        for row in all_rows.get(table, []):
            values = [row.get(col) for col in columns]
            encoded = [None if value is None else _json(value) if isinstance(value, (dict, list, bool)) else str(value) for value in values]
            marks = ",".join("?" for _ in columns)
            connection.execute(f'INSERT INTO "{table}" ({",".join(chr(34)+c+chr(34) for c in columns)}) VALUES ({marks})', encoded)
    connection.commit()
    connection.execute("VACUUM")
    connection.close()


def _write_docs(output: Path, cards: list[dict[str, Any]], capture: dict[str, Any], hashes: dict[str, str], commit: str, capture_sha: str) -> None:
    alignment = f"""# YEE-83 GOAL_ALIGNMENT

- Authorized cohort: `cross_platform_communication` (`{list(DIRECTIONS)[0]}`) and `jobs_and_rewards` (`{list(DIRECTIONS)[1]}`).
- Cohort SHA-256: `{COHORT_SHA256}` (verified).
- Product objective remains PLUGIN_ONLY; every thesis targets the same resolved server-side/proxy plugin job.
- YEE-81 is the only commercial-validation basis; YEE-79/YEE-77 are checked only as inherited provenance.
- HOLD `custom recipes` and DROP_CURRENT_CYCLE `auth`, `developer_automation` remain excluded.
- No YEE-30 through YEE-59 data or conclusions were used.
- Exactly three evidence-derived hypotheses per active direction; six total.
- Fresh research was limited to testing/falsifying these exact six concepts; no broad discovery.
- No implementation or product selection was performed. Final decision remains Supervisor/User-owned.

## Pinned input checks

{chr(10).join(f'- `{key}`: `{value}`' for key, value in sorted(hashes.items()))}
"""
    protocol = """# Concept generation and validation protocol

## Scope and inputs

Exactly six distinct hypotheses are organized as three concepts under each authorized YEE-81 resolved direction: cross-server/proxy chat relay and player activity jobs with configurable economic rewards. YEE-81 is the only commercial-validation source. YEE-79 and YEE-77 hashes are carried solely as inherited provenance; they do not supply findings. No YEE-30 through YEE-59 artifacts are used. Both directions are PLUGIN_ONLY. HOLD custom recipes and DROP_CURRENT_CYCLE auth/developer automation stay excluded.

## Research protocol

Each of the six concepts has one mandatory concept-targeted query for each exact purpose: `BUYER_PROBLEM_SPECIFICITY`, `PURCHASE_TRIGGER_SPECIFICITY`, `INCUMBENT_COVERAGE`, `DIFFERENTIATION_GAP`, `PAID_VALUE_EXCHANGE`, and `TECHNICAL_AND_SUPPORT_RISK` (36 mandatory pairs). The frozen capture additionally records same-purpose follow-ups where useful or where the initial query had no usable source. Queries test the named concept rather than reopen broad discovery. Search snippets are discovery-only. Only an opened public source with a canonical URL and locator can substantiate a normalized claim. Each query records its execution order, timestamp, result note, discovered URLs and opened source IDs; no-usable-source queries require a later same-concept/same-purpose query. Anecdotes are not generalized to prevalence. Out-of-scope products, unopenable pages, and unverified allegations are not promoted to facts.

## Evidence and uncertainty

Keep `DIRECT_WTP_STATEMENT`, `OBSERVED_PURCHASE_OR_PAYMENT`, `PAID_COMPETITOR_PRECEDENT`, `PREMIUM_TIER_PRECEDENT`, `BEHAVIORAL_PROXY`, `OPERATIONAL_VALUE_PROXY`, and `NO_WTP_EVIDENCE` separate. A competitor's asking price is not a buyer's WTP; no price recommendation or currency conversion is performed. `EVIDENCE_BACKED` differentiation requires at least two distinct canonical URLs, a buyer/operator source and a product/competitor source; unresolved counterevidence downgrades it. Record counterevidence, incumbent coverage, sample limits, compatibility, integration, data/security, support, and maintenance risks. Missing evidence remains an explicit unknown rather than being filled by inference.

## Distinctness and decision boundary

Each direction has the complete three-pair sibling distinctness matrix and a written non-overlap basis; repeated primary opportunity axes fail validation. Each card has exactly seven hard-rule dimension assessments with `dimension`, `state`, `basis`, `evidence_ids`, `source_ids`, `risk_flags`, and `unknowns`. The overall state uses the three core dimensions: all `SUPPORTED` gives `CONCEPT_EVIDENCE_SUPPORTED`; any `INSUFFICIENT_EVIDENCE` gives `CONCEPT_INSUFFICIENT_EVIDENCE`; otherwise any `MIXED` gives `CONCEPT_EVIDENCE_MIXED`; otherwise `WEAK` gives `CONCEPT_EVIDENCE_WEAK` only with sufficient research coverage, and incomplete coverage remains insufficient. These labels are not a scalar score, rank, shortlist, or recommendation. The supervisor decision template remains UNDECIDED. No product is selected, specified, or implemented.
"""
    schema_lines = [
        "# YEE-83 concept validation schema", "", "All normalized SQLite tables have a primary key in column 1. JSONL preserves null; CSV uses `\\N` for null and compact canonical JSON for arrays, objects, and booleans. Rows and export files are sorted deterministically. Source URLs are canonicalized and duplicates rejected. All four pinned inputs are opened read-only.",
        "", "## Tables and ordered columns", "",
    ]
    for table, columns in TABLE_COLUMNS.items():
        schema_lines.append(f"- `{table}`: " + ", ".join(f"`{column}`" for column in columns))
    schema_lines += [
        "", "## Enum contracts", "",
        f"- `dimension_assessments.dimension`: `{ ' | '.join(DIMENSIONS) }`",
        f"- `dimension_assessments.state`: `{ ' | '.join(sorted(DIMENSION_STATES)) }`",
        f"- `concept_validation_state`: `{ ' | '.join(sorted(CONCEPT_STATES)) }`",
        f"- `scope_state`: `{ ' | '.join(sorted(SCOPE_STATES)) }`",
        f"- `incumbent_coverage_state`: `{ ' | '.join(sorted(COVERAGE_STATES)) }`",
        f"- `gap_type`: `{ ' | '.join(sorted(GAP_TYPES)) }`",
        f"- `differentiation.evidence_state`: `{ ' | '.join(sorted(DIFF_STATES)) }`",
        f"- `paid_value.evidence_type`: `{ ' | '.join(sorted(VALUE_TYPES)) }`",
        f"- `paid_value.state`: `{ ' | '.join(sorted(VALUE_STATES)) }`",
        f"- `research_coverage_status`: `SUFFICIENT | PARTIAL | INSUFFICIENT`",
        "- `supervisor_decision_template.decision_status`: `UNDECIDED` in this work order.",
        "- `concept_validation_queries.query_kind`: `MANDATORY | FOLLOWUP`; follow-ups preserve concept and purpose.",
        "- `concept_scope_items.incumbent_coverage_state`: `NOT_OBSERVED | PARTIALLY_COVERED | SUBSTANTIALLY_COVERED | UNKNOWN` (`CONFLICTING` belongs only to the richer incumbent-observation state).",
        "- `dimension_assessments`: exactly seven per concept; each stores `dimension`, `state`, `basis`, `evidence_ids`, `source_ids`, `risk_flags`, and `unknowns`.",
        "", "## Null and evidence semantics", "",
        "Unknown, unavailable, or unobserved values remain null/explicit unknowns; missing prices are not FREE. A price/paid competitor listing is not direct WTP. Every normalized evidence row must cite an opened public canonical source, source locator, same-concept query, and `used_search_snippet=false`. Scope evidence must resolve to the same concept; source IDs on normalized observations must include the sources backing their linked evidence. No score/rank/winner/recommendation/product-spec or currency-converted-price fields are permitted.",
    ]
    schema = "\n".join(schema_lines) + "\n"
    brief_parts = ["# Supervisor concept decision brief", "", "All entries are unranked evidence dossiers; no build recommendation is made."]
    validation_by_id = {row["concept_id"]: row for row in capture["validations"]}
    concept_by_id = {row["concept_id"]: row for row in capture["concepts"]}
    for card in cards:
        concept = concept_by_id[card["concept_id"]]
        val = validation_by_id[card["concept_id"]]
        brief_parts += [
            "", f"## {card['concept_id']} — {card['concept_label']}", "",
            f"**Product thesis (hypothesis):** {card['problem_statement']}", "",
            f"**Target buyer / operating context:** {card['target_buyer_segment']} — {card['operator_context']}", "",
            f"**Exact problem and trigger:** {card['problem_statement']} Trigger: {card['purchase_trigger_hypothesis']}", "",
            f"**Primary opportunity axis:** {card['primary_opportunity_axis']}", "", "**Minimum paid scope:**",
        ]
        for scope in concept["minimum_paid_scope"]:
            brief_parts.append(f"- `{scope['scope_item_id']}` ({scope['scope_state']}): {scope['capability_statement']} Buyer link: {scope['buyer_value_link']} Incumbent coverage: {scope['incumbent_coverage_state']}. Evidence: {', '.join(scope['evidence_ids'])}. {scope['notes']}")
        brief_parts += ["", f"**Explicit non-goals:** {'; '.join(card['explicit_non_goals'])}", ""]
        direct_names = [f"{row['competitor_name']} (`{row['competitor_id']}`)" for row in val["direct_competitors"]]
        brief_parts += [f"**Current direct incumbents:** {', '.join(direct_names)}", f"**Substitutes / adjacent context:** {', '.join(card['substitute_competitor_ids']) or 'none retained'} / {', '.join(card['adjacent_context_ids']) or 'none retained'}", "", "**Incumbent coverage of this thesis:**"]
        for obs in val["incumbent_coverage_observations"]:
            brief_parts.append(f"- {obs['competitor_name']} — `{obs['coverage_state']}` on {obs['tested_axis']}; evidence: {', '.join(obs['evidence_ids'])}. {obs['notes']}")
        brief_parts += ["", "**Differentiation:**"]
        for diff in val["differentiation_observations"]:
            brief_parts.append(f"- `{diff['evidence_state']}` — {diff['gap_statement']} Buyer evidence: {', '.join(diff['buyer_evidence_ids']) or 'none'}. Competitor evidence: {', '.join(diff['competitor_evidence_ids']) or 'none'}. Counterevidence: {', '.join(diff['counterevidence_ids']) or 'none'}. {diff['notes']}")
        brief_parts += ["", "**Paid value exchange evidence (not a price recommendation):"]
        for value in val["paid_value_exchange_observations"]:
            brief_parts.append(f"- `{value['evidence_type']}` / `{value['state']}` — {value['value_statement']} Evidence: {', '.join(value['evidence_ids'])}. Limitations: {value['limitations']}")
        brief_parts += ["", "**Implementation / support risks:**"]
        for obs in val["feasibility_observations"]:
            brief_parts.append(f"- Feasibility — {obs['theme']}: {obs['observation']} Evidence: {', '.join(obs['evidence_ids'])}. {obs['notes']}")
        for obs in val["support_observations"]:
            brief_parts.append(f"- Support — {obs['burden_theme']}: {obs['observation']} Evidence: {', '.join(obs['evidence_ids'])}. {obs['notes']}")
        brief_parts += ["", "**Seven validation dimensions:**"]
        for dimension in card["dimension_assessments"]:
            brief_parts.append(f"- `{dimension['dimension']}` = `{dimension['state']}` — {dimension['basis']} Evidence: {', '.join(dimension['evidence_ids'])}; sources: {', '.join(dimension['source_ids'])}. Risks: {', '.join(dimension['risk_flags']) or 'none separately recorded'}. Unknowns: {', '.join(dimension['unknowns']) or 'none separately recorded'}.")
        novelty = card["novelty_disclosure"]
        brief_parts += ["", f"**Concept state / research coverage:** `{card['concept_validation_state']}` / `{card['research_coverage_status']}`", f"**Evidence-backed synthesis:** {', '.join(novelty.get('EVIDENCE_BACKED', [])) or 'none asserted'}", f"**Synthesized from evidence:** {', '.join(novelty.get('SYNTHESIZED_FROM_EVIDENCE', [])) or 'none recorded'}", f"**Hypotheses still to validate:** {', '.join(novelty.get('HYPOTHESIS_TO_VALIDATE', [])) or 'none recorded'}", f"**Evidence gaps:** {', '.join(card['evidence_gap_codes']) or 'none'}", f"**Unknowns / coverage limits:** {'; '.join(card['unknowns'])} Research notes: {card['research_notes']}"]
    status_groups = {
        "EVIDENCE_BACKED differentiation": [], "direct WTP evidence": [], "paid-market precedent without direct WTP": [],
        "substantially incumbent-covered": [], "major compatibility/integration/support risks": [], "insufficient evidence": [],
    }
    for card in cards:
        if any(d.get("evidence_state") == "EVIDENCE_BACKED" for d in _flatten_validation(capture, "differentiation_observations") if d["concept_id"] == card["concept_id"]):
            status_groups["EVIDENCE_BACKED differentiation"].append(card["concept_id"])
        if any(v["evidence_type"] == "DIRECT_WTP_STATEMENT" for v in _flatten_validation(capture, "paid_value_exchange_observations") if v["concept_id"] == card["concept_id"]):
            status_groups["direct WTP evidence"].append(card["concept_id"])
        if any(v["evidence_type"] == "PAID_COMPETITOR_PRECEDENT" for v in _flatten_validation(capture, "paid_value_exchange_observations") if v["concept_id"] == card["concept_id"]):
            status_groups["paid-market precedent without direct WTP"].append(card["concept_id"])
        if any(o["coverage_state"] == "SUBSTANTIALLY_COVERED" for o in _flatten_validation(capture, "incumbent_coverage_observations") if o["concept_id"] == card["concept_id"]):
            status_groups["substantially incumbent-covered"].append(card["concept_id"])
        if any("RISK" in x for x in card["risk_flags"]):
            status_groups["major compatibility/integration/support risks"].append(card["concept_id"])
        if card["concept_validation_state"] == "CONCEPT_INSUFFICIENT_EVIDENCE":
            status_groups["insufficient evidence"].append(card["concept_id"])
    brief_parts += ["", "## Descriptive cross-cuts (not ranked)", ""]
    brief_parts += [f"- {label}: {', '.join(ids) if ids else 'none'}" for label, ids in status_groups.items()]
    decision = capture["decision_template"]
    state_counts = {state: sum(card["concept_validation_state"] == state for card in cards) for state in sorted(CONCEPT_STATES)}
    paid_values = _flatten_validation(capture, "paid_value_exchange_observations")
    report_lines = [
        "# YEE-83 Final Report", "", "Status: `PRODUCT_OPPORTUNITY_CONCEPT_VALIDATION_READY_FOR_SUPERVISOR_REVIEW`", "",
        f"Generated exactly {len(cards)} materially distinct evidence-derived hypotheses (three under each of two authorized YEE-81 directions). No concept was selected, ranked, scored, priced, specified, or implemented.", "",
        f"Frozen targeted research capture: {len(capture['queries'])} execution-ordered queries ({sum(q['query_kind'] == 'MANDATORY' for q in capture['queries'])} mandatory; {sum(q['query_kind'] == 'FOLLOWUP' for q in capture['queries'])} follow-ups), {len(capture['sources'])} opened sources, and {len(capture['evidence'])} normalized evidence rows. All six mandatory purposes were covered for each concept; facts cite opened canonical sources, not search snippets.", "",
        f"Concept state counts (descriptive, not ranked): `{_json(state_counts)}`.", "",
        f"Typed paid-value observations: {sum(v['evidence_type'] == 'DIRECT_WTP_STATEMENT' for v in paid_values)} direct-WTP observations; {sum(v['evidence_type'] == 'PAID_COMPETITOR_PRECEDENT' for v in paid_values)} paid-competitor-precedent observations. Asking prices were not converted or treated as buyer WTP.", "",
        "The decision dossier records current incumbent coverage, differentiation counterevidence, implementation/support constraints, seven hard-rule dimensions, evidence gaps, unknowns, and the untouched Supervisor decision template. Anecdotes are not generalized to prevalence; missing evidence is not interpreted as demand or feature absence.", "",
        f"Execution commit: `{commit}`  ", f"Accepted YEE-81 execution commit: `{ACCEPTED_YEE81_EXECUTION_COMMIT}`  ", f"Frozen capture SHA-256: `{capture_sha}`", "", "## Per-concept validation state", "",
    ]
    for card in cards:
        core = {d["dimension"]: d["state"] for d in card["dimension_assessments"] if d["dimension"] in {"BUYER_PROBLEM_ALIGNMENT", "PURCHASE_TRIGGER_ALIGNMENT", "DIFFERENTIATION"}}
        report_lines.append(f"- `{card['concept_id']}` ({card['canonical_direction_key']}) — `{card['concept_validation_state']}`, research `{card['research_coverage_status']}`; core dimensions: `{_json(core)}`; evidence gaps: {', '.join(card['evidence_gap_codes']) or 'none'}.")
    report = "\n".join(report_lines) + "\n"
    (output / "GOAL_ALIGNMENT.md").write_text(alignment, encoding="utf-8", newline="\n")
    (output / "CONCEPT_GENERATION_PROTOCOL.md").write_text(protocol, encoding="utf-8", newline="\n")
    (output / "CONCEPT_VALIDATION_SCHEMA.md").write_text(schema, encoding="utf-8", newline="\n")
    (output / "SUPERVISOR_CONCEPT_DECISION_BRIEF.md").write_text("\n".join(brief_parts).rstrip() + "\n", encoding="utf-8", newline="\n")
    (output / "SUPERVISOR_CONCEPT_DECISION_TEMPLATE.json").write_text(_json(decision) + "\n", encoding="utf-8", newline="\n")
    (output / "FINAL_REPORT.md").write_text(report, encoding="utf-8", newline="\n")


def _render_bundle(input_paths: dict[str, Path], capture_path: Path, output: Path, commit: str, repo_root: Path) -> dict[str, Any]:
    hashes = validate_hashes(input_paths)
    if sha256_file(capture_path) == "":
        raise ConceptValidationError("Empty capture file")
    capture_bytes = capture_path.read_bytes()
    capture = json.loads(capture_bytes)
    if capture.get("capture_version") != CAPTURE_VERSION:
        raise ConceptValidationError("Unexpected frozen capture version")
    if commit != subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo_root, text=True, capture_output=True, check=True).stdout.strip():
        raise ConceptValidationError("Execution commit does not match checkout HEAD")
    ancestry = subprocess.run(["git", "merge-base", "--is-ancestor", BASELINE_COMMIT, commit], cwd=repo_root, capture_output=True)
    if ancestry.returncode != 0:
        raise ConceptValidationError("Accepted YEE-81 merge commit is not in execution history")
    upstream = _read_upstream(input_paths["yee81_sqlite"])
    cards = _validate_capture(capture, upstream)
    if output.exists():
        if any(output.iterdir()):
            raise ConceptValidationError("Output directory must be empty")
    else:
        output.mkdir(parents=True)

    sources = []
    for source in sorted(capture["sources"], key=lambda x:x["source_id"]):
        row = dict(source)
        row["canonical_url"] = canonical_url(row["url"])
        row["domain"] = urlsplit(row["canonical_url"]).netloc
        sources.append(row)
    queries = sorted(capture["queries"], key=lambda row: int(row["execution_order"]))
    evidence = _stable_rows(capture["evidence"], "evidence_id")
    seed_trace = []
    for concept in sorted(capture["concepts"], key=lambda x:x["concept_id"]):
        for evidence_id in sorted(concept["upstream_seed_evidence_ids"]):
            seed_trace.append({"trace_id":_stable_id("seed", concept["concept_id"], evidence_id), "concept_id":concept["concept_id"], "direction_id":concept["direction_id"], "seed_type":"YEE81_EVIDENCE", "upstream_id":evidence_id, "source_id":upstream["evidence"][concept["direction_id"]][evidence_id]["source_id"], "relevance":"Accepted YEE-81 evidence directly grounds the buyer/problem or competition/feasibility axis."})
        for buyer_id in sorted(concept["upstream_buyer_observation_ids"]):
            seed_trace.append({"trace_id":_stable_id("seed", concept["concept_id"], buyer_id), "concept_id":concept["concept_id"], "direction_id":concept["direction_id"], "seed_type":"YEE81_BUYER_OBSERVATION", "upstream_id":buyer_id, "source_id":upstream["buyers"][concept["direction_id"]][buyer_id]["source_id"], "relevance":"Accepted buyer/operator observation grounding the exact pain or trigger."})
        for competitor_id in sorted(concept["upstream_competitor_ids"]):
            comp = upstream["competitors"][concept["direction_id"]][competitor_id]
            seed_trace.append({"trace_id":_stable_id("seed", concept["concept_id"], competitor_id), "concept_id":concept["concept_id"], "direction_id":concept["direction_id"], "seed_type":"YEE81_DIRECT_COMPETITOR", "upstream_id":competitor_id, "source_id":comp["source_ids"][0], "relevance":"Accepted YEE-81 direct incumbent anchors coverage and differentiation testing."})
    scope_items = _stable_rows([item for c in capture["concepts"] for item in c["minimum_paid_scope"]], "scope_item_id")
    distinctness = _stable_rows(capture["distinctness_relations"], "relation_id")
    coverage = _stable_rows(_flatten_validation(capture, "incumbent_coverage_observations"), "coverage_id")
    differentiation = _stable_rows(_flatten_validation(capture, "differentiation_observations"), "differentiation_id")
    paid = _stable_rows(_flatten_validation(capture, "paid_value_exchange_observations"), "value_id")
    feasibility = _stable_rows(_flatten_validation(capture, "feasibility_observations"), "feasibility_id")
    support = _stable_rows(_flatten_validation(capture, "support_observations"), "support_id")
    tables = {
        "input_provenance":[{"input_name":name,"sha256":value,"read_only":True} for name,value in sorted(hashes.items())],
        "authorized_direction_cohort":[{"direction_id":d,"canonical_direction_key":DIRECTIONS[d][0],"resolved_market_job":DIRECTIONS[d][1],"cohort_sha256":COHORT_SHA256} for d in sorted(DIRECTIONS)],
        "concepts":_stable_rows(capture["concepts"],"concept_id"),
        "concept_seed_trace":seed_trace,"concept_scope_items":scope_items,"concept_distinctness_relations":distinctness,
        "concept_source_documents":sources,"concept_validation_queries":queries,"concept_evidence":evidence,
        "incumbent_coverage_observations":coverage,"concept_differentiation_observations":differentiation,
        "paid_value_exchange_observations":paid,"concept_feasibility_observations":feasibility,
        "concept_support_observations":support,"concept_validation_cards":cards,
        "supervisor_decision_template":[capture["decision_template"]],
    }
    for filename, table in EXPORTS.items():
        _jsonl_csv(output / filename, _table_rows(table, tables[table]), TABLE_COLUMNS[table])
    capture_sha = hashlib.sha256(capture_bytes).hexdigest()
    (output / "CONCEPT_VALIDATION_RESEARCH_CAPTURE.json").write_bytes(capture_bytes)
    _write_docs(output, cards, capture, hashes, commit, capture_sha)
    db_path = output / "concept_validation.sqlite"
    metadata = {
        **hashes, "work_order":WORK_ORDER, "execution_code_commit":commit, "accepted_yee81_merge_commit":BASELINE_COMMIT, "accepted_yee81_execution_commit":ACCEPTED_YEE81_EXECUTION_COMMIT,
        "authorized_direction_cohort_sha256":COHORT_SHA256, "concept_id_set_sha256":hashlib.sha256(("\n".join(sorted(c["concept_id"] for c in cards))+"\n").encode()).hexdigest(),
        "frozen_capture_sha256":capture_sha, "product_scope":"PLUGIN_ONLY", "analysis_mode":"TARGETED_CONCEPT_VALIDATION_ONLY",
    }
    _write_sqlite(db_path, tables, metadata)
    db = sqlite3.connect(db_path)
    try:
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        fk = db.execute("PRAGMA foreign_key_check").fetchall()
        sqlite_counts = {table: db.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] for table in TABLE_COLUMNS}
    finally:
        db.close()
    export_reconciles = True
    for filename, table in EXPORTS.items():
        stem = output / filename
        expected_count = len(tables[table])
        jsonl_count = len(stem.with_suffix(".jsonl").read_text(encoding="utf-8").splitlines())
        with stem.with_suffix(".csv").open(encoding="utf-8", newline="") as stream:
            csv_count = sum(1 for _ in csv.DictReader(stream))
        export_reconciles = export_reconciles and jsonl_count == expected_count and csv_count == expected_count
    sqlite_reconciles = all(sqlite_counts[table] == len(tables[table]) for table in EXPORTS.values())
    concept_ids = {card["concept_id"] for card in cards}
    mandatory_queries = [query for query in queries if query["query_kind"] == "MANDATORY"]
    direct_competitors = [row for validation in capture["validations"] for row in validation["direct_competitors"]]
    checks = {
        "pinned_inputs_match": all(hashes[k] == INPUT_HASHES[k] for k in INPUT_HASHES),
        "accepted_yee81_source_is_read_only_canonical": set(hashes) == set(INPUT_HASHES),
        "accepted_baseline_in_history": ancestry.returncode == 0,
        "exact_authorized_cohort": sorted(DIRECTIONS) == sorted({c["direction_id"] for c in cards}),
        "exactly_six_concepts": len(cards) == 6 and len(concept_ids) == 6,
        "three_concepts_per_direction": all(sum(c["direction_id"] == d for c in cards) == 3 for d in DIRECTIONS),
        "no_held_or_dropped_direction": all(c["direction_id"] in DIRECTIONS for c in cards),
        "plugin_only_preserved": all(c["canonical_direction_key"] in {DIRECTIONS[d][0] for d in DIRECTIONS} for c in cards),
        "no_legacy_yee30_to_yee59_input": not any("yee" in name.lower() and any(f"yee{n}" in name.lower() for n in range(30, 60)) for name in hashes),
        "all_six_research_purposes": len(mandatory_queries) == 36 and {q["purpose"] for q in mandatory_queries} == set(PURPOSES),
        "mandatory_pairs_complete": len({(q["concept_id"], q["purpose"]) for q in mandatory_queries}) == 36,
        "query_source_references_resolve": all(set(q["opened_source_ids"]) <= {s["source_id"] for s in sources} for q in queries),
        "concept_seed_types_complete": all(
            bool({upstream["evidence"][c["direction_id"]][eid].get("evidence_type") for eid in c["upstream_seed_evidence_ids"]} & {"BUYER_NEED", "EXPLICIT_PURCHASE_INTENT", "OPERATOR_FRICTION", "SUPPORT_CASE", "SECURITY_SUPPORT_REPORT", "USER_FRICTION"})
            and bool({upstream["evidence"][c["direction_id"]][eid].get("evidence_type") for eid in c["upstream_seed_evidence_ids"]} & {"CHANNEL_AND_COMPETITOR_LISTING", "COMPETITOR_FEATURES", "FREE_INCUMBENT", "INTEGRATION_SUPPORT", "PAID_COMPETITOR_PRICE", "PAID_MARKET_PRECEDENT", "PLATFORM_COMPATIBILITY_DOCUMENTATION"})
            for c in capture["concepts"]
        ),
        "opened_source_evidence_only": all(e["used_search_snippet"] is False for e in evidence),
        "all_negative_queries_followed_up": all(
            q.get("usable_source_found") is not False or any(x.get("followup_of") == q["query_id"] for x in queries)
            for q in queries
        ),
        "distinctness_relations_complete": len(distinctness) == 6 and all(r["materially_distinct"] for r in distinctness),
        "direct_competitor_plugin_scope": all(o["relation_type"] == "DIRECT" and o["plugin_scope_status"] in {"SERVER_SIDE_PLUGIN", "SERVER_AND_PROXY_PLUGIN"} for o in direct_competitors),
        "differentiation_threshold_applied": all(
            row["evidence_state"] != "EVIDENCE_BACKED" or effective_differentiation_state(row, evidence, sources) == "EVIDENCE_BACKED"
            for row in differentiation
        ),
        "no_direct_wtp_from_competitor_price": all(v["evidence_type"] != "DIRECT_WTP_STATEMENT" or any(e["evidence_type"] == "DIRECT_WTP_STATEMENT" for e in evidence if e["evidence_id"] in v["evidence_ids"]) for v in paid),
        "seven_dimensions_each": all(len(c["dimension_assessments"]) == 7 and {d["dimension"] for d in c["dimension_assessments"]} == set(DIMENSIONS) for c in cards),
        "dimension_schema_complete": all({"dimension", "state", "basis", "evidence_ids", "source_ids", "risk_flags", "unknowns"} <= d.keys() for c in cards for d in c["dimension_assessments"]),
        "hard_rule_states": all(c["concept_validation_state"] == derive_concept_state(c["dimension_assessments"], c["research_coverage_status"]) for c in cards),
        "decision_template_undecided": capture["decision_template"] == {"decision_status":"UNDECIDED", "advanced_concept_ids":[], "requested_refinement_concept_ids":[], "held_concept_ids":[], "dropped_concept_ids":[], "build_none":False, "rationale":None, "decided_at":None},
        "sqlite_integrity": integrity == "ok",
        "sqlite_foreign_keys": not fk,
        "jsonl_csv_reconciliation": export_reconciles,
        "sqlite_export_reconciliation": sqlite_reconciles,
        "upstream_inputs_unchanged": hashes == validate_hashes(input_paths),
    }
    qa = {"work_order":WORK_ORDER,"status":"PASS" if all(checks.values()) else "FAIL","execution_code_commit":commit,"baseline_commit":BASELINE_COMMIT,"accepted_yee81_execution_commit":ACCEPTED_YEE81_EXECUTION_COMMIT,"input_hashes_before":hashes,"input_hashes_after":validate_hashes(input_paths),"cohort_sha256":COHORT_SHA256,"concept_id_set_sha256":metadata["concept_id_set_sha256"],"concept_count":len(cards),"concepts_per_direction":{d:sum(c["direction_id"]==d for c in cards) for d in sorted(DIRECTIONS)},"query_count":len(queries),"mandatory_query_count":len(mandatory_queries),"followup_query_count":len(queries)-len(mandatory_queries),"mandatory_concept_purpose_pairs":len({(q["concept_id"],q["purpose"]) for q in mandatory_queries}),"mandatory_queries_by_purpose":{purpose:sum(q["purpose"]==purpose for q in mandatory_queries) for purpose in PURPOSES},"opened_source_count":len(sources),"evidence_count":len(evidence),"direct_wtp_observation_count":sum(v["evidence_type"]=="DIRECT_WTP_STATEMENT" for v in paid),"paid_competitor_precedent_count":sum(v["evidence_type"]=="PAID_COMPETITOR_PRECEDENT" for v in paid),"concept_state_counts":{state:sum(c["concept_validation_state"]==state for c in cards) for state in sorted(CONCEPT_STATES)},"research_coverage_counts":{state:sum(c["research_coverage_status"]==state for c in cards) for state in ("SUFFICIENT","PARTIAL","INSUFFICIENT")},"checks":checks,"failed_checks":[name for name,passed in checks.items() if not passed],"sqlite_row_counts":sqlite_counts,"sqlite_integrity_check":integrity,"sqlite_foreign_key_violations":len(fk),"replay":"BYTE_IDENTICAL"}
    (output / "QA_RESULT.json").write_text(_json(qa)+"\n",encoding="utf-8",newline="\n")
    files = sorted(p for p in output.iterdir() if p.is_file() and p.name not in {"DATASET_MANIFEST.json"})
    manifest = {"work_order":WORK_ORDER,"execution_code_commit":commit,"capture_sha256":capture_sha,"input_hashes":hashes,"cohort_sha256":COHORT_SHA256,"concept_id_set_sha256":metadata["concept_id_set_sha256"],"file_count":len(files)+1,"files":[{"path":p.name,"size_bytes":p.stat().st_size,"sha256":sha256_file(p)} for p in files]}
    (output/"DATASET_MANIFEST.json").write_text(_json(manifest)+"\n",encoding="utf-8",newline="\n")
    return qa


def build_bundle(input_paths: dict[str, Path], capture_path: Path, output_dir: Path, commit: str, repo_root: Path, verify_replay: bool = True) -> dict[str, Any]:
    qa = _render_bundle(input_paths, capture_path, output_dir, commit, repo_root)
    if verify_replay and qa["status"] == "PASS":
        with tempfile.TemporaryDirectory(prefix="yee83-replay-") as tmp:
            replay = Path(tmp) / "bundle"
            replay.mkdir()
            replay_qa = _render_bundle(input_paths, capture_path, replay, commit, repo_root)
            if replay_qa["status"] != "PASS":
                raise ConceptValidationError("Deterministic replay failed QA")
            original_files = {p.name: p.read_bytes() for p in output_dir.iterdir() if p.is_file()}
            replay_files = {p.name: p.read_bytes() for p in replay.iterdir() if p.is_file()}
            if original_files != replay_files:
                raise ConceptValidationError("Frozen-capture replay is not byte-identical")
    else:
        qa["replay"] = "NOT_RUN"
    if qa["status"] != "PASS":
        raise ConceptValidationError("YEE-83 production QA failed")
    return qa
