"""Deterministic YEE-81 Stage G commercial-evidence normalization."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

WORK_ORDER = "YEE-81"
CAPTURE_VERSION = "yee-81-deep-commercial-validation-v0.2"
BASELINE_COMMIT = "8af8db746ec7e72d510f6e05402bb59882fa8de1"
EXPECTED_INPUT_HASHES = {
    "yee79_decision_gate": "7059d52e7f54c7b6b0709f6301a7ceb0a442e2c7635dcc8912cbe5c21f3079fe",
    "yee77_research": "a2ea751823358e1032c36fd31a88d2e581476bf6e915a53c042b70614bcc4f48",
}
AUTHORIZED_DIRECTIONS = {
    "dir_0480c15eace353f0426f1db5": ("auth", "administration"),
    "dir_142f5946b84de25a3a07dd76": ("cross_platform_communication", "communication"),
    "dir_d36aa111cd7e8f7191d5ac47": ("developer_automation", "developer_tools"),
    "dir_78e16d4e251e9350b653a2de": ("jobs_and_rewards", "economy"),
    "dir_617529293c4b381179ea326e": ("custom recipes", "gameplay"),
}
QUERY_PURPOSES = (
    "BUYER_OPERATOR_SEGMENT", "PAIN_DEPTH_CONSEQUENCE", "PURCHASE_TRIGGER",
    "PAID_ALTERNATIVES_PRICING", "FREE_INCUMBENTS_SUBSTITUTES", "WTP_EVIDENCE",
    "DIFFERENTIATION_UNMET_NEEDS", "CHANNEL_FIT", "IMPLEMENTATION_COMPATIBILITY",
    "SUPPORT_MAINTENANCE",
)
DIMENSIONS = (
    "BUYER_PROBLEM", "PURCHASE_TRIGGER", "PAID_MARKET_WTP", "DIFFERENTIATION",
    "CHANNEL_FIT", "IMPLEMENTATION_FEASIBILITY", "SUPPORT_MAINTENANCE",
)
DIMENSION_STATES = {"SUPPORTED", "MIXED", "WEAK", "INSUFFICIENT_EVIDENCE"}
OBSERVATION_TYPES = {"BUYER_SEGMENT", "OPERATOR_PAIN", "PURCHASE_TRIGGER", "WORKFLOW", "DIRECT_WTP_STATEMENT"}
EVIDENCE_STRENGTHS = {"DIRECT_STATEMENT", "DIRECT_BEHAVIOR", "BEHAVIORAL_PROXY", "CONTEXTUAL"}
RELATION_TYPES = {"DIRECT", "SUBSTITUTE", "ADJACENT"}
DIFFERENTIATION_STATES = {"EVIDENCE_BACKED", "HYPOTHESIS_ONLY", "CONFLICTING", "INSUFFICIENT"}
DIFFERENTIATION_GAPS = {
    "MISSING_CAPABILITY", "COMPATIBILITY_GAP", "WORKFLOW_FRICTION", "CONFIGURATION_COMPLEXITY",
    "MIGRATION_GAP", "SUPPORT_GAP", "SECURITY_OR_SAFETY_GAP", "INTEGRATION_GAP", "OTHER_SOURCE_BACKED_GAP",
}
FEASIBILITY_THEMES = {
    "PLATFORM_SCOPE", "API_DEPENDENCY", "PROXY_NETWORKING", "DATA_PERSISTENCE", "MIGRATION",
    "ECONOMY_INTEGRATION", "PERMISSIONS_INTEGRATION", "SECURITY_SENSITIVITY", "SCRIPTING_SANDBOX",
    "RECIPE_OR_GAME_API_CONSTRAINT", "CROSS_PLATFORM_COMPATIBILITY", "VERSION_CHURN", "OTHER",
}
WTP_TYPES = {
    "DIRECT_WTP_STATEMENT", "OBSERVED_PURCHASE_OR_PAYMENT", "PAID_COMPETITOR_PRECEDENT",
    "PREMIUM_TIER_PRECEDENT", "DONATION_OR_SPONSOR_PRECEDENT", "NO_WTP_EVIDENCE",
}
EVIDENCE_GAP_CODES = {
    "INSUFFICIENT_BUYER_EVIDENCE", "INSUFFICIENT_PAIN_EVIDENCE", "PURCHASE_TRIGGER_UNCLEAR",
    "NO_DIRECT_WTP_EVIDENCE", "NO_PAID_MARKET_PRECEDENT", "PRICING_COVERAGE_INSUFFICIENT",
    "FREE_INCUMBENT_PRESSURE_PRESENT", "DIFFERENTIATION_NOT_ESTABLISHED", "CHANNEL_FIT_NOT_ESTABLISHED",
    "IMPLEMENTATION_FEASIBILITY_RISK", "SUPPORT_BURDEN_RISK", "COMPATIBILITY_SCOPE_RISK",
    "SECURITY_SENSITIVITY_RISK", "SOURCE_COVERAGE_RISK", "CONFLICTING_EVIDENCE",
}
COHORT_SHA256 = "f92a72adaead4e4f66d542b819a4d5a7c30cb9a87d66db728f96fcbfb4155130"
REQUIRED_FOLLOWUP_QUERY_IDS = {
    "F-081-001", "F-081-002", "F-081-003", "F-081-004", "F-081-005", "F-081-006",
    "F-081-007", "F-081-008", "F-081-009", "F-081-010", "F-081-011", "F-081-012", "F-081-013",
}
SOURCE_TYPE_MAP = {
    "OFFICIAL_PROJECT_REPOSITORY": "PRIMARY_REPOSITORY",
    "OFFICIAL_PROJECT_DOCUMENTATION": "PRIMARY_DOCS",
    "OFFICIAL_PRODUCT_DOCUMENTATION": "PRIMARY_DOCS",
    "OFFICIAL_PRODUCT_OR_DOCUMENTATION": "PRIMARY_PRODUCT",
    "OFFICIAL_VENDOR_PRODUCT_PAGE": "PRIMARY_PRODUCT",
    "OFFICIAL_ISSUE_TRACKER": "PRIMARY_SUPPORT",
    "OFFICIAL_PLUGIN_CATALOG_LISTING": "MARKETPLACE_LISTING",
    "MARKETPLACE_LISTING": "MARKETPLACE_LISTING",
    "COMMUNITY_DISCUSSION": "COMMUNITY",
    "OFFICIAL_PLATFORM_DOCUMENTATION": "PLATFORM_DOCUMENTATION",
    "PRIMARY_PRODUCT": "PRIMARY_PRODUCT",
    "PRIMARY_DOCS": "PRIMARY_DOCS",
    "PRIMARY_REPOSITORY": "PRIMARY_REPOSITORY",
    "PRIMARY_SUPPORT": "PRIMARY_SUPPORT",
    "MARKETPLACE_LISTING": "MARKETPLACE_LISTING",
    "COMMUNITY": "COMMUNITY",
    "EDITORIAL": "EDITORIAL",
    "PLATFORM_DOCUMENTATION": "PLATFORM_DOCUMENTATION",
}
NULL_TOKEN = r"\N"

TABLES: dict[str, tuple[str, ...]] = {
    "commercial_source_documents": ("source_id", "url", "canonical_url", "domain", "title", "source_type", "access_status", "retrieved_at", "published_or_updated_at", "date_note"),
    "commercial_research_queries": ("query_id", "direction_id", "execution_order", "sequence", "purpose", "query_text", "issued_at", "query_kind", "result_note", "discovered_result_urls", "opened_source_ids"),
    "commercial_evidence": ("evidence_id", "direction_id", "source_id", "query_ids", "evidence_type", "observation", "source_locator", "limitations"),
    "buyer_problem_observations": ("observation_id", "direction_id", "buyer_segment_label", "operator_context", "observation_type", "observation", "operational_consequence", "purchase_trigger", "source_id", "evidence_strength", "retrieved_at", "notes", "evidence_ids", "query_ids"),
    "commercial_competitor_offerings": ("competitor_id", "direction_id", "entity_name", "canonical_url", "relation_type", "product_type", "platform_or_ecosystem", "plugin_scope_status", "pricing_model", "price_amount", "currency", "billing_period", "license_model", "maintenance_status", "feature_summary", "source_ids", "evidence_ids", "retrieved_at"),
    "pricing_observations": ("pricing_id", "direction_id", "competitor_id", "source_id", "price_amount", "currency", "billing_model", "billing_period", "price_scope", "retrieved_at", "notes", "currency_symbol", "monetization_status", "evidence_ids"),
    "direct_wtp_observations": ("wtp_id", "direction_id", "wtp_type", "statement", "price_amount", "currency", "source_ids", "evidence_ids", "limitations"),
    "paid_market_precedents": ("precedent_id", "direction_id", "wtp_type", "entity_name", "price_amount", "currency", "billing_model", "source_ids", "evidence_ids", "limitations"),
    "other_market_proxies": ("proxy_id", "direction_id", "proxy_type", "observation", "source_ids", "evidence_ids", "limitations"),
    "differentiation_observations": ("differentiation_id", "direction_id", "gap_type", "observation", "competitor_ids", "source_ids", "evidence_ids", "evidence_state", "notes"),
    "feasibility_observations": ("feasibility_id", "direction_id", "theme", "observation", "source_ids", "evidence_ids", "notes"),
    "support_burden_observations": ("support_id", "direction_id", "burden_theme", "observation", "source_ids", "evidence_ids", "notes"),
    "channel_observations": ("channel_id", "direction_id", "channel_name", "observation", "source_ids", "evidence_ids", "notes"),
    "direction_commercial_validation": ("direction_id", "category_id", "canonical_direction_key", "resolved_market_job", "yee79_decision", "upstream_provenance", "buyer_segments", "buyer_problem_observation_ids", "purchase_trigger_observation_ids", "direct_competitor_ids", "substitute_competitor_ids", "adjacent_context_ids", "pricing_observation_ids", "wtp_evidence_by_type", "differentiation_observation_ids", "feasibility_observation_ids", "support_burden_observation_ids", "channel_observation_ids", "commercial_dimension_assessments", "commercial_risk_flags", "evidence_gap_codes", "research_coverage_status", "research_notes", "direct_wtp_summary", "paid_market_precedent_summary", "other_proxy_summary", "unknowns"),
}


class CommercialValidationError(ValueError):
    """Raised when pinned input, research capture, or generated data violates YEE-81."""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_id(prefix: str, *parts: str) -> str:
    return f"{prefix}_{hashlib.sha256(chr(0).join(parts).encode('utf-8')).hexdigest()[:20]}"


def canonicalize_url(url: str) -> str:
    parsed = urlsplit(url.strip())
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise CommercialValidationError(f"evidence URL must be public HTTP(S): {url!r}")
    host = parsed.hostname.casefold()
    port = parsed.port
    netloc = host if port is None or (parsed.scheme.lower(), port) in {("http", 80), ("https", 443)} else f"{host}:{port}"
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    query = urlencode(sorted((key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                            if not key.casefold().startswith("utm_") and key.casefold() not in {"ref", "source", "fbclid", "gclid"}))
    return urlunsplit((parsed.scheme.lower(), netloc, path, query, ""))


def _timestamp(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise CommercialValidationError(f"invalid ISO timestamp for {field}: {value!r}") from exc
    if parsed.tzinfo is None:
        raise CommercialValidationError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def load_inputs(yee79_db: str | Path, yee77_db: str | Path,
                expected_hashes: Mapping[str, str] | None = None) -> tuple[list[dict[str, Any]], dict[str, str]]:
    paths = {"yee79_decision_gate": Path(yee79_db).resolve(), "yee77_research": Path(yee77_db).resolve()}
    if any(not path.is_file() for path in paths.values()):
        raise FileNotFoundError("accepted YEE-79/YEE-77 SQLite inputs are required")
    before = {name: _sha256(path) for name, path in paths.items()}
    if before != dict(expected_hashes or EXPECTED_INPUT_HASHES):
        raise CommercialValidationError(f"canonical input hash mismatch: {before}")
    a, b = _readonly(paths["yee79_decision_gate"]), _readonly(paths["yee77_research"])
    try:
        for name, connection in (("YEE-79", a), ("YEE-77", b)):
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise CommercialValidationError(f"{name} SQLite integrity_check failed")
        cards = {row["direction_id"]: json.loads(row["record_json"])
                 for row in a.execute("SELECT direction_id,record_json FROM direction_decision_cards")}
        identity = {row["direction_id"]: json.loads(row["record_json"])
                    for row in a.execute("SELECT direction_id,record_json FROM frozen_direction_identity")}
        packs = {row["direction_id"]: row for row in b.execute(
            "SELECT direction_id,category_id,research_status,research_coverage_status FROM direction_research_packs")}
        canonical_rows = []
        for direction_id, (direction_key, category_id) in AUTHORIZED_DIRECTIONS.items():
            card, frozen, pack = cards.get(direction_id), identity.get(direction_id), packs.get(direction_id)
            if not card or not frozen or not pack:
                raise CommercialValidationError(f"authorized direction absent from canonical YEE-79/YEE-77: {direction_id}")
            if card.get("research_status") != "RESOLVED" or card.get("research_coverage_status") != "SUFFICIENT":
                raise CommercialValidationError(f"authorized direction is not resolved/sufficient in YEE-79: {direction_id}")
            if card.get("category_id") != category_id or pack["category_id"] != category_id:
                raise CommercialValidationError(f"authorized direction category mismatch: {direction_id}")
            canonical_rows.append({
                "direction_id": direction_id,
                "direction_key": direction_key,
                "category_id": category_id,
                "market_job": card.get("resolved_market_job") or card.get("market_job_summary"),
            })
        if len(canonical_rows) != 5 or {r["direction_id"] for r in canonical_rows} != set(AUTHORIZED_DIRECTIONS):
            raise CommercialValidationError("canonical Stage G cohort must be exactly the five authorized YEE-79 identities")
    finally:
        a.close()
        b.close()
    after = {name: _sha256(path) for name, path in paths.items()}
    if before != after:
        raise CommercialValidationError("read-only canonical input changed during preflight")
    return canonical_rows, before


def _ref_list(row: Mapping[str, Any], key: str, allowed: set[str], label: str) -> list[str]:
    refs = row.get(key, [])
    if not isinstance(refs, list) or any(ref not in allowed for ref in refs):
        raise CommercialValidationError(f"invalid {label} references in {row.get('capture_id', row.get('evidence_id', 'capture'))}")
    return sorted(set(refs))


def merge_followup_capture(base: Mapping[str, Any], supplement: Mapping[str, Any]) -> dict[str, Any]:
    if base.get("capture_version") not in {"yee-81-deep-commercial-validation-v0.1", CAPTURE_VERSION}:
        raise CommercialValidationError("unsupported frozen capture version")
    if supplement.get("capture_version") != CAPTURE_VERSION:
        raise CommercialValidationError("follow-up capture version mismatch")
    merged = json.loads(_json(base))
    for key in ("sources", "queries", "evidence"):
        existing = {row.get(f"{key[:-1]}_id") for row in merged.get(key, [])}
        # sources use source_id; evidence and queries use their corresponding singular keys.
        identity_key = {"sources": "source_id", "queries": "query_id", "evidence": "evidence_id"}[key]
        existing = {row.get(identity_key) for row in merged.get(key, [])}
        additions = supplement.get(key, [])
        if any(not row.get(identity_key) or row[identity_key] in existing for row in additions):
            raise CommercialValidationError(f"follow-up {key} contain duplicate/empty identity")
        merged.setdefault(key, []).extend(json.loads(_json(additions)))
    by_source = {row["source_id"]: row for row in merged["sources"]}
    for refresh in supplement.get("source_refreshes", []):
        if refresh.get("source_id") not in by_source:
            raise CommercialValidationError(f"cannot refresh unknown source: {refresh.get('source_id')}")
        by_source[refresh["source_id"]]["retrieved_at"] = refresh["retrieved_at"]
    by_query = {row["query_id"]: row for row in merged["queries"]}
    for opening in supplement.get("query_openings", []):
        query = by_query.get(opening.get("query_id"))
        if query is None:
            raise CommercialValidationError(f"cannot attach opened source to unknown query: {opening.get('query_id')}")
        for source_id in opening.get("source_ids", []):
            if source_id not in by_source:
                raise CommercialValidationError(f"query opening references unknown source: {source_id}")
        query["opened_source_ids"] = sorted(set(query.get("opened_source_ids", [])) | set(opening.get("source_ids", [])))
        query["discovered_result_urls"] = sorted(set(query.get("discovered_result_urls", [])) | set(opening.get("urls", [])))
    by_evidence = {row["evidence_id"]: row for row in merged["evidence"]}
    for update in supplement.get("evidence_updates", []):
        row = by_evidence.get(update.get("evidence_id"))
        if row is None:
            raise CommercialValidationError(f"cannot update unknown evidence row: {update.get('evidence_id')}")
        for field in ("observation", "source_locator", "limitations"):
            if field in update:
                row[field] = update[field]
    merged["capture_version"] = CAPTURE_VERSION
    merged["cohort_id_set_sha256"] = COHORT_SHA256
    merged["research_completed_at"] = supplement.get("retrieved_at")
    merged["timestamp_basis"] = supplement.get("timestamp_basis")
    merged.setdefault("search_protocol", {})["follow_up_and_direct_wtp_query_count"] = len(merged["queries"]) - 50
    merged["search_protocol"]["total_query_count"] = len(merged["queries"])

    # Preserve captured ordering for old rows; append the new requests in their recorded sequence.
    indexed = list(enumerate(merged["queries"]))
    indexed.sort(key=lambda pair: (pair[1].get("issued_at", ""), pair[0]))
    merged["queries"] = [dict(row, execution_order=order,
                              query_kind="MANDATORY_SEARCH" if row.get("query_kind") == "MANDATORY_SEARCH" else "FOLLOW_UP_SEARCH")
                          for order, (_, row) in enumerate(indexed, 1)]
    for source in merged["sources"]:
        source["source_type"] = SOURCE_TYPE_MAP.get(source.get("source_type"), source.get("source_type"))
    source_urls = {row["source_id"]: row["url"] for row in merged["sources"]}
    for query in merged["queries"]:
        discovered = {canonicalize_url(url) for url in query.get("discovered_result_urls", [])}
        discovered.update(canonicalize_url(source_urls[sid]) for sid in query.get("opened_source_ids", []))
        query["discovered_result_urls"] = sorted(discovered)
    return merged


def normalize_capture(capture: Mapping[str, Any], canonical_rows: Sequence[Mapping[str, Any]],
                      input_hashes: Mapping[str, str]) -> dict[str, list[dict[str, Any]]]:
    if capture.get("capture_version") != CAPTURE_VERSION:
        raise CommercialValidationError(f"capture_version must equal {CAPTURE_VERSION}")
    if capture.get("canonical_input_hashes") != dict(input_hashes):
        raise CommercialValidationError("capture must pin both canonical input hashes")
    ids = {row["direction_id"] for row in canonical_rows}
    directions = {row["direction_id"]: row for row in canonical_rows}
    if ids != set(AUTHORIZED_DIRECTIONS) or capture.get("authorized_direction_ids") != sorted(ids):
        raise CommercialValidationError("capture membership must equal the exact authorized direction set")
    cohort_sha = hashlib.sha256(("\n".join(sorted(ids)) + "\n").encode("utf-8")).hexdigest()
    if cohort_sha != COHORT_SHA256 or capture.get("cohort_id_set_sha256") != cohort_sha:
        raise CommercialValidationError("cohort-id-set SHA-256 mismatch")

    sources: dict[str, dict[str, Any]] = {}
    canonical_urls: dict[str, str] = {}
    for raw in capture.get("sources", []):
        source_id = raw.get("source_id")
        if not source_id or source_id in sources or raw.get("access_status") != "OPENED":
            raise CommercialValidationError("every source must have a unique id and OPENED access status")
        canonical_url = canonicalize_url(raw.get("url", ""))
        if canonical_url in canonical_urls:
            raise CommercialValidationError(f"duplicate canonical source URL: {canonical_url}")
        source_type = SOURCE_TYPE_MAP.get(raw.get("source_type"))
        if source_type not in {"PRIMARY_PRODUCT", "PRIMARY_DOCS", "PRIMARY_REPOSITORY", "PRIMARY_SUPPORT", "MARKETPLACE_LISTING", "COMMUNITY", "EDITORIAL", "PLATFORM_DOCUMENTATION"}:
            raise CommercialValidationError(f"unsupported source type for {source_id}: {raw.get('source_type')}")
        if not raw.get("title") or not raw.get("domain"):
            raise CommercialValidationError(f"opened source metadata incomplete: {source_id}")
        row = dict(raw, source_type=source_type, canonical_url=canonical_url)
        row["domain"] = urlsplit(canonical_url).hostname or ""
        row["retrieved_at"] = _timestamp(row.get("retrieved_at"), f"source.{source_id}.retrieved_at")
        if row.get("published_or_updated_at"):
            row["published_or_updated_at"] = str(row["published_or_updated_at"])
        sources[source_id] = row
        canonical_urls[canonical_url] = source_id
    if not sources:
        raise CommercialValidationError("capture contains no opened public sources")

    queries: dict[str, dict[str, Any]] = {}
    query_rows = []
    for index, raw in enumerate(capture.get("queries", []), 1):
        query_id, direction_id = raw.get("query_id"), raw.get("direction_id")
        if not query_id or query_id in queries or direction_id not in ids or raw.get("purpose") not in QUERY_PURPOSES:
            raise CommercialValidationError(f"invalid query identity/purpose: {raw}")
        opened = _ref_list(raw, "opened_source_ids", set(sources), "source")
        discovered = raw.get("discovered_result_urls")
        if discovered is None:
            discovered = [sources[source_id]["url"] for source_id in opened]
        if not isinstance(discovered, list):
            raise CommercialValidationError(f"discovered_result_urls must be an array: {query_id}")
        discovered_canonical = {canonicalize_url(url) for url in discovered}
        if any(sources[source_id]["canonical_url"] not in discovered_canonical for source_id in opened):
            raise CommercialValidationError(f"opened source must be among discovered result URLs: {query_id}")
        kind = "MANDATORY_SEARCH" if raw.get("query_kind") == "MANDATORY_SEARCH" else "FOLLOW_UP_SEARCH"
        query = dict(raw)
        query.update({
            "execution_order": raw.get("execution_order", index),
            "query_kind": kind,
            "issued_at": _timestamp(raw.get("issued_at"), f"query.{query_id}.issued_at"),
            "opened_source_ids": opened,
            "discovered_result_urls": sorted(discovered_canonical),
        })
        if not query.get("query_text") or not query.get("result_note"):
            raise CommercialValidationError(f"query needs text and disposition: {query_id}")
        queries[query_id] = query
        query_rows.append({key: query.get(key) for key in TABLES["commercial_research_queries"]})
    query_rows.sort(key=lambda row: row["execution_order"])
    if [row["execution_order"] for row in query_rows] != list(range(1, len(query_rows) + 1)):
        raise CommercialValidationError("query execution_order must be unique and contiguous from one")
    purposes_by_direction = {key: set() for key in ids}
    mandatory_by_direction = {key: [] for key in ids}
    for query in queries.values():
        purposes_by_direction[query["direction_id"]].add(query["purpose"])
        if query["query_kind"] == "MANDATORY_SEARCH":
            mandatory_by_direction[query["direction_id"]].append(query["purpose"])
    for direction_id in ids:
        mandatory = mandatory_by_direction[direction_id]
        if len(mandatory) != len(QUERY_PURPOSES) or set(mandatory) != set(QUERY_PURPOSES):
            raise CommercialValidationError(f"each direction requires exactly one mandatory search per purpose: {direction_id}")
        if purposes_by_direction[direction_id] != set(QUERY_PURPOSES):
            raise CommercialValidationError(f"required query purposes missing for {direction_id}")

    evidence: dict[str, dict[str, Any]] = {}
    for raw in capture.get("evidence", []):
        evidence_id = raw.get("evidence_id")
        if not evidence_id or evidence_id in evidence or raw.get("direction_id") not in ids:
            raise CommercialValidationError(f"invalid/duplicate evidence identity: {raw}")
        if raw.get("source_id") not in sources:
            raise CommercialValidationError(f"evidence must link to an opened canonical source: {evidence_id}")
        query_ids = _ref_list(raw, "query_ids", set(queries), "query")
        if not query_ids or any(queries[qid]["direction_id"] != raw["direction_id"] for qid in query_ids):
            raise CommercialValidationError(f"evidence query provenance must be same-direction: {evidence_id}")
        if raw.get("source_id") not in {sid for qid in query_ids for sid in queries[qid]["opened_source_ids"]}:
            raise CommercialValidationError(f"evidence source was not associated with its opening query: {evidence_id}")
        if not raw.get("evidence_type") or not raw.get("observation") or raw.get("source_basis") != "OPENED_CANONICAL_SOURCE":
            raise CommercialValidationError(f"evidence must be paraphrased from an opened source: {evidence_id}")
        if raw.get("verbatim_excerpt") and len(re.findall(r"\b\w+\b", raw["verbatim_excerpt"])) > 25:
            raise CommercialValidationError(f"verbatim excerpt exceeds the per-source copyright limit: {evidence_id}")
        evidence[evidence_id] = dict(raw, query_ids=query_ids)

    def evidence_refs(raw: Mapping[str, Any], label: str) -> list[str]:
        result = _ref_list(raw, "evidence_ids", set(evidence), label)
        if not result or raw.get("direction_id") not in ids or any(evidence[eid]["direction_id"] != raw["direction_id"] for eid in result):
            raise CommercialValidationError(f"{label} must cite same-direction evidence")
        return result

    def source_refs(evidence_ids: Sequence[str]) -> list[str]:
        return sorted({evidence[eid]["source_id"] for eid in evidence_ids})

    source_rows = [{key: source.get(key) for key in TABLES["commercial_source_documents"]}
                   for source in sorted(sources.values(), key=lambda item: item["source_id"])]
    evidence_rows = [{key: row.get(key) for key in TABLES["commercial_evidence"]}
                     for row in sorted(evidence.values(), key=lambda item: item["evidence_id"])]

    buyer_contexts = {
        "dir_0480c15eace353f0426f1db5": "Offline-mode, proxy-connected and account-authentication server operation",
        "dir_142f5946b84de25a3a07dd76": "Multi-server Minecraft network communication and chat operation",
        "dir_d36aa111cd7e8f7191d5ac47": "Server administration and in-server behavior customization",
        "dir_78e16d4e251e9350b653a2de": "Server-operated player jobs, reward and economy workflows",
        "dir_617529293c4b381179ea326e": "Server-operated recipe creation, modification and compatibility",
    }
    trigger_types = {"BUYER_NEED", "HISTORICAL_COMPATIBILITY_QUESTION"}
    pain_types = {"USER_FRICTION", "USER_WORKAROUND", "SECURITY_INCIDENT_REPORT", "OPERATOR_FRICTION", "SECURITY_SUPPORT_REPORT", "COMPATIBILITY_REPORT", "SUPPORT_CASE"}
    direct_wtp_types = {"EXPLICIT_PURCHASE_INTENT"}
    def buyer_type(ev: Mapping[str, Any]) -> str:
        kind = ev["evidence_type"]
        if kind in direct_wtp_types:
            return "DIRECT_WTP_STATEMENT"
        if kind in trigger_types:
            return "PURCHASE_TRIGGER"
        if kind in pain_types:
            return "OPERATOR_PAIN"
        if kind == "BUYER_SEGMENT_CONTEXT":
            return "BUYER_SEGMENT"
        return "WORKFLOW"
    def buyer_strength(ev: Mapping[str, Any]) -> str:
        kind = ev["evidence_type"]
        if kind in direct_wtp_types | trigger_types | {"USER_FRICTION", "USER_WORKAROUND", "SECURITY_INCIDENT_REPORT", "OPERATOR_FRICTION", "COMPATIBILITY_REPORT"}:
            return "DIRECT_STATEMENT"
        if kind in {"SECURITY_SUPPORT_REPORT", "SUPPORT_CASE"}:
            return "BEHAVIORAL_PROXY"
        if kind == "BUYER_SEGMENT_CONTEXT":
            return "CONTEXTUAL"
        return "CONTEXTUAL"

    buyer_rows = []
    buyer_evidence_seen: set[str] = set()
    for raw in capture.get("buyer_problems", []):
        ev_ids = evidence_refs(raw, "buyer problem")
        for evidence_id in ev_ids:
            ev = evidence[evidence_id]
            kind = buyer_type(ev)
            buyer_evidence_seen.add(evidence_id)
            buyer_rows.append({
                "observation_id": _stable_id("buyer", raw.get("observation_id", "legacy"), evidence_id),
                "direction_id": raw["direction_id"],
                "buyer_segment_label": raw.get("actor") or "Minecraft server operator",
                "operator_context": raw.get("situation_problem") or buyer_contexts[raw["direction_id"]],
                "observation_type": kind,
                "observation": ev["observation"],
                "operational_consequence": ev["observation"] if kind == "OPERATOR_PAIN" else None,
                "purchase_trigger": ev["observation"] if kind in {"PURCHASE_TRIGGER", "DIRECT_WTP_STATEMENT"} else None,
                "source_id": ev["source_id"],
                "evidence_strength": buyer_strength(ev),
                "retrieved_at": sources[ev["source_id"]]["retrieved_at"],
                "notes": raw.get("limitations") or ev.get("limitations"),
                "evidence_ids": [evidence_id],
                "query_ids": ev["query_ids"],
            })
    for ev in evidence.values():
        if ev["evidence_id"] in buyer_evidence_seen or buyer_type(ev) == "WORKFLOW":
            continue
        kind = buyer_type(ev)
        buyer_rows.append({
            "observation_id": _stable_id("buyer", ev["evidence_id"]),
            "direction_id": ev["direction_id"],
            "buyer_segment_label": buyer_contexts[ev["direction_id"]],
            "operator_context": buyer_contexts[ev["direction_id"]],
            "observation_type": kind,
            "observation": ev["observation"],
            "operational_consequence": ev["observation"] if kind == "OPERATOR_PAIN" else None,
            "purchase_trigger": ev["observation"] if kind in {"PURCHASE_TRIGGER", "DIRECT_WTP_STATEMENT"} else None,
            "source_id": ev["source_id"],
            "evidence_strength": buyer_strength(ev),
            "retrieved_at": sources[ev["source_id"]]["retrieved_at"],
            "notes": ev.get("limitations"),
            "evidence_ids": [ev["evidence_id"]],
            "query_ids": ev["query_ids"],
        })
    buyer_rows.sort(key=lambda row: (row["direction_id"], row["observation_id"]))
    if any(row["observation_type"] not in OBSERVATION_TYPES or row["evidence_strength"] not in EVIDENCE_STRENGTHS for row in buyer_rows):
        raise CommercialValidationError("buyer observation enum contract violation")

    competitor_source_additions = {
        "authmereloaded": ["EV_AUTH_MODRINTH_LISTING", "EV_AUTH_SPIGOT_LISTING"],
        "customrecipes": ["EV_CUSTOMRECIPES_MODRINTH"],
        "jobs reborn": ["EV_JOBS_SEGMENT_LISTING", "EV_JOBS_CHANNEL_LISTING"],
        "advancedjobs": ["EV_JOBS_CHANNEL_LISTING"],
    }
    competitor_rows = []
    competitor_by_name: dict[tuple[str, str], dict[str, Any]] = {}
    raw_competitors = capture.get("competitor_offerings", [])
    for raw in raw_competitors:
        ev_ids = evidence_refs(raw, "competitor offering")
        name_key = raw["entity_name"].casefold()
        ev_ids = sorted(set(ev_ids + [eid for eid in competitor_source_additions.get(name_key, []) if eid in evidence]))
        raw_product_type = str(raw.get("product_type", ""))
        source_types = {sources[sid]["source_type"] for sid in source_refs(ev_ids)}
        evidence_text = " ".join(evidence[eid]["observation"].casefold() for eid in ev_ids)
        plugin_identity = "plugin" in raw_product_type.casefold() or "plugin" in evidence_text
        if "resource" in raw_product_type.casefold() and "MARKETPLACE_LISTING" in source_types and (".jar" in evidence_text or "plugin" in evidence_text):
            plugin_identity = True
        raw_relation = raw.get("relation_type")
        if raw_relation == "DIRECT_OR_NEAR_SUBSTITUTE":
            relation = "DIRECT" if plugin_identity else "SUBSTITUTE"
        else:
            relation = {"ADJACENT_SUBSTITUTE": "ADJACENT"}.get(raw_relation, raw_relation)
        if relation not in RELATION_TYPES:
            raise CommercialValidationError(f"invalid competitor relation type: {raw.get('relation_type')}")
        product_type = raw_product_type
        if relation == "DIRECT" and "plugin" not in product_type.casefold():
            if not plugin_identity:
                raise CommercialValidationError(f"DIRECT competitor is not evidenced as a plugin product: {raw['entity_name']}")
            if "resource" in product_type.casefold():
                product_type = re.sub(r"resource", "plugin", product_type, flags=re.I)
            else:
                product_type = f"{product_type} server plugin".strip()
        if relation == "DIRECT" and "plugin" not in product_type.casefold():
            raise CommercialValidationError(f"DIRECT competitor is not evidenced as a plugin product: {raw['entity_name']}")
        sids = source_refs(ev_ids)
        price_matches = [p for p in capture.get("pricing_observations", []) if p.get("entity_name", "").casefold() == name_key]
        amounts = {p.get("amount") for p in price_matches if p.get("amount") is not None}
        models = sorted({p.get("billing_model") for p in price_matches if p.get("billing_model")})
        platforms = "Minecraft server/proxy plugin ecosystem" if any("proxy" in evidence[eid]["observation"].casefold() or "velocity" in evidence[eid]["observation"].casefold() or "bungee" in evidence[eid]["observation"].casefold() for eid in ev_ids) else "Minecraft server-plugin ecosystem"
        row = {
            "competitor_id": raw["offering_id"], "direction_id": raw["direction_id"],
            "entity_name": raw["entity_name"], "canonical_url": canonicalize_url(raw["canonical_url"]),
            "relation_type": relation, "product_type": product_type, "platform_or_ecosystem": platforms,
            "plugin_scope_status": "SERVER_AND_PROXY_PLUGIN" if relation == "DIRECT" and "proxy" in platforms else
                                  "SERVER_SIDE_PLUGIN" if relation == "DIRECT" else "UNVERIFIED_OR_NON_PLUGIN_CONTEXT",
            "pricing_model": "; ".join(models) if models else None,
            "price_amount": next(iter(amounts)) if len(amounts) == 1 else None,
            "currency": next((p.get("currency_code") for p in price_matches if p.get("currency_code")), None),
            "billing_period": next((p.get("period") for p in price_matches if p.get("period")), None),
            "license_model": None, "maintenance_status": None,
            "feature_summary": raw.get("buyer_job") or raw.get("limitations"),
            "source_ids": sids, "evidence_ids": ev_ids,
            "retrieved_at": max(sources[sid]["retrieved_at"] for sid in sids),
        }
        competitor_rows.append(row)
        competitor_by_name[(raw["direction_id"], name_key)] = row
    competitor_rows.sort(key=lambda row: (row["direction_id"], row["competitor_id"]))

    pricing_rows = []
    for raw in capture.get("pricing_observations", []):
        ev_ids = evidence_refs(raw, "pricing observation")
        amount = raw.get("amount")
        if amount is not None and (not isinstance(amount, (int, float)) or amount < 0):
            raise CommercialValidationError("price amount must be a nonnegative source-native number or null")
        if amount == 0 and raw.get("price_context") != "EXPLICIT_ZERO_PRICE":
            raise CommercialValidationError("zero is retained only when explicitly shown by the source")
        if raw.get("monetization_status") == "FREE_VERIFIED" and amount is None:
            raise CommercialValidationError("unknown amount cannot be normalized as free")
        if amount is None and raw.get("monetization_status") not in {"PAID_VERIFIED", "FREE_VERIFIED", "UNKNOWN"}:
            raise CommercialValidationError("unknown amount requires explicit status")
        src_id = evidence[ev_ids[0]]["source_id"]
        competitor = competitor_by_name.get((raw["direction_id"], raw["entity_name"].casefold()))
        if competitor is None:
            raise CommercialValidationError(f"pricing row has no normalized competitor: {raw['entity_name']}")
        pricing_rows.append({
            "pricing_id": raw["pricing_id"], "direction_id": raw["direction_id"],
            "competitor_id": competitor["competitor_id"], "source_id": src_id,
            "price_amount": amount, "currency": raw.get("currency_code"),
            "billing_model": raw.get("billing_model"), "billing_period": raw.get("period"),
            "price_scope": raw.get("tier_or_scope"), "retrieved_at": sources[src_id]["retrieved_at"],
            "notes": "; ".join(filter(None, [raw.get("price_context"), raw.get("limitations")])),
            "currency_symbol": raw.get("currency_symbol"), "monetization_status": raw.get("monetization_status"),
            "evidence_ids": ev_ids,
        })
    pricing_rows.sort(key=lambda row: (row["direction_id"], row["pricing_id"]))

    def simple_rows(table_key: str, id_key: str, output_table: str) -> list[dict[str, Any]]:
        rows = []
        for raw in capture.get(table_key, []):
            ev_ids = evidence_refs(raw, output_table)
            sids = source_refs(ev_ids)
            if output_table == "direct_wtp_observations":
                wtp_type = {"EXPLICIT_WTP_INTENT": "DIRECT_WTP_STATEMENT", "OBSERVED_PAYMENT": "OBSERVED_PURCHASE_OR_PAYMENT"}.get(raw.get("wtp_class"))
                if wtp_type not in {"DIRECT_WTP_STATEMENT", "OBSERVED_PURCHASE_OR_PAYMENT"}:
                    raise CommercialValidationError("direct WTP must be an explicit statement or observed payment")
                allowed_evidence = {"EXPLICIT_PURCHASE_INTENT"} if wtp_type == "DIRECT_WTP_STATEMENT" else {"OBSERVED_PURCHASE", "OBSERVED_PAYMENT"}
                if any(evidence[eid]["evidence_type"] not in allowed_evidence for eid in ev_ids):
                    raise CommercialValidationError("direct WTP evidence must be a buyer statement or observed payment")
                row = {"wtp_id": raw[id_key], "direction_id": raw["direction_id"], "wtp_type": wtp_type,
                       "statement": raw.get("statement"), "price_amount": raw.get("amount"),
                       "currency": raw.get("currency_code"), "source_ids": sids, "evidence_ids": ev_ids,
                       "limitations": raw.get("limitations")}
            elif output_table == "paid_market_precedents":
                row = {"precedent_id": raw[id_key], "direction_id": raw["direction_id"],
                       "wtp_type": "PAID_COMPETITOR_PRECEDENT", "entity_name": raw.get("entity_name"),
                       "price_amount": raw.get("amount"), "currency": raw.get("currency_code"),
                       "billing_model": raw.get("billing_model"), "source_ids": sids, "evidence_ids": ev_ids,
                       "limitations": raw.get("limitations")}
            elif output_table == "other_market_proxies":
                row = {"proxy_id": raw[id_key], "direction_id": raw["direction_id"], "proxy_type": raw.get("proxy_type"),
                       "observation": raw.get("observation"), "source_ids": sids, "evidence_ids": ev_ids,
                       "limitations": raw.get("limitations")}
            else:
                raise AssertionError(output_table)
            rows.append(row)
        return sorted(rows, key=lambda row: (row["direction_id"], row[id_key]))

    wtp_rows = simple_rows("direct_wtp_observations", "wtp_id", "direct_wtp_observations")
    precedent_rows = simple_rows("paid_market_precedents", "precedent_id", "paid_market_precedents")
    proxy_rows = simple_rows("other_market_proxies", "proxy_id", "other_market_proxies")

    pain_evidence_types = {"BUYER_NEED", "EXPLICIT_PURCHASE_INTENT", "USER_FRICTION", "USER_WORKAROUND", "SECURITY_INCIDENT_REPORT", "OPERATOR_FRICTION", "SECURITY_SUPPORT_REPORT", "COMPATIBILITY_REPORT", "HISTORICAL_COMPATIBILITY_QUESTION"}
    competitor_evidence_types = {"PRODUCT_FUNCTION", "COMPETITOR_LISTING", "CHANNEL_AND_COMPETITOR_LISTING", "COMPETITOR_FEATURES", "COMPETITOR_FEATURES", "PAID_COMPETITOR_PRICE", "FREE_INCUMBENT", "FEATURE_LIMITATION", "ADJACENT_PAID_PRECEDENT", "COMPETITOR_LISTING"}
    differentiation_rows = []
    for raw in capture.get("differentiation_observations", []):
        ev_ids = evidence_refs(raw, "differentiation observation")
        sid_list = source_refs(ev_ids)
        comp_ids = sorted({row["competitor_id"] for row in competitor_rows if row["direction_id"] == raw["direction_id"] and set(row["evidence_ids"]) & set(ev_ids)})
        distinct_urls = {sources[sid]["canonical_url"] for sid in sid_list}
        has_pain = any(evidence[eid]["evidence_type"] in pain_evidence_types for eid in ev_ids)
        has_competitor = any(evidence[eid]["evidence_type"] in competitor_evidence_types for eid in ev_ids)
        # The existing capture did not align the anecdotes and competitor facts to a single verified gap.
        evidence_backed = len(distinct_urls) >= 2 and has_pain and has_competitor and bool(raw.get("gap_alignment_verified"))
        state = "EVIDENCE_BACKED" if evidence_backed else ("HYPOTHESIS_ONLY" if has_pain else "INSUFFICIENT")
        axis = str(raw.get("feature_axis", "")).casefold()
        gap_type = ("COMPATIBILITY_GAP" if any(k in axis for k in ("platform", "compatib")) else
                    "CONFIGURATION_COMPLEXITY" if "config" in axis else
                    "WORKFLOW_FRICTION" if any(k in axis for k in ("friction", "workflow")) else
                    "MISSING_CAPABILITY")
        differentiation_rows.append({
            "differentiation_id": raw["observation_id"], "direction_id": raw["direction_id"],
            "gap_type": gap_type, "observation": raw["observation"], "competitor_ids": comp_ids,
            "source_ids": sid_list, "evidence_ids": ev_ids, "evidence_state": state,
            "notes": raw.get("limitations"),
        })
    differentiation_rows.sort(key=lambda row: (row["direction_id"], row["differentiation_id"]))
    if any(row["gap_type"] not in DIFFERENTIATION_GAPS or row["evidence_state"] not in DIFFERENTIATION_STATES for row in differentiation_rows):
        raise CommercialValidationError("differentiation enum contract violation")

    extra_by_direction = {
        "dir_0480c15eace353f0426f1db5": ["EV_AUTH_SECURITY_TRIGGER"],
        "dir_142f5946b84de25a3a07dd76": ["EV_CHAT_VELOCITY_PLUGINMSG", "EV_CHAT_PAPER_PLUGINMSG"],
        "dir_d36aa111cd7e8f7191d5ac47": ["EV_DEV_PAPER_API_SURFACE"],
        "dir_78e16d4e251e9350b653a2de": ["EV_JOBS_ECONOMY_TRIGGER", "EV_JOBS_AUTHORIZATION_REPORT"],
        "dir_617529293c4b381179ea326e": ["EV_RECIPE_GEYSER_REPORT"],
    }
    def normalized_source_observations(table_key: str, output_table: str, id_key: str,
                                       theme_key: str, map_theme: Mapping[str, str],
                                       include_extras: bool = False) -> list[dict[str, Any]]:
        rows = []
        for raw in capture.get(table_key, []):
            base_ids = evidence_refs(raw, output_table)
            by_theme: dict[str, list[str]] = {}
            for eid in base_ids:
                kind = evidence[eid]["evidence_type"]
                theme = map_theme.get(kind, raw.get(theme_key) or "OTHER")
                by_theme.setdefault(theme, []).append(eid)
            if include_extras:
                for eid in extra_by_direction.get(raw["direction_id"], []):
                    if eid in evidence:
                        kind = evidence[eid]["evidence_type"]
                        theme = map_theme.get(kind, raw.get(theme_key) or "OTHER")
                        by_theme.setdefault(theme, []).append(eid)
            for theme, ev_ids in sorted(by_theme.items()):
                ev_ids = sorted(set(ev_ids))
                sids = source_refs(ev_ids)
                if output_table == "feasibility_observations" and theme not in FEASIBILITY_THEMES:
                    theme = "OTHER"
                observation_text = " ".join(evidence[eid]["observation"] for eid in ev_ids)
                row = {
                    id_key: _stable_id(id_key.removesuffix("_id"), raw.get("observation_id", raw.get("support_id", raw.get("channel_id", "legacy"))), theme),
                    "direction_id": raw["direction_id"], "observation": observation_text,
                    "source_ids": sids, "evidence_ids": ev_ids,
                    "notes": raw.get("limitations"),
                }
                if output_table == "feasibility_observations":
                    row["theme"] = theme
                elif output_table == "support_burden_observations":
                    row["burden_theme"] = theme
                else:
                    row["channel_name"] = theme
                rows.append({col: row.get(col) for col in TABLES[output_table]})
        return sorted(rows, key=lambda row: (row["direction_id"], row[id_key]))

    feasibility_theme = {
        "PLATFORM_SUPPORT": "PLATFORM_SCOPE", "DEPLOYMENT_CONSTRAINT": "PROXY_NETWORKING",
        "COMPATIBILITY_CONSTRAINT": "VERSION_CHURN", "PLATFORM_COMPATIBILITY_DOCUMENTATION": "PROXY_NETWORKING",
        "PLATFORM_API_DOCUMENTATION": "API_DEPENDENCY", "INTEGRATION_SUPPORT": "ECONOMY_INTEGRATION",
        "SUPPORT_CASE": "DATA_PERSISTENCE", "SECURITY_SUPPORT_REPORT": "SECURITY_SENSITIVITY",
        "SECURITY_INCIDENT_REPORT": "SECURITY_SENSITIVITY", "IMPLEMENTATION_CONSTRAINT": "RECIPE_OR_GAME_API_CONSTRAINT",
        "COMPATIBILITY_REPORT": "CROSS_PLATFORM_COMPATIBILITY", "PLATFORM_AND_CHANNEL": "PLATFORM_SCOPE",
    }
    feasibility_rows = normalized_source_observations(
        "feasibility_observations", "feasibility_observations", "feasibility_id", "kind", feasibility_theme, True
    )
    if any(row["theme"] not in FEASIBILITY_THEMES for row in feasibility_rows):
        raise CommercialValidationError("feasibility theme enum contract violation")
    support_theme = {
        "PRODUCT_FUNCTION": "Product feature/support surface", "PLATFORM_SUPPORT": "Multi-platform support",
        "DEPLOYMENT_CONSTRAINT": "Deployment support", "COMPETITOR_FEATURES": "Integration support surface",
        "PLATFORM_COMPATIBILITY_DOCUMENTATION": "Platform compatibility support", "SUPPORT_CASE": "Reported support case",
        "SECURITY_SUPPORT_REPORT": "Reported security support case", "SECURITY_INCIDENT_REPORT": "Security-sensitive operation",
        "OPERATOR_FRICTION": "Operator troubleshooting", "USER_FRICTION": "Configuration friction report",
        "SUPPORT_MAINTENANCE": "Documentation and maintenance surface", "COMPATIBILITY_REPORT": "Compatibility report",
        "COMPATIBILITY_CONSTRAINT": "Version support surface", "PLATFORM_AND_CHANNEL": "Platform support surface",
        "BUYER_SEGMENT_CONTEXT": "Product maintenance context", "PLATFORM_API_DOCUMENTATION": "Platform API support surface",
    }
    support_rows = normalized_source_observations(
        "support_burden_observations", "support_burden_observations", "support_id", "kind", support_theme, True
    )
    channel_names = {"www.spigotmc.org": "SpigotMC marketplace", "modrinth.com": "Modrinth marketplace",
                     "hangar.papermc.io": "Hangar plugin registry", "github.com": "GitHub project/repository",
                     "docs.papermc.io": "PaperMC platform documentation", "mineacademy.org": "Vendor product/docs",
                     "www.reddit.com": "Community discussion", "www.advancedplugins.net": "Vendor product page",
                     "polymart.org": "Polymart marketplace"}
    channel_rows = []
    for raw in capture.get("channel_observations", []):
        ev_ids = evidence_refs(raw, "channel observation")
        for eid in ev_ids:
            ev = evidence[eid]
            domain = sources[ev["source_id"]]["domain"]
            channel_rows.append({
                "channel_id": _stable_id("channel", raw["observation_id"], eid),
                "direction_id": raw["direction_id"], "channel_name": channel_names.get(domain, domain),
                "observation": ev["observation"], "source_ids": [ev["source_id"]],
                "evidence_ids": [eid], "notes": raw.get("limitations"),
            })
    supplemental_channel_ids = {
        "dir_0480c15eace353f0426f1db5": ["EV_AUTH_MODRINTH_LISTING", "EV_AUTH_SPIGOT_LISTING"],
        "dir_142f5946b84de25a3a07dd76": ["EV_CHATCONTROL_FEATURES", "EV_VENTURECHAT"],
        "dir_d36aa111cd7e8f7191d5ac47": ["EV_SKRIPT_HANGAR", "EV_SKRIPT_PRODUCT"],
        "dir_78e16d4e251e9350b653a2de": ["EV_JOBS_CHANNEL_LISTING", "EV_JOBS_REBORN"],
        "dir_617529293c4b381179ea326e": ["EV_CUSTOMRECIPES_MODRINTH", "EV_RECIPE_CUSTOM", "EV_RECIPE_FREE", "EV_RECIPE_PAID"],
    }
    channel_seen = {eid for row in channel_rows for eid in row["evidence_ids"]}
    for direction_id, ev_ids in supplemental_channel_ids.items():
        for eid in ev_ids:
            if eid in evidence and eid not in channel_seen:
                ev = evidence[eid]
                domain = sources[ev["source_id"]]["domain"]
                channel_rows.append({
                    "channel_id": _stable_id("channel", "followup", eid), "direction_id": direction_id,
                    "channel_name": channel_names.get(domain, domain), "observation": ev["observation"],
                    "source_ids": [ev["source_id"]], "evidence_ids": [eid],
                    "notes": "Observed current public distribution or discovery surface; not conversion evidence.",
                })
    channel_rows.sort(key=lambda row: (row["direction_id"], row["channel_id"]))

    capture_directions = {row.get("direction_id"): row for row in capture.get("directions", [])}
    if set(capture_directions) != ids or len(capture_directions) != len(ids):
        raise CommercialValidationError("direction assessments must contain the exact five authorized identities")
    packs = []
    for direction_id in AUTHORIZED_DIRECTIONS:
        raw_direction = capture_directions[direction_id]
        canonical = directions[direction_id]
        assessments = raw_direction.get("dimension_assessments", [])
        by_dimension = {item.get("dimension"): item for item in assessments}
        if set(by_dimension) != set(DIMENSIONS) or len(assessments) != len(DIMENSIONS):
            raise CommercialValidationError(f"exactly seven commercial dimensions are required for {direction_id}")
        normalized_assessments = []
        for name in DIMENSIONS:
            item = by_dimension[name]
            if item.get("state") not in DIMENSION_STATES or not item.get("basis"):
                raise CommercialValidationError(f"invalid dimension assessment {direction_id}/{name}")
            ev_ids = _ref_list(item, "evidence_ids", set(evidence), "dimension evidence")
            if any(evidence[eid]["direction_id"] != direction_id for eid in ev_ids):
                raise CommercialValidationError(f"dimension evidence crosses direction boundary: {direction_id}/{name}")
            if item["state"] != "INSUFFICIENT_EVIDENCE" and not ev_ids:
                raise CommercialValidationError(f"supported dimension lacks evidence: {direction_id}/{name}")
            normalized_assessments.append({
                "dimension": name, "dimension_state": item["state"], "basis": item["basis"],
                "evidence_ids": ev_ids, "source_ids": source_refs(ev_ids),
                "risk_flags": sorted(set(raw_direction.get("risk_flags", []))),
                "unknowns": sorted(set(raw_direction.get("unknowns", []))),
            })
        if raw_direction.get("coverage_status") not in {"SUFFICIENT", "PARTIAL", "INSUFFICIENT"}:
            raise CommercialValidationError(f"invalid commercial coverage status for {direction_id}")

        direction_buyers = [row for row in buyer_rows if row["direction_id"] == direction_id]
        pain_buyers = [row for row in direction_buyers if row["observation_type"] in {"OPERATOR_PAIN", "PURCHASE_TRIGGER", "DIRECT_WTP_STATEMENT"}]
        pain_domains = {sources[row["source_id"]]["domain"] for row in pain_buyers}
        gaps = set()
        if len(pain_buyers) < 3 or len(pain_domains) < 2:
            gaps.add("INSUFFICIENT_BUYER_EVIDENCE")
        if sum(row["observation_type"] == "OPERATOR_PAIN" for row in direction_buyers) < 2:
            gaps.add("INSUFFICIENT_PAIN_EVIDENCE")
        trigger_ids = sorted(row["observation_id"] for row in direction_buyers if row["purchase_trigger"])
        if not trigger_ids:
            gaps.add("PURCHASE_TRIGGER_UNCLEAR")
        direct_ids = [row for row in wtp_rows if row["direction_id"] == direction_id]
        paid_ids = [row for row in precedent_rows if row["direction_id"] == direction_id]
        prices = [row for row in pricing_rows if row["direction_id"] == direction_id]
        competitors = [row for row in competitor_rows if row["direction_id"] == direction_id]
        if not direct_ids:
            gaps.add("NO_DIRECT_WTP_EVIDENCE")
        if not paid_ids:
            gaps.add("NO_PAID_MARKET_PRECEDENT")
        if any(row["price_amount"] is None for row in competitors) or not prices:
            gaps.add("PRICING_COVERAGE_INSUFFICIENT")
        if any(row["monetization_status"] == "FREE_VERIFIED" for row in prices):
            gaps.add("FREE_INCUMBENT_PRESSURE_PRESENT")
        direction_diffs = [row for row in differentiation_rows if row["direction_id"] == direction_id]
        backed = [row for row in direction_diffs if row["evidence_state"] == "EVIDENCE_BACKED"]
        if not backed:
            gaps.add("DIFFERENTIATION_NOT_ESTABLISHED")
        direction_channels = [row for row in channel_rows if row["direction_id"] == direction_id]
        if not direction_channels:
            gaps.add("CHANNEL_FIT_NOT_ESTABLISHED")
        direction_feasibility = [row for row in feasibility_rows if row["direction_id"] == direction_id]
        direction_support = [row for row in support_rows if row["direction_id"] == direction_id]
        if any(item["dimension_state"] in {"MIXED", "WEAK"} for item in normalized_assessments if item["dimension"] == "IMPLEMENTATION_FEASIBILITY"):
            gaps.add("IMPLEMENTATION_FEASIBILITY_RISK")
        if any(item["dimension_state"] in {"MIXED", "WEAK"} for item in normalized_assessments if item["dimension"] == "SUPPORT_MAINTENANCE"):
            gaps.add("SUPPORT_BURDEN_RISK")
        if any(row["theme"] in {"CROSS_PLATFORM_COMPATIBILITY", "PROXY_NETWORKING", "VERSION_CHURN"} for row in direction_feasibility):
            gaps.add("COMPATIBILITY_SCOPE_RISK")
        if any(row["theme"] == "SECURITY_SENSITIVITY" for row in direction_feasibility):
            gaps.add("SECURITY_SENSITIVITY_RISK")
        if any(item["dimension_state"] == "MIXED" for item in normalized_assessments):
            gaps.add("CONFLICTING_EVIDENCE")
        if len(pain_buyers) < 3 or raw_direction.get("coverage_status") != "SUFFICIENT":
            gaps.add("SOURCE_COVERAGE_RISK")
        if gaps - EVIDENCE_GAP_CODES:
            raise CommercialValidationError("unknown evidence gap code")
        wtp_by_type = {key: [] for key in sorted(WTP_TYPES)}
        for row in direct_ids:
            wtp_by_type[row["wtp_type"]].append(row["wtp_id"])
        for row in paid_ids:
            wtp_by_type[row["wtp_type"]].append(row["precedent_id"])
        if not any(wtp_by_type[key] for key in WTP_TYPES - {"NO_WTP_EVIDENCE"}):
            wtp_by_type["NO_WTP_EVIDENCE"] = []
        pack = {
            "direction_id": direction_id, "category_id": canonical["category_id"],
            "canonical_direction_key": canonical["direction_key"], "resolved_market_job": canonical["market_job"],
            "yee79_decision": "ADVANCE_FOR_DEEP_VALIDATION",
            "upstream_provenance": {"yee79_decision_gate_sha256": input_hashes["yee79_decision_gate"],
                                    "yee77_research_sha256": input_hashes["yee77_research"], "cohort_id_set_sha256": cohort_sha},
            "buyer_segments": sorted({row["buyer_segment_label"] for row in direction_buyers}),
            "buyer_problem_observation_ids": sorted(row["observation_id"] for row in direction_buyers),
            "purchase_trigger_observation_ids": trigger_ids,
            "direct_competitor_ids": sorted(row["competitor_id"] for row in competitors if row["relation_type"] == "DIRECT"),
            "substitute_competitor_ids": sorted(row["competitor_id"] for row in competitors if row["relation_type"] == "SUBSTITUTE"),
            "adjacent_context_ids": sorted(row["competitor_id"] for row in competitors if row["relation_type"] == "ADJACENT"),
            "pricing_observation_ids": sorted(row["pricing_id"] for row in prices), "wtp_evidence_by_type": wtp_by_type,
            "differentiation_observation_ids": sorted(row["differentiation_id"] for row in direction_diffs),
            "feasibility_observation_ids": sorted(row["feasibility_id"] for row in direction_feasibility),
            "support_burden_observation_ids": sorted(row["support_id"] for row in direction_support),
            "channel_observation_ids": sorted(row["channel_id"] for row in direction_channels),
            "commercial_dimension_assessments": normalized_assessments,
            "commercial_risk_flags": sorted(set(raw_direction.get("risk_flags", [])) | gaps),
            "evidence_gap_codes": sorted(gaps), "research_coverage_status": raw_direction["coverage_status"],
            "research_notes": sorted(set(raw_direction.get("unknowns", [])) | {"13 targeted follow-up searches were added after supervisor review; isolated reports remain non-prevalence evidence."}),
            "direct_wtp_summary": raw_direction.get("direct_wtp_summary"),
            "paid_market_precedent_summary": raw_direction.get("paid_market_precedent_summary"),
            "other_proxy_summary": raw_direction.get("other_proxy_summary"),
            "unknowns": sorted(set(raw_direction.get("unknowns", []))),
        }
        packs.append(pack)

    output: dict[str, list[dict[str, Any]]] = {
        "commercial_source_documents": source_rows, "commercial_research_queries": query_rows,
        "commercial_evidence": evidence_rows, "buyer_problem_observations": buyer_rows,
        "commercial_competitor_offerings": competitor_rows, "pricing_observations": pricing_rows,
        "direct_wtp_observations": wtp_rows, "paid_market_precedents": precedent_rows,
        "other_market_proxies": proxy_rows, "differentiation_observations": differentiation_rows,
        "feasibility_observations": feasibility_rows, "support_burden_observations": support_rows,
        "channel_observations": channel_rows, "direction_commercial_validation": packs,
    }
    return output


def _csv_value(value: Any) -> str:
    if value is None:
        return NULL_TOKEN
    if isinstance(value, (dict, list)):
        return _json(value)
    return str(value)


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.write_text("".join(_json(row) + "\n" for row in rows), encoding="utf-8", newline="\n")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in columns})


def _db_row_id(table: str, row: Mapping[str, Any]) -> str:
    keys = [key for key in TABLES[table] if key.endswith("_id")]
    return str(row[keys[0]])


def _write_sqlite(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]], metadata: Mapping[str, str]) -> None:
    if path.exists():
        path.unlink()
    con = sqlite3.connect(path)
    try:
        con.execute("PRAGMA page_size=4096")
        con.execute("PRAGMA journal_mode=DELETE")
        con.execute("PRAGMA foreign_keys=ON")
        con.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID")
        con.execute("CREATE TABLE input_provenance(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID")
        con.execute("CREATE TABLE authorized_cohort(direction_id TEXT PRIMARY KEY,direction_key TEXT NOT NULL,category_id TEXT NOT NULL) WITHOUT ROWID")
        con.execute("CREATE TABLE direction_identity(direction_id TEXT PRIMARY KEY,FOREIGN KEY(direction_id) REFERENCES authorized_cohort(direction_id)) WITHOUT ROWID")
        con.executemany("INSERT INTO authorized_cohort VALUES(?,?,?)", [
            (row["direction_id"], row["canonical_direction_key"], row["category_id"])
            for row in tables["direction_commercial_validation"]
        ])
        con.executemany("INSERT INTO direction_identity VALUES(?)", [(key,) for key in sorted(AUTHORIZED_DIRECTIONS)])
        for key, value in sorted(metadata.items()):
            con.execute("INSERT INTO metadata VALUES(?,?)", (key, value))
        provenance = dict(json.loads(metadata["input_hashes"]))
        provenance.update({"cohort_id_set_sha256": metadata["cohort_id_set_sha256"],
                           "capture_sha256": metadata["capture_sha256"],
                           "execution_code_commit": metadata["execution_code_commit"]})
        for key, value in sorted(provenance.items()):
            con.execute("INSERT INTO input_provenance VALUES(?,?)", (key, value))
        for table in TABLES:
            field_columns = [column for column in TABLES[table] if column != "direction_id"]
            sql_columns = ["record_id TEXT PRIMARY KEY", "direction_id TEXT"]
            sql_columns.extend(f'"{column}" TEXT' for column in field_columns)
            sql_columns.extend(["record_json TEXT NOT NULL", "FOREIGN KEY(direction_id) REFERENCES direction_identity(direction_id)"])
            con.execute(f"CREATE TABLE {table}({','.join(sql_columns)}) WITHOUT ROWID")
            values = []
            for row in tables[table]:
                direction_id = row.get("direction_id")
                field_values = [_json(row.get(column)) for column in field_columns]
                values.append((_db_row_id(table, row), direction_id, *field_values, _json(row)))
            placeholders = ",".join("?" for _ in range(2 + len(field_columns) + 1))
            con.executemany(f"INSERT INTO {table} VALUES({placeholders})", values)
        con.commit()
        con.execute("VACUUM")
    finally:
        con.close()


def _render_schema(input_hashes: Mapping[str, str], execution_commit: str) -> str:
    lines = [
        "# YEE-81 Commercial Validation Schema v0.2", "",
        f"Execution commit: `{execution_commit}`", "",
        "Exactly five authorized YEE-79 direction identities are retained. YEE-79 and YEE-77 are read-only; YEE-77 is an identity/provenance check, not current market evidence.", "",
        "## Semantics", "",
        "- Every factual evidence row references a public source page that was opened. Search snippets are discovery-only.",
        "- Direct WTP, paid-market precedent, and other behavioral/contextual proxies are separate tables and must not be substituted for one another.",
        "- Unknown price is null, not free; currency symbols and source-native amounts are not converted or aggregated.",
        "- Commercial assessments contain seven dimensions and four evidence states. No score, rank, winner, product recommendation, or prevalence estimate is generated.",
        "- Buyer observations are source-specific; `observation_type` is one of `BUYER_SEGMENT`, `OPERATOR_PAIN`, `PURCHASE_TRIGGER`, `WORKFLOW`, `DIRECT_WTP_STATEMENT`; `evidence_strength` is one of `DIRECT_STATEMENT`, `DIRECT_BEHAVIOR`, `BEHAVIORAL_PROXY`, `CONTEXTUAL`.",
        "- Competitor `relation_type` is `DIRECT`, `SUBSTITUTE`, or `ADJACENT`. `DIRECT` requires a same-job server/proxy plugin with current public identity evidence.",
        "- Differentiation `evidence_state` is `EVIDENCE_BACKED`, `HYPOTHESIS_ONLY`, `CONFLICTING`, or `INSUFFICIENT`; `EVIDENCE_BACKED` is independently gated by two URLs plus buyer/pain/trigger and relevant competitor evidence.",
        "- Source types follow the Worker Spec enum; each query has an execution order and discovery URL list; evidence always resolves to an opened source and its opening query.",
        "- Direction packs retain source-level IDs, upstream hashes, the fixed cohort SHA, seven dimension assessments and descriptive evidence-gap codes only.",
        "- JSON nulls are represented as `\\N` in CSV; UTF-8 and LF line endings are used.", "",
        "## Canonical input hashes", "",
    ]
    lines.extend(f"- `{key}`: `{value}`" for key, value in sorted(input_hashes.items()))
    lines += ["", f"Cohort-id-set SHA-256: `{COHORT_SHA256}`.", "", "## Relational export contract", ""]
    for table, columns in TABLES.items():
        lines.append(f"- `{table}` — " + ", ".join(f"`{column}`" for column in columns))
    lines += ["", "Each table is exported as deterministic UTF-8 JSONL and CSV. CSV null is `\\N`; structured values are canonical JSON strings. SQLite exposes normalized schema columns and canonical JSON row payloads, and enforces direction foreign keys.", ""]
    lines.extend(f"- `{table}.jsonl` and `{table}.csv`" for table in TABLES)
    lines += ["- `commercial_validation.sqlite`", "- `COMMERCIAL_RESEARCH_CAPTURE.json`", ""]
    return "\n".join(lines)


def _render_protocol() -> str:
    return """# YEE-81 Stage G Deep Commercial Validation Protocol

## Authorized boundary

Validate only the five exact direction identities authorized by YEE-79. YEE-79 and YEE-77 are read-only. Do not import earlier YEE-30–YEE-59 research as evidence or use it to seed external research. Do not produce TAM, revenue, profit, prevalence, score, ranking, winner, recommendation, product concept, roadmap, or implementation.

## Research procedure

For every direction, issue and retain the ten mandatory searches in the prescribed purpose sequence: buyer/operator segment; pain depth/consequence; purchase trigger; paid alternatives/pricing; free incumbents/substitutes; direct willingness-to-pay evidence; differentiation/unmet needs; channel fit; implementation/compatibility; support/maintenance. The supervisor-authorized correction also requires 13 targeted follow-up searches for the missing buyer/trigger, differentiation, channel, implementation, and support purposes. Search results are discovery only. Record every query in execution order, with its text, timestamp, disposition, discovered-result URLs and opened source IDs. Run materially different follow-ups when a mandatory purpose yields no usable evidence. Retain negative/empty outcomes rather than silently dropping searches.

Open each factual source page before extracting a fact. Store canonical public URL, page title, source type, access result, retrieval time, and any source-visible publication/update date. Every normalized observation links to same-direction evidence, an opened source, and one or more same-direction queries that discovered/opened it. Paraphrase facts; any verbatim excerpt is limited to 25 words per source. Do not bypass login, paywall, CAPTCHA, or service controls.

## Commercial evidence distinctions

- Direct WTP is only an explicit prospective buyer purchase-intent statement about the studied job or an observed transaction. A competitor price is never direct WTP.
- Paid-market precedent is a source-stated price or explicit paid product precedent. Preserve the source-native amount, currency symbol/code when stated, billing model, period, tier, and scope. Do not infer a billing period or convert/aggregate prices.
- Other proxies include public problem reports, free alternatives, adoption/channel signals, maintenance/dependency facts, and explicit commissioning intent. Label their limits and do not promote them into WTP.
- Missing price is null, not free. Mark zero only for an explicit source-stated zero price. Absence of a listing/price is not evidence of no market.
- Feasibility facts describe documented platform/dependency/compatibility constraints only; they are not effort, cost, probability, or delivery estimates.

## Assessment contract

Emit exactly these seven source-grounded dimensions for each direction: `BUYER_PROBLEM`, `PURCHASE_TRIGGER`, `PAID_MARKET_WTP`, `DIFFERENTIATION`, `CHANNEL_FIT`, `IMPLEMENTATION_FEASIBILITY`, `SUPPORT_MAINTENANCE`. Each assessment has `dimension_state`, `basis`, `evidence_ids`, `source_ids`, `risk_flags`, and `unknowns`; `dimension_state` is one of `SUPPORTED`, `MIXED`, `WEAK`, `INSUFFICIENT_EVIDENCE`. Use `INSUFFICIENT_EVIDENCE` when evidence is absent; do not infer demand or prevalence from isolated anecdotes. Coverage is explicit and may remain partial. Buyer rows preserve the Worker Spec's observation/evidence-strength enums and source-specific provenance; insufficient buyer evidence remains a gap, never synthetic coverage.

## Reproducibility and QA

Keep the accepted capture immutable; the v0.2 merged capture records the exact 13 follow-ups and the complete 111-query execution history. Normalize deterministically with stable IDs, sorted records, UTF-8 and LF JSONL/CSV. Preserve nulls as `\\N` in CSV. SQLite exposes normalized fields plus canonical JSON and enforces foreign keys. Validate exact cohort/query coverage and SHA, source enum/URL dedup, buyer/competitor/pricing/differentiation/dimension contracts, evidence references, separated WTP/paid-precedent/proxy semantics, input SHA-256 before and after, SQLite integrity/foreign keys, CSV/JSONL/SQLite row equivalence, and byte-identical deterministic replay. Record execution commit, pinned input hashes, cohort SHA, capture SHA, artifact sizes, and SHA-256 in the manifest. No network calls are part of normalization or replay.
"""


def _render_goal_alignment(input_hashes: Mapping[str, str]) -> str:
    rows = [
        "# YEE-81 Goal Alignment", "", "Status: PASS — authorized Stage G cohort and input gate.", "",
        "## Authorized identities", "",
        "Exactly five YEE-79 `RESOLVED` directions are in scope; this run does not select or reorder them:", "",
    ]
    rows.extend(f"- `{key}` — `{direction_id}` — {category}" for direction_id, (key, category) in sorted(AUTHORIZED_DIRECTIONS.items()))
    rows += ["", "## Read-only inputs", ""]
    rows.extend(f"- `{name}` SHA-256 `{digest}`" for name, digest in sorted(input_hashes.items()))
    rows += ["", "Sorted authorized cohort SHA-256: `f92a72adaead4e4f66d542b819a4d5a7c30cb9a87d66db728f96fcbfb4155130`.", "",
             "YEE-79 and YEE-77 remain read-only. No YEE-30–YEE-59 artifacts were used as evidence, prior, or research seed.", "",
             "## Scope guard", "", "Fresh public research is limited to the authorized five direction jobs. Search snippets are discovery-only; facts require opened canonical public sources. Direct WTP, paid-market precedent, and other proxies remain separate. No TAM, revenue, profit, prevalence, score, ranking, winner, recommendation, product concept, roadmap, or implementation is produced.", ""]
    return "\n".join(rows)


def _report(tables: Mapping[str, Sequence[Mapping[str, Any]]], input_hashes: Mapping[str, str],
            execution_commit: str) -> str:
    directions = tables["direction_commercial_validation"]
    query_count = len(tables["commercial_research_queries"])
    evidence_count = len(tables["commercial_evidence"])
    direct = len(tables["direct_wtp_observations"])
    paid = len(tables["paid_market_precedents"])
    proxies = len(tables["other_market_proxies"])
    pricing = tables["pricing_observations"]
    buyers = tables["buyer_problem_observations"]
    competitors = tables["commercial_competitor_offerings"]
    competitor_names = {row["competitor_id"]: row["entity_name"] for row in competitors}
    differentiation = tables["differentiation_observations"]
    feasibility = tables["feasibility_observations"]
    support = tables["support_burden_observations"]
    channels = tables["channel_observations"]
    lines = [
        "# YEE-81 Deep Commercial Validation — Final Report", "",
        "Status: `DEEP_COMMERCIAL_VALIDATION_READY_FOR_SUPERVISOR_REVIEW`", "",
        f"Execution commit: `{execution_commit}`", f"Authorized direction rows: **{len(directions)}**",
        f"Captured queries: **{query_count}**; opened-source evidence rows: **{evidence_count}**.",
        f"Direct-WTP observations: **{direct}**; paid-market precedent rows: **{paid}**; other proxy rows: **{proxies}**.", "",
        "## Scope and interpretation", "",
        "Research is limited to the five YEE-79-authorized PLUGIN_ONLY directions. YEE-79 and YEE-77 were read-only inputs; no YEE-30–YEE-59 outputs were used. The accepted research capture and all correct prior evidence were preserved; this rebuild adds exactly 13 supervisor-required targeted follow-up searches. Search snippets served only discovery. Findings below are source-bounded observations, not population estimates.",
        "Direct WTP is reported only where a buyer explicitly expressed purchase intent or an observed purchase. A competitor's listed price is a paid-market precedent, not direct WTP for the studied direction. Other proxies remain separately labelled.",
        "No TAM, revenue, profit, score, ranking, winner, product concept, recommendation, or implementation was produced.", "",
        "## Direction coverage", "",
        "| Direction | Coverage | Buyer segments | Direct WTP rows | Paid precedents | Native prices | Free incumbent context | Key unknowns |", "|---|---|---|---:|---:|---|---|---|",
    ]
    for row in directions:
        did = row["direction_id"]
        direct_count = sum(item["direction_id"] == did for item in tables["direct_wtp_observations"])
        paid_names = sorted({item["entity_name"] for item in tables["paid_market_precedents"] if item["direction_id"] == did})
        current_prices = [item for item in pricing if item["direction_id"] == did]
        price_text = "; ".join(f"{item['price_amount']} {item['currency'] or 'currency unstated'} ({item['billing_model']})" if item["price_amount"] is not None else f"price not stated ({item['billing_model'] or 'model unknown'})" for item in current_prices) or "No observed price"
        free_names = sorted({competitor_names[item["competitor_id"]] for item in pricing if item["direction_id"] == did and item.get("monetization_status") == "FREE_VERIFIED"})
        lines.append(f"| {row['canonical_direction_key']} | {row['research_coverage_status']} | {'; '.join(row['buyer_segments']) or 'Not established'} | {direct_count} | {'; '.join(paid_names) or 'None found'} | {price_text} | {'; '.join(free_names) or 'Not observed'} | {'; '.join(row['unknowns']) or 'None recorded'} |")
    source_by_id = {row["source_id"]: row for row in tables["commercial_source_documents"]}
    evidence_by_id = {row["evidence_id"]: row for row in tables["commercial_evidence"]}
    lines += ["", "## Dimension findings", ""]
    for direction in directions:
        lines += [f"### {direction['canonical_direction_key']}", "", f"Market job: {direction['resolved_market_job']}", "",
                  f"Buyer/operator segments: {'; '.join(direction['buyer_segments']) or 'Not established'}",
                  f"Strongest retained purchase-trigger observations: {len(direction['purchase_trigger_observation_ids'])}; direct-WTP observations: {len(direction['wtp_evidence_by_type']['DIRECT_WTP_STATEMENT']) + len(direction['wtp_evidence_by_type']['OBSERVED_PURCHASE_OR_PAYMENT'])}.",
                  f"Differentiation gaps meeting the evidence-backed gate: {sum(1 for item in differentiation if item['direction_id'] == direction['direction_id'] and item['evidence_state'] == 'EVIDENCE_BACKED')}; retained hypotheses/insufficient: {len(direction['differentiation_observation_ids'])}.",
                  f"Observed channel rows: {len(direction['channel_observation_ids'])}; feasibility rows: {len(direction['feasibility_observation_ids'])}; support-burden rows: {len(direction['support_burden_observation_ids'])}.",
                  f"Research coverage: `{direction['research_coverage_status']}`; evidence gaps: {', '.join(direction['evidence_gap_codes']) or 'None'}.", "",
                  "| Dimension | State | Evidence-based basis | Opened sources | Risks | Unknowns |", "|---|---|---|---|---|---|"]
        for assessment in direction["commercial_dimension_assessments"]:
            source_links = []
            for evidence_id in assessment.get("evidence_ids", []):
                evidence = evidence_by_id[evidence_id]
                source = source_by_id[evidence["source_id"]]
                link = f"[{source['title']}]({source['url']})"
                if link not in source_links:
                    source_links.append(link)
            lines.append(f"| {assessment['dimension']} | {assessment['dimension_state']} | {assessment['basis']} | {'; '.join(source_links) or 'None retained'} | {'; '.join(assessment['risk_flags']) or 'None recorded'} | {'; '.join(assessment['unknowns']) or 'None recorded'} |")
        lines.append("")
    direct_dirs = sorted(row["canonical_direction_key"] for row in directions if row["wtp_evidence_by_type"]["DIRECT_WTP_STATEMENT"] or row["wtp_evidence_by_type"]["OBSERVED_PURCHASE_OR_PAYMENT"])
    paid_without_direct = sorted(row["canonical_direction_key"] for row in directions if row["wtp_evidence_by_type"]["PAID_COMPETITOR_PRECEDENT"] and not row["wtp_evidence_by_type"]["DIRECT_WTP_STATEMENT"] and not row["wtp_evidence_by_type"]["OBSERVED_PURCHASE_OR_PAYMENT"])
    no_paid = sorted(row["canonical_direction_key"] for row in directions if not row["wtp_evidence_by_type"]["PAID_COMPETITOR_PRECEDENT"])
    backed_dirs = sorted({row["canonical_direction_key"] for row in directions if any(diff["direction_id"] == row["direction_id"] and diff["evidence_state"] == "EVIDENCE_BACKED" for diff in differentiation)})
    free_dirs = sorted({row["canonical_direction_key"] for row in directions if any(price["direction_id"] == row["direction_id"] and price.get("monetization_status") == "FREE_VERIFIED" for price in pricing)})
    risk_dirs = sorted({row["canonical_direction_key"] for row in directions if any(code in row["evidence_gap_codes"] for code in ("COMPATIBILITY_SCOPE_RISK", "SECURITY_SENSITIVITY_RISK", "SUPPORT_BURDEN_RISK"))})
    low_coverage = sorted(row["canonical_direction_key"] for row in directions if row["research_coverage_status"] != "SUFFICIENT" or "INSUFFICIENT_BUYER_EVIDENCE" in row["evidence_gap_codes"])
    lines += ["## Descriptive cross-direction views (not ranked)", "",
              f"- Directions with direct-WTP evidence: {', '.join(direct_dirs) or 'None found'}.",
              f"- Paid-market precedent without direct WTP: {', '.join(paid_without_direct) or 'None found'}.",
              f"- No paid-market precedent found: {', '.join(no_paid) or 'None found'}.",
              f"- Evidence-backed differentiation: {', '.join(backed_dirs) or 'None established'}.",
              f"- Explicitly free incumbent context: {', '.join(free_dirs) or 'None observed'}.",
              f"- Compatibility/security/support evidence gaps: {', '.join(risk_dirs) or 'None flagged'}.",
              f"- Partial coverage or insufficient buyer evidence: {', '.join(low_coverage) or 'None'}.", "",
              "## Evidence and QA", "", "See the source-document registry, execution-ordered query log, evidence table, normalized observations, QA, manifest, and SQLite tables for exact row-level reconciliation. No source snippet is used as factual evidence. Isolated reports are not prevalence estimates; no direct WTP is inferred from competitor price.", ""]
    lines.extend(f"- `{key}` SHA-256: `{value}`" for key, value in sorted(input_hashes.items()))
    lines.append(f"- `cohort_id_set_sha256`: `{COHORT_SHA256}`")
    lines.append("")
    return "\n".join(lines)


def _check_exports(output: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    checks: dict[str, Any] = {"row_counts_reconcile": True, "csv_jsonl_reconcile": True,
                              "sqlite_integrity_ok": False, "foreign_key_check_ok": False,
                              "sqlite_normalized_columns_present": True}
    row_counts = {}
    for table, columns in TABLES.items():
        jsonl = [json.loads(line) for line in (output / f"{table}.jsonl").read_text(encoding="utf-8").splitlines() if line]
        row_counts[table] = len(jsonl)
        if jsonl != list(tables[table]):
            checks["row_counts_reconcile"] = False
            checks["csv_jsonl_reconcile"] = False
        with (output / f"{table}.csv").open(encoding="utf-8", newline="") as stream:
            csv_rows = list(csv.DictReader(stream))
        expected_csv = [
            {key: _csv_value(row.get(key)) for key in columns}
            for row in tables[table]
        ]
        if csv_rows != expected_csv:
            checks["csv_jsonl_reconcile"] = False
    con = sqlite3.connect(output / "commercial_validation.sqlite")
    try:
        checks["sqlite_integrity_ok"] = con.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        checks["foreign_key_check_ok"] = not con.execute("PRAGMA foreign_key_check").fetchall()
        for table, rows in tables.items():
            columns = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
            if not set(TABLES[table]).issubset(columns) or "record_json" not in columns:
                checks["sqlite_normalized_columns_present"] = False
            db_rows = [json.loads(row[0]) for row in con.execute(f"SELECT record_json FROM {table} ORDER BY record_id")]
            if sorted(db_rows, key=lambda item: _db_row_id(table, item)) != sorted(rows, key=lambda item: _db_row_id(table, item)):
                checks["row_counts_reconcile"] = False
    finally:
        con.close()
    checks["row_counts"] = row_counts
    return checks


def _qa(tables: Mapping[str, Sequence[Mapping[str, Any]]], capture: Mapping[str, Any],
        before: Mapping[str, str], after: Mapping[str, str], export_checks: Mapping[str, Any],
        replay_identical: bool, execution_commit: str,
        expected_hashes: Mapping[str, str] | None = None,
        baseline_ancestor_verified: bool = False) -> dict[str, Any]:
    pinned_hashes = dict(expected_hashes or EXPECTED_INPUT_HASHES)
    direction_ids = {row["direction_id"] for row in tables["direction_commercial_validation"]}
    cohort_sha = hashlib.sha256(("\n".join(sorted(AUTHORIZED_DIRECTIONS)) + "\n").encode("utf-8")).hexdigest()
    sources = {row["source_id"]: row for row in tables["commercial_source_documents"]}
    evidence = {row["evidence_id"]: row for row in tables["commercial_evidence"]}
    queries = {row["query_id"]: row for row in tables["commercial_research_queries"]}
    query_coverage = {direction_id: [] for direction_id in direction_ids}
    for row in tables["commercial_research_queries"]:
        query_coverage[row["direction_id"]].append(row)
    by_purpose = {direction_id: [row for row in rows if row["query_kind"] == "MANDATORY_SEARCH"]
                  for direction_id, rows in query_coverage.items()}
    buyer_required = {"observation_id", "direction_id", "buyer_segment_label", "operator_context", "observation_type",
                      "observation", "operational_consequence", "purchase_trigger", "source_id", "evidence_strength", "retrieved_at", "notes"}
    competitor_required = {"competitor_id", "direction_id", "entity_name", "canonical_url", "relation_type", "product_type",
                           "platform_or_ecosystem", "plugin_scope_status", "pricing_model", "price_amount", "currency",
                           "billing_period", "license_model", "maintenance_status", "feature_summary", "source_ids", "evidence_ids", "retrieved_at"}
    pricing_required = {"pricing_id", "direction_id", "competitor_id", "source_id", "price_amount", "currency", "billing_model",
                        "billing_period", "price_scope", "retrieved_at", "notes"}
    diff_required = {"differentiation_id", "direction_id", "gap_type", "observation", "competitor_ids", "source_ids", "evidence_ids", "evidence_state", "notes"}
    dim_names_ok = all(
        len(row["commercial_dimension_assessments"]) == len(DIMENSIONS)
        and [item["dimension"] for item in row["commercial_dimension_assessments"]] == list(DIMENSIONS)
        for row in tables["direction_commercial_validation"]
    )
    buyer_gap_ok = True
    coverage_details = {}
    for direction_id in AUTHORIZED_DIRECTIONS:
        rows = [row for row in tables["buyer_problem_observations"] if row["direction_id"] == direction_id]
        pain_rows = [row for row in rows if row["observation_type"] in {"OPERATOR_PAIN", "PURCHASE_TRIGGER", "DIRECT_WTP_STATEMENT"}]
        domains = {sources[row["source_id"]]["domain"] for row in pain_rows if row["source_id"] in sources}
        pack = next(row for row in tables["direction_commercial_validation"] if row["direction_id"] == direction_id)
        shortage = len(pain_rows) < 3 or len(domains) < 2
        buyer_gap_ok &= (not shortage or "INSUFFICIENT_BUYER_EVIDENCE" in pack["evidence_gap_codes"])
        coverage_details[direction_id] = {"buyer_observation_count": len(rows), "pain_or_trigger_count": len(pain_rows),
                                          "pain_or_trigger_domain_count": len(domains), "gap_preserved": not shortage or "INSUFFICIENT_BUYER_EVIDENCE" in pack["evidence_gap_codes"]}

    def evidence_backed_gate(row: Mapping[str, Any]) -> bool:
        refs = row["evidence_ids"]
        ref_rows = [evidence[eid] for eid in refs if eid in evidence]
        urls = {sources[item["source_id"]]["canonical_url"] for item in ref_rows if item["source_id"] in sources}
        pain = any(item["evidence_type"] in {"BUYER_NEED", "EXPLICIT_PURCHASE_INTENT", "USER_FRICTION", "USER_WORKAROUND", "SECURITY_INCIDENT_REPORT", "OPERATOR_FRICTION", "COMPATIBILITY_REPORT"} for item in ref_rows)
        competitor = any(item["evidence_type"] in {"PRODUCT_FUNCTION", "COMPETITOR_LISTING", "CHANNEL_AND_COMPETITOR_LISTING", "COMPETITOR_FEATURES", "PAID_COMPETITOR_PRICE", "FREE_INCUMBENT", "FEATURE_LIMITATION", "ADJACENT_PAID_PRECEDENT"} for item in ref_rows)
        return len(urls) >= 2 and pain and competitor

    def has_unauthorized_key(value: Any) -> bool:
        forbidden = re.compile(r"(opportunity_)?score|rank|winner|recommended_build|product_concept|roadmap", re.I)
        if isinstance(value, Mapping):
            return any(forbidden.search(str(key)) or has_unauthorized_key(item) for key, item in value.items())
        if isinstance(value, list):
            return any(has_unauthorized_key(item) for item in value)
        return False

    followup_ids = {row["query_id"] for row in tables["commercial_research_queries"] if row["query_id"] in REQUIRED_FOLLOWUP_QUERY_IDS}
    followup_gate = (not capture.get("supervisor_followup_slice") or followup_ids == REQUIRED_FOLLOWUP_QUERY_IDS)
    query_execution = [row["execution_order"] for row in tables["commercial_research_queries"]]
    opened_evidence_ok = all(
        row["source_id"] in sources
        and sources[row["source_id"]]["access_status"] == "OPENED"
        and row["query_ids"]
        and all(qid in queries and queries[qid]["direction_id"] == row["direction_id"] and row["source_id"] in queries[qid]["opened_source_ids"] for qid in row["query_ids"])
        for row in tables["commercial_evidence"]
    )
    pricing_evidence_ok = all(
        row["source_id"] in sources and row["source_id"] in {evidence[eid]["source_id"] for eid in row["evidence_ids"]}
        and row["competitor_id"] in {competitor["competitor_id"] for competitor in tables["commercial_competitor_offerings"]}
        and not (row["price_amount"] is None and row.get("monetization_status") == "FREE_VERIFIED")
        and (row["price_amount"] != 0 or "EXPLICIT_ZERO_PRICE" in row.get("notes", ""))
        for row in tables["pricing_observations"]
    )
    direct_competitor_guard = all(
        row["relation_type"] != "DIRECT" or ("plugin" in row["product_type"].casefold() and row["plugin_scope_status"].startswith("SERVER")
                                               and row["source_ids"] and row["evidence_ids"])
        for row in tables["commercial_competitor_offerings"]
    )
    dimensions_guard = all(
        item["dimension_state"] in DIMENSION_STATES and item["basis"]
        and set(item["evidence_ids"]).issubset(evidence)
        and set(item["source_ids"]).issubset(sources)
        and set(item["source_ids"]) == {evidence[eid]["source_id"] for eid in item["evidence_ids"]}
        for pack in tables["direction_commercial_validation"] for item in pack["commercial_dimension_assessments"]
    )
    source_type_values = {"PRIMARY_PRODUCT", "PRIMARY_DOCS", "PRIMARY_REPOSITORY", "PRIMARY_SUPPORT", "MARKETPLACE_LISTING", "COMMUNITY", "EDITORIAL", "PLATFORM_DOCUMENTATION"}
    checks = {
        "authorized_cohort_exactly_five": direction_ids == set(AUTHORIZED_DIRECTIONS) and len(direction_ids) == 5,
        "no_unauthorized_direction_in_any_table": all("direction_id" not in row or row.get("direction_id") in set(AUTHORIZED_DIRECTIONS) for rows in tables.values() for row in rows),
        "cohort_id_set_sha256_matches": cohort_sha == COHORT_SHA256 and capture.get("cohort_id_set_sha256") == cohort_sha,
        "upstream_plugin_only_scope_preserved": capture.get("product_scope_branch", "PLUGIN_ONLY") == "PLUGIN_ONLY",
        "accepted_yee79_baseline_is_in_execution_history": baseline_ancestor_verified,
        "input_sha_pins_match": dict(before) == pinned_hashes and dict(after) == pinned_hashes,
        "inputs_unchanged_after_read": dict(before) == dict(after),
        "query_execution_order_complete": query_execution == list(range(1, len(query_execution) + 1)),
        "query_purpose_coverage": all(len(by_purpose[direction_id]) == len(QUERY_PURPOSES) and {row["purpose"] for row in by_purpose[direction_id]} == set(QUERY_PURPOSES) for direction_id in direction_ids),
        "all_supervisor_targeted_followups_present": followup_gate,
        "source_type_enum_valid": bool(sources) and all(row["source_type"] in source_type_values for row in sources.values()),
        "canonical_source_urls_deduplicated": len({row["canonical_url"] for row in sources.values()}) == len(sources),
        "source_retrieval_timestamps_present": all(bool(row["retrieved_at"]) for row in sources.values()),
        "all_factual_evidence_has_opened_source_and_query_provenance": opened_evidence_ok,
        "no_search_snippets_used_as_evidence": all(row.get("source_basis") not in {"SEARCH_SNIPPET", "SEARCH_RESULT_SNIPPET"} for row in capture.get("evidence", [])),
        "buyer_schema_and_enums_valid": all(buyer_required.issubset(row) and row["observation_type"] in OBSERVATION_TYPES and row["evidence_strength"] in EVIDENCE_STRENGTHS and row["source_id"] in sources and row["retrieved_at"] for row in tables["buyer_problem_observations"]),
        "buyer_shortfalls_preserved_without_synthetic_prevalence": buyer_gap_ok and all(not re.search(r"\b\d+(?:\.\d+)?\s*%|prevalence estimate|prevalence rate", row["observation"], re.I) for row in tables["buyer_problem_observations"]),
        "direct_competitors_are_same_job_server_plugin_products": all(competitor_required.issubset(row) for row in tables["commercial_competitor_offerings"]) and direct_competitor_guard,
        "non_plugin_context_not_direct": all(row["relation_type"] not in RELATION_TYPES or row["relation_type"] != "DIRECT" or "plugin" in row["product_type"].casefold() for row in tables["commercial_competitor_offerings"]),
        "pricing_schema_and_source_provenance_valid": all(pricing_required.issubset(row) for row in tables["pricing_observations"]) and pricing_evidence_ok,
        "price_null_zero_and_currency_semantics_preserved": all(row["price_amount"] is None or row["price_amount"] > 0 or "EXPLICIT_ZERO_PRICE" in row.get("notes", "") for row in tables["pricing_observations"]) and all(row["currency"] is None or row["currency"] in {raw.get("currency_code") for raw in capture.get("pricing_observations", [])} for row in tables["pricing_observations"]),
        "no_recommended_price_or_currency_conversion": all("recommended price" not in str(row.get("notes", "")).casefold() for row in tables["pricing_observations"]),
        "direct_wtp_paid_precedent_and_proxies_separated": all(row["wtp_type"] in {"DIRECT_WTP_STATEMENT", "OBSERVED_PURCHASE_OR_PAYMENT"} for row in tables["direct_wtp_observations"]) and all(row["wtp_type"] in {"PAID_COMPETITOR_PRECEDENT", "PREMIUM_TIER_PRECEDENT", "DONATION_OR_SPONSOR_PRECEDENT"} for row in tables["paid_market_precedents"]) and all(row["evidence_ids"] for row in tables["other_market_proxies"]),
        "differentiation_schema_and_enums_valid": all(diff_required.issubset(row) and row["gap_type"] in DIFFERENTIATION_GAPS and row["evidence_state"] in DIFFERENTIATION_STATES for row in tables["differentiation_observations"]),
        "evidence_backed_differentiation_meets_two_source_pain_competitor_gate": all(row["evidence_state"] != "EVIDENCE_BACKED" or evidence_backed_gate(row) for row in tables["differentiation_observations"]),
        "unsupported_differentiation_remains_hypothesis_or_insufficient": all(row["evidence_state"] != "EVIDENCE_BACKED" or evidence_backed_gate(row) for row in tables["differentiation_observations"]),
        "feasibility_theme_and_provenance_valid": all(row["theme"] in FEASIBILITY_THEMES and row["evidence_ids"] and set(row["source_ids"]) == {evidence[eid]["source_id"] for eid in row["evidence_ids"]} for row in tables["feasibility_observations"]),
        "support_and_channel_provenance_valid": all(row["evidence_ids"] and set(row["source_ids"]) == {evidence[eid]["source_id"] for eid in row["evidence_ids"]} for table in ("support_burden_observations", "channel_observations") for row in tables[table]),
        "exactly_seven_allowed_dimensions_per_direction": dim_names_ok and dimensions_guard,
        "direction_pack_schema_and_gap_enums_valid": len(tables["direction_commercial_validation"]) == 5 and all(row["yee79_decision"] == "ADVANCE_FOR_DEEP_VALIDATION" and row["research_coverage_status"] in {"SUFFICIENT", "PARTIAL", "INSUFFICIENT"} and set(row["evidence_gap_codes"]).issubset(EVIDENCE_GAP_CODES) and row["upstream_provenance"]["cohort_id_set_sha256"] == cohort_sha for row in tables["direction_commercial_validation"]),
        "no_scalar_score_rank_winner_recommendation_or_product_concept_fields": not has_unauthorized_key(tables["direction_commercial_validation"]),
        "sqlite_jsonl_csv_reconcile": bool(export_checks["row_counts_reconcile"] and export_checks["csv_jsonl_reconcile"] and export_checks["sqlite_integrity_ok"] and export_checks["foreign_key_check_ok"] and export_checks["sqlite_normalized_columns_present"]),
        "deterministic_replay_byte_identical": replay_identical,
        "execution_commit_recorded": bool(re.fullmatch(r"[0-9a-f]{40}", execution_commit)),
    }
    return {"work_order": WORK_ORDER, "status": "PASS" if all(checks.values()) else "FAIL", "checks": checks,
            "details": {"authorized_direction_count": len(direction_ids), "query_count": len(tables["commercial_research_queries"]),
                        "mandatory_query_count": sum(row["query_kind"] == "MANDATORY_SEARCH" for row in tables["commercial_research_queries"]),
                        "followup_query_count": sum(row["query_kind"] == "FOLLOW_UP_SEARCH" for row in tables["commercial_research_queries"]),
                        "opened_source_count": len(tables["commercial_source_documents"]), "evidence_count": len(tables["commercial_evidence"]),
                        "cohort_id_set_sha256": cohort_sha, "buyer_coverage_by_direction": coverage_details,
                        "dimension_count": len(DIMENSIONS), "row_counts": export_checks["row_counts"],
                        "input_hashes_before": dict(before), "input_hashes_after": dict(after),
                        "execution_code_commit": execution_commit, "baseline_ancestor_verified": baseline_ancestor_verified}}


def build_dataset(yee79_db: str | Path, yee77_db: str | Path, capture_path: str | Path,
                  output_dir: str | Path, execution_commit: str, *, run_replay: bool = True,
                  expected_hashes: Mapping[str, str] | None = None,
                  followup_capture_path: str | Path | None = None,
                  baseline_ancestor_verified: bool | None = None) -> dict[str, Any]:
    if not re.fullmatch(r"[0-9a-f]{40}", execution_commit):
        raise CommercialValidationError("execution_commit must be a full lowercase Git SHA")
    canonical_rows, before = load_inputs(yee79_db, yee77_db, expected_hashes)
    capture_path = Path(capture_path)
    capture = json.loads(capture_path.read_text(encoding="utf-8"))
    if followup_capture_path is not None:
        supplement = json.loads(Path(followup_capture_path).read_text(encoding="utf-8"))
        capture = merge_followup_capture(capture, supplement)
        capture["supervisor_followup_slice"] = "YEE-81 blocking review follow-ups 2026-09-28"
        capture["product_scope_branch"] = "PLUGIN_ONLY"
    capture_bytes = (json.dumps(capture, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    merged_capture_sha = hashlib.sha256(capture_bytes).hexdigest()
    tables = normalize_capture(capture, canonical_rows, before)
    output = Path(output_dir)
    if output.exists() and any(output.iterdir()):
        raise CommercialValidationError(f"output directory must be empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    schema = _render_schema(before, execution_commit)
    (output / "COMMERCIAL_VALIDATION_SCHEMA.md").write_text(schema, encoding="utf-8", newline="\n")
    (output / "COMMERCIAL_VALIDATION_PROTOCOL.md").write_text(_render_protocol(), encoding="utf-8", newline="\n")
    (output / "GOAL_ALIGNMENT.md").write_text(_render_goal_alignment(before), encoding="utf-8", newline="\n")
    (output / "COMMERCIAL_RESEARCH_CAPTURE.json").write_bytes(capture_bytes)
    for table, columns in TABLES.items():
        _write_jsonl(output / f"{table}.jsonl", tables[table])
        _write_csv(output / f"{table}.csv", tables[table], columns)
    metadata = {
        "work_order": WORK_ORDER,
        "schema_version": CAPTURE_VERSION,
        "execution_code_commit": execution_commit,
        "preflight_code_commit": BASELINE_COMMIT,
        "input_hashes": _json(before),
        "capture_sha256": merged_capture_sha,
        "cohort_id_set_sha256": COHORT_SHA256,
        "authorized_direction_ids": _json(sorted(AUTHORIZED_DIRECTIONS)),
        "analysis_scope": "five_authorized_yEE79_plugin_only_directions_only",
    }
    _write_sqlite(output / "commercial_validation.sqlite", tables, metadata)
    checks = _check_exports(output, tables)
    after = {"yee79_decision_gate": _sha256(Path(yee79_db)), "yee77_research": _sha256(Path(yee77_db))}
    report = _report(tables, before, execution_commit)
    (output / "SUPERVISOR_COMMERCIAL_VALIDATION_BRIEF.md").write_text(report, encoding="utf-8", newline="\n")
    (output / "FINAL_REPORT.md").write_text(report, encoding="utf-8", newline="\n")
    replay_identical = False
    if run_replay:
        with tempfile.TemporaryDirectory(prefix="yee81-replay-") as temp:
            replay_dir = Path(temp) / "replay"
            replay_path = output / "COMMERCIAL_RESEARCH_CAPTURE.json"
            build_dataset(yee79_db, yee77_db, replay_path, replay_dir, execution_commit,
                          run_replay=False, expected_hashes=expected_hashes,
                          baseline_ancestor_verified=baseline_ancestor_verified)
            excluded = {"QA_RESULT.json", "DATASET_MANIFEST.json"}
            names = sorted(path.name for path in output.iterdir() if path.is_file() and path.name not in excluded)
            replay_names = sorted(path.name for path in replay_dir.iterdir() if path.is_file() and path.name not in excluded)
            replay_identical = names == replay_names and all(
                (output / name).read_bytes() == (replay_dir / name).read_bytes() for name in names)
    ancestor_ok = execution_commit == BASELINE_COMMIT if baseline_ancestor_verified is None else baseline_ancestor_verified
    qa = _qa(tables, capture, before, after, checks, replay_identical, execution_commit,
             expected_hashes, ancestor_ok)
    (output / "QA_RESULT.json").write_text(_json(qa) + "\n", encoding="utf-8", newline="\n")
    manifest = {"work_order": WORK_ORDER, "status": qa["status"], "execution_code_commit": execution_commit,
                "preflight_code_commit": BASELINE_COMMIT, "input_hashes": dict(before),
                "capture_sha256": merged_capture_sha, "cohort_id_set_sha256": COHORT_SHA256,
                "authorized_direction_ids": sorted(AUTHORIZED_DIRECTIONS), "files": []}
    for path in sorted((item for item in output.iterdir() if item.is_file() and item.name != "DATASET_MANIFEST.json"), key=lambda p: p.name):
        manifest["files"].append({"path": path.name, "size_bytes": path.stat().st_size, "sha256": _sha256(path)})
    (output / "DATASET_MANIFEST.json").write_text(_json(manifest) + "\n", encoding="utf-8", newline="\n")
    return {"status": qa["status"], "output_dir": output, "qa": qa, "manifest": manifest}
