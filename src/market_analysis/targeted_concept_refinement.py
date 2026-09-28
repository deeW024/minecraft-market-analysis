"""Deterministic YEE-95 targeted concept-refinement bundle."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

WORK_ORDER = "YEE-95"
CAPTURE_VERSION = "yee-95-targeted-concept-refinement-v0.1"
BASELINE_COMMIT = "7667924820add3776d543427017fd7ab050a2ee7"
YEE83_EXECUTION_COMMIT = "92304e0b0bbed991c686f975aa5d5c50d1a5eb9b"
COHORT_SHA256 = "efaf80224f5d9b613a9b4917a726c93f946b5c8f9a8ec6466db041556d74fce5"
YEE83_CONCEPT_ID_SET_SHA256 = "5431678b22b1ac564c1260e24a261b83478611b6381850d750f7e3a3618253f7"
YEE83_CAPTURE_SHA256 = "dc4601e962a3e65809a320b2bf760440ed81b9cef717ddcffe68e7075bd8ab88"
INPUT_HASHES = {
    "yee83_sqlite": "27ee026ee1d57aaeba3b6a0042357b918e967c8b425fc1bd888a17e65dd98d17",
    "yee83_capture": YEE83_CAPTURE_SHA256,
    "yee81_sqlite": "470a7e160d9b0408b878206098980e07e92cc17f3c61448ee3eef2bb1f1ae7a4",
    "yee81_capture": "5e1dfd09e57433c1b8eae19d8d6930dd1447129cb5caef925b7d9367ec76c159",
    "yee79_sqlite": "7059d52e7f54c7b6b0709f6301a7ceb0a442e2c7635dcc8912cbe5c21f3079fe",
    "yee77_sqlite": "a2ea751823358e1032c36fd31a88d2e581476bf6e915a53c042b70614bcc4f48",
}
CONCEPTS = {
    "yee83-jobs-01-economy-consistency": "Job payout and server-economy consistency",
    "yee83-jobs-02-progress-recovery": "Restart-safe job progress recovery",
}
PURPOSES = (
    "OPERATOR_RECURRENCE_A", "OPERATOR_RECURRENCE_B", "CROSS_PRODUCT_OR_STACK_RECURRENCE",
    "ROOT_CAUSE_AND_RESOLUTION", "CURRENT_INCUMBENT_CAPABILITY", "DIAGNOSTICS_OR_RECOVERY_GAP",
    "PAID_VALUE_OR_BEHAVIORAL_PROXY", "SUPPORT_AND_IMPLEMENTATION_BOUNDARY",
)
ROOT_CAUSES = {
    "PRODUCT_OR_INTEGRATION_FAILURE", "CONFIGURATION_OR_OPERATOR_ERROR", "UPSTREAM_PROVIDER_OR_DATABASE",
    "THIRD_PARTY_INTERACTION", "VERSION_OR_MIGRATION", "CONFLICTING", "UNKNOWN",
}
APPLICABILITY = {"CURRENT_OR_RECENT", "HISTORICAL_ONLY", "VERSION_UNKNOWN", "RESOLVED_OR_OBSOLETE", "CONFLICTING"}
COVERAGE_STATES = {"NOT_OBSERVED", "PARTIALLY_COVERED", "SUBSTANTIALLY_COVERED", "CONFLICTING", "UNKNOWN"}
DIMENSIONS = (
    "PROBLEM_RECURRENCE", "ROOT_CAUSE_CLARITY", "CURRENT_INCUMBENT_GAP", "PAID_VALUE_EXCHANGE",
    "REFINED_DIFFERENTIATION", "IMPLEMENTATION_FEASIBILITY", "SUPPORT_MAINTENANCE",
)
DIMENSION_STATES = {"SUPPORTED", "MIXED", "WEAK", "INSUFFICIENT_EVIDENCE"}
WEDGE_STATES = {"REFINED_WEDGE", "NO_DEFENSIBLE_WEDGE"}
OVERALL_STATES = {
    "REFINEMENT_SIGNAL_SUPPORTED", "REFINEMENT_SIGNAL_MIXED", "REFINEMENT_SIGNAL_WEAK",
    "REFINEMENT_INSUFFICIENT_EVIDENCE",
}
EVIDENCE_GAPS = {
    "RECURRENCE_NOT_ESTABLISHED", "CROSS_PRODUCT_RECURRENCE_NOT_ESTABLISHED", "ROOT_CAUSE_AMBIGUOUS",
    "CONFIGURATION_EXPLAINS_CASES", "INCUMBENT_CAPABILITY_COVERS_WEDGE", "DIFFERENTIATION_NOT_ESTABLISHED",
    "DIFFERENTIATION_CONFLICTING", "NO_DIRECT_WTP_EVIDENCE", "PAID_VALUE_WEAK", "DATA_INTEGRITY_RISK",
    "DATABASE_COMPATIBILITY_RISK", "MULTI_SERVER_LIFECYCLE_RISK", "THIRD_PARTY_INTEGRATION_RISK",
    "SUPPORT_BURDEN_RISK", "SOURCE_COVERAGE_RISK", "HISTORICAL_EVIDENCE_ONLY", "OTHER_EVIDENCE_GAP",
}
VALUE_TYPES = {
    "DIRECT_WTP_STATEMENT", "OBSERVED_PURCHASE_OR_PAYMENT", "PAID_COMPETITOR_PRECEDENT",
    "PAID_SUPPORT_OR_COMMISSIONING_PROXY", "BEHAVIORAL_PROXY", "OPERATIONAL_VALUE_PROXY", "NO_WTP_EVIDENCE",
}
TABLE_EXPORTS = {
    "refinement_source_documents": "sources",
    "refinement_queries": "queries",
    "refinement_evidence": "evidence",
    "operator_case_observations": "operator_cases",
    "root_cause_observations": "root_causes",
    "recurrence_assessments": "recurrence_assessments",
    "incumbent_capability_observations": "incumbent_capabilities",
    "refined_wedges": "refined_wedges",
    "differentiation_assessments": "differentiation_assessments",
    "paid_value_observations": "paid_value_observations",
    "feasibility_observations": "feasibility_observations",
    "support_observations": "support_observations",
    "concept_refinement_cards": "concept_refinement_cards",
}


class RefinementError(ValueError):
    """Raised when YEE-95 pins, capture or dossier contract fail."""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cohort_sha(ids: list[str]) -> str:
    return hashlib.sha256(("\n".join(sorted(ids)) + "\n").encode("utf-8")).hexdigest()


def canonical_url(url: str) -> str:
    parts = urlsplit(url.strip())
    query = urlencode(sorted((k, v) for k, v in parse_qsl(parts.query) if not k.lower().startswith("utm_")))
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/") or "/", query, ""))


def validate_input_hashes(paths: dict[str, Path]) -> dict[str, str]:
    if set(paths) != set(INPUT_HASHES):
        raise RefinementError("Accepted input names do not match the YEE-95 pin set")
    actual = {name: sha256_file(path) for name, path in sorted(paths.items())}
    mismatches = [name for name, expected in INPUT_HASHES.items() if actual[name] != expected]
    if mismatches:
        raise RefinementError("Accepted input hash mismatch: " + ", ".join(sorted(mismatches)))
    return actual


def _validate_cohort(concept_ids: list[str], dropped_ids: list[str]) -> None:
    if set(dropped_ids) & set(concept_ids):
        raise RefinementError("A dropped YEE-83 concept leaked into the authorized cohort")
    if sorted(concept_ids) != sorted(CONCEPTS) or len(concept_ids) != 2 or cohort_sha(concept_ids) != COHORT_SHA256:
        raise RefinementError("YEE-95 must contain exactly the authorized two-concept cohort")


def _query_tokens(query_text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", query_text.casefold()))


def _query_targets_concept(concept_id: str, query_text: str, targeting_rationale: str | None = None) -> bool:
    tokens = _query_tokens(query_text)
    job_anchor = bool(tokens & {"job", "jobs", "jobsreborn"})
    if concept_id == "yee83-jobs-01-economy-consistency":
        boundary = bool(tokens & {"vault", "vaultapi", "economy", "provider", "shop", "transaction", "payout", "deposit", "balance", "reward", "payment", "money", "paid", "paying"})
        vault_response_api = {"vaultapi", "economyresponse", "transaction"} <= tokens and bool(tokens & {"depositplayer", "withdrawalplayer", "deposit", "withdrawal"})
        rationale_tokens = _query_tokens(targeting_rationale or "")
        api_boundary_context = {"job", "jobs", "reward", "payout"} & rationale_tokens and bool(rationale_tokens & {"vault", "provider", "economy"})
        return (job_anchor and boundary) or (vault_response_api and api_boundary_context)
    if concept_id == "yee83-jobs-02-progress-recovery":
        progress_or_save = bool(tokens & {"progress", "level", "levels", "experience", "xp", "save", "saved", "saving", "persist", "persistence", "restore", "recovery"})
        lifecycle_or_storage = bool(tokens & {"restart", "reboot", "disconnect", "shutdown", "crash", "save", "saved", "saving", "database", "sqlite", "mysql", "storage", "multiserver", "lifecycle", "migration", "restore", "recovery"})
        return job_anchor and progress_or_save and lifecycle_or_storage
    return False


def _validate_recurrence_followups(queries: list[dict[str, Any]]) -> None:
    for concept_id in CONCEPTS:
        primary = [q for q in queries if q.get("concept_id") == concept_id and q.get("query_kind") == "MANDATORY"
                   and q.get("purpose") in {"OPERATOR_RECURRENCE_A", "OPERATOR_RECURRENCE_B"}]
        for failed_query in (q for q in primary if q.get("new_independent_case_found") is False):
            followups = [q for q in queries if q.get("concept_id") == concept_id and q.get("query_kind") == "FOLLOWUP"
                         and q.get("purpose") == failed_query["purpose"]
                         and q.get("execution_order", 0) > failed_query.get("execution_order", 0)]
            if len(followups) < 2 or len({q.get("query_text") for q in followups}) < 2:
                raise RefinementError(f"Two materially distinct same-purpose recurrence follow-ups required: {concept_id}:{failed_query['purpose']}")


def _validate_case_discovery(queries: list[dict[str, Any]], cases: list[dict[str, Any]]) -> None:
    by_query = {q["query_id"]: q for q in queries}
    for case in cases:
        origin = case.get("case_origin")
        query_id = case.get("discovered_by_query_id")
        if origin == "ACCEPTED_UPSTREAM":
            if query_id is not None:
                raise RefinementError("Accepted upstream cases cannot be relabeled as newly discovered")
            continue
        if origin != "TARGETED_RESEARCH" or query_id not in by_query:
            raise RefinementError("Targeted operator cases require explicit first-discovery query provenance")
        query = by_query[query_id]
        if query.get("concept_id") != case.get("concept_id") or case.get("source_id") not in query.get("opened_source_ids", []):
            raise RefinementError("Case discovery query must open the case's source for the same concept")
        earlier_source_queries = [q for q in queries if q.get("concept_id") == case.get("concept_id")
                                 and q.get("execution_order", 0) < query.get("execution_order", 0)
                                 and case.get("source_id") in q.get("opened_source_ids", [])]
        if earlier_source_queries:
            raise RefinementError("A case cannot be newly discovered after an earlier query opened its source")
    for query in queries:
        found = any(case.get("case_origin") == "TARGETED_RESEARCH"
                    and case.get("supports_failure_class") is True
                    and case.get("discovered_by_query_id") == query.get("query_id") for case in cases)
        if query.get("new_independent_case_found") is not found:
            raise RefinementError(f"Query new-case flag disagrees with first-discovery provenance: {query.get('query_id')}")


def _validate_same_url_case_independence(case_rows: list[dict[str, Any]]) -> None:
    by_url: dict[str, list[dict[str, Any]]] = {}
    for case in case_rows:
        by_url.setdefault(canonical_url(case["source_url"]), []).append(case)
    for same_url in by_url.values():
        if len(same_url) <= 1:
            continue
        identities = {row.get("operator_identity") for row in same_url}
        locators = {row.get("incident_locator") for row in same_url}
        groups = {row["independence_group_id"] for row in same_url}
        if (None in identities or len(identities) != len(same_url) or None in locators
                or len(locators) != len(same_url) or len(groups) != len(same_url)):
            raise RefinementError("Multiple cases on one URL require distinct identifiable operators/incidents and groups")


def _normalize_case_discovery(capture: dict[str, Any], accepted_capture: dict[str, Any]) -> None:
    accepted_sources = {row["source_id"]: row for row in accepted_capture.get("sources", [])}
    accepted_evidence = [row for row in accepted_capture.get("evidence", [])
                         if row.get("concept_id") in CONCEPTS
                         and row.get("evidence_type") in {"BUYER_NEED", "SUPPORT_CASE", "HISTORICAL_USER_REPORT"}]
    queries = capture["queries"]
    accepted_case_ids: set[str] = set()
    for row in accepted_evidence:
        source = accepted_sources.get(row.get("source_id"))
        if not source:
            continue
        source_url = canonical_url(source.get("canonical_url") or source["url"])
        candidates = [case for case in capture["operator_cases"] if case["concept_id"] == row["concept_id"]
                      and canonical_url(case["source_url"]) == source_url]
        if len(candidates) == 1:
            accepted_case_ids.add(candidates[0]["case_id"])
            continue
        observation = row.get("observation", "").casefold()
        markers = set(re.findall(r"\b(sqlite|mysql|mariadb|vault|velocity|paper|spigot|essentialsx|economy|shop)\b", observation))
        matches = []
        for case in candidates:
            case_text = " ".join(str(case.get(key, "")) for key in (
                "operator_context", "triggering_event", "observed_consequence", "reported_workaround_or_resolution", "incident_locator"
            )).casefold()
            if markers and markers <= set(re.findall(r"\b[a-z0-9]+\b", case_text)):
                matches.append(case)
        if len(matches) != 1:
            raise RefinementError(f"Accepted parent evidence cannot be matched to one incident at {source_url}")
        accepted_case_ids.add(matches[0]["case_id"])
    for case in capture["operator_cases"]:
        if case["case_id"] in accepted_case_ids:
            case["case_origin"] = "ACCEPTED_UPSTREAM"
            case["discovered_by_query_id"] = None
            continue
        first_source_queries = [q for q in queries if q["concept_id"] == case["concept_id"]
                                and case["source_id"] in q.get("opened_source_ids", [])]
        if not first_source_queries:
            raise RefinementError(f"No executed query opened targeted case source: {case['case_id']}")
        first = min(first_source_queries, key=lambda q: (q["execution_order"], q["query_id"]))
        case["case_origin"] = "TARGETED_RESEARCH"
        case["discovered_by_query_id"] = first["query_id"]
    for query in queries:
        query["new_independent_case_found"] = any(
            case["case_origin"] == "TARGETED_RESEARCH" and case.get("supports_failure_class") is True
            and case["discovered_by_query_id"] == query["query_id"]
            for case in capture["operator_cases"]
        )


def _validate_wedge_record(wedge: dict[str, Any], evidence_ids: set[str]) -> None:
    if wedge.get("wedge_state") not in WEDGE_STATES:
        raise RefinementError("Invalid refined-wedge disposition")
    if not wedge.get("basis") or not wedge.get("scope_boundary"):
        raise RefinementError("Refined-wedge record requires basis and scope boundary")
    if not set(wedge.get("evidence_ids", [])) <= evidence_ids or not set(wedge.get("counterevidence_ids", [])) <= evidence_ids:
        raise RefinementError("Refined-wedge record refers to unknown evidence")
    candidate = wedge.get("refined_wedge")
    if wedge["wedge_state"] == "NO_DEFENSIBLE_WEDGE":
        if candidate is not None:
            raise RefinementError("NO_DEFENSIBLE_WEDGE must have a null refined_wedge")
    elif not isinstance(candidate, str) or not candidate.strip() or not wedge.get("evidence_ids"):
        raise RefinementError("A refined wedge requires a bounded statement and supporting evidence")


def _validate_paid_value_observations(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        if row.get("evidence_type") not in VALUE_TYPES:
            raise RefinementError("Invalid paid-value evidence type")
        if row.get("evidence_type") == "DIRECT_WTP_STATEMENT" and not row.get("direct_wtp_source_ids"):
            raise RefinementError("Direct WTP requires an explicit source reference")
        if row.get("evidence_type") == "PAID_COMPETITOR_PRECEDENT":
            if row.get("is_direct_wtp") is not False:
                raise RefinementError("Competitor price cannot be converted to WTP")
            if any(row.get(key) is not None for key in ("converted_amount", "converted_currency", "exchange_rate", "currency_conversion")):
                raise RefinementError("Competitor listing prices must remain source-native; currency conversion is prohibited")


def _validate_decision_template(decision: dict[str, Any]) -> None:
    if decision.get("decision_status") != "UNDECIDED" or any(
        decision.get(key) for key in (
            "advance_to_product_spec_concept_ids", "request_further_refinement_concept_ids",
            "held_concept_ids", "dropped_concept_ids",
        )
    ) or decision.get("build_none") is not False or decision.get("rationale") is not None or decision.get("decided_at") is not None:
        raise RefinementError("Supervisor decision template must remain empty and undecided")


def _forbidden_fields(value: Any) -> set[str]:
    forbidden_terms = ("score", "rank", "winner", "recommendation", "product_spec", "implementation_plan")
    found: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).casefold()
            if any(term in normalized for term in forbidden_terms):
                found.add(str(key))
            found.update(_forbidden_fields(child))
    elif isinstance(value, list):
        for child in value:
            found.update(_forbidden_fields(child))
    return found


def _differentiation_state(assessment: dict[str, Any], evidence: dict[str, dict[str, Any]], sources: dict[str, dict[str, Any]]) -> str:
    claimed = assessment["state"]
    if claimed != "SUPPORTED":
        return claimed
    case_sources = {evidence[eid]["source_id"] for eid in assessment.get("evidence_ids", [])
                    if evidence[eid].get("evidence_type") == "OPERATOR_CASE"}
    product_sources = {evidence[eid]["source_id"] for eid in assessment.get("evidence_ids", [])
                       if evidence[eid].get("evidence_type") in {"INCUMBENT_CAPABILITY", "COUNTEREVIDENCE"}}
    url_count = len({canonical_url(sources[sid]["canonical_url"]) for sid in case_sources | product_sources})
    supported = bool(case_sources and product_sources and url_count >= 2
                     and not assessment.get("unexplained_material_counterevidence"))
    if supported:
        return "SUPPORTED"
    if case_sources and (product_sources or assessment.get("counterevidence_ids")):
        return "MIXED"
    if case_sources or product_sources:
        return "WEAK"
    return "INSUFFICIENT_EVIDENCE"


def _unique_by(rows: list[dict[str, Any]], key: str, label: str) -> dict[str, dict[str, Any]]:
    result = {str(row.get(key)): row for row in rows}
    if len(result) != len(rows) or "None" in result:
        raise RefinementError(f"{label} IDs must be present and unique")
    return result


def _case_groups(rows: list[dict[str, Any]], concept_id: str) -> list[dict[str, Any]]:
    eligible = [r for r in rows if r["concept_id"] == concept_id and r.get("supports_failure_class") is True]
    by_group: dict[str, dict[str, Any]] = {}
    for row in eligible:
        group = row["independence_group_id"]
        if group in by_group:
            # A group may be represented once only; duplicate-source references belong on that row.
            raise RefinementError(f"Duplicate incident would inflate recurrence: {group}")
        by_group[group] = row
    return [by_group[k] for k in sorted(by_group)]


def derive_recurrence(case_rows: list[dict[str, Any]], concept_id: str) -> dict[str, Any]:
    cases = _case_groups(case_rows, concept_id)
    urls = sorted({canonical_url(r["source_url"]) for r in cases})
    environments = sorted({r["environment_key"] for r in cases})
    products = sorted({r["affected_product"] for r in cases if r.get("affected_product")})
    stacks = sorted({r["provider_stack"] for r in cases if r.get("provider_stack")})
    roots = {r["root_cause_state"] for r in cases}
    current_count = sum(r["current_applicability_state"] == "CURRENT_OR_RECENT" for r in cases)
    historic_count = sum(r["current_applicability_state"] == "HISTORICAL_ONLY" for r in cases)
    if not cases:
        state = "INSUFFICIENT_EVIDENCE"
    elif len(cases) == 1:
        state = "WEAK"
    elif len(cases) >= 2 and (len(products) == 1 or "CONFLICTING" in roots):
        state = "MIXED"
    elif len(cases) >= 3 and len(urls) >= 2 and len(environments) >= 2:
        state = "SUPPORTED"
    else:
        state = "WEAK"
    return {
        "concept_id": concept_id,
        "recurrence_state": state,
        "independent_case_count": len(cases),
        "canonical_url_count": len(urls),
        "distinct_environment_count": len(environments),
        "current_or_recent_case_count": current_count,
        "historical_case_count": historic_count,
        "case_ids": [r["case_id"] for r in cases],
        "independence_group_ids": [r["independence_group_id"] for r in cases],
        "canonical_urls": urls,
        "environment_keys": environments,
        "affected_products": products,
        "provider_stack_count": len(stacks),
        "provider_stack_contexts": stacks,
        "cross_stack_status": "OBSERVED_WITHIN_JOBS_REBORN" if len(environments) > 1 else "NOT_ESTABLISHED",
        "aligned_failure_class": cases[0]["failure_class"] if cases else None,
        "cross_product_status": "NOT_ESTABLISHED" if len(products) <= 1 else "OBSERVED",
        "basis": (
            "Evidence-sufficiency state only; not prevalence. Multiple independent Jobs Reborn reports are present, "
            "but the cases cluster on one jobs product and causes/configurations are not consistently established."
            if state == "MIXED" else
            "No usable independent operator cases remained after the required targeted searches."
            if state == "INSUFFICIENT_EVIDENCE" else
            "Conservative recurrence gate applied to independent operator cases and canonical URLs."
        ),
        "limitations": "Unresolved community issues are reports, not verified product defects; no prevalence inference.",
    }


def derive_overall(dimensions: list[dict[str, Any]], blocking_scope_contradiction: bool = False) -> str:
    by_name = {row["dimension"]: row for row in dimensions}
    if len(by_name) != len(DIMENSIONS) or set(by_name) != set(DIMENSIONS):
        raise RefinementError("Each concept must have exactly the seven specified dimensions")
    recurrence = by_name["PROBLEM_RECURRENCE"]
    differentiation = by_name["REFINED_DIFFERENTIATION"]
    core = (recurrence["state"], differentiation["state"])
    if core == ("SUPPORTED", "SUPPORTED") and not blocking_scope_contradiction:
        return "REFINEMENT_SIGNAL_SUPPORTED"
    if "INSUFFICIENT_EVIDENCE" in core:
        return "REFINEMENT_INSUFFICIENT_EVIDENCE"
    has_support = bool(recurrence.get("evidence_ids") or differentiation.get("evidence_ids"))
    if ("MIXED" in core and has_support) or blocking_scope_contradiction:
        return "REFINEMENT_SIGNAL_MIXED"
    if "WEAK" in core or "MIXED" in core:
        return "REFINEMENT_SIGNAL_WEAK"
    return "REFINEMENT_INSUFFICIENT_EVIDENCE"


def _validate_capture(capture: dict[str, Any]) -> None:
    if capture.get("capture_version") != CAPTURE_VERSION:
        raise RefinementError("Unexpected frozen research capture version")
    if capture.get("product_scope") != "PLUGIN_ONLY":
        raise RefinementError("YEE-95 must preserve the PLUGIN_ONLY product guard")
    if capture.get("pinned_input_hashes") != INPUT_HASHES:
        raise RefinementError("Frozen capture accepted-input pins do not match the Worker Spec")
    if capture.get("final_y83_decision") != {
        "decision_status": "FINAL", "requested_refinement_concept_ids": sorted(CONCEPTS),
        "advance_to_product_spec_concept_ids": [], "build_none": False,
    }:
        raise RefinementError("The final YEE-83 Supervisor/User decision is the sole cohort authorization")
    concepts = capture.get("concepts", [])
    ids = [r.get("concept_id") for r in concepts]
    dropped = set(capture.get("excluded_y83_concept_ids", []))
    if dropped != {
        "yee83-cpc-01-network-chat-continuity", "yee83-cpc-02-proxy-identity-parity",
        "yee83-cpc-03-low-friction-operations", "yee83-jobs-03-authorization-integrity",
    }:
        raise RefinementError("The four dropped YEE-83 concepts must remain explicitly excluded")
    sources = _unique_by(capture.get("sources", []), "source_id", "Source")
    queries = _unique_by(capture.get("queries", []), "query_id", "Query")
    evidence = _unique_by(capture.get("evidence", []), "evidence_id", "Evidence")
    case_rows = capture.get("operator_cases", [])
    _unique_by(case_rows, "case_id", "Operator case")
    _validate_cohort(ids, list(dropped))
    urls = [canonical_url(s["canonical_url"]) for s in sources.values()]
    if any(s.get("access_status") != "OPENED" or s.get("canonical_url") != canonical_url(s.get("url", "")) for s in sources.values()):
        raise RefinementError("Source documents must be opened and use canonical URLs")
    if len(urls) != len(set(urls)):
        raise RefinementError("Canonical source URLs must be deduplicated")
    mandatory = [q for q in queries.values() if q.get("query_kind") == "MANDATORY"]
    if len(mandatory) != 16:
        raise RefinementError("Exactly 16 mandatory targeted queries are required")
    for concept_id in CONCEPTS:
        qrows = [q for q in mandatory if q.get("concept_id") == concept_id]
        if len(qrows) != 8 or {q.get("purpose") for q in qrows} != set(PURPOSES):
            raise RefinementError(f"Each concept needs exactly one query per mandatory purpose: {concept_id}")
        if len([q for q in qrows if q["purpose"] in {"OPERATOR_RECURRENCE_A", "OPERATOR_RECURRENCE_B"}]) != 2:
            raise RefinementError(f"Exactly two primary recurrence queries required: {concept_id}")
    for query in queries.values():
        if query.get("concept_id") not in CONCEPTS or not query.get("query_text"):
            raise RefinementError("Every query must target one authorized concept")
        if query.get("query_kind") not in {"MANDATORY", "FOLLOWUP"}:
            raise RefinementError("Unexpected query kind")
        if any(source_id not in sources or sources[source_id].get("access_status") != "OPENED" for source_id in query.get("opened_source_ids", [])):
            raise RefinementError("Query opened-source references must resolve to captured opened sources")
        if not _query_targets_concept(query["concept_id"], query["query_text"], query.get("targeting_rationale")):
            raise RefinementError("Generic broad-market query cannot satisfy concept-targeted research")
    _validate_recurrence_followups(list(queries.values()))
    for row in evidence.values():
        source = sources.get(row.get("source_id"))
        qids = row.get("query_ids", [])
        if not source or source.get("access_status") != "OPENED" or not qids:
            raise RefinementError("Factual evidence must resolve to an opened source and query")
        if any(qid not in queries for qid in qids):
            raise RefinementError("Evidence refers to an unknown query")
        if row.get("used_search_snippet") is not False:
            raise RefinementError("Search snippets are discovery only, never evidence")
    _validate_case_discovery(list(queries.values()), case_rows)
    for case in case_rows:
        source = sources.get(case.get("source_id"))
        if not source or source.get("access_status") != "OPENED" or case.get("concept_id") not in CONCEPTS:
            raise RefinementError("Operator cases require an opened canonical source")
        if canonical_url(case["source_url"]) != canonical_url(source["canonical_url"]):
            raise RefinementError("Operator case URL must match its canonical source document")
        if case.get("root_cause_state") not in ROOT_CAUSES or case.get("current_applicability_state") not in APPLICABILITY:
            raise RefinementError("Invalid root-cause or current-applicability enum")
        if not case.get("independence_group_id") or not case.get("environment_key"):
            raise RefinementError("Operator cases need an independence group and environment key")
        if any(eid not in evidence for eid in case.get("evidence_ids", [])):
            raise RefinementError("Operator case refers to unknown evidence")
    _validate_same_url_case_independence(case_rows)
    for table in ("incumbent_capabilities", "paid_value_observations", "feasibility_observations", "support_observations"):
        _unique_by(capture.get(table, []), "observation_id", table)
        for row in capture.get(table, []):
            if row.get("concept_id") not in CONCEPTS or row.get("source_id") not in sources:
                raise RefinementError(f"{table} must be scoped to an authorized concept and opened source")
            if any(eid not in evidence for eid in row.get("evidence_ids", [])):
                raise RefinementError(f"{table} contains an unknown evidence reference")
            if table == "incumbent_capabilities" and row.get("coverage_state") not in COVERAGE_STATES:
                raise RefinementError("Invalid incumbent coverage state")
    for row in capture.get("differentiation_assessments", []):
        if row.get("concept_id") not in CONCEPTS or row.get("state") not in DIMENSION_STATES:
            raise RefinementError("Differentiation assessment has invalid concept or state")
        for eid in row.get("evidence_ids", []) + row.get("counterevidence_ids", []):
            if eid not in evidence:
                raise RefinementError("Differentiation assessment contains unknown evidence")
        if row["state"] == "SUPPORTED":
            _differentiation_state(row, evidence, sources)
    for concept in concepts:
        if not set(concept.get("evidence_gap_codes", [])) <= EVIDENCE_GAPS:
            raise RefinementError("Invalid evidence-gap code")
        wedge = {
            "wedge_id": f"wedge_{concept['concept_id']}", "concept_id": concept["concept_id"],
            "wedge_state": concept.get("wedge_state"), "refined_wedge": concept.get("refined_wedge"),
            "basis": concept.get("wedge_basis"), "evidence_ids": concept.get("wedge_evidence_ids", []),
            "counterevidence_ids": concept.get("wedge_counterevidence_ids", []),
            "scope_boundary": concept.get("wedge_scope_boundary"),
        }
        _validate_wedge_record(wedge, set(evidence))
    for concept in concepts:
        dimensions = concept.get("dimension_assessments", [])
        by_name = {r.get("dimension"): r for r in dimensions}
        if len(by_name) != 7 or set(by_name) != set(DIMENSIONS):
            raise RefinementError("Every concept requires exactly seven refinement dimensions")
        if any(r.get("state") not in DIMENSION_STATES for r in dimensions):
            raise RefinementError("Invalid refinement dimension state")
        for row in dimensions:
            if not all(field in row for field in ("basis", "case_ids", "evidence_ids", "source_ids", "counterevidence_ids", "risk_flags", "unknowns")):
                raise RefinementError("Dimension assessment is missing provenance or uncertainty fields")
            if any(eid not in evidence for eid in row["evidence_ids"]):
                raise RefinementError("Dimension assessment contains unknown evidence")
            if any(case_id not in {c["case_id"] for c in capture["operator_cases"]} for case_id in row["case_ids"]):
                raise RefinementError("Dimension assessment contains unknown case")
    _validate_paid_value_observations(capture.get("paid_value_observations", []))


def _records(capture: dict[str, Any], metadata: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    concepts = sorted(capture["concepts"], key=lambda r: r["concept_id"])
    recurrence = [derive_recurrence(capture["operator_cases"], c["concept_id"]) for c in concepts]
    recurrence_by_id = {r["concept_id"]: r for r in recurrence}
    roots = [{
        "root_cause_id": f"root_{c['case_id']}", "concept_id": c["concept_id"], "case_id": c["case_id"],
        "source_id": c["source_id"], "root_cause_state": c["root_cause_state"],
        "root_cause_basis": c["root_cause_basis"], "current_applicability_state": c["current_applicability_state"],
        "limitations": c["limitations"],
    } for c in sorted(capture["operator_cases"], key=lambda r: r["case_id"])]
    wedges = [{
        "wedge_id": f"wedge_{c['concept_id']}", "concept_id": c["concept_id"],
        "wedge_state": c["wedge_state"], "refined_wedge": c["refined_wedge"],
        "basis": c["wedge_basis"], "evidence_ids": c["wedge_evidence_ids"],
        "counterevidence_ids": c["wedge_counterevidence_ids"],
        "scope_boundary": c["wedge_scope_boundary"],
    } for c in concepts]
    evidence_by_id = {r["evidence_id"]: r for r in capture["evidence"]}
    sources_by_id = {r["source_id"]: r for r in capture["sources"]}
    diffs = []
    for assessment in sorted(capture["differentiation_assessments"], key=lambda r: r["concept_id"]):
        normalized = dict(assessment)
        normalized["source_claimed_state"] = assessment["state"]
        normalized["state"] = _differentiation_state(assessment, evidence_by_id, sources_by_id)
        diffs.append(normalized)
    cards = []
    for concept in concepts:
        cid = concept["concept_id"]
        dimensions = sorted(concept["dimension_assessments"], key=lambda r: DIMENSIONS.index(r["dimension"]))
        overall = derive_overall(dimensions, concept.get("blocking_scope_contradiction", False))
        recurrence_row = recurrence_by_id[cid]
        cards.append({
            **{k: v for k, v in concept.items() if k not in {"dimension_assessments", "wedge_basis", "wedge_evidence_ids", "wedge_counterevidence_ids", "wedge_scope_boundary", "wedge_state", "refined_wedge"}},
            "accepted_provenance": {
                "baseline_commit": BASELINE_COMMIT, "accepted_y83_execution_commit": YEE83_EXECUTION_COMMIT,
                "accepted_y83_sqlite_sha256": INPUT_HASHES["yee83_sqlite"],
                "accepted_y83_capture_sha256": INPUT_HASHES["yee83_capture"],
                "accepted_y83_concept_id_set_sha256": YEE83_CONCEPT_ID_SET_SHA256,
            },
            "recurrence_assessment": recurrence_row,
            "operator_case_ids": recurrence_row["case_ids"],
            "root_cause_observation_ids": [r["root_cause_id"] for r in roots if r["concept_id"] == cid],
            "incumbent_capability_ids": [r["observation_id"] for r in capture["incumbent_capabilities"] if r["concept_id"] == cid],
            "refined_wedge": next(r for r in wedges if r["concept_id"] == cid),
            "differentiation_assessment": next(r for r in diffs if r["concept_id"] == cid),
            "paid_value_observation_ids": [r["observation_id"] for r in capture["paid_value_observations"] if r["concept_id"] == cid],
            "feasibility_observation_ids": [r["observation_id"] for r in capture["feasibility_observations"] if r["concept_id"] == cid],
            "support_observation_ids": [r["observation_id"] for r in capture["support_observations"] if r["concept_id"] == cid],
            "dimension_assessments": dimensions,
            "overall_refinement_state": overall,
            "research_coverage_status": "COMPLETE",
            "research_notes": concept["research_notes"],
        })
    template = {
        "decision_status": "UNDECIDED", "advance_to_product_spec_concept_ids": [],
        "request_further_refinement_concept_ids": [], "held_concept_ids": [], "dropped_concept_ids": [],
        "build_none": False, "rationale": None, "decided_at": None,
        "valid_human_actions": ["ADVANCE_TO_PRODUCT_SPEC", "REQUEST_FURTHER_REFINEMENT", "HOLD", "DROP", "BUILD_NONE"],
    }
    return {
        "metadata": [{"key": k, "value": v} for k, v in sorted(metadata.items())],
        "input_provenance": [{"input_name": k, "sha256": metadata[f"input_sha256:{k}"], "read_only": True} for k in sorted(INPUT_HASHES)],
        "authorized_concept_cohort": [{"concept_id": cid, "cohort_sha256": COHORT_SHA256} for cid in sorted(CONCEPTS)],
        "concepts": [{"concept_id": c["concept_id"], "concept_label": c["concept_label"], "parent_work_order": "YEE-83", "plugin_only": True} for c in concepts],
        "sources": sorted(capture["sources"], key=lambda r: r["source_id"]),
        "queries": sorted(capture["queries"], key=lambda r: (r["concept_id"], 0 if r["query_kind"] == "MANDATORY" else 1, r["execution_order"])),
        "evidence": sorted(capture["evidence"], key=lambda r: r["evidence_id"]),
        "operator_cases": sorted(capture["operator_cases"], key=lambda r: r["case_id"]),
        "root_causes": roots,
        "recurrence_assessments": recurrence,
        "incumbent_capabilities": sorted(capture["incumbent_capabilities"], key=lambda r: r["observation_id"]),
        "refined_wedges": wedges,
        "differentiation_assessments": diffs,
        "paid_value_observations": sorted(capture["paid_value_observations"], key=lambda r: r["observation_id"]),
        "feasibility_observations": sorted(capture["feasibility_observations"], key=lambda r: r["observation_id"]),
        "support_observations": sorted(capture["support_observations"], key=lambda r: r["observation_id"]),
        "concept_refinement_cards": cards,
        "supervisor_decision_template": [template],
    }


def _csv_cell(value: Any) -> str:
    if value is None:
        return r"\N"
    return value if isinstance(value, str) else _json(value)


def _table_jsonl(rows: list[dict[str, Any]]) -> bytes:
    return "".join(_json(row) + "\n" for row in sorted(rows, key=lambda r: _json(r))).encode("utf-8")


def _table_csv(rows: list[dict[str, Any]]) -> bytes:
    columns = sorted({key for row in rows for key in row})
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(columns)
    for row in sorted(rows, key=lambda r: _json(r)):
        writer.writerow([_csv_cell(row.get(column)) for column in columns])
    return stream.getvalue().encode("utf-8")


def _write_table(out: Path, base: str, rows: list[dict[str, Any]]) -> None:
    (out / f"{base}.jsonl").write_bytes(_table_jsonl(rows))
    (out / f"{base}.csv").write_bytes(_table_csv(rows))


def _write_sqlite(path: Path, rows: dict[str, list[dict[str, Any]]]) -> None:
    if path.exists():
        path.unlink()
    db = sqlite3.connect(path)
    try:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA page_size=4096")
        db.executescript("""
            CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE input_provenance(input_name TEXT PRIMARY KEY, sha256 TEXT NOT NULL, read_only INTEGER NOT NULL CHECK(read_only=1));
            CREATE TABLE authorized_concept_cohort(concept_id TEXT PRIMARY KEY, cohort_sha256 TEXT NOT NULL);
            CREATE TABLE concepts(concept_id TEXT PRIMARY KEY, concept_label TEXT NOT NULL, parent_work_order TEXT NOT NULL, plugin_only INTEGER NOT NULL CHECK(plugin_only=1));
            CREATE TABLE sources(source_id TEXT PRIMARY KEY, canonical_url TEXT NOT NULL UNIQUE, title TEXT NOT NULL, access_status TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE queries(query_id TEXT PRIMARY KEY, concept_id TEXT NOT NULL REFERENCES concepts(concept_id), query_kind TEXT NOT NULL, purpose TEXT NOT NULL, execution_order INTEGER NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE evidence(evidence_id TEXT PRIMARY KEY, concept_id TEXT NOT NULL REFERENCES concepts(concept_id), source_id TEXT NOT NULL REFERENCES sources(source_id), query_ids_json TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE operator_cases(case_id TEXT PRIMARY KEY, concept_id TEXT NOT NULL REFERENCES concepts(concept_id), source_id TEXT NOT NULL REFERENCES sources(source_id), independence_group_id TEXT NOT NULL, failure_class TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE root_causes(root_cause_id TEXT PRIMARY KEY, concept_id TEXT NOT NULL REFERENCES concepts(concept_id), case_id TEXT NOT NULL REFERENCES operator_cases(case_id), root_cause_state TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE recurrence_assessments(concept_id TEXT PRIMARY KEY REFERENCES concepts(concept_id), recurrence_state TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE incumbent_capabilities(observation_id TEXT PRIMARY KEY, concept_id TEXT NOT NULL REFERENCES concepts(concept_id), source_id TEXT NOT NULL REFERENCES sources(source_id), coverage_state TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE refined_wedges(wedge_id TEXT PRIMARY KEY, concept_id TEXT NOT NULL UNIQUE REFERENCES concepts(concept_id), wedge_state TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE differentiation_assessments(concept_id TEXT PRIMARY KEY REFERENCES concepts(concept_id), differentiation_state TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE paid_value_observations(observation_id TEXT PRIMARY KEY, concept_id TEXT NOT NULL REFERENCES concepts(concept_id), evidence_type TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE feasibility_observations(observation_id TEXT PRIMARY KEY, concept_id TEXT NOT NULL REFERENCES concepts(concept_id), record_json TEXT NOT NULL);
            CREATE TABLE support_observations(observation_id TEXT PRIMARY KEY, concept_id TEXT NOT NULL REFERENCES concepts(concept_id), record_json TEXT NOT NULL);
            CREATE TABLE concept_refinement_cards(concept_id TEXT PRIMARY KEY REFERENCES concepts(concept_id), overall_refinement_state TEXT NOT NULL, record_json TEXT NOT NULL);
            CREATE TABLE supervisor_decision_template(singleton_id INTEGER PRIMARY KEY CHECK(singleton_id=1), record_json TEXT NOT NULL);
        """)
        for r in rows["metadata"]: db.execute("INSERT INTO metadata VALUES(?,?)", (r["key"], r["value"]))
        for r in rows["input_provenance"]: db.execute("INSERT INTO input_provenance VALUES(?,?,1)", (r["input_name"], r["sha256"]))
        for r in rows["authorized_concept_cohort"]: db.execute("INSERT INTO authorized_concept_cohort VALUES(?,?)", (r["concept_id"], r["cohort_sha256"]))
        for r in rows["concepts"]: db.execute("INSERT INTO concepts VALUES(?,?,?,1)", (r["concept_id"], r["concept_label"], r["parent_work_order"]))
        for r in rows["sources"]: db.execute("INSERT INTO sources VALUES(?,?,?,?,?)", (r["source_id"], r["canonical_url"], r["title"], r["access_status"], _json(r)))
        for r in rows["queries"]: db.execute("INSERT INTO queries VALUES(?,?,?,?,?,?)", (r["query_id"], r["concept_id"], r["query_kind"], r["purpose"], r["execution_order"], _json(r)))
        for r in rows["evidence"]: db.execute("INSERT INTO evidence VALUES(?,?,?,?,?)", (r["evidence_id"], r["concept_id"], r["source_id"], _json(r["query_ids"]), _json(r)))
        for r in rows["operator_cases"]: db.execute("INSERT INTO operator_cases VALUES(?,?,?,?,?,?)", (r["case_id"], r["concept_id"], r["source_id"], r["independence_group_id"], r["failure_class"], _json(r)))
        for r in rows["root_causes"]: db.execute("INSERT INTO root_causes VALUES(?,?,?,?,?)", (r["root_cause_id"], r["concept_id"], r["case_id"], r["root_cause_state"], _json(r)))
        for r in rows["recurrence_assessments"]: db.execute("INSERT INTO recurrence_assessments VALUES(?,?,?)", (r["concept_id"], r["recurrence_state"], _json(r)))
        for r in rows["incumbent_capabilities"]: db.execute("INSERT INTO incumbent_capabilities VALUES(?,?,?,?,?)", (r["observation_id"], r["concept_id"], r["source_id"], r["coverage_state"], _json(r)))
        for r in rows["refined_wedges"]: db.execute("INSERT INTO refined_wedges VALUES(?,?,?,?)", (r["wedge_id"], r["concept_id"], r["wedge_state"], _json(r)))
        for r in rows["differentiation_assessments"]: db.execute("INSERT INTO differentiation_assessments VALUES(?,?,?)", (r["concept_id"], r["state"], _json(r)))
        for r in rows["paid_value_observations"]: db.execute("INSERT INTO paid_value_observations VALUES(?,?,?,?)", (r["observation_id"], r["concept_id"], r["evidence_type"], _json(r)))
        for r in rows["feasibility_observations"]: db.execute("INSERT INTO feasibility_observations VALUES(?,?,?)", (r["observation_id"], r["concept_id"], _json(r)))
        for r in rows["support_observations"]: db.execute("INSERT INTO support_observations VALUES(?,?,?)", (r["observation_id"], r["concept_id"], _json(r)))
        for r in rows["concept_refinement_cards"]: db.execute("INSERT INTO concept_refinement_cards VALUES(?,?,?)", (r["concept_id"], r["overall_refinement_state"], _json(r)))
        db.execute("INSERT INTO supervisor_decision_template VALUES(1,?)", (_json(rows["supervisor_decision_template"][0]),))
        db.commit()
    finally:
        db.close()


def _brief(cards: list[dict[str, Any]]) -> str:
    sections = ["# Supervisor Refinement Decision Brief\n\nYEE-95 evaluates exactly two accepted YEE-83 concepts. This is an evidence dossier, not a ranking or product decision.\n"]
    for card in cards:
        rec = card["recurrence_assessment"]
        diff = card["differentiation_assessment"]
        wedge = card["refined_wedge"]
        wedge_label = wedge["refined_wedge"] if wedge["wedge_state"] == "REFINED_WEDGE" else "NO_DEFENSIBLE_WEDGE"
        sections.append(
            f"## {card['concept_id']} — {card['concept_label']}\n\n"
            f"- Parent question: {card['refinement_question']}\n"
            f"- Recurrence: **{rec['recurrence_state']}**, {rec['independent_case_count']} independent case groups, "
            f"{rec['canonical_url_count']} canonical URLs, {rec['distinct_environment_count']} environments/configurations. "
            f"This is evidence sufficiency, not prevalence.\n"
            f"- Cross-product / stack: {rec['cross_product_status']} / {rec['cross_stack_status']} ({rec['provider_stack_count']} reported stack contexts).\n"
            f"- Cases: {', '.join(rec['case_ids']) or 'none'}\n"
            f"- Root-cause pattern: {card['root_cause_summary']}\n"
            f"- Current incumbent gap: {card['incumbent_gap_summary']}\n"
            f"- Refined wedge: **{wedge_label}** — {card['wedge_summary']}\n"
            f"- Differentiation: **{diff['state']}** — {diff['basis']}\n"
            f"- Paid value: {card['paid_value_summary']}\n"
            f"- Feasibility/support: {card['feasibility_support_summary']}\n"
            f"- Overall descriptive state: **{card['overall_refinement_state']}**\n"
            f"- Evidence gaps: {', '.join(card['evidence_gap_codes'])}\n"
            f"- Unknowns: {'; '.join(card['unknowns'])}\n"
        )
    sections.append("## Descriptive comparison\n\nBoth theses have operator reports clustered on Jobs Reborn and unresolved cause attribution. J1 is constrained by provider/configuration responsibility and no direct WTP; J2 has additional recent lifecycle reports, offset by documented autosave/save-on-disconnect capabilities and unresolved reproductions. No direction is selected or ranked.\n")
    return "\n".join(sections)


def _write_core(out: Path, alignment_path: Path, rows: dict[str, list[dict[str, Any]]], capture: dict[str, Any]) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    (out / "GOAL_ALIGNMENT.md").write_bytes(alignment_path.read_bytes())
    (out / "REFINEMENT_RESEARCH_CAPTURE.json").write_text(_json(capture) + "\n", encoding="utf-8", newline="\n")
    (out / "REFINEMENT_RESEARCH_PROTOCOL.md").write_text(_protocol(), encoding="utf-8", newline="\n")
    (out / "REFINEMENT_SCHEMA.md").write_text(
        _schema_doc() + "\n\nOptional query field `targeting_rationale` explains a direct source/API query that operationalizes an authorized concept boundary when the search phrase names only one side of that integration. It supplements and does not rewrite the recorded query text.\n",
        encoding="utf-8", newline="\n",
    )
    (out / "SUPERVISOR_REFINEMENT_DECISION_BRIEF.md").write_text(_brief(rows["concept_refinement_cards"]), encoding="utf-8", newline="\n")
    (out / "SUPERVISOR_REFINEMENT_DECISION_TEMPLATE.json").write_text(_json(rows["supervisor_decision_template"][0]) + "\n", encoding="utf-8", newline="\n")
    exports = []
    for base, table in TABLE_EXPORTS.items():
        _write_table(out, base, rows[table])
        exports.extend([out / f"{base}.jsonl", out / f"{base}.csv"])
    dbpath = out / "concept_refinement.sqlite"
    _write_sqlite(dbpath, rows)
    return [out / "GOAL_ALIGNMENT.md", out / "REFINEMENT_RESEARCH_CAPTURE.json", out / "REFINEMENT_RESEARCH_PROTOCOL.md", out / "REFINEMENT_SCHEMA.md", out / "SUPERVISOR_REFINEMENT_DECISION_BRIEF.md", out / "SUPERVISOR_REFINEMENT_DECISION_TEMPLATE.json", *exports, dbpath]


def _protocol() -> str:
    return """# YEE-95 Research Protocol\n\n- Scope: exactly J1 `yee83-jobs-01-economy-consistency` and J2 `yee83-jobs-02-progress-recovery`; accepted YEE-83/YEE-81 plus inherited YEE-79/YEE-77 pins are read-only.\n- Search: 8 mandatory targeted query purposes per concept (16 total), followed by two materially different same-purpose operator-recurrence queries per concept where A/B found no new independent case. Search snippets are discovery-only.\n- Evidence: facts are retained only from opened canonical URLs. Reports are anecdotal and preserve author, product, version/date, environment, root-cause uncertainty, applicability, and limitations.\n- Independence: separate case groups only for separately described operator incidents. Cross-references to the same YEE-83/YEE-81 Reddit event remain one case; different users in the old Spigot thread are separately described events but share one URL.\n- Recurrence: hard evidence-sufficiency gates from Worker Spec; never incidence, prevalence, or market-size estimates.\n- Product boundary: PLUGIN_ONLY. No non-plugin product, external hosted replacement, rank, score, winner, Product Spec, or implementation.\n- Paid-value labels remain separate: no direct WTP or observed purchase; public competitor price and custom commission evidence are only bounded proxies.\n- Research date: 2026-09-28 (source capture records date precision when provider timestamps were unavailable).\n"""


def _schema_doc() -> str:
    return """# YEE-95 Refinement Schema\n\nTwo ordered parent identities only: `yee83-jobs-01-economy-consistency`, `yee83-jobs-02-progress-recovery`. Cohort SHA-256: `efaf80224f5d9b613a9b4917a726c93f946b5c8f9a8ec6466db041556d74fce5`.\n\n## Research records\n\nEach query has concept ID, execution order, mandatory/follow-up kind, exact purpose/text, date precision, opened sources and a `new_independent_case_found` flag reconciled to first-discovery case provenance. The 8 mandatory purposes per concept are `OPERATOR_RECURRENCE_A`, `OPERATOR_RECURRENCE_B`, `CROSS_PRODUCT_OR_STACK_RECURRENCE`, `ROOT_CAUSE_AND_RESOLUTION`, `CURRENT_INCUMBENT_CAPABILITY`, `DIAGNOSTICS_OR_RECOVERY_GAP`, `PAID_VALUE_OR_BEHAVIORAL_PROXY`, and `SUPPORT_AND_IMPLEMENTATION_BOUNDARY`. Queries must combine each concept's exact job/reward or progression target with its economy/provider or lifecycle/storage boundary; generic market queries do not qualify. Search result snippets are discovery only. Each fact row must cite an opened canonical source and query.\n\nOperator cases preserve operator context, product, failure class, trigger, consequence, reported workaround, root cause, current applicability, environment, provider stack, independence group and limitations. `case_origin` is `ACCEPTED_UPSTREAM` or `TARGETED_RESEARCH`; targeted cases carry the first query ID that opened their source, while inherited cases have no new-discovery query. Root cause = `PRODUCT_OR_INTEGRATION_FAILURE | CONFIGURATION_OR_OPERATOR_ERROR | UPSTREAM_PROVIDER_OR_DATABASE | THIRD_PARTY_INTERACTION | VERSION_OR_MIGRATION | CONFLICTING | UNKNOWN`; applicability = `CURRENT_OR_RECENT | HISTORICAL_ONLY | VERSION_UNKNOWN | RESOLVED_OR_OBSOLETE | CONFLICTING`. Repeated reports of one incident share an independence group; separately authored events on one URL require distinct operator/incident locators.\n\n## Wedges and assessments\n\nEach concept explicitly records `wedge_state` and nullable `refined_wedge`. `REFINED_WEDGE` requires a non-empty bounded statement, basis/scope boundary and supporting evidence IDs; `NO_DEFENSIBLE_WEDGE` requires `refined_wedge: null`. QA accepts both paths without preferring either. Differentiation claims are normalized against the evidence gate; an unsupported `SUPPORTED` claim is downgraded, preserving its source-claimed state.\n\nRecurrence = `SUPPORTED | MIXED | WEAK | INSUFFICIENT_EVIDENCE`; incumbent coverage = `NOT_OBSERVED | PARTIALLY_COVERED | SUBSTANTIALLY_COVERED | CONFLICTING | UNKNOWN`; each of the seven dimensions uses `SUPPORTED | MIXED | WEAK | INSUFFICIENT_EVIDENCE`. Dimensions: `PROBLEM_RECURRENCE`, `ROOT_CAUSE_CLARITY`, `CURRENT_INCUMBENT_GAP`, `PAID_VALUE_EXCHANGE`, `REFINED_DIFFERENTIATION`, `IMPLEMENTATION_FEASIBILITY`, `SUPPORT_MAINTENANCE`.\n\nOverall state follows the hard rules: both core dimensions supported and no scope contradiction => `REFINEMENT_SIGNAL_SUPPORTED`; otherwise, a non-insufficient mixed core with evidence => `REFINEMENT_SIGNAL_MIXED`; sufficient coverage with a weak core => `REFINEMENT_SIGNAL_WEAK`; either core insufficient => `REFINEMENT_INSUFFICIENT_EVIDENCE`.\n\nPaid-value types: `DIRECT_WTP_STATEMENT`, `OBSERVED_PURCHASE_OR_PAYMENT`, `PAID_COMPETITOR_PRECEDENT`, `PAID_SUPPORT_OR_COMMISSIONING_PROXY`, `BEHAVIORAL_PROXY`, `OPERATIONAL_VALUE_PROXY`, `NO_WTP_EVIDENCE`. Competitor price is not direct WTP and is never currency-converted. Evidence-gap codes are `RECURRENCE_NOT_ESTABLISHED`, `CROSS_PRODUCT_RECURRENCE_NOT_ESTABLISHED`, `ROOT_CAUSE_AMBIGUOUS`, `CONFIGURATION_EXPLAINS_CASES`, `INCUMBENT_CAPABILITY_COVERS_WEDGE`, `DIFFERENTIATION_NOT_ESTABLISHED`, `DIFFERENTIATION_CONFLICTING`, `NO_DIRECT_WTP_EVIDENCE`, `PAID_VALUE_WEAK`, `DATA_INTEGRITY_RISK`, `DATABASE_COMPATIBILITY_RISK`, `MULTI_SERVER_LIFECYCLE_RISK`, `THIRD_PARTY_INTEGRATION_RISK`, `SUPPORT_BURDEN_RISK`, `SOURCE_COVERAGE_RISK`, `HISTORICAL_EVIDENCE_ONLY`, `OTHER_EVIDENCE_GAP`.\n\nExactly two cards are exported in stable concept-ID order, with one explicit wedge disposition, exactly seven dimensions and no score/rank/winner/recommendation. JSONL uses JSON null; CSV uses `\\N` for null. All tables and rows are deterministically sorted; UTF-8/LF.\n\nSQLite normalizes metadata, input provenance, cohort, concepts, sources, queries, evidence, operator cases, root causes, recurrence, incumbents, wedges, differentiation, paid value, feasibility, support, cards and decision template. `PRAGMA integrity_check=ok`; `foreign_key_check` must return zero rows.\n"""


def _qa_oracle_recurrence(cases: list[dict[str, Any]], concept_id: str) -> dict[str, Any]:
    # Deliberately separate from derive_recurrence: QA rebuilds sets from case rows.
    subset = [c for c in cases if c["concept_id"] == concept_id and c["supports_failure_class"]]
    groups = {c["independence_group_id"] for c in subset}
    urls = {canonical_url(c["source_url"]) for c in subset}
    environments = {c["environment_key"] for c in subset}
    products = {c["affected_product"] for c in subset if c.get("affected_product")}
    roots = {c["root_cause_state"] for c in subset}
    if not groups:
        state = "INSUFFICIENT_EVIDENCE"
    elif len(groups) == 1:
        state = "WEAK"
    elif len(products) == 1 or "CONFLICTING" in roots:
        state = "MIXED"
    elif len(groups) >= 3 and len(urls) >= 2 and len(environments) >= 2:
        state = "SUPPORTED"
    else:
        state = "WEAK"
    stacks = {c["provider_stack"] for c in subset if c.get("provider_stack")}
    return {"state": state, "independent_case_count": len(groups), "canonical_url_count": len(urls), "distinct_environment_count": len(environments), "provider_stack_count": len(stacks),
            "cross_product_status": "NOT_ESTABLISHED" if len(products) <= 1 else "OBSERVED",
            "cross_stack_status": "OBSERVED_WITHIN_JOBS_REBORN" if len(environments) > 1 else "NOT_ESTABLISHED",
            "current_or_recent_case_count": sum(c["current_applicability_state"] == "CURRENT_OR_RECENT" for c in subset),
            "historical_case_count": sum(c["current_applicability_state"] == "HISTORICAL_ONLY" for c in subset)}


def _qa_overall_oracle(card: dict[str, Any]) -> str:
    # Independent hard-rule transcription for QA; deliberately does not call derive_overall.
    by_dimension = {item["dimension"]: item for item in card["dimension_assessments"]}
    states = {key: item["state"] for key, item in by_dimension.items()}
    if set(states) != set(DIMENSIONS) or len(card["dimension_assessments"]) != 7:
        return "INVALID_DIMENSION_SET"
    recurrence_state = states["PROBLEM_RECURRENCE"]
    difference_state = states["REFINED_DIFFERENTIATION"]
    if recurrence_state == "INSUFFICIENT_EVIDENCE" or difference_state == "INSUFFICIENT_EVIDENCE":
        return "REFINEMENT_INSUFFICIENT_EVIDENCE"
    if recurrence_state == "SUPPORTED" and difference_state == "SUPPORTED" and not card.get("blocking_scope_contradiction", False):
        return "REFINEMENT_SIGNAL_SUPPORTED"
    core_has_evidence = bool(by_dimension["PROBLEM_RECURRENCE"].get("evidence_ids") or by_dimension["REFINED_DIFFERENTIATION"].get("evidence_ids"))
    if ((recurrence_state == "MIXED" or difference_state == "MIXED") and core_has_evidence) or card.get("blocking_scope_contradiction", False):
        return "REFINEMENT_SIGNAL_MIXED"
    if recurrence_state == "WEAK" or difference_state == "WEAK":
        return "REFINEMENT_SIGNAL_WEAK"
    return "REFINEMENT_INSUFFICIENT_EVIDENCE"


def _qa_query_targets_concept(concept_id: str, query_text: str, targeting_rationale: str | None = None) -> bool:
    text = query_text.casefold()
    has_jobs = bool(re.search(r"\bjobs?(?:\s+reborn)?\b", text))
    if concept_id == "yee83-jobs-01-economy-consistency":
        has_money_boundary = bool(re.search(r"\b(vault|economy|provider|shop|transaction|payout|deposit|balance|reward|payment|money)\w*\b", text))
        rationale = (targeting_rationale or "").casefold()
        api_boundary_context = bool(re.search(r"\b(jobs?|reward|payout)\b", rationale)) and bool(re.search(r"\b(vault|provider|economy)\b", rationale))
        vault_contract = "vaultapi" in text and "economyresponse" in text and "transaction" in text and any(x in text for x in ("depositplayer", "withdrawalplayer")) and api_boundary_context
        return (has_jobs and has_money_boundary) or vault_contract
    if concept_id == "yee83-jobs-02-progress-recovery":
        has_progress_object = bool(re.search(r"\b(progress|level|experience|xp|save|saved|saving|persistence|restore|recovery)\w*\b", text))
        has_lifecycle = bool(re.search(r"\b(restart|reboot|disconnect|shutdown|crash|database|sqlite|mysql|storage|multi.server|lifecycle|migration|save|restore|recovery)\w*\b", text))
        return has_jobs and has_progress_object and has_lifecycle
    return False


def _qa_wedge_contract(wedge: dict[str, Any], evidence_ids: set[str]) -> bool:
    state = wedge.get("wedge_state")
    candidate = wedge.get("refined_wedge")
    references_valid = (set(wedge.get("evidence_ids", [])) | set(wedge.get("counterevidence_ids", []))) <= evidence_ids
    common = bool(wedge.get("basis") and wedge.get("scope_boundary") and references_valid)
    if state == "NO_DEFENSIBLE_WEDGE":
        return common and candidate is None
    if state == "REFINED_WEDGE":
        return common and isinstance(candidate, str) and bool(candidate.strip()) and bool(wedge.get("evidence_ids"))
    return False


def _qa_differentiation_state(assessment: dict[str, Any], evidence: dict[str, dict[str, Any]], sources: dict[str, dict[str, Any]]) -> str:
    if assessment.get("source_claimed_state") != "SUPPORTED":
        return assessment.get("source_claimed_state")
    cases = {evidence[eid]["source_id"] for eid in assessment.get("evidence_ids", []) if evidence[eid].get("evidence_type") == "OPERATOR_CASE"}
    incumbent = {evidence[eid]["source_id"] for eid in assessment.get("evidence_ids", []) if evidence[eid].get("evidence_type") in {"INCUMBENT_CAPABILITY", "COUNTEREVIDENCE"}}
    url_set = {canonical_url(sources[sid]["canonical_url"]) for sid in cases | incumbent}
    if cases and incumbent and len(url_set) >= 2 and not assessment.get("unexplained_material_counterevidence"):
        return "SUPPORTED"
    if cases and (incumbent or assessment.get("counterevidence_ids")):
        return "MIXED"
    if cases or incumbent:
        return "WEAK"
    return "INSUFFICIENT_EVIDENCE"


def _validate_sqlite(path: Path) -> tuple[str, int]:
    db = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    try:
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        foreign = len(db.execute("PRAGMA foreign_key_check").fetchall())
        return integrity, foreign
    finally:
        db.close()


def _reconcile_sqlite_and_exports(out: Path, rows: dict[str, list[dict[str, Any]]], sqlite_path: Path) -> bool:
    db = sqlite3.connect(f"file:{sqlite_path.resolve().as_posix()}?mode=ro", uri=True)
    try:
        expected_tables = {
            "metadata": rows["metadata"], "input_provenance": rows["input_provenance"],
            "authorized_concept_cohort": rows["authorized_concept_cohort"], "concepts": rows["concepts"],
            **{table: rows[row_key] for table, row_key in {
                "sources": "sources", "queries": "queries", "evidence": "evidence", "operator_cases": "operator_cases",
                "root_causes": "root_causes", "recurrence_assessments": "recurrence_assessments",
                "incumbent_capabilities": "incumbent_capabilities", "refined_wedges": "refined_wedges",
                "differentiation_assessments": "differentiation_assessments", "paid_value_observations": "paid_value_observations",
                "feasibility_observations": "feasibility_observations", "support_observations": "support_observations",
                "concept_refinement_cards": "concept_refinement_cards",
            }.items()},
            "supervisor_decision_template": rows["supervisor_decision_template"],
        }
        for table, expected in expected_tables.items():
            if db.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] != len(expected):
                return False
        if dict(db.execute("SELECT key,value FROM metadata")) != {r["key"]: r["value"] for r in rows["metadata"]}:
            return False
        if db.execute("SELECT input_name,sha256,read_only FROM input_provenance ORDER BY input_name").fetchall() != [
            (r["input_name"], r["sha256"], 1) for r in sorted(rows["input_provenance"], key=lambda item: item["input_name"])
        ]:
            return False
        if db.execute("SELECT concept_id,cohort_sha256 FROM authorized_concept_cohort ORDER BY concept_id").fetchall() != [
            (r["concept_id"], r["cohort_sha256"]) for r in sorted(rows["authorized_concept_cohort"], key=lambda item: item["concept_id"])
        ]:
            return False
        if db.execute("SELECT concept_id,concept_label,parent_work_order,plugin_only FROM concepts ORDER BY concept_id").fetchall() != [
            (r["concept_id"], r["concept_label"], r["parent_work_order"], 1) for r in sorted(rows["concepts"], key=lambda item: item["concept_id"])
        ]:
            return False
        json_tables = {
            "sources": "sources", "queries": "queries", "evidence": "evidence", "operator_cases": "operator_cases",
            "root_causes": "root_causes", "recurrence_assessments": "recurrence_assessments",
            "incumbent_capabilities": "incumbent_capabilities", "refined_wedges": "refined_wedges",
            "differentiation_assessments": "differentiation_assessments", "paid_value_observations": "paid_value_observations",
            "feasibility_observations": "feasibility_observations", "support_observations": "support_observations",
            "concept_refinement_cards": "concept_refinement_cards",
        }
        for table, row_key in json_tables.items():
            actual = sorted((json.loads(r[0]) for r in db.execute(f'SELECT record_json FROM "{table}"')), key=_json)
            expected = sorted(rows[row_key], key=_json)
            if actual != expected:
                return False
        if json.loads(db.execute("SELECT record_json FROM supervisor_decision_template WHERE singleton_id=1").fetchone()[0]) != rows["supervisor_decision_template"][0]:
            return False
        for filename, row_key in TABLE_EXPORTS.items():
            if (out / f"{filename}.jsonl").read_bytes() != _table_jsonl(rows[row_key]):
                return False
            if (out / f"{filename}.csv").read_bytes() != _table_csv(rows[row_key]):
                return False
        return True
    finally:
        db.close()


def _qa(capture: dict[str, Any], rows: dict[str, list[dict[str, Any]]], input_hashes: dict[str, str], input_paths: dict[str, Path], sqlite_path: Path, replay_identical: bool, execution_commit: str) -> dict[str, Any]:
    failed: list[str] = []
    cards = rows["concept_refinement_cards"]
    mandatory = [q for q in capture["queries"] if q["query_kind"] == "MANDATORY"]
    if len(cards) != 2 or [c["concept_id"] for c in cards] != sorted(CONCEPTS): failed.append("exact_two_cards_stable_order")
    if cohort_sha([c["concept_id"] for c in cards]) != COHORT_SHA256: failed.append("cohort_sha")
    if set(capture["excluded_y83_concept_ids"]) & {c["concept_id"] for c in cards}: failed.append("dropped_concept_leakage")
    if len(mandatory) != 16 or any(sum(q["concept_id"] == cid for q in mandatory) != 8 for cid in CONCEPTS): failed.append("mandatory_query_coverage")
    if any({q["purpose"] for q in mandatory if q["concept_id"] == cid} != set(PURPOSES) for cid in CONCEPTS): failed.append("mandatory_query_purposes")
    for cid in CONCEPTS:
        primary = [q for q in mandatory if q["concept_id"] == cid and q["purpose"] in {"OPERATOR_RECURRENCE_A", "OPERATOR_RECURRENCE_B"}]
        for failed_query in (q for q in primary if q["new_independent_case_found"] is False):
            fu = [q for q in capture["queries"] if q["concept_id"] == cid and q["query_kind"] == "FOLLOWUP"
                  and q["purpose"] == failed_query["purpose"] and q["execution_order"] > failed_query["execution_order"]]
            if len(fu) < 2 or len({q["query_text"] for q in fu}) < 2: failed.append(f"recurrence_followups:{cid}:{failed_query['purpose']}")
        oracle = _qa_oracle_recurrence(capture["operator_cases"], cid)
        actual = next(r for r in rows["recurrence_assessments"] if r["concept_id"] == cid)
        for field, key in (("state", "recurrence_state"), ("independent_case_count", "independent_case_count"), ("canonical_url_count", "canonical_url_count"), ("distinct_environment_count", "distinct_environment_count"), ("provider_stack_count", "provider_stack_count"), ("cross_product_status", "cross_product_status"), ("cross_stack_status", "cross_stack_status"), ("current_or_recent_case_count", "current_or_recent_case_count"), ("historical_case_count", "historical_case_count")):
            if oracle[field] != actual[key]: failed.append(f"independent_recurrence_oracle:{cid}:{field}")
    query_by_id = {q["query_id"]: q for q in capture["queries"]}
    cases_by_id = {c["case_id"]: c for c in capture["operator_cases"]}
    for case in capture["operator_cases"]:
        query_id = case.get("discovered_by_query_id")
        if case.get("case_origin") == "ACCEPTED_UPSTREAM":
            if query_id is not None: failed.append(f"accepted_case_relabelled:{case['case_id']}")
        elif case.get("case_origin") != "TARGETED_RESEARCH" or query_id not in query_by_id:
            failed.append(f"case_first_discovery_missing:{case['case_id']}")
        else:
            q = query_by_id[query_id]
            earliest = min((candidate["execution_order"] for candidate in capture["queries"]
                            if candidate["concept_id"] == case["concept_id"] and case["source_id"] in candidate["opened_source_ids"]), default=None)
            if q["concept_id"] != case["concept_id"] or case["source_id"] not in q["opened_source_ids"] or q["execution_order"] != earliest:
                failed.append(f"case_first_discovery_order:{case['case_id']}")
    expected_new_case_queries = {case.get("discovered_by_query_id") for case in capture["operator_cases"]
                                 if case.get("case_origin") == "TARGETED_RESEARCH" and case.get("supports_failure_class") is True}
    if any(q["new_independent_case_found"] != (q["query_id"] in expected_new_case_queries) for q in capture["queries"]):
        failed.append("query_case_discovery_reconciliation")
    if any(not _qa_query_targets_concept(q["concept_id"], q["query_text"], q.get("targeting_rationale")) for q in capture["queries"]):
        failed.append("independent_concept_targeted_query_guard")
    by_source = {s["source_id"]: s for s in capture["sources"]}
    evidence_by_id = {e["evidence_id"]: e for e in capture["evidence"]}
    by_query = {q["query_id"] for q in capture["queries"]}
    if any(by_source.get(e["source_id"], {}).get("access_status") != "OPENED" or not set(e["query_ids"]) <= by_query or e["used_search_snippet"] for e in capture["evidence"]): failed.append("opened_source_query_provenance")
    for paid in capture["paid_value_observations"]:
        if paid["evidence_type"] == "PAID_COMPETITOR_PRECEDENT" and paid.get("is_direct_wtp") is not False: failed.append("price_is_not_wtp")
        if paid["evidence_type"] == "PAID_COMPETITOR_PRECEDENT" and any(paid.get(key) is not None for key in ("converted_amount", "converted_currency", "exchange_rate", "currency_conversion")): failed.append("price_currency_conversion")
    if len(rows["refined_wedges"]) != 2 or {r["concept_id"] for r in rows["refined_wedges"]} != set(CONCEPTS): failed.append("exact_wedge_record_coverage")
    if any(not _qa_wedge_contract(w, set(evidence_by_id)) for w in rows["refined_wedges"]): failed.append("wedge_contract_paths")
    root_by_case = {r["case_id"]: r for r in rows["root_causes"]}
    if any(case_id not in root_by_case or root_by_case[case_id]["root_cause_state"] != case["root_cause_state"] or root_by_case[case_id]["current_applicability_state"] != case["current_applicability_state"] for case_id, case in cases_by_id.items()): failed.append("root_cause_counterevidence_preservation")
    actual_diff = {r["concept_id"]: r for r in rows["differentiation_assessments"]}
    source_by_id = {s["source_id"]: s for s in capture["sources"]}
    for raw in capture["differentiation_assessments"]:
        output = actual_diff.get(raw["concept_id"], {})
        if output.get("source_claimed_state") != raw["state"] or output.get("state") != _qa_differentiation_state(output, evidence_by_id, source_by_id):
            failed.append(f"independent_differentiation_gate:{raw['concept_id']}")
        if not set(raw.get("counterevidence_ids", [])) <= set(evidence_by_id): failed.append(f"counterevidence_preserved:{raw['concept_id']}")
    for card in cards:
        if len(card["dimension_assessments"]) != 7 or {d["dimension"] for d in card["dimension_assessments"]} != set(DIMENSIONS): failed.append(f"seven_dimensions:{card['concept_id']}")
        if card["overall_refinement_state"] != _qa_overall_oracle(card): failed.append(f"independent_overall_oracle:{card['concept_id']}")
        forbidden = _forbidden_fields(card)
        if forbidden: failed.append(f"forbidden_fields:{card['concept_id']}:{','.join(sorted(forbidden))}")
    db_integrity, fk_violations = _validate_sqlite(sqlite_path)
    reconcile_ok = _reconcile_sqlite_and_exports(sqlite_path.parent, rows, sqlite_path)
    if db_integrity != "ok": failed.append("sqlite_integrity")
    if fk_violations: failed.append("sqlite_foreign_keys")
    if not reconcile_ok: failed.append("jsonl_csv_sqlite_reconciliation")
    after_hashes = {name: sha256_file(path) for name, path in sorted(input_paths.items())}
    if after_hashes != input_hashes: failed.append("upstream_immutability")
    if not replay_identical: failed.append("frozen_capture_replay")
    decision = rows["supervisor_decision_template"][0]
    if decision.get("decision_status") != "UNDECIDED" or any(decision.get(k) for k in ("advance_to_product_spec_concept_ids", "request_further_refinement_concept_ids", "held_concept_ids", "dropped_concept_ids")) or decision.get("build_none") is not False or decision.get("rationale") is not None or decision.get("decided_at") is not None: failed.append("decision_template_unmodified")
    return {
        "work_order": WORK_ORDER, "status": "PASS" if not failed else "FAIL", "failed_checks": failed,
        "authorized_concept_count": len(cards), "concept_ids": [c["concept_id"] for c in cards], "cohort_sha256": COHORT_SHA256,
        "mandatory_query_count": len(mandatory), "mandatory_queries_per_concept": {cid: sum(q["concept_id"] == cid for q in mandatory) for cid in sorted(CONCEPTS)},
        "followup_query_count": sum(q["query_kind"] == "FOLLOWUP" for q in capture["queries"]),
        "new_case_discovery_query_ids": sorted(expected_new_case_queries),
        "source_document_count": len(capture["sources"]), "opened_source_count": sum(s["access_status"] == "OPENED" for s in capture["sources"]),
        "evidence_count": len(capture["evidence"]), "operator_case_count": len(capture["operator_cases"]),
        "new_independent_case_query_count": len(expected_new_case_queries),
        "wedge_state_counts": {state: sum(r["wedge_state"] == state for r in rows["refined_wedges"]) for state in sorted(WEDGE_STATES)},
        "wedge_dispositions": [{"concept_id": r["concept_id"], "wedge_state": r["wedge_state"], "refined_wedge": r["refined_wedge"]} for r in rows["refined_wedges"]],
        "differentiation_downgrade_count": sum(r["source_claimed_state"] != r["state"] for r in rows["differentiation_assessments"]),
        "recurrence": [{"concept_id": r["concept_id"], "state": r["recurrence_state"], "independent_case_count": r["independent_case_count"], "canonical_url_count": r["canonical_url_count"], "distinct_environment_count": r["distinct_environment_count"], "provider_stack_count": r["provider_stack_count"], "current_or_recent_case_count": r["current_or_recent_case_count"], "historical_case_count": r["historical_case_count"], "cross_product_status": r["cross_product_status"], "cross_stack_status": r["cross_stack_status"]} for r in rows["recurrence_assessments"]],
        "overall_states": [{"concept_id": c["concept_id"], "state": c["overall_refinement_state"]} for c in cards],
        "paid_value_types": sorted({row["evidence_type"] for row in rows["paid_value_observations"]}),
        "direct_wtp_observation_count": sum(row["evidence_type"] == "DIRECT_WTP_STATEMENT" for row in rows["paid_value_observations"]),
        "observed_purchase_or_payment_count": sum(row["evidence_type"] == "OBSERVED_PURCHASE_OR_PAYMENT" for row in rows["paid_value_observations"]),
        "input_sha256_before": input_hashes, "input_sha256_after": after_hashes,
        "sqlite_integrity_check": db_integrity, "sqlite_foreign_key_violations": fk_violations,
        "jsonl_csv_sqlite_reconciliation": reconcile_ok,
        "frozen_capture_replay_byte_identical": replay_identical, "execution_commit": execution_commit,
        "prohibited_scope_executed": False,
    }


def build_bundle(input_paths: dict[str, Path], capture_path: Path, alignment_path: Path, output_dir: Path, execution_commit: str, pull_request_url: str, drive_folder_url: str) -> dict[str, Any]:
    hashes = validate_input_hashes(input_paths)
    for name, path in input_paths.items():
        if name.endswith("sqlite"):
            db = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
            try:
                if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise RefinementError(f"Pinned SQLite is not intact: {name}")
            finally:
                db.close()
    if execution_commit != "" and len(execution_commit) != 40:
        raise RefinementError("Execution commit must be a full 40-character Git SHA")
    capture = json.loads(capture_path.read_text(encoding="utf-8-sig"))
    accepted_capture = json.loads(input_paths["yee83_capture"].read_text(encoding="utf-8-sig"))
    _normalize_case_discovery(capture, accepted_capture)
    _validate_capture(capture)
    yee83 = sqlite3.connect(f"file:{input_paths['yee83_sqlite'].resolve().as_posix()}?mode=ro", uri=True)
    try:
        ids = [r[0] for r in yee83.execute("SELECT concept_id FROM concept_validation_cards ORDER BY concept_id")]
    finally:
        yee83.close()
    if hashlib.sha256(("\n".join(ids) + "\n").encode()).hexdigest() != YEE83_CONCEPT_ID_SET_SHA256:
        raise RefinementError("Accepted YEE-83 concept-id-set hash mismatch")
    accepted_cards = {r["concept_id"]: r for r in accepted_capture.get("concepts", [])}
    for card in capture["concepts"]:
        parent = accepted_cards.get(card["concept_id"])
        if not parent or any(card.get(local) != parent.get(upstream) for local, upstream in (
            ("concept_label", "concept_label"), ("parent_problem_statement", "problem_statement"),
            ("parent_purchase_trigger", "purchase_trigger_hypothesis"), ("parent_paid_value_hypothesis", "paid_value_exchange_hypothesis"),
        )):
            raise RefinementError(f"Parent concept changed from accepted YEE-83: {card['concept_id']}")
    metadata = {
        "work_order": WORK_ORDER, "capture_version": CAPTURE_VERSION, "baseline_commit": BASELINE_COMMIT,
        "accepted_y83_execution_commit": YEE83_EXECUTION_COMMIT, "execution_commit": execution_commit,
        "pull_request_url": pull_request_url, "drive_folder_url": drive_folder_url,
        "cohort_sha256": COHORT_SHA256, "yee83_concept_id_set_sha256": YEE83_CONCEPT_ID_SET_SHA256,
        "yee83_frozen_capture_sha256": YEE83_CAPTURE_SHA256, "product_scope": "PLUGIN_ONLY",
        **{f"input_sha256:{k}": v for k, v in hashes.items()},
    }
    rows = _records(capture, metadata)
    output_dir.mkdir(parents=True, exist_ok=True)
    core = _write_core(output_dir, alignment_path, rows, capture)
    core_hashes = {p.name: sha256_file(p) for p in core}
    with tempfile.TemporaryDirectory(prefix="yee95-replay-") as temp_name:
        replay_dir = Path(temp_name)
        replay_core = _write_core(replay_dir, alignment_path, rows, capture)
        replay_identical = {p.name: sha256_file(p) for p in replay_core} == core_hashes
    qa = _qa(capture, rows, hashes, input_paths, output_dir / "concept_refinement.sqlite", replay_identical, execution_commit)
    (output_dir / "QA_RESULT.json").write_text(_json(qa) + "\n", encoding="utf-8", newline="\n")
    if qa["status"] != "PASS":
        raise RefinementError("Production QA failed: " + ", ".join(qa["failed_checks"]))
    report = _final_report(qa, execution_commit, pull_request_url, drive_folder_url)
    (output_dir / "FINAL_REPORT.md").write_text(report, encoding="utf-8", newline="\n")
    manifest_entries = []
    for path in sorted(p for p in output_dir.iterdir() if p.is_file() and p.name != "DATASET_MANIFEST.json"):
        manifest_entries.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path)})
    manifest = {
        "work_order": WORK_ORDER, "status": "PASS", "baseline_commit": BASELINE_COMMIT,
        "execution_commit": execution_commit, "pull_request_url": pull_request_url,
        "drive_folder_url": drive_folder_url, "cohort_sha256": COHORT_SHA256,
        "input_sha256": hashes, "files": manifest_entries,
        "manifest_self_hash_excluded": True,
    }
    (output_dir / "DATASET_MANIFEST.json").write_text(_json(manifest) + "\n", encoding="utf-8", newline="\n")
    return qa


def _final_report(qa: dict[str, Any], execution_commit: str, pull_request_url: str, drive_folder_url: str) -> str:
    recurrence = "\n".join(
        f"- `{r['concept_id']}`: {r['state']}; {r['independent_case_count']} independent groups / {r['canonical_url_count']} canonical URLs / "
        f"{r['distinct_environment_count']} environments / {r['provider_stack_count']} stack contexts; "
        f"current/recent={r['current_or_recent_case_count']}, historical={r['historical_case_count']}; "
        f"cross-product={r['cross_product_status']}, cross-stack={r['cross_stack_status']}."
        for r in qa["recurrence"]
    )
    states = "\n".join(f"- `{r['concept_id']}`: {r['state']}." for r in qa["overall_states"])
    input_hashes = "\n".join(f"- `{name}`: `{digest}`" for name, digest in sorted(qa["input_sha256_before"].items()))
    paid_types = qa["paid_value_types"]
    wedges = "\n".join(
        f"- `{r['concept_id']}`: `{r['wedge_state']}`" + (f" — {r['refined_wedge']}" if r["refined_wedge"] else ".")
        for r in qa["wedge_dispositions"]
    )
    return (
        "# YEE-95 Final Report\n\n"
        "## Status\n\n"
        "`TARGETED_CONCEPT_REFINEMENT_READY_FOR_SUPERVISOR_REVIEW` — evidence dossier complete; no product decision made.\n\n"
        f"- Baseline: `{BASELINE_COMMIT}`\n- Execution commit: `{execution_commit}`\n"
        f"- PR: {pull_request_url}\n- Drive folder: {drive_folder_url}\n"
        f"- Exact cohort: {qa['authorized_concept_count']} concepts; SHA-256 `{COHORT_SHA256}`\n"
        f"- Targeted research: {qa['mandatory_query_count']} mandatory queries plus {qa['followup_query_count']} recurrence follow-ups.\n"
        f"- Opened canonical source documents: {qa['opened_source_count']} / {qa['source_document_count']}; evidence records: {qa['evidence_count']}; operator cases: {qa['operator_case_count']}.\n\n"
        "## Recurrence and disposition\n\n" + recurrence + "\n\n" + states + "\n\n"
        "## Paid-value evidence types\n\n"
        f"Observed evidence types: {', '.join(paid_types)}. There is no `DIRECT_WTP_STATEMENT` and no verified purchase/payment. Competitor-listing price is kept as `PAID_COMPETITOR_PRECEDENT`, not WTP.\n\n"
        "Refined-wedge dispositions (the pipeline accepts either path and does not default to a negative state):\n\n" + wedges + "\n\n"
        "This is not a recommendation to abandon either direction. No direct WTP or observed purchase was found; public competitor pricing and custom commissioning remain explicitly limited proxies.\n\n"
        "## QA and input integrity\n\n"
        f"QA: `{qa['status']}`; SQLite `integrity_check={qa['sqlite_integrity_check']}`, FK violations `{qa['sqlite_foreign_key_violations']}`; frozen replay byte-identical `{str(qa['frozen_capture_replay_byte_identical']).lower()}`.\n\n"
        "Pinned input SHA-256 values (before and after are identical):\n\n" + input_hashes + "\n\n"
        "Reports from unresolved trackers remain anecdotal and are not treated as confirmed defects. Search snippets were discovery-only. No ranking, score, winner, Product Spec, implementation, or broader market rediscovery was performed.\n"
    )
