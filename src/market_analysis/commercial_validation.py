"""YEE-55 bounded commercial-validation pilot and deterministic exports."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sqlite3
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

WORK_ORDER = "YEE-55"
SCHEMA_VERSION = "yee-55-deep-commercial-validation-v0.1"
CAPTURE_VERSION = "yee-55-pilot-capture-v0.1"
EXPECTED_HASHES = {
    "next_validation_cohort": "e4137b27991640c3b12b6cfa3b66fc52ae533b65c5b7beaa066d7e44604da624",
    "opportunity_synthesis_db": "7fc256ee1c4649e56c274f39e2a321538758dff780d5ae196b277c67747fedd6",
    "yee47_external_research_db": "cce8b15c06b15a1094864ddd40653b9af7b185414a1207c25663f0f5dd55810f",
}
PILOT_FAMILIES = (
    (1, 1, "family_188439fdf256290b518101b998fcc1796fec6d84387beed586a81728b8978a4b"),
    (3, 2, "family_39b0610ee5b1f2bb0e371ad55075469dd18d86f57809972bcb967c45ecb26b48"),
    (4, 3, "family_a6ba31d88da34303daa3d4011e228f234a99de9f80cc40c4bcbe5554dcf99e6f"),
    (7, 4, "family_fde65554a094d6f5bf491eb9779a0adceeaeee8ca61501aab4b5475c2810486f"),
    (8, 5, "family_183dadbeb21f00e17773add50166a0c2b2257a1d884e5aef6c552d04c02ab8ca"),
)
PILOT_IDS = tuple(item[2] for item in PILOT_FAMILIES)
QUERY_PURPOSES = {
    "buyer/job validation", "pain prevalence discovery", "paid alternatives/pricing", "feasibility support",
}
PRICE_STATUSES = {
    "PAID_PRICE_VERIFIED", "FREEMIUM_PRICE_VERIFIED", "MONETIZED_PRICE_NOT_VISIBLE",
    "FREE_VERIFIED", "UNKNOWN",
}
CELL_STATUSES = {"EVIDENCED_PRESENT", "EXPLICITLY_UNSUPPORTED", "NOT_ESTABLISHED", "NOT_APPLICABLE"}
FEASIBILITY_STATUSES = {"COMPLETE", "PARTIAL", "NOT_ESTABLISHED"}
SAMPLE_STATUSES = {"REPEATED_SAMPLE", "LIMITED_SAMPLE", "NOT_ESTABLISHED"}
NULL_TOKEN = r"\N"

EVIDENCE_COLUMNS = (
    "evidence_id", "family_id", "deep_validation_order", "dimension", "claim_type", "source_url",
    "canonical_url", "domain", "title", "source_type", "retrieved_at", "published_or_updated_at",
    "observation", "entity_name", "numeric_value", "numeric_min", "numeric_max", "numeric_unit", "currency", "notes", "query_id",
)
PAIN_COLUMNS = (
    "cluster_id", "family_id", "deep_validation_order", "pain_theme", "sample_status",
    "observation_count", "source_page_count", "evidence_ids", "sample_caveat",
)
ALTERNATIVE_COLUMNS = (
    "alternative_id", "family_id", "deep_validation_order", "entity_name", "canonical_url",
    "relation_type", "product_type", "buyer_job", "monetization_status", "price_amount",
    "price_min", "price_max", "currency", "price_unit", "pricing_notes", "evidence_ids",
)
DIFFERENTIATION_COLUMNS = (
    "hypothesis_id", "family_id", "deep_validation_order", "comparison_target", "feature_axis",
    "target_cell", "comparison_cell", "candidate_hypothesis", "evidence_ids",
)
QUERY_COLUMNS = (
    "query_id", "family_id", "deep_validation_order", "query_sequence", "query_text", "issued_at",
    "purpose", "result_action",
)
TABLE_COLUMNS = {
    "commercial_validation_packs": None,
    "validation_evidence": EVIDENCE_COLUMNS,
    "pain_clusters": PAIN_COLUMNS,
    "market_alternatives": ALTERNATIVE_COLUMNS,
    "differentiation_hypotheses": DIFFERENTIATION_COLUMNS,
    "validation_queries": QUERY_COLUMNS,
}
JSON_FIELDS = {
    "member_topic_keys", "aliases", "candidate_classes", "evidence_count_by_claim_type",
    "evidence_count_by_source_type", "evidence_gap_flags", "next_validation_requirements",
    "source_presence", "competitor_count_by_relation_type", "buyer_job_hypotheses", "pain_clusters",
    "differentiation_hypotheses", "feasibility_facts", "inherited_yee47_evidence_ids",
    "yee55_evidence_ids", "dimension_statuses", "evidence_ids", "retained_alternative_ids",
}


class CommercialValidationError(ValueError):
    """Raised when canonical inputs or captured evidence violate the pilot contract."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_id(prefix: str, *parts: str) -> str:
    value = "\0".join(parts).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(value).hexdigest()[:20]}"


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
    query = urlencode(sorted((k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
                             if not k.casefold().startswith("utm_") and k.casefold() not in {"ref", "source", "fbclid", "gclid"}))
    return urlunsplit((parsed.scheme.lower(), netloc, path, query, ""))


def _timestamp(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise CommercialValidationError(f"invalid ISO timestamp for {field}: {value!r}") from exc
    if parsed.tzinfo is None:
        raise CommercialValidationError(f"{field} must include a timezone")
    utc = parsed.astimezone(timezone.utc)
    precision = "microseconds" if utc.microsecond else "seconds"
    return utc.isoformat(timespec=precision).replace("+00:00", "Z")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        raise CommercialValidationError(f"unable to read canonical JSONL: {path}") from exc


def _readonly_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _load_canonical_inputs(cohort_path: Path, synthesis_db_path: Path, yee47_db_path: Path):
    paths = {
        "next_validation_cohort": cohort_path.resolve(),
        "opportunity_synthesis_db": synthesis_db_path.resolve(),
        "yee47_external_research_db": yee47_db_path.resolve(),
    }
    if any(not path.is_file() for path in paths.values()):
        raise FileNotFoundError("one or more accepted YEE-54/YEE-47 input files are missing")
    before = {name: _sha256(path) for name, path in paths.items()}
    if before != EXPECTED_HASHES:
        raise CommercialValidationError(f"accepted input SHA-256 mismatch: {before}")
    cohort = _read_jsonl(paths["next_validation_cohort"])
    if len(cohort) != 15 or [row.get("deep_validation_order") for row in cohort] != list(range(1, 16)):
        raise CommercialValidationError("accepted YEE-54 COHORT_A input must contain ordered deep_validation_order 1..15")
    if any(row.get("validation_cohort") != "COHORT_A" for row in cohort):
        raise CommercialValidationError("accepted YEE-54 input contains a non-COHORT_A row")
    by_order = {row["deep_validation_order"]: row for row in cohort}
    for rank, order, family_id in PILOT_FAMILIES:
        row = by_order.get(order)
        if not row or row.get("family_id") != family_id or row.get("consensus_rank") != rank:
            raise CommercialValidationError(f"authorized YEE-55 pilot identity mismatch at order {order}")

    for key in ("opportunity_synthesis_db", "yee47_external_research_db"):
        connection = _readonly_connection(paths[key])
        try:
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise CommercialValidationError(f"canonical {key} failed SQLite integrity_check")
            if key == "yee47_external_research_db":
                inherited = {}
                for family_id in PILOT_IDS:
                    inherited[family_id] = [row[0] for row in connection.execute(
                        "SELECT evidence_id FROM external_evidence WHERE family_id=? ORDER BY evidence_id", (family_id,)
                    )]
                    if not inherited[family_id]:
                        raise CommercialValidationError(f"accepted YEE-47 evidence missing for {family_id}")
            else:
                stored_rows = connection.execute(
                    "SELECT family_id,consensus_rank,deep_validation_order,validation_cohort "
                    "FROM next_validation_cohort ORDER BY deep_validation_order"
                ).fetchall()
                if len(stored_rows) != 15:
                    raise CommercialValidationError("YEE-54 SQLite next_validation_cohort must contain 15 canonical rows")
                fields = ("family_id", "consensus_rank", "deep_validation_order", "validation_cohort")
                for source, stored in zip(cohort, stored_rows):
                    if tuple(stored) != tuple(source.get(field) for field in fields):
                        raise CommercialValidationError("YEE-54 cohort JSONL/SQLite identity fields do not reconcile")
        finally:
            connection.close()
    after = {name: _sha256(path) for name, path in paths.items()}
    if before != after:
        raise CommercialValidationError("canonical YEE-54/YEE-47 inputs changed during read-only preflight")
    return [by_order[order] for _, order, _ in PILOT_FAMILIES], inherited, paths, before


def _resolve_refs(refs: Sequence[str], id_map: Mapping[str, str], family_id: str) -> list[str]:
    resolved = []
    for ref in refs:
        if ref not in id_map:
            raise CommercialValidationError(f"unknown evidence reference {ref!r} for {family_id}")
        resolved.append(id_map[ref])
    return list(dict.fromkeys(resolved))


def _normalize_capture(capture: Mapping[str, Any], canonical_rows: Sequence[Mapping[str, Any]], inherited: Mapping[str, Sequence[str]]):
    if capture.get("capture_version") != CAPTURE_VERSION:
        raise CommercialValidationError(f"capture_version must equal {CAPTURE_VERSION}")
    families = {row["family_id"]: row for row in canonical_rows}
    family_capture = {row["family_id"]: row for row in capture.get("families", [])}
    if len(capture.get("families", [])) != 5 or set(family_capture) != set(PILOT_IDS):
        raise CommercialValidationError("capture family membership must equal the exact authorized 5-family pilot")

    query_ids: dict[str, str] = {}
    query_rows: list[dict[str, Any]] = []
    query_by_id: dict[str, dict[str, Any]] = {}
    seq_global = 0
    queries_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in capture.get("queries", []):
        family_id = raw.get("family_id")
        if family_id not in families or raw.get("purpose") not in QUERY_PURPOSES:
            raise CommercialValidationError(f"invalid query family or purpose: {raw}")
        sequence = raw.get("sequence")
        if not isinstance(sequence, int) or sequence < 1:
            raise CommercialValidationError("query sequence must be a positive per-family integer")
        query_id = _stable_id("q55", family_id, str(sequence), raw.get("query_text", ""))
        query = {
            "query_id": query_id, "family_id": family_id,
            "deep_validation_order": families[family_id]["deep_validation_order"],
            "query_sequence": sequence, "query_text": raw.get("query_text"),
            "issued_at": _timestamp(raw.get("issued_at"), "query.issued_at"),
            "purpose": raw["purpose"], "result_action": raw.get("result_action"),
        }
        if not query["query_text"] or not query["result_action"]:
            raise CommercialValidationError("each query requires text and a result_action")
        if raw.get("capture_id") in query_ids:
            raise CommercialValidationError("duplicate query capture_id")
        query_ids[raw["capture_id"]] = query_id
        seq_global += 1
        query_rows.append(query)
        query_by_id[raw["capture_id"]] = query
        queries_by_family[family_id].append(query)
    for family_id in PILOT_IDS:
        family_queries = sorted(queries_by_family[family_id], key=lambda row: row["query_sequence"])
        if [row["query_sequence"] for row in family_queries] != list(range(1, len(family_queries) + 1)):
            raise CommercialValidationError(f"query sequence is not contiguous for {family_id}")
        if len(family_queries) > 20:
            raise CommercialValidationError(f"query budget exceeded for {family_id}")
        if len({row["query_text"] for row in family_queries}) != len(family_queries):
            raise CommercialValidationError(f"duplicate search query text for {family_id}")
        purposes = Counter(row["purpose"] for row in family_queries)
        for purpose, minimum in (("buyer/job validation", 1), ("pain prevalence discovery", 3),
                                 ("paid alternatives/pricing", 3), ("feasibility support", 1)):
            if purposes[purpose] < minimum:
                raise CommercialValidationError(f"{family_id} has fewer than {minimum} {purpose} queries")

    pages_by_ref: dict[str, dict[str, Any]] = {}
    page_count_by_family: Counter[str] = Counter()
    for raw in capture.get("opened_pages", []):
        family_id = raw.get("family_id")
        if family_id not in families or raw.get("query_ref") not in query_ids:
            raise CommercialValidationError("opened page has unknown family or query reference")
        if query_by_id[raw["query_ref"]]["family_id"] != family_id:
            raise CommercialValidationError("opened page cannot reference another family's query")
        page_id = raw.get("capture_id")
        if not page_id or page_id in pages_by_ref:
            raise CommercialValidationError("opened page capture_id must be unique")
        canonical = canonicalize_url(raw.get("source_url", ""))
        domain = (urlsplit(canonical).hostname or "").casefold()
        if not raw.get("source_title") or not raw.get("source_type"):
            raise CommercialValidationError("opened page requires title and source_type")
        page = dict(raw)
        page.update({
            "canonical_url": canonical, "domain": domain,
            "retrieved_at": _timestamp(raw.get("retrieved_at"), "page.retrieved_at"),
            "published_or_updated_at": raw.get("published_or_updated_at"),
            "query_id": query_ids[raw["query_ref"]],
        })
        if page["published_or_updated_at"]:
            # Source-native relative freshness labels remain text rather than being fabricated into dates.
            page["published_or_updated_at"] = str(page["published_or_updated_at"])
        pages_by_ref[page_id] = page
        page_count_by_family[family_id] += 1
    if any(page_count_by_family[family_id] > 25 for family_id in PILOT_IDS):
        raise CommercialValidationError("retained opened-page budget exceeds 25 for a family")

    evidence_rows: list[dict[str, Any]] = []
    evidence_ids: dict[str, str] = {}
    evidence_raw_by_id: dict[str, dict[str, Any]] = {}
    for raw in capture.get("evidence", []):
        family_id = raw.get("family_id")
        page_ref = raw.get("page_ref")
        if family_id not in families or page_ref not in pages_by_ref:
            raise CommercialValidationError("evidence must reference an opened source page")
        page = pages_by_ref[page_ref]
        if page["family_id"] != family_id:
            raise CommercialValidationError("evidence cannot borrow another family's opened page")
        if not raw.get("observation") or not raw.get("dimension") or not raw.get("claim_type"):
            raise CommercialValidationError("evidence requires dimension, claim_type and observation")
        evidence_id = _stable_id("y55ev", family_id, raw.get("capture_id", ""), page["canonical_url"])
        if raw.get("capture_id") in evidence_ids:
            raise CommercialValidationError("duplicate evidence capture_id")
        evidence_ids[raw["capture_id"]] = evidence_id
        row = {
            "evidence_id": evidence_id, "family_id": family_id,
            "deep_validation_order": families[family_id]["deep_validation_order"],
            "dimension": raw["dimension"], "claim_type": raw["claim_type"],
            "source_url": page["source_url"], "canonical_url": page["canonical_url"],
            "domain": page["domain"], "title": page["source_title"], "source_type": page["source_type"],
            "retrieved_at": page["retrieved_at"],
            "published_or_updated_at": raw.get("published_or_updated_at", page["published_or_updated_at"]),
            "observation": raw["observation"], "entity_name": raw.get("entity_name"),
            "numeric_value": raw.get("numeric_value"), "numeric_min": raw.get("numeric_min"),
            "numeric_max": raw.get("numeric_max"), "numeric_unit": raw.get("numeric_unit"),
            "currency": raw.get("currency"), "notes": raw.get("notes"), "query_id": page["query_id"],
        }
        evidence_rows.append(row)
        evidence_raw_by_id[raw["capture_id"]] = {**raw, "evidence_id": evidence_id, "page": page}
    evidence_by_id = {row["evidence_id"]: row for row in evidence_rows}
    evidence_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evidence_rows:
        evidence_by_family[row["family_id"]].append(row)

    pain_rows: list[dict[str, Any]] = []
    alternative_rows: list[dict[str, Any]] = []
    differentiation_rows: list[dict[str, Any]] = []
    pack_rows: list[dict[str, Any]] = []
    alternatives_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    diff_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    pain_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for rank, order, family_id in PILOT_FAMILIES:
        raw_family = family_capture[family_id]
        source_row = families[family_id]
        evidence_ref_ids = [row["evidence_id"] for row in evidence_by_family[family_id]]
        dimensions = Counter(row["dimension"] for row in evidence_by_family[family_id])
        if dimensions["buyer_job"] < 2:
            raise CommercialValidationError(f"{family_id} needs at least two buyer/job evidence items when available")
        if dimensions["pain_point"] < 3:
            raise CommercialValidationError(f"{family_id} has fewer than three retained pain observations")
        if dimensions["pain_point"] > 10:
            raise CommercialValidationError(f"{family_id} exceeds the ten-observation pain evidence cap")
        if dimensions["feasibility"] < 2:
            raise CommercialValidationError(f"{family_id} has fewer than two feasibility evidence items")

        for raw_cluster in raw_family.get("pain_clusters", []):
            ids = _resolve_refs(raw_cluster.get("evidence_refs", []), evidence_ids, family_id)
            pages = {evidence_by_id[eid]["canonical_url"] for eid in ids}
            if not ids:
                raise CommercialValidationError("pain cluster must cite at least one observation")
            distinct_observations = {evidence_by_id[eid]["observation"].strip() for eid in ids}
            expected_sample = "REPEATED_SAMPLE" if len(distinct_observations) >= 3 and len(pages) >= 2 else "LIMITED_SAMPLE"
            requested_sample = raw_cluster.get("sample_status", expected_sample)
            if requested_sample == "REPEATED_SAMPLE" and expected_sample != "REPEATED_SAMPLE":
                raise CommercialValidationError("REPEATED_SAMPLE requires >=3 distinct observations across >=2 pages/threads")
            if requested_sample not in SAMPLE_STATUSES:
                raise CommercialValidationError("invalid pain cluster sample status")
            row = {
                "cluster_id": _stable_id("pain55", family_id, raw_cluster.get("pain_theme", "")),
                "family_id": family_id, "deep_validation_order": order,
                "pain_theme": raw_cluster.get("pain_theme"), "sample_status": requested_sample,
                "observation_count": len(ids), "source_page_count": len(pages),
                "evidence_ids": ids,
                "sample_caveat": "Independent public reports in this bounded sample; not a population estimate.",
            }
            pain_rows.append(row)
            pain_by_family[family_id].append(row)

        for raw_alt in raw_family.get("alternatives", []):
            ids = _resolve_refs(raw_alt.get("evidence_refs", []), evidence_ids, family_id)
            if not ids or raw_alt.get("monetization_status") not in PRICE_STATUSES:
                raise CommercialValidationError("alternative needs evidence and an explicit monetization status")
            if raw_alt["monetization_status"] in {"PAID_PRICE_VERIFIED", "FREEMIUM_PRICE_VERIFIED"} and raw_alt.get("price_amount") is None and raw_alt.get("price_min") is None:
                raise CommercialValidationError("verified paid pricing requires an amount or source-native range")
            price_evidence = [evidence_by_id[eid] for eid in ids if evidence_by_id[eid]["claim_type"] == "PRICING"]
            if raw_alt["monetization_status"] in {"PAID_PRICE_VERIFIED", "FREEMIUM_PRICE_VERIFIED"} and not price_evidence:
                raise CommercialValidationError("verified paid pricing requires a cited pricing evidence record")
            if raw_alt.get("price_amount") is not None and not any(
                    row["numeric_value"] == raw_alt["price_amount"] and row["currency"] == raw_alt.get("currency")
                    for row in price_evidence):
                raise CommercialValidationError("alternative amount/currency do not reconcile to source pricing evidence")
            if raw_alt.get("price_min") is not None and not any(
                    row["numeric_min"] == raw_alt["price_min"] and row["numeric_max"] == raw_alt.get("price_max")
                    and row["currency"] == raw_alt.get("currency") for row in price_evidence):
                raise CommercialValidationError("alternative range/currency do not reconcile to source pricing evidence")
            row = {
                "alternative_id": _stable_id("alt55", family_id, raw_alt.get("entity_name", ""), raw_alt.get("canonical_url", "")),
                "family_id": family_id, "deep_validation_order": order,
                "entity_name": raw_alt.get("entity_name"), "canonical_url": canonicalize_url(raw_alt.get("canonical_url", "")),
                "relation_type": raw_alt.get("relation_type"), "product_type": raw_alt.get("product_type"),
                "buyer_job": raw_alt.get("buyer_job"), "monetization_status": raw_alt["monetization_status"],
                "price_amount": raw_alt.get("price_amount"), "price_min": raw_alt.get("price_min"),
                "price_max": raw_alt.get("price_max"), "currency": raw_alt.get("currency"),
                "price_unit": raw_alt.get("price_unit"), "pricing_notes": raw_alt.get("pricing_notes"),
                "evidence_ids": ids,
            }
            if row["relation_type"] not in {"DIRECT", "SUBSTITUTE", "ADJACENT"}:
                raise CommercialValidationError("invalid alternative relation_type")
            alternative_rows.append(row)
            alternatives_by_family[family_id].append(row)

        for raw_diff in raw_family.get("differentiation_hypotheses", []):
            refs = raw_diff.get("evidence_refs", [])
            target_ids = _resolve_refs(raw_diff.get("target_evidence_refs", []), evidence_ids, family_id)
            comparison_ids = _resolve_refs(raw_diff.get("comparison_evidence_refs", []), evidence_ids, family_id)
            ids = _resolve_refs(refs, evidence_ids, family_id) if refs else list(dict.fromkeys(target_ids + comparison_ids))
            if len(target_ids) < 1 or len(comparison_ids) < 1 or not set(ids).issuperset(target_ids + comparison_ids):
                raise CommercialValidationError("differentiation candidate must cite target and competitor/pain evidence")
            if raw_diff.get("target_cell") not in CELL_STATUSES or raw_diff.get("comparison_cell") not in CELL_STATUSES:
                raise CommercialValidationError("invalid differentiation matrix cell status")
            row = {
                "hypothesis_id": _stable_id("diff55", family_id, raw_diff.get("feature_axis", ""), raw_diff.get("comparison_target", "")),
                "family_id": family_id, "deep_validation_order": order,
                "comparison_target": raw_diff.get("comparison_target"), "feature_axis": raw_diff.get("feature_axis"),
                "target_cell": raw_diff["target_cell"], "comparison_cell": raw_diff["comparison_cell"],
                "candidate_hypothesis": raw_diff.get("candidate_hypothesis"), "evidence_ids": ids,
            }
            differentiation_rows.append(row)
            diff_by_family[family_id].append(row)

        buyer_hypotheses = []
        for hypothesis in raw_family.get("buyer_job_hypotheses", []):
            ids = _resolve_refs(hypothesis.get("evidence_refs", []), evidence_ids, family_id)
            if not ids or not hypothesis.get("actor") or not hypothesis.get("situation_problem") or not hypothesis.get("desired_outcome"):
                raise CommercialValidationError("buyer/job hypothesis requires actor, situation/problem, outcome and evidence")
            buyer_hypotheses.append({
                "actor": hypothesis["actor"], "situation_problem": hypothesis["situation_problem"],
                "desired_outcome": hypothesis["desired_outcome"], "evidence_ids": ids,
                "status": "HYPOTHESIS_NOT_POPULATION_CLAIM",
            })
        validation_status = raw_family.get("validation_status", "COMPLETE_WITH_UNKNOWNS")
        if validation_status == "IDENTITY_CONFLICT":
            buyer_hypotheses = []
        elif not buyer_hypotheses:
            raise CommercialValidationError(f"{family_id} has no buyer/job hypothesis")

        feasibility_facts = []
        for fact in raw_family.get("feasibility_facts", []):
            ids = _resolve_refs(fact.get("evidence_refs", []), evidence_ids, family_id)
            if not fact.get("fact") or not ids:
                raise CommercialValidationError("feasibility facts require source evidence")
            feasibility_facts.append({"fact": fact["fact"], "evidence_ids": ids})
        feasibility_status = raw_family.get("feasibility_evidence_status")
        if feasibility_status not in FEASIBILITY_STATUSES:
            raise CommercialValidationError("invalid feasibility evidence status")
        if feasibility_status == "COMPLETE" and not {"platform", "dependency_integration", "implementation_source"}.issubset(
                {item.get("kind") for item in raw_family.get("feasibility_facts", [])}):
            raise CommercialValidationError("COMPLETE feasibility requires platform, dependency/integration and implementation/source facts")

        family_queries = queries_by_family[family_id]
        buyer_evidence = dimensions["buyer_job"]
        pain_ids = [row["evidence_id"] for row in evidence_by_family[family_id] if row["dimension"] == "pain_point"]
        pain_pages = {evidence_by_id[eid]["canonical_url"] for eid in pain_ids}
        pain_status = "REPEATED_SAMPLE" if len(pain_ids) >= 3 and len(pain_pages) >= 2 else ("LIMITED_SAMPLE" if pain_ids else "NOT_ESTABLISHED")
        paid_found = any(row["monetization_status"] in {"PAID_PRICE_VERIFIED", "FREEMIUM_PRICE_VERIFIED", "MONETIZED_PRICE_NOT_VISIBLE"}
                         for row in alternatives_by_family[family_id])
        paid_status = "ALTERNATIVES_LOCATED" if paid_found else "NONE_LOCATED_IN_BOUNDED_SEARCH"
        dimension_statuses = {
            "buyer_job": "EVIDENCED" if buyer_evidence >= 2 else "LIMITED_SAMPLE",
            "pain_prevalence_sample": pain_status,
            "paid_alternatives": paid_status,
            "differentiation": "CANDIDATE_HYPOTHESES_WITH_EVIDENCE" if diff_by_family[family_id] else "NOT_ESTABLISHED",
            "feasibility": feasibility_status,
        }
        if validation_status not in {"COMPLETE", "COMPLETE_WITH_UNKNOWNS", "IDENTITY_CONFLICT"}:
            raise CommercialValidationError("invalid validation_status")
        if validation_status == "COMPLETE" and "UNKNOWN" in str(raw_family.get("buyer_or_payer_role", "")):
            raise CommercialValidationError("COMPLETE status cannot hide an explicit unknown buyer/payer")
        if validation_status == "IDENTITY_CONFLICT":
            # Conflicted identities carry no commercial inference rows.
            if buyer_hypotheses or alternatives_by_family[family_id] or diff_by_family[family_id]:
                raise CommercialValidationError("identity-conflicted families cannot retain product-specific commercial inferences")
        reported_paid_status = raw_family.get("paid_alternative_search_status", paid_status)
        if reported_paid_status != paid_status:
            raise CommercialValidationError(f"paid alternative search status disagrees with retained alternatives for {family_id}")

        pack = dict(source_row)
        pack.update({
            "validation_status": validation_status,
            "primary_user_role": raw_family.get("primary_user_role"),
            "buyer_or_payer_role": raw_family.get("buyer_or_payer_role", "UNKNOWN"),
            "buyer_job_hypotheses": buyer_hypotheses,
            "pain_clusters": [{"cluster_id": row["cluster_id"], "pain_theme": row["pain_theme"],
                               "sample_status": row["sample_status"], "observation_count": row["observation_count"],
                               "evidence_ids": row["evidence_ids"]} for row in pain_by_family[family_id]],
            "pain_signal_summary": raw_family.get("pain_signal_summary"),
            "paid_alternative_search_status": reported_paid_status,
            "retained_alternative_ids": [row["alternative_id"] for row in alternatives_by_family[family_id]],
            "differentiation_hypotheses": [{"hypothesis_id": row["hypothesis_id"],
                                            "candidate_hypothesis": row["candidate_hypothesis"],
                                            "evidence_ids": row["evidence_ids"]} for row in diff_by_family[family_id]],
            "feasibility_facts": feasibility_facts,
            "feasibility_evidence_status": feasibility_status,
            "inherited_yee47_evidence_ids": list(inherited[family_id]),
            "yee55_evidence_ids": evidence_ref_ids,
            "dimension_statuses": dimension_statuses,
            "research_notes": raw_family.get("research_notes"),
        })
        if not pack["primary_user_role"] or not pack["pain_signal_summary"] or not pack["research_notes"]:
            raise CommercialValidationError(f"family pack is missing required narrative fields: {family_id}")
        pack_rows.append(pack)

    # The query table is ordered by the authorized family list, then query sequence.
    query_rows.sort(key=lambda row: (row["deep_validation_order"], row["query_sequence"]))
    payload = {
        "commercial_validation_packs": pack_rows,
        "validation_evidence": evidence_rows,
        "pain_clusters": pain_rows,
        "market_alternatives": alternative_rows,
        "differentiation_hypotheses": differentiation_rows,
        "validation_queries": query_rows,
    }
    return payload


def _infer_sql_type(values: Sequence[Any], column: str) -> str:
    nonnull = next((value for value in values if value is not None), None)
    if isinstance(nonnull, (list, dict)) or column in JSON_FIELDS:
        return "TEXT"
    if isinstance(nonnull, bool) or isinstance(nonnull, int):
        return "INTEGER"
    if isinstance(nonnull, float):
        return "REAL"
    return "TEXT"


def _stored(value: Any, column: str) -> Any:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return _canonical_json(value)
    if isinstance(value, bool):
        return int(value)
    return value


def _csv_value(value: Any) -> str:
    if value is None:
        return NULL_TOKEN
    if isinstance(value, (list, dict)):
        return _canonical_json(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _pack_columns(rows: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    return tuple(rows[0].keys())


def _table_columns(payload: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, tuple[str, ...]]:
    columns = dict(TABLE_COLUMNS)
    columns["commercial_validation_packs"] = _pack_columns(payload["commercial_validation_packs"])
    return columns


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(_canonical_json(row) + "\n")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n", extrasaction="raise")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_value(row.get(column)) for column in columns})


def _write_db(path: Path, payload: Mapping[str, Sequence[Mapping[str, Any]]], metadata: Mapping[str, Any]) -> None:
    columns_by_table = _table_columns(payload)
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        for table, rows in payload.items():
            columns = columns_by_table[table]
            definitions = ",".join(f'"{column}" {_infer_sql_type([row.get(column) for row in rows], column)}' for column in columns)
            constraints = []
            if table == "commercial_validation_packs":
                constraints += ['PRIMARY KEY("family_id")', 'UNIQUE("deep_validation_order")']
            else:
                primary = {
                    "validation_evidence": "evidence_id", "pain_clusters": "cluster_id",
                    "market_alternatives": "alternative_id", "differentiation_hypotheses": "hypothesis_id",
                    "validation_queries": "query_id",
                }[table]
                constraints.append(f'PRIMARY KEY("{primary}")')
                constraints.append(f'FOREIGN KEY("family_id") REFERENCES commercial_validation_packs("family_id")')
            connection.execute(f'CREATE TABLE "{table}" ({definitions},{",".join(constraints)})')
            placeholders = ",".join("?" for _ in columns)
            connection.executemany(
                f'INSERT INTO "{table}" ({",".join(f"\"{column}\"" for column in columns)}) VALUES ({placeholders})',
                [tuple(_stored(row.get(column), column) for column in columns) for row in rows],
            )
        connection.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value_json TEXT NOT NULL)")
        connection.executemany("INSERT INTO metadata VALUES (?,?)", [(key, _canonical_json(value)) for key, value in sorted(metadata.items())])
        connection.commit()
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise CommercialValidationError("output SQLite failed integrity_check")
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise CommercialValidationError("output SQLite failed foreign_key_check")
    connection.close()


def _render_schema(payload: Mapping[str, Sequence[Mapping[str, Any]]], hashes: Mapping[str, str]) -> str:
    cols = _table_columns(payload)
    lines = [
        "# YEE-55 Commercial Validation Schema v0.1", "",
        "Bounded pilot: COHORT_A deep_validation_order 1..5 only. This layer records sampled public evidence and hypotheses; it does not estimate population prevalence, revenue, TAM, or opportunity ranking.",
        "", "## Canonical inputs", "",
    ]
    lines += [f"- `{key}` SHA-256: `{value}` (read-only)" for key, value in sorted(hashes.items())]
    lines += ["", "## Tables and fields", ""]
    for table, columns in cols.items():
        lines.append(f"### `{table}`")
        lines.append("")
        lines += ["| Column | SQLite type / representation |", "|---|---|"]
        for column in columns:
            values = [row.get(column) for row in payload[table]]
            sql_type = _infer_sql_type(values, column)
            representation = "JSON encoded in TEXT" if column in JSON_FIELDS or any(isinstance(value, (list, dict)) for value in values) else sql_type
            lines.append(f"| `{column}` | {representation} |")
        lines.append("")
    lines += [
        "## Null and evidence semantics", "",
        "- `\\N` in CSV means SQL/JSON null; `UNKNOWN` is a positive statement that the bounded public sources did not establish the actor or price state.",
        "- Missing price is never interpreted as free. Monetization is one of `PAID_PRICE_VERIFIED`, `FREEMIUM_PRICE_VERIFIED`, `MONETIZED_PRICE_NOT_VISIBLE`, `FREE_VERIFIED`, or `UNKNOWN`.",
        "- Prices, currencies, billing units, ranges, and source wording remain native; no currency conversion is performed.",
        "- `REPEATED_SAMPLE` requires at least three distinct observations across at least two pages/threads. It describes only this bounded sample, not market prevalence.",
        "- Differentiation cells use `EVIDENCED_PRESENT`, `EXPLICITLY_UNSUPPORTED`, `NOT_ESTABLISHED`, or `NOT_APPLICABLE`; absent documentation alone never means unsupported.",
        "- `COMPLETE` feasibility requires evidenced platform, dependency/integration, and implementation/source; otherwise the status remains `PARTIAL` or `NOT_ESTABLISHED`.",
        "- Every new evidence record identifies an opened public source page and its discovery query. Inherited YEE-47 evidence IDs are references only and are not copied into YEE-55 evidence.",
        "- The accepted YEE-54 cohort row is retained field-for-field before YEE-55 pilot fields are appended.",
    ]
    return "\n".join(lines) + "\n"


def _write_core(output: Path, payload: Mapping[str, Sequence[Mapping[str, Any]]], capture: Mapping[str, Any], hashes: Mapping[str, str]) -> list[Path]:
    output.mkdir(parents=True, exist_ok=True)
    columns = _table_columns(payload)
    paths: list[Path] = []
    for table, rows in payload.items():
        jsonl = output / f"{table}.jsonl"
        csv_path = output / f"{table}.csv"
        _write_jsonl(jsonl, rows)
        _write_csv(csv_path, rows, columns[table])
        paths += [jsonl, csv_path]
    db_path = output / "deep_commercial_validation.sqlite"
    _write_db(db_path, payload, {
        "work_order": WORK_ORDER, "schema_version": SCHEMA_VERSION,
        "capture_version": CAPTURE_VERSION, "canonical_input_hashes": dict(hashes),
        "authorization": "PILOT_DEEP_VALIDATION_ORDER_1_TO_5",
    })
    schema_path = output / "COMMERCIAL_VALIDATION_SCHEMA.md"
    schema_path.write_text(_render_schema(payload, hashes), encoding="utf-8", newline="\n")
    capture_path = output / "VALIDATION_CAPTURE.json"
    capture_path.write_text(_canonical_json(capture) + "\n", encoding="utf-8", newline="\n")
    paths += [db_path, schema_path, capture_path]
    return paths


def _decode_db_row(row: Mapping[str, Any], columns: Sequence[str]) -> dict[str, Any]:
    result = dict(row)
    for column in columns:
        value = result.get(column)
        if value is not None and (column in JSON_FIELDS or (isinstance(value, str) and value[:1] in "[{")):
            try:
                result[column] = json.loads(value)
            except json.JSONDecodeError:
                pass
    return result


def _check_outputs(output: Path, payload: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    columns = _table_columns(payload)
    checks: dict[str, Any] = {"row_counts_reconcile": True, "schemas_reconcile": True}
    for table, rows in payload.items():
        jsonl_rows = _read_jsonl(output / f"{table}.jsonl")
        with (output / f"{table}.csv").open(encoding="utf-8", newline="") as stream:
            csv_rows = list(csv.DictReader(stream))
            csv_fields = tuple(csv_rows[0].keys()) if csv_rows else tuple(next(csv.reader((output / f"{table}.csv").open(encoding="utf-8"))))
        checks["row_counts_reconcile"] &= len(jsonl_rows) == len(csv_rows) == len(rows)
        checks["schemas_reconcile"] &= csv_fields == columns[table] and all(set(row.keys()) == set(columns[table]) for row in jsonl_rows)
        connection = sqlite3.connect(f"file:{(output / 'deep_commercial_validation.sqlite').resolve().as_posix()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            db_rows = [_decode_db_row(dict(row), columns[table]) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')]
            checks["row_counts_reconcile"] &= len(db_rows) == len(rows)
            checks["schemas_reconcile"] &= tuple(db_rows[0].keys()) == columns[table] if db_rows else True
            checks[f"{table}_jsonl_sqlite_reconcile"] = db_rows == jsonl_rows
        finally:
            connection.close()
    connection = sqlite3.connect(f"file:{(output / 'deep_commercial_validation.sqlite').resolve().as_posix()}?mode=ro", uri=True)
    try:
        checks["sqlite_integrity_ok"] = connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        checks["foreign_key_check_ok"] = not connection.execute("PRAGMA foreign_key_check").fetchall()
    finally:
        connection.close()
    return checks


def _validate_payload(payload: Mapping[str, Sequence[Mapping[str, Any]]], canonical_rows: Sequence[Mapping[str, Any]], output_checks: Mapping[str, Any], hashes_before: Mapping[str, str], hashes_after: Mapping[str, str], deterministic: bool, capture: Mapping[str, Any]) -> dict[str, Any]:
    packs = payload["commercial_validation_packs"]
    errors: list[str] = []
    expected = list(PILOT_IDS)
    got = [row["family_id"] for row in packs]
    if len(packs) != 5 or got != expected:
        errors.append("exact pilot membership/order is not the authorized five families")
    query_rows = payload["validation_queries"]
    query_ids = {row["query_id"] for row in query_rows}
    evidence_rows = payload["validation_evidence"]
    invalid_query_refs = [row["evidence_id"] for row in evidence_rows if row["query_id"] not in query_ids]
    if invalid_query_refs:
        errors.append("orphan query reference in validation evidence")
    family_checks = []
    query_count_by_family = {}
    query_purpose_counts_by_family = {}
    opened_page_count_by_family = {}
    evidence_page_count_by_family = {}
    for source, pack in zip(canonical_rows, packs):
        if {key: pack.get(key) for key in source} != dict(source):
            errors.append(f"accepted YEE-54 fields are not preserved for {source['family_id']}")
    evidence_by_id = {row["evidence_id"]: row for row in evidence_rows}
    for pack in packs:
        family_id = pack["family_id"]
        own = {eid for eid, row in evidence_by_id.items() if row["family_id"] == family_id}
        own_rows = [row for row in evidence_rows if row["family_id"] == family_id]
        family_queries = [row for row in query_rows if row["family_id"] == family_id]
        purpose_counts = Counter(row["purpose"] for row in family_queries)
        opened_pages = [row for row in capture.get("opened_pages", []) if row.get("family_id") == family_id]
        dimensions = Counter(row["dimension"] for row in own_rows)
        query_count_by_family[family_id] = len(family_queries)
        query_purpose_counts_by_family[family_id] = dict(sorted(purpose_counts.items()))
        opened_page_count_by_family[family_id] = len(opened_pages)
        evidence_page_count_by_family[family_id] = len({row["canonical_url"] for row in own_rows})
        if set(pack["yee55_evidence_ids"]) != own:
            errors.append(f"YEE-55 evidence references do not reconcile for {family_id}")
        for hypothesis in pack["buyer_job_hypotheses"] + pack["differentiation_hypotheses"] + pack["feasibility_facts"]:
            if not set(hypothesis["evidence_ids"]).issubset(own):
                errors.append(f"orphan/cross-family pack evidence reference for {family_id}")
        if len(family_queries) > 20 or len(opened_pages) > 25:
            errors.append(f"query or opened-page budget exceeded for {family_id}")
        if any(purpose_counts[purpose] < minimum for purpose, minimum in (
                ("buyer/job validation", 1), ("pain prevalence discovery", 3),
                ("paid alternatives/pricing", 3), ("feasibility support", 1))):
            errors.append(f"query-purpose minimum not met for {family_id}")
        if not (dimensions["buyer_job"] >= 2 and 3 <= dimensions["pain_point"] <= 10 and dimensions["feasibility"] >= 2):
            errors.append(f"buyer, pain, or feasibility evidence minimum/cap failed for {family_id}")
        family_checks.append({
            "deep_validation_order": pack["deep_validation_order"],
            "consensus_rank": pack["consensus_rank"],
            "family_id": family_id,
            "canonical_topic_key": pack.get("canonical_topic_key"),
            "validation_status": pack["validation_status"],
            "primary_user_role": pack["primary_user_role"],
            "buyer_or_payer_role": pack["buyer_or_payer_role"],
            "buyer_job_evidence_count": dimensions["buyer_job"],
            "pain_observation_count": dimensions["pain_point"],
            "distinct_pain_source_pages": len({row["canonical_url"] for row in own_rows if row["dimension"] == "pain_point"}),
            "query_count": len(family_queries),
            "query_purpose_counts": dict(sorted(purpose_counts.items())),
            "opened_page_count": len(opened_pages),
            "evidence_page_count": len({row["canonical_url"] for row in own_rows}),
            "retained_alternative_count": len(pack["retained_alternative_ids"]),
            "feasibility_evidence_count": dimensions["feasibility"],
            "feasibility_status": pack["feasibility_evidence_status"],
        })
    for table in ("pain_clusters", "market_alternatives", "differentiation_hypotheses"):
        for row in payload[table]:
            if row["family_id"] not in PILOT_IDS or not set(row["evidence_ids"]).issubset(
                    {eid for eid, ev in evidence_by_id.items() if ev["family_id"] == row["family_id"]}):
                errors.append(f"orphan or cross-family reference in {table}")
    for table_check, passed in output_checks.items():
        if table_check.endswith("_reconcile") or table_check in {"row_counts_reconcile", "schemas_reconcile", "sqlite_integrity_ok", "foreign_key_check_ok"}:
            if not passed:
                errors.append(f"output reconciliation failed: {table_check}")
    if not deterministic:
        errors.append("deterministic replay is not byte-identical")
    if dict(hashes_before) != dict(hashes_after) or dict(hashes_before) != EXPECTED_HASHES:
        errors.append("canonical input hashes changed or do not match accepted inputs")
    return {
        "status": "PASS" if not errors else "FAIL",
        "work_order": WORK_ORDER, "schema_version": SCHEMA_VERSION,
        "authorized_scope": "pilot deep_validation_order 1..5 only",
        "errors": errors,
        "canonical_input_hashes_before": dict(hashes_before),
        "canonical_input_hashes_after": dict(hashes_after),
        "checks": {
            "exactly_five_authorized_families": len(packs) == 5 and got == expected,
            "accepted_cohort_fields_preserved": all({key: pack.get(key) for key in source} == dict(source) for source, pack in zip(canonical_rows, packs)),
            "no_orphan_or_cross_family_evidence_refs": not any("orphan" in item for item in errors),
            "query_references_valid": not invalid_query_refs,
            "research_budgets_and_evidence_minima_respected": not any("budget" in item or "minimum" in item or "cap failed" in item for item in errors),
            "sqlite_integrity_ok": output_checks.get("sqlite_integrity_ok"),
            "foreign_key_check_ok": output_checks.get("foreign_key_check_ok"),
            "jsonl_csv_sqlite_reconciled": output_checks.get("row_counts_reconcile") and output_checks.get("schemas_reconcile") and all(value for key, value in output_checks.items() if key.endswith("_reconcile")),
            "deterministic_replay_byte_identical": deterministic,
            "canonical_inputs_read_only_hash_stable": dict(hashes_before) == dict(hashes_after) == EXPECTED_HASHES,
        },
        "details": {
            "family_count": len(packs),
            "row_counts": {table: len(rows) for table, rows in payload.items()},
            "family_checks": family_checks,
            "query_count_by_family": dict(sorted(query_count_by_family.items())),
            "query_purpose_counts_by_family": dict(sorted(query_purpose_counts_by_family.items())),
            "opened_page_count_by_family": dict(sorted(opened_page_count_by_family.items())),
            "evidence_page_count_by_family": dict(sorted(evidence_page_count_by_family.items())),
            "validation_status_counts": dict(Counter(row["validation_status"] for row in packs)),
            "paid_alternative_status_counts": dict(Counter(row["paid_alternative_search_status"] for row in packs)),
            "pain_sample_status_by_family": {row["family_id"]: row["dimension_statuses"]["pain_prevalence_sample"] for row in packs},
            "feasibility_status_counts": dict(Counter(row["feasibility_evidence_status"] for row in packs)),
            "monetization_status_counts": dict(Counter(row["monetization_status"] for row in payload["market_alternatives"])),
            "output_checks": dict(output_checks),
        },
    }


def _report(qa: Mapping[str, Any]) -> str:
    details = qa["details"]
    family_lines = [
        "| Order | Rank | Family | Status | User / payer | Buyer evidence | Pain observations/pages | Queries / opened pages | Alternatives | Feasibility |",
        "|---:|---:|---|---|---|---:|---:|---:|---:|---|",
    ]
    for row in details["family_checks"]:
        family_lines.append(
            f"| {row['deep_validation_order']} | {row['consensus_rank']} | `{row['canonical_topic_key']}` | "
            f"{row['validation_status']} | {row['primary_user_role']} / {row['buyer_or_payer_role']} | "
            f"{row['buyer_job_evidence_count']} | {row['pain_observation_count']} / {row['distinct_pain_source_pages']} | "
            f"{row['query_count']} / {row['opened_page_count']} | {row['retained_alternative_count']} | "
            f"{row['feasibility_status']} ({row['feasibility_evidence_count']} evidence) |"
        )
    return "\n".join([
        "# YEE-55 Deep Commercial Validation Pilot", "",
        f"Status: **{qa['status']}**", "",
        "Authorized scope is limited to deep_validation_order 1..5 (five families). Orders 6..15 were not researched or executed.", "",
        f"- Packs: {details['family_count']}",
        f"- Evidence: {details['row_counts']['validation_evidence']}",
        f"- Query log: {details['row_counts']['validation_queries']}",
        f"- Pain clusters: {details['row_counts']['pain_clusters']}",
        f"- Market alternatives: {details['row_counts']['market_alternatives']}",
        f"- Differentiation matrix/hypotheses: {details['row_counts']['differentiation_hypotheses']}",
        f"- Validation statuses: `{_canonical_json(details['validation_status_counts'])}`",
        f"- Pain sample labels: `{_canonical_json(details['pain_sample_status_by_family'])}`",
        f"- Monetization states (source-native; no currency conversion): `{_canonical_json(details['monetization_status_counts'])}`",
        "", "## Family coverage", "", *family_lines,
        "", "## Interpretation limits", "",
        "Community and support observations are individual public reports and are labeled as a bounded sample, not population prevalence. A candidate differentiation hypothesis is not proof of adoption or market advantage. Unknown prices are not treated as free. No revenue/TAM, market score, ranking, or product recommendation is produced.",
        "", "## QA", "",
        f"- SQLite integrity/FK: `{details['output_checks']['sqlite_integrity_ok']}` / `{details['output_checks']['foreign_key_check_ok']}`",
        f"- JSONL/CSV/SQLite reconciliation: `{details['output_checks']['row_counts_reconcile'] and details['output_checks']['schemas_reconcile']}`",
        f"- Deterministic replay: `{qa['checks']['deterministic_replay_byte_identical']}`",
        f"- Query/page budgets and evidence minimums: `{qa['checks']['research_budgets_and_evidence_minima_respected']}`",
        f"- Query-to-evidence references: `{qa['checks']['query_references_valid']}`",
        f"- Canonical input hash stable: `{qa['checks']['canonical_inputs_read_only_hash_stable']}`",
        "", "## Canonical inputs", "",
        *[f"- `{key}`: `{value}`" for key, value in sorted(qa["canonical_input_hashes_before"].items())],
        "", "See `QA_RESULT.json`, `DATASET_MANIFEST.json`, and `VALIDATION_CAPTURE.json` for row-level reconciliation and source provenance.", "",
    ])


def _write_final_docs(output: Path, qa: Mapping[str, Any]) -> None:
    (output / "QA_RESULT.json").write_text(_canonical_json(qa) + "\n", encoding="utf-8", newline="\n")
    (output / "PILOT_REPORT.md").write_text(_report(qa), encoding="utf-8", newline="\n")


def _manifest(output: Path, hashes: Mapping[str, str], counts: Mapping[str, int]) -> dict[str, Any]:
    files = []
    for path in sorted((item for item in output.iterdir() if item.is_file() and item.name != "DATASET_MANIFEST.json"), key=lambda item: item.name):
        files.append({"file": path.name, "size_bytes": path.stat().st_size, "sha256": _sha256(path)})
    return {
        "work_order": WORK_ORDER, "schema_version": SCHEMA_VERSION,
        "authorization": "PILOT_DEEP_VALIDATION_ORDER_1_TO_5",
        "canonical_input_hashes": dict(hashes), "row_counts": dict(counts),
        "files": files,
        "manifest_note": "Manifest enumerates all deliverables except itself to avoid recursive hashing.",
    }


def _write_manifest(output: Path, hashes: Mapping[str, str], counts: Mapping[str, int]) -> None:
    (output / "DATASET_MANIFEST.json").write_text(_canonical_json(_manifest(output, hashes, counts)) + "\n", encoding="utf-8", newline="\n")


def build_pilot(cohort_path: str | Path, synthesis_db_path: str | Path, yee47_db_path: str | Path,
                capture_path: str | Path, output_dir: str | Path) -> dict[str, Any]:
    canonical_rows, inherited, paths, hashes_before = _load_canonical_inputs(
        Path(cohort_path), Path(synthesis_db_path), Path(yee47_db_path)
    )
    try:
        capture = json.loads(Path(capture_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CommercialValidationError(f"unable to parse YEE-55 capture: {capture_path}") from exc
    if capture.get("canonical_input_hashes") != hashes_before:
        raise CommercialValidationError("capture canonical-input hashes do not match the verified read-only inputs")
    output = Path(output_dir).resolve()
    if output.exists() and any(output.iterdir()):
        raise CommercialValidationError(f"refusing to overwrite non-empty production output directory: {output}")
    payload = _normalize_capture(capture, canonical_rows, inherited)
    artifact_paths = _write_core(output, payload, capture, hashes_before)
    output_checks = _check_outputs(output, payload)

    with tempfile.TemporaryDirectory(prefix="yee55-replay-") as temp:
        replay = Path(temp)
        replay_payload = _normalize_capture(capture, canonical_rows, inherited)
        replay_core = _write_core(replay, replay_payload, capture, hashes_before)
        core_identical = (
            payload == replay_payload
            and {p.name: _sha256(p) for p in artifact_paths} == {p.name: _sha256(p) for p in replay_core}
        )
        replay_checks = _check_outputs(replay, replay_payload)
        deterministic = core_identical and replay_checks == output_checks
        hashes_after = {name: _sha256(path) for name, path in paths.items()}
        qa = _validate_payload(payload, canonical_rows, output_checks, hashes_before, hashes_after, deterministic, capture)
        _write_final_docs(output, qa)
        _write_final_docs(replay, qa)
        counts = {table: len(rows) for table, rows in payload.items()}
        _write_manifest(output, hashes_before, counts)
        _write_manifest(replay, hashes_before, counts)
        final_files = sorted(path.name for path in output.iterdir() if path.is_file())
        replay_files = sorted(path.name for path in replay.iterdir() if path.is_file())
        final_byte_identical = final_files == replay_files and all(
            _sha256(output / name) == _sha256(replay / name) for name in final_files
        )
        qa["checks"]["all_deliverables_byte_identical_on_replay"] = final_byte_identical
        if not final_byte_identical:
            qa["status"] = "FAIL"
            qa["errors"].append("final deliverables differ on deterministic replay")
        _write_final_docs(output, qa)
        _write_final_docs(replay, qa)
        # Refresh both manifests after final QA/report bytes are stable.
        _write_manifest(output, hashes_before, counts)
        _write_manifest(replay, hashes_before, counts)
        final_byte_identical = final_files == sorted(path.name for path in replay.iterdir() if path.is_file()) and all(
            _sha256(output / name) == _sha256(replay / name) for name in final_files
        )
        qa["checks"]["all_deliverables_byte_identical_on_replay"] = final_byte_identical
        if not final_byte_identical:
            qa["status"] = "FAIL"
            if "final deliverables differ on deterministic replay" not in qa["errors"]:
                qa["errors"].append("final deliverables differ on deterministic replay")
        _write_final_docs(output, qa)
        _write_manifest(output, hashes_before, counts)
    return {"status": qa["status"], "output_dir": output, "qa": qa, "manifest": _manifest(output, hashes_before, counts)}
