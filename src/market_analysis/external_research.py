"""YEE-47 evidence normalization, pilot validation, and deterministic exports."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import sqlite3
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

WORK_ORDER = "YEE-47"
SCHEMA_VERSION = "yee-47-external-market-evidence-v0.1"
BASELINE_COMMIT = "79ca2f68591a6b469b4bd57bc974584b364fe107"
ACCEPTED_SHORTLIST_SHA256 = "b9a9b41b054fa761945098054cffb52c1ab42d0779f4cfb1e459be824a119a0f"
ACCEPTED_ANALYSIS_DB_SHA256 = "770a70d03af05f651cb9e7282a21a6d8961dadaa547d017c3e144bffe4791156"
SPEC_SHORTLIST_SHA256_LITERAL = "b9a9b41b054fa761945098054cffb52c1ab42d0779f4c6fb1e459be824a119a0f"
ACCEPTED_YEE46_MANIFEST_URL = "https://drive.google.com/file/d/1_o5obdhL_qvjkt42jNOcFbanDGuSygqB/view"
PILOT_FAMILY_IDS = (
    "family_188439fdf256290b518101b998fcc1796fec6d84387beed586a81728b8978a4b",
    "family_7dcf5fc1be7927b96992d206c8fc61eba8ebb36a0a360f17cbad589785f0e5c3",
    "family_39b0610ee5b1f2bb0e371ad55075469dd18d86f57809972bcb967c45ecb26b48",
    "family_a6ba31d88da34303daa3d4011e228f234a99de9f80cc40c4bcbe5554dcf99e6f",
    "family_d521d7e56cb6bf59adeae431659bd85f83b147b4a221e759cad46917e97645fc",
    "family_64d4be37efa49f4e14c366998007d78347748029dc3540acae23c8931e9fcc78",
    "family_fde65554a094d6f5bf491eb9779a0adceeaeee8ca61501aab4b5475c2810486f",
    "family_183dadbeb21f00e17773add50166a0c2b2257a1d884e5aef6c552d04c02ab8ca",
    "family_bc76fab4be944ac77279467019f68a867a098e4e3e103c03923eb9b696feb2e2",
    "family_f907e12a6faa31c430ea8adbfbf77ac3ac7a3e3c6c9a1cc1e994467ce452fa9a",
)
NULL_TOKEN = r"\N"

SOURCE_TYPES = {
    "PRIMARY_PRODUCT", "PRIMARY_DOCS", "PRIMARY_REPOSITORY", "MARKETPLACE_LISTING",
    "PRIMARY_SUPPORT", "COMMUNITY", "EDITORIAL",
}
CLAIM_TYPES = {
    "SEMANTIC_IDENTITY", "COMPETITOR_RELATION", "FEATURE", "PRICING", "MAINTENANCE",
    "POPULARITY_PROXY", "PAIN_POINT", "GAP_SIGNAL",
}
RELATION_TYPES = {"DIRECT", "SUBSTITUTE", "ADJACENT"}
RESEARCH_STATUSES = {"RESOLVED", "AMBIGUOUS", "UNRESOLVED"}

PACK_BASE_COLUMNS = (
    "family_id", "consensus_rank", "canonical_topic_key", "member_topic_keys", "aliases",
    "candidate_classes", "consensus_score", "balanced_rank", "demand_first_rank",
    "whitespace_first_rank", "balanced_core_source_score", "balanced_final_score",
    "balanced_source_weight_coverage", "cross_market_validation_score",
    "demand_first_core_source_score", "demand_first_final_score",
    "demand_first_source_weight_coverage", "evidence_coverage_share", "family_status",
    "hangar_C", "hangar_D", "hangar_F", "hangar_W", "hangar_resource_count",
    "hangar_source_score_balanced", "hangar_source_score_demand_first",
    "hangar_source_score_whitespace_first", "has_voxel_paid_evidence", "mean_profile_rank",
    "median_profile_rank", "member_count", "modrinth_C", "modrinth_D", "modrinth_F",
    "modrinth_W", "modrinth_resource_count", "modrinth_source_score_balanced",
    "modrinth_source_score_demand_first", "modrinth_source_score_whitespace_first",
    "rank_span", "source_presence", "source_presence_count", "triage_bucket", "voxel_C",
    "voxel_D", "voxel_F", "voxel_W", "voxel_monetization_score", "voxel_resource_count",
    "voxel_source_score_balanced", "voxel_source_score_demand_first",
    "voxel_source_score_whitespace_first", "whitespace_first_core_source_score",
    "whitespace_first_final_score", "whitespace_first_source_weight_coverage",
)
PACK_COLUMNS = PACK_BASE_COLUMNS + (
    "research_status", "resolved_concept_name", "concept_summary", "primary_entity_url",
    "direct_competitor_count", "direct_competitor_ids", "feature_themes",
    "pricing_observations_by_currency", "maintenance_summary", "popularity_proxy_summary",
    "pain_point_hypotheses", "gap_hypotheses", "evidence_ids", "evidence_count",
    "primary_evidence_count", "community_evidence_count", "distinct_domain_count",
    "research_coverage_status", "research_notes", "field_evidence_map",
)
EVIDENCE_COLUMNS = (
    "evidence_id", "family_id", "consensus_rank", "source_url", "canonical_url",
    "source_domain", "source_title", "source_type", "retrieved_at", "published_or_updated_at",
    "claim_type", "observation", "optional_excerpt", "numeric_value", "numeric_unit",
    "currency", "entity_name", "notes", "query_id",
)
COMPETITOR_COLUMNS = (
    "competitor_id", "family_id", "consensus_rank", "entity_name", "canonical_url",
    "relation_type", "product_type", "platform_or_ecosystem", "pricing_model", "price_amount",
    "currency", "maintenance_status", "feature_summary", "evidence_ids",
)
QUERY_COLUMNS = (
    "query_id", "family_id", "consensus_rank", "query_sequence", "query_text", "issued_at",
    "purpose", "result_action",
)
EXPORT_TABLES = {
    "family_research_packs": PACK_COLUMNS,
    "external_evidence": EVIDENCE_COLUMNS,
    "competitor_entities": COMPETITOR_COLUMNS,
    "research_queries": QUERY_COLUMNS,
}
JSON_COLUMNS = {
    "member_topic_keys", "aliases", "candidate_classes", "source_presence", "direct_competitor_ids", "feature_themes",
    "pricing_observations_by_currency", "popularity_proxy_summary", "pain_point_hypotheses",
    "gap_hypotheses", "evidence_ids", "field_evidence_map", "optional_excerpt",
}
INTEGER_COLUMNS = {
    "consensus_rank", "balanced_rank", "demand_first_rank", "whitespace_first_rank",
    "direct_competitor_count", "evidence_count", "primary_evidence_count",
    "community_evidence_count", "distinct_domain_count", "query_sequence",
    "member_count", "rank_span", "source_presence_count", "hangar_resource_count",
    "modrinth_resource_count", "voxel_resource_count", "has_voxel_paid_evidence",
}
TEXT_COLUMNS = {
    "family_id", "canonical_topic_key", "resolved_concept_name", "concept_summary",
    "primary_entity_url", "research_status", "research_coverage_status", "research_notes",
    "evidence_id", "source_url", "canonical_url", "source_domain", "source_title", "source_type",
    "retrieved_at", "published_or_updated_at", "claim_type", "observation", "numeric_unit",
    "currency", "entity_name", "notes", "query_id", "competitor_id", "relation_type",
    "product_type", "platform_or_ecosystem", "pricing_model", "maintenance_status", "feature_summary",
    "query_text", "issued_at", "purpose", "result_action",
}


class ExternalResearchInputError(ValueError):
    """Raised when YEE-47 canonical inputs or the pilot capture violate the contract."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonicalize_url(url: str) -> str:
    parsed = urlsplit(url.strip())
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ExternalResearchInputError(f"evidence URL must be public HTTP(S): {url!r}")
    if parsed.username or parsed.password:
        raise ExternalResearchInputError("credential-bearing source URLs are forbidden")
    host = parsed.hostname.casefold()
    port = parsed.port
    netloc = host if port is None or (parsed.scheme.lower(), port) in {("http", 80), ("https", 443)} else f"{host}:{port}"
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    tracking = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source"}
    params = [
        (key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.casefold() not in tracking and not key.casefold().startswith("utm_")
    ]
    query = urlencode(sorted(params))
    return urlunsplit((parsed.scheme.lower(), netloc, path, query, ""))


def _timestamp(value: str | None, field: str, *, required: bool) -> str | None:
    if value is None or value == "":
        if required:
            raise ExternalResearchInputError(f"{field} is required")
        return None
    if not required and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ExternalResearchInputError(f"invalid ISO date for {field}: {value!r}") from exc
        return value
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise ExternalResearchInputError(f"invalid ISO timestamp for {field}: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ExternalResearchInputError(f"{field} must include a timezone")
    normalized = parsed.astimezone(timezone.utc)
    timespec = "microseconds" if normalized.microsecond else "seconds"
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        raise ExternalResearchInputError(f"unable to parse canonical shortlist JSONL: {path}") from exc


def _readonly_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _load_accepted_inputs(shortlist_path: str | Path, analysis_db_path: str | Path) -> tuple[list[dict[str, Any]], dict[str, str]]:
    shortlist_path = Path(shortlist_path).resolve()
    analysis_db_path = Path(analysis_db_path).resolve()
    if not shortlist_path.is_file() or not analysis_db_path.is_file():
        raise FileNotFoundError("accepted YEE-46 shortlist or analysis database not found")
    before = {"shortlist": _sha256_file(shortlist_path), "analysis_db": _sha256_file(analysis_db_path)}
    if before["shortlist"] != ACCEPTED_SHORTLIST_SHA256 or before["analysis_db"] != ACCEPTED_ANALYSIS_DB_SHA256:
        raise ExternalResearchInputError(f"accepted YEE-46 input hash mismatch: {before}")
    families = _read_jsonl(shortlist_path)
    if len(families) != 100 or [row.get("consensus_rank") for row in families] != list(range(1, 101)):
        raise ExternalResearchInputError("accepted YEE-46 shortlist must contain exactly ordered ranks 1..100")
    if any(row.get("triage_bucket") != "ADVANCE_RESEARCH" for row in families):
        raise ExternalResearchInputError("accepted YEE-46 input contains a non-ADVANCE_RESEARCH row")
    if [row.get("family_id") for row in families[:10]] != list(PILOT_FAMILY_IDS):
        raise ExternalResearchInputError("accepted YEE-46 ranks 1..10 differ from the authorized pilot identities")

    connection = _readonly_connection(analysis_db_path)
    try:
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ExternalResearchInputError("accepted YEE-46 analysis database failed integrity_check")
        db_rows = [dict(row) for row in connection.execute(
            "SELECT family_id,consensus_rank,consensus_score,balanced_rank,demand_first_rank,"
            "whitespace_first_rank,triage_bucket FROM family_opportunity_scores "
            "WHERE consensus_rank BETWEEN 1 AND 100 ORDER BY consensus_rank"
        )]
        if len(db_rows) != 100:
            raise ExternalResearchInputError("accepted YEE-46 SQLite context does not have the expected top 100")
        for json_row, db_row in zip(families, db_rows):
            for field in ("family_id", "consensus_rank", "balanced_rank", "demand_first_rank", "whitespace_first_rank", "triage_bucket"):
                if json_row.get(field) != db_row.get(field):
                    raise ExternalResearchInputError(f"YEE-46 JSONL/SQLite mismatch in {field} for rank {json_row.get('consensus_rank')}")
            if json_row.get("consensus_score") != db_row.get("consensus_score"):
                raise ExternalResearchInputError(f"YEE-46 JSONL/SQLite score mismatch for rank {json_row.get('consensus_rank')}")
    finally:
        connection.close()
    after = {"shortlist": _sha256_file(shortlist_path), "analysis_db": _sha256_file(analysis_db_path)}
    if before != after:
        raise ExternalResearchInputError("accepted YEE-46 canonical inputs changed during read-only preflight")
    return families, before


def _hash_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()[:20]
    return f"{prefix}_{digest}"


def _dedupe_refs(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _resolve_refs(
    raw_refs: Sequence[str], capture_to_evidence: Mapping[str, str], evidence_by_id: Mapping[str, Mapping[str, Any]],
    family_id: str,
) -> list[str]:
    result = []
    for raw_ref in raw_refs:
        if raw_ref not in capture_to_evidence:
            raise ExternalResearchInputError(f"unknown evidence ref {raw_ref!r} for {family_id}")
        evidence_id = capture_to_evidence[raw_ref]
        if evidence_by_id[evidence_id]["family_id"] != family_id:
            raise ExternalResearchInputError(f"cross-family evidence reference {raw_ref!r} for {family_id}")
        result.append(evidence_id)
    return _dedupe_refs(result)


def _normalize_capture(
    capture: Mapping[str, Any], canonical_families: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    by_family = {row["family_id"]: row for row in canonical_families[:10]}
    allowed_ids = set(by_family)
    queries: list[dict[str, Any]] = []
    query_capture_to_id: dict[str, str] = {}
    per_family_query_count: Counter[str] = Counter()
    for sequence, raw in enumerate(capture.get("queries", []), 1):
        family_id = raw.get("family_id")
        if family_id not in allowed_ids:
            raise ExternalResearchInputError(f"query outside authorized pilot: {family_id}")
        capture_id = raw.get("capture_id")
        if not capture_id or capture_id in query_capture_to_id:
            raise ExternalResearchInputError("research query capture_id must be present and unique")
        query_id = f"qry_{sequence:04d}"
        query_capture_to_id[capture_id] = query_id
        per_family_query_count[family_id] += 1
        queries.append({
            "query_id": query_id,
            "family_id": family_id,
            "consensus_rank": by_family[family_id]["consensus_rank"],
            "query_sequence": sequence,
            "query_text": str(raw.get("query_text", "")).strip(),
            "issued_at": _timestamp(raw.get("issued_at"), "issued_at", required=True),
            "purpose": str(raw.get("purpose", "")).strip(),
            "result_action": str(raw.get("result_action", "")).strip(),
        })

    if any(not row["query_text"] or not row["purpose"] or not row["result_action"] for row in queries):
        raise ExternalResearchInputError("every executed search query needs text, purpose, and result action")

    page_capture_to_row: dict[str, dict[str, Any]] = {}
    page_urls_by_family: dict[str, set[str]] = defaultdict(set)
    for raw in capture.get("opened_pages", []):
        family_id = raw.get("family_id")
        if family_id not in allowed_ids:
            raise ExternalResearchInputError(f"opened source page outside authorized pilot: {family_id}")
        capture_id = raw.get("capture_id")
        if not capture_id or capture_id in page_capture_to_row:
            raise ExternalResearchInputError("opened-page capture_id must be present and unique")
        if raw.get("query_ref") not in query_capture_to_id:
            raise ExternalResearchInputError(f"opened source page needs a logged discovery query: {capture_id}")
        if raw.get("source_type") not in SOURCE_TYPES:
            raise ExternalResearchInputError(f"invalid opened-page source_type: {capture_id}")
        canonical = canonicalize_url(str(raw.get("source_url", "")))
        source_title = str(raw.get("source_title", "")).strip()
        if not source_title:
            raise ExternalResearchInputError(f"opened source page needs a title: {capture_id}")
        page = {
            "capture_id": capture_id,
            "family_id": family_id,
            "source_url": str(raw["source_url"]).strip(),
            "canonical_url": canonical,
            "source_title": source_title,
            "source_type": raw["source_type"],
            "retrieved_at": _timestamp(raw.get("retrieved_at"), "retrieved_at", required=True),
            "published_or_updated_at": _timestamp(raw.get("published_or_updated_at"), "published_or_updated_at", required=False),
            "query_id": query_capture_to_id[raw["query_ref"]],
        }
        page_capture_to_row[capture_id] = page
        page_urls_by_family[family_id].add(canonical)
    if set(page_urls_by_family) and any(len(urls) > 15 for urls in page_urls_by_family.values()):
        raise ExternalResearchInputError("a family exceeds the 15 retained opened-page budget")

    capture_to_evidence: dict[str, str] = {}
    evidence: list[dict[str, Any]] = []
    evidence_dedupe: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for raw in capture.get("evidence", []):
        family_id = raw.get("family_id")
        if family_id not in allowed_ids:
            raise ExternalResearchInputError(f"evidence outside authorized pilot: {family_id}")
        capture_id = raw.get("capture_id")
        if not capture_id or capture_id in capture_to_evidence:
            raise ExternalResearchInputError("evidence capture_id must be present and unique")
        if raw.get("claim_type") not in CLAIM_TYPES:
            raise ExternalResearchInputError(f"invalid evidence enum for capture {capture_id}")
        page_ref = raw.get("page_ref")
        if page_ref not in page_capture_to_row:
            raise ExternalResearchInputError(f"evidence must reference an opened source page: {capture_id}")
        page = page_capture_to_row[page_ref]
        if page["family_id"] != family_id:
            raise ExternalResearchInputError(f"evidence references another family's opened page: {capture_id}")
        canonical = page["canonical_url"]
        parsed = urlsplit(canonical)
        observation = str(raw.get("observation", "")).strip()
        if not observation:
            raise ExternalResearchInputError(f"evidence {capture_id} needs an observation")
        excerpt = raw.get("optional_excerpt")
        if excerpt is not None and len(str(excerpt).split()) > 25:
            raise ExternalResearchInputError(f"optional_excerpt exceeds 25 words in {capture_id}")
        numeric_value = raw.get("numeric_value")
        if numeric_value is not None:
            if isinstance(numeric_value, bool) or not isinstance(numeric_value, (int, float)) or not math.isfinite(numeric_value):
                raise ExternalResearchInputError(f"numeric_value must be finite numeric data in {capture_id}")
            if not raw.get("numeric_unit"):
                raise ExternalResearchInputError(f"numeric_unit is required with numeric_value in {capture_id}")
        key = (family_id, canonical, raw["claim_type"], " ".join(observation.casefold().split()))
        eid = _hash_id("ev", *key)
        normalized = {
            "evidence_id": eid,
            "family_id": family_id,
            "consensus_rank": by_family[family_id]["consensus_rank"],
            "source_url": page["source_url"],
            "canonical_url": canonical,
            "source_domain": parsed.hostname.casefold(),
            "source_title": page["source_title"],
            "source_type": page["source_type"],
            "retrieved_at": page["retrieved_at"],
            "published_or_updated_at": page["published_or_updated_at"],
            "claim_type": raw["claim_type"],
            "observation": observation,
            "optional_excerpt": excerpt,
            "numeric_value": numeric_value,
            "numeric_unit": raw.get("numeric_unit"),
            "currency": raw.get("currency"),
            "entity_name": raw.get("entity_name"),
            "notes": raw.get("notes"),
            "query_id": page["query_id"],
        }
        existing = evidence_dedupe.get(key)
        if existing:
            comparable = (existing["numeric_value"], existing["numeric_unit"], existing["currency"], existing["source_type"])
            candidate = (normalized["numeric_value"], normalized["numeric_unit"], normalized["currency"], normalized["source_type"])
            if comparable != candidate:
                raise ExternalResearchInputError(f"conflicting duplicate evidence for {capture_id}")
            capture_to_evidence[capture_id] = existing["evidence_id"]
            continue
        evidence_dedupe[key] = normalized
        capture_to_evidence[capture_id] = eid
        evidence.append(normalized)

    evidence_by_id = {row["evidence_id"]: row for row in evidence}
    entities: list[dict[str, Any]] = []
    entity_capture_to_id: dict[str, str] = {}
    for raw in capture.get("competitors", []):
        family_id = raw.get("family_id")
        if family_id not in allowed_ids:
            raise ExternalResearchInputError(f"competitor outside authorized pilot: {family_id}")
        if raw.get("relation_type") not in RELATION_TYPES:
            raise ExternalResearchInputError(f"invalid competitor relation for {raw.get('capture_id')}")
        capture_id = raw.get("capture_id")
        if not capture_id or capture_id in entity_capture_to_id:
            raise ExternalResearchInputError("competitor capture_id must be present and unique")
        canonical = canonicalize_url(str(raw.get("canonical_url", "")))
        refs = _resolve_refs(raw.get("evidence_refs", []), capture_to_evidence, evidence_by_id, family_id)
        claims = {evidence_by_id[eid]["claim_type"] for eid in refs}
        if not claims.intersection({"COMPETITOR_RELATION", "SEMANTIC_IDENTITY"}):
            raise ExternalResearchInputError(f"competitor relationship lacks supporting relation evidence: {capture_id}")
        if raw.get("pricing_model") is not None and "PRICING" not in claims:
            raise ExternalResearchInputError(f"competitor pricing_model lacks PRICING evidence: {capture_id}")
        if raw.get("price_amount") is not None:
            if raw.get("currency") is None or "PRICING" not in claims:
                raise ExternalResearchInputError(f"explicit competitor price needs currency and PRICING evidence: {capture_id}")
            if isinstance(raw["price_amount"], bool) or not isinstance(raw["price_amount"], (int, float)):
                raise ExternalResearchInputError(f"price_amount must be numeric: {capture_id}")
        if raw.get("feature_summary") is not None and "FEATURE" not in claims:
            raise ExternalResearchInputError(f"competitor feature_summary lacks FEATURE evidence: {capture_id}")
        if raw.get("maintenance_status") is not None and "MAINTENANCE" not in claims:
            raise ExternalResearchInputError(f"competitor maintenance_status lacks MAINTENANCE evidence: {capture_id}")
        competitor_id = _hash_id("cmp", family_id, canonical, raw["relation_type"])
        entity_capture_to_id[capture_id] = competitor_id
        entities.append({
            "competitor_id": competitor_id,
            "family_id": family_id,
            "consensus_rank": by_family[family_id]["consensus_rank"],
            "entity_name": str(raw.get("entity_name", "")).strip(),
            "canonical_url": canonical,
            "relation_type": raw["relation_type"],
            "product_type": raw.get("product_type"),
            "platform_or_ecosystem": raw.get("platform_or_ecosystem"),
            "pricing_model": raw.get("pricing_model"),
            "price_amount": raw.get("price_amount"),
            "currency": raw.get("currency"),
            "maintenance_status": raw.get("maintenance_status"),
            "feature_summary": raw.get("feature_summary"),
            "evidence_ids": refs,
        })
        if not entities[-1]["entity_name"]:
            raise ExternalResearchInputError(f"competitor entity_name required: {capture_id}")

    packs_by_id = {row.get("family_id"): row for row in capture.get("families", [])}
    if set(packs_by_id) != allowed_ids or len(capture.get("families", [])) != 10:
        raise ExternalResearchInputError("capture must contain exactly one family research record for each pilot identity")
    evidence_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    entities_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evidence:
        evidence_by_family[row["family_id"]].append(row)
    for row in entities:
        entities_by_family[row["family_id"]].append(row)

    def refs(family_id: str, raw_refs: Sequence[str]) -> list[str]:
        return _resolve_refs(raw_refs or [], capture_to_evidence, evidence_by_id, family_id)

    packs: list[dict[str, Any]] = []
    for family_id in PILOT_FAMILY_IDS:
        seed = by_family[family_id]
        raw = packs_by_id[family_id]
        status = raw.get("research_status")
        if status not in RESEARCH_STATUSES:
            raise ExternalResearchInputError(f"invalid research_status for {family_id}")
        concept_refs = refs(family_id, raw.get("concept_evidence_refs", []))
        primary_refs = refs(family_id, raw.get("primary_entity_evidence_refs", []))
        features = [{
            "theme": str(item.get("theme", "")).strip(),
            "evidence_ids": refs(family_id, item.get("evidence_refs", [])),
        } for item in raw.get("feature_themes", [])]
        pricing = []
        for group in raw.get("pricing_observations_by_currency", []):
            currency = group.get("currency")
            if not currency:
                raise ExternalResearchInputError(f"pricing currency group must be explicit for {family_id}")
            observations = []
            for item in group.get("observations", []):
                item_refs = refs(family_id, item.get("evidence_refs", []))
                if not item_refs or any(evidence_by_id[eid]["claim_type"] != "PRICING" or evidence_by_id[eid]["currency"] != currency for eid in item_refs):
                    raise ExternalResearchInputError(f"pricing observation has unsupported/mixed currency evidence for {family_id}")
                observations.append({
                    "observation": str(item.get("observation", "")).strip(),
                    "numeric_value": item.get("numeric_value"),
                    "numeric_unit": item.get("numeric_unit"),
                    "evidence_ids": item_refs,
                })
            pricing.append({"currency": currency, "observations": observations})
        maintenance_refs = refs(family_id, raw.get("maintenance_evidence_refs", []))
        if raw.get("maintenance_summary") is not None and (not maintenance_refs or any(evidence_by_id[eid]["claim_type"] != "MAINTENANCE" for eid in maintenance_refs)):
            raise ExternalResearchInputError(f"maintenance summary lacks maintenance evidence for {family_id}")
        popularity = []
        for item in raw.get("popularity_proxy_summary", []):
            item_refs = refs(family_id, item.get("evidence_refs", []))
            if not item_refs or any(evidence_by_id[eid]["claim_type"] != "POPULARITY_PROXY" for eid in item_refs):
                raise ExternalResearchInputError(f"popularity proxy lacks matching evidence for {family_id}")
            if len({evidence_by_id[eid]["source_url"] for eid in item_refs}) != 1:
                raise ExternalResearchInputError(f"popularity metrics must remain source-native for {family_id}")
            popularity.append({
                "metric_name": item["metric_name"],
                "numeric_value": item.get("numeric_value"),
                "numeric_unit": item.get("numeric_unit"),
                "observation": item.get("observation"),
                "evidence_ids": item_refs,
            })
        pain_points = _normalize_hypotheses(family_id, raw.get("pain_point_hypotheses", []), refs, evidence_by_id, required_claim="PAIN_POINT")
        gaps = _normalize_hypotheses(family_id, raw.get("gap_hypotheses", []), refs, evidence_by_id, required_claim=None)

        family_evidence = evidence_by_family.get(family_id, [])
        family_entities = entities_by_family.get(family_id, [])
        direct_entities = [entity for entity in family_entities if entity["relation_type"] == "DIRECT"]
        all_ids = [item["evidence_id"] for item in family_evidence]
        direct_refs = _dedupe_refs([eid for entity in direct_entities for eid in entity["evidence_ids"]])
        feature_refs = _dedupe_refs([eid for item in features for eid in item["evidence_ids"]])
        pricing_refs = _dedupe_refs([eid for group in pricing for item in group["observations"] for eid in item["evidence_ids"]])
        popularity_refs = _dedupe_refs([eid for item in popularity for eid in item["evidence_ids"]])
        pain_refs = _dedupe_refs([eid for item in pain_points for eid in item["evidence_ids"]])
        gap_refs = _dedupe_refs([eid for item in gaps for eid in item["evidence_ids"]])
        field_map = {
            "research_status": concept_refs,
            "resolved_concept_name": concept_refs,
            "concept_summary": concept_refs,
            "primary_entity_url": primary_refs,
            "direct_competitor_ids": direct_refs,
            "feature_themes": feature_refs,
            "pricing_observations_by_currency": pricing_refs,
            "maintenance_summary": maintenance_refs,
            "popularity_proxy_summary": popularity_refs,
            "pain_point_hypotheses": pain_refs,
            "gap_hypotheses": gap_refs,
        }
        all_ids = sorted(set(all_ids))
        pack = {field: seed.get(field) for field in PACK_BASE_COLUMNS}
        pack.update({
            "research_status": status,
            "resolved_concept_name": raw.get("resolved_concept_name"),
            "concept_summary": raw.get("concept_summary"),
            "primary_entity_url": canonicalize_url(raw["primary_entity_url"]) if raw.get("primary_entity_url") else None,
            "direct_competitor_count": len(direct_entities),
            "direct_competitor_ids": sorted(entity["competitor_id"] for entity in direct_entities),
            "feature_themes": features,
            "pricing_observations_by_currency": pricing,
            "maintenance_summary": raw.get("maintenance_summary"),
            "popularity_proxy_summary": popularity,
            "pain_point_hypotheses": pain_points,
            "gap_hypotheses": gaps,
            "evidence_ids": all_ids,
            "evidence_count": len(family_evidence),
            "primary_evidence_count": sum(item["source_type"].startswith("PRIMARY_") for item in family_evidence),
            "community_evidence_count": sum(item["source_type"] == "COMMUNITY" for item in family_evidence),
            "distinct_domain_count": len({item["source_domain"] for item in family_evidence}),
            "research_coverage_status": _coverage_status(
                status, len(family_evidence), len({item["source_domain"] for item in family_evidence}),
                any(item["source_type"].startswith("PRIMARY_") for item in family_evidence), bool(direct_entities),
            ),
            "research_notes": str(raw.get("research_notes", "")).strip(),
            "field_evidence_map": field_map,
        })
        packs.append(pack)

    return {
        "family_research_packs": packs,
        "external_evidence": sorted(evidence, key=lambda row: (row["consensus_rank"], row["evidence_id"])),
        "competitor_entities": sorted(entities, key=lambda row: (row["consensus_rank"], row["competitor_id"])),
        "research_queries": queries,
    }


def _normalize_hypotheses(
    family_id: str, values: Sequence[Mapping[str, Any]], resolve_refs, evidence_by_id: Mapping[str, Mapping[str, Any]],
    *, required_claim: str | None,
) -> list[dict[str, Any]]:
    result = []
    for item in values:
        text = str(item.get("hypothesis", "")).strip()
        basis = str(item.get("concrete_basis", "")).strip()
        ids = resolve_refs(family_id, item.get("evidence_refs", []))
        if not text.casefold().startswith("hypothesis:") or not basis:
            raise ExternalResearchInputError(f"hypotheses need explicit label and concrete evidence basis for {family_id}")
        urls = {evidence_by_id[eid]["source_url"] for eid in ids}
        if len(ids) < 2 or len(urls) < 2:
            raise ExternalResearchInputError(f"hypotheses need evidence from two source URLs for {family_id}")
        if required_claim and any(evidence_by_id[eid]["claim_type"] != required_claim for eid in ids):
            raise ExternalResearchInputError(f"hypothesis cites evidence of wrong claim type for {family_id}")
        result.append({"hypothesis": text, "concrete_basis": basis, "evidence_ids": ids})
    return result


def _coverage_status(status: str, evidence_count: int, domain_count: int, has_primary: bool, has_direct: bool) -> str:
    if status == "AMBIGUOUS":
        return "AMBIGUOUS"
    if status == "UNRESOLVED":
        return "UNRESOLVED"
    if evidence_count >= 4 and domain_count >= 2 and has_primary and has_direct:
        return "SUFFICIENT"
    return "PARTIAL"


def _validate_payload(
    payload: Mapping[str, Sequence[Mapping[str, Any]]], canonical_families: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    families = list(payload["family_research_packs"])
    evidence = list(payload["external_evidence"])
    entities = list(payload["competitor_entities"])
    queries = list(payload["research_queries"])
    errors: list[str] = []
    seed_by_id = {row["family_id"]: row for row in canonical_families[:10]}
    pack_by_id = {row["family_id"]: row for row in families}
    evidence_by_id = {row["evidence_id"]: row for row in evidence}
    query_by_id = {row["query_id"]: row for row in queries}
    entity_by_id = {row["competitor_id"]: row for row in entities}

    if len(families) != 10 or set(pack_by_id) != set(PILOT_FAMILY_IDS):
        errors.append("family set is not exactly the authorized ranks 1..10")
    if len(evidence_by_id) != len(evidence) or len(query_by_id) != len(queries) or len(entity_by_id) != len(entities):
        errors.append("duplicate primary keys in normalized rows")
    if len({(row["family_id"], row["canonical_url"], row["claim_type"], " ".join(row["observation"].casefold().split())) for row in evidence}) != len(evidence):
        errors.append("duplicate evidence rows remain after URL canonicalization")

    queries_by_family: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    evidence_by_family: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    entities_by_family: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in queries:
        queries_by_family[row["family_id"]].append(row)
        if row["family_id"] not in seed_by_id or row["consensus_rank"] != seed_by_id[row["family_id"]]["consensus_rank"]:
            errors.append(f"query identity/rank mismatch: {row['query_id']}")
        if not row["query_text"] or not row["purpose"] or not row["result_action"]:
            errors.append(f"incomplete query provenance: {row['query_id']}")
    for row in evidence:
        evidence_by_family[row["family_id"]].append(row)
        if row["family_id"] not in seed_by_id or row["consensus_rank"] != seed_by_id[row["family_id"]]["consensus_rank"]:
            errors.append(f"evidence identity/rank mismatch: {row['evidence_id']}")
        try:
            if canonicalize_url(row["source_url"]) != row["canonical_url"]:
                errors.append(f"noncanonical source URL: {row['evidence_id']}")
        except ExternalResearchInputError:
            errors.append(f"invalid source URL: {row['evidence_id']}")
        if row["source_type"] not in SOURCE_TYPES or row["claim_type"] not in CLAIM_TYPES:
            errors.append(f"invalid evidence enum: {row['evidence_id']}")
        if row["query_id"] is None or row["query_id"] not in query_by_id or query_by_id[row["query_id"]]["family_id"] != row["family_id"]:
            errors.append(f"dangling query reference: {row['evidence_id']}")
        if row["optional_excerpt"] is not None and len(row["optional_excerpt"].split()) > 25:
            errors.append(f"excerpt exceeds 25 words: {row['evidence_id']}")
    for row in entities:
        entities_by_family[row["family_id"]].append(row)
        if row["family_id"] not in seed_by_id or row["consensus_rank"] != seed_by_id[row["family_id"]]["consensus_rank"]:
            errors.append(f"competitor identity/rank mismatch: {row['competitor_id']}")
        referenced = [evidence_by_id[eid] for eid in row["evidence_ids"] if eid in evidence_by_id]
        if len(referenced) != len(row["evidence_ids"]):
            errors.append(f"dangling competitor evidence reference: {row['competitor_id']}")
        if any(item["family_id"] != row["family_id"] for item in referenced):
            errors.append(f"cross-family competitor evidence reference: {row['competitor_id']}")
        if not {item["claim_type"] for item in referenced}.intersection({"COMPETITOR_RELATION", "SEMANTIC_IDENTITY"}):
            errors.append(f"unsupported competitor relation: {row['competitor_id']}")
        claims = {item["claim_type"] for item in referenced}
        if row["pricing_model"] is not None and "PRICING" not in claims:
            errors.append(f"competitor pricing lacks PRICING evidence: {row['competitor_id']}")
        if row["price_amount"] is not None and (row["currency"] is None or "PRICING" not in claims):
            errors.append(f"competitor price lacks currency/PRICING evidence: {row['competitor_id']}")
        if row["feature_summary"] is not None and "FEATURE" not in claims:
            errors.append(f"competitor feature summary lacks FEATURE evidence: {row['competitor_id']}")
        if row["maintenance_status"] is not None and "MAINTENANCE" not in claims:
            errors.append(f"competitor maintenance lacks MAINTENANCE evidence: {row['competitor_id']}")
    if any(len([e for e in entities if e["family_id"] == family_id and e["relation_type"] == "DIRECT"]) > 5 for family_id in seed_by_id):
        errors.append("a family exceeds the five DIRECT competitor limit")

    expected_map_claims = {
        "research_status": {"SEMANTIC_IDENTITY"},
        "resolved_concept_name": {"SEMANTIC_IDENTITY"},
        "concept_summary": {"SEMANTIC_IDENTITY"},
        "primary_entity_url": {"SEMANTIC_IDENTITY"},
        "direct_competitor_ids": {"COMPETITOR_RELATION", "SEMANTIC_IDENTITY"},
        "feature_themes": {"FEATURE"},
        "pricing_observations_by_currency": {"PRICING"},
        "maintenance_summary": {"MAINTENANCE"},
        "popularity_proxy_summary": {"POPULARITY_PROXY"},
        "pain_point_hypotheses": {"PAIN_POINT"},
        "gap_hypotheses": {"FEATURE", "PAIN_POINT", "GAP_SIGNAL", "COMPETITOR_RELATION"},
    }
    for family_id, pack in pack_by_id.items():
        seed = seed_by_id.get(family_id, {})
        if any(pack.get(field) != seed.get(field) for field in PACK_BASE_COLUMNS):
            errors.append(f"YEE-46 preserved context/rank mutated for {family_id}")
        family_queries = queries_by_family.get(family_id, [])
        if not family_queries:
            errors.append(f"family has no search-query provenance: {family_id}")
        if len(family_queries) > 10:
            errors.append(f"family exceeds 10 search queries: {family_id}")
        if pack["research_status"] in {"AMBIGUOUS", "UNRESOLVED"} and len({row["query_text"] for row in family_queries}) < 3:
            errors.append(f"ambiguous/unresolved family lacks three query attempts: {family_id}")
        if len({row["canonical_url"] for row in evidence_by_family.get(family_id, [])}) > 15:
            errors.append(f"family exceeds 15 retained opened pages: {family_id}")
        status = pack["research_status"]
        if status not in RESEARCH_STATUSES:
            errors.append(f"invalid research_status: {family_id}")
        elif status == "RESOLVED" and (not pack["resolved_concept_name"] or not pack["concept_summary"]):
            errors.append(f"RESOLVED family lacks resolved concept/summary: {family_id}")
        elif status != "RESOLVED" and (pack["resolved_concept_name"] is not None or pack["primary_entity_url"] is not None):
            errors.append(f"non-RESOLVED family is force-mapped to a primary entity: {family_id}")

        family_evidence_ids = {row["evidence_id"] for row in evidence_by_family.get(family_id, [])}
        if set(pack["evidence_ids"]) != family_evidence_ids:
            errors.append(f"family evidence_ids do not reconcile: {family_id}")
        if pack["evidence_count"] != len(family_evidence_ids):
            errors.append(f"family evidence_count does not reconcile: {family_id}")
        if pack["primary_evidence_count"] != sum(row["source_type"].startswith("PRIMARY_") for row in evidence_by_family.get(family_id, [])):
            errors.append(f"primary evidence count does not reconcile: {family_id}")
        if pack["community_evidence_count"] != sum(row["source_type"] == "COMMUNITY" for row in evidence_by_family.get(family_id, [])):
            errors.append(f"community evidence count does not reconcile: {family_id}")
        if pack["distinct_domain_count"] != len({row["source_domain"] for row in evidence_by_family.get(family_id, [])}):
            errors.append(f"distinct domain count does not reconcile: {family_id}")
        direct = [row for row in entities_by_family.get(family_id, []) if row["relation_type"] == "DIRECT"]
        if pack["direct_competitor_count"] != len(direct) or set(pack["direct_competitor_ids"]) != {row["competitor_id"] for row in direct}:
            errors.append(f"direct competitor list does not reconcile: {family_id}")

        field_map = pack["field_evidence_map"]
        for field, allowed_claims in expected_map_claims.items():
            refs = field_map.get(field, [])
            fact_present = bool(pack.get(field)) and not (field == "research_status" and status == "UNRESOLVED")
            if fact_present and not refs:
                errors.append(f"factual family-pack field has no evidence map: {family_id}:{field}")
            resolved = [evidence_by_id[eid] for eid in refs if eid in evidence_by_id]
            if len(resolved) != len(refs) or any(row["family_id"] != family_id for row in resolved):
                errors.append(f"family-pack field map has dangling/cross-family evidence: {family_id}:{field}")
            if refs and not {row["claim_type"] for row in resolved}.intersection(allowed_claims):
                errors.append(f"family-pack field map has wrong evidence type: {family_id}:{field}")
        if pack["primary_entity_url"] and not any(
            evidence_by_id[eid]["canonical_url"] == pack["primary_entity_url"]
            for eid in field_map.get("primary_entity_url", []) if eid in evidence_by_id
        ):
            errors.append(f"primary entity URL is not directly evidenced: {family_id}")
        for group in pack["pricing_observations_by_currency"]:
            for item in group["observations"]:
                if any(evidence_by_id[eid]["currency"] != group["currency"] for eid in item["evidence_ids"]):
                    errors.append(f"mixed-currency pricing observation: {family_id}")
        for hyp in (*pack["pain_point_hypotheses"], *pack["gap_hypotheses"]):
            refs = hyp["evidence_ids"]
            if len(refs) < 2 or len({evidence_by_id[eid]["source_url"] for eid in refs}) < 2:
                errors.append(f"hypothesis lacks two evidence URLs: {family_id}")

        expected_coverage = _coverage_status(
            status, pack["evidence_count"], pack["distinct_domain_count"],
            pack["primary_evidence_count"] > 0, pack["direct_competitor_count"] > 0,
        )
        if pack["research_coverage_status"] != expected_coverage:
            errors.append(f"coverage label does not follow the declared rule: {family_id}")

    errors = sorted(set(errors))
    expected_counts = {
        "family_research_packs": 10,
        "external_evidence": len(evidence),
        "competitor_entities": len(entities),
        "research_queries": len(queries),
    }
    return {
        "status": "PASS" if not errors else "FAIL",
        "errors": errors,
        "expected_counts": expected_counts,
        "research_status_counts": dict(sorted(Counter(row["research_status"] for row in families).items())),
        "coverage_status_counts": dict(sorted(Counter(row["research_coverage_status"] for row in families).items())),
        "source_type_counts": dict(sorted(Counter(row["source_type"] for row in evidence).items())),
        "claim_type_counts": dict(sorted(Counter(row["claim_type"] for row in evidence).items())),
        "relation_type_counts": dict(sorted(Counter(row["relation_type"] for row in entities).items())),
        "query_count_by_family": {family_id: len(queries_by_family.get(family_id, [])) for family_id in PILOT_FAMILY_IDS},
        "evidence_count_by_family": {family_id: len(evidence_by_family.get(family_id, [])) for family_id in PILOT_FAMILY_IDS},
    }


def _csv_value(value: Any) -> Any:
    if value is None:
        return NULL_TOKEN
    if isinstance(value, (dict, list)):
        return _canonical_json(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _sql_type(column: str) -> str:
    if column in JSON_COLUMNS or column in TEXT_COLUMNS:
        return "TEXT"
    if column in INTEGER_COLUMNS:
        return "INTEGER"
    return "REAL"


def _stored_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return _canonical_json(value)
    if isinstance(value, bool):
        return int(value)
    return value


def _write_database(path: Path, payload: Mapping[str, Sequence[Mapping[str, Any]]], metadata: Mapping[str, Any]) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        for table, columns in EXPORT_TABLES.items():
            definitions = ",".join(f'"{column}" {_sql_type(column)}' for column in columns)
            primary = {
                "family_research_packs": ",PRIMARY KEY(family_id),UNIQUE(consensus_rank)",
                "external_evidence": ",PRIMARY KEY(evidence_id),FOREIGN KEY(family_id) REFERENCES family_research_packs(family_id)",
                "competitor_entities": ",PRIMARY KEY(competitor_id),FOREIGN KEY(family_id) REFERENCES family_research_packs(family_id)",
                "research_queries": ",PRIMARY KEY(query_id),UNIQUE(query_sequence),FOREIGN KEY(family_id) REFERENCES family_research_packs(family_id)",
            }[table]
            connection.execute(f"CREATE TABLE {table} ({definitions}{primary})")
        connection.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY,value_json TEXT NOT NULL)")
        connection.execute("CREATE INDEX external_evidence_family_idx ON external_evidence(family_id,consensus_rank)")
        connection.execute("CREATE INDEX external_evidence_canonical_url_idx ON external_evidence(canonical_url)")
        connection.execute("CREATE INDEX competitor_entities_family_idx ON competitor_entities(family_id,relation_type)")
        connection.execute("CREATE INDEX research_queries_family_idx ON research_queries(family_id,query_sequence)")
        for table, columns in EXPORT_TABLES.items():
            placeholders = ",".join("?" for _ in columns)
            values = [tuple(_stored_value(row.get(column)) for column in columns) for row in payload[table]]
            connection.executemany(f"INSERT INTO {table} VALUES ({placeholders})", values)
        connection.executemany(
            "INSERT INTO metadata(key,value_json) VALUES (?,?)",
            [(key, _canonical_json(value)) for key, value in sorted(metadata.items())],
        )
        connection.commit()
        connection.execute("VACUUM")
    finally:
        connection.close()


def _write_dataset_files(
    output: Path, payload: Mapping[str, Sequence[Mapping[str, Any]]], metadata: Mapping[str, Any],
    capture: Mapping[str, Any],
) -> list[Path]:
    output.mkdir(parents=True, exist_ok=True)
    database = output / "external_market_research.sqlite"
    _write_database(database, payload, metadata)
    paths = [database]
    for table, columns in EXPORT_TABLES.items():
        jsonl = output / f"{table}.jsonl"
        with jsonl.open("w", encoding="utf-8", newline="\n") as handle:
            for row in payload[table]:
                handle.write(_canonical_json({column: row.get(column) for column in columns}) + "\n")
        csv_path = output / f"{table}.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
            writer.writeheader()
            for row in payload[table]:
                writer.writerow({column: _csv_value(row.get(column)) for column in columns})
        paths.extend((jsonl, csv_path))
    capture_path = output / "PILOT_CAPTURE.json"
    capture_path.write_text(_canonical_json(capture) + "\n", encoding="utf-8", newline="\n")
    paths.append(capture_path)
    schema_path = output / "EXTERNAL_RESEARCH_SCHEMA.md"
    schema_source = Path(__file__).resolve().parents[2] / "EXTERNAL_RESEARCH_SCHEMA.md"
    schema_path.write_text(schema_source.read_text(encoding="utf-8"), encoding="utf-8", newline="\n")
    paths.append(schema_path)
    return paths


def _output_checks(output: Path, payload: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    connection = sqlite3.connect(output / "external_market_research.sqlite")
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign = connection.execute("PRAGMA foreign_key_check").fetchall()
        counts = {table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in EXPORT_TABLES}
        columns_match = all(
            [row[1] for row in connection.execute(f"PRAGMA table_info({table})")] == list(columns)
            for table, columns in EXPORT_TABLES.items()
        )
        return {
            "sqlite_integrity_ok": integrity == "ok",
            "foreign_key_check_ok": not foreign,
            "table_counts": counts,
            "table_counts_match": counts == {table: len(rows) for table, rows in payload.items()},
            "table_columns_match_schema": columns_match,
        }
    finally:
        connection.close()


def _report(qa: Mapping[str, Any]) -> str:
    details = qa["details"]
    lines = [
        "# YEE-47 External Market Evidence v0 — Pilot Report",
        "",
        f"Status: **{qa['status']}**",
        "",
        "## Scope and inputs",
        "",
        f"- Authorized family ranks: {', '.join(str(rank) for rank in range(1, 11))}; no ranks 11..100 were researched.",
        f"- Accepted YEE-46 shortlist SHA-256: `{qa['inputs']['shortlist_sha256']}`.",
        f"- Accepted YEE-46 analysis DB SHA-256: `{qa['inputs']['analysis_db_sha256']}` (read-only).",
        f"- Shortlist hash reconciliation: {qa['details']['input_hash_reconciliation']['resolution']} Spec literal has {qa['details']['input_hash_reconciliation']['spec_literal_length']} characters (invalid SHA-256 length); accepted manifest records the verified 64-character hash. [YEE-46 manifest]({qa['details']['input_hash_reconciliation']['manifest_url']}).",
        f"- Input SHA/count/identity unchanged: {qa['checks']['canonical_inputs_unchanged']}.",
        "- No re-ranking, new opportunity score, product recommendation, revenue/TAM estimate, YEE-24 API/token work, or auth bypass was performed.",
        "",
        "## Pilot coverage",
        "",
        f"- Research packs: {details['row_counts']['family_research_packs']}; evidence: {details['row_counts']['external_evidence']}; competitor relationships: {details['row_counts']['competitor_entities']}; searches: {details['row_counts']['research_queries']}.",
        f"- Research statuses: `{_canonical_json(details['research_status_counts'])}`.",
        f"- Coverage labels: `{_canonical_json(details['coverage_status_counts'])}`.",
        f"- Source-type counts: `{_canonical_json(details['source_type_counts'])}`.",
        f"- Relation counts: `{_canonical_json(details['relation_type_counts'])}`.",
        f"- Claim-type counts: `{_canonical_json(details['claim_type_counts'])}`.",
        f"- Opened-page counts by rank: `{_canonical_json(details['opened_page_count_by_rank'])}`.",
        "",
        "## Family-level gaps/ambiguity",
        "",
    ]
    for pack in details["family_summaries"]:
        if pack["research_status"] != "RESOLVED" or pack["research_coverage_status"] != "SUFFICIENT":
            lines.append(f"- Rank {pack['consensus_rank']} `{pack['canonical_topic_key']}`: {pack['research_status']} / {pack['research_coverage_status']} — {pack['research_notes']}")
    if not any(pack["research_status"] != "RESOLVED" or pack["research_coverage_status"] != "SUFFICIENT" for pack in details["family_summaries"]):
        lines.append("- No pilot family was ambiguous, unresolved, or below the declared sufficient-coverage threshold.")
    lines.extend((
        "",
        "## QA and stop condition",
        "",
        f"- All acceptance gates: {qa['status']}; errors: {len(qa['errors'])}.",
        f"- SQLite integrity/FK, row reconciliation, and deterministic replay: {details['output_checks']['sqlite_integrity_ok']}/{details['output_checks']['foreign_key_check_ok']}/{qa['checks']['deterministic_replay_byte_identical']}.",
        "- Handoff state: `EXTERNAL_EVIDENCE_PILOT_READY_FOR_SUPERVISOR_REVIEW`; supervisor review is pending and ranks 11..100 remain unauthorized.",
        "",
    ))
    return "\n".join(lines)


def _write_final(output: Path, qa: dict[str, Any]) -> dict[str, Any]:
    (output / "QA_RESULT.json").write_text(_canonical_json(qa) + "\n", encoding="utf-8", newline="\n")
    (output / "PILOT_REPORT.md").write_text(_report(qa), encoding="utf-8", newline="\n")
    files = [path for path in output.iterdir() if path.is_file() and path.name != "DATASET_MANIFEST.json"]
    manifest = {
        "work_order": WORK_ORDER,
        "status": qa["status"],
        "schema_version": SCHEMA_VERSION,
        "baseline_commit": BASELINE_COMMIT,
        "authorized_scope": {"stage": "PILOT", "consensus_ranks": list(range(1, 11)), "ranks_11_100_authorized": False},
        "inputs": qa["inputs"],
        "row_counts": qa["details"]["row_counts"],
        "artifact_hash_note": "Manifest lists every pilot deliverable except itself to avoid recursive hashing.",
        "artifacts": [
            {"path": path.name, "bytes": path.stat().st_size, "sha256": _sha256_file(path)}
            for path in sorted(files, key=lambda item: item.name)
        ],
    }
    (output / "DATASET_MANIFEST.json").write_text(_canonical_json(manifest) + "\n", encoding="utf-8", newline="\n")
    return manifest


def build_pilot(
    shortlist_path: str | Path, analysis_db_path: str | Path, capture_path: str | Path, output_dir: str | Path,
) -> dict[str, Any]:
    output = Path(output_dir).resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"output directory is not empty: {output}")
    canonical_families, hashes_before = _load_accepted_inputs(shortlist_path, analysis_db_path)
    capture_path = Path(capture_path).resolve()
    capture = json.loads(capture_path.read_text(encoding="utf-8"))
    payload = _normalize_capture(capture, canonical_families)
    validation = _validate_payload(payload, canonical_families)
    opened_page_count_by_rank = {
        str(rank): len({
            canonicalize_url(str(page["source_url"])) for page in capture.get("opened_pages", [])
            if page.get("family_id") == PILOT_FAMILY_IDS[rank - 1]
        })
        for rank in range(1, 11)
    }

    metadata = {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "baseline_commit": BASELINE_COMMIT,
        "accepted_shortlist_sha256": hashes_before["shortlist"],
        "accepted_analysis_db_sha256": hashes_before["analysis_db"],
        "authorization": "PILOT_ONLY_CONSENSUS_RANKS_1_TO_10",
    }
    output.mkdir(parents=True, exist_ok=True)
    artifact_paths = _write_dataset_files(output, payload, metadata, capture)
    replay_identical = False
    with tempfile.TemporaryDirectory(prefix="yee47-replay-") as replay_temp:
        replay_path = Path(replay_temp)
        replay_families, replay_hashes = _load_accepted_inputs(shortlist_path, analysis_db_path)
        replay_payload = _normalize_capture(capture, replay_families)
        replay_paths = _write_dataset_files(replay_path, replay_payload, metadata, capture)
        actual = {path.name: _sha256_file(path) for path in artifact_paths}
        replay = {path.name: _sha256_file(path) for path in replay_paths}
        replay_identical = actual == replay and replay_hashes == hashes_before and replay_payload == payload

    hashes_after = {
        "shortlist": _sha256_file(Path(shortlist_path).resolve()),
        "analysis_db": _sha256_file(Path(analysis_db_path).resolve()),
    }
    output_checks = _output_checks(output, payload)
    counts = {table: len(rows) for table, rows in payload.items()}
    checks = {
        "accepted_input_hashes_match": hashes_before == {"shortlist": ACCEPTED_SHORTLIST_SHA256, "analysis_db": ACCEPTED_ANALYSIS_DB_SHA256},
        "accepted_shortlist_hash_reconciled_to_y46_manifest": len(SPEC_SHORTLIST_SHA256_LITERAL) != 64 and hashes_before["shortlist"] == ACCEPTED_SHORTLIST_SHA256,
        "canonical_inputs_unchanged": hashes_after == hashes_before,
        "exact_authorized_pilot_family_set": len(payload["family_research_packs"]) == 10 and [row["family_id"] for row in payload["family_research_packs"]] == list(PILOT_FAMILY_IDS),
        "exact_pilot_ranks_1_to_10": [row["consensus_rank"] for row in payload["family_research_packs"]] == list(range(1, 11)),
        "all_pilot_families_have_search_provenance": all(validation["query_count_by_family"].values()),
        "research_budgets_respected": all(count <= 10 for count in validation["query_count_by_family"].values()) and all(count <= 15 for count in opened_page_count_by_rank.values()),
        "evidence_and_field_references_reconcile": not validation["errors"],
        "no_mixed_currency_or_unsupported_aggregations": not any("mixed-currency" in error for error in validation["errors"]),
        "no_rank_change_or_forbidden_output": all(row.get("triage_bucket") == "ADVANCE_RESEARCH" for row in payload["family_research_packs"]) and not any("opportunity_score" in key or "recommendation" in key for table in payload.values() for row in table for key in row),
        "coverage_labels_follow_declared_rules": all(row["research_coverage_status"] == _coverage_status(row["research_status"], row["evidence_count"], row["distinct_domain_count"], row["primary_evidence_count"] > 0, row["direct_competitor_count"] > 0) for row in payload["family_research_packs"]),
        "sqlite_integrity_check": output_checks["sqlite_integrity_ok"],
        "sqlite_foreign_key_check": output_checks["foreign_key_check_ok"],
        "sqlite_jsonl_csv_counts_and_schema_reconcile": output_checks["table_counts_match"] and output_checks["table_columns_match_schema"],
        "deterministic_replay_byte_identical": replay_identical,
        "opened_source_pages_registered_and_within_budget": all(0 <= count <= 15 for count in opened_page_count_by_rank.values()) and all(
            evidence["query_id"] is not None for evidence in payload["external_evidence"]
        ),
    }
    status = "PASS" if validation["status"] == "PASS" and all(checks.values()) else "FAIL"
    qa = {
        "work_order": WORK_ORDER,
        "status": status,
        "schema_version": SCHEMA_VERSION,
        "stage": "PILOT",
        "inputs": {
            "shortlist_file": Path(shortlist_path).name,
            "shortlist_sha256": hashes_before["shortlist"],
            "analysis_db_file": Path(analysis_db_path).name,
            "analysis_db_sha256": hashes_before["analysis_db"],
            "sha256_after": hashes_after,
            "baseline_commit": BASELINE_COMMIT,
            "shortlist_hash_reconciliation": {
                "spec_literal": SPEC_SHORTLIST_SHA256_LITERAL,
                "spec_literal_length": len(SPEC_SHORTLIST_SHA256_LITERAL),
                "verified_local_sha256": hashes_before["shortlist"],
                "accepted_manifest_url": ACCEPTED_YEE46_MANIFEST_URL,
                "resolution": "Use the valid 64-character SHA-256 corroborated by the accepted YEE-46 manifest; the Worker Spec literal contains one extra character.",
            },
        },
        "checks": checks,
        "errors": validation["errors"],
        "details": {
            "row_counts": counts,
            "research_status_counts": validation["research_status_counts"],
            "coverage_status_counts": validation["coverage_status_counts"],
            "source_type_counts": validation["source_type_counts"],
            "claim_type_counts": validation["claim_type_counts"],
            "relation_type_counts": validation["relation_type_counts"],
            "query_count_by_family": validation["query_count_by_family"],
            "opened_page_count_by_rank": opened_page_count_by_rank,
            "evidence_count_by_family": validation["evidence_count_by_family"],
            "input_hash_reconciliation": {
                "spec_literal": SPEC_SHORTLIST_SHA256_LITERAL,
                "spec_literal_length": len(SPEC_SHORTLIST_SHA256_LITERAL),
                "verified_sha256": hashes_before["shortlist"],
                "accepted_manifest_sha256": ACCEPTED_SHORTLIST_SHA256,
                "manifest_url": ACCEPTED_YEE46_MANIFEST_URL,
                "resolution": "spec literal typo reconciled against accepted YEE-46 manifest",
            },
            "family_summaries": [
                {key: row[key] for key in ("consensus_rank", "canonical_topic_key", "research_status", "research_coverage_status", "research_notes")}
                for row in payload["family_research_packs"]
            ],
            "output_checks": output_checks,
            "dataset_artifact_sha256": dict(sorted((path.name, _sha256_file(path)) for path in artifact_paths)),
        },
        "authorization": {"pilot_ranks": list(range(1, 11)), "ranks_11_100_authorized": False},
    }
    manifest = _write_final(output, qa)
    if status != "PASS":
        raise RuntimeError(f"YEE-47 pilot QA failed; inspect {output / 'QA_RESULT.json'}")
    return {"status": status, "output_dir": output, "qa": qa, "manifest": manifest}
