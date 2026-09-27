"""Deterministic YEE-77 external-research bundle builder.

The only analytical input is the pinned YEE-76 SQLite snapshot. Research is
captured separately and normalized without making network requests.
"""

from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import sqlite3
import shutil
import tempfile
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


SCHEMA_VERSION = "yee-77-category-targeted-external-research-v0.1"
INPUT_SHA256 = "700c22ad7bdd2ba9502e5adf933b3994e4ad84a852caef26d400094a9e8734bb"
CAPTURE_SHA256 = "818c9734b55a0b168a16c5d7dce4f9efbcef106f726b60327c543797adec0ece"
INPUT_RUN_ID = "acfbe03b846c5b3387695b67ad2cef4cade33fa81eabb8069b038ac797615a7b"
INPUT_CODE_COMMIT = "71f24f8e825eaef5f989082d59ef0fa7e5f0bad4"
INPUT_MERGE_COMMIT = "03ffe29974681646780d5e6f40ade940d0480780"
INPUT_SCHEMA_VERSION = "yee-76-category-opportunity-map-v0.1"
INPUT_TAXONOMY_VERSION = "yee-61-functional-category-taxonomy-v0.1"
INPUT_TAXONOMY_SHA256 = "b5720325dea06863408dfa1a05e2f981ecfeb3fac4a9e7266083ee17839c5126"
DIRECTION_IDS_SHA256 = "5751f459900e9904368e06ac16c05d42620908520cdd610c1f2bf45dda9c4831"
IMMUTABLE_OPTION_FIELDS = (
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
REQUIRED_QUERY_PURPOSES = (
    "semantic resolution",
    "competitor discovery",
    "operator pain-point discovery",
)
ZERO_OPTION_CATEGORY_STATUS = {
    "minigames": "NO_STAGE_D_RESEARCH_OPTIONS",
    "roleplay": "NO_STAGE_D_RESEARCH_OPTIONS",
    "world_management": "NO_STAGE_D_RESEARCH_OPTIONS",
    "uncategorized": "NO_STAGE_D_RESEARCH_OPTIONS",
    "server_utilities": "INSUFFICIENT_STAGE_D_EVIDENCE",
}
EXPECTED_CATEGORY_ORDER = (
    "administration", "communication", "developer_tools", "economy", "gameplay",
    "minigames", "protection", "roleplay", "world_management", "server_utilities", "uncategorized",
)
EXPECTED_OPTION_COUNTS = {
    "administration": 1, "communication": 2, "developer_tools": 1, "economy": 2,
    "gameplay": 9, "protection": 1, "minigames": 0, "roleplay": 0,
    "server_utilities": 0, "uncategorized": 0, "world_management": 0,
}
ALLOWED_SOURCE_TYPES = {
    "PRIMARY_PRODUCT", "PRIMARY_DOCS", "PRIMARY_REPOSITORY", "MARKETPLACE_LISTING",
    "PRIMARY_SUPPORT", "COMMUNITY", "EDITORIAL",
}
ALLOWED_ACCESS_STATUSES = {"OPENED", "OPENED_PARTIAL", "UNAVAILABLE_AFTER_ATTEMPT"}
ALLOWED_RELATION_TYPES = {
    "EXTERNALLY_EQUIVALENT", "SUBSTANTIAL_OVERLAP", "DISTINCT_RELATED", "CONFLICTING_OR_UNRESOLVED",
}
TRACKING_QUERY_KEYS = {"fbclid", "gclid", "dclid", "mc_cid", "mc_eid", "ref", "source"}


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _current_execution_commit() -> str:
    repo_root = Path(__file__).resolve().parents[2]
    status = subprocess.run(
        ["git", "status", "--porcelain"], cwd=repo_root, check=True, capture_output=True, text=True
    ).stdout.strip()
    if status:
        raise RuntimeError("YEE-77 production artifact build requires a clean committed worktree")
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo_root, check=True, capture_output=True, text=True
    ).stdout.strip()
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise RuntimeError("Unable to determine the YEE-77 execution code commit")
    subprocess.run(
        ["git", "merge-base", "--is-ancestor", INPUT_MERGE_COMMIT, commit],
        cwd=repo_root, check=True, capture_output=True, text=True,
    )
    return commit


def _read_snapshot(path: Path) -> tuple[str, list[dict], list[dict], dict, dict, dict]:
    actual_sha = sha256_file(path)
    if actual_sha != INPUT_SHA256:
        raise ValueError(f"YEE-76 input hash mismatch: {actual_sha}")
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        integrity_ok = [tuple(row) for row in connection.execute("PRAGMA integrity_check")] == [("ok",)]
        foreign_keys_ok = connection.execute("PRAGMA foreign_key_check").fetchall() == []
        options = [
            json.loads(row["record_json"])
            for row in connection.execute(
                "SELECT record_json FROM category_research_options ORDER BY category_order, direction_id"
            )
        ]
        categories = [
            json.loads(row["category_json"])
            for row in connection.execute(
                "SELECT category_json FROM frozen_category_snapshot ORDER BY category_order"
            )
        ]
        metadata = dict(connection.execute("SELECT key, value FROM run_metadata"))
        provenance = dict(connection.execute("SELECT key, value FROM input_provenance"))
        row_counts = {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "category_opportunity_profiles", "category_direction_tiers", "category_research_options",
                "frozen_category_snapshot",
            )
        }
    finally:
        connection.close()
    identity_text = "".join(f"{row['direction_id']}\n" for row in sorted(options, key=lambda x: x["direction_id"]))
    option_counts = Counter(row["category_id"] for row in options)
    category_order = tuple(row["category_id"] for row in sorted(categories, key=lambda row: row["category_order"]))
    input_checks = {
        "input_opened_read_only": True,
        "accepted_input_sha256_before": actual_sha == INPUT_SHA256,
        "accepted_yee76_run_and_commit_pins": (
            metadata.get("run_id") == INPUT_RUN_ID
            and metadata.get("code_commit") == INPUT_CODE_COMMIT
            and metadata.get("work_order") == "YEE-76"
            and metadata.get("schema_version") == INPUT_SCHEMA_VERSION
        ),
        "accepted_taxonomy_provenance": (
            metadata.get("taxonomy_version") == INPUT_TAXONOMY_VERSION
            and metadata.get("taxonomy_sha256") == INPUT_TAXONOMY_SHA256
            and provenance.get("taxonomy_version") == INPUT_TAXONOMY_VERSION
            and provenance.get("taxonomy_sha256") == INPUT_TAXONOMY_SHA256
            and provenance.get("input_read_only") == "true"
        ),
        "input_row_counts_11_47_16": row_counts == {
            "category_opportunity_profiles": 11,
            "category_direction_tiers": 47,
            "category_research_options": 16,
            "frozen_category_snapshot": 11,
        },
        "input_category_order_and_option_distribution": (
            category_order == EXPECTED_CATEGORY_ORDER
            and dict(option_counts) == {key: value for key, value in EXPECTED_OPTION_COUNTS.items() if value}
        ),
        "input_sqlite_integrity_and_foreign_keys": integrity_ok and foreign_keys_ok,
    }
    input_checks["accepted_direction_identity_sha256"] = (
        hashlib.sha256(identity_text.encode("utf-8")).hexdigest() == DIRECTION_IDS_SHA256
    )
    if not all(input_checks.values()):
        raise ValueError(f"YEE-76 accepted snapshot failed pinned input gates: {[key for key, value in input_checks.items() if not value]}")
    return actual_sha, options, categories, metadata, provenance, input_checks


def _source_domains(sources: list[dict]) -> dict[str, str]:
    result = {}
    for source in sources:
        result[source["source_id"]] = (urlsplit(source["canonical_url"]).hostname or "").lower()
    return result


def _canonicalize_url(value: str) -> str:
    parsed = urlsplit(value.strip())
    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").lower()
    if scheme not in {"http", "https"} or not hostname or parsed.username or parsed.password:
        raise ValueError(f"Source URL is not public HTTP(S): {value}")
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError(f"Source URL has an invalid port: {value}") from error
    netloc = hostname
    if ":" in hostname and not hostname.startswith("["):
        netloc = f"[{hostname}]"
    if port is not None and not ((scheme == "https" and port == 443) or (scheme == "http" and port == 80)):
        netloc = f"{netloc}:{port}"
    query = [
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if key.casefold() not in TRACKING_QUERY_KEYS and not key.casefold().startswith("utm_")
    ]
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/") or "/"
    return urlunsplit((scheme, netloc, path, urlencode(sorted(query)), ""))


def _normalise_hypotheses(
    capture: dict, evidence: list[dict], sources_by_id: dict[str, dict], accepted_direction_ids: set[str]
) -> dict[str, list[dict]]:
    evidence_by_id = {row["evidence_id"]: row for row in evidence}
    hypotheses_by_direction: dict[str, list[dict]] = defaultdict(list)
    for row in capture.get("differentiation_hypotheses", []):
        direction_id = row["direction_id"]
        if direction_id not in accepted_direction_ids:
            raise ValueError("Differentiation hypothesis references an unknown direction")
        evidence_ids = sorted(set(row.get("evidence_ids", [])))
        if row.get("label") != "HYPOTHESIS" or not row.get("hypothesis") or not row.get("observed_evidence"):
            raise ValueError("Differentiation hypothesis must be labeled HYPOTHESIS and state observed evidence")
        if len(evidence_ids) < 2 or any(evidence_id not in evidence_by_id for evidence_id in evidence_ids):
            raise ValueError("Differentiation hypothesis must reference at least two retained evidence rows")
        rows = [evidence_by_id[evidence_id] for evidence_id in evidence_ids]
        if any(item["direction_id"] != direction_id for item in rows):
            raise ValueError("Differentiation hypothesis evidence must belong to the same direction")
        if len({sources_by_id[item["source_id"]]["canonical_url"] for item in rows}) < 2:
            raise ValueError("Differentiation hypothesis requires evidence from two canonical source URLs")
        semantic_only = row.get("purely_semantic_or_overlap") is True and all(
            item["claim_type"] in {"SEMANTIC_IDENTITY", "OVERLAP_SIGNAL"} for item in rows
        )
        if not semantic_only and not any(item["claim_type"] in {"FEATURE", "OPERATOR_PAIN"} for item in rows):
            raise ValueError("Differentiation hypothesis requires FEATURE or OPERATOR_PAIN evidence")
        hypotheses_by_direction[direction_id].append({
            "label": "HYPOTHESIS",
            "hypothesis": row["hypothesis"],
            "observed_evidence": row["observed_evidence"],
            "evidence_ids": evidence_ids,
            "purely_semantic_or_overlap": semantic_only,
        })
    return {
        key: sorted(rows, key=lambda item: (item["hypothesis"], item["evidence_ids"]))
        for key, rows in hypotheses_by_direction.items()
    }


def _normalise_capture(snapshot_options: list[dict], capture: dict) -> dict:
    options_by_id = {row["direction_id"]: row for row in snapshot_options}
    capture = json.loads(json.dumps(capture))
    raw_sources = sorted(capture["source_documents"], key=lambda row: row["source_id"])
    source_groups: dict[str, list[dict]] = defaultdict(list)
    source_alias_to_id: dict[str, str] = {}
    source_id_urls: dict[str, str] = {}
    for raw in raw_sources:
        canonical_url = _canonicalize_url(raw["canonical_url"])
        source_id = raw.get("source_id")
        if not source_id:
            raise ValueError("Source document is missing source_id")
        previous_url = source_id_urls.setdefault(source_id, canonical_url)
        if previous_url != canonical_url:
            raise ValueError(f"Source ID is reused for different canonical URLs: {source_id}")
        source_groups[canonical_url].append(raw)

    sources = []
    for canonical_url, group in sorted(source_groups.items()):
        group = sorted(group, key=lambda row: row["source_id"])
        if len({(row["source_type"], row["access_status"]) for row in group}) > 1:
            raise ValueError(f"Conflicting source metadata for canonical URL: {canonical_url}")
        primary = group[0]
        source = {
            "source_id": primary["source_id"],
            "canonical_url": canonical_url,
            "source_url": primary.get("source_url", primary["canonical_url"]),
            "source_domain": (urlsplit(canonical_url).hostname or "").lower(),
            "source_title": primary.get("source_title", primary.get("title")),
            "source_type": primary["source_type"],
            "retrieved_at": primary.get("retrieved_at", capture["retrieved_at"]),
            "published_or_updated_at": primary.get("published_or_updated_at"),
            "access_status": primary["access_status"],
            "notes": " ".join(sorted({
                row.get("notes", "").strip() for row in group if row.get("notes", "").strip()
            })),
            "source_id_aliases": sorted({row["source_id"] for row in group}),
        }
        if source["source_type"] not in ALLOWED_SOURCE_TYPES or source["access_status"] not in ALLOWED_ACCESS_STATUSES:
            raise ValueError(f"Unknown source type or access status: {source['source_id']}")
        if not source["source_title"]:
            raise ValueError(f"Missing source title: {source['source_id']}")
        sources.append(source)
        for row in group:
            source_alias_to_id[row["source_id"]] = source["source_id"]
    source_by_id = {row["source_id"]: row for row in sources}

    evidence = []
    for raw in sorted(capture["external_evidence"], key=lambda row: row["evidence_id"]):
        direction_id = raw["direction_id"]
        category_id = raw["category_id"]
        source_id = source_alias_to_id.get(raw["source_id"])
        option = options_by_id.get(direction_id)
        if option is None or option["category_id"] != category_id:
            raise ValueError(f"Evidence has invalid direction/category: {raw['evidence_id']}")
        if source_id is None:
            raise ValueError(f"Evidence has unknown source: {raw['evidence_id']}")
        if source_by_id[source_id]["access_status"] not in {"OPENED", "OPENED_PARTIAL"}:
            raise ValueError(f"Unavailable source cannot support evidence: {raw['evidence_id']}")
        claim_type = raw["claim_type"]
        if claim_type not in {
            "SEMANTIC_IDENTITY", "COMPETITOR_RELATION", "FEATURE", "PRICING",
            "MAINTENANCE", "POPULARITY_PROXY", "OPERATOR_PAIN",
            "DIFFERENTIATION_SIGNAL", "OVERLAP_SIGNAL", "ADJACENT_CONTEXT",
        }:
            raise ValueError(f"Unknown evidence claim type: {claim_type}")
        optional_excerpt = raw.get("optional_excerpt")
        if optional_excerpt is not None and len(str(optional_excerpt).split()) > 25:
            raise ValueError(f"Evidence excerpt exceeds 25 words: {raw['evidence_id']}")
        item = {
            "evidence_id": raw["evidence_id"],
            "direction_id": direction_id,
            "category_id": category_id,
            "source_id": source_id,
            "claim_type": claim_type,
            "observation": raw["observation"],
            "optional_excerpt": optional_excerpt,
            "numeric_value": raw.get("numeric_value"),
            "numeric_unit": raw.get("numeric_unit", raw.get("unit")),
            "currency": raw.get("currency"),
            "entity_name": raw.get("entity_name"),
            "retrieved_at": raw.get("retrieved_at", source_by_id[source_id]["retrieved_at"]),
            "notes": raw.get("notes", ""),
        }
        if "feature_theme" in raw:
            item["feature_theme"] = raw["feature_theme"]
        evidence.append(item)

    evidence_by_id = {row["evidence_id"]: row for row in evidence}
    pack_notes = capture["pack_notes"]
    if set(pack_notes) != set(options_by_id):
        raise ValueError("Research capture pack-note identities do not match accepted YEE-76 options")
    if any(note.get("research_status") not in {"RESOLVED", "AMBIGUOUS", "UNRESOLVED"} for note in pack_notes.values()):
        raise ValueError("Research capture has an unknown research status")

    entities = []
    demoted_entity_ids = set()
    for raw in sorted(capture["competitor_entities"], key=lambda row: row["competitor_id"]):
        option = options_by_id.get(raw["direction_id"])
        if option is None or option["category_id"] != raw["category_id"]:
            raise ValueError(f"Competitor has invalid direction/category: {raw['competitor_id']}")
        relation_type = raw["relation_type"]
        normalization_note = raw.get("normalization_note")
        if relation_type == "DIRECT" and pack_notes[raw["direction_id"]]["research_status"] != "RESOLVED":
            relation_type = "ADJACENT"
            normalization_note = (
                "Demoted from direction-level DIRECT: the broad option is not RESOLVED, "
                "so slice-specific evidence is retained as adjacent context."
            )
            demoted_entity_ids.add(raw["competitor_id"])
        if relation_type not in {"DIRECT", "SUBSTITUTE", "ADJACENT"}:
            raise ValueError(f"Unknown competitor relation type: {raw['competitor_id']}")
        entity = {
            "competitor_id": raw["competitor_id"],
            "direction_id": raw["direction_id"],
            "category_id": raw["category_id"],
            "entity_name": raw.get("entity_name", raw.get("name")),
            "canonical_url": _canonicalize_url(raw["canonical_url"]),
            "relation_type": relation_type,
            "product_type": raw.get("product_type"),
            "platform_or_ecosystem": raw.get("platform_or_ecosystem"),
            "plugin_scope_status": raw.get("plugin_scope_status"),
            "pricing_model": raw.get("pricing_model"),
            "price_amount": raw.get("price_amount", raw.get("price")),
            "currency": raw.get("currency"),
            "maintenance_status": raw.get("maintenance_status"),
            "feature_summary": raw.get("feature_summary"),
            "evidence_ids": sorted(set(raw.get("evidence_ids", []))),
            "lifecycle_evidence_ids": sorted(set(raw.get("lifecycle_evidence_ids", []))),
            "normalization_note": normalization_note,
        }
        if not entity["entity_name"]:
            raise ValueError(f"Competitor is missing entity_name: {raw['competitor_id']}")
        if entity["relation_type"] == "DIRECT" and entity["plugin_scope_status"] != "CONFIRMED":
            raise ValueError(f"DIRECT entity lacks positive plugin-scope evidence: {raw['competitor_id']}")
        if not set(entity["evidence_ids"] + entity["lifecycle_evidence_ids"]).issubset(evidence_by_id):
            raise ValueError(f"Competitor has unknown evidence: {raw['competitor_id']}")
        if entity["plugin_scope_status"] == "NOT_PLUGIN":
            if entity["relation_type"] != "ADJACENT":
                raise ValueError(f"Non-plugin context cannot be a DIRECT or SUBSTITUTE competitor: {raw['competitor_id']}")
            if any(evidence_by_id[evidence_id]["claim_type"] != "ADJACENT_CONTEXT" for evidence_id in entity["evidence_ids"]):
                raise ValueError(f"Non-plugin entity must use ADJACENT_CONTEXT evidence: {raw['competitor_id']}")
        entity_claims = {
            evidence_by_id[evidence_id]["claim_type"]
            for evidence_id in entity["evidence_ids"]
            if evidence_id in evidence_by_id
        }
        if entity["relation_type"] == "DIRECT" and (
            not entity["product_type"]
            or not entity["platform_or_ecosystem"]
            or not ({"COMPETITOR_RELATION", "SEMANTIC_IDENTITY"} & entity_claims)
        ):
            raise ValueError(f"DIRECT entity lacks product/platform identity evidence: {raw['competitor_id']}")
        has_pricing_evidence = any(
            evidence_by_id[evidence_id]["claim_type"] == "PRICING" for evidence_id in entity["evidence_ids"]
        )
        if entity["pricing_model"] == "UNKNOWN" and not has_pricing_evidence:
            entity["pricing_model"] = None
        if not has_pricing_evidence and any(
            entity[field] is not None for field in ("pricing_model", "price_amount", "currency")
        ):
            raise ValueError(f"Pricing fields require PRICING evidence: {raw['competitor_id']}")
        if entity["price_amount"] is not None and entity["currency"] is None:
            raise ValueError(f"A non-null price requires its source currency: {raw['competitor_id']}")
        entities.append(entity)

    for entity in entities:
        if entity["competitor_id"] not in demoted_entity_ids:
            continue
        for evidence_id in entity["evidence_ids"]:
            item = evidence_by_id[evidence_id]
            if item["claim_type"] == "COMPETITOR_RELATION":
                item["claim_type"] = "ADJACENT_CONTEXT"
                item["notes"] = (
                    f"{item['notes']} Direction-level relation downgraded because the broad option is not resolved."
                ).strip()

    queries = []
    query_sequence_by_direction: Counter = Counter()
    for raw in capture["research_queries"]:
        option = options_by_id.get(raw["direction_id"])
        if option is None:
            raise ValueError(f"Query references unknown direction {raw['direction_id']}")
        raw_source_ids = list(raw.get("result_source_ids", []))
        if not set(raw_source_ids).issubset(source_alias_to_id):
            missing = sorted(set(raw_source_ids) - set(source_alias_to_id))
            raise ValueError(f"Query {raw['query_id']} references unknown sources: {missing}")
        result_source_ids = sorted({source_alias_to_id[source_id] for source_id in raw_source_ids})
        query_sequence_by_direction[raw["direction_id"]] += 1
        purpose = raw.get("purpose", raw.get("query_purpose"))
        if purpose not in REQUIRED_QUERY_PURPOSES:
            raise ValueError(f"Query has unknown purpose: {raw['query_id']}")
        issued_at = raw.get("issued_at")
        issued_at_basis = "captured_individual_query_timestamp"
        if not issued_at:
            issued_at = capture["retrieved_at"]
            issued_at_basis = "frozen_capture_retrieved_at; individual query timestamp was not preserved"
        query = {
            "query_id": raw["query_id"],
            "direction_id": raw["direction_id"],
            "category_id": option["category_id"],
            "query_sequence": query_sequence_by_direction[raw["direction_id"]],
            "query_text": raw.get("query_text", raw.get("query")),
            "issued_at": issued_at,
            "purpose": purpose,
            "result_action": None,
            "result_source_ids": result_source_ids,
            "result_evidence_ids": [],
            "lifecycle_evidence_ids": [],
            "issued_at_basis": issued_at_basis,
        }
        if not query["query_text"]:
            raise ValueError(f"Query {raw['query_id']} is missing query_text")
        queries.append(query)

    relation_rows = []
    option_category = {direction_id: row["category_id"] for direction_id, row in options_by_id.items()}
    for raw in sorted(capture.get("direction_semantic_relations", []), key=lambda row: row["relation_id"]):
        relation = dict(raw)
        endpoints = {relation["direction_id"], relation["related_direction_id"]}
        if len(endpoints) != 2 or not endpoints.issubset(option_category):
            raise ValueError(f"Relation has unknown or duplicate endpoint: {relation['relation_id']}")
        if any(option_category[key] != relation["category_id"] for key in endpoints):
            raise ValueError(f"Relation has mismatched category/endpoints: {relation['relation_id']}")
        if relation["relation_type"] == "OVERLAP_SIGNAL":
            relation["relation_type"] = "SUBSTANTIAL_OVERLAP"
        if relation["relation_type"] not in ALLOWED_RELATION_TYPES:
            raise ValueError(f"Unknown semantic relation type: {relation['relation_id']}")
        evidence_ids = sorted(set(relation.get("evidence_ids", [])))
        if not evidence_ids or any(
            evidence_id not in evidence_by_id
            or evidence_by_id[evidence_id]["category_id"] != relation["category_id"]
            or evidence_by_id[evidence_id]["direction_id"] not in endpoints
            or evidence_by_id[evidence_id]["claim_type"] not in {"OVERLAP_SIGNAL", "SEMANTIC_IDENTITY"}
            for evidence_id in evidence_ids
        ):
            raise ValueError(f"Relation has invalid evidence: {relation['relation_id']}")
        relation["evidence_ids"] = evidence_ids
        relation_rows.append(relation)

    for query in queries:
        matching = sorted(
            row["evidence_id"] for row in evidence
            if row["direction_id"] == query["direction_id"] and row["source_id"] in query["result_source_ids"]
        )
        query["result_evidence_ids"] = matching
        status = pack_notes[query["direction_id"]]["research_status"]
        query["result_action"] = (
            "RETAINED_EVIDENCE" if matching else
            "AMBIGUITY" if status in {"AMBIGUOUS", "UNRESOLVED"} else
            "DISCOVERY_LINKS_ONLY" if query["result_source_ids"] else
            "NO_USEFUL_RESULT"
        )

    for entity in entities:
        if entity["relation_type"] not in {"DIRECT", "SUBSTITUTE"}:
            continue
        lifecycle_source_ids = {
            evidence_by_id[evidence_id]["source_id"] for evidence_id in entity["lifecycle_evidence_ids"]
        }
        matched_queries = [
            query for query in queries
            if query["direction_id"] == entity["direction_id"]
            and lifecycle_source_ids.intersection(query["result_source_ids"])
        ]
        for query in matched_queries:
            query["lifecycle_evidence_ids"] = sorted(
                set(query["lifecycle_evidence_ids"]) | set(entity["lifecycle_evidence_ids"])
            )

    by_direction_evidence: dict[str, list[dict]] = defaultdict(list)
    by_direction_queries: dict[str, list[dict]] = defaultdict(list)
    by_direction_entities: dict[str, list[dict]] = defaultdict(list)
    by_direction_relations: dict[str, list[dict]] = defaultdict(list)
    for item in evidence:
        by_direction_evidence[item["direction_id"]].append(item)
    for query in queries:
        by_direction_queries[query["direction_id"]].append(query)
    for entity in entities:
        by_direction_entities[entity["direction_id"]].append(entity)
    for relation in relation_rows:
        for direction_id in (relation["direction_id"], relation["related_direction_id"]):
            by_direction_relations[direction_id].append(relation)

    hypotheses_by_direction = _normalise_hypotheses(
        capture, evidence, source_by_id, set(options_by_id)
    )
    packs = []
    for option in sorted(snapshot_options, key=lambda row: row["direction_id"]):
        direction_id = option["direction_id"]
        note = pack_notes[direction_id]
        ev_rows = by_direction_evidence[direction_id]
        query_rows = by_direction_queries[direction_id]
        entity_rows = by_direction_entities[direction_id]
        source_ids = sorted(
            {row["source_id"] for row in ev_rows}
            | {source_id for query in query_rows for source_id in query["result_source_ids"]}
        )
        evidence_source_ids = {row["source_id"] for row in ev_rows}
        pack = {field: option[field] for field in IMMUTABLE_OPTION_FIELDS}
        pack.update({
            "research_status": note["research_status"],
            "resolved_market_job": note.get("resolved_market_job"),
            "market_job_summary": note.get("market_job_summary"),
            "direct_competitor_count": sum(row["relation_type"] == "DIRECT" for row in entity_rows),
            "direct_competitor_ids": sorted(row["competitor_id"] for row in entity_rows if row["relation_type"] == "DIRECT"),
            "substitute_competitor_ids": sorted(row["competitor_id"] for row in entity_rows if row["relation_type"] == "SUBSTITUTE"),
            "feature_themes": sorted({row.get("feature_theme") for row in ev_rows if row["claim_type"] == "FEATURE" and row.get("feature_theme")}),
            "pricing_observations_by_currency": _pricing_summary(ev_rows),
            "maintenance_summary": note.get("maintenance_summary"),
            "popularity_proxy_summary": [row for row in ev_rows if row["claim_type"] == "POPULARITY_PROXY"],
            "operator_pain_observations": [row for row in ev_rows if row["claim_type"] == "OPERATOR_PAIN"],
            "differentiation_hypotheses": hypotheses_by_direction.get(direction_id, []),
            "semantic_relation_ids": sorted({row["relation_id"] for row in by_direction_relations[direction_id]}),
            "evidence_ids": sorted(row["evidence_id"] for row in ev_rows),
            "source_ids": source_ids,
            "evidence_count": len(ev_rows),
            "primary_or_marketplace_evidence_count": sum(
                source_by_id[row["source_id"]]["source_type"] in {
                    "PRIMARY_PRODUCT", "PRIMARY_DOCS", "PRIMARY_REPOSITORY", "MARKETPLACE_LISTING", "PRIMARY_SUPPORT",
                }
                for row in ev_rows
            ),
            "community_evidence_count": sum(
                source_by_id[row["source_id"]]["source_type"] == "COMMUNITY" for row in ev_rows
            ),
            "distinct_domain_count": len({source_by_id[source_id]["source_domain"] for source_id in evidence_source_ids}),
            "query_count": len(query_rows),
            "research_coverage_status": "PENDING_QA",
            "research_notes": note.get("research_notes", ""),
        })
        packs.append(pack)

    for pack in packs:
        pack_queries = by_direction_queries[pack["direction_id"]]
        pack_evidence = by_direction_evidence[pack["direction_id"]]
        if pack["research_status"] == "AMBIGUOUS":
            pack["research_coverage_status"] = "AMBIGUOUS"
        elif pack["research_status"] == "UNRESOLVED":
            pack["research_coverage_status"] = "UNRESOLVED"
        else:
            claims = {row["claim_type"] for row in pack_evidence}
            requirements = (
                len(pack_evidence) >= 5,
                pack["distinct_domain_count"] >= 2,
                pack["primary_or_marketplace_evidence_count"] >= 1,
                "SEMANTIC_IDENTITY" in claims,
                "FEATURE" in claims,
                REQUIRED_QUERY_PURPOSES[0] in {row["purpose"] for row in pack_queries},
                REQUIRED_QUERY_PURPOSES[1] in {row["purpose"] for row in pack_queries},
                REQUIRED_QUERY_PURPOSES[2] in {row["purpose"] for row in pack_queries},
                all(
                    bool(row["lifecycle_evidence_ids"])
                    for row in by_direction_entities[pack["direction_id"]]
                    if row["relation_type"] == "DIRECT"
                ),
            )
            pack["research_coverage_status"] = "SUFFICIENT" if all(requirements) else "PARTIAL"

    return {
        "source_documents": sources,
        "research_queries": queries,
        "external_evidence": evidence,
        "competitor_entities": entities,
        "direction_semantic_relations": relation_rows,
        "direction_research_packs": packs,
        "normalization_summary": {
            "capture_retrieved_at": capture["retrieved_at"],
            "capture_source_document_count": len(raw_sources),
            "canonical_source_document_count": len(sources),
            "deduplicated_source_document_count": len(raw_sources) - len(sources),
            "deduplicated_source_groups": [
                {"canonical_url": row["canonical_url"], "source_id_aliases": row["source_id_aliases"]}
                for row in sources if len(row["source_id_aliases"]) > 1
            ],
            "timestamp_fallback_query_count": sum(
                query["issued_at_basis"].startswith("frozen_capture_retrieved_at") for query in queries
            ),
            "direction_level_competitor_downgrade_ids": sorted(demoted_entity_ids),
            "direction_level_competitor_downgrade_count": len(demoted_entity_ids),
        },
    }


def _pricing_summary(evidence: list[dict]) -> dict[str, list[dict]]:
    result: dict[str, list[dict]] = defaultdict(list)
    for row in evidence:
        if row["claim_type"] == "PRICING":
            result[row.get("currency") or "UNSPECIFIED"].append({
                "evidence_id": row["evidence_id"],
                "entity_name": row.get("entity_name"),
                "numeric_value": row.get("numeric_value"),
                "numeric_unit": row.get("numeric_unit"),
                "currency": row.get("currency"),
                "observation": row["observation"],
            })
    return {key: sorted(rows, key=lambda row: row["evidence_id"]) for key, rows in sorted(result.items())}


def _qa(
    snapshot_options: list[dict], categories: list[dict], data: dict, input_sha: str,
    input_checks: dict | None = None, input_sha_after: str | None = None,
    replay_result: dict | None = None,
) -> dict:
    checks = {}
    errors = []
    options_by_id = {row["direction_id"]: row for row in snapshot_options}
    sources_by_id = {row["source_id"]: row for row in data["source_documents"]}
    evidence_by_id = {row["evidence_id"]: row for row in data["external_evidence"]}
    entities = data["competitor_entities"]
    packs_by_id = {row["direction_id"]: row for row in data["direction_research_packs"]}
    queries = data["research_queries"]
    queries_by_direction: dict[str, list[dict]] = defaultdict(list)
    for query in queries:
        queries_by_direction[query["direction_id"]].append(query)

    checks["accepted_input_sha256_before"] = input_sha == INPUT_SHA256
    checks["accepted_input_sha256_before_and_after_unchanged"] = (
        input_sha_after == input_sha and input_sha_after == INPUT_SHA256
    )
    checks["accepted_input_read_only_and_pinned_gates"] = (
        input_checks is not None and bool(input_checks) and all(input_checks.values())
    )
    checks["exact_16_option_identity_reconciliation"] = (
        len(snapshot_options) == 16
        and len(data["direction_research_packs"]) == 16
        and len(options_by_id) == 16
        and set(options_by_id) == set(packs_by_id)
    )
    identity_text = "".join(f"{direction_id}\n" for direction_id in sorted(options_by_id))
    checks["accepted_direction_identity_set_sha256"] = (
        hashlib.sha256(identity_text.encode("utf-8")).hexdigest() == DIRECTION_IDS_SHA256
        and input_checks is not None
        and input_checks.get("accepted_direction_identity_sha256", False)
    )
    checks["all_11_input_categories_in_frozen_order"] = (
        len(categories) == 11
        and tuple(row["category_id"] for row in sorted(categories, key=lambda row: row["category_order"]))
        == EXPECTED_CATEGORY_ORDER
    )
    checks["deterministic_frozen_capture_replay"] = (
        replay_result is not None and replay_result.get("byte_identical") is True
        and replay_result.get("capture_sha256") == CAPTURE_SHA256
    )
    checks["immutable_stage_d_option_fields_preserved"] = all(
        all(pack.get(field) == options_by_id[direction_id].get(field) for field in IMMUTABLE_OPTION_FIELDS)
        for direction_id, pack in packs_by_id.items()
    )

    required_source_fields = {
        "source_id", "canonical_url", "source_url", "source_domain", "source_title",
        "source_type", "retrieved_at", "published_or_updated_at", "access_status", "notes",
    }
    required_query_fields = {
        "query_id", "direction_id", "category_id", "query_sequence", "query_text",
        "issued_at", "issued_at_basis", "purpose", "result_action", "result_source_ids",
        "result_evidence_ids", "lifecycle_evidence_ids",
    }
    required_evidence_fields = {
        "evidence_id", "direction_id", "category_id", "source_id", "claim_type",
        "observation", "optional_excerpt", "numeric_value", "numeric_unit",
        "currency", "entity_name", "retrieved_at", "notes",
    }
    required_entity_fields = {
        "competitor_id", "direction_id", "category_id", "entity_name", "canonical_url",
        "relation_type", "product_type", "platform_or_ecosystem", "plugin_scope_status",
        "pricing_model", "price_amount", "currency", "maintenance_status",
        "feature_summary", "evidence_ids",
    }
    required_pack_fields = set(IMMUTABLE_OPTION_FIELDS) | {
        "research_status", "resolved_market_job", "market_job_summary", "direct_competitor_count",
        "direct_competitor_ids", "substitute_competitor_ids", "feature_themes",
        "pricing_observations_by_currency", "maintenance_summary", "popularity_proxy_summary",
        "operator_pain_observations", "differentiation_hypotheses", "semantic_relation_ids",
        "evidence_ids", "source_ids", "evidence_count", "primary_or_marketplace_evidence_count",
        "community_evidence_count", "distinct_domain_count", "query_count", "research_coverage_status",
        "research_notes",
    }
    checks["normalized_source_schema_names"] = all(
        required_source_fields.issubset(row) and "domain" not in row and "title" not in row
        for row in data["source_documents"]
    )
    checks["normalized_query_schema_names"] = all(
        required_query_fields.issubset(row)
        and not {"query", "query_purpose"}.intersection(row)
        for row in queries
    )
    checks["normalized_evidence_schema_names"] = all(
        required_evidence_fields.issubset(row) and "unit" not in row
        for row in data["external_evidence"]
    )
    checks["normalized_competitor_schema_names"] = all(
        required_entity_fields.issubset(row) and not {"name", "price"}.intersection(row)
        for row in entities
    )
    checks["research_pack_schema_contract"] = all(
        required_pack_fields.issubset(row) for row in data["direction_research_packs"]
    )
    checks["source_urls_are_canonical_and_deduplicated"] = (
        len(sources_by_id) == len({row["canonical_url"] for row in data["source_documents"]})
        and all(
            row["canonical_url"] == _canonicalize_url(row["canonical_url"])
            and row["source_domain"] == (urlsplit(row["canonical_url"]).hostname or "").lower()
            and row["source_type"] in ALLOWED_SOURCE_TYPES
            and row["access_status"] in {"OPENED", "OPENED_PARTIAL", "UNAVAILABLE_AFTER_ATTEMPT"}
            for row in data["source_documents"]
        )
    )
    checks["research_row_ids_are_unique"] = all(
        len(rows) == len({row[key] for row in rows})
        for rows, key in (
            (data["source_documents"], "source_id"),
            (queries, "query_id"),
            (data["external_evidence"], "evidence_id"),
            (entities, "competitor_id"),
            (data["direction_semantic_relations"], "relation_id"),
        )
    )

    checks["all_query_source_references_resolve"] = all(
        query["direction_id"] in options_by_id
        and query["category_id"] == options_by_id[query["direction_id"]]["category_id"]
        and set(query.get("result_source_ids", [])).issubset(sources_by_id)
        and set(query.get("result_evidence_ids", [])).issubset(evidence_by_id)
        and all(
            evidence_by_id[evidence_id]["direction_id"] == query["direction_id"]
            and evidence_by_id[evidence_id]["source_id"] in query.get("result_source_ids", [])
            for evidence_id in query.get("result_evidence_ids", [])
            if evidence_id in evidence_by_id
        )
        and set(query.get("lifecycle_evidence_ids", [])).issubset(evidence_by_id)
        for query in queries
    )
    checks["all_evidence_references_resolve_to_open_sources"] = all(
        item["direction_id"] in options_by_id
        and item["category_id"] == options_by_id[item["direction_id"]]["category_id"]
        and item["source_id"] in sources_by_id
        and sources_by_id[item["source_id"]]["access_status"] in {"OPENED", "OPENED_PARTIAL"}
        for item in data["external_evidence"]
    )
    referenced_sources = (
        {item["source_id"] for item in data["external_evidence"]}
        | {source_id for query in queries for source_id in query.get("result_source_ids", [])}
    )
    checks["all_source_documents_are_used_by_queries_or_evidence"] = set(sources_by_id) == referenced_sources
    checks["evidence_excerpts_obey_25_word_limit"] = all(
        item.get("optional_excerpt") is None or len(str(item["optional_excerpt"]).split()) <= 25
        for item in data["external_evidence"]
    )

    query_actions = {"RETAINED_EVIDENCE", "DISCOVERY_LINKS_ONLY", "AMBIGUITY", "NO_USEFUL_RESULT"}
    query_purposes_ok = True
    query_sequence_ok = True
    for direction_id in options_by_id:
        rows = queries_by_direction[direction_id]
        ordered = sorted(rows, key=lambda row: row["query_sequence"])
        query_sequence_ok &= [row["query_sequence"] for row in ordered] == list(range(1, len(rows) + 1))
        query_purposes_ok &= (
            3 <= len(rows) <= 12
            and len({row["query_text"] for row in rows}) >= 3
            and set(REQUIRED_QUERY_PURPOSES).issubset({row["purpose"] for row in rows})
            and all(row["purpose"] in REQUIRED_QUERY_PURPOSES for row in rows)
            and all(row["result_action"] in query_actions and row["issued_at"] for row in rows)
        )
    checks["required_distinct_query_purposes_for_all_options"] = bool(query_purposes_ok)
    checks["query_sequence_is_execution_order_per_direction"] = bool(query_sequence_ok)
    valid_timestamp_basis = {
        "captured_individual_query_timestamp",
        "frozen_capture_retrieved_at; individual query timestamp was not preserved",
    }
    summary = data.get("normalization_summary", {})
    query_timestamps_ok = True
    for query in queries:
        try:
            parsed_issued_at = datetime.fromisoformat(str(query["issued_at"]).replace("Z", "+00:00"))
            timestamp_valid = parsed_issued_at.tzinfo is not None and parsed_issued_at.utcoffset() is not None
        except (TypeError, ValueError):
            timestamp_valid = False
        basis = query.get("issued_at_basis")
        if basis == "frozen_capture_retrieved_at; individual query timestamp was not preserved":
            timestamp_valid = timestamp_valid and query.get("issued_at") == summary.get("capture_retrieved_at")
        query_timestamps_ok &= timestamp_valid and basis in valid_timestamp_basis
    checks["query_issued_timestamps_have_explicit_or_disclosed_fallback_basis"] = bool(query_timestamps_ok)
    checks["capture_normalization_summary_reconciles"] = (
        summary.get("capture_source_document_count")
        == summary.get("canonical_source_document_count", -1) + summary.get("deduplicated_source_document_count", -1)
        and summary.get("canonical_source_document_count") == len(data["source_documents"])
        and sum(len(row.get("source_id_aliases", [])) for row in data["source_documents"])
        == summary.get("capture_source_document_count")
        and summary.get("timestamp_fallback_query_count")
        == sum(query.get("issued_at_basis") not in {"captured_individual_query_timestamp"} for query in queries)
        and summary.get("direction_level_competitor_downgrade_ids") == sorted(
            entity["competitor_id"] for entity in entities if entity.get("normalization_note")
        )
        and summary.get("direction_level_competitor_downgrade_count")
        == len(summary.get("direction_level_competitor_downgrade_ids", []))
    )
    checks["query_lifecycle_checks_are_provenanced"] = all(
        entity["relation_type"] != "DIRECT"
        or (
            bool(entity.get("lifecycle_evidence_ids"))
            and any(
                set(entity["lifecycle_evidence_ids"]).issubset(set(query.get("lifecycle_evidence_ids", [])))
                for query in queries_by_direction[entity["direction_id"]]
            )
        )
        for entity in entities
    )

    evidence_by_direction: dict[str, list[dict]] = defaultdict(list)
    for item in data["external_evidence"]:
        evidence_by_direction[item["direction_id"]].append(item)
    source_domains = _source_domains(data["source_documents"])
    page_limits_ok = True
    coverage_gate_ok = True
    for direction_id, pack in packs_by_id.items():
        option = options_by_id[direction_id]
        pack_evidence = evidence_by_direction[direction_id]
        pack_queries = queries_by_direction[direction_id]
        source_ids = (
            {row["source_id"] for row in pack_evidence}
            | {source_id for query in pack_queries for source_id in query.get("result_source_ids", [])}
        )
        opened_source_ids = {
            source_id for source_id in source_ids
            if source_id in sources_by_id
            and sources_by_id[source_id]["access_status"] in {"OPENED", "OPENED_PARTIAL"}
        }
        if len(opened_source_ids) > 18 or len(pack_queries) > 12:
            page_limits_ok = False
        if pack["research_status"] == "RESOLVED":
            types = {row["claim_type"] for row in pack_evidence}
            direct_entities = [
                entity for entity in entities
                if entity["direction_id"] == direction_id and entity["relation_type"] == "DIRECT"
            ]
            gates = (
                len(pack_evidence) >= 5,
                len({
                    source_domains[row["source_id"]] for row in evidence_by_direction[direction_id]
                }) >= 2,
                REQUIRED_QUERY_PURPOSES[0] in {row["purpose"] for row in pack_queries},
                pack["primary_or_marketplace_evidence_count"] >= 1,
                "SEMANTIC_IDENTITY" in types,
                "FEATURE" in types,
                REQUIRED_QUERY_PURPOSES[1] in {row["purpose"] for row in pack_queries},
                REQUIRED_QUERY_PURPOSES[2] in {row["purpose"] for row in pack_queries},
                all(bool(entity.get("lifecycle_evidence_ids")) for entity in direct_entities),
            )
            expected_status = "SUFFICIENT" if all(gates) else "PARTIAL"
            coverage_gate_ok &= pack["research_coverage_status"] == expected_status
        else:
            coverage_gate_ok &= pack["research_coverage_status"] == pack["research_status"]
        direct_ids = sorted(
            entity["competitor_id"] for entity in entities
            if entity["direction_id"] == direction_id and entity["relation_type"] == "DIRECT"
        )
        if pack["research_status"] != "RESOLVED":
            coverage_gate_ok &= not direct_ids
        coverage_gate_ok &= (
            pack["direct_competitor_ids"] == direct_ids
            and pack["direct_competitor_count"] == len(direct_ids)
            and len(direct_ids) <= 5
        )
        coverage_gate_ok &= not set(pack["direct_competitor_ids"]).intersection(
            entity["competitor_id"] for entity in entities if entity["relation_type"] == "ADJACENT"
        )
    checks["resolved_research_coverage_gates"] = bool(coverage_gate_ok)
    checks["retained_page_and_query_budgets"] = page_limits_ok

    checks["direct_competitors_have_relation_semantic_and_current_lifecycle_evidence"] = all(
        entity["plugin_scope_status"] == "CONFIRMED"
        and bool(entity.get("product_type"))
        and bool(entity.get("platform_or_ecosystem"))
        and bool({"COMPETITOR_RELATION", "SEMANTIC_IDENTITY"} & {
            evidence_by_id[evidence_id]["claim_type"] for evidence_id in entity["evidence_ids"]
            if evidence_id in evidence_by_id
        })
        and bool(entity.get("lifecycle_evidence_ids"))
        and all(
            evidence_id in evidence_by_id
            and evidence_by_id[evidence_id]["claim_type"] == "MAINTENANCE"
            and sources_by_id[evidence_by_id[evidence_id]["source_id"]]["access_status"] in {"OPENED", "OPENED_PARTIAL"}
            and sources_by_id[evidence_by_id[evidence_id]["source_id"]]["source_type"] in {
                "PRIMARY_PRODUCT", "PRIMARY_DOCS", "PRIMARY_REPOSITORY", "MARKETPLACE_LISTING", "PRIMARY_SUPPORT",
            }
            for evidence_id in entity["lifecycle_evidence_ids"]
        )
        and bool(entity.get("maintenance_status"))
        for entity in entities if entity["relation_type"] == "DIRECT"
    )
    checks["direct_entities_only_for_resolved_options"] = all(
        entity["relation_type"] != "DIRECT"
        or packs_by_id[entity["direction_id"]]["research_status"] == "RESOLVED"
        for entity in entities
    )
    checks["competitor_relation_and_lifecycle_evidence_match_direction_category"] = all(
        evidence_id in evidence_by_id
        and evidence_by_id[evidence_id]["direction_id"] == entity["direction_id"]
        and evidence_by_id[evidence_id]["category_id"] == entity["category_id"]
        for entity in entities
        for evidence_id in entity["evidence_ids"] + entity.get("lifecycle_evidence_ids", [])
    )
    checks["competitor_relation_types_are_explicit"] = all(
        entity["relation_type"] in {"DIRECT", "SUBSTITUTE", "ADJACENT"} for entity in entities
    )
    checks["non_plugin_entities_are_adjacent_context_only"] = all(
        entity["plugin_scope_status"] != "NOT_PLUGIN"
        or (
            entity["relation_type"] == "ADJACENT"
            and all(
                evidence_id in evidence_by_id
                and evidence_by_id[evidence_id]["claim_type"] == "ADJACENT_CONTEXT"
                for evidence_id in entity["evidence_ids"]
            )
        )
        for entity in entities
    )
    checks["all_competitors_have_identity_basis"] = all(
        entity["plugin_scope_status"] == "NOT_PLUGIN"
        or any(
            evidence_id in evidence_by_id
            and evidence_by_id[evidence_id]["claim_type"] == "SEMANTIC_IDENTITY"
            for evidence_id in entity["evidence_ids"]
        )
        for entity in entities
    )
    checks["retained_competitor_relations_link_to_current_entity"] = all(
        any(
            entity["direction_id"] == item["direction_id"]
            and entity["category_id"] == item["category_id"]
            and item["evidence_id"] in entity["evidence_ids"]
            for entity in entities
        )
        for item in data["external_evidence"] if item["claim_type"] == "COMPETITOR_RELATION"
    )
    def entity_pricing_is_evidence_backed(entity: dict) -> bool:
        pricing = [
            evidence_by_id[evidence_id] for evidence_id in entity["evidence_ids"]
            if evidence_id in evidence_by_id and evidence_by_id[evidence_id]["claim_type"] == "PRICING"
        ]
        has_fields = any(entity.get(field) is not None for field in ("pricing_model", "price_amount", "currency"))
        return (
            (not has_fields or bool(pricing))
            and (entity.get("price_amount") is None or entity.get("currency") is not None)
            and (
                entity.get("currency") is None
                or any(row.get("currency") == entity["currency"] for row in pricing)
            )
        )
    checks["pricing_fields_require_pricing_evidence_and_currency"] = all(
        entity_pricing_is_evidence_backed(entity) for entity in entities
    )
    checks["pricing_summaries_keep_currencies_separate"] = all(
        set(pack["pricing_observations_by_currency"]) == {
            row.get("currency") or "UNSPECIFIED"
            for row in evidence_by_direction[pack["direction_id"]]
            if row["claim_type"] == "PRICING"
        }
        for pack in packs_by_id.values()
    )
    checks["pack_factual_fields_are_traceable_to_option_evidence"] = all(
        set(pack["evidence_ids"]) == {
            row["evidence_id"] for row in evidence_by_direction[direction_id]
        }
        and set(pack["source_ids"]).issuperset(row["source_id"] for row in evidence_by_direction[direction_id])
        and pack["feature_themes"] == sorted({
            row["feature_theme"] for row in evidence_by_direction[direction_id]
            if row["claim_type"] == "FEATURE" and row.get("feature_theme")
        })
        and pack["operator_pain_observations"] == [
            row for row in evidence_by_direction[direction_id] if row["claim_type"] == "OPERATOR_PAIN"
        ]
        and pack["popularity_proxy_summary"] == [
            row for row in evidence_by_direction[direction_id] if row["claim_type"] == "POPULARITY_PROXY"
        ]
        and (not pack.get("resolved_market_job") or any(
            row["claim_type"] == "SEMANTIC_IDENTITY" for row in evidence_by_direction[direction_id]
        ))
        and (not pack.get("market_job_summary") or any(
            row["claim_type"] == "SEMANTIC_IDENTITY" for row in evidence_by_direction[direction_id]
        ))
        and (not pack.get("maintenance_summary") or any(
            row["claim_type"] == "MAINTENANCE" for row in evidence_by_direction[direction_id]
        ))
        for direction_id, pack in packs_by_id.items()
    )
    pack_rollups_match = True
    relations_by_direction: dict[str, set[str]] = defaultdict(set)
    for relation in data["direction_semantic_relations"]:
        relations_by_direction[relation["direction_id"]].add(relation["relation_id"])
        relations_by_direction[relation["related_direction_id"]].add(relation["relation_id"])
    for direction_id, pack in packs_by_id.items():
        pack_evidence = evidence_by_direction[direction_id]
        pack_entities = [entity for entity in entities if entity["direction_id"] == direction_id]
        pack_queries = queries_by_direction[direction_id]
        pack_sources = {
            row["source_id"] for row in pack_evidence
        } | {
            source_id for query in pack_queries for source_id in query["result_source_ids"]
        }
        pack_rollups_match &= (
            pack["source_ids"] == sorted(pack_sources)
            and pack["evidence_count"] == len(pack_evidence)
            and pack["query_count"] == len(pack_queries)
            and pack["direct_competitor_ids"] == sorted(
                entity["competitor_id"] for entity in pack_entities if entity["relation_type"] == "DIRECT"
            )
            and pack["substitute_competitor_ids"] == sorted(
                entity["competitor_id"] for entity in pack_entities if entity["relation_type"] == "SUBSTITUTE"
            )
            and pack["semantic_relation_ids"] == sorted(relations_by_direction[direction_id])
            and pack["pricing_observations_by_currency"] == _pricing_summary(pack_evidence)
            and pack["primary_or_marketplace_evidence_count"] == sum(
                sources_by_id[row["source_id"]]["source_type"] in {
                    "PRIMARY_PRODUCT", "PRIMARY_DOCS", "PRIMARY_REPOSITORY", "MARKETPLACE_LISTING", "PRIMARY_SUPPORT",
                }
                for row in pack_evidence
            )
            and pack["community_evidence_count"] == sum(
                sources_by_id[row["source_id"]]["source_type"] == "COMMUNITY" for row in pack_evidence
            )
            and pack["distinct_domain_count"] == len({
                sources_by_id[row["source_id"]]["source_domain"] for row in pack_evidence
            })
        )
    checks["pack_rollups_reconcile_to_source_rows"] = bool(pack_rollups_match)
    checks["popularity_metrics_keep_source_native_units"] = all(
        row.get("numeric_unit") is not None and "unit" not in row
        for row in data["external_evidence"] if row["claim_type"] == "POPULARITY_PROXY"
    )

    checks["semantic_relation_enum_category_and_evidence_are_valid"] = all(
        relation["relation_type"] in ALLOWED_RELATION_TYPES
        and relation["direction_id"] in options_by_id
        and relation["related_direction_id"] in options_by_id
        and relation["direction_id"] != relation["related_direction_id"]
        and relation["category_id"] == options_by_id[relation["direction_id"]]["category_id"]
        == options_by_id[relation["related_direction_id"]]["category_id"]
        and bool(relation["evidence_ids"])
        and all(
            evidence_id in evidence_by_id
            and evidence_by_id[evidence_id]["category_id"] == relation["category_id"]
            and evidence_by_id[evidence_id]["direction_id"] in {relation["direction_id"], relation["related_direction_id"]}
            for evidence_id in relation["evidence_ids"]
        )
        for relation in data["direction_semantic_relations"]
    )
    hypotheses_valid = True
    for pack in packs_by_id.values():
        for hypothesis in pack["differentiation_hypotheses"]:
            refs = [evidence_by_id.get(evidence_id) for evidence_id in hypothesis.get("evidence_ids", [])]
            refs = [row for row in refs if row is not None]
            semantic_only = hypothesis.get("purely_semantic_or_overlap") is True and all(
                row["claim_type"] in {"SEMANTIC_IDENTITY", "OVERLAP_SIGNAL"} for row in refs
            )
            hypotheses_valid &= (
                hypothesis.get("label") == "HYPOTHESIS"
                and bool(hypothesis.get("hypothesis"))
                and bool(hypothesis.get("observed_evidence"))
                and len(refs) >= 2
                and all(row["direction_id"] == pack["direction_id"] for row in refs)
                and len({sources_by_id[row["source_id"]]["canonical_url"] for row in refs}) >= 2
                and (semantic_only or any(row["claim_type"] in {"FEATURE", "OPERATOR_PAIN"} for row in refs))
            )
    checks["differentiation_hypotheses_are_evidence_backed"] = hypotheses_valid
    coverage = data.get("category_research_coverage", [])
    checks["all_11_taxonomy_category_coverage_rows"] = (
        len(coverage) == 11
        and tuple(row["category_id"] for row in sorted(coverage, key=lambda row: row["category_order"])) == EXPECTED_CATEGORY_ORDER
    )
    coverage_by_id = {row["category_id"]: row for row in coverage}
    checks["category_coverage_has_required_summaries"] = all(
        {
            "option_ids", "research_status_counts", "coverage_status_counts",
            "distinct_competitor_count", "evidence_count", "source_document_count",
            "semantic_relation_ids", "coverage_notes", "risk_notes",
        }.issubset(row)
        for row in coverage
    )
    checks["zero_option_category_statuses_match_spec"] = all(
        coverage_by_id.get(category_id, {}).get("category_research_status") == status
        for category_id, status in ZERO_OPTION_CATEGORY_STATUS.items()
    )
    checks["nonzero_category_statuses_match_spec"] = all(
        row["category_research_status"] == "RESEARCHED_OPTIONS"
        for row in coverage if row["option_count"] > 0
    )
    checks["category_counts_reconcile_to_16"] = sum(row["option_count"] for row in coverage) == 16
    expected_researched_categories = {
        category_id for category_id, count in EXPECTED_OPTION_COUNTS.items() if count > 0
    }
    checks["exact_researched_and_zero_option_category_statuses"] = (
        {row["category_id"] for row in coverage if row["category_research_status"] == "RESEARCHED_OPTIONS"}
        == expected_researched_categories
        and all(
            coverage_by_id.get(category_id, {}).get("category_research_status") == status
            for category_id, status in ZERO_OPTION_CATEGORY_STATUS.items()
        )
    )
    coverage_reconciles = True
    packs_by_category: dict[str, list[dict]] = defaultdict(list)
    evidence_by_category: dict[str, list[dict]] = defaultdict(list)
    queries_by_category: dict[str, list[dict]] = defaultdict(list)
    entities_by_category: dict[str, list[dict]] = defaultdict(list)
    for pack in packs_by_id.values():
        packs_by_category[pack["category_id"]].append(pack)
    for row in data["external_evidence"]:
        evidence_by_category[row["category_id"]].append(row)
    for row in queries:
        queries_by_category[row["category_id"]].append(row)
    for row in entities:
        entities_by_category[row["category_id"]].append(row)
    for relation in data["direction_semantic_relations"]:
        endpoints = {relation["direction_id"], relation["related_direction_id"]}
        coverage_reconciles &= all(
            relation["relation_id"] in packs_by_id[direction_id]["semantic_relation_ids"]
            for direction_id in endpoints if direction_id in packs_by_id
        )
    for category_id, row in coverage_by_id.items():
        category_packs = packs_by_category[category_id]
        category_entities = entities_by_category[category_id]
        category_evidence = evidence_by_category[category_id]
        category_queries = queries_by_category[category_id]
        expected_option_ids = sorted(pack["direction_id"] for pack in category_packs)
        expected_research_counts = dict(sorted(Counter(pack["research_status"] for pack in category_packs).items()))
        expected_coverage_counts = dict(sorted(Counter(pack["research_coverage_status"] for pack in category_packs).items()))
        expected_relations = sorted({
            relation_id for pack in category_packs for relation_id in pack["semantic_relation_ids"]
        })
        expected_source_ids = sorted(
            {item["source_id"] for item in category_evidence}
            | {source_id for query in category_queries for source_id in query["result_source_ids"]}
        )
        expected_direct_ids = sorted(entity["competitor_id"] for entity in category_entities if entity["relation_type"] == "DIRECT")
        coverage_reconciles &= (
            row["option_ids"] == expected_option_ids
            and row["research_status_counts"] == expected_research_counts
            and row["coverage_status_counts"] == expected_coverage_counts
            and row["semantic_relation_ids"] == expected_relations
            and row["competitor_ids"] == sorted(entity["competitor_id"] for entity in category_entities)
            and row["direct_competitor_ids"] == expected_direct_ids
            and row["direct_competitor_count"] == len(expected_direct_ids)
            and row["distinct_competitor_count"] == len({entity["canonical_url"] for entity in category_entities})
            and row["distinct_direct_competitor_count"] == len({
                entity["canonical_url"] for entity in category_entities if entity["relation_type"] == "DIRECT"
            })
            and row["evidence_count"] == len(category_evidence)
            and row["source_document_count"] == len(set(expected_source_ids))
            and row["query_count"] == len(category_queries)
            and row["option_count"] == len(category_packs)
        )
    checks["category_coverage_summaries_reconcile_to_normalized_rows"] = bool(coverage_reconciles)

    all_rows = [
        *data["source_documents"], *queries, *data["external_evidence"], *entities,
        *data["direction_semantic_relations"],
        *[
            {key: value for key, value in pack.items() if key not in IMMUTABLE_OPTION_FIELDS}
            for pack in data["direction_research_packs"]
        ],
        *coverage,
    ]
    forbidden_key_tokens = ("score", "rank", "winner", "recommendation", "shortlist")
    def has_forbidden_key(value: object) -> bool:
        if isinstance(value, dict):
            return any(
                any(token in str(key).casefold() for token in forbidden_key_tokens)
                or has_forbidden_key(item)
                for key, item in value.items()
            )
        if isinstance(value, list):
            return any(has_forbidden_key(item) for item in value)
        return False
    checks["no_rankings_scores_recommendations_or_stage_f_fields"] = not has_forbidden_key(all_rows)

    failed = [name for name, value in checks.items() if not value]
    return {
        "schema_version": SCHEMA_VERSION,
        "input_sha256_before": input_sha,
        "input_sha256_after": input_sha_after,
        "direction_ids_sha256": DIRECTION_IDS_SHA256,
        "option_count": len(snapshot_options),
        "category_count": len(categories),
        "source_document_count": len(data["source_documents"]),
        "research_query_count": len(queries),
        "evidence_count": len(data["external_evidence"]),
        "competitor_entity_count": len(entities),
        "relation_count": len(data["direction_semantic_relations"]),
        "research_status_counts": dict(sorted(Counter(pack["research_status"] for pack in packs_by_id.values()).items())),
        "coverage_status_counts": dict(sorted(Counter(pack["research_coverage_status"] for pack in packs_by_id.values()).items())),
        "input_checks": input_checks or {},
        "capture_sha256": (replay_result or {}).get("capture_sha256"),
        "replay": replay_result or {},
        "execution_code_commit": None,
        "checks": checks,
        "errors": errors,
        "failed_checks": failed,
        "normalization_summary": summary,
        "overall_status": "PASS" if not failed else "FAIL",
    }


def _build_coverage(
    categories: list[dict], packs: list[dict], entities: list[dict] | None = None,
    relations: list[dict] | None = None, evidence: list[dict] | None = None,
    queries: list[dict] | None = None,
) -> list[dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for pack in packs:
        grouped[pack["category_id"]].append(pack)
    entities_by_category: dict[str, list[dict]] = defaultdict(list)
    evidence_by_category: dict[str, list[dict]] = defaultdict(list)
    sources_by_category: dict[str, set[str]] = defaultdict(set)
    queries_by_category: dict[str, list[dict]] = defaultdict(list)
    for entity in entities or []:
        entities_by_category[entity["category_id"]].append(entity)
    for item in evidence or []:
        evidence_by_category[item["category_id"]].append(item)
        sources_by_category[item["category_id"]].add(item["source_id"])
    for query in queries or []:
        queries_by_category[query["category_id"]].append(query)
        sources_by_category[query["category_id"]].update(query.get("result_source_ids", []))
    rows = []
    for category in sorted(categories, key=lambda row: row.get("category_order", 0)):
        category_id = category["category_id"]
        items = grouped.get(category_id, [])
        if items:
            statuses = Counter(pack["research_status"] for pack in items)
            coverage_statuses = Counter(pack["research_coverage_status"] for pack in items)
            status = "RESEARCHED_OPTIONS"
        else:
            statuses = Counter()
            coverage_statuses = Counter()
            status = ZERO_OPTION_CATEGORY_STATUS.get(category_id, "NO_STAGE_D_RESEARCH_OPTIONS")
        option_ids = sorted(pack["direction_id"] for pack in items)
        evidence_ids = sorted(row["evidence_id"] for row in evidence_by_category[category_id])
        source_ids = sorted(sources_by_category[category_id])
        competitor_rows = sorted(entities_by_category[category_id], key=lambda row: row["competitor_id"])
        relation_ids = sorted({
            relation_id for pack in items for relation_id in pack.get("semantic_relation_ids", [])
        })
        risk_flags = sorted({
            flag for pack in items for flag in pack.get("risk_flags", [])
        })
        if items:
            coverage_notes = (
                f"Research packs cover {len(items)} accepted option(s); coverage statuses: "
                + ", ".join(f"{key}={value}" for key, value in sorted(coverage_statuses.items()))
                + "."
            )
        else:
            coverage_notes = "No Stage E research options in the frozen YEE-76 category universe."
        rows.append({
            "category_id": category_id,
            "category_order": category.get("category_order", 0),
            "category_name": category.get("category_name"),
            "option_count": len(items),
            "option_ids": option_ids,
            "research_status_counts": dict(sorted(statuses.items())),
            "coverage_status_counts": dict(sorted(coverage_statuses.items())),
            "category_research_status": status,
            "distinct_competitor_count": len({row["canonical_url"] for row in competitor_rows}),
            "distinct_direct_competitor_count": len({
                row["canonical_url"] for row in competitor_rows if row["relation_type"] == "DIRECT"
            }),
            "direct_competitor_count": sum(row["relation_type"] == "DIRECT" for row in competitor_rows),
            "competitor_ids": [row["competitor_id"] for row in competitor_rows],
            "direct_competitor_ids": sorted(row["competitor_id"] for row in competitor_rows if row["relation_type"] == "DIRECT"),
            "evidence_count": len(evidence_by_category[category_id]),
            "source_document_count": len(source_ids),
            "query_count": len(queries_by_category[category_id]),
            "source_ids": source_ids,
            "semantic_relation_ids": relation_ids,
            "coverage_notes": coverage_notes,
            "risk_notes": risk_flags,
        })
    return rows


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(canonical_json(row) + "\n" for row in rows), encoding="utf-8", newline="\n")


def _write_csv(path: Path, rows: list[dict]) -> None:
    keys = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=keys, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: "\\N" if row.get(key) is None
                else canonical_json(row[key]) if isinstance(row[key], (dict, list)) else row[key]
                for key in keys
            })


def _sqlite_reference_rows(data: dict) -> dict[str, list[tuple[str, str]]]:
    specs = (
        ("query_result_source_refs", "research_queries", "query_id", "result_source_ids"),
        ("query_result_evidence_refs", "research_queries", "query_id", "result_evidence_ids"),
        ("query_lifecycle_evidence_refs", "research_queries", "query_id", "lifecycle_evidence_ids"),
        ("competitor_evidence_refs", "competitor_entities", "competitor_id", "evidence_ids"),
        ("competitor_lifecycle_evidence_refs", "competitor_entities", "competitor_id", "lifecycle_evidence_ids"),
        ("semantic_relation_evidence_refs", "direction_semantic_relations", "relation_id", "evidence_ids"),
        ("research_pack_evidence_refs", "direction_research_packs", "direction_id", "evidence_ids"),
        ("research_pack_source_refs", "direction_research_packs", "direction_id", "source_ids"),
        ("research_pack_relation_refs", "direction_research_packs", "direction_id", "semantic_relation_ids"),
        ("category_coverage_option_refs", "category_research_coverage", "category_id", "option_ids"),
        ("category_coverage_source_refs", "category_research_coverage", "category_id", "source_ids"),
        ("category_coverage_competitor_refs", "category_research_coverage", "category_id", "competitor_ids"),
        ("category_coverage_relation_refs", "category_research_coverage", "category_id", "semantic_relation_ids"),
    )
    result = {}
    for table, rows_name, key, refs in specs:
        result[table] = sorted({
            (row[key], reference)
            for row in data[rows_name]
            for reference in row.get(refs, [])
        })
    return result


def _write_sqlite(
    path: Path, input_sha: str, execution_code_commit: str,
    categories: list[dict], options: list[dict], data: dict,
) -> None:
    if path.exists():
        path.unlink()
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.executescript("""
            CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
            CREATE TABLE input_provenance(key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
            CREATE TABLE frozen_category_snapshot(category_id TEXT PRIMARY KEY, category_order INTEGER NOT NULL, record_json TEXT NOT NULL) WITHOUT ROWID;
            CREATE TABLE frozen_option_snapshot(direction_id TEXT PRIMARY KEY, category_id TEXT NOT NULL, record_json TEXT NOT NULL,
                FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id)) WITHOUT ROWID;
            CREATE TABLE source_documents(source_id TEXT PRIMARY KEY, source_domain TEXT NOT NULL, canonical_url TEXT NOT NULL, record_json TEXT NOT NULL) WITHOUT ROWID;
            CREATE TABLE research_queries(query_id TEXT PRIMARY KEY, direction_id TEXT NOT NULL, category_id TEXT NOT NULL, record_json TEXT NOT NULL,
                FOREIGN KEY(direction_id) REFERENCES frozen_option_snapshot(direction_id), FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id)) WITHOUT ROWID;
            CREATE TABLE external_evidence(evidence_id TEXT PRIMARY KEY, direction_id TEXT NOT NULL, category_id TEXT NOT NULL, source_id TEXT NOT NULL, claim_type TEXT NOT NULL, record_json TEXT NOT NULL,
                FOREIGN KEY(direction_id) REFERENCES frozen_option_snapshot(direction_id), FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id), FOREIGN KEY(source_id) REFERENCES source_documents(source_id)) WITHOUT ROWID;
            CREATE TABLE competitor_entities(competitor_id TEXT PRIMARY KEY, direction_id TEXT NOT NULL, category_id TEXT NOT NULL, record_json TEXT NOT NULL,
                FOREIGN KEY(direction_id) REFERENCES frozen_option_snapshot(direction_id), FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id)) WITHOUT ROWID;
            CREATE TABLE direction_semantic_relations(relation_id TEXT PRIMARY KEY, direction_id TEXT NOT NULL, related_direction_id TEXT NOT NULL, category_id TEXT NOT NULL, record_json TEXT NOT NULL,
                FOREIGN KEY(direction_id) REFERENCES frozen_option_snapshot(direction_id), FOREIGN KEY(related_direction_id) REFERENCES frozen_option_snapshot(direction_id), FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id));
            CREATE TABLE direction_research_packs(direction_id TEXT PRIMARY KEY, category_id TEXT NOT NULL, research_status TEXT NOT NULL, research_coverage_status TEXT NOT NULL, record_json TEXT NOT NULL,
                FOREIGN KEY(direction_id) REFERENCES frozen_option_snapshot(direction_id), FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id)) WITHOUT ROWID;
            CREATE TABLE category_research_coverage(category_id TEXT PRIMARY KEY, option_count INTEGER NOT NULL, category_research_status TEXT NOT NULL, record_json TEXT NOT NULL,
                FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id)) WITHOUT ROWID;
            CREATE TABLE query_result_source_refs(query_id TEXT NOT NULL, source_id TEXT NOT NULL, PRIMARY KEY(query_id,source_id), FOREIGN KEY(query_id) REFERENCES research_queries(query_id), FOREIGN KEY(source_id) REFERENCES source_documents(source_id)) WITHOUT ROWID;
            CREATE TABLE query_result_evidence_refs(query_id TEXT NOT NULL, evidence_id TEXT NOT NULL, PRIMARY KEY(query_id,evidence_id), FOREIGN KEY(query_id) REFERENCES research_queries(query_id), FOREIGN KEY(evidence_id) REFERENCES external_evidence(evidence_id)) WITHOUT ROWID;
            CREATE TABLE query_lifecycle_evidence_refs(query_id TEXT NOT NULL, evidence_id TEXT NOT NULL, PRIMARY KEY(query_id,evidence_id), FOREIGN KEY(query_id) REFERENCES research_queries(query_id), FOREIGN KEY(evidence_id) REFERENCES external_evidence(evidence_id)) WITHOUT ROWID;
            CREATE TABLE competitor_evidence_refs(competitor_id TEXT NOT NULL, evidence_id TEXT NOT NULL, PRIMARY KEY(competitor_id,evidence_id), FOREIGN KEY(competitor_id) REFERENCES competitor_entities(competitor_id), FOREIGN KEY(evidence_id) REFERENCES external_evidence(evidence_id)) WITHOUT ROWID;
            CREATE TABLE competitor_lifecycle_evidence_refs(competitor_id TEXT NOT NULL, evidence_id TEXT NOT NULL, PRIMARY KEY(competitor_id,evidence_id), FOREIGN KEY(competitor_id) REFERENCES competitor_entities(competitor_id), FOREIGN KEY(evidence_id) REFERENCES external_evidence(evidence_id)) WITHOUT ROWID;
            CREATE TABLE semantic_relation_evidence_refs(relation_id TEXT NOT NULL, evidence_id TEXT NOT NULL, PRIMARY KEY(relation_id,evidence_id), FOREIGN KEY(relation_id) REFERENCES direction_semantic_relations(relation_id), FOREIGN KEY(evidence_id) REFERENCES external_evidence(evidence_id)) WITHOUT ROWID;
            CREATE TABLE research_pack_evidence_refs(direction_id TEXT NOT NULL, evidence_id TEXT NOT NULL, PRIMARY KEY(direction_id,evidence_id), FOREIGN KEY(direction_id) REFERENCES direction_research_packs(direction_id), FOREIGN KEY(evidence_id) REFERENCES external_evidence(evidence_id)) WITHOUT ROWID;
            CREATE TABLE research_pack_source_refs(direction_id TEXT NOT NULL, source_id TEXT NOT NULL, PRIMARY KEY(direction_id,source_id), FOREIGN KEY(direction_id) REFERENCES direction_research_packs(direction_id), FOREIGN KEY(source_id) REFERENCES source_documents(source_id)) WITHOUT ROWID;
            CREATE TABLE research_pack_relation_refs(direction_id TEXT NOT NULL, relation_id TEXT NOT NULL, PRIMARY KEY(direction_id,relation_id), FOREIGN KEY(direction_id) REFERENCES direction_research_packs(direction_id), FOREIGN KEY(relation_id) REFERENCES direction_semantic_relations(relation_id)) WITHOUT ROWID;
            CREATE TABLE category_coverage_option_refs(category_id TEXT NOT NULL, direction_id TEXT NOT NULL, PRIMARY KEY(category_id,direction_id), FOREIGN KEY(category_id) REFERENCES category_research_coverage(category_id), FOREIGN KEY(direction_id) REFERENCES frozen_option_snapshot(direction_id)) WITHOUT ROWID;
            CREATE TABLE category_coverage_source_refs(category_id TEXT NOT NULL, source_id TEXT NOT NULL, PRIMARY KEY(category_id,source_id), FOREIGN KEY(category_id) REFERENCES category_research_coverage(category_id), FOREIGN KEY(source_id) REFERENCES source_documents(source_id)) WITHOUT ROWID;
            CREATE TABLE category_coverage_competitor_refs(category_id TEXT NOT NULL, competitor_id TEXT NOT NULL, PRIMARY KEY(category_id,competitor_id), FOREIGN KEY(category_id) REFERENCES category_research_coverage(category_id), FOREIGN KEY(competitor_id) REFERENCES competitor_entities(competitor_id)) WITHOUT ROWID;
            CREATE TABLE category_coverage_relation_refs(category_id TEXT NOT NULL, relation_id TEXT NOT NULL, PRIMARY KEY(category_id,relation_id), FOREIGN KEY(category_id) REFERENCES category_research_coverage(category_id), FOREIGN KEY(relation_id) REFERENCES direction_semantic_relations(relation_id)) WITHOUT ROWID;
        """)
        meta = {
            "schema_version": SCHEMA_VERSION,
            "input_work_order": "YEE-76",
            "input_sha256_before": input_sha,
            "input_sha256_after": data.get("input_sha256_after", input_sha),
            "input_run_id": INPUT_RUN_ID,
            "input_schema_version": INPUT_SCHEMA_VERSION,
            "input_code_commit": INPUT_CODE_COMMIT,
            "input_merge_commit": INPUT_MERGE_COMMIT,
            "input_taxonomy_version": INPUT_TAXONOMY_VERSION,
            "input_taxonomy_sha256": INPUT_TAXONOMY_SHA256,
            "input_read_mode": "read-only",
            "input_category_profile_count": "11",
            "input_direction_tier_count": "47",
            "input_option_count": "16",
            "execution_code_commit": execution_code_commit,
            "direction_ids_sha256": DIRECTION_IDS_SHA256,
            "research_capture_sha256": CAPTURE_SHA256,
            "capture_source_document_count": str(data["normalization_summary"]["capture_source_document_count"]),
            "canonical_source_document_count": str(data["normalization_summary"]["canonical_source_document_count"]),
            "timestamp_fallback_query_count": str(data["normalization_summary"]["timestamp_fallback_query_count"]),
            "direction_level_competitor_downgrade_count": str(data["normalization_summary"]["direction_level_competitor_downgrade_count"]),
            "research_retrieved_at": "2026-09-27T14:05:19Z",
        }
        connection.executemany("INSERT INTO metadata VALUES (?,?)", sorted(meta.items()))
        connection.executemany("INSERT INTO input_provenance VALUES (?,?)", sorted(data.get("input_provenance", {}).items()))
        connection.executemany("INSERT INTO frozen_category_snapshot VALUES (?,?,?)", [
            (row["category_id"], row.get("category_order", 0), canonical_json(row)) for row in categories
        ])
        connection.executemany("INSERT INTO frozen_option_snapshot VALUES (?,?,?)", [
            (row["direction_id"], row["category_id"], canonical_json(row)) for row in options
        ])
        connection.executemany("INSERT INTO source_documents VALUES (?,?,?,?)", [
            (row["source_id"], row["source_domain"], row["canonical_url"], canonical_json(row)) for row in data["source_documents"]
        ])
        for table, key in (("research_queries", "query_id"), ("external_evidence", "evidence_id"),
                           ("competitor_entities", "competitor_id"), ("direction_semantic_relations", "relation_id"),
                           ("direction_research_packs", "direction_id"), ("category_research_coverage", "category_id")):
            for row in data[table]:
                if table == "research_queries":
                    values = (row[key], row["direction_id"], row["category_id"], canonical_json(row))
                    connection.execute("INSERT INTO research_queries VALUES (?,?,?,?)", values)
                elif table == "external_evidence":
                    values = (row[key], row["direction_id"], row["category_id"], row["source_id"], row["claim_type"], canonical_json(row))
                    connection.execute("INSERT INTO external_evidence VALUES (?,?,?,?,?,?)", values)
                elif table == "competitor_entities":
                    values = (row[key], row["direction_id"], row["category_id"], canonical_json(row))
                    connection.execute("INSERT INTO competitor_entities VALUES (?,?,?,?)", values)
                elif table == "direction_semantic_relations":
                    values = (row[key], row["direction_id"], row["related_direction_id"], row["category_id"], canonical_json(row))
                    connection.execute("INSERT INTO direction_semantic_relations VALUES (?,?,?,?,?)", values)
                elif table == "direction_research_packs":
                    values = (row[key], row["category_id"], row["research_status"], row["research_coverage_status"], canonical_json(row))
                    connection.execute("INSERT INTO direction_research_packs VALUES (?,?,?,?,?)", values)
                else:
                    values = (row[key], row["option_count"], row["category_research_status"], canonical_json(row))
                    connection.execute("INSERT INTO category_research_coverage VALUES (?,?,?,?)", values)
        for table, rows in _sqlite_reference_rows(data).items():
            connection.executemany(f"INSERT INTO {table} VALUES (?,?)", rows)
        connection.commit()
        connection.execute("PRAGMA optimize")
    finally:
        connection.close()


def _write_schema(path: Path) -> None:
    path.write_text(
        "# YEE-77 External Research Schema\n\n"
        f"Schema version: `{SCHEMA_VERSION}`. Research is keyed to the exact accepted YEE-76 direction IDs and category IDs.\n\n"
        "## Tables and keys\n\n"
        "- `source_documents`: `source_id`, `canonical_url`, `source_url`, `source_domain`, `source_title`, `source_type`, `retrieved_at`, `published_or_updated_at`, `access_status`, `notes`. One row per canonical URL.\n"
        "- `research_queries`: `query_id`, `direction_id`, `category_id`, `query_sequence`, `query_text`, `issued_at`, `issued_at_basis`, `purpose`, `result_action`; result-source/evidence and lifecycle-evidence references are additive provenance.\n"
        "- `external_evidence`: `evidence_id`, `direction_id`, `category_id`, `source_id`, `claim_type`, `observation`, `optional_excerpt`, `numeric_value`, `numeric_unit`, `currency`, `entity_name`, `retrieved_at`, `notes`.\n"
        "- `competitor_entities`: `competitor_id`, `direction_id`, `category_id`, `entity_name`, `canonical_url`, `relation_type`, `product_type`, `platform_or_ecosystem`, `plugin_scope_status`, `pricing_model`, `price_amount`, `currency`, `maintenance_status`, `feature_summary`, `evidence_ids`. Lifecycle references and normalization notes are additive.\n"
        "- `direction_semantic_relations`: evidence-backed category-local relation using only `EXTERNALLY_EQUIVALENT`, `SUBSTANTIAL_OVERLAP`, `DISTINCT_RELATED`, or `CONFLICTING_OR_UNRESOLVED`. A relation never changes canonical identities.\n"
        "- `direction_research_packs`: exactly one row per accepted option; all frozen Stage D fields are copied unchanged, then research fields are added. `differentiation_hypotheses` contains only explicit `HYPOTHESIS` records satisfying the two-canonical-URL evidence rule; an empty list is valid.\n"
        "- `category_research_coverage`: exactly 11 rows in frozen taxonomy order. Option-bearing rows use `category_research_status=RESEARCHED_OPTIONS`; empty categories retain their specified Stage D statuses. Rows include option/status counts, competitor/evidence/source counts, relation refs, and coverage/risk notes.\n"
        "- SQLite also stores immutable `metadata` and copied `input_provenance` alongside frozen snapshots and the normalized tables. Query/source, query/evidence, competitor/evidence, relation/evidence, research-pack, and category-coverage references are represented with foreign-key join tables.\n\n"
        "- `frozen_category_snapshot`: accepted category ID/order and original category JSON.\n"
        "- `frozen_option_snapshot`: accepted direction ID/category and original option JSON; immutable input snapshot.\n"
        "## Interpretation and serialization\n\n"
        "YEE-76 direction/category identity and opportunity fields are copied unchanged. Research statuses are `RESOLVED`, `AMBIGUOUS`, or `UNRESOLVED`; coverage does not upgrade ambiguous status. Missing price is null/unknown, never free. Currency groups and popularity metrics remain source-native; incomparable values are not combined. Search snippets are discovery-only and cannot support retained facts. Differentiation hypotheses are interpretive and may be empty.\n\n"
        "JSONL uses canonical UTF-8 JSON and JSON null. CSV uses UTF-8 without BOM, LF line endings, the literal `\\N` for null, and canonical compact JSON for nested values. SQLite must pass `PRAGMA integrity_check` and `PRAGMA foreign_key_check`. Frozen YEE-76 fields and the accepted 16 identities are not mutated, merged, ranked, or scored.\n",
        encoding="utf-8",
        newline="\n",
    )


def _write_reports(
    output: Path, input_sha: str, execution_code_commit: str,
    options: list[dict], data: dict, qa: dict,
) -> None:
    resolved = sum(pack["research_status"] == "RESOLVED" for pack in data["direction_research_packs"])
    ambiguous = sum(pack["research_status"] == "AMBIGUOUS" for pack in data["direction_research_packs"])
    unresolved = sum(pack["research_status"] == "UNRESOLVED" for pack in data["direction_research_packs"])
    goal = (
        "# Goal alignment — YEE-77\n\n"
        "- Product Objective is unchanged: identify a commercially viable paid Minecraft server-side/proxy plugin.\n"
        "- PLUGIN_ONLY remains binding; direct competitors require positive source-backed server/proxy plugin identity.\n"
        "- The sole analytical input is the accepted YEE-76 `category_opportunity_map.sqlite`, opened read-only and SHA-256 pinned before/after processing.\n"
        "- The Stage E universe is exactly the 16 frozen YEE-76 LEAD/WATCH options; all 11 categories remain represented in coverage.\n"
        "- No YEE-30 through YEE-59 dataset, evidence, shortlist, rank, or conclusion is used.\n"
        "- YEE-76 category/direction identities, states, tiers, support shape, risk flags, reason codes, and evidence-pack provenance are copied unchanged.\n"
        "- No option is ranked, scored, selected as a winner, shortlisted, merged, or recommended; no concept or commercial decision is generated.\n"
        "- Zero-option categories are recorded with their frozen non-research status; no external research is performed for them.\n"
        "- This bundle stops before Stage F.\n"
    )
    protocol = (
        "# External research protocol — YEE-77\n\n"
        "This artifact is a deterministic normalization of the frozen `YEE77_RESEARCH_CAPTURE.json`; rebuilding makes no live web requests. For each accepted option, preserve the issued query execution order and the semantic-resolution, competitor-discovery, and operator pain-point discovery attempts. Per-option limits are 12 queries and 18 retained opened pages; the capture has no individual query timestamps, so `issued_at` uses the frozen capture retrieval time and records that basis. Search results are discovery only. Retained facts cite opened public canonical pages. Each retained direct entity has a product-identity and current lifecycle check. Pricing is source-native and absent pricing is null. Pain statements are individual reports, not prevalence. Ambiguous broad options remain ambiguous; no option is deleted or merged.\n"
    )
    (output / "GOAL_ALIGNMENT.md").write_text(goal, encoding="utf-8", newline="\n")
    (output / "EXTERNAL_RESEARCH_PROTOCOL.md").write_text(protocol, encoding="utf-8", newline="\n")
    evidence_by_direction: dict[str, list[dict]] = defaultdict(list)
    entities_by_direction: dict[str, list[dict]] = defaultdict(list)
    sources_by_id = {row["source_id"]: row for row in data["source_documents"]}
    for row in data["external_evidence"]:
        evidence_by_direction[row["direction_id"]].append(row)
    for row in data["competitor_entities"]:
        entities_by_direction[row["direction_id"]].append(row)
    direction_sections = []
    for pack in data["direction_research_packs"]:
        direction_id = pack["direction_id"]
        evidence = evidence_by_direction[direction_id]
        by_type = Counter(row["claim_type"] for row in evidence)
        entities = entities_by_direction[direction_id]
        entity_text = "; ".join(
            f"{row['entity_name']} ({row['relation_type']}; maintenance: {row.get('maintenance_status') or 'not established'})"
            for row in entities
        ) or "No defensible competitor entity retained"
        themes = ", ".join(pack["feature_themes"]) or "not established"
        job = pack.get("resolved_market_job") or "not resolved; broad/overlapping scope retained as ambiguous"
        section = [
            f"### `{pack['canonical_direction_key']}` — `{direction_id}` ({pack['category_id']})",
            "",
            f"**Status:** {pack['research_status']} / {pack['research_coverage_status']}. **Research interpretation:** {job}",
            "",
            f"**Feature themes:** {themes}. **Competitor/adjacent landscape:** {entity_text}.",
            "",
            f"**Evidence inventory:** {len(evidence)} retained observations; "
            + ", ".join(f"{name}={count}" for name, count in sorted(by_type.items()))
            + f". **Queries:** {pack['query_count']}; **opened domains:** {pack['distinct_domain_count']}; **notes:** {pack['research_notes'] or 'none'}",
        ]
        pain = [row for row in evidence if row["claim_type"] == "OPERATOR_PAIN"]
        if pain:
            section.extend(["", "**Operator pain observations (anecdotal, not prevalence):**"])
            section.extend(
                f"- {row['observation']} ([{sources_by_id[row['source_id']]['source_title']}]({sources_by_id[row['source_id']]['canonical_url']}))."
                for row in pain
            )
        priced = [row for row in evidence if row["claim_type"] == "PRICING"]
        if priced:
            section.extend(["", "**Source-native pricing observations:**"])
            section.extend(
                f"- {row['observation']} ([{sources_by_id[row['source_id']]['source_title']}]({sources_by_id[row['source_id']]['canonical_url']}).)"
                for row in priced
            )
        direction_sections.append("\n".join(section))
    status_lines = "\n\n".join(direction_sections)
    report = (
        "# YEE-77 Final Report\n\n"
        "## Scope and input\n\n"
        f"Researched all {len(options)} accepted YEE-76 options. Read-only input SHA-256 before/after: `{qa['input_sha256_before']}` / `{qa['input_sha256_after']}`; frozen capture SHA-256: `{qa['replay']['capture_sha256']}`; accepted run `{INPUT_RUN_ID}`; schema `{INPUT_SCHEMA_VERSION}`; taxonomy `{INPUT_TAXONOMY_VERSION}` (`{INPUT_TAXONOMY_SHA256}`); input code commit `{INPUT_CODE_COMMIT}`; YEE-77 execution code commit `{execution_code_commit}`.\n\n"
        "## Results\n\n"
        f"Research statuses: {resolved} RESOLVED, {ambiguous} AMBIGUOUS, {unresolved} UNRESOLVED. All three required query purposes were recorded for every option. The frozen capture contains {data['normalization_summary']['capture_source_document_count']} source records; canonicalization retained {data['normalization_summary']['canonical_source_document_count']} unique source documents and deduplicated {data['normalization_summary']['deduplicated_source_document_count']} duplicate record(s). The capture contains {len(data['research_queries'])} queries, {len(data['external_evidence'])} source-backed observations, {len(data['competitor_entities'])} competitor/adjacent entities, and {len(data['direction_semantic_relations'])} evidence-backed relations. No product ranking, score, winner, recommendation, or merging was performed.\n\n"
        f"All {data['normalization_summary']['timestamp_fallback_query_count']} retained query rows use `issued_at={data['research_queries'][0]['issued_at']}` from the frozen capture `retrieved_at`; individual query issue times were not captured. Each row records `issued_at_basis` so this fallback is machine-visible and is not presented as an observed issue time.\n\n"
        f"Direction-level relation guard: {data['normalization_summary']['direction_level_competitor_downgrade_count']} slice-specific competitor relation(s) in unresolved directions were demoted to `ADJACENT` with associated relation evidence reclassified as `ADJACENT_CONTEXT` ({', '.join(data['normalization_summary']['direction_level_competitor_downgrade_ids']) or 'none'}). `abuse_and_spam_controls` remains AMBIGUOUS; Anti Spam and ChatControl describe the chat/command-spam slice only.\n\n"
        f"QA status: **{qa['overall_status']}** ({len(qa['failed_checks'])} failed checks). Frozen-capture deterministic replay: `{qa['replay']['byte_identical']}` across {qa['replay']['artifact_count']} normalized data artifacts.\n\n"
        f"PR #17: OPEN / UNMERGED at execution code commit `{execution_code_commit}`. No live research was repeated and no Stage F work was started.\n\n"
        "## Direction outcomes\n\n"
        f"Options are listed in stable direction-ID order, not ranked.\n\n{status_lines}\n\n"
        "## Caveats\n\n"
        "Community posts are individual reports and are not prevalence estimates. Listing popularity counters are retained in their marketplace-native units and are not interpreted as market size. Unpublished/unstated prices and maintenance details remain null. Individual query issue timestamps were not present in the frozen capture; logged `issued_at` values use the capture retrieval timestamp, with the fallback basis recorded per query. The accepted YEE-76 identities and status/tier fields remain unchanged. This work stops before Stage F.\n"
    )
    (output / "FINAL_REPORT.md").write_text(report, encoding="utf-8", newline="\n")


def _write_manifest(output: Path, input_sha: str, execution_code_commit: str, qa: dict) -> None:
    files = []
    for path in sorted(output.rglob("*")):
        if path.is_file() and path.name != "DATASET_MANIFEST.json":
            files.append({
                "path": path.relative_to(output).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            })
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "input_work_order": "YEE-76",
        "input_sha256_before": input_sha,
        "input_sha256_after": qa["input_sha256_after"],
        "input_run_id": INPUT_RUN_ID,
        "input_schema_version": INPUT_SCHEMA_VERSION,
        "input_code_commit": INPUT_CODE_COMMIT,
        "input_merge_commit": INPUT_MERGE_COMMIT,
        "research_capture_sha256": qa["replay"]["capture_sha256"],
        "input_taxonomy_version": INPUT_TAXONOMY_VERSION,
        "input_taxonomy_sha256": INPUT_TAXONOMY_SHA256,
        "execution_code_commit": execution_code_commit,
        "direction_ids_sha256": DIRECTION_IDS_SHA256,
        "option_count": qa["option_count"],
        "category_count": qa["category_count"],
        "input_row_counts": {"categories": 11, "direction_tiers": 47, "research_options": 16},
        "deterministic_replay": qa["replay"],
        "normalization_summary": qa["normalization_summary"],
        "artifact_count": len(files),
        "artifacts": files,
        "qa_status": qa["overall_status"],
    }
    (output / "DATASET_MANIFEST.json").write_text(canonical_json(manifest) + "\n", encoding="utf-8", newline="\n")


def _qa_artifacts(
    output: Path, options: list[dict], categories: list[dict], data: dict, qa: dict
) -> None:
    database_path = output / "category_targeted_external_research.sqlite"
    sqlite_records_match = True
    connection = sqlite3.connect(database_path)
    try:
        qa["checks"]["sqlite_integrity_check"] = connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        qa["checks"]["sqlite_foreign_key_check"] = connection.execute("PRAGMA foreign_key_check").fetchall() == []
        expected_counts = {
            "input_provenance": len(data.get("input_provenance", {})),
            "frozen_category_snapshot": len(categories),
            "frozen_option_snapshot": len(options),
            "source_documents": len(data["source_documents"]),
            "research_queries": len(data["research_queries"]),
            "external_evidence": len(data["external_evidence"]),
            "competitor_entities": len(data["competitor_entities"]),
            "direction_semantic_relations": len(data["direction_semantic_relations"]),
            "direction_research_packs": len(data["direction_research_packs"]),
            "category_research_coverage": len(data["category_research_coverage"]),
        }
        reference_rows = _sqlite_reference_rows(data)
        expected_counts.update({table: len(rows) for table, rows in reference_rows.items()})
        qa["checks"]["sqlite_table_counts_reconcile"] = all(
            connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == count
            for table, count in expected_counts.items()
        )
        stored_options = {
            row[0]: json.loads(row[1])
            for row in connection.execute("SELECT direction_id, record_json FROM frozen_option_snapshot")
        }
        qa["checks"]["sqlite_frozen_options_match_accepted_input"] = stored_options == {
            row["direction_id"]: row for row in options
        }
        qa["checks"]["sqlite_schema_uses_canonical_source_and_coverage_names"] = (
            "source_domain" in {row[1] for row in connection.execute("PRAGMA table_info(source_documents)")}
            and "domain" not in {row[1] for row in connection.execute("PRAGMA table_info(source_documents)")}
            and "category_research_status" in {row[1] for row in connection.execute("PRAGMA table_info(category_research_coverage)")}
        )
        metadata = dict(connection.execute("SELECT key,value FROM metadata"))
        qa["checks"]["sqlite_input_provenance_and_commit_metadata_match"] = (
            metadata.get("input_sha256_before") == qa["input_sha256_before"]
            and metadata.get("input_sha256_after") == qa["input_sha256_after"]
            and metadata.get("input_run_id") == INPUT_RUN_ID
            and metadata.get("input_schema_version") == INPUT_SCHEMA_VERSION
            and metadata.get("input_taxonomy_version") == INPUT_TAXONOMY_VERSION
            and metadata.get("input_taxonomy_sha256") == INPUT_TAXONOMY_SHA256
            and metadata.get("execution_code_commit") == qa.get("execution_code_commit")
            and metadata.get("research_capture_sha256") == qa.get("capture_sha256") == CAPTURE_SHA256
            and metadata.get("capture_source_document_count") == str(data["normalization_summary"]["capture_source_document_count"])
            and metadata.get("canonical_source_document_count") == str(data["normalization_summary"]["canonical_source_document_count"])
            and metadata.get("timestamp_fallback_query_count") == str(data["normalization_summary"]["timestamp_fallback_query_count"])
            and metadata.get("direction_level_competitor_downgrade_count") == str(data["normalization_summary"]["direction_level_competitor_downgrade_count"])
        )
        for table, rows, key in (
            ("source_documents", data["source_documents"], "source_id"),
            ("research_queries", data["research_queries"], "query_id"),
            ("external_evidence", data["external_evidence"], "evidence_id"),
            ("competitor_entities", data["competitor_entities"], "competitor_id"),
            ("direction_semantic_relations", data["direction_semantic_relations"], "relation_id"),
            ("direction_research_packs", data["direction_research_packs"], "direction_id"),
            ("category_research_coverage", data["category_research_coverage"], "category_id"),
        ):
            stored = {row[0]: json.loads(row[1]) for row in connection.execute(f"SELECT {key},record_json FROM {table}")}
            sqlite_records_match = sqlite_records_match and stored == {row[key]: row for row in rows}
        references_match = True
        for table, expected_rows in reference_rows.items():
            references_match = references_match and set(connection.execute(f"SELECT * FROM {table}")) == set(expected_rows)
    finally:
        connection.close()
    qa["checks"]["sqlite_normalized_records_match_expected_rows"] = sqlite_records_match
    qa["checks"]["sqlite_foreign_key_reference_tables_match_normalized_rows"] = references_match

    export_counts = {
        "source_documents": len(data["source_documents"]),
        "external_evidence": len(data["external_evidence"]),
        "competitor_entities": len(data["competitor_entities"]),
        "research_queries": len(data["research_queries"]),
        "direction_semantic_relations": len(data["direction_semantic_relations"]),
        "direction_research_packs": len(data["direction_research_packs"]),
        "category_research_coverage": len(data["category_research_coverage"]),
    }
    exports_match = True
    csv_contract = True
    for table, count in export_counts.items():
        rows = data[table]
        jsonl_path = output / f"{table}.jsonl"
        csv_path = output / f"{table}.csv"
        with jsonl_path.open("r", encoding="utf-8") as stream:
            stored_jsonl = [json.loads(line) for line in stream]
        raw_csv = csv_path.read_bytes()
        csv_contract = csv_contract and not raw_csv.startswith(b"\xef\xbb\xbf")
        with csv_path.open("r", encoding="utf-8", newline="") as stream:
            stored_csv = list(csv.DictReader(stream))
        keys = sorted({key for row in rows for key in row})
        expected_csv = [
            {
                key: "\\N" if row.get(key) is None
                else canonical_json(row[key]) if isinstance(row.get(key), (dict, list))
                else str(row[key])
                for key in keys
            }
            for row in rows
        ]
        exports_match = exports_match and len(stored_jsonl) == count and stored_jsonl == rows
        exports_match = exports_match and len(stored_csv) == count and stored_csv == expected_csv
        if any(value is None for row in rows for value in row.values()):
            csv_contract = csv_contract and b"\\N" in raw_csv
    qa["checks"]["jsonl_csv_values_and_row_counts_reconcile"] = exports_match
    qa["checks"]["csv_utf8_no_bom_lf_and_literal_null_contract"] = csv_contract
    qa["failed_checks"] = [name for name, passed in qa["checks"].items() if not passed]
    qa["overall_status"] = "PASS" if not qa["failed_checks"] else "FAIL"


def _write_core_artifacts(
    output: Path, input_sha: str, execution_code_commit: str,
    categories: list[dict], options: list[dict], data: dict, capture_path: Path | None = None,
) -> list[Path]:
    output.mkdir(parents=True, exist_ok=True)
    _write_schema(output / "EXTERNAL_RESEARCH_SCHEMA.md")
    tables = (
        "source_documents", "external_evidence", "competitor_entities", "research_queries",
        "direction_semantic_relations", "direction_research_packs", "category_research_coverage",
    )
    artifacts = [output / "EXTERNAL_RESEARCH_SCHEMA.md"]
    for table in tables:
        rows = data[table]
        jsonl_path = output / f"{table}.jsonl"
        csv_path = output / f"{table}.csv"
        _write_jsonl(jsonl_path, rows)
        _write_csv(csv_path, rows)
        artifacts.extend((jsonl_path, csv_path))
    database_path = output / "category_targeted_external_research.sqlite"
    _write_sqlite(database_path, input_sha, execution_code_commit, categories, options, data)
    artifacts.append(database_path)
    if capture_path is not None:
        capture_copy = output / "YEE77_RESEARCH_CAPTURE.json"
        shutil.copyfile(capture_path, capture_copy)
        artifacts.append(capture_copy)
    return artifacts


def _artifact_hashes(paths: list[Path], root: Path) -> dict[str, str]:
    return {path.relative_to(root).as_posix(): sha256_file(path) for path in sorted(paths)}


def build_bundle(input_path: Path, capture_path: Path, output_dir: Path) -> dict:
    execution_code_commit = _current_execution_commit()
    input_sha, options, categories, input_metadata, input_provenance, input_checks = _read_snapshot(input_path)
    capture_sha = sha256_file(capture_path)
    if capture_sha != CAPTURE_SHA256:
        raise ValueError(f"Frozen YEE-77 research capture hash mismatch: {capture_sha}")
    capture = json.loads(capture_path.read_text(encoding="utf-8"))
    if (
        capture.get("input_work_order") != "YEE-76"
        or capture.get("input_run_id") != INPUT_RUN_ID
        or capture.get("input_sha256") != INPUT_SHA256
    ):
        raise ValueError("Research capture is not pinned to accepted YEE-76 input")
    data = _normalise_capture(options, capture)
    replayed_data = _normalise_capture(options, json.loads(capture_path.read_text(encoding="utf-8")))
    normalization_replay_equal = canonical_json(data) == canonical_json(replayed_data)
    data["input_provenance"] = input_provenance

    data["category_research_coverage"] = _build_coverage(
        categories, data["direction_research_packs"], data["competitor_entities"],
        data["direction_semantic_relations"], data["external_evidence"], data["research_queries"],
    )
    input_sha_after = sha256_file(input_path)
    input_checks["input_sha256_after_unchanged"] = input_sha_after == input_sha == INPUT_SHA256
    data["input_sha256_after"] = input_sha_after

    with tempfile.TemporaryDirectory(prefix="yee77-replay-a-") as first_dir, tempfile.TemporaryDirectory(prefix="yee77-replay-b-") as second_dir:
        first_root, second_root = Path(first_dir), Path(second_dir)
        first_paths = _write_core_artifacts(
            first_root, input_sha, execution_code_commit, categories, options, data, capture_path,
        )
        second_paths = _write_core_artifacts(
            second_root, input_sha, execution_code_commit, categories, options, data, capture_path,
        )
        first_hashes = _artifact_hashes(first_paths, first_root)
        second_hashes = _artifact_hashes(second_paths, second_root)
    different_artifacts = sorted(
        key for key in set(first_hashes) | set(second_hashes)
        if first_hashes.get(key) != second_hashes.get(key)
    )
    replay_result = {
        "capture_sha256": capture_sha,
        "normalization_byte_identical": normalization_replay_equal,
        "exports_and_sqlite_byte_identical": not different_artifacts,
        "byte_identical": normalization_replay_equal and not different_artifacts,
        "artifact_count": len(first_hashes),
        "different_artifacts": different_artifacts,
    }

    qa = _qa(options, categories, data, input_sha, input_checks, input_sha_after, replay_result)
    qa["execution_code_commit"] = execution_code_commit
    qa["accepted_input_metadata"] = {
        "run_id": input_metadata["run_id"],
        "schema_version": input_metadata["schema_version"],
        "code_commit": input_metadata["code_commit"],
        "taxonomy_version": input_metadata["taxonomy_version"],
        "taxonomy_sha256": input_metadata["taxonomy_sha256"],
        "read_mode": "read-only",
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_core_artifacts(output_dir, input_sha, execution_code_commit, categories, options, data, capture_path)
    _qa_artifacts(output_dir, options, categories, data, qa)
    _write_reports(output_dir, input_sha, execution_code_commit, options, data, qa)
    (output_dir / "QA_RESULT.json").write_text(canonical_json(qa) + "\n", encoding="utf-8", newline="\n")
    _write_manifest(output_dir, input_sha, execution_code_commit, qa)
    return qa
