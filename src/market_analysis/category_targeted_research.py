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
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlparse


SCHEMA_VERSION = "yee-77-category-targeted-external-research-v0.1"
INPUT_SHA256 = "700c22ad7bdd2ba9502e5adf933b3994e4ad84a852caef26d400094a9e8734bb"
INPUT_RUN_ID = "acfbe03b846c5b3387695b67ad2cef4cade33fa81eabb8069b038ac797615a7b"
INPUT_CODE_COMMIT = "71f24f8e825eaef5f989082d59ef0fa7e5f0bad4"
INPUT_MERGE_COMMIT = "03ffe29974681646780d5e6f40ade940d0480780"
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
EXPECTED_CATEGORY_IDS = {
    "administration", "communication", "developer_tools", "economy", "gameplay",
    "minigames", "protection", "roleplay", "server_utilities", "uncategorized", "world_management",
}


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
    return commit


def _read_snapshot(path: Path) -> tuple[str, list[dict], list[dict], dict]:
    actual_sha = sha256_file(path)
    if actual_sha != INPUT_SHA256:
        raise ValueError(f"YEE-76 input hash mismatch: {actual_sha}")
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
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
    finally:
        connection.close()
    identity_text = "".join(f"{row['direction_id']}\n" for row in sorted(options, key=lambda x: x["direction_id"]))
    if hashlib.sha256(identity_text.encode("utf-8")).hexdigest() != DIRECTION_IDS_SHA256:
        raise ValueError("YEE-76 direction identity set does not match the accepted pin")
    if len(options) != 16 or metadata.get("run_id") != INPUT_RUN_ID:
        raise ValueError("YEE-76 accepted snapshot is not the expected 16-option run")
    return actual_sha, options, categories, metadata


def _source_domains(sources: list[dict]) -> dict[str, str]:
    result = {}
    for source in sources:
        result[source["source_id"]] = (urlparse(source["canonical_url"]).hostname or "").lower()
    return result


def _normalise_capture(snapshot_options: list[dict], capture: dict) -> dict[str, list[dict]]:
    options_by_id = {row["direction_id"]: row for row in snapshot_options}
    sources = sorted(capture["source_documents"], key=lambda row: row["source_id"])
    source_ids = {row["source_id"] for row in sources}
    queries = sorted(capture["research_queries"], key=lambda row: row["query_id"])
    evidence = sorted(capture["external_evidence"], key=lambda row: row["evidence_id"])
    entities = sorted(capture["competitor_entities"], key=lambda row: row["competitor_id"])
    relations = sorted(capture.get("direction_semantic_relations", []), key=lambda row: row["relation_id"])

    for query in queries:
        option = options_by_id.get(query["direction_id"])
        if option is None:
            raise ValueError(f"Query references unknown direction {query['direction_id']}")
        query["category_id"] = option["category_id"]
        query["result_source_ids"] = sorted(set(query.get("result_source_ids", [])))
        if not set(query["result_source_ids"]).issubset(source_ids):
            raise ValueError(f"Query {query['query_id']} references unknown source")

    for source in sources:
        if not source["canonical_url"].startswith(("https://", "http://")):
            raise ValueError(f"Source URL is not public HTTP(S): {source['source_id']}")
        source["domain"] = (urlparse(source["canonical_url"]).hostname or "").lower()
        source["source_url"] = source.get("source_url", source["canonical_url"])
        source["retrieved_at"] = source.get("retrieved_at", capture["retrieved_at"])
        source["published_or_updated_at"] = source.get("published_or_updated_at")
        source["notes"] = source.get("notes", "")

    for item in evidence:
        option = options_by_id.get(item["direction_id"])
        if option is None or option["category_id"] != item["category_id"]:
            raise ValueError(f"Evidence has invalid direction/category: {item['evidence_id']}")
        if item["source_id"] not in source_ids:
            raise ValueError(f"Evidence has unknown source: {item['evidence_id']}")
        if item["claim_type"] not in {
            "SEMANTIC_IDENTITY", "COMPETITOR_RELATION", "FEATURE", "PRICING",
            "MAINTENANCE", "POPULARITY_PROXY", "OPERATOR_PAIN",
            "DIFFERENTIATION_SIGNAL", "OVERLAP_SIGNAL", "ADJACENT_CONTEXT",
        }:
            raise ValueError(f"Unknown evidence claim type: {item['claim_type']}")

    for entity in entities:
        option = options_by_id.get(entity["direction_id"])
        if option is None or option["category_id"] != entity["category_id"]:
            raise ValueError(f"Competitor has invalid direction/category: {entity['competitor_id']}")
        entity["evidence_ids"] = sorted(set(entity.get("evidence_ids", [])))
        entity["lifecycle_evidence_ids"] = sorted(set(entity.get("lifecycle_evidence_ids", [])))
        if not set(entity["evidence_ids"] + entity["lifecycle_evidence_ids"]).issubset(
            {item["evidence_id"] for item in evidence}
        ):
            raise ValueError(f"Competitor has unknown evidence: {entity['competitor_id']}")

    evidence_by_id = {item["evidence_id"]: item for item in evidence}
    option_category = {direction_id: row["category_id"] for direction_id, row in options_by_id.items()}
    for relation in relations:
        endpoints = {relation["direction_id"], relation["related_direction_id"]}
        if len(endpoints) != 2 or any(option_category[key] != relation["category_id"] for key in endpoints):
            raise ValueError(f"Relation has mismatched category/endpoints: {relation['relation_id']}")
        if not relation.get("evidence_ids") or any(
            evidence_id not in evidence_by_id
            or evidence_by_id[evidence_id]["category_id"] != relation["category_id"]
            or evidence_by_id[evidence_id]["direction_id"] not in endpoints
            for evidence_id in relation["evidence_ids"]
        ):
            raise ValueError(f"Relation has invalid evidence: {relation['relation_id']}")

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
    for relation in relations:
        for key in ("direction_id", "related_direction_id"):
            direction_id = relation[key]
            if direction_id not in options_by_id:
                raise ValueError(f"Relation has unknown endpoint: {relation['relation_id']}")
            by_direction_relations[direction_id].append(relation)

    packs = []
    for option in sorted(snapshot_options, key=lambda row: row["direction_id"]):
        direction_id = option["direction_id"]
        note = capture["pack_notes"][direction_id]
        ev_rows = by_direction_evidence[direction_id]
        query_rows = by_direction_queries[direction_id]
        entity_rows = by_direction_entities[direction_id]
        source_list = sorted({row["source_id"] for row in ev_rows})
        related = sorted({
            endpoint
            for relation in by_direction_relations[direction_id]
            for endpoint in (relation["direction_id"], relation["related_direction_id"])
        })
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
            "differentiation_hypotheses": [row for row in ev_rows if row["claim_type"] in {"DIFFERENTIATION_SIGNAL", "OVERLAP_SIGNAL"}],
            "semantic_relation_ids": sorted({row["relation_id"] for row in by_direction_relations[direction_id]}),
            "evidence_ids": sorted(row["evidence_id"] for row in ev_rows),
            "source_ids": source_list,
            "evidence_count": len(ev_rows),
            "primary_or_marketplace_evidence_count": sum(
                next(source for source in sources if source["source_id"] == row["source_id"])["source_type"]
                in {"PRIMARY_PRODUCT", "PRIMARY_DOCS", "PRIMARY_REPOSITORY", "MARKETPLACE_LISTING", "PRIMARY_SUPPORT"}
                for row in ev_rows
            ),
            "community_evidence_count": sum(
                next(source for source in sources if source["source_id"] == row["source_id"])["source_type"] == "COMMUNITY"
                for row in ev_rows
            ),
            "distinct_domain_count": len({_source_domains(sources)[source_id] for source_id in source_list}),
            "query_count": len(query_rows),
            "research_coverage_status": "PENDING_QA",
            "research_notes": note.get("research_notes", ""),
        })
        packs.append(pack)

    return {
        "source_documents": sources,
        "research_queries": queries,
        "external_evidence": evidence,
        "competitor_entities": entities,
        "direction_semantic_relations": relations,
        "direction_research_packs": packs,
    }


def _pricing_summary(evidence: list[dict]) -> dict[str, list[dict]]:
    result: dict[str, list[dict]] = defaultdict(list)
    for row in evidence:
        if row["claim_type"] == "PRICING":
            result[row.get("currency") or "UNSPECIFIED"].append({
                "evidence_id": row["evidence_id"],
                "entity_name": row.get("entity_name"),
                "numeric_value": row.get("numeric_value"),
                "unit": row.get("unit"),
                "currency": row.get("currency"),
                "observation": row["observation"],
            })
    return {key: sorted(rows, key=lambda row: row["evidence_id"]) for key, rows in sorted(result.items())}


def _qa(snapshot_options: list[dict], categories: list[dict], data: dict, input_sha: str) -> dict:
    checks = {}
    errors = []
    options_by_id = {row["direction_id"]: row for row in snapshot_options}
    sources_by_id = {row["source_id"]: row for row in data["source_documents"]}
    evidence_by_id = {row["evidence_id"]: row for row in data["external_evidence"]}
    entities = data["competitor_entities"]
    packs_by_id = {row["direction_id"]: row for row in data["direction_research_packs"]}

    checks["accepted_input_sha256"] = input_sha == INPUT_SHA256
    checks["exact_16_option_identity_reconciliation"] = len(options_by_id) == 16 and set(options_by_id) == set(packs_by_id)
    checks["immutable_stage_d_option_fields_preserved"] = all(
        all(pack[field] == options_by_id[direction_id][field] for field in IMMUTABLE_OPTION_FIELDS)
        for direction_id, pack in packs_by_id.items()
    )
    checks["all_query_source_and_evidence_references_resolve"] = all(
        query["direction_id"] in options_by_id and set(query.get("result_source_ids", [])).issubset(sources_by_id)
        for query in data["research_queries"]
    ) and all(
        item["source_id"] in sources_by_id and sources_by_id[item["source_id"]]["access_status"] in {"OPENED", "OPENED_PARTIAL"}
        for item in data["external_evidence"]
    )
    referenced_sources = (
        {item["source_id"] for item in data["external_evidence"]}
        | {source_id for query in data["research_queries"] for source_id in query.get("result_source_ids", [])}
    )
    checks["all_source_documents_are_used_by_queries_or_evidence"] = set(sources_by_id) == referenced_sources
    checks["all_opened_sources_have_canonical_public_urls"] = all(
        row["access_status"] in {"OPENED", "OPENED_PARTIAL"}
        and row["canonical_url"].startswith(("https://", "http://"))
        and bool(row["domain"])
        for row in data["source_documents"]
    )
    checks["research_row_ids_are_unique"] = all(
        len(rows) == len({row[key] for row in rows})
        for rows, key in (
            (data["source_documents"], "source_id"),
            (data["research_queries"], "query_id"),
            (data["external_evidence"], "evidence_id"),
            (entities, "competitor_id"),
            (data["direction_semantic_relations"], "relation_id"),
        )
    )
    q_by_direction: dict[str, list[dict]] = defaultdict(list)
    for query in data["research_queries"]:
        q_by_direction[query["direction_id"]].append(query)
    checks["required_distinct_query_purposes_for_all_options"] = all(
        set(REQUIRED_QUERY_PURPOSES).issubset({query["query_purpose"] for query in q_by_direction[direction_id]})
        and len(q_by_direction[direction_id]) >= 3
        and len(q_by_direction[direction_id]) <= 12
        for direction_id in options_by_id
    )
    checks["unresolved_options_have_three_query_attempts"] = all(
        packs_by_id[direction_id]["research_status"] != "UNRESOLVED"
        or len(q_by_direction[direction_id]) >= 3
        for direction_id in options_by_id
    )
    e_by_direction: dict[str, list[dict]] = defaultdict(list)
    for item in data["external_evidence"]:
        e_by_direction[item["direction_id"]].append(item)
    source_domains = _source_domains(data["source_documents"])
    for direction_id, pack in packs_by_id.items():
        pack_evidence = e_by_direction[direction_id]
        source_ids = ({row["source_id"] for row in pack_evidence}
                      | {source_id for query in q_by_direction[direction_id]
                         for source_id in query.get("result_source_ids", [])})
        if len(source_ids) > 18:
            errors.append(f"{direction_id}: more than 18 retained source pages")
        if pack["research_status"] == "RESOLVED":
            types = {row["claim_type"] for row in pack_evidence}
            gates = (
                len(pack_evidence) >= 5,
                len({source_domains[source_id] for source_id in source_ids}) >= 2,
                pack["primary_or_marketplace_evidence_count"] >= 1,
                "SEMANTIC_IDENTITY" in types,
                "FEATURE" in types,
                bool(pack["feature_themes"]),
                REQUIRED_QUERY_PURPOSES[1] in {row["query_purpose"] for row in q_by_direction[direction_id]},
                REQUIRED_QUERY_PURPOSES[2] in {row["query_purpose"] for row in q_by_direction[direction_id]},
                all(entity.get("lifecycle_evidence_ids") for entity in entities if entity["direction_id"] == direction_id and entity["relation_type"] == "DIRECT"),
            )
            pack["research_coverage_status"] = "SUFFICIENT" if all(gates) else "PARTIAL"
        elif pack["research_status"] == "AMBIGUOUS":
            pack["research_coverage_status"] = "AMBIGUOUS"
        else:
            pack["research_coverage_status"] = "UNRESOLVED"
    checks["resolved_research_coverage_gates"] = all(
        pack["research_coverage_status"] == "SUFFICIENT"
        for pack in packs_by_id.values()
        if pack["research_status"] == "RESOLVED"
    )
    checks["direct_competitors_have_relation_semantic_and_current_lifecycle_evidence"] = all(
        {"COMPETITOR_RELATION", "SEMANTIC_IDENTITY"}.issubset(
            {evidence_by_id[evidence_id]["claim_type"] for evidence_id in entity["evidence_ids"]}
        )
        and bool(entity.get("lifecycle_evidence_ids"))
        and all(
            evidence_by_id[evidence_id]["claim_type"] == "MAINTENANCE"
            and sources_by_id[evidence_by_id[evidence_id]["source_id"]]["access_status"] in {"OPENED", "OPENED_PARTIAL"}
            and sources_by_id[evidence_by_id[evidence_id]["source_id"]]["source_type"] in {
                "PRIMARY_PRODUCT", "PRIMARY_DOCS", "PRIMARY_REPOSITORY", "MARKETPLACE_LISTING", "PRIMARY_SUPPORT",
            }
            for evidence_id in entity["lifecycle_evidence_ids"]
        )
        and bool(entity.get("maintenance_status"))
        for entity in entities if entity["relation_type"] == "DIRECT"
    )
    checks["competitor_evidence_is_same_direction_and_category"] = all(
        evidence_id in evidence_by_id
        and evidence_by_id[evidence_id]["direction_id"] == entity["direction_id"]
        and evidence_by_id[evidence_id]["category_id"] == entity["category_id"]
        for entity in entities
        for evidence_id in entity["evidence_ids"] + entity["lifecycle_evidence_ids"]
    )
    checks["competitor_relation_types_are_explicit"] = all(
        entity["relation_type"] in {"DIRECT", "SUBSTITUTE", "ADJACENT"}
        for entity in entities
    )
    checks["all_competitor_entities_have_identity_basis"] = all(
        any(evidence_by_id[evidence_id]["claim_type"] == "SEMANTIC_IDENTITY" for evidence_id in entity["evidence_ids"])
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
    checks["entity_prices_have_source_evidence_and_currency"] = all(
        entity.get("price") is None or (
            entity.get("currency") is not None
            and any(evidence_id in evidence_by_id and evidence_by_id[evidence_id]["claim_type"] == "PRICING"
                    and evidence_by_id[evidence_id].get("currency") == entity["currency"]
                    for evidence_id in entity["evidence_ids"])
        )
        for entity in entities
    )
    checks["no_rankings_scores_or_recommendations"] = all(
        not any(token in key.casefold() for token in ("score", "rank", "recommendation", "shortlist", "winner"))
        for pack in packs_by_id.values() for key in pack
    )
    coverage = _build_coverage(categories, list(packs_by_id.values()))
    checks["all_11_taxonomy_category_coverage_rows"] = (
        len(coverage) == 11 and {row["category_id"] for row in coverage} == EXPECTED_CATEGORY_IDS
    )
    coverage_by_id = {row["category_id"]: row for row in coverage}
    checks["zero_option_category_statuses_match_spec"] = all(
        coverage_by_id.get(category_id, {}).get("research_coverage_status") == status
        for category_id, status in ZERO_OPTION_CATEGORY_STATUS.items()
    )
    checks["category_counts_reconcile_to_16"] = sum(row["option_count"] for row in coverage) == 16
    if errors:
        checks["retained_page_limits"] = False
    else:
        checks["retained_page_limits"] = True
    failed = [name for name, value in checks.items() if not value]
    return {
        "schema_version": SCHEMA_VERSION,
        "input_sha256": input_sha,
        "direction_ids_sha256": DIRECTION_IDS_SHA256,
        "option_count": len(snapshot_options),
        "source_document_count": len(data["source_documents"]),
        "research_query_count": len(data["research_queries"]),
        "evidence_count": len(data["external_evidence"]),
        "competitor_entity_count": len(entities),
        "relation_count": len(data["direction_semantic_relations"]),
        "research_status_counts": dict(sorted(Counter(pack["research_status"] for pack in packs_by_id.values()).items())),
        "coverage_status_counts": dict(sorted(Counter(pack["research_coverage_status"] for pack in packs_by_id.values()).items())),
        "checks": checks,
        "errors": errors,
        "failed_checks": failed,
        "overall_status": "PASS" if not failed else "FAIL",
    }


def _build_coverage(categories: list[dict], packs: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for pack in packs:
        grouped[pack["category_id"]].append(pack)
    rows = []
    for category in sorted(categories, key=lambda row: row.get("category_order", 0)):
        category_id = category["category_id"]
        items = grouped.get(category_id, [])
        if items:
            statuses = Counter(pack["research_status"] for pack in items)
            status = "RESEARCHED"
        else:
            statuses = Counter()
            status = ZERO_OPTION_CATEGORY_STATUS.get(category_id, "NO_STAGE_D_RESEARCH_OPTIONS")
        rows.append({
            "category_id": category_id,
            "category_name": category.get("category_name"),
            "option_count": len(items),
            "research_status_counts": dict(sorted(statuses.items())),
            "research_coverage_status": status,
            "direction_ids": sorted(pack["direction_id"] for pack in items),
        })
    return rows


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(canonical_json(row) + "\n" for row in rows), encoding="utf-8", newline="\n")


def _write_csv(path: Path, rows: list[dict]) -> None:
    keys = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=keys, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: canonical_json(value) if isinstance(value, (dict, list)) else value for key, value in row.items()})


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
            CREATE TABLE frozen_category_snapshot(category_id TEXT PRIMARY KEY, category_order INTEGER NOT NULL, record_json TEXT NOT NULL) WITHOUT ROWID;
            CREATE TABLE frozen_option_snapshot(direction_id TEXT PRIMARY KEY, category_id TEXT NOT NULL, record_json TEXT NOT NULL,
                FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id)) WITHOUT ROWID;
            CREATE TABLE source_documents(source_id TEXT PRIMARY KEY, domain TEXT NOT NULL, canonical_url TEXT NOT NULL, record_json TEXT NOT NULL) WITHOUT ROWID;
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
            CREATE TABLE category_research_coverage(category_id TEXT PRIMARY KEY, option_count INTEGER NOT NULL, research_coverage_status TEXT NOT NULL, record_json TEXT NOT NULL,
                FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id)) WITHOUT ROWID;
        """)
        meta = {
            "schema_version": SCHEMA_VERSION,
            "input_work_order": "YEE-76",
            "input_sha256": input_sha,
            "input_run_id": INPUT_RUN_ID,
            "input_code_commit": INPUT_CODE_COMMIT,
            "input_merge_commit": INPUT_MERGE_COMMIT,
            "execution_code_commit": execution_code_commit,
            "direction_ids_sha256": DIRECTION_IDS_SHA256,
            "research_retrieved_at": "2026-09-27T14:05:19Z",
        }
        connection.executemany("INSERT INTO metadata VALUES (?,?)", sorted(meta.items()))
        connection.executemany("INSERT INTO frozen_category_snapshot VALUES (?,?,?)", [
            (row["category_id"], row.get("category_order", 0), canonical_json(row)) for row in categories
        ])
        connection.executemany("INSERT INTO frozen_option_snapshot VALUES (?,?,?)", [
            (row["direction_id"], row["category_id"], canonical_json(row)) for row in options
        ])
        connection.executemany("INSERT INTO source_documents VALUES (?,?,?,?)", [
            (row["source_id"], row["domain"], row["canonical_url"], canonical_json(row)) for row in data["source_documents"]
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
                    values = (row[key], row["option_count"], row["research_coverage_status"], canonical_json(row))
                    connection.execute("INSERT INTO category_research_coverage VALUES (?,?,?,?)", values)
        connection.commit()
        connection.execute("PRAGMA optimize")
    finally:
        connection.close()


def _write_schema(path: Path) -> None:
    path.write_text(
        "# YEE-77 External Research Schema\n\n"
        f"Schema version: `{SCHEMA_VERSION}`. Research is keyed to the exact accepted YEE-76 direction IDs and category IDs.\n\n"
        "## Tables and keys\n\n"
        "- `frozen_category_snapshot`: accepted category ID/order and original category JSON.\n"
        "- `frozen_option_snapshot`: accepted direction ID/category and original option JSON; immutable input snapshot.\n"
        "- `source_documents`: source ID, public canonical URL, host domain, title, source type, access status, retrieval/update dates and notes.\n"
        "- `research_queries`: query ID, direction/category, query purpose/text, and opened result source IDs. Every direction has semantic-resolution, competitor-discovery, and operator pain-point discovery attempts.\n"
        "- `external_evidence`: evidence ID, direction/category, source ID, typed claim, observation, and optional native value/unit/currency or feature theme. Every retained claim references an opened page.\n"
        "- `competitor_entities`: direction/category-scoped entity identity, DIRECT/SUBSTITUTE/ADJACENT relation, product/platform, source-native pricing and maintenance fields, and evidence/lifecycle evidence IDs.\n"
        "- `direction_semantic_relations`: explicit evidence-backed overlap/adjacency between accepted directions; relations do not merge or replace either identity.\n"
        "- `direction_research_packs`: one row per accepted direction, with unchanged YEE-76 fields plus bounded research status, market-job interpretation, feature themes, native observations, evidence/source IDs, and coverage.\n"
        "- `category_research_coverage`: one row per accepted taxonomy category, including explicit status for categories with zero Stage D options.\n\n"
        "## Interpretation and serialization\n\n"
        "YEE-76 direction/category identity and opportunity fields are copied unchanged. Research statuses are `RESOLVED`, `AMBIGUOUS`, or `UNRESOLVED`; coverage does not upgrade an ambiguous status. Missing price, maintenance, popularity, feature or pain evidence is null/not established, never interpreted as free, absent, or zero. Pricing and popularity retain source-native units; community pain is qualitative and not a prevalence estimate. Search snippets are discovery-only; citations resolve to opened public source documents. JSONL uses canonical UTF-8 JSON; CSV uses UTF-8 with BOM and canonical JSON for nested fields. SQLite has primary/foreign keys and stores the full records as JSON. No merge, score, ranking, shortlist, winner, or recommendation is produced.\n",
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
        "Stage E performs bounded, source-backed external research for exactly the 16 accepted YEE-76 LEAD/WATCH options. It does not alter category/direction identity, infer product demand beyond accepted inputs, rank opportunities, merge directions, or recommend products. YEE-76 SQLite is opened read-only and its SHA-256 is checked before build. YEE-47 is not used as input or source of labels/conclusions.\n"
    )
    protocol = (
        "# External research protocol — YEE-77\n\n"
        "For every accepted option, record distinct semantic-resolution, competitor-discovery, and operator pain-point discovery queries; cap each option at 12 searches and 18 retained opened pages. Search results are discovery only. Retained claims cite opened public canonical pages. Each direct entity receives an identity/lifecycle check from its current primary page when available. Pricing is source-native and absent pricing is null. Pain statements are qualitative single-source observations, not prevalence. Ambiguous broad options remain ambiguous; no option is deleted or merged.\n"
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
            f"{row['name']} ({row['relation_type']}; maintenance: {row.get('maintenance_status') or 'not established'})"
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
                f"- {row['observation']} ([{sources_by_id[row['source_id']]['title']}]({sources_by_id[row['source_id']]['canonical_url']}))."
                for row in pain
            )
        priced = [row for row in evidence if row["claim_type"] == "PRICING"]
        if priced:
            section.extend(["", "**Source-native pricing observations:**"])
            section.extend(
                f"- {row['observation']} ([{sources_by_id[row['source_id']]['title']}]({sources_by_id[row['source_id']]['canonical_url']}).)"
                for row in priced
            )
        direction_sections.append("\n".join(section))
    status_lines = "\n\n".join(direction_sections)
    report = (
        "# YEE-77 Final Report\n\n"
        "## Scope and input\n\n"
        f"Researched all {len(options)} accepted YEE-76 options. Read-only input SHA-256: `{input_sha}`; accepted run `{INPUT_RUN_ID}`; input code commit `{INPUT_CODE_COMMIT}`; YEE-77 execution code commit `{execution_code_commit}`.\n\n"
        "## Results\n\n"
        f"Research statuses: {resolved} RESOLVED, {ambiguous} AMBIGUOUS, {unresolved} UNRESOLVED. All three required query purposes were run for every option. Retained {len(data['source_documents'])} opened source documents and {len(data['external_evidence'])} source-backed observations. No product ranking, score, winner, recommendation, or merging was performed.\n\n"
        f"QA status: **{qa['overall_status']}** ({len(qa['failed_checks'])} failed checks).\n\n"
        "## Direction outcomes\n\n"
        f"Options are listed in stable direction-ID order, not ranked.\n\n{status_lines}\n\n"
        "## Caveats\n\n"
        "Community posts are individual reports and are not prevalence estimates. Listing popularity counters are retained in their marketplace-native units and are not interpreted as market size. Unpublished/unstated prices and maintenance details remain null. The accepted YEE-76 identities and status/tier fields remain unchanged. This work stops before Stage F.\n"
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
        "input_sha256": input_sha,
        "input_run_id": INPUT_RUN_ID,
        "input_code_commit": INPUT_CODE_COMMIT,
        "input_merge_commit": INPUT_MERGE_COMMIT,
        "execution_code_commit": execution_code_commit,
        "direction_ids_sha256": DIRECTION_IDS_SHA256,
        "artifact_count": len(files),
        "artifacts": files,
        "qa_status": qa["overall_status"],
    }
    (output / "DATASET_MANIFEST.json").write_text(canonical_json(manifest) + "\n", encoding="utf-8", newline="\n")


def _qa_artifacts(
    output: Path, options: list[dict], categories: list[dict], data: dict, qa: dict
) -> None:
    database_path = output / "category_targeted_external_research.sqlite"
    connection = sqlite3.connect(database_path)
    try:
        qa["checks"]["sqlite_integrity_check"] = connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        qa["checks"]["sqlite_foreign_key_check"] = connection.execute("PRAGMA foreign_key_check").fetchall() == []
        expected_counts = {
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
    finally:
        connection.close()

    export_counts = {
        "source_documents": len(data["source_documents"]),
        "external_evidence": len(data["external_evidence"]),
        "competitor_entities": len(data["competitor_entities"]),
        "research_queries": len(data["research_queries"]),
        "direction_semantic_relations": len(data["direction_semantic_relations"]),
        "direction_research_packs": len(data["direction_research_packs"]),
        "category_research_coverage": len(data["category_research_coverage"]),
    }
    def jsonl_count(path: Path) -> int:
        with path.open("r", encoding="utf-8") as stream:
            return sum(1 for _ in stream)

    def csv_count(path: Path) -> int:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            return sum(1 for _ in csv.DictReader(stream))

    qa["checks"]["jsonl_csv_export_counts_reconcile"] = all(
        jsonl_count(output / f"{table}.jsonl") == count
        and csv_count(output / f"{table}.csv") == count
        for table, count in export_counts.items()
    )
    qa["failed_checks"] = [name for name, passed in qa["checks"].items() if not passed]
    qa["overall_status"] = "PASS" if not qa["failed_checks"] else "FAIL"


def build_bundle(input_path: Path, capture_path: Path, output_dir: Path) -> dict:
    execution_code_commit = _current_execution_commit()
    input_sha, options, categories, _ = _read_snapshot(input_path)
    capture = json.loads(capture_path.read_text(encoding="utf-8"))
    if capture.get("input_run_id") != INPUT_RUN_ID or capture.get("input_sha256") != INPUT_SHA256:
        raise ValueError("Research capture is not pinned to accepted YEE-76 input")
    data = _normalise_capture(options, capture)
    coverage = _build_coverage(categories, data["direction_research_packs"])
    data["category_research_coverage"] = coverage
    qa = _qa(options, categories, data, input_sha)
    qa["execution_code_commit"] = execution_code_commit
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_schema(output_dir / "EXTERNAL_RESEARCH_SCHEMA.md")
    _write_jsonl(output_dir / "source_documents.jsonl", data["source_documents"])
    _write_csv(output_dir / "source_documents.csv", data["source_documents"])
    for table in ("external_evidence", "competitor_entities", "research_queries", "direction_semantic_relations", "direction_research_packs", "category_research_coverage"):
        rows = data[table]
        _write_jsonl(output_dir / f"{table}.jsonl", rows)
        _write_csv(output_dir / f"{table}.csv", rows)
    _write_sqlite(
        output_dir / "category_targeted_external_research.sqlite", input_sha,
        execution_code_commit, categories, options, data,
    )
    _qa_artifacts(output_dir, options, categories, data, qa)
    _write_reports(output_dir, input_sha, execution_code_commit, options, data, qa)
    (output_dir / "QA_RESULT.json").write_text(canonical_json(qa) + "\n", encoding="utf-8", newline="\n")
    _write_manifest(output_dir, input_sha, execution_code_commit, qa)
    return qa
