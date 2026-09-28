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
CAPTURE_VERSION = "yee-81-deep-commercial-validation-v0.1"
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
NULL_TOKEN = r"\N"

TABLES: dict[str, tuple[str, ...]] = {
    "commercial_source_documents": ("source_id", "url", "canonical_url", "domain", "title", "source_type", "access_status", "retrieved_at", "published_or_updated_at", "date_note"),
    "commercial_research_queries": ("query_id", "direction_id", "sequence", "purpose", "query_text", "issued_at", "query_kind", "result_note", "opened_source_ids"),
    "commercial_evidence": ("evidence_id", "direction_id", "source_id", "query_ids", "evidence_type", "observation", "source_locator", "limitations"),
    "buyer_problem_observations": ("observation_id", "direction_id", "actor", "situation_problem", "desired_outcome", "evidence_ids", "limitations"),
    "commercial_competitor_offerings": ("offering_id", "direction_id", "entity_name", "canonical_url", "relation_type", "product_type", "buyer_job", "monetization_status", "evidence_ids", "limitations"),
    "pricing_observations": ("pricing_id", "direction_id", "entity_name", "amount", "currency_code", "currency_symbol", "billing_model", "period", "tier_or_scope", "price_context", "monetization_status", "evidence_ids", "limitations"),
    "direct_wtp_observations": ("wtp_id", "direction_id", "wtp_class", "statement", "amount", "currency_code", "currency_symbol", "evidence_ids", "limitations"),
    "paid_market_precedents": ("precedent_id", "direction_id", "entity_name", "precedent_type", "amount", "currency_code", "currency_symbol", "billing_model", "evidence_ids", "limitations"),
    "other_market_proxies": ("proxy_id", "direction_id", "proxy_type", "observation", "evidence_ids", "limitations"),
    "differentiation_observations": ("observation_id", "direction_id", "comparison_target", "feature_axis", "observation", "status", "evidence_ids", "limitations"),
    "feasibility_observations": ("observation_id", "direction_id", "kind", "observation", "evidence_ids", "limitations"),
    "support_burden_observations": ("observation_id", "direction_id", "kind", "observation", "evidence_ids", "limitations"),
    "channel_observations": ("observation_id", "direction_id", "channel", "observation", "evidence_ids", "limitations"),
    "direction_commercial_validation": ("direction_id", "direction_key", "category_id", "market_job", "coverage_status", "dimension_assessments", "direct_wtp_summary", "paid_market_precedent_summary", "other_proxy_summary", "unknowns", "risk_flags"),
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


def normalize_capture(capture: Mapping[str, Any], canonical_rows: Sequence[Mapping[str, Any]],
                      input_hashes: Mapping[str, str]) -> dict[str, list[dict[str, Any]]]:
    if capture.get("capture_version") != CAPTURE_VERSION:
        raise CommercialValidationError(f"capture_version must equal {CAPTURE_VERSION}")
    if capture.get("canonical_input_hashes") != dict(input_hashes):
        raise CommercialValidationError("capture must pin both canonical input hashes")
    ids = {row["direction_id"] for row in canonical_rows}
    directions = {row["direction_id"]: row for row in canonical_rows}
    if capture.get("authorized_direction_ids") != sorted(ids):
        raise CommercialValidationError("capture membership must equal the exact authorized direction set")
    sources: dict[str, dict[str, Any]] = {}
    urls: dict[str, str] = {}
    for raw in capture.get("sources", []):
        source_id = raw.get("source_id")
        if not source_id or source_id in sources or raw.get("access_status") != "OPENED":
            raise CommercialValidationError("every source must have a unique id and OPENED access status")
        canonical_url = canonicalize_url(raw.get("url", ""))
        if canonical_url in urls:
            raise CommercialValidationError(f"duplicate canonical source URL: {canonical_url}")
        if not raw.get("title") or not raw.get("source_type") or not raw.get("domain"):
            raise CommercialValidationError(f"opened source metadata incomplete: {source_id}")
        row = dict(raw)
        row["canonical_url"] = canonical_url
        row["domain"] = urlsplit(canonical_url).hostname or ""
        row["retrieved_at"] = _timestamp(row.get("retrieved_at"), f"source.{source_id}.retrieved_at")
        sources[source_id] = row
        urls[canonical_url] = source_id
    if not sources:
        raise CommercialValidationError("capture contains no opened public sources")

    queries: dict[str, dict[str, Any]] = {}
    for raw in capture.get("queries", []):
        query_id = raw.get("query_id")
        direction_id = raw.get("direction_id")
        if not query_id or query_id in queries or direction_id not in ids or raw.get("purpose") not in QUERY_PURPOSES:
            raise CommercialValidationError(f"invalid query identity/purpose: {raw}")
        query = dict(raw)
        query["issued_at"] = _timestamp(raw.get("issued_at"), f"query.{query_id}.issued_at")
        query["opened_source_ids"] = _ref_list(raw, "opened_source_ids", set(sources), "source")
        queries[query_id] = query
    purposes_by_direction = {key: set() for key in ids}
    mandatory_by_direction = {key: [] for key in ids}
    for query in queries.values():
        purposes_by_direction[query["direction_id"]].add(query["purpose"])
        if query.get("query_kind") == "MANDATORY_SEARCH":
            mandatory_by_direction[query["direction_id"]].append(query["purpose"])
    for direction_id, purposes in purposes_by_direction.items():
        mandatory = mandatory_by_direction[direction_id]
        if len(mandatory) != len(QUERY_PURPOSES) or set(mandatory) != set(QUERY_PURPOSES):
            raise CommercialValidationError(f"each direction requires exactly one mandatory search per purpose: {direction_id}")
        missing = set(QUERY_PURPOSES) - purposes
        if missing:
            raise CommercialValidationError(f"required query purposes missing for {direction_id}: {sorted(missing)}")

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
            raise CommercialValidationError(f"evidence must be a paraphrased factual observation from an opened source: {evidence_id}")
        if raw.get("verbatim_excerpt"):
            words = re.findall(r"\b\w+\b", raw["verbatim_excerpt"])
            if len(words) > 25:
                raise CommercialValidationError(f"verbatim excerpt exceeds the per-source copyright limit: {evidence_id}")
        evidence[evidence_id] = dict(raw, query_ids=query_ids)

    def refs(raw: Mapping[str, Any], label: str) -> list[str]:
        result = _ref_list(raw, "evidence_ids", set(evidence), label)
        if not result or any(evidence[eid]["direction_id"] != raw["direction_id"] for eid in result):
            raise CommercialValidationError(f"{label} must cite same-direction evidence")
        return result

    source_rows = [{key: source.get(key) for key in TABLES["commercial_source_documents"]}
                   for source in sorted(sources.values(), key=lambda item: item["source_id"])]
    query_rows = []
    for query in sorted(queries.values(), key=lambda item: (item["direction_id"], item["sequence"], item["query_id"])):
        if not query.get("query_text") or not query.get("result_note"):
            raise CommercialValidationError(f"query needs a text and disposition: {query['query_id']}")
        query_rows.append({key: query.get(key) for key in TABLES["commercial_research_queries"]})
    evidence_rows = []
    for row in sorted(evidence.values(), key=lambda item: item["evidence_id"]):
        evidence_rows.append({key: row.get(key) for key in TABLES["commercial_evidence"]})

    output: dict[str, list[dict[str, Any]]] = {
        "commercial_source_documents": source_rows,
        "commercial_research_queries": query_rows,
        "commercial_evidence": evidence_rows,
    }
    capture_tables = {
        "buyer_problem_observations": "buyer_problems",
        "commercial_competitor_offerings": "competitor_offerings",
        "pricing_observations": "pricing_observations",
        "direct_wtp_observations": "direct_wtp_observations",
        "paid_market_precedents": "paid_market_precedents",
        "other_market_proxies": "other_market_proxies",
        "differentiation_observations": "differentiation_observations",
        "feasibility_observations": "feasibility_observations",
        "support_burden_observations": "support_burden_observations",
        "channel_observations": "channel_observations",
    }
    for table, key in capture_tables.items():
        rows = []
        for raw in capture.get(key, []):
            if raw.get("direction_id") not in ids:
                raise CommercialValidationError(f"{table} contains an out-of-cohort direction")
            evidence_ids = refs(raw, table)
            row = dict(raw)
            row["evidence_ids"] = evidence_ids
            if table == "pricing_observations":
                amount = row.get("amount")
                if amount is not None and (not isinstance(amount, (int, float)) or amount < 0):
                    raise CommercialValidationError("price amount must be a nonnegative source-native number or null")
                if amount == 0 and row.get("price_context") != "EXPLICIT_ZERO_PRICE":
                    raise CommercialValidationError("zero is retained only when the source explicitly states a zero price")
                if row.get("monetization_status") == "FREE_VERIFIED" and amount is None:
                    raise CommercialValidationError("unknown amount cannot be normalized as free")
                if amount is None and row.get("monetization_status") not in {"PAID_VERIFIED", "FREE_VERIFIED", "UNKNOWN"}:
                    raise CommercialValidationError("unknown amount requires explicit monetization status")
            if table == "direct_wtp_observations" and row.get("wtp_class") not in {"EXPLICIT_WTP_INTENT", "OBSERVED_PAYMENT"}:
                raise CommercialValidationError("direct WTP must be an explicit buyer intention or observed transaction")
            rows.append({column: row.get(column) for column in TABLES[table]})
        output[table] = sorted(rows, key=lambda item: tuple(str(item.get(k) or "") for k in TABLES[table][:2]))

    packs = []
    capture_directions = {row.get("direction_id"): row for row in capture.get("directions", [])}
    if set(capture_directions) != ids or len(capture_directions) != len(ids):
        raise CommercialValidationError("direction assessments must contain the exact five authorized identities")
    for direction_id in sorted(ids):
        raw = capture_directions[direction_id]
        canonical = directions[direction_id]
        assessments = raw.get("dimension_assessments", [])
        by_dimension = {item.get("dimension"): item for item in assessments}
        if set(by_dimension) != set(DIMENSIONS) or len(assessments) != len(DIMENSIONS):
            raise CommercialValidationError(f"exactly seven commercial dimensions are required for {direction_id}")
        for name, item in by_dimension.items():
            if item.get("state") not in DIMENSION_STATES or not item.get("basis"):
                raise CommercialValidationError(f"invalid dimension assessment {direction_id}/{name}")
            item["evidence_ids"] = _ref_list(item, "evidence_ids", set(evidence), "dimension evidence")
            if any(evidence[eid]["direction_id"] != direction_id for eid in item["evidence_ids"]):
                raise CommercialValidationError(f"dimension evidence crosses direction boundary: {direction_id}/{name}")
            if item["state"] != "INSUFFICIENT_EVIDENCE" and not item["evidence_ids"]:
                raise CommercialValidationError(f"supported dimension lacks evidence: {direction_id}/{name}")
        if raw.get("coverage_status") not in {"SUFFICIENT", "PARTIAL", "INSUFFICIENT"}:
            raise CommercialValidationError(f"invalid commercial coverage status for {direction_id}")
        pack = {
            "direction_id": direction_id,
            "direction_key": canonical["direction_key"],
            "category_id": canonical["category_id"],
            "market_job": canonical["market_job"],
            "coverage_status": raw["coverage_status"],
            "dimension_assessments": [by_dimension[name] for name in DIMENSIONS],
            "direct_wtp_summary": raw.get("direct_wtp_summary", "No direction-level inference from competitor prices."),
            "paid_market_precedent_summary": raw.get("paid_market_precedent_summary", "No direct-WTP inference."),
            "other_proxy_summary": raw.get("other_proxy_summary", "Not a demand or willingness-to-pay estimate."),
            "unknowns": sorted(set(raw.get("unknowns", []))),
            "risk_flags": sorted(set(raw.get("risk_flags", []))),
        }
        packs.append({key: pack.get(key) for key in TABLES["direction_commercial_validation"]})
    output["direction_commercial_validation"] = packs
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
            (row["direction_id"], row["direction_key"], row["category_id"])
            for row in tables["direction_commercial_validation"]
        ])
        con.executemany("INSERT INTO direction_identity VALUES(?)", [(key,) for key in sorted(AUTHORIZED_DIRECTIONS)])
        for key, value in sorted(metadata.items()):
            con.execute("INSERT INTO metadata VALUES(?,?)", (key, value))
        for key, value in sorted(json.loads(metadata["input_hashes"]).items()):
            con.execute("INSERT INTO input_provenance VALUES(?,?)", (key, value))
        for table in TABLES:
            con.execute(f"CREATE TABLE {table}(record_id TEXT PRIMARY KEY,direction_id TEXT,record_json TEXT NOT NULL,FOREIGN KEY(direction_id) REFERENCES direction_identity(direction_id)) WITHOUT ROWID")
            values = []
            for row in tables[table]:
                direction_id = row.get("direction_id")
                values.append((_db_row_id(table, row), direction_id, _json(row)))
            con.executemany(f"INSERT INTO {table} VALUES(?,?,?)", values)
        con.commit()
        con.execute("VACUUM")
    finally:
        con.close()


def _render_schema(input_hashes: Mapping[str, str], execution_commit: str) -> str:
    lines = [
        "# YEE-81 Commercial Validation Schema v0.1", "",
        f"Execution commit: `{execution_commit}`", "",
        "Exactly five authorized YEE-79 direction identities are retained. YEE-79 and YEE-77 are read-only; YEE-77 is an identity/provenance check, not current market evidence.", "",
        "## Semantics", "",
        "- Every factual evidence row references a public source page that was opened. Search snippets are discovery-only.",
        "- Direct WTP, paid-market precedent, and other behavioral/contextual proxies are separate tables and must not be substituted for one another.",
        "- Unknown price is null, not free; currency symbols and source-native amounts are not converted or aggregated.",
        "- Commercial assessments contain seven dimensions and four evidence states. No score, rank, winner, product recommendation, or prevalence estimate is generated.",
        "- JSON nulls are represented as `\\N` in CSV; UTF-8 and LF line endings are used.", "",
        "## Canonical input hashes", "",
    ]
    lines.extend(f"- `{key}`: `{value}`" for key, value in sorted(input_hashes.items()))
    lines += ["", "## Relational export contract", ""]
    for table, columns in TABLES.items():
        lines.append(f"- `{table}` — " + ", ".join(f"`{column}`" for column in columns))
    lines += ["", "Each table is exported as deterministic UTF-8 JSONL and CSV. CSV null is `\\N`; structured values are canonical JSON strings. SQLite stores one canonical JSON record per row and enforces direction foreign keys.", ""]
    lines.extend(f"- `{table}.jsonl` and `{table}.csv`" for table in TABLES)
    lines += ["- `commercial_validation.sqlite`", "- `COMMERCIAL_RESEARCH_CAPTURE.json`", ""]
    return "\n".join(lines)


def _render_protocol() -> str:
    return """# YEE-81 Stage G Deep Commercial Validation Protocol

## Authorized boundary

Validate only the five exact direction identities authorized by YEE-79. YEE-79 and YEE-77 are read-only. Do not import earlier YEE-30–YEE-59 research as evidence or use it to seed external research. Do not produce TAM, revenue, profit, prevalence, score, ranking, winner, recommendation, product concept, roadmap, or implementation.

## Research procedure

For every direction, issue and retain the ten mandatory searches in the prescribed purpose sequence: buyer/operator segment; pain depth/consequence; purchase trigger; paid alternatives/pricing; free incumbents/substitutes; direct willingness-to-pay evidence; differentiation/unmet needs; channel fit; implementation/compatibility; support/maintenance. Search results are discovery only. Record query text, issue time, disposition, and opened source IDs. Run materially different follow-ups when a mandatory purpose yields no usable evidence. Retain negative/empty outcomes rather than silently dropping searches.

Open each factual source page before extracting a fact. Store canonical public URL, page title, source type, access result, retrieval time, and any source-visible publication/update date. Every normalized observation links to same-direction evidence, an opened source, and one or more same-direction queries that discovered/opened it. Paraphrase facts; any verbatim excerpt is limited to 25 words per source. Do not bypass login, paywall, CAPTCHA, or service controls.

## Commercial evidence distinctions

- Direct WTP is only an explicit prospective buyer purchase-intent statement about the studied job or an observed transaction. A competitor price is never direct WTP.
- Paid-market precedent is a source-stated price or explicit paid product precedent. Preserve the source-native amount, currency symbol/code when stated, billing model, period, tier, and scope. Do not infer a billing period or convert/aggregate prices.
- Other proxies include public problem reports, free alternatives, adoption/channel signals, maintenance/dependency facts, and explicit commissioning intent. Label their limits and do not promote them into WTP.
- Missing price is null, not free. Mark zero only for an explicit source-stated zero price. Absence of a listing/price is not evidence of no market.
- Feasibility facts describe documented platform/dependency/compatibility constraints only; they are not effort, cost, probability, or delivery estimates.

## Assessment contract

Emit exactly these seven source-grounded dimensions for each direction: `BUYER_PROBLEM`, `PURCHASE_TRIGGER`, `PAID_MARKET_WTP`, `DIFFERENTIATION`, `CHANNEL_FIT`, `IMPLEMENTATION_FEASIBILITY`, `SUPPORT_MAINTENANCE`. Each state is one of `SUPPORTED`, `MIXED`, `WEAK`, `INSUFFICIENT_EVIDENCE`, with a concise basis and same-direction evidence IDs. Use `INSUFFICIENT_EVIDENCE` when evidence is absent; do not infer demand or prevalence from isolated anecdotes. Coverage is explicit and may remain partial.

## Reproducibility and QA

Keep the raw research capture immutable and preserve query/source/evidence provenance. Normalize deterministically with stable IDs, sorted records, UTF-8 and LF JSONL/CSV. Preserve nulls as `\\N` in CSV. SQLite is rebuilt from normalized rows with foreign keys enabled. Validate exact cohort/query coverage, evidence references, separate WTP/paid-precedent/proxy tables, input SHA-256 before and after, SQLite integrity/foreign keys, CSV/JSONL/SQLite row equivalence, and byte-identical deterministic replay. Record execution commit, pinned input hashes, artifact sizes, and SHA-256 in the manifest. No network calls are part of normalization or replay.
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
    lines = [
        "# YEE-81 Deep Commercial Validation — Final Report", "",
        "Status: `DEEP_COMMERCIAL_VALIDATION_READY_FOR_SUPERVISOR_REVIEW`", "",
        f"Execution commit: `{execution_commit}`", f"Authorized direction rows: **{len(directions)}**",
        f"Captured queries: **{query_count}**; opened-source evidence rows: **{evidence_count}**.",
        f"Direct-WTP observations: **{direct}**; paid-market precedent rows: **{paid}**; other proxy rows: **{proxies}**.", "",
        "## Scope and interpretation", "",
        "Research is limited to the five YEE-79-authorized directions. YEE-79 and YEE-77 were read-only inputs; no YEE-30–YEE-59 outputs were used. Search snippets served only discovery. Findings below are source-bounded observations, not population estimates.",
        "Direct WTP is reported only where a buyer explicitly expressed purchase intent or an observed purchase. A competitor's listed price is a paid-market precedent, not direct WTP for the studied direction. Other proxies remain separately labelled.",
        "No TAM, revenue, profit, score, ranking, winner, product concept, recommendation, or implementation was produced.", "",
        "## Direction coverage", "",
        "| Direction | Commercial evidence coverage | Direct WTP | Paid-market precedent | Key unknowns |", "|---|---|---|---|---|",
    ]
    for row in directions:
        lines.append(f"| {row['direction_key']} | {row['coverage_status']} | {row['direct_wtp_summary']} | {row['paid_market_precedent_summary']} | {'; '.join(row['unknowns']) or 'None recorded'} |")
    source_by_id = {row["source_id"]: row for row in tables["commercial_source_documents"]}
    evidence_by_id = {row["evidence_id"]: row for row in tables["commercial_evidence"]}
    lines += ["", "## Dimension findings", ""]
    for direction in directions:
        lines += [f"### {direction['direction_key']}", "", f"Market job: {direction['market_job']}", "", "| Dimension | State | Evidence-based basis | Opened sources |", "|---|---|---|---|"]
        for assessment in direction["dimension_assessments"]:
            source_links = []
            for evidence_id in assessment.get("evidence_ids", []):
                evidence = evidence_by_id[evidence_id]
                source = source_by_id[evidence["source_id"]]
                link = f"[{source['title']}]({source['url']})"
                if link not in source_links:
                    source_links.append(link)
            lines.append(f"| {assessment['dimension']} | {assessment['state']} | {assessment['basis']} | {'; '.join(source_links) or 'None retained'} |")
        lines.append("")
    lines += ["## Evidence and QA", "", "See the source-document registry, chronological query log, evidence table, normalized observations, QA, manifest, and SQLite tables for exact row-level reconciliation. No source snippet is used as factual evidence.", ""]
    lines.extend(f"- `{key}` SHA-256: `{value}`" for key, value in sorted(input_hashes.items()))
    lines.append("")
    return "\n".join(lines)


def _check_exports(output: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    checks: dict[str, Any] = {"row_counts_reconcile": True, "csv_jsonl_reconcile": True, "sqlite_integrity_ok": False, "foreign_key_check_ok": False}
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
        expected_hashes: Mapping[str, str] | None = None) -> dict[str, Any]:
    pinned_hashes = dict(expected_hashes or EXPECTED_INPUT_HASHES)
    direction_ids = {row["direction_id"] for row in tables["direction_commercial_validation"]}
    query_coverage = {direction_id: set() for direction_id in direction_ids}
    for row in tables["commercial_research_queries"]:
        query_coverage[row["direction_id"]].add(row["purpose"])
    checks = {
        "authorized_cohort_exactly_five": direction_ids == set(AUTHORIZED_DIRECTIONS) and len(direction_ids) == 5,
        "input_sha_pins_match": dict(before) == pinned_hashes and dict(after) == pinned_hashes,
        "inputs_unchanged_after_read": dict(before) == dict(after),
        "query_purpose_coverage": all(purposes == set(QUERY_PURPOSES) for purposes in query_coverage.values()),
        "all_factual_evidence_has_opened_source": all(row["source_id"] in {s["source_id"] for s in tables["commercial_source_documents"]} for row in tables["commercial_evidence"]),
        "no_search_snippets_used_as_evidence": all(row.get("source_basis") not in {"SEARCH_SNIPPET", "SEARCH_RESULT_SNIPPET"} for row in capture.get("evidence", [])),
        "direct_wtp_separated_from_paid_precedent": all(row["evidence_ids"] for row in tables["direct_wtp_observations"]) and not any(row.get("wtp_class") == "INFERRED_FROM_COMPETITOR_PRICE" for row in tables["direct_wtp_observations"]),
        "pricing_unknown_not_free": all(not (row["amount"] is None and row.get("monetization_status") == "FREE_VERIFIED") for row in tables["pricing_observations"]),
        "seven_dimensions_per_direction": len(capture.get("directions", [])) == 5 and all(len(row["dimension_assessments"]) == len(DIMENSIONS) for row in capture.get("directions", [])),
        "no_score_rank_winner_or_recommendation": all(not any(re.search(r"\b(score|rank|winner|recommendation|shortlist|tam|revenue|profit)\b", key, re.I) for key in row) for row in tables["direction_commercial_validation"]),
        "sqlite_jsonl_csv_reconcile": bool(export_checks["row_counts_reconcile"] and export_checks["csv_jsonl_reconcile"] and export_checks["sqlite_integrity_ok"] and export_checks["foreign_key_check_ok"]),
        "deterministic_replay_byte_identical": replay_identical,
        "execution_commit_recorded": bool(re.fullmatch(r"[0-9a-f]{40}", execution_commit)),
    }
    return {"work_order": WORK_ORDER, "status": "PASS" if all(checks.values()) else "FAIL", "checks": checks,
            "details": {"authorized_direction_count": len(direction_ids), "query_count": len(tables["commercial_research_queries"]),
                        "opened_source_count": len(tables["commercial_source_documents"]), "evidence_count": len(tables["commercial_evidence"]),
                        "dimension_count": len(DIMENSIONS), "row_counts": export_checks["row_counts"],
                        "input_hashes_before": dict(before), "input_hashes_after": dict(after),
                        "execution_code_commit": execution_commit}}


def build_dataset(yee79_db: str | Path, yee77_db: str | Path, capture_path: str | Path,
                  output_dir: str | Path, execution_commit: str, *, run_replay: bool = True,
                  expected_hashes: Mapping[str, str] | None = None) -> dict[str, Any]:
    if not re.fullmatch(r"[0-9a-f]{40}", execution_commit):
        raise CommercialValidationError("execution_commit must be a full lowercase Git SHA")
    canonical_rows, before = load_inputs(yee79_db, yee77_db, expected_hashes)
    capture_path = Path(capture_path)
    capture = json.loads(capture_path.read_text(encoding="utf-8"))
    tables = normalize_capture(capture, canonical_rows, before)
    output = Path(output_dir)
    if output.exists() and any(output.iterdir()):
        raise CommercialValidationError(f"output directory must be empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    schema = _render_schema(before, execution_commit)
    (output / "COMMERCIAL_VALIDATION_SCHEMA.md").write_text(schema, encoding="utf-8", newline="\n")
    (output / "COMMERCIAL_VALIDATION_PROTOCOL.md").write_text(_render_protocol(), encoding="utf-8", newline="\n")
    (output / "GOAL_ALIGNMENT.md").write_text(_render_goal_alignment(before), encoding="utf-8", newline="\n")
    (output / "COMMERCIAL_RESEARCH_CAPTURE.json").write_bytes(capture_path.read_bytes())
    for table, columns in TABLES.items():
        _write_jsonl(output / f"{table}.jsonl", tables[table])
        _write_csv(output / f"{table}.csv", tables[table], columns)
    metadata = {
        "work_order": WORK_ORDER,
        "schema_version": CAPTURE_VERSION,
        "execution_code_commit": execution_commit,
        "preflight_code_commit": BASELINE_COMMIT,
        "input_hashes": _json(before),
        "capture_sha256": _sha256(capture_path),
        "authorized_direction_ids": _json(sorted(AUTHORIZED_DIRECTIONS)),
        "analysis_scope": "five_authorized_yEE79_directions_only",
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
            build_dataset(yee79_db, yee77_db, capture_path, replay_dir, execution_commit,
                          run_replay=False, expected_hashes=expected_hashes)
            excluded = {"QA_RESULT.json", "DATASET_MANIFEST.json"}
            names = sorted(path.name for path in output.iterdir() if path.is_file() and path.name not in excluded)
            replay_names = sorted(path.name for path in replay_dir.iterdir() if path.is_file() and path.name not in excluded)
            replay_identical = names == replay_names and all(
                (output / name).read_bytes() == (replay_dir / name).read_bytes() for name in names)
    qa = _qa(tables, capture, before, after, checks, replay_identical, execution_commit, expected_hashes)
    (output / "QA_RESULT.json").write_text(_json(qa) + "\n", encoding="utf-8", newline="\n")
    manifest = {"work_order": WORK_ORDER, "status": qa["status"], "execution_code_commit": execution_commit,
                "preflight_code_commit": BASELINE_COMMIT, "input_hashes": dict(before),
                "capture_sha256": _sha256(capture_path), "files": []}
    for path in sorted((item for item in output.iterdir() if item.is_file() and item.name != "DATASET_MANIFEST.json"), key=lambda p: p.name):
        manifest["files"].append({"path": path.name, "size_bytes": path.stat().st_size, "sha256": _sha256(path)})
    (output / "DATASET_MANIFEST.json").write_text(_json(manifest) + "\n", encoding="utf-8", newline="\n")
    return {"status": qa["status"], "output_dir": output, "qa": qa, "manifest": manifest}
