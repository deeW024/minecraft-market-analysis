"""Deterministic YEE-54 synthesis over accepted YEE-46/YEE-47 artifacts."""

from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

WORK_ORDER = "YEE-54"
SCHEMA_VERSION = "yee-54-evidence-gated-opportunity-synthesis-v0.1"
BASELINE_COMMIT = "d2b80defd6d3e2e7e34de3491575dcf1cc61060e"
INPUT_SHA256 = {
    "yee46_shortlist": "b9a9b41b054fa761945098054cffb52c1ab42d0779f4cfb1e459be824a119a0f",
    "yee46_analysis_db": "770a70d03af05f651cb9e7282a21a6d8961dadaa547d017c3e144bffe4791156",
    "yee47_research_db": "cce8b15c06b15a1094864ddd40653b9af7b185414a1207c25663f0f5dd55810f",
    "yee47_family_packs": "6ee34c401f2366bcfc7230a24753a974b1844344e7dd5f87a24ee61bc695d050",
    "yee47_evidence": "d147591c38d0eaef186aadf274c7352b183b9cb4355ab9d3e15bd9b9cf2ee859",
    "yee47_competitors": "e7cccf77fdab537fe00366303d0ccc2ce6730a884aecc460d7831b724231d218",
    "yee47_queries": "705e4ab3e58f402be4049d67d5f8abb63ebab8c7881758f541a3f733217875f9",
}
RESOLVED_RANKS = (
    1, 3, 4, 7, 8, 9, 16, 18, 19, 22, 27, 28, 31, 32, 34, 37, 42, 44, 49, 50,
    51, 53, 56, 58, 60, 61, 62, 65, 66, 70, 73, 80, 81, 83, 85, 89, 90, 93, 94,
    95, 98,
)
COHORT_A_RANKS = (1, 3, 4, 7, 8, 9, 16, 18, 19, 22, 27, 28, 31, 32, 34)
STATE_MAP = {
    "RESOLVED": "DEEP_VALIDATE",
    "AMBIGUOUS": "NEEDS_DISAMBIGUATION",
    "UNRESOLVED": "STOP_UNRESOLVED",
}
COMMON_REQUIREMENTS = (
    "BUYER_JOB", "PAIN_PREVALENCE", "PAID_ALTERNATIVES", "DIFFERENTIATION",
    "FEASIBILITY_SUPPORT",
)
GAP_REQUIREMENTS = (
    ("NO_PAIN_POINT_EVIDENCE", "PAIN_POINT_RESEARCH"),
    ("NO_PRICING_EVIDENCE", "PRICING_MONETIZATION_RESEARCH"),
    ("NO_CURRENT_COMPETITOR_ENTITY", "COMPETITOR_EXPANSION"),
    ("NO_DIRECT_COMPETITOR", "DIRECT_COMPETITOR_CHECK"),
    ("NO_MAINTENANCE_EVIDENCE", "LIFECYCLE_VALIDATION"),
    ("NO_POPULARITY_PROXY", "POPULARITY_VALIDATION"),
    ("SINGLE_SOURCE_DOMAIN", "SECOND_SOURCE_VALIDATION"),
)
GAP_FLAG_ORDER = tuple(flag for flag, _ in GAP_REQUIREMENTS) + ("COVERAGE_PARTIAL",)
DERIVED_COLUMNS = (
    "research_status", "research_coverage_status", "synthesis_state",
    "evidence_count_by_claim_type", "evidence_count_by_source_type",
    "distinct_source_domain_count", "current_competitor_entity_count",
    "competitor_count_by_relation_type", "research_query_count", "evidence_gap_flags",
    "deep_validation_order", "validation_cohort", "next_validation_requirements",
)
TABLES = ("opportunity_synthesis", "deep_validation_queue", "next_validation_cohort")
NULL_TOKEN = r"\N"


class SynthesisInputError(ValueError):
    """Canonical input or synthesis contract failed validation."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        raise SynthesisInputError(f"cannot read JSONL input: {path}") from exc
    if any(not isinstance(row, dict) for row in rows):
        raise SynthesisInputError(f"JSONL rows must be objects: {path}")
    return rows


def _readonly_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _decode_db_value(value: Any, expected: Any) -> Any:
    if isinstance(expected, (list, dict)) and isinstance(value, str):
        return json.loads(value)
    if isinstance(expected, bool) and value is not None:
        return bool(value)
    return value


def _assert_db_mirror(
    path: Path,
    table: str,
    source_rows: Sequence[Mapping[str, Any]],
    key: str,
    *,
    top_100: bool = False,
) -> None:
    connection = _readonly_connection(path)
    try:
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise SynthesisInputError(f"{path.name} integrity_check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise SynthesisInputError(f"{path.name} foreign_key_check failed")
        if top_100:
            db_rows = [dict(row) for row in connection.execute(
                f'SELECT * FROM "{table}" WHERE consensus_rank BETWEEN 1 AND 100 ORDER BY consensus_rank'
            )]
        else:
            db_rows = [dict(row) for row in connection.execute(f'SELECT * FROM "{table}"')]
        by_key = {row.get(key): row for row in source_rows}
        db_by_key = {row.get(key): row for row in db_rows}
        if len(by_key) != len(source_rows) or set(by_key) != set(db_by_key):
            raise SynthesisInputError(f"{path.name}:{table} identities/count do not reconcile with JSONL")
        shared = set(db_rows[0]) & set(source_rows[0]) if db_rows and source_rows else set()
        if not {key, "consensus_rank"}.issubset(shared):
            raise SynthesisInputError(f"{path.name}:{table} is missing identity columns")
        for identity, source in by_key.items():
            db_row = db_by_key[identity]
            for column in shared:
                if _decode_db_value(db_row[column], source.get(column)) != source.get(column):
                    raise SynthesisInputError(f"{path.name}:{table} differs from JSONL at {identity}.{column}")
    finally:
        connection.close()


def _hash_inputs(paths: Mapping[str, Path], expected: Mapping[str, str] | None = None) -> dict[str, str]:
    hashes = {name: _sha256(path) for name, path in sorted(paths.items())}
    if expected is not None:
        mismatches = {name: (hashes.get(name), expected_hash) for name, expected_hash in expected.items() if hashes.get(name) != expected_hash}
        if mismatches:
            raise SynthesisInputError(f"canonical SHA-256 mismatch: {mismatches}")
    return hashes


def _validate_upstream_fields(
    canonical: Sequence[Mapping[str, Any]], packs: Sequence[Mapping[str, Any]],
) -> tuple[list[str], dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]:
    if len(canonical) != 100 or [row.get("consensus_rank") for row in canonical] != list(range(1, 101)):
        raise SynthesisInputError("YEE-46 universe must be exactly ordered consensus ranks 1..100")
    if any(row.get("triage_bucket") != "ADVANCE_RESEARCH" for row in canonical):
        raise SynthesisInputError("YEE-46 input contains a family outside ADVANCE_RESEARCH")
    columns = sorted(canonical[0])
    if any(sorted(row) != columns for row in canonical):
        raise SynthesisInputError("YEE-46 shortlist rows have inconsistent schemas")
    canonical_by_id = {row["family_id"]: row for row in canonical}
    pack_by_id = {row.get("family_id"): row for row in packs}
    if len(canonical_by_id) != 100 or len(pack_by_id) != len(packs) or set(canonical_by_id) != set(pack_by_id):
        raise SynthesisInputError("YEE-46 and YEE-47 family identities do not reconcile 1:1")
    for family_id, source in canonical_by_id.items():
        pack = pack_by_id[family_id]
        if pack.get("consensus_rank") != source["consensus_rank"]:
            raise SynthesisInputError(f"consensus-rank identity mismatch for {family_id}")
        for column in columns:
            if column not in pack or pack[column] != source[column]:
                raise SynthesisInputError(f"YEE-47 copied YEE-46 field mismatch at {family_id}.{column}")
        if pack.get("research_status") not in STATE_MAP:
            raise SynthesisInputError(f"invalid research_status for {family_id}")
        if not pack.get("research_coverage_status"):
            raise SynthesisInputError(f"missing accepted research_coverage_status for {family_id}")
    if Counter(pack["research_status"] for pack in packs) != Counter(
        {"RESOLVED": 41, "AMBIGUOUS": 48, "UNRESOLVED": 11}
    ):
        raise SynthesisInputError("YEE-47 research_status counts must be 41/48/11")
    return columns, canonical_by_id, pack_by_id


def _validate_child_rows(
    rows: Sequence[Mapping[str, Any]], canonical_by_id: Mapping[str, Mapping[str, Any]], table: str,
) -> None:
    for row in rows:
        family_id = row.get("family_id")
        source = canonical_by_id.get(family_id)
        if source is None:
            raise SynthesisInputError(f"{table} has unknown family_id: {family_id}")
        if row.get("consensus_rank") != source["consensus_rank"]:
            raise SynthesisInputError(f"{table} rank identity mismatch for {family_id}")


def _derive_one(
    source: Mapping[str, Any],
    pack: Mapping[str, Any],
    evidence: Sequence[Mapping[str, Any]],
    competitors: Sequence[Mapping[str, Any]],
    queries: Sequence[Mapping[str, Any]],
    deep_order: int | None,
) -> dict[str, Any]:
    claim_counts = dict(sorted(Counter(row["claim_type"] for row in evidence).items()))
    source_counts = dict(sorted(Counter(row["source_type"] for row in evidence).items()))
    relation_counts = dict(sorted(Counter(row["relation_type"] for row in competitors).items()))
    domains = {row.get("source_domain") for row in evidence if row.get("source_domain")}
    coverage = pack["research_coverage_status"]
    gaps = []
    for flag, missing in (
        ("NO_PAIN_POINT_EVIDENCE", claim_counts.get("PAIN_POINT", 0) == 0),
        ("NO_PRICING_EVIDENCE", claim_counts.get("PRICING", 0) == 0),
        ("NO_CURRENT_COMPETITOR_ENTITY", len(competitors) == 0),
        ("NO_DIRECT_COMPETITOR", relation_counts.get("DIRECT", 0) == 0),
        ("NO_MAINTENANCE_EVIDENCE", claim_counts.get("MAINTENANCE", 0) == 0),
        ("NO_POPULARITY_PROXY", claim_counts.get("POPULARITY_PROXY", 0) == 0),
        ("SINGLE_SOURCE_DOMAIN", len(domains) < 2),
        ("COVERAGE_PARTIAL", coverage == "PARTIAL"),
    ):
        if missing:
            gaps.append(flag)
    status = pack["research_status"]
    state = STATE_MAP[status]
    if deep_order is None:
        cohort = None
        requirements: list[str] = []
    else:
        cohort = "COHORT_A" if deep_order <= 15 else "COHORT_B" if deep_order <= 30 else "COHORT_C"
        requirements = list(COMMON_REQUIREMENTS)
        requirements.extend(task for flag, task in GAP_REQUIREMENTS if flag in gaps)
    result = dict(source)
    result.update({
        "research_status": status,
        "research_coverage_status": coverage,
        "synthesis_state": state,
        "evidence_count_by_claim_type": claim_counts,
        "evidence_count_by_source_type": source_counts,
        "distinct_source_domain_count": len(domains),
        "current_competitor_entity_count": len(competitors),
        "competitor_count_by_relation_type": relation_counts,
        "research_query_count": len(queries),
        "evidence_gap_flags": gaps,
        "deep_validation_order": deep_order,
        "validation_cohort": cohort,
        "next_validation_requirements": requirements,
    })
    return result


def derive_synthesis(
    canonical: Sequence[Mapping[str, Any]],
    packs: Sequence[Mapping[str, Any]],
    evidence: Sequence[Mapping[str, Any]],
    competitors: Sequence[Mapping[str, Any]],
    queries: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    source_columns, canonical_by_id, pack_by_id = _validate_upstream_fields(canonical, packs)
    for name, rows in (("external_evidence", evidence), ("competitor_entities", competitors), ("research_queries", queries)):
        _validate_child_rows(rows, canonical_by_id, name)
    by_family: dict[str, dict[str, list[Mapping[str, Any]]]] = defaultdict(lambda: {"evidence": [], "competitors": [], "queries": []})
    seen_ids: dict[str, set[Any]] = {"external_evidence": set(), "competitor_entities": set(), "research_queries": set()}
    child_key = {"external_evidence": "evidence_id", "competitor_entities": "competitor_id", "research_queries": "query_id"}
    for table, rows, bucket in (
        ("external_evidence", evidence, "evidence"),
        ("competitor_entities", competitors, "competitors"),
        ("research_queries", queries, "queries"),
    ):
        for row in rows:
            identity = row.get(child_key[table])
            if not identity or identity in seen_ids[table]:
                raise SynthesisInputError(f"{table} contains missing/duplicate row identity: {identity}")
            seen_ids[table].add(identity)
            by_family[row["family_id"]][bucket].append(row)
    resolved_ranks = [row["consensus_rank"] for row in packs if row["research_status"] == "RESOLVED"]
    if tuple(sorted(resolved_ranks)) != RESOLVED_RANKS:
        raise SynthesisInputError("YEE-47 RESOLVED rank sequence differs from the accepted synthesis contract")
    ordered_resolved = sorted(
        (row for row in canonical if pack_by_id[row["family_id"]]["research_status"] == "RESOLVED"),
        key=lambda row: row["consensus_rank"],
    )
    order_by_id = {row["family_id"]: index for index, row in enumerate(ordered_resolved, 1)}
    output = []
    for source in canonical:
        family_rows = by_family[source["family_id"]]
        output.append(_derive_one(
            source, pack_by_id[source["family_id"]], family_rows["evidence"],
            family_rows["competitors"], family_rows["queries"], order_by_id.get(source["family_id"]),
        ))
    columns = source_columns + list(DERIVED_COLUMNS)
    if len(set(columns)) != len(columns):
        raise SynthesisInputError("YEE-46 fields collide with YEE-54 derived field names")
    return output, columns


def _validate_derived_rows(
    rows: Sequence[Mapping[str, Any]], canonical: Sequence[Mapping[str, Any]], columns: Sequence[str],
) -> dict[str, Any]:
    if len(rows) != 100 or [row["family_id"] for row in rows] != [row["family_id"] for row in canonical]:
        raise SynthesisInputError("synthesis output is not the exact YEE-46 identity/rank universe")
    for actual, source in zip(rows, canonical):
        for column in source:
            if actual.get(column) != source[column]:
                raise SynthesisInputError(f"preserved YEE-46 field changed: {source['family_id']}.{column}")
        if actual["consensus_rank"] != source["consensus_rank"]:
            raise SynthesisInputError("consensus_rank changed")
        if actual["synthesis_state"] != STATE_MAP[actual["research_status"]]:
            raise SynthesisInputError("synthesis state does not map from immutable research_status")
        if actual["synthesis_state"] != "DEEP_VALIDATE" and any((
            actual["deep_validation_order"] is not None,
            actual["validation_cohort"] is not None,
            actual["next_validation_requirements"],
        )):
            raise SynthesisInputError("non-resolved family received deep-validation work")
    expected_derived = {name for name in DERIVED_COLUMNS}
    actual_derived = set(columns) - set(canonical[0])
    if actual_derived != expected_derived:
        raise SynthesisInputError("unexpected derived columns (possible score/rank mutation)")
    counts = Counter(row["synthesis_state"] for row in rows)
    if counts != Counter({"DEEP_VALIDATE": 41, "NEEDS_DISAMBIGUATION": 48, "STOP_UNRESOLVED": 11}):
        raise SynthesisInputError(f"unexpected synthesis-state counts: {dict(counts)}")
    queue = sorted((row for row in rows if row["synthesis_state"] == "DEEP_VALIDATE"), key=lambda row: row["deep_validation_order"])
    if [row["consensus_rank"] for row in queue] != list(RESOLVED_RANKS):
        raise SynthesisInputError("deep-validation queue does not match the fixed accepted rank sequence")
    if [row["deep_validation_order"] for row in queue] != list(range(1, 42)):
        raise SynthesisInputError("deep_validation_order must be unique and contiguous 1..41")
    cohort = [row for row in queue if row["validation_cohort"] == "COHORT_A"]
    if [row["consensus_rank"] for row in cohort] != list(COHORT_A_RANKS):
        raise SynthesisInputError("COHORT_A does not contain the exact accepted first 15 families")
    forbidden = {"opportunity_score", "new_rank", "recommendation", "winner", "market_exists"}
    if forbidden & actual_derived:
        raise SynthesisInputError("forbidden market-ranking/conclusion field introduced")
    claim_totals = _sum_nested_counts(rows, "evidence_count_by_claim_type")
    source_totals = _sum_nested_counts(rows, "evidence_count_by_source_type")
    relation_totals = _sum_nested_counts(rows, "competitor_count_by_relation_type")
    return {
        "state_counts": {key: counts[key] for key in ("DEEP_VALIDATE", "NEEDS_DISAMBIGUATION", "STOP_UNRESOLVED")},
        "deep_validation_ranks": [row["consensus_rank"] for row in queue],
        "cohort_a_ranks": [row["consensus_rank"] for row in cohort],
        "gap_flag_counts": dict(sorted(Counter(flag for row in rows for flag in row["evidence_gap_flags"]).items())),
        "evidence_claim_counts": dict(sorted(claim_totals.items())),
        "evidence_source_type_counts": dict(sorted(source_totals.items())),
        "competitor_relation_counts": dict(sorted(relation_totals.items())),
        "evidence_row_count": sum(claim_totals.values()),
        "competitor_entity_row_count": sum(row["current_competitor_entity_count"] for row in rows),
        "research_query_row_count": sum(row["research_query_count"] for row in rows),
    }


def _sum_nested_counts(rows: Sequence[Mapping[str, Any]], column: str) -> Counter[str]:
    result: Counter[str] = Counter()
    for row in rows:
        result.update(row[column])
    return result


def _load_inputs(paths: Mapping[str, Path]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    before = _hash_inputs(paths, INPUT_SHA256)
    canonical = _read_jsonl(paths["yee46_shortlist"])
    packs = _read_jsonl(paths["yee47_family_packs"])
    evidence = _read_jsonl(paths["yee47_evidence"])
    competitors = _read_jsonl(paths["yee47_competitors"])
    queries = _read_jsonl(paths["yee47_queries"])
    _validate_upstream_fields(canonical, packs)
    _assert_db_mirror(paths["yee46_analysis_db"], "family_opportunity_scores", canonical, "family_id", top_100=True)
    _assert_db_mirror(paths["yee47_research_db"], "family_research_packs", packs, "family_id")
    _assert_db_mirror(paths["yee47_research_db"], "external_evidence", evidence, "evidence_id")
    _assert_db_mirror(paths["yee47_research_db"], "competitor_entities", competitors, "competitor_id")
    _assert_db_mirror(paths["yee47_research_db"], "research_queries", queries, "query_id")
    after = _hash_inputs(paths, INPUT_SHA256)
    if before != after:
        raise SynthesisInputError("canonical inputs changed during read-only preflight")
    data = {
        "canonical": canonical, "packs": packs, "evidence": evidence,
        "competitors": competitors, "queries": queries,
    }
    return data, {"hashes": before, "yee46_sqlite_integrity_ok": True, "yee47_sqlite_integrity_ok": True}


def _column_type(rows: Sequence[Mapping[str, Any]], column: str) -> str:
    values = [row.get(column) for row in rows if row.get(column) is not None]
    if any(isinstance(value, (list, dict)) for value in values):
        return "TEXT"
    if values and all(isinstance(value, bool) for value in values):
        return "INTEGER"
    if values and all(isinstance(value, (bool, int)) for value in values):
        return "INTEGER"
    if any(isinstance(value, float) for value in values):
        return "REAL"
    return "TEXT"


def _stored_value(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return _canonical_json(value)
    if isinstance(value, bool):
        return int(value)
    return value


def _csv_value(value: Any) -> Any:
    if value is None:
        return NULL_TOKEN
    if isinstance(value, (list, dict)):
        return _canonical_json(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _render_schema(columns: Sequence[str], upstream_columns: Sequence[str]) -> str:
    path = Path(__file__).resolve().parents[2] / "SYNTHESIS_SCHEMA.md"
    base = path.read_text(encoding="utf-8").rstrip()
    lines = [base, "", "## Exact export columns", "", "The same ordered columns are used by all three row-level tables and CSV exports.", "", "### Preserved YEE-46 columns", ""]
    lines.extend(f"- `{column}`" for column in upstream_columns)
    lines.extend(("", "### YEE-54 derived columns", ""))
    lines.extend(f"- `{column}`" for column in DERIVED_COLUMNS)
    lines.extend(("", "### Complete ordered column list", "", "```text", ",".join(columns), "```", ""))
    return "\n".join(lines)


def _write_database(path: Path, rows: Mapping[str, Sequence[Mapping[str, Any]]], columns: Sequence[str], metadata: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        definitions = ",".join(f'"{column}" {_column_type(rows["opportunity_synthesis"], column)}' for column in columns)
        connection.execute(f"CREATE TABLE opportunity_synthesis ({definitions},PRIMARY KEY(family_id),UNIQUE(consensus_rank))")
        connection.execute(f"CREATE TABLE deep_validation_queue ({definitions},PRIMARY KEY(family_id),UNIQUE(consensus_rank),FOREIGN KEY(family_id) REFERENCES opportunity_synthesis(family_id))")
        connection.execute(f"CREATE TABLE next_validation_cohort ({definitions},PRIMARY KEY(family_id),UNIQUE(consensus_rank),FOREIGN KEY(family_id) REFERENCES opportunity_synthesis(family_id))")
        connection.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY,value_json TEXT NOT NULL)")
        connection.execute("CREATE INDEX opportunity_synthesis_state_rank_idx ON opportunity_synthesis(synthesis_state,consensus_rank)")
        connection.execute("CREATE INDEX deep_validation_order_idx ON deep_validation_queue(deep_validation_order)")
        placeholders = ",".join("?" for _ in columns)
        names = ",".join(f'"{column}"' for column in columns)
        for table in TABLES:
            connection.executemany(
                f'INSERT INTO "{table}" ({names}) VALUES ({placeholders})',
                ([_stored_value(row.get(column)) for column in columns] for row in rows[table]),
            )
        connection.executemany(
            "INSERT INTO metadata(key,value_json) VALUES (?,?)",
            [(key, _canonical_json(value)) for key, value in sorted(metadata.items())],
        )
        connection.execute("PRAGMA user_version=1")
        connection.commit()
        connection.execute("VACUUM")
    finally:
        connection.close()


def _write_core(output: Path, rows: Mapping[str, Sequence[Mapping[str, Any]]], columns: Sequence[str], upstream_columns: Sequence[str], metadata: Mapping[str, Any]) -> list[Path]:
    output.mkdir(parents=True, exist_ok=True)
    database = output / "opportunity_synthesis.sqlite"
    _write_database(database, rows, columns, metadata)
    paths = [database]
    for table in TABLES:
        jsonl = output / f"{table}.jsonl"
        with jsonl.open("w", encoding="utf-8", newline="\n") as handle:
            for row in rows[table]:
                handle.write(_canonical_json({column: row.get(column) for column in columns}) + "\n")
        csv_path = output / f"{table}.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(columns)
            writer.writerows([_csv_value(row.get(column)) for column in columns] for row in rows[table])
        paths.extend((jsonl, csv_path))
    schema = output / "SYNTHESIS_SCHEMA.md"
    schema.write_text(_render_schema(columns, upstream_columns), encoding="utf-8", newline="\n")
    paths.append(schema)
    return paths


def _check_outputs(output: Path, rows: Mapping[str, Sequence[Mapping[str, Any]]], columns: Sequence[str]) -> dict[str, Any]:
    connection = sqlite3.connect(output / "opportunity_synthesis.sqlite")
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign = connection.execute("PRAGMA foreign_key_check").fetchall()
        counts = {table: connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] for table in TABLES}
        columns_match = all(
            [row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')] == list(columns)
            for table in TABLES
        )
    finally:
        connection.close()
    jsonl_match = True
    csv_match = True
    sqlite_rows_match = True
    for table in TABLES:
        jsonl_path = output / f"{table}.jsonl"
        data = _read_jsonl(jsonl_path)
        jsonl_match &= data == [{column: row.get(column) for column in columns} for row in rows[table]]
        with (output / f"{table}.csv").open(newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            header = next(reader, [])
            body = list(reader)
        expected_body = [[_csv_value(row.get(column)) for column in columns] for row in rows[table]]
        csv_match &= header == list(columns) and body == expected_body
        connection = sqlite3.connect(output / "opportunity_synthesis.sqlite")
        try:
            connection.row_factory = sqlite3.Row
            db_rows = [dict(row) for row in connection.execute(f'SELECT * FROM "{table}"')]
        finally:
            connection.close()
        expected_by_id = {row["family_id"]: row for row in rows[table]}
        db_by_id = {row["family_id"]: row for row in db_rows}
        sqlite_rows_match &= set(expected_by_id) == set(db_by_id)
        if sqlite_rows_match:
            for family_id, expected in expected_by_id.items():
                db_row = db_by_id[family_id]
                if any(_decode_db_value(db_row[column], expected.get(column)) != expected.get(column) for column in columns):
                    sqlite_rows_match = False
                    break
    schema_text = (output / "SYNTHESIS_SCHEMA.md").read_text(encoding="utf-8")
    schema_match = f"```text\n{','.join(columns)}\n```" in schema_text
    db_count_match = counts == {table: len(rows[table]) for table in TABLES}
    return {
        "sqlite_integrity_ok": integrity == "ok",
        "foreign_key_check_ok": not foreign,
        "sqlite_columns_match": columns_match,
        "sqlite_row_counts": counts,
        "sqlite_counts_match": db_count_match,
        "sqlite_rows_match": sqlite_rows_match,
        "jsonl_counts_and_rows_match": jsonl_match,
        "csv_counts_schema_and_rows_match": csv_match,
        "schema_columns_match": schema_match,
    }


def _manifest(output: Path, inputs: Mapping[str, str], counts: Mapping[str, int]) -> dict[str, Any]:
    paths = sorted(path for path in output.iterdir() if path.is_file() and path.name != "DATASET_MANIFEST.json")
    return {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "baseline_commit": BASELINE_COMMIT,
        "status": "PASS",
        "inputs_sha256": dict(sorted(inputs.items())),
        "row_counts": dict(sorted(counts.items())),
        "artifacts": [
            {"path": path.name, "bytes": path.stat().st_size, "sha256": _sha256(path)}
            for path in paths
        ],
        "artifact_hash_note": "This manifest lists every deliverable except itself to avoid recursive hashing.",
    }


def _report(qa: Mapping[str, Any]) -> str:
    details = qa["details"]
    return "\n".join((
        "# YEE-54 Evidence-Gated Opportunity Synthesis v0",
        "",
        f"Status: **{qa['status']}**",
        "",
        "## Scope and canonical inputs",
        "",
        f"- Baseline: `{BASELINE_COMMIT}`.",
        "- 100 accepted YEE-46 ADVANCE_RESEARCH families; all ranks, scores, and upstream fields are preserved unchanged.",
        "- YEE-47 research_status and research_coverage_status are copied without reinterpretation; evidence inventories use row-level accepted data.",
        f"- Input hashes match before/after: `{qa['checks']['canonical_inputs_match_before_and_after']}`.",
        "- No live research/API, LLM/JEV, new score/ranking, revenue/TAM, winner, or implementation recommendation was produced.",
        "",
        "## Synthesis and queue",
        "",
        f"- State counts: `{_canonical_json(details['state_counts'])}`.",
        f"- DEEP_VALIDATE ranks in inherited order: `{','.join(map(str, details['deep_validation_ranks']))}`.",
        f"- COHORT_A ranks: `{','.join(map(str, details['cohort_a_ranks']))}`.",
        f"- Gap-flag counts: `{_canonical_json(details['gap_flag_counts'])}`.",
        f"- Row-level evidence claims/sources: `{_canonical_json(details['evidence_claim_counts'])}` / `{_canonical_json(details['evidence_source_type_counts'])}` ({details['evidence_row_count']} evidence rows).",
        f"- Competitor relation counts: `{_canonical_json(details['competitor_relation_counts'])}`; current entities: {details['competitor_entity_row_count']}; queries: {details['research_query_row_count']}.",
        "- Missing evidence is **UNKNOWN / NOT CAPTURED IN YEE-47**, never evidence of market or competitor absence.",
        "- Validation requirements are research tasks, not conclusions.",
        "",
        "## QA",
        "",
        f"- All acceptance checks pass: `{all(qa['checks'].values())}`.",
        f"- SQLite integrity / foreign keys: `{qa['checks']['sqlite_integrity_check']}` / `{qa['checks']['sqlite_foreign_key_check']}`.",
        f"- JSONL/CSV/SQLite/schema row reconciliation: `{qa['checks']['export_schema_and_counts_reconcile']}`.",
        f"- Deterministic replay byte-identical: `{qa['checks']['deterministic_replay_byte_identical']}`.",
        "",
        "## Handoff",
        "",
        "`SYNTHESIS_READY_FOR_SUPERVISOR_REVIEW` — no deep live validation or subsequent work order was started.",
        "",
    ))


def _write_json(path: Path, value: Any) -> None:
    path.write_text(_canonical_json(value) + "\n", encoding="utf-8", newline="\n")


def build_synthesis(paths: Mapping[str, str | Path], output_dir: str | Path) -> dict[str, Any]:
    input_paths = {key: Path(value).resolve() for key, value in paths.items()}
    if set(input_paths) != set(INPUT_SHA256):
        raise SynthesisInputError(f"exactly these input paths are required: {sorted(INPUT_SHA256)}")
    output = Path(output_dir).resolve()
    if output.exists() and any(output.iterdir()):
        raise SynthesisInputError(f"output directory must be new or empty: {output}")
    data, input_check = _load_inputs(input_paths)
    synthesis, columns = derive_synthesis(
        data["canonical"], data["packs"], data["evidence"], data["competitors"], data["queries"],
    )
    queue = sorted((row for row in synthesis if row["synthesis_state"] == "DEEP_VALIDATE"), key=lambda row: row["deep_validation_order"])
    cohort = [row for row in queue if row["validation_cohort"] == "COHORT_A"]
    rows = {"opportunity_synthesis": synthesis, "deep_validation_queue": queue, "next_validation_cohort": cohort}
    details = _validate_derived_rows(synthesis, data["canonical"], columns)
    upstream_columns = sorted(data["canonical"][0])
    metadata = {
        "work_order": WORK_ORDER, "schema_version": SCHEMA_VERSION, "baseline_commit": BASELINE_COMMIT,
        "input_sha256": dict(sorted(input_check["hashes"].items())),
        "row_counts": {table: len(rows[table]) for table in TABLES},
    }
    output.mkdir(parents=True, exist_ok=True)
    core_paths = _write_core(output, rows, columns, upstream_columns, metadata)
    output_check = _check_outputs(output, rows, columns)
    replay_identical = False
    with tempfile.TemporaryDirectory(prefix="yee54-replay-") as replay_directory:
        replay_root = Path(replay_directory)
        replay_data, replay_input_check = _load_inputs(input_paths)
        replay_synthesis, replay_columns = derive_synthesis(
            replay_data["canonical"], replay_data["packs"], replay_data["evidence"],
            replay_data["competitors"], replay_data["queries"],
        )
        replay_queue = sorted((row for row in replay_synthesis if row["synthesis_state"] == "DEEP_VALIDATE"), key=lambda row: row["deep_validation_order"])
        replay_rows = {
            "opportunity_synthesis": replay_synthesis,
            "deep_validation_queue": replay_queue,
            "next_validation_cohort": [row for row in replay_queue if row["validation_cohort"] == "COHORT_A"],
        }
        _write_core(replay_root, replay_rows, replay_columns, sorted(replay_data["canonical"][0]), metadata)
        replay_check = _check_outputs(replay_root, replay_rows, replay_columns)
        replay_identical = (
            replay_input_check["hashes"] == input_check["hashes"]
            and replay_rows == rows and replay_columns == columns and replay_check == output_check
            and {path.name: _sha256(path) for path in core_paths}
            == {path.name: _sha256(replay_root / path.name) for path in core_paths}
        )
        if replay_identical:
            counts = {table: len(rows[table]) for table in TABLES}
            checks = {
                "canonical_inputs_match_before_and_after": input_check["hashes"] == _hash_inputs(input_paths, INPUT_SHA256),
                "exact_100_yee46_identities_and_ranks": len(synthesis) == 100 and [row["consensus_rank"] for row in synthesis] == list(range(1, 101)),
                "all_yee46_fields_preserved": all(all(row.get(column) == source.get(column) for column in upstream_columns) for row, source in zip(synthesis, data["canonical"])),
                "research_status_and_coverage_preserved": all(
                    row["research_status"] == pack["research_status"] and row["research_coverage_status"] == pack["research_coverage_status"]
                    for row in synthesis for pack in data["packs"] if row["family_id"] == pack["family_id"]
                ),
                "state_counts_41_48_11": details["state_counts"] == {"DEEP_VALIDATE": 41, "NEEDS_DISAMBIGUATION": 48, "STOP_UNRESOLVED": 11},
                "row_level_inventory_counts_reconcile": (
                    details["evidence_row_count"] == len(data["evidence"])
                    and details["competitor_entity_row_count"] == len(data["competitors"])
                    and details["research_query_row_count"] == len(data["queries"])
                    and sum(details["evidence_source_type_counts"].values()) == len(data["evidence"])
                ),
                "exact_resolved_queue_and_order": details["deep_validation_ranks"] == list(RESOLVED_RANKS),
                "exact_cohort_a": details["cohort_a_ranks"] == list(COHORT_A_RANKS),
                "no_added_score_or_rank": set(columns) - set(upstream_columns) == set(DERIVED_COLUMNS),
                "unknown_semantics_explicit": True,
                "sqlite_integrity_check": output_check["sqlite_integrity_ok"],
                "sqlite_foreign_key_check": output_check["foreign_key_check_ok"],
                "export_schema_and_counts_reconcile": output_check["sqlite_columns_match"] and output_check["sqlite_counts_match"] and output_check["sqlite_rows_match"] and output_check["jsonl_counts_and_rows_match"] and output_check["csv_counts_schema_and_rows_match"] and output_check["schema_columns_match"],
                "deterministic_replay_byte_identical": replay_identical,
            }
            checks["canonical_inputs_match_before_and_after"] &= input_check["hashes"] == replay_input_check["hashes"]
            status = "PASS" if all(checks.values()) else "FAIL"
            qa = {
                "work_order": WORK_ORDER, "schema_version": SCHEMA_VERSION, "status": status,
                "inputs": {"sha256_before": input_check["hashes"], "sha256_after": _hash_inputs(input_paths, INPUT_SHA256)},
                "checks": checks,
                "details": {**details, "row_counts": counts, "output_checks": output_check,
                            "missing_evidence_semantics": "UNKNOWN / NOT CAPTURED IN YEE-47"},
            }
            _write_json(output / "QA_RESULT.json", qa)
            (output / "FINAL_REPORT.md").write_text(_report(qa), encoding="utf-8", newline="\n")
            _write_json(replay_root / "QA_RESULT.json", qa)
            (replay_root / "FINAL_REPORT.md").write_text(_report(qa), encoding="utf-8", newline="\n")
            report_qa_identical = all(
                _sha256(output / name) == _sha256(replay_root / name)
                for name in ("QA_RESULT.json", "FINAL_REPORT.md")
            )
            qa["checks"]["deterministic_replay_byte_identical"] &= report_qa_identical
            qa["status"] = "PASS" if all(qa["checks"].values()) else "FAIL"
            _write_json(output / "QA_RESULT.json", qa)
            _write_json(replay_root / "QA_RESULT.json", qa)
            if qa["status"] != "PASS":
                raise SynthesisInputError(f"YEE-54 QA failed: {qa['checks']}")
            final_hashes = _hash_inputs(input_paths, INPUT_SHA256)
            if final_hashes != input_check["hashes"]:
                raise SynthesisInputError("canonical inputs changed before final manifest")
            manifest = _manifest(output, final_hashes, counts)
            _write_json(output / "DATASET_MANIFEST.json", manifest)
            if final_hashes != _hash_inputs(input_paths, INPUT_SHA256):
                raise SynthesisInputError("canonical inputs changed after manifest generation")
            return {"status": qa["status"], "output_dir": output, "qa": qa, "manifest": manifest}
        raise SynthesisInputError("deterministic replay was not byte-identical")
