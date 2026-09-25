"""Deterministic YEE-60 source-native plugin eligibility gate."""

from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence


WORK_ORDER = "YEE-60"
SCHEMA_VERSION = "yee-60-plugin-only-universe-v0.1"
CLASSIFIER_VERSION = "yee-60-source-native-plugin-classifier-v0.1"
INPUT_DB_SHA256 = "f0712951d416b52260f524a3218abd8a54a087904d6b2553631f461f010f182e"
INPUT_JSONL_SHA256 = "c0de8eda1a030b3485f65903aaa065542e8d1b8e04946d3bb51c54a8d4ceccc9"
EXPECTED_SOURCE_COUNTS = {"voxel": 6639, "modrinth": 158507, "hangar": 3861}

PLUGIN_PLATFORM_TOKENS = frozenset({
    "bukkit", "spigot", "paper", "purpur", "folia", "sponge",
    "bungee", "bungeecord", "waterfall", "velocity",
})
NON_PLUGIN_PLATFORM_TOKENS = frozenset({"fabric", "forge", "neoforge", "quilt"})
EXPLICIT_PLUGIN_CLASS_TOKEN = "plugin"
EXPLICIT_NON_PLUGIN_CLASS_TOKENS = frozenset({
    "mod", "modpack", "shader", "resourcepack", "datapack",
})

ELIGIBILITY_COLUMNS = (
    "source", "source_resource_id", "canonical_identity", "plugin_eligibility",
    "eligibility_reason_codes", "positive_evidence", "conflicting_evidence",
    "project_type_norm", "loader_facets_json", "category_facets_json",
    "classifier_version",
)
ELIGIBILITY_JSON_FIELDS = {
    "eligibility_reason_codes", "positive_evidence", "conflicting_evidence",
}
STATUSES = ("PLUGIN_ELIGIBLE", "NON_PLUGIN", "AMBIGUOUS")
CORE_ARTIFACTS = (
    "plugin_eligibility.jsonl", "plugin_eligibility.csv",
    "plugin_only_resource_features.jsonl", "plugin_only_resource_features.csv",
    "plugin_taxonomy_audit.json", "plugin_taxonomy_audit.md",
    "PLUGIN_SCOPE_SCHEMA.md", "plugin_eligibility.sqlite",
)
OUTPUT_FILES = (*CORE_ARTIFACTS, "FINAL_REPORT.md", "QA_RESULT.json", "DATASET_MANIFEST.json")
_NULL_CSV = r"\N"


class PluginEligibilityError(ValueError):
    """Canonical input or YEE-60 output violated the eligibility contract."""


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
        allow_nan=False,
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_only_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _facet_tokens(value: Any, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise PluginEligibilityError(f"invalid {field} JSON") from exc
    if not isinstance(parsed, list) or any(not isinstance(token, str) for token in parsed):
        raise PluginEligibilityError(f"{field} must be a JSON array of strings")
    if parsed != sorted(set(parsed)):
        raise PluginEligibilityError(f"{field} must be sorted and deduplicated")
    if any(token != token.casefold() for token in parsed):
        raise PluginEligibilityError(f"{field} contains a non-normalized token")
    return tuple(parsed)


def classify_resource(row: Mapping[str, Any]) -> dict[str, Any]:
    """Classify only exact source-native type/platform facts; text is never read."""
    project_type = row.get("project_type_norm")
    if project_type is not None and not isinstance(project_type, str):
        raise PluginEligibilityError("project_type_norm must be a string or null")
    loader_tokens = _facet_tokens(row.get("loader_facets_json"), "loader_facets_json")
    category_tokens = _facet_tokens(row.get("category_facets_json"), "category_facets_json")

    positive: list[dict[str, str]] = []
    incompatible: list[dict[str, str]] = []
    if project_type == EXPLICIT_PLUGIN_CLASS_TOKEN:
        positive.append({"field": "project_type_norm", "token": project_type})
    if project_type in EXPLICIT_NON_PLUGIN_CLASS_TOKENS:
        incompatible.append({"field": "project_type_norm", "token": project_type})

    for field, tokens in (
        ("loader_facets_json", loader_tokens),
        ("category_facets_json", category_tokens),
    ):
        for token in tokens:
            if token in PLUGIN_PLATFORM_TOKENS:
                positive.append({"field": field, "token": token})
            elif token == EXPLICIT_PLUGIN_CLASS_TOKEN and field == "category_facets_json":
                positive.append({"field": field, "token": token})
            if token in EXPLICIT_NON_PLUGIN_CLASS_TOKENS:
                incompatible.append({"field": field, "token": token})
            elif token in NON_PLUGIN_PLATFORM_TOKENS:
                incompatible.append({"field": field, "token": token})

    positive.sort(key=lambda fact: (fact["field"], fact["token"]))
    incompatible.sort(key=lambda fact: (fact["field"], fact["token"]))
    has_positive = bool(positive)
    has_incompatible = bool(incompatible)

    if has_positive and has_incompatible:
        state = "AMBIGUOUS"
        reasons = ["PLUGIN_AND_NON_PLUGIN_EVIDENCE_CONFLICT"]
        conflicting = incompatible
    elif has_positive:
        state = "PLUGIN_ELIGIBLE"
        reasons = ["EXPLICIT_PLUGIN_CLASS_EVIDENCE"]
        conflicting = []
    elif has_incompatible:
        state = "NON_PLUGIN"
        has_class = any(
            fact["token"] in EXPLICIT_NON_PLUGIN_CLASS_TOKENS
            for fact in incompatible
        )
        reasons = []
        if has_class:
            reasons.append("EXPLICIT_NON_PLUGIN_PRODUCT_CLASS")
        if any(fact["token"] in NON_PLUGIN_PLATFORM_TOKENS for fact in incompatible):
            reasons.append("NON_PLUGIN_PLATFORM_ONLY")
        conflicting = []
    else:
        state = "AMBIGUOUS"
        reasons = ["NO_EXPLICIT_PLUGIN_CLASS_EVIDENCE"]
        if project_type is None and not loader_tokens and not category_tokens:
            reasons.append("PRODUCT_AND_PLATFORM_METADATA_MISSING")
        elif category_tokens:
            reasons.append("CATEGORY_TOKENS_DO_NOT_PROVE_PLUGIN_CLASS")
        if project_type is not None and project_type != EXPLICIT_PLUGIN_CLASS_TOKEN:
            reasons.append("PROJECT_TYPE_NOT_EXPLICIT_PLUGIN_CLASS")
        conflicting = []

    identity = row.get("canonical_identity")
    return {
        "source": row["source"],
        "source_resource_id": row["source_resource_id"],
        "canonical_identity": identity,
        "plugin_eligibility": state,
        "eligibility_reason_codes": reasons,
        "positive_evidence": positive,
        "conflicting_evidence": conflicting,
        "project_type_norm": project_type,
        "loader_facets_json": row.get("loader_facets_json"),
        "category_facets_json": row.get("category_facets_json"),
        "classifier_version": CLASSIFIER_VERSION,
    }


def _taxonomy_audit(
    rows: Mapping[tuple[str, str, str], dict[str, Any]],
    source_counts: Mapping[str, int],
    input_hashes: Mapping[str, str],
) -> dict[str, Any]:
    ordered = []
    for (source, field, state_token), record in sorted(rows.items()):
        token = None if state_token in {"<NULL>", "<EMPTY>"} else state_token
        value_state = "NULL" if state_token == "<NULL>" else "EMPTY" if state_token == "<EMPTY>" else "TOKEN"
        ordered.append({
            "source": source,
            "field": field,
            "value_state": value_state,
            "token": token,
            "resource_count": record["resource_count"],
            "representative_canonical_identity": record["representative_canonical_identity"],
            "classifier_mapping": record["classifier_mapping"],
        })
    return {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "classifier_version": CLASSIFIER_VERSION,
        "input_sha256": dict(sorted(input_hashes.items())),
        "identity_counts_by_source": dict(sorted(source_counts.items())),
        "facet_count_semantics": "A resource with multiple facet tokens is counted once for each token; facet totals are not mutually exclusive.",
        "classifier_contract": {
            "positive_project_type_token": EXPLICIT_PLUGIN_CLASS_TOKEN,
            "positive_category_type_token": EXPLICIT_PLUGIN_CLASS_TOKEN,
            "positive_platform_tokens": sorted(PLUGIN_PLATFORM_TOKENS),
            "explicit_non_plugin_class_tokens": sorted(EXPLICIT_NON_PLUGIN_CLASS_TOKENS),
            "non_plugin_platform_tokens": sorted(NON_PLUGIN_PLATFORM_TOKENS),
            "unknown_and_broad_tokens": "Documented but never promoted; no keyword inference.",
            "conflict_rule": "Any positive plugin-class fact plus any explicit incompatible class/platform fact yields AMBIGUOUS.",
        },
        "observed_taxonomy": ordered,
    }


def _mapping_used(field: str, token: str) -> str | None:
    if field == "project_type_norm":
        if token == EXPLICIT_PLUGIN_CLASS_TOKEN:
            return "PLUGIN_PROJECT_TYPE"
        if token in EXPLICIT_NON_PLUGIN_CLASS_TOKENS:
            return "NON_PLUGIN_PROJECT_TYPE"
    if field == "loader_facets_json":
        if token in PLUGIN_PLATFORM_TOKENS:
            return "PLUGIN_PLATFORM"
        if token in NON_PLUGIN_PLATFORM_TOKENS:
            return "NON_PLUGIN_PLATFORM"
    if field == "category_facets_json":
        if token == EXPLICIT_PLUGIN_CLASS_TOKEN:
            return "PLUGIN_CATEGORY_TYPE"
        if token in PLUGIN_PLATFORM_TOKENS:
            return "PLUGIN_PLATFORM"
        if token in EXPLICIT_NON_PLUGIN_CLASS_TOKENS:
            return "NON_PLUGIN_CATEGORY_TYPE"
        if token in NON_PLUGIN_PLATFORM_TOKENS:
            return "NON_PLUGIN_PLATFORM"
    return None


def _taxonomy_mark(
    counters: dict[tuple[str, str, str], dict[str, Any]],
    source: str,
    field: str,
    value: str,
    identity: str,
) -> None:
    key = (source, field, value)
    record = counters.setdefault(key, {
        "resource_count": 0,
        "representative_canonical_identity": identity,
        "classifier_mapping": _mapping_used(field, value) or "UNMAPPED_NOT_PROMOTED",
    })
    record["resource_count"] += 1


def _schema_text(repository_root: Path) -> str:
    path = repository_root / "PLUGIN_SCOPE_SCHEMA.md"
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PluginEligibilityError("PLUGIN_SCOPE_SCHEMA.md is missing from repository") from exc


def _csv_value(value: Any) -> Any:
    if value is None:
        return _NULL_CSV
    if isinstance(value, (list, dict)):
        return canonical_json(value)
    return value


def _write_csv_header(path: Path, columns: Sequence[str]) -> tuple[Any, Any]:
    stream = path.open("w", encoding="utf-8", newline="")
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n", extrasaction="raise")
    writer.writeheader()
    return stream, writer


def _write_json_line(stream: Any, row: Mapping[str, Any]) -> None:
    stream.write(canonical_json(row))
    stream.write("\n")


def _record_taxonomy(
    counters: dict[tuple[str, str, str], dict[str, Any]],
    source: str,
    field: str,
    value: Any,
    identity: str,
) -> None:
    if field == "project_type_norm":
        token_value = "<NULL>" if value is None else value
        _taxonomy_mark(counters, source, field, token_value, identity)
        return
    tokens = _facet_tokens(value, field)
    if not tokens:
        _taxonomy_mark(counters, source, field, "<EMPTY>" if value is not None else "<NULL>", identity)
        return
    for token in tokens:
        _taxonomy_mark(counters, source, field, token, identity)


def _verify_canonical_inputs(
    input_db_path: Path,
    input_jsonl_path: Path,
    connection: sqlite3.Connection,
    columns: Sequence[str],
    *,
    enforce_pinned_inputs: bool,
) -> tuple[str, str, dict[str, int]]:
    db_hash = sha256_file(input_db_path)
    jsonl_hash = sha256_file(input_jsonl_path)
    if enforce_pinned_inputs and (db_hash != INPUT_DB_SHA256 or jsonl_hash != INPUT_JSONL_SHA256):
        raise PluginEligibilityError("YEE-29 canonical input SHA-256 does not match pinned accepted artifacts")

    expected_columns = {"source", "source_resource_id", "canonical_identity", "project_type_norm", "loader_facets_json", "category_facets_json"}
    if not expected_columns.issubset(columns):
        raise PluginEligibilityError("resource_features is missing required YEE-29 source-native fields")
    count = connection.execute("SELECT COUNT(*) FROM resource_features").fetchone()[0]
    unique_identities = connection.execute(
        "SELECT COUNT(DISTINCT canonical_identity) FROM resource_features"
    ).fetchone()[0]
    malformed_identities = connection.execute(
        """SELECT COUNT(*) FROM resource_features
           WHERE canonical_identity IS NULL
              OR canonical_identity IS NOT (source || ':' || source_resource_id)"""
    ).fetchone()[0]
    if unique_identities != count or malformed_identities:
        raise PluginEligibilityError("YEE-29 canonical identities are duplicated or malformed")
    source_counts = {
        source: total
        for source, total in connection.execute(
            "SELECT source, COUNT(*) FROM resource_features GROUP BY source ORDER BY source"
        )
    }
    if enforce_pinned_inputs and (count != sum(EXPECTED_SOURCE_COUNTS.values()) or source_counts != EXPECTED_SOURCE_COUNTS):
        raise PluginEligibilityError("YEE-29 resource_features universe/source counts do not match the accepted baseline")

    select_sql = "SELECT " + ", ".join(f'"{column}"' for column in columns)
    select_sql += " FROM resource_features ORDER BY source, source_resource_id"
    db_rows = connection.execute(select_sql)
    jsonl_count = 0
    with input_jsonl_path.open("r", encoding="utf-8-sig", newline="") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                raise PluginEligibilityError(f"blank line in canonical resource_features.jsonl at line {line_number}")
            try:
                json_row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise PluginEligibilityError(f"invalid canonical resource_features.jsonl at line {line_number}") from exc
            source_row = next(db_rows, None)
            if source_row is None or not isinstance(json_row, dict) or json_row != dict(source_row):
                raise PluginEligibilityError(f"canonical resource_features JSONL/SQLite mismatch at line {line_number}")
            jsonl_count += 1
    if next(db_rows, None) is not None or jsonl_count != count:
        raise PluginEligibilityError("canonical resource_features JSONL/SQLite row counts do not reconcile")
    return db_hash, jsonl_hash, source_counts


def _create_output_database(
    output_path: Path,
    source_create_sql: str,
) -> sqlite3.Connection:
    connection = sqlite3.connect(output_path)
    connection.execute("PRAGMA page_size=4096")
    connection.execute("PRAGMA encoding='UTF-8'")
    connection.execute("PRAGMA journal_mode=DELETE")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute(
        """CREATE TABLE plugin_eligibility (
            source TEXT NOT NULL,
            source_resource_id TEXT NOT NULL,
            canonical_identity TEXT NOT NULL,
            plugin_eligibility TEXT NOT NULL CHECK(plugin_eligibility IN ('PLUGIN_ELIGIBLE','NON_PLUGIN','AMBIGUOUS')),
            eligibility_reason_codes TEXT NOT NULL,
            positive_evidence TEXT NOT NULL,
            conflicting_evidence TEXT NOT NULL,
            project_type_norm TEXT,
            loader_facets_json TEXT,
            category_facets_json TEXT,
            classifier_version TEXT NOT NULL,
            PRIMARY KEY(source, source_resource_id),
            UNIQUE(canonical_identity)
        )"""
    )
    connection.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    connection.execute(
        """CREATE TABLE plugin_taxonomy_audit (
            source TEXT NOT NULL,
            field TEXT NOT NULL,
            value_state TEXT NOT NULL,
            token TEXT,
            resource_count INTEGER NOT NULL,
            representative_canonical_identity TEXT NOT NULL,
            classifier_mapping TEXT NOT NULL,
            PRIMARY KEY(source, field, value_state, token)
        )"""
    )
    renamed_sql = source_create_sql.replace(
        "CREATE TABLE resource_features", "CREATE TABLE plugin_only_resource_features", 1
    )
    if renamed_sql == source_create_sql:
        connection.close()
        raise PluginEligibilityError("could not preserve YEE-29 resource_features schema")
    connection.execute(renamed_sql)
    return connection


def _feature_insert_sql(columns: Sequence[str]) -> str:
    quoted = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO plugin_only_resource_features ({quoted}) VALUES ({placeholders})"


def _eligibility_insert_sql() -> str:
    return "INSERT INTO plugin_eligibility VALUES (" + ", ".join("?" for _ in ELIGIBILITY_COLUMNS) + ")"


def _taxonomy_mark_for_state(state_token: str) -> tuple[str, str | None]:
    if state_token == "<NULL>":
        return "NULL", None
    if state_token == "<EMPTY>":
        return "EMPTY", None
    return "TOKEN", state_token


def _build_core(
    input_db_path: Path,
    input_jsonl_path: Path,
    output_dir: Path,
    schema_text: str,
    *,
    enforce_pinned_inputs: bool,
) -> dict[str, Any]:
    if output_dir.exists() and any(output_dir.iterdir()):
        raise PluginEligibilityError(f"output directory must be empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    input_db = _read_only_connection(input_db_path)
    try:
        table_row = input_db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='resource_features'"
        ).fetchone()
        if table_row is None or not table_row[0]:
            raise PluginEligibilityError("accepted YEE-29 analysis.db has no resource_features table")
        columns = [row[1] for row in input_db.execute("PRAGMA table_info(resource_features)")]
        db_hash, jsonl_hash, source_counts = _verify_canonical_inputs(
            input_db_path, input_jsonl_path, input_db, columns,
            enforce_pinned_inputs=enforce_pinned_inputs,
        )
        input_sha_after_compare = {
            "analysis.db": sha256_file(input_db_path),
            "resource_features.jsonl": sha256_file(input_jsonl_path),
        }
        if input_sha_after_compare != {"analysis.db": db_hash, "resource_features.jsonl": jsonl_hash}:
            raise PluginEligibilityError("YEE-29 input changed while being verified")

        taxonomy_counts: dict[tuple[str, str, str], dict[str, Any]] = {}
        status_counts: dict[str, Counter[str]] = defaultdict(Counter)
        reason_counts: dict[str, Counter[str]] = defaultdict(Counter)
        eligible_rows = 0
        eligible_positive_evidence_rows = 0
        eligible_conflict_free_rows = 0
        explicit_conflict_rows = 0
        explicit_conflict_ambiguous_rows = 0
        select_sql = "SELECT " + ", ".join(f'"{column}"' for column in columns)
        select_sql += " FROM resource_features ORDER BY source, source_resource_id"
        source_create_sql = table_row[0]
        output_db = _create_output_database(output_dir / "plugin_eligibility.sqlite", source_create_sql)
        eligibility_jsonl = (output_dir / "plugin_eligibility.jsonl").open("w", encoding="utf-8", newline="\n")
        feature_jsonl = (output_dir / "plugin_only_resource_features.jsonl").open("w", encoding="utf-8", newline="\n")
        eligibility_csv_stream, eligibility_csv = _write_csv_header(
            output_dir / "plugin_eligibility.csv", ELIGIBILITY_COLUMNS
        )
        feature_csv_stream, feature_csv = _write_csv_header(
            output_dir / "plugin_only_resource_features.csv", columns
        )
        eligibility_batch: list[tuple[Any, ...]] = []
        feature_batch: list[tuple[Any, ...]] = []
        try:
            for source_row in input_db.execute(select_sql):
                feature = dict(source_row)
                identity = feature["canonical_identity"]
                for field in ("project_type_norm", "loader_facets_json", "category_facets_json"):
                    _record_taxonomy(taxonomy_counts, feature["source"], field, feature[field], identity)
                classified = classify_resource(feature)
                status = classified["plugin_eligibility"]
                status_counts[feature["source"]][status] += 1
                for reason in classified["eligibility_reason_codes"]:
                    reason_counts[feature["source"]][reason] += 1
                has_positive = bool(classified["positive_evidence"])
                has_conflict = bool(classified["conflicting_evidence"])
                if has_positive and has_conflict:
                    explicit_conflict_rows += 1
                    if status == "AMBIGUOUS":
                        explicit_conflict_ambiguous_rows += 1
                _write_json_line(eligibility_jsonl, classified)
                eligibility_csv.writerow({key: _csv_value(classified[key]) for key in ELIGIBILITY_COLUMNS})
                eligibility_batch.append(tuple(
                    canonical_json(classified[key]) if key in ELIGIBILITY_JSON_FIELDS else classified[key]
                    for key in ELIGIBILITY_COLUMNS
                ))
                if status == "PLUGIN_ELIGIBLE":
                    eligible_rows += 1
                    eligible_positive_evidence_rows += int(has_positive)
                    eligible_conflict_free_rows += int(not has_conflict)
                    _write_json_line(feature_jsonl, feature)
                    feature_csv.writerow({key: _csv_value(feature[key]) for key in columns})
                    feature_batch.append(tuple(feature[column] for column in columns))
                if len(eligibility_batch) >= 2000:
                    output_db.executemany(_eligibility_insert_sql(), eligibility_batch)
                    eligibility_batch.clear()
                if len(feature_batch) >= 2000:
                    output_db.executemany(_feature_insert_sql(columns), feature_batch)
                    feature_batch.clear()
            if eligibility_batch:
                output_db.executemany(_eligibility_insert_sql(), eligibility_batch)
            if feature_batch:
                output_db.executemany(_feature_insert_sql(columns), feature_batch)
        finally:
            eligibility_jsonl.close()
            feature_jsonl.close()
            eligibility_csv_stream.close()
            feature_csv_stream.close()

        audit = _taxonomy_audit(
            taxonomy_counts,
            source_counts,
            {"analysis.db": db_hash, "resource_features.jsonl": jsonl_hash},
        )
        (output_dir / "plugin_taxonomy_audit.json").write_text(
            canonical_json(audit) + "\n", encoding="utf-8", newline="\n"
        )
        _write_taxonomy_markdown(output_dir / "plugin_taxonomy_audit.md", audit)
        (output_dir / "PLUGIN_SCOPE_SCHEMA.md").write_text(schema_text, encoding="utf-8", newline="\n")
        metadata = {
            "work_order": WORK_ORDER,
            "schema_version": SCHEMA_VERSION,
            "classifier_version": CLASSIFIER_VERSION,
            "input_analysis_db_sha256": db_hash,
            "input_resource_features_jsonl_sha256": jsonl_hash,
            "input_resource_count": sum(source_counts.values()),
            "input_source_counts": source_counts,
            "plugin_eligible_count": eligible_rows,
            "classifier_contract": audit["classifier_contract"],
        }
        output_db.executemany(
            "INSERT INTO metadata(key, value) VALUES (?, ?)",
            [(key, canonical_json(value)) for key, value in sorted(metadata.items())],
        )
        taxonomy_inserts = []
        for (source, field, state_token), record in sorted(taxonomy_counts.items()):
            value_state, token = _taxonomy_mark_for_state(state_token)
            taxonomy_inserts.append((
                source, field, value_state, token, record["resource_count"],
                record["representative_canonical_identity"], record["classifier_mapping"],
            ))
        output_db.executemany("INSERT INTO plugin_taxonomy_audit VALUES (?, ?, ?, ?, ?, ?, ?)", taxonomy_inserts)
        output_db.commit()
        integrity = output_db.execute("PRAGMA integrity_check").fetchone()[0]
        fk_violations = list(output_db.execute("PRAGMA foreign_key_check"))
        output_db.close()
        if integrity != "ok" or fk_violations:
            raise PluginEligibilityError("output SQLite integrity or foreign-key check failed")

        return {
            "input_sha256_before": {"analysis.db": db_hash, "resource_features.jsonl": jsonl_hash},
            "source_counts": source_counts,
            "status_counts_by_source": {
                source: {status: status_counts[source][status] for status in STATUSES}
                for source in sorted(source_counts)
            },
            "reason_counts_by_source": {
                source: dict(sorted(reason_counts[source].items()))
                for source in sorted(source_counts)
            },
            "eligibility_rows": sum(sum(values.values()) for values in status_counts.values()),
            "plugin_only_rows": eligible_rows,
            "taxonomy_rows": len(taxonomy_counts),
            "eligible_positive_evidence_rows": eligible_positive_evidence_rows,
            "eligible_conflict_free_rows": eligible_conflict_free_rows,
            "explicit_conflict_rows": explicit_conflict_rows,
            "explicit_conflict_ambiguous_rows": explicit_conflict_ambiguous_rows,
            "input_columns": columns,
            "enforce_pinned_inputs": enforce_pinned_inputs,
        }
    finally:
        input_db.close()


def _write_taxonomy_markdown(path: Path, audit: Mapping[str, Any]) -> None:
    lines = [
        "# YEE-60 source-native taxonomy audit", "",
        f"- Classifier: `{audit['classifier_version']}`",
        f"- YEE-29 `analysis.db` SHA-256: `{audit['input_sha256']['analysis.db']}`",
        f"- YEE-29 `resource_features.jsonl` SHA-256: `{audit['input_sha256']['resource_features.jsonl']}`",
        "- Canonical identity counts: " + ", ".join(
            f"{source}={count:,}" for source, count in sorted(audit["identity_counts_by_source"].items())
        ),
        "", "Only exact source-native project-class/platform tokens listed in the classifier contract can affect eligibility. Other tokens are documented and unpromoted.",
        "", "| Source | Field | Value state | Token | Resource count | Representative canonical identity | Mapping |",
        "|---|---|---|---|---:|---|---|",
    ]
    for item in audit["observed_taxonomy"]:
        token = "∅" if item["token"] is None else str(item["token"]).replace("|", "\\|")
        identity = item["representative_canonical_identity"].replace("|", "\\|")
        lines.append(
            f"| {item['source']} | {item['field']} | {item['value_state']} | {token} | "
            f"{item['resource_count']} | `{identity}` | {item['classifier_mapping']} |"
        )
    lines.extend([
        "", "## Classification rules", "",
        "- `PLUGIN_ELIGIBLE`: explicit `plugin` project/category class token or an exact allowlisted server/proxy platform token in the accepted loader/category facets, with no explicit incompatible class/platform fact.",
        "- `NON_PLUGIN`: explicit incompatible product-class token or only explicit Fabric/Forge/NeoForge/Quilt platform evidence, with no positive plugin fact.",
        "- `AMBIGUOUS`: no explicit class proof, or a positive plugin fact conflicts with incompatible source-native evidence.",
        "- Titles, summaries, authors, popularity, demand, prices and historical YEE-30–YEE-59 outputs are not classifier inputs.",
        "- Category counts are multi-valued and are not mutually exclusive.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8", newline="\n")


def _write_report(path: Path, qa: Mapping[str, Any]) -> None:
    counts = qa["status_counts_by_source"]
    lines = [
        "# YEE-60 final report", "",
        "Status: `PLUGIN_UNIVERSE_READY_FOR_SUPERVISOR_REVIEW`" if qa["status"] == "PASS" else "Status: `BLOCKED`",
        "", "## Scope and inputs", "",
        "This work classifies the accepted YEE-29 `resource_features` universe using only exact source-native project type, loader, and category facets. The accepted database and JSONL were opened/verified read-only and their pinned SHA-256 values reconciled before and after processing.",
        "", f"- Input identities: {qa['row_counts']['input_resource_features']:,}",
        f"- Input `analysis.db` SHA-256: `{qa['input_sha256_before']['analysis.db']}`",
        f"- Input `resource_features.jsonl` SHA-256: `{qa['input_sha256_before']['resource_features.jsonl']}`",
        f"- Input hashes unchanged after processing: `{qa['checks']['input_sha256_unchanged_after_processing']}`",
        f"- Classifier version: `{CLASSIFIER_VERSION}`",
        "", "## Eligibility counts", "",
        "| Source | PLUGIN_ELIGIBLE | NON_PLUGIN | AMBIGUOUS | Total |",
        "|---|---:|---:|---:|---:|",
    ]
    for source in sorted(counts):
        row = counts[source]
        lines.append(
            f"| {source} | {row['PLUGIN_ELIGIBLE']:,} | {row['NON_PLUGIN']:,} | "
            f"{row['AMBIGUOUS']:,} | {sum(row.values()):,} |"
        )
    totals = {status: sum(row[status] for row in counts.values()) for status in STATUSES}
    lines.extend([
        f"| **Total** | **{totals['PLUGIN_ELIGIBLE']:,}** | **{totals['NON_PLUGIN']:,}** | **{totals['AMBIGUOUS']:,}** | **{sum(totals.values()):,}** |",
        "", "## QA", "",
        f"- QA status: `{qa['status']}`",
        f"- Deterministic replay: `{qa['checks']['deterministic_replay_byte_identical']}`",
        f"- SQLite integrity: `{qa['checks']['sqlite_integrity_check']}`",
        f"- NON_PLUGIN/AMBIGUOUS leakage: `{qa['checks']['zero_non_plugin_or_ambiguous_leakage']}`",
        "", "## Stop boundary", "",
        "YEE-60 stops at the plugin-only resource universe. No topics, families, shortlist, ranking, external research, or product concepts were generated.",
        "", "See `plugin_taxonomy_audit.md`, `QA_RESULT.json`, and `DATASET_MANIFEST.json` for token-level audit and artifact hashes.", "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8", newline="\n")


def _write_qa_and_manifest(
    output_dir: Path,
    core: Mapping[str, Any],
    *,
    deterministic_replay: bool,
) -> dict[str, Any]:
    eligibility_jsonl = output_dir / "plugin_eligibility.jsonl"
    eligibility_csv_path = output_dir / "plugin_eligibility.csv"
    feature_jsonl = output_dir / "plugin_only_resource_features.jsonl"
    feature_csv_path = output_dir / "plugin_only_resource_features.csv"
    output_db_path = output_dir / "plugin_eligibility.sqlite"

    status_counts = core["status_counts_by_source"]
    total_counts = {status: sum(row[status] for row in status_counts.values()) for status in STATUSES}
    db = _read_only_connection(output_db_path)
    canonical_db = _read_only_connection(Path(core["input_db_path"]))
    try:
        sqlite_integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        fk_violations = list(db.execute("PRAGMA foreign_key_check"))
        sqlite_counts = {
            "plugin_eligibility": db.execute("SELECT COUNT(*) FROM plugin_eligibility").fetchone()[0],
            "plugin_only_resource_features": db.execute("SELECT COUNT(*) FROM plugin_only_resource_features").fetchone()[0],
        }
        sqlite_states = {
            state: count for state, count in db.execute(
                "SELECT plugin_eligibility, COUNT(*) FROM plugin_eligibility GROUP BY plugin_eligibility"
            )
        }
        leaked = db.execute(
            """SELECT COUNT(*) FROM plugin_eligibility e
               LEFT JOIN plugin_only_resource_features f
               ON f.source=e.source AND f.source_resource_id=e.source_resource_id
               WHERE (e.plugin_eligibility='PLUGIN_ELIGIBLE' AND f.source_resource_id IS NULL)
                  OR (e.plugin_eligibility!='PLUGIN_ELIGIBLE' AND f.source_resource_id IS NOT NULL)"""
        ).fetchone()[0]
        extra_features = db.execute(
            """SELECT COUNT(*) FROM plugin_only_resource_features f
               LEFT JOIN plugin_eligibility e
               ON e.source=f.source AND e.source_resource_id=f.source_resource_id
               WHERE e.source_resource_id IS NULL OR e.plugin_eligibility!='PLUGIN_ELIGIBLE'"""
        ).fetchone()[0]
        eligible_keys = {
            (source, resource_id)
            for source, resource_id in db.execute(
                "SELECT source, source_resource_id FROM plugin_eligibility WHERE plugin_eligibility='PLUGIN_ELIGIBLE'"
            )
        }
        input_columns_sql = ", ".join(f'"{column}"' for column in core["input_columns"])
        input_cursor = canonical_db.execute(
            f"SELECT {input_columns_sql} FROM resource_features ORDER BY source, source_resource_id"
        )
        feature_cursor = iter(db.execute(
            f"SELECT {input_columns_sql} FROM plugin_only_resource_features ORDER BY source, source_resource_id"
        ))
        canonical_differences = 0
        for input_row in input_cursor:
            key = (input_row["source"], input_row["source_resource_id"])
            if key not in eligible_keys:
                continue
            eligible_keys.remove(key)
            output_row = next(feature_cursor, None)
            if output_row is None or tuple(input_row) != tuple(output_row):
                canonical_differences += 1
        if eligible_keys:
            canonical_differences += len(eligible_keys)
        if next(feature_cursor, None) is not None:
            canonical_differences += 1
        taxonomy_sqlite_rows = db.execute("SELECT COUNT(*) FROM plugin_taxonomy_audit").fetchone()[0]
        reconciled_eligibility, eligibility_states, eligibility_export_count = _reconcile_exports(
            db, "plugin_eligibility", ELIGIBILITY_COLUMNS, ELIGIBILITY_JSON_FIELDS,
            eligibility_jsonl, eligibility_csv_path,
        )
        reconciled_features, _, feature_export_count = _reconcile_exports(
            db, "plugin_only_resource_features", core["input_columns"], set(),
            feature_jsonl, feature_csv_path,
        )
    finally:
        db.close()
        canonical_db.close()

    jsonl_counts = {
        "plugin_eligibility": eligibility_export_count,
        "plugin_only_resource_features": feature_export_count,
    }
    jsonl_states = Counter(eligibility_states)
    output_counts_reconcile = (
        reconciled_eligibility and reconciled_features
        and jsonl_counts == {
            "plugin_eligibility": core["eligibility_rows"],
            "plugin_only_resource_features": core["plugin_only_rows"],
        }
        and sqlite_counts == jsonl_counts
        and dict(jsonl_states) == {key: value for key, value in total_counts.items() if value}
        and sqlite_states == dict(jsonl_states)
    )
    input_sha_after = {
        "analysis.db": sha256_file(Path(core["input_db_path"])),
        "resource_features.jsonl": sha256_file(Path(core["input_jsonl_path"])),
    }
    taxonomy = json.loads((output_dir / "plugin_taxonomy_audit.json").read_text(encoding="utf-8"))
    taxonomy_consistent = (
        len(taxonomy["observed_taxonomy"]) == core["taxonomy_rows"] == taxonomy_sqlite_rows
        and taxonomy["identity_counts_by_source"] == core["source_counts"]
    )
    checks = {
        "canonical_input_hashes_match_pinned": (
            core["input_sha256_before"]["analysis.db"] == INPUT_DB_SHA256
            and core["input_sha256_before"]["resource_features.jsonl"] == INPUT_JSONL_SHA256
        ) if core["enforce_pinned_inputs"] else None,
        "canonical_input_jsonl_sqlite_reconcile": core["input_jsonl_sqlite_reconcile"],
        "canonical_identity_universe_exact": (
            core["eligibility_rows"] == sum(EXPECTED_SOURCE_COUNTS.values())
            and core["source_counts"] == EXPECTED_SOURCE_COUNTS
        ) if core["enforce_pinned_inputs"] else core["eligibility_rows"] == sum(core["source_counts"].values()),
        "all_identities_classified_exactly_once": core["eligibility_rows"] == sum(total_counts.values()),
        "eligible_rows_have_positive_source_native_evidence": core["eligible_positive_evidence_rows"] == total_counts["PLUGIN_ELIGIBLE"],
        "eligible_rows_have_no_explicit_conflicting_evidence": core["eligible_conflict_free_rows"] == total_counts["PLUGIN_ELIGIBLE"],
        "explicit_positive_negative_conflicts_are_ambiguous": core["explicit_conflict_ambiguous_rows"] == core["explicit_conflict_rows"],
        "plugin_only_membership_equals_eligible_identity_set": leaked == 0 and sqlite_counts["plugin_only_resource_features"] == total_counts["PLUGIN_ELIGIBLE"],
        "zero_non_plugin_or_ambiguous_leakage": leaked == 0 and extra_features == 0,
        "eligible_feature_fields_preserved_from_canonical_input": canonical_differences == 0,
        "taxonomy_audit_covers_observed_tokens_and_sqlite": taxonomy_consistent,
        "jsonl_csv_sqlite_reconciliation": output_counts_reconcile,
        "sqlite_integrity_check": sqlite_integrity == "ok",
        "sqlite_foreign_key_check": len(fk_violations) == 0,
        "input_sha256_unchanged_after_processing": input_sha_after == core["input_sha256_before"],
        "deterministic_replay_byte_identical": deterministic_replay,
    }
    blocking_checks = [passed for passed in checks.values() if passed is not None]
    qa = {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "classifier_version": CLASSIFIER_VERSION,
        "status": "PASS" if all(blocking_checks) else "FAIL",
        "input_sha256_before": core["input_sha256_before"],
        "input_sha256_after": input_sha_after,
        "row_counts": {
            "input_resource_features": sum(core["source_counts"].values()),
            "plugin_eligibility": core["eligibility_rows"],
            "plugin_only_resource_features": core["plugin_only_rows"],
            "taxonomy_audit_values": core["taxonomy_rows"],
        },
        "source_counts": core["source_counts"],
        "status_counts": total_counts,
        "status_counts_by_source": status_counts,
        "reason_counts_by_source": core["reason_counts_by_source"],
        "checks": checks,
        "failed_checks": sorted(name for name, passed in checks.items() if passed is False),
    }
    (output_dir / "QA_RESULT.json").write_text(
        canonical_json(qa) + "\n", encoding="utf-8", newline="\n"
    )
    return qa


def _reconcile_exports(
    db: sqlite3.Connection,
    table: str,
    columns: Sequence[str],
    json_fields: set[str],
    jsonl_path: Path,
    csv_path: Path,
) -> tuple[bool, Counter[str], int]:
    quoted = ", ".join(f'"{column}"' for column in columns)
    cursor = iter(db.execute(
        f'SELECT {quoted} FROM "{table}" ORDER BY source, source_resource_id'
    ))
    state_counts: Counter[str] = Counter()
    count = 0
    with jsonl_path.open("r", encoding="utf-8") as jsonl_stream, csv_path.open(
        "r", encoding="utf-8", newline=""
    ) as csv_stream:
        csv_reader = csv.DictReader(csv_stream)
        if csv_reader.fieldnames != list(columns):
            return False, state_counts, 0
        for line, csv_row in zip(jsonl_stream, csv_reader, strict=False):
            if not line.strip():
                return False, state_counts, count
            try:
                json_row = json.loads(line)
            except json.JSONDecodeError:
                return False, state_counts, count
            database_row = next(cursor, None)
            if database_row is None or not isinstance(json_row, dict):
                return False, state_counts, count
            database_values = dict(zip(columns, database_row))
            for column in json_fields:
                try:
                    database_values[column] = json.loads(database_values[column])
                except (TypeError, json.JSONDecodeError):
                    return False, state_counts, count
            if json_row != database_values:
                return False, state_counts, count
            expected_csv = {column: str(_csv_value(json_row[column])) for column in columns}
            if csv_row != expected_csv:
                return False, state_counts, count
            if table == "plugin_eligibility":
                state_counts[json_row["plugin_eligibility"]] += 1
            count += 1
        if next(cursor, None) is not None:
            return False, state_counts, count
        if next(csv_reader, None) is not None:
            return False, state_counts, count
    return count > 0 or not columns, state_counts, count


def _write_manifest(output_dir: Path, qa: Mapping[str, Any]) -> None:
    artifacts = {}
    for name in OUTPUT_FILES:
        if name == "DATASET_MANIFEST.json":
            continue
        path = output_dir / name
        artifacts[name] = {"size_bytes": path.stat().st_size, "sha256": sha256_file(path)}
    manifest = {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "classifier_version": CLASSIFIER_VERSION,
        "status": qa["status"],
        "input_sha256": qa["input_sha256_before"],
        "row_counts": qa["row_counts"],
        "status_counts": qa["status_counts"],
        "artifacts": artifacts,
        "manifest_self_hash": "omitted to avoid a recursive hash",
    }
    (output_dir / "DATASET_MANIFEST.json").write_text(
        canonical_json(manifest) + "\n", encoding="utf-8", newline="\n"
    )


def _finalize_directory(
    output_dir: Path,
    core: Mapping[str, Any],
    *,
    deterministic_replay: bool,
) -> dict[str, Any]:
    qa = _write_qa_and_manifest(output_dir, core, deterministic_replay=deterministic_replay)
    _write_report(output_dir / "FINAL_REPORT.md", qa)
    # Report is included in the manifest; regenerate it after its final bytes exist.
    _write_manifest(output_dir, qa)
    return qa


def _build_and_validate_core(
    input_db_path: Path,
    input_jsonl_path: Path,
    output_dir: Path,
    schema_text: str,
    *,
    enforce_pinned_inputs: bool,
) -> dict[str, Any]:
    core = _build_core(
        input_db_path, input_jsonl_path, output_dir, schema_text,
        enforce_pinned_inputs=enforce_pinned_inputs,
    )
    # Facts needed later are explicit and deterministic; no run timestamp or output path enters artifacts.
    core["input_db_path"] = str(input_db_path.resolve())
    core["input_jsonl_path"] = str(input_jsonl_path.resolve())
    core["input_jsonl_sqlite_reconcile"] = True
    return core


def build_plugin_universe(
    input_db_path: Path,
    input_jsonl_path: Path,
    output_dir: Path,
    *,
    enforce_pinned_inputs: bool = True,
    verify_replay: bool = True,
) -> dict[str, Any]:
    """Build YEE-60 exports from accepted YEE-29, using a read-only input DB."""
    input_db_path = Path(input_db_path)
    input_jsonl_path = Path(input_jsonl_path)
    output_dir = Path(output_dir)
    repository_root = Path(__file__).resolve().parents[2]
    schema_text = _schema_text(repository_root)

    main_core = _build_and_validate_core(
        input_db_path, input_jsonl_path, output_dir, schema_text,
        enforce_pinned_inputs=enforce_pinned_inputs,
    )
    if not verify_replay:
        return _finalize_directory(output_dir, main_core, deterministic_replay=False)

    replay_parent = output_dir.parent
    with tempfile.TemporaryDirectory(prefix="yee60-replay-", dir=replay_parent) as temporary:
        replay_dir = Path(temporary)
        replay_core = _build_and_validate_core(
            input_db_path, input_jsonl_path, replay_dir, schema_text,
            enforce_pinned_inputs=enforce_pinned_inputs,
        )
        core_identical = all(
            (output_dir / name).read_bytes() == (replay_dir / name).read_bytes()
            for name in CORE_ARTIFACTS
        )
        if core_identical:
            main_qa = _finalize_directory(output_dir, main_core, deterministic_replay=True)
            _finalize_directory(replay_dir, replay_core, deterministic_replay=True)
            all_identical = all(
                (output_dir / name).read_bytes() == (replay_dir / name).read_bytes()
                for name in OUTPUT_FILES
            )
        else:
            all_identical = False
            main_qa = _finalize_directory(output_dir, main_core, deterministic_replay=False)
        if not all_identical and main_qa["checks"]["deterministic_replay_byte_identical"]:
            main_qa = _finalize_directory(output_dir, main_core, deterministic_replay=False)
        return main_qa
