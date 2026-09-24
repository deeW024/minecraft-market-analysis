"""Deterministic YEE-46 opportunity scoring, ranking, and shortlist build."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import sqlite3
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

from .pipeline import round_number

WORK_ORDER = "YEE-46"
SCHEMA_VERSION = "yee-46-opportunity-triage-v0.1"
INPUT_SCHEMA_VERSION = "yee-43-family-signal-layer-v0.1"
EXPECTED_INPUT_SHA256 = "6f5aced5942e519717df7112d7cd2e042cb1c99ac32875d904a48eedf4344764"
EXPECTED_INPUT_COUNTS = {
    "family_features": 1618,
    "family_resource_memberships": 141084,
    "family_source_signals": 2503,
    "family_voxel_price_signals": 568,
}
SOURCE_ORDER = ("voxel", "modrinth", "hangar")
SOURCE_WEIGHTS = {"modrinth": 0.45, "voxel": 0.40, "hangar": 0.15}
PROFILE_ORDER = ("balanced", "demand_first", "whitespace_first")
PROFILE_WEIGHTS = {
    "balanced": {"D": 0.50, "W": 0.25, "F": 0.15, "C": 0.10},
    "demand_first": {"D": 0.65, "W": 0.15, "F": 0.10, "C": 0.10},
    "whitespace_first": {"D": 0.40, "W": 0.40, "F": 0.10, "C": 0.10},
}
NULL_TOKEN = r"\N"

FAMILY_BASE_COLUMNS = (
    "family_id", "canonical_topic_key", "family_status", "member_count",
    "member_topic_keys", "aliases", "candidate_classes", "source_presence",
    "source_presence_count", "has_voxel_paid_evidence", "evidence_coverage_share",
)
FAMILY_SCORE_COLUMNS = (
    FAMILY_BASE_COLUMNS
    + tuple(f"{profile}_core_source_score" for profile in PROFILE_ORDER)
    + tuple(f"{profile}_source_weight_coverage" for profile in PROFILE_ORDER)
    + ("cross_market_validation_score", "voxel_monetization_score")
    + tuple(f"{profile}_final_score" for profile in PROFILE_ORDER)
    + tuple(f"{profile}_rank" for profile in PROFILE_ORDER)
    + ("median_profile_rank", "mean_profile_rank", "rank_span", "consensus_score", "consensus_rank", "triage_bucket")
)
SOURCE_INPUT_COLUMNS = (
    "resource_count", "demand_available_count", "freshness_available_count",
    "demand_percentile_p75", "demand_percentile_p90",
    "demand_percentile_ge90_share", "demand_percentile_ge95_share",
    "freshness_age_days_p50", "freshness_le90_share", "freshness_gt365_share",
    "demand_concentration_top1_share", "demand_concentration_hhi",
    "paid_count", "paid_known_count", "paid_share_known",
)
VOXEL_PAID_COLUMNS = ("paid_count", "paid_known_count", "paid_share_known")
SOURCE_COMPONENT_COLUMNS = (
    "family_id", "source", "D", "W", "F", "C",
    "source_score_balanced", "source_score_demand_first", "source_score_whitespace_first",
    *SOURCE_INPUT_COLUMNS,
)
SHORTLIST_SOURCE_FIELDS = (
    "resource_count", "D", "W", "F", "C",
    "source_score_balanced", "source_score_demand_first", "source_score_whitespace_first",
)
SHORTLIST_COLUMNS = FAMILY_SCORE_COLUMNS + tuple(
    f"{source}_{field}" for source in SOURCE_ORDER for field in SHORTLIST_SOURCE_FIELDS
)
EXPORT_TABLES = {
    "family_opportunity_scores": FAMILY_SCORE_COLUMNS,
    "source_opportunity_components": SOURCE_COMPONENT_COLUMNS,
    "opportunity_shortlist": SHORTLIST_COLUMNS,
}

TEXT_COLUMNS = {
    "family_id", "canonical_topic_key", "family_status", "source", "triage_bucket",
}
JSON_COLUMNS = {"member_topic_keys", "aliases", "candidate_classes", "source_presence"}
INTEGER_COLUMNS = {
    "member_count", "source_presence_count", "has_voxel_paid_evidence", "resource_count",
    "demand_available_count", "freshness_available_count", "paid_count", "paid_known_count",
    "balanced_rank", "demand_first_rank",
    "whitespace_first_rank", "median_profile_rank", "rank_span", "consensus_rank",
} | {f"{source}_resource_count" for source in SOURCE_ORDER}


class OpportunityInputError(ValueError):
    """Raised when the accepted YEE-43 input differs from the pinned contract."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_only_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _input_contract(connection: sqlite3.Connection) -> dict[str, Any]:
    metadata = {
        row["key"]: json.loads(row["value_json"])
        for row in connection.execute("SELECT key,value_json FROM metadata ORDER BY key")
    }
    schema_version = metadata.get("signal_schema_version")
    if schema_version != INPUT_SCHEMA_VERSION:
        raise OpportunityInputError(f"unexpected YEE-43 schema version: {schema_version!r}")
    counts = {
        table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in EXPECTED_INPUT_COUNTS
    }
    if counts != EXPECTED_INPUT_COUNTS:
        raise OpportunityInputError(f"unexpected accepted YEE-43 table counts: {counts}")
    return {"schema_version": schema_version, "table_counts": counts}


def _decode_json_columns(row: Mapping[str, Any], columns: Sequence[str]) -> dict[str, Any]:
    result = dict(row)
    for column in columns:
        value = result.get(column)
        if isinstance(value, str):
            result[column] = json.loads(value)
    return result


def _read_input(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    connection = _read_only_connection(path)
    try:
        contract = _input_contract(connection)
        features = [
            _decode_json_columns(row, ("member_topic_keys", "aliases", "candidate_classes", "source_presence"))
            for row in connection.execute("SELECT * FROM family_features ORDER BY family_id")
        ]
        signals = [dict(row) for row in connection.execute(
            "SELECT * FROM family_source_signals ORDER BY family_id,source"
        )]
        if len({row["family_id"] for row in features}) != len(features):
            raise OpportunityInputError("duplicate family_id in accepted YEE-43 family_features")
        if len({(row["family_id"], row["source"]) for row in signals}) != len(signals):
            raise OpportunityInputError("duplicate family/source row in accepted YEE-43 family_source_signals")
        return features, signals, contract
    finally:
        connection.close()


def _numeric(value: Any, name: str) -> float | None:
    if value is None:
        return None
    result = float(value)
    if not math.isfinite(result):
        raise OpportunityInputError(f"non-finite YEE-43 metric {name}: {value!r}")
    return result


def _required(row: Mapping[str, Any], name: str) -> float:
    value = _numeric(row.get(name), name)
    if value is None:
        raise OpportunityInputError(f"required YEE-43 metric is null: {name}")
    return value


def _check_range(value: float | int | None, name: str) -> None:
    if value is not None and not 0 <= value <= 100:
        raise OpportunityInputError(f"{name} is outside [0,100]: {value}")


def _midrank_percentiles(values: Sequence[float]) -> dict[float, float]:
    if not values:
        return {}
    counts = Counter(values)
    n = len(values)
    lower = 0
    result = {}
    for value, ties in sorted(counts.items()):
        result[value] = 0.5 if n == 1 else (lower + 0.5 * (ties - 1)) / (n - 1)
        lower += ties
    return result


def _demand_strength(row: Mapping[str, Any]) -> float | None:
    available_value = _required(row, "demand_available_count")
    if available_value < 0 or not available_value.is_integer():
        raise OpportunityInputError("demand_available_count cannot be negative")
    available = int(available_value)
    if available == 0:
        return None
    p90 = _required(row, "demand_percentile_p90")
    p75 = _required(row, "demand_percentile_p75")
    ge90 = _required(row, "demand_percentile_ge90_share")
    ge95 = _required(row, "demand_percentile_ge95_share")
    _check_range(p90, "demand_percentile_p90")
    _check_range(p75, "demand_percentile_p75")
    if not 0 <= ge90 <= 1 or not 0 <= ge95 <= 1:
        raise OpportunityInputError("YEE-43 demand percentile shares must be inside [0,1]")
    return round_number(0.45 * p90 + 0.25 * p75 + 0.20 * ge90 * 100 + 0.10 * ge95 * 100)


def _profile_score(components: Mapping[str, float | int | None], profile: str) -> float | int | None:
    demand = components["D"]
    if demand is None:
        return None
    weights = PROFILE_WEIGHTS[profile]
    available = [(components[name], weight) for name, weight in weights.items() if components[name] is not None]
    result = sum(float(value) * weight for value, weight in available) / sum(weight for _, weight in available)
    return round_number(result)


def compute_source_components(signal_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_source: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in signal_rows:
        source = row["source"]
        if source not in SOURCE_ORDER:
            raise OpportunityInputError(f"unsupported YEE-43 source: {source!r}")
        by_source[source].append(row)

    output = []
    for source in SOURCE_ORDER:
        rows = by_source[source]
        resource_counts = [_required(row, "resource_count") for row in rows]
        if any(value < 1 or not value.is_integer() for value in resource_counts):
            raise OpportunityInputError(f"invalid {source} resource_count")
        whitespace_ranks = _midrank_percentiles(resource_counts)
        freshness_values = [
            _numeric(row.get("freshness_age_days_p50"), "freshness_age_days_p50")
            for row in rows
        ]
        freshness_ranks = _midrank_percentiles([value for value in freshness_values if value is not None])

        for row, resource_count, freshness_age in zip(rows, resource_counts, freshness_values):
            demand = _demand_strength(row)
            whitespace = round_number(100 * (1 - whitespace_ranks[resource_count]))
            if freshness_age is None:
                freshness = None
            else:
                recent90 = _required(row, "freshness_le90_share")
                gt365 = _required(row, "freshness_gt365_share")
                if not 0 <= recent90 <= 1 or not 0 <= gt365 <= 1:
                    raise OpportunityInputError("YEE-43 freshness shares must be inside [0,1]")
                freshness = round_number(
                    0.50 * recent90 * 100
                    + 0.30 * 100 * (1 - freshness_ranks[freshness_age])
                    + 0.20 * (1 - gt365) * 100
                )

            hhi = _numeric(row.get("demand_concentration_hhi"), "demand_concentration_hhi")
            top1 = _numeric(row.get("demand_concentration_top1_share"), "demand_concentration_top1_share")
            if hhi is None or top1 is None:
                contestability = None
            else:
                if not 0 <= hhi <= 1 or not 0 <= top1 <= 1:
                    raise OpportunityInputError("YEE-43 concentration shares must be inside [0,1]")
                contestability = round_number(100 * (0.60 * (1 - hhi) + 0.40 * (1 - top1)))

            components = {"D": demand, "W": whitespace, "F": freshness, "C": contestability}
            for name, value in components.items():
                _check_range(value, name)
            result = {
                "family_id": row["family_id"],
                "source": source,
                **components,
            }
            for profile in PROFILE_ORDER:
                score = _profile_score(components, profile)
                _check_range(score, f"source_score_{profile}")
                result[f"source_score_{profile}"] = score
            for name in SOURCE_INPUT_COLUMNS:
                result[name] = row.get(name)
            output.append(result)
    return sorted(output, key=lambda row: (row["family_id"], SOURCE_ORDER.index(row["source"])))


def _weighted_core(
    source_scores: Mapping[str, float | int | None],
) -> tuple[float | int | None, float | int]:
    available = [
        (float(source_scores[source]), SOURCE_WEIGHTS[source])
        for source in SOURCE_ORDER
        if source_scores.get(source) is not None
    ]
    coverage = round_number(sum(weight for _, weight in available))
    if not available:
        return None, coverage
    return round_number(sum(score * weight for score, weight in available) / sum(weight for _, weight in available)), coverage


def _monetization_score(source_rows: Mapping[str, Mapping[str, Any]]) -> float | int | None:
    voxel = source_rows.get("voxel")
    if voxel is None:
        return None
    known = _numeric(voxel.get("paid_known_count"), "paid_known_count")
    if known is None or known == 0:
        return None
    if known < 0 or not known.is_integer():
        raise OpportunityInputError("invalid Voxel paid_known_count")
    paid = _required(voxel, "paid_count")
    share = _required(voxel, "paid_share_known")
    if paid < 0 or paid > known or not paid.is_integer() or not 0 <= share <= 1:
        raise OpportunityInputError("invalid YEE-43 Voxel paid-state metrics")
    result = round_number(50 * (paid > 0) + 50 * share)
    _check_range(result, "voxel_monetization_score")
    return result


def _monetization_counts(values: Sequence[float | int | None]) -> dict[str, int]:
    return {
        "non_null": sum(value is not None for value in values),
        "positive": sum(value is not None and value > 0 for value in values),
        "zero": sum(value == 0 for value in values),
    }


def _voxel_monetization_qa(
    signal_rows: Sequence[Mapping[str, Any]],
    component_rows: Sequence[Mapping[str, Any]],
    family_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    canonical_voxel = {
        row["family_id"]: row for row in signal_rows if row["source"] == "voxel"
    }
    components = {(row["family_id"], row["source"]): row for row in component_rows}
    families = {row["family_id"]: row for row in family_rows}
    component_field_mismatches = sum(
        components.get((family_id, "voxel"), {}).get(field) != row.get(field)
        for family_id, row in canonical_voxel.items()
        for field in VOXEL_PAID_COLUMNS
    )
    expected_scores = {
        family_id: _monetization_score({"voxel": row})
        for family_id, row in canonical_voxel.items()
    }
    expected_scores.update({
        family_id: None for family_id in families if family_id not in expected_scores
    })
    family_score_mismatches = sum(
        row.get("voxel_monetization_score") != expected_scores[family_id]
        for family_id, row in families.items()
    )
    expected_counts = _monetization_counts(list(expected_scores.values()))
    actual_counts = _monetization_counts([
        row.get("voxel_monetization_score") for row in family_rows
    ])
    production_counts = {"non_null": 655, "positive": 568, "zero": 87}
    return {
        "canonical_voxel_row_count": len(canonical_voxel),
        "component_paid_field_mismatch_count": component_field_mismatches,
        "family_score_mismatch_count": family_score_mismatches,
        "canonical_input_expected_counts": expected_counts,
        "family_score_actual_counts": actual_counts,
        "pinned_production_expected_counts": production_counts,
        "matches_canonical_input": component_field_mismatches == 0 and family_score_mismatches == 0,
        "matches_pinned_production_counts": expected_counts == production_counts and actual_counts == production_counts,
    }


def _triage_bucket(rank: int) -> str:
    if 1 <= rank <= 100:
        return "ADVANCE_RESEARCH"
    if 101 <= rank <= 300:
        return "WATCH"
    if 301 <= rank <= 1618:
        return "DEFER"
    raise ValueError(f"consensus rank outside accepted family universe: {rank}")


def _consensus_order_key(row: Mapping[str, Any]) -> tuple[Any, Any, Any, Any]:
    return (row["median_profile_rank"], row["mean_profile_rank"], row["balanced_rank"], row["family_id"])


def _assign_ranks(rows: list[dict[str, Any]]) -> None:
    for profile in PROFILE_ORDER:
        score_field = f"{profile}_final_score"
        if any(row[score_field] is None for row in rows):
            raise OpportunityInputError(f"cannot rank null {profile} final scores; Worker Spec has no null rank rule")
        ordered = sorted(rows, key=lambda row: (-float(row[score_field]), row["family_id"]))
        for rank, row in enumerate(ordered, 1):
            row[f"{profile}_rank"] = rank

    for row in rows:
        ranks = [row[f"{profile}_rank"] for profile in PROFILE_ORDER]
        row["median_profile_rank"] = int(median(ranks))
        row["mean_profile_rank"] = round_number(sum(ranks) / len(ranks))
        row["rank_span"] = max(ranks) - min(ranks)
        row["consensus_score"] = round_number(median([float(row[f"{profile}_final_score"]) for profile in PROFILE_ORDER]))

    ordered_consensus = sorted(rows, key=_consensus_order_key)
    for rank, row in enumerate(ordered_consensus, 1):
        row["consensus_rank"] = rank
        row["triage_bucket"] = _triage_bucket(rank)


def compute_family_scores(
    feature_rows: Sequence[Mapping[str, Any]], component_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    components_by_family: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for row in component_rows:
        if row["source"] in components_by_family[row["family_id"]]:
            raise OpportunityInputError("duplicate source component family/source key")
        components_by_family[row["family_id"]][row["source"]] = row

    output = []
    for feature in feature_rows:
        family_id = feature["family_id"]
        family_components = components_by_family.get(family_id, {})
        presence = feature.get("source_presence")
        if not isinstance(presence, list) or len(set(presence)) != len(presence):
            raise OpportunityInputError(f"invalid source_presence for family {family_id}")
        if set(presence) != set(family_components) or int(feature["source_presence_count"]) != len(presence):
            raise OpportunityInputError(f"YEE-43 source-presence mismatch for family {family_id}")
        if not 1 <= len(presence) <= len(SOURCE_ORDER):
            raise OpportunityInputError(f"invalid source-presence count for family {family_id}")

        result = {
            name: feature.get(name)
            for name in FAMILY_BASE_COLUMNS
        }
        result["has_voxel_paid_evidence"] = bool(feature["has_voxel_paid_evidence"])
        result["cross_market_validation_score"] = 50 * (len(presence) - 1)
        result["voxel_monetization_score"] = _monetization_score(family_components)

        for profile in PROFILE_ORDER:
            source_scores = {
                source: family_components.get(source, {}).get(f"source_score_{profile}")
                for source in SOURCE_ORDER
            }
            core, coverage = _weighted_core(source_scores)
            result[f"{profile}_core_source_score"] = core
            result[f"{profile}_source_weight_coverage"] = coverage
            if core is None:
                result[f"{profile}_final_score"] = None
            elif result["voxel_monetization_score"] is None:
                result[f"{profile}_final_score"] = round_number(
                    (0.80 * float(core) + 0.15 * result["cross_market_validation_score"]) / 0.95
                )
            else:
                result[f"{profile}_final_score"] = round_number(
                    0.80 * float(core)
                    + 0.15 * result["cross_market_validation_score"]
                    + 0.05 * float(result["voxel_monetization_score"])
                )
            _check_range(result[f"{profile}_core_source_score"], f"{profile}_core_source_score")
            _check_range(result[f"{profile}_final_score"], f"{profile}_final_score")
        output.append(result)

    if set(components_by_family) != {row["family_id"] for row in feature_rows}:
        raise OpportunityInputError("YEE-43 family IDs do not reconcile with source component rows")
    _assign_ranks(output)
    output.sort(key=lambda row: row["family_id"])
    shortlist_base = sorted(
        (row for row in output if row["triage_bucket"] == "ADVANCE_RESEARCH"),
        key=lambda row: row["consensus_rank"],
    )
    shortlist = []
    for row in shortlist_base:
        expanded = dict(row)
        for source in SOURCE_ORDER:
            source_row = components_by_family[row["family_id"]].get(source, {})
            for field in SHORTLIST_SOURCE_FIELDS:
                expanded[f"{source}_{field}"] = source_row.get(field)
        shortlist.append(expanded)
    return output, shortlist


def _csv_value(value: Any) -> Any:
    if value is None:
        return NULL_TOKEN
    if isinstance(value, (dict, list)):
        return _canonical_json(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _sql_type(column: str) -> str:
    if column in TEXT_COLUMNS:
        return "TEXT"
    if column in JSON_COLUMNS:
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


def _write_database(path: Path, rows: Mapping[str, Sequence[Mapping[str, Any]]], metadata: Mapping[str, Any]) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        for table, columns in EXPORT_TABLES.items():
            definitions = ",".join(f'"{column}" {_sql_type(column)}' for column in columns)
            if table == "family_opportunity_scores":
                primary = ",PRIMARY KEY(family_id)"
                foreign = ""
            elif table == "source_opportunity_components":
                primary = ",PRIMARY KEY(family_id,source)"
                foreign = ",FOREIGN KEY(family_id) REFERENCES family_opportunity_scores(family_id)"
            else:
                primary = ",PRIMARY KEY(family_id),UNIQUE(consensus_rank)"
                foreign = ",FOREIGN KEY(family_id) REFERENCES family_opportunity_scores(family_id)"
            connection.execute(f"CREATE TABLE {table} ({definitions}{primary}{foreign})")
        connection.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY,value_json TEXT NOT NULL)")
        connection.execute("CREATE INDEX source_opportunity_source_idx ON source_opportunity_components(source)")
        connection.execute("CREATE INDEX family_opportunity_consensus_rank_idx ON family_opportunity_scores(consensus_rank)")
        connection.execute("CREATE INDEX family_opportunity_triage_bucket_idx ON family_opportunity_scores(triage_bucket)")
        connection.execute("CREATE INDEX family_opportunity_topic_key_idx ON family_opportunity_scores(canonical_topic_key)")
        for table, columns in EXPORT_TABLES.items():
            placeholders = ",".join("?" for _ in columns)
            names = ",".join(f'"{column}"' for column in columns)
            connection.executemany(
                f"INSERT INTO {table} ({names}) VALUES ({placeholders})",
                ([_stored_value(row.get(column)) for column in columns] for row in rows[table]),
            )
        connection.executemany(
            "INSERT INTO metadata VALUES (?,?)",
            [(key, _canonical_json(value)) for key, value in sorted(metadata.items())],
        )
        connection.execute("PRAGMA user_version=1")
        connection.commit()
        connection.execute("VACUUM")
    finally:
        connection.close()


def _write_artifacts(output: Path, rows: Mapping[str, Sequence[Mapping[str, Any]]], metadata: Mapping[str, Any]) -> list[Path]:
    output.mkdir(parents=True, exist_ok=True)
    database = output / "opportunity_triage_analysis.sqlite"
    _write_database(database, rows, metadata)
    paths = [database]
    for table, columns in EXPORT_TABLES.items():
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
    schema_source = Path(__file__).resolve().parents[2] / "OPPORTUNITY_SCORE_SCHEMA.md"
    schema_path = output / schema_source.name
    schema_path.write_text(schema_source.read_text(encoding="utf-8"), encoding="utf-8", newline="\n")
    paths.append(schema_path)
    return paths


def _output_checks(output: Path, rows: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    connection = sqlite3.connect(output / "opportunity_triage_analysis.sqlite")
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = list(connection.execute("PRAGMA foreign_key_check"))
        table_counts = {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in EXPORT_TABLES
        }
        database_matches = all(table_counts[table] == len(rows[table]) for table in EXPORT_TABLES)
        export_matches = True
        for table, columns in EXPORT_TABLES.items():
            database_columns = tuple(row[1] for row in connection.execute(f"PRAGMA table_info({table})"))
            if database_columns != columns:
                export_matches = False
        return {
            "sqlite_integrity_ok": integrity == "ok",
            "foreign_key_check_ok": not foreign_keys,
            "table_counts": table_counts,
            "table_counts_match": database_matches,
            "table_columns_match_schema": export_matches,
        }
    finally:
        connection.close()


def _range_violations(component_rows: Sequence[Mapping[str, Any]], family_rows: Sequence[Mapping[str, Any]]) -> int:
    fields = ("D", "W", "F", "C", "source_score_balanced", "source_score_demand_first", "source_score_whitespace_first")
    fields += tuple(f"{profile}_{suffix}" for profile in PROFILE_ORDER for suffix in ("core_source_score", "final_score"))
    fields += ("cross_market_validation_score", "voxel_monetization_score", "consensus_score")
    return sum(
        value is not None and not 0 <= value <= 100
        for row in (*component_rows, *family_rows)
        for field in fields
        if (value := row.get(field)) is not None
    )


def _formula_mismatches(
    component_rows: Sequence[Mapping[str, Any]], family_rows: Sequence[Mapping[str, Any]],
    canonical_signal_rows: Sequence[Mapping[str, Any]],
) -> int:
    mismatches = 0
    components_by_source: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    components_by_family: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    canonical_voxel = {
        row["family_id"]: row for row in canonical_signal_rows if row["source"] == "voxel"
    }
    for row in component_rows:
        components_by_source[row["source"]].append(row)
        components_by_family[row["family_id"]][row["source"]] = row
    for family_id, canonical_row in canonical_voxel.items():
        component = components_by_family.get(family_id, {}).get("voxel", {})
        mismatches += sum(component.get(field) != canonical_row.get(field) for field in VOXEL_PAID_COLUMNS)
    for source_rows in components_by_source.values():
        supply_ranks = _midrank_percentiles([float(row["resource_count"]) for row in source_rows])
        ages = [row["freshness_age_days_p50"] for row in source_rows if row["freshness_age_days_p50"] is not None]
        age_ranks = _midrank_percentiles([float(value) for value in ages])
        for row in source_rows:
            expected_d = _demand_strength(row)
            expected_w = round_number(100 * (1 - supply_ranks[float(row["resource_count"])]))
            age = _numeric(row["freshness_age_days_p50"], "freshness_age_days_p50")
            if age is None:
                expected_f = None
            else:
                expected_f = round_number(
                    0.50 * _required(row, "freshness_le90_share") * 100
                    + 0.30 * 100 * (1 - age_ranks[age])
                    + 0.20 * (1 - _required(row, "freshness_gt365_share")) * 100
                )
            hhi = row["demand_concentration_hhi"]
            top1 = row["demand_concentration_top1_share"]
            expected_c = None if hhi is None or top1 is None else round_number(100 * (0.60 * (1 - hhi) + 0.40 * (1 - top1)))
            expected = {"D": expected_d, "W": expected_w, "F": expected_f, "C": expected_c}
            mismatches += sum(row[name] != value for name, value in expected.items())
            mismatches += sum(
                row[f"source_score_{profile}"] != _profile_score(expected, profile)
                for profile in PROFILE_ORDER
            )

    for row in family_rows:
        source_rows = components_by_family[row["family_id"]]
        expected_v = 50 * (int(row["source_presence_count"]) - 1)
        expected_m = _monetization_score({"voxel": canonical_voxel.get(row["family_id"])})
        mismatches += int(row["cross_market_validation_score"] != expected_v)
        mismatches += int(row["voxel_monetization_score"] != expected_m)
        for profile in PROFILE_ORDER:
            scores = {source: source_rows.get(source, {}).get(f"source_score_{profile}") for source in SOURCE_ORDER}
            expected_core, expected_coverage = _weighted_core(scores)
            expected_final = None
            if expected_core is not None and expected_m is None:
                expected_final = round_number((0.80 * expected_core + 0.15 * expected_v) / 0.95)
            elif expected_core is not None:
                expected_final = round_number(0.80 * expected_core + 0.15 * expected_v + 0.05 * expected_m)
            mismatches += int(row[f"{profile}_core_source_score"] != expected_core)
            mismatches += int(row[f"{profile}_source_weight_coverage"] != expected_coverage)
            mismatches += int(row[f"{profile}_final_score"] != expected_final)
        ranks = [row[f"{profile}_rank"] for profile in PROFILE_ORDER]
        mismatches += int(row["median_profile_rank"] != median(ranks))
        mismatches += int(row["mean_profile_rank"] != round_number(sum(ranks) / 3))
        mismatches += int(row["rank_span"] != max(ranks) - min(ranks))
        mismatches += int(row["consensus_score"] != round_number(median([row[f"{profile}_final_score"] for profile in PROFILE_ORDER])))
        mismatches += int(row["triage_bucket"] != _triage_bucket(row["consensus_rank"]))
    return mismatches


def _make_report(qa: Mapping[str, Any]) -> str:
    counts = qa["details"]["output_counts"]
    buckets = qa["details"]["triage_bucket_counts"]
    voxel_qa = qa["details"]["voxel_monetization_qa"]
    voxel_counts = voxel_qa["family_score_actual_counts"]
    return "\n".join((
        "# YEE-46 Opportunity Triage v0 — Final Report",
        "",
        f"Status: **{qa['status']}**",
        "",
        "## Input and reproducibility",
        "",
        f"- Accepted YEE-43 input SHA-256: `{qa['input_sha256_before']}` (after: `{qa['input_sha256_after']}`).",
        f"- Input schema: `{qa['input_schema_version']}`; input tables/counts matched before and after: {qa['checks']['input_contract_unchanged']}.",
        "- The input was opened read-only; no YEE-24/BuiltByBit, external/API/web, or model data was used.",
        "",
        "## Output",
        "",
        f"- Families: {counts['family_opportunity_scores']}; source component rows: {counts['source_opportunity_components']}; shortlist rows: {counts['opportunity_shortlist']}.",
        f"- Buckets: {buckets['ADVANCE_RESEARCH']} ADVANCE_RESEARCH, {buckets['WATCH']} WATCH, {buckets['DEFER']} DEFER.",
        f"- Voxel monetization: {voxel_counts['non_null']} non-null M ({voxel_counts['positive']} positive, {voxel_counts['zero']} zero); canonical field/score mismatches: {voxel_qa['component_paid_field_mismatch_count']}/{voxel_qa['family_score_mismatch_count']}.",
        f"- Deterministic clean replay (SQLite + JSONL/CSV exports): {qa['checks']['deterministic_replay_byte_identical']}.",
        "- SQLite integrity/FK checks and all ranking, reconciliation, range, and boundary checks are recorded in `QA_RESULT.json`.",
        "",
        "## Scope exclusions",
        "",
        "No external research, marketplace/API calls, Jev/LLM inference, manual rank overrides, revenue/profit estimation, or product recommendation was performed.",
        "",
    ))


def _make_rows(
    features: Sequence[Mapping[str, Any]], signal_rows: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    components = compute_source_components(signal_rows)
    scores, shortlist = compute_family_scores(features, components)
    return {
        "family_opportunity_scores": scores,
        "source_opportunity_components": components,
        "opportunity_shortlist": shortlist,
    }


def build_opportunity_triage(input_db: str | Path, output_dir: str | Path) -> dict[str, Any]:
    input_path = Path(input_db).resolve()
    if not input_path.is_file():
        raise FileNotFoundError(input_path)
    output = Path(output_dir).resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"output directory is not empty: {output}")

    input_hash_before = _sha256_file(input_path)
    if input_hash_before != EXPECTED_INPUT_SHA256:
        raise OpportunityInputError(f"accepted YEE-43 input SHA-256 mismatch: {input_hash_before}")
    features, signal_rows, contract_before = _read_input(input_path)
    metadata = {
        "work_order": WORK_ORDER,
        "triage_schema_version": SCHEMA_VERSION,
        "input_schema_version": contract_before["schema_version"],
        "input_sha256": input_hash_before,
        "profile_weights": PROFILE_WEIGHTS,
        "source_weights": SOURCE_WEIGHTS,
        "midrank_semantics": "source-local average rank: (count less + 0.5*(tie count-1))/(N-1); 0.5 when N=1",
        "null_semantics": "missing source-native metrics remain JSON null and CSV \\N; recorded zero remains zero",
        "rank_tie_break": "family_id ascending",
    }
    rows = _make_rows(features, signal_rows)
    if len(rows["family_opportunity_scores"]) != EXPECTED_INPUT_COUNTS["family_features"]:
        raise OpportunityInputError("family score output count does not match accepted YEE-43")

    output.mkdir(parents=True, exist_ok=True)
    artifact_paths = _write_artifacts(output, rows, metadata)
    with tempfile.TemporaryDirectory(prefix="yee46-replay-") as replay_temp:
        replay_path = Path(replay_temp)
        replay_features, replay_signals, replay_contract = _read_input(input_path)
        replay_rows = _make_rows(replay_features, replay_signals)
        _write_artifacts(replay_path, replay_rows, metadata)
        actual_hashes = {path.name: _sha256_file(path) for path in artifact_paths}
        replay_hashes = {name: _sha256_file(replay_path / name) for name in actual_hashes}
        replay_identical = actual_hashes == replay_hashes
        replay_inputs_identical = replay_contract == contract_before and replay_rows == rows

    input_hash_after = _sha256_file(input_path)
    contract_after = _read_input_contract(input_path)
    checks_output = _output_checks(output, rows)
    family_ids = {row["family_id"] for row in features}
    output_family_ids = {row["family_id"] for row in rows["family_opportunity_scores"]}
    source_keys = {(row["family_id"], row["source"]) for row in signal_rows}
    component_keys = {(row["family_id"], row["source"]) for row in rows["source_opportunity_components"]}
    bucket_counts = dict(Counter(row["triage_bucket"] for row in rows["family_opportunity_scores"]))
    expected_buckets = {"ADVANCE_RESEARCH": 100, "WATCH": 200, "DEFER": 1318}
    rank_fields = tuple(f"{profile}_rank" for profile in PROFILE_ORDER) + ("consensus_rank",)
    ranks_valid = all(
        sorted(row[field] for row in rows["family_opportunity_scores"]) == list(range(1, EXPECTED_INPUT_COUNTS["family_features"] + 1))
        for field in rank_fields
    )
    shortlist_valid = [row["consensus_rank"] for row in rows["opportunity_shortlist"]] == list(range(1, 101))
    score_by_id = {row["family_id"]: row for row in rows["family_opportunity_scores"]}
    component_by_key = {
        (row["family_id"], row["source"]): row
        for row in rows["source_opportunity_components"]
    }
    expected_shortlist_ids = [
        row["family_id"]
        for row in sorted(rows["family_opportunity_scores"], key=lambda item: item["consensus_rank"])[:100]
    ]
    shortlist_context_matches = [row["family_id"] for row in rows["opportunity_shortlist"]] == expected_shortlist_ids
    for row in rows["opportunity_shortlist"]:
        shortlist_context_matches = shortlist_context_matches and all(
            row.get(column) == score_by_id[row["family_id"]].get(column)
            for column in FAMILY_SCORE_COLUMNS
        )
        for source in SOURCE_ORDER:
            source_row = component_by_key.get((row["family_id"], source), {})
            shortlist_context_matches = shortlist_context_matches and all(
                row.get(f"{source}_{field}") == source_row.get(field)
                for field in SHORTLIST_SOURCE_FIELDS
            )
    forbidden_fields = {"downloads_total", "total_downloads", "opportunity_score", "recommendation"}
    exported_columns = {column for columns in EXPORT_TABLES.values() for column in columns}
    no_forbidden_fields = not any(
        any(term in column.casefold() for term in forbidden_fields | {"downloads_total"})
        for column in exported_columns
    )
    ranges_valid = _range_violations(rows["source_opportunity_components"], rows["family_opportunity_scores"]) == 0
    formula_mismatch_count = _formula_mismatches(
        rows["source_opportunity_components"], rows["family_opportunity_scores"], signal_rows
    )
    voxel_monetization_qa = _voxel_monetization_qa(
        signal_rows, rows["source_opportunity_components"], rows["family_opportunity_scores"]
    )
    consensus_order = sorted(
        rows["family_opportunity_scores"],
        key=_consensus_order_key,
    )
    consensus_order_matches = all(row["consensus_rank"] == rank for rank, row in enumerate(consensus_order, 1))
    checks = {
        "input_sha256_matches_accepted": input_hash_before == EXPECTED_INPUT_SHA256,
        "input_contract_unchanged": input_hash_after == input_hash_before and contract_after == contract_before,
        "exact_family_count_and_set": len(output_family_ids) == 1618 and output_family_ids == family_ids,
        "exact_source_component_reconciliation": len(rows["source_opportunity_components"]) == 2503 and component_keys == source_keys,
        "component_and_profile_scores_in_range": ranges_valid,
        "score_component_and_aggregation_formulas_reconcile": formula_mismatch_count == 0,
        "voxel_monetization_reconciles_to_canonical_input": voxel_monetization_qa["matches_canonical_input"],
        "voxel_monetization_production_counts_match": voxel_monetization_qa["matches_pinned_production_counts"],
        "ranks_are_unique_1_to_1618": ranks_valid,
        "consensus_order_matches_spec": consensus_order_matches,
        "triage_bucket_boundaries_exact": bucket_counts == expected_buckets,
        "shortlist_is_exact_consensus_top_100": len(rows["opportunity_shortlist"]) == 100 and shortlist_valid,
        "shortlist_context_and_order_reconcile": shortlist_context_matches,
        "no_cross_source_raw_downloads_or_decision_overrides": no_forbidden_fields,
        "sqlite_integrity_check": checks_output["sqlite_integrity_ok"],
        "sqlite_foreign_key_check": checks_output["foreign_key_check_ok"],
        "sqlite_tables_match_exports_and_schema": checks_output["table_counts_match"] and checks_output["table_columns_match_schema"],
        "deterministic_replay_byte_identical": replay_identical and replay_inputs_identical,
    }
    details = {
        "input_contract_before": contract_before,
        "input_contract_after": contract_after,
        "input_table_counts": contract_before["table_counts"],
        "output_counts": {table: len(rows[table]) for table in EXPORT_TABLES},
        "triage_bucket_counts": {bucket: bucket_counts.get(bucket, 0) for bucket in ("ADVANCE_RESEARCH", "WATCH", "DEFER")},
        "source_component_counts": dict(sorted(Counter(row["source"] for row in rows["source_opportunity_components"]).items())),
        "null_profile_score_counts": {
            field: sum(row[field] is None for row in rows["family_opportunity_scores"])
            for field in tuple(f"{profile}_final_score" for profile in PROFILE_ORDER)
        },
        "formula_mismatch_count": formula_mismatch_count,
        "voxel_monetization_qa": voxel_monetization_qa,
        "output_checks": checks_output,
        "replay_artifact_sha256": dict(sorted(actual_hashes.items())),
    }
    qa = {
        "work_order": WORK_ORDER,
        "status": "PASS" if all(checks.values()) else "FAIL",
        "triage_schema_version": SCHEMA_VERSION,
        "input_schema_version": contract_before["schema_version"],
        "input_sha256_before": input_hash_before,
        "input_sha256_after": input_hash_after,
        "checks": checks,
        "details": details,
        "null_semantics": "missing values remain JSON null and CSV \\N; recorded zero remains zero",
        "non_goals_verified": [
            "no API/network/web research", "no BuiltByBit/YEE-24 input", "no Jev/LLM inference",
            "no manual rank overrides", "no revenue/profit estimation", "no product recommendation",
        ],
    }
    (output / "QA_RESULT.json").write_text(_canonical_json(qa) + "\n", encoding="utf-8", newline="\n")
    (output / "FINAL_REPORT.md").write_text(_make_report(qa), encoding="utf-8", newline="\n")
    manifest = {
        "work_order": WORK_ORDER,
        "status": qa["status"],
        "triage_schema_version": SCHEMA_VERSION,
        "input": {
            "file": input_path.name,
            "bytes": input_path.stat().st_size,
            "sha256": input_hash_before,
            "schema_version": contract_before["schema_version"],
            "table_counts": contract_before["table_counts"],
        },
        "output_counts": details["output_counts"],
        "artifact_hash_note": "Manifest lists every deliverable except itself to avoid recursive hashing.",
        "artifacts": [
            {"path": path.name, "bytes": path.stat().st_size, "sha256": _sha256_file(path)}
            for path in sorted(output.iterdir(), key=lambda item: item.name)
            if path.is_file() and path.name != "DATASET_MANIFEST.json"
        ],
    }
    (output / "DATASET_MANIFEST.json").write_text(_canonical_json(manifest) + "\n", encoding="utf-8", newline="\n")
    if qa["status"] != "PASS":
        raise RuntimeError(f"YEE-46 acceptance QA failed; inspect {output / 'QA_RESULT.json'}")
    return {"status": qa["status"], "output_dir": output, "qa": qa, "manifest": manifest}


def _read_input_contract(path: Path) -> dict[str, Any]:
    connection = _read_only_connection(path)
    try:
        return _input_contract(connection)
    finally:
        connection.close()
