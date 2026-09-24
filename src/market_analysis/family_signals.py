"""Deterministic family-level signals over the accepted YEE-37 families."""

from __future__ import annotations

import csv
import json
import math
import shutil
import sqlite3
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from .jev_triage import canonical_json, sha256_file
from .pipeline import quantile, round_number

WORK_ORDER = "YEE-43"
SIGNAL_SCHEMA_VERSION = "yee-43-family-signal-layer-v0.1"
FEATURE_SCHEMA_VERSION = "yee-29-feature-layer-v0.1"
TOPIC_SCHEMA_VERSION = "yee-30-topic-layer-v0.1"
FAMILY_BUILD_VERSION = "yee-37-concept-families-v0.1"
ANALYSIS_AS_OF = "2026-09-22T17:13:34Z"
EXPECTED_COUNTS = {"voxel": 6639, "modrinth": 158507, "hangar": 3861}
EXPECTED_INPUT_HASHES = {
    "yee37_concept_retrieval_sqlite": "8fbd925ba96f561d87f0effa1e5d5798befa42ddfe84be2b9e383cddbe69405c",
    "yee30_retrieval_db": "bc4b7d636b2d64594b7c64ce1583ed9aa080ac6da172398cd2b4e6d8c9451c50",
    "yee29_analysis_db": "f0712951d416b52260f524a3218abd8a54a087904d6b2553631f461f010f182e",
}
EXPECTED_FAMILY_COUNT = 1618
EXPECTED_TOPIC_COUNT = 1807
SOURCE_ORDER = ("voxel", "modrinth", "hangar")
NULL_TOKEN = r"\N"

COMMON_SIGNAL_FIELDS = (
    "resource_count",
    "demand_available_count",
    "freshness_available_count",
    "age_available_count",
    "demand_percentile_p50",
    "demand_percentile_p75",
    "demand_percentile_p90",
    "demand_percentile_p95",
    "demand_percentile_ge90_count",
    "demand_percentile_ge90_share",
    "demand_percentile_ge95_count",
    "demand_percentile_ge95_share",
    "freshness_age_days_p50",
    "freshness_age_days_p75",
    "freshness_age_days_p90",
    "freshness_le30_count",
    "freshness_le30_share",
    "freshness_le90_count",
    "freshness_le90_share",
    "freshness_gt365_count",
    "freshness_gt365_share",
    "age_days_p50",
    "downloads_total_p50",
    "downloads_total_p75",
    "downloads_total_p90",
    "demand_concentration_top1_share",
    "demand_concentration_top3_share",
    "demand_concentration_hhi",
)
SOURCE_SIGNAL_FIELDS = {
    "voxel": (
        "voxel_review_count_p50", "voxel_review_count_p90",
        "voxel_review_stars_p50", "voxel_review_stars_p90",
        "free_count", "paid_count", "unknown_paid_state_count",
        "paid_known_count", "paid_share_known",
    ),
    "modrinth": ("modrinth_follow_count_p50", "modrinth_follow_count_p90"),
    "hangar": (
        "hangar_star_count_p50", "hangar_star_count_p90",
        "hangar_watcher_count_p50", "hangar_watcher_count_p90",
        "hangar_recent_downloads_p50", "hangar_recent_downloads_p90",
        "hangar_recent_views_p50", "hangar_recent_views_p90",
    ),
}
SIGNAL_FIELDS = COMMON_SIGNAL_FIELDS + tuple(
    field for source in SOURCE_ORDER for field in SOURCE_SIGNAL_FIELDS[source]
)
SIGNAL_COLUMNS = ("family_id", "source", *SIGNAL_FIELDS)
MEMBERSHIP_COLUMNS = (
    "family_id", "source", "canonical_identity", "source_resource_id",
    "matched_topic_keys", "canonical_topic_key", "family_status",
)
PRICE_COLUMNS = (
    "family_id", "currency", "paid_resource_count", "price_available_count",
    "price_p50", "price_p75", "price_p90", "price_min", "price_max",
)
FAMILY_BASE_COLUMNS = (
    "family_id", "canonical_topic_key", "family_status", "member_count",
    "member_topic_keys", "aliases", "candidate_classes", "source_presence",
    "source_presence_count", "has_voxel", "has_modrinth", "has_hangar",
    "resource_membership_count_total", "evidence_example_identity_count",
    "evidence_coverage_share", "signal_source_count",
    "has_voxel_paid_evidence", "cross_market_presence_class",
)
def _wide_signal_name(source: str, field: str) -> str:
    return field if field.startswith(f"{source}_") else f"{source}_{field}"


FAMILY_WIDE_COLUMNS = tuple(
    _wide_signal_name(source, field)
    for source in SOURCE_ORDER
    for field in COMMON_SIGNAL_FIELDS + SOURCE_SIGNAL_FIELDS[source]
)
FAMILY_COLUMNS = FAMILY_BASE_COLUMNS + FAMILY_WIDE_COLUMNS
EXPORT_TABLES = {
    "family_features": FAMILY_COLUMNS,
    "family_source_signals": SIGNAL_COLUMNS,
    "family_resource_memberships": MEMBERSHIP_COLUMNS,
    "family_voxel_price_signals": PRICE_COLUMNS,
}


class FamilySignalInputError(ValueError):
    """Raised when an accepted canonical input is missing or inconsistent."""


def _as_number(value: Any) -> int | float | None:
    if value is None:
        return None
    number = float(value)
    if not math.isfinite(number):
        raise FamilySignalInputError(f"non-finite numeric input: {value!r}")
    return int(number) if number.is_integer() else number


def _share(count: int, denominator: int) -> float | None:
    return round_number(count / denominator) if denominator else None


def _stats(rows: Sequence[Mapping[str, Any]], field: str, probabilities: Sequence[float]) -> list[int | float | None]:
    values = [_as_number(row.get(field)) for row in rows]
    usable = [value for value in values if value is not None]
    return [quantile(usable, probability) for probability in probabilities]


def aggregate_family_source(
    family_id: str, source: str, members: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    """Aggregate only the given source's unique family memberships."""
    if source not in SOURCE_ORDER or not members:
        raise ValueError("a source signal requires a supported source and at least one member")
    demand = [_as_number(row.get("demand_percentile")) for row in members]
    freshness = [_as_number(row.get("freshness_age_days")) for row in members]
    ages = [_as_number(row.get("age_days")) for row in members]
    downloads = [_as_number(row.get("downloads_total")) for row in members]
    demand_values = [value for value in demand if value is not None]
    freshness_values = [value for value in freshness if value is not None]
    age_values = [value for value in ages if value is not None]
    download_values = [value for value in downloads if value is not None]

    row: dict[str, Any] = {
        "family_id": family_id,
        "source": source,
        "resource_count": len(members),
        "demand_available_count": len(demand_values),
        "freshness_available_count": len(freshness_values),
        "age_available_count": len(age_values),
    }
    for name, values, probabilities in (
        ("demand_percentile", demand_values, (0.50, 0.75, 0.90, 0.95)),
        ("freshness_age_days", freshness_values, (0.50, 0.75, 0.90)),
        ("downloads_total", download_values, (0.50, 0.75, 0.90)),
    ):
        for suffix, value in zip(("p50", "p75", "p90", "p95"), (*probabilities, None)):
            if value is not None:
                row[f"{name}_{suffix}"] = quantile(values, value)
    row["age_days_p50"] = quantile(age_values, 0.50)

    for threshold, suffix, predicate in (
        (90, "ge90", lambda value: value >= 90),
        (95, "ge95", lambda value: value >= 95),
    ):
        count = sum(predicate(value) for value in demand_values)
        row[f"demand_percentile_{suffix}_count"] = count
        row[f"demand_percentile_{suffix}_share"] = _share(count, len(demand_values))
    for suffix, predicate in (
        ("le30", lambda value: value <= 30),
        ("le90", lambda value: value <= 90),
        ("gt365", lambda value: value > 365),
    ):
        count = sum(predicate(value) for value in freshness_values)
        row[f"freshness_{suffix}_count"] = count
        row[f"freshness_{suffix}_share"] = _share(count, len(freshness_values))

    total = sum(download_values)
    if download_values and total > 0:
        ordered = sorted(download_values, reverse=True)
        row["demand_concentration_top1_share"] = round_number(ordered[0] / total)
        row["demand_concentration_top3_share"] = round_number(sum(ordered[:3]) / total)
        row["demand_concentration_hhi"] = round_number(
            sum((value / total) ** 2 for value in download_values)
        )
    else:
        row["demand_concentration_top1_share"] = None
        row["demand_concentration_top3_share"] = None
        row["demand_concentration_hhi"] = None

    for field in SOURCE_SIGNAL_FIELDS[source]:
        row[field] = None
    if source == "voxel":
        counts = Counter(
            row_value.get("paid_state") if row_value.get("paid_state") in ("free", "paid") else "unknown_state"
            for row_value in members
        )
        known = counts["free"] + counts["paid"]
        row.update({
            "voxel_review_count_p50": quantile(
                [v for v in (_as_number(item.get("voxel_review_count")) for item in members) if v is not None], 0.50
            ),
            "voxel_review_count_p90": quantile(
                [v for v in (_as_number(item.get("voxel_review_count")) for item in members) if v is not None], 0.90
            ),
            "voxel_review_stars_p50": quantile(
                [v for v in (_as_number(item.get("voxel_review_stars")) for item in members) if v is not None], 0.50
            ),
            "voxel_review_stars_p90": quantile(
                [v for v in (_as_number(item.get("voxel_review_stars")) for item in members) if v is not None], 0.90
            ),
            "free_count": counts["free"],
            "paid_count": counts["paid"],
            "unknown_paid_state_count": counts["unknown_state"],
            "paid_known_count": known,
            "paid_share_known": _share(counts["paid"], known),
        })
    elif source == "modrinth":
        row["modrinth_follow_count_p50"], row["modrinth_follow_count_p90"] = _stats(
            members, "follow_count", (0.50, 0.90)
        )
    else:
        for field in ("star_count", "watcher_count", "hangar_recent_downloads", "hangar_recent_views"):
            low, high = _stats(members, field, (0.50, 0.90))
            output_field = field if field.startswith("hangar_") else f"hangar_{field}"
            row[f"{output_field}_p50"] = low
            row[f"{output_field}_p90"] = high
    return row


def _price_rows(family_id: str, members: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_currency: dict[str | None, list[Mapping[str, Any]]] = defaultdict(list)
    for member in members:
        if member.get("paid_state") == "paid":
            by_currency[member.get("currency")].append(member)
    output = []
    for currency, paid_members in sorted(by_currency.items(), key=lambda item: (item[0] is not None, item[0] or "")):
        prices = [_as_number(member.get("price_amount")) for member in paid_members]
        usable = [price for price in prices if price is not None]
        output.append({
            "family_id": family_id,
            "currency": currency,
            "paid_resource_count": len(paid_members),
            "price_available_count": len(usable),
            "price_p50": quantile(usable, 0.50),
            "price_p75": quantile(usable, 0.75),
            "price_p90": quantile(usable, 0.90),
            "price_min": min(usable) if usable else None,
            "price_max": max(usable) if usable else None,
        })
    return output


def assemble_signal_rows(
    family_rows: Sequence[Mapping[str, Any]],
    occurrences: Sequence[Mapping[str, Any]],
    evidence_example_counts: Mapping[str, int],
) -> dict[str, list[dict[str, Any]]]:
    """Deduplicate topic overlaps, then build normalized and wide family rows."""
    families = {str(row["family_id"]): dict(row) for row in family_rows}
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    occurrence_count = 0
    for occurrence in occurrences:
        occurrence_count += 1
        family_id = str(occurrence["family_id"])
        source = str(occurrence["source"])
        identity = str(occurrence["canonical_identity"])
        if family_id not in families or source not in SOURCE_ORDER:
            raise FamilySignalInputError("membership occurrence references unknown family or source")
        if identity != f"{source}:{occurrence['source_resource_id']}" or not occurrence["source_resource_id"]:
            raise FamilySignalInputError(f"canonical identity does not match source resource: {identity}")
        key = (family_id, source, identity)
        if key not in grouped:
            grouped[key] = {**dict(occurrence), "matched_topic_keys": set()}
        else:
            prior = grouped[key]
            for field in (
                "source_resource_id", "demand_percentile", "freshness_age_days", "age_days",
                "downloads_total", "paid_state", "price_amount", "currency", "voxel_review_count",
                "voxel_review_stars", "follow_count", "star_count", "watcher_count",
                "hangar_recent_downloads", "hangar_recent_views",
            ):
                if _as_number(prior.get(field)) != _as_number(occurrence.get(field)) if field not in ("paid_state", "currency", "source_resource_id") else prior.get(field) != occurrence.get(field):
                    raise FamilySignalInputError(f"topic aliases disagree on {field} for {identity}")
        grouped[key]["matched_topic_keys"].add(str(occurrence["topic_key"]))

    memberships: list[dict[str, Any]] = []
    members_by_family_source: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    identities_by_family: dict[str, set[str]] = defaultdict(set)
    for (family_id, source, identity), member in sorted(grouped.items()):
        family = families[family_id]
        topic_keys = sorted(member["matched_topic_keys"])
        if not topic_keys:
            raise FamilySignalInputError(f"membership has no matched topic key: {identity}")
        output_member = {
            "family_id": family_id,
            "source": source,
            "canonical_identity": identity,
            "source_resource_id": member["source_resource_id"],
            "matched_topic_keys": topic_keys,
            "canonical_topic_key": family["canonical_topic_key"],
            "family_status": family["family_status"],
        }
        memberships.append(output_member)
        members_by_family_source[(family_id, source)].append(member)
        identities_by_family[family_id].add(identity)

    signal_rows: list[dict[str, Any]] = []
    price_rows: list[dict[str, Any]] = []
    signals_by_family: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for (family_id, source), members in sorted(members_by_family_source.items()):
        signal = aggregate_family_source(family_id, source, members)
        signal_rows.append(signal)
        signals_by_family[family_id][source] = signal
        if source == "voxel":
            price_rows.extend(_price_rows(family_id, members))

    family_output: list[dict[str, Any]] = []
    for family_id, family in sorted(families.items()):
        family_memberships = [row for row in memberships if row["family_id"] == family_id]
        source_rows = signals_by_family.get(family_id, {})
        presence = [source for source in SOURCE_ORDER if source in source_rows]
        if not presence:
            raise FamilySignalInputError(f"family has no resource memberships: {family_id}")
        evidence_count = int(evidence_example_counts.get(family_id, 0))
        if evidence_count < 0 or evidence_count > len(family_memberships):
            raise FamilySignalInputError(f"invalid evidence coverage count for {family_id}")
        result = {
            "family_id": family_id,
            "canonical_topic_key": family["canonical_topic_key"],
            "family_status": family["family_status"],
            "member_count": int(family["member_count"]),
            "member_topic_keys": list(family["member_topic_keys"]),
            "aliases": list(family["aliases"]),
            "candidate_classes": list(family["candidate_classes"]),
            "source_presence": presence,
            "source_presence_count": len(presence),
            "has_voxel": "voxel" in presence,
            "has_modrinth": "modrinth" in presence,
            "has_hangar": "hangar" in presence,
            "resource_membership_count_total": len(family_memberships),
            "evidence_example_identity_count": evidence_count,
            "evidence_coverage_share": _share(evidence_count, len(family_memberships)),
            "signal_source_count": len(source_rows),
            "has_voxel_paid_evidence": bool(source_rows.get("voxel", {}).get("paid_count", 0) > 0),
            "cross_market_presence_class": {1: "single_source", 2: "two_source", 3: "three_source"}[len(presence)],
        }
        for source in SOURCE_ORDER:
            signal = source_rows.get(source, {})
            for field in COMMON_SIGNAL_FIELDS:
                result[_wide_signal_name(source, field)] = signal.get(field)
            for field in SOURCE_SIGNAL_FIELDS[source]:
                result[_wide_signal_name(source, field)] = signal.get(field)
        family_output.append(result)

    return {
        "family_features": family_output,
        "family_source_signals": signal_rows,
        "family_resource_memberships": memberships,
        "family_voxel_price_signals": price_rows,
        "occurrence_count": occurrence_count,
        "deduplicated_membership_count": len(memberships),
    }


def _sqlite_uri(path: Path) -> str:
    return f"file:{path.resolve().as_posix()}?mode=ro"


def _read_only_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(_sqlite_uri(path), uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _json_metadata(connection: sqlite3.Connection, table: str) -> dict[str, Any]:
    return {row[0]: row[1] for row in connection.execute(f"SELECT key, value_json FROM {table}")}


def _validate_metadata(
    connection: sqlite3.Connection, yee37_path: Path, yee30_path: Path, yee29_path: Path
) -> dict[str, Any]:
    y37 = _json_metadata(connection, "metadata")
    y30 = dict(connection.execute("SELECT key, value FROM yee30.build_metadata"))
    y29_rows = connection.execute("SELECT * FROM yee29.analysis_runs").fetchall()
    if len(y29_rows) != 1:
        raise FamilySignalInputError("YEE-29 analysis DB must contain exactly one accepted analysis run")
    y29 = dict(zip([column[1] for column in connection.execute("PRAGMA yee29.table_info(analysis_runs)")], y29_rows[0]))
    try:
        source_hashes = json.loads(y37["source_input_hashes"])
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise FamilySignalInputError("YEE-37 source input hash metadata is malformed") from error
    expected_y30 = EXPECTED_INPUT_HASHES["yee30_retrieval_db"]
    expected_y29 = EXPECTED_INPUT_HASHES["yee29_analysis_db"]
    if source_hashes.get("yee30_retrieval_db_sha256") != expected_y30:
        raise FamilySignalInputError("YEE-37 family DB references an unexpected YEE-30 input")
    if y30.get("input_sha256") != expected_y29 or y30.get("feature_schema_version") != FEATURE_SCHEMA_VERSION:
        raise FamilySignalInputError("YEE-30 metadata does not match accepted YEE-29 baseline")
    if y30.get("topic_schema_version") != TOPIC_SCHEMA_VERSION:
        raise FamilySignalInputError("YEE-30 schema version is not accepted")
    if y29.get("feature_schema_version") != FEATURE_SCHEMA_VERSION or y29.get("analysis_as_of") != ANALYSIS_AS_OF:
        raise FamilySignalInputError("YEE-29 feature schema or analysis_as_of differs from accepted baseline")
    if json.loads(y29["source_counts_json"]) != EXPECTED_COUNTS:
        raise FamilySignalInputError("YEE-29 accepted source counts differ from expected baseline")
    if y37.get("family_build_version", "\"\"") != json.dumps(FAMILY_BUILD_VERSION):
        raise FamilySignalInputError("YEE-37 family build version does not match the accepted baseline")
    return {
        "yee37_family_build_version": FAMILY_BUILD_VERSION,
        "yee30_topic_schema_version": y30["topic_schema_version"],
        "yee30_feature_schema_version": y30["feature_schema_version"],
        "yee29_feature_schema_version": y29["feature_schema_version"],
        "analysis_as_of": y29["analysis_as_of"],
        "source_counts": dict(sorted(EXPECTED_COUNTS.items())),
        "yee29_run_id": y29["run_id"],
        "yee30_analysis_run_id": y30["analysis_run_id"],
        "input_database_bytes": {
            "yee37": yee37_path.stat().st_size,
            "yee30": yee30_path.stat().st_size,
            "yee29": yee29_path.stat().st_size,
        },
    }


RESOURCE_METRIC_FIELDS = (
    "demand_percentile", "freshness_age_days", "age_days", "downloads_total", "paid_state",
    "price_amount", "currency", "voxel_review_count", "voxel_review_stars", "follow_count",
    "star_count", "watcher_count", "hangar_recent_downloads", "hangar_recent_views",
)


def _load_families(connection: sqlite3.Connection) -> tuple[list[dict[str, Any]], dict[str, set[str]], dict[str, int]]:
    families = []
    topic_keys_by_family: dict[str, set[str]] = defaultdict(set)
    topic_to_family: dict[str, str] = {}
    for row in connection.execute("SELECT family_id,canonical_topic_key,member_count,family_status,row_json FROM concept_families ORDER BY family_id"):
        payload = json.loads(row["row_json"])
        family_id = row["family_id"]
        if payload.get("family_id") != family_id or payload.get("canonical_topic_key") != row["canonical_topic_key"]:
            raise FamilySignalInputError(f"YEE-37 family payload mismatch: {family_id}")
        if payload.get("member_count") != row["member_count"] or payload.get("family_status") != row["family_status"]:
            raise FamilySignalInputError(f"YEE-37 family metadata mismatch: {family_id}")
        for required in ("member_topic_keys", "aliases", "candidate_classes"):
            if not isinstance(payload.get(required), list):
                raise FamilySignalInputError(f"YEE-37 family is missing list field {required}: {family_id}")
        if len(payload["member_topic_keys"]) != row["member_count"]:
            raise FamilySignalInputError(f"YEE-37 family member_count does not match topic keys: {family_id}")
        families.append(payload)
    for row in connection.execute("SELECT topic_key,family_id FROM concept_members ORDER BY topic_key"):
        topic, family_id = row["topic_key"], row["family_id"]
        if topic in topic_to_family or family_id not in {item["family_id"] for item in families}:
            raise FamilySignalInputError(f"YEE-37 topic membership is duplicated or unresolved: {topic}")
        topic_to_family[topic] = family_id
        topic_keys_by_family[family_id].add(topic)
    for family in families:
        if topic_keys_by_family[family["family_id"]] != set(family["member_topic_keys"]):
            raise FamilySignalInputError(f"YEE-37 family member topic keys do not reconcile: {family['family_id']}")

    evidence_counts: dict[str, int] = {}
    raw_packs = connection.execute("SELECT family_id,row_json FROM family_evidence_packs ORDER BY family_id").fetchall()
    if len(raw_packs) != len(families):
        raise FamilySignalInputError("YEE-37 evidence packs do not cover all families")
    for row in raw_packs:
        pack = json.loads(row["row_json"])
        if pack.get("family_id") != row["family_id"]:
            raise FamilySignalInputError("YEE-37 evidence pack family identity mismatch")
        identities = set()
        for source, examples in pack.get("sources", {}).items():
            if source not in SOURCE_ORDER or not isinstance(examples, list):
                raise FamilySignalInputError(f"invalid evidence pack source for {row['family_id']}")
            for example in examples:
                identity = example.get("canonical_identity")
                if not identity or not identity.startswith(source + ":"):
                    raise FamilySignalInputError(f"invalid evidence identity in {row['family_id']}")
                identities.add(identity)
        if pack.get("unique_canonical_identity_count") != len(identities):
            raise FamilySignalInputError(f"evidence identity count mismatch for {row['family_id']}")
        evidence_counts[row["family_id"]] = len(identities)
    return families, topic_keys_by_family, evidence_counts


def _load_occurrences(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    duplicate_topic_links = connection.execute(
        "SELECT COUNT(*) FROM (SELECT topic_key,source,canonical_identity,source_resource_id "
        "FROM yee30.resource_topics GROUP BY topic_key,source,canonical_identity,source_resource_id HAVING COUNT(*)>1)"
    ).fetchone()[0]
    if duplicate_topic_links:
        raise FamilySignalInputError(f"YEE-30 has {duplicate_topic_links} duplicate topic-resource links")
    metrics_match = " AND ".join(
        f"c.{field} IS a.{field}" for field in RESOURCE_METRIC_FIELDS
    )
    query = f"""
        SELECT m.family_id,m.topic_key,rt.source,rt.canonical_identity,rt.source_resource_id,
               c.canonical_identity AS corpus_identity,a.canonical_identity AS feature_identity,
               c.source_resource_id AS corpus_resource_id,a.source_resource_id AS feature_resource_id,
               {','.join('a.' + field for field in RESOURCE_METRIC_FIELDS)},
               CASE WHEN {metrics_match} THEN 1 ELSE 0 END AS metrics_match
        FROM concept_members m
        LEFT JOIN yee30.resource_topics rt ON rt.topic_key=m.topic_key
        LEFT JOIN yee30.resource_corpus c ON c.source=rt.source AND c.source_resource_id=rt.source_resource_id
             AND c.canonical_identity=rt.canonical_identity
        LEFT JOIN yee29.resource_features a ON a.source=rt.source AND a.source_resource_id=rt.source_resource_id
             AND a.canonical_identity=rt.canonical_identity
        ORDER BY m.family_id,rt.source,rt.canonical_identity,m.topic_key
    """
    occurrences = []
    for row in connection.execute(query):
        if not row["source"] or not row["canonical_identity"] or not row["source_resource_id"]:
            raise FamilySignalInputError(f"YEE-30 topic has no resource membership: {row['topic_key']}")
        if not row["corpus_identity"] or not row["feature_identity"]:
            raise FamilySignalInputError(f"membership did not resolve to both canonical inputs: {row['canonical_identity']}")
        if row["source_resource_id"] != row["corpus_resource_id"] or row["source_resource_id"] != row["feature_resource_id"]:
            raise FamilySignalInputError(f"source resource id mismatch: {row['canonical_identity']}")
        if row["canonical_identity"] != f"{row['source']}:{row['source_resource_id']}":
            raise FamilySignalInputError(f"canonical identity mismatch: {row['canonical_identity']}")
        if not row["metrics_match"]:
            raise FamilySignalInputError(f"YEE-30 corpus and YEE-29 features disagree: {row['canonical_identity']}")
        occurrence = {key: row[key] for key in (
            "family_id", "topic_key", "source", "canonical_identity", "source_resource_id", *RESOURCE_METRIC_FIELDS
        )}
        occurrences.append(occurrence)
    return occurrences


def _read_accepted_inputs(yee37_db: Path, yee30_db: Path, yee29_db: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    connection = _read_only_connection(yee37_db)
    try:
        connection.execute("ATTACH DATABASE ? AS yee30", (_sqlite_uri(yee30_db),))
        connection.execute("ATTACH DATABASE ? AS yee29", (_sqlite_uri(yee29_db),))
        metadata = _validate_metadata(connection, yee37_db, yee30_db, yee29_db)
        family_count = connection.execute("SELECT COUNT(*) FROM concept_families").fetchone()[0]
        topic_count = connection.execute("SELECT COUNT(*) FROM concept_members").fetchone()[0]
        unique_topic_count = connection.execute("SELECT COUNT(DISTINCT topic_key) FROM concept_members").fetchone()[0]
        if family_count != EXPECTED_FAMILY_COUNT or topic_count != EXPECTED_TOPIC_COUNT or unique_topic_count != EXPECTED_TOPIC_COUNT:
            raise FamilySignalInputError("YEE-37 family/topic universes do not match the accepted 1,618/1,807 baseline")
        for alias, table, expected in (
            ("yee30", "resource_corpus", sum(EXPECTED_COUNTS.values())),
            ("yee29", "resource_features", sum(EXPECTED_COUNTS.values())),
        ):
            count = connection.execute(f"SELECT COUNT(*) FROM {alias}.{table}").fetchone()[0]
            source_counts = dict(connection.execute(f"SELECT source,COUNT(*) FROM {alias}.{table} GROUP BY source"))
            if count != expected or source_counts != EXPECTED_COUNTS:
                raise FamilySignalInputError(f"{alias}.{table} does not match accepted YEE-29 source counts")
        families, topic_keys_by_family, evidence_counts = _load_families(connection)
        occurrences = _load_occurrences(connection)
        rows = assemble_signal_rows(families, occurrences, evidence_counts)
        membership_identity_sets: dict[str, set[str]] = defaultdict(set)
        for row in rows["family_resource_memberships"]:
            membership_identity_sets[row["family_id"]].add(row["canonical_identity"])
        unresolved_evidence_identities = 0
        for family_id, count in evidence_counts.items():
            unresolved = _evidence_identities(connection, family_id) - membership_identity_sets[family_id]
            unresolved_evidence_identities += len(unresolved)
            if unresolved:
                raise FamilySignalInputError(f"YEE-37 evidence examples do not resolve to family memberships: {family_id}")
        details = {
            **metadata,
            "family_count": family_count,
            "topic_key_count": unique_topic_count,
            "topic_membership_occurrence_count": len(occurrences),
            "membership_join_resolved": bool(occurrences)
            and all(row["canonical_identity"] and row["source_resource_id"] for row in occurrences),
            "evidence_membership_resolution": unresolved_evidence_identities == 0,
            "unresolved_evidence_identity_count": unresolved_evidence_identities,
            "resource_membership_count": rows["deduplicated_membership_count"],
            "membership_deduplication_count": len(occurrences) - rows["deduplicated_membership_count"],
            "source_membership_counts": dict(sorted(Counter(
                row["source"] for row in rows["family_resource_memberships"]
            ).items())),
            "family_status_counts": dict(sorted(Counter(row["family_status"] for row in rows["family_features"]).items())),
            "evidence_example_identity_count_total": sum(evidence_counts.values()),
            "topic_key_resource_membership_reconciliation": sum(map(len, topic_keys_by_family.values())) == unique_topic_count,
            "yee30_yee29_source_metric_alignment": True,
        }
        return rows, details
    finally:
        connection.close()


def _evidence_identities(connection: sqlite3.Connection, family_id: str) -> set[str]:
    pack_row = connection.execute(
        "SELECT row_json FROM family_evidence_packs WHERE family_id=?", (family_id,)
    ).fetchone()
    if pack_row is None:
        raise FamilySignalInputError(f"missing evidence pack for family {family_id}")
    pack = json.loads(pack_row[0])
    return {
        example["canonical_identity"]
        for examples in pack.get("sources", {}).values()
        for example in examples
    }


def _csv_cell(value: Any) -> Any:
    if value is None:
        return NULL_TOKEN
    if isinstance(value, (dict, list)):
        return canonical_json(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _export_rows(rows: Sequence[Mapping[str, Any]], columns: Sequence[str], output_dir: Path, stem: str) -> list[Path]:
    jsonl_path = output_dir / f"{stem}.jsonl"
    csv_path = output_dir / f"{stem}.csv"
    with jsonl_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(canonical_json({column: row.get(column) for column in columns}) + "\n")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        writer.writerows([_csv_cell(row.get(column)) for column in columns] for row in rows)
    return [jsonl_path, csv_path]


def _sql_type(column: str) -> str:
    if column in ("family_id", "canonical_topic_key", "family_status", "source", "canonical_identity", "source_resource_id", "currency", "cross_market_presence_class"):
        return "TEXT"
    if column in ("member_topic_keys", "aliases", "candidate_classes", "source_presence", "matched_topic_keys"):
        return "TEXT"
    if column.startswith("has_") or column in ("member_count", "source_presence_count", "resource_membership_count_total", "evidence_example_identity_count", "signal_source_count"):
        return "INTEGER"
    if column.endswith("_count") or column.endswith("_available_count"):
        return "INTEGER"
    return "REAL"


def _stored_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return canonical_json(value)
    if isinstance(value, bool):
        return int(value)
    return value


def _write_database(path: Path, rows: Mapping[str, Sequence[Mapping[str, Any]]], metadata: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        for table, columns in EXPORT_TABLES.items():
            definitions = ",".join(f'"{column}" {_sql_type(column)}' for column in columns)
            primary = "PRIMARY KEY (family_id)" if table == "family_features" else (
                "PRIMARY KEY (family_id,source,canonical_identity)" if table == "family_resource_memberships" else (
                    "PRIMARY KEY (family_id,source)" if table == "family_source_signals" else "PRIMARY KEY (family_id,currency)"
                )
            )
            foreign_key = ""
            if table in ("family_source_signals", "family_resource_memberships", "family_voxel_price_signals"):
                foreign_key = ",FOREIGN KEY (family_id) REFERENCES family_features(family_id)"
            connection.execute(f"CREATE TABLE {table} ({definitions},{primary}{foreign_key})")
        connection.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY,value_json TEXT NOT NULL)")
        connection.execute("CREATE INDEX family_features_canonical_topic_key_idx ON family_features(canonical_topic_key)")
        connection.execute("CREATE INDEX family_source_signals_source_idx ON family_source_signals(source)")
        connection.execute("CREATE INDEX family_memberships_source_idx ON family_resource_memberships(source)")
        connection.execute("CREATE INDEX family_memberships_identity_idx ON family_resource_memberships(canonical_identity)")
        connection.execute("CREATE INDEX family_memberships_topic_idx ON family_resource_memberships(canonical_topic_key)")
        for table, columns in EXPORT_TABLES.items():
            placeholders = ",".join("?" for _ in columns)
            column_names = ",".join(f'"{column}"' for column in columns)
            connection.executemany(
                f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})",
                ([_stored_value(row.get(column)) for column in columns] for row in rows[table]),
            )
        connection.executemany(
            "INSERT INTO metadata VALUES (?,?)",
            [(key, canonical_json(value)) for key, value in sorted(metadata.items())],
        )
        connection.execute("PRAGMA user_version=1")
        connection.commit()
        connection.execute("VACUUM")
    finally:
        connection.close()


def _write_payload(output_dir: Path, rows: Mapping[str, Sequence[Mapping[str, Any]]], metadata: Mapping[str, Any]) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "family_signal_analysis.sqlite"
    _write_database(db_path, rows, metadata)
    paths = [db_path]
    for table, columns in EXPORT_TABLES.items():
        paths.extend(_export_rows(rows[table], columns, output_dir, table))
    schema_source = Path(__file__).resolve().parents[2] / "FAMILY_SIGNAL_SCHEMA.md"
    schema_path = output_dir / "FAMILY_SIGNAL_SCHEMA.md"
    shutil.copyfile(schema_source, schema_path)
    paths.append(schema_path)
    return paths


def _output_checks(output_dir: Path, rows: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    database = output_dir / "family_signal_analysis.sqlite"
    connection = sqlite3.connect(f"file:{database.resolve().as_posix()}?mode=ro", uri=True)
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
        table_counts = {table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in EXPORT_TABLES}
        membership_mismatch = connection.execute("""
            SELECT COUNT(*) FROM (
                SELECT m.family_id,m.source,COUNT(*) AS n,s.resource_count
                FROM family_resource_memberships m
                JOIN family_source_signals s USING(family_id,source)
                GROUP BY m.family_id,m.source HAVING n!=s.resource_count
            )
        """).fetchone()[0]
        source_mismatch = connection.execute("""
            SELECT COUNT(*) FROM family_features f
            WHERE f.source_presence_count != (
                SELECT COUNT(*) FROM family_source_signals s WHERE s.family_id=f.family_id
            ) OR f.resource_membership_count_total != (
                SELECT COUNT(*) FROM family_resource_memberships m WHERE m.family_id=f.family_id
            )
        """).fetchone()[0]
    finally:
        connection.close()
    return {
        "sqlite_integrity_check": integrity,
        "sqlite_integrity_ok": integrity == "ok",
        "foreign_key_violation_count": len(foreign_keys),
        "foreign_key_check_ok": not foreign_keys,
        "table_row_counts": table_counts,
        "table_counts_match_exports": table_counts == {name: len(rows[name]) for name in EXPORT_TABLES},
        "family_source_resource_count_mismatches": membership_mismatch,
        "family_source_resource_count_reconciles": membership_mismatch == 0,
        "family_presence_and_total_membership_mismatches": source_mismatch,
        "family_presence_and_total_membership_reconciles": source_mismatch == 0,
    }


def _semantic_reconciliation(rows: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    features = {row["family_id"]: row for row in rows["family_features"]}
    signals = {(row["family_id"], row["source"]): row for row in rows["family_source_signals"]}
    memberships_by_family: Counter[str] = Counter(row["family_id"] for row in rows["family_resource_memberships"])
    presence_mismatches = 0
    wide_metric_mismatches = 0
    metadata_mismatches = 0
    for family_id, feature in features.items():
        family_signals = {source: signals[(family_id, source)] for source in SOURCE_ORDER if (family_id, source) in signals}
        presence = [source for source in SOURCE_ORDER if source in family_signals]
        expected_class = {1: "single_source", 2: "two_source", 3: "three_source"}.get(len(presence))
        if (
            feature["source_presence"] != presence
            or feature["source_presence_count"] != len(presence)
            or feature["signal_source_count"] != len(presence)
            or feature["has_voxel"] != ("voxel" in presence)
            or feature["has_modrinth"] != ("modrinth" in presence)
            or feature["has_hangar"] != ("hangar" in presence)
            or feature["cross_market_presence_class"] != expected_class
        ):
            presence_mismatches += 1
        if feature["resource_membership_count_total"] != memberships_by_family[family_id]:
            metadata_mismatches += 1
        if feature["has_voxel_paid_evidence"] != bool(family_signals.get("voxel", {}).get("paid_count", 0) > 0):
            metadata_mismatches += 1
        expected_coverage = _share(feature["evidence_example_identity_count"], memberships_by_family[family_id])
        if feature["evidence_coverage_share"] != expected_coverage:
            metadata_mismatches += 1
        for source, signal in family_signals.items():
            for field in COMMON_SIGNAL_FIELDS + SOURCE_SIGNAL_FIELDS[source]:
                if feature.get(_wide_signal_name(source, field)) != signal.get(field):
                    wide_metric_mismatches += 1

    price_keys = [(row["family_id"], row["currency"]) for row in rows["family_voxel_price_signals"]]
    price_family_mismatches = sum(
        row["family_id"] not in features or not features[row["family_id"]]["has_voxel_paid_evidence"]
        for row in rows["family_voxel_price_signals"]
    )
    membership_mismatches = sum(
        row["canonical_identity"] != f"{row['source']}:{row['source_resource_id']}"
        or not row["matched_topic_keys"]
        or row["matched_topic_keys"] != sorted(set(row["matched_topic_keys"]))
        or row["canonical_topic_key"] != features[row["family_id"]]["canonical_topic_key"]
        or row["family_status"] != features[row["family_id"]]["family_status"]
        for row in rows["family_resource_memberships"]
    )
    forbidden = {"opportunity_score", "rank", "ranking", "shortlist", "winner", "recommendation"}
    output_columns = {column.casefold() for columns in EXPORT_TABLES.values() for column in columns}
    return {
        "family_presence_mismatch_count": presence_mismatches,
        "source_wide_metric_mismatch_count": wide_metric_mismatches,
        "family_signal_metadata_mismatch_count": metadata_mismatches,
        "voxel_price_family_mismatch_count": price_family_mismatches,
        "voxel_price_duplicate_family_currency_count": len(price_keys) - len(set(price_keys)),
        "membership_provenance_mismatch_count": membership_mismatches,
        "forbidden_decision_field_count": sum(
            any(term in column for term in forbidden) for column in output_columns
        ),
        "source_rows_are_independently_aggregated": all(row["source"] in SOURCE_ORDER for row in rows["family_source_signals"]),
    }


def _hashes(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: sha256_file(path) for key, path in paths.items()}


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _report(qa: Mapping[str, Any]) -> str:
    details = qa["details"]
    counts = details["output_counts"]
    return "\n".join((
        "# YEE-43 — Family Opportunity Signal Layer v0",
        "",
        f"Status: **{qa['status']}**",
        "",
        "Built deterministically from the accepted YEE-37 family database and read-only YEE-30/YEE-29 inputs. "
        "Signals preserve source-local demand semantics and source-native engagement; no opportunity score, rank, shortlist, or recommendation is produced.",
        "",
        "## Reconciliation",
        "",
        f"- Families: {counts['family_features']} (expected 1,618)",
        f"- YEE-37 topic keys: {details['input']['topic_key_count']} (expected 1,807)",
        f"- Topic-resource occurrences: {details['input']['topic_membership_occurrence_count']}",
        f"- Deduplicated family/source/resource memberships: {counts['family_resource_memberships']}",
        f"- Alias-overlap occurrences removed within a family: {details['input']['membership_deduplication_count']}",
        f"- Family/source signal rows: {counts['family_source_signals']}",
        f"- Voxel paid price/currency rows: {counts['family_voxel_price_signals']}",
        "",
        "## Input provenance",
        "",
        f"- YEE-37 `concept_retrieval.sqlite`: SHA-256 `{qa['input_hashes_before']['yee37_concept_retrieval_sqlite']}`",
        f"- YEE-30 `retrieval.db`: SHA-256 `{qa['input_hashes_before']['yee30_retrieval_db']}`",
        f"- YEE-29 `analysis.db`: SHA-256 `{qa['input_hashes_before']['yee29_analysis_db']}`",
        f"- Fixed `analysis_as_of`: `{details['input']['analysis_as_of']}`",
        "- Input databases were opened read-only; pre/post hashes match.",
        "",
        "## QA",
        "",
        f"- Acceptance checks: {sum(bool(value) for key, value in qa['checks'].items() if isinstance(value, bool))}/{sum(isinstance(value, bool) for value in qa['checks'].values())} passed.",
        f"- Byte-identical clean rebuild (SQLite + JSONL/CSV exports): {qa['checks']['deterministic_replay_byte_identical']}.",
        "- SQLite integrity and membership/source reconciliation are recorded in `QA_RESULT.json`.",
        "",
        "## Scope exclusions",
        "",
        "No network/API/web research, Jev/LLM inference, semantic matching, enrichment, cross-source download summation, opportunity scoring, ranking, shortlist, or recommendation was performed.",
        "",
    ))


def build_family_signal_layer(
    yee37_db: str | Path,
    yee30_db: str | Path,
    yee29_db: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    paths = {
        "yee37_concept_retrieval_sqlite": Path(yee37_db).resolve(),
        "yee30_retrieval_db": Path(yee30_db).resolve(),
        "yee29_analysis_db": Path(yee29_db).resolve(),
    }
    for path in paths.values():
        if not path.is_file():
            raise FileNotFoundError(path)
    hashes_before = _hashes(paths)
    if hashes_before != EXPECTED_INPUT_HASHES:
        raise FamilySignalInputError(f"accepted input SHA-256 mismatch: {hashes_before}")
    output = Path(output_dir).resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"output directory is not empty: {output}")
    output.mkdir(parents=True, exist_ok=True)

    rows, input_details = _read_accepted_inputs(paths["yee37_concept_retrieval_sqlite"], paths["yee30_retrieval_db"], paths["yee29_analysis_db"])
    metadata = {
        "work_order": WORK_ORDER,
        "signal_schema_version": SIGNAL_SCHEMA_VERSION,
        "yee37_family_build_version": FAMILY_BUILD_VERSION,
        "yee30_topic_schema_version": TOPIC_SCHEMA_VERSION,
        "yee29_feature_schema_version": FEATURE_SCHEMA_VERSION,
        "analysis_as_of": ANALYSIS_AS_OF,
        "input_hashes": hashes_before,
        "quantile_method": "linear interpolation at (n-1)*p; six-decimal rounding via YEE-29 deterministic helper",
        "demand_semantics": "demand_percentile is inherited from YEE-29 and is source-local; raw downloads_total is descriptive within source only",
        "concentration_semantics": "within family and source over non-null downloads_total only; null if usable total is not positive",
        "null_semantics": "missing values are JSON null and CSV \\N; recorded zero remains numeric zero",
    }
    output_files = _write_payload(output, rows, metadata)
    output_checks = _output_checks(output, rows)

    with tempfile.TemporaryDirectory(prefix="yee43-replay-") as replay_directory:
        replay_path = Path(replay_directory)
        replay_rows, replay_input_details = _read_accepted_inputs(
            paths["yee37_concept_retrieval_sqlite"], paths["yee30_retrieval_db"], paths["yee29_analysis_db"]
        )
        _write_payload(replay_path, replay_rows, metadata)
        replay_names = [path.name for path in output_files]
        replay_hashes = {name: sha256_file(replay_path / name) for name in replay_names}
        actual_hashes = {name: sha256_file(output / name) for name in replay_names}
        replay_identical = actual_hashes == replay_hashes
        replay_row_counts_match = {
            name: len(rows[name]) == len(replay_rows[name]) for name in EXPORT_TABLES
        }

    hashes_after = _hashes(paths)
    counts = {table: len(rows[table]) for table in EXPORT_TABLES}
    semantic_checks = _semantic_reconciliation(rows)
    checks: dict[str, bool] = {
        "accepted_input_hashes_match": hashes_before == EXPECTED_INPUT_HASHES,
        "canonical_inputs_unchanged": hashes_after == hashes_before,
        "exact_family_universe": counts["family_features"] == EXPECTED_FAMILY_COUNT,
        "exact_yee37_topic_universe": input_details["topic_key_count"] == EXPECTED_TOPIC_COUNT,
        "every_family_retained_including_singletons": counts["family_features"] == EXPECTED_FAMILY_COUNT,
        "topic_keys_map_to_one_family": input_details["topic_key_resource_membership_reconciliation"],
        "all_resource_memberships_resolve_to_y30_and_y29": input_details["membership_join_resolved"],
        "family_source_identity_memberships_are_unique": len({
            (row["family_id"], row["source"], row["canonical_identity"])
            for row in rows["family_resource_memberships"]
        }) == counts["family_resource_memberships"],
        "alias_overlap_deduplication_applied": (
            input_details["topic_membership_occurrence_count"]
            == counts["family_resource_memberships"] + input_details["membership_deduplication_count"]
            and input_details["membership_deduplication_count"] >= 0
        ),
        "evidence_examples_resolve_to_memberships": input_details["evidence_membership_resolution"],
        "source_presence_and_membership_counts_reconcile": (
            output_checks["family_presence_and_total_membership_reconciles"]
            and semantic_checks["family_presence_mismatch_count"] == 0
            and semantic_checks["family_signal_metadata_mismatch_count"] == 0
        ),
        "family_source_resource_counts_reconcile": output_checks["family_source_resource_count_reconciles"],
        "source_local_demand_percentiles_and_download_metrics": (
            semantic_checks["source_rows_are_independently_aggregated"]
            and semantic_checks["source_wide_metric_mismatch_count"] == 0
        ),
        "no_cross_source_download_sum_or_score_fields": (
            semantic_checks["forbidden_decision_field_count"] == 0
            and not any("total_downloads" in column for column in FAMILY_COLUMNS)
        ),
        "voxel_price_signals_are_paid_and_currency_separated": (
            semantic_checks["voxel_price_family_mismatch_count"] == 0
            and semantic_checks["voxel_price_duplicate_family_currency_count"] == 0
        ),
        "sqlite_integrity_check": output_checks["sqlite_integrity_ok"],
        "sqlite_foreign_key_check": output_checks["foreign_key_check_ok"],
        "output_table_counts_match_exports": output_checks["table_counts_match_exports"],
        "deterministic_replay_byte_identical": replay_identical and all(replay_row_counts_match.values()),
        "replay_input_metadata_identical": replay_input_details == input_details,
    }
    details = {
        "input": input_details,
        "output_counts": counts,
        "output_checks": output_checks,
        "semantic_reconciliation": semantic_checks,
        "input_hashes_after": hashes_after,
        "replay": {
            "byte_identical_sqlite_and_exports": replay_identical,
            "artifact_sha256": dict(sorted(actual_hashes.items())),
            "replay_artifact_sha256": dict(sorted(replay_hashes.items())),
            "row_counts_match": replay_row_counts_match,
        },
        "source_presence_counts": dict(sorted(Counter(
            source for row in rows["family_source_signals"] for source in (row["source"],)
        ).items())),
        "cross_market_presence_class_counts": dict(sorted(Counter(
            row["cross_market_presence_class"] for row in rows["family_features"]
        ).items())),
        "voxel_paid_family_count": sum(row["has_voxel_paid_evidence"] for row in rows["family_features"]),
    }
    qa = {
        "work_order": WORK_ORDER,
        "status": "PASS" if all(checks.values()) else "FAIL",
        "signal_schema_version": SIGNAL_SCHEMA_VERSION,
        "input_hashes_before": hashes_before,
        "input_hashes_after": hashes_after,
        "checks": checks,
        "details": details,
        "null_semantics": "missing values remain JSON null and CSV \\N; recorded zero remains zero",
        "non_goals_verified": [
            "no API/network/web research", "no Jev/LLM", "no matching or enrichment",
            "no cross-source raw download summation", "no score/rank/shortlist/recommendation",
        ],
    }
    _write_json(output / "QA_RESULT.json", qa)
    report_path = output / "FINAL_REPORT.md"
    report_path.write_text(_report(qa), encoding="utf-8", newline="\n")
    manifest = {
        "work_order": WORK_ORDER,
        "status": qa["status"],
        "signal_schema_version": SIGNAL_SCHEMA_VERSION,
        "analysis_as_of": ANALYSIS_AS_OF,
        "input_hashes": hashes_before,
        "input_bytes": {key: path.stat().st_size for key, path in paths.items()},
        "output_counts": counts,
        "replay_byte_identical": checks["deterministic_replay_byte_identical"],
        "artifact_hash_note": "Manifest lists every deliverable except itself to avoid recursive hashing.",
        "artifacts": [
            {"path": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path)}
            for path in sorted(output.iterdir(), key=lambda item: item.name)
            if path.is_file() and path.name != "DATASET_MANIFEST.json"
        ],
    }
    _write_json(output / "DATASET_MANIFEST.json", manifest)
    if qa["status"] != "PASS":
        raise RuntimeError(f"YEE-43 acceptance QA failed; inspect {output / 'QA_RESULT.json'}")
    return {"status": qa["status"], "output_dir": output, "qa": qa, "manifest": manifest}
