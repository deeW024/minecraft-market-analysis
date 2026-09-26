"""Deterministic YEE-73 source-aware category signal layer."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import sqlite3
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .pipeline import quantile


WORK_ORDER = "YEE-73"
SIGNAL_SCHEMA_VERSION = "yee-73-category-signal-layer-v0.1"
DELIVERY_STATUS = "CATEGORY_SIGNAL_LAYER_READY_FOR_SUPERVISOR_REVIEW"
YEE61_MERGE_COMMIT = "a0b838f525017e6d132005778cd9a468fae92564"
YEE61_DB_SHA256 = "12fa10a45a98df954c220f741a21ae8c8ed537a953415954e71fdab23bdd17ed"
YEE61_MEMBERSHIPS_SHA256 = "e4254b9ccdb4e051036b3a7ce46f5a6b59b42be578e2fe0d5a66c5af988a993b"
YEE61_SCOPE_SHA256 = "432968f99bf5b80cf7703fb88ad6141ba8504608564364db80e86c0cd7c85894"
YEE61_TAXONOMY_EXPORT_SHA256 = "d28df5cf55f0ccd77660fb3dc62db6da0c505ed38a7e1a1e2b0bf339fd156e67"
YEE61_TAXONOMY_VERSION = "yee-61-functional-category-taxonomy-v0.1"
YEE61_TAXONOMY_SHA256 = "b5720325dea06863408dfa1a05e2f981ecfeb3fac4a9e7266083ee17839c5126"
YEE61_SCHEMA_VERSION = "yee-61-category-first-foundation-v0.1"
ANALYSIS_AS_OF = "2026-09-22T17:13:34Z"
CSV_NULL = r"\N"
SOURCES = ("hangar", "voxel")
SCOPE_STATUSES = (
    "PLUGIN_PRODUCT_CONFIRMED",
    "PLUGIN_PRODUCT_REVIEW",
    "OUT_OF_SCOPE_PRODUCT_FORM",
)
EXPECTED_SCOPE_COUNTS = {
    "hangar": {"PLUGIN_PRODUCT_CONFIRMED": 1221, "PLUGIN_PRODUCT_REVIEW": 2600, "OUT_OF_SCOPE_PRODUCT_FORM": 40},
    "voxel": {"PLUGIN_PRODUCT_CONFIRMED": 131, "PLUGIN_PRODUCT_REVIEW": 4633, "OUT_OF_SCOPE_PRODUCT_FORM": 1393},
}
EXPECTED_PRIMARY_COUNTS = {
    "administration": 345,
    "communication": 97,
    "developer_tools": 72,
    "economy": 61,
    "gameplay": 477,
    "minigames": 26,
    "protection": 58,
    "roleplay": 20,
    "server_utilities": 0,
    "uncategorized": 136,
    "world_management": 60,
}
PRICE_BANDS = ("free", ">0-5", ">5-10", ">10-20", ">20-50", ">50", "unknown")
FRESHNESS_BINS = ("<=30d", "31-90d", "91-365d", ">365d", "unknown")
TABLE_FILES = {
    "source_scope_coverage": "source_scope_coverage",
    "category_signal_member_features": "category_signal_member_features",
    "category_source_facts": "category_source_facts",
    "subcategory_source_facts": "subcategory_source_facts",
    "category_signal_overview": "category_signal_overview",
}
PRIMARY_KEYS = {
    "source_scope_coverage": ("source",),
    "category_signal_member_features": ("source", "source_resource_id"),
    "category_source_facts": ("category_id", "source"),
    "subcategory_source_facts": ("category_id", "subcategory_id", "source"),
    "category_signal_overview": ("category_id",),
}
ORDER_BY = {
    "source_scope_coverage": ("source",),
    "category_signal_member_features": ("source", "source_resource_id"),
    "category_source_facts": ("category_order", "source"),
    "subcategory_source_facts": ("category_order", "subcategory_order", "source"),
    "category_signal_overview": ("category_order",),
}
EXPORTS = (
    "GOAL_ALIGNMENT.md",
    "CATEGORY_SIGNAL_SCHEMA.md",
    "SIGNAL_SEMANTICS.md",
    "source_scope_coverage.jsonl",
    "source_scope_coverage.csv",
    "category_signal_member_features.jsonl",
    "category_signal_member_features.csv",
    "category_source_facts.jsonl",
    "category_source_facts.csv",
    "subcategory_source_facts.jsonl",
    "subcategory_source_facts.csv",
    "category_signal_overview.jsonl",
    "category_signal_overview.csv",
    "category_signal_layer.sqlite",
    "QA_RESULT.json",
    "FINAL_REPORT.md",
)
ALL_ARTIFACTS = (*EXPORTS, "DATASET_MANIFEST.json")


class CategorySignalError(ValueError):
    """The accepted YEE-61 input or generated YEE-73 layer failed its contract."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_only_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    return connection


def _loads(value: str | None, field: str) -> Any:
    if value is None:
        raise CategorySignalError(f"Required YEE-61 JSON field is null: {field}")
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise CategorySignalError(f"Malformed YEE-61 JSON field: {field}") from exc


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return number


def _round(value: float | int | None) -> float | int | None:
    if value is None:
        return None
    rounded = round(float(value), 6)
    return int(rounded) if rounded.is_integer() else rounded


def _share(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def sample_size_band(count: int) -> str:
    if count < 0:
        raise ValueError("sample size cannot be negative")
    if count == 0:
        return "ZERO"
    if count == 1:
        return "SINGLETON"
    if count <= 4:
        return "N_2_4"
    if count <= 19:
        return "N_5_19"
    return "N_20_PLUS"


def freshness_bucket(age_days: Any) -> str:
    age = _number(age_days)
    if age is None:
        return "unknown"
    if age <= 30:
        return "<=30d"
    if age <= 90:
        return "31-90d"
    if age <= 365:
        return "91-365d"
    return ">365d"


def add_group_demand_percentiles(rows: list[dict[str, Any]], group_fields: Sequence[str], output_field: str, n_field: str) -> None:
    """Attach source-local average-rank percentiles and non-null group sizes."""
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row[field] for field in group_fields)].append(row)
    for group_rows in groups.values():
        values = [_number(row.get("downloads_total")) for row in group_rows]
        usable = [value for value in values if value is not None]
        counts = Counter(usable)
        lower = 0
        percentile_by_value: dict[float, float] = {}
        n = len(usable)
        for value, ties in sorted(counts.items()):
            percentile_by_value[value] = 100.0 if n == 1 else round(100 * (lower + (ties - 1) / 2) / (n - 1), 6)
            lower += ties
        for row, value in zip(group_rows, values, strict=True):
            row[output_field] = None if value is None else percentile_by_value[value]
            row[n_field] = n


def _feature_sort_key(row: Mapping[str, Any]) -> tuple[str, str]:
    return str(row["source"]), str(row["source_resource_id"])


def _load_input(input_db: Path, input_sha: str, enforce_pinned_inputs: bool) -> dict[str, Any]:
    if enforce_pinned_inputs and input_sha != YEE61_DB_SHA256:
        raise CategorySignalError(f"YEE-61 input SHA-256 mismatch: {input_sha}")
    connection = _read_only_connection(input_db)
    try:
        tables = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        required_tables = {"metadata", "plugin_product_scope", "plugin_category_taxonomy", "plugin_category_memberships", "plugin_category_inventory"}
        if not required_tables.issubset(tables):
            raise CategorySignalError(f"YEE-61 database missing tables: {sorted(required_tables - tables)}")
        metadata = {str(row["key"]): str(row["value"]) for row in connection.execute("SELECT key,value FROM metadata")}
        scope = []
        for row in connection.execute("SELECT source,source_resource_id,canonical_identity,product_scope_status,source_feature_json FROM plugin_product_scope ORDER BY source,source_resource_id"):
            feature = _loads(row["source_feature_json"], "plugin_product_scope.source_feature_json")
            scope.append({
                "source": str(row["source"]),
                "source_resource_id": str(row["source_resource_id"]),
                "canonical_identity": str(row["canonical_identity"]),
                "product_scope_status": str(row["product_scope_status"]),
                "feature": feature,
            })
        taxonomy = []
        for row in connection.execute("SELECT * FROM plugin_category_taxonomy ORDER BY category_order"):
            taxonomy.append({
                "category_id": str(row["category_id"]),
                "category_order": int(row["category_order"]),
                "category_name": str(row["category_name"]),
                "definition": str(row["definition"]),
                "subcategories": _loads(row["subcategories_json"], "plugin_category_taxonomy.subcategories_json"),
                "source_native_anchors": _loads(row["source_native_anchors_json"], "plugin_category_taxonomy.source_native_anchors_json"),
                "known_ambiguity_risk_notes": _loads(row["known_ambiguity_risk_notes_json"], "plugin_category_taxonomy.known_ambiguity_risk_notes_json"),
                "taxonomy_version": str(row["taxonomy_version"]),
            })
        memberships = []
        for row in connection.execute("SELECT * FROM plugin_category_memberships ORDER BY source,source_resource_id"):
            memberships.append({
                "source": str(row["source"]),
                "source_resource_id": str(row["source_resource_id"]),
                "canonical_identity": str(row["canonical_identity"]),
                "primary_category_id": str(row["primary_category_id"]),
                "subcategory_id": str(row["subcategory_id"]),
                "secondary_category_ids": _loads(row["secondary_category_ids_json"], "plugin_category_memberships.secondary_category_ids_json"),
                "assignment_method": str(row["assignment_method"]),
                "assignment_confidence": str(row["assignment_confidence"]),
                "assignment_evidence": _loads(row["assignment_evidence_json"], "plugin_category_memberships.assignment_evidence_json"),
                "assignment_risk_notes": _loads(row["assignment_risk_notes_json"], "plugin_category_memberships.assignment_risk_notes_json"),
                "source_native_category_facets": _loads(row["source_native_category_facets_json"], "plugin_category_memberships.source_native_category_facets_json"),
                "source_native_loader_facets": _loads(row["source_native_loader_facets_json"], "plugin_category_memberships.source_native_loader_facets_json"),
                "taxonomy_version": str(row["taxonomy_version"]),
            })
        inventory = []
        for row in connection.execute("SELECT * FROM plugin_category_inventory ORDER BY category_id"):
            inventory.append({
                "category_id": str(row["category_id"]),
                "member_count_by_source": _loads(row["member_count_by_source_json"], "plugin_category_inventory.member_count_by_source_json"),
                "primary_member_count": int(row["primary_member_count"]),
            })
    finally:
        connection.close()

    scope_by_identity = {(row["source"], row["source_resource_id"]): row for row in scope}
    member_by_identity = {(row["source"], row["source_resource_id"]): row for row in memberships}
    if len(scope_by_identity) != len(scope) or len(member_by_identity) != len(memberships):
        raise CategorySignalError("YEE-61 input has duplicate source identities")
    if not taxonomy or [row["category_order"] for row in taxonomy] != list(range(len(taxonomy))):
        raise CategorySignalError("YEE-61 category order is missing or non-contiguous")
    taxonomy_by_id = {row["category_id"]: row for row in taxonomy}
    if len(taxonomy_by_id) != len(taxonomy):
        raise CategorySignalError("YEE-61 taxonomy has duplicate category IDs")
    taxonomy_version = metadata.get("taxonomy_version")
    taxonomy_sha = metadata.get("taxonomy_sha256")
    if not taxonomy_version or not taxonomy_sha:
        raise CategorySignalError("YEE-61 taxonomy version/hash metadata is missing")
    if enforce_pinned_inputs and (
        metadata.get("schema_version") != YEE61_SCHEMA_VERSION
        or taxonomy_version != YEE61_TAXONOMY_VERSION
        or taxonomy_sha != YEE61_TAXONOMY_SHA256
        or any(row["taxonomy_version"] != YEE61_TAXONOMY_VERSION for row in taxonomy)
    ):
        raise CategorySignalError("YEE-61 schema or frozen taxonomy version/hash mismatch")
    if set(scope_by_identity) != {
        (row["source"], row["source_resource_id"]) for row in scope
    }:
        raise CategorySignalError("YEE-61 scope identity set is inconsistent")
    if {row["source"] for row in scope} != set(SOURCES):
        raise CategorySignalError("YEE-61 scope sources must be exactly Hangar and Voxel")

    status_counts: dict[str, Counter[str]] = {source: Counter() for source in SOURCES}
    for row in scope:
        if row["product_scope_status"] not in SCOPE_STATUSES:
            raise CategorySignalError("Unknown YEE-61 product_scope_status")
        status_counts[row["source"]][row["product_scope_status"]] += 1
        feature = row["feature"]
        if (feature.get("source"), str(feature.get("source_resource_id")), feature.get("canonical_identity")) != (
            row["source"], row["source_resource_id"], row["canonical_identity"]
        ):
            raise CategorySignalError(f"YEE-61 scope feature identity mismatch: {row['canonical_identity']}")
    confirmed_ids = {key for key, row in scope_by_identity.items() if row["product_scope_status"] == "PLUGIN_PRODUCT_CONFIRMED"}
    if set(member_by_identity) != confirmed_ids:
        raise CategorySignalError("YEE-61 memberships do not exactly equal the confirmed scope identity set")
    for key, membership in member_by_identity.items():
        scope_row = scope_by_identity[key]
        if scope_row["product_scope_status"] != "PLUGIN_PRODUCT_CONFIRMED":
            raise CategorySignalError(f"Non-confirmed identity leaked into memberships: {scope_row['canonical_identity']}")
        if membership["canonical_identity"] != scope_row["canonical_identity"]:
            raise CategorySignalError(f"YEE-61 membership identity mismatch: {membership['canonical_identity']}")
        category = taxonomy_by_id.get(membership["primary_category_id"])
        if category is None:
            raise CategorySignalError(f"Membership category is not frozen: {membership['primary_category_id']}")
        subcategory_ids = [item["subcategory_id"] for item in category["subcategories"]]
        if membership["subcategory_id"] not in subcategory_ids:
            raise CategorySignalError(f"Membership subcategory is not frozen: {membership['subcategory_id']}")
        secondary = membership["secondary_category_ids"]
        if not isinstance(secondary, list) or len(secondary) > 3 or len(set(secondary)) != len(secondary):
            raise CategorySignalError(f"Invalid YEE-61 secondary category list: {membership['canonical_identity']}")
        if membership["primary_category_id"] in secondary or any(value not in taxonomy_by_id for value in secondary):
            raise CategorySignalError(f"Invalid YEE-61 secondary category reference: {membership['canonical_identity']}")
        membership.update(scope_row["feature"])

    inventory_by_id = {row["category_id"]: row for row in inventory}
    if set(inventory_by_id) != set(taxonomy_by_id):
        raise CategorySignalError("YEE-61 inventory and frozen taxonomy category sets differ")
    primary_counts: Counter[str] = Counter({category_id: 0 for category_id in taxonomy_by_id})
    primary_source_counts: dict[str, Counter[str]] = {category_id: Counter() for category_id in taxonomy_by_id}
    for membership in memberships:
        primary_counts[membership["primary_category_id"]] += 1
        primary_source_counts[membership["primary_category_id"]][membership["source"]] += 1
    for category_id in taxonomy_by_id:
        inventory_row = inventory_by_id[category_id]
        expected_sources = {source: primary_source_counts[category_id].get(source, 0) for source in SOURCES}
        if inventory_row["primary_member_count"] != primary_counts[category_id] or inventory_row["member_count_by_source"] != expected_sources:
            raise CategorySignalError(f"YEE-61 inventory does not reconcile for {category_id}")
    as_of_values = {row["analysis_as_of"] for row in memberships}
    if len(as_of_values) != 1:
        raise CategorySignalError("YEE-61 confirmed features have inconsistent analysis_as_of values")
    analysis_as_of = next(iter(as_of_values))
    if enforce_pinned_inputs and analysis_as_of != ANALYSIS_AS_OF:
        raise CategorySignalError(f"YEE-29 analysis_as_of mismatch: {analysis_as_of}")

    if enforce_pinned_inputs:
        expected_statuses = EXPECTED_SCOPE_COUNTS
        actual_statuses = {
            source: {status: status_counts[source].get(status, 0) for status in SCOPE_STATUSES}
            for source in SOURCES
        }
        if actual_statuses != expected_statuses:
            raise CategorySignalError(f"YEE-61 scope status counts mismatch: {actual_statuses}")
        if len(scope) != 10018 or len(memberships) != 1352:
            raise CategorySignalError("YEE-61 pinned scope/membership row count mismatch")
        confirmed_by_source = Counter(row["source"] for row in memberships)
        if dict(confirmed_by_source) != {"hangar": 1221, "voxel": 131}:
            raise CategorySignalError(f"YEE-61 confirmed source counts mismatch: {dict(confirmed_by_source)}")
        actual_primary = {category_id: primary_counts.get(category_id, 0) for category_id in taxonomy_by_id}
        if actual_primary != EXPECTED_PRIMARY_COUNTS:
            raise CategorySignalError(f"YEE-61 primary category counts mismatch: {actual_primary}")
        if len(taxonomy) != 11 or sum(len(row["subcategories"]) for row in taxonomy) != 38:
            raise CategorySignalError("YEE-61 pinned category/subcategory inventory mismatch")

    return {
        "metadata": metadata,
        "scope": scope,
        "scope_by_identity": scope_by_identity,
        "status_counts": status_counts,
        "taxonomy": taxonomy,
        "taxonomy_by_id": taxonomy_by_id,
        "memberships": memberships,
        "inventory": inventory,
        "primary_counts": primary_counts,
        "primary_source_counts": primary_source_counts,
        "analysis_as_of": analysis_as_of,
        "input_sha256": input_sha,
    }


def _member_features(loaded: Mapping[str, Any]) -> list[dict[str, Any]]:
    taxonomy = loaded["taxonomy_by_id"]
    rows: list[dict[str, Any]] = []
    for source_row in loaded["memberships"]:
        row = dict(source_row)
        category = taxonomy[row["primary_category_id"]]
        row["primary_category_order"] = category["category_order"]
        row["subcategory_order"] = next(
            index for index, item in enumerate(category["subcategories"])
            if item["subcategory_id"] == row["subcategory_id"]
        )
        rows.append(row)
    rows.sort(key=_feature_sort_key)
    add_group_demand_percentiles(rows, ("source",), "confirmed_source_demand_percentile", "confirmed_source_demand_group_n")
    add_group_demand_percentiles(rows, ("source", "primary_category_id"), "source_category_demand_percentile", "source_category_demand_group_n")
    add_group_demand_percentiles(rows, ("source", "primary_category_id", "subcategory_id"), "source_subcategory_demand_percentile", "source_subcategory_demand_group_n")
    for row in rows:
        row["source_category_sample_size_band"] = sample_size_band(row["source_category_demand_group_n"])
        row["source_subcategory_sample_size_band"] = sample_size_band(row["source_subcategory_demand_group_n"])
        row["signal_schema_version"] = SIGNAL_SCHEMA_VERSION
    return rows


def _scope_coverage(loaded: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    schema_version = loaded["metadata"].get("schema_version")
    for source in SOURCES:
        source_scope = [row for row in loaded["scope"] if row["source"] == source]
        counts = Counter(row["product_scope_status"] for row in source_scope)
        confirmed = [row["feature"] for row in source_scope if row["product_scope_status"] == "PLUGIN_PRODUCT_CONFIRMED"]
        confirmed_count = len(confirmed)
        demand_count = sum(_number(row.get("downloads_total")) is not None for row in confirmed)
        freshness_count = sum(_number(row.get("freshness_age_days")) is not None for row in confirmed)
        if source == "voxel":
            paid_count: int | None = sum(row.get("paid_state") in ("paid", "free") for row in confirmed)
            paid_rate = _share(paid_count, confirmed_count)
        else:
            paid_count = None
            paid_rate = None
        rows.append({
            "source": source,
            "yee61_input_count": len(source_scope),
            "yee61_confirmed_count": counts.get("PLUGIN_PRODUCT_CONFIRMED", 0),
            "yee61_review_count": counts.get("PLUGIN_PRODUCT_REVIEW", 0),
            "yee61_out_of_scope_count": counts.get("OUT_OF_SCOPE_PRODUCT_FORM", 0),
            "yee61_confirmed_fraction": _share(counts.get("PLUGIN_PRODUCT_CONFIRMED", 0), len(source_scope)),
            "yee61_review_fraction": _share(counts.get("PLUGIN_PRODUCT_REVIEW", 0), len(source_scope)),
            "yee61_out_of_scope_fraction": _share(counts.get("OUT_OF_SCOPE_PRODUCT_FORM", 0), len(source_scope)),
            "stage_b_signal_member_count": confirmed_count,
            "demand_coverage_count_among_confirmed": demand_count,
            "demand_coverage_rate_among_confirmed": _share(demand_count, confirmed_count),
            "freshness_coverage_count_among_confirmed": freshness_count,
            "freshness_coverage_rate_among_confirmed": _share(freshness_count, confirmed_count),
            "paid_evidence_scope": "SOURCE_AVAILABLE" if source == "voxel" else "NOT_AVAILABLE_FOR_SOURCE",
            "paid_evidence_observed_count_among_confirmed": paid_count,
            "paid_evidence_observed_rate_among_confirmed": paid_rate,
            "yee61_schema_version": schema_version,
            "yee61_scope_classifier_version": loaded["metadata"].get("scope_classifier_version"),
            "input_sqlite_sha256": loaded["input_sha256"],
            "input_memberships_jsonl_sha256": YEE61_MEMBERSHIPS_SHA256,
            "input_scope_jsonl_sha256": YEE61_SCOPE_SHA256,
            "input_taxonomy_json_sha256": YEE61_TAXONOMY_EXPORT_SHA256,
            "taxonomy_version": loaded["metadata"]["taxonomy_version"],
            "taxonomy_sha256": loaded["metadata"]["taxonomy_sha256"],
            "signal_schema_version": SIGNAL_SCHEMA_VERSION,
        })
    return rows


def _source_scope_context(row: Mapping[str, Any]) -> dict[str, Any]:
    return dict(row)


def _metric_values(rows: Sequence[Mapping[str, Any]], field: str) -> list[float]:
    return [value for row in rows if (value := _number(row.get(field))) is not None]


def _add_engagement(row: dict[str, Any], members: Sequence[Mapping[str, Any]], source: str) -> None:
    metrics = {
        "hangar": (
            ("hangar_recent_downloads", "hangar_recent_downloads"),
            ("hangar_recent_views", "hangar_recent_views"),
            ("star_count", "star_count"),
            ("watcher_count", "watcher_count"),
        ),
        "voxel": (
            ("voxel_review_count", "voxel_review_count"),
            ("voxel_review_stars", "voxel_review_stars"),
        ),
    }
    for metric_source, metric_fields in metrics.items():
        for output_prefix, input_field in metric_fields:
            values = _metric_values(members, input_field) if metric_source == source else None
            if values is None:
                row[f"{output_prefix}_available_count"] = None
                row[f"{output_prefix}_available_rate"] = None
                row[f"{output_prefix}_p50"] = None
                row[f"{output_prefix}_p90"] = None
            else:
                row[f"{output_prefix}_available_count"] = len(values)
                row[f"{output_prefix}_available_rate"] = _share(len(values), len(members))
                row[f"{output_prefix}_p50"] = quantile(values, 0.50)
                row[f"{output_prefix}_p90"] = quantile(values, 0.90)


def _add_paid_price(row: dict[str, Any], members: Sequence[Mapping[str, Any]], source: str) -> None:
    if source != "voxel":
        row.update({
            "paid_evidence_scope": "NOT_AVAILABLE_FOR_SOURCE",
            "paid_state_counts": None,
            "paid_evidence_observed_count": None,
            "paid_evidence_observed_rate": None,
            "paid_share_among_observed": None,
            "price_amount_present_count": None,
            "price_available_count": None,
            "price_available_rate": None,
            "price_band_counts": None,
            "price_quantiles_by_currency": None,
        })
        return
    states = Counter(row.get("paid_state") if row.get("paid_state") in ("paid", "free") else "unknown" for row in members)
    known = states["paid"] + states["free"]
    amount_present = 0
    currency_groups: dict[str, list[float]] = defaultdict(list)
    for member in members:
        amount = _number(member.get("price_amount"))
        currency = member.get("currency")
        if amount is not None:
            amount_present += 1
            if isinstance(currency, str) and currency.strip():
                currency_groups[currency.strip()].append(amount)
    available_count = sum(len(values) for values in currency_groups.values())
    price_quantiles = [
        {
            "currency": currency,
            "available_count": len(values),
            "price_p50": quantile(values, 0.50),
            "price_p75": quantile(values, 0.75),
            "price_p90": quantile(values, 0.90),
            "price_p95": quantile(values, 0.95),
            "price_p99": quantile(values, 0.99),
        }
        for currency, values in sorted(currency_groups.items())
    ]
    price_bands = Counter(
        member.get("voxel_price_band") if member.get("voxel_price_band") in PRICE_BANDS[:-1] else "unknown"
        for member in members
    )
    row.update({
        "paid_evidence_scope": "SOURCE_AVAILABLE",
        "paid_state_counts": {state: states.get(state, 0) for state in ("paid", "free", "unknown")},
        "paid_evidence_observed_count": known,
        "paid_evidence_observed_rate": _share(known, len(members)),
        "paid_share_among_observed": _share(states["paid"], known),
        "price_amount_present_count": amount_present,
        "price_available_count": available_count,
        "price_available_rate": _share(available_count, len(members)),
        "price_band_counts": {band: price_bands.get(band, 0) for band in PRICE_BANDS},
        "price_quantiles_by_currency": price_quantiles,
    })


def _fact_signal_metrics(members: Sequence[Mapping[str, Any]], source: str) -> dict[str, Any]:
    member_count = len(members)
    demand = _metric_values(members, "downloads_total")
    yee29_percentiles = _metric_values(members, "demand_percentile")
    confirmed_percentiles = _metric_values(members, "confirmed_source_demand_percentile")
    ge75 = sum(value >= 75 for value in confirmed_percentiles)
    ge90 = sum(value >= 90 for value in confirmed_percentiles)
    fresh = _metric_values(members, "freshness_age_days")
    freshness_counts = Counter(freshness_bucket(member.get("freshness_age_days")) for member in members)
    freshness_row = {
        f"freshness_count_{'unknown' if bucket == 'unknown' else bucket.replace('<=','le_').replace('-','_').replace('>','gt_')}" : freshness_counts.get(bucket, 0)
        for bucket in FRESHNESS_BINS
    }
    freshness_shares = {
        f"freshness_share_{'unknown' if bucket == 'unknown' else bucket.replace('<=','le_').replace('-','_').replace('>','gt_')}": _share(freshness_counts.get(bucket, 0), member_count)
        for bucket in FRESHNESS_BINS
    }
    metrics: dict[str, Any] = {
        "member_count": member_count,
        "demand_available_count": len(demand),
        "demand_available_rate": _share(len(demand), member_count),
        "demand_metric_sources": sorted({str(member["demand_metric_source"]) for member in members if member.get("demand_metric_source") is not None}),
        **{f"downloads_total_{suffix}": quantile(demand, p) for suffix, p in (("p50", .50), ("p75", .75), ("p90", .90), ("p95", .95), ("p99", .99))},
        **{f"yee29_demand_percentile_{suffix}": quantile(yee29_percentiles, p) for suffix, p in (("p50", .50), ("p75", .75), ("p90", .90))},
        **{f"confirmed_source_demand_percentile_{suffix}": quantile(confirmed_percentiles, p) for suffix, p in (("p50", .50), ("p75", .75), ("p90", .90))},
        "confirmed_source_demand_percentile_ge75_count": ge75,
        "confirmed_source_demand_percentile_ge75_share": _share(ge75, len(confirmed_percentiles)),
        "confirmed_source_demand_percentile_ge90_count": ge90,
        "confirmed_source_demand_percentile_ge90_share": _share(ge90, len(confirmed_percentiles)),
        "sample_size_band": sample_size_band(len(demand)),
        "freshness_available_count": len(fresh),
        "freshness_available_rate": _share(len(fresh), member_count),
        "freshness_age_days_p50": quantile(fresh, .50),
        "freshness_age_days_p75": quantile(fresh, .75),
        "freshness_age_days_p90": quantile(fresh, .90),
        **freshness_row,
        **freshness_shares,
    }
    _add_engagement(metrics, members, source)
    _add_paid_price(metrics, members, source)
    return metrics


def _category_source_facts(loaded: Mapping[str, Any], members: Sequence[Mapping[str, Any]], coverage: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    coverage_by_source = {row["source"]: row for row in coverage}
    members_by_category_source: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for member in members:
        members_by_category_source[(member["primary_category_id"], member["source"])].append(member)
    rows = []
    for category in loaded["taxonomy"]:
        for source in SOURCES:
            primary = [row for row in loaded["memberships"] if row["primary_category_id"] == category["category_id"] and row["source"] == source]
            secondary = [row for row in loaded["memberships"] if category["category_id"] in row["secondary_category_ids"] and row["source"] == source]
            any_ids = {(row["source"], row["source_resource_id"]) for row in (*primary, *secondary)}
            confirmed_count = coverage_by_source[source]["yee61_confirmed_count"]
            row = {
                "category_order": category["category_order"],
                "category_id": category["category_id"],
                "category_name": category["category_name"],
                "definition": category["definition"],
                "source": source,
                "observed_confirmed_primary_count": len(primary),
                "observed_confirmed_secondary_count": len({(item["source"], item["source_resource_id"]) for item in secondary}),
                "observed_confirmed_any_membership_count": len(any_ids),
                "primary_share_of_source_confirmed": _share(len(primary), confirmed_count),
                "any_membership_share_of_source_confirmed": _share(len(any_ids), confirmed_count),
                "source_scope_coverage": _source_scope_context(coverage_by_source[source]),
                "taxonomy_version": loaded["metadata"]["taxonomy_version"],
                "taxonomy_sha256": loaded["metadata"]["taxonomy_sha256"],
                "input_sqlite_sha256": loaded["input_sha256"],
                "signal_schema_version": SIGNAL_SCHEMA_VERSION,
            }
            row.update(_fact_signal_metrics(members_by_category_source[(category["category_id"], source)], source))
            rows.append(row)
    return rows


def _subcategory_source_facts(loaded: Mapping[str, Any], members: Sequence[Mapping[str, Any]], coverage: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    coverage_by_source = {row["source"]: row for row in coverage}
    members_by_group: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for member in members:
        members_by_group[(member["primary_category_id"], member["subcategory_id"], member["source"])].append(member)
    rows = []
    for category in loaded["taxonomy"]:
        for subcategory_order, subcategory in enumerate(category["subcategories"]):
            for source in SOURCES:
                group = members_by_group[(category["category_id"], subcategory["subcategory_id"], source)]
                member_count = len(group)
                confirmed_count = coverage_by_source[source]["yee61_confirmed_count"]
                row = {
                    "category_order": category["category_order"],
                    "category_id": category["category_id"],
                    "category_name": category["category_name"],
                    "subcategory_order": subcategory_order,
                    "subcategory_id": subcategory["subcategory_id"],
                    "subcategory_definition": subcategory["definition"],
                    "source": source,
                    "observed_confirmed_primary_count": member_count,
                    "observed_confirmed_secondary_count": None,
                    "observed_confirmed_any_membership_count": member_count,
                    "secondary_subcategory_memberships_inferred": False,
                    "primary_share_of_source_confirmed": _share(member_count, confirmed_count),
                    "any_membership_share_of_source_confirmed": _share(member_count, confirmed_count),
                    "source_scope_coverage": _source_scope_context(coverage_by_source[source]),
                    "taxonomy_version": loaded["metadata"]["taxonomy_version"],
                    "taxonomy_sha256": loaded["metadata"]["taxonomy_sha256"],
                    "input_sqlite_sha256": loaded["input_sha256"],
                    "signal_schema_version": SIGNAL_SCHEMA_VERSION,
                }
                row.update(_fact_signal_metrics(group, source))
                rows.append(row)
    return rows


def _category_overview(loaded: Mapping[str, Any], category_facts: Sequence[Mapping[str, Any]], coverage: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    facts_by_category_source = {(row["category_id"], row["source"]): row for row in category_facts}
    coverage_by_source = {row["source"]: row for row in coverage}
    rows = []
    for category in loaded["taxonomy"]:
        source_counts = {
            source: facts_by_category_source[(category["category_id"], source)]["observed_confirmed_primary_count"]
            for source in SOURCES
        }
        source_signals = {
            source: facts_by_category_source[(category["category_id"], source)]
            for source in SOURCES
        }
        notes = [
            "Counts describe accepted YEE-61 PLUGIN_PRODUCT_CONFIRMED memberships only; REVIEW and OUT_OF_SCOPE identities are excluded.",
            "Primary counts are the exclusive category supply lens; secondary category memberships are contextual overlap and are not added to primary counts.",
        ]
        if coverage_by_source["voxel"]["yee61_review_count"]:
            notes.append(
                f"Voxel upstream coverage includes {coverage_by_source['voxel']['yee61_review_count']} REVIEW identities; category signals represent only confirmed Voxel memberships, not the full plausible source universe."
            )
        for source in SOURCES:
            if source_counts[source] == 0:
                notes.append(f"No confirmed primary members for this category are present from {source} in accepted YEE-61.")
        rows.append({
            "category_order": category["category_order"],
            "category_id": category["category_id"],
            "category_name": category["category_name"],
            "definition": category["definition"],
            "subcategories": category["subcategories"],
            "observed_confirmed_primary_count_by_source": source_counts,
            "observed_confirmed_primary_count_non_dedup": sum(source_counts.values()),
            "source_presence_count": sum(count > 0 for count in source_counts.values()),
            "source_signals": source_signals,
            "upstream_source_coverage": {
                source: {
                    "yee61_input_count": coverage_by_source[source]["yee61_input_count"],
                    "yee61_confirmed_count": coverage_by_source[source]["yee61_confirmed_count"],
                    "yee61_review_count": coverage_by_source[source]["yee61_review_count"],
                    "yee61_out_of_scope_count": coverage_by_source[source]["yee61_out_of_scope_count"],
                    "yee61_confirmed_fraction": coverage_by_source[source]["yee61_confirmed_fraction"],
                    "yee61_review_fraction": coverage_by_source[source]["yee61_review_fraction"],
                    "yee61_out_of_scope_fraction": coverage_by_source[source]["yee61_out_of_scope_fraction"],
                }
                for source in SOURCES
            },
            "coverage_notes": notes,
            "taxonomy_version": loaded["metadata"]["taxonomy_version"],
            "taxonomy_sha256": loaded["metadata"]["taxonomy_sha256"],
            "input_sqlite_sha256": loaded["input_sha256"],
            "signal_schema_version": SIGNAL_SCHEMA_VERSION,
        })
    return rows


def _all_numeric_fields(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    columns = sorted({key for row in rows for key in row})
    json_fields = {key for row in rows for key, value in row.items() if isinstance(value, (dict, list))}
    integer_fields = {key for row in rows for key, value in row.items() if isinstance(value, int) and not isinstance(value, bool)}
    boolean_fields = {key for row in rows for key, value in row.items() if isinstance(value, bool)}
    real_fields = {key for row in rows for key, value in row.items() if isinstance(value, float)}
    types = {}
    for key in columns:
        if key in json_fields:
            types[key] = "TEXT"
        elif key in real_fields:
            types[key] = "REAL"
        elif key in integer_fields or key in boolean_fields:
            types[key] = "INTEGER"
        else:
            types[key] = "TEXT"
    return types


def _sql_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return canonical_json(value)
    if isinstance(value, bool):
        return int(value)
    return value


def _quote(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise CategorySignalError(f"Unsafe generated SQLite identifier: {identifier}")
    return f'"{identifier}"'


def _write_sqlite(path: Path, loaded: Mapping[str, Any], tables: Mapping[str, Sequence[Mapping[str, Any]]], run_metadata: Mapping[str, Any]) -> None:
    if path.exists():
        path.unlink()
    db = sqlite3.connect(path)
    try:
        db.execute("PRAGMA page_size = 4096")
        db.execute("PRAGMA journal_mode = DELETE")
        db.execute("PRAGMA synchronous = FULL")
        db.execute("PRAGMA foreign_keys = ON")
        db.execute("PRAGMA user_version = 1")
        db.execute("CREATE TABLE run_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID")
        db.executemany(
            "INSERT INTO run_metadata(key,value) VALUES (?,?)",
            [(key, canonical_json(value) if isinstance(value, (dict, list)) else str(value)) for key, value in sorted(run_metadata.items())],
        )
        db.execute(
            """CREATE TABLE frozen_taxonomy_snapshot (
                category_id TEXT PRIMARY KEY,
                category_order INTEGER NOT NULL UNIQUE,
                taxonomy_version TEXT NOT NULL,
                category_json TEXT NOT NULL
            ) WITHOUT ROWID"""
        )
        db.executemany(
            "INSERT INTO frozen_taxonomy_snapshot VALUES (?,?,?,?)",
            [(
                row["category_id"], row["category_order"], row["taxonomy_version"],
                canonical_json(row),
            ) for row in loaded["taxonomy"]],
        )
        for table, rows in tables.items():
            columns = sorted({key for row in rows for key in row})
            field_types = _all_numeric_fields(rows)
            definitions = [f"{_quote(column)} {field_types[column]}" for column in columns]
            definitions.append('"record_json" TEXT NOT NULL')
            primary = PRIMARY_KEYS[table]
            definitions.append("PRIMARY KEY (" + ",".join(_quote(key) for key in primary) + ")")
            if table == "category_signal_member_features":
                definitions.append('UNIQUE ("canonical_identity")')
                definitions.append('FOREIGN KEY ("primary_category_id") REFERENCES frozen_taxonomy_snapshot(category_id)')
            elif table in ("category_source_facts", "subcategory_source_facts", "category_signal_overview"):
                definitions.append('FOREIGN KEY ("category_id") REFERENCES frozen_taxonomy_snapshot(category_id)')
            if table == "subcategory_source_facts":
                definitions.append('CHECK ("secondary_subcategory_memberships_inferred" = 0)')
            db.execute(f"CREATE TABLE {_quote(table)} ({','.join(definitions)}) WITHOUT ROWID")
            insert_columns = (*columns, "record_json")
            insert_sql = (
                f"INSERT INTO {_quote(table)} ({','.join(_quote(name) for name in insert_columns)}) "
                f"VALUES ({','.join('?' for _ in insert_columns)})"
            )
            db.executemany(
                insert_sql,
                [tuple(_sql_value(row.get(column)) for column in columns) + (canonical_json(row),) for row in rows],
            )
        db.commit()
    finally:
        db.close()


def _preferred_columns(table: str, rows: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    present = {key for row in rows for key in row}
    if table == "source_scope_coverage":
        preferred = ("source",)
    elif table == "category_signal_member_features":
        preferred = ("source", "source_resource_id", "canonical_identity")
    elif table == "category_source_facts":
        preferred = ("category_order", "category_id", "source")
    elif table == "subcategory_source_facts":
        preferred = ("category_order", "category_id", "subcategory_order", "subcategory_id", "source")
    else:
        preferred = ("category_order", "category_id")
    return (*[key for key in preferred if key in present], *sorted(present - set(preferred)))


def _csv_cell(value: Any) -> Any:
    if value is None:
        return CSV_NULL
    if isinstance(value, (dict, list)):
        return canonical_json(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return ("".join(canonical_json(row) + "\n" for row in rows)).encode("utf-8")


def _csv_bytes(table: str, rows: Sequence[Mapping[str, Any]]) -> bytes:
    columns = _preferred_columns(table, rows)
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n", extrasaction="raise")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: _csv_cell(row.get(column)) for column in columns})
    return buffer.getvalue().encode("utf-8")


def _repo_doc(name: str) -> bytes:
    path = Path(__file__).resolve().parents[2] / name
    if not path.is_file():
        raise CategorySignalError(f"Required YEE-73 document missing from repository: {name}")
    return path.read_bytes()


def _run_metadata(loaded: Mapping[str, Any], code_commit: str | None) -> dict[str, Any]:
    commit = code_commit or "working-tree"
    identity = {
        "work_order": WORK_ORDER,
        "signal_schema_version": SIGNAL_SCHEMA_VERSION,
        "input_sqlite_sha256": loaded["input_sha256"],
        "taxonomy_sha256": loaded["metadata"]["taxonomy_sha256"],
        "analysis_as_of": loaded["analysis_as_of"],
        "code_commit": commit,
    }
    run_id = hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()
    return {
        **identity,
        "run_id": run_id,
        "yee61_merge_commit": YEE61_MERGE_COMMIT,
        "input_memberships_jsonl_sha256": YEE61_MEMBERSHIPS_SHA256,
        "input_scope_jsonl_sha256": YEE61_SCOPE_SHA256,
        "input_taxonomy_json_sha256": YEE61_TAXONOMY_EXPORT_SHA256,
        "input_schema_version": loaded["metadata"].get("schema_version"),
        "input_taxonomy_version": loaded["metadata"].get("taxonomy_version"),
    }


def _write_core(output: Path, loaded: Mapping[str, Any], tables: Mapping[str, Sequence[Mapping[str, Any]]], run_metadata: Mapping[str, Any]) -> None:
    for table, stem in TABLE_FILES.items():
        (output / f"{stem}.jsonl").write_bytes(_jsonl_bytes(tables[table]))
        (output / f"{stem}.csv").write_bytes(_csv_bytes(table, tables[table]))
    _write_sqlite(output / "category_signal_layer.sqlite", loaded, tables, run_metadata)
    for name in ("GOAL_ALIGNMENT.md", "CATEGORY_SIGNAL_SCHEMA.md", "SIGNAL_SEMANTICS.md"):
        (output / name).write_bytes(_repo_doc(name))


def _export_checks(output: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    counts = {}
    checks = {}
    for table, stem in TABLE_FILES.items():
        expected_rows = list(tables[table])
        jsonl_path = output / f"{stem}.jsonl"
        csv_path = output / f"{stem}.csv"
        with jsonl_path.open("r", encoding="utf-8") as stream:
            jsonl_rows = [json.loads(line) for line in stream if line.strip()]
        with csv_path.open("r", encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            csv_rows = list(reader)
            csv_columns = tuple(reader.fieldnames or ())
        expected_columns = _preferred_columns(table, expected_rows)
        counts[f"{table}_jsonl"] = len(jsonl_rows)
        counts[f"{table}_csv"] = len(csv_rows)
        checks[f"{table}_jsonl_exact"] = jsonl_rows == expected_rows
        checks[f"{table}_csv_bytes_exact"] = csv_path.read_bytes() == _csv_bytes(table, expected_rows)
        checks[f"{table}_csv_columns_exact"] = csv_columns == expected_columns
        checks[f"{table}_row_counts_match"] = len(jsonl_rows) == len(csv_rows) == len(expected_rows)
    db_path = output / "category_signal_layer.sqlite"
    connection = sqlite3.connect(db_path)
    try:
        counts["sqlite_integrity_check"] = connection.execute("PRAGMA integrity_check").fetchone()[0]
        counts["sqlite_foreign_key_violations"] = len(connection.execute("PRAGMA foreign_key_check").fetchall())
        table_names = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        checks["sqlite_required_tables"] = {
            "run_metadata", "source_scope_coverage", "category_signal_member_features", "category_source_facts",
            "subcategory_source_facts", "category_signal_overview", "frozen_taxonomy_snapshot",
        }.issubset(table_names)
        for table, expected_rows in tables.items():
            db_rows = [json.loads(row[0]) for row in connection.execute(f"SELECT record_json FROM {_quote(table)} ORDER BY " + ",".join(_quote(name) for name in ORDER_BY[table]))]
            counts[f"{table}_sqlite"] = len(db_rows)
            checks[f"{table}_sqlite_exact"] = db_rows == list(expected_rows)
    finally:
        connection.close()
    checks["sqlite_integrity_ok"] = counts["sqlite_integrity_check"] == "ok"
    checks["sqlite_foreign_key_check_zero"] = counts["sqlite_foreign_key_violations"] == 0
    return {"counts": counts, "checks": checks}


def _manifest(output: Path, run_metadata: Mapping[str, Any]) -> dict[str, Any]:
    artifacts = []
    for name in sorted(EXPORTS):
        path = output / name
        artifacts.append({"path": name, "size_bytes": path.stat().st_size, "sha256": sha256_file(path)})
    return {
        "work_order": WORK_ORDER,
        "delivery_status": DELIVERY_STATUS,
        "signal_schema_version": SIGNAL_SCHEMA_VERSION,
        "run_id": run_metadata["run_id"],
        "input": {
            "name": "category_first_foundation.sqlite",
            "sha256": run_metadata["input_sqlite_sha256"],
            "source": "accepted YEE-61, opened read-only",
            "yee61_merge_commit": YEE61_MERGE_COMMIT,
            "memberships_jsonl_sha256": YEE61_MEMBERSHIPS_SHA256,
            "scope_jsonl_sha256": YEE61_SCOPE_SHA256,
            "taxonomy_json_sha256": YEE61_TAXONOMY_EXPORT_SHA256,
        },
        "taxonomy": {
            "version": run_metadata["input_taxonomy_version"],
            "sha256": run_metadata["taxonomy_sha256"],
        },
        "analysis_as_of": run_metadata["analysis_as_of"],
        "code_commit": run_metadata["code_commit"],
        "manifest_self_hash": "omitted_to_avoid_recursive_hash",
        "artifacts": artifacts,
    }


def _final_report(qa: Mapping[str, Any]) -> str:
    rows = qa["category_primary_counts_by_source"]
    lines = [
        "# YEE-73 final report",
        "",
        f"Status: `{qa['delivery_status']}`",
        f"Production QA: `{qa['status']}`",
        "",
        "## Scope and provenance",
        "",
        "This is a descriptive Stage B signal layer over accepted YEE-61 `PLUGIN_PRODUCT_CONFIRMED` memberships only. It does not describe the complete plausible plugin market. YEE-61 remains read-only; YEE-29 inherited facts and its fixed `analysis_as_of` are preserved.",
        "",
        f"- Accepted YEE-61 database SHA-256 before/after: `{qa['input_sha256_before']}` / `{qa['input_sha256_after']}`",
        f"- Taxonomy version/hash: `{qa['taxonomy_version']}` / `{qa['taxonomy_sha256']}`",
        f"- Analysis as of: `{qa['analysis_as_of']}`",
        f"- Run ID: `{qa['run_id']}`",
        f"- Code commit: `{qa['code_commit']}`",
        "",
        "## Reconciliation",
        "",
        f"- YEE-61 scope identities: {qa['input_scope_rows']:,}",
        f"- Confirmed signal members: {qa['signal_member_rows']:,} (Hangar {qa['signal_member_source_counts']['hangar']:,}; Voxel {qa['signal_member_source_counts']['voxel']:,})",
        f"- Frozen categories: {qa['row_counts']['category_signal_overview']}; category/source facts: {qa['row_counts']['category_source_facts']}; subcategory/source facts: {qa['row_counts']['subcategory_source_facts']}; source coverage rows: {qa['row_counts']['source_scope_coverage']}",
        "- SQLite integrity: `ok`; foreign-key violations: 0",
        f"- Deterministic replay: `{qa['deterministic_replay']['all_exports_and_sqlite_byte_identical']}`",
        "",
        "## Observed confirmed primary membership counts",
        "",
        "Counts below are YEE-61 observed confirmed listing memberships by frozen taxonomy order. Cross-source counts are not deduplicated products.",
        "",
        "| Category | Hangar | Voxel | Non-deduplicated listing memberships |",
        "| --- | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(f"| {row['category_id']} | {row['hangar']} | {row['voxel']} | {row['non_dedup']} |")
    lines.extend([
        "",
        "## Coverage and interpretation",
        "",
        f"- Hangar confirmed/review/out-of-scope: {qa['source_scope_coverage']['hangar']['yee61_confirmed_count']:,}/{qa['source_scope_coverage']['hangar']['yee61_review_count']:,}/{qa['source_scope_coverage']['hangar']['yee61_out_of_scope_count']:,}.",
        f"- Voxel confirmed/review/out-of-scope: {qa['source_scope_coverage']['voxel']['yee61_confirmed_count']:,}/{qa['source_scope_coverage']['voxel']['yee61_review_count']:,}/{qa['source_scope_coverage']['voxel']['yee61_out_of_scope_count']:,}. Voxel category counts cover confirmed rows only; low or zero counts are not evidence of low true supply.",
        "- Raw source-native downloads remain inside source/category/subcategory distributions. No cross-source raw download sum or unified engagement metric is produced.",
        "- Voxel paid/price facts remain Voxel-only and currency-grouped; Hangar paid/price is unavailable, not free or zero.",
        "- Zero-member categories and subcategories remain in the exports and database.",
        "",
        "## Stop boundary",
        "",
        "Outputs stop at source coverage and deterministic member/category/subcategory facts. No Stage C, whitespace analysis, candidate discovery, research, opportunity state, score, rank, shortlist, matching, or recommendation was run.",
        "",
    ])
    return "\n".join(lines)


def _write_final_metadata(output: Path, qa: Mapping[str, Any], run_metadata: Mapping[str, Any]) -> None:
    (output / "QA_RESULT.json").write_text(canonical_json(qa) + "\n", encoding="utf-8", newline="\n")
    (output / "FINAL_REPORT.md").write_text(_final_report(qa), encoding="utf-8", newline="\n")
    manifest = _manifest(output, run_metadata)
    (output / "DATASET_MANIFEST.json").write_text(canonical_json(manifest) + "\n", encoding="utf-8", newline="\n")


def _taxonomy_counts(loaded: Mapping[str, Any]) -> list[dict[str, Any]]:
    result = []
    for category in loaded["taxonomy"]:
        result.append({
            "category_id": category["category_id"],
            "hangar": loaded["primary_source_counts"][category["category_id"]].get("hangar", 0),
            "voxel": loaded["primary_source_counts"][category["category_id"]].get("voxel", 0),
            "non_dedup": loaded["primary_counts"][category["category_id"]],
        })
    return result


def _qa_checks(loaded: Mapping[str, Any], tables: Mapping[str, Sequence[Mapping[str, Any]]], export_check: Mapping[str, Any], input_sha_after: str, replay_ok: bool, enforce_pinned_inputs: bool) -> dict[str, Any]:
    scope_counts = {
        source: {status: loaded["status_counts"][source].get(status, 0) for status in SCOPE_STATUSES}
        for source in SOURCES
    }
    member_counts = Counter(row["source"] for row in tables["category_signal_member_features"])
    category_counts = {category_id: loaded["primary_counts"].get(category_id, 0) for category_id in loaded["taxonomy_by_id"]}
    overview_order = [row["category_id"] for row in tables["category_signal_overview"]]
    expected_order = [row["category_id"] for row in loaded["taxonomy"]]
    freshness_matches = all(
        member.get("freshness_cohort") == freshness_bucket(member.get("freshness_age_days"))
        for member in tables["category_signal_member_features"]
    )
    hangar_paid_unavailable = all(
        row["paid_evidence_scope"] == "NOT_AVAILABLE_FOR_SOURCE"
        and row["paid_state_counts"] is None
        and row["price_quantiles_by_currency"] is None
        and row["price_available_count"] is None
        for row in tables["category_source_facts"] + tables["subcategory_source_facts"]
        if row["source"] == "hangar"
    )
    voxel_paid_partition = all(
        sum(row["paid_state_counts"].values()) == row["member_count"]
        and row["paid_evidence_observed_count"] == row["paid_state_counts"]["paid"] + row["paid_state_counts"]["free"]
        for row in tables["category_source_facts"] + tables["subcategory_source_facts"]
        if row["source"] == "voxel"
    )
    currency_separated = all(
        row["price_available_count"] == sum(item["available_count"] for item in (row["price_quantiles_by_currency"] or []))
        and [item["currency"] for item in (row["price_quantiles_by_currency"] or [])]
        == sorted(item["currency"] for item in (row["price_quantiles_by_currency"] or []))
        for row in tables["category_source_facts"] + tables["subcategory_source_facts"]
        if row["source"] == "voxel"
    )
    category_primary = {row["category_id"]: row["observed_confirmed_primary_count"] for row in tables["category_source_facts"] if row["source"] == "hangar"}
    category_primary_voxel = {row["category_id"]: row["observed_confirmed_primary_count"] for row in tables["category_source_facts"] if row["source"] == "voxel"}
    primary_reconciled = all(
        category_primary.get(category_id, 0) + category_primary_voxel.get(category_id, 0) == category_counts[category_id]
        for category_id in category_counts
    )
    zero_category_source_rows = sum(row["member_count"] == 0 for row in tables["category_source_facts"])
    zero_subcategory_source_rows = sum(row["member_count"] == 0 for row in tables["subcategory_source_facts"])
    forbidden_fields = {
        "opportunity_state", "whitespace_result", "candidate_direction", "candidate_plugin",
        "attractiveness_score", "category_rank", "winner", "recommended_category", "combined_demand_score",
    }
    exported_rows = [row for rows in tables.values() for row in rows]
    no_forbidden_fields = all(not (forbidden_fields & set(row)) for row in exported_rows)
    no_download_aggregate = all(
        not any(key in row for key in ("downloads_total_sum", "combined_downloads", "cross_source_downloads_total"))
        for row in exported_rows
    )
    no_combined_percentile = all(
        not any("combined" in key.lower() and "percentile" in key.lower() for key in row)
        for row in exported_rows
    )
    export_checks = export_check["checks"]
    checks = {
        "input_sha256_matches_pin": (not enforce_pinned_inputs) or loaded["input_sha256"] == YEE61_DB_SHA256,
        "input_sha256_unchanged": loaded["input_sha256"] == input_sha_after,
        "scope_rows_exact": len(loaded["scope"]) == (10018 if enforce_pinned_inputs else len(loaded["scope"])),
        "confirmed_membership_rows_exact": len(tables["category_signal_member_features"]) == (1352 if enforce_pinned_inputs else len(loaded["memberships"])),
        "confirmed_source_counts_exact": (dict(member_counts) == {"hangar": 1221, "voxel": 131}) if enforce_pinned_inputs else set(member_counts).issubset(SOURCES),
        "scope_status_counts_exact": (scope_counts == EXPECTED_SCOPE_COUNTS) if enforce_pinned_inputs else all(sum(scope_counts[s].values()) > 0 for s in SOURCES),
        "primary_category_counts_exact": (category_counts == EXPECTED_PRIMARY_COUNTS) if enforce_pinned_inputs else primary_reconciled,
        "taxonomy_version_and_hash_exact": (
            loaded["metadata"].get("taxonomy_version") == YEE61_TAXONOMY_VERSION
            and loaded["metadata"].get("taxonomy_sha256") == YEE61_TAXONOMY_SHA256
        ) if enforce_pinned_inputs else bool(loaded["metadata"].get("taxonomy_version") and loaded["metadata"].get("taxonomy_sha256")),
        "category_source_facts_22": len(tables["category_source_facts"]) == (22 if enforce_pinned_inputs else len(loaded["taxonomy"]) * len(SOURCES)),
        "subcategory_source_facts_76": len(tables["subcategory_source_facts"]) == (76 if enforce_pinned_inputs else sum(len(row["subcategories"]) for row in loaded["taxonomy"]) * len(SOURCES)),
        "overview_rows_11": len(tables["category_signal_overview"]) == (11 if enforce_pinned_inputs else len(loaded["taxonomy"])),
        "source_scope_coverage_rows_2": len(tables["source_scope_coverage"]) == len(SOURCES),
        "membership_identity_set_matches_confirmed": len(tables["category_signal_member_features"]) == len(loaded["memberships"])
        and {(row["source"], row["source_resource_id"]) for row in tables["category_signal_member_features"]}
        == {(row["source"], row["source_resource_id"]) for row in loaded["memberships"]},
        "zero_review_or_out_of_scope_leakage": all(
            loaded["scope_by_identity"][(row["source"], row["source_resource_id"])]["product_scope_status"] == "PLUGIN_PRODUCT_CONFIRMED"
            for row in tables["category_signal_member_features"]
        ),
        "category_primary_supply_reconciles": primary_reconciled,
        "zero_member_rows_retained": (
            zero_category_source_rows > 0 and zero_subcategory_source_rows > 0
            and len(tables["category_source_facts"]) == len(loaded["taxonomy"]) * len(SOURCES)
            and len(tables["subcategory_source_facts"]) == sum(len(row["subcategories"]) for row in loaded["taxonomy"]) * len(SOURCES)
        ),
        "overview_is_frozen_taxonomy_order": overview_order == expected_order,
        "no_raw_cross_source_download_aggregate": no_download_aggregate,
        "no_combined_cross_source_percentile": no_combined_percentile,
        "freshness_cohorts_reconcile": freshness_matches,
        "hangar_paid_price_unavailable_semantics": hangar_paid_unavailable,
        "voxel_paid_state_partition_and_denominator": voxel_paid_partition,
        "currency_grouping_is_separate_and_reconciles": currency_separated,
        "no_stage_c_or_opportunity_fields": no_forbidden_fields,
        "jsonl_csv_sqlite_reconcile": all(export_checks.get(f"{table}_{suffix}", False) for table in TABLE_FILES for suffix in ("jsonl_exact", "csv_bytes_exact", "csv_columns_exact", "row_counts_match", "sqlite_exact")),
        "sqlite_integrity_check_ok": export_checks.get("sqlite_integrity_ok", False),
        "sqlite_foreign_key_check_zero": export_checks.get("sqlite_foreign_key_check_zero", False),
        "deterministic_replay_byte_identical": replay_ok,
    }
    return checks


def _create_tables(loaded: Mapping[str, Any], code_commit: str | None) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    member_rows = _member_features(loaded)
    coverage = _scope_coverage(loaded)
    category_rows = _category_source_facts(loaded, member_rows, coverage)
    subcategory_rows = _subcategory_source_facts(loaded, member_rows, coverage)
    overview_rows = _category_overview(loaded, category_rows, coverage)
    tables = {
        "source_scope_coverage": coverage,
        "category_signal_member_features": member_rows,
        "category_source_facts": category_rows,
        "subcategory_source_facts": subcategory_rows,
        "category_signal_overview": overview_rows,
    }
    run_metadata = _run_metadata(loaded, code_commit)
    return tables, run_metadata


def _write_initial_goal(output: Path) -> None:
    goal_path = Path(__file__).resolve().parents[2] / "GOAL_ALIGNMENT.md"
    goal_text = goal_path.read_bytes()
    existing = output / "GOAL_ALIGNMENT.md"
    if existing.exists() and existing.read_bytes() != goal_text:
        raise CategorySignalError("Pre-existing GOAL_ALIGNMENT.md differs from repository alignment gate")
    existing.write_bytes(goal_text)


def build_category_signal_layer(
    input_db: Path | str,
    output_dir: Path | str,
    *,
    code_commit: str | None = None,
    enforce_pinned_inputs: bool = True,
) -> dict[str, Any]:
    """Build the deterministic YEE-73 layer from the accepted YEE-61 SQLite."""
    input_path = Path(input_db).resolve()
    output = Path(output_dir).resolve()
    if not input_path.is_file():
        raise CategorySignalError(f"YEE-61 input database not found: {input_path}")
    output.mkdir(parents=True, exist_ok=True)
    initial_files = {path.name for path in output.iterdir() if path.is_file()}
    if initial_files - {"GOAL_ALIGNMENT.md"}:
        raise CategorySignalError("YEE-73 output directory must be empty except for the pre-implementation GOAL_ALIGNMENT.md")
    input_sha_before = sha256_file(input_path)
    loaded = _load_input(input_path, input_sha_before, enforce_pinned_inputs)
    # Fail the pre-generation semantic gate before writing datasets or the database.
    goal = (Path(__file__).resolve().parents[2] / "GOAL_ALIGNMENT.md").read_text(encoding="utf-8")
    if "GOAL_ALIGNMENT: PASS" not in goal or "PLUGIN_ONLY" not in goal or "Stage B" not in goal:
        raise CategorySignalError("GOAL_ALIGNMENT semantic smoke gate did not pass")
    tables, run_metadata = _create_tables(loaded, code_commit)
    _write_initial_goal(output)
    _write_core(output, loaded, tables, run_metadata)

    export_check = _export_checks(output, tables)
    input_sha_after = sha256_file(input_path)
    replay_ok = False
    with tempfile.TemporaryDirectory(prefix="yee73-replay-") as temp_name:
        replay_dir = Path(temp_name)
        _write_core(replay_dir, loaded, tables, run_metadata)
        replay_ok = all(
            (output / name).read_bytes() == (replay_dir / name).read_bytes()
            for name in (
                "GOAL_ALIGNMENT.md", "CATEGORY_SIGNAL_SCHEMA.md", "SIGNAL_SEMANTICS.md",
                "category_signal_layer.sqlite",
                *(f"{stem}.{ext}" for stem in TABLE_FILES.values() for ext in ("jsonl", "csv")),
            )
        )
    checks = _qa_checks(loaded, tables, export_check, input_sha_after, replay_ok, enforce_pinned_inputs)
    failed = sorted(name for name, passed in checks.items() if not passed)
    if failed:
        raise CategorySignalError(f"YEE-73 output acceptance checks failed: {failed}")
    member_counts = Counter(row["source"] for row in tables["category_signal_member_features"])
    coverage_by_source = {row["source"]: row for row in tables["source_scope_coverage"]}
    qa = {
        "work_order": WORK_ORDER,
        "delivery_status": DELIVERY_STATUS,
        "status": "PASS",
        "signal_schema_version": SIGNAL_SCHEMA_VERSION,
        "run_id": run_metadata["run_id"],
        "code_commit": run_metadata["code_commit"],
        "analysis_as_of": loaded["analysis_as_of"],
        "input_sha256_before": input_sha_before,
        "input_sha256_after": input_sha_after,
        "taxonomy_version": loaded["metadata"]["taxonomy_version"],
        "taxonomy_sha256": loaded["metadata"]["taxonomy_sha256"],
        "input_scope_rows": len(loaded["scope"]),
        "signal_member_rows": len(tables["category_signal_member_features"]),
        "signal_member_source_counts": {source: member_counts.get(source, 0) for source in SOURCES},
        "scope_status_counts": {
            source: {status: loaded["status_counts"][source].get(status, 0) for status in SCOPE_STATUSES}
            for source in SOURCES
        },
        "source_scope_coverage": {source: coverage_by_source[source] for source in SOURCES},
        "category_primary_counts_by_source": _taxonomy_counts(loaded),
        "row_counts": {table: len(rows) for table, rows in tables.items()},
        "export_row_counts": export_check["counts"],
        "checks": checks,
        "check_count": len(checks),
        "passing_checks": sum(checks.values()),
        "failed_checks": failed,
        "deterministic_replay": {
            "all_exports_and_sqlite_byte_identical": replay_ok,
            "verified_artifact_count": len(EXPORTS) - 2,
        },
        "manifest_artifact_count_excluding_manifest": len(EXPORTS),
    }
    _write_final_metadata(output, qa, run_metadata)

    # Replay the complete serialized artifact set, including QA/report/manifest.
    with tempfile.TemporaryDirectory(prefix="yee73-final-replay-") as temp_name:
        replay_dir = Path(temp_name)
        _write_core(replay_dir, loaded, tables, run_metadata)
        _write_final_metadata(replay_dir, qa, run_metadata)
        final_replay_ok = all(
            (output / name).read_bytes() == (replay_dir / name).read_bytes()
            for name in ALL_ARTIFACTS
        )
    if not final_replay_ok:
        raise CategorySignalError("YEE-73 complete deterministic artifact replay was not byte-identical")
    return qa
