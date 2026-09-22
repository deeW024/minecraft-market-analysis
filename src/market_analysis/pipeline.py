from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from . import SCHEMA_VERSION

EXPECTED_SOURCE_COUNTS = {"voxel": 6639, "modrinth": 158507, "hangar": 3861}
EXPECTED_TOTAL = sum(EXPECTED_SOURCE_COUNTS.values())
EXPECTED_VOXEL_ENRICHMENT = 6639
NULL_TOKEN = r"\N"

RESOURCE_COLUMNS = [
    "source", "source_resource_id", "slug", "title", "summary", "author",
    "author_id", "project_type", "categories_json", "paid", "price_amount",
    "currency", "download_count", "review_count", "review_average",
    "follow_count", "star_count", "watcher_count", "published_at", "updated_at",
    "supported_versions_json", "loaders_json", "source_url", "source_metrics_json",
    "first_seen_at", "last_seen_at",
]

FEATURE_COLUMNS = RESOURCE_COLUMNS + [
    "canonical_identity", "snapshot_observed_at", "snapshot_raw_evidence_path",
    "enrichment_id", "voxel_demand_download_count", "voxel_review_count",
    "voxel_review_stars", "voxel_latest_update_id", "voxel_latest_update_version",
    "voxel_latest_update_at", "voxel_source_metrics_json", "modrinth_source_metrics_json",
    "hangar_source_metrics_json", "hangar_recent_downloads", "hangar_recent_views",
    "age_days", "update_age_days", "latest_update_age_days", "freshness_age_days",
    "freshness_cohort", "age_cohort", "paid_state", "voxel_price_band",
    "project_type_norm", "category_facets_json", "loader_facets_json",
    "version_facets_json", "demand_download_count", "downloads_total",
    "demand_metric_source", "demand_percentile", "feature_schema_version",
    "analysis_as_of",
]

SECONDARY_COLUMNS = [
    "voxel_review_count", "voxel_review_stars", "follow_count", "star_count",
    "watcher_count", "hangar_recent_downloads", "hangar_recent_views",
]

SEGMENT_COLUMNS = [
    "lens", "source", "facet_value", "resource_count", "demand_available_count",
    "downloads_p50", "downloads_p75", "downloads_p90", "downloads_p95", "downloads_p99",
    "age_days_p50", "update_age_days_p50", "latest_update_age_days_p50",
    "voxel_review_count_p50", "voxel_review_stars_p50", "follow_count_p50",
    "star_count_p50", "watcher_count_p50", "hangar_recent_downloads_p50",
    "hangar_recent_views_p50",
]

DISTRIBUTION_COLUMNS = [
    "distribution", "source", "metric", "bucket", "resource_count", "available_count",
    "p50", "p75", "p90", "p95", "p99", "total", "top_1_percent_share",
    "top_10_percent_share",
]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def canonical_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def whole_age_days(value: str | None, analysis_as_of: datetime) -> int | None:
    parsed = parse_iso(value)
    if parsed is None:
        return None
    delta = (analysis_as_of - parsed).total_seconds()
    if delta < 0:
        return None
    return math.floor(delta / 86400)


def parse_json(value: str | None, field: str) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON in {field}") from exc


def normalize_token(value: Any) -> str | None:
    if value is None:
        return None
    token = re.sub(r"[^a-z0-9._-]+", "-", str(value).strip().lower())
    token = re.sub(r"-+", "-", token).strip("-")
    return token or None


def normalized_facets(value: str | None, field: str) -> list[str]:
    parsed = parse_json(value, field)
    if parsed is None:
        return []
    values = parsed if isinstance(parsed, list) else [parsed]
    tokens: set[str] = set()
    for item in values:
        for part in re.split(r"[,;|]", str(item)):
            token = normalize_token(part)
            if token:
                tokens.add(token)
    return sorted(tokens)


def json_compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def age_cohort(age: int | None) -> str:
    if age is None:
        return "unknown"
    if age <= 90:
        return "<=90d"
    if age <= 365:
        return "91-365d"
    if age <= 1095:
        return "366-1095d"
    return ">1095d"


def freshness_cohort(age: int | None) -> str:
    if age is None:
        return "unknown"
    if age <= 30:
        return "<=30d"
    if age <= 90:
        return "31-90d"
    if age <= 365:
        return "91-365d"
    return ">365d"


def paid_state(source: str, paid: int | None) -> str:
    if source != "voxel" or paid is None:
        return "unknown"
    return "paid" if int(paid) else "free"


def voxel_price_band(source: str, paid: int | None, amount: float | None) -> str | None:
    if source != "voxel":
        return None
    if paid == 0 or (amount is not None and amount <= 0):
        return "free"
    if amount is None:
        return "unknown"
    if amount <= 5:
        return ">0-5"
    if amount <= 10:
        return ">5-10"
    if amount <= 20:
        return ">10-20"
    if amount <= 50:
        return ">20-50"
    return ">50"


def round_number(value: float | int | None) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    rounded = round(float(value), 6)
    return int(rounded) if rounded.is_integer() else rounded


def quantile(values: Iterable[float | int], probability: float) -> float | int | None:
    ordered = sorted(float(value) for value in values if value is not None)
    if not ordered:
        return None
    if len(ordered) == 1:
        return round_number(ordered[0])
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round_number(ordered[lower])
    return round_number(ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower))


def percentile_map(rows: list[dict[str, Any]]) -> None:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        value = row["downloads_total"]
        if value is not None:
            grouped[row["source"]].append(float(value))
    for source, values in grouped.items():
        counts = Counter(values)
        n = len(values)
        for row in rows:
            if row["source"] != source or row["downloads_total"] is None:
                continue
            value = float(row["downloads_total"])
            lower = sum(count for candidate, count in counts.items() if candidate < value)
            ties = counts[value]
            percentile = 100.0 if n == 1 else 100.0 * (lower + (ties - 1) / 2) / (n - 1)
            row["demand_percentile"] = round_number(percentile)


def _metric_stats(values: Iterable[Any]) -> dict[str, Any]:
    usable = [value for value in values if value is not None]
    return {
        "available_count": len(usable),
        "p50": quantile(usable, 0.50),
        "p75": quantile(usable, 0.75),
        "p90": quantile(usable, 0.90),
        "p95": quantile(usable, 0.95),
        "p99": quantile(usable, 0.99),
    }


def _source_metric_values(row: dict[str, Any], metric: str) -> Any:
    return row.get(metric)


def build_feature(resource: dict[str, Any], snapshot: dict[str, Any], observation: dict[str, Any] | None,
                  analysis_as_of: datetime) -> dict[str, Any]:
    source = resource["source"]
    row = {column: resource.get(column) for column in RESOURCE_COLUMNS}
    row["canonical_identity"] = f"{source}:{resource['source_resource_id']}"
    row["snapshot_observed_at"] = snapshot.get("observed_at") if snapshot else None
    row["snapshot_raw_evidence_path"] = snapshot.get("raw_evidence_path") if snapshot else None
    row["enrichment_id"] = observation.get("enrichment_id") if observation else None
    row["voxel_demand_download_count"] = observation.get("download_count") if source == "voxel" and observation else None
    row["voxel_review_count"] = observation.get("review_count") if source == "voxel" and observation else None
    row["voxel_review_stars"] = observation.get("review_stars") if source == "voxel" and observation else None
    row["voxel_latest_update_id"] = observation.get("latest_update_id") if source == "voxel" and observation else None
    row["voxel_latest_update_version"] = observation.get("latest_update_version") if source == "voxel" and observation else None
    latest_at = observation.get("latest_update_at") if source == "voxel" and observation else None
    if latest_at and latest_at.startswith("1970-") and not row["voxel_latest_update_id"]:
        latest_at = None
    row["voxel_latest_update_at"] = latest_at
    row["voxel_source_metrics_json"] = observation.get("source_metrics_json") if source == "voxel" and observation else None
    row["modrinth_source_metrics_json"] = resource.get("source_metrics_json") if source == "modrinth" else None
    row["hangar_source_metrics_json"] = resource.get("source_metrics_json") if source == "hangar" else None

    hangar_metrics = parse_json(resource.get("source_metrics_json"), "source_metrics_json") if source == "hangar" else {}
    stats = hangar_metrics.get("stats") if isinstance(hangar_metrics, dict) else {}
    stats = stats if isinstance(stats, dict) else {}
    row["hangar_recent_downloads"] = stats.get("recentDownloads")
    row["hangar_recent_views"] = stats.get("recentViews")

    row["age_days"] = whole_age_days(resource.get("published_at"), analysis_as_of)
    row["update_age_days"] = whole_age_days(resource.get("updated_at"), analysis_as_of)
    row["latest_update_age_days"] = whole_age_days(latest_at, analysis_as_of) if source == "voxel" else None
    row["freshness_age_days"] = row["latest_update_age_days"] if source == "voxel" and row["latest_update_age_days"] is not None else row["update_age_days"]
    row["freshness_cohort"] = freshness_cohort(row["freshness_age_days"])
    row["age_cohort"] = age_cohort(row["age_days"])
    row["paid_state"] = paid_state(source, resource.get("paid"))
    row["voxel_price_band"] = voxel_price_band(source, resource.get("paid"), resource.get("price_amount"))
    row["project_type_norm"] = normalize_token(resource.get("project_type"))
    categories = normalized_facets(resource.get("categories_json"), "categories_json")
    if not categories and row["project_type_norm"]:
        categories = [row["project_type_norm"]]
    row["category_facets_json"] = json_compact(categories)
    row["loader_facets_json"] = json_compact(normalized_facets(resource.get("loaders_json"), "loaders_json"))
    row["version_facets_json"] = json_compact(normalized_facets(resource.get("supported_versions_json"), "supported_versions_json"))

    if source == "voxel":
        demand = row["voxel_demand_download_count"]
        metric_source = "voxel.getResourceInfo.downloads" if demand is not None else None
    elif source == "modrinth":
        demand = resource.get("download_count")
        metric_source = "modrinth.project.downloads" if demand is not None else None
    elif source == "hangar":
        demand = resource.get("download_count")
        metric_source = "hangar.project.stats.downloads" if demand is not None else None
    else:
        raise ValueError(f"unexpected source: {source}")
    row["demand_download_count"] = demand
    row["downloads_total"] = demand
    row["demand_metric_source"] = metric_source
    row["demand_percentile"] = None
    row["feature_schema_version"] = SCHEMA_VERSION
    row["analysis_as_of"] = canonical_iso(analysis_as_of)
    return row


def _facet_values(row: dict[str, Any], column: str) -> list[str]:
    return json.loads(row[column])


def segment_memberships(row: dict[str, Any]) -> list[tuple[str, str, str]]:
    memberships: list[tuple[str, str, str]] = [("source", row["source"], row["source"])]
    if row["project_type_norm"]:
        memberships.append(("source_project_type", row["source"], row["project_type_norm"]))
    for value in _facet_values(row, "category_facets_json"):
        memberships.append(("source_category_tag", row["source"], value))
    for value in _facet_values(row, "loader_facets_json"):
        memberships.append(("source_loader", row["source"], value))
    for value in _facet_values(row, "version_facets_json"):
        memberships.append(("source_supported_version", row["source"], value))
    if row["source"] == "voxel":
        memberships.append(("voxel_paid_state", row["source"], row["paid_state"]))
        if row["voxel_price_band"]:
            memberships.append(("voxel_price_band", row["source"], row["voxel_price_band"]))
    memberships.append(("freshness_cohort", row["source"], row["freshness_cohort"]))
    memberships.append(("age_cohort", row["source"], row["age_cohort"]))
    return memberships


def build_segment_facts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for membership in segment_memberships(row):
            groups[membership].append(row)
    facts: list[dict[str, Any]] = []
    for (lens, source, facet_value), members in sorted(groups.items()):
        demand = _metric_stats(row["downloads_total"] for row in members)
        age = _metric_stats(row["age_days"] for row in members)
        update = _metric_stats(row["update_age_days"] for row in members)
        latest = _metric_stats(row["latest_update_age_days"] for row in members)
        fact: dict[str, Any] = {
            "lens": lens,
            "source": source,
            "facet_value": facet_value,
            "resource_count": len(members),
            "demand_available_count": demand["available_count"],
            "downloads_p50": demand["p50"], "downloads_p75": demand["p75"],
            "downloads_p90": demand["p90"], "downloads_p95": demand["p95"], "downloads_p99": demand["p99"],
            "age_days_p50": age["p50"], "update_age_days_p50": update["p50"],
            "latest_update_age_days_p50": latest["p50"],
        }
        for source_column, output_column in [
            ("voxel_review_count", "voxel_review_count_p50"),
            ("voxel_review_stars", "voxel_review_stars_p50"),
            ("follow_count", "follow_count_p50"),
            ("star_count", "star_count_p50"),
            ("watcher_count", "watcher_count_p50"),
            ("hangar_recent_downloads", "hangar_recent_downloads_p50"),
            ("hangar_recent_views", "hangar_recent_views_p50"),
        ]:
            fact[output_column] = quantile((row.get(source_column) for row in members), 0.50)
        facts.append(fact)
    return facts


def build_source_distributions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    distributions: list[dict[str, Any]] = []
    for source in sorted({row["source"] for row in rows}):
        members = [row for row in rows if row["source"] == source]
        values = [float(row["downloads_total"]) for row in members if row["downloads_total"] is not None]
        stats = _metric_stats(values)
        total = sum(values) if values else None
        top_1 = math.ceil(len(values) * 0.01) if values else 0
        top_10 = math.ceil(len(values) * 0.10) if values else 0
        ordered = sorted(values, reverse=True)
        def share(count: int) -> float | None:
            if not values or not total:
                return None
            return round_number(sum(ordered[:count]) / total)
        distributions.append({
            "distribution": "demand", "source": source, "metric": "downloads_total", "bucket": None,
            "resource_count": len(members), "available_count": stats["available_count"],
            "p50": stats["p50"], "p75": stats["p75"], "p90": stats["p90"],
            "p95": stats["p95"], "p99": stats["p99"], "total": round_number(total),
            "top_1_percent_share": share(top_1), "top_10_percent_share": share(top_10),
        })

    voxel = [row for row in rows if row["source"] == "voxel"]
    for bucket in ["all", "free", "paid", "unknown"]:
        members = voxel if bucket == "all" else [row for row in voxel if row["paid_state"] == bucket]
        values = [float(row["price_amount"]) for row in members if row["price_amount"] is not None]
        stats = _metric_stats(values)
        distributions.append({
            "distribution": "price", "source": "voxel", "metric": "price_amount", "bucket": bucket,
            "resource_count": len(members), "available_count": stats["available_count"],
            "p50": stats["p50"], "p75": stats["p75"], "p90": stats["p90"],
            "p95": stats["p95"], "p99": stats["p99"], "total": round_number(sum(values)) if values else None,
            "top_1_percent_share": None, "top_10_percent_share": None,
        })
    return sorted(distributions, key=lambda row: (row["distribution"], row["source"], row["metric"], row["bucket"] or ""))


def validate_input(conn: sqlite3.Connection, expected_counts: dict[str, int] = EXPECTED_SOURCE_COUNTS,
                   expected_voxel_enrichment: int = EXPECTED_VOXEL_ENRICHMENT) -> dict[str, Any]:
    counts = dict(conn.execute("SELECT source, COUNT(*) FROM resources GROUP BY source").fetchall())
    expected_total = sum(expected_counts.values())
    if counts != expected_counts:
        raise ValueError(f"input source counts mismatch: expected {expected_counts}, got {counts}")
    resources_total = conn.execute("SELECT COUNT(*) FROM resources").fetchone()[0]
    snapshots_total = conn.execute("SELECT COUNT(*) FROM resource_snapshots").fetchone()[0]
    if resources_total != expected_total or snapshots_total != expected_total:
        raise ValueError(f"input totals mismatch: resources={resources_total}, snapshots={snapshots_total}")
    duplicates = conn.execute("SELECT COUNT(*) FROM (SELECT source, source_resource_id FROM resources GROUP BY source, source_resource_id HAVING COUNT(*) > 1)").fetchone()[0]
    if duplicates:
        raise ValueError(f"input has {duplicates} duplicate identities")
    voxel_enrichment = conn.execute("SELECT COUNT(*) FROM enrichment_observations WHERE source='voxel' AND outcome='succeeded'").fetchone()[0]
    if voxel_enrichment != expected_voxel_enrichment:
        raise ValueError(f"Voxel enrichment count mismatch: expected {expected_voxel_enrichment}, got {voxel_enrichment}")
    non_success = conn.execute("SELECT COUNT(*) FROM enrichment_observations WHERE source='voxel' AND outcome!='succeeded'").fetchone()[0]
    if non_success:
        raise ValueError(f"input contains {non_success} non-success Voxel observations")
    failed_runs = conn.execute("SELECT COUNT(*) FROM sync_runs WHERE status!='succeeded'").fetchone()[0]
    if failed_runs:
        raise ValueError(f"input contains {failed_runs} non-success sync runs")
    return {"source_counts": counts, "resources_total": resources_total, "snapshots_total": snapshots_total,
            "voxel_enrichment_succeeded": voxel_enrichment, "voxel_enrichment_non_success": non_success}


def derive_analysis_as_of(conn: sqlite3.Connection) -> datetime:
    values = [row[0] for row in conn.execute("SELECT completed_at FROM sync_runs WHERE status='succeeded' AND completed_at IS NOT NULL")]
    values += [row[0] for row in conn.execute("SELECT completed_at FROM enrichment_runs WHERE status='succeeded' AND completed_at IS NOT NULL")]
    parsed = [parse_iso(value) for value in values]
    parsed = [value for value in parsed if value is not None]
    if not parsed:
        raise ValueError("cannot derive analysis_as_of from accepted run metadata")
    return max(parsed)


def _latest_snapshot_rows(conn: sqlite3.Connection) -> dict[tuple[str, str], dict[str, Any]]:
    latest: dict[str, tuple[str, str]] = {}
    for row in conn.execute("SELECT source, sync_id, completed_at FROM sync_runs WHERE status='succeeded' ORDER BY source, completed_at, sync_id"):
        latest[row[0]] = (row[1], row[2])
    selected = {sync_id for sync_id, _ in latest.values()}
    placeholders = ",".join("?" for _ in selected)
    result: dict[tuple[str, str], dict[str, Any]] = {}
    if not selected:
        return result
    cursor = conn.execute(f"SELECT source, source_resource_id, observed_at, raw_evidence_path FROM resource_snapshots WHERE sync_id IN ({placeholders})", tuple(sorted(selected)))
    for row in cursor:
        result[(row[0], row[1])] = {"source": row[0], "source_resource_id": row[1], "observed_at": row[2], "raw_evidence_path": row[3]}
    return result


def _latest_voxel_observations(conn: sqlite3.Connection) -> dict[tuple[str, str], dict[str, Any]]:
    run = conn.execute("SELECT enrichment_id FROM enrichment_runs WHERE source='voxel' AND status='succeeded' ORDER BY completed_at DESC, enrichment_id DESC LIMIT 1").fetchone()
    if not run:
        return {}
    columns = ["enrichment_id", "source_resource_id", "outcome", "download_count", "review_count", "review_stars",
               "latest_update_id", "latest_update_version", "latest_update_at", "source_metrics_json"]
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for values in conn.execute("SELECT enrichment_id, source_resource_id, outcome, download_count, review_count, review_stars, latest_update_id, latest_update_version, latest_update_at, source_metrics_json FROM enrichment_observations WHERE enrichment_id=? AND source='voxel'", (run[0],)):
        row = dict(zip(columns, values))
        if row["outcome"] == "succeeded":
            result[("voxel", row["source_resource_id"])] = row
    return result


def create_analysis_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(f"""
    PRAGMA foreign_keys=ON;
    CREATE TABLE analysis_runs (
        run_id TEXT PRIMARY KEY,
        analysis_as_of TEXT NOT NULL,
        input_db_sha256 TEXT NOT NULL,
        input_db_size_bytes INTEGER NOT NULL,
        input_resources_count INTEGER NOT NULL,
        input_snapshots_count INTEGER NOT NULL,
        input_voxel_enrichment_count INTEGER NOT NULL,
        source_counts_json TEXT NOT NULL,
        analysis_code_version TEXT NOT NULL,
        feature_schema_version TEXT NOT NULL
    );
    CREATE TABLE resource_features (
        {', '.join(f'{column} {"INTEGER" if column in ("paid", "download_count", "review_count", "follow_count", "star_count", "watcher_count", "voxel_demand_download_count", "voxel_review_count", "hangar_recent_downloads", "hangar_recent_views", "age_days", "update_age_days", "latest_update_age_days", "freshness_age_days") else "REAL" if column in ("price_amount", "review_average", "voxel_review_stars", "demand_percentile") else "TEXT"}' for column in FEATURE_COLUMNS)},
        PRIMARY KEY (source, source_resource_id)
    );
    CREATE TABLE segment_facts (
        {', '.join(f'{column} {"INTEGER" if column in ("resource_count", "demand_available_count") else "REAL" if column.endswith('_p50') or column.startswith('downloads_p') else "TEXT"}' for column in SEGMENT_COLUMNS)},
        PRIMARY KEY (lens, source, facet_value)
    );
    CREATE TABLE source_distributions (
        {', '.join(f'{column} {"INTEGER" if column in ("resource_count", "available_count") else "REAL" if column.startswith('p') or column.endswith('_share') or column == 'total' else "TEXT"}' for column in DISTRIBUTION_COLUMNS)},
        PRIMARY KEY (distribution, source, metric, bucket)
    );
    """)


def _insert_rows(conn: sqlite3.Connection, table: str, columns: list[str], rows: list[dict[str, Any]]) -> None:
    placeholders = ",".join("?" for _ in columns)
    sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
    conn.executemany(sql, ([row.get(column) for column in columns] for row in rows))


def export_table(conn: sqlite3.Connection, table: str, columns: list[str], output_dir: Path, stem: str,
                 order_by: str) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = [dict(zip(columns, values)) for values in conn.execute(f"SELECT {','.join(columns)} FROM {table} ORDER BY {order_by}")]
    jsonl = output_dir / f"{stem}.jsonl"
    csv_path = output_dir / f"{stem}.csv"
    with jsonl.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=False) + "\n")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow([NULL_TOKEN if row[column] is None else row[column] for column in columns])
    return [jsonl, csv_path]


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _artifact_info(path: Path, root: Path) -> dict[str, Any]:
    return {"path": str(path.relative_to(root)).replace("\\", "/"), "bytes": path.stat().st_size, "sha256": sha256_file(path)}


def run_pipeline(input_db: Path, output_dir: Path, analysis_as_of: str | None = None,
                 code_version: str = "working-tree", expected_counts: dict[str, int] = EXPECTED_SOURCE_COUNTS,
                 expected_voxel_enrichment: int = EXPECTED_VOXEL_ENRICHMENT) -> dict[str, Any]:
    input_db = input_db.resolve()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    analysis_db = output_dir / "analysis.db"
    if analysis_db.exists():
        raise FileExistsError(f"refusing to overwrite immutable output: {analysis_db}")
    input_hash_before = sha256_file(input_db)
    input_size = input_db.stat().st_size
    read_uri = f"file:{input_db.as_posix()}?mode=ro"
    input_conn = sqlite3.connect(read_uri, uri=True)
    input_conn.row_factory = sqlite3.Row
    input_conn.execute("PRAGMA query_only=ON")
    try:
        input_qa = validate_input(input_conn, expected_counts, expected_voxel_enrichment)
        as_of = parse_iso(analysis_as_of) if analysis_as_of else derive_analysis_as_of(input_conn)
        if as_of is None:
            raise ValueError("invalid analysis_as_of")
        analysis_as_of_iso = canonical_iso(as_of)
        snapshots = _latest_snapshot_rows(input_conn)
        observations = _latest_voxel_observations(input_conn)
        rows: list[dict[str, Any]] = []
        for values in input_conn.execute("SELECT * FROM resources ORDER BY source, source_resource_id"):
            resource = {key: values[key] for key in values.keys()}
            key = (resource["source"], resource["source_resource_id"])
            if key not in snapshots:
                raise ValueError(f"missing baseline snapshot for {key}")
            rows.append(build_feature(resource, snapshots[key], observations.get(key), as_of))
    finally:
        input_conn.close()
    percentile_map(rows)
    segments = build_segment_facts(rows)
    distributions = build_source_distributions(rows)

    run_id = hashlib.sha256(f"{input_hash_before}:{analysis_as_of_iso}:{SCHEMA_VERSION}".encode()).hexdigest()
    out_conn = sqlite3.connect(analysis_db)
    try:
        create_analysis_schema(out_conn)
        out_conn.execute("INSERT INTO analysis_runs VALUES (?,?,?,?,?,?,?,?,?,?)", (
            run_id, analysis_as_of_iso, input_hash_before, input_size, len(rows),
            input_qa["snapshots_total"], input_qa["voxel_enrichment_succeeded"],
            json_compact(input_qa["source_counts"]), code_version, SCHEMA_VERSION,
        ))
        _insert_rows(out_conn, "resource_features", FEATURE_COLUMNS, rows)
        _insert_rows(out_conn, "segment_facts", SEGMENT_COLUMNS, segments)
        _insert_rows(out_conn, "source_distributions", DISTRIBUTION_COLUMNS, distributions)
        out_conn.commit()
        exports_dir = output_dir / "exports"
        export_paths = []
        export_paths += export_table(out_conn, "resource_features", FEATURE_COLUMNS, exports_dir, "resource_features", "source, source_resource_id")
        export_paths += export_table(out_conn, "segment_facts", SEGMENT_COLUMNS, exports_dir, "segment_facts", "lens, source, facet_value")
        export_paths += export_table(out_conn, "source_distributions", DISTRIBUTION_COLUMNS, exports_dir, "source_distributions", "distribution, source, metric, bucket")
    finally:
        out_conn.close()

    input_hash_after = sha256_file(input_db)
    qa = {
        "status": "PASS" if input_hash_before == input_hash_after and len(rows) == sum(expected_counts.values()) else "FAIL",
        "input": input_qa,
        "output": {"resource_features": len(rows), "segment_facts": len(segments), "source_distributions": len(distributions),
                   "source_counts": dict(sorted(Counter(row["source"] for row in rows).items()))},
        "analysis_as_of": analysis_as_of_iso,
        "run_id": run_id,
        "input_hash_before": input_hash_before,
        "input_hash_after": input_hash_after,
        "input_unchanged": input_hash_before == input_hash_after,
        "null_semantics": "missing values remain null in JSONL and \\N in CSV; recorded zero remains zero",
        "percentile_semantics": "source-local average rank, rounded to six decimals; nulls excluded",
        "facet_semantics": "multi-valued facets are membership counts, not mutually exclusive totals",
        "non_goals_verified": ["no API calls", "no matching", "no embeddings", "no JEV/LLM", "no scoring", "no ranking", "no shortlist"],
    }
    _write_json(output_dir / "YEE-29_QA_RESULT.json", qa)
    artifacts = [_artifact_info(analysis_db, output_dir)] + [_artifact_info(path, output_dir) for path in export_paths]
    artifacts += [_artifact_info(output_dir / "YEE-29_QA_RESULT.json", output_dir)]
    manifest = {"status": qa["status"], "run_id": run_id, "analysis_as_of": analysis_as_of_iso,
                "analysis_code_version": code_version, "feature_schema_version": SCHEMA_VERSION,
                "input": {"path": str(input_db), "bytes": input_size, "sha256": input_hash_before},
                "artifacts": artifacts, "qa": qa}
    _write_json(output_dir / "YEE-29_DATASET_MANIFEST.json", manifest)
    return {"run_id": run_id, "analysis_as_of": analysis_as_of_iso, "qa": qa,
            "manifest": manifest, "analysis_db": analysis_db, "export_paths": export_paths}

