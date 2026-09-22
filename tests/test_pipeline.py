from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from market_analysis.pipeline import (
    EXPECTED_SOURCE_COUNTS,
    age_cohort,
    build_feature,
    freshness_cohort,
    normalized_facets,
    percentile_map,
    run_pipeline,
    segment_memberships,
    validate_input,
    voxel_price_band,
)


def _resource(source: str, resource_id: str, **overrides):
    row = {
        "source": source, "source_resource_id": resource_id, "slug": resource_id,
        "title": resource_id, "summary": None, "author": None, "author_id": None,
        "project_type": None, "categories_json": None, "paid": None, "price_amount": None,
        "currency": None, "download_count": None, "review_count": None, "review_average": None,
        "follow_count": None, "star_count": None, "watcher_count": None,
        "published_at": "2026-01-01T00:00:00Z", "updated_at": "2026-09-01T00:00:00Z",
        "supported_versions_json": '["1.20, 1.21"]', "loaders_json": '["Paper, Fabric"]',
        "source_url": None, "source_metrics_json": "{}",
        "first_seen_at": "2026-09-01T00:00:00Z", "last_seen_at": "2026-09-01T00:00:00Z",
    }
    row.update(overrides)
    return row


def _snapshot(source: str, resource_id: str):
    return {"observed_at": "2026-09-01T00:00:00Z", "raw_evidence_path": f"raw/{source}/{resource_id}.json"}


def test_cohort_and_price_boundaries():
    assert age_cohort(90) == "<=90d"
    assert age_cohort(91) == "91-365d"
    assert age_cohort(365) == "91-365d"
    assert age_cohort(366) == "366-1095d"
    assert age_cohort(1095) == "366-1095d"
    assert age_cohort(1096) == ">1095d"
    assert freshness_cohort(30) == "<=30d"
    assert freshness_cohort(31) == "31-90d"
    assert freshness_cohort(90) == "31-90d"
    assert freshness_cohort(91) == "91-365d"
    assert freshness_cohort(366) == ">365d"
    assert voxel_price_band("voxel", 0, 0) == "free"
    assert voxel_price_band("voxel", 1, 5) == ">0-5"
    assert voxel_price_band("voxel", 1, 10) == ">5-10"
    assert voxel_price_band("voxel", 1, 50) == ">20-50"
    assert voxel_price_band("voxel", 1, 50.01) == ">50"
    assert voxel_price_band("modrinth", None, None) is None


def test_feature_source_mapping_nulls_and_facets():
    as_of = __import__("datetime").datetime.fromisoformat("2026-09-22T17:13:34+00:00")
    voxel = build_feature(
        _resource("voxel", "1", paid=1, price_amount=7.5, download_count=None,
                  categories_json=None, loaders_json='["Spigot, Paper"]'),
        _snapshot("voxel", "1"),
        {"enrichment_id": "e", "download_count": 9, "review_count": 2, "review_stars": 4.5,
         "latest_update_id": "u", "latest_update_version": "1.2", "latest_update_at": "2026-09-20T00:00:00Z",
         "source_metrics_json": '{"latest_update_beta":false}'},
        as_of,
    )
    modrinth = build_feature(
        _resource("modrinth", "m", project_type="mod", categories_json='["Magic, Fabric"]',
                  download_count=0, follow_count=0, source_metrics_json='{"license":"MIT"}'),
        _snapshot("modrinth", "m"), None, as_of,
    )
    hangar = build_feature(
        _resource("hangar", "h", project_type="admin_tools", download_count=12, star_count=3,
                  watcher_count=2, source_metrics_json='{"stats":{"recentDownloads":4,"recentViews":8}}'),
        _snapshot("hangar", "h"), None, as_of,
    )
    assert voxel["downloads_total"] == 9 and voxel["download_count"] is None
    assert voxel["demand_metric_source"] == "voxel.getResourceInfo.downloads"
    assert voxel["voxel_price_band"] == ">5-10"
    assert json.loads(voxel["loader_facets_json"]) == ["paper", "spigot"]
    assert modrinth["downloads_total"] == 0 and modrinth["follow_count"] == 0
    assert json.loads(modrinth["category_facets_json"]) == ["fabric", "magic"]
    assert hangar["hangar_recent_downloads"] == 4 and hangar["hangar_recent_views"] == 8
    assert set(segment_memberships(modrinth)) >= {("source", "modrinth", "modrinth"), ("source_category_tag", "modrinth", "fabric")}


def test_percentile_ties_are_source_local():
    rows = [{"source": "a", "downloads_total": value} for value in [1, 1, 2, 3]]
    rows += [{"source": "b", "downloads_total": value} for value in [1, 100]]
    for row in rows:
        row["demand_percentile"] = None
    percentile_map(rows)
    assert rows[0]["demand_percentile"] == rows[1]["demand_percentile"]
    assert rows[3]["demand_percentile"] == 100
    assert rows[4]["demand_percentile"] == 0
    assert rows[5]["demand_percentile"] == 100


def _make_fixture_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript("""
    CREATE TABLE resources (source TEXT, source_resource_id TEXT, slug TEXT, title TEXT, summary TEXT,
      author TEXT, author_id TEXT, project_type TEXT, categories_json TEXT, paid INTEGER, price_amount REAL,
      currency TEXT, download_count INTEGER, review_count INTEGER, review_average REAL, follow_count INTEGER,
      star_count INTEGER, watcher_count INTEGER, published_at TEXT, updated_at TEXT, supported_versions_json TEXT,
      loaders_json TEXT, source_url TEXT, source_metrics_json TEXT, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
      PRIMARY KEY(source, source_resource_id));
    CREATE TABLE resource_snapshots (snapshot_id INTEGER PRIMARY KEY, sync_id TEXT, source TEXT,
      source_resource_id TEXT, observed_at TEXT, title TEXT, price_amount REAL, currency TEXT, download_count INTEGER,
      review_count INTEGER, review_average REAL, follow_count INTEGER, star_count INTEGER, watcher_count INTEGER,
      updated_at TEXT, source_metrics_json TEXT, raw_evidence_path TEXT);
    CREATE TABLE sync_runs (sync_id TEXT PRIMARY KEY, source TEXT, started_at TEXT, completed_at TEXT,
      status TEXT, pages_requested INTEGER, pages_completed INTEGER, resources_seen INTEGER, resources_inserted INTEGER,
      resources_updated INTEGER, http_requests INTEGER, rate_limit_events INTEGER, retry_count INTEGER,
      rate_limit_wait_seconds REAL, error_message TEXT, next_cursor TEXT, limit_count INTEGER, page_size INTEGER);
    CREATE TABLE enrichment_runs (enrichment_id TEXT PRIMARY KEY, source TEXT, started_at TEXT, completed_at TEXT,
      status TEXT, target_count INTEGER, processed_count INTEGER, succeeded_count INTEGER, unavailable_count INTEGER,
      failed_count INTEGER, http_requests INTEGER, rate_limit_events INTEGER, retry_count INTEGER,
      rate_limit_wait_seconds REAL, next_ordinal INTEGER, limit_count INTEGER, error_message TEXT);
    CREATE TABLE enrichment_observations (enrichment_id TEXT, ordinal INTEGER, source TEXT, source_resource_id TEXT,
      outcome TEXT, attempt_count INTEGER, observed_at TEXT, raw_evidence_path TEXT, http_status INTEGER,
      error_message TEXT, download_count INTEGER, review_count INTEGER, review_stars REAL, latest_update_id TEXT,
      latest_update_version TEXT, latest_update_at TEXT, source_metrics_json TEXT);
    """)
    rows = [
        _resource("voxel", "1", paid=0, price_amount=0, download_count=None, categories_json=None,
                  source_metrics_json='{"can_download":true}'),
        _resource("modrinth", "m", project_type="mod", categories_json='["Fabric"]', download_count=0,
                  follow_count=0, source_metrics_json='{}'),
        _resource("hangar", "h", project_type="admin_tools", download_count=12, star_count=1,
                  watcher_count=2, source_metrics_json='{"stats":{"recentDownloads":4,"recentViews":8}}'),
    ]
    cols = list(rows[0])
    conn.executemany(f"INSERT INTO resources ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})", ([row[c] for c in cols] for row in rows))
    for i, row in enumerate(rows):
        conn.execute("INSERT INTO resource_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (i+1, row["source"], row["source"], row["source_resource_id"], "2026-09-01T00:00:00Z", row["title"], row["price_amount"], row["currency"], row["download_count"], row["review_count"], row["review_average"], row["follow_count"], row["star_count"], row["watcher_count"], row["updated_at"], row["source_metrics_json"], f"raw/{row['source']}/{row['source_resource_id']}.json"))
        conn.execute("INSERT INTO sync_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (row["source"], row["source"], "2026-09-01T00:00:00Z", "2026-09-01T00:01:00Z", "succeeded", 1, 1, 1, 1, 0, 1, 0, 0, 0, None, None, None, 1))
    conn.execute("INSERT INTO enrichment_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ("e", "voxel", "2026-09-01T00:00:00Z", "2026-09-01T00:02:00Z", "succeeded", 1, 1, 1, 0, 0, 1, 0, 0, 0, 1, None, None))
    conn.execute("INSERT INTO enrichment_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ("e", 0, "voxel", "1", "succeeded", 1, "2026-09-01T00:00:00Z", "raw/voxel/1.json", 200, None, 8, 1, 5.0, "u", "1.0", "2026-08-01T00:00:00Z", "{}"))
    conn.commit()
    conn.close()


def test_fixture_pipeline_is_deterministic_and_read_only(tmp_path):
    input_db = tmp_path / "input.db"
    _make_fixture_db(input_db)
    before = input_db.read_bytes()
    first = run_pipeline(input_db, tmp_path / "out1", "2026-09-22T17:13:34Z", code_version="fixture", expected_counts={"voxel": 1, "modrinth": 1, "hangar": 1}, expected_voxel_enrichment=1)
    second = run_pipeline(input_db, tmp_path / "out2", "2026-09-22T17:13:34Z", code_version="fixture", expected_counts={"voxel": 1, "modrinth": 1, "hangar": 1}, expected_voxel_enrichment=1)
    assert input_db.read_bytes() == before
    assert first["qa"]["status"] == "PASS"
    assert first["run_id"] == second["run_id"]
    assert (tmp_path / "out1/exports/resource_features.jsonl").read_bytes() == (tmp_path / "out2/exports/resource_features.jsonl").read_bytes()
    assert json.loads((tmp_path / "out1/YEE-29_DATASET_MANIFEST.json").read_text(encoding="utf-8"))["qa"]["output"]["resource_features"] == 3


def test_input_count_guardrail(tmp_path):
    input_db = tmp_path / "input.db"
    _make_fixture_db(input_db)
    conn = sqlite3.connect(input_db)
    with pytest.raises(ValueError, match="source counts mismatch"):
        validate_input(conn, EXPECTED_SOURCE_COUNTS)
    conn.close()

