from __future__ import annotations

import copy
import json
import sqlite3
from collections import Counter
from pathlib import Path

import pytest

from market_analysis.category_signals import (
    CategorySignalError,
    _fact_signal_metrics,
    add_group_demand_percentiles,
    build_category_signal_layer,
    canonical_json,
    freshness_bucket,
    sample_size_band,
    sha256_file,
)


def _feature(source: str, resource_id: str, **overrides):
    values = {
        "source": source,
        "source_resource_id": resource_id,
        "canonical_identity": f"{source}:{resource_id}",
        "title": resource_id,
        "summary": None,
        "downloads_total": None,
        "demand_download_count": None,
        "demand_metric_source": None,
        "demand_percentile": None,
        "freshness_age_days": None,
        "freshness_cohort": "unknown",
        "hangar_recent_downloads": None,
        "hangar_recent_views": None,
        "star_count": None,
        "watcher_count": None,
        "voxel_review_count": None,
        "voxel_review_stars": None,
        "paid_state": "unknown",
        "currency": None,
        "price_amount": None,
        "voxel_price_band": None,
        "analysis_as_of": "2026-09-22T17:13:34Z",
        "feature_schema_version": "yee-29-feature-layer-v0.1",
        "source_metrics_json": None,
    }
    values.update(overrides)
    return values


def _fixture_db(path: Path, *, leak_review: bool = False) -> Path:
    taxonomy = [
        {
            "category_id": "c1", "category_order": 0, "category_name": "Category 1", "definition": "First function.",
            "subcategories": [{"subcategory_id": "s1", "definition": "First subcategory."}, {"subcategory_id": "s2", "definition": "Second subcategory."}],
            "source_native_anchors": [], "known_ambiguity_risk_notes": [], "taxonomy_version": "fixture-taxonomy-v1",
        },
        {
            "category_id": "c2", "category_order": 1, "category_name": "Category 2", "definition": "Second function.",
            "subcategories": [{"subcategory_id": "s3", "definition": "Third subcategory."}],
            "source_native_anchors": [], "known_ambiguity_risk_notes": [], "taxonomy_version": "fixture-taxonomy-v1",
        },
        {
            "category_id": "c3", "category_order": 2, "category_name": "Category 3", "definition": "No confirmed members.",
            "subcategories": [{"subcategory_id": "s4", "definition": "Empty subcategory."}],
            "source_native_anchors": [], "known_ambiguity_risk_notes": [], "taxonomy_version": "fixture-taxonomy-v1",
        },
    ]
    features = [
        _feature("hangar", "h1", downloads_total="10", demand_download_count="10", demand_metric_source="hangar.project.stats.downloads", demand_percentile=10.0, freshness_age_days=30, freshness_cohort="<=30d", hangar_recent_downloads=0, hangar_recent_views=4, star_count=0, watcher_count=1),
        _feature("hangar", "h2", downloads_total="10", demand_download_count="10", demand_metric_source="hangar.project.stats.downloads", demand_percentile=20.0, freshness_age_days=31, freshness_cohort="31-90d", hangar_recent_downloads=2, hangar_recent_views=6, star_count=2, watcher_count=3),
        _feature("hangar", "h3", downloads_total="100", demand_download_count="100", demand_metric_source="hangar.project.stats.downloads", demand_percentile=90.0, freshness_age_days=365, freshness_cohort="91-365d", hangar_recent_downloads=8, hangar_recent_views=10, star_count=5, watcher_count=8),
        _feature("hangar", "h4", downloads_total=None, freshness_age_days=None, freshness_cohort="unknown"),
        _feature("hangar", "hr", downloads_total="7"),
        _feature("hangar", "ho", downloads_total="8"),
        _feature("voxel", "v1", downloads_total="5000", demand_download_count="5000", demand_metric_source="voxel.getResourceInfo.downloads", demand_percentile=100.0, freshness_age_days=0, freshness_cohort="<=30d", voxel_review_count=3, voxel_review_stars=4.0, paid_state="paid", currency="USD", price_amount=10.0, voxel_price_band=">5-10"),
        _feature("voxel", "v2", downloads_total="0", demand_download_count="0", demand_metric_source="voxel.getResourceInfo.downloads", demand_percentile=0.0, freshness_age_days=91, freshness_cohort="91-365d", voxel_review_count=0, voxel_review_stars=None, paid_state="free", currency="EUR", price_amount=0.0, voxel_price_band="free"),
        _feature("voxel", "v3", downloads_total="100", demand_download_count="100", demand_metric_source="voxel.getResourceInfo.downloads", demand_percentile=50.0, freshness_age_days=366, freshness_cohort=">365d", voxel_review_count=2, voxel_review_stars=5.0, paid_state="free", currency="USD", price_amount=0.0, voxel_price_band="free"),
        _feature("voxel", "vr", downloads_total="11"),
        _feature("voxel", "vo", downloads_total="12"),
    ]
    statuses = {
        ("hangar", "h1"): "PLUGIN_PRODUCT_CONFIRMED",
        ("hangar", "h2"): "PLUGIN_PRODUCT_CONFIRMED",
        ("hangar", "h3"): "PLUGIN_PRODUCT_CONFIRMED",
        ("hangar", "h4"): "PLUGIN_PRODUCT_CONFIRMED",
        ("hangar", "hr"): "PLUGIN_PRODUCT_REVIEW",
        ("hangar", "ho"): "OUT_OF_SCOPE_PRODUCT_FORM",
        ("voxel", "v1"): "PLUGIN_PRODUCT_CONFIRMED",
        ("voxel", "v2"): "PLUGIN_PRODUCT_CONFIRMED",
        ("voxel", "v3"): "PLUGIN_PRODUCT_CONFIRMED",
        ("voxel", "vr"): "PLUGIN_PRODUCT_REVIEW",
        ("voxel", "vo"): "OUT_OF_SCOPE_PRODUCT_FORM",
    }
    memberships = {
        ("hangar", "h1"): ("c1", "s1", ["c2"]),
        ("hangar", "h2"): ("c1", "s1", []),
        ("hangar", "h3"): ("c2", "s3", []),
        ("hangar", "h4"): ("c1", "s2", []),
        ("voxel", "v1"): ("c1", "s1", []),
        ("voxel", "v2"): ("c2", "s3", []),
        ("voxel", "v3"): ("c1", "s1", []),
    }
    if leak_review:
        memberships[("hangar", "hr")] = ("c1", "s1", [])

    with sqlite3.connect(path) as db:
        db.executescript(
            """
            CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE plugin_product_scope (
                source TEXT, source_resource_id TEXT, canonical_identity TEXT,
                product_scope_status TEXT, source_feature_json TEXT,
                PRIMARY KEY(source,source_resource_id)
            );
            CREATE TABLE plugin_category_taxonomy (
                category_id TEXT PRIMARY KEY, category_order INTEGER, category_name TEXT,
                definition TEXT, subcategories_json TEXT, source_native_anchors_json TEXT,
                known_ambiguity_risk_notes_json TEXT, taxonomy_version TEXT
            );
            CREATE TABLE plugin_category_memberships (
                source TEXT, source_resource_id TEXT, canonical_identity TEXT, primary_category_id TEXT,
                subcategory_id TEXT, secondary_category_ids_json TEXT, assignment_method TEXT,
                assignment_confidence TEXT, assignment_evidence_json TEXT, assignment_risk_notes_json TEXT,
                source_native_category_facets_json TEXT, source_native_loader_facets_json TEXT, taxonomy_version TEXT,
                PRIMARY KEY(source,source_resource_id)
            );
            CREATE TABLE plugin_category_inventory (
                category_id TEXT PRIMARY KEY, member_count_by_source_json TEXT, primary_member_count INTEGER
            );
            """
        )
        metadata = {
            "schema_version": "fixture-schema-v1",
            "taxonomy_version": "fixture-taxonomy-v1",
            "taxonomy_sha256": "fixture-hash-v1",
            "scope_classifier_version": "fixture-scope-v1",
        }
        db.executemany("INSERT INTO metadata VALUES (?,?)", sorted(metadata.items()))
        for category in taxonomy:
            db.execute(
                "INSERT INTO plugin_category_taxonomy VALUES (?,?,?,?,?,?,?,?)",
                (
                    category["category_id"], category["category_order"], category["category_name"], category["definition"],
                    canonical_json(category["subcategories"]), canonical_json(category["source_native_anchors"]),
                    canonical_json(category["known_ambiguity_risk_notes"]), category["taxonomy_version"],
                ),
            )
        for feature in features:
            key = (feature["source"], feature["source_resource_id"])
            db.execute(
                "INSERT INTO plugin_product_scope VALUES (?,?,?,?,?)",
                (*key, feature["canonical_identity"], statuses[key], canonical_json(feature)),
            )
        primary_counts = Counter()
        source_counts = Counter()
        for key, (category_id, subcategory_id, secondary) in memberships.items():
            feature = next(item for item in features if (item["source"], item["source_resource_id"]) == key)
            primary_counts[category_id] += 1
            source_counts[(category_id, key[0])] += 1
            db.execute(
                "INSERT INTO plugin_category_memberships VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    *key, feature["canonical_identity"], category_id, subcategory_id, canonical_json(secondary),
                    "FIXTURE", "HIGH", "[]", "[]", "[]", "[]", "fixture-taxonomy-v1",
                ),
            )
        for category in taxonomy:
            category_id = category["category_id"]
            source_member_counts = {source: source_counts[(category_id, source)] for source in ("hangar", "voxel")}
            db.execute(
                "INSERT INTO plugin_category_inventory VALUES (?,?,?)",
                (category_id, canonical_json(source_member_counts), primary_counts[category_id]),
            )
    return path


def _read_jsonl(path: Path):
    with path.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def test_average_rank_percentiles_handle_ties_nulls_and_singletons():
    rows = [
        {"source": "hangar", "primary_category_id": "a", "subcategory_id": "x", "downloads_total": "10"},
        {"source": "hangar", "primary_category_id": "a", "subcategory_id": "x", "downloads_total": "10"},
        {"source": "hangar", "primary_category_id": "a", "subcategory_id": "x", "downloads_total": None},
        {"source": "hangar", "primary_category_id": "b", "subcategory_id": "y", "downloads_total": "0"},
        {"source": "voxel", "primary_category_id": "a", "subcategory_id": "x", "downloads_total": "90000"},
    ]
    add_group_demand_percentiles(rows, ("source", "primary_category_id", "subcategory_id"), "p", "n")
    assert [row["p"] for row in rows] == [50.0, 50.0, None, 100.0, 100.0]
    assert [row["n"] for row in rows] == [2, 2, 2, 1, 1]


@pytest.mark.parametrize(
    ("count", "expected"),
    [(0, "ZERO"), (1, "SINGLETON"), (2, "N_2_4"), (4, "N_2_4"), (5, "N_5_19"), (19, "N_5_19"), (20, "N_20_PLUS")],
)
def test_sample_size_band_boundaries(count, expected):
    assert sample_size_band(count) == expected


@pytest.mark.parametrize(
    ("age", "expected"),
    [(None, "unknown"), (0, "<=30d"), (30, "<=30d"), (31, "31-90d"), (90, "31-90d"), (91, "91-365d"), (365, "91-365d"), (366, ">365d")],
)
def test_freshness_boundaries_keep_null_distinct_from_zero(age, expected):
    assert freshness_bucket(age) == expected


def test_group_percentiles_are_isolated_by_source_and_category():
    rows = [
        {"source": "hangar", "primary_category_id": "c1", "subcategory_id": "s1", "downloads_total": "10"},
        {"source": "hangar", "primary_category_id": "c1", "subcategory_id": "s1", "downloads_total": "20"},
        {"source": "hangar", "primary_category_id": "c2", "subcategory_id": "s2", "downloads_total": "30"},
        {"source": "voxel", "primary_category_id": "c1", "subcategory_id": "s1", "downloads_total": "100"},
        {"source": "voxel", "primary_category_id": "c1", "subcategory_id": "s1", "downloads_total": "200"},
        {"source": "voxel", "primary_category_id": "c2", "subcategory_id": "s2", "downloads_total": "50"},
    ]
    baseline = copy.deepcopy(rows)
    add_group_demand_percentiles(baseline, ("source", "primary_category_id"), "p", "n")
    changed = copy.deepcopy(rows)
    changed[3]["downloads_total"] = "99999999"
    add_group_demand_percentiles(changed, ("source", "primary_category_id"), "p", "n")
    baseline_by_identity = {(row["source"], row["primary_category_id"], row["subcategory_id"]): row["p"] for row in baseline if row["source"] == "hangar"}
    changed_hangar = {(row["source"], row["primary_category_id"], row["subcategory_id"]): row["p"] for row in changed if row["source"] == "hangar"}
    assert changed_hangar == baseline_by_identity
    assert changed[5]["p"] == baseline[5]["p"]
    baseline_facts = _fact_signal_metrics([row for row in baseline if row["source"] == "hangar" and row["primary_category_id"] == "c1"], "hangar")
    changed_facts = _fact_signal_metrics([row for row in changed if row["source"] == "hangar" and row["primary_category_id"] == "c1"], "hangar")
    assert changed_facts == baseline_facts


def test_production_fixture_exports_reconcile_and_replay(tmp_path):
    input_db = _fixture_db(tmp_path / "yee61-fixture.sqlite")
    output_a = tmp_path / "out-a"
    output_b = tmp_path / "out-b"
    input_before = sha256_file(input_db)
    qa_a = build_category_signal_layer(input_db, output_a, code_commit="fixture-commit", enforce_pinned_inputs=False)
    qa_b = build_category_signal_layer(input_db, output_b, code_commit="fixture-commit", enforce_pinned_inputs=False)
    assert input_before == sha256_file(input_db)
    assert qa_a["status"] == qa_b["status"] == "PASS"
    assert all(qa_a["checks"].values()) and all(qa_b["checks"].values())
    assert qa_a["row_counts"] == {
        "source_scope_coverage": 2,
        "category_signal_member_features": 7,
        "category_source_facts": 6,
        "subcategory_source_facts": 8,
        "category_signal_overview": 3,
    }
    assert qa_a["input_sha256_before"] == qa_a["input_sha256_after"]
    assert qa_a["deterministic_replay"]["all_exports_and_sqlite_byte_identical"] is True
    assert {path.name for path in output_a.iterdir()} == {
        "GOAL_ALIGNMENT.md", "CATEGORY_SIGNAL_SCHEMA.md", "SIGNAL_SEMANTICS.md",
        "source_scope_coverage.jsonl", "source_scope_coverage.csv",
        "category_signal_member_features.jsonl", "category_signal_member_features.csv",
        "category_source_facts.jsonl", "category_source_facts.csv",
        "subcategory_source_facts.jsonl", "subcategory_source_facts.csv",
        "category_signal_overview.jsonl", "category_signal_overview.csv",
        "category_signal_layer.sqlite", "QA_RESULT.json", "FINAL_REPORT.md", "DATASET_MANIFEST.json",
    }
    for artifact in output_a.iterdir():
        assert artifact.read_bytes() == (output_b / artifact.name).read_bytes()

    members = _read_jsonl(output_a / "category_signal_member_features.jsonl")
    h1 = next(row for row in members if row["canonical_identity"] == "hangar:h1")
    h2 = next(row for row in members if row["canonical_identity"] == "hangar:h2")
    h4 = next(row for row in members if row["canonical_identity"] == "hangar:h4")
    h3 = next(row for row in members if row["canonical_identity"] == "hangar:h3")
    assert (h1["confirmed_source_demand_percentile"], h2["confirmed_source_demand_percentile"]) == (25.0, 25.0)
    assert h1["source_category_demand_percentile"] == h2["source_category_demand_percentile"] == 50.0
    assert h4["confirmed_source_demand_percentile"] is None
    assert h4["downloads_total"] is None
    assert h3["source_subcategory_demand_group_n"] == 1
    assert h3["source_subcategory_demand_percentile"] == 100.0

    category_rows = _read_jsonl(output_a / "category_source_facts.jsonl")
    c2_hangar = next(row for row in category_rows if row["category_id"] == "c2" and row["source"] == "hangar")
    c1_voxel = next(row for row in category_rows if row["category_id"] == "c1" and row["source"] == "voxel")
    c3_voxel = next(row for row in category_rows if row["category_id"] == "c3" and row["source"] == "voxel")
    assert c2_hangar["observed_confirmed_primary_count"] == 1
    assert c2_hangar["observed_confirmed_secondary_count"] == 1
    assert c2_hangar["observed_confirmed_any_membership_count"] == 2
    assert c1_voxel["price_available_count"] == 2
    assert [item["currency"] for item in c1_voxel["price_quantiles_by_currency"]] == ["USD"]
    assert c1_voxel["price_quantiles_by_currency"][0]["price_p50"] == 5
    assert c1_voxel["voxel_review_count_p50"] == 2.5
    assert c1_voxel["star_count_p50"] is None
    assert c1_voxel["paid_state_counts"] == {"paid": 1, "free": 1, "unknown": 0}
    assert c3_voxel["member_count"] == 0
    assert c3_voxel["paid_state_counts"] == {"paid": 0, "free": 0, "unknown": 0}
    assert c3_voxel["paid_evidence_observed_rate"] is None
    c1_hangar = next(row for row in category_rows if row["category_id"] == "c1" and row["source"] == "hangar")
    assert c1_hangar["freshness_count_le_30d"] == 1
    assert c1_hangar["freshness_count_31_90d"] == 1
    assert c1_hangar["freshness_count_unknown"] == 1
    assert c1_hangar["freshness_count_le_30d"] + c1_hangar["freshness_count_31_90d"] + c1_hangar["freshness_count_91_365d"] + c1_hangar["freshness_count_gt_365d"] + c1_hangar["freshness_count_unknown"] == c1_hangar["member_count"]
    assert c1_hangar["paid_evidence_scope"] == "NOT_AVAILABLE_FOR_SOURCE"
    assert c1_hangar["paid_state_counts"] is None
    assert c1_hangar["price_quantiles_by_currency"] is None

    manifest = json.loads((output_a / "DATASET_MANIFEST.json").read_text(encoding="utf-8"))
    assert len(manifest["artifacts"]) == 16
    assert {entry["path"] for entry in manifest["artifacts"]} == {path.name for path in output_a.iterdir() if path.name != "DATASET_MANIFEST.json"}


def test_nonconfirmed_membership_is_rejected_before_signal_generation(tmp_path):
    input_db = _fixture_db(tmp_path / "leaking.sqlite", leak_review=True)
    with pytest.raises(CategorySignalError, match="exactly equal the confirmed scope identity set"):
        build_category_signal_layer(input_db, tmp_path / "out", enforce_pinned_inputs=False)
