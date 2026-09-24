from __future__ import annotations

import hashlib
import json
import sqlite3

import pytest

from market_analysis.family_signals import (
    EXPORT_TABLES,
    SOURCE_SIGNAL_FIELDS,
    FamilySignalInputError,
    _csv_cell,
    _output_checks,
    _read_only_connection,
    _semantic_reconciliation,
    _write_payload,
    aggregate_family_source,
    assemble_signal_rows,
)


def _occurrence(
    family_id: str,
    topic_key: str,
    source: str,
    resource_id: str,
    **overrides,
):
    row = {
        "family_id": family_id,
        "topic_key": topic_key,
        "source": source,
        "source_resource_id": resource_id,
        "canonical_identity": f"{source}:{resource_id}",
        "demand_percentile": None,
        "freshness_age_days": None,
        "age_days": None,
        "downloads_total": None,
        "paid_state": "unknown",
        "price_amount": None,
        "currency": None,
        "voxel_review_count": None,
        "voxel_review_stars": None,
        "follow_count": None,
        "star_count": None,
        "watcher_count": None,
        "hangar_recent_downloads": None,
        "hangar_recent_views": None,
    }
    row.update(overrides)
    return row


def _fixture_rows():
    families = [
        {
            "family_id": "family-a",
            "canonical_topic_key": "a",
            "family_status": "MERGED",
            "member_count": 2,
            "member_topic_keys": ["a", "a-alias"],
            "aliases": ["a-alias"],
            "candidate_classes": ["overlap"],
        },
        {
            "family_id": "family-singleton",
            "canonical_topic_key": "singleton",
            "family_status": "REVIEW_SINGLETON",
            "member_count": 1,
            "member_topic_keys": ["singleton"],
            "aliases": [],
            "candidate_classes": ["free_demand_only"],
        },
    ]
    occurrences = [
        _occurrence("family-a", "a", "voxel", "v-free", demand_percentile=0, freshness_age_days=0,
                    age_days=0, downloads_total="0", paid_state="free", voxel_review_count=0,
                    voxel_review_stars=0),
        _occurrence("family-a", "a-alias", "voxel", "v-free", demand_percentile=0, freshness_age_days=0,
                    age_days=0, downloads_total="0", paid_state="free", voxel_review_count=0,
                    voxel_review_stars=0),
        _occurrence("family-a", "a", "voxel", "v-usd", demand_percentile=100, freshness_age_days=40,
                    age_days=10, downloads_total="100", paid_state="paid", price_amount=10,
                    currency="USD", voxel_review_count=5, voxel_review_stars=4.5),
        _occurrence("family-a", "a-alias", "voxel", "v-eur", demand_percentile=90, freshness_age_days=100,
                    age_days=5, downloads_total="300", paid_state="paid", price_amount=8,
                    currency="EUR", voxel_review_count=4, voxel_review_stars=4),
        _occurrence("family-a", "a", "voxel", "v-unknown", demand_percentile=None, freshness_age_days=None,
                    age_days=None, downloads_total=None, paid_state="unknown"),
        _occurrence("family-a", "a", "modrinth", "m-1", demand_percentile=80, freshness_age_days=20,
                    age_days=12, downloads_total="1000", follow_count=100, paid_state="unknown"),
        _occurrence("family-a", "a-alias", "hangar", "h-1", demand_percentile=50, freshness_age_days=400,
                    age_days=100, downloads_total="50", star_count=0, watcher_count=3,
                    hangar_recent_downloads=0, hangar_recent_views=12, paid_state="unknown"),
        # A source-qualified identity may legitimately occur in another family.
        _occurrence("family-singleton", "singleton", "voxel", "v-usd", demand_percentile=100,
                    freshness_age_days=40, age_days=10, downloads_total="100", paid_state="paid",
                    price_amount=10, currency="USD", voxel_review_count=5, voxel_review_stars=4.5),
    ]
    return assemble_signal_rows(families, occurrences, {"family-a": 2, "family-singleton": 1})


def test_alias_overlap_deduplicates_with_topic_provenance_and_keeps_singletons():
    rows = _fixture_rows()
    memberships = rows["family_resource_memberships"]
    free = next(row for row in memberships if row["canonical_identity"] == "voxel:v-free")
    assert free["matched_topic_keys"] == ["a", "a-alias"]
    assert len(memberships) == 7
    assert rows["occurrence_count"] == 8
    assert rows["deduplicated_membership_count"] == 7
    assert any(row["family_status"] == "REVIEW_SINGLETON" for row in rows["family_features"])
    assert sum(row["family_id"] == "family-a" and row["canonical_identity"] == "voxel:v-usd" for row in memberships) == 1
    assert sum(row["family_id"] == "family-singleton" and row["canonical_identity"] == "voxel:v-usd" for row in memberships) == 1


def test_metrics_are_source_local_and_preserve_null_zero_and_concentration():
    rows = _fixture_rows()
    voxel = next(row for row in rows["family_source_signals"] if row["family_id"] == "family-a" and row["source"] == "voxel")
    modrinth = next(row for row in rows["family_source_signals"] if row["family_id"] == "family-a" and row["source"] == "modrinth")
    hangar = next(row for row in rows["family_source_signals"] if row["family_id"] == "family-a" and row["source"] == "hangar")
    assert voxel["resource_count"] == 4
    assert voxel["demand_available_count"] == 3
    assert voxel["demand_percentile_p50"] == 90
    assert voxel["demand_percentile_ge90_count"] == 2
    assert voxel["demand_percentile_ge90_share"] == 0.666667
    assert voxel["freshness_le30_count"] == 1
    assert voxel["freshness_le30_share"] == 0.333333
    assert voxel["freshness_le90_count"] == 2
    assert voxel["freshness_gt365_count"] == 0
    assert voxel["downloads_total_p50"] == 100
    assert voxel["demand_concentration_top1_share"] == 0.75
    assert voxel["demand_concentration_top3_share"] == 1
    assert voxel["demand_concentration_hhi"] == 0.625
    assert modrinth["downloads_total_p50"] == 1000
    assert modrinth["demand_concentration_top1_share"] == 1
    assert hangar["downloads_total_p50"] == 50
    assert voxel["voxel_review_count_p50"] == 4
    assert voxel["voxel_review_stars_p50"] == 4
    assert voxel["free_count"] == 1
    assert voxel["paid_count"] == 2
    assert voxel["unknown_paid_state_count"] == 1
    assert voxel["paid_known_count"] == 3
    assert voxel["paid_share_known"] == 0.666667
    assert modrinth["modrinth_follow_count_p50"] == 100
    assert hangar["hangar_star_count_p50"] == 0
    assert hangar["hangar_recent_downloads_p50"] == 0
    assert modrinth.get("paid_count") is None
    assert _csv_cell(None) == r"\N"
    assert _csv_cell(0) == 0


def test_concentration_is_null_for_zero_or_missing_usable_total():
    zero_rows = [
        _occurrence("f", "t", "voxel", "a", downloads_total="0"),
        _occurrence("f", "t", "voxel", "b", downloads_total=0),
    ]
    metrics = aggregate_family_source("f", "voxel", zero_rows)
    assert metrics["demand_concentration_top1_share"] is None
    assert metrics["demand_concentration_top3_share"] is None
    assert metrics["demand_concentration_hhi"] is None
    assert metrics["downloads_total_p50"] == 0
    missing = aggregate_family_source("f", "voxel", [_occurrence("f", "t", "voxel", "c")])
    assert missing["demand_percentile_p50"] is None
    assert missing["freshness_age_days_p50"] is None
    assert missing["demand_concentration_hhi"] is None


def test_voxel_price_signals_are_paid_and_currency_separated():
    rows = _fixture_rows()
    family_a = [row for row in rows["family_voxel_price_signals"] if row["family_id"] == "family-a"]
    assert [(row["currency"], row["paid_resource_count"], row["price_p50"]) for row in family_a] == [
        ("EUR", 1, 8), ("USD", 1, 10)
    ]
    features = {row["family_id"]: row for row in rows["family_features"]}
    assert features["family-a"]["has_voxel_paid_evidence"] is True
    assert features["family-a"]["cross_market_presence_class"] == "three_source"
    assert features["family-a"]["resource_membership_count_total"] == 6
    assert features["family-a"]["evidence_example_identity_count"] == 2
    assert features["family-a"]["evidence_coverage_share"] == 0.333333
    assert features["family-a"]["voxel_downloads_total_p50"] == 100
    assert features["family-a"]["modrinth_downloads_total_p50"] == 1000
    assert features["family-a"]["voxel_review_count_p50"] == 4


def test_alias_occurrences_must_agree_on_resource_facts():
    families = [{
        "family_id": "f", "canonical_topic_key": "a", "family_status": "MERGED", "member_count": 2,
        "member_topic_keys": ["a", "alias"], "aliases": [], "candidate_classes": [],
    }]
    first = _occurrence("f", "a", "voxel", "id", downloads_total=4)
    second = _occurrence("f", "alias", "voxel", "id", downloads_total=5)
    with pytest.raises(FamilySignalInputError, match="disagree on downloads_total"):
        assemble_signal_rows(families, [first, second], {"f": 1})


def test_serialized_sqlite_and_exports_replay_byte_identically(tmp_path):
    rows = _fixture_rows()
    metadata = {"work_order": "YEE-43", "input_hashes": {"fixture": "fixed"}}
    first, second = tmp_path / "first", tmp_path / "second"
    first_files = _write_payload(first, rows, metadata)
    second_files = _write_payload(second, rows, metadata)
    assert {path.name: path.read_bytes() for path in first_files} == {
        path.name: path.read_bytes() for path in second_files
    }
    checks = _output_checks(first, rows)
    assert checks["sqlite_integrity_ok"] is True
    assert checks["foreign_key_check_ok"] is True
    assert checks["table_counts_match_exports"] is True
    assert checks["family_source_resource_count_reconciles"] is True
    assert checks["family_presence_and_total_membership_reconciles"] is True
    assert hashlib.sha256((first / "family_features.jsonl").read_bytes()).digest() == hashlib.sha256(
        (second / "family_features.jsonl").read_bytes()
    ).digest()
    exported = json.loads((first / "family_features.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert isinstance(exported["source_presence"], list)
    with sqlite3.connect(first / "family_signal_analysis.sqlite") as connection:
        assert connection.execute("SELECT COUNT(*) FROM family_features").fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM family_resource_memberships").fetchone()[0] == 7
        assert connection.execute("SELECT COUNT(*) FROM metadata").fetchone()[0] == 2


def test_canonical_database_reader_is_read_only_and_preserves_hash(tmp_path):
    database = tmp_path / "canonical input.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE sample(value TEXT)")
        connection.execute("INSERT INTO sample VALUES ('accepted')")
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    connection = _read_only_connection(database)
    try:
        assert connection.execute("SELECT value FROM sample").fetchone()[0] == "accepted"
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("INSERT INTO sample VALUES ('changed')")
    finally:
        connection.close()
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


def test_export_tables_cover_required_deliverables():
    assert set(EXPORT_TABLES) == {
        "family_features",
        "family_source_signals",
        "family_resource_memberships",
        "family_voxel_price_signals",
    }
    assert "cross_market_presence_class" in EXPORT_TABLES["family_features"]
    assert "canonical_topic_key" in EXPORT_TABLES["family_resource_memberships"]
    checks = _semantic_reconciliation(_fixture_rows())
    assert checks["family_presence_mismatch_count"] == 0
    assert checks["source_wide_metric_mismatch_count"] == 0
    assert checks["family_signal_metadata_mismatch_count"] == 0
    assert checks["voxel_price_family_mismatch_count"] == 0
    assert checks["voxel_price_duplicate_family_currency_count"] == 0
    assert checks["membership_provenance_mismatch_count"] == 0
    assert checks["forbidden_decision_field_count"] == 0


def test_schema_contract_uses_authoritative_concentration_and_voxel_paid_names():
    concentration = (
        "demand_concentration_top1_share",
        "demand_concentration_top3_share",
        "demand_concentration_hhi",
    )
    voxel_paid = (
        "free_count",
        "paid_count",
        "unknown_paid_state_count",
        "paid_known_count",
        "paid_share_known",
    )
    normalized = EXPORT_TABLES["family_source_signals"]
    wide = EXPORT_TABLES["family_features"]
    assert tuple(name for name in normalized if name.startswith("demand_concentration_")) == concentration
    assert tuple(name for name in SOURCE_SIGNAL_FIELDS["voxel"] if name in voxel_paid) == voxel_paid
    assert set(voxel_paid) <= set(normalized)
    assert not {f"voxel_{name}" for name in voxel_paid} & set(normalized)
    assert tuple(
        name for name in wide
        if name.endswith(("demand_concentration_top1_share", "demand_concentration_top3_share", "demand_concentration_hhi"))
    ) == tuple(f"{source}_{field}" for source in ("voxel", "modrinth", "hangar") for field in concentration)
    assert tuple(name for name in wide if name in {f"voxel_{field}" for field in voxel_paid}) == tuple(
        f"voxel_{field}" for field in voxel_paid
    )
    assert not any("downloads_total_concentration_" in name for name in normalized + wide)
    assert not any(name in normalized for name in (
        "voxel_free_count", "voxel_paid_count", "voxel_unknown_state_count",
        "voxel_paid_known_count", "voxel_paid_share_known",
    ))
