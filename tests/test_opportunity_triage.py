from __future__ import annotations

import hashlib
import sqlite3

import pytest

from market_analysis.opportunity_triage import (
    EXPORT_TABLES,
    FAMILY_SCORE_COLUMNS,
    SCHEMA_VERSION,
    _assign_ranks,
    _consensus_order_key,
    _formula_mismatches,
    _read_only_connection,
    _triage_bucket,
    _voxel_monetization_qa,
    _write_artifacts,
    compute_family_scores,
    compute_source_components,
)


def _signal(family_id="f", source="modrinth", **overrides):
    row = {
        "family_id": family_id,
        "source": source,
        "resource_count": 1,
        "demand_available_count": 1,
        "freshness_available_count": 1,
        "demand_percentile_p75": 60,
        "demand_percentile_p90": 80,
        "demand_percentile_ge90_share": 0.5,
        "demand_percentile_ge95_share": 0.25,
        "freshness_age_days_p50": 10,
        "freshness_le90_share": 0.5,
        "freshness_gt365_share": 0.25,
        "demand_concentration_top1_share": 0.5,
        "demand_concentration_hhi": 0.25,
    }
    row.update(overrides)
    return row


def _feature(family_id="f", sources=("modrinth",), status="MERGED", paid_evidence=False):
    return {
        "family_id": family_id,
        "canonical_topic_key": f"topic-{family_id}",
        "family_status": status,
        "member_count": 1,
        "member_topic_keys": [f"topic-{family_id}"],
        "aliases": [],
        "candidate_classes": ["overlap"],
        "source_presence": list(sources),
        "source_presence_count": len(sources),
        "has_voxel_paid_evidence": paid_evidence,
        "evidence_coverage_share": 0.5,
    }


def _component(family_id, source, scores, **overrides):
    row = {"family_id": family_id, "source": source}
    for profile, score in zip(("balanced", "demand_first", "whitespace_first"), scores):
        row[f"source_score_{profile}"] = score
    row.update(overrides)
    return row


def test_source_formulas_and_midrank_ties_are_source_local():
    rows = [
        _signal("a", "modrinth", resource_count=1, freshness_age_days_p50=10),
        _signal("b", "modrinth", resource_count=1, freshness_age_days_p50=10),
        _signal("c", "modrinth", resource_count=3, freshness_age_days_p50=30),
        _signal("other", "hangar", resource_count=999, freshness_age_days_p50=900),
    ]
    components = {row["family_id"]: row for row in compute_source_components(rows)}
    assert components["a"]["D"] == 63.5
    assert components["a"]["W"] == 75
    assert components["a"]["F"] == 62.5
    assert components["a"]["C"] == 65
    assert components["a"]["source_score_balanced"] == 66.375
    assert components["c"]["W"] == 0
    assert components["other"]["W"] == 50


def test_null_semantics_and_missing_component_weight_renormalization():
    no_demand = _signal(
        demand_available_count=0,
        freshness_age_days_p50=None,
        demand_concentration_hhi=None,
    )
    row = compute_source_components([no_demand])[0]
    assert (row["D"], row["W"], row["F"], row["C"]) == (None, 50, None, None)
    assert row["source_score_balanced"] is None
    assert row["source_score_demand_first"] is None
    assert row["source_score_whitespace_first"] is None

    missing_freshness_and_concentration = _signal(
        demand_percentile_p75=80,
        demand_percentile_p90=100,
        demand_percentile_ge90_share=0.5,
        demand_percentile_ge95_share=0.5,
        freshness_age_days_p50=None,
        demand_concentration_hhi=None,
    )
    row = compute_source_components([missing_freshness_and_concentration])[0]
    assert row["F"] is None and row["C"] is None
    assert row["W"] == 50
    assert row["source_score_balanced"] == 70
    assert row["source_score_demand_first"] == 74.375
    assert row["source_score_whitespace_first"] == 65


def test_family_weights_validation_monetization_and_status_are_descriptive_only():
    features = [_feature("review", ("voxel", "modrinth"), "REVIEW_SINGLETON", True),
                _feature("accepted", ("voxel", "modrinth"), "ACCEPTED", True)]
    components = []
    for family_id in ("review", "accepted"):
        components.extend((
            _component(
                family_id, "voxel", (60, 70, 80), D=60, W=50, F=70, C=65,
                resource_count=3, paid_count=2, paid_known_count=3, paid_share_known=0.666667,
            ),
            _component(family_id, "modrinth", (90, 85, 75), D=90, W=85, F=75, C=80, resource_count=5),
        ))
    scores, shortlist = compute_family_scores(features, components)
    assert len(shortlist) == len(scores)
    by_id = {row["family_id"]: row for row in scores}
    review = by_id["review"]
    accepted = by_id["accepted"]
    assert review["balanced_core_source_score"] == 75.882353
    assert review["balanced_source_weight_coverage"] == 0.85
    assert review["cross_market_validation_score"] == 50
    assert review["voxel_monetization_score"] == 83.33335
    assert review["balanced_final_score"] == accepted["balanced_final_score"]
    assert review["demand_first_final_score"] == accepted["demand_first_final_score"]
    assert review["whitespace_first_final_score"] == accepted["whitespace_first_final_score"]
    assert review["family_status"] == "REVIEW_SINGLETON"
    assert shortlist[0]["voxel_D"] == 60
    assert shortlist[0]["modrinth_source_score_balanced"] == 90
    assert shortlist[0]["hangar_D"] is None


def test_voxel_monetization_null_and_missing_m_weight_renormalization():
    feature = _feature("f", ("modrinth",))
    scores, _ = compute_family_scores(
        [feature], [_component("f", "modrinth", (90, 90, 90))]
    )
    row = scores[0]
    assert row["voxel_monetization_score"] is None
    assert row["balanced_final_score"] == 75.789474

    feature = _feature("f", ("voxel",))
    voxel = _component("f", "voxel", (90, 90, 90), paid_count=0, paid_known_count=0, paid_share_known=0)
    scores, _ = compute_family_scores([feature], [voxel])
    assert scores[0]["voxel_monetization_score"] is None
    assert scores[0]["balanced_final_score"] == 75.789474


def test_voxel_monetization_end_to_end_uses_canonical_signal_fields():
    features = [
        _feature("paid", ("voxel", "modrinth")),
        _feature("free", ("voxel", "modrinth")),
    ]
    signals = [
        _signal("paid", "voxel", paid_count=2, paid_known_count=4, paid_share_known=0.5),
        _signal("paid", "modrinth"),
        _signal("free", "voxel", paid_count=0, paid_known_count=3, paid_share_known=0),
        _signal("free", "modrinth"),
    ]
    components = compute_source_components(signals)
    voxel_components = {row["family_id"]: row for row in components if row["source"] == "voxel"}
    assert {field: voxel_components["paid"][field] for field in ("paid_count", "paid_known_count", "paid_share_known")} == {
        "paid_count": 2, "paid_known_count": 4, "paid_share_known": 0.5,
    }
    assert {field: voxel_components["free"][field] for field in ("paid_count", "paid_known_count", "paid_share_known")} == {
        "paid_count": 0, "paid_known_count": 3, "paid_share_known": 0,
    }

    family_scores, _ = compute_family_scores(features, components)
    scores_by_id = {row["family_id"]: row for row in family_scores}
    assert scores_by_id["paid"]["voxel_monetization_score"] == 75
    assert scores_by_id["free"]["voxel_monetization_score"] == 0
    assert _formula_mismatches(components, family_scores, signals) == 0
    qa = _voxel_monetization_qa(signals, components, family_scores)
    assert qa["matches_canonical_input"] is True

    stripped_components = [dict(row) for row in components]
    for row in stripped_components:
        if row["source"] == "voxel":
            for field in ("paid_count", "paid_known_count", "paid_share_known"):
                row.pop(field)
    stripped_scores, _ = compute_family_scores(features, stripped_components)
    assert _formula_mismatches(stripped_components, stripped_scores, signals) > 0
    assert _voxel_monetization_qa(signals, stripped_components, stripped_scores)["matches_canonical_input"] is False


def test_profile_ranks_consensus_ties_and_bucket_boundaries():
    rows = [
        {"family_id": "a", "balanced_final_score": 10, "demand_first_final_score": 8, "whitespace_first_final_score": 5},
        {"family_id": "b", "balanced_final_score": 10, "demand_first_final_score": 8, "whitespace_first_final_score": 4},
        {"family_id": "c", "balanced_final_score": 9, "demand_first_final_score": 10, "whitespace_first_final_score": 6},
    ]
    _assign_ranks(rows)
    by_id = {row["family_id"]: row for row in rows}
    assert (by_id["a"]["balanced_rank"], by_id["b"]["balanced_rank"]) == (1, 2)
    assert [row["family_id"] for row in sorted(rows, key=lambda row: row["consensus_rank"])] == ["c", "a", "b"]

    tied = [
        {"family_id": "b", "median_profile_rank": 3, "mean_profile_rank": 3.0, "balanced_rank": 2},
        {"family_id": "z", "median_profile_rank": 3, "mean_profile_rank": 3.0, "balanced_rank": 2},
        {"family_id": "a", "median_profile_rank": 3, "mean_profile_rank": 3.0, "balanced_rank": 1},
    ]
    assert [row["family_id"] for row in sorted(tied, key=_consensus_order_key)] == ["a", "b", "z"]
    assert [_triage_bucket(rank) for rank in (1, 100, 101, 300, 301, 1618)] == [
        "ADVANCE_RESEARCH", "ADVANCE_RESEARCH", "WATCH", "WATCH", "DEFER", "DEFER"
    ]
    with pytest.raises(ValueError):
        _triage_bucket(1619)


def test_export_schema_has_required_context_and_excludes_raw_downloads():
    assert set(EXPORT_TABLES) == {
        "family_opportunity_scores", "source_opportunity_components", "opportunity_shortlist"
    }
    assert all(field in FAMILY_SCORE_COLUMNS for field in (
        "family_id", "family_status", "source_presence", "balanced_core_source_score",
        "balanced_final_score", "balanced_rank", "consensus_rank", "consensus_score", "triage_bucket"
    ))
    assert not any("downloads_total" in column for columns in EXPORT_TABLES.values() for column in columns)
    assert {"voxel_D", "modrinth_source_score_balanced", "hangar_resource_count"} <= set(
        EXPORT_TABLES["opportunity_shortlist"]
    )
    assert {"paid_count", "paid_known_count", "paid_share_known"} <= set(
        EXPORT_TABLES["source_opportunity_components"]
    )


def test_sqlite_exports_replay_byte_identically_and_input_reader_is_read_only(tmp_path):
    features = [_feature()]
    components = [_component("f", "modrinth", (80, 70, 60), resource_count=1)]
    scores, shortlist = compute_family_scores(features, components)
    payload = {
        "family_opportunity_scores": scores,
        "source_opportunity_components": components,
        "opportunity_shortlist": shortlist,
    }
    metadata = {"work_order": "YEE-46", "triage_schema_version": SCHEMA_VERSION}
    first, second = tmp_path / "first", tmp_path / "second"
    first_paths = _write_artifacts(first, payload, metadata)
    second_paths = _write_artifacts(second, payload, metadata)
    assert {path.name: path.read_bytes() for path in first_paths} == {
        path.name: path.read_bytes() for path in second_paths
    }
    with sqlite3.connect(first / "opportunity_triage_analysis.sqlite") as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("SELECT COUNT(*) FROM opportunity_shortlist").fetchone()[0] == 1

    input_db = tmp_path / "canonical.sqlite"
    with sqlite3.connect(input_db) as connection:
        connection.execute("CREATE TABLE sample(value TEXT)")
        connection.execute("INSERT INTO sample VALUES ('accepted')")
    before = hashlib.sha256(input_db.read_bytes()).hexdigest()
    connection = _read_only_connection(input_db)
    try:
        assert connection.execute("SELECT value FROM sample").fetchone()[0] == "accepted"
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("INSERT INTO sample VALUES ('changed')")
    finally:
        connection.close()
    assert hashlib.sha256(input_db.read_bytes()).hexdigest() == before
