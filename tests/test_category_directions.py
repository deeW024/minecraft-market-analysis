from __future__ import annotations

import json
import shutil
import sqlite3
from collections import Counter
from pathlib import Path

import pytest

from market_analysis.category_directions import (
    INPUT_SIGNAL_SCHEMA_VERSION,
    SOURCES,
    TAXONOMY_SHA256,
    TAXONOMY_VERSION,
    _baseline_directions,
    _capped_surface_tokens,
    _coverage_risk,
    _demand_strength,
    _first_nonempty_sentence,
    _load_input,
    _mine_lexical_directions,
    _normalized_tokens,
    _observed_supply_band,
    _representative_members,
    _source_direction_fact,
    _whitespace_state,
    build_category_direction_discovery,
)
from market_analysis.category_signals import canonical_json, sha256_file


ANALYSIS_AS_OF = "2026-09-22T17:13:34Z"


def _member(source: str, source_id: str, category: str, subcategory: str, title: str, **overrides):
    row = {
        "source": source,
        "source_resource_id": source_id,
        "canonical_identity": f"{source}:{source_id}",
        "title": title,
        "summary": "A concise first sentence. A deliberately ignored second sentence.",
        "source_url": f"https://{source}.example/{source_id}",
        "primary_category_id": category,
        "primary_category_order": 0 if category == "c1" else 1,
        "subcategory_id": subcategory,
        "subcategory_order": 0 if subcategory in {"s1", "s3"} else 1,
        "analysis_as_of": ANALYSIS_AS_OF,
        "signal_schema_version": INPUT_SIGNAL_SCHEMA_VERSION,
        "source_category_demand_percentile": 90.0,
        "downloads_total": 100 if source == "hangar" else 9000,
        "freshness_age_days": 30,
        "freshness_cohort": "<=30d",
        "hangar_recent_downloads": 5 if source == "hangar" else None,
        "hangar_recent_views": 12 if source == "hangar" else None,
        "star_count": 4 if source == "hangar" else None,
        "watcher_count": 7 if source == "hangar" else None,
        "voxel_review_count": 2 if source == "voxel" else None,
        "voxel_review_stars": 4.5 if source == "voxel" else None,
        "paid_state": "paid" if source == "voxel" else None,
        "currency": "USD" if source == "voxel" else None,
        "price_amount": 5.0 if source == "voxel" else None,
        "voxel_price_band": "gt_0_5" if source == "voxel" else None,
    }
    row.update(overrides)
    return row


def _fixture_rows():
    return [
        _member("hangar", "h1", "c1", "s1", "AquaRegionTool"),
        _member("hangar", "h2", "c1", "s1", "AquaRegionTool Plus"),
        _member("hangar", "h3", "c1", "s2", "AquaRegionTool Lite"),
        _member("hangar", "h4", "c1", "s2", "Other Resource One"),
        _member("hangar", "h5", "c1", "s2", "Other Resource Two"),
        _member("hangar", "h6", "c1", "s2", "Pair Only Prime"),
        _member("voxel", "v1", "c1", "s1", "AquaRegionTool"),
        _member("voxel", "v2", "c1", "s2", "Alternative Resource"),
        _member("voxel", "v3", "c1", "s2", "Different Resource"),
        _member("hangar", "h7", "c2", "s3", "Shared Resource Prime"),
        _member("hangar", "h8", "c2", "s3", "Some Separate Tool"),
        _member("hangar", "h9", "c2", "s3", "Pair Only Plus"),
        _member("voxel", "v4", "c2", "s3", "Shared Resource Basic"),
    ]


def _write_input_db(path: Path) -> Path:
    taxonomy = [
        {
            "category_id": "c1", "category_order": 0, "category_name": "Category One",
            "definition": "First frozen category.",
            "subcategories": [
                {"subcategory_id": "s1", "definition": "First subcategory."},
                {"subcategory_id": "s2", "definition": "Second subcategory."},
            ],
        },
        {
            "category_id": "c2", "category_order": 1, "category_name": "Category Two",
            "definition": "Second frozen category.",
            "subcategories": [{"subcategory_id": "s3", "definition": "Third subcategory."}],
        },
        {
            "category_id": "c3", "category_order": 2, "category_name": "Empty Category",
            "definition": "Frozen category with no confirmed members.",
            "subcategories": [{"subcategory_id": "s4", "definition": "Empty baseline."}],
        },
    ]
    members = _fixture_rows()
    category_counts = Counter((row["primary_category_id"], row["source"]) for row in members)
    subcategory_counts = Counter((row["primary_category_id"], row["subcategory_id"], row["source"]) for row in members)
    source_counts = Counter(row["source"] for row in members)
    with sqlite3.connect(path) as db:
        db.executescript(
            """
            CREATE TABLE run_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE frozen_taxonomy_snapshot (category_order INTEGER, category_json TEXT);
            CREATE TABLE source_scope_coverage (source TEXT, record_json TEXT);
            CREATE TABLE category_signal_member_features (source TEXT, source_resource_id TEXT, record_json TEXT);
            CREATE TABLE category_source_facts (category_order INTEGER, source TEXT, record_json TEXT);
            CREATE TABLE subcategory_source_facts (
                category_order INTEGER, subcategory_order INTEGER, source TEXT, record_json TEXT
            );
            """
        )
        db.executemany("INSERT INTO run_metadata VALUES (?,?)", [
            ("work_order", "YEE-73"),
            ("signal_schema_version", INPUT_SIGNAL_SCHEMA_VERSION),
            ("input_taxonomy_version", TAXONOMY_VERSION),
            ("taxonomy_sha256", TAXONOMY_SHA256),
            ("analysis_as_of", ANALYSIS_AS_OF),
        ])
        db.executemany(
            "INSERT INTO frozen_taxonomy_snapshot VALUES (?,?)",
            [(row["category_order"], canonical_json(row)) for row in taxonomy],
        )
        for source in SOURCES:
            coverage_fraction = 0.30 if source == "hangar" else 0.02
            db.execute(
                "INSERT INTO source_scope_coverage VALUES (?,?)",
                (source, canonical_json({
                    "source": source,
                    "stage_b_signal_member_count": source_counts[source],
                    "yee61_confirmed_count": source_counts[source],
                    "yee61_confirmed_fraction": coverage_fraction,
                    "taxonomy_version": TAXONOMY_VERSION,
                    "taxonomy_sha256": TAXONOMY_SHA256,
                })),
            )
        db.executemany(
            "INSERT INTO category_signal_member_features VALUES (?,?,?)",
            [(row["source"], row["source_resource_id"], canonical_json(row)) for row in members],
        )
        for category in taxonomy:
            for source in SOURCES:
                key = (category["category_id"], source)
                db.execute(
                    "INSERT INTO category_source_facts VALUES (?,?,?)",
                    (category["category_order"], source, canonical_json({
                        "category_id": key[0], "source": source, "member_count": category_counts[key],
                    })),
                )
            for subcategory_order, subcategory in enumerate(category["subcategories"]):
                for source in SOURCES:
                    key = (category["category_id"], subcategory["subcategory_id"], source)
                    db.execute(
                        "INSERT INTO subcategory_source_facts VALUES (?,?,?,?)",
                        (category["category_order"], subcategory_order, source, canonical_json({
                            "category_id": key[0], "subcategory_id": key[1], "source": source,
                            "member_count": subcategory_counts[key],
                        })),
                    )
    return path


@pytest.fixture
def loaded_fixture(tmp_path):
    db_path = _write_input_db(tmp_path / "yee73-fixture.sqlite")
    loaded = _load_input(db_path, sha256_file(db_path), enforce_pinned_inputs=False)
    return db_path, loaded


def test_normalization_is_nfkc_casefolded_version_stripped_and_camel_split():
    assert [item["token"] for item in _normalized_tokens("ＦａｓｔTeleport-Paper 1.20.4 to 1.21!")] == [
        "fast", "teleport", "paper",
    ]
    assert [item["token"] for item in _normalized_tokens("MyPlugin_v2.3 setup")] == [
        "my", "plugin", "v2", "setup",
    ]


def test_first_nonempty_summary_sentence_is_selected_deterministically():
    assert _first_nonempty_sentence("  . First sentence! ignored second. ") == "First sentence!"
    assert _first_nonempty_sentence(None) == ""
    assert [row["token"] for row in _capped_surface_tokens("Alpha   beta, gamma", cap=10)] == ["alpha"]


def test_lexical_discovery_is_category_local_gated_and_collapses_exact_member_set_aliases(loaded_fixture):
    _, loaded = loaded_fixture
    directions, memberships, exclusions = _mine_lexical_directions(loaded)
    by_category_phrase = {(row["primary_category_id"], row["canonical_direction_key"]): row for row in directions}

    # The longest recurring alias wins because the aliases have exactly the same member identities.
    assert ("c1", "aqua region tool") in by_category_phrase
    assert ("c1", "region tool") not in by_category_phrase
    assert ("c1", "aqua region") not in by_category_phrase
    assert by_category_phrase[("c1", "aqua region tool")]["observed_member_identity_count"] == 4
    assert exclusions["EXACT_MEMBER_SET_REDUNDANCY"] > 0

    # Two identities across different categories do not pool into a supported phrase.
    assert not any(row["canonical_direction_key"] == "pair only" for row in directions)
    assert ("c2", "shared resource") in by_category_phrase
    assert ("c1", "shared resource") not in by_category_phrase
    assert all(row["primary_category_id"] == "c1" for row in memberships if row["canonical_identity"].startswith(("hangar:h", "voxel:v")) and row["direction_id"] == by_category_phrase[("c1", "aqua region tool")]["direction_id"])


def test_summary_match_evidence_has_exact_surface_span_and_nonnfkc_span_is_null(loaded_fixture):
    _, loaded = loaded_fixture
    altered = dict(loaded)
    altered["members"] = [dict(row) for row in loaded["members"]]
    altered["members_by_identity"] = {
        key: dict(row) for key, row in loaded["members_by_identity"].items()
    }
    # For c1 the canonical phrase is still supported by the unchanged members; a summary-only
    # unique-to-two-members phrase must carry the precise first sentence rather than sentence two.
    for member in altered["members"]:
        if member["source_resource_id"] in {"h4", "h5"}:
            member["summary"] = "Quiet Realm Ledger. Pair Only should not enter the first-sentence phrase."
            altered["members_by_identity"][(member["source"], member["source_resource_id"])] = member
    directions, memberships, _ = _mine_lexical_directions(altered)
    match = next(row for row in memberships if row["normalized_match"] == "quiet realm ledger")
    assert match["match_field"] == "summary_first_sentence"
    assert match["source_text_span"] == "Quiet Realm Ledger"
    assert match["source_text_span"] != "Pair Only"

    nfkc_rows = [
        dict(row, title="Ｑｕｉｅｔ Ｒｅａｌｍ Ｌｅｄｇｅｒ") if row["source_resource_id"] in {"h4", "h5"} else dict(row)
        for row in loaded["members"]
    ]
    nfkc = dict(loaded, members=nfkc_rows, members_by_identity={
        (row["source"], row["source_resource_id"]): row for row in nfkc_rows
    })
    _, nfkc_memberships, _ = _mine_lexical_directions(nfkc)
    normalized = next(row for row in nfkc_memberships if row["normalized_match"] == "quiet realm ledger")
    assert normalized["source_text_span"] is None
    assert normalized["source_span_start"] is None


def test_source_facts_are_isolated_and_preserve_native_metrics(loaded_fixture):
    _, loaded = loaded_fixture
    directions, _ = _baseline_directions(loaded)
    direction = next(row for row in directions if row["primary_category_id"] == "c1" and row["baseline_subcategory_id"] == "s1")
    members = [row for row in loaded["members"] if row["primary_category_id"] == "c1" and row["subcategory_id"] == "s1"]
    hangar_before = _source_direction_fact(direction, members, loaded, "hangar")
    voxel_before = _source_direction_fact(direction, members, loaded, "voxel")
    changed_members = [dict(row) for row in members]
    for row in changed_members:
        if row["source"] == "voxel":
            row["downloads_total"] = 999999999
            row["source_category_demand_percentile"] = 0
            row["price_amount"] = 777
    hangar_after = _source_direction_fact(direction, changed_members, loaded, "hangar")
    voxel_after = _source_direction_fact(direction, changed_members, loaded, "voxel")
    assert hangar_after == hangar_before
    assert voxel_after["downloads_total_p50"] != voxel_before["downloads_total_p50"]
    assert voxel_after["price_quantiles_by_currency"]["USD"]["p50"] != voxel_before["price_quantiles_by_currency"]["USD"]["p50"]
    assert hangar_before["paid_evidence_scope"] == "NOT_AVAILABLE_FOR_SOURCE"
    assert hangar_before["paid_share_among_observed"] is None


def test_representative_examples_are_bounded_and_use_specified_tie_breaks(loaded_fixture):
    _, loaded = loaded_fixture
    selected_members = [row for row in loaded["members"] if row["source"] == "hangar"]
    percentile_and_age = {
        "h1": (90, 20), "h2": (90, 10), "h3": (90, 10), "h4": (75, 1),
        "h5": (75, 1), "h6": (75, None), "h7": (50, 0), "h8": (None, 0), "h9": (10, 1),
    }
    loaded_for_sampling = dict(loaded, members_by_identity={
        (row["source"], row["source_resource_id"]): dict(
            row,
            source_category_demand_percentile=percentile_and_age[row["source_resource_id"]][0],
            freshness_age_days=percentile_and_age[row["source_resource_id"]][1],
        ) if row["source"] == "hangar" else dict(row)
        for row in loaded["members"]
    })
    memberships = [{
        "direction_id": "sample", "direction_type": "LEXICAL_SUBNICHE", "source": row["source"],
        "source_resource_id": row["source_resource_id"], "canonical_identity": row["canonical_identity"],
        "match_field": "title", "normalized_match": "sample phrase", "source_text_span": None,
        "source_span_start": None, "source_span_end": None,
    } for row in selected_members]
    sampled = _representative_members("sample", memberships, loaded_for_sampling)["hangar"]
    assert len(sampled) == 6
    assert [row["canonical_identity"] for row in sampled] == [
        "hangar:h2", "hangar:h3", "hangar:h1", "hangar:h4", "hangar:h5", "hangar:h6",
    ]


@pytest.mark.parametrize(("fraction", "expected"), [
    (0.0499, "VERY_HIGH"), (0.05, "HIGH"), (0.1999, "HIGH"),
    (0.20, "MATERIAL"), (0.3999, "MATERIAL"), (0.40, "LOWER"),
])
def test_coverage_risk_boundaries(fraction, expected):
    assert _coverage_risk(fraction) == expected


@pytest.mark.parametrize(("count", "denominator", "expected"), [
    (0, 0, ("ZERO", None, "INSUFFICIENT")),
    (0, 10, ("ZERO", 0.0, "AVAILABLE")),
    (5, 25, ("SPARSE", 0.2, "AVAILABLE")),
    (6, 25, ("LIMITED", 0.24, "AVAILABLE")),
    (10, 29, ("LIMITED", 0.344828, "AVAILABLE")),
    (11, 29, ("BROAD", 0.37931, "AVAILABLE")),
])
def test_observed_supply_band_boundaries(count, denominator, expected):
    assert _observed_supply_band(count, denominator) == expected


def test_demand_and_whitespace_state_boundaries():
    assert _demand_strength(1, 100, 1.0) == "INSUFFICIENT"
    assert _demand_strength(2, 65, 0.40) == "HIGH"
    assert _demand_strength(2, 54, 0.33) == "MODERATE"
    assert _demand_strength(2, 44.999, 0.249) == "LOW"
    assert _demand_strength(2, 50, 0.30) == "MIXED"
    assert _whitespace_state(1, 3, 5, "HIGH", "SPARSE") == "INSUFFICIENT_EVIDENCE"
    assert _whitespace_state(3, 3, 10, "HIGH", "SPARSE") == "OBSERVED_STRONG_PATTERN"
    assert _whitespace_state(2, 2, 10, "MODERATE", "LIMITED") == "OBSERVED_SUPPORTED_PATTERN"
    assert _whitespace_state(3, 3, 10, "HIGH", "BROAD") == "DEMAND_WITH_BROAD_SUPPLY"


def test_full_fixture_build_reconciles_exports_and_does_not_change_read_only_input(tmp_path):
    input_db = _write_input_db(tmp_path / "yee73-fixture.sqlite")
    input_sha = sha256_file(input_db)
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    repo_root = Path(__file__).resolve().parents[1]
    shutil.copyfile(repo_root / "GOAL_ALIGNMENT.md", output_dir / "GOAL_ALIGNMENT.md")

    qa = build_category_direction_discovery(
        input_db, output_dir, code_commit="fixture-commit", enforce_pinned_inputs=False,
    )

    assert qa["status"] == "PASS"
    assert qa["failed_checks"] == []
    assert qa["row_counts"]["direction_universe"] >= 3
    summary_rows = [
        json.loads(line)
        for line in (output_dir / "category_direction_summary.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    empty_category = next(row for row in summary_rows if row["category_id"] == "c3")
    assert empty_category["confirmed_member_count_by_source"] == {"hangar": 0, "voxel": 0}
    assert empty_category["baseline_direction_count"] == 1
    assert empty_category["candidate_direction_ids_by_state"]["ADVANCE_TO_STAGE_D"] == []
    assert empty_category["candidate_direction_ids_by_state"]["WATCH_COVERAGE_LIMITED"] == []
    assert qa["row_counts"]["direction_source_facts"] == 2 * qa["row_counts"]["direction_universe"]
    assert qa["input"]["sqlite_sha256_before"] == input_sha == qa["input"]["sqlite_sha256_after"]
    assert qa["sqlite_checks"] == {"integrity_ok": True, "foreign_key_violation_count": 0}
    assert json.loads((output_dir / "QA_RESULT.json").read_text(encoding="utf-8"))["status"] == "PASS"
    assert (output_dir / "DATASET_MANIFEST.json").is_file()
    assert "Stage D" in (output_dir / "FINAL_REPORT.md").read_text(encoding="utf-8")
