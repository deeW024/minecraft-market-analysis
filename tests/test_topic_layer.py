from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from market_analysis.topic_layer import (
    _select_evidence_rows,
    complete_replay_check,
    normalize_tokens,
    run_pipeline,
    title_ngrams,
)


FIXTURE_COUNTS = {"voxel": 2, "modrinth": 10, "hangar": 3}


def _feature(source: str, resource_id: str, title: str, demand: int, **overrides):
    row = {
        "source": source,
        "source_resource_id": resource_id,
        "canonical_identity": f"{source}:{resource_id}",
        "slug": resource_id,
        "title": title,
        "summary": "A useful Minecraft server tool",
        "project_type": "plugin",
        "project_type_norm": "plugin",
        "category_facets_json": '["utility"]',
        "loader_facets_json": '["paper"]',
        "version_facets_json": '["1.21"]',
        "source_url": f"https://example.test/{source}/{resource_id}",
        "demand_percentile": demand,
        "downloads_total": str(demand * 10),
        "demand_metric_source": f"{source}.downloads",
        "freshness_age_days": int(resource_id) if resource_id.isdigit() else 10,
        "age_days": 100,
        "paid_state": "free" if source == "voxel" else "unknown",
        "price_amount": 0 if source == "voxel" else None,
        "currency": "USD" if source == "voxel" else None,
        "voxel_review_count": 1 if source == "voxel" else None,
        "voxel_review_stars": 4.0 if source == "voxel" else None,
        "follow_count": 2 if source == "modrinth" else None,
        "star_count": 3 if source == "hangar" else None,
        "watcher_count": 1 if source == "hangar" else None,
        "hangar_recent_downloads": 4 if source == "hangar" else None,
        "hangar_recent_views": 8 if source == "hangar" else None,
        "analysis_as_of": "2026-09-22T17:13:34Z",
        "feature_schema_version": "yee-29-feature-layer-v0.1",
    }
    row.update(overrides)
    return row


def _fixture_db(path: Path) -> None:
    rows = [
        _feature("voxel", "1", "Skyblock Premium", 20),
        _feature("voxel", "2", "Skyblock Free", 30),
    ]
    rows += [
        _feature("modrinth", str(index), "Skyblock Market", 100 if index >= 7 else index * 5)
        for index in range(10)
    ]
    rows += [
        _feature("hangar", str(index), "Market Tools", 90 + index)
        for index in range(3)
    ]
    columns = list(rows[0])
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE analysis_runs (run_id TEXT, analysis_as_of TEXT, feature_schema_version TEXT)"
    )
    connection.execute(
        "CREATE TABLE resource_features (" + ",".join(f"{column} TEXT" for column in columns) + ", PRIMARY KEY(source,source_resource_id))"
    )
    connection.executemany(
        "INSERT INTO resource_features (" + ",".join(columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")",
        [[row[column] for column in columns] for row in rows],
    )
    connection.execute(
        "INSERT INTO analysis_runs VALUES (?,?,?)",
        ("fixture-run", "2026-09-22T17:13:34Z", "yee-29-feature-layer-v0.1"),
    )
    connection.commit()
    connection.close()


def test_normalization_stopwords_versions_and_deduplicated_ngrams():
    assert normalize_tokens("The Minecraft v1.21 plugin!!!", frozenset({"the", "minecraft", "plugin"})) == []
    assert title_ngrams("The Minecraft SkyBlock SkyBlock 1.21 Plugin") == ["skyblock", "skyblock skyblock"]


def test_ascii_and_typographic_possessives_do_not_create_s_token():
    examples = [
        ("Farmer's Delight", "farmer"),
        ("Farmer’s Delight", "farmer"),
        ("Saro´s Delight", "saro"),
        ("Felix′s Delight", "felix"),
        ("mrqx`s Delight", "mrqx"),
        ("Farmer 's Delight", "farmer"),
    ]
    for title, expected_name in examples:
        assert normalize_tokens(title) == [expected_name, "delight"]
        assert "s" not in title_ngrams(title)
    assert title_ngrams("Farmer's Delight") == ["delight", "farmer", "farmer delight"]
    assert title_ngrams("Farmer’s Delight") == ["delight", "farmer", "farmer delight"]
    assert normalize_tokens("Fiasco's_47 Carrot") == ["fiasco", "carrot"]


def test_representative_examples_are_new_and_use_source_id_tie_break():
    rows = []
    for number in range(1, 21):
        rows.append({
            "canonical_identity": f"modrinth:id-{number:02d}",
            "source_resource_id": f"id-{number:02d}",
            "demand_percentile": 101 - number,
            "freshness_age_days": number - 6 if 6 <= number <= 10 else 100 + number,
        })

    selected = _select_evidence_rows(list(reversed(rows)))
    highest = {key for key, roles in selected.items() if "highest_demand_percentile" in roles}
    freshest = {key for key, roles in selected.items() if "freshest" in roles}
    representatives = {key for key, roles in selected.items() if "representative" in roles}

    assert highest == {f"modrinth:id-{number:02d}" for number in range(1, 6)}
    assert freshest == {f"modrinth:id-{number:02d}" for number in range(6, 11)}
    assert representatives == {f"modrinth:id-{number:02d}" for number in range(11, 16)}
    assert representatives.isdisjoint(highest | freshest)
    assert len(selected) == 15
    assert all(selected[key] == {"representative"} for key in representatives)


def test_fixture_build_gates_candidates_facts_and_retrieval(tmp_path):
    input_db = tmp_path / "analysis.db"
    _fixture_db(input_db)
    before = input_db.read_bytes()
    first = run_pipeline(input_db, tmp_path / "first", "fixture", expected_counts=FIXTURE_COUNTS)
    replay = run_pipeline(input_db, tmp_path / "replay", "fixture", expected_counts=FIXTURE_COUNTS)
    replay_check = complete_replay_check(first, replay)
    assert replay_check["status"] == "PASS"
    assert first["qa"]["status"] == "PASS"
    assert first["qa"]["output"]["corpus_identity_count"] == sum(FIXTURE_COUNTS.values())
    assert first["qa"]["output"]["topic_pairs_after_support_gates"] > 0
    assert input_db.read_bytes() == before

    connection = sqlite3.connect(first["retrieval_db"])
    classes = dict(connection.execute("SELECT candidate_class,COUNT(*) FROM candidate_topics GROUP BY candidate_class"))
    assert classes["overlap"] >= 1
    assert classes["free_demand_only"] >= 1
    eligible = dict(connection.execute("SELECT candidate_class,MAX(research_eligible) FROM candidate_topics GROUP BY candidate_class"))
    assert eligible["overlap"] == 1
    assert eligible["free_demand_only"] == 1
    assert connection.execute("SELECT COUNT(*) FROM evidence_packs").fetchone()[0] >= 1
    assert connection.execute("SELECT MAX(n) FROM (SELECT topic_key,source,COUNT(*) n FROM evidence_examples GROUP BY topic_key,source)").fetchone()[0] <= 15
    assert connection.execute("SELECT value FROM build_metadata WHERE key='index_type'").fetchone()[0] in {"fts5", "deterministic_inverted_index"}
    connection.close()


def test_input_guardrail_fails_closed(tmp_path):
    input_db = tmp_path / "analysis.db"
    _fixture_db(input_db)
    with pytest.raises(ValueError, match="source counts mismatch"):
        run_pipeline(input_db, tmp_path / "bad", "fixture", expected_counts={"voxel": 1, "modrinth": 11, "hangar": 3})

