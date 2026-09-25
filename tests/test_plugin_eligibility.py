from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from market_analysis import plugin_eligibility as gate


FEATURE_COLUMNS = (
    "source", "source_resource_id", "canonical_identity", "title", "summary",
    "project_type_norm", "loader_facets_json", "category_facets_json", "download_count",
)


def _row(source_id, *, project_type=None, loaders=(), categories=(), title="Fixture title", summary="Fixture summary", downloads=None):
    return {
        "source": "fixture",
        "source_resource_id": source_id,
        "canonical_identity": f"fixture:{source_id}",
        "title": title,
        "summary": summary,
        "project_type_norm": project_type,
        "loader_facets_json": json.dumps(sorted(set(loaders)), separators=(",", ":")),
        "category_facets_json": json.dumps(sorted(set(categories)), separators=(",", ":")),
        "download_count": downloads,
    }


def _write_fixture(tmp_path: Path, rows):
    input_db = tmp_path / "analysis.db"
    input_jsonl = tmp_path / "resource_features.jsonl"
    with sqlite3.connect(input_db) as db:
        db.execute(
            """CREATE TABLE resource_features (
                source TEXT, source_resource_id TEXT, canonical_identity TEXT,
                title TEXT, summary TEXT, project_type_norm TEXT,
                loader_facets_json TEXT, category_facets_json TEXT,
                download_count INTEGER, PRIMARY KEY(source, source_resource_id)
            )"""
        )
        db.executemany(
            "INSERT INTO resource_features VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [tuple(row[column] for column in FEATURE_COLUMNS) for row in rows],
        )
    input_jsonl.write_text(
        "".join(gate.canonical_json(row) + "\n" for row in rows),
        encoding="utf-8",
        newline="\n",
    )
    return input_db, input_jsonl


def test_explicit_plugin_project_type_is_positive_source_native_evidence():
    result = gate.classify_resource(_row("1", project_type="plugin"))

    assert result["plugin_eligibility"] == "PLUGIN_ELIGIBLE"
    assert result["positive_evidence"] == [
        {"field": "project_type_norm", "token": "plugin"}
    ]
    assert result["conflicting_evidence"] == []


@pytest.mark.parametrize("token", sorted(gate.PLUGIN_PLATFORM_TOKENS))
def test_exact_server_proxy_platform_allowlist_is_positive(token):
    result = gate.classify_resource(_row(token, loaders=(token,)))

    assert result["plugin_eligibility"] == "PLUGIN_ELIGIBLE"
    assert result["positive_evidence"] == [
        {"field": "loader_facets_json", "token": token}
    ]


def test_exact_platform_token_in_source_native_category_facet_is_positive():
    result = gate.classify_resource(_row("paper-category", categories=("paper",)))

    assert result["plugin_eligibility"] == "PLUGIN_ELIGIBLE"
    assert result["positive_evidence"] == [
        {"field": "category_facets_json", "token": "paper"}
    ]


@pytest.mark.parametrize("token", sorted(gate.EXPLICIT_NON_PLUGIN_CLASS_TOKENS))
def test_explicit_non_plugin_project_class_is_rejected(token):
    result = gate.classify_resource(_row(token, project_type=token))

    assert result["plugin_eligibility"] == "NON_PLUGIN"
    assert result["positive_evidence"] == []
    assert result["conflicting_evidence"] == []
    assert "EXPLICIT_NON_PLUGIN_PRODUCT_CLASS" in result["eligibility_reason_codes"]


@pytest.mark.parametrize("token", sorted(gate.NON_PLUGIN_PLATFORM_TOKENS))
def test_mod_platform_only_is_non_plugin(token):
    result = gate.classify_resource(_row(token, categories=(token,)))

    assert result["plugin_eligibility"] == "NON_PLUGIN"
    assert result["positive_evidence"] == []
    assert "NON_PLUGIN_PLATFORM_ONLY" in result["eligibility_reason_codes"]


def test_positive_plugin_and_explicit_non_plugin_evidence_is_ambiguous():
    result = gate.classify_resource(_row("conflict", project_type="mod", loaders=("paper",)))

    assert result["plugin_eligibility"] == "AMBIGUOUS"
    assert result["positive_evidence"] == [
        {"field": "loader_facets_json", "token": "paper"}
    ]
    assert result["conflicting_evidence"] == [
        {"field": "project_type_norm", "token": "mod"}
    ]
    assert result["eligibility_reason_codes"] == ["PLUGIN_AND_NON_PLUGIN_EVIDENCE_CONFLICT"]


def test_missing_or_broad_metadata_is_ambiguous_and_text_is_ignored():
    missing = gate.classify_resource(_row(
        "missing", title="Paper plugin", summary="Bukkit server plugin", downloads=999999
    ))
    broad = gate.classify_resource(_row("broad", project_type="minecraft_java_server", categories=("gameplay",)))

    assert missing["plugin_eligibility"] == "AMBIGUOUS"
    assert broad["plugin_eligibility"] == "AMBIGUOUS"
    assert missing["positive_evidence"] == broad["positive_evidence"] == []


def test_full_gate_preserves_universe_and_is_byte_deterministic(tmp_path):
    rows = [
        _row("01", project_type="plugin", downloads=1),
        _row("02", loaders=("paper",), downloads=2),
        _row("03", categories=("velocity",), downloads=3),
        _row("04", project_type="mod", downloads=4),
        _row("05", categories=("datapack",), downloads=5),
        _row("06", categories=("fabric",), downloads=6),
        _row("07", project_type="mod", loaders=("paper",), downloads=7),
        _row("08", categories=("gameplay",), downloads=8),
        _row("09", project_type="minecraft_java_server", downloads=9),
        _row("10", title="Only a title says plugin", downloads=10),
    ]
    input_db, input_jsonl = _write_fixture(tmp_path, rows)
    input_hashes_before = {
        "db": hashlib.sha256(input_db.read_bytes()).hexdigest(),
        "jsonl": hashlib.sha256(input_jsonl.read_bytes()).hexdigest(),
    }
    output_a = tmp_path / "out-a"
    output_b = tmp_path / "out-b"

    qa = gate.build_plugin_universe(input_db, input_jsonl, output_a, enforce_pinned_inputs=False)
    qa_replay = gate.build_plugin_universe(input_db, input_jsonl, output_b, enforce_pinned_inputs=False)

    assert qa["status"] == "PASS"
    assert qa["checks"]["deterministic_replay_byte_identical"] is True
    assert qa["row_counts"] == {
        "input_resource_features": 10,
        "plugin_eligibility": 10,
        "plugin_only_resource_features": 3,
        "taxonomy_audit_values": qa["row_counts"]["taxonomy_audit_values"],
    }
    assert qa["status_counts"] == {
        "PLUGIN_ELIGIBLE": 3, "NON_PLUGIN": 3, "AMBIGUOUS": 4,
    }
    assert qa_replay["status"] == "PASS"
    assert {item.name for item in output_a.iterdir()} == set(gate.OUTPUT_FILES)
    for name in gate.OUTPUT_FILES:
        assert (output_a / name).read_bytes() == (output_b / name).read_bytes()

    eligible = [
        row for row in rows
        if row["source_resource_id"] in {"01", "02", "03"}
    ]
    exported = [
        json.loads(line)
        for line in (output_a / "plugin_only_resource_features.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert exported == eligible
    assert {
        "db": hashlib.sha256(input_db.read_bytes()).hexdigest(),
        "jsonl": hashlib.sha256(input_jsonl.read_bytes()).hexdigest(),
    } == input_hashes_before

    with sqlite3.connect(output_a / "plugin_eligibility.sqlite") as db:
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []
        assert db.execute("SELECT COUNT(*) FROM plugin_eligibility").fetchone()[0] == 10
        assert db.execute("SELECT COUNT(*) FROM plugin_only_resource_features").fetchone()[0] == 3
        assert db.execute(
            "SELECT COUNT(*) FROM plugin_eligibility WHERE plugin_eligibility!='PLUGIN_ELIGIBLE' AND "
            "(source,source_resource_id) IN (SELECT source,source_resource_id FROM plugin_only_resource_features)"
        ).fetchone()[0] == 0

    manifest = json.loads((output_a / "DATASET_MANIFEST.json").read_text(encoding="utf-8"))
    for name, details in manifest["artifacts"].items():
        artifact = output_a / name
        assert artifact.stat().st_size == details["size_bytes"]
        assert hashlib.sha256(artifact.read_bytes()).hexdigest() == details["sha256"]
