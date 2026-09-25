from __future__ import annotations

import copy
import hashlib
import sqlite3

import pytest

from market_analysis import opportunity_synthesis as synthesis


def _fixture_inputs():
    resolved = set(synthesis.RESOLVED_RANKS)
    other = [rank for rank in range(1, 101) if rank not in resolved]
    ambiguous = set(other[:48])
    canonical = []
    packs = []
    for rank in range(1, 101):
        family_id = f"family-{rank:03d}"
        row = {
            "family_id": family_id,
            "consensus_rank": rank,
            "triage_bucket": "ADVANCE_RESEARCH",
            "consensus_score": 101 - rank,
            "balanced_rank": rank,
            "aliases": [f"alias-{rank}"],
        }
        canonical.append(row)
        status = "RESOLVED" if rank in resolved else "AMBIGUOUS" if rank in ambiguous else "UNRESOLVED"
        packs.append({
            **row,
            "research_status": status,
            "research_coverage_status": "PARTIAL" if rank == 1 else "COMPLETE",
        })
    evidence = []
    for index, claim in enumerate(("PAIN_POINT", "PRICING", "MAINTENANCE", "POPULARITY_PROXY"), 1):
        evidence.append({
            "evidence_id": f"evidence-{index}", "family_id": "family-003", "consensus_rank": 3,
            "claim_type": claim, "source_type": "PRIMARY_PRODUCT" if index % 2 else "COMMUNITY",
            "source_domain": f"source{index % 2}.example",
        })
    competitors = [{
        "competitor_id": "competitor-1", "family_id": "family-003", "consensus_rank": 3,
        "relation_type": "DIRECT",
    }]
    queries = [{"query_id": "query-1", "family_id": "family-003", "consensus_rank": 3}]
    return canonical, packs, evidence, competitors, queries


def _derived():
    canonical, packs, evidence, competitors, queries = _fixture_inputs()
    rows, columns = synthesis.derive_synthesis(canonical, packs, evidence, competitors, queries)
    return canonical, packs, rows, columns


def test_join_preserves_every_yee46_field_and_yee47_status():
    canonical, packs, rows, columns = _derived()
    assert len(rows) == 100
    assert [row["family_id"] for row in rows] == [row["family_id"] for row in canonical]
    for source, pack, row in zip(canonical, packs, rows):
        assert {key: row[key] for key in source} == source
        assert row["research_status"] == pack["research_status"]
        assert row["research_coverage_status"] == pack["research_coverage_status"]
    assert set(columns) - set(canonical[0]) == set(synthesis.DERIVED_COLUMNS)


def test_identity_rank_drift_and_copied_field_drift_fail_closed():
    canonical, packs, evidence, competitors, queries = _fixture_inputs()
    packs[0]["consensus_rank"] = 2
    with pytest.raises(synthesis.SynthesisInputError, match="rank identity"):
        synthesis.derive_synthesis(canonical, packs, evidence, competitors, queries)
    packs[0]["consensus_rank"] = 1
    packs[0]["consensus_score"] += 1
    with pytest.raises(synthesis.SynthesisInputError, match="field mismatch"):
        synthesis.derive_synthesis(canonical, packs, evidence, competitors, queries)


def test_row_level_evidence_inventory_and_all_gap_flags():
    _, _, rows, _ = _derived()
    absent = rows[0]
    assert absent["evidence_gap_flags"] == list(synthesis.GAP_FLAG_ORDER)
    assert absent["evidence_count_by_claim_type"] == {}
    assert absent["evidence_count_by_source_type"] == {}
    assert absent["distinct_source_domain_count"] == 0
    assert absent["current_competitor_entity_count"] == 0
    assert absent["competitor_count_by_relation_type"] == {}
    assert absent["research_query_count"] == 0
    populated = rows[2]
    assert populated["evidence_count_by_claim_type"] == {
        "MAINTENANCE": 1, "PAIN_POINT": 1, "POPULARITY_PROXY": 1, "PRICING": 1,
    }
    assert populated["evidence_count_by_source_type"] == {"COMMUNITY": 2, "PRIMARY_PRODUCT": 2}
    assert populated["distinct_source_domain_count"] == 2
    assert populated["current_competitor_entity_count"] == 1
    assert populated["competitor_count_by_relation_type"] == {"DIRECT": 1}
    assert populated["research_query_count"] == 1
    assert populated["evidence_gap_flags"] == []


def test_missing_evidence_is_unknown_and_only_creates_research_tasks():
    _, _, rows, _ = _derived()
    row = rows[0]
    assert row["synthesis_state"] == "DEEP_VALIDATE"
    assert row["next_validation_requirements"] == [
        *synthesis.COMMON_REQUIREMENTS,
        "PAIN_POINT_RESEARCH", "PRICING_MONETIZATION_RESEARCH", "COMPETITOR_EXPANSION",
        "DIRECT_COMPETITOR_CHECK", "LIFECYCLE_VALIDATION", "POPULARITY_VALIDATION",
        "SECOND_SOURCE_VALIDATION",
    ]
    assert "market_exists" not in row
    assert "opportunity_score" not in row


def test_only_resolved_families_enter_exact_queue_and_cohort():
    canonical, _, rows, columns = _derived()
    details = synthesis._validate_derived_rows(rows, canonical, columns)
    assert details["state_counts"] == {
        "DEEP_VALIDATE": 41, "NEEDS_DISAMBIGUATION": 48, "STOP_UNRESOLVED": 11,
    }
    assert details["deep_validation_ranks"] == list(synthesis.RESOLVED_RANKS)
    assert details["cohort_a_ranks"] == list(synthesis.COHORT_A_RANKS)
    queue = sorted((row for row in rows if row["synthesis_state"] == "DEEP_VALIDATE"), key=lambda row: row["deep_validation_order"])
    cohort_a = [row for row in queue if row["validation_cohort"] == "COHORT_A"]
    cohort_b = [row for row in queue if row["validation_cohort"] == "COHORT_B"]
    cohort_c = [row for row in queue if row["validation_cohort"] == "COHORT_C"]
    assert len(queue) == 41 and len(cohort_a) == 15
    assert [row["consensus_rank"] for row in cohort_b] == list(synthesis.RESOLVED_RANKS[15:30])
    assert [row["consensus_rank"] for row in cohort_c] == list(synthesis.RESOLVED_RANKS[30:])
    assert [row["consensus_rank"] for row in cohort_a] == list(synthesis.COHORT_A_RANKS)


@pytest.mark.parametrize("field,value", [("consensus_score", -1), ("consensus_rank", 99)])
def test_mutating_upstream_score_or_rank_is_rejected(field, value):
    canonical, _, rows, columns = _derived()
    changed = copy.deepcopy(rows)
    changed[0][field] = value
    with pytest.raises(synthesis.SynthesisInputError, match="preserved YEE-46 field"):
        synthesis._validate_derived_rows(changed, canonical, columns)


def test_research_status_mutation_is_rejected():
    canonical, _, rows, columns = _derived()
    changed = copy.deepcopy(rows)
    changed[0]["research_status"] = "UNRESOLVED"
    with pytest.raises(synthesis.SynthesisInputError, match="state does not map"):
        synthesis._validate_derived_rows(changed, canonical, columns)


def test_input_hash_guard_and_sqlite_reader_are_read_only(tmp_path):
    source = tmp_path / "accepted.jsonl"
    source.write_text('{"x":1}\n', encoding="utf-8")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    paths = {"fixture": source}
    assert synthesis._hash_inputs(paths, {"fixture": digest}) == {"fixture": digest}
    with pytest.raises(synthesis.SynthesisInputError, match="SHA-256 mismatch"):
        synthesis._hash_inputs(paths, {"fixture": "0" * 64})

    database = tmp_path / "canonical.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE fixture (value INTEGER)")
        connection.execute("INSERT INTO fixture VALUES (1)")
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    read_only = synthesis._readonly_connection(database)
    try:
        assert read_only.execute("SELECT value FROM fixture").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError):
            read_only.execute("UPDATE fixture SET value=2")
    finally:
        read_only.close()
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


def test_sqlite_jsonl_csv_schema_and_deterministic_replay(tmp_path):
    canonical, _, rows, columns = _derived()
    queue = sorted((row for row in rows if row["synthesis_state"] == "DEEP_VALIDATE"), key=lambda row: row["deep_validation_order"])
    payload = {
        "opportunity_synthesis": rows,
        "deep_validation_queue": queue,
        "next_validation_cohort": [row for row in queue if row["validation_cohort"] == "COHORT_A"],
    }
    metadata = {"work_order": "YEE-54", "input_sha256": {"fixture": "0" * 64}}
    dirs = [tmp_path / "first", tmp_path / "replay"]
    files = []
    for directory in dirs:
        files.append(synthesis._write_core(directory, payload, columns, sorted(canonical[0]), metadata))
        checks = synthesis._check_outputs(directory, payload, columns)
        assert checks["sqlite_integrity_ok"]
        assert checks["foreign_key_check_ok"]
        assert checks["sqlite_rows_match"]
        assert checks["jsonl_counts_and_rows_match"]
        assert checks["csv_counts_schema_and_rows_match"]
        assert checks["schema_columns_match"]
    assert {path.name: synthesis._sha256(path) for path in files[0]} == {
        path.name: synthesis._sha256(path) for path in files[1]
    }
