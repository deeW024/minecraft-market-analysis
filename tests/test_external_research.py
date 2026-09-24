import csv
import hashlib
import sqlite3

import pytest

from market_analysis import external_research as research


def _canonical_families():
    rows = []
    for rank, family_id in enumerate(research.PILOT_FAMILY_IDS, 1):
        row = {column: None for column in research.PACK_BASE_COLUMNS}
        row.update({
            "family_id": family_id,
            "consensus_rank": rank,
            "canonical_topic_key": f"topic {rank}",
            "member_topic_keys": [f"topic {rank}"],
            "aliases": [],
            "candidate_classes": ["overlap"],
            "consensus_score": 1.0,
            "balanced_rank": rank,
            "demand_first_rank": rank,
            "whitespace_first_rank": rank,
            "source_presence": ["modrinth"],
            "triage_bucket": "ADVANCE_RESEARCH",
        })
        rows.append(row)
    return rows


def _capture():
    families = _canonical_families()
    return {
        "queries": [
            {
                "capture_id": f"q-{rank}-{n}",
                "family_id": family["family_id"],
                "query_text": f"distinct query {rank} {n}",
                "issued_at": f"2026-09-24T12:{rank:02d}:{n:02d}Z",
                "purpose": "identity discovery",
                "result_action": "No opened result retained",
            }
            for rank, family in enumerate(families, 1)
            for n in range(1, 4)
        ],
        "opened_pages": [],
        "evidence": [],
        "competitors": [],
        "families": [
            {
                "family_id": family["family_id"],
                "research_status": "UNRESOLVED",
                "research_notes": "Three distinct public searches did not establish a coherent concept.",
            }
            for family in families
        ],
    }


def test_canonicalize_url_removes_tracking_fragment_and_default_port():
    assert research.canonicalize_url(
        "HTTPS://Example.COM:443/path/?utm_source=search&b=2&a=1#section"
    ) == "https://example.com/path?a=1&b=2"


def test_evidence_requires_registered_open_page_and_query_provenance():
    capture = _capture()
    family_id = research.PILOT_FAMILY_IDS[0]
    capture["opened_pages"].append({
        "capture_id": "page-1",
        "family_id": family_id,
        "query_ref": "q-1-1",
        "source_url": "https://example.org/product",
        "source_title": "Product page",
        "source_type": "PRIMARY_PRODUCT",
        "retrieved_at": "2026-09-24T13:00:00Z",
    })
    capture["evidence"].append({
        "capture_id": "ev-1",
        "family_id": family_id,
        "page_ref": "page-1",
        "claim_type": "SEMANTIC_IDENTITY",
        "observation": "The opened page describes a Minecraft resource.",
    })
    normalized = research._normalize_capture(capture, _canonical_families())
    row = normalized["external_evidence"][0]
    assert row["query_id"] == "qry_0001"
    assert row["canonical_url"] == "https://example.org/product"

    capture["evidence"][0]["page_ref"] = "unopened-page"
    with pytest.raises(research.ExternalResearchInputError, match="opened source page"):
        research._normalize_capture(capture, _canonical_families())


def test_evidence_cannot_borrow_another_familys_open_page():
    capture = _capture()
    capture["opened_pages"].append({
        "capture_id": "page-1",
        "family_id": research.PILOT_FAMILY_IDS[0],
        "query_ref": "q-1-1",
        "source_url": "https://example.org/product",
        "source_title": "Product page",
        "source_type": "PRIMARY_PRODUCT",
        "retrieved_at": "2026-09-24T13:00:00Z",
    })
    capture["evidence"].append({
        "capture_id": "ev-1",
        "family_id": research.PILOT_FAMILY_IDS[1],
        "page_ref": "page-1",
        "claim_type": "SEMANTIC_IDENTITY",
        "observation": "A source fact.",
    })
    with pytest.raises(research.ExternalResearchInputError, match="another family's"):
        research._normalize_capture(capture, _canonical_families())


def test_opened_page_budget_counts_distinct_pages_even_without_evidence():
    capture = _capture()
    family_id = research.PILOT_FAMILY_IDS[0]
    for number in range(16):
        capture["opened_pages"].append({
            "capture_id": f"page-{number}",
            "family_id": family_id,
            "query_ref": "q-1-1",
            "source_url": f"https://example.org/product/{number}",
            "source_title": f"Product page {number}",
            "source_type": "PRIMARY_PRODUCT",
            "retrieved_at": "2026-09-24T13:00:00Z",
        })
    with pytest.raises(research.ExternalResearchInputError, match="15 retained opened-page"):
        research._normalize_capture(capture, _canonical_families())


def test_unresolved_family_requires_three_distinct_search_queries():
    capture = _capture()
    for query in capture["queries"]:
        if query["family_id"] == research.PILOT_FAMILY_IDS[0]:
            query["query_text"] = "same query"
    normalized = research._normalize_capture(capture, _canonical_families())
    qa = research._validate_payload(normalized, _canonical_families())
    assert any("lacks three query attempts" in error for error in qa["errors"])


def _resolved_capture_with_required_queries():
    capture = _capture()
    family_id = research.PILOT_FAMILY_IDS[0]
    pack = capture["families"][0]
    pack.update({
        "research_status": "RESOLVED",
        "resolved_concept_name": "Example Minecraft mod",
        "concept_summary": "A documented Minecraft mod.",
        "primary_entity_url": "https://example.org/product",
        "concept_evidence_refs": ["ev-1"],
        "primary_entity_evidence_refs": ["ev-1"],
    })
    capture["queries"][0]["purpose"] = "current lifecycle/identity check"
    capture["queries"][1]["purpose"] = "pain-point discovery"
    capture["opened_pages"].append({
        "capture_id": "page-1",
        "family_id": family_id,
        "query_ref": "q-1-1",
        "source_url": "https://example.org/product",
        "source_title": "Example product",
        "source_type": "PRIMARY_PRODUCT",
        "retrieved_at": "2026-09-24T13:00:00Z",
    })
    capture["evidence"].append({
        "capture_id": "ev-1",
        "family_id": family_id,
        "page_ref": "page-1",
        "claim_type": "SEMANTIC_IDENTITY",
        "observation": "The publisher identifies this page as its Minecraft mod.",
    })
    return capture


@pytest.mark.parametrize("missing_purpose", research.REQUIRED_RESOLVED_QUERY_PURPOSES)
def test_resolved_family_requires_each_exact_lifecycle_and_pain_query_purpose(missing_purpose):
    capture = _resolved_capture_with_required_queries()
    purpose_query = next(row for row in capture["queries"] if row["purpose"] == missing_purpose)
    purpose_query["purpose"] = "general research"
    normalized = research._normalize_capture(capture, _canonical_families())
    qa = research._validate_payload(normalized, _canonical_families())
    assert f"RESOLVED family lacks required query purpose {missing_purpose!r}" in "\n".join(qa["errors"])


def test_resolved_family_passes_exact_lifecycle_and_pain_query_purpose_gate():
    capture = _resolved_capture_with_required_queries()
    normalized = research._normalize_capture(capture, _canonical_families())
    qa = research._validate_payload(normalized, _canonical_families())
    assert qa["status"] == "PASS"
    assert qa["query_purpose_presence_by_rank"]["1"] == {
        "current lifecycle/identity check": True,
        "pain-point discovery": True,
    }


def test_query_count_by_rank_reports_executed_attempts():
    capture = _capture()
    normalized = research._normalize_capture(capture, _canonical_families())
    qa = research._validate_payload(normalized, _canonical_families())
    assert qa["query_count_by_rank"]["2"] == 3


def test_dawn_is_current_rank7_competitor_and_feather_is_transition_evidence():
    capture = _capture()
    family_id = research.PILOT_FAMILY_IDS[6]
    pack = capture["families"][6]
    pack.update({
        "research_status": "RESOLVED",
        "resolved_concept_name": "Lunar Client modded Minecraft client/launcher",
        "concept_summary": "A bundled Minecraft client with integrated mods.",
        "primary_entity_url": "https://www.lunarclient.com/features",
        "concept_evidence_refs": ["ev-lunar-id"],
        "primary_entity_evidence_refs": ["ev-lunar-id"],
    })
    queries = {row["capture_id"]: row for row in capture["queries"] if row["family_id"] == family_id}
    queries["q-7-1"]["purpose"] = "current lifecycle/identity check"
    queries["q-7-2"]["purpose"] = "pain-point discovery"
    capture["opened_pages"].extend([
        {
            "capture_id": "page-lunar",
            "family_id": family_id,
            "query_ref": "q-7-1",
            "source_url": "https://www.lunarclient.com/features",
            "source_title": "Lunar Client Features",
            "source_type": "PRIMARY_PRODUCT",
            "retrieved_at": "2026-09-24T13:00:00Z",
        },
        {
            "capture_id": "page-feather",
            "family_id": family_id,
            "query_ref": "q-7-1",
            "source_url": "https://feathermc.com/",
            "source_title": "Feather is now Dawn",
            "source_type": "PRIMARY_PRODUCT",
            "retrieved_at": "2026-09-24T13:00:00Z",
        },
        {
            "capture_id": "page-dawn",
            "family_id": family_id,
            "query_ref": "q-7-1",
            "source_url": "https://dawn.gg/",
            "source_title": "Dawn Minecraft Client",
            "source_type": "PRIMARY_PRODUCT",
            "retrieved_at": "2026-09-24T13:00:00Z",
        },
    ])
    capture["evidence"].extend([
        {
            "capture_id": "ev-lunar-id",
            "family_id": family_id,
            "page_ref": "page-lunar",
            "claim_type": "SEMANTIC_IDENTITY",
            "observation": "The official page identifies Lunar Client as a Minecraft client with integrated mods.",
        },
        {
            "capture_id": "ev-feather-migration",
            "family_id": family_id,
            "page_ref": "page-feather",
            "claim_type": "SEMANTIC_IDENTITY",
            "observation": "The official Feather page says Feather is now Dawn and directs users to Dawn.",
        },
        {
            "capture_id": "ev-dawn-id",
            "family_id": family_id,
            "page_ref": "page-dawn",
            "claim_type": "SEMANTIC_IDENTITY",
            "observation": "The official site identifies Dawn as a Minecraft client.",
        },
        {
            "capture_id": "ev-dawn-relation",
            "family_id": family_id,
            "page_ref": "page-dawn",
            "claim_type": "COMPETITOR_RELATION",
            "observation": "The official Dawn and Lunar client descriptions support overlap in bundled modded-client use.",
        },
    ])
    capture["competitors"].append({
        "capture_id": "cmp-dawn",
        "family_id": family_id,
        "entity_name": "Dawn",
        "canonical_url": "https://dawn.gg/",
        "relation_type": "DIRECT",
        "product_type": "Minecraft client/launcher",
        "platform_or_ecosystem": "Minecraft Java Edition",
        "evidence_refs": ["ev-lunar-id", "ev-feather-migration", "ev-dawn-id", "ev-dawn-relation"],
    })

    payload = research._normalize_capture(capture, _canonical_families())
    qa = research._validate_payload(payload, _canonical_families())
    rank7_entities = [row for row in payload["competitor_entities"] if row["family_id"] == family_id]
    dawn = next(row for row in rank7_entities if row["entity_name"] == "Dawn")
    cited = [row for row in payload["external_evidence"] if row["evidence_id"] in dawn["evidence_ids"]]

    assert qa["status"] == "PASS"
    assert {row["entity_name"] for row in rank7_entities} == {"Dawn"}
    assert dawn["canonical_url"] == "https://dawn.gg/"
    assert any("Feather is now Dawn" in row["observation"] for row in cited)


def test_pack_copies_every_yee46_baseline_field_without_recalculation():
    canonical = _canonical_families()
    payload = research._normalize_capture(_capture(), canonical)
    for packed, source in zip(payload["family_research_packs"], canonical):
        assert {key: packed[key] for key in research.PACK_BASE_COLUMNS} == {
            key: source.get(key) for key in research.PACK_BASE_COLUMNS
        }
    payload["family_research_packs"][0]["balanced_rank"] = 99
    qa = research._validate_payload(payload, canonical)
    assert any("context/rank mutated" in error for error in qa["errors"])


def test_coverage_thresholds_are_explicit():
    assert research._coverage_status("RESOLVED", 4, 2, True, True) == "SUFFICIENT"
    assert research._coverage_status("RESOLVED", 3, 2, True, True) == "PARTIAL"
    assert research._coverage_status("AMBIGUOUS", 20, 10, True, True) == "AMBIGUOUS"
    assert research._coverage_status("UNRESOLVED", 20, 10, True, True) == "UNRESOLVED"


def test_explicit_source_date_keeps_date_precision():
    assert research._timestamp("2025-11-20", "published_or_updated_at", required=False) == "2025-11-20"
    with pytest.raises(research.ExternalResearchInputError, match="invalid ISO date"):
        research._timestamp("2025-19-20", "published_or_updated_at", required=False)


def test_csv_null_token_and_sqlite_reconcile_and_replay_bytes(tmp_path):
    capture = _capture()
    canonical = _canonical_families()
    payload = research._normalize_capture(capture, canonical)
    metadata = {"work_order": "YEE-47", "baseline_commit": research.BASELINE_COMMIT}
    first = tmp_path / "first"
    replay = tmp_path / "replay"
    files = research._write_dataset_files(first, payload, metadata, capture)
    replay_files = research._write_dataset_files(replay, payload, metadata, capture)
    assert {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in files} == {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in replay_files
    }
    checks = research._output_checks(first, payload)
    assert checks["sqlite_integrity_ok"] and checks["foreign_key_check_ok"]
    assert checks["table_counts_match"] and checks["table_columns_match_schema"]
    with (first / "family_research_packs.csv").open(newline="", encoding="utf-8") as handle:
        row = next(csv.DictReader(handle))
    assert row["resolved_concept_name"] == research.NULL_TOKEN
    connection = sqlite3.connect(first / "external_market_research.sqlite")
    try:
        assert connection.execute("SELECT triage_bucket FROM family_research_packs WHERE consensus_rank=1").fetchone()[0] == "ADVANCE_RESEARCH"
        metadata_read = connection.execute("SELECT COUNT(*) FROM metadata").fetchone()[0]
        assert metadata_read == len(metadata)
    finally:
        connection.close()


def test_complete_yee46_baseline_row_is_preserved_in_pack():
    # Every upstream rank/score/source-native field is in the pack's copied baseline contract.
    assert "triage_bucket" in research.PACK_BASE_COLUMNS
    assert "voxel_monetization_score" in research.PACK_BASE_COLUMNS
    assert "modrinth_D" in research.PACK_BASE_COLUMNS
    assert "hangar_W" in research.PACK_BASE_COLUMNS
