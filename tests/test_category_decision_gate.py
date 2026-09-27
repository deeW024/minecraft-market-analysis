from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from dataclasses import replace
from pathlib import Path

import pytest

from market_analysis import category_decision_gate as gate


@pytest.fixture
def accepted_input(tmp_path: Path) -> tuple[Path, gate.InputPins, dict[str, list[dict]]]:
    path = tmp_path / "synthetic-stage-e.sqlite"
    ids = [f"direction-{index:02d}" for index in range(16)]
    categories = [
        {
            "category_id": category_id,
            "category_order": order,
            "category_name": category_id.replace("_", " ").title(),
            "definition": f"Synthetic {category_id} category",
            "taxonomy_version": "synthetic-v0",
        }
        for order, category_id in enumerate(gate.CATEGORY_ORDER)
    ]
    categories = [{**row, "record": dict(row)} for row in categories]
    options = []
    packs = []
    for index, direction_id in enumerate(ids):
        category_id = gate.CATEGORY_ORDER[index % 9]
        ready = index < 12
        option = {
            "direction_id": direction_id,
            "category_id": category_id,
            "category_opportunity_state": "PROMISING",
            "direction_type": "LEXICAL_SUBNICHE",
            "canonical_direction_key": f"synthetic-{index:02d}",
            "candidate_state": "ADVANCE_TO_STAGE_D",
            "direction_tier": "LEAD_DIRECTION",
            "positive_support_shape": "LEXICAL_ONLY",
            "risk_flags": ["SYNTHETIC_RISK"],
            "reason_codes": ["SYNTHETIC_FIXTURE"],
            "direction_evidence_pack": {"direction_id": direction_id, "source": "synthetic"},
        }
        options.append({"direction_id": direction_id, "category_id": category_id, "record": option})
        pack = {
            **option,
            "research_status": "RESOLVED" if ready else "AMBIGUOUS",
            "research_coverage_status": "SUFFICIENT" if ready else "AMBIGUOUS",
            "resolved_market_job": f"Synthetic job {index}" if ready else None,
            "market_job_summary": f"Synthetic market job summary {index}",
            "research_notes": "Synthetic unresolved interpretation" if not ready else None,
            "maintenance_summary": None,
            "popularity_proxy_summary": None,
            "pricing_observations_by_currency": {},
            "semantic_relation_ids": ["relation-home"] if index in (0, 9) else [],
            "source_ids": [],
            "differentiation_hypotheses": ([{"hypothesis_id": "hypothesis-0", "evidence_ids": ["ev-feature"]}] if index == 0 else []),
            "feature_themes": (["synthetic-feature-theme"] if index == 0 else []),
        }
        packs.append({
            "direction_id": direction_id,
            "category_id": category_id,
            "research_status": pack["research_status"],
            "research_coverage_status": pack["research_coverage_status"],
            "record": pack,
        })

    sources = []
    for source_id, domain, source_type in (
        ("source-market", "market.example", "MARKETPLACE_LISTING"),
        ("source-pain-a", "community-a.example", "COMMUNITY"),
        ("source-pain-b", "community-b.example", "COMMUNITY"),
        ("source-repo", "repo.example", "PRIMARY_REPOSITORY"),
    ):
        record = {"source_id": source_id, "source_domain": domain, "canonical_url": f"https://{domain}/item", "source_type": source_type}
        sources.append({"source_id": source_id, "source_domain": domain, "canonical_url": record["canonical_url"], "record": record})

    evidence_specs = [
        ("ev-price-usd", "source-market", "PRICING", "Fixture Paid", 12.5, "USD"),
        ("ev-price-eur-free", "source-market", "PRICING", "Fixture Free", 0, "EUR"),
        ("ev-pain-a", "source-pain-a", "OPERATOR_PAIN", "Pain A", None, None),
        ("ev-pain-b", "source-pain-b", "OPERATOR_PAIN", "Pain B", None, None),
        ("ev-feature", "source-market", "FEATURE", "Fixture Paid", None, None),
        ("ev-popularity", "source-repo", "POPULARITY_PROXY", "Fixture Repo", 341, None),
        ("ev-relation", "source-market", "OVERLAP_SIGNAL", None, None, None),
    ]
    evidence = []
    for evidence_id, source_id, claim, entity, value, currency in evidence_specs:
        evidence.append({
            "evidence_id": evidence_id,
            "direction_id": "direction-00",
            "category_id": "administration",
            "source_id": source_id,
            "claim_type": claim,
            "record": {
                "evidence_id": evidence_id,
                "direction_id": "direction-00",
                "category_id": "administration",
                "source_id": source_id,
                "claim_type": claim,
                "entity_name": entity,
                "numeric_value": value,
                "numeric_unit": "public stars" if claim == "POPULARITY_PROXY" else None,
                "currency": currency,
                "observation": f"Synthetic {claim} observation {evidence_id}",
                "notes": "Synthetic source-native observation",
            },
        })
    queries = []
    for index, direction_id in enumerate(ids):
        queries.append({"query_id": f"query-{index:02d}", "direction_id": direction_id, "category_id": options[index]["category_id"], "record": {"query_id": f"query-{index:02d}", "direction_id": direction_id, "category_id": options[index]["category_id"], "purpose": "synthetic"}})
    competitors = []
    for competitor_id, relation, evidence_ids, name in (
        ("competitor-direct-paid", "DIRECT", ["ev-price-usd", "ev-feature"], "Fixture Paid"),
        ("competitor-direct-free", "DIRECT", ["ev-price-eur-free"], "Fixture Free"),
        ("competitor-direct-unknown", "DIRECT", [], "Fixture Unknown Price"),
        ("competitor-substitute", "SUBSTITUTE", ["ev-feature"], "Fixture Substitute"),
        ("competitor-adjacent", "ADJACENT", ["ev-relation"], "Fixture Adjacent"),
    ):
        competitors.append({
            "competitor_id": competitor_id,
            "direction_id": "direction-00",
            "category_id": "administration",
            "record": {
                "competitor_id": competitor_id,
                "direction_id": "direction-00",
                "category_id": "administration",
                "relation_type": relation,
                "entity_name": name,
                "evidence_ids": evidence_ids,
                "lifecycle_evidence_ids": [],
                "maintenance_status": "SYNTHETIC_CURRENT",
                "price_amount": None,
                "currency": None,
                "pricing_model": None,
                "product_type": "Minecraft plugin",
            },
        })
    relation_record = {
        "relation_id": "relation-home",
        "direction_id": "direction-00",
        "related_direction_id": "direction-09",
        "category_id": "administration",
        "relation_type": "SUBSTANTIAL_OVERLAP",
        "evidence_ids": ["ev-relation"],
        "rationale": "Synthetic relation remains a relation, not a merge.",
    }
    relations = [{"relation_id": "relation-home", "direction_id": "direction-00", "related_direction_id": "direction-09", "category_id": "administration", "record": relation_record}]
    coverage = []
    option_counts = Counter(row["category_id"] for row in options)
    for order, category_id in enumerate(gate.CATEGORY_ORDER):
        record = {
            "category_id": category_id,
            "category_name": category_id,
            "category_order": order,
            "option_count": option_counts[category_id],
            "category_research_status": "RESEARCHED_OPTIONS" if option_counts[category_id] else "ZERO_OPTIONS",
            "coverage_notes": None,
            "risk_notes": [],
        }
        coverage.append({"category_id": category_id, "option_count": option_counts[category_id], "category_research_status": record["category_research_status"], "record": record})
    for row in packs:
        row["record"]["source_ids"] = sorted({item["source_id"] for item in evidence if item["direction_id"] == row["direction_id"]})
    table_counts = (
        ("frozen_option_snapshot", len(options)),
        ("frozen_category_snapshot", len(categories)),
        ("direction_research_packs", len(packs)),
        ("category_research_coverage", len(coverage)),
        ("source_documents", len(sources)),
        ("research_queries", len(queries)),
        ("external_evidence", len(evidence)),
        ("competitor_entities", len(competitors)),
        ("direction_semantic_relations", len(relations)),
    )
    direction_hash = gate._direction_ids_sha256(ids)
    metadata = {
        "schema_version": gate.YEE77_SCHEMA_VERSION,
        "execution_code_commit": gate.YEE77_REVIEWED_HEAD,
        "input_sha256_before": gate.YEE76_SQLITE_SHA256,
        "input_sha256_after": gate.YEE76_SQLITE_SHA256,
        "input_run_id": gate.YEE76_RUN_ID,
        "input_schema_version": gate.YEE76_SCHEMA_VERSION,
        "input_code_commit": gate.YEE76_CODE_COMMIT,
        "input_merge_commit": gate.YEE76_MERGE_COMMIT,
        "input_taxonomy_sha256": gate.YEE76_TAXONOMY_SHA256,
        "input_taxonomy_version": "yee-61-functional-category-taxonomy-v0.1",
        "input_work_order": "YEE-76",
        "input_read_mode": "read-only",
        "direction_ids_sha256": direction_hash,
    }
    with sqlite3.connect(path) as db:
        db.executescript("""
            CREATE TABLE metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE input_provenance (key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE frozen_option_snapshot (direction_id TEXT PRIMARY KEY,category_id TEXT NOT NULL,record_json TEXT NOT NULL);
            CREATE TABLE frozen_category_snapshot (category_id TEXT PRIMARY KEY,category_order INTEGER NOT NULL,record_json TEXT NOT NULL);
            CREATE TABLE direction_research_packs (direction_id TEXT PRIMARY KEY,category_id TEXT NOT NULL,research_status TEXT,research_coverage_status TEXT,record_json TEXT NOT NULL);
            CREATE TABLE category_research_coverage (category_id TEXT PRIMARY KEY,option_count INTEGER NOT NULL,category_research_status TEXT,record_json TEXT NOT NULL);
            CREATE TABLE source_documents (source_id TEXT PRIMARY KEY,source_domain TEXT,canonical_url TEXT,record_json TEXT NOT NULL);
            CREATE TABLE research_queries (query_id TEXT PRIMARY KEY,direction_id TEXT,category_id TEXT,record_json TEXT NOT NULL);
            CREATE TABLE external_evidence (evidence_id TEXT PRIMARY KEY,direction_id TEXT,category_id TEXT,source_id TEXT,claim_type TEXT,record_json TEXT NOT NULL);
            CREATE TABLE competitor_entities (competitor_id TEXT PRIMARY KEY,direction_id TEXT,category_id TEXT,record_json TEXT NOT NULL);
            CREATE TABLE direction_semantic_relations (relation_id TEXT PRIMARY KEY,direction_id TEXT,related_direction_id TEXT,category_id TEXT,record_json TEXT NOT NULL);
        """)
        db.executemany("INSERT INTO metadata VALUES(?,?)", sorted(metadata.items()))
        db.executemany("INSERT INTO input_provenance VALUES(?,?)", [("input_read_only", "true")])
        db.executemany("INSERT INTO frozen_option_snapshot VALUES(?,?,?)", [(r["direction_id"], r["category_id"], gate.canonical_json(r["record"])) for r in options])
        db.executemany("INSERT INTO frozen_category_snapshot VALUES(?,?,?)", [(r["category_id"], r["category_order"], gate.canonical_json(r["record"])) for r in categories])
        db.executemany("INSERT INTO direction_research_packs VALUES(?,?,?,?,?)", [(r["direction_id"], r["category_id"], r["research_status"], r["research_coverage_status"], gate.canonical_json(r["record"])) for r in packs])
        db.executemany("INSERT INTO category_research_coverage VALUES(?,?,?,?)", [(r["category_id"], r["option_count"], r["category_research_status"], gate.canonical_json(r["record"])) for r in coverage])
        db.executemany("INSERT INTO source_documents VALUES(?,?,?,?)", [(r["source_id"], r["source_domain"], r["canonical_url"], gate.canonical_json(r["record"])) for r in sources])
        db.executemany("INSERT INTO research_queries VALUES(?,?,?,?)", [(r["query_id"], r["direction_id"], r["category_id"], gate.canonical_json(r["record"])) for r in queries])
        db.executemany("INSERT INTO external_evidence VALUES(?,?,?,?,?,?)", [(r["evidence_id"], r["direction_id"], r["category_id"], r["source_id"], r["claim_type"], gate.canonical_json(r["record"])) for r in evidence])
        db.executemany("INSERT INTO competitor_entities VALUES(?,?,?,?)", [(r["competitor_id"], r["direction_id"], r["category_id"], gate.canonical_json(r["record"])) for r in competitors])
        db.executemany("INSERT INTO direction_semantic_relations VALUES(?,?,?,?,?)", [(r["relation_id"], r["direction_id"], r["related_direction_id"], r["category_id"], gate.canonical_json(r["record"])) for r in relations])
    sha = gate._sha256_file(path)
    pins = gate.InputPins(
        sqlite_sha256=sha,
        direction_ids_sha256=direction_hash,
        table_counts=table_counts,
    )
    payload = {"options": options, "categories": categories, "packs": packs, "coverage": coverage, "sources": sources, "queries": queries, "evidence": evidence, "competitors": competitors, "relations": relations}
    return path, pins, payload


def _build(accepted_input, tmp_path: Path):
    input_path, pins, _ = accepted_input
    result = gate.build_bundle(input_path, tmp_path / "bundle", code_commit="a" * 40, pins=pins)
    return result, tmp_path / "bundle"


def test_exact_16_identities_stage_fields_and_readiness_are_preserved(accepted_input, tmp_path):
    result, output = _build(accepted_input, tmp_path)
    cards = gate._read_jsonl(output / "direction_decision_cards.jsonl")
    _, pins, source = accepted_input
    assert len(cards) == pins.option_count == 16
    assert {row["direction_id"] for row in cards} == {row["direction_id"] for row in source["options"]}
    assert Counter(row["decision_readiness_state"] for row in cards) == {"DECISION_READY": 12, "HOLD_AMBIGUOUS": 4}
    assert all(all(row[field] == next(src["record"][field] for src in source["options"] if src["direction_id"] == row["direction_id"]) for field in gate.STAGE_D_FIELDS) for row in cards)
    assert result["qa"]["status"] == "PASS"


@pytest.mark.parametrize(("research", "coverage", "state"), [
    ("RESOLVED", "SUFFICIENT", "DECISION_READY"),
    ("AMBIGUOUS", "AMBIGUOUS", "HOLD_AMBIGUOUS"),
    ("RESOLVED", "PARTIAL", "HOLD_PARTIAL"),
    ("UNRESOLVED", "SUFFICIENT", "HOLD_UNRESOLVED"),
])
def test_readiness_mapping(research, coverage, state):
    assert gate._readiness(research, coverage)[0] == state


def test_unsupported_readiness_pair_fails_closed():
    with pytest.raises(ValueError, match="Unsupported YEE-77 readiness"):
        gate._readiness("RESOLVED", "UNKNOWN")


def test_no_hidden_drop_and_category_context_keeps_zero_option_categories(accepted_input, tmp_path):
    _, output = _build(accepted_input, tmp_path)
    categories = gate._read_jsonl(output / "category_decision_context.jsonl")
    assert len(categories) == 11
    assert [row["category_id"] for row in categories] == list(gate.CATEGORY_ORDER)
    assert categories[-1]["option_ids"] == []
    assert any("Zero accepted options" in note for note in categories[-1]["descriptive_notes"])


def test_competitor_counts_reconcile_and_adjacent_is_not_direct(accepted_input, tmp_path):
    _, output = _build(accepted_input, tmp_path)
    card = next(row for row in gate._read_jsonl(output / "direction_decision_cards.jsonl") if row["direction_id"] == "direction-00")
    assert (card["direct_competitor_count"], card["substitute_competitor_count"], card["adjacent_context_count"]) == (3, 1, 1)
    assert set(card["direct_competitor_ids"]).isdisjoint(card["adjacent_context_ids"])


def test_pricing_only_null_free_and_paid_are_distinct(accepted_input, tmp_path):
    _, output = _build(accepted_input, tmp_path)
    card = next(row for row in gate._read_jsonl(output / "direction_decision_cards.jsonl") if row["direction_id"] == "direction-00")
    assert card["pricing_evidence_count"] == 2
    assert card["explicit_paid_price_point_count"] == 1
    assert card["explicit_free_price_point_count"] == 1
    assert set(card["pricing_observations_by_currency"]) == {"USD", "EUR"}
    assert card["unknown_price_entity_count"] >= 1
    assert card["unknown_price_entity_count"] > card["explicit_free_price_point_count"]
    assert "NO_EXPLICIT_PRICING_EVIDENCE" not in card["evidence_gap_codes"]


def test_competitor_price_without_linked_pricing_evidence_fails_closed(accepted_input, tmp_path):
    input_path, pins, _ = accepted_input
    altered = tmp_path / "unbacked-competitor-price.sqlite"
    altered.write_bytes(input_path.read_bytes())
    with sqlite3.connect(altered) as db:
        raw = db.execute("SELECT record_json FROM competitor_entities WHERE competitor_id='competitor-direct-unknown'").fetchone()[0]
        record = json.loads(raw)
        record["price_amount"] = 19.99
        db.execute("UPDATE competitor_entities SET record_json=? WHERE competitor_id='competitor-direct-unknown'", (gate.canonical_json(record),))
    altered_pins = replace(pins, sqlite_sha256=gate._sha256_file(altered))
    with pytest.raises(gate.InputMismatch, match="lacks linked PRICING evidence"):
        gate.build_bundle(altered, tmp_path / "rejected-bundle", code_commit="a" * 40, pins=altered_pins)


def test_pain_counts_are_by_evidence_source_and_domain(accepted_input, tmp_path):
    _, output = _build(accepted_input, tmp_path)
    card = next(row for row in gate._read_jsonl(output / "direction_decision_cards.jsonl") if row["direction_id"] == "direction-00")
    assert card["operator_pain_evidence_count"] == 2
    assert card["operator_pain_distinct_source_count"] == 2
    assert card["operator_pain_distinct_domain_count"] == 2


def test_popularity_metrics_keep_source_native_units(accepted_input, tmp_path):
    _, output = _build(accepted_input, tmp_path)
    card = next(row for row in gate._read_jsonl(output / "direction_decision_cards.jsonl") if row["direction_id"] == "direction-00")
    metric = card["popularity_proxy_native_metrics"][0]
    assert (metric["numeric_value"], metric["numeric_unit"], metric["source_domain"]) == (341, "public stars", "repo.example")


def test_hypotheses_are_copied_not_synthesized(accepted_input, tmp_path):
    _, output = _build(accepted_input, tmp_path)
    card = next(row for row in gate._read_jsonl(output / "direction_decision_cards.jsonl") if row["direction_id"] == "direction-00")
    assert card["differentiation_hypotheses"] == [{"hypothesis_id": "hypothesis-0", "evidence_ids": ["ev-feature"]}]
    assert card["differentiation_hypothesis_count"] == 1


def test_semantic_relation_is_copied_without_mutation(accepted_input, tmp_path):
    _, output = _build(accepted_input, tmp_path)
    cards = gate._read_jsonl(output / "direction_decision_cards.jsonl")
    source = accepted_input[2]["relations"][0]["record"]
    for direction_id in ("direction-00", "direction-09"):
        card = next(row for row in cards if row["direction_id"] == direction_id)
        assert card["semantic_relations"] == [source]


def test_gap_codes_are_deterministic_descriptive_flags(accepted_input, tmp_path):
    _, output = _build(accepted_input, tmp_path)
    card = next(row for row in gate._read_jsonl(output / "direction_decision_cards.jsonl") if row["direction_id"] == "direction-01")
    assert card["evidence_gap_codes"] == ["NO_EXPLICIT_PRICING_EVIDENCE", "NO_OPERATOR_PAIN_EVIDENCE", "NO_POPULARITY_PROXY", "NO_DIFFERENTIATION_HYPOTHESIS", "NO_DIRECT_COMPETITOR"]


def test_single_domain_and_semantic_overlap_gap_codes_are_explicit():
    codes = gate._gap_codes("RESOLVED", "SUFFICIENT", 1, 1, 1, 1, 1, 1, 1)
    assert codes == ["SINGLE_EVIDENCE_DOMAIN", "SEMANTIC_OVERLAP_PRESENT"]


def test_stable_order_has_no_rank_score_winner_or_concept_fields(accepted_input, tmp_path):
    _, output = _build(accepted_input, tmp_path)
    cards = gate._read_jsonl(output / "direction_decision_cards.jsonl")
    assert cards == sorted(cards, key=lambda row: (row["category_order"], row["direction_id"]))
    assert not gate._forbidden_keys(cards)


def test_decision_worksheet_is_unset_for_all_ready_rows(accepted_input, tmp_path):
    _, output = _build(accepted_input, tmp_path)
    with sqlite3.connect(f"file:{(output / 'decision_gate.sqlite').as_posix()}?mode=ro", uri=True) as db:
        rows = db.execute("SELECT decision,permitted_choices_json FROM supervisor_decision_worksheet").fetchall()
    assert len(rows) == 12
    assert all(decision == "UNSET" and json.loads(choices) == list(gate.WORKSHEET_CHOICES) for decision, choices in rows)


def test_jsonl_csv_and_sqlite_reconcile(accepted_input, tmp_path):
    result, output = _build(accepted_input, tmp_path)
    assert result["qa"]["checks"]["jsonl_csv_and_sqlite_reconcile"]
    assert result["qa"]["checks"]["sqlite_integrity_check_ok"]
    assert result["qa"]["checks"]["sqlite_foreign_key_check_zero"]


def test_deterministic_replay_and_input_immutability(accepted_input, tmp_path):
    input_path, pins, _ = accepted_input
    before = gate._sha256_file(input_path)
    result, _ = _build(accepted_input, tmp_path)
    assert result["qa"]["checks"]["deterministic_complete_replay"]
    assert gate._sha256_file(input_path) == before == pins.sqlite_sha256


@pytest.mark.parametrize(("field", "bad_value", "message"), [
    ("schema_version", "unexpected-schema", "schema version mismatch"),
    ("execution_code_commit", "0" * 40, "execution commit mismatch"),
    ("input_run_id", "unexpected-run", "run_id mismatch"),
])
def test_pinned_schema_execution_and_upstream_run_mismatches_fail_closed(accepted_input, tmp_path, field, bad_value, message):
    input_path, pins, _ = accepted_input
    altered = tmp_path / f"altered-{field}.sqlite"
    altered.write_bytes(input_path.read_bytes())
    with sqlite3.connect(altered) as db:
        db.execute("UPDATE metadata SET value=? WHERE key=?", (bad_value, field))
    actual_pins = replace(pins, sqlite_sha256=gate._sha256_file(altered))
    with pytest.raises(gate.InputMismatch, match=message):
        gate._load_input(altered, actual_pins)


def test_wrong_input_sha_fails_closed(accepted_input, tmp_path):
    input_path, pins, _ = accepted_input
    altered = tmp_path / "wrong-sha.sqlite"
    altered.write_bytes(input_path.read_bytes() + b"\n")
    with pytest.raises(gate.InputMismatch, match="SHA-256 mismatch"):
        gate._load_input(altered, pins)


def test_productive_bundle_records_exact_manifest_hashes(accepted_input, tmp_path):
    _, output = _build(accepted_input, tmp_path)
    manifest = json.loads((output / "DATASET_MANIFEST.json").read_text(encoding="utf-8"))
    listed = {row["file_name"]: row for row in manifest["artifacts"]}
    assert set(listed) == set(gate.REQUIRED_ARTIFACTS) - {"DATASET_MANIFEST.json"}
    assert listed["decision_gate.sqlite"]["sha256"] == hashlib.sha256((output / "decision_gate.sqlite").read_bytes()).hexdigest()
