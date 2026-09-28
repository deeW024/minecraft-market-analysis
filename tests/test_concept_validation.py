from __future__ import annotations

import itertools
import json
import sqlite3
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import market_analysis.concept_validation as validation


def _fixture():
    concepts = []
    upstream = {"evidence_ids": {}, "buyer_ids": {}, "competitor_ids": {}, "evidence": {}, "buyers": {}, "competitors": {}}
    validations = []
    queries = []
    for direction_id, (direction_key, _) in validation.DIRECTIONS.items():
        upstream["evidence_ids"][direction_id] = ["seed-a", "seed-b"]
        upstream["buyer_ids"][direction_id] = ["buyer-a"]
        upstream["competitor_ids"][direction_id] = ["competitor-a"]
        upstream["evidence"][direction_id] = {
            "seed-a": {"source_id": "s1", "evidence_type": "BUYER_NEED"},
            "seed-b": {"source_id": "s1", "evidence_type": "COMPETITOR_FEATURES"},
        }
        upstream["buyers"][direction_id] = {"buyer-a": {"source_id": "s1"}}
        upstream["competitors"][direction_id] = {"competitor-a": {"source_ids": ["s1"]}}
        direction_concepts = []
        for index in range(1, 4):
            concept_id = f"{direction_key}-{index}"
            concept = {
                "concept_id": concept_id,
                "direction_id": direction_id,
                "canonical_direction_key": direction_key,
                "concept_label": f"Concept {direction_key} {index}",
                "target_buyer_segment": "Plugin server/network operators",
                "operator_context": "A self-hosted Minecraft server or proxy network.",
                "problem_statement": "A specific observed operator task is not reliably covered.",
                "purchase_trigger_hypothesis": "The operator experiences the recorded task failure.",
                "primary_opportunity_axis": f"Axis {index}",
                "paid_value_exchange_hypothesis": "A narrow server-side plugin capability might be paid, but no direct WTP is assumed.",
                "minimum_paid_scope": [{
                    "scope_item_id": f"scope-{concept_id}", "concept_id": concept_id,
                    "capability_statement": "A bounded plugin capability.", "scope_state": "HYPOTHESIS_TO_VALIDATE",
                    "evidence_ids": ["seed-a"], "buyer_value_link": "Grounded in the accepted operator report.",
                    "incumbent_coverage_state": "PARTIALLY_COVERED", "notes": "Narrow hypothesis only.",
                }],
                "explicit_non_goals": ["No implementation specification", "No ranking"],
                "upstream_seed_evidence_ids": ["seed-a", "seed-b"],
                "upstream_buyer_observation_ids": ["buyer-a"],
                "upstream_competitor_ids": ["competitor-a"],
                "upstream_risk_flags": [], "synthesis_rationale": "Synthetic test fixture.",
                "novelty_disclosure": {"EVIDENCE_BACKED": [], "SYNTHESIZED_FROM_EVIDENCE": [], "HYPOTHESIS_TO_VALIDATE": ["bounded capability"]},
                "query_target_terms": ["conceptspecific"],
            }
            concepts.append(concept)
            direction_concepts.append(concept_id)
            for purpose in validation.PURPOSES:
                queries.append({
                    "query_id": f"q-{concept_id}-{purpose}", "concept_id": concept_id,
                    "direction_id": direction_id, "execution_order": len(queries) + 1,
                    "purpose": purpose, "query_text": f"conceptspecific evidence for {purpose}",
                    "issued_at": "2026-09-28T00:00:00Z", "query_kind": "MANDATORY",
                    "result_note": "Opened canonical source inspected.", "discovered_result_urls": ["https://example.org/source"],
                    "opened_source_ids": ["s1"], "usable_source_found": True,
                })
            dimensions = [{"dimension": name, "state": "WEAK", "basis": "Test-only.", "evidence_ids": [f"fresh-evidence-{concept_id}"], "source_ids": ["s1"], "risk_flags": [], "unknowns": []} for name in validation.DIMENSIONS]
            validations.append({
                "concept_id": concept_id,
                "direct_competitor_ids": ["competitor-a"],
                "substitute_competitor_ids": [], "adjacent_context_ids": [],
                "direct_competitors": [{
                    "competitor_id": "competitor-a", "competitor_name": "Example Plugin",
                    "relation_type": "DIRECT", "plugin_scope_status": "SERVER_SIDE_PLUGIN",
                    "source_ids": ["s1"], "evidence_ids": [f"fresh-evidence-{concept_id}"],
                }],
                "incumbent_coverage_observations": [{
                    "coverage_id": f"coverage-{concept_id}", "concept_id": concept_id,
                    "competitor_id": "competitor-a", "competitor_name": "Example Plugin",
                    "relation_type": "DIRECT", "tested_axis": f"Axis {index}", "coverage_state": "PARTIALLY_COVERED",
                    "source_ids": ["s1"], "evidence_ids": [f"fresh-evidence-{concept_id}"],
                    "retrieved_at": "2026-09-28T00:00:00Z", "notes": "Test only.",
                }],
                "differentiation_observations": [{
                    "differentiation_id": f"diff-{concept_id}", "concept_id": concept_id,
                    "gap_type": "MISSING_CAPABILITY", "gap_statement": "A specific gap remains unproven.",
                    "buyer_evidence_ids": [f"fresh-evidence-{concept_id}"], "competitor_evidence_ids": [f"fresh-evidence-{concept_id}"],
                    "source_ids": ["s1"], "evidence_state": "HYPOTHESIS_ONLY", "counterevidence_ids": [], "notes": "Test only.",
                }],
                "paid_value_exchange_observations": [{
                    "value_id": f"value-{concept_id}", "concept_id": concept_id,
                    "value_statement": "No direct WTP evidence was found.", "evidence_type": "NO_WTP_EVIDENCE",
                    "source_ids": ["s1"], "evidence_ids": [f"fresh-evidence-{concept_id}"],
                    "state": "INSUFFICIENT_EVIDENCE", "limitations": "Test fixture.",
                }],
                "feasibility_observations": [{"feasibility_id": f"feas-{concept_id}", "concept_id": concept_id,
                    "theme": "platform", "observation": "Needs source-specific compatibility validation.",
                    "source_ids": ["s1"], "evidence_ids": [f"fresh-evidence-{concept_id}"], "notes": "Test only."}],
                "support_observations": [{"support_id": f"support-{concept_id}", "concept_id": concept_id,
                    "burden_theme": "maintenance", "observation": "Support burden is unknown.",
                    "source_ids": ["s1"], "evidence_ids": [f"fresh-evidence-{concept_id}"], "notes": "Test only."}],
                "dimension_assessments": dimensions, "evidence_gap_codes": ["NO_DIRECT_WTP_EVIDENCE"],
                "risk_flags": ["SUPPORT_BURDEN_RISK"], "unknowns": ["Direct WTP is unknown."],
                "research_coverage_status": "PARTIAL", "research_notes": "Test fixture only.",
            })
    distinctness = []
    for direction_id in validation.DIRECTIONS:
        ids = [c["concept_id"] for c in concepts if c["direction_id"] == direction_id]
        for a, b in itertools.combinations(sorted(ids), 2):
            distinctness.append({"relation_id": f"distinct-{a}-{b}", "direction_id": direction_id,
                "concept_a_id": a, "concept_b_id": b, "distinctness_basis": "Distinct primary operator problem axis.",
                "overlap_notes": "Shared market job does not make the primary axis interchangeable.", "materially_distinct": True})
    sources = [{"source_id": "s1", "url": "https://example.org/source", "title": "Opened fixture source",
        "source_type": "PRIMARY_DOCS", "access_status": "OPENED", "retrieved_at": "2026-09-28T00:00:00Z",
        "published_or_updated_at": None, "date_note": "Fixture."}]
    evidence = [{"evidence_id": f"fresh-evidence-{c['concept_id']}", "concept_id": c["concept_id"], "direction_id": c["direction_id"],
        "source_id": "s1", "query_ids": [next(q["query_id"] for q in queries if q["concept_id"] == c["concept_id"])],
        "evidence_type": "PRODUCT_CAPABILITY", "observation": "The opened fixture source describes a plugin capability.",
        "source_locator": "section 1", "limitations": "Test fixture.", "used_search_snippet": False} for c in concepts]
    capture = {"capture_version": validation.CAPTURE_VERSION, "captured_at": "2026-09-28T00:00:00Z", "concepts": concepts, "queries": queries,
        "sources": sources, "evidence": evidence, "validations": validations,
        "distinctness_relations": distinctness,
        "decision_template": {"decision_status": "UNDECIDED", "advanced_concept_ids": [],
            "requested_refinement_concept_ids": [], "held_concept_ids": [], "dropped_concept_ids": [],
            "build_none": False, "rationale": None, "decided_at": None}}
    upstream["evidence_ids"] = {d: ["seed-a", "seed-b"] for d in validation.DIRECTIONS}
    return capture, upstream


def test_exact_two_directions_three_concepts_each_and_complete_distinctness():
    capture, upstream = _fixture()
    cards = validation._validate_capture(capture, upstream)
    assert len(cards) == 6
    assert {d: sum(c["direction_id"] == d for c in cards) for d in validation.DIRECTIONS} == {d: 3 for d in validation.DIRECTIONS}
    assert len(capture["distinctness_relations"]) == 6


def test_wrong_cohort_or_concept_count_fails_closed():
    capture, _ = _fixture()
    capture["concepts"].pop()
    with pytest.raises(validation.ConceptValidationError, match="three concepts"):
        validation.validate_concept_cohort(capture["concepts"])


def test_wrong_cohort_hash_and_outside_direction_fail_closed():
    capture, _ = _fixture()
    with pytest.raises(validation.ConceptValidationError, match="cohort mismatch"):
        validation.validate_concept_cohort(capture["concepts"], expected_sha="0" * 64)
    capture["concepts"][0]["direction_id"] = "dir-outside-cohort"
    with pytest.raises(validation.ConceptValidationError, match="cohort mismatch"):
        validation.validate_concept_cohort(capture["concepts"])


def test_sibling_distinctness_claim_cannot_be_false_or_duplicate_primary_axis():
    capture, upstream = _fixture()
    capture["distinctness_relations"][0]["materially_distinct"] = False
    with pytest.raises(validation.ConceptValidationError, match="distinctness failed"):
        validation._validate_capture(capture, upstream)
    capture, upstream = _fixture()
    capture["concepts"][1]["primary_opportunity_axis"] = capture["concepts"][0]["primary_opportunity_axis"]
    with pytest.raises(validation.ConceptValidationError, match="distinct primary opportunity axes"):
        validation._validate_capture(capture, upstream)


def test_missing_or_outside_upstream_seeds_fail_closed():
    capture, upstream = _fixture()
    concept = capture["concepts"][0]
    concept["upstream_seed_evidence_ids"] = ["unaccepted-seed", "seed-b"]
    with pytest.raises(validation.ConceptValidationError, match="evidence seeds"):
        validation.validate_concept_seeds(concept, upstream)


def test_concept_seeds_require_buyer_and_competitor_evidence_types():
    capture, upstream = _fixture()
    concept = capture["concepts"][0]
    upstream["evidence"][concept["direction_id"]]["seed-b"]["evidence_type"] = "BUYER_SEGMENT_CONTEXT"
    with pytest.raises(validation.ConceptValidationError, match="both buyer/problem and incumbent/technical"):
        validation.validate_concept_seeds(concept, upstream)


def test_missing_buyer_or_competitor_seed_fails_closed():
    capture, upstream = _fixture()
    concept = capture["concepts"][0]
    concept["upstream_buyer_observation_ids"] = []
    with pytest.raises(validation.ConceptValidationError, match="buyer/problem"):
        validation.validate_concept_seeds(concept, upstream)
    concept["upstream_buyer_observation_ids"] = ["buyer-a"]
    concept["upstream_competitor_ids"] = []
    with pytest.raises(validation.ConceptValidationError, match="incumbent seed"):
        validation.validate_concept_seeds(concept, upstream)


def test_generic_direction_query_cannot_satisfy_concept_targeting():
    capture, _ = _fixture()
    capture["queries"][0]["query_text"] = "generic Minecraft server plugins"
    with pytest.raises(validation.ConceptValidationError, match="Generic direction-level"):
        validation.validate_query_coverage(capture["concepts"], capture["queries"])


def test_generic_followup_cannot_bypass_concept_targeting():
    capture, _ = _fixture()
    first = capture["queries"][0]
    followup = dict(first, query_id="q-followup", execution_order=999, query_kind="FOLLOWUP", followup_of=first["query_id"])
    followup["query_text"] = "generic Minecraft server plugins"
    capture["queries"].append(followup)
    with pytest.raises(validation.ConceptValidationError, match="Generic direction-level"):
        validation.validate_query_coverage(capture["concepts"], capture["queries"])


def test_every_concept_has_each_mandatory_purpose_exactly_once():
    capture, _ = _fixture()
    capture["queries"].append(dict(capture["queries"][0], query_id="q-duplicate-pair", execution_order=999))
    with pytest.raises(validation.ConceptValidationError, match="mandatory targeted"):
        validation.validate_query_coverage(capture["concepts"], capture["queries"])


def test_no_usable_query_requires_same_concept_same_purpose_followup():
    capture, _ = _fixture()
    capture["queries"][0]["usable_source_found"] = False
    with pytest.raises(validation.ConceptValidationError, match="same-concept/purpose follow-up"):
        validation.validate_query_coverage(capture["concepts"], capture["queries"])
    first = capture["queries"][0]
    followup = dict(first, query_id="q-followup", execution_order=999, query_kind="FOLLOWUP", followup_of=first["query_id"], usable_source_found=True)
    capture["queries"].append(followup)
    validation.validate_query_coverage(capture["concepts"], capture["queries"])


def test_followup_must_keep_same_concept_and_purpose():
    capture, _ = _fixture()
    first = capture["queries"][0]
    followup = dict(first, query_id="q-followup", execution_order=999, query_kind="FOLLOWUP", followup_of=first["query_id"])
    followup["purpose"] = validation.PURPOSES[1]
    capture["queries"].append(followup)
    with pytest.raises(validation.ConceptValidationError, match="preserve concept and purpose"):
        validation.validate_query_coverage(capture["concepts"], capture["queries"])


def test_unopened_sources_and_search_snippets_cannot_be_evidence():
    capture, _ = _fixture()
    evidence = capture["evidence"][:1]
    queries = capture["queries"]
    sources = capture["sources"]
    evidence[0]["used_search_snippet"] = True
    with pytest.raises(validation.ConceptValidationError, match="Search snippets"):
        validation.validate_evidence_sources(sources, evidence, queries)
    evidence[0]["used_search_snippet"] = False
    sources[0]["access_status"] = "UNAVAILABLE"
    with pytest.raises(validation.ConceptValidationError, match="opened public source"):
        validation.validate_evidence_sources(sources, evidence, queries)


def test_duplicate_canonical_urls_fail_closed():
    capture, _ = _fixture()
    sources = [capture["sources"][0], dict(capture["sources"][0], source_id="s2", url="https://EXAMPLE.org/source/?utm_source=x")]
    with pytest.raises(validation.ConceptValidationError, match="Duplicate canonical"):
        validation.validate_evidence_sources(sources, [], capture["queries"])


def test_direct_competitor_must_be_server_or_proxy_plugin():
    capture, upstream = _fixture()
    capture["validations"][0]["direct_competitors"][0]["plugin_scope_status"] = "MOD_OR_SERVICE"
    with pytest.raises(validation.ConceptValidationError, match="confirmed Minecraft server/proxy plugin"):
        validation._validate_capture(capture, upstream)


def test_scope_incumbent_state_uses_scope_enum_not_conflicting_coverage_enum():
    capture, upstream = _fixture()
    capture["concepts"][0]["minimum_paid_scope"][0]["incumbent_coverage_state"] = "CONFLICTING"
    with pytest.raises(validation.ConceptValidationError, match="Invalid minimum paid scope"):
        validation._validate_capture(capture, upstream)


def test_dimension_fields_and_same_concept_evidence_are_required():
    capture, upstream = _fixture()
    del capture["validations"][0]["dimension_assessments"][0]["basis"]
    with pytest.raises(validation.ConceptValidationError, match="Worker Spec fields"):
        validation._validate_capture(capture, upstream)
    capture, upstream = _fixture()
    capture["validations"][0]["dimension_assessments"][0]["evidence_ids"] = ["missing-evidence"]
    with pytest.raises(validation.ConceptValidationError, match="same-concept evidence"):
        validation._validate_capture(capture, upstream)


def test_unsupported_evidence_backed_differentiation_is_downgraded():
    row = {"evidence_state": "EVIDENCE_BACKED", "buyer_evidence_ids": [], "competitor_evidence_ids": []}
    assert validation.effective_differentiation_state(row, [], []) == "HYPOTHESIS_ONLY"


def test_evidence_backed_differentiation_requires_distinct_buyer_and_competitor_sources():
    evidence = [
        {"evidence_id": "buyer", "source_id": "buyer-source", "evidence_type": "USER_FRICTION"},
        {"evidence_id": "competitor", "source_id": "product-source", "evidence_type": "PRODUCT_CAPABILITY"},
    ]
    sources = [
        {"source_id": "buyer-source", "url": "https://example.org/operator", "source_type": "COMMUNITY_DISCUSSION"},
        {"source_id": "product-source", "url": "https://vendor.example/plugin", "source_type": "MARKETPLACE_LISTING"},
    ]
    row = {"evidence_state": "EVIDENCE_BACKED", "buyer_evidence_ids": ["buyer"], "competitor_evidence_ids": ["competitor"], "counterevidence_ids": []}
    assert validation.effective_differentiation_state(row, evidence, sources) == "EVIDENCE_BACKED"
    assert validation.effective_differentiation_state(row, evidence, [sources[0], dict(sources[1], url=sources[0]["url"])]) == "HYPOTHESIS_ONLY"


def test_unresolved_differentiation_counterevidence_prevents_evidence_backed_state():
    evidence = [
        {"evidence_id": "buyer", "source_id": "buyer-source", "evidence_type": "USER_FRICTION"},
        {"evidence_id": "competitor", "source_id": "product-source", "evidence_type": "PRODUCT_CAPABILITY"},
        {"evidence_id": "counter", "source_id": "counter-source", "evidence_type": "PRODUCT_CAPABILITY"},
    ]
    sources = [
        {"source_id": "buyer-source", "url": "https://example.org/operator", "source_type": "COMMUNITY_DISCUSSION"},
        {"source_id": "product-source", "url": "https://vendor.example/plugin", "source_type": "MARKETPLACE_LISTING"},
        {"source_id": "counter-source", "url": "https://vendor.example/docs", "source_type": "PRIMARY_DOCS"},
    ]
    row = {"evidence_state": "EVIDENCE_BACKED", "buyer_evidence_ids": ["buyer"], "competitor_evidence_ids": ["competitor"], "counterevidence_ids": ["counter"]}
    assert validation.effective_differentiation_state(row, evidence, sources) == "HYPOTHESIS_ONLY"


def test_competitor_price_is_paid_precedent_not_direct_wtp():
    capture, upstream = _fixture()
    value = capture["validations"][0]["paid_value_exchange_observations"][0]
    value["evidence_type"] = "DIRECT_WTP_STATEMENT"
    with pytest.raises(validation.ConceptValidationError, match="direct WTP"):
        validation._validate_capture(capture, upstream)


def test_exactly_seven_dimensions_and_hard_rule_state():
    assert validation.derive_concept_state([{"dimension": name, "state": "WEAK"} for name in validation.DIMENSIONS]) == "CONCEPT_EVIDENCE_WEAK"
    with pytest.raises(validation.ConceptValidationError, match="seven"):
        validation.derive_concept_state([{"dimension": name, "state": "WEAK"} for name in validation.DIMENSIONS[:-1]])


def test_concept_state_uses_only_core_dimensions_and_requires_adequate_coverage_for_weak():
    rows = [{"dimension": name, "state": "SUPPORTED"} for name in validation.DIMENSIONS]
    assert validation.derive_concept_state(rows) == "CONCEPT_EVIDENCE_SUPPORTED"
    rows[0]["state"] = "MIXED"
    assert validation.derive_concept_state(rows) == "CONCEPT_EVIDENCE_MIXED"
    rows[0]["state"] = "INSUFFICIENT_EVIDENCE"
    assert validation.derive_concept_state(rows) == "CONCEPT_INSUFFICIENT_EVIDENCE"
    rows[0]["state"] = "WEAK"
    assert validation.derive_concept_state(rows, "PARTIAL") == "CONCEPT_INSUFFICIENT_EVIDENCE"
    assert validation.derive_concept_state(rows, "SUFFICIENT") == "CONCEPT_EVIDENCE_WEAK"


def test_invalid_gap_code_fails_closed():
    capture, upstream = _fixture()
    capture["validations"][0]["evidence_gap_codes"] = ["MADE_UP"]
    with pytest.raises(validation.ConceptValidationError, match="evidence-gap"):
        validation._validate_capture(capture, upstream)


def test_decision_template_must_remain_undecided():
    template = {"decision_status": "UNDECIDED", "advanced_concept_ids": [], "requested_refinement_concept_ids": [],
        "held_concept_ids": [], "dropped_concept_ids": [], "build_none": False, "rationale": None, "decided_at": None}
    validation.validate_decision_template(template)
    template["advanced_concept_ids"] = ["concept-1"]
    with pytest.raises(validation.ConceptValidationError, match="undecided"):
        validation.validate_decision_template(template)


def test_frozen_capture_decision_template_is_validated():
    capture, upstream = _fixture()
    capture["decision_template"]["advanced_concept_ids"] = [capture["concepts"][0]["concept_id"]]
    with pytest.raises(validation.ConceptValidationError, match="decision template must remain undecided"):
        validation._validate_capture(capture, upstream)


def test_forbidden_score_rank_winner_and_recommendation_fields_fail():
    for payload in ({"score": 0.2}, {"rank": 1}, {"winner": "x"}, {"recommended_build": "x"}, {"recommendation": "x"}, {"price_usd": 12}, {"converted_price": 8}):
        with pytest.raises(validation.ConceptValidationError, match="forbidden"):
            validation._reject_forbidden_fields(payload)


def test_read_only_sqlite_connection_cannot_write(tmp_path: Path):
    path = tmp_path / "input.sqlite"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE t (id INTEGER)")
    connection = validation._load_read_only(path)
    with pytest.raises(sqlite3.OperationalError):
        connection.execute("INSERT INTO t VALUES (1)")
    connection.close()


def test_frozen_capture_replay_is_byte_identical(tmp_path: Path, monkeypatch):
    capture, upstream = _fixture()
    capture_path = tmp_path / "capture.json"
    capture_path.write_text(json.dumps(capture, sort_keys=True), encoding="utf-8")
    input_paths = {name: tmp_path / f"{name}.bin" for name in validation.INPUT_HASHES}
    monkeypatch.setattr(validation, "validate_hashes", lambda paths: dict(validation.INPUT_HASHES))
    monkeypatch.setattr(validation, "_read_upstream", lambda path: upstream)
    def git_run(args, **kwargs):
        if args[1] == "rev-parse":
            return SimpleNamespace(stdout="a" * 40 + "\n", returncode=0)
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(subprocess, "run", git_run)
    output = tmp_path / "bundle"
    qa = validation.build_bundle(input_paths, capture_path, output, "a" * 40, tmp_path)
    assert qa["status"] == "PASS"
    assert qa["replay"] == "BYTE_IDENTICAL"
    assert (output / "concept_validation.sqlite").exists()
    query_rows = [json.loads(line) for line in (output / "concept_validation_queries.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [row["execution_order"] for row in query_rows] == sorted(row["execution_order"] for row in query_rows)
    decision = json.loads((output / "SUPERVISOR_CONCEPT_DECISION_TEMPLATE.json").read_text(encoding="utf-8"))
    assert decision["decision_status"] == "UNDECIDED" and not decision["advanced_concept_ids"]
    brief = (output / "SUPERVISOR_CONCEPT_DECISION_BRIEF.md").read_text(encoding="utf-8")
    assert "Incumbent coverage of this thesis" in brief and "Seven validation dimensions" in brief


def test_wrong_pinned_input_hash_fails_closed(tmp_path: Path, monkeypatch):
    path = tmp_path / "wrong.bin"
    path.write_bytes(b"wrong")
    monkeypatch.setattr(validation, "sha256_file", lambda _: "0" * 64)
    with pytest.raises(validation.ConceptValidationError, match="Pinned input hash mismatch"):
        validation.validate_hashes({"yee81_sqlite": path})
