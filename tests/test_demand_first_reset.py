import json
from pathlib import Path
import sqlite3

import pytest

from market_analysis import demand_first_reset as reset


@pytest.mark.parametrize("kind,value,semantics,expected", [
    ("public_purchase_counter", 12, "product_attributable_purchases", True),
    ("public_purchase_counter", 12, "downloads", False),
    ("public_purchase_counter", 0, "product_attributable_purchases", False),
    ("public_purchase_counter", None, "product_attributable_purchases", False),
    ("public_purchase_counter", -1, "product_attributable_purchases", False),
    ("public_purchase_counter", True, "product_attributable_purchases", False),
    ("displayed_price", 20, None, False),
    ("download_count", 90000, None, False),
    ("review_count", 300, None, False),
    ("buyer_purchase_intent", None, None, False),
])
def test_only_attributable_nonzero_purchase_counters_are_direct(kind, value, semantics, expected):
    assert reset.direct_paid({"kind": kind, "value": value, "counter_semantics": semantics}) is expected


@pytest.mark.parametrize("feature,expected", [
    ({"paid_state": "paid", "price_amount": None}, "PAID_PRECEDENT_ONLY"),
    ({"paid_state": "unknown", "price_amount": 3}, "PAID_PRECEDENT_ONLY"),
    ({"paid_state": "unknown", "downloads_total": "100"}, "NON_COMMERCIAL_SIGNAL"),
    ({"paid_state": "unknown", "price_amount": None, "downloads_total": None}, "UNKNOWN"),
    ({"paid_state": "free", "price_amount": 0, "downloads_total": "0"}, "UNKNOWN"),
])
def test_source_native_context_never_invents_paid_demand(feature, expected):
    assert reset.initial_tier(feature) == expected


def paid(identity="voxel:1", vendor="one", source="source_one"):
    return {"observation_id": identity + ":" + source, "canonical_identity": identity,
            "vendor": vendor, "source_id": source, "kind": "public_purchase_counter",
            "value": 4, "counter_semantics": "product_attributable_purchases", "tier": "DIRECT_PAID_SIGNAL"}


def test_marketplace_alias_and_same_vendor_are_not_independent_paid_products():
    assert reset.pool_state([paid(), paid(source="another_marketplace")]) == "PAID_SIGNAL_SINGLE_SOURCE"
    assert reset.pool_state([paid(), paid(identity="voxel:2")]) == "PAID_SIGNAL_SINGLE_SOURCE"


def test_independent_vendors_have_explicit_corroboration_witness():
    rows = [paid(), paid(identity="voxel:2", vendor="two")]
    assert reset.pool_state(rows) == "EVIDENCE_BACKED_PAID_DEMAND"
    assert reset.independence_witness(rows)["observation_ids"] == [r["observation_id"] for r in rows]


def test_price_from_another_vendor_cannot_corroborate_direct_demand():
    rows = [paid(), {"kind": "displayed_price", "value": 99, "vendor": "two", "tier": "PAID_PRECEDENT_ONLY"}]
    assert reset.pool_state(rows) == "PAID_SIGNAL_SINGLE_SOURCE"


def test_explicit_independent_buyer_channel_is_not_a_review_proxy():
    buyer = {**paid(source="buyer_forum"), "kind": "explicit_buyer_payment", "value": None,
             "payment_evidence": "First-person statement of completed purchase", "actor_id": "person",
             "independent_of_vendor": True}
    assert reset.pool_state([paid(), buyer]) == "EVIDENCE_BACKED_PAID_DEMAND"
    buyer.pop("payment_evidence")
    assert reset.pool_state([paid(), buyer]) == "PAID_SIGNAL_SINGLE_SOURCE"


def fixture_stage(tmp_path):
    stage = tmp_path / "stage"
    stage.mkdir()
    confirmed = {"canonical_identity": "voxel:1", "initial_evidence_tier": "PAID_PRECEDENT_ONLY",
                 "upstream_scope_status": "PLUGIN_PRODUCT_CONFIRMED", "membership": {"category": "accepted"}}
    review = {"canonical_identity": "voxel:2", "initial_evidence_tier": "PAID_PRECEDENT_ONLY",
              "upstream_scope_status": "PLUGIN_PRODUCT_REVIEW", "verification_state": "EXCLUDED_UNVERIFIED",
              "verification_source_id": None}
    reset.write_jsonl(stage / "COMMERCIAL_SIGNAL_UNIVERSE.jsonl", [confirmed])
    reset.write_jsonl(stage / "REVIEW_PRODUCT_FORM_VERIFICATION.jsonl", [review])
    reset.write_jsonl(stage / "TAXONOMY_COVERAGE.jsonl", [{"category_id": "gameplay", "subcategories_json": '[{"subcategory_id":"progression"}]'}])
    reset.write_jsonl(stage / "SCOPE_SCAN.jsonl", [confirmed, review, {"canonical_identity":"voxel:3", "upstream_scope_status":"OUT_OF_SCOPE_PRODUCT_FORM"}])
    source = {"source_id": "s", "canonical_identity": "voxel:2", "opened": True,
              "product_form_verification_kind": "PLUGIN_JAR_INSTALLATION",
              "canonical_url": "https://example.org/plugin", "retrieval_date": "2026-10-03",
              "limitations": "No unique-buyer or revenue meaning", "facts": {"public_purchase_counter": 12}}
    product = {"canonical_identity": "voxel:2", "vendor": "vendor", "source_id": "s", "pool_id": "skills",
               "positive_product_form_evidence": "Official installation explicitly places the server plugin jar in plugins/.",
               "identity_match_basis": "Exact resource and author matched", "lane": "UPSTREAM_OR_RECOVERED"}
    capture = {"schema_version": "fixture", "as_of": "2026-10-03", "sources": [source], "products": [product],
               "pools": [{"pool_id": "skills", "buyer_job":"Operate player progression", "category_id":"gameplay", "subcategory_id":"progression", "advancement_decision":None, "selected_for_next_work_order":None}],
               "free_context": [], "coverage_limits": ["fixture"]}
    return stage, capture


@pytest.mark.parametrize("mutation", ["missing_form", "loader_only", "unopened", "wrong_identity", "decision", "taxonomy", "outside_scope_alias", "outside_universe"])
def test_fail_closed_on_invalid_recovery_or_scope(tmp_path, mutation):
    stage, capture = fixture_stage(tmp_path)
    if mutation == "missing_form":
        capture["products"][0]["positive_product_form_evidence"] = None
    elif mutation == "loader_only":
        capture["sources"][0]["product_form_verification_kind"] = "COMPATIBILITY_ONLY"
    elif mutation == "unopened":
        capture["sources"][0]["opened"] = False
    elif mutation == "wrong_identity":
        capture["sources"][0]["canonical_identity"] = "voxel:999"
    elif mutation == "decision":
        capture["pools"][0]["advancement_decision"] = "advance"
    elif mutation == "taxonomy":
        capture["pools"][0]["subcategory_id"] = "invented"
    elif mutation == "outside_scope_alias":
        capture["products"][0]["upstream_aliases"] = ["voxel:3"]
    else:
        capture["products"][0]["canonical_identity"] = "voxel:999"
        capture["sources"][0]["canonical_identity"] = "voxel:999"
    with pytest.raises(ValueError):
        reset.normalize(stage, capture)


def test_recovery_preserves_upstream_and_exports_replay_identically(tmp_path):
    stage, capture = fixture_stage(tmp_path)
    before = {p.name: reset.digest(p) for p in stage.iterdir()}
    path = tmp_path / "capture.json"
    reset.write_json(path, capture)
    for name in ("one", "two"):
        qa = reset.synthesize(stage, path, tmp_path / name)
        assert qa["recovered_rows"] == 1
        assert qa["foreign_key_violations"] == 0
    assert before == {p.name: reset.digest(p) for p in stage.iterdir()}
    first = {p.name: reset.digest(p) for p in (tmp_path / "one").iterdir()}
    assert first == {p.name: reset.digest(p) for p in (tmp_path / "two").iterdir()}
    assert all(b"\r\n" not in p.read_bytes() for p in (tmp_path / "one").iterdir() if p.suffix in {".json", ".jsonl"})
    db = sqlite3.connect(tmp_path / "one" / "demand_first_commercial_signal.sqlite")
    assert db.execute("select count(*) from pool_members").fetchone()[0] == 1
    assert reset.load_jsonl(stage / "REVIEW_PRODUCT_FORM_VERIFICATION.jsonl")[0]["verification_state"] == "EXCLUDED_UNVERIFIED"


def test_free_context_cannot_create_a_pool_without_paid_signal(tmp_path):
    stage, capture = fixture_stage(tmp_path)
    capture["sources"][0]["facts"] = {}
    capture["free_context"] = [{"pool_id":"skills", "source_id":"s"}]
    with pytest.raises(ValueError, match="prior direct commercial signal"):
        reset.normalize(stage, capture)


def test_shipping_capture_keeps_price_only_jobs_and_correlated_buyer_separate():
    capture = json.loads((Path(__file__).parents[1] / "research" / "yee143_capture.json").read_text(encoding="utf-8"))
    assert capture["additional_observations"][0]["tier"] == "PAID_PRECEDENT_ONLY"
    diary = [p for p in capture["buyer_payments"] if p["source_id"] == "buyer_diary"]
    assert len({p["actor_id"] for p in diary}) == 1
    assert all(p["advancement_decision"] is None for p in capture["pools"])
    assert not any(p["canonical_identity"] in {"voxel:1851", "builtbybit:10839"} for p in capture["products"])
