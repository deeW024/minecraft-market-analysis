"""YEE-143: read-only, demand-first commercial evidence extraction.

Downloads, prices and reviews never become observed payments. REVIEW recovery
is local to this work order and requires an explicit opened-source assertion.
"""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sqlite3
from urllib.parse import urlparse


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_only(path):
    connection = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8", newline="\n")


def write_jsonl(path, rows):
    Path(path).write_text("".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows), encoding="utf-8", newline="\n")


def commercial_relevance(feature):
    """Nonzero price or native paid flag; no download/review threshold."""
    return feature.get("paid_state") == "paid" or (feature.get("price_amount") is not None and feature["price_amount"] > 0)


def initial_tier(feature):
    if commercial_relevance(feature):
        return "PAID_PRECEDENT_ONLY"
    engagement = ("downloads_total", "voxel_review_count", "star_count", "watcher_count")
    if any(feature.get(key) is not None and float(feature[key]) > 0 for key in engagement):
        return "NON_COMMERCIAL_SIGNAL"
    return "UNKNOWN"


def scan(foundation, signals, features, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    before = {str(path): digest(path) for path in (foundation, signals, features)}
    expected = ["12fa10a45a98df954c220f741a21ae8c8ed537a953415954e71fdab23bdd17ed", "374b5955d11764a9637e63f9611416ac4d6bc157c40f1582bebfb0eeaa1e11a3", "f0712951d416b52260f524a3218abd8a54a087904d6b2553631f461f010f182e"]
    if list(before.values()) != expected:
        raise ValueError("Canonical input hashes do not match accepted YEE-61/73/29")
    with read_only(foundation) as db, read_only(signals) as signal_db, read_only(features) as feature_db:
        members = {row["canonical_identity"]: dict(row) for row in db.execute("SELECT * FROM plugin_category_memberships")}
        signal_rows = {row["canonical_identity"]: json.loads(row["record_json"]) for row in signal_db.execute("SELECT * FROM category_signal_member_features")}
        if set(members) != set(signal_rows):
            raise ValueError("YEE-61/73 member identities differ")
        scope_rows, eligible, recovery = [], [], []
        for row in db.execute("SELECT * FROM plugin_product_scope ORDER BY source, source_resource_id"):
            feature = json.loads(row["source_feature_json"])
            upstream = dict(feature_db.execute("SELECT * FROM resource_features WHERE source=? AND source_resource_id=?", (row["source"], row["source_resource_id"])).fetchone())
            if feature != upstream:
                raise ValueError("YEE-61 source feature differs from accepted YEE-29")
            identity = row["canonical_identity"]
            record = {"canonical_identity": identity, "source": row["source"], "source_resource_id": row["source_resource_id"], "upstream_scope_status": row["product_scope_status"], "feature": feature, "initial_evidence_tier": initial_tier(feature), "commercial_relevance": commercial_relevance(feature), "membership": members.get(identity), "signal_context": signal_rows.get(identity)}
            scope_rows.append(record)
            if row["product_scope_status"] == "PLUGIN_PRODUCT_CONFIRMED":
                eligible.append(record)
            elif row["product_scope_status"] == "PLUGIN_PRODUCT_REVIEW" and commercial_relevance(feature):
                recovery.append({**record, "verification_state": "EXCLUDED_UNVERIFIED", "positive_product_form_evidence": None, "verification_source_id": None, "verification_basis": "Fresh positive server/proxy-plugin proof required; paidness and loaders do not prove form."})
        taxonomy = [dict(row) for row in db.execute("SELECT * FROM plugin_category_taxonomy ORDER BY category_order")]
        for name, rows in [("SCOPE_SCAN", scope_rows), ("COMMERCIAL_SIGNAL_UNIVERSE", eligible), ("REVIEW_PRODUCT_FORM_VERIFICATION", recovery), ("TAXONOMY_COVERAGE", taxonomy)]:
            write_jsonl(output / (name + ".jsonl"), rows)
        counts = Counter((r["source"], r["upstream_scope_status"], r["initial_evidence_tier"]) for r in scope_rows)
        reconciliation = {"work_order": "YEE-143", "input_sha256": before, "scope_rows": len(scope_rows), "confirmed_rows": len(eligible), "recovery_candidates": len(recovery), "source_scope_tier_counts": [{"source": a, "status": b, "tier": c, "count": n} for (a,b,c),n in sorted(counts.items())], "taxonomy_categories": len(taxonomy), "candidate_universe_frozen_before_web_research": True, "source_metric_semantics": "downloads/reviews/stars are NON_COMMERCIAL; price/paid state is precedent only; no payment counter inherited", "feature_rows": feature_db.execute("SELECT count(*) FROM resource_features").fetchone()[0], "yee29_source_counts": dict(feature_db.execute("SELECT source,count(*) FROM resource_features GROUP BY source")), "input_integrity": [db.execute("PRAGMA integrity_check").fetchone()[0], signal_db.execute("PRAGMA integrity_check").fetchone()[0], feature_db.execute("PRAGMA integrity_check").fetchone()[0]]}
    if before != {str(path): digest(path) for path in (foundation, signals, features)}:
        raise ValueError("Input changed during read-only scan")
    write_json(output / "INPUT_RECONCILIATION.json", reconciliation)
    return reconciliation


def recovered_allowed(verification, sources):
    source = sources.get(verification.get("verification_source_id"), {})
    return (verification.get("verification_state") == "VERIFIED_SERVER_PROXY_PLUGIN"
            and bool(verification.get("positive_product_form_evidence"))
            and bool(verification.get("verification_basis"))
            and source.get("opened") is True
            and source.get("product_form_verification_kind") in {"PLUGIN_JAR_INSTALLATION", "EXPLICIT_SERVER_PLUGIN_RUNTIME"}
            and source.get("canonical_identity") == verification.get("canonical_identity"))


def direct_paid(observation):
    """Whitelist observable payment semantics, never price, download or review proxies."""
    kind = observation.get("kind")
    if kind == "public_purchase_counter":
        value = observation.get("value")
        return (isinstance(value, (int, float)) and not isinstance(value, bool)
                and value > 0 and observation.get("counter_semantics") == "product_attributable_purchases")
    return kind in {"explicit_buyer_payment", "observed_transaction"} and bool(observation.get("payment_evidence"))


def independence_witness(observations):
    """Two vendors/products or an explicit independent buyer channel, never URLs alone."""
    direct = [o for o in observations if direct_paid(o)]
    for index, first in enumerate(direct):
        for second in direct[index + 1:]:
            if (first["canonical_identity"] != second["canonical_identity"]
                    and first["vendor"] != second["vendor"]):
                return {"observation_ids": [first["observation_id"], second["observation_id"]],
                        "basis": "Distinct products and vendors; marketplace infrastructure may be shared."}
            for buyer, other in ((first, second), (second, first)):
                if (buyer.get("kind") == "explicit_buyer_payment"
                        and buyer.get("actor_id") and buyer.get("independent_of_vendor") is True
                        and buyer["source_id"] != other["source_id"]):
                    return {"observation_ids": [other["observation_id"], buyer["observation_id"]],
                            "basis": "Independent first-person buyer channel corroborates product counter; self-report, not receipt."}
    return None


def pool_state(observations):
    if any(direct_paid(o) for o in observations):
        return "EVIDENCE_BACKED_PAID_DEMAND" if independence_witness(observations) else "PAID_SIGNAL_SINGLE_SOURCE"
    if any(o.get("tier") == "PAID_PRECEDENT_ONLY" for o in observations):
        return "PAID_PRECEDENT_ONLY"
    if any(o.get("tier") == "NON_COMMERCIAL_SIGNAL" for o in observations):
        return "NON_COMMERCIAL_INTEREST_ONLY"
    return "INSUFFICIENT_EVIDENCE"


def load_jsonl(path):
    return [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line]


def require_opened(source_id, sources):
    source = sources.get(source_id, {})
    if (source.get("opened") is not True or not source.get("canonical_url")
            or not source.get("retrieval_date")):
        raise ValueError("Factual observation lacks dated opened source: " + str(source_id))
    return source


def normalize(stage_a, capture):
    """Normalize a reviewed factual capture; never discover concepts or scrape live pages."""
    stage_a = Path(stage_a)
    sources = {s["source_id"]: s for s in capture["sources"]}
    if len(sources) != len(capture["sources"]):
        raise ValueError("Duplicate source identities")
    universe = {r["canonical_identity"]: r for r in load_jsonl(stage_a / "COMMERCIAL_SIGNAL_UNIVERSE.jsonl")}
    verification = {r["canonical_identity"]: r for r in load_jsonl(stage_a / "REVIEW_PRODUCT_FORM_VERIFICATION.jsonl")}
    products = {p["canonical_identity"]: p for p in capture["products"]}
    if len(products) != len(capture["products"]):
        raise ValueError("Duplicate canonical products")
    pool_ids = {p["pool_id"] for p in capture["pools"]}
    scope = {r["canonical_identity"]: r["upstream_scope_status"] for r in load_jsonl(stage_a / "SCOPE_SCAN.jsonl")}
    observations = []
    taxonomy = {r["category_id"]: {s["subcategory_id"] for s in json.loads(r["subcategories_json"])}
                for r in load_jsonl(stage_a / "TAXONOMY_COVERAGE.jsonl")}
    for note in capture.get("verification_notes", []):
        identity = note["canonical_identity"]
        if identity not in verification or identity in products:
            raise ValueError("Rejection note must refer to an unverified REVIEW candidate")
        for source_id in note.get("opened_source_ids", []):
            require_opened(source_id, sources)
        verification[identity]["fresh_verification_note"] = note
    for identity, product in sorted(products.items()):
        if any(scope.get(alias) == "OUT_OF_SCOPE_PRODUCT_FORM" for alias in product.get("upstream_aliases", [])):
            raise ValueError("External marketplace alias cannot bypass upstream OUT_OF_SCOPE")
        source = require_opened(product["source_id"], sources)
        if (not product.get("positive_product_form_evidence") or not product.get("identity_match_basis")
                or source.get("canonical_identity") != identity
                or source.get("product_form_verification_kind") not in {"PLUGIN_JAR_INSTALLATION", "EXPLICIT_SERVER_PLUGIN_RUNTIME"}):
            raise ValueError("Missing positive plugin-form/identity proof: " + identity)
        if product["pool_id"] not in pool_ids:
            raise ValueError("Product refers to missing buyer-job pool")
        if identity in verification:
            verification[identity].update(verification_state="VERIFIED_SERVER_PROXY_PLUGIN",
                verification_source_id=product["source_id"],
                positive_product_form_evidence=product["positive_product_form_evidence"],
                verification_basis=product["identity_match_basis"], canonical_url=source["canonical_url"],
                local_category_assignment="Pool crosswalk only; upstream membership remains REVIEW")
            verification[identity]["fresh_attempt_status"] = "POSITIVE_FORM_VERIFIED"
            if not recovered_allowed(verification[identity], sources):
                raise ValueError("Recovery gate rejected: " + identity)
            universe[identity] = {**verification[identity], "eligibility_lane": "YEE143_LOCAL_RECOVERY"}
        elif identity not in universe and product.get("lane") != "EXTERNAL_CORROBORATION":
            raise ValueError("Product outside accepted/recovered universe: " + identity)
        facts = source.get("facts", {})
        common = {"canonical_identity": identity, "source_id": product["source_id"],
                  "vendor": product["vendor"], "pool_id": product["pool_id"],
                  "retrieval_date": source["retrieval_date"], "canonical_url": source["canonical_url"]}
        if facts.get("public_purchase_counter") is not None:
            observations.append({**common, "observation_id": identity + ":counter", "kind": "public_purchase_counter",
                "value": facts["public_purchase_counter"], "unit": "source-labelled Purchases",
                "counter_semantics": "product_attributable_purchases", "tier": "DIRECT_PAID_SIGNAL",
                "limitations": source["limitations"]})
        if facts.get("price_display_amount") is not None:
            observations.append({**common, "observation_id": identity + ":price", "kind": "displayed_price",
                "value": facts["price_display_amount"], "currency_display": facts["currency_display"],
                "unit": "offered license price", "tier": "PAID_PRECEDENT_ONLY"})
    for payment in capture.get("buyer_payments", []):
        identity = payment["canonical_identity"]
        product = products[identity]
        source = require_opened(payment["source_id"], sources)
        observations.append({**payment, "observation_id": identity + ":buyer:" + payment["actor_id"],
            "kind": "explicit_buyer_payment", "tier": "DIRECT_PAID_SIGNAL", "value": None,
            "unit": "explicit self-reported purchase", "independent_of_vendor": True,
            "vendor": product["vendor"], "pool_id": product["pool_id"],
            "canonical_url": source["canonical_url"], "retrieval_date": source["retrieval_date"],
            "event_period": source.get("source_published_period"), "limitations": "Anonymous historical self-report, not verified receipt; shared actor IDs preserve correlation."})
    for observation in capture.get("additional_observations", []):
        source = require_opened(observation["source_id"], sources)
        if observation["canonical_identity"] not in products:
            raise ValueError("Observation has no verified product")
        observations.append({**observation, "retrieval_date": source["retrieval_date"], "canonical_url": source["canonical_url"]})
    if capture.get("purchase_intent"):
        intent = capture["purchase_intent"]
        source = require_opened(intent["source_id"], sources)
        observations.append({**intent, "observation_id": "chat:tentative_intent", "canonical_identity": None,
            "kind": "buyer_purchase_intent", "tier": "CORROBORATING_PAID_SIGNAL", "value": None,
            "vendor": None, "canonical_url": source["canonical_url"], "retrieval_date": source["retrieval_date"],
            "limitations": "Intent concerns ChatControl/VelocityControl, not completed purchase or RedisChat product attribution."})
    if len({o["observation_id"] for o in observations}) != len(observations):
        raise ValueError("Duplicate observations")
    for observation in observations:
        require_opened(observation["source_id"], sources)
        if (observation["tier"] == "DIRECT_PAID_SIGNAL") != direct_paid(observation):
            raise ValueError("Payment taxonomy inconsistent with semantic whitelist")
    profiles = []
    for pool in sorted(capture["pools"], key=lambda p: p["pool_id"]):
        if pool.get("advancement_decision") is not None or pool.get("selected_for_next_work_order") is not None:
            raise ValueError("Supervisor-owned advancement fields must remain null")
        if pool["subcategory_id"] not in taxonomy.get(pool["category_id"], set()):
            raise ValueError("Pool crosswalk outside accepted taxonomy")
        members = [p for p in products.values() if p["pool_id"] == pool["pool_id"]]
        obs = [o for o in observations if o["pool_id"] == pool["pool_id"]]
        direct = [o for o in obs if direct_paid(o)]
        if not any(p["canonical_identity"] in universe for p in members):
            raise ValueError("Pool has no accepted or recovered canonical anchor")
        witness = independence_witness(obs)
        profiles.append({**pool, "evidence_state": pool_state(obs), "independent_corroboration": witness,
            "products": sorted(p["canonical_identity"] for p in members),
            "observations": sorted(o["observation_id"] for o in obs),
            "paid_signal_products": len({o["canonical_identity"] for o in direct}),
            "paid_signal_observations": len(direct), "vendor_count": len({o["vendor"] for o in direct}),
            "buyer_channels": sorted({o["actor_id"] for o in direct if o.get("actor_id")}),
            "source_domains": sorted({urlparse(o["canonical_url"]).netloc for o in obs}),
            "maintenance_observations": [{"product": p["canonical_identity"], "updated_display": sources[p["source_id"]].get("facts", {}).get("updated_display")} for p in members],
            "free_context_source_ids": sorted(c["source_id"] for c in capture["free_context"] if c["pool_id"] == pool["pool_id"]),
            "compatibility_context": "Vendor claims only; no runtime support matrix or effectiveness test created.",
            "concentration_risk": "Single vendor/product" if len({o["vendor"] for o in direct}) <= 1 else "Multiple vendors, shared marketplace counter infrastructure",
            "unresolved_unknowns": ["Unique paying buyers, refunds, license grants/transfers and purchase time windows unknown.", "Commercial traction does not establish unmet need, retention, market size or a viable new paid wedge.", "Buyer motives not representative; current support quality and compatibility untested."],
            "paid_signal_tier": "DIRECT_PAID_SIGNAL" if direct else "PAID_PRECEDENT_ONLY"})
    for context in capture["free_context"]:
        require_opened(context["source_id"], sources)
        if not any(direct_paid(o) for o in observations if o["pool_id"] == context["pool_id"]):
            raise ValueError("Free-incumbent context has no prior direct commercial signal")
    for identity, row in universe.items():
        fresh = [o for o in observations if o.get("canonical_identity") == identity]
        row["final_evidence_tier"] = "DIRECT_PAID_SIGNAL" if any(direct_paid(o) for o in fresh) else row["initial_evidence_tier"]
        row["fresh_observation_ids"] = sorted(o["observation_id"] for o in fresh)
    return {"universe": sorted(universe.values(), key=lambda r: r["canonical_identity"]),
            "verification": sorted(verification.values(), key=lambda r: r["canonical_identity"]),
            "sources": sorted(sources.values(), key=lambda s: s["source_id"]),
            "products": sorted(products.values(), key=lambda p: p["canonical_identity"]),
            "observations": sorted(observations, key=lambda o: o["observation_id"]), "profiles": profiles,
            "free_context": sorted(capture["free_context"], key=lambda c: (c["pool_id"], c["source_id"]))}


def synthesize(stage_a, capture_path, output):
    capture = json.loads(Path(capture_path).read_text(encoding="utf-8"))
    data = normalize(stage_a, capture)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    names = {"universe": "COMMERCIAL_SIGNAL_UNIVERSE", "verification": "REVIEW_PRODUCT_FORM_VERIFICATION",
             "sources": "SOURCE_DOCUMENTS", "products": "VERIFIED_PRODUCTS", "observations": "PAID_SIGNAL_EVIDENCE",
             "profiles": "DEMAND_POOL_PROFILES", "free_context": "FREE_INCUMBENT_CONTEXT"}
    for key, name in names.items():
        write_jsonl(output / (name + ".jsonl"), data[key])
    write_jsonl(output / "RESEARCH_QUERIES.jsonl", capture.get("research_queries", []))
    db_path = output / "demand_first_commercial_signal.sqlite"
    if db_path.exists():
        raise ValueError("Use a fresh output directory; never overwrite a dataset")
    with sqlite3.connect(db_path) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript("""
        CREATE TABLE resources(id TEXT PRIMARY KEY, record_json TEXT NOT NULL);
        CREATE TABLE sources(id TEXT PRIMARY KEY, record_json TEXT NOT NULL);
        CREATE TABLE verification(id TEXT PRIMARY KEY REFERENCES resources(id), source_id TEXT REFERENCES sources(id), state TEXT NOT NULL, record_json TEXT NOT NULL);
        CREATE TABLE observations(id TEXT PRIMARY KEY, product_id TEXT REFERENCES resources(id), source_id TEXT NOT NULL REFERENCES sources(id), record_json TEXT NOT NULL);
        CREATE TABLE pools(id TEXT PRIMARY KEY, state TEXT NOT NULL, record_json TEXT NOT NULL);
        CREATE TABLE pool_members(pool_id TEXT REFERENCES pools(id), product_id TEXT REFERENCES resources(id), PRIMARY KEY(pool_id,product_id));
        CREATE TABLE free_context(pool_id TEXT REFERENCES pools(id), source_id TEXT REFERENCES sources(id), record_json TEXT NOT NULL, PRIMARY KEY(pool_id,source_id));
        """)
        records = {r["canonical_identity"]: r for r in data["universe"] + data["verification"]}
        for p in data["products"]:
            records.setdefault(p["canonical_identity"], {"external_corroboration_only": True})["fresh_product"] = p
        encoded = lambda r: json.dumps(r, ensure_ascii=False, sort_keys=True)
        db.executemany("INSERT INTO resources VALUES (?,?)", [(i, encoded(r)) for i,r in sorted(records.items())])
        db.executemany("INSERT INTO sources VALUES (?,?)", [(r["source_id"], encoded(r)) for r in data["sources"]])
        db.executemany("INSERT INTO verification VALUES (?,?,?,?)", [(r["canonical_identity"], r["verification_source_id"], r["verification_state"], encoded(r)) for r in data["verification"]])
        db.executemany("INSERT INTO observations VALUES (?,?,?,?)", [(r["observation_id"], r["canonical_identity"], r["source_id"], encoded(r)) for r in data["observations"]])
        db.executemany("INSERT INTO pools VALUES (?,?,?)", [(r["pool_id"], r["evidence_state"], encoded(r)) for r in data["profiles"]])
        db.executemany("INSERT INTO pool_members VALUES (?,?)", [(r["pool_id"], p) for r in data["profiles"] for p in r["products"]])
        db.executemany("INSERT INTO free_context VALUES (?,?,?)", [(r["pool_id"], r["source_id"], encoded(r)) for r in data["free_context"]])
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = db.execute("PRAGMA foreign_key_check").fetchall()
    qa = {"status": "PASS", "schema_version": capture["schema_version"], "capture_sha256": digest(capture_path),
          "accepted_rows": len(load_jsonl(Path(stage_a) / "COMMERCIAL_SIGNAL_UNIVERSE.jsonl")),
          "recovery_candidates": len(data["verification"]),
          "recovered_rows": sum(r["verification_state"] == "VERIFIED_SERVER_PROXY_PLUGIN" for r in data["verification"]),
          "excluded_unverified_rows": sum(r["verification_state"] == "EXCLUDED_UNVERIFIED" for r in data["verification"]),
          "final_eligible_rows": len(data["universe"]), "fresh_verified_products": len(data["products"]),
          "observations": len(data["observations"]), "pool_states": dict(sorted(Counter(p["evidence_state"] for p in data["profiles"]).items())),
          "sqlite_integrity": integrity, "foreign_key_violations": len(foreign_keys),
          "all_factual_observations_have_dated_opened_sources": True, "price_download_review_to_payment_promotions": 0,
          "review_promotions_without_positive_form_proof": 0, "advancement_fields_all_null": True,
          "upstream_writes": 0, "counter_sums_or_revenue_estimates": 0,
          "coverage_caveat": capture["coverage_limits"]}
    if integrity != "ok" or foreign_keys:
        raise ValueError("SQLite validation failed")
    write_json(output / "QA_RESULT.json", qa)
    write_json(output / "MANIFEST.json", {"work_order": "YEE-143", "as_of": capture["as_of"],
        "capture_sha256": digest(capture_path), "stage_a_sha256": {p.name: digest(p) for p in sorted(Path(stage_a).glob("*")) if p.is_file()},
        "artifacts": [{"file": p.name, "sha256": digest(p), "bytes": p.stat().st_size} for p in sorted(output.iterdir()) if p.is_file()]})
    return qa


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--foundation")
    parser.add_argument("--signals")
    parser.add_argument("--features")
    parser.add_argument("--stage-a")
    parser.add_argument("--capture")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.capture:
        if not args.stage_a:
            parser.error("--capture requires --stage-a")
        result = synthesize(args.stage_a, args.capture, args.output)
    else:
        if not all((args.foundation, args.signals, args.features)):
            parser.error("scan requires --foundation, --signals and --features")
        result = scan(args.foundation, args.signals, args.features, args.output)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
