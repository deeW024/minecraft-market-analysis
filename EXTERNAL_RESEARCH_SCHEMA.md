# YEE-47 External Market Evidence v0 — schema and null semantics

Schema version: `yee-47-external-market-evidence-v0.1`

Pilot authorization: consensus ranks 1–10 only. The YEE-46 shortlist and analysis database are immutable, read-only inputs.

## Tables and exports

The SQLite database and matching JSONL/CSV exports contain four tables: `family_research_packs`, `external_evidence`, `competitor_entities`, and `research_queries`. JSONL preserves JSON values and explicit `null`. CSV uses the literal `\\N` for null; JSON-valued columns are canonical compact JSON strings. Every row carries its YEE-46 `family_id` and `consensus_rank`.

### `family_research_packs`

The complete accepted YEE-46 shortlist row is copied unchanged, including all identity, rank, score, profile, per-source native, candidate-class, and triage fields. This preserves `family_id`, `canonical_topic_key`, member/alias context, `triage_bucket`, all three rank/rank-profile scores, source-weight coverage, source-native D/W/F/C values, monetization evidence, and source-presence fields. The immutable upstream row is the authority for the exact baseline columns; no value is recomputed or rewritten by this layer.

Research fields: `research_status` (`RESOLVED`, `AMBIGUOUS`, `UNRESOLVED`), `resolved_concept_name`, `concept_summary`, `primary_entity_url`, `direct_competitor_count`, `direct_competitor_ids`, `feature_themes`, `pricing_observations_by_currency`, `maintenance_summary`, `popularity_proxy_summary`, `pain_point_hypotheses`, `gap_hypotheses`, `evidence_ids`, `evidence_count`, `primary_evidence_count`, `community_evidence_count`, `distinct_domain_count`, `research_coverage_status`, `research_notes`, `field_evidence_map`.

`resolved_concept_name`, `concept_summary`, and `primary_entity_url` are null unless the concept is resolved and directly evidenced. Missing/unknown prices and unknown maintenance are null, never inferred to mean free/current. Numeric values are null when no source states an explicit value; currencies are kept in separate groups and are never converted or aggregated. YEE-46 score/ranks are copied without modification; no new opportunity score is defined here.

`research_coverage_status` is `SUFFICIENT` only for a resolved family with at least four evidence items, two distinct source domains, at least one primary-source item, and at least one supported DIRECT competitor. Otherwise resolved packs are `PARTIAL`; unresolved labels remain `AMBIGUOUS` or `UNRESOLVED`.

`field_evidence_map` maps each factual research field to the evidence IDs that support it. It is an additive provenance field; no pack-level fact is intended to stand without source evidence.

### `external_evidence`

Columns: `evidence_id`, `family_id`, `consensus_rank`, `source_url`, `canonical_url`, `source_domain`, `source_title`, `source_type`, `retrieved_at`, `published_or_updated_at`, `claim_type`, `observation`, `optional_excerpt`, `numeric_value`, `numeric_unit`, `currency`, `entity_name`, `notes`, `query_id`.

`source_type` is one of `PRIMARY_PRODUCT`, `PRIMARY_DOCS`, `PRIMARY_REPOSITORY`, `MARKETPLACE_LISTING`, `PRIMARY_SUPPORT`, `COMMUNITY`, `EDITORIAL`. `claim_type` is one of `SEMANTIC_IDENTITY`, `COMPETITOR_RELATION`, `FEATURE`, `PRICING`, `MAINTENANCE`, `POPULARITY_PROXY`, `PAIN_POINT`, `GAP_SIGNAL`. All source pages retained as evidence have a public URL, captured title, UTC `retrieved_at`, and a `query_id` tied to the execution log. Evidence may only reference a separately recorded opened-page entry in `PILOT_CAPTURE.json`; search snippets alone cannot create evidence. `published_or_updated_at`, excerpt, numeric value/unit, currency, entity, and notes remain null when the opened source does not explicitly provide them. Excerpts are optional and at most 25 words.

### `competitor_entities`

Columns: `competitor_id`, `family_id`, `consensus_rank`, `entity_name`, `canonical_url`, `relation_type`, `product_type`, `platform_or_ecosystem`, `pricing_model`, `price_amount`, `currency`, `maintenance_status`, `feature_summary`, `evidence_ids`.

`relation_type` is `DIRECT`, `SUBSTITUTE`, or `ADJACENT`; at most five DIRECT entities are retained per family. Relation evidence must cite `COMPETITOR_RELATION` or `SEMANTIC_IDENTITY`. A non-null pricing model/amount requires `PRICING` evidence, and explicit prices require currency. Unknown fields remain null; absence of a price is not evidence of a free product.

### `research_queries`

Columns: `query_id`, `family_id`, `consensus_rank`, `query_sequence`, `query_text`, `issued_at`, `purpose`, `result_action`. Query rows are recorded in actual execution order. Search snippets are discovery only and are not evidence items.

## Evidence, provenance, and hypotheses

Evidence IDs are deterministic over family, canonical URL, claim type, and normalized observation. URLs are canonicalized by lowercasing scheme/host, removing default ports/fragments and common tracking parameters, and sorting remaining query parameters. Duplicate evidence with the same family/canonical URL/claim/normalized observation is deduplicated; conflicting numeric/source facts fail validation.

Competitor relations, pricing, features, maintenance, source-native popularity proxies, and family research fields must point to compatible evidence IDs. Hypotheses are explicitly prefixed `Hypothesis:` and include a concrete basis plus at least two evidence IDs from distinct source URLs. Feature absence alone is not a gap signal. Hypotheses are research statements, not product recommendations or revenue/TAM estimates.

Minimum three distinct search queries are required before labeling a family `UNRESOLVED`, unless the capture documents a conclusive disproof. The maximum is ten executed queries and fifteen retained opened pages per family. Coverage labels do not imply market size, ranking, or recommendation.

## Integrity and reproducibility

SQLite has primary/foreign-key constraints, `PRAGMA integrity_check` and `foreign_key_check` are required to pass, and row counts must reconcile exactly with JSONL/CSV. Production generation also replays the normalized capture in an isolated temporary directory and checks all deterministic data artifact bytes. The manifest inventories artifact byte sizes and SHA-256 values; it excludes itself to avoid recursive hashing.
