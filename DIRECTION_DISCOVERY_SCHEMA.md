# YEE-75 Direction Discovery schema

Schema version: `yee-75-category-direction-discovery-v0.3`.

Lexical discovery rules: `yee-75-category-local-lexical-mining-v0.3`; functional-direction guard: `yee-75-functional-direction-guard-v0.1`; functional-cohesion contract: `yee-75-functional-cohesion-contract-v0.1`.

## Canonical grain and provenance

The sole input is the accepted YEE-73 `category_signal_layer.sqlite`, opened read-only and pinned by SHA-256 `374b5955d11764a9637e63f9611416ac4d6bc157c40f1582bebfb0eeaa1e11a3`. The identity key is `(source, source_resource_id)` and contains exactly 1,352 YEE-73 confirmed signal members (Hangar 1,221; Voxel 131). No YEE-61 REVIEW/OUT_OF_SCOPE identity rows are loaded. The YEE-61 taxonomy and YEE-73 analysis_as_of are inherited unchanged.

Stable `direction_id` is `dir_` plus the first 24 lowercase hex characters of SHA-256 over UTF-8 `primary_category_id + NUL + direction_type + NUL + canonical_direction_key`. The two direction types are `SUBCATEGORY_BASELINE` and `LEXICAL_SUBNICHE`.

## Tables and exports

| Table/export | Grain | Production expectation |
| --- | --- | ---: |
| `direction_universe` | direction | 38 baselines plus all qualifying lexical directions |
| `direction_memberships` | direction × confirmed source identity | one exact baseline membership per member plus lexical phrase memberships |
| `direction_source_facts` | direction × source | exactly 2 × direction count |
| `direction_evaluations` | direction | exactly one per direction |
| `candidate_directions` | candidate direction | exact ADVANCE/WATCH subset; may be empty |
| `direction_evidence_packs` | candidate direction | exactly candidate count |
| `category_direction_summary` | frozen category | exactly 11, in taxonomy order |

`category_direction_discovery.sqlite` additionally contains immutable `run_metadata`, `frozen_taxonomy_snapshot`, and `source_coverage_snapshot`. Every data table retains its scalar/nested columns and canonical `record_json`; run metadata is protected from UPDATE/DELETE by SQLite triggers.

`direction_universe` records direction type/category, canonical key/label, baseline subcategory when applicable, lexical token count, rule/schema/input/taxonomy provenance, observed identity counts by source, observed subcategory distribution, unique dominant subcategory when one exists, evidence semantics, and exact-member-set alias exclusion provenance. Lexical rows additionally carry the functional-direction guard version/classification/content tokens and the functional-cohesion contract version, `FUNCTIONAL_COHESION` classification, and cohesive content terms. Cohesion is fail-closed: single-token directions must be in the versioned stable-function allowlist; multiword directions must match an exact reviewed coherent-function phrase entry. Ambiguous, brand-only, polysemous, generic prose, and unreviewed phrases are excluded before source facts/evaluation. Equivalent member sets prefer the phrase with more explicitly cohesive terms, then fewer nonfunctional tokens, title evidence, and deterministic lexical tie-breaks. No minimum lexical-direction or candidate count is imposed. Independent QA rechecks every retained lexical direction, candidate direction, and ADVANCE direction against a separately implemented copy of the versioned cohesion contract.

`direction_memberships` records direction, frozen category/subcategory, source identity, method, and (for lexical membership) exact normalized match, source field/span when recoverable, phrase key, and normalization/rule versions. JSONL uses JSON `null`; CSV uses `\\N`; all exports are UTF-8, LF, deterministic.

Source facts preserve source-local confirmed denominators, source-category percentile distributions, raw download quantiles confined to one source/direction, freshness coverage, source-native engagement, coverage-risk context, and Voxel-only paid/price distributions grouped by currency. Hangar paid/price remains unavailable, not free/zero. No cross-source raw download aggregate exists.

Evidence packs exist only for candidate directions, include both source fact slices and up to six per-source examples, and explicitly mark examples as evidence sampling rather than product ranking. No external validation is claimed.

## Nulls and ordering

JSONL `null` and CSV `\\N` mean unavailable/not applicable. A recorded numeric zero remains zero. Empty frozen baselines and zero-member source slices are retained. Direction rows use frozen category order, baselines by frozen subcategory order, then lexical key; memberships/facts/candidates use stable direction/source identity keys; category summary uses frozen category order. Voxel currencies are never combined.

## Prohibited fields

This schema contains no opportunity state/score, ranking, winner, recommendation, semantic/fuzzy merge, market-size estimate, commercial validation, external research, product concept, or Stage D output. `candidate_state` only routes observed evidence to a later stage and is not a Stage D result.
