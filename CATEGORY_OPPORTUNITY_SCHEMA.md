# YEE-76 Category Opportunity Map Schema

Schema version: `yee-76-category-opportunity-map-v0.1`.

## Deliverables

- `category_opportunity_profiles.jsonl/.csv`: one record per each of the 11 frozen categories, in taxonomy order. Includes category state/reasons, confirmed member counts and presence per source, upstream source-supply risks, the baseline evidence profile, direction counts and candidate-state counts, positive-support shape, lead/watch evidence, full direction evidence, Voxel paid-evidence availability, Hangar paid-evidence availability, source-coverage context, and risk flags.
- Each profile includes explicit `lead_watch_counts_by_direction_type` for baseline and lexical directions, separate from the underlying candidate-state counts.
- `category_direction_tiers.jsonl/.csv`: exactly one record per each of the 47 accepted Stage C directions. Tiers are a deterministic state mapping and do not imply rank.
- `category_research_options.jsonl/.csv`: exact accepted Stage C candidate set (ADVANCE and WATCH only), enriched with its category-local state, evidence pack, source facts, and risk context. Rows are unranked.
- `category_opportunity_map.sqlite`: immutable run metadata, `input_provenance`, a frozen taxonomy snapshot, `category_opportunity_profiles`, `category_direction_tiers`, and `category_research_options`.
- `GOAL_ALIGNMENT.md`, `CATEGORY_OPPORTUNITY_SEMANTICS.md`, this schema, `CATEGORY_OPPORTUNITY_MAP.md`, `QA_RESULT.json`, `DATASET_MANIFEST.json`, and `FINAL_REPORT.md`.

## Nulls and deterministic serialization

JSON uses explicit `null`; CSV renders null as `\N`. JSON is UTF-8, compact, key-sorted, and disallows NaN. JSONL/CSV use LF line endings. Arrays and records have stable ordering. SQLite uses stable primary keys and JSON mirrors. The manifest records the exact file byte size and SHA-256 for each output other than the manifest itself.

## State, tier, and option vocabulary

Category states: `PROMISING`, `MIXED_OPPORTUNITY`, `LOW_OPPORTUNITY`, `NO_CLEAR_OPPORTUNITY`, `INSUFFICIENT_EVIDENCE`.

Direction states are preserved from Stage C. Tiers are exactly `LEAD_DIRECTION`, `WATCH_DIRECTION`, `CONTEXT_DIRECTION`, and `INSUFFICIENT_DIRECTION` as defined in the semantics document. `category_research_options` is the unranked ADVANCE/WATCH subset only.

Forbidden analytical outputs include numeric opportunity/attractiveness score, global rank, winner, recommendation, universal shortlist, and Stage E research output.
