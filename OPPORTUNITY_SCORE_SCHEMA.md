# YEE-46 Opportunity Triage v0

Schema version: `yee-46-opportunity-triage-v0.1`
Input: accepted YEE-43 `family_signal_analysis.sqlite`, opened read-only and pinned by SHA-256.

## Deterministic semantics

All derived numeric values use six-decimal rounding. Missing source-native values remain null (`null` in JSONL, `\N` in CSV); a recorded zero remains zero. All demand, supply, freshness and concentration comparisons are source-local. Raw `downloads_total` is never combined or compared across marketplaces.

For source-level whitespace, the midrank percentile is `(count(x_i < x) + 0.5*(count(x_i = x)-1))/(N-1)`, calculated within source; it is `0.5` when `N=1`. Whitespace is `100*(1-p)`. Freshness-age midranks are calculated within source over non-null `freshness_age_days_p50` values only.

## `source_opportunity_components`

One row per accepted YEE-43 `(family_id, source)` row, with columns:

`family_id`, `source`, `D`, `W`, `F`, `C`, `source_score_balanced`, `source_score_demand_first`, `source_score_whitespace_first`, `resource_count`, `demand_available_count`, `freshness_available_count`, `demand_percentile_p75`, `demand_percentile_p90`, `demand_percentile_ge90_share`, `demand_percentile_ge95_share`, `freshness_age_days_p50`, `freshness_le90_share`, `freshness_gt365_share`, `demand_concentration_top1_share`, `demand_concentration_hhi`, `paid_count`, `paid_known_count`, `paid_share_known`.

The three `paid_*` fields are preserved from canonical YEE-43 source rows in this audit table; they remain null for sources where the input does not provide them.

- `D` exists only when `demand_available_count > 0`: `0.45*p90 + 0.25*p75 + 0.20*(ge90_share*100) + 0.10*(ge95_share*100)`.
- `W` uses the source-local `resource_count` midrank described above.
- `F` exists only when `freshness_age_days_p50` is non-null: `0.50*(le90_share*100) + 0.30*(100*(1-freshness_age_midrank)) + 0.20*((1-gt365_share)*100)`.
- `C` exists only when both concentration inputs are non-null: `100*(0.60*(1-hhi) + 0.40*(1-top1_share))`.
- Source profile weights `(D,W,F,C)` are BALANCED `(0.50,0.25,0.15,0.10)`, DEMAND_FIRST `(0.65,0.15,0.10,0.10)`, and WHITESPACE_FIRST `(0.40,0.40,0.10,0.10)`. D is required; other missing components are omitted and remaining weights renormalized.

## `family_opportunity_scores`

Exactly one row per YEE-43 family. Preserved metadata columns: `family_id`, `canonical_topic_key`, `family_status`, `member_count`, `member_topic_keys`, `aliases`, `candidate_classes`, `source_presence`, `source_presence_count`, `has_voxel_paid_evidence`, `evidence_coverage_share`.

Score columns:

- `{profile}_core_source_score` and `{profile}_source_weight_coverage` for each of `balanced`, `demand_first`, and `whitespace_first`. Core aggregates source profile scores with nominal Modrinth/Voxel/Hangar weights `0.45/0.40/0.15`, renormalized over available source scores; coverage is the unnormalized sum of available nominal weights.
- `cross_market_validation_score` is `50*(source_presence_count-1)`.
- `voxel_monetization_score` is null without a Voxel row or when `paid_known_count` is null/zero; otherwise it is exactly `50*I(paid_count>0) + 50*paid_share_known`. It uses canonical YEE-43 Voxel paid fields, never price/currency. QA independently reconciles the score and preserved paid fields against those canonical source rows.
- `{profile}_final_score` combines core/Voxel monetization/cross-market validation with weights `0.80/0.05/0.15`; when monetization is null, the remaining `0.80/0.15` weights are renormalized.
- `{profile}_rank`, `median_profile_rank`, `mean_profile_rank`, `rank_span`, `consensus_score`, `consensus_rank`, `triage_bucket`.

Each profile ranks by descending final score, then ascending `family_id`. Consensus order is ascending median rank, mean rank, BALANCED rank, and `family_id`. `consensus_score` is the median of the three final scores. Buckets are ranks 1–100 `ADVANCE_RESEARCH`, 101–300 `WATCH`, and 301–1,618 `DEFER`. `family_status` is preserved but never enters score arithmetic.

## `opportunity_shortlist`

Exactly the 100 `ADVANCE_RESEARCH` score rows in ascending `consensus_rank`; the export retains the full family scores plus each present source's `resource_count`, D/W/F/C, and three source profile scores in source-prefixed columns. Missing source rows remain null. It is a research shortlist, not a product recommendation.

## Storage and exports

The SQLite database contains `family_opportunity_scores`, `source_opportunity_components`, `opportunity_shortlist`, and deterministic run `metadata`, with indexes for family, source, consensus rank, triage bucket, and canonical topic key. JSONL objects and CSV rows have stable ordering; CSV arrays use compact canonical JSON.
