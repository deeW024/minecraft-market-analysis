# YEE-29 FEATURE_SCHEMA v0.1

## Scope and identity

The canonical identity is `(source, source_resource_id)`. The accepted YEE-28
input must contain exactly 169,007 identities: Voxel 6,639, Modrinth 158,507,
and Hangar 3,861. `resource_features` contains exactly one row per identity.

The pipeline reads the accepted YEE-28 `resources`, baseline snapshot, sync-run,
and successful Voxel enrichment records. It never writes to the input database.

`analysis_as_of` is explicit in every run. For the accepted dataset, the
deterministic derived value is `2026-09-22T17:13:34Z`, the latest completed
successful accepted run. An explicit CLI value is recorded verbatim after UTC
normalization; wall-clock time is never used implicitly.

## Resource features

The table preserves every baseline `resources` column. Additional columns are:

| Column | Meaning |
| --- | --- |
| `canonical_identity` | Stable `source:source_resource_id` display key. |
| `snapshot_observed_at`, `snapshot_raw_evidence_path` | Baseline snapshot provenance. |
| `enrichment_id` | Accepted Voxel enrichment run ID, otherwise null. |
| `voxel_demand_download_count` | Accepted YEE-28 `getResourceInfo` download count. |
| `voxel_review_count`, `voxel_review_stars` | Voxel source-native review metrics. |
| `voxel_latest_update_id`, `voxel_latest_update_version`, `voxel_latest_update_at` | Voxel source-native latest update fields. A 1970 timestamp without an update ID is treated as unavailable. |
| `voxel_source_metrics_json` | Raw accepted Voxel enrichment metrics JSON. |
| `hangar_recent_downloads`, `hangar_recent_views` | Hangar source-native recent metrics extracted from raw `stats`. |
| `hangar_source_metrics_json` | Raw Hangar source metrics JSON. |
| `modrinth_source_metrics_json` | Raw Modrinth source metrics JSON. |
| `age_days` | Whole elapsed UTC days from `published_at`; invalid/future/missing dates are null. |
| `update_age_days` | Whole elapsed UTC days from baseline `updated_at`. |
| `latest_update_age_days` | Whole elapsed UTC days from valid Voxel `latest_update_at`; null for non-Voxel. |
| `freshness_age_days` | `latest_update_age_days` for Voxel when available, otherwise `update_age_days`. |
| `freshness_cohort` | `<=30d`, `31-90d`, `91-365d`, `>365d`, or `unknown`. |
| `age_cohort` | `<=90d`, `91-365d`, `366-1095d`, `>1095d`, or `unknown`. |
| `paid_state` | `free`, `paid`, or `unknown`; non-Voxel paidness remains unknown. |
| `voxel_price_band` | For Voxel: `free`, `>0-5`, `>5-10`, `>10-20`, `>20-50`, `>50`, or `unknown`; null for other sources. |
| `demand_download_count` | Source-native aggregate demand value used by the common analytical field. |
| `downloads_total` | Common analytical demand field only; it is not sales, revenue, or a cross-source comparable raw count. |
| `demand_metric_source` | Source-native semantic label: `voxel.getResourceInfo.downloads`, `modrinth.project.downloads`, or `hangar.project.stats.downloads`. Null when the source metric is unavailable. |
| `demand_percentile` | Percentile rank within the same source only, 0–100. Null demand produces null percentile. |
| `category_facets_json`, `loader_facets_json`, `version_facets_json` | Sorted, deduplicated, lowercase token-safe facet arrays. Comma-delimited source values are expanded. |
| `project_type_norm` | Lowercase token-safe project type. |
| `feature_schema_version`, `analysis_as_of` | Reproduction metadata. |

Baseline raw `download_count`, `follow_count`, `star_count`, `watcher_count`,
`source_metrics_json`, and all other `resources` fields remain preserved. A
missing metric is null, never zero-filled. A recorded zero remains zero.

## Demand semantics

Voxel demand comes from accepted YEE-28 enrichment observations; Modrinth demand
comes from the accepted baseline `resources.download_count`; Hangar demand comes
from the accepted baseline `resources.download_count` populated from Hangar
`stats.downloads`. The common field is a convenience for within-source
descriptive analysis. Raw counts and their populations are not assumed
comparable between sources.

Percentiles use source-local average ranks: for a non-null value `x`, with `n`
source values, `lower` values strictly below `x`, and `ties` equal to `x`, the
rank is `lower + (ties - 1) / 2`; the percentile is `100 * (rank)/(n - 1)` for
`n > 1`, rounded to six decimal places. A singleton source value is 100.
All equal values receive the same average-rank percentile; nulls are excluded.

## Segment facts

`segment_facts` has one aggregate row per `(lens, source, facet_value)`.
Required lenses are:

`source`, `source_project_type`, `source_category_tag`, `source_loader`,
`source_supported_version`, `voxel_paid_state`, `voxel_price_band`,
`freshness_cohort`, and `age_cohort`.

Facet rows contain resource count, available-demand count, downloads p50/p75/p90
/p95/p99, median age/update/latest-update ages, and p50 source-native secondary
metrics. Multi-valued category, loader, and version facets are membership facts:
one resource can contribute to several rows, so those facet totals are not
mutually exclusive market totals.

## Source distributions

`source_distributions` contains source-local demand rows with p50/p75/p90/p95
/p99, non-null count, total, and top-1%/top-10% share of that source's non-null
demand total. Voxel also has price distribution rows for all resources and each
`paid_state`. These are descriptive distributions only; no winner, score, rank,
or recommendation is produced.

## Export/null rules

JSONL encodes missing values as JSON `null`. CSV uses `\\N` as the missing-value
token. All exports are UTF-8, LF-terminated, and sorted by stable schema keys.


