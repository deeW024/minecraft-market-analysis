# YEE-73 signal semantics

## Scope and inherited facts

The layer describes observed confirmed YEE-61 plugin listings only, not the complete plausible market. REVIEW rows are never category-imputed. Accepted YEE-29 `analysis_as_of`, demand fields, freshness, paid state, price bands, and source-native engagement are inherited unchanged. Missing values remain null; zero remains zero.

## Percentiles and quantiles

New percentiles use non-null `downloads_total` within confirmed YEE-61 rows. Groups are: source; source + primary category; and source + primary category + primary subcategory. Average rank is `lower + (ties - 1)/2`; percentile is `100 * rank/(n-1)`, rounded to six decimals; singleton is 100; null has no percentile. Group `n` counts non-null demand observations. Demand sample-size bands are based on that same available-observation count: `ZERO`, `SINGLETON`, `N_2_4`, `N_5_19`, `N_20_PLUS`. Numeric p-quantiles reuse the YEE-29 linear interpolation helper.

Demand availability rates use primary member count as denominator. Shares at or above confirmed-source percentile 75/90 use demand-available members as denominator. Raw `downloads_total` quantiles, inherited YEE-29 `demand_percentile` quantiles, and the new confirmed-source percentile quantiles are emitted only within a single source/category/subcategory fact row. They are not summed or directly compared across sources.

## Supply and coverage

Primary category counts are mutually exclusive observed confirmed membership counts. Secondary category counts are unique identities whose accepted secondary category list includes the category; the any-membership count deduplicates primary-or-secondary identities. Shares use the source's confirmed YEE-61 count. Cross-source overview counts are named `observed_confirmed_primary_count_non_dedup` and are listing counts, not a deduplicated market. Subcategory supply uses primary subcategory membership only; secondary subcategories are not inferred.

`source_scope_coverage` reports all YEE-61 source identities partitioned as confirmed/review/out-of-scope, plus Stage B member count and demand/freshness coverage among confirmed. Voxel paid-evidence coverage counts only `paid` and `free` states; Hangar paid/price evidence is `NOT_AVAILABLE_FOR_SOURCE`, represented as null rather than free/zero. Voxel counts do not describe hidden REVIEW identities or true market supply.

## Freshness and engagement

Freshness buckets are `<=30d`, `31-90d`, `91-365d`, `>365d`, and `unknown`, counted over primary members. Available rate uses member count; bucket shares include unknown and use member count. Empty groups have zero bucket counts, null shares and null distributions.

Hangar recent downloads, recent views, stars, and watchers remain Hangar-native p50/p90 values with per-metric available count/rate. Voxel review count and review stars remain Voxel-native p50/p90 values with their own availability. Non-native metrics are null, never mapped into another source's fields; no unified engagement score is created.

## Paidness and price

Voxel paid state counts partition Voxel category/subcategory members into `paid`, `free`, and `unknown`. Paid evidence observed count/rate counts `paid + free`; `paid_share_among_observed` is `paid/(paid+free)` and is null when no paid-state evidence exists. Price is available only when both inherited numeric `price_amount` and `currency` are present. Price quantiles are independently grouped by currency and include available observation count. Inherited price bands are counted separately. For Hangar, paid-state/price distributions are null and the scope is `NOT_AVAILABLE_FOR_SOURCE`; absence is not free and price zero is never inferred.
