# YEE-75 observed whitespace semantics

Version: `yee-75-observed-whitespace-semantics-v0.1`.

All states describe *observed confirmed supply* from accepted YEE-73 rows, not total market supply or a true market gap. Demand uses only each member's inherited `source_category_demand_percentile`, calculated within source/category in YEE-73. No raw Hangar/Voxel download values are summed or directly compared.

## Source-local supply and demand

`observed_supply_share_within_category_source` = direction/source confirmed members ÷ accepted YEE-73 primary category/source confirmed members. With a zero denominator the share is null and source evidence is `INSUFFICIENT`. Supply bands, evaluated in order:

- `ZERO`: zero direction members.
- `SPARSE`: 1–5 members and share ≤ 0.20.
- `LIMITED`: not SPARSE, at most 10 members and share ≤ 0.35.
- `BROAD`: otherwise.

Demand availability is the non-null count of inherited source-category percentiles. `demand_strength`:

- `INSUFFICIENT`: fewer than 2 available values.
- `HIGH`: p50 ≥ 65 and share at/above percentile 75 ≥ 0.40.
- `MODERATE`: p50 ≥ 55 or share at/above 75 ≥ 0.33.
- `LOW`: p50 < 45 and share at/above 75 < 0.25.
- `MIXED`: otherwise.

Shares at/above 75/90 use available demand observations as denominator. Freshness availability uses direction members as denominator; `freshness_share_le_90d` and `freshness_share_le_365d` use all direction members, with unknown ages counted separately and not treated as stale. Raw `downloads_total` p50/p90 are source-local descriptive evidence only.

## Observed source-local pattern states

Apply in order:

1. `INSUFFICIENT_EVIDENCE` if members < 2, demand-available count < 2, or category/source confirmed denominator is zero.
2. `OBSERVED_STRONG_PATTERN` if members ≥ 3, demand is HIGH, and observed supply is SPARSE.
3. `OBSERVED_SUPPORTED_PATTERN` if members ≥ 2, demand is HIGH or MODERATE, and observed supply is SPARSE or LIMITED.
4. `DEMAND_WITH_BROAD_SUPPLY` if demand is HIGH and supply is BROAD.
5. `LOW_DEMAND_PATTERN` if demand is LOW.
6. `MIXED_PATTERN` otherwise.

Freshness, source-native engagement, Voxel paidness, and prices are evidence/risk dimensions only; they do not change these states.

## Coverage risk and candidate-state logic

Coverage risk is based only on accepted YEE-73 `yee61_confirmed_fraction`: `<0.05 VERY_HIGH`, `<0.20 HIGH`, `<0.40 MATERIAL`, otherwise `LOWER`. Pinned-input expectations: Voxel `VERY_HIGH`, Hangar `MATERIAL`. This is uncertainty in inferring supply, not a quality judgment.

`ADVANCE_TO_STAGE_D` if either (A) a source has `OBSERVED_STRONG_PATTERN` and its risk is not `VERY_HIGH`, or (B) both sources are `OBSERVED_STRONG_PATTERN`/`OBSERVED_SUPPORTED_PATTERN` and there are at least three distinct source identities. A Voxel-only strong pattern cannot advance through A because Voxel risk is `VERY_HIGH`.

If not ADVANCE, `WATCH_COVERAGE_LIMITED` if any source has a strong/supported state, or `DEMAND_WITH_BROAD_SUPPLY` with at least 3 members. If both source states are `INSUFFICIENT_EVIDENCE`, use `INSUFFICIENT_EVIDENCE`; otherwise use `NO_CLEAR_PATTERN`. There is no quota and no ranking. Zero positive candidates in any category is valid.

The word “observed” is mandatory. These outputs are evidence for later review only; they are not opportunity states, market-gap claims, scores, recommendations, or completed Stage D analyses.
