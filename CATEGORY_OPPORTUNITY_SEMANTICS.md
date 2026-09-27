# YEE-76 Category Opportunity Semantics

Version: `yee-76-category-opportunity-state-rules-v0.1`.

## Category state precedence

Evaluate each frozen taxonomy category independently, in this order:

1. `INSUFFICIENT_EVIDENCE` when the sum of source-native confirmed-member counts is zero, or every frozen baseline direction in the category has `candidate_state=INSUFFICIENT_EVIDENCE`.
2. `PROMISING` when at least one direction in the category is `ADVANCE_TO_STAGE_D`.
3. `MIXED_OPPORTUNITY` when no direction advances and at least one is `WATCH_COVERAGE_LIMITED`.
4. `LOW_OPPORTUNITY` when at least two baseline directions are interpretable and every interpretable baseline is evidence-against-whitespace. A baseline is evidence-against only when its candidate state is not insufficient, it has at least one non-insufficient source pattern, and every non-insufficient source pattern is `LOW_DEMAND_PATTERN` or `DEMAND_WITH_BROAD_SUPPLY`.
5. `NO_CLEAR_OPPORTUNITY` otherwise.

Lexical-direction absence is not negative evidence. Lexical directions can support `PROMISING` or `MIXED_OPPORTUNITY`, but cannot satisfy the baseline-only `LOW_OPPORTUNITY` condition. An insufficient source pattern is excluded from a baseline's interpretable patterns, never treated as a negative.

## Direction evidence and display tiers

Stage C states map without reinterpretation: `ADVANCE_TO_STAGE_D` → `LEAD_DIRECTION`; `WATCH_COVERAGE_LIMITED` → `WATCH_DIRECTION`; `NO_CLEAR_PATTERN` → `CONTEXT_DIRECTION`; `INSUFFICIENT_EVIDENCE` → `INSUFFICIENT_DIRECTION`. All 47 frozen directions appear once. Display is unranked: tier grouping is presentational only; within each tier use category taxonomy order, then baseline before lexical direction, then canonical direction key and ID.

Only the exact Stage C ADVANCE/WATCH candidate set appears in `category_research_options`, as unranked research options. This is not a shortlist or recommendation.

## Source-aware evidence and risk

Hangar and Voxel member counts, observed direction metrics, pattern states, and supply-inference risks remain source-native. Counts are not summed across marketplaces as comparable demand. Voxel paid-state and price-band availability is carried from accepted source facts; Hangar paid evidence remains explicitly unavailable where the source contract says so. Unknown/unavailable values remain JSON `null` and CSV `\N`; missing price is not interpreted as free.

Risk flags describe upstream coverage or evidence limitations and do not affect category-state precedence. `LEXICAL_ONLY_LEAD_SUPPORT` identifies a promising category whose lead evidence comes only from lexical directions. `UNCATEGORIZED_BUCKET` is retained as a structural warning, not hidden or reassigned.

No YEE-76 external market validation, model inference, category reassignment, product scoring, ranking, winner selection, or Stage E work is performed.
