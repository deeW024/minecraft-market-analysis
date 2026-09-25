# YEE-54 Evidence-Gated Opportunity Synthesis v0

Schema version: `yee-54-evidence-gated-opportunity-synthesis-v0.1`.

## Canonical identity and inherited fields

There is exactly one output row per accepted YEE-46 `ADVANCE_RESEARCH` family,
joined to YEE-47 by `family_id` and checked against the inherited
`consensus_rank`. Every YEE-46 shortlist field is copied verbatim. The YEE-47
`research_status` and `research_coverage_status` values are also preserved
verbatim. No upstream score or rank is recalculated.

## Derived fields

| Field | Definition |
| --- | --- |
| `synthesis_state` | `RESOLVED` → `DEEP_VALIDATE`; `AMBIGUOUS` → `NEEDS_DISAMBIGUATION`; `UNRESOLVED` → `STOP_UNRESOLVED`. |
| `evidence_count_by_claim_type` | Counts from accepted YEE-47 `external_evidence` rows for the family. |
| `evidence_count_by_source_type` | Counts from accepted YEE-47 `external_evidence` rows for the family. |
| `distinct_source_domain_count` | Number of distinct non-empty `source_domain` values in those evidence rows. |
| `current_competitor_entity_count` | Number of accepted YEE-47 `competitor_entities` rows for the family. |
| `competitor_count_by_relation_type` | Counts from those competitor rows grouped by `relation_type`. |
| `research_query_count` | Number of accepted YEE-47 `research_queries` rows for the family. |
| `evidence_gap_flags` | Ordered evidence-only flags: `NO_PAIN_POINT_EVIDENCE`, `NO_PRICING_EVIDENCE`, `NO_CURRENT_COMPETITOR_ENTITY`, `NO_DIRECT_COMPETITOR`, `NO_MAINTENANCE_EVIDENCE`, `NO_POPULARITY_PROXY`, `SINGLE_SOURCE_DOMAIN`, `COVERAGE_PARTIAL`. |
| `deep_validation_order` | Existing YEE-46 rank order among `DEEP_VALIDATE` families only; null otherwise. |
| `validation_cohort` | `COHORT_A` for deep order 1–15, `COHORT_B` for 16–30, `COHORT_C` for 31–41; null otherwise. |
| `next_validation_requirements` | Common validation dimensions followed by gap-driven research tasks, for `DEEP_VALIDATE` rows only; empty otherwise. |

The queue contains only `DEEP_VALIDATE` rows in inherited `consensus_rank`
order. `next_validation_cohort` is exactly the first 15 queue rows. Cohorts are
workload batches, not a market verdict.

Every `DEEP_VALIDATE` row receives the common requirements, in order:
`BUYER_JOB`, `PAIN_PREVALENCE`, `PAID_ALTERNATIVES`, `DIFFERENTIATION`,
`FEASIBILITY_SUPPORT`. Gap tasks append in the matching flag order:
`PAIN_POINT_RESEARCH`, `PRICING_MONETIZATION_RESEARCH`,
`COMPETITOR_EXPANSION`, `DIRECT_COMPETITOR_CHECK`, `LIFECYCLE_VALIDATION`,
`POPULARITY_VALIDATION`, `SECOND_SOURCE_VALIDATION`. These are research tasks,
not market conclusions. `COVERAGE_PARTIAL` adds no separate task.

## Null and missing-evidence semantics

JSON `null` remains null; CSV uses the explicit token `\N`; structured values
are canonical JSON in CSV/SQLite text columns. Missing evidence means
**UNKNOWN / NOT CAPTURED IN YEE-47**. It is not evidence of no market, no
competitor, no pain, free pricing, or lack of monetization.

## Persistence and deterministic exports

`opportunity_synthesis.sqlite` contains `opportunity_synthesis`,
`deep_validation_queue`, and `next_validation_cohort` with the same ordered
row schema and family foreign keys, plus deterministic `metadata`. JSONL,
CSV, SQLite and this generated schema inventory reconcile to the same columns
and row counts. The production schema artifact appends the exact inherited and
derived column inventory from the accepted YEE-46 input.
