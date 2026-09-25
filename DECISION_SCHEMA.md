# YEE-59 Decision Schema v0.1

## Canonical inputs

All inputs are read-only and must match the pinned SHA-256 values recorded in DATASET_MANIFEST.json.

- YEE-57 concept_synthesis.sqlite, opportunity_concept_cards.jsonl, and concept_readiness_matrix.jsonl.
- YEE-55 deep_commercial_validation.sqlite and market_alternatives.jsonl; only retained alternatives referenced by an accepted YEE-57 card may influence paid-market proximity.

The exact card universe is deep-validation orders 1, 2, 3, 4, 5, 8, 10, and 11. All original YEE-57 card/readiness fields are copied without modification.

## Decision dimensions

| Dimension | Source/output fields | Ordinal comparison |
|---|---|---|
| Demand prior | consensus_rank, demand_prior_rank | Lower inherited rank is stronger. |
| Buyer/job evidence | buyer_job_status, buyer_job_strength | EVIDENCED=2; LIMITED_SAMPLE=1; other=0. |
| Pain evidence | pain_status, pain_evidence_strength | REPEATED_SAMPLE=2; LIMITED_SAMPLE=1; other=0. |
| Paid-market proximity | paid_market_proximity, paid_market_proximity_strength, paid_market_evidence | Verified paid DIRECT/SUBSTITUTE=2; verified paid ADJACENT-only=1; none=0. |
| Payer/buyer signal | payer_role, payer_or_buyer_signal, payer_or_buyer_signal_strength | SIGNAL_PRESENT=1 only when payer_role lacks the token UNKNOWN (case-insensitive); otherwise UNKNOWN=0. |
| Feasibility evidence | feasibility_status, feasibility_evidence_strength | COMPLETE=2; PARTIAL=1; other=0. |

The per-dimension strengths are ordinal comparison values only. They must never be summed, averaged, weighted, normalized, or exposed as an overall score. differentiation_count and uncertainty_flags are descriptive and do not participate in dominance.

## Pareto policy

A row dominates another iff it is no worse on every dimension (rank lower-is-stronger; all other strengths higher-is-stronger) and strictly better on at least one. pareto_state is PARETO_FRONTIER or DOMINATED_EVIDENCE_PROFILE. all_dominator_orders contains every dominator sorted by deep-validation order; primary_dominance_witness is its smallest order or null for frontier rows.

Dominance describes only the declared evidence profile. It is not a product rejection, quality judgment, ranking within the frontier, or build recommendation.

## Outputs

- concept_decision_matrix: one row per accepted card with original YEE-57 fields and the six dimensions.
- pareto_dominance: one row per card with frontier state and complete dominance provenance.
- supervisor_decision_set: the matrix rows whose state is PARETO_FRONTIER, in inherited deep-validation order.

JSONL is canonical UTF-8 with one compact JSON object per line. CSV uses \N for null and canonical JSON for structured values. SQLite mirrors the three exports and stores deterministic metadata. DECISION_BRIEF.md, QA_RESULT.json, and DATASET_MANIFEST.json document the output and QA.

No composite score, weights, manual selection, market research, recommendation, pricing, TAM, revenue, or profit field is part of this schema.
