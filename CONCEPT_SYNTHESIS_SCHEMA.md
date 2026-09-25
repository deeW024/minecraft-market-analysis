# YEE-57 Concept Synthesis Schema v0.1

Deterministic concept-readiness matrix for all accepted YEE-55 COHORT_A families and evidence-linked candidate concept cards for the authorized pilot families only.
Candidate concepts are hypotheses for review, not recommendations or market conclusions. No score, ranking, price recommendation, TAM, revenue, profit, or prevalence estimate is produced.

## Canonical inputs

- `commercial_validation_packs.jsonl` SHA-256 `cfa996476ccfcf92f7c2ed03420ce0cf66265f528c16dccb2aec1c463778007e` (read-only)
- `deep_commercial_validation.sqlite` SHA-256 `e82fa072e3e75a2b3deef7fa88fdc1f958928b75916708ffbf4e2a181260d6c1` (read-only)
- `differentiation_hypotheses.jsonl` SHA-256 `ee099fdcbcf3fe8d637a7bc5f4a767dc05957f4462c4b267bd3081fa447230c9` (read-only)
- `market_alternatives.jsonl` SHA-256 `8c29ae2bb050852734506453d42e70962fe81501c5272255c4fc2c3f3c15d429` (read-only)
- `pain_clusters.jsonl` SHA-256 `60b40aa06474184046657f23cf0be3e297fdbb75823c9a3673afb62e89d9e1a5` (read-only)
- `validation_evidence.jsonl` SHA-256 `1319a9b6d2996ba89509978dcec737b301ad4c19d8cc3bd793dd3197e9f905a2` (read-only)
- `validation_queries.jsonl` SHA-256 `d499c3aef99376e6bfe6f58c3a042721b3bd4649e27bd17d762e844b92fd4aff` (read-only)

## Deterministic readiness

CONCEPT_READY iff buyer_job_status ∈ {EVIDENCED, LIMITED_SAMPLE}; pain_status ∈ {REPEATED_SAMPLE, LIMITED_SAMPLE}; retained_alternative_count > 0 OR differentiation_count > 0; and feasibility_status ∈ {COMPLETE, PARTIAL}.
Otherwise readiness_reasons contains every applicable exact enum: BUYER_JOB_NOT_ESTABLISHED, PAIN_NOT_ESTABLISHED, COMPARATIVE_CONTEXT_NOT_ESTABLISHED, FEASIBILITY_NOT_ESTABLISHED.
Commercial signal states are VERIFIED_PAID_SIGNAL, COMPARATIVE_SIGNAL_ONLY, and NO_RETAINED_COMPARATIVE_SIGNAL. They are evidence states, not scores.

## Tables

### concept_readiness_matrix

| Field | Meaning |
|---|---|
| `family_id` | Accepted YEE-55 family identity; primary key. |
| `deep_validation_order` | Inherited YEE-55 order; deterministic output order. |
| `consensus_rank` | Inherited rank, not recomputed or used to rank this output. |
| `canonical_topic_key` | Inherited canonical topic. |
| `validation_cohort` | Inherited cohort; expected COHORT_A. |
| `buyer_job_status` | Copied YEE-55 dimension_statuses.buyer_job. |
| `pain_status` | Copied YEE-55 dimension_statuses.pain_prevalence_sample. |
| `feasibility_status` | Copied YEE-55 feasibility_evidence_status. |
| `retained_alternative_count` | Count of same-family YEE-55 market_alternatives rows. |
| `differentiation_count` | Count of same-family YEE-55 differentiation_hypotheses rows. |
| `commercial_signal_state` | Deterministic evidence-state over same-family alternatives; missing price is not free. |
| `concept_readiness` | CONCEPT_READY or NEEDS_EVIDENCE from the fixed gate. |
| `readiness_reasons` | Ordered array of exact reasons; empty for CONCEPT_READY. |

### opportunity_concept_cards

| Field | Meaning |
|---|---|
| `concept_id` | Deterministic ID derived from family_id. |
| `family_id`, `deep_validation_order`, `consensus_rank`, `canonical_topic_key` | Inherited YEE-55 identity fields. |
| `concept_readiness` | Derived state; cards exist only for CONCEPT_READY. |
| `target_user_role`, `payer_role` | Exact copies of YEE-55 primary_user_role and buyer_or_payer_role. |
| `buyer_job_basis` | Exact YEE-55 buyer_job_hypotheses. |
| `pain_basis` | Exact family pain status/summary and same-family YEE-55 pain clusters. |
| `commercial_signal_state` | Derived evidence state; never a recommended business model or price. |
| `retained_alternative_ids`, `differentiation_basis` | Same-family YEE-55 comparative records. |
| `feasibility_status`, `feasibility_basis` | Exact YEE-55 feasibility status and facts. |
| `concept_statement`, `solution_primitives`, `synthesis_notes` | Interpretive fields sourced only from CONCEPT_CAPTURE. |
| `uncertainty_flags`, `next_validation_questions` | Deterministically derived flags/prompts; not new findings. |
| `evidence_refs` | Sorted same-family YEE-55 evidence/cluster/alternative/hypothesis IDs. |

## Capture and reference semantics

CONCEPT_CAPTURE version `yee-57-concept-capture-v0.1` contains exactly the authorized pilot families and only family_id plus concept_statement, solution_primitives, and synthesis_notes. Derived fields are never manually authored.
Solution primitive basis_type is PAIN_RESPONSE, DIFFERENTIATION_DIRECTION, or JOB_ENABLEMENT. Every basis_refs ID must resolve within the same family; PAIN_RESPONSE cites pain evidence/cluster, DIFFERENTIATION_DIRECTION cites its hypothesis and evidence, and JOB_ENABLEMENT cites buyer/job evidence.
Pain status and cluster sample statuses are preserved verbatim. LIMITED_SAMPLE stays limited; UNKNOWN and missing remain unknown/missing. CSV nulls use \N; JSON/SQLite preserve null values.

## Pilot authorization

Readiness matrix: all 15 rows. Concept cards: exactly deep_validation_order 1..5. Orders 8, 10, and 11 are not authorized in this pilot.
