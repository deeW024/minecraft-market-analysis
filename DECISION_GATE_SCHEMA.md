# YEE-78 Decision Gate Schema

Schema version: yee-78-supervisor-user-decision-gate-v0.1

## Authority and ordering

The sole production analytical input is the accepted YEE-77
category_targeted_external_research.sqlite, pinned by SHA-256 in the run
manifest. It is opened read-only. The direction identity set, accepted category
order, research status/coverage, and semantic relations are immutable.

Order decision cards by frozen category order, then ascending direction_id.
Order category context by the frozen 11-row taxonomy order. Order relations by
relation_id, question catalog by the listed question order, and mapped
questions by direction order then catalog order. JSONL is UTF-8, canonical
JSON, one row per LF-terminated line. CSV uses the same row order and column
order; JSON null is represented as \N, and nested objects/arrays are
canonical-JSON cells.

## Decision direction card

Each of exactly 16 cards contains these preserved YEE-77 fields:

direction_id, category_id, category_opportunity_state, direction_type,
canonical_direction_key, candidate_state, direction_tier,
positive_support_shape, risk_flags, reason_codes, research_status,
research_coverage_status, resolved_market_job.

The YEE-78-only fields are:

- decision_readiness: READY_FOR_SUPERVISOR_DECISION,
  REQUIRES_SCOPE_REFINEMENT, or REQUIRES_MORE_EVIDENCE.
- evidence_profile: accepted evidence/query counts, distinct source domains,
  primary/marketplace and community evidence counts, and the evidence/source IDs.
- competition_context: source-backed direct/substitute/adjacent identities,
  feature themes/evidence, maintenance references, and clearly labeled
  unvalidated hypotheses. No saturation grade is permitted.
- operator_pain_context: only accepted OPERATOR_PAIN observations, with
  source IDs and prevalence_inferred=false.
- monetization_evidence_context: OBSERVED or NOT_OBSERVED; currencies stay
  separate. Missing price is not free/zero.
- popularity_proxy_context: source-native counters retain their native unit
  and evidence; Stage D source-local demand/supply facts remain separated by
  source. No cross-source aggregation or market-size inference.
- maintenance_context: accepted summary/evidence and source-backed
  competitor maintenance states.
- overlap_context: relation IDs and full member context; no identity merge.
- uncertainty_context: accepted risks, caveats, ambiguity, coverage and query
  timestamp provenance. The accepted YEE-77 category_research_coverage.coverage_notes
  scalar string is carried as one complete note in category_coverage_notes;
  it is never iterated into individual characters.
- validation_gap_codes, validation_question_ids, allowed_human_actions.
- evidence_ids, source_ids, competitor_ids, semantic_relation_ids.

Readiness is derived only from accepted research status and coverage:

| YEE-77 status | YEE-77 coverage | Decision readiness |
| --- | --- | --- |
| RESOLVED | SUFFICIENT | READY_FOR_SUPERVISOR_DECISION |
| AMBIGUOUS | any accepted matching coverage | REQUIRES_SCOPE_REFINEMENT |
| UNRESOLVED | any accepted matching coverage | REQUIRES_MORE_EVIDENCE |
| RESOLVED | PARTIAL | REQUIRES_MORE_EVIDENCE |

An ambiguous direction cannot be promoted to ready. Readiness is not
attractiveness, ranking, or recommendation.

## Category decision context

Exactly 11 rows preserve category_id, frozen order/name,
category_opportunity_state, Stage E category_research_status, option IDs,
readiness and research status counts, overlap-group IDs, and coverage
caveats. A scalar coverage_notes value is represented as a one-element list
containing the unchanged full string (an empty or null value remains an empty
list). A category with no Stage E options remains visible. Where the
accepted YEE-77 SQLite contains no category opportunity state for a
zero-option category, the value is JSON null with an explicit unavailable-state
basis; no prohibited upstream side input is used to reconstruct it.

## Overlap groups

Preserve each accepted semantic relation once with relation_id,
relation_type, category_id, the exact member direction IDs/keys, rationale,
evidence IDs, and a double-counting warning. Current relation type is
SUBSTANTIAL_OVERLAP for both accepted groups. Related directions remain
separate cards and are not independent market observations.

## Validation questions and gaps

The frozen catalog contains Q_PAIN_PREVALENCE, Q_BUYER_SEGMENT,
Q_WILLINGNESS_TO_PAY, Q_PAID_ALTERNATIVES, Q_DIFFERENTIATION,
Q_PURCHASE_TRIGGER, Q_SUPPORT_BURDEN, and Q_CHANNEL_FIT. Each mapped question
is marked FUTURE_QUESTION_ONLY_NOT_ANSWERED; YEE-78 does not answer it or add
market facts.

Allowed gap codes:

PAIN_PREVALENCE_UNVALIDATED, BUYER_WILLINGNESS_TO_PAY_UNVALIDATED,
PRICING_EVIDENCE_NOT_OBSERVED, DIFFERENTIATION_NEEDS_DEEP_VALIDATION,
BUYER_SEGMENT_NEEDS_DEEP_VALIDATION, PURCHASE_TRIGGER_NEEDS_DEEP_VALIDATION,
SUPPORT_MAINTENANCE_BURDEN_UNVALIDATED, OVERLAP_REQUIRES_DECISION_CONTEXT,
SCOPE_REFINEMENT_REQUIRED, SOURCE_COVERAGE_RISK_PRESENT.

## Human decision template

SUPERVISOR_DECISION_TEMPLATE.json starts UNDECIDED; selection,
scope-refinement, hold, and drop arrays are empty; build-none is false;
rationale and decision time are null. Zero, one, or multiple human selections
and build-none are valid. The worker does not populate a choice.

## Database and artifacts

decision_gate.sqlite stores run/input provenance; frozen category/direction,
source, evidence, query, competitor, and relation snapshots; normalized
decision cards, category context, overlap groups, validation question catalog
and mappings, and the empty human template. Foreign keys must pass,
PRAGMA integrity_check must be ok, and PRAGMA foreign_key_check must
return no rows.

Required tables include frozen_category_snapshot,
frozen_direction_snapshot, decision_direction_cards,
category_decision_context, decision_overlap_groups,
validation_question_catalog, decision_validation_questions, and
supervisor_decision_template.

No global rank, score, winner, shortlist, recommendation, or default human
selection field is part of this schema. No external research or API calls are
part of production generation.
