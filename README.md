# minecraft-market-analysis

YEE-29 deterministic analytical feature layer for the BBB Market project.

The pipeline consumes the accepted YEE-28 SQLite database read-only and produces:

`resource_features` → `segment_facts` → `source_distributions`

It makes no network calls and does not perform matching, enrichment, embeddings,
JEV/LLM analysis, scoring, ranking, or product selection.

## Run

```text
python -m market_analysis.cli build \
  --input-db <accepted YEE-28 production.db> \
  --output-dir <new output directory> \
  --analysis-as-of 2026-09-22T17:13:34Z \
  --code-version <analysis commit SHA>
```

The input is opened with SQLite `mode=ro` and `PRAGMA query_only=ON`. The command
fails closed unless the accepted source and enrichment counts match the YEE-29
baseline. Output files are ordered by stable keys and include a manifest with
byte sizes and SHA-256 digests.

See [FEATURE_SCHEMA.md](FEATURE_SCHEMA.md) for field definitions, null semantics,
cohort boundaries, percentile tie handling, and facet counting rules.

## YEE-30 topic layer

YEE-30 adds a deterministic read-only transformation of the accepted YEE-29
analysis database:

`resource corpus` → `topic_source_facts` → `candidate_topics` → `evidence packs` → `retrieval.db`

Run `python -m market_analysis.topic_cli build` with `--input-db`,
`--output-dir`, `--replay-output-dir`, and the YEE-30 commit SHA. See
[TOPIC_SCHEMA.md](TOPIC_SCHEMA.md) for normalization, support gates, candidate
classes, evidence sampling and retrieval details.

YEE-30 does not make marketplace API calls or perform cross-market identity
merge, embeddings, semantic clustering, JEV/LLM analysis, scoring, ranking,
recommendation or YEE-31 work.

## YEE-31 JEV triage harness

The YEE-31 Jev-native `state + Choice/Score/Noul questions` transport,
deterministic derived output, credential handling and fixture tests are
documented in [YEE31_HARNESS.md](YEE31_HARNESS.md). No model-generated prose or
free-form JSON is part of the production path. Live inference remains gated on
an authorized runtime credential and an explicit pilot decision.

## YEE-37 concept-pair pilot

YEE-37 loads only the accepted 1,807-topic YEE-31 ADVANCE review set and opens
the accepted YEE-30 retrieval DB read-only. It builds a deterministic,
versioned candidate-pair universe locally, applies the full corpus sentinel
set, and uses local lexical metrics only for blocking and stratification.
Fuzzy similarity never selects a merge.

The Jev pair contract is in [JEV_PAIR_QUESTIONS.json](JEV_PAIR_QUESTIONS.json)
and uses pair_state/questions/policy/output v0.2. Each topic sends at most
eight deterministic source-balanced semantic examples; all 1,190 serialized
pair states are preflighted at <=16 KiB before any model call. The native
`merge_disposition` Choice drives the deterministic policy: MERGE requires
SAME_CONCEPT + MERGE, KEEP_SEPARATE requires a distinct relation +
KEEP_SEPARATE, and insufficient or contradictory answers map to REVIEW.
`merge_safe` remains diagnostic and is never thresholded. Direct TypeSafe
requests use the accepted YEE-31 typesafe-sdk==0.7.1 transport, requested
alias jev-latest, and fail-closed expected model jev-1.13.0.

The offline correction preflight compares v0.2 pair and selection artifacts
byte-for-byte to the accepted v0.1 evidence and makes no Jev/network calls;
HTTP 400 `max_tokens_exceeded` is terminal for its cache identity.

Run the first gate with a fresh output directory and runtime-injected
JEV_TRANSPORT=typesafe and TYPESAFE_API_KEY:

    python -m market_analysis.concept_pairs_cli
      --advance-set <accepted-ADVANCE_REVIEW_SET_UNRANKED.jsonl>
      --input-db <accepted-YEE-30-retrieval.db>
      --output-dir <new-YEE-37-pilot-directory>

The command builds and byte-checks the full candidate-pair file before any
inference, then runs exactly 120 pilot pairs and 40 pairs with three fresh
stability replicates. It does not adjudicate the remaining pairs or build
concept families. After a pilot, `--finalize-only` recalculates pilot/stability
QA and exports from existing SQLite outcomes without inference dispatch. It
still validates the configured TypeSafe runtime and initializes the pinned
SDK client, so it requires `JEV_TRANSPORT=typesafe` and `TYPESAFE_API_KEY` and
is not a credential-free offline mode. Use the same input/output paths and
persisted run state; production family finalization is a separate workflow.
