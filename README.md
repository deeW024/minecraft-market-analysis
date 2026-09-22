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

