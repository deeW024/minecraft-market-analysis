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


