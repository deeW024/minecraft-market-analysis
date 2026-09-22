# minecraft-market-analysis

This repository contains deterministic BBB Market analytical work-order
pipelines. The YEE-29 feature layer is retained as the baseline; YEE-30 adds a
read-only lexical topic/candidate and local retrieval layer.

## YEE-30 build

Run from the repository root with the accepted YEE-29 `analysis.db`:

```powershell
python -m market_analysis.topic_cli build `
  --input-db F:\path\to\YEE-29\analysis.db `
  --output-dir outputs\yee-30 `
  --replay-output-dir outputs\yee-30-replay `
  --code-version <YEE-30 commit SHA>
```

The build refuses non-canonical source counts, opens the input in SQLite
read-only mode, writes `retrieval.db`, deterministic exports, QA and a SHA-256
manifest. See [TOPIC_SCHEMA.md](TOPIC_SCHEMA.md) for normalization, support
gates, candidate classes, evidence sampling and retrieval details.

No marketplace API calls, cross-market identity merge, embeddings, JEV/LLM,
scoring, ranking or recommendation are part of YEE-30.

