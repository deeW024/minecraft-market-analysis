# YEE-30 TOPIC_SCHEMA v0.1

## Purpose and boundary

YEE-30 transforms the accepted YEE-29 `analysis.db` into a deterministic
lexical research index:

`resource corpus -> topic_source_facts -> candidate_topics -> evidence packs -> retrieval DB/index`

The input is opened with SQLite `mode=ro` and is never changed. Canonical
identity remains `(source, source_resource_id)`; this layer does not merge
resources across markets.

The accepted input guardrail is exactly 169,007 identities: Voxel 6,639,
Modrinth 158,507 and Hangar 3,861. The fixed `analysis_as_of`, analysis run ID,
YEE-29 feature schema version, input SHA-256 and YEE-30 code version are stored
in the retrieval DB metadata and production manifest.

## Versioned normalization

`NORMALIZATION_VERSION=yee-30-topic-normalization-v2` and
`STOPWORD_VERSION=yee-30-stopwords-v1` are versioned with the code. The
normalizer applies Unicode NFKC, case folding, whitespace normalization and
punctuation boundaries, then stable tokenization. It does not stem words.
Before punctuation tokenization, ASCII and typographic apostrophes are
canonicalized and possessive `'s` clitics are removed, so `Farmer's` and
`Farmer’s` normalize identically to `farmer` without emitting an `s` token.
Numeric-only and version-only tokens are removed from title topic phrases.
The explicit stopword/domain-stopword file is `src/market_analysis/topic_stopwords.txt`.

Topic mining uses only deduplicated contiguous title n-grams of lengths 1, 2
and 3 after normalization. Summary and normalized category/loader/version
facets are corpus and retrieval context; they do not independently create
topic keys. Each resource retains its original display fields alongside the
normalized corpus text.

## Relational outputs

### `resource_corpus`

One row per accepted canonical identity. It contains source identity, original
title/summary/project type, normalized title tokens, normalized facet text,
source URL, source-relative demand percentile, source-native demand metric label,
freshness/age, Voxel paid/price/review fields, Modrinth follows and Hangar
stars/watchers/recent downloads/recent views.

### `resource_topics`

Deduplicated membership rows `(topic_key, source, canonical_identity)`. Repeated
phrase occurrences in one resource contribute one document-support row.

### `topic_source_facts`

One row per retained `(topic_key, source)` after the source gates:

| Source | Minimum distinct resources |
| --- | ---: |
| Voxel | 2 |
| Hangar | 3 |
| Modrinth | 10 |

Facts contain resource count, demand-available count, source-local demand
percentile p50/p75/p90, count/share at percentile >=90, freshness p50 and
count/share with age <=90 days. Raw `downloads_total` is summarized only within
the source and is never summed between sources.

Voxel facts additionally contain free/paid/unknown counts, paid-resource count,
paid price p50/p75/p90, review-count and review-star distributions. Modrinth
facts preserve follow-count distributions. Hangar facts preserve star, watcher,
recent-download and recent-view distributions. Missing values remain null;
recorded zero values remain zero.

### `candidate_topics`

One row per normalized topic that has retained cross-market source facts:

- `overlap`: retained Voxel support and retained Modrinth and/or Hangar support;
- `free_demand_only`: no retained Voxel support, but retained Modrinth and/or
  Hangar support.

`free_demand_only` is research-eligible only when at least one Modrinth/Hangar
fact has at least three resources at source-local demand percentile >=90.
`overlap` is research-eligible by the descriptive cross-market presence gate.
Eligibility reasons and source presence are persisted explicitly. There is no
scalar opportunity score, ranking, recommendation or winner field.

### `evidence_packs` and `evidence_examples`

There is one pack for every research-eligible candidate. Each source contributes
at most 15 unique resources: up to five highest source-local demand percentiles,
up to five freshest resources, and up to five deterministic representative
resources, with source-resource-ID tie-breaking. The pack preserves source
identity, original text and URL, source-relative demand evidence, freshness,
facets and source-native engagement/paid fields.

## Retrieval

`retrieval.db` contains the relational tables above and an FTS5 index over
resource title, summary and normalized facet text when the runtime supports
FTS5. Otherwise it contains a deterministic `(token, canonical_identity,
source)` inverted index plus an `__all__` identity marker. Exact topic lookup
and evidence lookup use relational keys; resource text retrieval uses the FTS
or fallback index. No external search service is used.

## Export and null rules

JSONL exports are UTF-8, LF-terminated and sorted by stable schema keys. CSV
exports are UTF-8, LF-terminated and use `\\N` for null. JSONL missing values
are JSON `null`. Production artifacts include `resource_corpus.jsonl`, topic
facts JSONL/CSV, candidate JSONL/CSV, evidence-pack JSONL and evidence-example
JSONL/CSV, plus QA and the SHA-256 manifest.

## QA

The build fails closed on input identity/source-count mismatch. QA reconciles
corpus and index identity counts, unique topic/candidate keys, every fact's
distinct resource support, evidence identity membership and the per-source
15-example limit. The complete build is replayed from the same input and code
configuration; retrieval DB and all deterministic exports must be byte-identical.

