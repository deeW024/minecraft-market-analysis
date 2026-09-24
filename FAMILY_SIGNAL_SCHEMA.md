# YEE-43 Family Opportunity Signal Layer v0

Schema version: `yee-43-family-signal-layer-v0.1`

Fixed analysis time: `2026-09-22T17:13:34Z`
Quantiles: deterministic linear interpolation at `(n - 1) * p`, rounded to six decimals using the YEE-29 helper.

## Canonical inputs

The builder accepts only the pinned YEE-37 `concept_retrieval.sqlite`, YEE-30 `retrieval.db`, and YEE-29 `analysis.db` hashes in the source. All three are opened read-only and their SHA-256 values are checked before and after the build. YEE-37 family/topic records define the full family universe; YEE-30 `resource_topics` defines membership; YEE-29 `resource_features` supplies source-aware metrics. Each membership must resolve to both YEE-30 `resource_corpus` and YEE-29 `resource_features` using `(source, source_resource_id, canonical_identity)`.

The accepted universe is 1,618 families and 1,807 topic keys. A resource is unique within a family/source by `(family_id, source, canonical_identity)`. If one resource matches multiple topic aliases in one family, it remains one membership and `matched_topic_keys` preserves all matches. The same resource can be present in multiple families.

## Output tables and exports

Each table is written to `family_signal_analysis.sqlite`, with a UTF-8 JSONL and CSV export. JSONL preserves missing values as `null`; CSV uses `\\N` for null. JSON arrays in CSV are compact canonical JSON. Rows are ordered deterministically by their primary key; JSON object keys are canonicalized.

- `family_features` — exactly one row for every YEE-37 family. Preserves `family_id`, `canonical_topic_key`, `family_status`, `member_count`, `member_topic_keys`, `aliases`, and `candidate_classes`. Adds ordered `source_presence`, source-presence booleans/count, total unique source-qualified memberships, the unique evidence example identity count from the accepted YEE-37 evidence pack, coverage share, signal-source count, Voxel paid-evidence boolean, and `single_source` / `two_source` / `three_source` presence class. Per-source wide metrics use `voxel_`, `modrinth_`, or `hangar_` prefixes. This table deliberately has no score, rank, tier, winner, shortlist, or recommendation field.
- `family_source_signals` — one row per `(family_id, source)` with at least one membership. `resource_count` and metric availability counts are explicit. `demand_percentile_p50/p75/p90/p95` and `ge90/ge95` counts/shares summarize YEE-29's source-local percentile only. `freshness_age_days` p50/p75/p90 plus le30/le90/gt365 counts/shares and `age_days_p50` retain their YEE-29 meanings. `downloads_total` p50/p75/p90 remain source-native and are never summed across sources. Top-1/top-3 shares and HHI use non-null downloads within only that family/source; concentration is null if the usable total is not positive.
- `family_resource_memberships` — deduplicated source-qualified identities and their matched topic keys, plus canonical family topic and status.
- `family_voxel_price_signals` — one row per family/currency for paid Voxel resources only. Currency groups are never combined. Price percentiles/min/max use non-null prices; `paid_resource_count` and `price_available_count` distinguish record presence from metric availability.

Voxel signals preserve `voxel_review_count` and `voxel_review_stars` p50/p90. Modrinth preserves `follow_count` p50/p90. Hangar preserves `star_count`, `watcher_count`, `hangar_recent_downloads`, and `hangar_recent_views` p50/p90. Unsupported source/metric combinations are null, not zero. Recorded numeric zero remains zero.

Voxel `paid_state` is summarized as free, paid, or unknown-state count; `paid_share_known` is paid divided by known free+paid resources, or null when there are none. Non-Voxel paid state is not evidence of free or paid supply.

## Deliverables and reproducibility

`family_signal_analysis.sqlite` has explicit indexes for family ID, source, canonical identity, and canonical topic key. `QA_RESULT.json` records input immutability, universe/membership reconciliation, source-presence and source-local metric checks, SQLite integrity/foreign-key checks, and clean rebuild byte equality. `DATASET_MANIFEST.json` lists byte sizes and SHA-256 digests for every deliverable except itself. `FINAL_REPORT.md` summarizes the accepted run.

Run from the repository root with:

```text
python -m market_analysis.family_signals_cli --yee37-db PATH --yee30-db PATH --yee29-db PATH --output-dir NEW_OR_EMPTY_DIRECTORY
```
