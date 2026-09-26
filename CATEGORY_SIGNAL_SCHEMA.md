# YEE-73 Category Signal Layer v0 schema

Schema version: `yee-73-category-signal-layer-v0.1`.

## Canonical input and identity

The read-only input is the accepted YEE-61 `category_first_foundation.sqlite`, SHA-256 `12fa10a45a98df954c220f741a21ae8c8ed537a953415954e71fdab23bdd17ed`. The output member identity is `(source, source_resource_id)` and exactly matches the 1,352 `PLUGIN_PRODUCT_CONFIRMED` YEE-61 memberships. `PLUGIN_PRODUCT_REVIEW` and `OUT_OF_SCOPE_PRODUCT_FORM` remain outside signal computation.

## Tables and exports

| Table / export stem | Grain | Required production rows |
| --- | --- | ---: |
| `source_scope_coverage` | source | 2 |
| `category_signal_member_features` | confirmed identity | 1,352 |
| `category_source_facts` | frozen category × source | 22 |
| `subcategory_source_facts` | frozen subcategory × source | 76 |
| `category_signal_overview` | frozen category | 11 |

Every JSONL record preserves all accepted YEE-61 membership fields and all inherited source-feature fields, then adds the Stage B member fields: `primary_category_order`, `subcategory_order`, the three source-local confirmed-membership demand percentiles, their non-null group sizes, category/subcategory demand sample-size bands, and `signal_schema_version`. List/dictionary membership values are retained as JSON values.

Each source fact carries frozen taxonomy order/identity, observed confirmed supply, source-local demand distributions, freshness distributions, applicable source-native engagement, Voxel-only paid/price evidence, source-scope coverage context, and input/taxonomy/schema provenance. Category demand/freshness/engagement distributions use primary category members. Secondary category membership is reported as contextual supply overlap only. Subcategory facts use primary category/subcategory assignments; no secondary subcategory is inferred.

`category_signal_layer.sqlite` contains `run_metadata`, `source_scope_coverage`, `category_signal_member_features`, `category_source_facts`, `subcategory_source_facts`, `category_signal_overview`, and `frozen_taxonomy_snapshot`. Data tables retain scalar columns and a canonical `record_json` for exact export reconciliation.

## Nulls, ordering, and units

JSONL uses JSON `null`; CSV uses `\\N`. All files are UTF-8 with LF line endings. Rows use source then identity, taxonomy order then source, category/subcategory order then source, and frozen taxonomy order respectively. CSV columns and JSON object keys are sorted deterministically. Missing metrics are null, recorded zero remains zero. Source-native download counts and engagement remain separate by source; there is no combined demand percentile or raw cross-source download sum. The overview's explicitly named `observed_confirmed_primary_count_non_dedup` is a listing-membership sum, not unique market supply.

## Prohibited outputs

This layer contains no opportunity state, whitespace result, candidate direction, candidate/resource shortlist, rank, winner, attractiveness score, external research, commercial validation, concept, or build recommendation. All 11 categories, including `server_utilities` and `uncategorized`, are frozen and retained.
