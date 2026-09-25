# YEE-60 PLUGIN_SCOPE_SCHEMA v0.1

## Canonical input

The only resource universe is accepted YEE-29 `resource_features`, with exactly
169,007 identities: Voxel 6,639; Modrinth 158,507; Hangar 3,861. The accepted
`analysis.db` SHA-256 is
`f0712951d416b52260f524a3218abd8a54a087904d6b2553631f461f010f182e`; the
accepted `resource_features.jsonl` SHA-256 is
`c0de8eda1a030b3485f65903aaa065542e8d1b8e04946d3bb51c54a8d4ceccc9`. Both are
opened/processed read-only and checked before and after the build.

## `plugin_eligibility`

Exactly one row per canonical YEE-29 identity, sorted by `(source,
source_resource_id)`. The source identity fields are preserved. Every row has
exactly one state:

- `PLUGIN_ELIGIBLE`: at least one exact source-native plugin class or
  server/proxy platform fact, with no explicit incompatible class/platform fact.
- `NON_PLUGIN`: explicit incompatible product-class evidence or only explicit
  Fabric/Forge/NeoForge/Quilt platform evidence, with no positive plugin fact.
- `AMBIGUOUS`: metadata does not prove a plugin class, or positive and
  incompatible facts conflict.

| Field | Type | Meaning |
|---|---|---|
| `source` | string | YEE-29 source key |
| `source_resource_id` | string | Source-native identity |
| `canonical_identity` | string | YEE-29 canonical identity |
| `plugin_eligibility` | enum | `PLUGIN_ELIGIBLE`, `NON_PLUGIN`, or `AMBIGUOUS` |
| `eligibility_reason_codes` | JSON array | Deterministic reason enums tied to the source-native facts |
| `positive_evidence` | JSON array | Exact source-native field/token facts supporting plugin class |
| `conflicting_evidence` | JSON array | Exact incompatible source-native facts when a positive fact conflicts |
| `project_type_norm` | string/null | Preserved YEE-29 source-native normalized project type |
| `loader_facets_json` | JSON text/null | Preserved YEE-29 source-native normalized loader facets |
| `category_facets_json` | JSON text/null | Preserved YEE-29 source-native normalized category facets |
| `classifier_version` | string | `yee-60-source-native-plugin-classifier-v0.1` |

The exact positive platform allowlist is Bukkit, Spigot, Paper, Purpur, Folia,
Sponge, Bungee/BungeeCord, Waterfall, and Velocity. Exact `plugin` class tokens
are positive. Exact Mod/Modpack/Shader/Resourcepack/Datapack class tokens and
Fabric/Forge/NeoForge/Quilt platform tokens are incompatible evidence. A
positive/incompatible collision is always `AMBIGUOUS`. No other observed token
is promoted. Broad functional categories (for example `gameplay`, `economy`, or
`admin_tools`), a broad server project type, title, summary, author, demand,
price, popularity, or historical downstream analysis do not prove plugin class.

## `plugin_only_resource_features`

Contains exactly the YEE-29 feature rows whose identity is `PLUGIN_ELIGIBLE`.
Every original YEE-29 feature column/value is preserved. `NON_PLUGIN` and
`AMBIGUOUS` are excluded. The source feature schema is copied from the canonical
`resource_features` table without enriching or rewriting fields.

## Exports and QA

`plugin_eligibility.jsonl/csv` contain all 169,007 classifications.
`plugin_only_resource_features.jsonl/csv` contain only eligible rows. JSONL is
UTF-8, LF terminated, canonical-key serialized; CSV is UTF-8/LF and encodes null
as `\N`. Both datasets sort by `(source, source_resource_id)`.

`plugin_taxonomy_audit.json/md` accounts for each observed source, field, token,
count, and a deterministic representative canonical identity.
`plugin_eligibility.sqlite` contains `plugin_eligibility`,
`plugin_only_resource_features`, `plugin_taxonomy_audit`, and `metadata`.
`QA_RESULT.json` reconciles canonical inputs, eligibility evidence, membership,
exports, SQLite integrity, input immutability, and byte-identical deterministic
replay. `DATASET_MANIFEST.json` records each other artifact's size and SHA-256;
its own hash is omitted to avoid a recursive value.

YEE-60 ends at the plugin-only resource universe. It does not create topics,
families, rankings, shortlists, external research, or product concepts.
