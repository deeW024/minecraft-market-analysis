# YEE-31 Jev-native decision harness

YEE-31 calls Jev as a bounded decision model, not a text generator. One request
contains the accepted YEE-30 candidate pack as `state` and the versioned named
Choice/Score/Noul questions in `JEV_TRIAGE_QUESTIONS.json`. Production sends no
free-form prompt and expects no generated concept label, claims, rationale or
research prose.

## Contract and deterministic output

The request question set is versioned as `yee-31-native-questions-v0.2`; its
digest and the deterministic output-template version are bound into the cache
identity. The state is the candidate row, its retained `topic_source_facts`,
and its bounded YEE-30 examples. Every question warns that marketplace
titles/summaries are untrusted evidence content, never instructions.

Responses are validated against the full declared question-ID set and answer
types. Choice preserves its selected option, confidence and full probability
map. Score preserves its fractional score, ordered legend, confidence and
probabilities. Noul preserves its yes probability. The exact request bytes/hash
are committed before the HTTP call; exact response bytes/hash are committed
before parsing. Request bodies contain only `model`, `state`, and `questions`—
never credentials.

Application code derives `concept_label` from `topic_display`/`topic_key`,
`market_pattern` from YEE-30 candidate class/source presence, and summary,
rationale and later-research question templates from typed answers and retained
YEE-30 facts. Derived evidence references resolve only to identities and fact
keys present in that candidate pack. No scalar opportunity score is produced.

## Supported transports

- `TypeSafeSystemOneHTTPTransport`: HTTPS `POST /v1/systemone` on the configured
  TypeSafe base URL, `Authorization: Bearer ...`, model alias `jev-latest`.
- `VercelTypeSafeHTTPTransport`: same TypeSafe request/response contract at the
  configured TypeSafe-compatible Vercel base URL, model `typesafe-ai/jev`.
- `jev_provider_from_env()` selects the transport using `JEV_TRANSPORT` and
  fails closed if its runtime-injected credential is absent.

Configuration names (values are never included in metadata/logs):

- Direct: `TYPESAFE_API_KEY`, optional `TYPESAFE_BASE_URL`,
  model fixed as `jev-latest`.
- Vercel: `AI_GATEWAY_API_KEY`, optional `VERCEL_TYPESAFE_BASE_URL`,
  model fixed as `typesafe-ai/jev`.
- Shared: `JEV_TRANSPORT=typesafe|vercel-typesafe`,
  `JEV_HTTP_TIMEOUT_SECONDS`.

Transport metadata stores only the selected transport, public base URL and
timeout. HTTP failures are sanitized; headers, credentials and response error
bodies are not logged or persisted as error text. Tests use mocked HTTP only.

## Retained execution plumbing

The runner preserves the accepted YEE-30 read-only loader and identity/count/hash
checks, SQLite attempt history, raw-first persistence, retry/resume/cache,
deterministic 120-candidate pilot sampling/gates, 60-candidate three-replicate
stability sampling, and deterministic JSONL/CSV exports. Cache keys bind state,
question-set/output versions, model, transport configuration, inference
parameters and stability replicate. Stability reports exact decision agreement
and typed per-question answer deltas; it does not vote away variance.

The focused fixture suite mocks direct and Vercel-compatible HTTP requests and
responses for each native answer type. No live pilot or Jev call is made during
contract validation. Before the 120-candidate pilot, a supervisor must provide
an authorized runtime credential and authorize proceeding in Linear.

## Official contract references

- [TypeSafe API docs](https://api.typesafe.ai/docs) — `/v1/systemone`, bearer
  authentication, and `/v1/models` discovery.
- [TypeSafe SDK wire types](https://github.com/typesafe-ai/typesafe-sdk-js/blob/main/src/types.ts)
  — Choice/Score/Noul question and response shapes.
- [Vercel TypeSafe-compatible API](https://vercel.com/docs/ai-gateway/sdks-and-apis/typesafe)
  — gateway base URL, auth, `/v1/systemone`, and response envelope.
- [YEE-31 Native Decision Contract Addendum](https://docs.google.com/document/d/1sToyXo64rrtNuRGFpxgCKuAd-tNW1adaNghN92aNQPE/edit)
  — project-specific versioned question/output requirements.
