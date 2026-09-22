# YEE-31 reasoning harness

This change prepares the strict, evidence-only JEV runner. Its provider boundary accepts an injected transport and preserves the exact response bytes before parsing. It intentionally does not guess a JEV endpoint, credential name, or undocumented request format.

## Input and contract

`market_analysis.jev_triage.load_eligible_candidates()` opens the accepted YEE-30 `retrieval.db` with SQLite `mode=ro` and `query_only=ON`, verifies the caller-supplied manifest SHA-256, checks the YEE-30 schema and expected counts (169,007 corpus identities, 8,095 topic-source facts, 5,563 candidates, 3,036 eligible packs), reconciles eligible candidate/pack keys, and verifies the input hash again after loading. Each provider payload contains only the candidate record, its source facts, and its evidence pack.

The system prompt and strict JSON Schema are `JEV_TRIAGE_PROMPT.md` and `JEV_TRIAGE_SCHEMA.json`. Claims, including rationale, must cite an identity in that evidence pack or a named field in its topic-source facts. Unknowns and inferences are explicit; no scalar opportunity score is part of the schema.

## Runner guarantees

- Provider-neutral `Reasoner` protocol; production provider is `JEVProviderAdapter` with an authorized JEV transport injected by the runtime.
- Cache identity includes canonical candidate input hash, prompt/schema hash, provider, model identifier/version, and inference parameters.
- Run metadata is immutable. Candidate targets and raw attempts are stored in SQLite.
- Exact response bytes are committed to `candidate_attempts` before strict UTF-8/JSON parsing or normalization. A saved-but-unparsed response is revalidated on resume before another provider call.
- The deterministic 120-pack pilot is stratified by candidate class, source-presence pattern, support band, and source-local demand band. `run_full()` refuses to start unless every pilot gate is explicitly PASS and the target count reconciles to 3,036.
- The fixed 60-pack stability subset uses stable topic-key hashes. Its three repeated calls have explicit replicate cache identities; each raw response and each decision remains separately stored without majority-vote replacement.
- Bounded retry, cached completed outcomes, explicit failed/pending rows, and deterministic topic-key-ordered JSONL/CSV decision partitions are supported.
- Fixture providers are test-only. Unit tests do not make network or model calls.

## Current execution boundary

The current environment exposed no JEV connector, authorized transport, or JEV credentials. Therefore the fixture suite validates the harness only; no pilot, stability calls, candidate decisions, or substitute-model results have been generated. YEE-31 is blocked before its 120-candidate pilot until the supervisor provides JEV access/transport.
