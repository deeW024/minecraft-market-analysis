# YEE-79 Category Direction Decision Gate v0

Build the read-only Stage F decision dossier from the accepted, hash-pinned YEE-77 SQLite bundle. The builder does not perform network research and must stop before the Supervisor/User decision or Stage G.

```powershell
python -m market_analysis.category_decision_gate_cli `
  --input-db <accepted-YEE-77/category_targeted_external_research.sqlite> `
  --output-dir <new-empty-output-folder> `
  --pull-request-url <open-YEE-79-PR-URL> `
  --verification-json <repository-verification.json>
```

The output directory must be new or empty. Input pins are constants in `src/market_analysis/category_decision_gate.py`; a mismatch fails closed. The input is opened in SQLite read-only/query-only mode and its SHA-256 is checked before and after processing. The generated manifest lists the size and SHA-256 of every other required artifact; the QA report records the SQLite digest, which cannot be self-stored inside that same database without creating a hash cycle.

Artifacts are UTF-8/LF. JSONL uses canonical JSON and JSON null. CSV has no BOM, uses LF, canonicalizes nested fields, and serializes null as `\N`. Stable presentation order is frozen category order, then direction ID; it does not represent attractiveness.

Run tests with `python -m pytest`. Synthetic fixtures cover the decision mapping, provenance, descriptive facts, exports, SQLite, fail-closed pins, and repeatability. Captured YEE-77 market evidence belongs in the YEE-79 Drive artifacts, not the repository.
