# YEE-143 demand-first commercial signal reset

The accepted YEE-61/YEE-73 universe is scanned before research. Native paid flags and prices are precedent, downloads/reviews are non-commercial context, and unavailable paid data remains unknown. Fresh positive server/proxy-plugin proof is mandatory for local REVIEW recovery; accepted memberships and all upstream files remain unchanged.

`research/yee143_capture.json` is the reviewed, dated factual capture. It contains canonical URLs, source locators, counter semantics, identity/form proof, licensing, contradictory evidence, correlated buyer channels and all retained research batches. Search snippets never support claims. Historical intent and paid offers remain separate from completed payments. Native OUT_OF_SCOPE products cannot return through marketplace aliases.

Run with Python 3.12 in this repository (the accepted baseline has unrelated Python 3.12-only syntax):

```powershell
python src/market_analysis/demand_first_reset.py --foundation YEE61.sqlite --signals YEE73.sqlite --features YEE29.db --output stage_a
python src/market_analysis/demand_first_reset.py --stage-a stage_a --capture research/yee143_capture.json --output normalized
python src/market_analysis/demand_first_reset.py --stage-a stage_a --capture research/yee143_capture.json --output replay
python -m pytest tests/test_demand_first_reset.py -q
```

Use the accepted source files and pinned SHA-256 values in the module. Output directories must be fresh. Compare all normalized and replay file hashes, including SQLite. The code performs no live web requests: a replay proves normalization, not continuing truth of public pages.

The Drive dossier supplies the upstream manifest audit, frozen Stage A, all normalized JSONL/SQLite, source ledger, evidence, profiles, free context, QA, deterministic replay and exact test results. Reports are generated from those same rows. All pool advancement fields remain null. No product concepts, recommendations for pricing/building, weighted scores or market-size estimates are produced.

Known baseline failure: `test_full_captured_cohort_has_no_unsupported_drop_or_product_decision_fields` reads `YEE95_GOAL_ALIGNMENT.md`, absent from accepted baseline `19fbfac376d836951c52db15b8e0a0d694937c58`. The YEE-143 branch does not supply or change that unrelated document.

Stop: `DEMAND_FIRST_COMMERCIAL_SIGNAL_RESET_READY_FOR_SUPERVISOR_REVIEW`.
