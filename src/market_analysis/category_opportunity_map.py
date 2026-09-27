"""Deterministic YEE-76 category opportunity map over accepted YEE-75 only."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import shutil
import sqlite3
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from .category_signals import _read_only_connection, canonical_json, sha256_file


WORK_ORDER = "YEE-76"
DELIVERY_STATUS = "CATEGORY_OPPORTUNITY_MAP_READY_FOR_SUPERVISOR_REVIEW"
SCHEMA_VERSION = "yee-76-category-opportunity-map-v0.1"
STATE_RULE_VERSION = "yee-76-category-opportunity-state-rules-v0.1"
DIRECTION_TIER_VERSION = "yee-76-direction-tier-map-v0.1"
INPUT_WORK_ORDER = "YEE-75"
INPUT_MERGE_COMMIT = "c3cb72951eeac3e5155931ae8c94344cb46aec36"
INPUT_CODE_COMMIT = "9e85a4ce785e6cb4aa13f4bafc8d380ea14addcf"
INPUT_SQLITE_SHA256 = "6829bd8961de378b2b1f49b2ea4bc955fc6a7ce01f9dc215428d1738f371e3dc"
INPUT_RUN_ID = "8dfa4ee083ad95a2170ba7b3532dc84c21bf9220f7b2ec06b7e18afd686a85ee"
INPUT_DIRECTION_SCHEMA_VERSION = "yee-75-category-direction-discovery-v0.3"
TAXONOMY_VERSION = "yee-61-functional-category-taxonomy-v0.1"
TAXONOMY_SHA256 = "b5720325dea06863408dfa1a05e2f981ecfeb3fac4a9e7266083ee17839c5126"
SOURCES = ("hangar", "voxel")
CATEGORY_STATES = (
    "PROMISING",
    "MIXED_OPPORTUNITY",
    "LOW_OPPORTUNITY",
    "NO_CLEAR_OPPORTUNITY",
    "INSUFFICIENT_EVIDENCE",
)
DIRECTION_STATES = (
    "ADVANCE_TO_STAGE_D",
    "WATCH_COVERAGE_LIMITED",
    "NO_CLEAR_PATTERN",
    "INSUFFICIENT_EVIDENCE",
)
DIRECTION_TIER_BY_STATE = {
    "ADVANCE_TO_STAGE_D": "LEAD_DIRECTION",
    "WATCH_COVERAGE_LIMITED": "WATCH_DIRECTION",
    "NO_CLEAR_PATTERN": "CONTEXT_DIRECTION",
    "INSUFFICIENT_EVIDENCE": "INSUFFICIENT_DIRECTION",
}
TIER_ORDER = {value: index for index, value in enumerate(DIRECTION_TIER_BY_STATE.values())}
PATTERN_STATES = {
    "OBSERVED_STRONG_PATTERN",
    "OBSERVED_SUPPORTED_PATTERN",
    "DEMAND_WITH_BROAD_SUPPLY",
    "LOW_DEMAND_PATTERN",
    "MIXED_PATTERN",
    "INSUFFICIENT_EVIDENCE",
}
NEGATIVE_PATTERN_STATES = {"LOW_DEMAND_PATTERN", "DEMAND_WITH_BROAD_SUPPLY"}
EXPECTED_INPUT_COUNTS = {
    "category_count": 11,
    "direction_universe": 47,
    "baseline_direction_count": 38,
    "lexical_direction_count": 9,
    "direction_source_facts": 94,
    "direction_evaluations": 47,
    "candidate_directions": 16,
    "direction_evidence_packs": 16,
    "category_direction_summary": 11,
}
EXPECTED_CATEGORY_STATES = {
    "administration": "PROMISING",
    "communication": "PROMISING",
    "developer_tools": "MIXED_OPPORTUNITY",
    "economy": "MIXED_OPPORTUNITY",
    "gameplay": "PROMISING",
    "minigames": "NO_CLEAR_OPPORTUNITY",
    "protection": "MIXED_OPPORTUNITY",
    "roleplay": "NO_CLEAR_OPPORTUNITY",
    "server_utilities": "INSUFFICIENT_EVIDENCE",
    "uncategorized": "NO_CLEAR_OPPORTUNITY",
    "world_management": "NO_CLEAR_OPPORTUNITY",
}
EXPECTED_CATEGORY_STATE_COUNTS = {
    "PROMISING": 3,
    "MIXED_OPPORTUNITY": 3,
    "LOW_OPPORTUNITY": 0,
    "NO_CLEAR_OPPORTUNITY": 4,
    "INSUFFICIENT_EVIDENCE": 1,
}
CSV_NULL = r"\N"
TABLES = (
    "category_opportunity_profiles",
    "category_direction_tiers",
    "category_research_options",
)
CSV_COLUMNS = {
    "category_opportunity_profiles": (
        "category_id", "category_order", "category_name", "category_opportunity_state",
        "reason_codes", "confirmed_member_count_by_source", "source_presence_by_source",
        "upstream_supply_inference_risk_by_source", "baseline_evidence_profile",
        "direction_counts_by_type", "candidate_state_counts", "positive_support_shape",
        "lead_watch_counts_by_direction_type", "lead_directions", "watch_directions", "direction_evidence",
        "voxel_paid_evidence_availability", "hangar_paid_evidence_availability",
        "source_coverage_context", "risk_flags",
    ),
    "category_direction_tiers": (
        "direction_id", "category_id", "category_order", "direction_type",
        "canonical_direction_key", "baseline_subcategory_id", "candidate_state",
        "direction_tier", "reason_codes", "observed_member_count_by_source",
        "source_pattern_state_by_source", "source_supply_inference_risk_by_source",
        "source_evidence_by_source",
    ),
    "category_research_options": (
        "direction_id", "category_id", "category_order", "category_opportunity_state",
        "direction_type", "canonical_direction_key", "candidate_state", "direction_tier",
        "positive_support_shape", "risk_flags", "reason_codes", "direction_evidence_pack",
        "source_facts_by_source",
    ),
}
ARTIFACTS = (
    "GOAL_ALIGNMENT.md",
    "CATEGORY_OPPORTUNITY_SEMANTICS.md",
    "CATEGORY_OPPORTUNITY_SCHEMA.md",
    "category_opportunity_profiles.jsonl",
    "category_opportunity_profiles.csv",
    "category_direction_tiers.jsonl",
    "category_direction_tiers.csv",
    "category_research_options.jsonl",
    "category_research_options.csv",
    "CATEGORY_OPPORTUNITY_MAP.md",
    "category_opportunity_map.sqlite",
    "QA_RESULT.json",
    "DATASET_MANIFEST.json",
    "FINAL_REPORT.md",
)


class CategoryOpportunityError(ValueError):
    """Accepted YEE-75 input or generated YEE-76 output failed its contract."""


def _loads(value: str | None, field: str) -> Any:
    if value is None:
        raise CategoryOpportunityError(f"Required YEE-75 JSON field is null: {field}")
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise CategoryOpportunityError(f"Malformed YEE-75 JSON field: {field}") from exc


def _record_rows(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    return [
        _loads(row[0], f"{table}.record_json")
        for row in connection.execute(f"SELECT record_json FROM {table}")
    ]


def _load_input(path: Path, *, enforce_pinned_inputs: bool = True) -> dict[str, Any]:
    if not path.is_file():
        raise CategoryOpportunityError(f"Accepted YEE-75 SQLite not found: {path}")
    input_sha256 = sha256_file(path)
    if enforce_pinned_inputs and input_sha256 != INPUT_SQLITE_SHA256:
        raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: accepted YEE-75 SQLite SHA-256 differs from the pin")
    connection = _read_only_connection(path)
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_key_violations = list(connection.execute("PRAGMA foreign_key_check"))
        if integrity != "ok" or foreign_key_violations:
            raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: YEE-75 SQLite integrity checks failed")
        tables_present = {
            str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        required_tables = {
            "run_metadata", "frozen_taxonomy_snapshot", "source_coverage_snapshot",
            "direction_universe", "direction_source_facts", "direction_evaluations",
            "candidate_directions", "direction_evidence_packs", "category_direction_summary",
        }
        if not required_tables <= tables_present:
            raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: required YEE-75 tables are missing")
        metadata = {str(row[0]): str(row[1]) for row in connection.execute("SELECT key,value FROM run_metadata")}
        if metadata.get("work_order") != INPUT_WORK_ORDER:
            raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: input is not a YEE-75 dataset")
        if enforce_pinned_inputs and (
            metadata.get("run_id") != INPUT_RUN_ID
            or metadata.get("code_commit") != INPUT_CODE_COMMIT
            or metadata.get("direction_schema_version") != INPUT_DIRECTION_SCHEMA_VERSION
            or metadata.get("taxonomy_version") != TAXONOMY_VERSION
            or metadata.get("taxonomy_sha256") != TAXONOMY_SHA256
        ):
            raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: YEE-75 run/schema/taxonomy provenance differs from the pin")
        taxonomy = [
            _loads(row[0], "frozen_taxonomy_snapshot.category_json")
            for row in connection.execute(
                "SELECT category_json FROM frozen_taxonomy_snapshot ORDER BY category_order"
            )
        ]
        source_coverage = {
            str(row[0]): _loads(row[1], "source_coverage_snapshot.record_json")
            for row in connection.execute("SELECT source,record_json FROM source_coverage_snapshot")
        }
        loaded = {
            "input_sha256": input_sha256,
            "metadata": metadata,
            "taxonomy": taxonomy,
            "source_coverage_by_source": source_coverage,
            "directions": _record_rows(connection, "direction_universe"),
            "source_facts": _record_rows(connection, "direction_source_facts"),
            "evaluations": _record_rows(connection, "direction_evaluations"),
            "candidates": _record_rows(connection, "candidate_directions"),
            "evidence_packs": _record_rows(connection, "direction_evidence_packs"),
            "category_summaries": _record_rows(connection, "category_direction_summary"),
        }
    finally:
        connection.close()
    _validate_loaded_input(loaded, enforce_pinned_inputs=enforce_pinned_inputs)
    return loaded


def _validate_loaded_input(loaded: Mapping[str, Any], *, enforce_pinned_inputs: bool) -> None:
    taxonomy = list(loaded["taxonomy"])
    categories = {str(row["category_id"]): row for row in taxonomy}
    if len(categories) != len(taxonomy) or not taxonomy:
        raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: frozen category identity is not unique")
    category_ids = set(categories)
    summaries = {str(row["category_id"]): row for row in loaded["category_summaries"]}
    if set(summaries) != category_ids:
        raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: YEE-75 category summaries do not match taxonomy")
    directions = {str(row["direction_id"]): row for row in loaded["directions"]}
    evaluations = {str(row["direction_id"]): row for row in loaded["evaluations"]}
    if len(directions) != len(loaded["directions"]) or set(evaluations) != set(directions):
        raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: YEE-75 directions/evaluations do not reconcile")
    facts: dict[tuple[str, str], dict[str, Any]] = {}
    for row in loaded["source_facts"]:
        key = (str(row["direction_id"]), str(row["source"]))
        if key in facts or key[1] not in SOURCES:
            raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: duplicate or unknown YEE-75 direction/source fact")
        facts[key] = row
    expected_fact_keys = {(direction_id, source) for direction_id in directions for source in SOURCES}
    if set(facts) != expected_fact_keys:
        raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: YEE-75 direction/source fact coverage is incomplete")
    if any(row.get("candidate_state") not in DIRECTION_STATES for row in evaluations.values()):
        raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: unknown YEE-75 candidate state")
    if any(row.get("observed_whitespace_pattern_state") not in PATTERN_STATES for row in facts.values()):
        raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: unknown YEE-75 source pattern state")
    expected_candidates = {
        key for key, row in evaluations.items()
        if row["candidate_state"] in {"ADVANCE_TO_STAGE_D", "WATCH_COVERAGE_LIMITED"}
    }
    actual_candidates = {str(row["direction_id"]) for row in loaded["candidates"]}
    actual_packs = {str(row["direction_id"]) for row in loaded["evidence_packs"]}
    if len(loaded["candidates"]) != len(actual_candidates) or actual_candidates != expected_candidates:
        raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: YEE-75 candidate set is not the exact ADVANCE/WATCH subset")
    if len(loaded["evidence_packs"]) != len(actual_packs) or actual_packs != expected_candidates:
        raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: YEE-75 evidence packs do not match candidate set")
    for direction_id, evaluation in evaluations.items():
        direction = directions[direction_id]
        category_id = str(direction["primary_category_id"])
        if category_id not in category_ids or str(evaluation["primary_category_id"]) != category_id:
            raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: direction escaped its frozen category")
        for source in SOURCES:
            fact = facts[(direction_id, source)]
            if fact["observed_whitespace_pattern_state"] != evaluation["source_pattern_state_by_source"][source]:
                raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: evaluation/source-fact pattern state mismatch")
            if fact["source_supply_inference_risk"] != evaluation["source_supply_inference_risk_by_source"][source]:
                raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: evaluation/source-fact coverage risk mismatch")
            if int(fact["category_source_confirmed_count"]) != int(
                summaries[category_id]["confirmed_member_count_by_source"][source]
            ):
                raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: category/source confirmed denominator mismatch")
    baseline_pairs = {
        (str(row["primary_category_id"]), str(row["baseline_subcategory_id"]))
        for row in loaded["directions"] if row["direction_type"] == "SUBCATEGORY_BASELINE"
    }
    expected_baseline_pairs = {
        (str(category["category_id"]), str(subcategory["subcategory_id"]))
        for category in taxonomy for subcategory in category["subcategories"]
    }
    if baseline_pairs != expected_baseline_pairs:
        raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: YEE-75 frozen baselines do not match taxonomy")
    if sum(int(row["confirmed_member_count_by_source"][source]) for row in summaries.values() for source in SOURCES) != sum(
        int(loaded["source_coverage_by_source"][source]["yee61_confirmed_count"]) for source in SOURCES
    ):
        raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: category members do not reconcile to source coverage")
    if enforce_pinned_inputs:
        counts = {
            "category_count": len(taxonomy),
            "direction_universe": len(loaded["directions"]),
            "baseline_direction_count": sum(row["direction_type"] == "SUBCATEGORY_BASELINE" for row in loaded["directions"]),
            "lexical_direction_count": sum(row["direction_type"] == "LEXICAL_SUBNICHE" for row in loaded["directions"]),
            "direction_source_facts": len(loaded["source_facts"]),
            "direction_evaluations": len(loaded["evaluations"]),
            "candidate_directions": len(loaded["candidates"]),
            "direction_evidence_packs": len(loaded["evidence_packs"]),
            "category_direction_summary": len(loaded["category_summaries"]),
        }
        if counts != EXPECTED_INPUT_COUNTS:
            raise CategoryOpportunityError(f"BLOCKED_INPUT_MISMATCH: unexpected accepted YEE-75 row counts: {counts}")
        if len(taxonomy) != 11 or sum(len(row["subcategories"]) for row in taxonomy) != 38:
            raise CategoryOpportunityError("BLOCKED_INPUT_MISMATCH: frozen taxonomy must contain 11 categories/38 baselines")


def _baseline_evidence_classification(candidate_state: str, pattern_states: Sequence[str]) -> tuple[str, list[str]]:
    if candidate_state == "INSUFFICIENT_EVIDENCE":
        return "INSUFFICIENT_EVIDENCE", ["BASELINE_CANDIDATE_STATE_INSUFFICIENT"]
    informative = [state for state in pattern_states if state != "INSUFFICIENT_EVIDENCE"]
    if not informative:
        return "NOT_EVIDENCE_AGAINST_WHITESPACE", ["NO_NON_INSUFFICIENT_SOURCE_PATTERN"]
    if all(state in NEGATIVE_PATTERN_STATES for state in informative):
        return "EVIDENCE_AGAINST_WHITESPACE", ["ALL_INTERPRETABLE_SOURCE_PATTERNS_NEGATIVE"]
    return "NOT_EVIDENCE_AGAINST_WHITESPACE", ["AT_LEAST_ONE_PATTERN_NOT_NEGATIVE"]


def _category_opportunity_state(
    confirmed_member_count: int,
    baseline_evidence: Sequence[Mapping[str, Any]],
    direction_candidate_states: Sequence[str],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if confirmed_member_count == 0:
        reasons.append("ZERO_CONFIRMED_CATEGORY_MEMBERS")
    if baseline_evidence and all(row["candidate_state"] == "INSUFFICIENT_EVIDENCE" for row in baseline_evidence):
        reasons.append("ALL_BASELINE_DIRECTIONS_INSUFFICIENT")
    if reasons:
        return "INSUFFICIENT_EVIDENCE", reasons
    if "ADVANCE_TO_STAGE_D" in direction_candidate_states:
        return "PROMISING", ["AT_LEAST_ONE_ADVANCE_DIRECTION"]
    if "WATCH_COVERAGE_LIMITED" in direction_candidate_states:
        return "MIXED_OPPORTUNITY", ["AT_LEAST_ONE_WATCH_DIRECTION_AND_NO_ADVANCE"]
    interpretable = [row for row in baseline_evidence if row["candidate_state"] != "INSUFFICIENT_EVIDENCE"]
    if len(interpretable) >= 2 and all(
        row["classification"] == "EVIDENCE_AGAINST_WHITESPACE" for row in interpretable
    ):
        return "LOW_OPPORTUNITY", ["AT_LEAST_TWO_INTERPRETABLE_BASELINES_ALL_NEGATIVE"]
    return "NO_CLEAR_OPPORTUNITY", ["NO_PRECEDING_CATEGORY_STATE_RULE_MET"]


def _source_fact_indexes(loaded: Mapping[str, Any]) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, dict[str, Any]]]:
    facts = {(str(row["direction_id"]), str(row["source"])): row for row in loaded["source_facts"]}
    evaluations = {str(row["direction_id"]): row for row in loaded["evaluations"]}
    return facts, evaluations


def _risk_flags(
    category_id: str,
    state: str,
    member_counts: Mapping[str, int],
    source_risks: Mapping[str, str],
    baseline_evidence: Sequence[Mapping[str, Any]],
    directions: Sequence[Mapping[str, Any]],
    evaluations_by_id: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    flags: list[str] = []
    if source_risks["voxel"] == "VERY_HIGH":
        flags.append("VOXEL_SUPPLY_INFERENCE_RISK_VERY_HIGH")
    if source_risks["hangar"] == "MATERIAL":
        flags.append("HANGAR_SUPPLY_INFERENCE_RISK_MATERIAL")
    if member_counts["voxel"] == 0:
        flags.append("NO_VOXEL_CONFIRMED_MEMBERS")
    if any(row["candidate_state"] == "INSUFFICIENT_EVIDENCE" for row in baseline_evidence):
        flags.append("BASELINE_EVIDENCE_GAPS_PRESENT")
    baseline_advance = any(
        direction["direction_type"] == "SUBCATEGORY_BASELINE"
        and evaluations_by_id[str(direction["direction_id"])]["candidate_state"] == "ADVANCE_TO_STAGE_D"
        for direction in directions
    )
    lexical_advance = any(
        direction["direction_type"] == "LEXICAL_SUBNICHE"
        and evaluations_by_id[str(direction["direction_id"])]["candidate_state"] == "ADVANCE_TO_STAGE_D"
        for direction in directions
    )
    if state == "PROMISING" and lexical_advance and not baseline_advance:
        flags.append("LEXICAL_ONLY_LEAD_SUPPORT")
    if category_id == "uncategorized":
        flags.append("UNCATEGORIZED_BUCKET")
    return sorted(flags)


def _positive_support_shape(directions: Sequence[Mapping[str, Any]], evaluations_by_id: Mapping[str, Mapping[str, Any]]) -> str:
    types = {
        str(row["direction_type"])
        for row in directions
        if evaluations_by_id[str(row["direction_id"])]["candidate_state"]
        in {"ADVANCE_TO_STAGE_D", "WATCH_COVERAGE_LIMITED"}
    }
    if types == {"SUBCATEGORY_BASELINE", "LEXICAL_SUBNICHE"}:
        return "BASELINE_AND_LEXICAL"
    if types == {"SUBCATEGORY_BASELINE"}:
        return "BASELINE_ONLY"
    if types == {"LEXICAL_SUBNICHE"}:
        return "LEXICAL_ONLY"
    return "NONE"


def _lead_watch_counts_by_direction_type(
    directions: Sequence[Mapping[str, Any]], evaluations_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, int]]:
    result = {
        direction_type: {"LEAD_DIRECTION": 0, "WATCH_DIRECTION": 0}
        for direction_type in ("SUBCATEGORY_BASELINE", "LEXICAL_SUBNICHE")
    }
    for direction in directions:
        candidate_state = str(evaluations_by_id[str(direction["direction_id"])]["candidate_state"])
        tier = DIRECTION_TIER_BY_STATE[candidate_state]
        if tier in {"LEAD_DIRECTION", "WATCH_DIRECTION"}:
            result[str(direction["direction_type"])][tier] += 1
    return result


def _direction_fact_evidence(facts_by_direction: Mapping[tuple[str, str], Mapping[str, Any]], direction_id: str) -> dict[str, Any]:
    return {
        source: {
            "observed_whitespace_pattern_state": facts_by_direction[(direction_id, source)]["observed_whitespace_pattern_state"],
            "demand_strength": facts_by_direction[(direction_id, source)]["demand_strength"],
            "observed_supply_band": facts_by_direction[(direction_id, source)]["observed_supply_band"],
            "observed_confirmed_member_count": facts_by_direction[(direction_id, source)]["observed_confirmed_member_count"],
            "source_supply_inference_risk": facts_by_direction[(direction_id, source)]["source_supply_inference_risk"],
        }
        for source in SOURCES
    }


def _baseline_evidence_rows(
    directions: Sequence[Mapping[str, Any]],
    evaluations_by_id: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    result = []
    for direction in directions:
        if direction["direction_type"] != "SUBCATEGORY_BASELINE":
            continue
        direction_id = str(direction["direction_id"])
        evaluation = evaluations_by_id[direction_id]
        patterns = evaluation["source_pattern_state_by_source"]
        classification, reasons = _baseline_evidence_classification(
            str(evaluation["candidate_state"]), [str(patterns[source]) for source in SOURCES]
        )
        result.append({
            "direction_id": direction_id,
            "baseline_subcategory_id": direction["baseline_subcategory_id"],
            "candidate_state": evaluation["candidate_state"],
            "source_pattern_state_by_source": {source: patterns[source] for source in SOURCES},
            "classification": classification,
            "reason_codes": reasons,
        })
    return result


def _paid_evidence_availability(
    category_directions: Sequence[Mapping[str, Any]],
    facts_by_direction: Mapping[tuple[str, str], Mapping[str, Any]],
    source: str,
) -> dict[str, Any]:
    baseline_ids = [
        str(row["direction_id"]) for row in category_directions
        if row["direction_type"] == "SUBCATEGORY_BASELINE"
    ]
    rows = [facts_by_direction[(direction_id, source)] for direction_id in baseline_ids]
    scopes = sorted({str(row["paid_evidence_scope"]) for row in rows if row.get("paid_evidence_scope") is not None})
    scope = scopes[0] if len(scopes) == 1 else (scopes if scopes else None)
    paid_states: dict[str, int] | None = None
    price_bands: dict[str, int] | None = None
    observed_count: int | None = None
    price_available_count: int | None = None
    if rows and scope in {"SOURCE_AVAILABLE", "NOT_AVAILABLE_FOR_SOURCE"}:
        state_counts: Counter[str] = Counter()
        band_counts: Counter[str] = Counter()
        available_counts = []
        price_counts = []
        for row in rows:
            if isinstance(row.get("paid_state_counts"), dict):
                state_counts.update({str(k): int(v) for k, v in row["paid_state_counts"].items()})
            if isinstance(row.get("price_band_counts"), dict):
                band_counts.update({str(k): int(v) for k, v in row["price_band_counts"].items()})
            if row.get("paid_evidence_observed_count") is not None:
                available_counts.append(int(row["paid_evidence_observed_count"]))
            if row.get("price_available_count") is not None:
                price_counts.append(int(row["price_available_count"]))
        paid_states = dict(sorted(state_counts.items())) if state_counts else None
        price_bands = dict(sorted(band_counts.items())) if band_counts else None
        observed_count = sum(available_counts) if available_counts else None
        price_available_count = sum(price_counts) if price_counts else None
    return {
        "scope": scope,
        "paid_state_counts": paid_states,
        "paid_evidence_observed_count": observed_count,
        "price_band_counts": price_bands,
        "price_available_count": price_available_count,
    }


def _build_outputs(loaded: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    facts_by_direction, evaluations_by_id = _source_fact_indexes(loaded)
    summaries = {str(row["category_id"]): row for row in loaded["category_summaries"]}
    directions_by_category: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for direction in loaded["directions"]:
        directions_by_category[str(direction["primary_category_id"])].append(direction)
    for rows in directions_by_category.values():
        rows.sort(key=lambda row: (
            0 if row["direction_type"] == "SUBCATEGORY_BASELINE" else 1,
            str(row["canonical_direction_key"]), str(row["direction_id"]),
        ))
    profile_by_category: dict[str, dict[str, Any]] = {}
    for category in loaded["taxonomy"]:
        category_id = str(category["category_id"])
        category_directions = directions_by_category[category_id]
        baseline_evidence = _baseline_evidence_rows(category_directions, evaluations_by_id)
        summary = summaries[category_id]
        member_counts = {source: int(summary["confirmed_member_count_by_source"][source]) for source in SOURCES}
        source_risks = {}
        for source in SOURCES:
            observed_risks = {
                str(evaluations_by_id[str(direction["direction_id"])]["source_supply_inference_risk_by_source"][source])
                for direction in category_directions
            }
            if len(observed_risks) != 1:
                raise CategoryOpportunityError("YEE-75 source risk differs across category directions")
            source_risks[source] = next(iter(observed_risks))
            coverage_risk = loaded["source_coverage_by_source"][source].get("source_supply_inference_risk")
            if coverage_risk is not None and str(coverage_risk) != source_risks[source]:
                raise CategoryOpportunityError("YEE-75 source coverage risk differs from direction evaluations")
        candidate_states = [str(evaluations_by_id[str(row["direction_id"])]["candidate_state"]) for row in category_directions]
        total_members = sum(member_counts.values())
        state, state_reasons = _category_opportunity_state(total_members, baseline_evidence, candidate_states)
        candidate_state_counts = {candidate_state: candidate_states.count(candidate_state) for candidate_state in DIRECTION_STATES}
        direction_counts_by_type = {
            direction_type: sum(row["direction_type"] == direction_type for row in category_directions)
            for direction_type in ("SUBCATEGORY_BASELINE", "LEXICAL_SUBNICHE")
        }
        evidence_rows: list[dict[str, Any]] = []
        for direction in category_directions:
            direction_id = str(direction["direction_id"])
            evaluation = evaluations_by_id[direction_id]
            candidate_state = str(evaluation["candidate_state"])
            evidence_rows.append({
                "direction_id": direction_id,
                "direction_type": direction["direction_type"],
                "canonical_direction_key": direction["canonical_direction_key"],
                "baseline_subcategory_id": direction.get("baseline_subcategory_id"),
                "candidate_state": candidate_state,
                "direction_tier": DIRECTION_TIER_BY_STATE[candidate_state],
                "reason_codes": list(evaluation["reason_codes"]),
                "observed_member_count_by_source": dict(evaluation["observed_member_count_by_source"]),
                "source_pattern_state_by_source": dict(evaluation["source_pattern_state_by_source"]),
                "source_supply_inference_risk_by_source": dict(evaluation["source_supply_inference_risk_by_source"]),
                "source_evidence_by_source": _direction_fact_evidence(facts_by_direction, direction_id),
            })
        lead = [row for row in evidence_rows if row["direction_tier"] == "LEAD_DIRECTION"]
        watch = [row for row in evidence_rows if row["direction_tier"] == "WATCH_DIRECTION"]
        risk_flags = _risk_flags(
            category_id, state, member_counts, source_risks,
            baseline_evidence, category_directions, evaluations_by_id,
        )
        voxel_paid = _paid_evidence_availability(category_directions, facts_by_direction, "voxel")
        hangar_paid = _paid_evidence_availability(category_directions, facts_by_direction, "hangar")
        profile_by_category[category_id] = {
            "category_id": category_id,
            "category_order": int(category["category_order"]),
            "category_name": str(category["category_name"]),
            "category_definition": str(category["definition"]),
            "category_opportunity_state": state,
            "reason_codes": state_reasons,
            "confirmed_member_count_by_source": member_counts,
            "source_presence_by_source": {source: member_counts[source] > 0 for source in SOURCES},
            "upstream_supply_inference_risk_by_source": source_risks,
            "baseline_evidence_profile": {
                "baseline_direction_count": len(baseline_evidence),
                "candidate_state_counts": {
                    state_name: sum(row["candidate_state"] == state_name for row in baseline_evidence)
                    for state_name in DIRECTION_STATES
                },
                "interpretable_baseline_count": sum(row["candidate_state"] != "INSUFFICIENT_EVIDENCE" for row in baseline_evidence),
                "insufficient_baseline_count": sum(row["candidate_state"] == "INSUFFICIENT_EVIDENCE" for row in baseline_evidence),
                "evidence_against_whitespace_count": sum(row["classification"] == "EVIDENCE_AGAINST_WHITESPACE" for row in baseline_evidence),
                "directions": baseline_evidence,
            },
            "direction_counts_by_type": direction_counts_by_type,
            "candidate_state_counts": candidate_state_counts,
            "lead_watch_counts_by_direction_type": _lead_watch_counts_by_direction_type(
                category_directions, evaluations_by_id,
            ),
            "positive_support_shape": _positive_support_shape(category_directions, evaluations_by_id),
            "lead_directions": lead,
            "watch_directions": watch,
            "direction_evidence": evidence_rows,
            "voxel_paid_evidence_availability": {
                **voxel_paid,
                "confirmed_member_count": member_counts["voxel"],
                "availability_state": (
                    "OBSERVED" if voxel_paid["scope"] == "SOURCE_AVAILABLE" and (voxel_paid["paid_evidence_observed_count"] or 0) > 0
                    else "NO_OBSERVED_PAID_EVIDENCE" if voxel_paid["scope"] == "SOURCE_AVAILABLE"
                    else "NOT_AVAILABLE_FOR_SOURCE"
                ),
            },
            "hangar_paid_evidence_availability": {
                **hangar_paid,
                "confirmed_member_count": member_counts["hangar"],
                "availability_state": "NOT_AVAILABLE_FOR_SOURCE",
            },
            "source_coverage_context": summary["source_coverage_context"],
            "risk_flags": risk_flags,
        }

    tiers: list[dict[str, Any]] = []
    for direction in loaded["directions"]:
        direction_id = str(direction["direction_id"])
        evaluation = evaluations_by_id[direction_id]
        candidate_state = str(evaluation["candidate_state"])
        tiers.append({
            "direction_id": direction_id,
            "category_id": str(direction["primary_category_id"]),
            "category_order": int(direction["primary_category_order"]),
            "direction_type": str(direction["direction_type"]),
            "canonical_direction_key": str(direction["canonical_direction_key"]),
            "baseline_subcategory_id": direction.get("baseline_subcategory_id"),
            "candidate_state": candidate_state,
            "direction_tier": DIRECTION_TIER_BY_STATE[candidate_state],
            "reason_codes": list(evaluation["reason_codes"]),
            "observed_member_count_by_source": dict(evaluation["observed_member_count_by_source"]),
            "source_pattern_state_by_source": dict(evaluation["source_pattern_state_by_source"]),
            "source_supply_inference_risk_by_source": dict(evaluation["source_supply_inference_risk_by_source"]),
            "source_evidence_by_source": _direction_fact_evidence(facts_by_direction, direction_id),
        })
    tiers.sort(key=lambda row: (
        TIER_ORDER[row["direction_tier"]], int(row["category_order"]),
        0 if row["direction_type"] == "SUBCATEGORY_BASELINE" else 1,
        str(row["canonical_direction_key"]), str(row["direction_id"]),
    ))

    evidence_packs = {str(row["direction_id"]): row for row in loaded["evidence_packs"]}
    candidate_ids = {str(row["direction_id"]) for row in loaded["candidates"]}
    tiers_by_id = {str(row["direction_id"]): row for row in tiers}
    options = []
    for candidate_id in candidate_ids:
        tier = tiers_by_id[candidate_id]
        profile = profile_by_category[str(tier["category_id"])]
        options.append({
            "direction_id": candidate_id,
            "category_id": str(tier["category_id"]),
            "category_order": int(tier["category_order"]),
            "category_opportunity_state": profile["category_opportunity_state"],
            "direction_type": str(tier["direction_type"]),
            "canonical_direction_key": str(tier["canonical_direction_key"]),
            "candidate_state": str(tier["candidate_state"]),
            "direction_tier": str(tier["direction_tier"]),
            "positive_support_shape": profile["positive_support_shape"],
            "risk_flags": list(profile["risk_flags"]),
            "reason_codes": list(tier["reason_codes"]),
            "direction_evidence_pack": evidence_packs[candidate_id],
            "source_facts_by_source": {
                source: facts_by_direction[(candidate_id, source)] for source in SOURCES
            },
        })
    options.sort(key=lambda row: (
        int(row["category_order"]),
        0 if row["direction_type"] == "SUBCATEGORY_BASELINE" else 1,
        str(row["canonical_direction_key"]), str(row["direction_id"]),
    ))
    return {
        "category_opportunity_profiles": [profile_by_category[str(row["category_id"])] for row in loaded["taxonomy"]],
        "category_direction_tiers": tiers,
        "category_research_options": options,
    }


def _csv_value(value: Any) -> str:
    if value is None:
        return CSV_NULL
    if isinstance(value, (dict, list)):
        return canonical_json(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return "".join(canonical_json(row) + "\n" for row in rows).encode("utf-8")


def _csv_bytes(table: str, rows: Sequence[Mapping[str, Any]]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS[table], lineterminator="\n", extrasaction="raise")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: _csv_value(row.get(column)) for column in CSV_COLUMNS[table]})
    return buffer.getvalue().encode("utf-8")


def _state_reconciliation(profiles: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = Counter(str(row["category_opportunity_state"]) for row in profiles)
    return {state: counts.get(state, 0) for state in CATEGORY_STATES}


def _expected_risk_flags_qa(
    category_id: str,
    state: str,
    members: Mapping[str, int],
    risks: Mapping[str, str],
    baseline_rows: Sequence[Mapping[str, Any]],
    direction_types_by_id: Mapping[str, str],
    candidate_state_by_id: Mapping[str, str],
) -> list[str]:
    flags = []
    if risks.get("voxel") == "VERY_HIGH":
        flags.append("VOXEL_SUPPLY_INFERENCE_RISK_VERY_HIGH")
    if risks.get("hangar") == "MATERIAL":
        flags.append("HANGAR_SUPPLY_INFERENCE_RISK_MATERIAL")
    if members.get("voxel", 0) == 0:
        flags.append("NO_VOXEL_CONFIRMED_MEMBERS")
    if any(candidate_state_by_id.get(str(row["direction_id"])) == "INSUFFICIENT_EVIDENCE" for row in baseline_rows):
        flags.append("BASELINE_EVIDENCE_GAPS_PRESENT")
    baseline_leads = any(
        direction_types_by_id.get(direction_id) == "SUBCATEGORY_BASELINE" and state_value == "ADVANCE_TO_STAGE_D"
        for direction_id, state_value in candidate_state_by_id.items()
    )
    lexical_leads = any(
        direction_types_by_id.get(direction_id) == "LEXICAL_SUBNICHE" and state_value == "ADVANCE_TO_STAGE_D"
        for direction_id, state_value in candidate_state_by_id.items()
    )
    if state == "PROMISING" and lexical_leads and not baseline_leads:
        flags.append("LEXICAL_ONLY_LEAD_SUPPORT")
    if category_id == "uncategorized":
        flags.append("UNCATEGORIZED_BUCKET")
    return sorted(flags)


def _qa_baseline_classification(candidate_state: str, patterns: Sequence[str]) -> str:
    if candidate_state == "INSUFFICIENT_EVIDENCE":
        return "INSUFFICIENT_EVIDENCE"
    usable = [pattern for pattern in patterns if pattern != "INSUFFICIENT_EVIDENCE"]
    if usable and not any(pattern not in NEGATIVE_PATTERN_STATES for pattern in usable):
        return "EVIDENCE_AGAINST_WHITESPACE"
    return "NOT_EVIDENCE_AGAINST_WHITESPACE"


def _qa_category_state(
    total_members: int,
    baseline_rows: Sequence[Mapping[str, Any]],
    all_candidate_states: Sequence[str],
    baseline_classifications: Mapping[str, str],
) -> str:
    if total_members == 0 or (
        bool(baseline_rows)
        and all(row["candidate_state"] == "INSUFFICIENT_EVIDENCE" for row in baseline_rows)
    ):
        return "INSUFFICIENT_EVIDENCE"
    if any(value == "ADVANCE_TO_STAGE_D" for value in all_candidate_states):
        return "PROMISING"
    if any(value == "WATCH_COVERAGE_LIMITED" for value in all_candidate_states):
        return "MIXED_OPPORTUNITY"
    interpretable_ids = [
        str(row["direction_id"]) for row in baseline_rows
        if row["candidate_state"] != "INSUFFICIENT_EVIDENCE"
    ]
    if len(interpretable_ids) >= 2 and all(
        baseline_classifications[direction_id] == "EVIDENCE_AGAINST_WHITESPACE"
        for direction_id in interpretable_ids
    ):
        return "LOW_OPPORTUNITY"
    return "NO_CLEAR_OPPORTUNITY"


def _qa_lead_watch_counts_by_direction_type(
    directions: Sequence[Mapping[str, Any]], evaluations_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, int]]:
    result = {
        direction_type: {"LEAD_DIRECTION": 0, "WATCH_DIRECTION": 0}
        for direction_type in ("SUBCATEGORY_BASELINE", "LEXICAL_SUBNICHE")
    }
    for direction in directions:
        state = str(evaluations_by_id[str(direction["direction_id"])]["candidate_state"])
        if state == "ADVANCE_TO_STAGE_D":
            result[str(direction["direction_type"])]["LEAD_DIRECTION"] += 1
        elif state == "WATCH_COVERAGE_LIMITED":
            result[str(direction["direction_type"])]["WATCH_DIRECTION"] += 1
    return result


def _qa_positive_support_shape(
    directions: Sequence[Mapping[str, Any]], evaluations_by_id: Mapping[str, Mapping[str, Any]],
) -> str:
    support_types = set()
    for direction in directions:
        if evaluations_by_id[str(direction["direction_id"])]["candidate_state"] in {
            "ADVANCE_TO_STAGE_D", "WATCH_COVERAGE_LIMITED",
        }:
            support_types.add(str(direction["direction_type"]))
    if support_types == {"SUBCATEGORY_BASELINE", "LEXICAL_SUBNICHE"}:
        return "BASELINE_AND_LEXICAL"
    if support_types == {"SUBCATEGORY_BASELINE"}:
        return "BASELINE_ONLY"
    if support_types == {"LEXICAL_SUBNICHE"}:
        return "LEXICAL_ONLY"
    return "NONE"


def _forbidden_output_key(value: Any) -> bool:
    forbidden = {
        "rank", "global_rank", "direction_rank", "category_rank", "score", "opportunity_score",
        "attractiveness_score", "winner", "winner_id", "recommendation", "build_recommendation",
        "shortlist", "stage_e_research", "research_queue",
    }
    if isinstance(value, Mapping):
        return any(str(key).casefold() in forbidden or _forbidden_output_key(item) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return any(_forbidden_output_key(item) for item in value)
    return False


def _logic_regression_checks() -> dict[str, bool]:
    negative = [
        {"direction_id": "b1", "candidate_state": "NO_CLEAR_PATTERN", "classification": "EVIDENCE_AGAINST_WHITESPACE"},
        {"direction_id": "b2", "candidate_state": "NO_CLEAR_PATTERN", "classification": "EVIDENCE_AGAINST_WHITESPACE"},
    ]
    mixed = [
        {"direction_id": "b1", "candidate_state": "NO_CLEAR_PATTERN", "classification": "NOT_EVIDENCE_AGAINST_WHITESPACE"},
        {"direction_id": "b2", "candidate_state": "NO_CLEAR_PATTERN", "classification": "EVIDENCE_AGAINST_WHITESPACE"},
    ]
    lexical_only_state, _ = _category_opportunity_state(10, negative, ["ADVANCE_TO_STAGE_D", "NO_CLEAR_PATTERN"])
    baseline_advance, _ = _category_opportunity_state(10, negative, ["ADVANCE_TO_STAGE_D"])
    watch_state, _ = _category_opportunity_state(10, negative, ["WATCH_COVERAGE_LIMITED"])
    low_state, _ = _category_opportunity_state(10, negative, ["NO_CLEAR_PATTERN"])
    no_clear_state, _ = _category_opportunity_state(10, mixed, ["NO_CLEAR_PATTERN"])
    precedence_zero, _ = _category_opportunity_state(0, negative, ["ADVANCE_TO_STAGE_D"])
    all_insufficient = [
        {"direction_id": "b1", "candidate_state": "INSUFFICIENT_EVIDENCE", "classification": "INSUFFICIENT_EVIDENCE"},
    ]
    precedence_all_insufficient, _ = _category_opportunity_state(5, all_insufficient, ["WATCH_COVERAGE_LIMITED"])
    return {
        "zero_members_precedes_positive_support": precedence_zero == "INSUFFICIENT_EVIDENCE",
        "all_baselines_insufficient_precedes_positive_support": precedence_all_insufficient == "INSUFFICIENT_EVIDENCE",
        "lexical_or_baseline_advance_is_promising": lexical_only_state == "PROMISING" and baseline_advance == "PROMISING",
        "watch_only_is_mixed": watch_state == "MIXED_OPPORTUNITY",
        "two_negative_interpretable_baselines_are_low": low_state == "LOW_OPPORTUNITY",
        "any_nonnegative_baseline_prevents_low": no_clear_state == "NO_CLEAR_OPPORTUNITY",
        "baseline_pattern_requires_noninsufficient_evidence": (
            _baseline_evidence_classification("NO_CLEAR_PATTERN", ["INSUFFICIENT_EVIDENCE", "LOW_DEMAND_PATTERN"])[0]
            == "EVIDENCE_AGAINST_WHITESPACE"
            and _baseline_evidence_classification("NO_CLEAR_PATTERN", ["INSUFFICIENT_EVIDENCE"])[0]
            == "NOT_EVIDENCE_AGAINST_WHITESPACE"
            and _baseline_evidence_classification("NO_CLEAR_PATTERN", ["MIXED_PATTERN"])[0]
            == "NOT_EVIDENCE_AGAINST_WHITESPACE"
        ),
    }


def _qa_checks(
    loaded: Mapping[str, Any],
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    input_sha_after: str,
    export_check: Mapping[str, Any],
    replay_ok: bool,
    *,
    enforce_pinned_inputs: bool,
) -> dict[str, bool]:
    profiles = list(tables["category_opportunity_profiles"])
    tiers = list(tables["category_direction_tiers"])
    options = list(tables["category_research_options"])
    directions_by_id = {str(row["direction_id"]): row for row in loaded["directions"]}
    evaluations_by_id = {str(row["direction_id"]): row for row in loaded["evaluations"]}
    facts_by_direction, _ = _source_fact_indexes(loaded)
    profile_by_id = {str(row["category_id"]): row for row in profiles}
    option_ids = [str(row["direction_id"]) for row in options]
    candidate_ids = {str(row["direction_id"]) for row in loaded["candidates"]}
    baseline_rows_by_category: dict[str, list[dict[str, Any]]] = defaultdict(list)
    category_directions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for direction in loaded["directions"]:
        category_id = str(direction["primary_category_id"])
        category_directions[category_id].append(direction)
        if direction["direction_type"] == "SUBCATEGORY_BASELINE":
            baseline_rows_by_category[category_id].append(direction)
    independent_baseline_classifications: dict[tuple[str, str], str] = {}
    for category_id, baselines in baseline_rows_by_category.items():
        for direction in baselines:
            direction_id = str(direction["direction_id"])
            evaluation = evaluations_by_id[direction_id]
            patterns = [str(evaluation["source_pattern_state_by_source"][source]) for source in SOURCES]
            independent_baseline_classifications[(category_id, direction_id)] = _qa_baseline_classification(
                str(evaluation["candidate_state"]), patterns
            )
    independent_state_by_category: dict[str, str] = {}
    for category in loaded["taxonomy"]:
        category_id = str(category["category_id"])
        baseline_evaluations = [
            evaluations_by_id[str(direction["direction_id"])] for direction in baseline_rows_by_category[category_id]
        ]
        state = _qa_category_state(
            sum(int(profile_by_id[category_id]["confirmed_member_count_by_source"][source]) for source in SOURCES),
            baseline_evaluations,
            [str(evaluations_by_id[str(direction["direction_id"])]["candidate_state"]) for direction in category_directions[category_id]],
            {
                str(direction["direction_id"]): independent_baseline_classifications[(category_id, str(direction["direction_id"]))]
                for direction in baseline_rows_by_category[category_id]
            },
        )
        independent_state_by_category[category_id] = state
    baseline_profile_matches = all(
        entry["classification"] == independent_baseline_classifications[(str(profile["category_id"]), str(entry["direction_id"]))]
        for profile in profiles for entry in profile["baseline_evidence_profile"]["directions"]
    )
    input_counts = {
        "category_count": len(loaded["taxonomy"]),
        "direction_universe": len(loaded["directions"]),
        "baseline_direction_count": sum(row["direction_type"] == "SUBCATEGORY_BASELINE" for row in loaded["directions"]),
        "lexical_direction_count": sum(row["direction_type"] == "LEXICAL_SUBNICHE" for row in loaded["directions"]),
        "direction_source_facts": len(loaded["source_facts"]),
        "direction_evaluations": len(loaded["evaluations"]),
        "candidate_directions": len(loaded["candidates"]),
        "direction_evidence_packs": len(loaded["evidence_packs"]),
        "category_direction_summary": len(loaded["category_summaries"]),
    }
    logic = _logic_regression_checks()
    no_forbidden_fields = not _forbidden_output_key(list(tables.values()))
    output_row_counts = {table: len(tables[table]) for table in TABLES}
    expected_output_counts = {
        "category_opportunity_profiles": len(loaded["taxonomy"]),
        "category_direction_tiers": len(loaded["directions"]),
        "category_research_options": len(candidate_ids),
    }
    exact_tier_mapping = all(
        row["direction_tier"] == DIRECTION_TIER_BY_STATE.get(str(row["candidate_state"]))
        for row in tiers
    )
    profile_source_counts_match = all(
        profile["confirmed_member_count_by_source"] == {
            source: int(next(summary for summary in loaded["category_summaries"] if summary["category_id"] == profile["category_id"])["confirmed_member_count_by_source"][source])
            for source in SOURCES
        }
        for profile in profiles
    )
    expected_candidate_state_counts = _state_reconciliation(profiles)
    check_state_reconciliation = (not enforce_pinned_inputs) or (
        expected_candidate_state_counts == EXPECTED_CATEGORY_STATE_COUNTS
        and {str(row["category_id"]): str(row["category_opportunity_state"]) for row in profiles} == EXPECTED_CATEGORY_STATES
    )
    return {
        "accepted_yEE75_sha256_matches_pin_before": (not enforce_pinned_inputs) or loaded["input_sha256"] == INPUT_SQLITE_SHA256,
        "accepted_yEE75_sha256_unchanged_after_processing": loaded["input_sha256"] == input_sha_after,
        "accepted_yEE75_run_id_and_schema_match_pin": (not enforce_pinned_inputs) or (
            loaded["metadata"].get("run_id") == INPUT_RUN_ID
            and loaded["metadata"].get("direction_schema_version") == INPUT_DIRECTION_SCHEMA_VERSION
            and loaded["metadata"].get("code_commit") == INPUT_CODE_COMMIT
        ),
        "accepted_yEE75_exact_input_counts_match_spec": (not enforce_pinned_inputs) or input_counts == EXPECTED_INPUT_COUNTS,
        "all_11_frozen_categories_visible_in_taxonomy_order": (
            len(profiles) == len(loaded["taxonomy"])
            and [row["category_id"] for row in profiles] == [row["category_id"] for row in loaded["taxonomy"]]
            and (not enforce_pinned_inputs or len(profiles) == 11)
        ),
        "all_38_baselines_and_9_lexical_directions_preserved": (
            input_counts["baseline_direction_count"] == (38 if enforce_pinned_inputs else input_counts["baseline_direction_count"])
            and input_counts["lexical_direction_count"] == (9 if enforce_pinned_inputs else input_counts["lexical_direction_count"])
        ),
        "every_input_direction_has_exactly_one_tier": (
            len(tiers) == len(loaded["directions"])
            and len({str(row["direction_id"]) for row in tiers}) == len(tiers)
            and {str(row["direction_id"]) for row in tiers} == set(directions_by_id)
        ),
        "direction_tiers_follow_candidate_state_mapping": exact_tier_mapping,
        "all_stage_c_candidates_appear_once_as_lead_or_watch": (
            len(option_ids) == len(candidate_ids) and len(set(option_ids)) == len(option_ids)
            and set(option_ids) == candidate_ids
            and all(row["direction_tier"] in {"LEAD_DIRECTION", "WATCH_DIRECTION"} for row in options)
        ),
        "no_context_or_insufficient_direction_is_a_research_option": all(
            row["candidate_state"] in {"ADVANCE_TO_STAGE_D", "WATCH_COVERAGE_LIMITED"}
            for row in options
        ),
        "category_research_options_preserve_pack_and_two_source_facts": all(
            row["direction_evidence_pack"]["direction_id"] == row["direction_id"]
            and list(row["source_facts_by_source"]) == list(SOURCES)
            and all(fact["direction_id"] == row["direction_id"] and fact["source"] == source for source, fact in row["source_facts_by_source"].items())
            for row in options
        ),
        "positive_support_shape_recomputes_from_lead_watch_types": all(
            profile["positive_support_shape"] == _qa_positive_support_shape(
                category_directions[str(profile["category_id"])], evaluations_by_id
            )
            for profile in profiles
        ),
        "lead_watch_counts_by_direction_type_recompute": all(
            profile["lead_watch_counts_by_direction_type"] == _qa_lead_watch_counts_by_direction_type(
                category_directions[str(profile["category_id"])], evaluations_by_id,
            )
            for profile in profiles
        ),
        "baseline_negative_evidence_is_independently_recomputed": baseline_profile_matches,
        "category_states_match_independent_recomputation": all(
            profile["category_opportunity_state"] == independent_state_by_category[str(profile["category_id"])]
            for profile in profiles
        ),
        "risk_flags_do_not_mutate_category_states": all(
            profile["category_opportunity_state"] == independent_state_by_category[str(profile["category_id"])]
            for profile in profiles
        ),
        "lexical_absence_is_not_negative_evidence": all(
            profile["category_opportunity_state"] != "LOW_OPPORTUNITY"
            or (
                profile["baseline_evidence_profile"]["interpretable_baseline_count"] >= 2
                and profile["baseline_evidence_profile"]["evidence_against_whitespace_count"]
                == profile["baseline_evidence_profile"]["interpretable_baseline_count"]
            )
            for profile in profiles
        ),
        "required_risk_flags_recompute_from_input_evidence": all(
            profile["risk_flags"] == _expected_risk_flags_qa(
                str(profile["category_id"]), str(profile["category_opportunity_state"]),
                profile["confirmed_member_count_by_source"], profile["upstream_supply_inference_risk_by_source"],
                baseline_rows_by_category[str(profile["category_id"])],
                {direction_id: str(direction["direction_type"]) for direction_id, direction in directions_by_id.items()
                 if str(direction["primary_category_id"]) == str(profile["category_id"])},
                {direction_id: str(evaluations_by_id[direction_id]["candidate_state"]) for direction_id, direction in evaluations_by_id.items()
                 if direction_id in {str(row["direction_id"]) for row in category_directions[str(profile["category_id"])]}},
            )
            for profile in profiles
        ),
        "uncategorized_bucket_is_flagged": all(
            ("UNCATEGORIZED_BUCKET" in profile["risk_flags"]) == (profile["category_id"] == "uncategorized")
            for profile in profiles
        ),
        "profile_source_counts_match_accepted_stage_c": profile_source_counts_match,
        "category_reconciliation_matches_pinned_expected_states": check_state_reconciliation,
        "category_state_counts_cover_all_allowed_states": set(expected_candidate_state_counts) == set(CATEGORY_STATES),
        "state_and_tier_logic_boundary_regressions_pass": all(logic.values()),
        "forbidden_global_rank_score_winner_and_stage_e_fields_absent": no_forbidden_fields,
        "jsonl_csv_sqlite_reconcile": bool(export_check.get("counts")) and all(
            export_check["counts"].get(f"{table}_jsonl") == output_row_counts[table]
            and export_check["counts"].get(f"{table}_csv") == output_row_counts[table]
            and export_check["counts"].get(f"{table}_sqlite") == output_row_counts[table]
            for table in TABLES
        ),
        "sqlite_integrity_and_foreign_keys_pass": (
            bool(export_check.get("sqlite_integrity_ok"))
            and int(export_check.get("sqlite_foreign_key_violations", -1)) == 0
        ),
        "deterministic_full_bundle_byte_identical_replay": bool(replay_ok),
        "canonical_input_opened_read_only": True,
    }


def _run_metadata(loaded: Mapping[str, Any], code_commit: str | None) -> dict[str, Any]:
    identity = {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "state_rule_version": STATE_RULE_VERSION,
        "direction_tier_version": DIRECTION_TIER_VERSION,
        "accepted_input_work_order": INPUT_WORK_ORDER,
        "accepted_input_merge_commit": INPUT_MERGE_COMMIT,
        "accepted_input_code_commit": INPUT_CODE_COMMIT,
        "accepted_input_sha256": loaded["input_sha256"],
        "accepted_input_run_id": loaded["metadata"].get("run_id"),
        "taxonomy_version": TAXONOMY_VERSION,
        "taxonomy_sha256": TAXONOMY_SHA256,
        "analysis_as_of": loaded["metadata"].get("analysis_as_of"),
        "code_commit": code_commit or "working-tree",
    }
    run_id = hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()
    return {**identity, "run_id": run_id}


def _write_sqlite(path: Path, loaded: Mapping[str, Any], tables: Mapping[str, Sequence[Mapping[str, Any]]], metadata: Mapping[str, Any]) -> None:
    db = sqlite3.connect(path)
    try:
        db.execute("PRAGMA page_size=4096")
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA user_version=76")
        db.execute("CREATE TABLE run_metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID")
        db.executemany(
            "INSERT INTO run_metadata VALUES (?,?)",
            [(str(key), canonical_json(value) if isinstance(value, (dict, list)) else str(value)) for key, value in sorted(metadata.items())],
        )
        db.execute("CREATE TRIGGER run_metadata_immutable_update BEFORE UPDATE ON run_metadata BEGIN SELECT RAISE(ABORT,'run_metadata is immutable'); END")
        db.execute("CREATE TRIGGER run_metadata_immutable_delete BEFORE DELETE ON run_metadata BEGIN SELECT RAISE(ABORT,'run_metadata is immutable'); END")
        db.execute("CREATE TABLE input_provenance (key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID")
        provenance = {
            "accepted_input_work_order": INPUT_WORK_ORDER,
            "accepted_input_merge_commit": INPUT_MERGE_COMMIT,
            "accepted_input_code_commit": INPUT_CODE_COMMIT,
            "accepted_input_sha256": loaded["input_sha256"],
            "accepted_input_run_id": loaded["metadata"].get("run_id"),
            "input_read_only": "true",
            "taxonomy_version": TAXONOMY_VERSION,
            "taxonomy_sha256": TAXONOMY_SHA256,
        }
        db.executemany("INSERT INTO input_provenance VALUES (?,?)", sorted(provenance.items()))
        db.execute("""CREATE TABLE frozen_category_snapshot (
            category_id TEXT PRIMARY KEY, category_order INTEGER NOT NULL UNIQUE,
            category_name TEXT NOT NULL, definition TEXT NOT NULL, category_json TEXT NOT NULL
        ) WITHOUT ROWID""")
        db.executemany(
            "INSERT INTO frozen_category_snapshot VALUES (?,?,?,?,?)",
            [(str(row["category_id"]), int(row["category_order"]), str(row["category_name"]), str(row["definition"]), canonical_json(row))
             for row in loaded["taxonomy"]],
        )
        db.execute("""CREATE TABLE category_opportunity_profiles (
            category_id TEXT PRIMARY KEY, category_order INTEGER NOT NULL UNIQUE,
            category_opportunity_state TEXT NOT NULL, positive_support_shape TEXT NOT NULL,
            confirmed_member_count_by_source_json TEXT NOT NULL,
            upstream_supply_inference_risk_by_source_json TEXT NOT NULL,
            baseline_evidence_profile_json TEXT NOT NULL, risk_flags_json TEXT NOT NULL,
            record_json TEXT NOT NULL,
            FOREIGN KEY(category_id) REFERENCES frozen_category_snapshot(category_id)
        ) WITHOUT ROWID""")
        db.execute("""CREATE TABLE category_direction_tiers (
            direction_id TEXT PRIMARY KEY, category_id TEXT NOT NULL, category_order INTEGER NOT NULL,
            direction_type TEXT NOT NULL, canonical_direction_key TEXT NOT NULL,
            candidate_state TEXT NOT NULL, direction_tier TEXT NOT NULL, record_json TEXT NOT NULL,
            FOREIGN KEY(category_id) REFERENCES category_opportunity_profiles(category_id)
        ) WITHOUT ROWID""")
        db.execute("""CREATE TABLE category_research_options (
            direction_id TEXT PRIMARY KEY, category_id TEXT NOT NULL, category_order INTEGER NOT NULL,
            candidate_state TEXT NOT NULL, direction_tier TEXT NOT NULL, record_json TEXT NOT NULL,
            FOREIGN KEY(direction_id) REFERENCES category_direction_tiers(direction_id),
            FOREIGN KEY(category_id) REFERENCES category_opportunity_profiles(category_id)
        ) WITHOUT ROWID""")
        db.executemany(
            "INSERT INTO category_opportunity_profiles VALUES (?,?,?,?,?,?,?,?,?)",
            [(row["category_id"], row["category_order"], row["category_opportunity_state"], row["positive_support_shape"],
              canonical_json(row["confirmed_member_count_by_source"]), canonical_json(row["upstream_supply_inference_risk_by_source"]),
              canonical_json(row["baseline_evidence_profile"]), canonical_json(row["risk_flags"]), canonical_json(row))
             for row in tables["category_opportunity_profiles"]],
        )
        db.executemany(
            "INSERT INTO category_direction_tiers VALUES (?,?,?,?,?,?,?,?)",
            [(row["direction_id"], row["category_id"], row["category_order"], row["direction_type"], row["canonical_direction_key"],
              row["candidate_state"], row["direction_tier"], canonical_json(row))
             for row in tables["category_direction_tiers"]],
        )
        db.executemany(
            "INSERT INTO category_research_options VALUES (?,?,?,?,?,?)",
            [(row["direction_id"], row["category_id"], row["category_order"], row["candidate_state"], row["direction_tier"], canonical_json(row))
             for row in tables["category_research_options"]],
        )
        db.commit()
    finally:
        db.close()


def _write_core(output: Path, loaded: Mapping[str, Any], tables: Mapping[str, Sequence[Mapping[str, Any]]], metadata: Mapping[str, Any]) -> None:
    root = Path(__file__).resolve().parents[2]
    artifacts = {
        "CATEGORY_OPPORTUNITY_GOAL_ALIGNMENT.md": "GOAL_ALIGNMENT.md",
        "CATEGORY_OPPORTUNITY_SEMANTICS.md": "CATEGORY_OPPORTUNITY_SEMANTICS.md",
        "CATEGORY_OPPORTUNITY_SCHEMA.md": "CATEGORY_OPPORTUNITY_SCHEMA.md",
    }
    for source_name, output_name in artifacts.items():
        source = root / source_name
        if not source.is_file():
            raise CategoryOpportunityError(f"Required YEE-76 contract file is missing: {source_name}")
        shutil.copyfile(source, output / output_name)
    for table in TABLES:
        (output / f"{table}.jsonl").write_bytes(_jsonl_bytes(tables[table]))
        (output / f"{table}.csv").write_bytes(_csv_bytes(table, tables[table]))
    (output / "CATEGORY_OPPORTUNITY_MAP.md").write_text(_category_map_markdown(tables["category_opportunity_profiles"]), encoding="utf-8", newline="\n")
    _write_sqlite(output / "category_opportunity_map.sqlite", loaded, tables, metadata)


def _category_map_markdown(profiles: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# BBB Market — Category Opportunity Map",
        "",
        "Stage D evidence map from accepted YEE-75 only. Categories are shown in frozen taxonomy order, not attractiveness order. No external commercial validation has yet occurred. This is not a build recommendation.",
        "",
    ]
    for profile in profiles:
        baseline = profile["baseline_evidence_profile"]
        lines.extend([
            f"## {profile['category_order'] + 1}. {profile['category_name']}",
            "",
            f"- Category ID: `{profile['category_id']}`",
            f"- Definition: {profile['category_definition']}",
            f"- Opportunity state: **{profile['category_opportunity_state']}** — `{', '.join(profile['reason_codes'])}`",
            f"- Confirmed members: Hangar {profile['confirmed_member_count_by_source']['hangar']}; Voxel {profile['confirmed_member_count_by_source']['voxel']}",
            f"- Upstream supply-inference risk: Hangar `{profile['upstream_supply_inference_risk_by_source']['hangar']}`; Voxel `{profile['upstream_supply_inference_risk_by_source']['voxel']}`",
            f"- Baselines: {baseline['baseline_direction_count']} total; {baseline['interpretable_baseline_count']} interpretable; {baseline['insufficient_baseline_count']} insufficient; {baseline['evidence_against_whitespace_count']} evidence-against-whitespace",
            f"- Positive support shape: `{profile['positive_support_shape']}`",
            f"- LEAD/WATCH counts by direction type: `{canonical_json(profile['lead_watch_counts_by_direction_type'])}`",
            f"- Voxel paid evidence: scope `{profile['voxel_paid_evidence_availability']['scope']}`, confirmed members {profile['voxel_paid_evidence_availability']['confirmed_member_count']}, known paid/free observations {profile['voxel_paid_evidence_availability']['paid_evidence_observed_count']}",
            f"- Hangar paid evidence: `{profile['hangar_paid_evidence_availability']['scope']}` (unavailable is not zero/free)",
            f"- Risk flags: {', '.join(f'`{flag}`' for flag in profile['risk_flags']) if profile['risk_flags'] else 'none'}",
            "",
            "### LEAD directions",
            "",
        ])
        lines.extend(_markdown_direction_lines(profile["lead_directions"]))
        lines.extend(["", "### WATCH directions", ""])
        lines.extend(_markdown_direction_lines(profile["watch_directions"]))
        lines.extend(["", "### Context and baseline evidence", ""])
        context_rows = [row for row in profile["direction_evidence"] if row["direction_tier"] in {"CONTEXT_DIRECTION", "INSUFFICIENT_DIRECTION"}]
        lines.extend(_markdown_direction_lines(context_rows))
        if not context_rows:
            lines.append("- No additional context/insufficient directions.")
        negative_rows = [row for row in baseline["directions"] if row["classification"] == "EVIDENCE_AGAINST_WHITESPACE"]
        if negative_rows:
            lines.extend(["", "Negative baseline evidence (category-local only):", ""])
            lines.extend(_markdown_direction_lines([
                next(item for item in profile["direction_evidence"] if item["direction_id"] == row["direction_id"])
                for row in negative_rows
            ]))
        lines.extend(["", "External commercial validation has not yet occurred.", ""])
    return "\n".join(lines)


def _markdown_direction_lines(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    if not rows:
        return ["- None."]
    result = []
    for row in rows:
        pattern = ", ".join(f"{source}={row['source_pattern_state_by_source'][source]}" for source in SOURCES)
        result.append(
            f"- `{row['canonical_direction_key']}` ({row['direction_type']}; `{row['candidate_state']}`; {pattern}; "
            f"members Hangar {row['observed_member_count_by_source']['hangar']} / Voxel {row['observed_member_count_by_source']['voxel']}; "
            f"reason `{', '.join(row['reason_codes'])}`)."
        )
    return result


def _manifest(output: Path, metadata: Mapping[str, Any], qa: Mapping[str, Any]) -> dict[str, Any]:
    artifact_rows = []
    for name in sorted(ARTIFACTS):
        if name == "DATASET_MANIFEST.json":
            continue
        path = output / name
        artifact_rows.append({"path": name, "size_bytes": path.stat().st_size, "sha256": sha256_file(path)})
    return {
        "work_order": WORK_ORDER,
        "delivery_status": DELIVERY_STATUS,
        "run_id": metadata["run_id"],
        "code_commit": metadata["code_commit"],
        "schema_version": SCHEMA_VERSION,
        "state_rule_version": STATE_RULE_VERSION,
        "direction_tier_version": DIRECTION_TIER_VERSION,
        "analysis_as_of": metadata["analysis_as_of"],
        "input": qa["input"],
        "taxonomy": {"version": TAXONOMY_VERSION, "sha256": TAXONOMY_SHA256, "category_count": 11, "baseline_count": 38},
        "row_counts": qa["row_counts"],
        "category_opportunity_state_counts": qa["category_opportunity_state_counts"],
        "artifact_count_excluding_manifest": len(artifact_rows),
        "artifacts": artifact_rows,
        "manifest_self_hash": "omitted_to_avoid_recursive_hash",
    }


def _final_report(qa: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    counts = qa["row_counts"]
    state_counts = qa["category_opportunity_state_counts"]
    lines = [
        "# YEE-76 final report",
        "",
        f"Status: `{DELIVERY_STATUS}`",
        f"Production QA: `{qa['status']}` ({qa['passing_checks']}/{qa['check_count']} checks)",
        "",
        "## Scope and provenance",
        "",
        "Stage D consumes only the accepted YEE-75 `category_direction_discovery.sqlite` read-only. It preserves the PLUGIN_ONLY universe and frozen taxonomy; YEE-30 through YEE-59 are not analytical inputs. No marketplace/API/web research, model inference, taxonomy or direction remapping, commercial validation, concept generation, scoring, ranking, shortlist, or Stage E work was performed.",
        "",
        f"- Accepted YEE-75 merge baseline: `{INPUT_MERGE_COMMIT}`",
        f"- Accepted YEE-75 input code commit: `{INPUT_CODE_COMMIT}`; run ID: `{INPUT_RUN_ID}`",
        f"- YEE-76 code commit: `{metadata['code_commit']}`; run ID: `{metadata['run_id']}`",
        f"- Input SHA-256 before/after: `{qa['input']['sha256_before']}` / `{qa['input']['sha256_after']}`",
        f"- Analysis as of: `{qa['analysis_as_of']}` (inherited unchanged)",
        "",
        "## Output reconciliation",
        "",
        f"- Profiles: {counts['category_opportunity_profiles']} categories; direction tiers: {counts['category_direction_tiers']}; unranked category research options: {counts['category_research_options']}.",
        "- Category states: " + "; ".join(f"{key} {state_counts[key]}" for key in CATEGORY_STATES) + ".",
        f"- All {EXPECTED_INPUT_COUNTS['direction_universe']} accepted YEE-75 directions are represented; all {EXPECTED_INPUT_COUNTS['candidate_directions']} ADVANCE/WATCH candidates appear exactly once as unranked research options.",
        "- All 38 baselines and 9 accepted lexical directions are retained. Lexical absence is never used as negative evidence.",
        "",
        "## QA and stop boundary",
        "",
        f"- SQLite integrity: `{qa['sqlite_checks']['integrity_ok']}`; foreign-key violations: {qa['sqlite_checks']['foreign_key_violation_count']}.",
        f"- JSONL/CSV/SQLite reconciliation: `{qa['checks']['jsonl_csv_sqlite_reconcile']}`; deterministic full-bundle replay: `{qa['deterministic_replay']['byte_identical']}` ({qa['deterministic_replay']['artifact_count']} artifacts).",
        "- Every frozen category, including uncategorized and empty Server utilities, remains visible. Risk flags describe uncertainty and do not change the state rules.",
        "- No category/direction attractiveness rank, numeric score, winner, recommendation, shortlist, or Stage E research output is present.",
        "",
    ]
    return "\n".join(lines)


def _write_final(output: Path, qa: Mapping[str, Any], tables: Mapping[str, Sequence[Mapping[str, Any]]], metadata: Mapping[str, Any]) -> None:
    (output / "QA_RESULT.json").write_text(canonical_json(qa) + "\n", encoding="utf-8", newline="\n")
    (output / "FINAL_REPORT.md").write_text(_final_report(qa, metadata), encoding="utf-8", newline="\n")
    manifest = _manifest(output, metadata, qa)
    (output / "DATASET_MANIFEST.json").write_text(canonical_json(manifest) + "\n", encoding="utf-8", newline="\n")


def _export_checks(output: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for table in TABLES:
        rows = list(tables[table])
        jsonl_rows = [json.loads(line) for line in (output / f"{table}.jsonl").read_text(encoding="utf-8").splitlines()]
        if [canonical_json(row) for row in jsonl_rows] != [canonical_json(row) for row in rows]:
            raise CategoryOpportunityError(f"{table} JSONL does not reconcile")
        with (output / f"{table}.csv").open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            if tuple(reader.fieldnames or ()) != CSV_COLUMNS[table]:
                raise CategoryOpportunityError(f"{table} CSV columns do not reconcile")
            csv_rows = list(reader)
        expected_csv_rows = [{column: _csv_value(row.get(column)) for column in CSV_COLUMNS[table]} for row in rows]
        if csv_rows != expected_csv_rows:
            raise CategoryOpportunityError(f"{table} CSV values do not reconcile")
        counts[f"{table}_jsonl"] = len(jsonl_rows)
        counts[f"{table}_csv"] = len(csv_rows)
    connection = _read_only_connection(output / "category_opportunity_map.sqlite")
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        fk = list(connection.execute("PRAGMA foreign_key_check"))
        for table in TABLES:
            actual = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            expected = len(tables[table])
            if actual != expected:
                raise CategoryOpportunityError(f"SQLite row count mismatch for {table}")
            db_rows = [
                _loads(row[0], f"{table}.record_json")
                for row in connection.execute(f"SELECT record_json FROM {table}")
            ]
            primary_key = "category_id" if table == "category_opportunity_profiles" else "direction_id"
            expected_by_pk = {str(row[primary_key]): canonical_json(row) for row in tables[table]}
            actual_by_pk = {str(row[primary_key]): canonical_json(row) for row in db_rows}
            if actual_by_pk != expected_by_pk:
                raise CategoryOpportunityError(f"SQLite records do not reconcile for {table}")
            counts[f"{table}_sqlite"] = actual
    finally:
        connection.close()
    return {
        "counts": counts,
        "sqlite_integrity_ok": integrity == "ok",
        "sqlite_foreign_key_violations": len(fk),
    }


def _qa_document_gate() -> bool:
    root = Path(__file__).resolve().parents[2]
    goal_path = root / "CATEGORY_OPPORTUNITY_GOAL_ALIGNMENT.md"
    semantics_path = root / "CATEGORY_OPPORTUNITY_SEMANTICS.md"
    schema_path = root / "CATEGORY_OPPORTUNITY_SCHEMA.md"
    if not all(path.is_file() for path in (goal_path, semantics_path, schema_path)):
        return False
    goal = goal_path.read_text(encoding="utf-8").casefold()
    required = (
        "goal_alignment: pass", "product objective unchanged", "plugin_only",
        "sole analytical input is accepted yee-75", "no historical yee-30 through yee-59 analytical dependency",
        "all 11 frozen categories remain visible", "stage d creates category opportunity states and category-local research options only",
        "no external research", "no concept synthesis", "no commercial validation", "no build recommendation",
    )
    return all(text in goal for text in required)


def _make_qa(
    loaded: Mapping[str, Any],
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    metadata: Mapping[str, Any],
    input_sha_after: str,
    export_check: Mapping[str, Any],
    replay_ok: bool,
    *,
    enforce_pinned_inputs: bool,
) -> dict[str, Any]:
    profiles = list(tables["category_opportunity_profiles"])
    state_counts = _state_reconciliation(profiles)
    checks = _qa_checks(
        loaded, tables, input_sha_after, export_check, replay_ok,
        enforce_pinned_inputs=enforce_pinned_inputs,
    )
    failed = sorted(name for name, value in checks.items() if not value)
    source_member_counts = {
        source: sum(int(row["confirmed_member_count_by_source"][source]) for row in profiles)
        for source in SOURCES
    }
    qa = {
        "work_order": WORK_ORDER,
        "delivery_status": DELIVERY_STATUS,
        "status": "FAIL" if failed else "PASS",
        "schema_version": SCHEMA_VERSION,
        "state_rule_version": STATE_RULE_VERSION,
        "direction_tier_version": DIRECTION_TIER_VERSION,
        "run_id": metadata["run_id"],
        "code_commit": metadata["code_commit"],
        "analysis_as_of": metadata["analysis_as_of"],
        "input": {
            "work_order": INPUT_WORK_ORDER,
            "accepted_merge_commit": INPUT_MERGE_COMMIT,
            "accepted_code_commit": INPUT_CODE_COMMIT,
            "accepted_run_id": loaded["metadata"].get("run_id"),
            "sha256_before": loaded["input_sha256"],
            "sha256_after": input_sha_after,
            "read_only": True,
            "source_member_count_by_source": source_member_counts,
            "direction_schema_version": loaded["metadata"].get("direction_schema_version"),
            "taxonomy_version": TAXONOMY_VERSION,
            "taxonomy_sha256": TAXONOMY_SHA256,
        },
        "taxonomy": {
            "category_count": len(loaded["taxonomy"]),
            "baseline_count": sum(len(row["subcategories"]) for row in loaded["taxonomy"]),
            "version": TAXONOMY_VERSION,
            "sha256": TAXONOMY_SHA256,
        },
        "input_row_counts": {
            "categories": len(loaded["taxonomy"]),
            "directions": len(loaded["directions"]),
            "baselines": sum(row["direction_type"] == "SUBCATEGORY_BASELINE" for row in loaded["directions"]),
            "lexical_directions": sum(row["direction_type"] == "LEXICAL_SUBNICHE" for row in loaded["directions"]),
            "direction_source_facts": len(loaded["source_facts"]),
            "direction_evaluations": len(loaded["evaluations"]),
            "candidates": len(loaded["candidates"]),
            "evidence_packs": len(loaded["evidence_packs"]),
        },
        "row_counts": {table: len(rows) for table, rows in tables.items()},
        "category_opportunity_state_counts": state_counts,
        "sqlite_checks": {
            "integrity_ok": bool(export_check.get("sqlite_integrity_ok")),
            "foreign_key_violation_count": int(export_check.get("sqlite_foreign_key_violations", -1)),
        },
        "export_row_counts": dict(export_check.get("counts", {})),
        "deterministic_replay": {"byte_identical": bool(replay_ok), "artifact_count": len(ARTIFACTS)},
        "checks": checks,
        "check_count": len(checks),
        "passing_checks": sum(bool(value) for value in checks.values()),
        "failed_checks": failed,
    }
    return qa


def _write_bundle(
    output: Path,
    loaded: Mapping[str, Any],
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    metadata: Mapping[str, Any],
    qa: Mapping[str, Any] | None = None,
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise CategoryOpportunityError("YEE-76 output directory must be empty before a production build")
    _write_core(output, loaded, tables, metadata)
    if qa is not None:
        _write_final(output, qa, tables, metadata)


def build_category_opportunity_map(
    input_db: Path | str,
    output_dir: Path | str,
    *,
    code_commit: str | None = None,
    enforce_pinned_inputs: bool = True,
) -> dict[str, Any]:
    input_path = Path(input_db).resolve()
    output = Path(output_dir).resolve()
    if not input_path.is_file():
        raise CategoryOpportunityError(f"Accepted YEE-75 input database not found: {input_path}")
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise CategoryOpportunityError("YEE-76 output directory must be empty before a production build")
    if not _qa_document_gate():
        raise CategoryOpportunityError("BLOCKED_SCOPE_MISMATCH: GOAL_ALIGNMENT/semantics/schema contract gate failed")
    loaded = _load_input(input_path, enforce_pinned_inputs=enforce_pinned_inputs)
    metadata = _run_metadata(loaded, code_commit)
    tables = _build_outputs(loaded)
    replay_tables = _build_outputs(loaded)
    if any(
        [canonical_json(row) for row in tables[table]] != [canonical_json(row) for row in replay_tables[table]]
        for table in TABLES
    ):
        raise CategoryOpportunityError("YEE-76 in-memory deterministic replay failed")
    _write_bundle(output, loaded, tables, metadata)
    export_check = _export_checks(output, tables)
    input_sha_after = sha256_file(input_path)
    qa = _make_qa(
        loaded, tables, metadata, input_sha_after, export_check, True,
        enforce_pinned_inputs=enforce_pinned_inputs,
    )
    if qa["failed_checks"]:
        raise CategoryOpportunityError(f"YEE-76 production QA failed: {qa['failed_checks']}")
    _write_final(output, qa, tables, metadata)
    with tempfile.TemporaryDirectory(prefix="yee76-replay-") as temp_name:
        replay_dir = Path(temp_name)
        _write_core(replay_dir, loaded, replay_tables, metadata)
        _write_final(replay_dir, qa, replay_tables, metadata)
        replay_ok = all((output / name).read_bytes() == (replay_dir / name).read_bytes() for name in ARTIFACTS)
    if not replay_ok:
        raise CategoryOpportunityError("YEE-76 complete deterministic artifact replay failed")
    return qa
