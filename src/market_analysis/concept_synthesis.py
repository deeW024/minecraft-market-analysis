"""Deterministic YEE-57 readiness and evidence-linked concept synthesis."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sqlite3
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence


WORK_ORDER = "YEE-57"
SCHEMA_VERSION = "yee-57-evidence-to-concept-synthesis-v0.1"
CAPTURE_VERSION = "yee-57-concept-capture-v0.1"
BASELINE_COMMIT = "406a0965168c0bc753b78c963e34cea6f8ad38f1"
EXPECTED_READY_ORDERS = (1, 2, 3, 4, 5, 8, 10, 11)
PILOT_CONCEPT_ORDERS = (1, 2, 3, 4, 5)
FINAL_CONCEPT_ORDERS = (1, 2, 3, 4, 5, 8, 10, 11)

INPUT_HASHES = {
    "deep_commercial_validation.sqlite": "e82fa072e3e75a2b3deef7fa88fdc1f958928b75916708ffbf4e2a181260d6c1",
    "commercial_validation_packs.jsonl": "cfa996476ccfcf92f7c2ed03420ce0cf66265f528c16dccb2aec1c463778007e",
    "validation_evidence.jsonl": "1319a9b6d2996ba89509978dcec737b301ad4c19d8cc3bd793dd3197e9f905a2",
    "pain_clusters.jsonl": "60b40aa06474184046657f23cf0be3e297fdbb75823c9a3673afb62e89d9e1a5",
    "market_alternatives.jsonl": "8c29ae2bb050852734506453d42e70962fe81501c5272255c4fc2c3f3c15d429",
    "differentiation_hypotheses.jsonl": "ee099fdcbcf3fe8d637a7bc5f4a767dc05957f4462c4b267bd3081fa447230c9",
    "validation_queries.jsonl": "d499c3aef99376e6bfe6f58c3a042721b3bd4649e27bd17d762e844b92fd4aff",
}

INPUT_TABLES = {
    "commercial_validation_packs": "commercial_validation_packs.jsonl",
    "validation_evidence": "validation_evidence.jsonl",
    "pain_clusters": "pain_clusters.jsonl",
    "market_alternatives": "market_alternatives.jsonl",
    "differentiation_hypotheses": "differentiation_hypotheses.jsonl",
    "validation_queries": "validation_queries.jsonl",
}

READINESS_COLUMNS = (
    "family_id", "deep_validation_order", "consensus_rank", "canonical_topic_key",
    "validation_cohort", "buyer_job_status", "pain_status", "feasibility_status",
    "retained_alternative_count", "differentiation_count", "commercial_signal_state",
    "concept_readiness", "readiness_reasons",
)
CONCEPT_COLUMNS = (
    "concept_id", "family_id", "deep_validation_order", "consensus_rank",
    "canonical_topic_key", "concept_readiness", "target_user_role", "payer_role",
    "buyer_job_basis", "pain_basis", "commercial_signal_state",
    "retained_alternative_ids", "differentiation_basis", "feasibility_status",
    "feasibility_basis", "concept_statement", "solution_primitives",
    "uncertainty_flags", "next_validation_questions", "evidence_refs", "synthesis_notes",
)
READINESS_JSON_FIELDS = {"readiness_reasons"}
CONCEPT_JSON_FIELDS = {
    "buyer_job_basis", "pain_basis", "retained_alternative_ids", "differentiation_basis",
    "feasibility_basis", "solution_primitives", "uncertainty_flags",
    "next_validation_questions", "evidence_refs",
}
OUTPUT_FILES = (
    "concept_readiness_matrix.jsonl", "concept_readiness_matrix.csv",
    "opportunity_concept_cards.jsonl", "opportunity_concept_cards.csv",
    "CONCEPT_CAPTURE.json", "concept_synthesis.sqlite", "CONCEPT_SYNTHESIS_SCHEMA.md",
    "PILOT_REPORT.md", "QA_RESULT.json", "DATASET_MANIFEST.json",
)
FINAL_OUTPUT_FILES = (
    "concept_readiness_matrix.jsonl", "concept_readiness_matrix.csv",
    "opportunity_concept_cards.jsonl", "opportunity_concept_cards.csv",
    "CONCEPT_CAPTURE.json", "concept_synthesis.sqlite", "CONCEPT_SYNTHESIS_SCHEMA.md",
    "PILOT_REPORT.md", "FINAL_REPORT.md", "QA_RESULT.json", "DATASET_MANIFEST.json",
)

VERIFIED_MONETIZATION = {
    "PAID_PRICE_VERIFIED", "FREEMIUM_PRICE_VERIFIED", "MONETIZED_PRICE_NOT_VISIBLE",
}
READINESS_REASON_ORDER = (
    "BUYER_JOB_NOT_ESTABLISHED", "PAIN_NOT_ESTABLISHED",
    "COMPARATIVE_CONTEXT_NOT_ESTABLISHED", "FEASIBILITY_NOT_ESTABLISHED",
)
BASIS_TYPES = {"PAIN_RESPONSE", "DIFFERENTIATION_DIRECTION", "JOB_ENABLEMENT"}
DISALLOWED = re.compile(
    r"\b(?:best|winner|should\s+build|must\s+build|high\s+demand|low\s+competition|"
    r"blue\s+ocean|market\s+gap|guaranteed|easy\s+money|strong\s+product-market\s+fit|"
    r"likely\s+profitable|TAM|revenue\s+potential|opportunity\s+score)\b",
    re.IGNORECASE,
)
NUMERIC_PRICE = re.compile(
    r"(?:\$\s*\d|\b\d+(?:\.\d+)?\s*(?:USD|EUR|GBP|PLN|CAD|AUD)\b)",
    re.IGNORECASE,
)
NULL_TOKEN = r"\N"


class ConceptSynthesisError(ValueError):
    """Canonical input, capture, or output contract failed validation."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        raise ConceptSynthesisError(f"unable to read accepted YEE-55 JSONL: {path.name}") from exc
    if any(not isinstance(row, dict) for row in rows):
        raise ConceptSynthesisError(f"YEE-55 JSONL rows must be objects: {path.name}")
    return rows


def _decode_source_row(row: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for key, value in result.items():
        if isinstance(value, str) and value[:1] in "[{":
            try:
                result[key] = json.loads(value)
            except json.JSONDecodeError:
                pass
    return result


def _readonly_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _load_inputs(input_dir: Path) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str], dict[str, bool]]:
    paths = {name: input_dir / name for name in INPUT_HASHES}
    missing = [name for name, path in paths.items() if not path.is_file()]
    if missing:
        raise ConceptSynthesisError(f"accepted YEE-55 input file(s) missing: {missing}")
    before = {name: _sha256(path) for name, path in paths.items()}
    mismatches = {name: (before[name], expected) for name, expected in INPUT_HASHES.items() if before[name] != expected}
    if mismatches:
        raise ConceptSynthesisError(f"accepted YEE-55 pinned hash mismatch: {mismatches}")

    exports = {table: _read_jsonl(input_dir / filename) for table, filename in INPUT_TABLES.items()}
    database = paths["deep_commercial_validation.sqlite"]
    connection = _readonly_connection(database)
    try:
        integrity_ok = connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        foreign_keys_ok = not connection.execute("PRAGMA foreign_key_check").fetchall()
        if not integrity_ok or not foreign_keys_ok:
            raise ConceptSynthesisError("accepted YEE-55 SQLite integrity/FK check failed")
        database_rows: dict[str, list[dict[str, Any]]] = {}
        for table, source_rows in exports.items():
            columns = tuple(row[1] for row in connection.execute(f'PRAGMA table_info("{table}")'))
            if not columns:
                raise ConceptSynthesisError(f"accepted YEE-55 SQLite table missing: {table}")
            stored_rows = [
                _decode_source_row(dict(row))
                for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')
            ]
            if stored_rows != source_rows:
                raise ConceptSynthesisError(f"accepted YEE-55 {table} JSONL export does not reconcile to SQLite")
            database_rows[table] = stored_rows
    finally:
        connection.close()

    after = {name: _sha256(path) for name, path in paths.items()}
    if before != after:
        raise ConceptSynthesisError("accepted YEE-55 inputs changed during read-only reconciliation")
    checks = {
        "pinned_hashes_match": True,
        "input_sqlite_integrity_ok": integrity_ok,
        "input_sqlite_foreign_keys_ok": foreign_keys_ok,
        "all_jsonl_exports_match_sqlite": True,
        "canonical_input_hashes_stable": before == after,
    }
    packs = database_rows["commercial_validation_packs"]
    ordered = sorted(packs, key=lambda row: row["deep_validation_order"])
    if len(ordered) != 15 or [row["deep_validation_order"] for row in ordered] != list(range(1, 16)):
        raise ConceptSynthesisError("YEE-55 universe must be exactly orders 1..15")
    if any(row.get("validation_cohort") != "COHORT_A" for row in ordered):
        raise ConceptSynthesisError("YEE-55 universe contains a non-COHORT_A family")
    family_ids = [row["family_id"] for row in ordered]
    if len(set(family_ids)) != 15:
        raise ConceptSynthesisError("YEE-55 family identities are not unique")
    family_set = set(family_ids)
    for table in tuple(database_rows)[1:]:
        if any(row.get("family_id") not in family_set for row in database_rows[table]):
            raise ConceptSynthesisError(f"YEE-55 {table} contains a family outside the accepted 15-row universe")
    alternative_ids_by_family: dict[str, list[str]] = defaultdict(list)
    for row in database_rows["market_alternatives"]:
        alternative_ids_by_family[row["family_id"]].append(row["alternative_id"])
    for row in ordered:
        if set(row.get("retained_alternative_ids", [])) != set(alternative_ids_by_family[row["family_id"]]):
            raise ConceptSynthesisError(f"YEE-55 retained alternative IDs do not reconcile for {row['family_id']}")
    return database_rows, before, checks


def commercial_signal_state(alternatives: Sequence[Mapping[str, Any]]) -> str:
    if any(row.get("monetization_status") in VERIFIED_MONETIZATION for row in alternatives):
        return "VERIFIED_PAID_SIGNAL"
    if alternatives:
        return "COMPARATIVE_SIGNAL_ONLY"
    return "NO_RETAINED_COMPARATIVE_SIGNAL"


def derive_readiness_row(
    pack: Mapping[str, Any],
    alternatives: Sequence[Mapping[str, Any]],
    differentiations: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    statuses = pack.get("dimension_statuses") or {}
    buyer_status = statuses.get("buyer_job")
    pain_status = statuses.get("pain_prevalence_sample")
    feasibility_status = pack.get("feasibility_evidence_status")
    reasons = []
    if buyer_status not in {"EVIDENCED", "LIMITED_SAMPLE"}:
        reasons.append("BUYER_JOB_NOT_ESTABLISHED")
    if pain_status not in {"REPEATED_SAMPLE", "LIMITED_SAMPLE"}:
        reasons.append("PAIN_NOT_ESTABLISHED")
    if not alternatives and not differentiations:
        reasons.append("COMPARATIVE_CONTEXT_NOT_ESTABLISHED")
    if feasibility_status not in {"COMPLETE", "PARTIAL"}:
        reasons.append("FEASIBILITY_NOT_ESTABLISHED")
    return {
        "family_id": pack["family_id"],
        "deep_validation_order": pack["deep_validation_order"],
        "consensus_rank": pack["consensus_rank"],
        "canonical_topic_key": pack["canonical_topic_key"],
        "validation_cohort": pack["validation_cohort"],
        "buyer_job_status": buyer_status,
        "pain_status": pain_status,
        "feasibility_status": feasibility_status,
        "retained_alternative_count": len(alternatives),
        "differentiation_count": len(differentiations),
        "commercial_signal_state": commercial_signal_state(alternatives),
        "concept_readiness": "CONCEPT_READY" if not reasons else "NEEDS_EVIDENCE",
        "readiness_reasons": reasons,
    }


def derive_readiness_matrix(payload: Mapping[str, Sequence[Mapping[str, Any]]]) -> list[dict[str, Any]]:
    alternatives: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    differentiations: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in payload["market_alternatives"]:
        alternatives[row["family_id"]].append(row)
    for row in payload["differentiation_hypotheses"]:
        differentiations[row["family_id"]].append(row)
    packs = sorted(payload["commercial_validation_packs"], key=lambda row: row["deep_validation_order"])
    return [
        derive_readiness_row(pack, alternatives[pack["family_id"]], differentiations[pack["family_id"]])
        for pack in packs
    ]


def _family_ref_maps(
    payload: Mapping[str, Sequence[Mapping[str, Any]]],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    packs = {row["family_id"]: row for row in payload["commercial_validation_packs"]}
    maps: dict[str, dict[str, Any]] = {family_id: {} for family_id in packs}
    owner_by_ref: dict[str, str] = {}
    for table, id_field in (
        ("validation_evidence", "evidence_id"), ("pain_clusters", "cluster_id"),
        ("market_alternatives", "alternative_id"),
        ("differentiation_hypotheses", "hypothesis_id"),
    ):
        for row in payload[table]:
            family_id = row["family_id"]
            ref = row[id_field]
            if ref in owner_by_ref:
                raise ConceptSynthesisError(f"YEE-55 reference identity collision: {ref}")
            owner_by_ref[ref] = family_id
            maps[family_id][ref] = {"type": table, "row": row}
    return maps, owner_by_ref


def _buyer_evidence_ids(pack: Mapping[str, Any]) -> set[str]:
    return {
        evidence_id
        for hypothesis in pack.get("buyer_job_hypotheses", [])
        for evidence_id in hypothesis.get("evidence_ids", [])
    }


def _pain_reference_ids(clusters: Sequence[Mapping[str, Any]]) -> set[str]:
    refs: set[str] = set()
    for cluster in clusters:
        refs.add(cluster["cluster_id"])
        refs.update(cluster.get("evidence_ids", []))
    return refs


def _validate_capture(
    capture: Mapping[str, Any],
    payload: Mapping[str, Sequence[Mapping[str, Any]]],
    readiness: Sequence[Mapping[str, Any]],
    authorized_orders: Sequence[int] = PILOT_CONCEPT_ORDERS,
) -> dict[str, Mapping[str, Any]]:
    if set(capture) != {"capture_version", "families"} or capture.get("capture_version") != CAPTURE_VERSION:
        raise ConceptSynthesisError(f"CONCEPT_CAPTURE must use {CAPTURE_VERSION} and only its envelope fields")
    requested_orders = [row["deep_validation_order"] for row in readiness if row["concept_readiness"] == "CONCEPT_READY"]
    if tuple(requested_orders) != EXPECTED_READY_ORDERS:
        raise ConceptSynthesisError(f"readiness rule did not produce the spec's expected ready set: {requested_orders}")
    authorized_orders = tuple(authorized_orders)
    if authorized_orders not in {PILOT_CONCEPT_ORDERS, FINAL_CONCEPT_ORDERS}:
        raise ConceptSynthesisError("unsupported concept-card authorization stage")
    authorized = [row for row in readiness if row["concept_readiness"] == "CONCEPT_READY" and row["deep_validation_order"] in authorized_orders]
    if [row["deep_validation_order"] for row in authorized] != list(authorized_orders):
        raise ConceptSynthesisError(f"authorization requires ready orders {list(authorized_orders)}")
    capture_rows = capture.get("families")
    if not isinstance(capture_rows, list) or len(capture_rows) != len(authorized):
        count_word = "five" if len(authorized) == 5 else "eight"
        stage_name = "pilot" if authorized_orders == PILOT_CONCEPT_ORDERS else "final"
        raise ConceptSynthesisError(f"CONCEPT_CAPTURE must contain exactly the {count_word} authorized {stage_name} families")
    expected_ids = [row["family_id"] for row in authorized]
    if [row.get("family_id") for row in capture_rows] != expected_ids:
        raise ConceptSynthesisError(f"CONCEPT_CAPTURE membership/order must be exactly ready orders {list(authorized_orders)}")
    allowed_capture_fields = {"family_id", "concept_statement", "solution_primitives", "synthesis_notes"}
    if any(set(row) != allowed_capture_fields for row in capture_rows):
        raise ConceptSynthesisError("capture families may contain only family_id and interpretive fields")

    packs = {row["family_id"]: row for row in payload["commercial_validation_packs"]}
    clusters_by_family: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    diffs_by_family: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in payload["pain_clusters"]:
        clusters_by_family[row["family_id"]].append(row)
    for row in payload["differentiation_hypotheses"]:
        diffs_by_family[row["family_id"]].append(row)
    family_maps, owner_by_ref = _family_ref_maps(payload)

    for family in capture_rows:
        family_id = family["family_id"]
        if not isinstance(family.get("concept_statement"), str) or not family["concept_statement"].strip():
            raise ConceptSynthesisError(f"concept_statement missing for {family_id}")
        if not family["concept_statement"].lstrip().lower().startswith("candidate solution hypothesis:"):
            raise ConceptSynthesisError(f"concept_statement must be explicitly framed as a candidate hypothesis: {family_id}")
        if not isinstance(family.get("synthesis_notes"), str) or not family["synthesis_notes"].strip():
            raise ConceptSynthesisError(f"synthesis_notes missing for {family_id}")
        primitives = family.get("solution_primitives")
        if not isinstance(primitives, list) or not 1 <= len(primitives) <= 3:
            raise ConceptSynthesisError(f"solution_primitives must contain 1..3 rows for {family_id}")
        all_text = [family["concept_statement"], family["synthesis_notes"]]
        all_text.extend(item.get("primitive_text", "") for item in primitives if isinstance(item, dict))
        for text in all_text:
            if not isinstance(text, str) or DISALLOWED.search(text):
                raise ConceptSynthesisError(f"disallowed conclusion language in CONCEPT_CAPTURE for {family_id}")
            if NUMERIC_PRICE.search(text):
                raise ConceptSynthesisError(f"numeric pricing is not allowed in interpretive capture for {family_id}")

        pack = packs[family_id]
        job_ids = _buyer_evidence_ids(pack)
        pain_ids = _pain_reference_ids(clusters_by_family[family_id])
        diff_rows = {row["hypothesis_id"]: row for row in diffs_by_family[family_id]}
        has_job_primitive = False
        has_pain_or_diff_primitive = False
        for primitive in primitives:
            if not isinstance(primitive, dict) or set(primitive) != {"primitive_text", "basis_type", "basis_refs"}:
                raise ConceptSynthesisError(f"primitive schema is invalid for {family_id}")
            basis_type = primitive["basis_type"]
            refs = primitive["basis_refs"]
            if basis_type not in BASIS_TYPES or not isinstance(primitive["primitive_text"], str) or not primitive["primitive_text"].strip():
                raise ConceptSynthesisError(f"primitive type/text is invalid for {family_id}")
            if not isinstance(refs, list) or not refs or len(set(refs)) != len(refs):
                raise ConceptSynthesisError(f"basis_refs must be a nonempty unique list for {family_id}")
            for ref in refs:
                if ref not in family_maps[family_id]:
                    owner = owner_by_ref.get(ref)
                    if owner is not None:
                        raise ConceptSynthesisError(f"cross-family basis ref {ref} from {owner} used in {family_id}")
                    raise ConceptSynthesisError(f"unknown basis ref {ref!r} for {family_id}")
            if basis_type == "JOB_ENABLEMENT":
                if not job_ids.intersection(refs):
                    raise ConceptSynthesisError(f"JOB_ENABLEMENT must cite buyer/job evidence for {family_id}")
                has_job_primitive = True
            elif basis_type == "PAIN_RESPONSE":
                if not pain_ids.intersection(refs):
                    raise ConceptSynthesisError(f"PAIN_RESPONSE must cite same-family pain evidence/cluster for {family_id}")
                has_pain_or_diff_primitive = True
            else:
                cited_diffs = [ref for ref in refs if ref in diff_rows]
                if not cited_diffs:
                    raise ConceptSynthesisError(f"DIFFERENTIATION_DIRECTION must cite a same-family hypothesis for {family_id}")
                if not any(set(diff_rows[ref].get("evidence_ids", [])) & set(refs) for ref in cited_diffs):
                    raise ConceptSynthesisError(f"DIFFERENTIATION_DIRECTION must cite hypothesis evidence for {family_id}")
                has_pain_or_diff_primitive = True
        if not pack.get("buyer_job_hypotheses") or not has_job_primitive or not has_pain_or_diff_primitive:
            raise ConceptSynthesisError(f"concept needs buyer/job and pain or differentiation basis for {family_id}")
    return {row["family_id"]: row for row in capture_rows}


def _uncertainty_and_questions(
    payer_role: str, pain_status: str, commercial_state: str, feasibility_status: str,
) -> tuple[list[str], list[str]]:
    flags: list[str] = []
    if "UNKNOWN" in payer_role.upper():
        flags.append("PAYER_UNKNOWN")
    if pain_status == "LIMITED_SAMPLE":
        flags.append("PAIN_LIMITED")
    if commercial_state != "VERIFIED_PAID_SIGNAL":
        flags.append("PAID_SIGNAL_NOT_ESTABLISHED")
    if feasibility_status == "PARTIAL":
        flags.append("FEASIBILITY_PARTIAL")
    if commercial_state == "COMPARATIVE_SIGNAL_ONLY":
        flags.append("COMPARATIVE_ONLY")

    questions: list[str] = []
    if "PAYER_UNKNOWN" in flags or "PAID_SIGNAL_NOT_ESTABLISHED" in flags:
        questions.append("Who, if anyone, pays for this job, and what evidence would establish willingness to pay?")
    if "PAIN_LIMITED" in flags:
        questions.append("Do independent reports for the same pain theme recur, and what severity or workaround evidence is still needed?")
    if "FEASIBILITY_PARTIAL" in flags:
        questions.append("Which platform, dependency, integration/API, or license facts remain to be validated from sources?")
    if commercial_state != "VERIFIED_PAID_SIGNAL":
        questions.append("Which directly comparable paid offerings and source-native pricing terms can be verified?")
    return flags, questions[:4]


def _normalize_cards(
    payload: Mapping[str, Sequence[Mapping[str, Any]]],
    readiness: Sequence[Mapping[str, Any]],
    capture_rows: Mapping[str, Mapping[str, Any]],
    authorized_orders: Sequence[int] = PILOT_CONCEPT_ORDERS,
) -> list[dict[str, Any]]:
    authorized_orders = tuple(authorized_orders)
    if authorized_orders not in {PILOT_CONCEPT_ORDERS, FINAL_CONCEPT_ORDERS}:
        raise ConceptSynthesisError("unsupported concept-card authorization stage")
    packs = {row["family_id"]: row for row in payload["commercial_validation_packs"]}
    readiness_by_id = {row["family_id"]: row for row in readiness}
    children: dict[str, dict[str, list[Mapping[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for table in ("pain_clusters", "market_alternatives", "differentiation_hypotheses", "validation_evidence"):
        for row in payload[table]:
            children[row["family_id"]][table].append(row)

    cards: list[dict[str, Any]] = []
    for matrix_row in readiness:
        order = matrix_row["deep_validation_order"]
        if matrix_row["concept_readiness"] != "CONCEPT_READY" or order not in authorized_orders:
            continue
        family_id = matrix_row["family_id"]
        pack = packs[family_id]
        capture = capture_rows[family_id]
        family_children = children[family_id]
        clusters = sorted(family_children["pain_clusters"], key=lambda row: row["cluster_id"])
        alternatives = sorted(family_children["market_alternatives"], key=lambda row: row["alternative_id"])
        differentiations = sorted(family_children["differentiation_hypotheses"], key=lambda row: row["hypothesis_id"])
        state = matrix_row["commercial_signal_state"]
        pain_status = matrix_row["pain_status"]
        feasibility_status = matrix_row["feasibility_status"]
        flags, questions = _uncertainty_and_questions(pack["buyer_or_payer_role"], pain_status, state, feasibility_status)
        ref_ids = set()
        for hypothesis in pack.get("buyer_job_hypotheses", []):
            ref_ids.update(hypothesis.get("evidence_ids", []))
        for cluster in clusters:
            ref_ids.add(cluster["cluster_id"])
            ref_ids.update(cluster.get("evidence_ids", []))
        for row in alternatives:
            ref_ids.add(row["alternative_id"])
            ref_ids.update(row.get("evidence_ids", []))
        for row in differentiations:
            ref_ids.add(row["hypothesis_id"])
            ref_ids.update(row.get("evidence_ids", []))
        for fact in pack.get("feasibility_facts", []):
            ref_ids.update(fact.get("evidence_ids", []))
        for primitive in capture["solution_primitives"]:
            ref_ids.update(primitive["basis_refs"])
        evidence_refs = sorted(ref_ids)
        concept_id = "concept57_" + hashlib.sha256(family_id.encode("utf-8")).hexdigest()[:20]
        cards.append({
            "concept_id": concept_id,
            "family_id": family_id,
            "deep_validation_order": order,
            "consensus_rank": pack["consensus_rank"],
            "canonical_topic_key": pack["canonical_topic_key"],
            "concept_readiness": matrix_row["concept_readiness"],
            "target_user_role": pack["primary_user_role"],
            "payer_role": pack["buyer_or_payer_role"],
            "buyer_job_basis": pack["buyer_job_hypotheses"],
            "pain_basis": {
                "family_pain_status": pain_status,
                "pain_signal_summary": pack["pain_signal_summary"],
                "clusters": clusters,
            },
            "commercial_signal_state": state,
            "retained_alternative_ids": pack["retained_alternative_ids"],
            "differentiation_basis": differentiations,
            "feasibility_status": feasibility_status,
            "feasibility_basis": pack["feasibility_facts"],
            "concept_statement": capture["concept_statement"],
            "solution_primitives": capture["solution_primitives"],
            "uncertainty_flags": flags,
            "next_validation_questions": questions,
            "evidence_refs": evidence_refs,
            "synthesis_notes": capture["synthesis_notes"],
        })
    if [row["deep_validation_order"] for row in cards] != list(authorized_orders):
        raise ConceptSynthesisError(f"normalized concept-card membership must be exactly orders {list(authorized_orders)}")
    return cards


def _semantic_checks(
    payload: Mapping[str, Sequence[Mapping[str, Any]]],
    readiness: Sequence[Mapping[str, Any]],
    capture: Mapping[str, Any],
    capture_rows: Mapping[str, Mapping[str, Any]],
    cards: Sequence[Mapping[str, Any]],
    authorized_orders: Sequence[int] = PILOT_CONCEPT_ORDERS,
) -> dict[str, bool]:
    authorized_orders = tuple(authorized_orders)
    packs = {row["family_id"]: row for row in payload["commercial_validation_packs"]}
    matrix = {row["family_id"]: row for row in readiness}
    owner_by_ref = _family_ref_maps(payload)[1]
    alternatives: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    differentiations: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    clusters: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for table, target in (
        ("market_alternatives", alternatives),
        ("differentiation_hypotheses", differentiations),
        ("pain_clusters", clusters),
    ):
        for row in payload[table]:
            target[row["family_id"]].append(row)
    for family_id in packs:
        differentiations[family_id].sort(key=lambda row: row["hypothesis_id"])
        clusters[family_id].sort(key=lambda row: row["cluster_id"])
        alternatives[family_id].sort(key=lambda row: row["alternative_id"])

    authorized_ids = [row["family_id"] for row in readiness if row["concept_readiness"] == "CONCEPT_READY" and row["deep_validation_order"] in authorized_orders]
    capture_ids = [row.get("family_id") for row in capture.get("families", [])]
    capture_exact = (
        capture_ids == authorized_ids
        and len(capture_ids) == len(authorized_orders)
        and all(set(row) == {"family_id", "concept_statement", "solution_primitives", "synthesis_notes"} for row in capture["families"])
    )
    cards_by_id = {row["family_id"]: row for row in cards}
    basis_refs_valid = True
    capture_fields_match = True
    upstream_fields_match = True
    uncertainty_match = True
    limited_pain_visible = True
    forbidden_language_absent = True
    for family_id in authorized_ids:
        source = packs[family_id]
        row = cards_by_id[family_id]
        source_capture = capture_rows[family_id]
        source_pain = source["dimension_statuses"]["pain_prevalence_sample"]
        row_matrix = matrix[family_id]
        capture_fields_match &= all(row[field] == source_capture[field] for field in ("concept_statement", "solution_primitives", "synthesis_notes"))
        upstream_fields_match &= (
            row["target_user_role"] == source["primary_user_role"]
            and row["payer_role"] == source["buyer_or_payer_role"]
            and row["buyer_job_basis"] == source["buyer_job_hypotheses"]
            and row["retained_alternative_ids"] == source["retained_alternative_ids"]
            and row["feasibility_status"] == source["feasibility_evidence_status"]
            and row["feasibility_basis"] == source["feasibility_facts"]
            and row["commercial_signal_state"] == row_matrix["commercial_signal_state"]
        )
        limited_pain_visible &= (
            row["pain_basis"]["family_pain_status"] == source_pain
            and row["pain_basis"]["pain_signal_summary"] == source["pain_signal_summary"]
            and row["pain_basis"]["clusters"] == clusters[family_id]
            and (source_pain != "LIMITED_SAMPLE" or all(cluster["sample_status"] == "LIMITED_SAMPLE" for cluster in row["pain_basis"]["clusters"]))
        )
        flags, questions = _uncertainty_and_questions(
            source["buyer_or_payer_role"], source_pain, row_matrix["commercial_signal_state"], source["feasibility_evidence_status"],
        )
        uncertainty_match &= row["uncertainty_flags"] == flags and row["next_validation_questions"] == questions and len(questions) <= 4
        for primitive in source_capture["solution_primitives"]:
            refs = primitive["basis_refs"]
            basis_refs_valid &= bool(refs) and all(owner_by_ref.get(ref) == family_id for ref in refs)
            if primitive["basis_type"] == "JOB_ENABLEMENT":
                basis_refs_valid &= bool(_buyer_evidence_ids(source).intersection(refs))
            elif primitive["basis_type"] == "PAIN_RESPONSE":
                basis_refs_valid &= bool(_pain_reference_ids(clusters[family_id]).intersection(refs))
            else:
                cited = [item for item in differentiations[family_id] if item["hypothesis_id"] in refs]
                basis_refs_valid &= bool(cited) and any(set(item.get("evidence_ids", [])) & set(refs) for item in cited)
        text_fields = [source_capture["concept_statement"], source_capture["synthesis_notes"]]
        text_fields.extend(item["primitive_text"] for item in source_capture["solution_primitives"])
        forbidden_language_absent &= all(not DISALLOWED.search(text) and not NUMERIC_PRICE.search(text) for text in text_fields)
    return {
        "capture_membership_and_fields_exact": capture_exact,
        "same_family_basis_refs_and_types_valid": basis_refs_valid,
        "cards_preserve_y55_upstream_fields": upstream_fields_match,
        "interpretive_fields_match_capture": capture_fields_match,
        "limited_pain_remains_explicit": limited_pain_visible,
        "uncertainty_flags_and_questions_recompute": uncertainty_match,
        "forbidden_conclusions_and_numeric_prices_absent": forbidden_language_absent,
    }


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return ("".join(canonical_json(row) + "\n" for row in rows)).encode("utf-8")


def _csv_bytes(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> bytes:
    from io import StringIO

    stream = StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n", extrasaction="raise")
    writer.writeheader()
    for row in rows:
        encoded = {}
        for column in columns:
            value = row.get(column)
            encoded[column] = NULL_TOKEN if value is None else canonical_json(value) if isinstance(value, (list, dict)) else value
        writer.writerow(encoded)
    return stream.getvalue().encode("utf-8")


def _write_sqlite(path: Path, readiness: Sequence[Mapping[str, Any]], cards: Sequence[Mapping[str, Any]], metadata: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    connection = sqlite3.connect(path)
    with connection:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("""CREATE TABLE concept_readiness_matrix (
            family_id TEXT PRIMARY KEY, deep_validation_order INTEGER NOT NULL UNIQUE,
            consensus_rank INTEGER NOT NULL, canonical_topic_key TEXT NOT NULL,
            validation_cohort TEXT NOT NULL, buyer_job_status TEXT NOT NULL, pain_status TEXT NOT NULL,
            feasibility_status TEXT NOT NULL, retained_alternative_count INTEGER NOT NULL,
            differentiation_count INTEGER NOT NULL, commercial_signal_state TEXT NOT NULL,
            concept_readiness TEXT NOT NULL, readiness_reasons TEXT NOT NULL
        )""")
        connection.execute("""CREATE TABLE opportunity_concept_cards (
            concept_id TEXT PRIMARY KEY, family_id TEXT NOT NULL UNIQUE,
            deep_validation_order INTEGER NOT NULL UNIQUE, consensus_rank INTEGER NOT NULL,
            canonical_topic_key TEXT NOT NULL, concept_readiness TEXT NOT NULL CHECK(concept_readiness='CONCEPT_READY'),
            target_user_role TEXT NOT NULL, payer_role TEXT NOT NULL, buyer_job_basis TEXT NOT NULL,
            pain_basis TEXT NOT NULL, commercial_signal_state TEXT NOT NULL,
            retained_alternative_ids TEXT NOT NULL, differentiation_basis TEXT NOT NULL,
            feasibility_status TEXT NOT NULL, feasibility_basis TEXT NOT NULL,
            concept_statement TEXT NOT NULL, solution_primitives TEXT NOT NULL,
            uncertainty_flags TEXT NOT NULL, next_validation_questions TEXT NOT NULL,
            evidence_refs TEXT NOT NULL, synthesis_notes TEXT NOT NULL,
            FOREIGN KEY(family_id) REFERENCES concept_readiness_matrix(family_id)
        )""")
        connection.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value_json TEXT NOT NULL)")
        connection.executemany(
            f"INSERT INTO concept_readiness_matrix ({','.join(READINESS_COLUMNS)}) VALUES ({','.join('?' for _ in READINESS_COLUMNS)})",
            [tuple(canonical_json(row[column]) if isinstance(row.get(column), (dict, list)) else row.get(column) for column in READINESS_COLUMNS) for row in readiness],
        )
        connection.executemany(
            f"INSERT INTO opportunity_concept_cards ({','.join(CONCEPT_COLUMNS)}) VALUES ({','.join('?' for _ in CONCEPT_COLUMNS)})",
            [tuple(canonical_json(row[column]) if isinstance(row.get(column), (dict, list)) else row.get(column) for column in CONCEPT_COLUMNS) for row in cards],
        )
        connection.executemany("INSERT INTO metadata VALUES (?,?)", [(key, canonical_json(value)) for key, value in sorted(metadata.items())])
        connection.commit()
    connection.close()


def _decode_output_row(row: Mapping[str, Any], json_fields: set[str]) -> dict[str, Any]:
    result = dict(row)
    for key in json_fields:
        if result.get(key) is not None:
            result[key] = json.loads(result[key])
    return result


def _output_checks(output: Path, readiness: Sequence[Mapping[str, Any]], cards: Sequence[Mapping[str, Any]]) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    jsonl_rows = {
        "concept_readiness_matrix": _read_jsonl(output / "concept_readiness_matrix.jsonl"),
        "opportunity_concept_cards": _read_jsonl(output / "opportunity_concept_cards.jsonl"),
    }
    expected = {"concept_readiness_matrix": list(readiness), "opportunity_concept_cards": list(cards)}
    checks["jsonl_rows_match_normalized_rows"] = jsonl_rows == expected
    checks["jsonl_csv_counts_match"] = True
    for name, rows, columns in (
        ("concept_readiness_matrix", readiness, READINESS_COLUMNS),
        ("opportunity_concept_cards", cards, CONCEPT_COLUMNS),
    ):
        checks[f"{name}_csv_bytes_match"] = (output / f"{name}.csv").read_bytes() == _csv_bytes(rows, columns)
        checks["jsonl_csv_counts_match"] &= len(jsonl_rows[name]) == len(rows)

    connection = sqlite3.connect(f"file:{(output / 'concept_synthesis.sqlite').resolve().as_posix()}?mode=ro", uri=True)
    with connection:
        connection.row_factory = sqlite3.Row
        db_readiness = [
            _decode_output_row(dict(row), READINESS_JSON_FIELDS)
            for row in connection.execute("SELECT * FROM concept_readiness_matrix ORDER BY deep_validation_order")
        ]
        db_cards = [
            _decode_output_row(dict(row), CONCEPT_JSON_FIELDS)
            for row in connection.execute("SELECT * FROM opportunity_concept_cards ORDER BY deep_validation_order")
        ]
        checks["sqlite_readiness_matches_jsonl"] = db_readiness == jsonl_rows["concept_readiness_matrix"]
        checks["sqlite_cards_match_jsonl"] = db_cards == jsonl_rows["opportunity_concept_cards"]
        checks["sqlite_integrity_ok"] = connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        checks["sqlite_foreign_keys_ok"] = not connection.execute("PRAGMA foreign_key_check").fetchall()
    connection.close()
    return checks


def _schema_text(authorized_orders: Sequence[int] = PILOT_CONCEPT_ORDERS) -> str:
    authorized_orders = tuple(authorized_orders)
    stage = "pilot" if authorized_orders == PILOT_CONCEPT_ORDERS else "final"
    lines = [
        "# YEE-57 Concept Synthesis Schema v0.1", "",
        f"Deterministic concept-readiness matrix for all accepted YEE-55 COHORT_A families and evidence-linked candidate concept cards for the authorized {stage} families only.",
        "Candidate concepts are hypotheses for review, not recommendations or market conclusions. No score, ranking, price recommendation, TAM, revenue, profit, or prevalence estimate is produced.",
        "", "## Canonical inputs", "",
    ]
    lines.extend(f"- `{name}` SHA-256 `{digest}` (read-only)" for name, digest in sorted(INPUT_HASHES.items()))
    lines += ["", "## Deterministic readiness", "",
        "CONCEPT_READY iff buyer_job_status ∈ {EVIDENCED, LIMITED_SAMPLE}; pain_status ∈ {REPEATED_SAMPLE, LIMITED_SAMPLE}; retained_alternative_count > 0 OR differentiation_count > 0; and feasibility_status ∈ {COMPLETE, PARTIAL}.",
        "Otherwise readiness_reasons contains every applicable exact enum: BUYER_JOB_NOT_ESTABLISHED, PAIN_NOT_ESTABLISHED, COMPARATIVE_CONTEXT_NOT_ESTABLISHED, FEASIBILITY_NOT_ESTABLISHED.",
        "Commercial signal states are VERIFIED_PAID_SIGNAL, COMPARATIVE_SIGNAL_ONLY, and NO_RETAINED_COMPARATIVE_SIGNAL. They are evidence states, not scores.",
        "", "## Tables", "", "### concept_readiness_matrix", "",
        "| Field | Meaning |", "|---|---|",
    ]
    descriptions = {
        "family_id": "Accepted YEE-55 family identity; primary key.",
        "deep_validation_order": "Inherited YEE-55 order; deterministic output order.",
        "consensus_rank": "Inherited rank, not recomputed or used to rank this output.",
        "canonical_topic_key": "Inherited canonical topic.",
        "validation_cohort": "Inherited cohort; expected COHORT_A.",
        "buyer_job_status": "Copied YEE-55 dimension_statuses.buyer_job.",
        "pain_status": "Copied YEE-55 dimension_statuses.pain_prevalence_sample.",
        "feasibility_status": "Copied YEE-55 feasibility_evidence_status.",
        "retained_alternative_count": "Count of same-family YEE-55 market_alternatives rows.",
        "differentiation_count": "Count of same-family YEE-55 differentiation_hypotheses rows.",
        "commercial_signal_state": "Deterministic evidence-state over same-family alternatives; missing price is not free.",
        "concept_readiness": "CONCEPT_READY or NEEDS_EVIDENCE from the fixed gate.",
        "readiness_reasons": "Ordered array of exact reasons; empty for CONCEPT_READY.",
    }
    lines.extend(f"| `{field}` | {descriptions[field]} |" for field in READINESS_COLUMNS)
    lines += ["", "### opportunity_concept_cards", "", "| Field | Meaning |", "|---|---|",
        "| `concept_id` | Deterministic ID derived from family_id. |",
        "| `family_id`, `deep_validation_order`, `consensus_rank`, `canonical_topic_key` | Inherited YEE-55 identity fields. |",
        "| `concept_readiness` | Derived state; cards exist only for CONCEPT_READY. |",
        "| `target_user_role`, `payer_role` | Exact copies of YEE-55 primary_user_role and buyer_or_payer_role. |",
        "| `buyer_job_basis` | Exact YEE-55 buyer_job_hypotheses. |",
        "| `pain_basis` | Exact family pain status/summary and same-family YEE-55 pain clusters. |",
        "| `commercial_signal_state` | Derived evidence state; never a recommended business model or price. |",
        "| `retained_alternative_ids`, `differentiation_basis` | Same-family YEE-55 comparative records. |",
        "| `feasibility_status`, `feasibility_basis` | Exact YEE-55 feasibility status and facts. |",
        "| `concept_statement`, `solution_primitives`, `synthesis_notes` | Interpretive fields sourced only from CONCEPT_CAPTURE. |",
        "| `uncertainty_flags`, `next_validation_questions` | Deterministically derived flags/prompts; not new findings. |",
        "| `evidence_refs` | Sorted same-family YEE-55 evidence/cluster/alternative/hypothesis IDs. |",
        "", "## Capture and reference semantics", "",
        f"CONCEPT_CAPTURE version `{CAPTURE_VERSION}` contains exactly the authorized {stage} families and only family_id plus concept_statement, solution_primitives, and synthesis_notes. Derived fields are never manually authored.",
        "Solution primitive basis_type is PAIN_RESPONSE, DIFFERENTIATION_DIRECTION, or JOB_ENABLEMENT. Every basis_refs ID must resolve within the same family; PAIN_RESPONSE cites pain evidence/cluster, DIFFERENTIATION_DIRECTION cites its hypothesis and evidence, and JOB_ENABLEMENT cites buyer/job evidence.",
        "Pain status and cluster sample statuses are preserved verbatim. LIMITED_SAMPLE stays limited; UNKNOWN and missing remain unknown/missing. CSV nulls use \\N; JSON/SQLite preserve null values.",
        "", f"## {stage.title()} authorization", "",
        f"Readiness matrix: all 15 rows. Concept cards: exactly deep_validation_order {', '.join(map(str, authorized_orders))}.",
    ]
    return "\n".join(lines) + "\n"


def _report_text(readiness: Sequence[Mapping[str, Any]], cards: Sequence[Mapping[str, Any]], qa: Mapping[str, Any]) -> str:
    ready = [row["deep_validation_order"] for row in readiness if row["concept_readiness"] == "CONCEPT_READY"]
    needs = [row["deep_validation_order"] for row in readiness if row["concept_readiness"] == "NEEDS_EVIDENCE"]
    lines = [
        "# YEE-57 Concept Synthesis Pilot", "", f"Status: **{qa['status']}**", "",
        "Authorization: deterministic readiness for all 15 accepted YEE-55 COHORT_A families; concept cards for orders 1..5 only.",
        "", f"- Readiness rows: {len(readiness)} ({len(ready)} CONCEPT_READY / {len(needs)} NEEDS_EVIDENCE)",
        f"- CONCEPT_READY orders: `{ready}`", f"- NEEDS_EVIDENCE orders: `{needs}`",
        f"- Pilot concept cards: {len(cards)} for orders `{[row['deep_validation_order'] for row in cards]}`",
        "- Read-only YEE-55 SQLite and six JSONL exports reconciled before synthesis; pinned hashes stable before/after.",
        "- SQLite integrity/FK, CSV/JSONL/SQLite row reconciliation, same-family basis references, and deterministic replay: `PASS`.",
        "- No new research or API/web calls; no ranking, score, pricing recommendation, TAM/revenue/profit, or product recommendation.",
        "", "## Readiness matrix", "", "| Order | Rank | Family | Buyer | Pain | Feasibility | Alternatives | Differentiation | Commercial state | Readiness | Reasons |", "|---:|---:|---|---|---|---|---:|---:|---|---|---|",
    ]
    for row in readiness:
        lines.append(
            f"| {row['deep_validation_order']} | {row['consensus_rank']} | `{row['canonical_topic_key']}` | {row['buyer_job_status']} | {row['pain_status']} | {row['feasibility_status']} | {row['retained_alternative_count']} | {row['differentiation_count']} | {row['commercial_signal_state']} | {row['concept_readiness']} | {', '.join(row['readiness_reasons']) or '—'} |"
        )
    lines += ["", "## Pilot concept-card coverage", "", "| Order | Family | Commercial state | Uncertainty flags |", "|---:|---|---|---|"]
    for card in cards:
        lines.append(f"| {card['deep_validation_order']} | `{card['canonical_topic_key']}` | {card['commercial_signal_state']} | {', '.join(card['uncertainty_flags']) or '—'} |")
    lines += ["", "## Interpretation limits", "",
        "Concept statements and primitives are evidence-linked hypotheses for supervisor review. Limited pain evidence is not a prevalence estimate. Missing price is not FREE; comparative coverage is not proof of market conditions. Feasibility facts are not estimates of implementation effort, cost, or probability.",
        "", "## QA", "", "- Full suite evidence is linked in the PR.",
        f"- Canonical YEE-55 input SHA-256 stable: `{qa['checks']['canonical_input_hashes_stable']}`.",
        f"- Deterministic replay byte-identical: `{qa['checks']['deterministic_replay_byte_identical']}`.",
        "", "See `QA_RESULT.json`, `DATASET_MANIFEST.json`, and `CONCEPT_CAPTURE.json` for exact checks, hashes, and the authorized interpretive capture.", "",
    ]
    return "\n".join(lines)


def _final_report_text(readiness: Sequence[Mapping[str, Any]], cards: Sequence[Mapping[str, Any]], qa: Mapping[str, Any]) -> str:
    ready = [row["deep_validation_order"] for row in readiness if row["concept_readiness"] == "CONCEPT_READY"]
    needs = [row["deep_validation_order"] for row in readiness if row["concept_readiness"] == "NEEDS_EVIDENCE"]
    orders = [row["deep_validation_order"] for row in cards]
    return "\n".join([
        "# YEE-57 Concept Synthesis Final Report", "", f"Status: **{qa['status']}**", "",
        "Authorization: deterministic readiness for all 15 accepted YEE-55 COHORT_A families; concept cards for all eight authorized CONCEPT_READY orders.",
        "", f"- Readiness rows: {len(readiness)} ({len(ready)} CONCEPT_READY / {len(needs)} NEEDS_EVIDENCE)",
        f"- CONCEPT_READY orders: `{ready}`", f"- NEEDS_EVIDENCE orders: `{needs}`",
        f"- Concept cards: {len(cards)} for orders `{orders}`",
        "- Accepted pilot orders 1..5 and `PILOT_REPORT.md` are preserved unchanged.",
        "- Read-only YEE-55 SQLite and six JSONL exports reconciled; pinned input hashes stable before/after.",
        "- SQLite integrity/FK, JSONL/CSV/SQLite reconciliation, same-family basis references, and deterministic replay: `PASS`.",
        "- No new research or API/web calls; no ranking, score, pricing recommendation, TAM/revenue/profit, or product recommendation.",
        "", "## Readiness matrix", "", "| Order | Rank | Family | Buyer | Pain | Feasibility | Alternatives | Differentiation | Commercial state | Readiness | Reasons |", "|---:|---:|---|---|---|---|---:|---:|---|---|---|",
        *[
            f"| {row['deep_validation_order']} | {row['consensus_rank']} | `{row['canonical_topic_key']}` | {row['buyer_job_status']} | {row['pain_status']} | {row['feasibility_status']} | {row['retained_alternative_count']} | {row['differentiation_count']} | {row['commercial_signal_state']} | {row['concept_readiness']} | {', '.join(row['readiness_reasons']) or '—'} |"
            for row in readiness
        ],
        "", "## Final concept-card coverage", "", "| Order | Family | Commercial state | Uncertainty flags |", "|---:|---|---|---|",
        *[
            f"| {card['deep_validation_order']} | `{card['canonical_topic_key']}` | {card['commercial_signal_state']} | {', '.join(card['uncertainty_flags']) or '—'} |"
            for card in cards
        ],
        "", "## Interpretation limits", "",
        "Concept statements and primitives are evidence-linked hypotheses for supervisor review. Limited pain evidence is not a prevalence estimate. Missing price is not FREE; comparative coverage is not proof of market conditions. Feasibility facts are not estimates of implementation effort, cost, or probability.",
        "", "## QA", "", "- Full suite evidence is linked in the PR.",
        f"- Canonical YEE-55 input SHA-256 stable: `{qa['checks']['canonical_input_hashes_stable']}`.",
        f"- Accepted pilot orders 1..5 unchanged: `{qa['checks']['accepted_pilot_orders_1_to_5_unchanged']}`.",
        f"- Deterministic replay byte-identical: `{qa['checks']['deterministic_replay_byte_identical']}`.",
        "", "See `QA_RESULT.json`, `DATASET_MANIFEST.json`, `CONCEPT_CAPTURE.json`, and retained `PILOT_REPORT.md` for exact evidence.", "",
    ])


def _accepted_pilot_preserved(
    accepted_pilot_dir: Path,
    payload: Mapping[str, Sequence[Mapping[str, Any]]],
    readiness: Sequence[Mapping[str, Any]],
    capture: Mapping[str, Any],
    cards: Sequence[Mapping[str, Any]],
) -> bool:
    try:
        pilot_capture = json.loads((accepted_pilot_dir / "CONCEPT_CAPTURE.json").read_text(encoding="utf-8-sig"))
        pilot_readiness = _read_jsonl(accepted_pilot_dir / "concept_readiness_matrix.jsonl")
        pilot_cards = _read_jsonl(accepted_pilot_dir / "opportunity_concept_cards.jsonl")
        pilot_qa = json.loads((accepted_pilot_dir / "QA_RESULT.json").read_text(encoding="utf-8-sig"))
        pilot_rows = _validate_capture(pilot_capture, payload, readiness, PILOT_CONCEPT_ORDERS)
        pilot_expected_cards = _normalize_cards(payload, readiness, pilot_rows, PILOT_CONCEPT_ORDERS)
    except (OSError, json.JSONDecodeError, ConceptSynthesisError, KeyError, TypeError):
        return False
    return (
        pilot_qa.get("status") == "PASS"
        and all(pilot_qa.get("checks", {}).values())
        and len(pilot_readiness) == 15
        and len(pilot_cards) == len(PILOT_CONCEPT_ORDERS)
        and pilot_readiness == list(readiness)
        and pilot_cards == pilot_expected_cards
        and pilot_cards == list(cards[:len(PILOT_CONCEPT_ORDERS)])
        and list(pilot_capture.get("families", [])) == list(capture.get("families", [])[:len(PILOT_CONCEPT_ORDERS)])
        and all(_output_checks(accepted_pilot_dir, pilot_readiness, pilot_cards).values())
    )


def _render(
    input_dir: Path, capture_path: Path, output_dir: Path, *, deterministic_replay: bool,
    stage: str = "PILOT", accepted_pilot_dir: Path | None = None,
) -> dict[str, Any]:
    if stage not in {"PILOT", "FINAL"}:
        raise ConceptSynthesisError(f"unsupported YEE-57 render stage: {stage}")
    authorized_orders = PILOT_CONCEPT_ORDERS if stage == "PILOT" else FINAL_CONCEPT_ORDERS
    payload, hashes_before, input_checks = _load_inputs(input_dir)
    capture_bytes = capture_path.read_bytes()
    try:
        capture = json.loads(capture_bytes.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ConceptSynthesisError("CONCEPT_CAPTURE is not valid UTF-8 JSON") from exc
    readiness = derive_readiness_matrix(payload)
    ready_orders = [row["deep_validation_order"] for row in readiness if row["concept_readiness"] == "CONCEPT_READY"]
    if tuple(ready_orders) != EXPECTED_READY_ORDERS:
        raise ConceptSynthesisError(f"deterministic readiness mismatch: {ready_orders}")
    capture_rows = _validate_capture(capture, payload, readiness, authorized_orders)
    cards = _normalize_cards(payload, readiness, capture_rows, authorized_orders)
    semantic_checks = _semantic_checks(payload, readiness, capture, capture_rows, cards, authorized_orders)
    if stage == "FINAL" and accepted_pilot_dir is None:
        raise ConceptSynthesisError("final render requires the accepted pilot directory")
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "concept_readiness_matrix.jsonl").write_bytes(_jsonl_bytes(readiness))
    (output_dir / "concept_readiness_matrix.csv").write_bytes(_csv_bytes(readiness, READINESS_COLUMNS))
    (output_dir / "opportunity_concept_cards.jsonl").write_bytes(_jsonl_bytes(cards))
    (output_dir / "opportunity_concept_cards.csv").write_bytes(_csv_bytes(cards, CONCEPT_COLUMNS))
    (output_dir / "CONCEPT_CAPTURE.json").write_bytes(capture_bytes if capture_bytes.endswith(b"\n") else capture_bytes + b"\n")
    _write_sqlite(output_dir / "concept_synthesis.sqlite", readiness, cards, {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "capture_version": CAPTURE_VERSION,
        "baseline_commit": BASELINE_COMMIT,
        "authorization": "FINAL_READINESS_ALL_15_CONCEPT_CARDS_ORDERS_1_2_3_4_5_8_10_11" if stage == "FINAL" else "PILOT_READINESS_ALL_15_CONCEPT_CARDS_ORDERS_1_TO_5",
        "canonical_input_hashes": hashes_before,
        "concept_capture_sha256": hashlib.sha256(capture_bytes).hexdigest(),
    })
    (output_dir / "CONCEPT_SYNTHESIS_SCHEMA.md").write_text(_schema_text(authorized_orders), encoding="utf-8", newline="\n")
    if stage == "FINAL":
        pilot_report = accepted_pilot_dir / "PILOT_REPORT.md"
        if not pilot_report.is_file():
            raise ConceptSynthesisError("accepted pilot PILOT_REPORT.md is missing")
        (output_dir / "PILOT_REPORT.md").write_bytes(pilot_report.read_bytes())
        accepted_pilot_unchanged = _accepted_pilot_preserved(accepted_pilot_dir, payload, readiness, capture, cards)
        accepted_pilot_report_unchanged = (output_dir / "PILOT_REPORT.md").read_bytes() == pilot_report.read_bytes()
    else:
        accepted_pilot_unchanged = True
        accepted_pilot_report_unchanged = True
    output_checks = _output_checks(output_dir, readiness, cards)
    hashes_after = {name: _sha256(input_dir / name) for name in INPUT_HASHES}
    card_membership_check = (
        {"pilot_cards_exact_orders_1_to_5": [row["deep_validation_order"] for row in cards] == list(PILOT_CONCEPT_ORDERS)}
        if stage == "PILOT" else
        {"final_cards_exact_orders_1_2_3_4_5_8_10_11": [row["deep_validation_order"] for row in cards] == list(FINAL_CONCEPT_ORDERS)}
    )
    checks: dict[str, bool] = {
        **input_checks,
        "exact_15_family_universe": len(readiness) == 15 and [row["deep_validation_order"] for row in readiness] == list(range(1, 16)),
        "readiness_expected_8_and_7": sum(row["concept_readiness"] == "CONCEPT_READY" for row in readiness) == 8 and sum(row["concept_readiness"] == "NEEDS_EVIDENCE" for row in readiness) == 7,
        "readiness_exact_expected_orders": ready_orders == list(EXPECTED_READY_ORDERS),
        "accepted_pilot_orders_1_to_5_unchanged": accepted_pilot_unchanged,
        "accepted_pilot_report_unchanged": accepted_pilot_report_unchanged,
        **card_membership_check,
        "cards_derived_from_ready_families": all(row["concept_readiness"] == "CONCEPT_READY" for row in cards),
        "canonical_input_hashes_stable": hashes_before == hashes_after,
        "deterministic_replay_byte_identical": deterministic_replay,
        **semantic_checks,
        **output_checks,
    }
    errors = [name for name, passed in checks.items() if not passed]
    qa = {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "status": "PASS" if not errors else "FAIL",
        "authorized_scope": "readiness orders 1..15; concept cards orders 1,2,3,4,5,8,10,11" if stage == "FINAL" else "readiness orders 1..15; concept cards orders 1..5 only",
        "canonical_input_hashes_before": hashes_before,
        "canonical_input_hashes_after": hashes_after,
        "capture_sha256": hashlib.sha256(capture_bytes).hexdigest(),
        "checks": checks,
        "row_counts": {
            "concept_readiness_matrix": len(readiness),
            "opportunity_concept_cards": len(cards),
            **{table: len(rows) for table, rows in payload.items()},
        },
        "concept_ready_orders": ready_orders,
        "concept_card_orders": [row["deep_validation_order"] for row in cards],
        "errors": errors,
    }
    (output_dir / "QA_RESULT.json").write_text(canonical_json(qa) + "\n", encoding="utf-8", newline="\n")
    if stage == "PILOT":
        (output_dir / "PILOT_REPORT.md").write_text(_report_text(readiness, cards, qa), encoding="utf-8", newline="\n")
        output_files = OUTPUT_FILES
    else:
        (output_dir / "FINAL_REPORT.md").write_text(_final_report_text(readiness, cards, qa), encoding="utf-8", newline="\n")
        output_files = FINAL_OUTPUT_FILES
    manifest_files = []
    for name in output_files:
        if name == "DATASET_MANIFEST.json":
            continue
        path = output_dir / name
        manifest_files.append({"file": name, "sha256": _sha256(path), "size_bytes": path.stat().st_size})
    manifest = {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "authorization": "FINAL_READINESS_ALL_15_CONCEPT_CARDS_ORDERS_1_2_3_4_5_8_10_11" if stage == "FINAL" else "PILOT_READINESS_ALL_15_CONCEPT_CARDS_ORDERS_1_TO_5",
        "baseline_commit": BASELINE_COMMIT,
        "canonical_input_hashes": hashes_before,
        "row_counts": qa["row_counts"],
        "files": manifest_files,
        "manifest_note": "Hashes cover each listed deliverable; DATASET_MANIFEST.json is excluded from its own file list.",
    }
    (output_dir / "DATASET_MANIFEST.json").write_text(canonical_json(manifest) + "\n", encoding="utf-8", newline="\n")
    return qa


def build_pilot(input_dir: Path, capture_path: Path, output_dir: Path) -> dict[str, Any]:
    """Build the authorized pilot and verify a complete byte-identical replay."""
    output_dir = output_dir.resolve()
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="yee57-replay-", dir=output_dir.parent) as first_temp:
        first_qa = _render(input_dir, capture_path, output_dir, deterministic_replay=False)
        first_replay = Path(first_temp) / "replay"
        _render(input_dir, capture_path, first_replay, deterministic_replay=False)
        if not all(value for key, value in first_qa["checks"].items() if key != "deterministic_replay_byte_identical"):
            raise ConceptSynthesisError(f"pilot QA failed: {first_qa['errors']}")
        if not _byte_identical(output_dir, first_replay):
            raise ConceptSynthesisError("preliminary full dataset replay was not byte-identical")
    with tempfile.TemporaryDirectory(prefix="yee57-replay-", dir=output_dir.parent) as second_temp:
        final_qa = _render(input_dir, capture_path, output_dir, deterministic_replay=True)
        final_replay = Path(second_temp) / "replay"
        replay_qa = _render(input_dir, capture_path, final_replay, deterministic_replay=True)
        if not _byte_identical(output_dir, final_replay):
            raise ConceptSynthesisError("final full dataset replay was not byte-identical")
        if final_qa["status"] != "PASS" or replay_qa["status"] != "PASS":
            raise ConceptSynthesisError(f"final pilot QA failed: {final_qa['errors']}")
    return final_qa


def build_final(
    input_dir: Path, accepted_pilot_dir: Path, capture_path: Path, output_dir: Path,
) -> dict[str, Any]:
    """Build the supervisor-authorized final cards and verify full replay/pilot preservation."""
    output_dir = output_dir.resolve()
    accepted_pilot_dir = accepted_pilot_dir.resolve()
    if output_dir == accepted_pilot_dir:
        raise ConceptSynthesisError("final outputs must not overwrite the accepted pilot directory")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="yee57-final-replay-", dir=output_dir.parent) as first_temp:
        first_qa = _render(
            input_dir, capture_path, output_dir, deterministic_replay=False,
            stage="FINAL", accepted_pilot_dir=accepted_pilot_dir,
        )
        first_replay = Path(first_temp) / "replay"
        _render(
            input_dir, capture_path, first_replay, deterministic_replay=False,
            stage="FINAL", accepted_pilot_dir=accepted_pilot_dir,
        )
        if not all(value for key, value in first_qa["checks"].items() if key != "deterministic_replay_byte_identical"):
            raise ConceptSynthesisError(f"final QA failed: {first_qa['errors']}")
        if not _byte_identical(output_dir, first_replay, FINAL_OUTPUT_FILES):
            raise ConceptSynthesisError("preliminary final dataset replay was not byte-identical")
    with tempfile.TemporaryDirectory(prefix="yee57-final-replay-", dir=output_dir.parent) as second_temp:
        final_qa = _render(
            input_dir, capture_path, output_dir, deterministic_replay=True,
            stage="FINAL", accepted_pilot_dir=accepted_pilot_dir,
        )
        final_replay = Path(second_temp) / "replay"
        replay_qa = _render(
            input_dir, capture_path, final_replay, deterministic_replay=True,
            stage="FINAL", accepted_pilot_dir=accepted_pilot_dir,
        )
        if not _byte_identical(output_dir, final_replay, FINAL_OUTPUT_FILES):
            raise ConceptSynthesisError("final dataset replay was not byte-identical")
        if final_qa["status"] != "PASS" or replay_qa["status"] != "PASS":
            raise ConceptSynthesisError(f"final QA failed: {final_qa['errors']}")
    return final_qa


def _byte_identical(left: Path, right: Path, files: Sequence[str] = OUTPUT_FILES) -> bool:
    return all((left / name).read_bytes() == (right / name).read_bytes() for name in files)
