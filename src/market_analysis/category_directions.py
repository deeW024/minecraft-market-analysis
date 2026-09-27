"""Deterministic YEE-75 category-local direction discovery over accepted YEE-73."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import sqlite3
import tempfile
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .category_signals import _number, _read_only_connection, canonical_json, sample_size_band, sha256_file
from .pipeline import quantile


WORK_ORDER = "YEE-75"
DELIVERY_STATUS = "CATEGORY_DIRECTION_DISCOVERY_READY_FOR_SUPERVISOR_REVIEW"
SCHEMA_VERSION = "yee-75-category-direction-discovery-v0.3"
DISCOVERY_RULE_VERSION = "yee-75-category-local-lexical-mining-v0.3"
NORMALIZATION_VERSION = "yee-75-text-normalization-v0.1"
WHITESPACE_SEMANTICS_VERSION = "yee-75-observed-whitespace-semantics-v0.1"
FUNCTIONAL_DIRECTION_GUARD_VERSION = "yee-75-functional-direction-guard-v0.1"
FUNCTIONAL_COHESION_VERSION = "yee-75-functional-cohesion-contract-v0.1"
INPUT_WORK_ORDER = "YEE-73"
INPUT_SIGNAL_SCHEMA_VERSION = "yee-73-category-signal-layer-v0.1"
INPUT_MERGE_COMMIT = "3f7dc4c55973dafb8fc56bfda25f7c3e595ef872"
INPUT_SQLITE_SHA256 = "374b5955d11764a9637e63f9611416ac4d6bc157c40f1582bebfb0eeaa1e11a3"
TAXONOMY_VERSION = "yee-61-functional-category-taxonomy-v0.1"
TAXONOMY_SHA256 = "b5720325dea06863408dfa1a05e2f981ecfeb3fac4a9e7266083ee17839c5126"
SOURCES = ("hangar", "voxel")
CONFIRMED_SOURCE_COUNTS = {"hangar": 1221, "voxel": 131}
CSV_NULL = r"\N"
MAX_SUMMARY_SENTENCE_CHARS = 240
REPRESENTATIVE_LIMIT_PER_SOURCE = 6

CANDIDATE_STATES = (
    "ADVANCE_TO_STAGE_D",
    "WATCH_COVERAGE_LIMITED",
    "NO_CLEAR_PATTERN",
    "INSUFFICIENT_EVIDENCE",
)
SOURCE_PATTERN_STATES = (
    "OBSERVED_STRONG_PATTERN",
    "OBSERVED_SUPPORTED_PATTERN",
    "DEMAND_WITH_BROAD_SUPPLY",
    "LOW_DEMAND_PATTERN",
    "MIXED_PATTERN",
    "INSUFFICIENT_EVIDENCE",
)
SUPPLY_BANDS = ("ZERO", "SPARSE", "LIMITED", "BROAD")
DEMAND_STRENGTHS = ("INSUFFICIENT", "HIGH", "MODERATE", "LOW", "MIXED")

STOP_WORDS = frozenset(
    """
    a an and are as at be been being but by can could do does for from had has have he her here hers
    herself him himself his how i if in into is it its itself just me more most my myself new no nor
    not of off on once only or other our ours ourselves out over own same she should so some such than
    that the their theirs them themselves then there these they this those through to too under until
    up very was we were what when where which while who whom why will with would you your yours
    yourself yourselves allow allows allowed add adds added also make makes made use uses used using
    work works working get gets getting give gives giving include includes including support supports
    supported compatible compatibility version versions resource resources plugin plugins minecraft
    paper spigot bukkit purpur folia velocity waterfall server servers proxy premium free paid addon
    add-on support supports download downloads updated update updates
    """.split()
)
NON_DIRECTION_BOILERPLATE_TOKENS = frozenset(
    """
    simple simpler simplest fully single better best improved improve easy easily easier easiest
    lightweight powerful advanced customizable custom designed design players player want wants
    wanted way lets let create creates created creating provide provides provided providing enable
    enables enabled enabling skip
    """.split()
)
NON_DIRECTION_PREFIX_TOKENS = frozenset(
    """
    a an the and but or to for with from in on at by of
    """.split()
)
NON_DIRECTION_SUFFIX_TOKENS = frozenset("to for with from and or of that which".split())
# Deliberately duplicated from the generation contract so QA independently recomputes it.
_QA_NON_DIRECTION_BOILERPLATE_TOKENS = frozenset(
    """
    simple simpler simplest fully single better best improved improve easy easily easier easiest
    lightweight powerful advanced customizable custom designed design players player want wants
    wanted way lets let create creates created creating provide provides provided providing enable
    enables enabled enabling skip
    """.split()
)
_QA_NON_DIRECTION_PREFIX_TOKENS = frozenset(
    """
    a an the and but or to for with from in on at by of
    """.split()
)
_QA_NON_DIRECTION_SUFFIX_TOKENS = frozenset("to for with from and or of that which".split())
STABLE_FUNCTION_SINGLE_TOKEN_ALLOWLIST = frozenset("auth home homes sleep teleport".split())
COHERENT_FUNCTIONAL_PHRASE_TERMS = {
    "custom recipes": ("recipes",),
    "death message": ("death", "message"),
    "item frames": ("item", "frames"),
    "skip the night": ("skip", "night"),
}
_QA_STABLE_FUNCTION_SINGLE_TOKEN_ALLOWLIST = frozenset("auth home homes sleep teleport".split())
_QA_COHERENT_FUNCTIONAL_PHRASE_TERMS = {
    "custom recipes": ("recipes",),
    "death message": ("death", "message"),
    "item frames": ("item", "frames"),
    "skip the night": ("skip", "night"),
}
VERSION_RE = re.compile(
    r"(?<![\w])\d+(?:\.\d+)+(?:\s*(?:-|–|—|to)\s*\d+(?:\.\d+)+)?(?:\.x|\+)?(?![\w])",
    re.IGNORECASE,
)
TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)
CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|[\r\n]+")

TABLES = (
    "direction_universe",
    "direction_memberships",
    "direction_source_facts",
    "direction_evaluations",
    "candidate_directions",
    "direction_evidence_packs",
    "category_direction_summary",
)
CSV_TABLES = (
    "direction_universe",
    "direction_memberships",
    "direction_source_facts",
    "direction_evaluations",
    "candidate_directions",
    "category_direction_summary",
)
CORE_FILES = (
    "GOAL_ALIGNMENT.md",
    "DIRECTION_DISCOVERY_SCHEMA.md",
    "WHITESPACE_SEMANTICS.md",
    "TEXT_NORMALIZATION.md",
    "category_direction_discovery.sqlite",
    *(f"{table}.{ext}" for table in CSV_TABLES for ext in ("jsonl", "csv")),
    "direction_evidence_packs.jsonl",
)
ALL_FILES = (*CORE_FILES, "QA_RESULT.json", "FINAL_REPORT.md", "DATASET_MANIFEST.json")


class CategoryDirectionError(ValueError):
    """The accepted YEE-73 input or YEE-75 output failed its contract."""


def _json_value(raw: str | None, field: str) -> Any:
    if raw is None:
        raise CategoryDirectionError(f"Required YEE-73 JSON field is null: {field}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CategoryDirectionError(f"Malformed YEE-73 JSON field: {field}") from exc


def _share(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def _identity_set_sha256(members: Sequence[Mapping[str, Any]]) -> str:
    payload = "".join(
        f"{row['source']}\t{row['source_resource_id']}\n"
        for row in sorted(members, key=lambda item: (item["source"], item["source_resource_id"]))
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _load_input(input_db: Path, input_sha: str, *, enforce_pinned_inputs: bool) -> dict[str, Any]:
    if enforce_pinned_inputs and input_sha != INPUT_SQLITE_SHA256:
        raise CategoryDirectionError(f"Accepted YEE-73 SQLite SHA-256 mismatch: {input_sha}")

    connection = _read_only_connection(input_db)
    try:
        table_names = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        required = {
            "run_metadata",
            "frozen_taxonomy_snapshot",
            "source_scope_coverage",
            "category_signal_member_features",
            "category_source_facts",
            "subcategory_source_facts",
        }
        if not required.issubset(table_names):
            raise CategoryDirectionError(f"YEE-73 input missing tables: {sorted(required - table_names)}")

        metadata = {
            str(row["key"]): str(row["value"])
            for row in connection.execute("SELECT key,value FROM run_metadata")
        }
        taxonomy = [
            _json_value(row["category_json"], "frozen_taxonomy_snapshot.category_json")
            for row in connection.execute(
                "SELECT category_json FROM frozen_taxonomy_snapshot ORDER BY category_order"
            )
        ]
        members = [
            _json_value(row["record_json"], "category_signal_member_features.record_json")
            for row in connection.execute(
                "SELECT record_json FROM category_signal_member_features "
                "ORDER BY source,source_resource_id"
            )
        ]
        category_facts = [
            _json_value(row["record_json"], "category_source_facts.record_json")
            for row in connection.execute(
                "SELECT record_json FROM category_source_facts ORDER BY category_order,source"
            )
        ]
        subcategory_facts = [
            _json_value(row["record_json"], "subcategory_source_facts.record_json")
            for row in connection.execute(
                "SELECT record_json FROM subcategory_source_facts "
                "ORDER BY category_order,subcategory_order,source"
            )
        ]
        source_coverage = [
            _json_value(row["record_json"], "source_scope_coverage.record_json")
            for row in connection.execute("SELECT record_json FROM source_scope_coverage ORDER BY source")
        ]
    finally:
        connection.close()

    if metadata.get("work_order") != INPUT_WORK_ORDER:
        raise CategoryDirectionError("The only accepted input must identify itself as YEE-73")
    if metadata.get("signal_schema_version") != INPUT_SIGNAL_SCHEMA_VERSION:
        raise CategoryDirectionError("YEE-73 signal schema version mismatch")
    if metadata.get("input_taxonomy_version") != TAXONOMY_VERSION or metadata.get("taxonomy_sha256") != TAXONOMY_SHA256:
        raise CategoryDirectionError("Accepted YEE-73 frozen taxonomy version/hash mismatch")

    category_ids = [str(row["category_id"]) for row in taxonomy]
    category_orders = [int(row["category_order"]) for row in taxonomy]
    if len(set(category_ids)) != len(taxonomy) or category_orders != list(range(len(taxonomy))):
        raise CategoryDirectionError("Frozen category IDs/order are duplicated or non-contiguous")
    taxonomy_by_id = {str(row["category_id"]): row for row in taxonomy}
    subcategory_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for category in taxonomy:
        category_id = str(category["category_id"])
        subcategories = category.get("subcategories")
        if not isinstance(subcategories, list):
            raise CategoryDirectionError(f"Malformed frozen subcategories for {category_id}")
        for subcategory_order, subcategory in enumerate(subcategories):
            subcategory_id = str(subcategory["subcategory_id"])
            key = (category_id, subcategory_id)
            if key in subcategory_by_key:
                raise CategoryDirectionError(f"Duplicate frozen subcategory: {key}")
            subcategory_by_key[key] = {
                **subcategory,
                "category_id": category_id,
                "category_order": int(category["category_order"]),
                "subcategory_order": subcategory_order,
            }
    if enforce_pinned_inputs and (len(taxonomy) != 11 or len(subcategory_by_key) != 38):
        raise CategoryDirectionError("Pinned input must contain exactly 11 categories and 38 subcategories")

    members_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    identities: set[str] = set()
    for row in members:
        source = str(row.get("source"))
        source_id = str(row.get("source_resource_id"))
        canonical_identity = str(row.get("canonical_identity"))
        key = (source, source_id)
        if source not in SOURCES or key in members_by_identity:
            raise CategoryDirectionError(f"Invalid or duplicate YEE-73 signal identity: {key}")
        if canonical_identity != f"{source}:{source_id}" or canonical_identity in identities:
            raise CategoryDirectionError(f"YEE-73 canonical identity mismatch: {canonical_identity}")
        category_id = str(row.get("primary_category_id"))
        subcategory_id = str(row.get("subcategory_id"))
        category = taxonomy_by_id.get(category_id)
        if category is None or (category_id, subcategory_id) not in subcategory_by_key:
            raise CategoryDirectionError(f"YEE-73 member outside frozen taxonomy: {canonical_identity}")
        if int(row.get("primary_category_order", -1)) != int(category["category_order"]):
            raise CategoryDirectionError(f"YEE-73 category order mismatch: {canonical_identity}")
        if int(row.get("subcategory_order", -1)) != subcategory_by_key[(category_id, subcategory_id)]["subcategory_order"]:
            raise CategoryDirectionError(f"YEE-73 subcategory order mismatch: {canonical_identity}")
        if row.get("signal_schema_version") != INPUT_SIGNAL_SCHEMA_VERSION:
            raise CategoryDirectionError(f"YEE-73 member schema mismatch: {canonical_identity}")
        members_by_identity[key] = row
        identities.add(canonical_identity)

    member_counts = Counter(row["source"] for row in members)
    expected_source_counts = dict(CONFIRMED_SOURCE_COUNTS) if enforce_pinned_inputs else dict(member_counts)
    if enforce_pinned_inputs and (len(members) != 1352 or dict(member_counts) != expected_source_counts):
        raise CategoryDirectionError(f"YEE-73 confirmed member counts mismatch: {dict(member_counts)}")
    analysis_as_of_values = {row.get("analysis_as_of") for row in members}
    if len(analysis_as_of_values) != 1 or None in analysis_as_of_values:
        raise CategoryDirectionError("YEE-73 members do not have one fixed analysis_as_of")
    analysis_as_of = next(iter(analysis_as_of_values))
    if metadata.get("analysis_as_of") != analysis_as_of:
        raise CategoryDirectionError("YEE-73 run metadata/member analysis_as_of mismatch")

    coverage_by_source = {str(row.get("source")): row for row in source_coverage}
    if set(coverage_by_source) != set(SOURCES) or len(source_coverage) != 2:
        raise CategoryDirectionError("YEE-73 source coverage must have exactly Hangar and Voxel")
    for source in SOURCES:
        coverage = coverage_by_source[source]
        if int(coverage.get("stage_b_signal_member_count", -1)) != member_counts.get(source, 0):
            raise CategoryDirectionError(f"YEE-73 coverage/member count mismatch for {source}")
        if int(coverage.get("yee61_confirmed_count", -1)) != member_counts.get(source, 0):
            raise CategoryDirectionError(f"YEE-61 confirmed/source count mismatch for {source}")
        if coverage.get("taxonomy_version") != TAXONOMY_VERSION or coverage.get("taxonomy_sha256") != TAXONOMY_SHA256:
            raise CategoryDirectionError(f"YEE-73 coverage taxonomy mismatch for {source}")

    category_fact_by_key = {}
    for row in category_facts:
        key = (str(row.get("category_id")), str(row.get("source")))
        if key in category_fact_by_key:
            raise CategoryDirectionError(f"Duplicate YEE-73 category/source fact: {key}")
        category_fact_by_key[key] = row
    expected_category_keys = {(category_id, source) for category_id in category_ids for source in SOURCES}
    if set(category_fact_by_key) != expected_category_keys:
        raise CategoryDirectionError("YEE-73 category/source facts are not a complete frozen cross product")
    subcategory_fact_by_key = {}
    for row in subcategory_facts:
        key = (str(row.get("category_id")), str(row.get("subcategory_id")), str(row.get("source")))
        if key in subcategory_fact_by_key:
            raise CategoryDirectionError(f"Duplicate YEE-73 subcategory/source fact: {key}")
        subcategory_fact_by_key[key] = row
    expected_subcategory_keys = {
        (category_id, subcategory_id, source)
        for category_id, subcategory_id in subcategory_by_key
        for source in SOURCES
    }
    if set(subcategory_fact_by_key) != expected_subcategory_keys:
        raise CategoryDirectionError("YEE-73 subcategory/source facts are not a complete frozen cross product")
    if enforce_pinned_inputs and (len(category_facts) != 22 or len(subcategory_facts) != 76):
        raise CategoryDirectionError("YEE-73 category/subcategory fact row counts mismatch")

    actual_category_counts = Counter((row["primary_category_id"], row["source"]) for row in members)
    actual_subcategory_counts = Counter((row["primary_category_id"], row["subcategory_id"], row["source"]) for row in members)
    for key, fact in category_fact_by_key.items():
        if int(fact.get("member_count", -1)) != actual_category_counts.get(key, 0):
            raise CategoryDirectionError(f"YEE-73 primary category fact/member mismatch: {key}")
    for key, fact in subcategory_fact_by_key.items():
        if int(fact.get("member_count", -1)) != actual_subcategory_counts.get(key, 0):
            raise CategoryDirectionError(f"YEE-73 primary subcategory fact/member mismatch: {key}")

    if enforce_pinned_inputs:
        if metadata.get("yee73_merge_commit") and metadata.get("yee73_merge_commit") != INPUT_MERGE_COMMIT:
            raise CategoryDirectionError("YEE-73 accepted merge commit metadata mismatch")
        if expected_source_counts != CONFIRMED_SOURCE_COUNTS:
            raise CategoryDirectionError("Pinned YEE-73 source counts mismatch")

    return {
        "metadata": metadata,
        "input_sha256": input_sha,
        "analysis_as_of": analysis_as_of,
        "taxonomy": taxonomy,
        "taxonomy_by_id": taxonomy_by_id,
        "subcategory_by_key": subcategory_by_key,
        "members": sorted(members, key=lambda row: (row["source"], row["source_resource_id"])),
        "members_by_identity": members_by_identity,
        "member_counts_by_source": {source: member_counts.get(source, 0) for source in SOURCES},
        "category_fact_by_key": category_fact_by_key,
        "subcategory_fact_by_key": subcategory_fact_by_key,
        "source_coverage_by_source": coverage_by_source,
        "identity_set_sha256": _identity_set_sha256(members),
    }


def _normalized_tokens(text: Any) -> list[dict[str, Any]]:
    if not isinstance(text, str) or not text:
        return []
    normalized = unicodedata.normalize("NFKC", text)
    without_versions = VERSION_RE.sub(lambda match: " " * len(match.group(0)), normalized)
    tokens: list[dict[str, Any]] = []
    for word_match in TOKEN_RE.finditer(without_versions):
        word = word_match.group(0)
        boundaries = [0, *(match.start() for match in CAMEL_BOUNDARY_RE.finditer(word)), len(word)]
        for start, end in zip(boundaries[:-1], boundaries[1:], strict=True):
            token = word[start:end].lower()
            if not token or token.isdigit() or re.fullmatch(r"\d+(?:\.\d+)*", token):
                continue
            tokens.append({
                "token": token,
                "start": word_match.start() + start,
                "end": word_match.start() + end,
                "normalized_source": normalized,
                "raw_source": text,
            })
    return tokens


def _first_nonempty_sentence(text: Any) -> str:
    if not isinstance(text, str):
        return ""
    for part in SENTENCE_RE.split(text):
        if _normalized_tokens(part):
            return part.strip()
    return ""


def _capped_surface_tokens(text: Any, *, cap: int | None = None) -> list[dict[str, Any]]:
    tokens = _normalized_tokens(text)
    if cap is None:
        return tokens
    return [token for token in tokens if int(token["end"]) <= cap]


def _phrase_occurrences(tokens: Sequence[Mapping[str, Any]], *, min_n: int, max_n: int) -> Iterable[tuple[str, int, int, int]]:
    values = [str(item["token"]) for item in tokens]
    for size in range(min_n, max_n + 1):
        for start in range(0, len(values) - size + 1):
            phrase_tokens = values[start : start + size]
            if all(token in STOP_WORDS or token.isdigit() for token in phrase_tokens):
                continue
            yield " ".join(phrase_tokens), start, start + size, size


def _direction_id(category_id: str, direction_type: str, canonical_key: str) -> str:
    seed = f"{category_id}\0{direction_type}\0{canonical_key}".encode("utf-8")
    return f"dir_{hashlib.sha256(seed).hexdigest()[:24]}"


def _functional_direction_guard(phrase: str) -> tuple[str, tuple[str, ...]]:
    """Require lexical content beyond versioned prose/marketing scaffolding."""
    tokens = tuple(phrase.split())
    functional_tokens = tuple(
        token for token in tokens
        if token not in STOP_WORDS and token not in NON_DIRECTION_BOILERPLATE_TOKENS and not token.isdigit()
    )
    if (
        not functional_tokens
        or (tokens and tokens[0] in NON_DIRECTION_PREFIX_TOKENS)
        or (tokens and tokens[-1] in NON_DIRECTION_SUFFIX_TOKENS)
    ):
        return "NON_DIRECTION_BOILERPLATE", functional_tokens
    return "FUNCTIONAL_DIRECTION", functional_tokens


def _qa_functional_direction_guard(phrase: str) -> str:
    """Independent QA implementation; do not call the generation guard."""
    tokens = [token for token in re.split(r"\s+", phrase.strip()) if token]
    content_tokens = [
        token for token in tokens
        if token not in STOP_WORDS
        and token not in _QA_NON_DIRECTION_BOILERPLATE_TOKENS
        and not token.isdecimal()
    ]
    boilerplate_prefix = bool(tokens and tokens[0] in _QA_NON_DIRECTION_PREFIX_TOKENS)
    dangling_suffix = bool(tokens and tokens[-1] in _QA_NON_DIRECTION_SUFFIX_TOKENS)
    if content_tokens and not boilerplate_prefix and not dangling_suffix:
        return "FUNCTIONAL_DIRECTION"
    return "NON_DIRECTION_BOILERPLATE"


def _functional_cohesion_guard(phrase: str) -> tuple[str, tuple[str, ...]]:
    """Fail closed unless the exact term/phrase is in the versioned cohesion contract."""
    tokens = tuple(phrase.split())
    if len(tokens) == 1:
        if tokens[0] in STABLE_FUNCTION_SINGLE_TOKEN_ALLOWLIST:
            return "FUNCTIONAL_COHESION", tokens
        return "NON_COHESIVE", ()
    phrase_terms = COHERENT_FUNCTIONAL_PHRASE_TERMS.get(phrase)
    if phrase_terms is not None:
        return "FUNCTIONAL_COHESION", phrase_terms
    return "NON_COHESIVE", ()


def _qa_functional_cohesion_guard(phrase: str) -> tuple[str, tuple[str, ...]]:
    """Independent QA contract check; do not call the generation cohesion gate."""
    words = [word for word in re.split(r"\s+", phrase.strip()) if word]
    if len(words) == 1:
        if words[0] in _QA_STABLE_FUNCTION_SINGLE_TOKEN_ALLOWLIST:
            return "FUNCTIONAL_COHESION", (words[0],)
        return "NON_COHESIVE", ()
    accepted_terms = _QA_COHERENT_FUNCTIONAL_PHRASE_TERMS.get(" ".join(words))
    if accepted_terms is not None:
        return "FUNCTIONAL_COHESION", accepted_terms
    return "NON_COHESIVE", ()


def _normalized_label_key(text: str) -> str:
    return " ".join(str(item["token"]) for item in _normalized_tokens(text))


def _mine_lexical_directions(
    loaded: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    members_by_category: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for member in loaded["members"]:
        members_by_category[str(member["primary_category_id"])].append(member)

    all_lexical: list[dict[str, Any]] = []
    all_memberships: list[dict[str, Any]] = []
    exclusions: Counter[str] = Counter()
    for category in loaded["taxonomy"]:
        category_id = str(category["category_id"])
        category_members = members_by_category.get(category_id, [])
        if not category_members:
            continue
        taxonomy_label_keys = {_normalized_label_key(str(category.get("category_name", "")))}
        taxonomy_label_keys.update(
            _normalized_label_key(str(subcategory["subcategory_id"]).replace("_", " "))
            for subcategory in category["subcategories"]
        )
        phrases: dict[str, dict[str, Any]] = {}
        for member in category_members:
            identity = str(member["canonical_identity"])
            title_tokens = _capped_surface_tokens(member.get("title"))
            summary_sentence = _first_nonempty_sentence(member.get("summary"))
            summary_tokens = _capped_surface_tokens(summary_sentence, cap=MAX_SUMMARY_SENTENCE_CHARS)
            surfaces = (("title", title_tokens, 1, 3), ("summary_first_sentence", summary_tokens, 2, 3))
            for field, tokens, min_n, max_n in surfaces:
                for phrase_key, start, end, size in _phrase_occurrences(tokens, min_n=min_n, max_n=max_n):
                    entry = phrases.setdefault(phrase_key, {"members": {}, "title_occurrences": 0})
                    if field == "title":
                        entry["title_occurrences"] += 1
                    token_slice = tokens[start:end]
                    first_token = token_slice[0]
                    last_token = token_slice[-1]
                    nfk_source = str(first_token["normalized_source"])
                    raw_source = str(first_token["raw_source"])
                    recoverable_span = unicodedata.normalize("NFKC", raw_source) == raw_source
                    match = {
                        "field": field,
                        "normalized_match": phrase_key,
                        "source_text_span": raw_source[first_token["start"] : last_token["end"]] if recoverable_span else None,
                        "source_span_start": first_token["start"] if recoverable_span else None,
                        "source_span_end": last_token["end"] if recoverable_span else None,
                        "span_source_is_nfkc_identical": recoverable_span,
                        "normalized_surface": " ".join(str(token["token"]) for token in tokens),
                    }
                    prior = entry["members"].get(identity)
                    field_priority = 0 if field == "title" else 1
                    prior_priority = 0 if prior and prior["field"] == "title" else 1
                    if prior is None or (field_priority, int(first_token["start"])) < (
                        prior_priority,
                        int(prior["source_span_start"] if prior["source_span_start"] is not None else 2**31),
                    ):
                        entry["members"][identity] = match

        eligible: list[tuple[str, dict[str, Any], tuple[str, ...]]] = []
        for phrase_key, entry in sorted(phrases.items()):
            phrase_tokens = tuple(phrase_key.split())
            if phrase_key in taxonomy_label_keys:
                exclusions["TAXONOMY_LABEL_DUPLICATE"] += 1
                continue
            semantic_classification, functional_content_tokens = _functional_direction_guard(phrase_key)
            if semantic_classification != "FUNCTIONAL_DIRECTION":
                exclusions["NON_DIRECTION_BOILERPLATE"] += 1
                continue
            cohesion_classification, cohesion_content_tokens = _functional_cohesion_guard(phrase_key)
            if cohesion_classification != "FUNCTIONAL_COHESION":
                exclusions["NON_COHESIVE_FUNCTIONAL_DIRECTION"] += 1
                continue
            required_support = 3 if len(phrase_tokens) == 1 else 2
            support = len(entry["members"])
            if support < required_support:
                exclusions["BELOW_MINIMUM_CATEGORY_SUPPORT"] += 1
                continue
            if support / len(category_members) > 0.5:
                exclusions["TOO_BROAD_FOR_DIRECTION"] += 1
                continue
            eligible.append((phrase_key, entry, cohesion_content_tokens))

        by_member_set: dict[frozenset[str], list[tuple[str, dict[str, Any], tuple[str, ...]]]] = defaultdict(list)
        for phrase_key, entry, cohesion_content_tokens in eligible:
            by_member_set[frozenset(entry["members"])].append((phrase_key, entry, cohesion_content_tokens))
        for member_set, aliases in sorted(by_member_set.items(), key=lambda item: tuple(sorted(item[0]))):
            ordered_aliases = sorted(
                aliases,
                key=lambda item: (
                    -len(item[2]),
                    len(item[0].split()) - len(item[2]),
                    -int(item[1]["title_occurrences"]),
                    -len(item[0].split()),
                    item[0],
                ),
            )
            canonical_key, chosen, _ = ordered_aliases[0]
            semantic_classification, functional_content_tokens = _functional_direction_guard(canonical_key)
            cohesion_classification, cohesion_content_tokens = _functional_cohesion_guard(canonical_key)
            redundant = [phrase for phrase, _, _ in ordered_aliases[1:]]
            exclusions["EXACT_MEMBER_SET_REDUNDANCY"] += len(redundant)
            # Membership matches are already keyed by the stable canonical identity.
            member_rows = {
                str(member["canonical_identity"]): member
                for member in category_members
                if str(member["canonical_identity"]) in member_set
            }
            if set(member_rows) != set(member_set):
                raise CategoryDirectionError("Lexical member set no longer resolves to the accepted identities")
            direction_id = _direction_id(category_id, "LEXICAL_SUBNICHE", canonical_key)
            all_lexical.append({
                "direction_id": direction_id,
                "direction_type": "LEXICAL_SUBNICHE",
                "primary_category_id": category_id,
                "primary_category_order": int(category["category_order"]),
                "direction_label": canonical_key,
                "canonical_direction_key": canonical_key,
                "baseline_subcategory_id": None,
                "lexical_token_count": len(canonical_key.split()),
                "discovery_rule_version": DISCOVERY_RULE_VERSION,
                "observed_member_identity_count": len(member_rows),
                "observed_member_count_by_source": {
                    source: sum(str(row["source"]) == source for row in member_rows.values())
                    for source in SOURCES
                },
                "observed_subcategory_distribution": dict(sorted(Counter(
                    str(row["subcategory_id"]) for row in member_rows.values()
                ).items())),
                "dominant_subcategory_id": _dominant_subcategory(member_rows.values()),
                "definition_semantics": f"Exact recurring normalized phrase {canonical_key!r} in accepted YEE-73 title or first summary sentence; no semantic generalization.",
                "exclusion_provenance": {
                    "same_member_set_aliases_not_retained": redundant,
                    "canonical_choice_rule": "more_functional_content_tokens_then_fewer_nonfunctional_tokens_then_more_title_occurrences_then_more_tokens_then_lexical_order",
                },
                "input_sqlite_sha256": loaded["input_sha256"],
                "taxonomy_version": TAXONOMY_VERSION,
                "taxonomy_sha256": TAXONOMY_SHA256,
                "signal_schema_version": INPUT_SIGNAL_SCHEMA_VERSION,
                "direction_schema_version": SCHEMA_VERSION,
                "functional_direction_guard_version": FUNCTIONAL_DIRECTION_GUARD_VERSION,
                "semantic_guard_classification": semantic_classification,
                "functional_content_tokens": list(functional_content_tokens),
                "functional_cohesion_version": FUNCTIONAL_COHESION_VERSION,
                "functional_cohesion_classification": cohesion_classification,
                "cohesive_content_tokens": list(cohesion_content_tokens),
                "normalization_version": NORMALIZATION_VERSION,
                "member_identity_set": sorted(member_rows),
                "title_occurrence_count": int(chosen["title_occurrences"]),
            })
            for identity in sorted(member_rows):
                member = member_rows[identity]
                match = chosen["members"][identity]
                all_memberships.append({
                    "direction_id": direction_id,
                    "direction_type": "LEXICAL_SUBNICHE",
                    "primary_category_id": category_id,
                    "primary_category_order": int(category["category_order"]),
                    "subcategory_id": member["subcategory_id"],
                    "subcategory_order": int(member["subcategory_order"]),
                    "source": member["source"],
                    "source_resource_id": member["source_resource_id"],
                    "canonical_identity": identity,
                    "membership_method": "EXACT_NORMALIZED_NGRAM_OCCURRENCE",
                    "match_field": match["field"],
                    "normalized_match": match["normalized_match"],
                    "source_text_span": match["source_text_span"],
                    "source_span_start": match["source_span_start"],
                    "source_span_end": match["source_span_end"],
                    "phrase_key": canonical_key,
                    "normalization_version": NORMALIZATION_VERSION,
                    "discovery_rule_version": DISCOVERY_RULE_VERSION,
                    "functional_direction_guard_version": FUNCTIONAL_DIRECTION_GUARD_VERSION,
                    "functional_cohesion_version": FUNCTIONAL_COHESION_VERSION,
                    "input_sqlite_sha256": loaded["input_sha256"],
                    "taxonomy_version": TAXONOMY_VERSION,
                })
    all_lexical.sort(key=lambda row: (row["primary_category_order"], row["canonical_direction_key"]))
    all_memberships.sort(key=lambda row: (row["primary_category_order"], row["direction_id"], row["source"], row["source_resource_id"]))
    return all_lexical, all_memberships, dict(sorted(exclusions.items()))


def _dominant_subcategory(members: Iterable[Mapping[str, Any]]) -> str | None:
    counts = Counter(str(member["subcategory_id"]) for member in members)
    if not counts:
        return None
    maximum = max(counts.values())
    leaders = sorted(key for key, count in counts.items() if count == maximum)
    return leaders[0] if len(leaders) == 1 else None


def _baseline_directions(loaded: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    members_by_subcategory: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for member in loaded["members"]:
        members_by_subcategory[(str(member["primary_category_id"]), str(member["subcategory_id"]))].append(member)
    directions: list[dict[str, Any]] = []
    memberships: list[dict[str, Any]] = []
    for category in loaded["taxonomy"]:
        category_id = str(category["category_id"])
        for subcategory_order, subcategory in enumerate(category["subcategories"]):
            subcategory_id = str(subcategory["subcategory_id"])
            members = members_by_subcategory.get((category_id, subcategory_id), [])
            direction_id = _direction_id(category_id, "SUBCATEGORY_BASELINE", subcategory_id)
            directions.append({
                "direction_id": direction_id,
                "direction_type": "SUBCATEGORY_BASELINE",
                "primary_category_id": category_id,
                "primary_category_order": int(category["category_order"]),
                "direction_label": subcategory_id,
                "canonical_direction_key": subcategory_id,
                "baseline_subcategory_id": subcategory_id,
                "lexical_token_count": None,
                "discovery_rule_version": DISCOVERY_RULE_VERSION,
                "observed_member_identity_count": len(members),
                "observed_member_count_by_source": {
                    source: sum(str(row["source"]) == source for row in members)
                    for source in SOURCES
                },
                "observed_subcategory_distribution": ({subcategory_id: len(members)} if members else {}),
                "dominant_subcategory_id": subcategory_id if members else None,
                "definition_semantics": str(subcategory.get("definition", "")),
                "exclusion_provenance": None,
                "input_sqlite_sha256": loaded["input_sha256"],
                "taxonomy_version": TAXONOMY_VERSION,
                "taxonomy_sha256": TAXONOMY_SHA256,
                "signal_schema_version": INPUT_SIGNAL_SCHEMA_VERSION,
                "direction_schema_version": SCHEMA_VERSION,
                "normalization_version": NORMALIZATION_VERSION,
                "member_identity_set": sorted(str(row["canonical_identity"]) for row in members),
            })
            for member in sorted(members, key=lambda row: (row["source"], row["source_resource_id"])):
                memberships.append({
                    "direction_id": direction_id,
                    "direction_type": "SUBCATEGORY_BASELINE",
                    "primary_category_id": category_id,
                    "primary_category_order": int(category["category_order"]),
                    "subcategory_id": subcategory_id,
                        "subcategory_order": subcategory_order,
                    "source": member["source"],
                    "source_resource_id": member["source_resource_id"],
                    "canonical_identity": member["canonical_identity"],
                    "membership_method": "PRIMARY_SUBCATEGORY_BASELINE",
                    "match_field": None,
                    "normalized_match": None,
                    "source_text_span": None,
                    "source_span_start": None,
                    "source_span_end": None,
                    "phrase_key": None,
                    "normalization_version": NORMALIZATION_VERSION,
                    "discovery_rule_version": DISCOVERY_RULE_VERSION,
                    "input_sqlite_sha256": loaded["input_sha256"],
                    "taxonomy_version": TAXONOMY_VERSION,
                })
    return directions, memberships


def _build_direction_universe(
    loaded: Mapping[str, Any],
    lexical: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    baseline, _ = _baseline_directions(loaded)
    directions = [*baseline, *[dict(row) for row in lexical]]
    directions.sort(key=lambda row: (
        int(row["primary_category_order"]),
        0 if row["direction_type"] == "SUBCATEGORY_BASELINE" else 1,
        next((index for index, sub in enumerate(loaded["taxonomy_by_id"][row["primary_category_id"]]["subcategories"])
              if sub["subcategory_id"] == row["baseline_subcategory_id"]), 0)
        if row["direction_type"] == "SUBCATEGORY_BASELINE" else str(row["canonical_direction_key"]),
        str(row["direction_id"]),
    ))
    for row in directions:
        row.pop("member_identity_set", None)
    return directions


def _build_memberships(
    loaded: Mapping[str, Any],
    lexical_memberships: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    _, baseline = _baseline_directions(loaded)
    rows = [*baseline, *[dict(row) for row in lexical_memberships]]
    rows.sort(key=lambda row: (
        int(row["primary_category_order"]),
        0 if row["direction_type"] == "SUBCATEGORY_BASELINE" else 1,
        str(row["direction_id"]),
        row["source"],
        row["source_resource_id"],
    ))
    return rows


def _coverage_risk(confirmed_fraction: Any) -> str:
    fraction = _number(confirmed_fraction)
    if fraction is None:
        raise CategoryDirectionError("YEE-73 confirmed source fraction is unavailable")
    if fraction < 0.05:
        return "VERY_HIGH"
    if fraction < 0.20:
        return "HIGH"
    if fraction < 0.40:
        return "MATERIAL"
    return "LOWER"


def _demand_strength(demand_count: int, p50: float | int | None, share_ge75: float | None) -> str:
    if demand_count < 2:
        return "INSUFFICIENT"
    if p50 is not None and p50 >= 65 and share_ge75 is not None and share_ge75 >= 0.40:
        return "HIGH"
    if (p50 is not None and p50 >= 55) or (share_ge75 is not None and share_ge75 >= 0.33):
        return "MODERATE"
    if p50 is not None and p50 < 45 and share_ge75 is not None and share_ge75 < 0.25:
        return "LOW"
    return "MIXED"


def _observed_supply_band(member_count: int, category_source_count: int) -> tuple[str | None, float | None, str]:
    if category_source_count == 0:
        return ("ZERO" if member_count == 0 else None), None, "INSUFFICIENT"
    share = _share(member_count, category_source_count)
    if member_count == 0:
        return "ZERO", share, "AVAILABLE"
    if member_count <= 5 and share is not None and share <= 0.20:
        return "SPARSE", share, "AVAILABLE"
    if member_count <= 10 and share is not None and share <= 0.35:
        return "LIMITED", share, "AVAILABLE"
    return "BROAD", share, "AVAILABLE"


def _whitespace_state(
    member_count: int,
    demand_available_count: int,
    category_source_count: int,
    demand_strength: str,
    observed_supply_band: str | None,
) -> str:
    if member_count < 2 or demand_available_count < 2 or category_source_count == 0:
        return "INSUFFICIENT_EVIDENCE"
    if member_count >= 3 and demand_strength == "HIGH" and observed_supply_band == "SPARSE":
        return "OBSERVED_STRONG_PATTERN"
    if member_count >= 2 and demand_strength in {"HIGH", "MODERATE"} and observed_supply_band in {"SPARSE", "LIMITED"}:
        return "OBSERVED_SUPPORTED_PATTERN"
    if demand_strength == "HIGH" and observed_supply_band == "BROAD":
        return "DEMAND_WITH_BROAD_SUPPLY"
    if demand_strength == "LOW":
        return "LOW_DEMAND_PATTERN"
    return "MIXED_PATTERN"


def _evaluate_candidate_state(
    source_facts: Mapping[str, Mapping[str, Any]],
    distinct_member_count: int,
) -> tuple[str, list[str]]:
    strong_or_supported = {"OBSERVED_STRONG_PATTERN", "OBSERVED_SUPPORTED_PATTERN"}
    strong_non_very_high = any(
        row["observed_whitespace_pattern_state"] == "OBSERVED_STRONG_PATTERN"
        and row["source_supply_inference_risk"] != "VERY_HIGH"
        for row in source_facts.values()
    )
    both_sources_supported = all(
        row["observed_whitespace_pattern_state"] in strong_or_supported
        for row in source_facts.values()
    )
    if strong_non_very_high or (both_sources_supported and distinct_member_count >= 3):
        reasons = []
        if strong_non_very_high:
            reasons.append("STRONG_PATTERN_WITHOUT_VERY_HIGH_COVERAGE_RISK")
        if both_sources_supported and distinct_member_count >= 3:
            reasons.append("BOTH_SOURCES_SUPPORT_PATTERN_WITH_AT_LEAST_THREE_MEMBERS")
        return "ADVANCE_TO_STAGE_D", reasons

    watch_reasons: list[str] = []
    for source in SOURCES:
        fact = source_facts[source]
        state = fact["observed_whitespace_pattern_state"]
        if state == "OBSERVED_STRONG_PATTERN":
            watch_reasons.append(f"{source.upper()}_STRONG_PATTERN_COVERAGE_LIMITED")
        elif state == "OBSERVED_SUPPORTED_PATTERN":
            watch_reasons.append(f"{source.upper()}_SUPPORTED_PATTERN")
        elif state == "DEMAND_WITH_BROAD_SUPPLY" and int(fact["observed_confirmed_member_count"]) >= 3:
            watch_reasons.append(f"{source.upper()}_HIGH_DEMAND_WITH_BROAD_OBSERVED_SUPPLY")
    if watch_reasons:
        return "WATCH_COVERAGE_LIMITED", sorted(set(watch_reasons))
    if all(row["observed_whitespace_pattern_state"] == "INSUFFICIENT_EVIDENCE" for row in source_facts.values()):
        return "INSUFFICIENT_EVIDENCE", ["BOTH_SOURCE_SLICES_INSUFFICIENT"]
    return "NO_CLEAR_PATTERN", ["NO_ADVANCE_OR_WATCH_RULE_MET"]


def _metric_summary(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, Any]:
    values = [value for row in rows if (value := _number(row.get(field))) is not None]
    return {
        "available_count": len(values),
        "available_rate": _share(len(values), len(rows)),
        "p50": quantile(values, 0.50),
        "p90": quantile(values, 0.90),
    }


def _source_direction_fact(
    direction: Mapping[str, Any],
    direction_members: Sequence[Mapping[str, Any]],
    loaded: Mapping[str, Any],
    source: str,
) -> dict[str, Any]:
    members = [row for row in direction_members if row["source"] == source]
    category_id = str(direction["primary_category_id"])
    category_source = loaded["category_fact_by_key"][(category_id, source)]
    category_source_count = int(category_source["member_count"])
    member_count = len(members)
    supply_band, supply_share, source_evidence_state = _observed_supply_band(member_count, category_source_count)

    demand_values = [
        value for row in members
        if (value := _number(row.get("source_category_demand_percentile"))) is not None
    ]
    share_ge75_count = sum(value >= 75 for value in demand_values)
    share_ge90_count = sum(value >= 90 for value in demand_values)
    share_ge75 = _share(share_ge75_count, len(demand_values))
    demand_strength = _demand_strength(len(demand_values), quantile(demand_values, 0.50), share_ge75)

    ages = [value for row in members if (value := _number(row.get("freshness_age_days"))) is not None]
    freshness_le90_count = sum(value <= 90 for value in ages)
    freshness_le365_count = sum(value <= 365 for value in ages)
    coverage = dict(loaded["source_coverage_by_source"][source])
    supply_risk = _coverage_risk(coverage.get("yee61_confirmed_fraction"))
    state = _whitespace_state(member_count, len(demand_values), category_source_count, demand_strength, supply_band)

    engagement_fields = (
        ("hangar_recent_downloads", "hangar_recent_downloads"),
        ("hangar_recent_views", "hangar_recent_views"),
        ("star_count", "star_count"),
        ("watcher_count", "watcher_count"),
    ) if source == "hangar" else (
        ("voxel_review_count", "voxel_review_count"),
        ("voxel_review_stars", "voxel_review_stars"),
    )
    engagement = {name: _metric_summary(members, field) for name, field in engagement_fields}

    row: dict[str, Any] = {
        "direction_id": direction["direction_id"],
        "direction_type": direction["direction_type"],
        "primary_category_id": category_id,
        "primary_category_order": int(direction["primary_category_order"]),
        "source": source,
        "observed_confirmed_member_count": member_count,
        "category_source_confirmed_count": category_source_count,
        "observed_supply_share_within_category_source": supply_share,
        "observed_supply_band": supply_band,
        "member_count_sample_size_band": sample_size_band(member_count),
        "source_evidence_state": source_evidence_state,
        "demand_available_count": len(demand_values),
        "demand_available_rate": _share(len(demand_values), member_count),
        "source_category_demand_percentile_p50": quantile(demand_values, 0.50),
        "source_category_demand_percentile_p75": quantile(demand_values, 0.75),
        "source_category_demand_percentile_p90": quantile(demand_values, 0.90),
        "share_at_or_above_source_category_demand_percentile_75": share_ge75,
        "share_at_or_above_source_category_demand_percentile_90": _share(share_ge90_count, len(demand_values)),
        "count_at_or_above_source_category_demand_percentile_75": share_ge75_count,
        "count_at_or_above_source_category_demand_percentile_90": share_ge90_count,
        "downloads_total_p50": quantile(
            [value for member in members if (value := _number(member.get("downloads_total"))) is not None], 0.50
        ),
        "downloads_total_p90": quantile(
            [value for member in members if (value := _number(member.get("downloads_total"))) is not None], 0.90
        ),
        "freshness_available_count": len(ages),
        "freshness_available_rate": _share(len(ages), member_count),
        "freshness_unknown_count": member_count - len(ages),
        "freshness_share_le_90d": _share(freshness_le90_count, member_count),
        "freshness_share_le_365d": _share(freshness_le365_count, member_count),
        "freshness_age_days_p50": quantile(ages, 0.50),
        "freshness_age_days_p90": quantile(ages, 0.90),
        "source_native_engagement": engagement,
        "source_scope_coverage": coverage,
        "source_supply_inference_risk": supply_risk,
        "demand_strength": demand_strength,
        "observed_whitespace_pattern_state": state,
        "input_sqlite_sha256": loaded["input_sha256"],
        "taxonomy_version": TAXONOMY_VERSION,
        "taxonomy_sha256": TAXONOMY_SHA256,
        "direction_schema_version": SCHEMA_VERSION,
    }
    if source == "voxel":
        paid_counts = Counter(str(member.get("paid_state") or "unknown") for member in members)
        unexpected_paid = set(paid_counts) - {"paid", "free", "unknown"}
        if unexpected_paid:
            raise CategoryDirectionError(f"Unexpected Voxel paid_state values: {sorted(unexpected_paid)}")
        paid_counts = {state_name: paid_counts.get(state_name, 0) for state_name in ("paid", "free", "unknown")}
        observed_paid = paid_counts["paid"] + paid_counts["free"]
        price_band_counts = Counter(str(member.get("voxel_price_band") or "unknown") for member in members)
        price_groups: dict[str, list[float]] = defaultdict(list)
        for member in members:
            amount = _number(member.get("price_amount"))
            currency = member.get("currency")
            if amount is not None and isinstance(currency, str) and currency:
                price_groups[currency].append(amount)
        row.update({
            "paid_evidence_scope": "SOURCE_AVAILABLE",
            "paid_state_counts": paid_counts,
            "paid_evidence_observed_count": observed_paid,
            "paid_share_among_observed": _share(paid_counts["paid"], observed_paid),
            "price_band_counts": dict(sorted(price_band_counts.items())),
            "price_available_count": sum(len(values) for values in price_groups.values()),
            "price_quantiles_by_currency": {
                currency: {
                    "available_count": len(values),
                    "p50": quantile(values, 0.50),
                    "p90": quantile(values, 0.90),
                }
                for currency, values in sorted(price_groups.items())
            },
        })
    else:
        row.update({
            "paid_evidence_scope": "NOT_AVAILABLE_FOR_SOURCE",
            "paid_state_counts": None,
            "paid_evidence_observed_count": None,
            "paid_share_among_observed": None,
            "price_band_counts": None,
            "price_available_count": None,
            "price_quantiles_by_currency": None,
        })
    return row


def _build_source_facts(
    loaded: Mapping[str, Any],
    directions: Sequence[Mapping[str, Any]],
    memberships: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    members_by_direction: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for membership in memberships:
        key = (str(membership["source"]), str(membership["source_resource_id"]))
        member = loaded["members_by_identity"].get(key)
        if member is None or member["canonical_identity"] != membership["canonical_identity"]:
            raise CategoryDirectionError("Direction membership does not resolve to accepted YEE-73 identity")
        members_by_direction[str(membership["direction_id"])].append(member)
    result = [
        _source_direction_fact(direction, members_by_direction.get(str(direction["direction_id"]), []), loaded, source)
        for direction in directions
        for source in SOURCES
    ]
    return result


def _build_evaluations(
    directions: Sequence[Mapping[str, Any]],
    source_facts: Sequence[Mapping[str, Any]],
    memberships: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    facts_by_direction: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for fact in source_facts:
        facts_by_direction[str(fact["direction_id"])][str(fact["source"])] = fact
    counts = Counter(str(row["direction_id"]) for row in memberships)
    rows: list[dict[str, Any]] = []
    for direction in directions:
        direction_id = str(direction["direction_id"])
        per_source = facts_by_direction[direction_id]
        state, reasons = _evaluate_candidate_state(per_source, counts[direction_id])
        risk_by_source = {source: per_source[source]["source_supply_inference_risk"] for source in SOURCES}
        coverage_notes = [
            {
                "source": source,
                "source_supply_inference_risk": risk_by_source[source],
                "yee61_confirmed_fraction": per_source[source]["source_scope_coverage"]["yee61_confirmed_fraction"],
            }
            for source in SOURCES
        ]
        rows.append({
            "direction_id": direction_id,
            "direction_type": direction["direction_type"],
            "primary_category_id": direction["primary_category_id"],
            "primary_category_order": int(direction["primary_category_order"]),
            "direction_label": direction["direction_label"],
            "observed_member_identity_count": int(direction["observed_member_identity_count"]),
            "observed_member_count_by_source": dict(direction["observed_member_count_by_source"]),
            "candidate_state": state,
            "reason_codes": reasons,
            "source_pattern_state_by_source": {
                source: per_source[source]["observed_whitespace_pattern_state"] for source in SOURCES
            },
            "source_supply_inference_risk_by_source": risk_by_source,
            "coverage_risk_notes": coverage_notes,
            "input_sqlite_sha256": direction["input_sqlite_sha256"],
            "taxonomy_version": TAXONOMY_VERSION,
            "taxonomy_sha256": TAXONOMY_SHA256,
            "direction_schema_version": SCHEMA_VERSION,
        })
    return rows


def _representative_members(
    direction_id: str,
    memberships: Sequence[Mapping[str, Any]],
    loaded: Mapping[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    selected: dict[str, list[dict[str, Any]]] = {}
    matches_by_identity = {
        str(row["canonical_identity"]): row
        for row in memberships
        if str(row["direction_id"]) == direction_id and row["direction_type"] == "LEXICAL_SUBNICHE"
    }
    direction_memberships = [row for row in memberships if str(row["direction_id"]) == direction_id]
    for source in SOURCES:
        members: list[dict[str, Any]] = []
        for membership in direction_memberships:
            if membership["source"] != source:
                continue
            member = loaded["members_by_identity"][(source, str(membership["source_resource_id"]))]
            member_copy = {
                "source": source,
                "source_resource_id": member["source_resource_id"],
                "canonical_identity": member["canonical_identity"],
                "title": member.get("title"),
                "summary": member.get("summary"),
                "source_url": member.get("source_url"),
                "primary_category_id": member["primary_category_id"],
                "subcategory_id": member["subcategory_id"],
                "source_category_demand_percentile": _number(member.get("source_category_demand_percentile")),
                "freshness_age_days": _number(member.get("freshness_age_days")),
                "freshness_cohort": member.get("freshness_cohort"),
            }
            match = matches_by_identity.get(str(member["canonical_identity"]))
            if match is not None:
                member_copy["lexical_match_evidence"] = {
                    "field": match["match_field"],
                    "normalized_match": match["normalized_match"],
                    "source_text_span": match["source_text_span"],
                    "source_span_start": match["source_span_start"],
                    "source_span_end": match["source_span_end"],
                }
            members.append(member_copy)
        members.sort(key=lambda member: (
            member["source_category_demand_percentile"] is None,
            -(member["source_category_demand_percentile"] or 0.0),
            member["freshness_age_days"] is None,
            member["freshness_age_days"] if member["freshness_age_days"] is not None else 0,
            member["canonical_identity"],
        ))
        selected[source] = members[:REPRESENTATIVE_LIMIT_PER_SOURCE]
    return selected


def _build_candidate_tables(
    loaded: Mapping[str, Any],
    directions: Sequence[Mapping[str, Any]],
    memberships: Sequence[Mapping[str, Any]],
    source_facts: Sequence[Mapping[str, Any]],
    evaluations: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    facts_by_direction: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for row in source_facts:
        facts_by_direction[str(row["direction_id"])][str(row["source"])] = row
    candidates = [
        dict(row) for row in evaluations
        if row["candidate_state"] in {"ADVANCE_TO_STAGE_D", "WATCH_COVERAGE_LIMITED"}
    ]
    candidates.sort(key=lambda row: (int(row["primary_category_order"]), str(row["direction_id"])))
    directions_by_id = {str(row["direction_id"]): row for row in directions}
    packs: list[dict[str, Any]] = []
    for candidate in candidates:
        direction_id = str(candidate["direction_id"])
        direction = directions_by_id[direction_id]
        selected = _representative_members(direction_id, memberships, loaded)
        per_source = facts_by_direction[direction_id]
        risk_notes = [
            f"{source}: {per_source[source]['source_supply_inference_risk']} YEE-61 confirmation coverage risk; patterns describe observed confirmed supply only."
            for source in SOURCES
            if per_source[source]["source_supply_inference_risk"] in {"VERY_HIGH", "HIGH", "MATERIAL"}
        ]
        lexical_evidence = [
            {
                "source": source,
                "canonical_identity": member["canonical_identity"],
                **member["lexical_match_evidence"],
            }
            for source in SOURCES
            for member in selected[source]
            if "lexical_match_evidence" in member
        ]
        packs.append({
            "direction_id": direction_id,
            "direction_type": direction["direction_type"],
            "primary_category_id": direction["primary_category_id"],
            "direction_label": direction["direction_label"],
            "candidate_state": candidate["candidate_state"],
            "reason_codes": list(candidate["reason_codes"]),
            "source_facts_side_by_side": {source: dict(per_source[source]) for source in SOURCES},
            "source_supply_inference_risk_by_source": dict(candidate["source_supply_inference_risk_by_source"]),
            "observed_subcategory_distribution": dict(direction["observed_subcategory_distribution"]),
            "representative_confirmed_members_by_source": selected,
            "lexical_match_evidence": lexical_evidence,
            "freshness_evidence_by_source": {
                source: {
                    "available_count": per_source[source]["freshness_available_count"],
                    "available_rate": per_source[source]["freshness_available_rate"],
                    "share_le_90d": per_source[source]["freshness_share_le_90d"],
                    "share_le_365d": per_source[source]["freshness_share_le_365d"],
                    "age_days_p50": per_source[source]["freshness_age_days_p50"],
                    "age_days_p90": per_source[source]["freshness_age_days_p90"],
                }
                for source in SOURCES
            },
            "source_native_engagement_by_source": {
                source: per_source[source]["source_native_engagement"] for source in SOURCES
            },
            "voxel_paid_price_evidence": {
                "paid_evidence_scope": per_source["voxel"]["paid_evidence_scope"],
                "paid_state_counts": per_source["voxel"]["paid_state_counts"],
                "paid_evidence_observed_count": per_source["voxel"]["paid_evidence_observed_count"],
                "paid_share_among_observed": per_source["voxel"]["paid_share_among_observed"],
                "price_band_counts": per_source["voxel"]["price_band_counts"],
                "price_available_count": per_source["voxel"]["price_available_count"],
                "price_quantiles_by_currency": per_source["voxel"]["price_quantiles_by_currency"],
            },
            "risk_notes": risk_notes,
            "external_validation_statement": "No external validation has been performed.",
            "representative_order_is_evidence_sampling_not_product_ranking": True,
            "input_sqlite_sha256": loaded["input_sha256"],
            "taxonomy_version": TAXONOMY_VERSION,
            "taxonomy_sha256": TAXONOMY_SHA256,
            "direction_schema_version": SCHEMA_VERSION,
        })

    by_category = {str(row["category_id"]): row for row in loaded["taxonomy"]}
    source_member_counts = Counter((str(row["primary_category_id"]), str(row["source"])) for row in loaded["members"])
    directions_by_category: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    evaluations_by_category: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for direction in directions:
        directions_by_category[str(direction["primary_category_id"])].append(direction)
    for evaluation in evaluations:
        evaluations_by_category[str(evaluation["primary_category_id"])].append(evaluation)
    candidates_by_category_state: dict[tuple[str, str], list[str]] = defaultdict(list)
    for evaluation in evaluations:
        if evaluation["candidate_state"] in {"ADVANCE_TO_STAGE_D", "WATCH_COVERAGE_LIMITED"}:
            candidates_by_category_state[(str(evaluation["primary_category_id"]), str(evaluation["candidate_state"]))].append(str(evaluation["direction_id"]))
    facts_by_id_source = {(str(row["direction_id"]), str(row["source"])): row for row in source_facts}

    summaries: list[dict[str, Any]] = []
    for category in loaded["taxonomy"]:
        category_id = str(category["category_id"])
        category_directions = directions_by_category[category_id]
        state_counts = Counter(str(row["candidate_state"]) for row in evaluations_by_category[category_id])
        ids_by_state = {
            state: sorted(candidates_by_category_state[(category_id, state)])
            for state in CANDIDATE_STATES
        }
        zero_low_notes: list[str] = []
        if sum(source_member_counts[(category_id, source)] for source in SOURCES) == 0:
            zero_low_notes.append("NO_CONFIRMED_MEMBERS_IN_CATEGORY")
        if not any(row["direction_type"] == "LEXICAL_SUBNICHE" for row in category_directions):
            zero_low_notes.append("NO_LEXICAL_SUBNICHE_SURVIVED_CATEGORY_LOCAL_GATES")
        if not ids_by_state["ADVANCE_TO_STAGE_D"] and not ids_by_state["WATCH_COVERAGE_LIMITED"]:
            zero_low_notes.append("NO_ADVANCE_OR_WATCH_DIRECTION")
        risky_ids = {
            str(direction["direction_id"])
            for direction in category_directions
            if any(
                facts_by_id_source[(str(direction["direction_id"]), source)]["source_supply_inference_risk"]
                in {"VERY_HIGH", "HIGH", "MATERIAL"}
                for source in SOURCES
            )
        }
        voxel_paid_directions = sum(
            int(facts_by_id_source[(str(direction["direction_id"]), "voxel")]["paid_evidence_observed_count"] or 0) > 0
            for direction in category_directions
        )
        summaries.append({
            "category_id": category_id,
            "category_order": int(category["category_order"]),
            "category_name": category["category_name"],
            "category_definition": category["definition"],
            "confirmed_member_count_by_source": {
                source: source_member_counts[(category_id, source)] for source in SOURCES
            },
            "source_coverage_context": {
                source: dict(loaded["source_coverage_by_source"][source]) for source in SOURCES
            },
            "baseline_direction_count": sum(row["direction_type"] == "SUBCATEGORY_BASELINE" for row in category_directions),
            "lexical_direction_count": sum(row["direction_type"] == "LEXICAL_SUBNICHE" for row in category_directions),
            "candidate_state_counts": {state: state_counts.get(state, 0) for state in CANDIDATE_STATES},
            "candidate_direction_ids_by_state": ids_by_state,
            "zero_low_evidence_notes": zero_low_notes,
            "directions_with_voxel_paid_evidence_count": int(voxel_paid_directions),
            "directions_affected_by_very_high_high_or_material_supply_risk_count": len(risky_ids),
            "taxonomy_version": TAXONOMY_VERSION,
            "taxonomy_sha256": TAXONOMY_SHA256,
            "direction_schema_version": SCHEMA_VERSION,
        })

    return candidates, packs, summaries


def _build_tables(loaded: Mapping[str, Any]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    lexical, lexical_memberships, phrase_exclusions = _mine_lexical_directions(loaded)
    directions = _build_direction_universe(loaded, lexical)
    memberships = _build_memberships(loaded, lexical_memberships)
    source_facts = _build_source_facts(loaded, directions, memberships)
    evaluations = _build_evaluations(directions, source_facts, memberships)
    candidates, packs, summaries = _build_candidate_tables(
        loaded, directions, memberships, source_facts, evaluations
    )
    tables = {
        "direction_universe": directions,
        "direction_memberships": memberships,
        "direction_source_facts": source_facts,
        "direction_evaluations": evaluations,
        "candidate_directions": candidates,
        "direction_evidence_packs": packs,
        "category_direction_summary": summaries,
    }
    return tables, phrase_exclusions


PREFERRED_COLUMNS = {
    "direction_universe": (
        "primary_category_order", "primary_category_id", "direction_type", "direction_id",
        "direction_label", "canonical_direction_key", "baseline_subcategory_id",
    ),
    "direction_memberships": (
        "primary_category_order", "primary_category_id", "direction_type", "direction_id",
        "source", "source_resource_id", "canonical_identity", "subcategory_id",
    ),
    "direction_source_facts": (
        "primary_category_order", "primary_category_id", "direction_type", "direction_id", "source",
    ),
    "direction_evaluations": (
        "primary_category_order", "primary_category_id", "direction_type", "direction_id", "candidate_state",
    ),
    "candidate_directions": (
        "primary_category_order", "primary_category_id", "direction_type", "direction_id", "candidate_state",
    ),
    "direction_evidence_packs": ("primary_category_id", "direction_id"),
    "category_direction_summary": ("category_order", "category_id", "category_name"),
}
EMPTY_TABLE_COLUMNS = {
    "direction_evidence_packs": ("direction_id", "direction_type", "primary_category_id", "candidate_state", "record_json"),
    "candidate_directions": (
        "direction_id", "direction_type", "primary_category_id", "primary_category_order",
        "direction_label", "observed_member_identity_count", "observed_member_count_by_source",
        "candidate_state", "reason_codes", "source_pattern_state_by_source",
        "source_supply_inference_risk_by_source", "coverage_risk_notes", "input_sqlite_sha256",
        "taxonomy_version", "taxonomy_sha256", "direction_schema_version", "record_json",
    ),
}
FORCED_REAL_COLUMNS = {
    "observed_supply_share_within_category_source", "demand_available_rate",
    "source_category_demand_percentile_p50", "source_category_demand_percentile_p75",
    "source_category_demand_percentile_p90", "share_at_or_above_source_category_demand_percentile_75",
    "share_at_or_above_source_category_demand_percentile_90", "downloads_total_p50", "downloads_total_p90",
    "freshness_available_rate", "freshness_share_le_90d", "freshness_share_le_365d",
    "freshness_age_days_p50", "freshness_age_days_p90", "price_amount",
}
FORCED_INTEGER_COLUMNS = {
    "primary_category_order", "observed_member_identity_count", "lexical_token_count", "title_occurrence_count",
    "subcategory_order", "source_span_start", "source_span_end", "observed_confirmed_member_count",
    "category_source_confirmed_count", "demand_available_count",
    "count_at_or_above_source_category_demand_percentile_75",
    "count_at_or_above_source_category_demand_percentile_90", "freshness_available_count",
    "freshness_unknown_count", "price_available_count", "member_count",
    "baseline_direction_count", "lexical_direction_count", "directions_with_voxel_paid_evidence_count",
    "directions_affected_by_very_high_high_or_material_supply_risk_count",
}


def _output_columns(table: str, rows: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    present = {key for row in rows for key in row}
    if not present:
        return tuple(name for name in EMPTY_TABLE_COLUMNS.get(table, ()) if name != "record_json")
    preferred = PREFERRED_COLUMNS[table]
    return (*[name for name in preferred if name in present], *sorted(present - set(preferred)))


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
    columns = _output_columns(table, rows)
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n", extrasaction="raise")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: _csv_value(row.get(column)) for column in columns})
    return buffer.getvalue().encode("utf-8")


def _sqlite_type(column: str, rows: Sequence[Mapping[str, Any]]) -> str:
    values = [row.get(column) for row in rows if row.get(column) is not None]
    if any(isinstance(value, (dict, list)) for value in values):
        return "TEXT"
    if any(isinstance(value, float) for value in values) or column in FORCED_REAL_COLUMNS:
        return "REAL"
    if any(isinstance(value, int) and not isinstance(value, bool) for value in values) or column in FORCED_INTEGER_COLUMNS:
        return "INTEGER"
    return "TEXT"


def _sqlite_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return canonical_json(value)
    if isinstance(value, bool):
        return int(value)
    return value


def _quote(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise CategoryDirectionError(f"Unsafe generated SQLite identifier: {identifier}")
    return f'"{identifier}"'


PRIMARY_KEYS = {
    "direction_universe": ("direction_id",),
    "direction_memberships": ("direction_id", "source", "source_resource_id"),
    "direction_source_facts": ("direction_id", "source"),
    "direction_evaluations": ("direction_id",),
    "candidate_directions": ("direction_id",),
    "direction_evidence_packs": ("direction_id",),
    "category_direction_summary": ("category_id",),
}


def _write_sqlite(
    path: Path,
    loaded: Mapping[str, Any],
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    run_metadata: Mapping[str, Any],
) -> None:
    if path.exists():
        path.unlink()
    db = sqlite3.connect(path)
    try:
        db.execute("PRAGMA page_size = 4096")
        db.execute("PRAGMA journal_mode = DELETE")
        db.execute("PRAGMA synchronous = FULL")
        db.execute("PRAGMA foreign_keys = ON")
        db.execute("PRAGMA user_version = 75")
        db.execute("CREATE TABLE run_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID")
        db.executemany(
            "INSERT INTO run_metadata(key,value) VALUES (?,?)",
            [(key, canonical_json(value) if isinstance(value, (dict, list)) else str(value))
             for key, value in sorted(run_metadata.items())],
        )
        db.execute("""CREATE TRIGGER run_metadata_immutable_update BEFORE UPDATE ON run_metadata
            BEGIN SELECT RAISE(ABORT,'run_metadata is immutable'); END""")
        db.execute("""CREATE TRIGGER run_metadata_immutable_delete BEFORE DELETE ON run_metadata
            BEGIN SELECT RAISE(ABORT,'run_metadata is immutable'); END""")
        db.execute("""CREATE TABLE frozen_taxonomy_snapshot (
            category_id TEXT PRIMARY KEY,
            category_order INTEGER NOT NULL UNIQUE,
            taxonomy_version TEXT NOT NULL,
            category_json TEXT NOT NULL
        ) WITHOUT ROWID""")
        db.executemany(
            "INSERT INTO frozen_taxonomy_snapshot VALUES (?,?,?,?)",
            [(str(row["category_id"]), int(row["category_order"]), TAXONOMY_VERSION, canonical_json(row))
             for row in loaded["taxonomy"]],
        )
        db.execute("""CREATE TABLE source_coverage_snapshot (
            source TEXT PRIMARY KEY, record_json TEXT NOT NULL
        ) WITHOUT ROWID""")
        db.executemany(
            "INSERT INTO source_coverage_snapshot VALUES (?,?)",
            [(source, canonical_json(loaded["source_coverage_by_source"][source])) for source in SOURCES],
        )
        for table in TABLES:
            rows = list(tables[table])
            columns = _output_columns(table, rows)
            if not columns:
                columns = tuple(name for name in EMPTY_TABLE_COLUMNS.get(table, ()) if name != "record_json")
            definitions = [f"{_quote(column)} {_sqlite_type(column, rows)}" for column in columns]
            definitions.append('"record_json" TEXT NOT NULL')
            definitions.append("PRIMARY KEY (" + ",".join(_quote(key) for key in PRIMARY_KEYS[table]) + ")")
            if table == "direction_universe":
                definitions.append('FOREIGN KEY ("primary_category_id") REFERENCES frozen_taxonomy_snapshot(category_id)')
            elif table == "direction_memberships":
                definitions.append('FOREIGN KEY ("direction_id") REFERENCES direction_universe(direction_id)')
                definitions.append('FOREIGN KEY ("primary_category_id") REFERENCES frozen_taxonomy_snapshot(category_id)')
            elif table in {"direction_source_facts", "direction_evaluations", "candidate_directions", "direction_evidence_packs"}:
                definitions.append('FOREIGN KEY ("direction_id") REFERENCES direction_universe(direction_id)')
                if table != "direction_source_facts":
                    definitions.append('FOREIGN KEY ("primary_category_id") REFERENCES frozen_taxonomy_snapshot(category_id)')
            elif table == "category_direction_summary":
                definitions.append('FOREIGN KEY ("category_id") REFERENCES frozen_taxonomy_snapshot(category_id)')
            db.execute(f"CREATE TABLE {_quote(table)} ({','.join(definitions)}) WITHOUT ROWID")
            if rows:
                insert_columns = (*columns, "record_json")
                insert_sql = (
                    f"INSERT INTO {_quote(table)} ({','.join(_quote(name) for name in insert_columns)}) "
                    f"VALUES ({','.join('?' for _ in insert_columns)})"
                )
                db.executemany(
                    insert_sql,
                    [tuple(_sqlite_value(row.get(column)) for column in columns) + (canonical_json(row),) for row in rows],
                )
        db.commit()
    finally:
        db.close()


def _write_core(
    output: Path,
    loaded: Mapping[str, Any],
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    run_metadata: Mapping[str, Any],
) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    for filename in (
        "GOAL_ALIGNMENT.md", "DIRECTION_DISCOVERY_SCHEMA.md", "WHITESPACE_SEMANTICS.md", "TEXT_NORMALIZATION.md"
    ):
        source = repo_root / filename
        if not source.is_file():
            raise CategoryDirectionError(f"Required YEE-75 contract file is missing: {filename}")
        (output / filename).write_bytes(source.read_bytes())
    for table in CSV_TABLES:
        (output / f"{table}.jsonl").write_bytes(_jsonl_bytes(tables[table]))
        (output / f"{table}.csv").write_bytes(_csv_bytes(table, tables[table]))
    (output / "direction_evidence_packs.jsonl").write_bytes(_jsonl_bytes(tables["direction_evidence_packs"]))
    _write_sqlite(output / "category_direction_discovery.sqlite", loaded, tables, run_metadata)


def _run_metadata(loaded: Mapping[str, Any], code_commit: str | None) -> dict[str, Any]:
    commit = code_commit or "working-tree"
    identity = {
        "work_order": WORK_ORDER,
        "direction_schema_version": SCHEMA_VERSION,
        "discovery_rule_version": DISCOVERY_RULE_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "whitespace_semantics_version": WHITESPACE_SEMANTICS_VERSION,
        "functional_direction_guard_version": FUNCTIONAL_DIRECTION_GUARD_VERSION,
        "functional_cohesion_version": FUNCTIONAL_COHESION_VERSION,
        "input_work_order": INPUT_WORK_ORDER,
        "input_merge_commit": INPUT_MERGE_COMMIT,
        "input_sqlite_sha256": loaded["input_sha256"],
        "taxonomy_version": TAXONOMY_VERSION,
        "taxonomy_sha256": TAXONOMY_SHA256,
        "analysis_as_of": loaded["analysis_as_of"],
        "code_commit": commit,
    }
    run_id = hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()
    return {**identity, "run_id": run_id, "input_identity_set_sha256": loaded["identity_set_sha256"]}


def _export_checks(output: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for table in CSV_TABLES:
        rows = list(tables[table])
        jsonl_path = output / f"{table}.jsonl"
        csv_path = output / f"{table}.csv"
        jsonl_rows = [json.loads(line) for line in jsonl_path.read_text(encoding="utf-8").splitlines()]
        if [canonical_json(row) for row in jsonl_rows] != [canonical_json(row) for row in rows]:
            raise CategoryDirectionError(f"{table} JSONL does not reconcile to generated rows")
        with csv_path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            columns = _output_columns(table, rows)
            if tuple(reader.fieldnames or ()) != columns:
                raise CategoryDirectionError(f"{table} CSV columns do not reconcile")
            csv_rows = list(reader)
        expected_csv_rows = [
            {column: _csv_value(row.get(column)) for column in columns}
            for row in rows
        ]
        if csv_rows != expected_csv_rows:
            raise CategoryDirectionError(f"{table} CSV cells do not reconcile")
        counts[f"{table}_jsonl"] = len(jsonl_rows)
        counts[f"{table}_csv"] = len(csv_rows)

    pack_rows = list(tables["direction_evidence_packs"])
    pack_export = [json.loads(line) for line in (output / "direction_evidence_packs.jsonl").read_text(encoding="utf-8").splitlines()]
    if [canonical_json(row) for row in pack_export] != [canonical_json(row) for row in pack_rows]:
        raise CategoryDirectionError("Evidence pack JSONL does not reconcile")
    counts["direction_evidence_packs_jsonl"] = len(pack_export)

    connection = sqlite3.connect(f"file:{(output / 'category_direction_discovery.sqlite').resolve().as_posix()}?mode=ro", uri=True)
    try:
        integrity_result = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        foreign_key_violations = int(connection.execute("SELECT COUNT(*) FROM pragma_foreign_key_check").fetchone()[0])
        if integrity_result != "ok" or foreign_key_violations:
            raise CategoryDirectionError("Generated SQLite integrity or foreign-key check failed")
        for table in TABLES:
            rows = list(tables[table])
            actual_count = int(connection.execute(f"SELECT COUNT(*) FROM {_quote(table)}").fetchone()[0])
            if actual_count != len(rows):
                raise CategoryDirectionError(f"SQLite row count mismatch for {table}")
            counts[f"{table}_sqlite"] = actual_count
            if rows:
                key = PRIMARY_KEYS[table]
                order = ",".join(_quote(name) for name in key)
                serialized = [str(row[0]) for row in connection.execute(f"SELECT record_json FROM {_quote(table)} ORDER BY {order}")]
                expected = [canonical_json(row) for row in sorted(rows, key=lambda row: tuple(str(row[name]) for name in key))]
                if serialized != expected:
                    raise CategoryDirectionError(f"SQLite records do not reconcile for {table}")
    finally:
        connection.close()
    return {
        "counts": counts,
        "sqlite_integrity_ok": integrity_result == "ok",
        "sqlite_foreign_key_violations": foreign_key_violations,
    }


def _forbidden_output_key(rows: Any) -> bool:
    forbidden = {
        "opportunity_state", "category_opportunity_state", "opportunity_score", "score", "rank", "ranking",
        "winner", "winner_id", "recommendation", "recommendations", "product_concept", "concept_card",
    }
    if isinstance(rows, Mapping):
        return any(str(key).casefold() in forbidden or _forbidden_output_key(value) for key, value in rows.items())
    if isinstance(rows, list):
        return any(_forbidden_output_key(value) for value in rows)
    return False


def _logic_regression_checks() -> dict[str, bool]:
    def fact(state: str, risk: str, members: int) -> dict[str, Any]:
        return {
            "observed_whitespace_pattern_state": state,
            "source_supply_inference_risk": risk,
            "observed_confirmed_member_count": members,
        }

    voxel_only, _ = _evaluate_candidate_state({
        "hangar": fact("INSUFFICIENT_EVIDENCE", "MATERIAL", 0),
        "voxel": fact("OBSERVED_STRONG_PATTERN", "VERY_HIGH", 3),
    }, 3)
    two_source, _ = _evaluate_candidate_state({
        "hangar": fact("OBSERVED_SUPPORTED_PATTERN", "MATERIAL", 2),
        "voxel": fact("OBSERVED_SUPPORTED_PATTERN", "VERY_HIGH", 1),
    }, 3)
    strong_hangar, _ = _evaluate_candidate_state({
        "hangar": fact("OBSERVED_STRONG_PATTERN", "MATERIAL", 3),
        "voxel": fact("INSUFFICIENT_EVIDENCE", "VERY_HIGH", 0),
    }, 3)
    return {
        "voxel_very_high_strong_does_not_advance_alone": voxel_only == "WATCH_COVERAGE_LIMITED",
        "two_source_supported_evidence_advances": two_source == "ADVANCE_TO_STAGE_D",
        "strong_non_very_high_source_advances": strong_hangar == "ADVANCE_TO_STAGE_D",
        "supply_band_boundary_contract": (
            _observed_supply_band(1, 5)[0] == "SPARSE"
            and _observed_supply_band(5, 25)[0] == "SPARSE"
            and _observed_supply_band(5, 24)[0] == "LIMITED"
            and _observed_supply_band(10, 29)[0] == "LIMITED"
            and _observed_supply_band(10, 28)[0] == "BROAD"
            and _observed_supply_band(0, 0) == ("ZERO", None, "INSUFFICIENT")
        ),
        "demand_strength_boundary_contract": (
            _demand_strength(1, 99, 1.0) == "INSUFFICIENT"
            and _demand_strength(2, 65, 0.40) == "HIGH"
            and _demand_strength(2, 54, 0.33) == "MODERATE"
            and _demand_strength(2, 44.999, 0.249) == "LOW"
            and _demand_strength(2, 50, 0.30) == "MIXED"
        ),
        "whitespace_state_boundary_contract": (
            _whitespace_state(1, 2, 5, "HIGH", "SPARSE") == "INSUFFICIENT_EVIDENCE"
            and _whitespace_state(3, 3, 10, "HIGH", "SPARSE") == "OBSERVED_STRONG_PATTERN"
            and _whitespace_state(2, 2, 10, "MODERATE", "LIMITED") == "OBSERVED_SUPPORTED_PATTERN"
            and _whitespace_state(3, 3, 10, "HIGH", "BROAD") == "DEMAND_WITH_BROAD_SUPPLY"
            and _whitespace_state(2, 2, 10, "LOW", "LIMITED") == "LOW_DEMAND_PATTERN"
        ),
    }


def _qa_checks(
    loaded: Mapping[str, Any],
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    phrase_exclusions: Mapping[str, int],
    export_check: Mapping[str, Any],
    input_sha256_after: str,
    replay_ok: bool,
    *,
    enforce_pinned_inputs: bool,
) -> dict[str, bool]:
    directions = list(tables["direction_universe"])
    memberships = list(tables["direction_memberships"])
    source_facts = list(tables["direction_source_facts"])
    evaluations = list(tables["direction_evaluations"])
    candidates = list(tables["candidate_directions"])
    packs = list(tables["direction_evidence_packs"])
    summaries = list(tables["category_direction_summary"])
    directions_by_id = {str(row["direction_id"]): row for row in directions}
    category_member_counts = Counter((str(row["primary_category_id"]), str(row["source"])) for row in loaded["members"])
    baseline_by_identity: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    lexical_members_by_direction: dict[str, set[str]] = defaultdict(set)
    membership_by_direction: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    members_seen: set[tuple[str, str]] = set()
    for row in memberships:
        identity_key = (str(row["source"]), str(row["source_resource_id"]))
        member = loaded["members_by_identity"].get(identity_key)
        if member is None or str(member["canonical_identity"]) != str(row["canonical_identity"]):
            raise CategoryDirectionError("Output membership references a non-input identity")
        direction = directions_by_id.get(str(row["direction_id"]))
        if direction is None or direction["primary_category_id"] != row["primary_category_id"]:
            raise CategoryDirectionError("Output membership has missing or cross-category direction reference")
        membership_by_direction[str(row["direction_id"])].append(row)
        members_seen.add(identity_key)
        if row["direction_type"] == "SUBCATEGORY_BASELINE":
            baseline_by_identity[str(row["canonical_identity"])].append(row)
            if row["subcategory_id"] != member["subcategory_id"] or row["primary_category_id"] != member["primary_category_id"]:
                raise CategoryDirectionError("Baseline membership changed the accepted primary subcategory")
        else:
            lexical_members_by_direction[str(row["direction_id"])].add(str(row["canonical_identity"]))

    baseline_directions = [row for row in directions if row["direction_type"] == "SUBCATEGORY_BASELINE"]
    lexical_directions = [row for row in directions if row["direction_type"] == "LEXICAL_SUBNICHE"]
    baseline_member_count = sum(row["direction_type"] == "SUBCATEGORY_BASELINE" for row in memberships)
    member_set_redundancy: set[tuple[str, tuple[str, ...]]] = set()
    label_key_by_category: dict[str, set[str]] = {}
    for category in loaded["taxonomy"]:
        keys = {_normalized_label_key(str(category.get("category_name", "")))}
        keys.update(_normalized_label_key(str(item["subcategory_id"]).replace("_", " ")) for item in category["subcategories"])
        label_key_by_category[str(category["category_id"])] = keys
    for direction in lexical_directions:
        direction_id = str(direction["direction_id"])
        category_id = str(direction["primary_category_id"])
        member_ids = lexical_members_by_direction[direction_id]
        category_size = sum(category_member_counts[(category_id, source)] for source in SOURCES)
        min_support = 3 if int(direction["lexical_token_count"]) == 1 else 2
        if len(member_ids) < min_support or len(member_ids) / category_size > 0.5:
            raise CategoryDirectionError(f"Lexical support/broadness gate failed for {direction_id}")
        if direction["canonical_direction_key"] in label_key_by_category[category_id]:
            raise CategoryDirectionError(f"Taxonomy label was emitted as lexical direction: {direction_id}")
        if direction["direction_id"] != _direction_id(category_id, "LEXICAL_SUBNICHE", str(direction["canonical_direction_key"])):
            raise CategoryDirectionError(f"Unstable lexical direction ID: {direction_id}")
        members_key = (category_id, tuple(sorted(member_ids)))
        if members_key in member_set_redundancy:
            raise CategoryDirectionError("Equivalent lexical member sets were not deterministically collapsed")
        member_set_redundancy.add(members_key)
        for membership in membership_by_direction[direction_id]:
            if membership["normalized_match"] != direction["canonical_direction_key"]:
                raise CategoryDirectionError("Lexical membership does not contain an exact canonical phrase match")
            if membership["match_field"] not in {"title", "summary_first_sentence"}:
                raise CategoryDirectionError("Lexical membership came from a prohibited text surface")

    for member in loaded["members"]:
        identity = str(member["canonical_identity"])
        rows = baseline_by_identity.get(identity, [])
        if len(rows) != 1:
            raise CategoryDirectionError(f"Confirmed member does not have exactly one baseline membership: {identity}")

    expected_candidate_ids = {
        str(row["direction_id"]) for row in evaluations
        if row["candidate_state"] in {"ADVANCE_TO_STAGE_D", "WATCH_COVERAGE_LIMITED"}
    }
    actual_candidate_ids = {str(row["direction_id"]) for row in candidates}
    pack_ids = {str(row["direction_id"]) for row in packs}
    if actual_candidate_ids != expected_candidate_ids or pack_ids != expected_candidate_ids:
        raise CategoryDirectionError("Candidate/evidence-pack subset does not reconcile to evaluation states")
    for fact in source_facts:
        direction_id = str(fact["direction_id"])
        direction = directions_by_id[direction_id]
        expected_denominator = int(loaded["category_fact_by_key"][(str(direction["primary_category_id"]), str(fact["source"]))]["member_count"])
        if int(fact["category_source_confirmed_count"]) != expected_denominator:
            raise CategoryDirectionError("Observed-supply denominator is not the frozen category/source confirmed count")
        if int(fact["observed_confirmed_member_count"]) > expected_denominator:
            raise CategoryDirectionError("Direction members exceed their category/source denominator")
        if fact["source"] == "hangar" and any(fact[field] is not None for field in (
            "paid_state_counts", "paid_evidence_observed_count", "paid_share_among_observed",
            "price_band_counts", "price_available_count", "price_quantiles_by_currency",
        )):
            raise CategoryDirectionError("Hangar paid/price evidence must remain unavailable, not free or zero")
        if fact["source"] == "voxel" and fact["paid_evidence_scope"] != "SOURCE_AVAILABLE":
            raise CategoryDirectionError("Voxel paid/price evidence scope was lost")

    source_rows_per_direction = Counter(str(row["direction_id"]) for row in source_facts)
    source_by_direction = {(str(row["direction_id"]), str(row["source"])) for row in source_facts}
    expected_source_keys = {(str(row["direction_id"]), source) for row in directions for source in SOURCES}
    logic_checks = _logic_regression_checks()
    summary_orders = [int(row["category_order"]) for row in summaries]
    expected_category_orders = [int(row["category_order"]) for row in loaded["taxonomy"]]
    all_output_rows = [row for table in TABLES for row in tables[table]]
    no_forbidden_fields = not _forbidden_output_key(all_output_rows)
    candidate_state_counts = Counter(str(row["candidate_state"]) for row in evaluations)
    lexical_directions_by_id = {str(row["direction_id"]): row for row in lexical_directions}
    lexical_candidate_ids = {
        str(row["direction_id"]) for row in candidates if row["direction_type"] == "LEXICAL_SUBNICHE"
    }
    lexical_advance_ids = {
        str(row["direction_id"]) for row in evaluations
        if row["candidate_state"] == "ADVANCE_TO_STAGE_D"
        and str(row["direction_id"]) in lexical_directions_by_id
    }
    sample_ok = True
    for pack in packs:
        for source in SOURCES:
            sample = pack["representative_confirmed_members_by_source"][source]
            if len(sample) > REPRESENTATIVE_LIMIT_PER_SOURCE or len({row["canonical_identity"] for row in sample}) != len(sample):
                sample_ok = False
            sort_keys = [(
                row["source_category_demand_percentile"] is None,
                -(row["source_category_demand_percentile"] or 0.0),
                row["freshness_age_days"] is None,
                row["freshness_age_days"] if row["freshness_age_days"] is not None else 0,
                row["canonical_identity"],
            ) for row in sample]
            if sort_keys != sorted(sort_keys):
                sample_ok = False
    coverage_risks = {
        source: _coverage_risk(loaded["source_coverage_by_source"][source]["yee61_confirmed_fraction"])
        for source in SOURCES
    }
    expected_rows = {
        "direction_universe": len(directions),
        "direction_memberships": len(memberships),
        "direction_source_facts": len(source_facts),
        "direction_evaluations": len(evaluations),
        "candidate_directions": len(candidates),
        "direction_evidence_packs": len(packs),
        "category_direction_summary": len(summaries),
    }
    checks = {
        "input_sha256_matches_pin_before": (not enforce_pinned_inputs) or loaded["input_sha256"] == INPUT_SQLITE_SHA256,
        "input_sha256_unchanged_after_processing": loaded["input_sha256"] == input_sha256_after,
        "input_identity_set_exact": set(members_seen) == set(loaded["members_by_identity"]),
        "confirmed_source_counts_exact": (not enforce_pinned_inputs) or loaded["member_counts_by_source"] == CONFIRMED_SOURCE_COUNTS,
        "frozen_taxonomy_version_hash_and_counts_exact": (
            loaded["metadata"].get("input_taxonomy_version") == TAXONOMY_VERSION
            and loaded["metadata"].get("taxonomy_sha256") == TAXONOMY_SHA256
            and (not enforce_pinned_inputs or (
                len(loaded["taxonomy"]) == 11
                and sum(len(row["subcategories"]) for row in loaded["taxonomy"]) == 38
            ))
        ),
        "exactly_38_baseline_directions": (
            len(baseline_directions) == 38 if enforce_pinned_inputs
            else len(baseline_directions) == sum(len(row["subcategories"]) for row in loaded["taxonomy"])
        ),
        "one_correct_baseline_membership_per_confirmed_member": baseline_member_count == len(loaded["members"]) and all(len(baseline_by_identity[str(row["canonical_identity"])]) == 1 for row in loaded["members"]),
        "lexical_direction_memberships_stay_category_local": all(
            row["primary_category_id"] == directions_by_id[str(row["direction_id"])]["primary_category_id"]
            for row in memberships if row["direction_type"] == "LEXICAL_SUBNICHE"
        ),
        "candidate_lexical_directions_pass_independent_functional_guard": all(
            _qa_functional_direction_guard(
                str(lexical_directions_by_id[direction_id]["canonical_direction_key"])
            ) == "FUNCTIONAL_DIRECTION"
            for direction_id in lexical_candidate_ids
        ),
        "candidate_lexical_directions_pass_independent_functional_cohesion": all(
            _qa_functional_cohesion_guard(
                str(lexical_directions_by_id[direction_id]["canonical_direction_key"])
            )[0] == "FUNCTIONAL_COHESION"
            for direction_id in lexical_candidate_ids
        ),
        "advance_lexical_directions_pass_independent_functional_cohesion": all(
            _qa_functional_cohesion_guard(
                str(lexical_directions_by_id[direction_id]["canonical_direction_key"])
            )[0] == "FUNCTIONAL_COHESION"
            for direction_id in lexical_advance_ids
        ),
        "retained_lexical_directions_pass_independent_functional_guard": all(
            _qa_functional_direction_guard(str(row["canonical_direction_key"])) == "FUNCTIONAL_DIRECTION"
            for row in lexical_directions
        ),
        "retained_lexical_directions_pass_independent_functional_cohesion": all(
            _qa_functional_cohesion_guard(str(row["canonical_direction_key"]))
            == ("FUNCTIONAL_COHESION", tuple(row.get("cohesive_content_tokens", ())))
            for row in lexical_directions
        ),
        "retained_lexical_directions_have_versioned_guard_provenance": all(
            row.get("functional_direction_guard_version") == FUNCTIONAL_DIRECTION_GUARD_VERSION
            and row.get("semantic_guard_classification") == "FUNCTIONAL_DIRECTION"
            and bool(row.get("functional_content_tokens"))
            for row in lexical_directions
        ),
        "retained_lexical_directions_have_versioned_cohesion_provenance": all(
            row.get("functional_cohesion_version") == FUNCTIONAL_COHESION_VERSION
            and row.get("functional_cohesion_classification") == "FUNCTIONAL_COHESION"
            and bool(row.get("cohesive_content_tokens"))
            for row in lexical_directions
        ),
        "generation_and_independent_qa_guard_vocabularies_match": (
            NON_DIRECTION_BOILERPLATE_TOKENS == _QA_NON_DIRECTION_BOILERPLATE_TOKENS
            and NON_DIRECTION_PREFIX_TOKENS == _QA_NON_DIRECTION_PREFIX_TOKENS
            and NON_DIRECTION_SUFFIX_TOKENS == _QA_NON_DIRECTION_SUFFIX_TOKENS
        ),
        "generation_and_independent_qa_cohesion_contracts_match": (
            STABLE_FUNCTION_SINGLE_TOKEN_ALLOWLIST == _QA_STABLE_FUNCTION_SINGLE_TOKEN_ALLOWLIST
            and COHERENT_FUNCTIONAL_PHRASE_TERMS == _QA_COHERENT_FUNCTIONAL_PHRASE_TERMS
        ),
        "lexical_support_thresholds_enforced": all(
            len(lexical_members_by_direction[str(row["direction_id"])]) >= (3 if int(row["lexical_token_count"]) == 1 else 2)
            for row in lexical_directions
        ),
        "too_broad_phrases_excluded": all(
            len(lexical_members_by_direction[str(row["direction_id"])])
            <= 0.5 * sum(category_member_counts[(str(row["primary_category_id"]), source)] for source in SOURCES)
            for row in lexical_directions
        ) and int(phrase_exclusions.get("TOO_BROAD_FOR_DIRECTION", 0)) >= 0,
        "category_and_subcategory_label_duplicates_excluded": all(
            str(row["canonical_direction_key"]) not in label_key_by_category[str(row["primary_category_id"])]
            for row in lexical_directions
        ),
        "exact_member_set_redundancy_collapsed_deterministically": len(member_set_redundancy) == len(lexical_directions),
        "no_fuzzy_semantic_or_model_merge": all(
            row["discovery_rule_version"] == DISCOVERY_RULE_VERSION
            and row["direction_id"] == _direction_id(str(row["primary_category_id"]), "LEXICAL_SUBNICHE", str(row["canonical_direction_key"]))
            for row in lexical_directions
        ),
        "no_historical_yee30_to_yee59_data_dependency": loaded["metadata"].get("work_order") == INPUT_WORK_ORDER,
        "direction_source_facts_have_two_rows_per_direction": len(source_facts) == 2 * len(directions) and all(source_rows_per_direction[str(row["direction_id"])] == 2 for row in directions) and source_by_direction == expected_source_keys,
        "raw_download_metrics_are_source_local_only": all(row["source"] in SOURCES and "downloads_total_sum" not in row for row in source_facts),
        "observed_supply_uses_category_source_confirmed_denominator": all(
            int(row["category_source_confirmed_count"]) == int(loaded["category_fact_by_key"][(str(row["primary_category_id"]), str(row["source"]))]["member_count"])
            for row in source_facts
        ),
        "supply_demand_and_whitespace_boundaries_hold": all(logic_checks[name] for name in (
            "supply_band_boundary_contract", "demand_strength_boundary_contract", "whitespace_state_boundary_contract"
        )),
        "pinned_voxel_coverage_risk_is_very_high": (not enforce_pinned_inputs) or coverage_risks["voxel"] == "VERY_HIGH",
        "pinned_hangar_coverage_risk_is_material": (not enforce_pinned_inputs) or coverage_risks["hangar"] == "MATERIAL",
        "voxel_only_pattern_cannot_advance_on_branch_a": logic_checks["voxel_very_high_strong_does_not_advance_alone"],
        "two_source_supported_pattern_can_advance": logic_checks["two_source_supported_evidence_advances"],
        "strong_non_very_high_pattern_can_advance": logic_checks["strong_non_very_high_source_advances"],
        "candidate_directions_equal_advance_and_watch_subset": actual_candidate_ids == expected_candidate_ids,
        "evidence_pack_count_equals_candidate_count": len(packs) == len(candidates) and pack_ids == actual_candidate_ids,
        "representative_sampling_bounded_and_deterministic_order": sample_ok,
        "category_summary_has_11_rows_in_frozen_taxonomy_order": (
            len(summaries) == 11 and summary_orders == expected_category_orders
            if enforce_pinned_inputs
            else len(summaries) == len(loaded["taxonomy"]) and summary_orders == expected_category_orders
        ),
        "stage_d_scoring_ranking_and_recommendation_fields_absent": no_forbidden_fields,
        "hangar_paid_unavailable_voxel_currency_groups_preserved": all(
            row["paid_evidence_scope"] == ("SOURCE_AVAILABLE" if row["source"] == "voxel" else "NOT_AVAILABLE_FOR_SOURCE")
            for row in source_facts
        ),
        "jsonl_csv_sqlite_reconcile": bool(export_check.get("counts")) and all(
            export_check["counts"].get(f"{table}_jsonl") == expected_rows[table]
            and export_check["counts"].get(f"{table}_csv") == expected_rows[table]
            for table in CSV_TABLES
        ) and all(export_check["counts"].get(f"{table}_sqlite") == expected_rows[table] for table in TABLES),
        "sqlite_integrity_and_foreign_keys_pass": (
            bool(export_check.get("sqlite_integrity_ok"))
            and int(export_check.get("sqlite_foreign_key_violations", -1)) == 0
        ),
        "deterministic_byte_identical_replay": replay_ok,
        "input_was_opened_read_only": True,
    }
    return checks


def _run_qa(
    loaded: Mapping[str, Any],
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    phrase_exclusions: Mapping[str, int],
    export_check: Mapping[str, Any],
    input_sha256_after: str,
    replay_ok: bool,
    code_commit: str,
    *,
    enforce_pinned_inputs: bool,
) -> dict[str, Any]:
    member_counts_by_category_source = {
        category_id: {
            source: int(loaded["category_fact_by_key"][(category_id, source)]["member_count"])
            for source in SOURCES
        }
        for category_id in loaded["taxonomy_by_id"]
    }
    candidate_states = Counter(str(row["candidate_state"]) for row in tables["direction_evaluations"])
    source_risks = {
        source: _coverage_risk(loaded["source_coverage_by_source"][source]["yee61_confirmed_fraction"])
        for source in SOURCES
    }
    qa = {
        "work_order": WORK_ORDER,
        "delivery_status": DELIVERY_STATUS,
        "status": "PASS",
        "direction_schema_version": SCHEMA_VERSION,
        "discovery_rule_version": DISCOVERY_RULE_VERSION,
        "functional_direction_guard_version": FUNCTIONAL_DIRECTION_GUARD_VERSION,
        "functional_cohesion_version": FUNCTIONAL_COHESION_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "whitespace_semantics_version": WHITESPACE_SEMANTICS_VERSION,
        "run_id": None,
        "code_commit": code_commit,
        "analysis_as_of": loaded["analysis_as_of"],
        "input": {
            "work_order": INPUT_WORK_ORDER,
            "accepted_merge_commit": INPUT_MERGE_COMMIT,
            "sqlite_sha256_before": loaded["input_sha256"],
            "sqlite_sha256_after": input_sha256_after,
            "identity_set_sha256": loaded["identity_set_sha256"],
            "read_only": True,
            "signal_member_count": len(loaded["members"]),
            "member_count_by_source": dict(loaded["member_counts_by_source"]),
            "primary_category_member_count_by_source": member_counts_by_category_source,
            "review_and_out_of_scope_identity_rows_loaded": 0,
        },
        "taxonomy": {
            "version": TAXONOMY_VERSION,
            "sha256": TAXONOMY_SHA256,
            "category_count": len(loaded["taxonomy"]),
            "subcategory_count": sum(len(row["subcategories"]) for row in loaded["taxonomy"]),
        },
        "upstream_supply_inference_risk_by_source": source_risks,
        "phrase_exclusion_counts": dict(phrase_exclusions),
        "row_counts": {table: len(rows) for table, rows in tables.items()},
        "candidate_state_counts": {state: candidate_states.get(state, 0) for state in CANDIDATE_STATES},
        "export_row_counts": dict(export_check.get("counts", {})),
        "deterministic_replay": {
            "complete_artifact_bundle_byte_identical": bool(replay_ok),
            "verified_artifact_count": len(ALL_FILES),
        },
    }
    qa["sqlite_checks"] = {
        "integrity_ok": bool(export_check.get("sqlite_integrity_ok")),
        "foreign_key_violation_count": int(export_check.get("sqlite_foreign_key_violations", -1)),
    }
    checks = _qa_checks(
        loaded, tables, phrase_exclusions, export_check, input_sha256_after, replay_ok,
        enforce_pinned_inputs=enforce_pinned_inputs,
    )
    failed = sorted(name for name, passed in checks.items() if not passed)
    if failed:
        qa["status"] = "FAIL"
    qa.update({
        "checks": checks,
        "check_count": len(checks),
        "passing_checks": sum(bool(value) for value in checks.values()),
        "failed_checks": failed,
    })
    return qa


def _final_report(qa: Mapping[str, Any], tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
    counts = qa["row_counts"]
    state_counts = qa["candidate_state_counts"]
    risks = qa["upstream_supply_inference_risk_by_source"]
    lines = [
        "# YEE-75 final report",
        "",
        f"Status: `{DELIVERY_STATUS}`",
        f"Production QA: `{qa['status']}` ({qa['passing_checks']}/{qa['check_count']} checks)",
        "",
        "## Scope and provenance",
        "",
        "Stage C creates auditable category-local direction evidence from accepted YEE-73 signal members only. The YEE-73 database is read-only; no historical YEE-30 through YEE-59 dataset or decision was used. All supply statements mean observed confirmed supply and do not establish true-market whitespace.",
        "",
        f"- Accepted YEE-73 merge baseline: `{INPUT_MERGE_COMMIT}`",
        f"- Input SQLite SHA-256 before/after: `{qa['input']['sqlite_sha256_before']}` / `{qa['input']['sqlite_sha256_after']}`",
        f"- Input identities: {qa['input']['signal_member_count']} (Hangar {qa['input']['member_count_by_source']['hangar']}; Voxel {qa['input']['member_count_by_source']['voxel']})",
        f"- Analysis as of: `{qa['analysis_as_of']}` (inherited unchanged)",
        f"- Taxonomy: `{TAXONOMY_VERSION}` / `{TAXONOMY_SHA256}`; {qa['taxonomy']['category_count']} categories and {qa['taxonomy']['subcategory_count']} subcategories",
        f"- Lexical guard: `{FUNCTIONAL_DIRECTION_GUARD_VERSION}`; discovery rules `{DISCOVERY_RULE_VERSION}`",
        f"- Functional cohesion: `{FUNCTIONAL_COHESION_VERSION}`; exact reviewed phrase and stable single-function allowlists; no lexical/candidate minimum",
        f"- Recurring prose/marketing n-grams excluded by the functional-direction guard: {qa['phrase_exclusion_counts'].get('NON_DIRECTION_BOILERPLATE', 0)}",
        f"- Non-cohesive/ambiguous lexical phrases excluded before facts/evaluation: {qa['phrase_exclusion_counts'].get('NON_COHESIVE_FUNCTIONAL_DIRECTION', 0)}",
        f"- Code commit: `{qa['code_commit']}`; run ID: `{qa['run_id']}`",
        "",
        "## Direction and candidate reconciliation",
        "",
        f"- Frozen subcategory baselines: {counts['direction_universe'] - sum(row['direction_type'] == 'LEXICAL_SUBNICHE' for row in tables['direction_universe'])}",
        f"- Lexical subniches: {sum(row['direction_type'] == 'LEXICAL_SUBNICHE' for row in tables['direction_universe'])}",
        f"- Direction memberships: {counts['direction_memberships']}; source facts: {counts['direction_source_facts']} (= 2 × directions)",
        f"- Candidate directions/evidence packs: {counts['candidate_directions']} / {counts['direction_evidence_packs']}",
        f"- Candidate states (unranked): ADVANCE_TO_STAGE_D {state_counts['ADVANCE_TO_STAGE_D']}; WATCH_COVERAGE_LIMITED {state_counts['WATCH_COVERAGE_LIMITED']}; NO_CLEAR_PATTERN {state_counts['NO_CLEAR_PATTERN']}; INSUFFICIENT_EVIDENCE {state_counts['INSUFFICIENT_EVIDENCE']}",
        f"- Category summaries: {counts['category_direction_summary']} in frozen taxonomy order",
        "",
        "## Coverage and interpretation guardrails",
        "",
        f"- Upstream confirmation supply-inference risk: Hangar `{risks['hangar']}`, Voxel `{risks['voxel']}`. This measures coverage uncertainty, not source quality.",
        "- Demand percentiles and raw downloads summaries remain direction × source local. No cross-source raw download total or demand comparison is produced.",
        "- Freshness availability and unknown counts are explicit. Hangar paid/price remains unavailable; Voxel paid/price is source-native and currency-separated.",
        "- Baselines include empty frozen subcategories. Lexical phrases are exact normalized n-grams; there is no fuzzy, semantic, embedding, or model-based merge.",
        "- Evidence-pack members are bounded source-local examples ordered for evidence sampling only, not product ranking.",
        "- No external validation has been performed.",
        "",
        "## Validation and stop boundary",
        "",
        f"- Deterministic full artifact replay: `{qa['deterministic_replay']['complete_artifact_bundle_byte_identical']}` ({qa['deterministic_replay']['verified_artifact_count']} artifacts checked).",
        "- Stage C outputs stop at observed category-local direction patterns and evidence packs. No Stage D category opportunity state, global/category ranking, winner, score, research, commercial validation, concept, or recommendation was generated.",
        "",
    ]
    return "\n".join(lines)


def _manifest(output: Path, run_metadata: Mapping[str, Any], qa: Mapping[str, Any]) -> dict[str, Any]:
    artifacts = []
    for name in sorted(ALL_FILES):
        if name == "DATASET_MANIFEST.json":
            continue
        path = output / name
        artifacts.append({"path": name, "size_bytes": path.stat().st_size, "sha256": sha256_file(path)})
    return {
        "work_order": WORK_ORDER,
        "delivery_status": DELIVERY_STATUS,
        "run_id": run_metadata["run_id"],
        "code_commit": run_metadata["code_commit"],
        "analysis_as_of": run_metadata["analysis_as_of"],
        "direction_schema_version": SCHEMA_VERSION,
        "discovery_rule_version": DISCOVERY_RULE_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "whitespace_semantics_version": WHITESPACE_SEMANTICS_VERSION,
        "functional_direction_guard_version": FUNCTIONAL_DIRECTION_GUARD_VERSION,
        "functional_cohesion_version": FUNCTIONAL_COHESION_VERSION,
        "input": {
            "work_order": INPUT_WORK_ORDER,
            "accepted_merge_commit": INPUT_MERGE_COMMIT,
            "filename": "category_signal_layer.sqlite",
            "sha256_before": qa["input"]["sqlite_sha256_before"],
            "sha256_after": qa["input"]["sqlite_sha256_after"],
            "read_only": True,
        },
        "taxonomy": {"version": TAXONOMY_VERSION, "sha256": TAXONOMY_SHA256},
        "artifact_count_excluding_manifest": len(artifacts),
        "artifacts": artifacts,
        "manifest_self_hash": "omitted_to_avoid_recursive_hash",
    }


def _write_final_metadata(output: Path, qa: Mapping[str, Any], tables: Mapping[str, Sequence[Mapping[str, Any]]], run_metadata: Mapping[str, Any]) -> None:
    (output / "QA_RESULT.json").write_text(canonical_json(qa) + "\n", encoding="utf-8", newline="\n")
    (output / "FINAL_REPORT.md").write_text(_final_report(qa, tables), encoding="utf-8", newline="\n")
    manifest = _manifest(output, run_metadata, qa)
    (output / "DATASET_MANIFEST.json").write_text(canonical_json(manifest) + "\n", encoding="utf-8", newline="\n")


def _goal_alignment_gate() -> bool:
    goal_path = Path(__file__).resolve().parents[2] / "GOAL_ALIGNMENT.md"
    if not goal_path.is_file():
        return False
    text = goal_path.read_text(encoding="utf-8")
    required = (
        "GOAL_ALIGNMENT: PASS", "PLUGIN_ONLY", "accepted YEE-73", "read-only",
        "REVIEW", "OUT_OF_SCOPE", "frozen", "Stage C", "Stage D", "true-market",
    )
    return all(term.casefold() in text.casefold() for term in required)


def build_category_direction_discovery(
    input_db: Path | str,
    output_dir: Path | str,
    *,
    code_commit: str | None = None,
    enforce_pinned_inputs: bool = True,
) -> dict[str, Any]:
    """Build deterministic YEE-75 directions and evidence from accepted YEE-73 SQLite only."""
    input_path = Path(input_db).resolve()
    output = Path(output_dir).resolve()
    if not input_path.is_file():
        raise CategoryDirectionError(f"Accepted YEE-73 input database not found: {input_path}")
    output.mkdir(parents=True, exist_ok=True)
    initial_files = {path.name for path in output.iterdir() if path.is_file()}
    if initial_files - {"GOAL_ALIGNMENT.md"}:
        raise CategoryDirectionError("YEE-75 output directory must be empty except for GOAL_ALIGNMENT.md")
    if not _goal_alignment_gate():
        raise CategoryDirectionError("GOAL_ALIGNMENT gate did not pass before dataset generation")

    input_sha_before = sha256_file(input_path)
    loaded = _load_input(input_path, input_sha_before, enforce_pinned_inputs=enforce_pinned_inputs)
    run_metadata = _run_metadata(loaded, code_commit)
    tables, phrase_exclusions = _build_tables(loaded)

    # Independently recompute the deterministic core before accepting its QA state.
    with tempfile.TemporaryDirectory(prefix="yee75-core-replay-") as temp_name:
        replay_dir = Path(temp_name)
        replay_tables, replay_exclusions = _build_tables(loaded)
        replay_metadata = _run_metadata(loaded, code_commit)
        _write_core(replay_dir, loaded, replay_tables, replay_metadata)
        _write_core(output, loaded, tables, run_metadata)
        core_replay_ok = (
            phrase_exclusions == replay_exclusions
            and all((output / name).read_bytes() == (replay_dir / name).read_bytes() for name in CORE_FILES)
        )

    export_check = _export_checks(output, tables)
    input_sha_after = sha256_file(input_path)
    qa = _run_qa(
        loaded, tables, phrase_exclusions, export_check, input_sha_after, core_replay_ok,
        run_metadata["code_commit"], enforce_pinned_inputs=enforce_pinned_inputs,
    )
    qa["run_id"] = run_metadata["run_id"]
    if qa["failed_checks"]:
        raise CategoryDirectionError(f"YEE-75 production QA failed: {qa['failed_checks']}")
    _write_final_metadata(output, qa, tables, run_metadata)

    # Finalize a second independently generated bundle and compare every required byte.
    with tempfile.TemporaryDirectory(prefix="yee75-final-replay-") as temp_name:
        replay_dir = Path(temp_name)
        replay_tables, replay_exclusions = _build_tables(loaded)
        _write_core(replay_dir, loaded, replay_tables, _run_metadata(loaded, code_commit))
        _write_final_metadata(replay_dir, qa, replay_tables, run_metadata)
        final_replay_ok = (
            phrase_exclusions == replay_exclusions
            and all((output / name).read_bytes() == (replay_dir / name).read_bytes() for name in ALL_FILES)
        )
    if not final_replay_ok:
        raise CategoryDirectionError("YEE-75 final byte-identical artifact replay failed")
    return qa
