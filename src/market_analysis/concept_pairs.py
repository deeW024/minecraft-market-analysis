"""Deterministic YEE-37 candidate-pair generation and Jev pilot execution."""

from __future__ import annotations

import base64
import hashlib
import itertools
import json
import math
import re
import sqlite3
import time
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping

from .jev_triage import (
    EXPECTED_JEV_MODEL_VERSION,
    InferenceRequest,
    InputIntegrityError,
    JevTransportError,
    ModelVersionMismatchError,
    RawEvidenceCapture,
    TriageValidationError,
    canonical_json,
    input_identity_set,
    jev_provider_from_env,
    load_eligible_candidates,
    parse_response,
    sha256_bytes,
    sha256_file,
)
from .topic_layer import normalize_tokens


YEE31_RUN_ID = "92c0d3fc21cd63e56d102f3eaa0cf7a3f8f24b29754e11cb8dd4b05c45351086"
YEE31_ACCEPTED_INPUT_SHA256 = "f7958d82803fd4e2081b549568d2fbe9a268dbc06a1a38c60ab56b6f4e43f92e"
YEE30_ACCEPTED_DB_SHA256 = "bc4b7d636b2d64594b7c64ce1583ed9aa080ac6da172398cd2b4e6d8c9451c50"
EXPECTED_ADVANCE_TOPICS = 1_807
PAIR_GENERATION_VERSION = "yee-37-candidate-pairs-v0.1"
PAIR_NORMALIZATION_VERSION = "yee-37-pair-normalization-v0.1"
PAIR_STATE_VERSION = "yee-37-pair-state-v0.2"
PAIR_POLICY_VERSION = "yee-37-pair-policy-v0.2"
PAIR_QUESTION_SET_VERSION = "yee-37-native-pair-questions-v0.2"
PAIR_OUTPUT_VERSION = "yee-37-pair-outcomes-v0.2"
RESOLUTION_POLICY_VERSION = "yee-37-resolution-policy-v0.3"
PAIR_STATE_MAX_BYTES = 16 * 1024
PAIR_MAX_EXAMPLES_PER_TOPIC = 8
EXPECTED_MODEL_ALIAS = "jev-latest"
EXPECTED_MODEL_VERSION = "jev-1.13.0"
FUZZY_EDIT_THRESHOLD = 0.86
FUZZY_JACCARD_THRESHOLD = 0.67
FUZZY_NEIGHBOR_LIMIT = 8
PILOT_SIZE = 120
STABILITY_SIZE = 40
STABILITY_REPETITIONS = 3
QUESTION_SET_PATH = Path(__file__).resolve().parents[2] / "JEV_PAIR_QUESTIONS.json"
QUESTION_IDS = (
    "concept_relation",
    "merge_disposition",
    "merge_safe",
    "lexical_alias",
    "evidence_alignment",
    "relation_direction",
)
FUZZY_REASONS = {
    "normalized_edit_similarity_ge_0_86",
    "token_jaccard_ge_0_67",
    "one_token_containment",
}
ALL_RELATIONS = {
    "SAME_CONCEPT",
    "BROADER_NARROWER",
    "RELATED_DISTINCT",
    "UNRELATED",
    "INSUFFICIENT",
}

_SENTINELS = (
    ("anti cheat", "anticheat", "MERGE"),
    ("archaeology", "archeology", "MERGE"),
    ("advancement", "advancements", "MERGE"),
    ("animation", "animations", "MERGE"),
    ("armor", "armour", "MERGE"),
    ("name tag", "nametag", "MERGE"),
    ("one block", "oneblock", "MERGE"),
    ("village", "villager", "NOT_MERGE"),
    ("create ore", "create more", "NOT_MERGE"),
    ("announce", "announcer", "NOT_MERGE"),
    ("armor", "armor trim", "NOT_MERGE"),
    ("illager", "villager", "NOT_MERGE"),
)
def load_pair_questions() -> tuple[dict[str, Any], str]:
    contract = json.loads(QUESTION_SET_PATH.read_text(encoding="utf-8"))
    questions = contract.get("questions")
    if (
        contract.get("version") != PAIR_QUESTION_SET_VERSION
        or not isinstance(questions, dict)
        or set(questions) != set(QUESTION_IDS)
    ):
        raise ValueError("checked-in YEE-37 pair question contract is incomplete or has the wrong version")
    expected_types = {
        "concept_relation": "choice",
        "merge_disposition": "choice",
        "merge_safe": "noul",
        "lexical_alias": "noul",
        "evidence_alignment": "score",
        "relation_direction": "choice",
    }
    if any(questions[key].get("type") != value for key, value in expected_types.items()):
        raise ValueError("YEE-37 question types do not match the native typed contract")
    digest_input = (
        f"{PAIR_QUESTION_SET_VERSION}\n{canonical_json(questions)}\n"
        f"{PAIR_STATE_VERSION}\n{PAIR_POLICY_VERSION}\n{PAIR_OUTPUT_VERSION}"
    ).encode("utf-8")
    return questions, sha256_bytes(digest_input)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise InputIntegrityError(f"duplicate JSON key in accepted YEE-31 input: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise InputIntegrityError(f"non-JSON numeric constant in accepted YEE-31 input: {value}")


def load_accepted_advance_topics(
    advance_jsonl: str | Path,
    retrieval_db: str | Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, str]]:
    """Load the exact accepted ADVANCE set and its YEE-30 evidence read-only."""
    advance_path = Path(advance_jsonl).resolve()
    input_before = sha256_file(advance_path)
    if input_before.lower() != YEE31_ACCEPTED_INPUT_SHA256:
        raise InputIntegrityError("YEE-31 ADVANCE review-set SHA-256 does not match its accepted manifest")

    records: list[dict[str, Any]] = []
    with advance_path.open("r", encoding="utf-8-sig", newline="") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                raise InputIntegrityError(f"blank line in accepted YEE-31 JSONL at line {line_number}")
            try:
                record = json.loads(
                    line,
                    object_pairs_hook=_unique_object,
                    parse_constant=_reject_constant,
                )
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise InputIntegrityError(f"invalid accepted YEE-31 JSONL at line {line_number}") from exc
            records.append(record)
    if len(records) != EXPECTED_ADVANCE_TOPICS:
        raise InputIntegrityError(f"expected 1,807 YEE-31 ADVANCE topics, received {len(records)}")

    by_key: dict[str, dict[str, Any]] = {}
    for record in records:
        topic_key = record.get("topic_key")
        triage = record.get("triage")
        reference = record.get("evidence_pack_ref")
        if not isinstance(topic_key, str) or not topic_key or topic_key in by_key:
            raise InputIntegrityError("accepted YEE-31 ADVANCE set has an empty or duplicate topic_key")
        if not isinstance(triage, dict) or triage.get("policy_decision") != "ADVANCE":
            raise InputIntegrityError(f"non-ADVANCE topic found in accepted review set: {topic_key}")
        if not isinstance(reference, dict) or reference.get("topic_key") != topic_key:
            raise InputIntegrityError(f"YEE-31 evidence reference does not resolve for {topic_key}")
        if reference.get("retrieval_db_sha256", "").lower() != YEE30_ACCEPTED_DB_SHA256:
            raise InputIntegrityError(f"YEE-31 record points to a different YEE-30 DB for {topic_key}")
        evidence_references = triage.get("evidence_references")
        if not isinstance(evidence_references, dict):
            raise InputIntegrityError(f"YEE-31 evidence identity references are missing for {topic_key}")
        identities = evidence_references.get("canonical_identities")
        if not isinstance(identities, list) or any(not isinstance(item, str) for item in identities):
            raise InputIntegrityError(f"YEE-31 canonical identity references are invalid for {topic_key}")
        by_key[topic_key] = record

    y30_metadata, all_candidates = load_eligible_candidates(
        retrieval_db,
        YEE30_ACCEPTED_DB_SHA256,
    )
    candidate_by_key = {candidate["topic_key"]: candidate for candidate in all_candidates}
    if not set(by_key).issubset(candidate_by_key):
        missing = sorted(set(by_key) - set(candidate_by_key))
        raise InputIntegrityError(f"accepted YEE-31 topics do not resolve in YEE-30: {missing[:5]}")

    topics: list[dict[str, Any]] = []
    for topic_key in sorted(by_key):
        record = by_key[topic_key]
        triage = record["triage"]
        candidate = candidate_by_key[topic_key]
        reference_ids = set(triage["evidence_references"]["canonical_identities"])
        pack_ids = input_identity_set(candidate)
        if reference_ids != pack_ids:
            raise InputIntegrityError(f"YEE-31 and YEE-30 evidence identities differ for {topic_key}")
        candidate["accepted_y31"] = {
            "run_id": YEE31_RUN_ID,
            "policy_decision": "ADVANCE",
            "state_sha256": triage.get("state_sha256"),
            "question_set_sha256": triage.get("question_set_sha256"),
            "evidence_pack_ref": record["evidence_pack_ref"],
            "canonical_identity_references": sorted(reference_ids),
        }
        topics.append(candidate)

    input_after = sha256_file(advance_path)
    y30_after = sha256_file(retrieval_db)
    if input_after != input_before:
        raise InputIntegrityError("accepted YEE-31 ADVANCE review set changed while loading")
    if y30_after.lower() != YEE30_ACCEPTED_DB_SHA256:
        raise InputIntegrityError("read-only accepted YEE-30 input changed while loading")
    hashes = {
        "yee31_advance_review_set_sha256": input_before,
        "yee30_retrieval_db_sha256_before": YEE30_ACCEPTED_DB_SHA256,
        "yee30_retrieval_db_sha256_after_load": y30_after.lower(),
    }
    return y30_metadata, topics, hashes


def _pair_tokens(topic_key: str) -> tuple[str, ...]:
    return tuple(normalize_tokens(topic_key, frozenset()))


def _separator_insensitive(tokens: tuple[str, ...]) -> str:
    text = unicodedata.normalize("NFKC", " ".join(tokens)).casefold()
    return "".join(character for character in text if character.isalnum())


def _singular_token(token: str) -> str:
    lower = token.casefold()
    if lower in {"series", "species"}:
        return lower
    if len(lower) > 4 and lower.endswith("ies"):
        return lower[:-3] + "y"
    if (
        len(lower) > 4
        and lower.endswith("s")
        and not lower.endswith(("ss", "us", "is", "ous", "ews", "ics"))
    ):
        return lower[:-1]
    return lower


def _plural_signature(tokens: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(_singular_token(token) for token in tokens)


def _levenshtein(left: str, right: str) -> int:
    if left == right:
        return 0
    if len(left) < len(right):
        left, right = right, left
    previous = list(range(len(right) + 1))
    for row, left_character in enumerate(left, start=1):
        current = [row]
        for column, right_character in enumerate(right, start=1):
            current.append(
                min(
                    previous[column] + 1,
                    current[column - 1] + 1,
                    previous[column - 1] + (left_character != right_character),
                )
            )
        previous = current
    return previous[-1]


def _levenshtein_at_most(left: str, right: str, limit: int) -> int | None:
    """Return the exact distance only when it is within the supplied band."""
    if abs(len(left) - len(right)) > limit:
        return None
    if left == right:
        return 0
    if len(left) < len(right):
        left, right = right, left
    width = len(right)
    ceiling = limit + 1
    previous = [column if column <= limit else ceiling for column in range(width + 1)]
    for row, left_character in enumerate(left, start=1):
        current = [row if row <= limit else ceiling] + [ceiling] * width
        start = max(1, row - limit)
        stop = min(width, row + limit)
        for column in range(start, stop + 1):
            current[column] = min(
                previous[column] + 1,
                current[column - 1] + 1,
                previous[column - 1] + (left_character != right[column - 1]),
            )
        if min(current[start:stop + 1], default=ceiling) > limit:
            return None
        previous = current
    return previous[width] if previous[width] <= limit else None


def _normalized_edit_similarity(left: str, right: str) -> float:
    denominator = max(len(left), len(right))
    if denominator == 0:
        return 1.0
    return 1.0 - _levenshtein(left, right) / denominator


def _pair_key(left: str, right: str) -> tuple[str, str]:
    return (left, right) if left < right else (right, left)


def _pair_id(left: str, right: str) -> str:
    payload = f"{PAIR_GENERATION_VERSION}\n{left}\n{right}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _sentinel_index(topics: list[Mapping[str, Any]]) -> dict[str, str]:
    by_folded: dict[str, list[str]] = defaultdict(list)
    for topic in topics:
        by_folded[str(topic["topic_key"]).casefold()].append(str(topic["topic_key"]))
    result: dict[str, str] = {}
    for left, right, expected in _SENTINELS:
        left_keys = by_folded.get(left.casefold(), [])
        right_keys = by_folded.get(right.casefold(), [])
        if not left_keys or not right_keys:
            continue
        for left_key in left_keys:
            for right_key in right_keys:
                if left_key != right_key:
                    result["|".join(_pair_key(left_key, right_key))] = expected
    return result


def generate_candidate_pairs(
    topics: list[Mapping[str, Any]],
    input_hashes: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Generate and deterministically cap the complete local candidate-pair universe."""
    if len(topics) != EXPECTED_ADVANCE_TOPICS:
        raise InputIntegrityError(f"candidate generation requires exactly {EXPECTED_ADVANCE_TOPICS} topics")
    by_key = {str(topic["topic_key"]): topic for topic in topics}
    if len(by_key) != len(topics):
        raise InputIntegrityError("candidate generation received duplicate topic keys")
    ordered_keys = sorted(by_key)
    token_map = {key: _pair_tokens(key) for key in ordered_keys}
    normalized_map = {key: " ".join(token_map[key]) for key in ordered_keys}
    identity_sets = {
        key: set(by_key[key]["accepted_y31"]["canonical_identity_references"])
        for key in ordered_keys
    }
    reasons: dict[tuple[str, str], set[str]] = defaultdict(set)
    shared_identities: dict[tuple[str, str], set[str]] = defaultdict(set)
    edit_values: dict[tuple[str, str], float] = {}
    jaccard_values: dict[tuple[str, str], float] = {}

    def add(left: str, right: str, reason: str) -> None:
        if left != right:
            reasons[_pair_key(left, right)].add(reason)

    exact_groups: dict[str, list[str]] = defaultdict(list)
    plural_groups: dict[tuple[str, ...], list[str]] = defaultdict(list)
    identity_topics: dict[str, list[str]] = defaultdict(list)
    token_postings: dict[str, list[str]] = defaultdict(list)
    for key in ordered_keys:
        exact_groups[_separator_insensitive(token_map[key])].append(key)
        plural_groups[_plural_signature(token_map[key])].append(key)
        for identity in identity_sets[key]:
            identity_topics[identity].append(key)
        for token in set(token_map[key]):
            token_postings[token].append(key)

    for group in exact_groups.values():
        if len(group) > 1:
            for left, right in itertools.combinations(group, 2):
                if " ".join(token_map[left]) != " ".join(token_map[right]):
                    add(left, right, "separator_insensitive_alphanumeric_equality")

    for group in plural_groups.values():
        if len(group) > 1:
            for left, right in itertools.combinations(group, 2):
                if token_map[left] != token_map[right]:
                    add(left, right, "whole_phrase_singular_plural_equivalence")

    for identity, members in identity_topics.items():
        if len(members) > 1:
            for left, right in itertools.combinations(sorted(members), 2):
                shared_identities[(left, right)].add(identity)
    for pair, identities in shared_identities.items():
        if len(identities) >= 2:
            reasons[pair].add("retained_evidence_identity_overlap_ge_2")

    shared_token_counts: Counter[tuple[str, str]] = Counter()
    for members in token_postings.values():
        for left, right in itertools.combinations(members, 2):
            shared_token_counts[_pair_key(left, right)] += 1
    for pair, shared_count in shared_token_counts.items():
        left_tokens = set(token_map[pair[0]])
        right_tokens = set(token_map[pair[1]])
        union_count = len(left_tokens | right_tokens)
        jaccard = shared_count / union_count if union_count else 0.0
        if jaccard >= FUZZY_JACCARD_THRESHOLD:
            reasons[pair].add("token_jaccard_ge_0_67")
            jaccard_values[pair] = jaccard
        smaller, larger = (left_tokens, right_tokens) if len(left_tokens) < len(right_tokens) else (right_tokens, left_tokens)
        if len(larger) - len(smaller) == 1 and smaller < larger:
            left_text, right_text = normalized_map[pair[0]], normalized_map[pair[1]]
            similarity = _normalized_edit_similarity(left_text, right_text)
            if similarity >= 0.70 or len(shared_identities.get(pair, set())) > 0:
                reasons[pair].add("one_token_containment")

    for first_index, left in enumerate(ordered_keys):
        left_text = normalized_map[left]
        for right in ordered_keys[first_index + 1 :]:
            right_text = normalized_map[right]
            max_length = max(len(left_text), len(right_text))
            allowed_distance = math.floor((1.0 - FUZZY_EDIT_THRESHOLD) * max_length + 1e-12)
            distance = _levenshtein_at_most(left_text, right_text, allowed_distance)
            if distance is not None:
                similarity = 1.0 if max_length == 0 else 1.0 - distance / max_length
                pair = (left, right)
                reasons[pair].add("normalized_edit_similarity_ge_0_86")
                edit_values[pair] = similarity

    sentinel_expectations: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    sentinel_keys = _sentinel_index(topics)
    for encoded, expected in sentinel_keys.items():
        left, right = encoded.split("|", 1)
        pair = _pair_key(left, right)
        reasons[pair].add("sentinel_guard")
        label = next(
            f"{candidate_left} / {candidate_right}"
            for candidate_left, candidate_right, _ in _SENTINELS
            if {candidate_left.casefold(), candidate_right.casefold()} == {left.casefold(), right.casefold()}
        )
        sentinel_expectations[pair].append({"sentinel_id": label, "expected_policy": expected})

    pair_metrics: dict[tuple[str, str], dict[str, Any]] = {}
    for pair, pair_reasons in reasons.items():
        left, right = pair
        left_tokens = set(token_map[left])
        right_tokens = set(token_map[right])
        union_count = len(left_tokens | right_tokens)
        jaccard = jaccard_values.get(
            pair,
            len(left_tokens & right_tokens) / union_count if union_count else 0.0,
        )
        similarity = edit_values.get(
            pair,
            _normalized_edit_similarity(normalized_map[left], normalized_map[right]),
        )
        shared = sorted(shared_identities.get(pair, set()))
        fuzzy_score = max(similarity, jaccard)
        pair_metrics[pair] = {
            "normalized_edit_similarity": round(similarity, 6),
            "token_jaccard": round(jaccard, 6),
            "fuzzy_neighbor_score": round(fuzzy_score, 6),
            "evidence_overlap_count": len(shared),
            "shared_canonical_identities": shared,
            "blocking_reasons": sorted(pair_reasons),
        }
    top8_selected: dict[tuple[str, str], set[str]] = defaultdict(set)
    fuzzy_only_pairs = [
        (metric["fuzzy_neighbor_score"], pair)
        for pair, pair_reasons in reasons.items()
        if not (pair_reasons - FUZZY_REASONS)
        for metric in (pair_metrics[pair],)
    ]
    selected_degree: Counter[str] = Counter()
    for _score, pair in sorted(fuzzy_only_pairs, key=lambda item: (-item[0], item[1])):
        left, right = pair
        if (
            selected_degree[left] >= FUZZY_NEIGHBOR_LIMIT
            or selected_degree[right] >= FUZZY_NEIGHBOR_LIMIT
        ):
            continue
        top8_selected[pair].update((left, right))
        selected_degree[left] += 1
        selected_degree[right] += 1

    sentinel_by_pair = {
        pair: sorted(values, key=lambda value: value["sentinel_id"])
        for pair, values in sentinel_expectations.items()
    }
    output: list[dict[str, Any]] = []
    for pair in sorted(pair_metrics):
        metric = pair_metrics[pair]
        pair_reasons = reasons[pair]
        fuzzy_only = not bool(pair_reasons - FUZZY_REASONS)
        if fuzzy_only and not top8_selected.get(pair):
            continue
        left_topic, right_topic = by_key[pair[0]], by_key[pair[1]]
        left_sources = sorted(
            source for source, count in left_topic["candidate"]["source_presence"].items() if count
        )
        right_sources = sorted(
            source for source, count in right_topic["candidate"]["source_presence"].items() if count
        )
        row = {
            "pair_id": _pair_id(*pair),
            "left_topic_key": pair[0],
            "right_topic_key": pair[1],
            **metric,
            "fuzzy_only": fuzzy_only,
            "fuzzy_top8_selected_by": sorted(top8_selected.get(pair, set())),
            "sentinel_expectations": sentinel_by_pair.get(pair, []),
            "candidate_classes": [
                left_topic["candidate"]["candidate_class"],
                right_topic["candidate"]["candidate_class"],
            ],
            "source_presence": [left_sources, right_sources],
            "input_hashes": dict(sorted(input_hashes.items())),
            "pair_generation_version": PAIR_GENERATION_VERSION,
            "pair_normalization_version": PAIR_NORMALIZATION_VERSION,
        }
        output.append(row)
    return output


def _lexical_band(pair: Mapping[str, Any]) -> str:
    score = max(pair["normalized_edit_similarity"], pair["token_jaccard"])
    if score >= 0.92:
        return "high"
    if score >= 0.70:
        return "mid"
    return "low"


def _pair_strata(pair: Mapping[str, Any]) -> list[str]:
    classes = ">".join(pair["candidate_classes"])
    source_presence = ">".join("+".join(sources) or "none" for sources in pair["source_presence"])
    overlap = "yes" if pair["evidence_overlap_count"] > 0 else "no"
    dimensions = {
        "lexical_band": _lexical_band(pair),
        "evidence_overlap": overlap,
        "candidate_classes": classes,
        "source_presence": source_presence,
    }
    return [
        canonical_json({"blocking_reason": reason, **dimensions})
        for reason in pair["blocking_reasons"]
    ]


def select_stratified_pairs(
    pairs: list[Mapping[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    """Round-robin deterministic strata while forcing all available sentinels."""
    if len({str(pair["pair_id"]) for pair in pairs}) != len(pairs) or limit < 1 or len(pairs) < limit:
        raise ValueError("stratified pair selection requires unique pairs and enough candidates")
    forced = [
        pair for pair in pairs if pair.get("sentinel_expectations")
    ]
    forced.sort(key=lambda pair: (
        pair["sentinel_expectations"][0]["sentinel_id"],
        pair["left_topic_key"],
        pair["right_topic_key"],
    ))
    if len(forced) > limit:
        raise ValueError("sentinel count exceeds the requested pilot/stability size")
    selected: dict[str, dict[str, Any]] = {}
    selection_meta: dict[str, dict[str, Any]] = {}
    for pair in forced:
        pair_id = str(pair["pair_id"])
        selected[pair_id] = dict(pair)
        selection_meta[pair_id] = {"selection_mode": "forced_sentinel", "selected_stratum": None}

    buckets: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for pair in pairs:
        if pair.get("sentinel_expectations"):
            continue
        for stratum in _pair_strata(pair):
            buckets[stratum].append(pair)
    for stratum, members in buckets.items():
        members.sort(
            key=lambda pair: (
                sha256_bytes(f"yee-37-stratified-v0.1\n{stratum}\n{pair['pair_id']}".encode("utf-8")),
                str(pair["pair_id"]),
            )
        )
    ordered_strata = sorted(buckets)
    positions = {stratum: 0 for stratum in ordered_strata}
    while len(selected) < limit:
        progressed = False
        for stratum in ordered_strata:
            members = buckets[stratum]
            while positions[stratum] < len(members):
                pair = members[positions[stratum]]
                positions[stratum] += 1
                pair_id = str(pair["pair_id"])
                if pair_id in selected:
                    continue
                selected[pair_id] = dict(pair)
                selection_meta[pair_id] = {
                    "selection_mode": "stratified_round_robin",
                    "selected_stratum": stratum,
                }
                progressed = True
                break
            if len(selected) == limit:
                break
        if not progressed and len(selected) < limit:
            raise ValueError("stratified pair selector could not fill the requested sample")
    output = []
    for pair_id, pair in selected.items():
        pair["selection"] = selection_meta[pair_id]
        output.append(pair)
    return output


def pair_policy(answers: Mapping[str, Any]) -> str:
    relation = answers["concept_relation"]["choice"]
    disposition = answers["merge_disposition"]["choice"]
    if relation not in ALL_RELATIONS:
        raise TriageValidationError(f"unsupported pair relation: {relation}")
    if disposition not in {"MERGE", "KEEP_SEPARATE", "REVIEW"}:
        raise TriageValidationError(f"unsupported merge disposition: {disposition}")
    if disposition == "REVIEW" or relation == "INSUFFICIENT":
        return "REVIEW"
    if disposition == "MERGE":
        return "MERGE" if relation == "SAME_CONCEPT" else "REVIEW"
    if disposition == "KEEP_SEPARATE":
        return (
            "KEEP_SEPARATE"
            if relation in {"BROADER_NARROWER", "RELATED_DISTINCT", "UNRELATED"}
            else "REVIEW"
        )
    raise AssertionError("validated pair disposition was not handled")


_RESOLUTION_TOKEN_RE = re.compile(
    r"v?\d+(?:\.\d+)*|mk\d+|[^\W\d_]+|\d+",
    flags=re.UNICODE,
)
_VERSION_NUMBER_RE = re.compile(r"v?\d+(?:\.\d+)*", flags=re.IGNORECASE)
_ROMAN_VERSION_TOKENS = frozenset({"i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x"})
_RESOLUTION_APOSTROPHE_TRANSLATION = str.maketrans({
    "’": "'", "‘": "'", "‛": "'", "ʼ": "'", "ʻ": "'", "′": "'",
    "´": "'", "`": "'", "＇": "'",
})
RETRYABLE_JEV_HTTP_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504, 529})


def _resolution_tokens(value: str) -> tuple[str, ...]:
    text = unicodedata.normalize(
        "NFKC", str(value).translate(_RESOLUTION_APOSTROPHE_TRANSLATION)
    ).casefold()
    text = text.translate(_RESOLUTION_APOSTROPHE_TRANSLATION)
    text = re.sub(r"(?<=\w)'s(?=$|[\W_])", "", text, flags=re.UNICODE)
    return tuple(_RESOLUTION_TOKEN_RE.findall(text))


def _is_explicit_version_qualifier(tokens: tuple[str, ...]) -> bool:
    if len(tokens) == 1:
        token = tokens[0]
        return (
            token in _ROMAN_VERSION_TOKENS
            or bool(_VERSION_NUMBER_RE.fullmatch(token))
            or bool(re.fullmatch(r"mk\d+", token))
        )
    return len(tokens) == 2 and tokens[0] == "mk" and tokens[1].isdigit()


def has_version_qualifier_pair(left_topic_key: str, right_topic_key: str) -> bool:
    """Return true only when one normalized label adds one explicit trailing version."""
    left = _resolution_tokens(left_topic_key)
    right = _resolution_tokens(right_topic_key)
    shorter, longer = (left, right) if len(left) < len(right) else (right, left)
    if len(longer) <= len(shorter) or longer[:len(shorter)] != shorter:
        return False
    return _is_explicit_version_qualifier(longer[len(shorter):])


def apply_resolution_policy_v03(
    pair: Mapping[str, Any],
    normalized: Mapping[str, Any],
) -> dict[str, str]:
    """Add deterministic resolution provenance without rewriting Jev v0.2 evidence."""
    jev_decision = normalized["policy_decision"]
    if jev_decision not in {"MERGE", "KEEP_SEPARATE", "REVIEW"}:
        raise TriageValidationError(f"unsupported Jev v0.2 policy decision: {jev_decision}")
    if has_version_qualifier_pair(
        str(pair["left_topic_key"]), str(pair["right_topic_key"])
    ):
        decision, source = "KEEP_SEPARATE", "VERSION_QUALIFIER"
    elif "separator_insensitive_alphanumeric_equality" in pair.get("blocking_reasons", []):
        decision, source = "MERGE", "EXACT_FORM_ALIAS"
    else:
        decision, source = jev_decision, "JEV_V02"
    return {
        "jev_policy_decision_v02": jev_decision,
        "resolution_decision": decision,
        "resolution_source": source,
        "resolution_policy_version": RESOLUTION_POLICY_VERSION,
    }


def is_retryable_jev_transport_error(error: Exception) -> bool:
    """Strict retry allowlist for JevTransportError failures only."""
    if not isinstance(error, JevTransportError):
        return False
    if error.status_code is None:
        return error.connection_timeout_failure
    return error.status_code in RETRYABLE_JEV_HTTP_STATUS_CODES


def _is_terminal_max_tokens_error(error: Exception) -> bool:
    if not isinstance(error, JevTransportError) or error.status_code != 400:
        return False
    body = error.error_body or ""
    if not body:
        return False
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return "max_tokens_exceeded" in body.casefold()

    def contains_code(value: Any) -> bool:
        if isinstance(value, Mapping):
            return any(
                (key == "error_type" and item == "max_tokens_exceeded")
                or contains_code(item)
                for key, item in value.items()
            )
        if isinstance(value, list):
            return any(contains_code(item) for item in value)
        return False

    return contains_code(parsed)


def _role_priority(example: Mapping[str, Any]) -> int:
    raw_roles = example.get("selection_roles", [])
    roles = {
        str(role).strip().casefold().replace("-", "_")
        for role in raw_roles
    } if isinstance(raw_roles, (list, tuple, set)) else set()
    if "representative" in roles:
        return 0
    if "highest_demand_percentile" in roles:
        return 1
    if "freshest" in roles:
        return 2
    return 3


def _category_facets(example: Mapping[str, Any]) -> list[str]:
    value = example.get("category_facets_json", example.get("category_facets", []))
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    if not isinstance(value, list):
        return []
    return sorted({item for item in value if isinstance(item, str) and item})


def _member_examples(
    topic: Mapping[str, Any],
    pair_shared_identities: set[str],
) -> list[dict[str, Any]]:
    by_source: dict[str, list[tuple[tuple[int, int, str, str, str], dict[str, Any]]]] = {}
    for source, source_pack in sorted(topic["evidence_pack"]["sources"].items()):
        ranked = []
        for example in source_pack["examples"]:
            identity = str(example.get("canonical_identity", ""))
            if not identity:
                continue
            raw_roles = example.get("selection_roles", [])
            item = {
                "source": str(source),
                "canonical_identity": identity,
                "title": str(example.get("title") or ""),
                "summary": str(example.get("summary") or ""),
                "project_type_norm": example.get("project_type_norm"),
                "category_facets": _category_facets(example),
                "selection_roles": sorted(
                    {str(role) for role in raw_roles}
                ) if isinstance(raw_roles, (list, tuple, set)) else [],
            }
            rank = (
                0 if identity in pair_shared_identities else 1,
                _role_priority(example),
                identity,
                item["title"],
                item["summary"],
            )
            ranked.append((rank, item))
        by_source[str(source)] = sorted(ranked, key=lambda row: row[0])

    selected: list[dict[str, Any]] = []
    seen_by_source: dict[str, set[str]] = {source: set() for source in by_source}
    for shared_rank in (0, 1):
        for role_rank in range(4):
            source_rows = {
                source: [
                    item for rank, item in rows
                    if rank[0] == shared_rank and rank[1] == role_rank
                ]
                for source, rows in by_source.items()
            }
            positions = {source: 0 for source in source_rows}
            while len(selected) < PAIR_MAX_EXAMPLES_PER_TOPIC:
                progressed = False
                for source in sorted(source_rows):
                    rows = source_rows[source]
                    while positions[source] < len(rows):
                        item = rows[positions[source]]
                        positions[source] += 1
                        identity = item["canonical_identity"]
                        if identity in seen_by_source[source]:
                            continue
                        seen_by_source[source].add(identity)
                        selected.append(item)
                        progressed = True
                        break
                    if len(selected) == PAIR_MAX_EXAMPLES_PER_TOPIC:
                        break
                if not progressed:
                    break
    return selected


def build_pair_state(
    pair: Mapping[str, Any],
    topics_by_key: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    pair_shared_identities = {
        str(identity) for identity in pair.get("shared_canonical_identities", [])
    }

    def member(topic_key: str) -> dict[str, Any]:
        topic = topics_by_key[topic_key]
        pack = topic["evidence_pack"]
        candidate = topic["candidate"]
        source_presence = candidate.get("source_presence", {})
        if isinstance(source_presence, Mapping) and source_presence:
            source_names = sorted(str(source) for source in source_presence)
        elif isinstance(source_presence, list) and source_presence:
            source_names = sorted(str(source) for source in source_presence)
        else:
            source_names = sorted(str(source) for source in pack["sources"])
        provenance = topic["accepted_y31"]
        evidence_ref = provenance["evidence_pack_ref"]
        return {
            "topic_key": topic_key,
            "topic_display": candidate["topic_display"],
            "candidate_class": pack["candidate_class"],
            "source_presence": source_names,
            "examples": _member_examples(
                topic,
                pair_shared_identities,
            ),
            "provenance": {
                "yee31_run_id": provenance.get("run_id"),
                "yee31_state_sha256": provenance.get("state_sha256"),
                "yee31_question_set_sha256": provenance.get("question_set_sha256"),
                "yee30_retrieval_db_sha256": evidence_ref.get("retrieval_db_sha256"),
            },
        }

    return {
        "pair_state_version": PAIR_STATE_VERSION,
        "pair_id": pair["pair_id"],
        "left": member(pair["left_topic_key"]),
        "right": member(pair["right_topic_key"]),
    }


def _probability_deviation(answers: Mapping[str, Any]) -> dict[str, dict[str, float]]:
    deviations: dict[str, dict[str, float]] = {}
    for question_id, answer in answers.items():
        probabilities = answer.get("probabilities") if isinstance(answer, Mapping) else None
        if not isinstance(probabilities, Mapping):
            continue
        total = math.fsum(probabilities.values())
        deviation = round(total - 1.0, 12)
        if deviation:
            deviations[question_id] = {
                "sum": round(total, 12),
                "deviation_from_one": deviation,
            }
    return dict(sorted(deviations.items()))


def normalize_pair_response(
    envelope: Mapping[str, Any],
    pair: Mapping[str, Any],
    state_sha256: str,
    question_set_sha256: str,
    request_sha256: str,
    response_sha256: str,
    cache_key: str,
    replicate_id: str | None,
) -> dict[str, Any]:
    answers = envelope["answers"]
    return {
        "pair_id": pair["pair_id"],
        "left_topic_key": pair["left_topic_key"],
        "right_topic_key": pair["right_topic_key"],
        "returned_model": envelope["model"],
        "concept_relation": answers["concept_relation"],
        "merge_disposition": answers["merge_disposition"],
        "merge_safe": answers["merge_safe"],
        "lexical_alias": answers["lexical_alias"],
        "evidence_alignment": answers["evidence_alignment"],
        "relation_direction": answers["relation_direction"],
        "typed_answers": answers,
        "policy_decision": pair_policy(answers),
        "probability_sum_deviations": _probability_deviation(answers),
        "usage": envelope.get("usage", {}),
        "state_sha256": state_sha256,
        "question_set_sha256": question_set_sha256,
        "request_sha256": request_sha256,
        "response_sha256": response_sha256,
        "cache_key": cache_key,
        "replicate_id": replicate_id,
    }


class PairAdjudicationRunner:
    """Resumable SQLite persistence around the accepted YEE-31 SDK transport."""

    def __init__(
        self,
        database: str | Path,
        provider: Any,
        questions: Mapping[str, Any],
        question_set_sha256: str,
        run_metadata: Mapping[str, Any],
        max_attempts: int = 3,
        retry_delays: tuple[float, ...] = (0.5, 1.0),
    ) -> None:
        if provider.model_identifier != EXPECTED_MODEL_ALIAS or provider.model_version != EXPECTED_MODEL_VERSION:
            raise ValueError("YEE-37 requires requested jev-latest pinned to concrete jev-1.13.0")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.provider = provider
        self.questions = dict(questions)
        self.question_set_sha256 = question_set_sha256
        self.run_metadata = dict(run_metadata)
        self.max_attempts = max_attempts
        self.retry_delays = retry_delays
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self._initialize()

    def close(self) -> None:
        self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.connection.commit()
        self.connection.close()

    def __enter__(self) -> "PairAdjudicationRunner":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _initialize(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS run_metadata (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS candidate_pairs (
                pair_id TEXT PRIMARY KEY,
                left_topic_key TEXT NOT NULL,
                right_topic_key TEXT NOT NULL,
                pair_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pair_runs (
                cache_key TEXT PRIMARY KEY,
                pair_id TEXT NOT NULL,
                state_sha256 TEXT NOT NULL,
                question_set_sha256 TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                model_identifier TEXT NOT NULL,
                model_version TEXT NOT NULL,
                transport_id TEXT NOT NULL,
                inference_parameters_json TEXT NOT NULL,
                replicate_id TEXT,
                status TEXT NOT NULL,
                normalized_json TEXT,
                last_error TEXT,
                FOREIGN KEY (pair_id) REFERENCES candidate_pairs(pair_id)
            );
            CREATE TABLE IF NOT EXISTS pair_attempts (
                cache_key TEXT NOT NULL,
                attempt_no INTEGER NOT NULL,
                stage TEXT NOT NULL,
                request_body BLOB,
                request_sha256 TEXT,
                wire_request_captured INTEGER NOT NULL DEFAULT 0,
                raw_response BLOB,
                raw_response_sha256 TEXT,
                request_id TEXT,
                error TEXT,
                usage_json TEXT,
                parsed_at_ns INTEGER,
                PRIMARY KEY (cache_key,attempt_no),
                FOREIGN KEY (cache_key) REFERENCES pair_runs(cache_key)
            );
            """
        )
        desired = {key: canonical_json(value) for key, value in sorted(self.run_metadata.items())}
        existing = dict(self.connection.execute("SELECT key,value_json FROM run_metadata"))
        if existing and existing != desired:
            raise InputIntegrityError("YEE-37 run metadata does not match the existing SQLite cache identity")
        if not existing:
            self.connection.executemany(
                "INSERT INTO run_metadata(key,value_json) VALUES (?,?)",
                sorted(desired.items()),
            )
        self.connection.commit()

    def register_candidate_pairs(self, pairs: list[Mapping[str, Any]]) -> None:
        for pair in pairs:
            pair_json = canonical_json(pair)
            self.connection.execute(
                "INSERT INTO candidate_pairs(pair_id,left_topic_key,right_topic_key,pair_json) VALUES (?,?,?,?) "
                "ON CONFLICT(pair_id) DO NOTHING",
                (pair["pair_id"], pair["left_topic_key"], pair["right_topic_key"], pair_json),
            )
            saved = self.connection.execute(
                "SELECT pair_json FROM candidate_pairs WHERE pair_id=?", (pair["pair_id"],)
            ).fetchone()
            if saved["pair_json"] != pair_json:
                raise InputIntegrityError(f"candidate-pair evidence changed for {pair['pair_id']}")
        self.connection.commit()

    def _request(self, pair: Mapping[str, Any], state: Mapping[str, Any], replicate_id: str | None) -> tuple[InferenceRequest, str]:
        if state.get("pair_state_version") != PAIR_STATE_VERSION:
            raise InputIntegrityError("YEE-37 request state is not pair_state v0.2")
        state_json = canonical_json(state)
        if len(state_json.encode("utf-8")) > PAIR_STATE_MAX_BYTES:
            raise InputIntegrityError("YEE-37 pair_state v0.2 exceeds the 16 KiB request-state bound")
        body = canonical_json({
            "model": self.provider.model_identifier,
            "state": state,
            "questions": self.questions,
        }).encode("utf-8")
        state_sha = sha256_bytes(state_json.encode("utf-8"))
        request_sha = sha256_bytes(body)
        cache_identity = {
            "pair_id": pair["pair_id"],
            "state_sha256": state_sha,
            "question_set_sha256": self.question_set_sha256,
            "request_sha256": request_sha,
            "requested_model_alias": self.provider.model_identifier,
            "expected_model_version": self.provider.model_version,
            "transport_id": self.provider.transport_id,
            "transport_config": dict(self.provider.transport_config),
            "inference_parameters": self.run_metadata.get("inference_parameters", {}),
            "pair_generation_version": PAIR_GENERATION_VERSION,
            "pair_state_version": PAIR_STATE_VERSION,
            "pair_question_set_version": PAIR_QUESTION_SET_VERSION,
            "pair_policy_version": PAIR_POLICY_VERSION,
            "pair_output_version": PAIR_OUTPUT_VERSION,
            "replicate_id": replicate_id,
        }
        cache_key = sha256_bytes(canonical_json(cache_identity).encode("utf-8"))
        request = InferenceRequest(
            topic_key=str(pair["pair_id"]),
            model_identifier=self.provider.model_identifier,
            state=state,
            questions=self.questions,
            body=body,
            input_sha256=state_sha,
            question_set_sha256=self.question_set_sha256,
            request_sha256=request_sha,
            cache_key=cache_key,
            replicate_id=replicate_id,
        )
        return request, cache_key

    def _ensure_run(self, pair: Mapping[str, Any], request: InferenceRequest, cache_key: str, replicate_id: str | None) -> sqlite3.Row:
        self.connection.execute(
            "INSERT OR IGNORE INTO pair_runs "
            "(cache_key,pair_id,state_sha256,question_set_sha256,provider_id,model_identifier,model_version,"
            "transport_id,inference_parameters_json,replicate_id,status,normalized_json,last_error) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,'pending',NULL,NULL)",
            (
                cache_key,
                pair["pair_id"],
                request.input_sha256,
                self.question_set_sha256,
                self.provider.provider_id,
                self.provider.model_identifier,
                self.provider.model_version,
                self.provider.transport_id,
                canonical_json(self.run_metadata.get("inference_parameters", {})),
                replicate_id,
            ),
        )
        self.connection.commit()
        return self.connection.execute("SELECT * FROM pair_runs WHERE cache_key=?", (cache_key,)).fetchone()

    def _parse_and_save(
        self,
        pair: Mapping[str, Any],
        request: InferenceRequest,
        cache_key: str,
        attempt_no: int,
        raw_response: bytes,
        replicate_id: str | None,
    ) -> dict[str, Any] | None:
        try:
            envelope = parse_response(
                raw_response,
                self.questions,
                expected_model_version=self.provider.model_version,
            )
            normalized = normalize_pair_response(
                envelope,
                pair,
                request.input_sha256,
                self.question_set_sha256,
                request.request_sha256,
                sha256_bytes(raw_response),
                cache_key,
                replicate_id,
            )
        except ModelVersionMismatchError as exc:
            self.connection.execute(
                "UPDATE pair_attempts SET stage='model_version_mismatch',error=?,parsed_at_ns=? "
                "WHERE cache_key=? AND attempt_no=?",
                (str(exc), time.time_ns(), cache_key, attempt_no),
            )
            self.connection.execute(
                "UPDATE pair_runs SET status='failed',last_error=? WHERE cache_key=?",
                (str(exc), cache_key),
            )
            self.connection.commit()
            raise
        except TriageValidationError as exc:
            self.connection.execute(
                "UPDATE pair_attempts SET stage='parse_error',error=?,parsed_at_ns=? "
                "WHERE cache_key=? AND attempt_no=?",
                (str(exc), time.time_ns(), cache_key, attempt_no),
            )
            self.connection.execute(
                "UPDATE pair_runs SET status='failed',last_error=? WHERE cache_key=?",
                (str(exc), cache_key),
            )
            self.connection.commit()
            return None
        normalized_json = canonical_json(normalized)
        self.connection.execute(
            "UPDATE pair_attempts SET stage='completed',usage_json=?,parsed_at_ns=? "
            "WHERE cache_key=? AND attempt_no=?",
            (canonical_json(normalized.get("usage", {})), time.time_ns(), cache_key, attempt_no),
        )
        self.connection.execute(
            "UPDATE pair_runs SET status='completed',normalized_json=?,last_error=NULL WHERE cache_key=?",
            (normalized_json, cache_key),
        )
        self.connection.commit()
        return normalized

    def run_pair(
        self,
        pair: Mapping[str, Any],
        state: Mapping[str, Any],
        replicate_id: str | None = None,
    ) -> dict[str, Any]:
        request, cache_key = self._request(pair, state, replicate_id)
        run = self._ensure_run(pair, request, cache_key, replicate_id)
        if run["status"] == "completed":
            return {
                "pair_id": pair["pair_id"],
                "cache_key": cache_key,
                "replicate_id": replicate_id,
                "status": "completed",
                "normalized": json.loads(run["normalized_json"]),
            }
        if run["status"] == "failed" and run["last_error"] and "model version mismatch" in run["last_error"]:
            raise ModelVersionMismatchError(self.provider.model_version, "previously mismatched model")

        terminal = self.connection.execute(
            "SELECT attempt_no,stage FROM pair_attempts WHERE cache_key=? "
            "AND stage IN ('terminal_max_tokens_exceeded','terminal_nonretryable_error','parse_error') "
            "ORDER BY attempt_no LIMIT 1",
            (cache_key,),
        ).fetchone()
        if terminal is not None:
            return {
                "pair_id": pair["pair_id"],
                "cache_key": cache_key,
                "replicate_id": replicate_id,
                "status": "failed",
                "normalized": None,
                "error": run["last_error"] or "max_tokens_exceeded is terminal for this cache identity",
            }

        saved = self.connection.execute(
            "SELECT attempt_no,raw_response FROM pair_attempts "
            "WHERE cache_key=? AND stage='raw_saved' ORDER BY attempt_no DESC LIMIT 1",
            (cache_key,),
        ).fetchone()
        if saved is not None and saved["raw_response"] is not None:
            normalized = self._parse_and_save(
                pair, request, cache_key, saved["attempt_no"], saved["raw_response"], replicate_id
            )
            if normalized is not None:
                return {
                    "pair_id": pair["pair_id"],
                    "cache_key": cache_key,
                    "replicate_id": replicate_id,
                    "status": "completed",
                    "normalized": normalized,
                }
            failed = self.connection.execute(
                "SELECT last_error FROM pair_runs WHERE cache_key=?", (cache_key,)
            ).fetchone()
            return {
                "pair_id": pair["pair_id"],
                "cache_key": cache_key,
                "replicate_id": replicate_id,
                "status": "failed",
                "normalized": None,
                "error": failed["last_error"] if failed else "saved response failed contract validation",
            }

        attempts = self.connection.execute(
            "SELECT COALESCE(MAX(attempt_no),0) FROM pair_attempts WHERE cache_key=?",
            (cache_key,),
        ).fetchone()[0]
        while attempts < self.max_attempts:
            attempts += 1
            self.connection.execute(
                "INSERT INTO pair_attempts(cache_key,attempt_no,stage,request_body,request_sha256) "
                "VALUES (?,?,'calling',?,?)",
                (cache_key, attempts, request.body, request.request_sha256),
            )
            self.connection.execute(
                "UPDATE pair_runs SET status='running' WHERE cache_key=?", (cache_key,)
            )
            self.connection.commit()

            def persist_request(raw_body: bytes) -> None:
                try:
                    payload = json.loads(raw_body)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValueError("SDK request bytes are not valid JSON") from exc
                expected = json.loads(request.body)
                if payload != expected:
                    raise ValueError("SDK request bytes differ from the deterministic YEE-37 payload")
                self.connection.execute(
                    "UPDATE pair_attempts SET request_body=?,request_sha256=?,wire_request_captured=1 "
                    "WHERE cache_key=? AND attempt_no=?",
                    (raw_body, sha256_bytes(raw_body), cache_key, attempts),
                )
                self.connection.commit()

            def persist_response(status: int, raw_body: bytes, request_id: str | None) -> None:
                self.connection.execute(
                    "UPDATE pair_attempts SET stage='raw_saved',raw_response=?,raw_response_sha256=?,request_id=? "
                    "WHERE cache_key=? AND attempt_no=?",
                    (raw_body, sha256_bytes(raw_body), request_id, cache_key, attempts),
                )
                self.connection.commit()

            try:
                response = self.provider.complete(
                    request,
                    RawEvidenceCapture(persist_request, persist_response),
                )
            except Exception as exc:
                message = str(exc) if isinstance(exc, JevTransportError) else "provider call failed"
                terminal_max_tokens = _is_terminal_max_tokens_error(exc)
                retryable = is_retryable_jev_transport_error(exc)
                stage = (
                    "terminal_max_tokens_exceeded" if terminal_max_tokens
                    else "retryable_transport_error" if retryable
                    else "terminal_nonretryable_error"
                )
                self.connection.execute(
                    "UPDATE pair_attempts SET stage=?,error=? WHERE cache_key=? AND attempt_no=?",
                    (stage, message, cache_key, attempts),
                )
                self.connection.execute(
                    "UPDATE pair_runs SET status='failed',last_error=? WHERE cache_key=?",
                    (message, cache_key),
                )
                self.connection.commit()
                if not retryable:
                    break
                if attempts < self.max_attempts:
                    time.sleep(self.retry_delays[min(attempts - 1, len(self.retry_delays) - 1)] if self.retry_delays else 0)
                continue

            if not isinstance(response.raw_body, bytes):
                raise TypeError("Jev provider did not return the exact response bytes")
            self.connection.execute(
                "UPDATE pair_attempts SET stage='raw_saved',raw_response=?,raw_response_sha256=?,request_id=? "
                "WHERE cache_key=? AND attempt_no=?",
                (
                    response.raw_body,
                    sha256_bytes(response.raw_body),
                    response.request_id,
                    cache_key,
                    attempts,
                ),
            )
            self.connection.commit()
            normalized = self._parse_and_save(
                pair, request, cache_key, attempts, response.raw_body, replicate_id
            )
            if normalized is not None:
                return {
                    "pair_id": pair["pair_id"],
                    "cache_key": cache_key,
                    "replicate_id": replicate_id,
                    "status": "completed",
                    "normalized": normalized,
                }
            break

        final = self.connection.execute(
            "SELECT last_error FROM pair_runs WHERE cache_key=?", (cache_key,)
        ).fetchone()
        return {
            "pair_id": pair["pair_id"],
            "cache_key": cache_key,
            "replicate_id": replicate_id,
            "status": "failed",
            "normalized": None,
            "error": final["last_error"] if final else "pair failed",
        }

    def run_pairs(
        self,
        pairs: list[Mapping[str, Any]],
        topics_by_key: Mapping[str, Mapping[str, Any]],
        replicate_id: str | None = None,
    ) -> list[dict[str, Any]]:
        results = []
        for pair in pairs:
            state = build_pair_state(pair, topics_by_key)
            results.append(self.run_pair(pair, state, replicate_id))
        return results

    def export_results(self, replicate_ids: set[str | None]) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT r.*,p.left_topic_key,p.right_topic_key,p.pair_json FROM pair_runs r "
            "JOIN candidate_pairs p USING(pair_id) ORDER BY p.left_topic_key,p.right_topic_key,"
            "COALESCE(r.replicate_id,''),r.cache_key"
        ).fetchall()
        results = []
        for row in rows:
            if row["replicate_id"] not in replicate_ids:
                continue
            attempts = self.connection.execute(
                "SELECT COUNT(*) FROM pair_attempts WHERE cache_key=?", (row["cache_key"],)
            ).fetchone()[0]
            results.append({
                "pair_id": row["pair_id"],
                "left_topic_key": row["left_topic_key"],
                "right_topic_key": row["right_topic_key"],
                "replicate_id": row["replicate_id"],
                "status": row["status"],
                "attempt_count": attempts,
                "last_error": row["last_error"],
                "normalized": json.loads(row["normalized_json"]) if row["normalized_json"] else None,
            })
        return results

    def raw_attempts(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT a.*,r.pair_id,r.replicate_id,p.left_topic_key,p.right_topic_key "
            "FROM pair_attempts a JOIN pair_runs r USING(cache_key) "
            "JOIN candidate_pairs p USING(pair_id) "
            "ORDER BY p.left_topic_key,p.right_topic_key,COALESCE(r.replicate_id,''),a.attempt_no"
        ).fetchall()
        return [
            {
                "pair_id": row["pair_id"],
                "cache_key": row["cache_key"],
                "left_topic_key": row["left_topic_key"],
                "right_topic_key": row["right_topic_key"],
                "replicate_id": row["replicate_id"],
                "attempt_no": row["attempt_no"],
                "stage": row["stage"],
                "wire_request_captured": bool(row["wire_request_captured"]),
                "request_sha256": row["request_sha256"],
                "request_body_base64": base64.b64encode(row["request_body"]).decode("ascii") if row["request_body"] else None,
                "raw_response_sha256": row["raw_response_sha256"],
                "raw_response_base64": base64.b64encode(row["raw_response"]).decode("ascii") if row["raw_response"] else None,
                "request_id": row["request_id"],
                "error": row["error"],
            }
            for row in rows
        ]


def _value(answer: Mapping[str, Any]) -> Any:
    answer_type = answer["type"]
    value_key = {"choice": "choice", "score": "score", "noul": "noul"}[answer_type]
    return answer[value_key]


def build_stability_report(
    pairs: list[Mapping[str, Any]],
    outcomes: list[Mapping[str, Any]],
) -> dict[str, Any]:
    by_pair: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in outcomes:
        by_pair[str(row["pair_id"])].append(row)
    policy_transitions: Counter[str] = Counter()
    relation_transitions: Counter[str] = Counter()
    relation_agreement = 0
    policy_agreement = 0
    complete_count = 0
    per_question = {
        key: {"adjacent_changed_pairs": 0, "adjacent_compared_pairs": 0}
        for key in QUESTION_IDS
    }
    probabilities = {
        key: {"adjacent_changed_pairs": 0, "adjacent_compared_pairs": 0}
        for key in QUESTION_IDS
    }
    confidences = {
        key: {"adjacent_changed_pairs": 0, "adjacent_compared_pairs": 0}
        for key in QUESTION_IDS
    }
    stability_rows = []
    for pair in pairs:
        runs = sorted(
            by_pair.get(str(pair["pair_id"]), []),
            key=lambda row: (str(row.get("replicate_id")), str(row.get("cache_key", ""))),
        )
        if len(runs) != STABILITY_REPETITIONS or any(row["status"] != "completed" for row in runs):
            stability_rows.append({
                "pair_id": pair["pair_id"],
                "status": "incomplete",
                "replicates": runs,
            })
            continue
        complete_count += 1
        normalized = [row["normalized"] for row in runs]
        policies = [row["policy_decision"] for row in normalized]
        relations = [row["concept_relation"]["choice"] for row in normalized]
        policy_agreement += len(set(policies)) == 1
        relation_agreement += len(set(relations)) == 1
        for first, second in zip(normalized, normalized[1:]):
            policy_transitions[f"{first['policy_decision']}->{second['policy_decision']}"] += 1
            relation_transitions[
                f"{first['concept_relation']['choice']}->{second['concept_relation']['choice']}"
            ] += 1
        for question_id in QUESTION_IDS:
            answers = [row["typed_answers"][question_id] for row in normalized]
            for first, second in zip(answers, answers[1:]):
                per_question[question_id]["adjacent_compared_pairs"] += 1
                per_question[question_id]["adjacent_changed_pairs"] += _value(first) != _value(second)
                if "probabilities" in first and "probabilities" in second:
                    probabilities[question_id]["adjacent_compared_pairs"] += 1
                    probabilities[question_id]["adjacent_changed_pairs"] += (
                        canonical_json(first["probabilities"]) != canonical_json(second["probabilities"])
                    )
                if "confidence" in first and "confidence" in second:
                    confidences[question_id]["adjacent_compared_pairs"] += 1
                    confidences[question_id]["adjacent_changed_pairs"] += first["confidence"] != second["confidence"]
        stability_rows.append({
            "pair_id": pair["pair_id"],
            "status": "complete",
            "policy_decisions": policies,
            "relations": relations,
            "merge_dispositions": [
                row["merge_disposition"]["choice"] for row in normalized
            ],
            "merge_safe_values": [row["merge_safe"]["noul"] for row in normalized],
            "replicate_cache_keys": [row["cache_key"] for row in normalized],
        })
    sentinel_flips = []
    for pair in pairs:
        sentinels = pair.get("sentinel_expectations", [])
        if not sentinels:
            continue
        rows = by_pair.get(str(pair["pair_id"]), [])
        policies = [
            row["normalized"]["policy_decision"]
            for row in rows
            if row["status"] == "completed" and row["normalized"]
        ]
        if policies and ("MERGE" in policies) != all(policy == "MERGE" for policy in policies):
            sentinel_flips.append({
                "pair_id": pair["pair_id"],
                "sentinel_expectations": sentinels,
                "policy_decisions": policies,
            })
    total = len(pairs)
    rate = round(policy_agreement / total, 6) if total else 0.0
    return {
        "status": "COMPLETE" if complete_count == total else "INCOMPLETE",
        "candidate_count": total,
        "runs_per_candidate": STABILITY_REPETITIONS,
        "http_targets": total * STABILITY_REPETITIONS,
        "complete_candidate_count": complete_count,
        "authoritative_policy_exact_agreement_count": policy_agreement,
        "authoritative_policy_exact_agreement_rate": rate,
        "authoritative_policy_transition_patterns": dict(sorted(policy_transitions.items())),
        "relation_exact_agreement_count": relation_agreement,
        "relation_exact_agreement_rate": round(relation_agreement / total, 6) if total else 0.0,
        "relation_transition_patterns": dict(sorted(relation_transitions.items())),
        "sentinel_merge_flip_count": len(sentinel_flips),
        "sentinel_merge_flips": sentinel_flips,
        "per_question_value_deltas": {
            question_id: values
            for question_id, values in sorted(per_question.items())
        },
        "per_question_probability_deltas": {
            question_id: values
            for question_id, values in sorted(probabilities.items())
        },
        "per_question_confidence_deltas": {
            question_id: values
            for question_id, values in sorted(confidences.items())
        },
        "outcomes": stability_rows,
        "gates": {
            "exactly_40_pairs": total == STABILITY_SIZE,
            "three_replicates_per_pair": complete_count == total,
            "policy_exact_agreement_ge_95_percent": rate >= 0.95,
            "no_sentinel_merge_nonmerge_flip": not sentinel_flips,
        },
    }


def build_pilot_report(
    pairs: list[Mapping[str, Any]],
    outcomes: list[Mapping[str, Any]],
    manual_audit: list[Mapping[str, Any]],
) -> dict[str, Any]:
    by_id = {str(pair["pair_id"]): pair for pair in pairs}
    successful = [
        row for row in outcomes
        if row["status"] == "completed" and row.get("normalized") is not None
    ]
    decisions = Counter(row["normalized"]["policy_decision"] for row in successful)
    native_relations = Counter(row["normalized"]["concept_relation"]["choice"] for row in successful)
    native_dispositions = Counter(
        row["normalized"]["merge_disposition"]["choice"] for row in successful
    )
    returned_models = Counter(row["normalized"]["returned_model"] for row in successful)
    sentinel_results = []
    for pair_id, pair in sorted(by_id.items()):
        for expectation in pair.get("sentinel_expectations", []):
            row = next((item for item in outcomes if item["pair_id"] == pair_id), None)
            decision = row["normalized"]["policy_decision"] if row and row.get("normalized") else None
            accepted = (
                decision == "MERGE" if expectation["expected_policy"] == "MERGE"
                else decision in {"KEEP_SEPARATE", "REVIEW"}
            )
            sentinel_results.append({
                "pair_id": pair_id,
                **expectation,
                "observed_policy": decision,
                "pass": accepted,
            })
    observed_decisions = set(decisions)
    audited_decisions = {
        row["model_policy_decision"]
        for row in manual_audit
        if row.get("manual_policy_decision") in {"MERGE", "KEEP_SEPARATE", "REVIEW"}
    }
    manual_coverage = observed_decisions.issubset(audited_decisions)
    audited_rows = [
        row for row in manual_audit
        if row.get("manual_policy_decision") in {"MERGE", "KEEP_SEPARATE", "REVIEW"}
    ]
    manual_policy_agreements = sum(
        row["model_policy_decision"] == row["manual_policy_decision"]
        for row in audited_rows
    )
    manual_relation_agreements = sum(
        row.get("model_relation") == row.get("manual_relation")
        for row in audited_rows
    )
    gates = {
        "exact_pilot_size": len(pairs) == PILOT_SIZE,
        "at_least_119_valid_typed_responses": len(successful) >= 119,
        "concrete_model_identity_exact": all(
            row["normalized"]["returned_model"] == EXPECTED_MODEL_VERSION for row in successful
        ) and bool(successful),
        "all_available_merge_sentinels_resolve_to_merge": all(
            row["pass"] for row in sentinel_results if row["expected_policy"] == "MERGE"
        ),
        "all_available_not_merge_sentinels_resolve_to_separate_or_review": all(
            row["pass"] for row in sentinel_results if row["expected_policy"] == "NOT_MERGE"
        ),
        "deterministic_manual_audit_at_least_40_covers_decisions": (
            len(manual_audit) >= 40 and manual_coverage
        ),
    }
    return {
        "status": "PASS" if all(gates.values()) else "FAIL",
        "pair_count": len(pairs),
        "valid_typed_responses": len(successful),
        "failed_pairs": sum(row["status"] != "completed" for row in outcomes),
        "http_attempts": sum(int(row.get("attempt_count", 0)) for row in outcomes),
        "policy_decision_counts": dict(sorted(decisions.items())),
        "native_relation_counts": dict(sorted(native_relations.items())),
        "native_merge_disposition_counts": dict(sorted(native_dispositions.items())),
        "returned_model_distribution": dict(sorted(returned_models.items())),
        "sentinel_results": sentinel_results,
        "manual_audit_count": len(manual_audit),
        "manual_audit_completed_count": len(audited_rows),
        "manual_policy_agreement_count": manual_policy_agreements,
        "manual_relation_agreement_count": manual_relation_agreements,
        "manual_audit_disagreements": [
            {
                "pair_id": row["pair_id"],
                "model_relation": row.get("model_relation"),
                "manual_relation": row.get("manual_relation"),
                "model_policy_decision": row.get("model_policy_decision"),
                "manual_policy_decision": row.get("manual_policy_decision"),
            }
            for row in audited_rows
            if (
                row.get("model_relation") != row.get("manual_relation")
                or row.get("model_policy_decision") != row.get("manual_policy_decision")
            )
        ],
        "manual_audit_decision_coverage": {
            decision: decision in audited_decisions
            for decision in ("MERGE", "KEEP_SEPARATE", "REVIEW")
        },
        "gates": gates,
    }


def deterministic_manual_audit_sample(
    pairs: list[Mapping[str, Any]],
    outcomes: list[Mapping[str, Any]],
    limit: int = 40,
) -> list[dict[str, Any]]:
    by_pair = {str(pair["pair_id"]): pair for pair in pairs}
    completed = {
        str(row["pair_id"]): row
        for row in outcomes
        if row.get("status") == "completed" and row.get("normalized")
    }
    grouped: dict[str, list[str]] = defaultdict(list)
    for pair_id, outcome in completed.items():
        grouped[outcome["normalized"]["policy_decision"]].append(pair_id)
    for decision, ids in grouped.items():
        ids.sort(key=lambda pair_id: (
            sha256_bytes(f"yee-37-manual-audit-v0.1\n{decision}\n{pair_id}".encode("utf-8")),
            pair_id,
        ))
    selected: list[str] = []
    sentinel_ids = sorted(
        (
            pair_id
            for pair_id, pair in by_pair.items()
            if pair.get("sentinel_expectations") and pair_id in completed
        ),
        key=lambda pair_id: (
            by_pair[pair_id]["sentinel_expectations"][0]["sentinel_id"],
            pair_id,
        ),
    )
    selected.extend(sentinel_ids)
    for decision in ("MERGE", "KEEP_SEPARATE", "REVIEW"):
        next_pair = next(
            (pair_id for pair_id in grouped.get(decision, []) if pair_id not in selected),
            None,
        )
        if next_pair is not None:
            selected.append(next_pair)
    if len(selected) > limit:
        raise ValueError("sentinel and decision coverage exceed the manual-audit sample limit")
    remaining = sorted(
        set(completed) - set(selected),
        key=lambda pair_id: (
            sha256_bytes(f"yee-37-manual-audit-v0.1\n{pair_id}".encode("utf-8")),
            pair_id,
        ),
    )
    selected.extend(remaining[: max(0, limit - len(selected))])
    return [
        {
            "audit_ordinal": index,
            "pair_id": pair_id,
            "left_topic_key": by_pair[pair_id]["left_topic_key"],
            "right_topic_key": by_pair[pair_id]["right_topic_key"],
            "blocking_reasons": by_pair[pair_id]["blocking_reasons"],
            "normalized_edit_similarity": by_pair[pair_id]["normalized_edit_similarity"],
            "token_jaccard": by_pair[pair_id]["token_jaccard"],
            "evidence_overlap_count": by_pair[pair_id]["evidence_overlap_count"],
            "sentinel_expectations": by_pair[pair_id]["sentinel_expectations"],
            "model_relation": completed[pair_id]["normalized"]["concept_relation"]["choice"],
            "model_merge_disposition": completed[pair_id]["normalized"]["merge_disposition"]["choice"],
            "model_merge_safe": completed[pair_id]["normalized"]["merge_safe"]["noul"],
            "model_policy_decision": completed[pair_id]["normalized"]["policy_decision"],
            "manual_relation": None,
            "manual_policy_decision": None,
            "audit_note": None,
        }
        for index, pair_id in enumerate(selected, start=1)
    ]
