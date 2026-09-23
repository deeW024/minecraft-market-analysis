"""Auditable YEE-31 triage using Jev's native typed-decision contract."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import math
import os
import sqlite3
import threading
import time
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol
from urllib.parse import urlsplit

import httpx2
from typesafe_sdk import RetryPolicy, TypeSafeClient


QUESTION_SET_VERSION = "yee-31-native-questions-v0.2"
DERIVED_OUTPUT_VERSION = "yee-31-deterministic-output-v0.2"
DECISION_POLICY_VERSION = "yee-31-topic-quality-policy-v1"
EXPECTED_INPUT_COUNTS = {
    "resource_corpus": 169_007,
    "topic_source_facts": 8_095,
    "candidate_topics": 5_563,
    "eligible_evidence_packs": 3_036,
}
_ROOT = Path(__file__).resolve().parents[2]
QUESTION_SET_PATH = _ROOT / "JEV_TRIAGE_QUESTIONS.json"
QUESTION_IDS = (
    "triage_decision",
    "topic_quality",
    "semantic_alignment",
    "demand_signal_strength",
    "saturation_risk",
    "evidence_sufficiency",
    "paid_supply_pattern",
    "external_research_needed",
    "lexical_noise",
    "cross_market_mismatch",
    "strong_demand_evidence",
    "paid_supply_sparse",
    "stale_evidence",
    "ambiguity_requires_review",
)
DIRECT_BASE_URL = "https://api.typesafe.ai"
VERCEL_TYPESAFE_BASE_URL = "https://ai-gateway.vercel.sh/typesafe"
DIRECT_MODEL = "jev-latest"
VERCEL_MODEL = "typesafe-ai/jev"


class TriageValidationError(ValueError):
    """A native Jev response does not satisfy the typed contract."""


class InputIntegrityError(ValueError):
    """The accepted YEE-30 input does not match the expected identity/hash set."""


@dataclass(frozen=True)
class InferenceRequest:
    topic_key: str
    model_identifier: str
    state: Mapping[str, Any]
    questions: Mapping[str, Any]
    body: bytes
    input_sha256: str
    question_set_sha256: str
    request_sha256: str
    cache_key: str
    replicate_id: str | None = None


@dataclass(frozen=True)
class ProviderResponse:
    """Exact UTF-8 response bytes plus provider metadata, before parsing."""

    raw_body: bytes
    request_id: str | None = None


@dataclass(frozen=True)
class RawEvidenceCapture:
    """Persistence callbacks run at the HTTP transport boundary."""

    before_dispatch: Callable[[bytes], None]
    before_parse: Callable[[int, bytes, str | None], None]


@dataclass(frozen=True)
class _CapturedHTTPResponse:
    status_code: int
    raw_body: bytes
    request_id: str | None


class Reasoner(Protocol):
    provider_id: str
    model_identifier: str
    model_version: str
    transport_id: str
    transport_config: Mapping[str, Any]

    def complete(
        self, request: InferenceRequest, capture: RawEvidenceCapture | None = None
    ) -> ProviderResponse: ...


class JEVProviderAdapter:
    """Jev native-contract adapter around one runtime-configured transport."""

    provider_id = "JEV"

    def __init__(
        self,
        transport: Callable[[InferenceRequest], ProviderResponse],
        model_identifier: str,
        transport_id: str,
        transport_config: Mapping[str, Any],
    ) -> None:
        if not callable(transport) or not model_identifier or not transport_id:
            raise ValueError("a Jev transport, requested model, and transport identity are required")
        self._transport = transport
        self.model_identifier = model_identifier
        # The alias is for cache/run identity; every response records the concrete model.
        self.model_version = model_identifier
        self.transport_id = transport_id
        self.transport_config = dict(transport_config)

    def complete(
        self, request: InferenceRequest, capture: RawEvidenceCapture | None = None
    ) -> ProviderResponse:
        captured_call = getattr(self._transport, "complete_with_capture", None)
        response = captured_call(request, capture) if capture is not None and callable(captured_call) else self._transport(request)
        if not isinstance(response, ProviderResponse) or not isinstance(response.raw_body, bytes):
            raise TypeError("JEV transport must return ProviderResponse with exact raw bytes")
        return response

    def close(self) -> None:
        close = getattr(self._transport, "close", None)
        if callable(close):
            close()


class MissingCredentialError(RuntimeError):
    """The selected Jev transport has no runtime-injected credential."""


class JevTransportError(RuntimeError):
    """An HTTP transport failure with credential-scrubbed, bounded error evidence."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        request_id: str | None = None,
        error_body: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.request_id = request_id
        self.error_body = error_body
        details = message
        if status_code is not None:
            details = f"{message} (status={status_code})"
        if request_id:
            details += f"; request_id={request_id}"
        if error_body:
            details += f"; sanitized_error_body={error_body[:2048]}"
        super().__init__(details)


class _EvidenceHTTPTransport(httpx2.BaseTransport):
    """Capture SDK wire bytes before dispatch/parse without persisting headers."""

    def __init__(self, inner: httpx2.BaseTransport, api_key: str) -> None:
        self._inner = inner
        self._credential = api_key.encode("utf-8")
        self._capture: RawEvidenceCapture | None = None
        self.last_response: _CapturedHTTPResponse | None = None
        self.persistence_failure: str | None = None

    def begin_call(self, capture: RawEvidenceCapture | None) -> None:
        self._capture = capture
        self.last_response = None
        self.persistence_failure = None

    def end_call(self) -> None:
        self._capture = None

    def handle_request(self, request: httpx2.Request) -> httpx2.Response:
        request_body = bytes(request.content or b"")
        if self._credential and self._credential in request_body:
            self.persistence_failure = "request"
            raise RuntimeError("request body contains a runtime credential")
        if self._capture is not None:
            try:
                self._capture.before_dispatch(request_body)
            except Exception:
                self.persistence_failure = "request"
                raise RuntimeError("request evidence could not be persisted before dispatch") from None

        response = self._inner.handle_request(request)
        response.read()
        raw_response = bytes(response.content)
        if self._credential:
            raw_response = raw_response.replace(self._credential, b"[REDACTED]")
        request_id = response.headers.get("x-typesafe-request-id")
        captured = _CapturedHTTPResponse(response.status_code, raw_response, request_id)
        self.last_response = captured
        if self._capture is not None:
            try:
                self._capture.before_parse(captured.status_code, captured.raw_body, captured.request_id)
            except Exception:
                self.persistence_failure = "response"
                raise RuntimeError("response evidence could not be persisted before SDK parsing") from None
        return response

    def close(self) -> None:
        self._inner.close()


class TypeSafeSystemOneHTTPTransport:
    """Direct TypeSafe transport using the official Python SDK."""

    transport_id = "typesafe-systemone-http-v1"

    def __init__(
        self,
        api_key: str,
        base_url: str = DIRECT_BASE_URL,
        timeout: float = 30.0,
        http_transport: httpx2.BaseTransport | None = None,
    ) -> None:
        if not api_key:
            raise MissingCredentialError("TYPESAFE_API_KEY is required at runtime")
        self.base_url = _validate_base_url(base_url)
        self.timeout = _validate_timeout(timeout)
        self._evidence_transport = _EvidenceHTTPTransport(http_transport or httpx2.HTTPTransport(), api_key)
        self._http_client = httpx2.Client(transport=self._evidence_transport, timeout=self.timeout)
        self._sdk_client = TypeSafeClient(
            api_key=api_key,
            model=DIRECT_MODEL,
            base_url=self.base_url,
            timeout=self.timeout,
            retry=RetryPolicy(max_retries=0),
            http_client=self._http_client,
        )
        self._call_lock = threading.Lock()

    def __call__(
        self, request: InferenceRequest, capture: RawEvidenceCapture | None = None
    ) -> ProviderResponse:
        return self.complete_with_capture(request, capture)

    def complete_with_capture(
        self, request: InferenceRequest, capture: RawEvidenceCapture | None
    ) -> ProviderResponse:
        return _complete_with_sdk(self, request, capture)

    def close(self) -> None:
        self._sdk_client.close()


class VercelTypeSafeHTTPTransport:
    """Vercel AI Gateway's TypeSafe-compatible transport using the official SDK."""

    transport_id = "vercel-typesafe-systemone-http-v1"

    def __init__(
        self,
        api_key: str,
        base_url: str = VERCEL_TYPESAFE_BASE_URL,
        timeout: float = 30.0,
        http_transport: httpx2.BaseTransport | None = None,
    ) -> None:
        if not api_key:
            raise MissingCredentialError("AI_GATEWAY_API_KEY is required at runtime")
        self.base_url = _validate_base_url(base_url)
        self.timeout = _validate_timeout(timeout)
        self._evidence_transport = _EvidenceHTTPTransport(http_transport or httpx2.HTTPTransport(), api_key)
        self._http_client = httpx2.Client(transport=self._evidence_transport, timeout=self.timeout)
        self._sdk_client = TypeSafeClient(
            api_key=api_key,
            model=VERCEL_MODEL,
            base_url=self.base_url,
            timeout=self.timeout,
            retry=RetryPolicy(max_retries=0),
            http_client=self._http_client,
        )
        self._call_lock = threading.Lock()

    def __call__(
        self, request: InferenceRequest, capture: RawEvidenceCapture | None = None
    ) -> ProviderResponse:
        return self.complete_with_capture(request, capture)

    def complete_with_capture(
        self, request: InferenceRequest, capture: RawEvidenceCapture | None
    ) -> ProviderResponse:
        return _complete_with_sdk(self, request, capture)

    def close(self) -> None:
        self._sdk_client.close()


def _validate_base_url(value: str) -> str:
    parsed = urlsplit(value.strip())
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("Jev transport base URL must be an HTTPS origin/path without credentials or query data")
    return value.strip().rstrip("/")


def _validate_timeout(value: float) -> float:
    timeout = float(value)
    if not math.isfinite(timeout) or timeout <= 0 or timeout > 600:
        raise ValueError("Jev HTTP timeout must be between 0 and 600 seconds")
    return timeout


def _complete_with_sdk(
    transport: Any,
    request: InferenceRequest,
    capture: RawEvidenceCapture | None,
) -> ProviderResponse:
    with transport._call_lock:
        transport._evidence_transport.begin_call(capture)
        sdk_logger = logging.getLogger("typesafe_sdk")
        was_disabled = sdk_logger.disabled
        sdk_logger.disabled = True
        try:
            try:
                transport._sdk_client.system_one(
                    state=request.state,
                    questions=request.questions,
                    model=request.model_identifier,
                    retry=RetryPolicy(max_retries=0),
                    timeout=transport.timeout,
                )
            except Exception:
                captured = transport._evidence_transport.last_response
                if transport._evidence_transport.persistence_failure:
                    stage = transport._evidence_transport.persistence_failure
                    raise JevTransportError(f"{stage} evidence persistence failed") from None
                if captured is not None and 200 <= captured.status_code < 300:
                    return ProviderResponse(captured.raw_body, captured.request_id)
                if captured is not None:
                    error_body = captured.raw_body.decode("utf-8", errors="replace")
                    raise JevTransportError(
                        "Jev HTTP request failed",
                        status_code=captured.status_code,
                        request_id=captured.request_id,
                        error_body=error_body or None,
                    ) from None
                raise JevTransportError("Jev HTTP request failed (connection or timeout)") from None
            captured = transport._evidence_transport.last_response
            if captured is None:
                raise JevTransportError("Jev HTTP request completed without a captured response")
            if not 200 <= captured.status_code < 300:
                error_body = captured.raw_body.decode("utf-8", errors="replace")
                raise JevTransportError(
                    "Jev HTTP request failed",
                    status_code=captured.status_code,
                    request_id=captured.request_id,
                    error_body=error_body or None,
                )
            return ProviderResponse(captured.raw_body, captured.request_id)
        finally:
            sdk_logger.disabled = was_disabled
            transport._evidence_transport.end_call()


def jev_provider_from_env(
    environ: Mapping[str, str] | None = None,
    http_transport: httpx2.BaseTransport | None = None,
) -> JEVProviderAdapter:
    """Create a fail-closed direct or Vercel transport from runtime env only."""
    env = os.environ if environ is None else environ
    kind = env.get("JEV_TRANSPORT", "typesafe").strip().casefold()
    try:
        timeout = float(env.get("JEV_HTTP_TIMEOUT_SECONDS", "30"))
    except ValueError as exc:
        raise ValueError("JEV_HTTP_TIMEOUT_SECONDS must be numeric") from exc
    if kind == "typesafe":
        model = DIRECT_MODEL
        transport = TypeSafeSystemOneHTTPTransport(
            env.get("TYPESAFE_API_KEY", ""),
            env.get("TYPESAFE_BASE_URL", DIRECT_BASE_URL),
            timeout,
            http_transport,
        )
    elif kind in {"vercel", "vercel-typesafe"}:
        model = VERCEL_MODEL
        transport = VercelTypeSafeHTTPTransport(
            env.get("AI_GATEWAY_API_KEY", ""),
            env.get("VERCEL_TYPESAFE_BASE_URL", VERCEL_TYPESAFE_BASE_URL),
            timeout,
            http_transport,
        )
    else:
        raise ValueError("JEV_TRANSPORT must be 'typesafe' or 'vercel-typesafe'")
    config = {"base_url": transport.base_url, "timeout_seconds": transport.timeout}
    return JEVProviderAdapter(transport, model, transport.transport_id, config)


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_contract() -> tuple[dict[str, Any], str]:
    contract = json.loads(QUESTION_SET_PATH.read_text(encoding="utf-8"))
    if contract.get("version") != QUESTION_SET_VERSION or set(contract.get("questions", {})) != set(QUESTION_IDS):
        raise ValueError("checked-in Jev question contract is incomplete or has the wrong version")
    questions = contract["questions"]
    digest_input = f"{QUESTION_SET_VERSION}\n{canonical_json(questions)}\n{DERIVED_OUTPUT_VERSION}".encode("utf-8")
    return questions, sha256_bytes(digest_input)


def _candidate_json(candidate: Mapping[str, Any]) -> str:
    topic_key = candidate.get("topic_key")
    if not isinstance(topic_key, str) or not topic_key:
        raise ValueError("candidate requires a non-empty topic_key")
    if not isinstance(candidate.get("evidence_pack"), Mapping):
        raise ValueError("candidate requires the accepted YEE-30 evidence_pack")
    return canonical_json(candidate)


def input_identity_set(candidate: Mapping[str, Any]) -> set[str]:
    identities: set[str] = set()
    sources = candidate.get("evidence_pack", {}).get("sources", {})
    if isinstance(sources, Mapping):
        for source in sources.values():
            if not isinstance(source, Mapping):
                continue
            for example in source.get("examples", []):
                if isinstance(example, Mapping) and isinstance(example.get("canonical_identity"), str):
                    identities.add(example["canonical_identity"])
    return identities


def _probability_map(value: Any, keys: set[str], name: str) -> None:
    if not isinstance(value, dict) or set(value) != keys:
        raise TriageValidationError(f"{name} probability keys do not match the declared answer space")
    numbers = list(value.values())
    if any(type(number) not in (int, float) or not math.isfinite(number) or not 0 <= number <= 1 for number in numbers):
        raise TriageValidationError(f"{name} probabilities must be finite values from 0 to 1")


def _probability_sum_deviations(answers: Mapping[str, Any]) -> dict[str, dict[str, float]]:
    deviations = {}
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


def validate_response(value: Any, expected_questions: Mapping[str, Any] | None = None) -> dict[str, Any]:
    questions = expected_questions or load_contract()[0]
    if not isinstance(value, dict) or not {"model", "answers"}.issubset(value):
        raise TriageValidationError("Jev response must contain model and answers")
    if set(value) - {"model", "answers", "usage", "provider_metadata"}:
        raise TriageValidationError("Jev response contains unknown top-level fields")
    if not isinstance(value["model"], str) or not value["model"].strip():
        raise TriageValidationError("Jev response model identity is missing")
    answers = value["answers"]
    if not isinstance(answers, dict) or set(answers) != set(questions):
        raise TriageValidationError("Jev answer IDs do not exactly match the declared question set")
    for question_id, question in questions.items():
        answer = answers[question_id]
        kind = question["type"]
        if not isinstance(answer, dict) or answer.get("type") != kind:
            raise TriageValidationError(f"{question_id} answer type does not match {kind}")
        if kind == "choice":
            labels = set(question["criteria"])
            if (
                set(answer) != {"type", "choice", "confidence", "probabilities"}
                or not isinstance(answer["choice"], str)
                or answer["choice"] not in labels
            ):
                raise TriageValidationError(f"{question_id} has an out-of-contract choice answer")
            if type(answer["confidence"]) not in (int, float) or not math.isfinite(answer["confidence"]) or not 0 <= answer["confidence"] <= 1:
                raise TriageValidationError(f"{question_id} confidence must be from 0 to 1")
            _probability_map(answer["probabilities"], labels, question_id)
        elif kind == "score":
            level_keys = {str(index) for index in range(len(question["criteria"]))}
            if set(answer) != {"type", "score", "confidence", "legend", "probabilities"}:
                raise TriageValidationError(f"{question_id} has an invalid score answer shape")
            score = answer["score"]
            confidence = answer["confidence"]
            if type(score) not in (int, float) or not math.isfinite(score) or not 0 <= score <= len(level_keys) - 1:
                raise TriageValidationError(f"{question_id} score is outside its ordered rubric")
            if type(confidence) not in (int, float) or not math.isfinite(confidence) or not 0 <= confidence <= 1:
                raise TriageValidationError(f"{question_id} confidence must be from 0 to 1")
            expected_legend = {str(index): label for index, label in enumerate(question["criteria"])}
            if not isinstance(answer["legend"], dict) or answer["legend"] != expected_legend:
                raise TriageValidationError(f"{question_id} legend does not match its ordered rubric")
            _probability_map(answer["probabilities"], level_keys, question_id)
        elif kind == "noul":
            if set(answer) != {"type", "noul"} or type(answer["noul"]) not in (int, float):
                raise TriageValidationError(f"{question_id} has an invalid Noul answer shape")
            if not math.isfinite(answer["noul"]) or not 0 <= answer["noul"] <= 1:
                raise TriageValidationError(f"{question_id} Noul probability must be from 0 to 1")
        else:
            raise TriageValidationError(f"unsupported question type for {question_id}")
    if "usage" in value:
        usage = value["usage"]
        if not isinstance(usage, dict) or set(usage) - {"input_tokens", "output_tokens"}:
            raise TriageValidationError("Jev usage must contain only documented token counts")
        if any(type(count) is not int or count < 0 for count in usage.values()):
            raise TriageValidationError("Jev usage token counts must be non-negative integers")
    if "provider_metadata" in value and not isinstance(value["provider_metadata"], dict):
        raise TriageValidationError("provider_metadata must be an object when present")
    return value


def _reject_constant(value: str) -> None:
    raise TriageValidationError(f"non-JSON numeric constant is forbidden: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise TriageValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def parse_response(raw_body: bytes, expected_questions: Mapping[str, Any] | None = None) -> dict[str, Any]:
    try:
        decoded = raw_body.decode("utf-8", errors="strict")
        value = json.loads(decoded, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TriageValidationError(f"response is not strict UTF-8 JSON: {exc}") from exc
    return validate_response(value, expected_questions)


def load_eligible_candidates(
    retrieval_db: str | Path,
    expected_sha256: str,
    expected_counts: Mapping[str, int] = EXPECTED_INPUT_COUNTS,
) -> tuple[dict[str, str], list[dict[str, Any]]]:
    """Validate and load eligible YEE-30 packs through a read-only SQLite URI."""
    path = Path(retrieval_db).resolve()
    before = sha256_file(path)
    if before.lower() != expected_sha256.lower():
        raise InputIntegrityError("YEE-30 retrieval DB SHA-256 does not match the accepted manifest")
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    try:
        metadata = dict(connection.execute("SELECT key,value FROM build_metadata"))
        actual_counts = {
            "resource_corpus": connection.execute("SELECT COUNT(*) FROM resource_corpus").fetchone()[0],
            "topic_source_facts": connection.execute("SELECT COUNT(*) FROM topic_source_facts").fetchone()[0],
            "candidate_topics": connection.execute("SELECT COUNT(*) FROM candidate_topics").fetchone()[0],
            "eligible_evidence_packs": connection.execute(
                "SELECT COUNT(*) FROM evidence_packs WHERE research_eligible=1"
            ).fetchone()[0],
        }
        if metadata.get("work_order") != "YEE-30" or metadata.get("topic_schema_version") != "yee-30-topic-layer-v0.1":
            raise InputIntegrityError("retrieval DB is not the accepted YEE-30 schema")
        if actual_counts != dict(expected_counts):
            raise InputIntegrityError(f"YEE-30 identity/pack counts mismatch: {actual_counts}")
        candidate_rows = connection.execute(
            "SELECT * FROM candidate_topics WHERE research_eligible=1 ORDER BY topic_key"
        ).fetchall()
        packs = connection.execute(
            "SELECT * FROM evidence_packs WHERE research_eligible=1 ORDER BY topic_key"
        ).fetchall()
        if [row["topic_key"] for row in candidate_rows] != [row["topic_key"] for row in packs]:
            raise InputIntegrityError("eligible candidate keys do not reconcile to evidence packs")

        candidates: list[dict[str, Any]] = []
        for candidate_row, pack_row in zip(candidate_rows, packs, strict=True):
            candidate = dict(candidate_row)
            topic_key = candidate["topic_key"]
            facts = [dict(row) for row in connection.execute(
                "SELECT * FROM topic_source_facts WHERE topic_key=? ORDER BY source", (topic_key,)
            )]
            candidates.append({
                "topic_key": topic_key,
                "candidate": {
                    "topic_display": candidate["topic_display"],
                    "candidate_class": candidate["candidate_class"],
                    "research_eligible": bool(candidate["research_eligible"]),
                    "source_presence": json.loads(candidate["source_presence_json"]),
                    "source_fact_keys": json.loads(candidate["source_fact_keys_json"]),
                    "demand_gate_sources": json.loads(candidate["demand_gate_sources_json"]),
                    "eligibility_reasons": json.loads(candidate["eligibility_reasons_json"]),
                },
                "topic_source_facts": facts,
                "evidence_pack": json.loads(pack_row["pack_json"]),
            })
    finally:
        connection.close()
    after = sha256_file(path)
    if after != before:
        raise InputIntegrityError("read-only YEE-30 input changed while loading")
    return metadata, candidates


def select_stratified_pilot(candidates: list[Mapping[str, Any]], limit: int = 120) -> list[Mapping[str, Any]]:
    """Select a deterministic round-robin pilot across class, market pattern, support, and demand bands."""
    if limit < 1 or len(candidates) < limit:
        raise ValueError("pilot requires at least `limit` eligible candidate packs")
    by_key = {str(candidate.get("topic_key", "")): candidate for candidate in candidates}
    if "" in by_key or len(by_key) != len(candidates):
        raise ValueError("pilot candidates require unique non-empty topic_key values")

    support: dict[str, float] = {}
    demand: dict[str, float] = {}
    for key, candidate in by_key.items():
        facts = candidate.get("topic_source_facts", [])
        support[key] = sum(float(fact.get("resource_count") or 0) for fact in facts)
        # This is a source-local percentile share, not a cross-source raw-count sum.
        demand[key] = max((float(fact.get("demand_percentile_ge90_share") or 0) for fact in facts), default=0.0)

    def bands(values: Mapping[str, float]) -> dict[str, str]:
        ordered = sorted(values, key=lambda key: (values[key], key))
        split = len(ordered) // 2
        return {key: ("low" if index < split else "high") for index, key in enumerate(ordered)}

    support_band = bands(support)
    demand_band = bands(demand)
    strata: dict[tuple[str, str, str, str], list[str]] = {}
    for key, candidate in by_key.items():
        row = candidate.get("candidate", {})
        candidate_class = row.get("candidate_class")
        if candidate_class not in {"overlap", "free_demand_only"}:
            raise ValueError(f"unsupported candidate_class for pilot: {candidate_class}")
        source_presence = row.get("source_presence", {})
        if not isinstance(source_presence, Mapping):
            raise ValueError("source_presence must be an object")
        pattern = "+".join(sorted(str(source) for source, count in source_presence.items() if count)) or "none"
        bucket = (candidate_class, pattern, support_band[key], demand_band[key])
        strata.setdefault(bucket, []).append(key)
    for keys in strata.values():
        keys.sort()

    selected: list[str] = []
    offsets = {bucket: 0 for bucket in strata}
    buckets = sorted(strata)
    while len(selected) < limit:
        progressed = False
        for bucket in buckets:
            offset = offsets[bucket]
            if offset < len(strata[bucket]):
                selected.append(strata[bucket][offset])
                offsets[bucket] += 1
                progressed = True
                if len(selected) == limit:
                    break
        if not progressed:
            raise ValueError("pilot selection exhausted eligible candidates")
    return [by_key[key] for key in sorted(selected)]


def select_stability_subset(pilot: list[Mapping[str, Any]], limit: int = 60) -> list[Mapping[str, Any]]:
    """Choose a fixed subset by a stable hash of topic_key, independent of row order."""
    if limit < 1 or len(pilot) < limit:
        raise ValueError("stability subset requires at least `limit` pilot candidates")
    ordered = sorted(
        pilot,
        key=lambda candidate: (
            sha256_bytes(str(candidate["topic_key"]).encode("utf-8")),
            str(candidate["topic_key"]),
        ),
    )
    return sorted(ordered[:limit], key=lambda candidate: str(candidate["topic_key"]))


def evaluate_pilot_gates(
    results: list[Mapping[str, Any]],
    audited_topic_keys: list[str],
    unsupported_external_calls: int,
    expected_count: int = 120,
) -> dict[str, Any]:
    """Evaluate the Jev-native gates; manual audit membership is supplied explicitly."""
    completed = [
        row for row in results
        if row.get("status") == "completed" and isinstance(row.get("normalized"), Mapping)
    ]
    valid_share = len(completed) / len(results) if results else 0.0
    decisions = {row["normalized"].get("policy_decision") for row in completed}
    audited = set(audited_topic_keys)
    completed_keys = {str(row.get("topic_key")) for row in completed}
    expected_question_ids = set(QUESTION_IDS)
    gates = {
        "exact_pilot_size": len(results) == expected_count,
        "native_answer_contract_valid_rate_ge_99_percent": valid_share >= 0.99,
        "concrete_model_identity_present": all(
            isinstance(row["normalized"].get("model_identity", {}).get("returned_model"), str)
            and row["normalized"]["model_identity"]["returned_model"]
            for row in completed
        ),
        "raw_request_and_response_persisted": all(
            row["normalized"].get("raw_evidence", {}).get("request_sha256")
            and row["normalized"].get("raw_evidence", {}).get("response_sha256")
            for row in completed
        ),
        "no_unknown_question_ids": all(
            set(row["normalized"].get("typed_answers", {})) == expected_question_ids
            for row in completed
        ),
        "zero_out_of_contract_labels": not any(
            "out-of-contract choice" in str(row.get("error", "")) for row in results
        ),
        "zero_unsupported_external_calls": unsupported_external_calls == 0,
        "manual_audit_at_least_30_and_covers_decisions": (
            len(audited) >= 30
            and audited.issubset(completed_keys)
            and {row["normalized"].get("policy_decision") for row in completed if row.get("topic_key") in audited} >= decisions
        ),
    }
    return {
        "status": "PASS" if all(gates.values()) else "FAIL",
        "candidate_count": len(results),
        "native_contract_valid_count": len(completed),
        "native_contract_valid_rate": round(valid_share, 6),
        "decision_counts": {
            decision: sum(row["normalized"].get("policy_decision") == decision for row in completed)
            for decision in ("ADVANCE", "HOLD", "REJECT")
        },
        "native_triage_decision_counts": {
            decision: sum(row["normalized"].get("triage_decision", {}).get("choice") == decision for row in completed)
            for decision in ("ADVANCE", "HOLD", "REJECT")
        },
        "manual_audit_count": len(audited),
        "unsupported_external_calls": unsupported_external_calls,
        "gates": gates,
    }


def require_pilot_pass(report: Mapping[str, Any]) -> None:
    required_gates = {
        "exact_pilot_size",
        "native_answer_contract_valid_rate_ge_99_percent",
        "concrete_model_identity_present",
        "raw_request_and_response_persisted",
        "no_unknown_question_ids",
        "zero_out_of_contract_labels",
        "zero_unsupported_external_calls",
        "manual_audit_at_least_30_and_covers_decisions",
    }
    gates = report.get("gates")
    if (
        report.get("status") != "PASS"
        or report.get("candidate_count") != 120
        or int(report.get("native_contract_valid_count", 0)) < 119
        or float(report.get("native_contract_valid_rate", 0)) < 0.99
        or int(report.get("manual_audit_count", 0)) < 30
        or not isinstance(gates, Mapping)
        or set(gates) != required_gates
        or not all(gates.values())
    ):
        raise RuntimeError("full triage is blocked until every YEE-31 pilot acceptance gate passes")


def build_run_metadata(
    accepted_input_sha256: str,
    topic_schema_version: str,
    normalization_version: str,
    analysis_as_of: str,
    code_version: str,
    provider: Reasoner,
    inference_parameters: Mapping[str, Any],
) -> dict[str, Any]:
    if inference_parameters:
        raise ValueError("TypeSafe System One has no inference-parameter fields; keep this mapping empty")
    _, question_set_sha256 = load_contract()
    return {
        "accepted_input_sha256": accepted_input_sha256,
        "topic_schema_version": topic_schema_version,
        "normalization_version": normalization_version,
        "analysis_as_of": analysis_as_of,
        "code_version": code_version,
        "question_set_version": QUESTION_SET_VERSION,
        "derived_output_version": DERIVED_OUTPUT_VERSION,
        "question_set_sha256": question_set_sha256,
        "provider_id": provider.provider_id,
        "model_identifier": provider.model_identifier,
        "model_version": provider.model_version,
        "transport_id": provider.transport_id,
        "transport_config": dict(provider.transport_config),
        "inference_parameters": dict(inference_parameters),
    }


def _question_set_hash() -> str:
    return load_contract()[1]


def _score_label(answer: Mapping[str, Any]) -> str:
    index = min(range(len(answer["legend"])), key=lambda item: abs(float(answer["score"]) - item))
    return str(answer["legend"][str(index)])


def _fact_summary(candidate: Mapping[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    summaries: list[str] = []
    references: list[dict[str, Any]] = []
    for fact in sorted(candidate.get("topic_source_facts", []), key=lambda row: str(row.get("source", ""))):
        source = str(fact.get("source", "unknown"))
        fields = ["resource_count", "demand_percentile_ge90_share", "freshness_le90_share"]
        references.append({"source": source, "fields": fields})
        count = fact.get("resource_count")
        demand_share = fact.get("demand_percentile_ge90_share")
        freshness_share = fact.get("freshness_le90_share")
        demand_text = "unavailable" if demand_share is None else f"{float(demand_share) * 100:.1f}%"
        freshness_text = "unavailable" if freshness_share is None else f"{float(freshness_share) * 100:.1f}%"
        summaries.append(
            f"{source}: {count} examples; {demand_text} at/above its source-local demand p90; "
            f"{freshness_text} fresh within 90 days"
        )
    return "; ".join(summaries) if summaries else "No source facts retained", references


def _research_question_templates(answers: Mapping[str, Any], concept_label: str) -> list[str]:
    templates = {
        "lexical_noise": f"What share of results for {concept_label} are unrelated to this topic?",
        "cross_market_mismatch": f"Do marketplace entries for {concept_label} refer to the same underlying product or problem?",
        "strong_demand_evidence": f"Which user tasks explain the source-relative demand evidence for {concept_label}?",
        "paid_supply_sparse": f"Why is paid supply sparse for {concept_label} in the retained YEE-30 evidence?",
        "stale_evidence": f"Is the observed evidence for {concept_label} still current?",
        "ambiguity_requires_review": f"Which distinct concepts should be separated within {concept_label}?",
    }
    if answers["external_research_needed"]["noul"] < 0.5:
        return []
    return [
        templates[key]
        for key in templates
        if answers[key]["noul"] >= 0.5
    ] or [f"Is there an externally verifiable unmet need related to {concept_label}?"]


def normalize_native_response(
    envelope: Mapping[str, Any],
    candidate: Mapping[str, Any],
    provider: Reasoner,
    state_sha256: str,
    question_set_sha256: str,
    request_sha256: str,
    response_sha256: str,
) -> dict[str, Any]:
    """Derive all prose/labels locally from typed answers and accepted YEE-30 facts."""
    answers = envelope["answers"]
    candidate_row = candidate["candidate"]
    concept_label = str(candidate_row.get("topic_display") or candidate["topic_key"])
    source_presence = candidate_row.get("source_presence", {})
    source_parts = [f"{source}={source_presence[source]}" for source in sorted(source_presence)]
    candidate_class = str(candidate_row.get("candidate_class", "unknown"))
    market_pattern = f"{candidate_class}; " + (", ".join(source_parts) if source_parts else "no source presence")
    fact_summary, fact_references = _fact_summary(candidate)
    decision = answers["triage_decision"]
    quality = answers["topic_quality"]["choice"]
    policy_decision = {
        "coherent": "ADVANCE",
        "ambiguous": "HOLD",
        "generic_or_noise": "REJECT",
    }[quality]
    scores = {
        question_id: answers[question_id]
        for question_id in ("semantic_alignment", "demand_signal_strength", "saturation_risk", "evidence_sufficiency")
    }
    paid_supply = answers["paid_supply_pattern"]["choice"]
    rationale = (
        f"Jev's native triage_decision diagnostic is {decision['choice']} "
        f"(confidence {float(decision['confidence']):.3f}; probabilities {canonical_json(decision['probabilities'])}). "
        f"The deterministic policy_decision is {policy_decision} from topic_quality={quality}; "
        f"semantic alignment is {_score_label(scores['semantic_alignment'])}; "
        f"source-relative demand strength is {_score_label(scores['demand_signal_strength'])}; "
        f"saturation risk is {_score_label(scores['saturation_risk'])}; evidence sufficiency is "
        f"{_score_label(scores['evidence_sufficiency'])}; paid-supply pattern is {paid_supply}. "
        f"YEE-30 facts: {fact_summary}."
    )
    identities = sorted(input_identity_set(candidate))
    return {
        "topic_key": candidate["topic_key"],
        "candidate_class": candidate_class,
        "concept_label": concept_label,
        "market_pattern": market_pattern,
        "summary": f"{concept_label} is a {candidate_class.replace('_', ' ')} candidate. YEE-30 facts: {fact_summary}.",
        "triage_decision": decision,
        "policy_decision": policy_decision,
        "decision_policy_version": DECISION_POLICY_VERSION,
        "topic_quality": answers["topic_quality"],
        "scores": scores,
        "paid_supply_pattern": answers["paid_supply_pattern"],
        "noul_answers": {key: answers[key] for key in QUESTION_IDS if answers[key]["type"] == "noul"},
        "typed_answers": answers,
        "probability_sum_deviations": _probability_sum_deviations(answers),
        "rationale": rationale,
        "external_research_needed": answers["external_research_needed"]["noul"] >= 0.5,
        "external_research_questions": _research_question_templates(answers, concept_label),
        "evidence_references": {
            "candidate_fields": ["candidate.topic_display", "candidate.candidate_class", "candidate.source_presence"],
            "canonical_identities": identities,
            "topic_source_facts": fact_references,
        },
        "model_identity": {
            "provider_id": provider.provider_id,
            "transport_id": provider.transport_id,
            "requested_model": provider.model_identifier,
            "returned_model": envelope["model"],
            "provider_metadata": envelope.get("provider_metadata", {}),
        },
        "usage": envelope.get("usage", {}),
        "state_sha256": state_sha256,
        "question_set_sha256": question_set_sha256,
        "raw_evidence": {"request_sha256": request_sha256, "response_sha256": response_sha256},
    }


class TriageRunner:
    """SQLite-backed cache, raw-first HTTP evidence, retries and exports."""

    def __init__(
        self,
        database: str | Path,
        provider: Reasoner,
        run_metadata: Mapping[str, Any],
        inference_parameters: Mapping[str, Any],
        max_attempts: int = 2,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        self.provider = provider
        self.inference_parameters = dict(inference_parameters)
        if self.inference_parameters:
            raise ValueError("TypeSafe System One has no inference-parameter fields; keep this mapping empty")
        self.max_attempts = max_attempts
        self.questions, contract_hash = load_contract()
        if run_metadata.get("question_set_sha256") != contract_hash:
            raise ValueError("run metadata question-set hash does not match the checked-in contract")
        if run_metadata.get("question_set_version") != QUESTION_SET_VERSION:
            raise ValueError("run metadata question-set version does not match the checked-in contract")
        if run_metadata.get("derived_output_version") != DERIVED_OUTPUT_VERSION:
            raise ValueError("run metadata derived-output version does not match the checked-in contract")
        if run_metadata.get("provider_id") != provider.provider_id:
            raise ValueError("run metadata provider does not match the injected provider")
        if run_metadata.get("model_identifier") != provider.model_identifier or run_metadata.get("model_version") != provider.model_version:
            raise ValueError("run metadata model identity does not match the injected provider")
        if run_metadata.get("transport_id") != provider.transport_id or run_metadata.get("transport_config") != dict(provider.transport_config):
            raise ValueError("run metadata transport does not match the injected provider")
        if run_metadata.get("inference_parameters") != self.inference_parameters:
            raise ValueError("run metadata inference parameters do not match runner settings")
        self.connection = sqlite3.connect(Path(database).as_posix(), isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self._initialize(run_metadata)

    def close(self) -> None:
        self.connection.close()
        close = getattr(self.provider, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> "TriageRunner":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _initialize(self, metadata: Mapping[str, Any]) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS run_metadata (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS candidate_targets (
                topic_key TEXT PRIMARY KEY,
                candidate_json TEXT NOT NULL,
                input_sha256 TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS candidate_runs (
                cache_key TEXT PRIMARY KEY,
                topic_key TEXT NOT NULL,
                input_sha256 TEXT NOT NULL,
                question_set_sha256 TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                model_identifier TEXT NOT NULL,
                model_version TEXT NOT NULL,
                inference_parameters_json TEXT NOT NULL,
                replicate_id TEXT,
                status TEXT NOT NULL CHECK(status IN ('pending','running','completed','failed')),
                normalized_json TEXT,
                last_error TEXT
            );
            CREATE TABLE IF NOT EXISTS candidate_attempts (
                cache_key TEXT NOT NULL REFERENCES candidate_runs(cache_key),
                attempt_no INTEGER NOT NULL,
                stage TEXT NOT NULL CHECK(stage IN ('calling','raw_saved','completed','provider_error','parse_error')),
                request_body BLOB,
                request_sha256 TEXT,
                raw_response BLOB,
                raw_response_sha256 TEXT,
                request_id TEXT,
                usage_json TEXT,
                error TEXT,
                parsed_at_ns INTEGER,
                PRIMARY KEY(cache_key, attempt_no)
            );
            CREATE INDEX IF NOT EXISTS candidate_runs_topic_idx ON candidate_runs(topic_key);
            """
        )
        run_columns = {row["name"] for row in self.connection.execute("PRAGMA table_info(candidate_runs)")}
        if "question_set_sha256" not in run_columns:
            self.connection.execute("ALTER TABLE candidate_runs ADD COLUMN question_set_sha256 TEXT")
            if "prompt_sha256" in run_columns:
                self.connection.execute(
                    "UPDATE candidate_runs SET question_set_sha256=prompt_sha256 WHERE question_set_sha256 IS NULL"
                )
        attempt_columns = {row["name"] for row in self.connection.execute("PRAGMA table_info(candidate_attempts)")}
        if "request_body" not in attempt_columns:
            self.connection.execute("ALTER TABLE candidate_attempts ADD COLUMN request_body BLOB")
        if "request_sha256" not in attempt_columns:
            self.connection.execute("ALTER TABLE candidate_attempts ADD COLUMN request_sha256 TEXT")
        actual = {row["key"]: json.loads(row["value_json"]) for row in self.connection.execute("SELECT * FROM run_metadata")}
        canonical = {key: json.loads(canonical_json(value)) for key, value in metadata.items()}
        if actual and actual != canonical:
            raise ValueError("database run metadata is immutable and differs from requested configuration")
        if not actual:
            self.connection.executemany(
                "INSERT INTO run_metadata(key,value_json) VALUES (?,?)",
                [(key, canonical_json(value)) for key, value in sorted(metadata.items())],
            )

    def _cache_key(self, candidate_json: str, replicate_id: str | None = None) -> tuple[str, str, str]:
        state_sha = sha256_bytes(candidate_json.encode("utf-8"))
        question_sha = self._question_set_sha256()
        body = self._request_body(json.loads(candidate_json))
        request_sha = sha256_bytes(body)
        identity = {
            "state_sha256": state_sha,
            "question_set_sha256": question_sha,
            "request_sha256": request_sha,
            "provider_id": self.provider.provider_id,
            "model_identifier": self.provider.model_identifier,
            "model_version": self.provider.model_version,
            "transport_id": self.provider.transport_id,
            "transport_config": dict(self.provider.transport_config),
            "inference_parameters": self.inference_parameters,
            "replicate_id": replicate_id,
        }
        return sha256_bytes(canonical_json(identity).encode("utf-8")), state_sha, question_sha

    def _question_set_sha256(self) -> str:
        return _question_set_hash()

    def _request_body(self, candidate: Mapping[str, Any]) -> bytes:
        return canonical_json({
            "model": self.provider.model_identifier,
            "state": candidate,
            "questions": self.questions,
        }).encode("utf-8")

    def register_targets(self, candidates: list[Mapping[str, Any]]) -> None:
        seen: set[str] = set()
        for candidate in candidates:
            candidate_json = _candidate_json(candidate)
            topic_key = str(candidate["topic_key"])
            if topic_key in seen:
                raise ValueError(f"duplicate target topic_key: {topic_key}")
            seen.add(topic_key)
            _, input_sha, _ = self._cache_key(candidate_json)
            self.connection.execute(
                "INSERT INTO candidate_targets(topic_key,candidate_json,input_sha256) VALUES (?,?,?) "
                "ON CONFLICT(topic_key) DO NOTHING",
                (topic_key, candidate_json, input_sha),
            )
            saved = self.connection.execute(
                "SELECT candidate_json,input_sha256 FROM candidate_targets WHERE topic_key=?", (topic_key,)
            ).fetchone()
            if saved["candidate_json"] != candidate_json or saved["input_sha256"] != input_sha:
                raise ValueError(f"target input changed for topic_key {topic_key}")
            self._ensure_run_record(candidate_json, None)

    def _ensure_run_record(self, candidate_json: str, replicate_id: str | None) -> tuple[str, str, str]:
        candidate = json.loads(candidate_json)
        cache_key, input_sha, question_sha = self._cache_key(candidate_json, replicate_id)
        self.connection.execute(
            "INSERT OR IGNORE INTO candidate_runs "
            "(cache_key,topic_key,input_sha256,question_set_sha256,provider_id,model_identifier,model_version,"
            "inference_parameters_json,replicate_id,status,normalized_json,last_error) "
            "VALUES (?,?,?,?,?,?,?,?,?,'pending',NULL,NULL)",
            (
                cache_key,
                candidate["topic_key"],
                input_sha,
                question_sha,
                self.provider.provider_id,
                self.provider.model_identifier,
                self.provider.model_version,
                canonical_json(self.inference_parameters),
                replicate_id,
            ),
        )
        return cache_key, input_sha, question_sha

    def _request(
        self,
        candidate: Mapping[str, Any],
        candidate_json: str,
        cache_key: str,
        input_sha: str,
        question_sha: str,
        replicate_id: str | None,
    ) -> InferenceRequest:
        state = json.loads(candidate_json)
        body = self._request_body(state)
        return InferenceRequest(
            topic_key=str(candidate["topic_key"]),
            model_identifier=self.provider.model_identifier,
            state=state,
            questions=self.questions,
            body=body,
            input_sha256=input_sha,
            question_set_sha256=question_sha,
            request_sha256=sha256_bytes(body),
            cache_key=cache_key,
            replicate_id=replicate_id,
        )

    def _parse_saved_attempt(
        self,
        cache_key: str,
        attempt_no: int,
        raw_response: bytes,
        candidate: Mapping[str, Any],
        request_sha256: str,
    ) -> dict[str, Any] | None:
        try:
            envelope = parse_response(raw_response, self.questions)
            run = self.connection.execute("SELECT input_sha256 FROM candidate_runs WHERE cache_key=?", (cache_key,)).fetchone()
            normalized = normalize_native_response(
                envelope,
                candidate,
                self.provider,
                run["input_sha256"],
                self._question_set_sha256(),
                request_sha256,
                sha256_bytes(raw_response),
            )
        except TriageValidationError as exc:
            self.connection.execute(
                "UPDATE candidate_attempts SET stage='parse_error',error=?,parsed_at_ns=? WHERE cache_key=? AND attempt_no=?",
                (str(exc), time.time_ns(), cache_key, attempt_no),
            )
            self.connection.execute(
                "UPDATE candidate_runs SET status='failed',last_error=? WHERE cache_key=?", (str(exc), cache_key)
            )
            return None
        normalized["raw_evidence"]["selected_attempt_no"] = attempt_no
        normalized_json = canonical_json(normalized)
        self.connection.execute(
            "UPDATE candidate_attempts SET stage='completed',usage_json=?,parsed_at_ns=? WHERE cache_key=? AND attempt_no=?",
            (canonical_json(normalized.get("usage", {})), time.time_ns(), cache_key, attempt_no),
        )
        self.connection.execute(
            "UPDATE candidate_runs SET status='completed',normalized_json=?,last_error=NULL WHERE cache_key=?",
            (normalized_json, cache_key),
        )
        return normalized

    def replay_saved_attempts(self) -> dict[str, Any]:
        """Reparse stored HTTP responses in original order without calling the provider."""
        runs = self.connection.execute(
            "SELECT r.*,t.candidate_json,MIN(a.rowid) AS first_saved_order "
            "FROM candidate_runs r JOIN candidate_targets t ON t.topic_key=r.topic_key "
            "LEFT JOIN candidate_attempts a ON a.cache_key=r.cache_key "
            "WHERE r.replicate_id IS NULL GROUP BY r.cache_key "
            "ORDER BY first_saved_order,r.topic_key"
        ).fetchall()
        if not runs:
            return {"results": [], "attempts": []}

        run_by_key = {row["cache_key"]: row for row in runs}
        results = {
            row["cache_key"]: {
                "topic_key": row["topic_key"],
                "cache_key": row["cache_key"],
                "status": "failed",
                "normalized": None,
                "error": None,
                "selected_attempt_no": None,
            }
            for row in runs
        }
        selected: set[str] = set()
        last_errors: dict[str, str] = {}
        replay_attempts: list[dict[str, Any]] = []

        self.connection.execute("BEGIN")
        try:
            for cache_key in run_by_key:
                self.connection.execute(
                    "UPDATE candidate_runs SET status='pending',normalized_json=NULL,last_error=NULL WHERE cache_key=?",
                    (cache_key,),
                )
            attempts = self.connection.execute(
                "SELECT a.rowid AS saved_order,a.cache_key,a.attempt_no,a.raw_response,a.raw_response_sha256,"
                "a.request_sha256,a.request_id,r.topic_key,r.input_sha256,r.question_set_sha256 "
                "FROM candidate_attempts a JOIN candidate_runs r ON r.cache_key=a.cache_key "
                "WHERE r.replicate_id IS NULL ORDER BY a.rowid"
            ).fetchall()
            for attempt in attempts:
                record = {
                    "saved_order": attempt["saved_order"],
                    "topic_key": attempt["topic_key"],
                    "attempt_no": attempt["attempt_no"],
                    "request_id": attempt["request_id"],
                    "response_sha256": attempt["raw_response_sha256"],
                }
                raw_response = attempt["raw_response"]
                if raw_response is None:
                    error = "saved attempt has no raw response body"
                    record.update({"validation_status": "missing_raw_response", "error": error})
                    last_errors[attempt["cache_key"]] = error
                    replay_attempts.append(record)
                    continue
                try:
                    envelope = parse_response(raw_response, self.questions)
                except TriageValidationError as exc:
                    error = str(exc)
                    record.update({"validation_status": "invalid", "error": error})
                    last_errors[attempt["cache_key"]] = error
                    replay_attempts.append(record)
                    continue

                deviations = _probability_sum_deviations(envelope["answers"])
                record.update({
                    "validation_status": "valid_not_selected" if attempt["cache_key"] in selected else "selected",
                    "probability_sum_deviations": deviations,
                })
                replay_attempts.append(record)
                if attempt["cache_key"] in selected:
                    continue

                run = run_by_key[attempt["cache_key"]]
                candidate = json.loads(run["candidate_json"])
                normalized = normalize_native_response(
                    envelope,
                    candidate,
                    self.provider,
                    attempt["input_sha256"],
                    attempt["question_set_sha256"],
                    attempt["request_sha256"] or "",
                    attempt["raw_response_sha256"] or sha256_bytes(raw_response),
                )
                normalized["raw_evidence"]["selected_attempt_no"] = attempt["attempt_no"]
                normalized_json = canonical_json(normalized)
                self.connection.execute(
                    "UPDATE candidate_runs SET status='completed',normalized_json=?,last_error=NULL WHERE cache_key=?",
                    (normalized_json, attempt["cache_key"]),
                )
                result = results[attempt["cache_key"]]
                result.update({
                    "status": "completed",
                    "normalized": normalized,
                    "error": None,
                    "selected_attempt_no": attempt["attempt_no"],
                })
                selected.add(attempt["cache_key"])

            for cache_key, result in results.items():
                if cache_key in selected:
                    continue
                error = last_errors.get(cache_key, "no saved raw response was available")
                self.connection.execute(
                    "UPDATE candidate_runs SET status='failed',normalized_json=NULL,last_error=? WHERE cache_key=?",
                    (error, cache_key),
                )
                result["error"] = error
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

        return {
            "results": [results[row["cache_key"]] for row in runs],
            "attempts": replay_attempts,
        }

    def run_candidate(self, candidate: Mapping[str, Any], replicate_id: str | None = None) -> dict[str, Any]:
        candidate_json = _candidate_json(candidate)
        topic_key = str(candidate["topic_key"])
        cache_key, input_sha, question_sha = self._cache_key(candidate_json, replicate_id)
        self.register_targets([candidate])
        if replicate_id is not None:
            cache_key, input_sha, question_sha = self._ensure_run_record(candidate_json, replicate_id)
        run = self.connection.execute("SELECT * FROM candidate_runs WHERE cache_key=?", (cache_key,)).fetchone()
        if run["status"] == "completed":
            return {"topic_key": topic_key, "cache_key": cache_key, "status": "completed", "normalized": json.loads(run["normalized_json"])}

        # Recover an exact raw response left between durable capture and parsing.
        saved = self.connection.execute(
            "SELECT attempt_no,raw_response,request_sha256 FROM candidate_attempts "
            "WHERE cache_key=? AND stage='raw_saved' ORDER BY attempt_no DESC LIMIT 1",
            (cache_key,),
        ).fetchone()
        if saved is not None:
            normalized = self._parse_saved_attempt(
                cache_key, saved["attempt_no"], saved["raw_response"], candidate, saved["request_sha256"]
            )
            if normalized is not None:
                return {"topic_key": topic_key, "cache_key": cache_key, "status": "completed", "normalized": normalized}

        attempts = self.connection.execute(
            "SELECT COALESCE(MAX(attempt_no),0) FROM candidate_attempts WHERE cache_key=?", (cache_key,)
        ).fetchone()[0]
        while attempts < self.max_attempts:
            attempts += 1
            request = self._request(candidate, candidate_json, cache_key, input_sha, question_sha, replicate_id)
            self.connection.execute(
                "INSERT INTO candidate_attempts(cache_key,attempt_no,stage,request_body,request_sha256) VALUES (?,?,'calling',?,?)",
                (cache_key, attempts, request.body, request.request_sha256),
            )
            self.connection.execute("UPDATE candidate_runs SET status='running' WHERE cache_key=?", (cache_key,))

            def persist_sdk_request(raw_body: bytes) -> None:
                try:
                    payload = json.loads(raw_body)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValueError("SDK request bytes are not valid JSON") from exc
                if payload != json.loads(request.body):
                    raise ValueError("SDK request bytes differ from the deterministic request payload")
                self.connection.execute(
                    "UPDATE candidate_attempts SET request_body=?,request_sha256=? WHERE cache_key=? AND attempt_no=?",
                    (raw_body, sha256_bytes(raw_body), cache_key, attempts),
                )

            def persist_sdk_response(status_code: int, raw_body: bytes, request_id: str | None) -> None:
                self.connection.execute(
                    "UPDATE candidate_attempts SET stage='raw_saved',raw_response=?,raw_response_sha256=?,request_id=? "
                    "WHERE cache_key=? AND attempt_no=?",
                    (raw_body, sha256_bytes(raw_body), request_id, cache_key, attempts),
                )

            capture = RawEvidenceCapture(persist_sdk_request, persist_sdk_response)
            try:
                response = self.provider.complete(request, capture)
                if not isinstance(response, ProviderResponse) or not isinstance(response.raw_body, bytes):
                    raise TypeError("provider must return exact raw response bytes")
            except Exception as exc:
                message = str(exc) if isinstance(exc, JevTransportError) else "provider call failed"
                self.connection.execute(
                    "UPDATE candidate_attempts SET stage='provider_error',error=? WHERE cache_key=? AND attempt_no=?",
                    (message, cache_key, attempts),
                )
                self.connection.execute(
                    "UPDATE candidate_runs SET status='failed',last_error=? WHERE cache_key=?", (message, cache_key)
                )
                continue

            # Commit the exact raw response before invoking any parser/validator.
            self.connection.execute(
                "UPDATE candidate_attempts SET stage='raw_saved',raw_response=?,raw_response_sha256=?,request_id=? "
                "WHERE cache_key=? AND attempt_no=?",
                (
                    response.raw_body,
                    sha256_bytes(response.raw_body),
                    response.request_id,
                    cache_key,
                    attempts,
                ),
            )
            attempt = self.connection.execute(
                "SELECT request_sha256 FROM candidate_attempts WHERE cache_key=? AND attempt_no=?",
                (cache_key, attempts),
            ).fetchone()
            saved_request_sha256 = attempt["request_sha256"] or request.request_sha256
            normalized = self._parse_saved_attempt(
                cache_key, attempts, response.raw_body, candidate, saved_request_sha256
            )
            if normalized is not None:
                return {"topic_key": topic_key, "cache_key": cache_key, "status": "completed", "normalized": normalized}

        final = self.connection.execute("SELECT last_error FROM candidate_runs WHERE cache_key=?", (cache_key,)).fetchone()
        return {"topic_key": topic_key, "cache_key": cache_key, "status": "failed", "normalized": None, "error": final["last_error"]}

    def run_pilot(
        self,
        candidates: list[Mapping[str, Any]],
    ) -> tuple[list[Mapping[str, Any]], list[dict[str, Any]]]:
        expected_target_count = EXPECTED_INPUT_COUNTS["eligible_evidence_packs"]
        if len(candidates) != expected_target_count:
            raise InputIntegrityError(f"expected {expected_target_count} eligible targets, received {len(candidates)}")
        self.register_targets(candidates)
        pilot = select_stratified_pilot(candidates, 120)
        return pilot, [self.run_candidate(candidate) for candidate in pilot]

    def run_full(
        self,
        candidates: list[Mapping[str, Any]],
        pilot_report: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        require_pilot_pass(pilot_report)
        expected_target_count = EXPECTED_INPUT_COUNTS["eligible_evidence_packs"]
        if len(candidates) != expected_target_count:
            raise InputIntegrityError(f"expected {expected_target_count} eligible targets, received {len(candidates)}")
        self.register_targets(candidates)
        return [self.run_candidate(candidate) for candidate in sorted(candidates, key=lambda item: str(item["topic_key"]))]

    def stability_audit(
        self,
        pilot: list[Mapping[str, Any]],
        repetitions: int = 3,
    ) -> dict[str, Any]:
        if repetitions != 3:
            raise ValueError("YEE-31 stability audit requires exactly three independent runs")
        subset = select_stability_subset(pilot, 60)
        outcomes: dict[str, list[dict[str, Any]]] = {}
        for candidate in subset:
            topic_key = str(candidate["topic_key"])
            outcomes[topic_key] = [
                self.run_candidate(candidate, replicate_id=f"stability-{replicate}")
                for replicate in range(1, repetitions + 1)
            ]
        agreement_count = 0
        adjacent: dict[str, int] = {}
        question_deltas = {
            question_id: {"adjacent_changed_pairs": 0, "adjacent_compared_pairs": 0}
            for question_id in QUESTION_IDS
        }
        for runs in outcomes.values():
            decisions = [
                run["normalized"]["triage_decision"]["choice"]
                for run in runs if run["status"] == "completed"
            ]
            if len(decisions) == repetitions and len(set(decisions)) == 1:
                agreement_count += 1
            for first, second in zip(runs, runs[1:]):
                if first["status"] == second["status"] == "completed":
                    pair = (
                        f"{first['normalized']['triage_decision']['choice']}->"
                        f"{second['normalized']['triage_decision']['choice']}"
                    )
                    adjacent[pair] = adjacent.get(pair, 0) + 1
                    first_answers = first["normalized"]["typed_answers"]
                    second_answers = second["normalized"]["typed_answers"]
                    for question_id in QUESTION_IDS:
                        question_deltas[question_id]["adjacent_compared_pairs"] += 1
                        if canonical_json(first_answers[question_id]) != canonical_json(second_answers[question_id]):
                            question_deltas[question_id]["adjacent_changed_pairs"] += 1
        return {
            "status": "COMPLETE" if all(run["status"] == "completed" for runs in outcomes.values() for run in runs) else "INCOMPLETE",
            "candidate_count": len(subset),
            "runs_per_candidate": repetitions,
            "exact_decision_agreement_count": agreement_count,
            "exact_decision_agreement_rate": round(agreement_count / len(subset), 6) if subset else 0.0,
            "adjacent_disagreement_patterns": dict(sorted(adjacent.items())),
            "per_question_answer_deltas": question_deltas,
            "outcomes": outcomes,
        }

    def export_payloads(self) -> dict[str, str]:
        partitions: dict[str, list[dict[str, Any]]] = {
            "candidate_triage": [],
            "ADVANCE": [],
            "HOLD": [],
            "REJECT": [],
            "failed_pending": [],
        }
        targets = self.connection.execute("SELECT * FROM candidate_targets ORDER BY topic_key").fetchall()
        for target in targets:
            candidate = json.loads(target["candidate_json"])
            cache_key, _, _ = self._cache_key(target["candidate_json"])
            run = self.connection.execute("SELECT * FROM candidate_runs WHERE cache_key=?", (cache_key,)).fetchone()
            if run is None or run["status"] != "completed":
                record = {
                    "topic_key": target["topic_key"],
                    "cache_key": cache_key,
                    "status": run["status"] if run else "pending",
                    "error": run["last_error"] if run else None,
                }
                partitions["failed_pending"].append(record)
                continue
            result = json.loads(run["normalized_json"])
            record = {"topic_key": target["topic_key"], "cache_key": cache_key, "triage": result}
            partitions["candidate_triage"].append(record)
            partitions[result["policy_decision"]].append(record)
        exports = {
            f"{name}.jsonl": "".join(canonical_json(record) + "\n" for record in rows)
            for name, rows in partitions.items()
        }
        csv_fields = (
            "topic_key",
            "cache_key",
            "status",
            "policy_decision",
            "native_triage_decision",
            "triage_json",
            "error",
        )
        for name, rows in partitions.items():
            output = io.StringIO(newline="")
            writer = csv.DictWriter(output, fieldnames=csv_fields, lineterminator="\n")
            writer.writeheader()
            for record in rows:
                triage = record.get("triage")
                writer.writerow({
                    "topic_key": record["topic_key"],
                    "cache_key": record["cache_key"],
                    "status": record.get("status", "completed"),
                    "policy_decision": triage.get("policy_decision", "") if triage else "",
                    "native_triage_decision": triage.get("triage_decision", {}).get("choice", "") if triage else "",
                    "triage_json": canonical_json(triage) if triage else "",
                    "error": record.get("error") or "",
                })
            exports[f"{name}.csv"] = output.getvalue()
        return exports
