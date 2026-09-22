"""Deterministic YEE-30 lexical topic and retrieval layer.

The module intentionally reads the accepted YEE-29 analysis database through a
read-only SQLite connection.  It creates a separate retrieval database and
never mutates the canonical input.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import shutil
import sqlite3
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


EXPECTED_SOURCE_COUNTS = {"voxel": 6639, "modrinth": 158507, "hangar": 3861}
EXPECTED_TOTAL = sum(EXPECTED_SOURCE_COUNTS.values())
SUPPORT_GATES = {"voxel": 2, "hangar": 3, "modrinth": 10}
DEMAND_SOURCES = ("modrinth", "hangar")
EVIDENCE_EXAMPLES_PER_SOURCE = 15
EVIDENCE_EXAMPLES_PER_SELECTION = 5
NORMALIZATION_VERSION = "yee-30-topic-normalization-v2"
STOPWORD_VERSION = "yee-30-stopwords-v1"
TOPIC_SCHEMA_VERSION = "yee-30-topic-layer-v0.1"
NULL_TOKEN = r"\N"

CORPUS_COLUMNS = [
    "source", "source_resource_id", "canonical_identity", "slug", "title", "summary",
    "project_type", "project_type_norm", "categories_json", "loaders_json", "versions_json",
    "title_tokens_json", "title_normalized", "facet_text", "corpus_text", "source_url",
    "demand_percentile", "downloads_total", "demand_metric_source", "freshness_age_days",
    "age_days", "paid_state", "price_amount", "currency", "voxel_review_count",
    "voxel_review_stars", "follow_count", "star_count", "watcher_count",
    "hangar_recent_downloads", "hangar_recent_views",
]

TOPIC_FACT_COLUMNS = [
    "topic_key", "topic_display", "source", "support_gate_min_resources", "resource_count",
    "demand_available_count", "demand_percentile_p50", "demand_percentile_p75",
    "demand_percentile_p90", "demand_percentile_ge90_count", "demand_percentile_ge90_share",
    "freshness_age_days_p50", "freshness_le90_count", "freshness_le90_share",
    "voxel_free_count", "voxel_paid_count", "voxel_unknown_count", "voxel_paid_resource_count",
    "voxel_price_p50", "voxel_price_p75", "voxel_price_p90", "demand_download_count_p50",
    "demand_download_count_p75", "demand_download_count_p90", "voxel_review_count_p50",
    "voxel_review_count_p75", "voxel_review_count_p90", "voxel_review_stars_p50",
    "voxel_review_stars_p75", "voxel_review_stars_p90", "modrinth_follow_count_p50",
    "modrinth_follow_count_p75", "modrinth_follow_count_p90", "hangar_star_count_p50",
    "hangar_star_count_p75", "hangar_star_count_p90", "hangar_watcher_count_p50",
    "hangar_watcher_count_p75", "hangar_watcher_count_p90", "hangar_recent_downloads_p50",
    "hangar_recent_downloads_p75", "hangar_recent_downloads_p90", "hangar_recent_views_p50",
    "hangar_recent_views_p75", "hangar_recent_views_p90", "source_native_semantics_json",
    "analysis_as_of", "topic_schema_version",
]

CANDIDATE_COLUMNS = [
    "topic_key", "topic_display", "candidate_class", "research_eligible",
    "source_presence_json", "source_fact_keys_json", "demand_gate_sources_json",
    "eligibility_reasons_json", "analysis_as_of", "topic_schema_version",
]

EVIDENCE_COLUMNS = [
    "topic_key", "source", "example_ordinal", "canonical_identity", "source_resource_id",
    "title", "summary", "source_url", "demand_percentile", "downloads_total",
    "demand_metric_source", "freshness_age_days", "age_days", "project_type_norm",
    "category_facets_json", "loader_facets_json", "version_facets_json", "paid_state",
    "price_amount", "currency", "voxel_review_count", "voxel_review_stars", "follow_count",
    "star_count", "watcher_count", "hangar_recent_downloads", "hangar_recent_views",
    "selection_roles_json",
]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _round_number(value: float | int | None) -> float | int | None:
    if value is None:
        return None
    rounded = round(float(value), 6)
    return int(rounded) if rounded.is_integer() else rounded


def _quantile(values: Iterable[Any], probability: float) -> float | int | None:
    usable = sorted(float(value) for value in values if value is not None)
    if not usable:
        return None
    if len(usable) == 1:
        return _round_number(usable[0])
    position = (len(usable) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return _round_number(usable[lower])
    return _round_number(usable[lower] + (usable[upper] - usable[lower]) * (position - lower))


def _number(value: Any) -> float | int | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return int(number) if number.is_integer() else number


def _metric_stats(values: Iterable[Any]) -> dict[str, Any]:
    usable = [_number(value) for value in values]
    usable = [value for value in usable if value is not None]
    return {
        "available_count": len(usable),
        "p50": _quantile(usable, 0.50),
        "p75": _quantile(usable, 0.75),
        "p90": _quantile(usable, 0.90),
    }


def _share(count: int, denominator: int) -> float:
    return _round_number(count / denominator) if denominator else 0


def _parse_json(value: str | None) -> Any:
    if value is None or value == "":
        return None
    return json.loads(value)


def _load_stopwords() -> frozenset[str]:
    path = Path(__file__).with_name("topic_stopwords.txt")
    words = {
        line.strip().casefold()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    return frozenset(words)


def _is_numeric_or_version(token: str) -> bool:
    if re.fullmatch(r"(?:v)?\d+(?:[a-z])?", token):
        return True
    return bool(re.fullmatch(r"(?:v)?\d+(?:[._-]\d+[a-z]?)+", token))


def normalize_tokens(value: Any, stopwords: frozenset[str] | set[str] | None = None) -> list[str]:
    """Apply the versioned topic tokenization rules without stemming."""
    if value is None:
        return []
    stopwords = stopwords or frozenset()
    text = unicodedata.normalize("NFKC", str(value)).casefold()
    text = text.translate(str.maketrans({"’": "'", "‘": "'", "ʼ": "'", "＇": "'"}))
    text = re.sub(r"(?<=\w)'s\b", "", text, flags=re.UNICODE)
    text = re.sub(r"[\W_]+", " ", text, flags=re.UNICODE)
    tokens = []
    for token in text.split():
        if token in stopwords or _is_numeric_or_version(token):
            continue
        if not any(character.isalpha() for character in token):
            continue
        tokens.append(token)
    return tokens


def _normalize_facet_values(value: str | None) -> list[str]:
    parsed = _parse_json(value)
    if parsed is None:
        return []
    values = parsed if isinstance(parsed, list) else [parsed]
    normalized: set[str] = set()
    for item in values:
        text = unicodedata.normalize("NFKC", str(item)).casefold()
        for part in re.split(r"[,;|]", text):
            part = re.sub(r"\s+", " ", part).strip()
            part = re.sub(r"[^\w.-]+", "_", part, flags=re.UNICODE).strip("_.-")
            if part:
                normalized.add(part)
    return sorted(normalized)


def _normalize_project_type(value: Any) -> str | None:
    tokens = normalize_tokens(value)
    return " ".join(tokens) or None


def title_ngrams(title: Any, stopwords: frozenset[str] | set[str] | None = None) -> list[str]:
    """Return deduplicated contiguous normalized title phrases of length 1..3."""
    tokens = normalize_tokens(title, stopwords or _load_stopwords())
    phrases = {
        " ".join(tokens[index:index + size])
        for size in range(1, 4)
        for index in range(0, len(tokens) - size + 1)
    }
    return sorted(phrase for phrase in phrases if phrase)


def _retrieval_tokens(value: str) -> list[str]:
    return sorted(set(normalize_tokens(value)))


def _select_evidence_rows(rows: list[Any]) -> dict[str, set[str]]:
    """Select up to five rows per role, keeping representative identities new."""
    selected: dict[str, set[str]] = {}

    def add(row: Any, role: str) -> None:
        selected.setdefault(row["canonical_identity"], set()).add(role)

    demand_rows = sorted(
        (row for row in rows if row["demand_percentile"] is not None),
        key=lambda row: (-float(row["demand_percentile"]), str(row["source_resource_id"])),
    )
    for row in demand_rows[:EVIDENCE_EXAMPLES_PER_SELECTION]:
        add(row, "highest_demand_percentile")
    fresh_rows = sorted(
        (row for row in rows if row["freshness_age_days"] is not None),
        key=lambda row: (int(row["freshness_age_days"]), str(row["source_resource_id"])),
    )
    for row in fresh_rows[:EVIDENCE_EXAMPLES_PER_SELECTION]:
        add(row, "freshest")
    representatives = sorted(
        (row for row in rows if row["canonical_identity"] not in selected),
        key=lambda row: str(row["source_resource_id"]),
    )
    for row in representatives[:EVIDENCE_EXAMPLES_PER_SELECTION]:
        add(row, "representative")
    return selected


def _input_connection(input_db: Path) -> sqlite3.Connection:
    uri = f"file:{input_db.as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _validate_input(connection: sqlite3.Connection, expected_counts: dict[str, int]) -> dict[str, Any]:
    table_names = {
        row[0]
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    if "analysis_runs" not in table_names or "resource_features" not in table_names:
        raise ValueError("accepted YEE-29 analysis DB is missing required tables")
    total = connection.execute("SELECT COUNT(*) FROM resource_features").fetchone()[0]
    distinct_identities = connection.execute(
        "SELECT COUNT(DISTINCT canonical_identity) FROM resource_features"
    ).fetchone()[0]
    source_counts = {
        row[0]: row[1]
        for row in connection.execute(
            "SELECT source, COUNT(*) FROM resource_features GROUP BY source ORDER BY source"
        )
    }
    expected_total = sum(expected_counts.values())
    if total != expected_total or distinct_identities != expected_total:
        raise ValueError(f"canonical identity count mismatch: {total}/{distinct_identities}")
    if source_counts != dict(sorted(expected_counts.items())):
        raise ValueError(f"source counts mismatch: {source_counts}")
    run = connection.execute("SELECT * FROM analysis_runs ORDER BY rowid DESC LIMIT 1").fetchone()
    if run is None:
        raise ValueError("accepted YEE-29 analysis DB has no analysis_runs row")
    return {
        "resource_features": total,
        "distinct_canonical_identities": distinct_identities,
        "source_counts": dict(sorted(source_counts.items())),
        "analysis_run_id": run["run_id"],
        "analysis_as_of": run["analysis_as_of"],
        "feature_schema_version": run["feature_schema_version"],
    }


def _create_schema(connection: sqlite3.Connection) -> str:
    connection.executescript(
        """
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=FULL;
        CREATE TABLE build_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE resource_corpus (
            source TEXT NOT NULL,
            source_resource_id TEXT NOT NULL,
            canonical_identity TEXT PRIMARY KEY,
            slug TEXT,
            title TEXT,
            summary TEXT,
            project_type TEXT,
            project_type_norm TEXT,
            categories_json TEXT,
            loaders_json TEXT,
            versions_json TEXT,
            title_tokens_json TEXT NOT NULL,
            title_normalized TEXT NOT NULL,
            facet_text TEXT NOT NULL,
            corpus_text TEXT NOT NULL,
            source_url TEXT,
            demand_percentile REAL,
            downloads_total TEXT,
            demand_metric_source TEXT,
            freshness_age_days INTEGER,
            age_days INTEGER,
            paid_state TEXT,
            price_amount REAL,
            currency TEXT,
            voxel_review_count REAL,
            voxel_review_stars REAL,
            follow_count REAL,
            star_count REAL,
            watcher_count REAL,
            hangar_recent_downloads REAL,
            hangar_recent_views REAL
        );
        CREATE TABLE resource_topics (
            topic_key TEXT NOT NULL,
            source TEXT NOT NULL,
            canonical_identity TEXT NOT NULL,
            source_resource_id TEXT NOT NULL,
            PRIMARY KEY (topic_key, source, canonical_identity),
            FOREIGN KEY (canonical_identity) REFERENCES resource_corpus(canonical_identity)
        );
        CREATE TABLE topic_source_facts (
            topic_key TEXT NOT NULL,
            topic_display TEXT NOT NULL,
            source TEXT NOT NULL,
            support_gate_min_resources INTEGER NOT NULL,
            resource_count INTEGER NOT NULL,
            demand_available_count INTEGER NOT NULL,
            demand_percentile_p50 REAL,
            demand_percentile_p75 REAL,
            demand_percentile_p90 REAL,
            demand_percentile_ge90_count INTEGER NOT NULL,
            demand_percentile_ge90_share REAL NOT NULL,
            freshness_age_days_p50 REAL,
            freshness_le90_count INTEGER NOT NULL,
            freshness_le90_share REAL NOT NULL,
            voxel_free_count INTEGER,
            voxel_paid_count INTEGER,
            voxel_unknown_count INTEGER,
            voxel_paid_resource_count INTEGER,
            voxel_price_p50 REAL,
            voxel_price_p75 REAL,
            voxel_price_p90 REAL,
            demand_download_count_p50 REAL,
            demand_download_count_p75 REAL,
            demand_download_count_p90 REAL,
            voxel_review_count_p50 REAL,
            voxel_review_count_p75 REAL,
            voxel_review_count_p90 REAL,
            voxel_review_stars_p50 REAL,
            voxel_review_stars_p75 REAL,
            voxel_review_stars_p90 REAL,
            modrinth_follow_count_p50 REAL,
            modrinth_follow_count_p75 REAL,
            modrinth_follow_count_p90 REAL,
            hangar_star_count_p50 REAL,
            hangar_star_count_p75 REAL,
            hangar_star_count_p90 REAL,
            hangar_watcher_count_p50 REAL,
            hangar_watcher_count_p75 REAL,
            hangar_watcher_count_p90 REAL,
            hangar_recent_downloads_p50 REAL,
            hangar_recent_downloads_p75 REAL,
            hangar_recent_downloads_p90 REAL,
            hangar_recent_views_p50 REAL,
            hangar_recent_views_p75 REAL,
            hangar_recent_views_p90 REAL,
            source_native_semantics_json TEXT NOT NULL,
            analysis_as_of TEXT NOT NULL,
            topic_schema_version TEXT NOT NULL,
            PRIMARY KEY (topic_key, source)
        );
        CREATE TABLE candidate_topics (
            topic_key TEXT PRIMARY KEY,
            topic_display TEXT NOT NULL,
            candidate_class TEXT NOT NULL CHECK(candidate_class IN ('overlap', 'free_demand_only')),
            research_eligible INTEGER NOT NULL CHECK(research_eligible IN (0, 1)),
            source_presence_json TEXT NOT NULL,
            source_fact_keys_json TEXT NOT NULL,
            demand_gate_sources_json TEXT NOT NULL,
            eligibility_reasons_json TEXT NOT NULL,
            analysis_as_of TEXT NOT NULL,
            topic_schema_version TEXT NOT NULL
        );
        CREATE TABLE evidence_packs (
            topic_key TEXT PRIMARY KEY,
            candidate_class TEXT NOT NULL,
            research_eligible INTEGER NOT NULL,
            pack_json TEXT NOT NULL,
            topic_schema_version TEXT NOT NULL
        );
        CREATE TABLE evidence_examples (
            topic_key TEXT NOT NULL,
            source TEXT NOT NULL,
            example_ordinal INTEGER NOT NULL,
            canonical_identity TEXT NOT NULL,
            source_resource_id TEXT NOT NULL,
            title TEXT,
            summary TEXT,
            source_url TEXT,
            demand_percentile REAL,
            downloads_total TEXT,
            demand_metric_source TEXT,
            freshness_age_days INTEGER,
            age_days INTEGER,
            project_type_norm TEXT,
            category_facets_json TEXT,
            loader_facets_json TEXT,
            version_facets_json TEXT,
            paid_state TEXT,
            price_amount REAL,
            currency TEXT,
            voxel_review_count REAL,
            voxel_review_stars REAL,
            follow_count REAL,
            star_count REAL,
            watcher_count REAL,
            hangar_recent_downloads REAL,
            hangar_recent_views REAL,
            selection_roles_json TEXT NOT NULL,
            PRIMARY KEY (topic_key, source, canonical_identity),
            FOREIGN KEY (canonical_identity) REFERENCES resource_corpus(canonical_identity)
        );
        """
    )
    try:
        connection.execute(
            """CREATE VIRTUAL TABLE resource_fts USING fts5(
                canonical_identity UNINDEXED,
                source UNINDEXED,
                source_resource_id UNINDEXED,
                title,
                summary,
                facet_text,
                tokenize='unicode61'
            )"""
        )
        return "fts5"
    except sqlite3.OperationalError:
        connection.execute(
            """CREATE TABLE resource_inverted_index(
                token TEXT NOT NULL,
                canonical_identity TEXT NOT NULL,
                source TEXT NOT NULL,
                PRIMARY KEY(token, canonical_identity)
            )"""
        )
        return "deterministic_inverted_index"


def _feature_to_corpus(row: sqlite3.Row, stopwords: frozenset[str]) -> tuple[dict[str, Any], list[str], str]:
    category_values = _normalize_facet_values(row["category_facets_json"])
    loader_values = _normalize_facet_values(row["loader_facets_json"])
    version_values = _normalize_facet_values(row["version_facets_json"])
    title_tokens = normalize_tokens(row["title"], stopwords)
    title_normalized = " ".join(title_tokens)
    project_type_norm = _normalize_project_type(row["project_type_norm"] or row["project_type"])
    facet_text = " ".join(
        value
        for value in [project_type_norm, *category_values, *loader_values, *version_values]
        if value
    )
    normalized_summary = " ".join(normalize_tokens(row["summary"]))
    corpus_text = " ".join(value for value in [title_normalized, normalized_summary, facet_text] if value)
    identity = row["canonical_identity"] or f"{row['source']}:{row['source_resource_id']}"
    corpus = {
        "source": row["source"],
        "source_resource_id": row["source_resource_id"],
        "canonical_identity": identity,
        "slug": row["slug"],
        "title": row["title"],
        "summary": row["summary"],
        "project_type": row["project_type"],
        "project_type_norm": project_type_norm,
        "categories_json": json_compact(category_values),
        "loaders_json": json_compact(loader_values),
        "versions_json": json_compact(version_values),
        "title_tokens_json": json_compact(title_tokens),
        "title_normalized": title_normalized,
        "facet_text": facet_text,
        "corpus_text": corpus_text,
        "source_url": row["source_url"],
        "demand_percentile": _number(row["demand_percentile"]),
        "downloads_total": None if row["downloads_total"] is None else str(row["downloads_total"]),
        "demand_metric_source": row["demand_metric_source"],
        "freshness_age_days": row["freshness_age_days"],
        "age_days": row["age_days"],
        "paid_state": row["paid_state"],
        "price_amount": _number(row["price_amount"]),
        "currency": row["currency"],
        "voxel_review_count": _number(row["voxel_review_count"]),
        "voxel_review_stars": _number(row["voxel_review_stars"]),
        "follow_count": _number(row["follow_count"]),
        "star_count": _number(row["star_count"]),
        "watcher_count": _number(row["watcher_count"]),
        "hangar_recent_downloads": _number(row["hangar_recent_downloads"]),
        "hangar_recent_views": _number(row["hangar_recent_views"]),
    }
    return corpus, title_ngrams(row["title"], stopwords), identity


def _insert_corpus(
    connection: sqlite3.Connection,
    corpus: dict[str, Any],
    topics: list[str],
    identity: str,
    index_type: str,
) -> None:
    connection.execute(
        f"INSERT INTO resource_corpus ({','.join(CORPUS_COLUMNS)}) VALUES ({','.join('?' for _ in CORPUS_COLUMNS)})",
        [corpus[column] for column in CORPUS_COLUMNS],
    )
    connection.executemany(
        "INSERT INTO resource_topics(topic_key,source,canonical_identity,source_resource_id) VALUES (?,?,?,?)",
        [(topic, corpus["source"], identity, corpus["source_resource_id"]) for topic in topics],
    )
    if index_type == "fts5":
        connection.execute(
            "INSERT INTO resource_fts(canonical_identity,source,source_resource_id,title,summary,facet_text) VALUES (?,?,?,?,?,?)",
            (identity, corpus["source"], corpus["source_resource_id"], corpus["title"] or "", corpus["summary"] or "", corpus["facet_text"]),
        )
    else:
        tokens = set(_retrieval_tokens(corpus["corpus_text"]))
        tokens.add("__all__")
        connection.executemany(
            "INSERT INTO resource_inverted_index(token,canonical_identity,source) VALUES (?,?,?)",
            [(token, identity, corpus["source"]) for token in sorted(tokens)],
        )


def _fact_row(topic_key: str, source: str, rows: list[sqlite3.Row], analysis_as_of: str) -> dict[str, Any]:
    count = len(rows)
    demand_percentiles = [_number(row["demand_percentile"]) for row in rows]
    demand_percentiles = [value for value in demand_percentiles if value is not None]
    demand_values = [_number(row["downloads_total"]) for row in rows]
    demand_values = [value for value in demand_values if value is not None]
    freshness_values = [_number(row["freshness_age_days"]) for row in rows]
    freshness_values = [value for value in freshness_values if value is not None]
    ge90 = sum(1 for value in demand_percentiles if value >= 90)
    fresh_le90 = sum(1 for value in freshness_values if value <= 90)
    row: dict[str, Any] = {
        "topic_key": topic_key,
        "topic_display": topic_key,
        "source": source,
        "support_gate_min_resources": SUPPORT_GATES[source],
        "resource_count": count,
        "demand_available_count": len(demand_percentiles),
        "demand_percentile_p50": _quantile(demand_percentiles, 0.50),
        "demand_percentile_p75": _quantile(demand_percentiles, 0.75),
        "demand_percentile_p90": _quantile(demand_percentiles, 0.90),
        "demand_percentile_ge90_count": ge90,
        "demand_percentile_ge90_share": _share(ge90, count),
        "freshness_age_days_p50": _quantile(freshness_values, 0.50),
        "freshness_le90_count": fresh_le90,
        "freshness_le90_share": _share(fresh_le90, count),
        "voxel_free_count": None,
        "voxel_paid_count": None,
        "voxel_unknown_count": None,
        "voxel_paid_resource_count": None,
        "voxel_price_p50": None,
        "voxel_price_p75": None,
        "voxel_price_p90": None,
        "demand_download_count_p50": _quantile(demand_values, 0.50),
        "demand_download_count_p75": _quantile(demand_values, 0.75),
        "demand_download_count_p90": _quantile(demand_values, 0.90),
        "voxel_review_count_p50": None,
        "voxel_review_count_p75": None,
        "voxel_review_count_p90": None,
        "voxel_review_stars_p50": None,
        "voxel_review_stars_p75": None,
        "voxel_review_stars_p90": None,
        "modrinth_follow_count_p50": None,
        "modrinth_follow_count_p75": None,
        "modrinth_follow_count_p90": None,
        "hangar_star_count_p50": None,
        "hangar_star_count_p75": None,
        "hangar_star_count_p90": None,
        "hangar_watcher_count_p50": None,
        "hangar_watcher_count_p75": None,
        "hangar_watcher_count_p90": None,
        "hangar_recent_downloads_p50": None,
        "hangar_recent_downloads_p75": None,
        "hangar_recent_downloads_p90": None,
        "hangar_recent_views_p50": None,
        "hangar_recent_views_p75": None,
        "hangar_recent_views_p90": None,
        "analysis_as_of": analysis_as_of,
        "topic_schema_version": TOPIC_SCHEMA_VERSION,
    }
    if source == "voxel":
        paid_counts = defaultdict(int)
        for item in rows:
            paid_counts[item["paid_state"] or "unknown"] += 1
        row["voxel_free_count"] = paid_counts["free"]
        row["voxel_paid_count"] = paid_counts["paid"]
        row["voxel_unknown_count"] = paid_counts["unknown"]
        row["voxel_paid_resource_count"] = paid_counts["paid"]
        prices = [_number(item["price_amount"]) for item in rows if item["paid_state"] == "paid"]
        prices = [value for value in prices if value is not None]
        row["voxel_price_p50"] = _quantile(prices, 0.50)
        row["voxel_price_p75"] = _quantile(prices, 0.75)
        row["voxel_price_p90"] = _quantile(prices, 0.90)
        for field, prefix in [("voxel_review_count", "voxel_review_count"), ("voxel_review_stars", "voxel_review_stars")]:
            values = [item[field] for item in rows]
            stats = _metric_stats(values)
            row[prefix + "_p50"] = stats["p50"]
            row[prefix + "_p75"] = stats["p75"]
            row[prefix + "_p90"] = stats["p90"]
    elif source == "modrinth":
        stats = _metric_stats(item["follow_count"] for item in rows)
        row["modrinth_follow_count_p50"] = stats["p50"]
        row["modrinth_follow_count_p75"] = stats["p75"]
        row["modrinth_follow_count_p90"] = stats["p90"]
    elif source == "hangar":
        for field, prefix in [
            ("star_count", "hangar_star_count"),
            ("watcher_count", "hangar_watcher_count"),
            ("hangar_recent_downloads", "hangar_recent_downloads"),
            ("hangar_recent_views", "hangar_recent_views"),
        ]:
            stats = _metric_stats(item[field] for item in rows)
            row[prefix + "_p50"] = stats["p50"]
            row[prefix + "_p75"] = stats["p75"]
            row[prefix + "_p90"] = stats["p90"]
    row["source_native_semantics_json"] = json_compact(
        {
            "demand_percentile": "YEE-29 demand_percentile calculated within this source only",
            "downloads_total": "source-native raw demand metric; never summed across sources",
            "secondary_metrics": {
                "voxel": ["voxel_review_count", "voxel_review_stars", "voxel_price_amount"],
                "modrinth": ["follow_count"],
                "hangar": ["star_count", "watcher_count", "hangar_recent_downloads", "hangar_recent_views"],
            }.get(source, []),
        }
    )
    return row


def _build_facts(connection: sqlite3.Connection, analysis_as_of: str) -> tuple[int, int]:
    before = connection.execute("SELECT COUNT(*) FROM (SELECT topic_key, source FROM resource_topics GROUP BY topic_key, source)").fetchone()[0]
    cursor = connection.execute(
        """SELECT rt.topic_key, rt.source AS topic_source, rc.*
           FROM resource_topics rt
           JOIN resource_corpus rc ON rc.canonical_identity = rt.canonical_identity
           ORDER BY rt.topic_key, rt.source, rt.canonical_identity"""
    )
    current_key: tuple[str, str] | None = None
    group: list[sqlite3.Row] = []
    fact_rows: list[dict[str, Any]] = []

    def flush() -> None:
        nonlocal group, current_key
        if current_key is None:
            return
        topic, source = current_key
        if len(group) >= SUPPORT_GATES[source]:
            fact_rows.append(_fact_row(topic, source, group, analysis_as_of))
        group = []

    for row in cursor:
        key = (row["topic_key"], row["topic_source"])
        if current_key is not None and key != current_key:
            flush()
        current_key = key
        group.append(row)
    flush()
    connection.executemany(
        f"INSERT INTO topic_source_facts ({','.join(TOPIC_FACT_COLUMNS)}) VALUES ({','.join('?' for _ in TOPIC_FACT_COLUMNS)})",
        [[row[column] for column in TOPIC_FACT_COLUMNS] for row in fact_rows],
    )
    return before, len(fact_rows)


def _candidate_rows(connection: sqlite3.Connection, analysis_as_of: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in connection.execute(
        "SELECT * FROM topic_source_facts ORDER BY topic_key, source"
    ):
        grouped[row["topic_key"]].append(row)
    candidates: list[dict[str, Any]] = []
    for topic_key in sorted(grouped):
        facts = grouped[topic_key]
        sources = {row["source"] for row in facts}
        non_voxel = sorted(sources.intersection(DEMAND_SOURCES))
        if "voxel" in sources and non_voxel:
            candidate_class = "overlap"
            eligible = True
            reasons = ["voxel_support_gate_and_non_voxel_source_present"]
        elif "voxel" not in sources and non_voxel:
            candidate_class = "free_demand_only"
            demand_gate_sources = [
                row["source"]
                for row in facts
                if row["source"] in DEMAND_SOURCES and row["demand_percentile_ge90_count"] >= 3
            ]
            demand_gate_sources = sorted(demand_gate_sources)
            eligible = bool(demand_gate_sources)
            reasons = [
                "no_retained_voxel_support",
                "demand_gate:" + ",".join(demand_gate_sources) if demand_gate_sources else "no_demand_source_with_three_ge90_resources",
            ]
        else:
            continue
        source_presence = {row["source"]: row["resource_count"] for row in facts}
        demand_gate_sources = sorted(
            row["source"]
            for row in facts
            if row["source"] in DEMAND_SOURCES and row["demand_percentile_ge90_count"] >= 3
        )
        candidate = {
            "topic_key": topic_key,
            "topic_display": topic_key,
            "candidate_class": candidate_class,
            "research_eligible": int(eligible),
            "source_presence_json": json_compact(dict(sorted(source_presence.items()))),
            "source_fact_keys_json": json_compact(sorted(sources)),
            "demand_gate_sources_json": json_compact(demand_gate_sources),
            "eligibility_reasons_json": json_compact(reasons),
            "analysis_as_of": analysis_as_of,
            "topic_schema_version": TOPIC_SCHEMA_VERSION,
        }
        candidates.append(candidate)
    connection.executemany(
        f"INSERT INTO candidate_topics ({','.join(CANDIDATE_COLUMNS)}) VALUES ({','.join('?' for _ in CANDIDATE_COLUMNS)})",
        [[row[column] for column in CANDIDATE_COLUMNS] for row in candidates],
    )
    return candidates


def _evidence_example(row: sqlite3.Row, roles: list[str]) -> dict[str, Any]:
    return {
        "source": row["source"],
        "source_resource_id": row["source_resource_id"],
        "canonical_identity": row["canonical_identity"],
        "title": row["title"],
        "summary": row["summary"],
        "source_url": row["source_url"],
        "demand_percentile": row["demand_percentile"],
        "downloads_total": row["downloads_total"],
        "demand_metric_source": row["demand_metric_source"],
        "freshness_age_days": row["freshness_age_days"],
        "age_days": row["age_days"],
        "project_type_norm": row["project_type_norm"],
        "category_facets_json": row["categories_json"],
        "loader_facets_json": row["loaders_json"],
        "version_facets_json": row["versions_json"],
        "paid_state": row["paid_state"],
        "price_amount": row["price_amount"],
        "currency": row["currency"],
        "voxel_review_count": row["voxel_review_count"],
        "voxel_review_stars": row["voxel_review_stars"],
        "follow_count": row["follow_count"],
        "star_count": row["star_count"],
        "watcher_count": row["watcher_count"],
        "hangar_recent_downloads": row["hangar_recent_downloads"],
        "hangar_recent_views": row["hangar_recent_views"],
        "selection_roles": roles,
    }


def _build_evidence(connection: sqlite3.Connection, candidates: list[dict[str, Any]]) -> int:
    packs = 0
    for candidate in candidates:
        if not candidate["research_eligible"]:
            continue
        topic_key = candidate["topic_key"]
        facts = {
            row["source"]: row
            for row in connection.execute(
                "SELECT * FROM topic_source_facts WHERE topic_key=? ORDER BY source", (topic_key,)
            )
        }
        source_payload: dict[str, Any] = {}
        for source in sorted(facts):
            rows = list(
                connection.execute(
                    """SELECT rc.* FROM resource_topics rt
                       JOIN resource_corpus rc ON rc.canonical_identity=rt.canonical_identity
                       WHERE rt.topic_key=? AND rt.source=?
                       ORDER BY rc.source_resource_id""",
                    (topic_key, source),
                )
            )
            selected = _select_evidence_rows(rows)
            row_by_identity = {row["canonical_identity"]: row for row in rows}
            chosen = [row_by_identity[identity] for identity in sorted(selected)[:EVIDENCE_EXAMPLES_PER_SOURCE]]
            examples = []
            for ordinal, row in enumerate(chosen, start=1):
                roles = sorted(selected[row["canonical_identity"]])
                example = _evidence_example(row, roles)
                examples.append(example)
                evidence_row = {
                    **{column: row[column] for column in EVIDENCE_COLUMNS if column in row.keys()},
                    "topic_key": topic_key,
                    "source": source,
                    "example_ordinal": ordinal,
                    "canonical_identity": row["canonical_identity"],
                    "source_resource_id": row["source_resource_id"],
                    "category_facets_json": row["categories_json"],
                    "loader_facets_json": row["loaders_json"],
                    "version_facets_json": row["versions_json"],
                    "selection_roles_json": json_compact(roles),
                }
                connection.execute(
                    f"INSERT INTO evidence_examples ({','.join(EVIDENCE_COLUMNS)}) VALUES ({','.join('?' for _ in EVIDENCE_COLUMNS)})",
                    [evidence_row.get(column) for column in EVIDENCE_COLUMNS],
                )
            source_payload[source] = {
                "resource_count": facts[source]["resource_count"],
                "demand_percentile_ge90_count": facts[source]["demand_percentile_ge90_count"],
                "example_count": len(examples),
                "examples": examples,
            }
        pack = {
            "topic_key": topic_key,
            "candidate_class": candidate["candidate_class"],
            "research_eligible": True,
            "max_examples_per_source": EVIDENCE_EXAMPLES_PER_SOURCE,
            "sources": source_payload,
            "topic_schema_version": TOPIC_SCHEMA_VERSION,
        }
        connection.execute(
            "INSERT INTO evidence_packs(topic_key,candidate_class,research_eligible,pack_json,topic_schema_version) VALUES (?,?,?,?,?)",
            (topic_key, candidate["candidate_class"], 1, json_compact(pack), TOPIC_SCHEMA_VERSION),
        )
        packs += 1
    return packs


def _write_jsonl(connection: sqlite3.Connection, path: Path, query: str) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in connection.execute(query):
            if "pack_json" in row.keys():
                value = json.loads(row["pack_json"])
            else:
                value = {key: row[key] for key in row.keys()}
            handle.write(json_compact(value) + "\n")


def _write_csv(connection: sqlite3.Connection, path: Path, table: str, columns: list[str], order: str) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        for row in connection.execute(f"SELECT {','.join(columns)} FROM {table} ORDER BY {order}"):
            writer.writerow([NULL_TOKEN if value is None else value for value in row])


def _write_exports(connection: sqlite3.Connection, output_dir: Path) -> list[Path]:
    exports_dir = output_dir / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    paths = [
        exports_dir / "resource_corpus.jsonl",
        exports_dir / "topic_source_facts.jsonl",
        exports_dir / "topic_source_facts.csv",
        exports_dir / "candidate_topics.jsonl",
        exports_dir / "candidate_topics.csv",
        exports_dir / "evidence_packs.jsonl",
        exports_dir / "evidence_examples.jsonl",
        exports_dir / "evidence_examples.csv",
    ]
    _write_jsonl(connection, paths[0], "SELECT * FROM resource_corpus ORDER BY source, source_resource_id")
    _write_jsonl(connection, paths[1], "SELECT * FROM topic_source_facts ORDER BY topic_key, source")
    _write_csv(connection, paths[2], "topic_source_facts", TOPIC_FACT_COLUMNS, "topic_key, source")
    _write_jsonl(connection, paths[3], "SELECT * FROM candidate_topics ORDER BY topic_key")
    _write_csv(connection, paths[4], "candidate_topics", CANDIDATE_COLUMNS, "topic_key")
    _write_jsonl(connection, paths[5], "SELECT * FROM evidence_packs ORDER BY topic_key")
    _write_jsonl(connection, paths[6], "SELECT * FROM evidence_examples ORDER BY topic_key, source, example_ordinal")
    _write_csv(connection, paths[7], "evidence_examples", EVIDENCE_COLUMNS, "topic_key, source, example_ordinal")
    return paths


def _artifact(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _qa(
    input_db: Path,
    retrieval_db: Path,
    input_info: dict[str, Any],
    input_sha_before: str,
    input_sha_after: str,
    topic_before: int,
    topic_after: int,
    candidate_rows: list[dict[str, Any]],
    evidence_pack_count: int,
    index_type: str,
    expected_counts: dict[str, int],
) -> dict[str, Any]:
    connection = sqlite3.connect(retrieval_db.as_posix())
    connection.row_factory = sqlite3.Row
    corpus_count = connection.execute("SELECT COUNT(*) FROM resource_corpus").fetchone()[0]
    corpus_sources = dict(connection.execute("SELECT source,COUNT(*) FROM resource_corpus GROUP BY source").fetchall())
    fact_count = connection.execute("SELECT COUNT(*) FROM topic_source_facts").fetchone()[0]
    fact_distinct = connection.execute("SELECT COUNT(*) FROM (SELECT topic_key,source FROM topic_source_facts GROUP BY topic_key,source)").fetchone()[0]
    candidate_count = connection.execute("SELECT COUNT(*) FROM candidate_topics").fetchone()[0]
    candidate_distinct = connection.execute("SELECT COUNT(DISTINCT topic_key) FROM candidate_topics").fetchone()[0]
    topic_counts = {
        (row["topic_key"], row["source"]): row["n"]
        for row in connection.execute("SELECT topic_key,source,COUNT(*) n FROM resource_topics GROUP BY topic_key,source")
    }
    fact_reconciliation_failures = []
    for row in connection.execute("SELECT topic_key,source,resource_count FROM topic_source_facts ORDER BY topic_key,source"):
        if topic_counts.get((row["topic_key"], row["source"]), 0) != row["resource_count"]:
            fact_reconciliation_failures.append({"topic_key": row["topic_key"], "source": row["source"]})
    missing_evidence = connection.execute(
        """SELECT COUNT(*) FROM evidence_examples e
           LEFT JOIN resource_corpus c ON c.canonical_identity=e.canonical_identity
           WHERE c.canonical_identity IS NULL"""
    ).fetchone()[0]
    pack_limit_failures = connection.execute(
        """SELECT COUNT(*) FROM (
             SELECT topic_key,source,COUNT(*) n FROM evidence_examples
             GROUP BY topic_key,source HAVING n > ?
        )""",
        (EVIDENCE_EXAMPLES_PER_SOURCE,),
    ).fetchone()[0]
    if index_type == "fts5":
        index_identity_count = connection.execute("SELECT COUNT(*) FROM resource_fts").fetchone()[0]
    else:
        index_identity_count = connection.execute(
            "SELECT COUNT(DISTINCT canonical_identity) FROM resource_inverted_index WHERE token='__all__'"
        ).fetchone()[0]
    candidate_classes = dict(
        connection.execute("SELECT candidate_class,COUNT(*) FROM candidate_topics GROUP BY candidate_class")
    )
    eligible_counts = dict(
        connection.execute("SELECT research_eligible,COUNT(*) FROM candidate_topics GROUP BY research_eligible")
    )
    expected_total = sum(expected_counts.values())
    checks = {
        "canonical_input_counts": input_info["resource_features"] == expected_total and input_info["source_counts"] == expected_counts,
        "canonical_input_sha_unchanged": input_sha_before == input_sha_after,
        "corpus_identity_count": corpus_count == expected_total,
        "corpus_source_counts": corpus_sources == expected_counts,
        "index_identity_count": index_identity_count == expected_total,
        "topic_fact_keys_unique": fact_count == fact_distinct,
        "candidate_topic_keys_unique": candidate_count == candidate_distinct,
        "topic_fact_reconciliation": not fact_reconciliation_failures,
        "evidence_identities_exist": missing_evidence == 0,
        "evidence_pack_source_limit": pack_limit_failures == 0,
        "no_scalar_score_or_rank": not any("score" in column or "rank" in column for column in CANDIDATE_COLUMNS),
    }
    connection.close()
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "input": {
            "path": str(input_db),
            "bytes": input_db.stat().st_size,
            "sha256_before": input_sha_before,
            "sha256_after": input_sha_after,
            **input_info,
        },
        "output": {
            "corpus_identity_count": corpus_count,
            "corpus_source_counts": dict(sorted(corpus_sources.items())),
            "topic_source_facts": fact_count,
            "candidate_topics": candidate_count,
            "candidate_counts_by_class": dict(sorted(candidate_classes.items())),
            "candidate_counts_by_research_eligible": {str(k): v for k, v in sorted(eligible_counts.items())},
            "evidence_packs": evidence_pack_count,
            "evidence_pack_limit_per_source": EVIDENCE_EXAMPLES_PER_SOURCE,
            "retrieval_index_type": index_type,
            "retrieval_index_identity_count": index_identity_count,
            "topic_pairs_before_support_gates": topic_before,
            "topic_pairs_after_support_gates": topic_after,
        },
        "support_gates": SUPPORT_GATES,
        "normalization_version": NORMALIZATION_VERSION,
        "stopword_version": STOPWORD_VERSION,
        "topic_schema_version": TOPIC_SCHEMA_VERSION,
        "fact_reconciliation_failures": fact_reconciliation_failures[:20],
        "missing_evidence_identity_count": missing_evidence,
        "evidence_pack_limit_failures": pack_limit_failures,
        "source_relative_demand_note": "downloads_total p-statistics are source-native; no raw download counts are summed across markets",
        "deterministic_replay": {"status": "PENDING"},
        "non_goals_verified": [
            "no new API calls", "no embeddings", "no semantic clustering", "no fuzzy entity merge",
            "no JEV/LLM", "no opportunity score", "no ranking", "no recommendation", "no YEE-31",
        ],
    }


def _write_manifest(output_dir: Path, input_db: Path, input_info: dict[str, Any], input_sha_before: str,
                    input_sha_after: str, code_version: str, qa: dict[str, Any]) -> dict[str, Any]:
    files = [
        path for path in output_dir.rglob("*")
        if path.is_file() and path.name != "YEE-30_DATASET_MANIFEST.json"
    ]
    manifest = {
        "status": qa["status"],
        "work_order": "YEE-30",
        "code_version": code_version,
        "input": {
            "accepted_dataset": "YEE-29 analysis.db",
            "filename": input_db.name,
            "bytes": input_db.stat().st_size,
            "sha256_before": input_sha_before,
            "sha256_after": input_sha_after,
            "analysis_run_id": input_info["analysis_run_id"],
            "analysis_as_of": input_info["analysis_as_of"],
            "feature_schema_version": input_info["feature_schema_version"],
        },
        "configuration": {
            "normalization_version": NORMALIZATION_VERSION,
            "stopword_version": STOPWORD_VERSION,
            "topic_schema_version": TOPIC_SCHEMA_VERSION,
            "support_gates": SUPPORT_GATES,
            "evidence_examples_per_source": EVIDENCE_EXAMPLES_PER_SOURCE,
        },
        "qa": qa,
        "artifacts": [_artifact(path, output_dir) for path in sorted(files, key=lambda item: item.relative_to(output_dir).as_posix())],
    }
    _write_json(output_dir / "YEE-30_DATASET_MANIFEST.json", manifest)
    return manifest


def run_pipeline(
    input_db: Path,
    output_dir: Path,
    code_version: str,
    expected_counts: dict[str, int] = EXPECTED_SOURCE_COUNTS,
) -> dict[str, Any]:
    input_db = input_db.resolve()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    retrieval_db = output_dir / "retrieval.db"
    if retrieval_db.exists():
        raise FileExistsError(f"refusing to overwrite retrieval DB: {retrieval_db}")
    input_sha_before = sha256_file(input_db)
    input_connection = _input_connection(input_db)
    try:
        input_info = _validate_input(input_connection, expected_counts)
        analysis_as_of = input_info["analysis_as_of"]
        stopwords = _load_stopwords()
        output_connection = sqlite3.connect(retrieval_db.as_posix())
        output_connection.row_factory = sqlite3.Row
        try:
            index_type = _create_schema(output_connection)
            metadata = {
                "work_order": "YEE-30",
                "code_version": code_version,
                "input_sha256": input_sha_before,
                "input_bytes": str(input_db.stat().st_size),
                "analysis_run_id": input_info["analysis_run_id"],
                "analysis_as_of": analysis_as_of,
                "feature_schema_version": input_info["feature_schema_version"],
                "normalization_version": NORMALIZATION_VERSION,
                "stopword_version": STOPWORD_VERSION,
                "topic_schema_version": TOPIC_SCHEMA_VERSION,
                "support_gates": json_compact(SUPPORT_GATES),
                "index_type": index_type,
            }
            output_connection.executemany("INSERT INTO build_metadata(key,value) VALUES (?,?)", sorted(metadata.items()))
            for row in input_connection.execute("SELECT * FROM resource_features ORDER BY source, source_resource_id"):
                corpus, topics, identity = _feature_to_corpus(row, stopwords)
                _insert_corpus(output_connection, corpus, topics, identity, index_type)
            output_connection.execute("CREATE INDEX resource_topics_topic_source_idx ON resource_topics(topic_key,source)")
            output_connection.execute("CREATE INDEX resource_topics_identity_idx ON resource_topics(canonical_identity)")
            topic_before, topic_after = _build_facts(output_connection, analysis_as_of)
            candidate_rows = _candidate_rows(output_connection, analysis_as_of)
            evidence_pack_count = _build_evidence(output_connection, candidate_rows)
            output_connection.commit()
            export_paths = _write_exports(output_connection, output_dir)
        finally:
            output_connection.close()
    finally:
        input_connection.close()
    input_sha_after = sha256_file(input_db)
    schema_source = Path(__file__).resolve().parents[2] / "TOPIC_SCHEMA.md"
    stopword_source = Path(__file__).with_name("topic_stopwords.txt")
    schema_path = output_dir / "TOPIC_SCHEMA.md"
    stopword_path = output_dir / "TOPIC_STOPWORDS.txt"
    shutil.copyfile(schema_source, schema_path)
    shutil.copyfile(stopword_source, stopword_path)
    qa = _qa(input_db, retrieval_db, input_info, input_sha_before, input_sha_after, topic_before, topic_after, candidate_rows, evidence_pack_count, index_type, expected_counts)
    qa["code_version"] = code_version
    _write_json(output_dir / "YEE-30_QA_RESULT.json", qa)
    manifest = _write_manifest(output_dir, input_db, input_info, input_sha_before, input_sha_after, code_version, qa)
    return {
        "output_dir": output_dir,
        "retrieval_db": retrieval_db,
        "qa": qa,
        "manifest": manifest,
        "export_paths": export_paths,
    }


def complete_replay_check(first: dict[str, Any], replay: dict[str, Any]) -> dict[str, Any]:
    first_dir = Path(first["output_dir"])
    replay_dir = Path(replay["output_dir"])
    relative_paths = [
        path.relative_to(first_dir).as_posix()
        for path in first_dir.rglob("*")
        if path.is_file() and (path.is_relative_to(first_dir / "exports") or path.name == "retrieval.db")
    ]
    comparisons = {}
    for relative in sorted(relative_paths):
        left = first_dir / relative
        right = replay_dir / relative
        comparisons[relative] = {
            "left_sha256": sha256_file(left),
            "right_sha256": sha256_file(right),
            "byte_identical": right.exists() and left.read_bytes() == right.read_bytes(),
        }
    passed = all(item["byte_identical"] for item in comparisons.values())
    qa_path = first_dir / "YEE-30_QA_RESULT.json"
    qa = json.loads(qa_path.read_text(encoding="utf-8"))
    qa["deterministic_replay"] = {"status": "PASS" if passed else "FAIL", "files": comparisons}
    qa["checks"]["deterministic_byte_identical_replay"] = passed
    qa["status"] = "PASS" if all(qa["checks"].values()) else "FAIL"
    _write_json(qa_path, qa)
    input_info = qa["input"]
    manifest = _write_manifest(
        first_dir,
        Path(input_info["path"] if "path" in input_info else "analysis.db"),
        input_info,
        input_info["sha256_before"],
        input_info["sha256_after"],
        qa.get("code_version", "unknown"),
        qa,
    )
    return {"status": qa["status"], "comparisons": comparisons, "manifest": manifest}


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the deterministic YEE-30 topic candidate layer")
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--input-db", required=True, type=Path)
    build.add_argument("--output-dir", required=True, type=Path)
    build.add_argument("--replay-output-dir", type=Path)
    build.add_argument("--code-version", required=True)
    args = parser.parse_args()
    result = run_pipeline(args.input_db, args.output_dir, args.code_version)
    replay_result = None
    if args.replay_output_dir:
        replay_result = run_pipeline(args.input_db, args.replay_output_dir, args.code_version)
        replay_result = complete_replay_check(result, replay_result)
    print(json_compact({"status": result["qa"]["status"], "replay": replay_result}))


if __name__ == "__main__":
    main()

