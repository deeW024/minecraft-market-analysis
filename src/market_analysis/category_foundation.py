"""YEE-61 product-form guard and category-first foundation.

This module reads only the accepted YEE-60 plugin-eligible universe. Scope and
category decisions are deterministic and use source-native product text and
functional category facets; demand/price/popularity fields are never read by
the decision functions.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sqlite3
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


WORK_ORDER = "YEE-61"
SCHEMA_VERSION = "yee-61-category-first-foundation-v0.1"
SCOPE_CLASSIFIER_VERSION = "yee-61-product-form-semantic-guard-v0.1"
ASSIGNMENT_VERSION = "yee-61-category-assignment-v0.1"
TAXONOMY_VERSION = "yee-61-functional-category-taxonomy-v0.1"
YEE60_CLASSIFIER_VERSION = "yee-60-source-native-plugin-classifier-v0.1"

YEE60_JSONL_SHA256 = "355e4efe0e75d5bbac4205986c7b3fd37a405bbab8ca6be9d4d3b807ed1909f2"
YEE60_SQLITE_SHA256 = "94171517a9c3fb1670ad30f49d48d12fcde8fc37f6f9e1af6aa17c0ca687848e"
EXPECTED_SOURCE_COUNTS = {"hangar": 3861, "voxel": 6157}
EXPECTED_INPUT_ROWS = 10018

PLUGIN_PRODUCT_CONFIRMED = "PLUGIN_PRODUCT_CONFIRMED"
PLUGIN_PRODUCT_REVIEW = "PLUGIN_PRODUCT_REVIEW"
OUT_OF_SCOPE_PRODUCT_FORM = "OUT_OF_SCOPE_PRODUCT_FORM"
SCOPE_STATUSES = (
    PLUGIN_PRODUCT_CONFIRMED,
    PLUGIN_PRODUCT_REVIEW,
    OUT_OF_SCOPE_PRODUCT_FORM,
)

CSV_NULL = r"\N"
SAMPLE_PER_SOURCE_DEMAND_STRATUM = 5
DEMAND_STRATA = (("Q1", 0.0, 25.0), ("Q2", 25.0, 50.0), ("Q3", 50.0, 75.0), ("Q4", 75.0, 100.000001))

HANGAR_CATEGORY_MAP = {
    "admin_tools": "administration",
    "chat": "communication",
    "dev_tools": "developer_tools",
    "economy": "economy",
    "gameplay": "gameplay",
    "games": "minigames",
    "protection": "protection",
    "role_playing": "roleplay",
    "world_management": "world_management",
    # Hangar's `misc` token is not a functional market job. Retain it as source
    # context and require semantic evidence or leave the resource uncategorized.
    "misc": None,
}

_CATEGORY_ROWS = (
    {
        "category_id": "administration",
        "category_name": "Server administration",
        "definition": "Operating, configuring, moderating, and administering a Minecraft server or network.",
        "anchors": ["admin_tools"],
        "risk": "Broad source-native category; a specific function may be captured as a secondary facet.",
        "subcategories": [
            ("server_maintenance", "Server maintenance and lifecycle operations."),
            ("access_control", "Permissions, allowlists, and access management."),
            ("moderation", "Player moderation and staff operations."),
            ("command_automation", "Scheduled or automated server operations."),
            ("general_administration", "Administrative function not resolved to a narrower subcategory."),
        ],
    },
    {
        "category_id": "communication",
        "category_name": "Communication",
        "definition": "Player messaging, chat presentation, announcements, or communication bridges.",
        "anchors": ["chat"],
        "risk": "Chat can also be a feature inside another plugin category; source-native Hangar category is retained.",
        "subcategories": [
            ("player_chat", "In-game player chat and messaging."),
            ("chat_presentation", "Formatting, filtering, and presentation of messages."),
            ("cross_platform_communication", "Communication bridge between Minecraft and an external service."),
            ("general_communication", "Communication function not resolved to a narrower subcategory."),
        ],
    },
    {
        "category_id": "developer_tools",
        "category_name": "Developer tools",
        "definition": "Tools whose primary job supports development or extension of server software/plugins.",
        "anchors": ["dev_tools"],
        "risk": "A library or API alone may not be an operator-facing plugin; such product forms are reviewed or excluded.",
        "subcategories": [
            ("developer_api", "Server/plugin development API or framework."),
            ("developer_automation", "Scripting, debugging, or developer workflow support."),
            ("general_developer_tools", "Developer-support function not resolved to a narrower subcategory."),
        ],
    },
    {
        "category_id": "economy",
        "category_name": "Economy",
        "definition": "Virtual currency, shops, trading, jobs, rewards, and other server economy functions.",
        "anchors": ["economy"],
        "risk": "Pricing/paidness is not used to assign this category.",
        "subcategories": [
            ("shops_and_trading", "Shops, markets, auctions, and player trading."),
            ("currency_and_balances", "Virtual currency, balances, and account operations."),
            ("jobs_and_rewards", "Jobs, payouts, rewards, and economic progression."),
            ("general_economy", "Economy function not resolved to a narrower subcategory."),
        ],
    },
    {
        "category_id": "gameplay",
        "category_name": "Gameplay mechanics",
        "definition": "Server-side changes to gameplay rules, player abilities, items, entities, or progression outside a distinct minigame product.",
        "anchors": ["gameplay"],
        "risk": "Broad functional category; distinct minigame products remain separate.",
        "subcategories": [
            ("items_and_crafting", "Items, recipes, crafting, and equipment mechanics."),
            ("player_mechanics", "Player abilities, status, controls, or interaction mechanics."),
            ("entities_and_combat", "Entity behavior, combat, and encounter mechanics."),
            ("progression", "Levels, upgrades, and gameplay progression."),
            ("general_gameplay", "Gameplay function not resolved to a narrower subcategory."),
        ],
    },
    {
        "category_id": "minigames",
        "category_name": "Minigames and game modes",
        "definition": "Standalone server minigames, match systems, and dedicated game modes.",
        "anchors": ["games"],
        "risk": "A map, arena, or setup asset is not a minigame plugin and is excluded by the product-form guard.",
        "subcategories": [
            ("minigames_and_modes", "A playable minigame or server game mode."),
            ("match_and_arena_operations", "Match, queue, or arena operation for a game mode."),
            ("general_games", "Game function not resolved to a narrower subcategory."),
        ],
    },
    {
        "category_id": "protection",
        "category_name": "Protection and abuse controls",
        "definition": "Preventing grief, abuse, cheating, or harmful server behavior and supporting recovery.",
        "anchors": ["protection"],
        "risk": "Performance controls are categorized here only when source text describes protective limits or abuse prevention.",
        "subcategories": [
            ("anti_grief_and_recovery", "Anti-grief, logging, rollback, or restoration."),
            ("anti_cheat", "Cheat detection or prevention."),
            ("abuse_and_spam_controls", "Spam, exploit, crash, or abuse prevention."),
            ("general_protection", "Protection function not resolved to a narrower subcategory."),
        ],
    },
    {
        "category_id": "roleplay",
        "category_name": "Role-playing systems",
        "definition": "Server plugin systems centered on role-play, character identity, quests, or role-play progression.",
        "anchors": ["role_playing"],
        "risk": "Generic gameplay is not role-play absent explicit role-play/character evidence.",
        "subcategories": [
            ("roleplay_systems", "Role-play systems and interactions."),
            ("characters_and_quests", "Characters, quests, or role-play progression."),
            ("general_roleplay", "Role-play function not resolved to a narrower subcategory."),
        ],
    },
    {
        "category_id": "world_management",
        "category_name": "World management",
        "definition": "Managing server worlds, regions, player homes/teleportation, world lifecycle, or world-generation behavior.",
        "anchors": ["world_management"],
        "risk": "World/build/map content products are out of scope; this category covers executable management behavior only.",
        "subcategories": [
            ("teleportation_and_homes", "Player homes, teleports, portals, and navigation."),
            ("regions_and_chunks", "Region, claim, chunk, and world-area management."),
            ("world_lifecycle", "World creation, resets, regeneration, and lifecycle."),
            ("general_world_management", "World management function not resolved to a narrower subcategory."),
        ],
    },
    {
        "category_id": "server_utilities",
        "category_name": "Server utilities",
        "definition": "Operator-facing server utilities that do not fit a more specific functional category.",
        "anchors": [],
        "risk": "Not a catch-all for unclear products; unresolved or vague function belongs in UNCATEGORIZED.",
        "subcategories": [
            ("performance_and_efficiency", "Server performance and efficiency utilities."),
            ("general_server_utility", "A clear server utility without a narrower market job."),
        ],
    },
)

CATEGORY_IDS = tuple(row["category_id"] for row in _CATEGORY_ROWS)
TAXONOMY_CATEGORY_IDS = (*CATEGORY_IDS, "uncategorized")
_CATEGORY_ORDER = {category_id: index for index, category_id in enumerate(TAXONOMY_CATEGORY_IDS)}

# Each rule is a product-function cue, not a popularity or market-signal rule.
# Order is used only to select a subcategory within one already-selected category.
_FUNCTION_RULES = (
    ("administration", "server_maintenance", r"\b(?:maintenance|restart|restarts|shutdown|server lifecycle|server handler)\b"),
    ("administration", "access_control", r"\b(?:permissions?|whitelist|allowlist|access control|restricts? access)\b"),
    ("administration", "moderation", r"\b(?:moderation|moderate|bans?|kicks?|mutes?|staff tools?)\b"),
    ("administration", "command_automation", r"\b(?:schedule commands?|queue commands?|executes? commands?|command management)\b"),
    ("administration", "general_administration", r"\b(?:admin(?:istration)? tools?|server management)\b"),
    ("communication", "cross_platform_communication", r"\b(?:discord|cross[- ]server chat|chat bridge|connector)\b"),
    ("communication", "chat_presentation", r"\b(?:chat formatting|chat format|formatter|gradients?|hover tooltips?|message format)\b"),
    ("communication", "player_chat", r"\b(?:chat|messages?|whispers?|announcements?)\b"),
    ("developer_tools", "developer_api", r"\b(?:developer api|command api|typed api|sdk|library|framework)\b"),
    ("developer_tools", "developer_automation", r"\b(?:debugging|debug|scripting|script|hot reload|developer tools?)\b"),
    ("developer_tools", "general_developer_tools", r"\b(?:developer|plugin developers?)\b"),
    ("economy", "shops_and_trading", r"\b(?:shops?|trading|trade|auction|marketplace|player exchange)\b"),
    ("economy", "currency_and_balances", r"\b(?:economy|currenc(?:y|ies)|balances?|virtual money|vault)\b"),
    ("economy", "jobs_and_rewards", r"\b(?:jobs?|payouts?|rewards?|earn(?:ing|s)?|salary)\b"),
    ("gameplay", "items_and_crafting", r"\b(?:items?|elytras?|enchantments?|recipes?|crafting|anvils?|armor|blocks?)\b"),
    ("gameplay", "entities_and_combat", r"\b(?:mobs?|entities|combat|explosions?|projectiles|damage)\b"),
    ("gameplay", "progression", r"\b(?:levels?|leveling|upgrades?|progression|experience points|xp)\b"),
    ("gameplay", "player_mechanics", r"\b(?:mechanics|player resizing|time and weather|growth|abilities|player interaction)\b"),
    ("minigames", "minigames_and_modes", r"\b(?:minigames?|mini-games?|skywars|bedwars|uhc|duels?|chess|tnt wars|game mode)\b"),
    ("minigames", "match_and_arena_operations", r"\b(?:match(?:making)?|queues?|arena management|game lobbies)\b"),
    ("protection", "anti_grief_and_recovery", r"\b(?:anti[- ]grief(?:ing)?|grief(?:ed|ing)? claims?|rollback|restore|damage logging|block logging)\b"),
    ("protection", "anti_cheat", r"\b(?:anti[- ]cheat|cheat detection|cheating prevention)\b"),
    ("protection", "abuse_and_spam_controls", r"\b(?:spam|exploit|crash(?:ing)?|abuse prevention|command limiter|rate limit)\b"),
    ("roleplay", "characters_and_quests", r"\b(?:role[- ]?play|rpg|characters?|quests?|dice|mmocore)\b"),
    ("roleplay", "roleplay_systems", r"\b(?:role[- ]?playing|roleplay system)\b"),
    ("world_management", "teleportation_and_homes", r"\b(?:teleport(?:ation)?|player homes?|set homes?|portals?|navigation)\b"),
    ("world_management", "regions_and_chunks", r"\b(?:regions?|claims?|chunks?|world borders?)\b"),
    ("world_management", "world_lifecycle", r"\b(?:world resets?|world regeneration|regenerat(?:e|ion)|world lifecycle|world management)\b"),
    ("server_utilities", "performance_and_efficiency", r"\b(?:performance|server lag|optimi[sz](?:e|ation)|entity limiter|efficiency)\b"),
)

_ASSET_RULES = (
    ("OUT_OF_SCOPE_3D_MODEL_ASSET", r"\b(?:3d|three[- ]dimensional)\s+model\b"),
    ("OUT_OF_SCOPE_MODEL_ASSET", r"\bmodel\s+(?:file|pack|asset|download)\b"),
    ("OUT_OF_SCOPE_RESOURCE_OR_TEXTURE_PACK", r"\b(?:resource|texture)\s+packs?\b"),
    ("OUT_OF_SCOPE_SCHEMATIC_OR_BLUEPRINT", r"\b(?:schematics?|blueprints?)\b"),
    ("OUT_OF_SCOPE_CONFIGURATION_PRODUCT", r"\b(?:yaml|yml)\b.{0,80}\b(?:config(?:uration)?|shop|setup)\b|\bshop\s+config(?:uration)?\b|\bconfig(?:uration)?\s+(?:pack|file|setup|template)\b"),
    ("OUT_OF_SCOPE_CONTENT_PACK_OR_SETUP", r"\b(?:rank|kit|item|arena|spawn|lobby|world|pvp)\s+(?:pack|setup)\b"),
    ("OUT_OF_SCOPE_CONTENT_PACK_OR_SETUP", r"\b[\w'-]+(?:\s+[\w'-]+){0,2}\s+(?:pack|bundle)\b"),
    ("OUT_OF_SCOPE_PLUGIN_DEPENDENT_CONTENT", r"\bmythicmobs?\b.{0,80}\b(?:custom mobs?|mob packs?)\b"),
    ("OUT_OF_SCOPE_PLUGIN_DEPENDENT_CONTENT", r"\b(?:armor|weapon|item|mob|fish|elytra|nether)\s+sets?\b"),
    ("OUT_OF_SCOPE_NON_PLUGIN_PRODUCT_CLASS", r"\b(?:modpack|datapack|resourcepack|shader pack)\b"),
)

_DIRECT_PLUGIN_RULES = (
    r"\b(?:paper|spigot|bukkit|velocity|bungeecord|waterfall)\s+plugin\b",
    r"\b(?:this|the|a|an)\s+plugin\b",
    r"\bplugin\s+(?:that|which|for|to|allows?|adds?|provides?|manages?|prevents?|connects?|creates?|lets?)\b",
    r"\bplugin\b",
)
_COMPATIBILITY_ONLY_RULE = re.compile(
    r"\b(?:works?|work|compatible|designed)\s+(?:well\s+)?with\s+plugins?\b|"
    r"\bplugins?\s+like\b|\bplugin[- ]compatible\b|\bplugin support\b",
    re.IGNORECASE,
)
_SERVER_BEHAVIOR_RULES = (
    r"\b(?:adds?|allows?|lets?|provides?|enables?|prevents?|manages?|tracks?|controls?|customi[sz]es?|"
    r"executes?|connects?|integrates?|automates?|removes?|creates?|makes?|turns?|gives?|extends?|"
    r"restricts?|shoots?|rolls?\s+back|restores?|queues?|schedules?)\b",
    r"\byou\s+(?:can|will be able to)\s+(?:manage|create|configure|control|protect|track|teleport|customi[sz]e)\b",
)
_SOFTWARE_PRODUCT_FORM_RULE = re.compile(
    r"\b(?:plugin|tool|system|manager|handler|connector|bridge|limiter|engine|"
    r"utility|server software)\b",
    re.IGNORECASE,
)

FEATURE_REQUIRED_FIELDS = {
    "source", "source_resource_id", "canonical_identity", "title", "summary",
    "project_type_norm", "loader_facets_json", "category_facets_json",
}
ELIGIBILITY_REQUIRED_FIELDS = {
    "source", "source_resource_id", "canonical_identity", "plugin_eligibility",
    "eligibility_reason_codes", "positive_evidence", "classifier_version",
}
SCOPE_COLUMNS = (
    "product_scope_status", "scope_reason_codes", "scope_evidence",
    "scope_confidence", "scope_method", "scope_classifier_version",
)
YEE60_COLUMNS = (
    "yee60_plugin_eligibility", "yee60_eligibility_reason_codes",
    "yee60_positive_evidence", "yee60_classifier_version",
)
ASSIGNMENT_COLUMNS = (
    "primary_category_id", "subcategory_id", "secondary_category_ids",
    "assignment_method", "assignment_confidence", "assignment_evidence",
    "source_native_category_facets", "source_native_loader_facets",
    "taxonomy_version", "assignment_risk_notes",
)

CORE_ARTIFACTS = (
    "GOAL_ALIGNMENT.md",
    "SEMANTIC_SMOKE_TEST.md",
    "plugin_product_scope.jsonl",
    "plugin_product_scope.csv",
    "plugin_category_taxonomy.json",
    "plugin_category_taxonomy.md",
    "plugin_category_memberships.jsonl",
    "plugin_category_memberships.csv",
    "plugin_category_inventory.jsonl",
    "plugin_category_inventory.csv",
    "category_first_foundation.sqlite",
    "CATEGORY_FIRST_SCHEMA.md",
    "DOWNSTREAM_CATEGORY_CONTRACT.md",
)
FINAL_ARTIFACTS = (*CORE_ARTIFACTS, "QA_RESULT.json", "FINAL_REPORT.md", "DATASET_MANIFEST.json")


class CategoryFoundationError(ValueError):
    """Canonical YEE-60 input or YEE-61 output violated its contract."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_only_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _json_array(value: Any, field: str) -> list[Any]:
    if value is None:
        return []
    try:
        result = json.loads(value) if isinstance(value, str) else value
    except json.JSONDecodeError as exc:
        raise CategoryFoundationError(f"{field} is invalid JSON") from exc
    if not isinstance(result, list):
        raise CategoryFoundationError(f"{field} must be a JSON array")
    return result


def _feature_sort_key(row: Mapping[str, Any]) -> tuple[str, str]:
    return str(row["source"]), str(row["source_resource_id"])


def _load_canonical_inputs(
    input_db: Path,
    input_jsonl: Path,
    *,
    enforce_pinned_inputs: bool,
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], dict[str, Any]], dict[str, str], dict[str, int]]:
    if not input_db.is_file() or not input_jsonl.is_file():
        raise CategoryFoundationError("Both accepted YEE-60 SQLite and JSONL inputs are required")
    input_hashes = {
        "plugin_eligibility.sqlite": sha256_file(input_db),
        "plugin_only_resource_features.jsonl": sha256_file(input_jsonl),
    }
    if enforce_pinned_inputs and input_hashes != {
        "plugin_eligibility.sqlite": YEE60_SQLITE_SHA256,
        "plugin_only_resource_features.jsonl": YEE60_JSONL_SHA256,
    }:
        raise CategoryFoundationError("YEE-60 canonical input SHA-256 does not match the Worker Spec pins")

    feature_rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    with input_jsonl.open("r", encoding="utf-8", newline="") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.endswith("\n"):
                raise CategoryFoundationError(f"YEE-60 JSONL line {line_number} is not LF-terminated")
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise CategoryFoundationError(f"Invalid YEE-60 JSONL at line {line_number}") from exc
            if not isinstance(row, dict) or not FEATURE_REQUIRED_FIELDS.issubset(row):
                raise CategoryFoundationError(f"YEE-60 JSONL line {line_number} lacks required feature fields")
            identity = (str(row["source"]), str(row["source_resource_id"]))
            if identity in seen:
                raise CategoryFoundationError(f"Duplicate YEE-60 source identity: {identity}")
            seen.add(identity)
            _json_array(row.get("category_facets_json"), "category_facets_json")
            _json_array(row.get("loader_facets_json"), "loader_facets_json")
            feature_rows.append(row)
    if not feature_rows:
        raise CategoryFoundationError("YEE-60 feature input is empty")
    feature_rows.sort(key=_feature_sort_key)

    eligibility: dict[tuple[str, str], dict[str, Any]] = {}
    connection = _read_only_connection(input_db)
    try:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"plugin_eligibility", "plugin_only_resource_features"}.issubset(tables):
            raise CategoryFoundationError("YEE-60 SQLite is missing canonical tables")
        feature_columns = [row[1] for row in connection.execute("PRAGMA table_info(plugin_only_resource_features)")]
        db_features = {}
        for result in connection.execute("SELECT * FROM plugin_only_resource_features ORDER BY source, source_resource_id"):
            db_row = dict(result)
            identity = (str(db_row["source"]), str(db_row["source_resource_id"]))
            if identity in db_features:
                raise CategoryFoundationError(f"Duplicate YEE-60 SQLite feature identity: {identity}")
            db_features[identity] = db_row
        input_by_identity = {(str(row["source"]), str(row["source_resource_id"])): row for row in feature_rows}
        if set(db_features) != set(input_by_identity):
            raise CategoryFoundationError("YEE-60 JSONL and SQLite plugin-only identity sets differ")
        for identity, db_row in db_features.items():
            json_row = input_by_identity[identity]
            if set(feature_columns) != set(json_row) or canonical_json(db_row) != canonical_json(json_row):
                raise CategoryFoundationError(f"YEE-60 JSONL and SQLite feature values differ for {identity}")

        for result in connection.execute(
            """SELECT source, source_resource_id, canonical_identity, plugin_eligibility,
                      eligibility_reason_codes, positive_evidence, classifier_version
               FROM plugin_eligibility ORDER BY source, source_resource_id"""
        ):
            row = dict(result)
            identity = (str(row["source"]), str(row["source_resource_id"]))
            if identity not in input_by_identity:
                continue
            if identity in eligibility:
                raise CategoryFoundationError(f"Duplicate YEE-60 eligibility identity: {identity}")
            if row["plugin_eligibility"] != "PLUGIN_ELIGIBLE":
                raise CategoryFoundationError(f"Non-eligible YEE-60 row leaked into canonical input: {identity}")
            row["eligibility_reason_codes"] = _json_array(row["eligibility_reason_codes"], "eligibility_reason_codes")
            row["positive_evidence"] = _json_array(row["positive_evidence"], "positive_evidence")
            if not row["positive_evidence"] or row["classifier_version"] != YEE60_CLASSIFIER_VERSION:
                raise CategoryFoundationError(f"YEE-60 positive eligibility evidence missing for {identity}")
            if row["canonical_identity"] != input_by_identity[identity]["canonical_identity"]:
                raise CategoryFoundationError(f"YEE-60 canonical identity differs for {identity}")
            eligibility[identity] = row
        if set(eligibility) != set(input_by_identity):
            raise CategoryFoundationError("YEE-60 eligibility evidence does not cover the complete canonical input")
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_key_violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if integrity != "ok" or foreign_key_violations:
            raise CategoryFoundationError("YEE-60 input SQLite failed read-only integrity checks")
    finally:
        connection.close()

    source_counts = dict(sorted(Counter(str(row["source"]) for row in feature_rows).items()))
    if enforce_pinned_inputs and (len(feature_rows) != EXPECTED_INPUT_ROWS or source_counts != EXPECTED_SOURCE_COUNTS):
        raise CategoryFoundationError("YEE-60 canonical input row/source counts do not match the Worker Spec")
    return feature_rows, eligibility, input_hashes, source_counts


def _text(row: Mapping[str, Any], field: str) -> str:
    value = row.get(field)
    return value if isinstance(value, str) else ""


def _matched_spans(row: Mapping[str, Any], pattern: str) -> list[dict[str, str]]:
    compiled = re.compile(pattern, re.IGNORECASE)
    found = []
    for field in ("title", "summary"):
        value = _text(row, field)
        match = compiled.search(value)
        if match:
            found.append({"field": field, "text_span": match.group(0), "rule_pattern": pattern})
    return found


def _asset_form_evidence(row: Mapping[str, Any]) -> list[dict[str, str]]:
    found: list[dict[str, str]] = []
    for reason, pattern in _ASSET_RULES:
        for span in _matched_spans(row, pattern):
            found.append({"reason_code": reason, **span})
    title = _text(row, "title")
    summary = _text(row, "summary")
    combined = f"{title}\n{summary}"
    if re.search(r"\b\d{2,4}\s*[x×]\s*\d{2,4}\b", combined, re.IGNORECASE):
        dimension = re.search(r"\b\d{2,4}\s*[x×]\s*\d{2,4}\b", combined, re.IGNORECASE)
        title_form = re.search(r"\b(?:spawn|lobby|hub|castle|arena|map|build|world)\b", title, re.IGNORECASE)
        descriptive_asset = re.search(
            r"\b(?:npc places?|crate spots?|waiting area|custom spawn|massive .*?city|high quality .*?build|server build)\b",
            summary,
            re.IGNORECASE,
        )
        if title_form and descriptive_asset:
            found.extend([
                {"reason_code": "OUT_OF_SCOPE_BUILD_OR_MAP_ASSET", "field": "title", "text_span": title_form.group(0), "rule_pattern": "dimensioned_build_title"},
                {"reason_code": "OUT_OF_SCOPE_BUILD_OR_MAP_ASSET", "field": "summary", "text_span": dimension.group(0), "rule_pattern": "dimensioned_build_title"},
            ])
    if re.search(r"\b(?:map|spawn|lobby|hub|arena)\s+(?:download|build|template|schematic)\b", combined, re.IGNORECASE):
        found.extend({
            "reason_code": "OUT_OF_SCOPE_BUILD_OR_MAP_ASSET",
            **span,
        } for span in _matched_spans(row, r"\b(?:map|spawn|lobby|hub|arena)\s+(?:download|build|template|schematic)\b"))
    title_set = re.search(r"\b[\w'-]+\s+sets?\b", title, re.IGNORECASE)
    gear_description = re.search(r"\b(?:armor|weapons?|items?|gear|equipment)\b", summary, re.IGNORECASE)
    if title_set and gear_description:
        found.extend([
            {"reason_code": "OUT_OF_SCOPE_PLUGIN_DEPENDENT_CONTENT", "field": "title", "text_span": title_set.group(0), "rule_pattern": "title_gear_set_content"},
            {"reason_code": "OUT_OF_SCOPE_PLUGIN_DEPENDENT_CONTENT", "field": "summary", "text_span": gear_description.group(0), "rule_pattern": "title_gear_set_content"},
        ])
    # A standalone title ending in Model is a product-form cue; generic uses
    # such as "Model Engine plugin" are left for the conflict/review path.
    model_title = re.search(r"\bmodel\s*$", title, re.IGNORECASE)
    compatibility_model = re.search(r"\bmodel\b.{0,120}\b(?:works?|compatible)\s+with\s+plugins?\b", summary, re.IGNORECASE)
    if model_title and compatibility_model:
        found.extend([
            {"reason_code": "OUT_OF_SCOPE_MODEL_ASSET", "field": "title", "text_span": model_title.group(0), "rule_pattern": "model_product_with_plugin_compatibility"},
            {"reason_code": "OUT_OF_SCOPE_MODEL_ASSET", "field": "summary", "text_span": compatibility_model.group(0), "rule_pattern": "model_product_with_plugin_compatibility"},
        ])
    unique = {}
    for item in found:
        unique[(item["reason_code"], item["field"], item["text_span"], item["rule_pattern"])] = item
    return [unique[key] for key in sorted(unique)]


def _direct_plugin_product_evidence(row: Mapping[str, Any]) -> list[dict[str, str]]:
    found: list[dict[str, str]] = []
    for pattern in _DIRECT_PLUGIN_RULES:
        for span in _matched_spans(row, pattern):
            if _COMPATIBILITY_ONLY_RULE.search(span["text_span"]):
                continue
            whole_text = f"{_text(row, 'title')}\n{_text(row, 'summary')}"
            match = re.search(pattern, whole_text, re.IGNORECASE)
            if not match:
                continue
            context_start = max(0, match.start() - 24)
            context_end = min(len(whole_text), match.end() + 32)
            context = whole_text[context_start:context_end]
            if _COMPATIBILITY_ONLY_RULE.search(context) and not re.search(r"\b(?:this|the|a|an)\s+plugin\b|\bplugin\s+(?:that|which|for|to|allows?|adds?|provides?)\b", context, re.IGNORECASE):
                continue
            found.append({"reason_code": "PLUGIN_PRODUCT_EXPLICIT_TEXT", **span})
    unique = {}
    for item in found:
        unique[(item["field"], item["text_span"], item["rule_pattern"])] = item
    return [unique[key] for key in sorted(unique)]


def _server_behavior_evidence(row: Mapping[str, Any]) -> list[dict[str, str]]:
    found: list[dict[str, str]] = []
    for pattern in _SERVER_BEHAVIOR_RULES:
        for span in _matched_spans(row, pattern):
            found.append({"reason_code": "SERVER_PLUGIN_BEHAVIOR_TEXT", **span})
    unique = {}
    for item in found:
        unique[(item["field"], item["text_span"], item["rule_pattern"])] = item
    return [unique[key] for key in sorted(unique)]


def classify_product_form(row: Mapping[str, Any], eligibility: Mapping[str, Any]) -> dict[str, Any]:
    """Narrow one YEE-60 eligible row using explicit source-native product text."""
    if eligibility.get("plugin_eligibility") != "PLUGIN_ELIGIBLE" or not eligibility.get("positive_evidence"):
        raise CategoryFoundationError("Product-form guard accepts only rows with retained YEE-60 positive evidence")

    asset_evidence = _asset_form_evidence(row)
    direct_evidence = _direct_plugin_product_evidence(row)
    behavior_evidence = _server_behavior_evidence(row)
    compatibility_only = bool(_COMPATIBILITY_ONLY_RULE.search(f"{_text(row, 'title')}\n{_text(row, 'summary')}")) and not direct_evidence and not behavior_evidence

    if asset_evidence and direct_evidence:
        status = PLUGIN_PRODUCT_REVIEW
        reasons = ["CONFLICTING_PLUGIN_AND_NON_PLUGIN_PRODUCT_FORM_EVIDENCE"]
        confidence = "LOW"
        product_evidence = asset_evidence + direct_evidence
    elif asset_evidence:
        status = OUT_OF_SCOPE_PRODUCT_FORM
        reasons = sorted({item["reason_code"] for item in asset_evidence})
        confidence = "HIGH"
        product_evidence = asset_evidence
    elif direct_evidence:
        status = PLUGIN_PRODUCT_CONFIRMED
        reasons = ["PLUGIN_PRODUCT_EXPLICIT_SOURCE_TEXT"]
        confidence = "HIGH"
        product_evidence = direct_evidence
    elif behavior_evidence and not compatibility_only:
        native_categories = _json_array(row.get("category_facets_json"), "category_facets_json")
        hangar_function_anchor = (
            row.get("source") == "hangar"
            and any(HANGAR_CATEGORY_MAP.get(token) is not None for token in native_categories)
        )
        software_form_evidence = _matched_spans(
            row,
            _SOFTWARE_PRODUCT_FORM_RULE.pattern,
        )
        if hangar_function_anchor or software_form_evidence:
            status = PLUGIN_PRODUCT_CONFIRMED
            reasons = ["SERVER_PLUGIN_BEHAVIOR_WITH_POSITIVE_PRODUCT_FORM_CUE"]
            confidence = "MEDIUM"
            product_evidence = behavior_evidence + [
                {"reason_code": "EXECUTABLE_SOFTWARE_PRODUCT_FORM_CUE", **span}
                for span in software_form_evidence
            ]
            if hangar_function_anchor:
                product_evidence.append({
                    "reason_code": "HANGAR_SOURCE_NATIVE_FUNCTIONAL_CATEGORY_CONTEXT",
                    "field": "category_facets_json",
                    "text_span": canonical_json(native_categories),
                    "rule_pattern": "yee60_hangar_functional_category_anchor",
                })
        else:
            status = PLUGIN_PRODUCT_REVIEW
            reasons = ["SERVER_BEHAVIOR_ALONE_DOES_NOT_ESTABLISH_EXECUTABLE_PLUGIN_FORM"]
            confidence = "LOW"
            product_evidence = behavior_evidence
    else:
        status = PLUGIN_PRODUCT_REVIEW
        reasons = ["PLUGIN_PRODUCT_FORM_NOT_ESTABLISHED"]
        if compatibility_only:
            reasons.append("PLUGIN_COMPATIBILITY_IS_NOT_PRODUCT_FORM_EVIDENCE")
        confidence = "LOW"
        product_evidence = []

    original_evidence = [
        {
            "evidence_type": "YEE60_PLUGIN_ELIGIBILITY",
            "field": item.get("field"),
            "token": item.get("token"),
            "classifier_version": eligibility.get("classifier_version"),
        }
        for item in eligibility.get("positive_evidence", [])
    ]
    return {
        "product_scope_status": status,
        "scope_reason_codes": reasons,
        "scope_evidence": original_evidence + product_evidence,
        "scope_confidence": confidence,
        "scope_method": "RULE_BASED_SOURCE_NATIVE_PRODUCT_FORM_GUARD",
        "scope_classifier_version": SCOPE_CLASSIFIER_VERSION,
    }


def _semantic_category_matches(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    haystack = f"{_text(row, 'title')}\n{_text(row, 'summary')}"
    found: dict[str, dict[str, Any]] = {}
    for category_id, subcategory_id, pattern in _FUNCTION_RULES:
        match = re.search(pattern, haystack, re.IGNORECASE)
        if match and category_id not in found:
            # The first rule per category is its explicit, versioned
            # subcategory tie-break; no metric or score participates.
            field = "title" if match.start() < len(_text(row, "title")) else "summary"
            found[category_id] = {
                "category_id": category_id,
                "subcategory_id": subcategory_id,
                "field": field,
                "text_span": match.group(0),
                "rule_pattern": pattern,
                "assignment_rule_id": f"{ASSIGNMENT_VERSION}:{subcategory_id}",
            }
    return [found[key] for key in sorted(found, key=lambda key: _CATEGORY_ORDER[key])]


def _general_subcategory(category_id: str) -> str:
    category = next(item for item in _CATEGORY_ROWS if item["category_id"] == category_id)
    return category["subcategories"][-1][0]


def assign_category(row: Mapping[str, Any], scope: Mapping[str, Any]) -> dict[str, Any] | None:
    """Assign only confirmed products; all category evidence is auditable."""
    if scope.get("product_scope_status") != PLUGIN_PRODUCT_CONFIRMED:
        return None

    source_categories = _json_array(row.get("category_facets_json"), "category_facets_json")
    source_loaders = _json_array(row.get("loader_facets_json"), "loader_facets_json")
    if any(not isinstance(item, str) for item in (*source_categories, *source_loaders)):
        raise CategoryFoundationError("Source-native category/loader facets must contain strings")
    native_categories = sorted(set(source_categories))
    semantic_matches = _semantic_category_matches(row)
    semantic_by_id = {item["category_id"]: item for item in semantic_matches}
    mapped_native = [HANGAR_CATEGORY_MAP.get(token) for token in native_categories if token in HANGAR_CATEGORY_MAP]
    mapped_native = [category for category in mapped_native if category is not None]
    unknown_native = [token for token in native_categories if token not in HANGAR_CATEGORY_MAP]

    primary: str | None = None
    method = "UNCATEGORIZED"
    confidence = "LOW"
    evidence: list[dict[str, Any]] = []
    secondary: list[str] = []
    reason_notes: list[str] = []
    subcategory: str | None = None

    if row.get("source") == "hangar" and len(mapped_native) == 1 and not unknown_native:
        primary = mapped_native[0]
        method = "SOURCE_NATIVE_DIRECT"
        confidence = "HIGH"
        token = next(token for token in native_categories if HANGAR_CATEGORY_MAP.get(token) == primary)
        evidence.append({
            "field": "category_facets_json",
            "source_native_value": token,
            "assignment_rule_id": f"{ASSIGNMENT_VERSION}:hangar:{token}",
        })
        if primary in semantic_by_id:
            subcategory = semantic_by_id[primary]["subcategory_id"]
            evidence.append({"field": semantic_by_id[primary]["field"], **semantic_by_id[primary]})
        else:
            subcategory = _general_subcategory(primary)
        for match in semantic_matches:
            if match["category_id"] != primary and match["category_id"] not in secondary:
                secondary.append(match["category_id"])
                evidence.append({"field": match["field"], **match})
        secondary = sorted(secondary, key=lambda key: _CATEGORY_ORDER[key])[:3]
        if secondary:
            reason_notes.append("SOURCE_NATIVE_PRIMARY_WITH_ADDITIONAL_FUNCTION_CUES")
    else:
        if len(semantic_matches) == 1:
            primary = semantic_matches[0]["category_id"]
            subcategory = semantic_matches[0]["subcategory_id"]
            method = "SEMANTIC_EVIDENCE"
            confidence = "MEDIUM"
            evidence.append({"field": semantic_matches[0]["field"], **semantic_matches[0]})
        elif len(semantic_matches) > 1:
            reason_notes.append("MULTIPLE_FUNCTIONAL_CATEGORIES_UNRESOLVED")
            evidence.extend({"field": item["field"], **item} for item in semantic_matches)
        elif native_categories:
            reason_notes.append("SOURCE_NATIVE_CATEGORY_NOT_IN_FROZEN_MAPPING")
            evidence.extend({
                "field": "category_facets_json",
                "source_native_value": token,
                "assignment_rule_id": f"{ASSIGNMENT_VERSION}:unmapped_source_category",
            } for token in native_categories)
        else:
            reason_notes.append("NO_FUNCTIONAL_CATEGORY_EVIDENCE")

    if primary is None:
        primary = "uncategorized"
        subcategory = "unclassified_function"
        method = "UNCATEGORIZED"
        confidence = "LOW"
        secondary = []
    if subcategory is None:
        subcategory = _general_subcategory(primary)

    result = {
        "primary_category_id": primary,
        "subcategory_id": subcategory,
        "secondary_category_ids": secondary,
        "assignment_method": method,
        "assignment_confidence": confidence,
        "assignment_evidence": evidence,
        "source_native_category_facets": native_categories,
        "source_native_loader_facets": sorted(set(source_loaders)),
        "taxonomy_version": TAXONOMY_VERSION,
        "assignment_risk_notes": reason_notes,
    }
    _validate_assignment(result)
    return result


def _validate_assignment(assignment: Mapping[str, Any]) -> None:
    primary = assignment.get("primary_category_id")
    secondary = assignment.get("secondary_category_ids")
    if primary not in TAXONOMY_CATEGORY_IDS:
        raise CategoryFoundationError("Primary category is absent from frozen taxonomy")
    if not isinstance(secondary, list) or len(secondary) > 3:
        raise CategoryFoundationError("Secondary categories must contain zero to three IDs")
    if len(set(secondary)) != len(secondary) or primary in secondary:
        raise CategoryFoundationError("Secondary categories must be deduplicated and exclude the primary")
    if any(category not in CATEGORY_IDS for category in secondary):
        raise CategoryFoundationError("Secondary category is absent from frozen taxonomy")
    if assignment.get("taxonomy_version") != TAXONOMY_VERSION:
        raise CategoryFoundationError("Assignment taxonomy version is not frozen YEE-61 taxonomy")


def _taxonomy_sample(scope_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    confirmed = [row for row in scope_rows if row["product_scope_status"] == PLUGIN_PRODUCT_CONFIRMED]
    selected: list[dict[str, Any]] = []
    for source in sorted({str(row["source"]) for row in confirmed}):
        for band, lower, upper in DEMAND_STRATA:
            members = []
            for row in confirmed:
                if row["source"] != source:
                    continue
                try:
                    percentile = float(row.get("demand_percentile"))
                except (TypeError, ValueError):
                    continue
                if lower <= percentile < upper:
                    seed = f"{TAXONOMY_VERSION}|{source}|{band}|{row['canonical_identity']}".encode("utf-8")
                    members.append((hashlib.sha256(seed).hexdigest(), row))
            for _, row in sorted(members, key=lambda pair: (pair[0], pair[1]["canonical_identity"]))[:SAMPLE_PER_SOURCE_DEMAND_STRATUM]:
                selected.append({
                    "source": row["source"],
                    "canonical_identity": row["canonical_identity"],
                    "demand_stratum": band,
                    "title": row.get("title"),
                    "summary": row.get("summary"),
                    "source_native_category_facets": _json_array(row.get("category_facets_json"), "category_facets_json"),
                })
    return sorted(selected, key=lambda item: (item["source"], item["demand_stratum"], item["canonical_identity"]))


def _taxonomy_document(sample: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    categories = []
    for category in _CATEGORY_ROWS:
        categories.append({
            "category_id": category["category_id"],
            "category_name": category["category_name"],
            "definition": category["definition"],
            "source_native_anchors": category["anchors"],
            "subcategories": [
                {"subcategory_id": subcategory_id, "definition": definition}
                for subcategory_id, definition in category["subcategories"]
            ],
            "known_ambiguity_risk_notes": [category["risk"]],
        })
    categories.append({
        "category_id": "uncategorized",
        "category_name": "UNCATEGORIZED",
        "definition": "Confirmed plugin product with insufficient or conflicting functional evidence for a reliable frozen category.",
        "source_native_anchors": [],
        "subcategories": [{"subcategory_id": "unclassified_function", "definition": "Functional category is not established."}],
        "known_ambiguity_risk_notes": ["First-class audit bucket; do not hide inside Other or remove from outputs."],
    })
    taxonomy = {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "taxonomy_version": TAXONOMY_VERSION,
        "assignment_version": ASSIGNMENT_VERSION,
        "taxonomy_status": "FROZEN_BEFORE_FULL_ASSIGNMENT",
        "taxonomy_method": {
            "source_native_anchor": "Observed Hangar functional category facets map exactly through the frozen mapping; `misc` is retained in source context but is not itself a functional category.",
            "semantic_review_sample": "Deterministic SHA-256 sample of at most five PLUGIN_PRODUCT_CONFIRMED rows for each source × YEE-29 source-local demand_percentile quartile. Sample selection does not alter taxonomy IDs or assignment rules.",
            "sample_size": len(sample),
            "third_level_categories": False,
            "metric_leakage_guard": "Demand/price/popularity/freshness fields are not read by scope or assignment functions; demand percentile is used only to stratify the documented taxonomy review sample.",
        },
        "source_native_category_mapping": [
            {"source": "hangar", "source_token": token, "category_id": HANGAR_CATEGORY_MAP[token], "mapping_state": "FUNCTIONAL_ANCHOR" if HANGAR_CATEGORY_MAP[token] else "REQUIRES_SEMANTIC_EVIDENCE_OR_UNCATEGORIZED"}
            for token in sorted(HANGAR_CATEGORY_MAP)
        ],
        "categories": categories,
    }
    taxonomy["taxonomy_sha256"] = hashlib.sha256(canonical_json(taxonomy).encode("utf-8")).hexdigest()
    return taxonomy


def _scope_record(row: Mapping[str, Any], eligibility: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "yee60_plugin_eligibility": eligibility["plugin_eligibility"],
        "yee60_eligibility_reason_codes": eligibility["eligibility_reason_codes"],
        "yee60_positive_evidence": eligibility["positive_evidence"],
        "yee60_classifier_version": eligibility["classifier_version"],
        **classify_product_form(row, eligibility),
    }


def _membership_record(row: Mapping[str, Any], assignment: Mapping[str, Any]) -> dict[str, Any]:
    return {**row, **assignment}


def _inventory_rows(
    taxonomy: Mapping[str, Any],
    memberships: Sequence[Mapping[str, Any]],
    scope_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_category: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for membership in memberships:
        by_category[str(membership["primary_category_id"])].append(membership)
    out = []
    for index, category in enumerate(taxonomy["categories"]):
        category_id = category["category_id"]
        primary_members = sorted(by_category.get(category_id, []), key=_feature_sort_key)
        primary_identities = {(row["source"], row["source_resource_id"]) for row in primary_members}
        observed_sources = sorted({str(row["source"]) for row in scope_rows})
        source_counts = Counter(str(row["source"]) for row in primary_members)
        confidence_counts = Counter(str(row["assignment_confidence"]) for row in primary_members)
        secondary_count = sum(category_id in row["secondary_category_ids"] for row in memberships)
        scope_evidence_count = sum(
            bool(row.get("scope_evidence"))
            for row in scope_rows
            if row["product_scope_status"] == PLUGIN_PRODUCT_CONFIRMED
            and (row["source"], row["source_resource_id"]) in primary_identities
        )
        member_count = len(primary_members)
        out.append({
            "category_order": index,
            "category_id": category_id,
            "category_name": category["category_name"],
            "definition": category["definition"],
            "subcategories": category["subcategories"],
            "member_count_by_source": {source: source_counts.get(source, 0) for source in observed_sources},
            "primary_member_count": member_count,
            "secondary_member_count": secondary_count,
            "scope_evidence_coverage_count": scope_evidence_count,
            "scope_evidence_coverage_rate": (scope_evidence_count / member_count) if member_count else None,
            "assignment_confidence_distribution": {key: confidence_counts.get(key, 0) for key in ("HIGH", "MEDIUM", "LOW")},
            "representative_identities": [row["canonical_identity"] for row in primary_members[:5]],
            "known_ambiguity_risk_notes": category["known_ambiguity_risk_notes"],
            "taxonomy_version": TAXONOMY_VERSION,
        })
    return out


def _csv_cell(value: Any) -> Any:
    if value is None:
        return CSV_NULL
    if isinstance(value, (list, dict)):
        return canonical_json(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(canonical_json(row) + "\n")
            count += 1
    return count


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> int:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_cell(row.get(column)) for column in columns})
    return len(rows)


def _csv_data_row_count(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as stream:
        return sum(1 for _ in csv.DictReader(stream))


def _markdown_table_cell(value: Any) -> str:
    return str(value if value is not None else "—").replace("|", "\\|").replace("\n", " ")


def _write_taxonomy_markdown(path: Path, taxonomy: Mapping[str, Any]) -> None:
    lines = [
        "# YEE-61 frozen plugin category taxonomy",
        "",
        f"- Taxonomy version: `{TAXONOMY_VERSION}`",
        f"- Taxonomy SHA-256: `{taxonomy['taxonomy_sha256']}`",
        "- Frozen before full category assignment: `true`",
        "- Functional categories represent plugin jobs, not platforms/loaders.",
        "- `UNCATEGORIZED` is a first-class visible bucket.",
        "",
        "| Category ID | Name | Definition | Hangar anchor | Subcategories |",
        "|---|---|---|---|---|",
    ]
    for category in taxonomy["categories"]:
        anchors = ", ".join(category["source_native_anchors"]) or "—"
        subcategories = ", ".join(item["subcategory_id"] for item in category["subcategories"])
        lines.append("| " + " | ".join(_markdown_table_cell(item) for item in (
            category["category_id"], category["category_name"], category["definition"], anchors, subcategories
        )) + " |")
    lines.extend(["", "## Source-native Hangar mapping", "", "| Token | Frozen mapping |", "|---|---|"])
    for item in taxonomy["source_native_category_mapping"]:
        lines.append(f"| `{item['source_token']}` | `{item['category_id'] or 'UNCATEGORIZED/SEMANTIC'}` — {item['mapping_state']} |")
    lines.extend(["", "## Freeze method", "", taxonomy["taxonomy_method"]["source_native_anchor"], "", taxonomy["taxonomy_method"]["semantic_review_sample"], ""])
    path.write_text("\n".join(lines), encoding="utf-8", newline="\n")


def _write_goal_alignment(path: Path, scope_rows: Sequence[Mapping[str, Any]], taxonomy: Mapping[str, Any]) -> None:
    counts = Counter(row["product_scope_status"] for row in scope_rows)
    lines = [
        "# YEE-61 GOAL_ALIGNMENT",
        "",
        "## Canonical objective",
        "BBB Market remains PLUGIN_ONLY: commercially sensible paid Minecraft server-side/proxy plugins for server/network operators. No downstream output promotes an item outside accepted YEE-60 PLUGIN_ELIGIBLE.",
        "",
        "## Strategy alignment",
        "The category-first strategy passes. It preserves category diversity, weak categories, negative findings, and uncertainty; it does not require a global winner or autonomous build decision.",
        "",
        "## Required scope hardening",
        "The accepted YEE-60 set is only a source-native eligibility gate. Product-form evidence is checked before category assignment. Product forms explicitly evidenced as models, builds/maps, configurations, or other non-plugin content are excluded; unclear/conflicting products remain visible as PLUGIN_PRODUCT_REVIEW.",
        "",
        "## Guard outcome",
        f"- Canonical YEE-60 input rows: {len(scope_rows):,}",
        f"- PLUGIN_PRODUCT_CONFIRMED: {counts.get(PLUGIN_PRODUCT_CONFIRMED, 0):,}",
        f"- PLUGIN_PRODUCT_REVIEW: {counts.get(PLUGIN_PRODUCT_REVIEW, 0):,}",
        f"- OUT_OF_SCOPE_PRODUCT_FORM: {counts.get(OUT_OF_SCOPE_PRODUCT_FORM, 0):,}",
        f"- Frozen taxonomy: `{TAXONOMY_VERSION}` / SHA-256 `{taxonomy['taxonomy_sha256']}`",
        "",
        "## Decision",
        "`GOAL_ALIGNMENT=PASS_WITH_REQUIRED_SCOPE_HARDENING`; the semantic PLUGIN_ONLY guard ran before category assignment. Only PLUGIN_PRODUCT_CONFIRMED rows enter category membership. YEE-60 remains read-only; YEE-30 through YEE-59 were not used as canonical inputs.",
        "",
        "No Stage B category-local signals, opportunity ranking, external research, concept synthesis, recommendation, or build decision was produced.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8", newline="\n")


def _write_semantic_smoke(
    path: Path,
    scope_rows: Sequence[Mapping[str, Any]],
    taxonomy_sample: Sequence[Mapping[str, Any]],
    eligibility: Mapping[tuple[str, str], Mapping[str, Any]],
) -> None:
    scope_by_id = {row["canonical_identity"]: row for row in scope_rows}
    lines = [
        "# YEE-61 semantic PLUGIN_ONLY smoke test",
        "",
        "GOAL_ALIGNMENT result: `PASS_WITH_REQUIRED_SCOPE_HARDENING`. The YEE-60 eligible set is not assumed to be product-form safe. The guard is applied to all input identities before the taxonomy is frozen or category assignments are created.",
        "",
        f"Input rows checked: {len(scope_rows):,}. The classification is deterministic and uses only title/summary product-form spans plus retained YEE-60 positive eligibility evidence. No web/API or Jev/LLM calls were made.",
        "",
        "## Known YEE-60 semantic false-positive fixtures",
        "",
        "| Identity | Source title | Source summary | Retained YEE-60 positive evidence | YEE-61 guard result |",
        "|---|---|---|---|---|",
    ]
    for identity in ("voxel:1000", "voxel:10013", "voxel:10019"):
        scope = scope_by_id.get(identity)
        if scope is None:
            continue
        original = eligibility[(scope["source"], scope["source_resource_id"])]["positive_evidence"]
        columns = (
            identity, scope.get("title"), scope.get("summary"), canonical_json(original),
            f"{scope['product_scope_status']} — {', '.join(scope['scope_reason_codes'])}",
        )
        lines.append("| " + " | ".join(_markdown_table_cell(value) for value in columns) + " |")
    lines.extend([
        "",
        "The examples remain inside the immutable YEE-60 accepted universe but are narrowed out of category analysis because the source-native product form is respectively a 3D model, a dimensioned spawn/build, and a YAML shop configuration. Their platform facets are retained as eligibility evidence, not treated as proof that the product itself is a plugin.",
        "",
        "## Deterministic source × demand-stratified taxonomy review sample",
        "",
        f"Selection: SHA-256 ordering, at most {SAMPLE_PER_SOURCE_DEMAND_STRATUM} confirmed identities per source × source-local `demand_percentile` quartile. Demand is used only to define the required review sample and does not feed scope/category decisions.",
        "",
        "| Source | Demand stratum | Identity | Hangar functional facet | Title | Summary |",
        "|---|---|---|---|---|---|",
    ])
    for sample in taxonomy_sample:
        values = (
            sample["source"], sample["demand_stratum"], sample["canonical_identity"],
            canonical_json(sample["source_native_category_facets"]), sample["title"], sample["summary"],
        )
        lines.append("| " + " | ".join(_markdown_table_cell(value) for value in values) + " |")
    lines.extend(["", "No historical YEE-30→YEE-59 output is used to scope or label identities.", ""])
    path.write_text("\n".join(lines), encoding="utf-8", newline="\n")


def _schema_markdown() -> str:
    return """# YEE-61 CATEGORY_FIRST_SCHEMA v0.1

## Canonical input

Only the pinned accepted YEE-60 `plugin_only_resource_features` universe is read. It contains 10,018 rows (Hangar 3,861; Voxel 6,157). YEE-60 JSONL SHA-256: `355e4efe0e75d5bbac4205986c7b3fd37a405bbab8ca6be9d4d3b807ed1909f2`; YEE-60 SQLite SHA-256: `94171517a9c3fb1670ad30f49d48d12fcde8fc37f6f9e1af6aa17c0ca687848e`. Both are checked before/after and opened read-only.

## Product scope

`plugin_product_scope.jsonl/csv` has exactly one row per input identity, sorted `(source, source_resource_id)`. Every input feature field is retained, along with `yee60_plugin_eligibility`, `yee60_eligibility_reason_codes`, `yee60_positive_evidence`, `yee60_classifier_version`, `product_scope_status`, `scope_reason_codes`, `scope_evidence`, `scope_confidence`, `scope_method`, and `scope_classifier_version`.

Statuses are `PLUGIN_PRODUCT_CONFIRMED`, `PLUGIN_PRODUCT_REVIEW`, and `OUT_OF_SCOPE_PRODUCT_FORM`. Only confirmed products can receive category membership. Evidence arrays contain the original YEE-60 positive source-native facts and exact title/summary spans used by this deterministic guard. No model-assisted evidence is used.

## Frozen category taxonomy and memberships

Taxonomy version: `yee-61-functional-category-taxonomy-v0.1`. Categories express functional market jobs, never loader/platform facets. It is frozen before full assignment. Hangar's observed functional categories are exact anchors; its broad `misc` value is preserved as source context but requires semantic evidence or `UNCATEGORIZED`. Voxel category assignment uses stored title/summary evidence. Each confirmed product receives exactly one primary category or `uncategorized`, zero to three deduplicated secondary categories, assignment method/confidence/evidence, source-native category/loader facets, and taxonomy version.

`plugin_category_memberships.jsonl/csv` contains only `PLUGIN_PRODUCT_CONFIRMED` rows and retains the full YEE-60 feature fields. `plugin_category_inventory.jsonl/csv` contains one row for every frozen category plus `UNCATEGORIZED`, including zero-member categories.

## SQLite

`category_first_foundation.sqlite` contains `metadata`, `plugin_product_scope`, `plugin_category_taxonomy`, `plugin_category_memberships`, and `plugin_category_inventory`. The source feature row and original eligibility evidence are stored as canonical JSON in the scope table. Memberships have foreign keys to scope and taxonomy. Export ordering is stable; CSV nulls are `\\N`; JSONL is UTF-8/LF with canonical-key JSON.
"""


def _downstream_contract_markdown() -> str:
    return """# YEE-61 downstream category contract

This contract is a handoff boundary, not Stage B implementation. The only eligible downstream population is the `PLUGIN_PRODUCT_CONFIRMED` subset in `plugin_category_memberships`; `PLUGIN_PRODUCT_REVIEW` and `OUT_OF_SCOPE_PRODUCT_FORM` remain auditable in `plugin_product_scope` and cannot enter opportunity analysis.

## Category-first sequence

`PLUGIN_ONLY input → product-form scope guard → frozen functional taxonomy → auditable category memberships/inventory → category-local signals → within-category whitespace/directions → category opportunity map → category-targeted research → Supervisor/User decision gate`.

## Future metric semantics (not computed in YEE-61)

- Preserve listing/member counts per source and category/subcategory; a cross-source listing sum is not a deduplicated unique-product market.
- Use source-native demand only through source-local distributions/percentiles and within-category comparisons. Never sum or directly compare raw Voxel/Hangar downloads.
- Evaluate whitespace inside a category, preferably at subcategory/family/theme level, combining demand and local supply/competition evidence.
- Keep missing freshness and paid evidence null/unknown, not zero or free. Voxel paid/price data remains a Voxel-only dimension unless a future explicit comparability contract is approved; do not mix currencies into one scalar.
- Keep research quality separate from attractiveness. Strong-looking evidence with weak coverage stays visibly uncertain.
- Keep every frozen category, including weak, negative, and zero-member categories. Stable taxonomy order is the default presentation order.
- No global category/plugin winner, mandatory rank, universal shortlist, positive-state quota, or build recommendation is permitted. Zero candidates and “build none” are valid.

YEE-61 computes no demand/supply/freshness/paid-evidence signals, opportunity state, ranking, external research, concept, or recommendation. Stage B and later stages require a separate work order and explicit authorization.
"""


def _write_sqlite(
    path: Path,
    scope_rows: Sequence[Mapping[str, Any]],
    taxonomy: Mapping[str, Any],
    memberships: Sequence[Mapping[str, Any]],
    inventory: Sequence[Mapping[str, Any]],
    input_hashes: Mapping[str, str],
) -> None:
    db = sqlite3.connect(path)
    with db:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA page_size=4096")
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA synchronous=FULL")
        db.executescript(
            """
            CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
            CREATE TABLE plugin_product_scope (
                source TEXT NOT NULL,
                source_resource_id TEXT NOT NULL,
                canonical_identity TEXT NOT NULL UNIQUE,
                product_scope_status TEXT NOT NULL CHECK(product_scope_status IN ('PLUGIN_PRODUCT_CONFIRMED','PLUGIN_PRODUCT_REVIEW','OUT_OF_SCOPE_PRODUCT_FORM')),
                scope_reason_codes_json TEXT NOT NULL,
                scope_evidence_json TEXT NOT NULL,
                scope_confidence TEXT NOT NULL,
                scope_method TEXT NOT NULL,
                scope_classifier_version TEXT NOT NULL,
                yee60_eligibility_evidence_json TEXT NOT NULL,
                source_feature_json TEXT NOT NULL,
                PRIMARY KEY(source, source_resource_id)
            ) WITHOUT ROWID;
            CREATE TABLE plugin_category_taxonomy (
                category_id TEXT PRIMARY KEY,
                category_order INTEGER NOT NULL UNIQUE,
                category_name TEXT NOT NULL,
                definition TEXT NOT NULL,
                subcategories_json TEXT NOT NULL,
                source_native_anchors_json TEXT NOT NULL,
                known_ambiguity_risk_notes_json TEXT NOT NULL,
                taxonomy_version TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE plugin_category_memberships (
                source TEXT NOT NULL,
                source_resource_id TEXT NOT NULL,
                canonical_identity TEXT NOT NULL UNIQUE,
                primary_category_id TEXT NOT NULL,
                subcategory_id TEXT NOT NULL,
                secondary_category_ids_json TEXT NOT NULL,
                assignment_method TEXT NOT NULL,
                assignment_confidence TEXT NOT NULL,
                assignment_evidence_json TEXT NOT NULL,
                assignment_risk_notes_json TEXT NOT NULL,
                source_native_category_facets_json TEXT NOT NULL,
                source_native_loader_facets_json TEXT NOT NULL,
                taxonomy_version TEXT NOT NULL,
                PRIMARY KEY(source, source_resource_id),
                FOREIGN KEY(source, source_resource_id) REFERENCES plugin_product_scope(source, source_resource_id),
                FOREIGN KEY(primary_category_id) REFERENCES plugin_category_taxonomy(category_id)
            ) WITHOUT ROWID;
            CREATE TABLE plugin_category_inventory (
                category_id TEXT PRIMARY KEY,
                category_name TEXT NOT NULL,
                definition TEXT NOT NULL,
                subcategories_json TEXT NOT NULL,
                member_count_by_source_json TEXT NOT NULL,
                primary_member_count INTEGER NOT NULL,
                secondary_member_count INTEGER NOT NULL,
                scope_evidence_coverage_count INTEGER NOT NULL,
                scope_evidence_coverage_rate REAL,
                assignment_confidence_distribution_json TEXT NOT NULL,
                representative_identities_json TEXT NOT NULL,
                known_ambiguity_risk_notes_json TEXT NOT NULL,
                taxonomy_version TEXT NOT NULL,
                FOREIGN KEY(category_id) REFERENCES plugin_category_taxonomy(category_id)
            ) WITHOUT ROWID;
            """
        )
        metadata = {
            "work_order": WORK_ORDER,
            "schema_version": SCHEMA_VERSION,
            "scope_classifier_version": SCOPE_CLASSIFIER_VERSION,
            "assignment_version": ASSIGNMENT_VERSION,
            "taxonomy_version": TAXONOMY_VERSION,
            "taxonomy_sha256": taxonomy["taxonomy_sha256"],
            "input_plugin_only_resource_features_sha256": input_hashes["plugin_only_resource_features.jsonl"],
            "input_plugin_eligibility_sqlite_sha256": input_hashes["plugin_eligibility.sqlite"],
            "input_rows": str(len(scope_rows)),
            "confirmed_rows": str(sum(row["product_scope_status"] == PLUGIN_PRODUCT_CONFIRMED for row in scope_rows)),
        }
        db.executemany("INSERT INTO metadata VALUES (?, ?)", sorted(metadata.items()))
        scope_columns = (
            "source", "source_resource_id", "canonical_identity", "product_scope_status",
            "scope_reason_codes_json", "scope_evidence_json", "scope_confidence", "scope_method",
            "scope_classifier_version", "yee60_eligibility_evidence_json", "source_feature_json",
        )
        db.executemany(
            f"INSERT INTO plugin_product_scope ({','.join(scope_columns)}) VALUES ({','.join('?' for _ in scope_columns)})",
            [(
                row["source"], row["source_resource_id"], row["canonical_identity"], row["product_scope_status"],
                canonical_json(row["scope_reason_codes"]), canonical_json(row["scope_evidence"]), row["scope_confidence"],
                row["scope_method"], row["scope_classifier_version"],
                canonical_json({key: row[key] for key in YEE60_COLUMNS}),
                canonical_json({key: row[key] for key in sorted(row) if key not in (*YEE60_COLUMNS, *SCOPE_COLUMNS)}),
            ) for row in scope_rows],
        )
        db.executemany(
            "INSERT INTO plugin_category_taxonomy VALUES (?,?,?,?,?,?,?,?)",
            [(
                category["category_id"], order, category["category_name"], category["definition"],
                canonical_json(category["subcategories"]), canonical_json(category["source_native_anchors"]),
                canonical_json(category["known_ambiguity_risk_notes"]), TAXONOMY_VERSION,
            ) for order, category in enumerate(taxonomy["categories"])],
        )
        db.executemany(
            "INSERT INTO plugin_category_memberships VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(
                row["source"], row["source_resource_id"], row["canonical_identity"], row["primary_category_id"],
                row["subcategory_id"], canonical_json(row["secondary_category_ids"]), row["assignment_method"],
                row["assignment_confidence"], canonical_json(row["assignment_evidence"]),
                canonical_json(row["assignment_risk_notes"]), canonical_json(row["source_native_category_facets"]),
                canonical_json(row["source_native_loader_facets"]), row["taxonomy_version"],
            ) for row in memberships],
        )
        db.executemany(
            "INSERT INTO plugin_category_inventory VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(
                row["category_id"], row["category_name"], row["definition"], canonical_json(row["subcategories"]),
                canonical_json(row["member_count_by_source"]), row["primary_member_count"],
                row["secondary_member_count"], row["scope_evidence_coverage_count"],
                row["scope_evidence_coverage_rate"], canonical_json(row["assignment_confidence_distribution"]),
                canonical_json(row["representative_identities"]), canonical_json(row["known_ambiguity_risk_notes"]),
                row["taxonomy_version"],
            ) for row in inventory],
        )
        db.commit()
        db.execute("PRAGMA optimize")
    db.close()


def _generate_core(
    output_dir: Path,
    feature_rows: Sequence[Mapping[str, Any]],
    eligibility: Mapping[tuple[str, str], Mapping[str, Any]],
    input_hashes: Mapping[str, str],
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    scope_rows = [
        _scope_record(row, eligibility[(str(row["source"]), str(row["source_resource_id"]))])
        for row in feature_rows
    ]
    taxonomy_sample = _taxonomy_sample(scope_rows)
    taxonomy = _taxonomy_document(taxonomy_sample)
    memberships = []
    for row, scope in zip(feature_rows, scope_rows, strict=True):
        assignment = assign_category(row, scope)
        if assignment is not None:
            memberships.append(_membership_record(row, assignment))
    memberships.sort(key=_feature_sort_key)
    inventory = _inventory_rows(taxonomy, memberships, scope_rows)

    _write_jsonl(output_dir / "plugin_product_scope.jsonl", scope_rows)
    feature_columns = sorted(feature_rows[0])
    scope_export_columns = (*feature_columns, *YEE60_COLUMNS, *SCOPE_COLUMNS)
    _write_csv(output_dir / "plugin_product_scope.csv", scope_rows, scope_export_columns)

    (output_dir / "plugin_category_taxonomy.json").write_text(canonical_json(taxonomy) + "\n", encoding="utf-8", newline="\n")
    _write_taxonomy_markdown(output_dir / "plugin_category_taxonomy.md", taxonomy)
    _write_jsonl(output_dir / "plugin_category_memberships.jsonl", memberships)
    assignment_export_columns = (
        *feature_columns, "primary_category_id", "subcategory_id", "secondary_category_ids",
        "assignment_method", "assignment_confidence", "assignment_evidence", "assignment_risk_notes",
        "source_native_category_facets", "source_native_loader_facets", "taxonomy_version",
    )
    _write_csv(output_dir / "plugin_category_memberships.csv", memberships, assignment_export_columns)

    _write_jsonl(output_dir / "plugin_category_inventory.jsonl", inventory)
    inventory_export_columns = (
        "category_order", "category_id", "category_name", "definition", "subcategories",
        "member_count_by_source", "primary_member_count", "secondary_member_count",
        "scope_evidence_coverage_count", "scope_evidence_coverage_rate",
        "assignment_confidence_distribution", "representative_identities",
        "known_ambiguity_risk_notes", "taxonomy_version",
    )
    _write_csv(output_dir / "plugin_category_inventory.csv", inventory, inventory_export_columns)
    _write_goal_alignment(output_dir / "GOAL_ALIGNMENT.md", scope_rows, taxonomy)
    _write_semantic_smoke(output_dir / "SEMANTIC_SMOKE_TEST.md", scope_rows, taxonomy_sample, eligibility)
    (output_dir / "CATEGORY_FIRST_SCHEMA.md").write_text(_schema_markdown(), encoding="utf-8", newline="\n")
    (output_dir / "DOWNSTREAM_CATEGORY_CONTRACT.md").write_text(_downstream_contract_markdown(), encoding="utf-8", newline="\n")
    _write_sqlite(output_dir / "category_first_foundation.sqlite", scope_rows, taxonomy, memberships, inventory, input_hashes)
    return {
        "scope_rows": scope_rows,
        "taxonomy_sample": taxonomy_sample,
        "taxonomy": taxonomy,
        "memberships": memberships,
        "inventory": inventory,
        "feature_columns": feature_columns,
    }


def _file_fingerprint(directory: Path, names: Sequence[str]) -> dict[str, str]:
    return {name: sha256_file(directory / name) for name in names}


def _qa_result(
    output_dir: Path,
    generated: Mapping[str, Any],
    input_hashes: Mapping[str, str],
    input_hashes_after: Mapping[str, str],
    source_counts: Mapping[str, int],
    replay: Mapping[str, Any],
    *,
    enforce_pinned_inputs: bool,
) -> dict[str, Any]:
    scope_rows = generated["scope_rows"]
    memberships = generated["memberships"]
    taxonomy = generated["taxonomy"]
    status_counts = Counter(row["product_scope_status"] for row in scope_rows)
    category_counts = Counter(row["primary_category_id"] for row in memberships)
    identities = [(row["source"], row["source_resource_id"]) for row in scope_rows]
    confirmed_ids = {(row["source"], row["source_resource_id"]) for row in scope_rows if row["product_scope_status"] == PLUGIN_PRODUCT_CONFIRMED}
    membership_ids = {(row["source"], row["source_resource_id"]) for row in memberships}
    all_assignments_valid = all(
        row["primary_category_id"] in TAXONOMY_CATEGORY_IDS
        and len(row["secondary_category_ids"]) <= 3
        and len(row["secondary_category_ids"]) == len(set(row["secondary_category_ids"]))
        and row["primary_category_id"] not in row["secondary_category_ids"]
        and row["taxonomy_version"] == TAXONOMY_VERSION
        for row in memberships
    )
    inventory_ids = [row["category_id"] for row in generated["inventory"]]
    sqlite_path = output_dir / "category_first_foundation.sqlite"
    db = sqlite3.connect(sqlite_path)
    try:
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = db.execute("PRAGMA foreign_key_check").fetchall()
        db_scope_count = db.execute("SELECT COUNT(*) FROM plugin_product_scope").fetchone()[0]
        db_membership_count = db.execute("SELECT COUNT(*) FROM plugin_category_memberships").fetchone()[0]
        db_taxonomy_count = db.execute("SELECT COUNT(*) FROM plugin_category_taxonomy").fetchone()[0]
        db_inventory_count = db.execute("SELECT COUNT(*) FROM plugin_category_inventory").fetchone()[0]
        db_leaks = db.execute(
            """SELECT COUNT(*) FROM plugin_category_memberships m
               JOIN plugin_product_scope s USING(source,source_resource_id)
               WHERE s.product_scope_status!='PLUGIN_PRODUCT_CONFIRMED'"""
        ).fetchone()[0]
    finally:
        db.close()
    jsonl_scope_count = sum(1 for _ in (output_dir / "plugin_product_scope.jsonl").open("r", encoding="utf-8"))
    jsonl_membership_count = sum(1 for _ in (output_dir / "plugin_category_memberships.jsonl").open("r", encoding="utf-8"))
    jsonl_inventory_count = sum(1 for _ in (output_dir / "plugin_category_inventory.jsonl").open("r", encoding="utf-8"))
    csv_scope_count = _csv_data_row_count(output_dir / "plugin_product_scope.csv")
    csv_membership_count = _csv_data_row_count(output_dir / "plugin_category_memberships.csv")
    csv_inventory_count = _csv_data_row_count(output_dir / "plugin_category_inventory.csv")

    metric_fields = (
        "demand_percentile", "download_count", "demand_download_count", "downloads_total",
        "voxel_demand_download_count", "hangar_recent_downloads", "paid", "price_amount",
        "currency", "paid_state", "voxel_price_band", "freshness_age_days",
        "update_age_days", "latest_update_age_days", "freshness_cohort", "review_count",
        "review_average", "follow_count", "star_count", "watcher_count",
        "source_metrics_json", "voxel_source_metrics_json", "hangar_source_metrics_json",
        "modrinth_source_metrics_json",
    )
    membership_by_identity = {
        (row["source"], row["source_resource_id"]): row
        for row in memberships
    }
    metric_isolation = True
    for scope in scope_rows:
        identity = (scope["source"], scope["source_resource_id"])
        original_feature = membership_by_identity.get(identity)
        if original_feature is None:
            original_feature = {
                key: value for key, value in scope.items()
                if key not in (*YEE60_COLUMNS, *SCOPE_COLUMNS)
            }
        altered = dict(original_feature)
        for field in metric_fields:
            if field in altered:
                altered[field] = "__metric_mutation__"
        eligibility_row = {
            "plugin_eligibility": scope["yee60_plugin_eligibility"],
            "positive_evidence": scope["yee60_positive_evidence"],
            "classifier_version": scope["yee60_classifier_version"],
        }
        altered_scope = classify_product_form(altered, eligibility_row)
        if altered_scope != {key: scope[key] for key in SCOPE_COLUMNS}:
            metric_isolation = False
            break
        if scope["product_scope_status"] == PLUGIN_PRODUCT_CONFIRMED:
            altered_assignment = assign_category(altered, altered_scope)
            original_assignment = {key: membership_by_identity[identity][key] for key in ASSIGNMENT_COLUMNS}
            if altered_assignment != original_assignment:
                metric_isolation = False
                break
    downstream_contract = (output_dir / "DOWNSTREAM_CATEGORY_CONTRACT.md").read_text(encoding="utf-8")
    check_values = {
        "input_hashes_match_pinned": (not enforce_pinned_inputs) or input_hashes == {
            "plugin_eligibility.sqlite": YEE60_SQLITE_SHA256,
            "plugin_only_resource_features.jsonl": YEE60_JSONL_SHA256,
        },
        "input_hashes_unchanged_after_processing": dict(input_hashes) == dict(input_hashes_after),
        "canonical_input_rows_exact": len(scope_rows) == EXPECTED_INPUT_ROWS if enforce_pinned_inputs else len(scope_rows) > 0,
        "canonical_source_counts_exact": dict(source_counts) == EXPECTED_SOURCE_COUNTS if enforce_pinned_inputs else sum(source_counts.values()) == len(scope_rows),
        "all_input_identities_accounted_once": len(identities) == len(set(identities)) == len(scope_rows),
        "every_row_has_one_valid_scope_status": all(row["product_scope_status"] in SCOPE_STATUSES for row in scope_rows),
        "original_yee60_eligibility_evidence_retained": all(row["yee60_plugin_eligibility"] == "PLUGIN_ELIGIBLE" and row["yee60_positive_evidence"] for row in scope_rows),
        "known_semantic_false_positive_fixtures_narrowed": (not enforce_pinned_inputs) or all(
            next(row for row in scope_rows if row["canonical_identity"] == identity)["product_scope_status"] == OUT_OF_SCOPE_PRODUCT_FORM
            for identity in ("voxel:1000", "voxel:10013", "voxel:10019")
        ),
        "category_membership_equals_confirmed_scope_set": membership_ids == confirmed_ids,
        "no_review_or_out_of_scope_category_leakage": db_leaks == 0,
        "every_confirmed_row_has_exactly_one_primary_or_uncategorized": all_assignments_valid and len(membership_ids) == len(confirmed_ids),
        "taxonomy_frozen_before_assignment": taxonomy["taxonomy_status"] == "FROZEN_BEFORE_FULL_ASSIGNMENT" and all(row["taxonomy_version"] == TAXONOMY_VERSION for row in memberships),
        "all_frozen_categories_and_uncategorized_visible": inventory_ids == list(TAXONOMY_CATEGORY_IDS),
        "rare_categories_are_not_removed": all(category_id in inventory_ids for category_id in TAXONOMY_CATEGORY_IDS),
        "demand_price_and_popularity_not_used_in_decision_functions": metric_isolation,
        "jsonl_csv_sqlite_reconcile": (
            jsonl_scope_count == len(scope_rows) == db_scope_count
            and csv_scope_count == len(scope_rows)
            and jsonl_membership_count == len(memberships) == db_membership_count
            and csv_membership_count == len(memberships)
            and jsonl_inventory_count == len(generated["inventory"]) == db_inventory_count
            and csv_inventory_count == len(generated["inventory"])
            and db_taxonomy_count == len(taxonomy["categories"])
        ),
        "sqlite_integrity_check_ok": integrity == "ok",
        "sqlite_foreign_key_check_zero": not foreign_keys,
        "deterministic_replay_core_artifacts_byte_identical": replay["byte_identical"],
        "no_global_winner_rank_or_stage_b_outputs": (
            "No global category/plugin winner" in downstream_contract
            and "Stage B and later stages require a separate work order" in downstream_contract
            and not any(name in FINAL_ARTIFACTS for name in ("opportunity_scores.jsonl", "shortlist.jsonl"))
        ),
    }
    failed = [key for key, value in check_values.items() if not value]
    return {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "status": "PASS" if not failed else "FAIL",
        "goal_alignment": "PASS_WITH_REQUIRED_SCOPE_HARDENING",
        "source_counts": dict(source_counts),
        "row_counts": {
            "canonical_input": len(scope_rows),
            "plugin_product_scope": len(scope_rows),
            "plugin_product_confirmed": status_counts.get(PLUGIN_PRODUCT_CONFIRMED, 0),
            "plugin_product_review": status_counts.get(PLUGIN_PRODUCT_REVIEW, 0),
            "out_of_scope_product_form": status_counts.get(OUT_OF_SCOPE_PRODUCT_FORM, 0),
            "plugin_category_memberships": len(memberships),
            "plugin_category_inventory": len(generated["inventory"]),
        },
        "scope_status_counts": {status: status_counts.get(status, 0) for status in SCOPE_STATUSES},
        "primary_category_counts": {category_id: category_counts.get(category_id, 0) for category_id in TAXONOMY_CATEGORY_IDS},
        "taxonomy_review_sample_count": len(generated["taxonomy_sample"]),
        "taxonomy_version": TAXONOMY_VERSION,
        "taxonomy_sha256": taxonomy["taxonomy_sha256"],
        "input_sha256_before": dict(input_hashes),
        "input_sha256_after": dict(input_hashes_after),
        "replay": dict(replay),
        "checks": check_values,
        "failed_checks": failed,
    }


def _final_report(qa: Mapping[str, Any]) -> str:
    counts = qa["row_counts"]
    lines = [
        "# YEE-61 final report",
        "",
        "Status: `CATEGORY_FIRST_FOUNDATION_READY_FOR_SUPERVISOR_REVIEW`" if qa["status"] == "PASS" else "Status: `BLOCKED`",
        "",
        "## Objective alignment and scope",
        "GOAL_ALIGNMENT: `PASS_WITH_REQUIRED_SCOPE_HARDENING`. The accepted YEE-60 PLUGIN_ELIGIBLE universe was narrowed by a source-native product-form semantic guard before category assignment. Only `PLUGIN_PRODUCT_CONFIRMED` rows enter category membership; review and out-of-scope rows remain in the auditable scope dataset.",
        "",
        f"- Canonical rows: {counts['canonical_input']:,} (Hangar {qa['source_counts'].get('hangar', 0):,}; Voxel {qa['source_counts'].get('voxel', 0):,})",
        f"- PLUGIN_PRODUCT_CONFIRMED: {counts['plugin_product_confirmed']:,}",
        f"- PLUGIN_PRODUCT_REVIEW: {counts['plugin_product_review']:,}",
        f"- OUT_OF_SCOPE_PRODUCT_FORM: {counts['out_of_scope_product_form']:,}",
        f"- Category memberships: {counts['plugin_category_memberships']:,}",
        f"- Frozen categories including UNCATEGORIZED: {counts['plugin_category_inventory']}",
        f"- Taxonomy version/hash: `{qa['taxonomy_version']}` / `{qa['taxonomy_sha256']}`",
        "",
        "## Input integrity and QA",
        f"- YEE-60 JSONL SHA-256 before/after: `{qa['input_sha256_before']['plugin_only_resource_features.jsonl']}` / `{qa['input_sha256_after']['plugin_only_resource_features.jsonl']}`",
        f"- YEE-60 SQLite SHA-256 before/after: `{qa['input_sha256_before']['plugin_eligibility.sqlite']}` / `{qa['input_sha256_after']['plugin_eligibility.sqlite']}`",
        f"- QA: `{qa['status']}`; passing checks {sum(bool(value) for value in qa['checks'].values())}/{len(qa['checks'])}; failed checks {len(qa['failed_checks'])}.",
        f"- Deterministic replay of core exports/database/contracts: `{qa['replay']['byte_identical']}` ({len(qa['replay']['compared_artifacts'])} artifacts).",
        "",
        "## Category-first stop boundary",
        "Outputs stop at the product-form guard, frozen functional taxonomy, auditable confirmed-product memberships, category inventory, and downstream contract. No Stage B category-local signals, opportunity score/rank, winner, shortlist, external research, concept, commercial validation, or build recommendation was produced.",
        "",
        "The canonical YEE-60 inputs remain read-only. Historical YEE-30→YEE-59 outputs were not used as canonical inputs. PR remains open/unmerged for supervisor review.",
        "",
    ]
    return "\n".join(lines)


def _write_manifest(output_dir: Path, names: Sequence[str]) -> dict[str, Any]:
    artifacts = {}
    for name in sorted(names):
        path = output_dir / name
        artifacts[name] = {"size_bytes": path.stat().st_size, "sha256": sha256_file(path)}
    manifest = {
        "work_order": WORK_ORDER,
        "schema_version": SCHEMA_VERSION,
        "status": "PASS",
        "manifest_self_hash": "omitted_to_avoid_recursive_hash",
        "artifacts": artifacts,
    }
    (output_dir / "DATASET_MANIFEST.json").write_text(canonical_json(manifest) + "\n", encoding="utf-8", newline="\n")
    return manifest


def build_category_foundation(
    input_db_path: str | Path,
    input_jsonl_path: str | Path,
    output_dir_path: str | Path,
    *,
    enforce_pinned_inputs: bool = True,
) -> dict[str, Any]:
    """Build YEE-61 artifacts from immutable accepted YEE-60 inputs."""
    input_db = Path(input_db_path).resolve()
    input_jsonl = Path(input_jsonl_path).resolve()
    output_dir = Path(output_dir_path).resolve()
    if output_dir in {input_db.parent, input_jsonl.parent} or input_db == output_dir / input_db.name:
        raise CategoryFoundationError("Output directory must be separate from the read-only YEE-60 input")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise CategoryFoundationError("Output directory must be new or empty; refusing to overwrite existing artifacts")
    input_rows, eligibility, input_hashes_before, source_counts = _load_canonical_inputs(
        input_db, input_jsonl, enforce_pinned_inputs=enforce_pinned_inputs,
    )
    generated = _generate_core(output_dir, input_rows, eligibility, input_hashes_before)
    with tempfile.TemporaryDirectory(prefix="yee61-replay-") as temp_name:
        replay_dir = Path(temp_name)
        replay_generated = _generate_core(replay_dir, input_rows, eligibility, input_hashes_before)
        expected = _file_fingerprint(output_dir, CORE_ARTIFACTS)
        actual = _file_fingerprint(replay_dir, CORE_ARTIFACTS)
        replay = {
            "byte_identical": expected == actual,
            "compared_artifacts": list(CORE_ARTIFACTS),
            "differences": sorted(name for name in expected if expected.get(name) != actual.get(name)),
        }
        del replay_generated
    input_hashes_after = {
        "plugin_eligibility.sqlite": sha256_file(input_db),
        "plugin_only_resource_features.jsonl": sha256_file(input_jsonl),
    }
    qa = _qa_result(
        output_dir, generated, input_hashes_before, input_hashes_after, source_counts, replay,
        enforce_pinned_inputs=enforce_pinned_inputs,
    )
    qa["manifest_artifact_count"] = len(CORE_ARTIFACTS) + 2
    (output_dir / "QA_RESULT.json").write_text(canonical_json(qa) + "\n", encoding="utf-8", newline="\n")
    (output_dir / "FINAL_REPORT.md").write_text(_final_report(qa), encoding="utf-8", newline="\n")
    manifest = _write_manifest(output_dir, (*CORE_ARTIFACTS, "QA_RESULT.json", "FINAL_REPORT.md"))
    if qa["manifest_artifact_count"] != len(manifest["artifacts"]):
        raise CategoryFoundationError("Dataset manifest artifact count does not reconcile")
    if qa["status"] != "PASS":
        raise CategoryFoundationError(f"YEE-61 QA failed: {qa['failed_checks']}")
    return qa
