from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from market_analysis import category_foundation as foundation


def _row(source_id: str, *, source: str = "voxel", title: str = "", summary: str = "", category: str | None = None):
    category_facets = [category] if category else []
    return {
        "source": source,
        "source_resource_id": source_id,
        "canonical_identity": f"{source}:{source_id}",
        "title": title,
        "summary": summary,
        "project_type_norm": None,
        "loader_facets_json": '["paper"]',
        "category_facets_json": json.dumps(category_facets, separators=(",", ":")),
        "demand_percentile": 50.0,
        "download_count": None,
        "price_amount": None,
        "paid_state": "UNKNOWN",
    }


def _eligible():
    return {
        "plugin_eligibility": "PLUGIN_ELIGIBLE",
        "eligibility_reason_codes": ["EXPLICIT_PLUGIN_CLASS_EVIDENCE"],
        "positive_evidence": [{"field": "loader_facets_json", "token": "paper"}],
        "classifier_version": foundation.YEE60_CLASSIFIER_VERSION,
    }


def _write_fixture_input(tmp_path: Path, rows):
    input_db = tmp_path / "plugin_eligibility.sqlite"
    input_jsonl = tmp_path / "plugin_only_resource_features.jsonl"
    columns = sorted(rows[0])
    with sqlite3.connect(input_db) as db:
        def sqlite_type(column):
            value = next((row.get(column) for row in rows if row.get(column) is not None), None)
            if isinstance(value, bool) or isinstance(value, int):
                return "INTEGER"
            if isinstance(value, float):
                return "REAL"
            return "TEXT"

        defs = ",".join(f'"{column}" {sqlite_type(column)}' for column in columns)
        db.execute(f"CREATE TABLE plugin_only_resource_features ({defs}, PRIMARY KEY(source,source_resource_id))")
        db.executemany(
            f"INSERT INTO plugin_only_resource_features VALUES ({','.join('?' for _ in columns)})",
            [[row.get(column) for column in columns] for row in rows],
        )
        db.execute(
            """CREATE TABLE plugin_eligibility (
                source TEXT, source_resource_id TEXT, canonical_identity TEXT,
                plugin_eligibility TEXT, eligibility_reason_codes TEXT,
                positive_evidence TEXT, classifier_version TEXT,
                PRIMARY KEY(source,source_resource_id)
            )"""
        )
        db.executemany(
            "INSERT INTO plugin_eligibility VALUES (?,?,?,?,?,?,?)",
            [(
                row["source"], row["source_resource_id"], row["canonical_identity"],
                "PLUGIN_ELIGIBLE", foundation.canonical_json(_eligible()["eligibility_reason_codes"]),
                foundation.canonical_json(_eligible()["positive_evidence"]), foundation.YEE60_CLASSIFIER_VERSION,
            ) for row in rows],
        )
    input_jsonl.write_text(
        "".join(foundation.canonical_json(row) + "\n" for row in rows),
        encoding="utf-8",
        newline="\n",
    )
    return input_db, input_jsonl


def _fixture_rows():
    return [
        _row(
            "1", source="hangar", title="Maintenance",
            summary="This Paper plugin manages server maintenance and restarts.", category="admin_tools",
        ),
        _row(
            "1000", title="Lamborghini Huracan Evo Model",
            summary="High quality 3D model that will work with plugins like VehiclesPlus.",
        ),
        _row(
            "10013", title="Sandstone Castle Spawn",
            summary="153x187 | 8 Crate spots | 3 NPC Places | 2 Markets",
        ),
        _row(
            "10019", title="Balanced Survival Shop Config",
            summary="Balanced EconomyShopGUI YAML shop config with five categories.",
        ),
        _row("2", title="Base Regenerator Plugin", summary="This Paper plugin gives faction leaders the ability to regenerate griefed claims."),
        _row("3", title="Unclear Listing", summary="A Minecraft server project."),
    ]


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (_row("1000", title="Lamborghini Huracan Evo Model", summary="High quality 3D model that will work with plugins like VehiclesPlus."), "OUT_OF_SCOPE_3D_MODEL_ASSET"),
        (_row("10013", title="Sandstone Castle Spawn", summary="153x187 | 8 Crate spots | 3 NPC Places | 2 Markets"), "OUT_OF_SCOPE_BUILD_OR_MAP_ASSET"),
        (_row("10019", title="Balanced Survival Shop Config", summary="Balanced EconomyShopGUI YAML shop config with five categories."), "OUT_OF_SCOPE_CONFIGURATION_PRODUCT"),
        (_row("10020", title="Void Fishing Pack", summary="16 fish and 5 rarities for a LiteFish server."), "OUT_OF_SCOPE_CONTENT_PACK_OR_SETUP"),
        (_row("10021", title="Addwild - Crocodile", summary="MythicMobs - Custom Mob. Add 3 Mobs."), "OUT_OF_SCOPE_PLUGIN_DEPENDENT_CONTENT"),
    ],
)
def test_known_voxel_model_build_and_config_false_positives_are_narrowed(row, reason):
    result = foundation.classify_product_form(row, _eligible())

    assert result["product_scope_status"] == foundation.OUT_OF_SCOPE_PRODUCT_FORM
    assert reason in result["scope_reason_codes"]
    assert any(item["evidence_type"] == "YEE60_PLUGIN_ELIGIBILITY" for item in result["scope_evidence"])


def test_server_plugin_product_is_confirmed_with_both_evidence_layers():
    row = _row("7", title="Claim Guard", summary="This Paper plugin protects and restores player claims.")

    result = foundation.classify_product_form(row, _eligible())

    assert result["product_scope_status"] == foundation.PLUGIN_PRODUCT_CONFIRMED
    assert result["scope_confidence"] == "HIGH"
    assert any(item.get("reason_code") == "PLUGIN_PRODUCT_EXPLICIT_TEXT" for item in result["scope_evidence"])
    assert any(item["evidence_type"] == "YEE60_PLUGIN_ELIGIBILITY" for item in result["scope_evidence"])


def _supervisor_blocker_rows():
    return [
        _row("2227", title="3 Planes for Cold war", summary="This is not a plugin, they are models for model engine or quality vehicles"),
        _row("1139", title="Essentials X Clean Configuration", summary="Custom Messages Configuration for the Essentials X Plugin (English)"),
        _row("1238", title="JNBans [EN/SP]", summary="Professionally designed setup for AdvancedBan plugin"),
        _row("1386", title="AutoAnnouncer Config", summary="A Configuration For Your AutoAnnouncer Plugin"),
        _row("4400", title="Hypixel Maps Setup (BW-1058)", summary="Hypixel Bedwars Maps Configuration for BedWars1058 Plugin"),
        _row("4382", title="Gestures Package | Vanilla Like", summary="The ultimate package for any player emotes plugin."),
        _row("4698", title="MMO TABs Configs 7colors+", summary="MMo+Color Configs for TAB plugin"),
        _row("6353", title="EconomyShopGUI Configuration", summary="#1 EconomyShopGUI Plugin Configuration - Organized, Fancy, Balanced, 250+ Items"),
        _row("6356", title="DeathMessages | 250+ Messages", summary="#1 DeathMessages Plugin Configuration - 250+ Custom Death Messages & 5 Fancy Colors"),
        _row("6606", title="Survival Tab Config", summary="Tab Plugin config specially made for Survival based servers"),
        _row("6868", title="Maintenance plugin Config", summary="Professional Maintenance plugin config"),
        _row("6873", title="Barricades Props | SaturnStudio", summary="Barricades Props Models for ItemsAdder Plugin"),
        _row("6874", title="Parking Gate Set | SaturnStudio", summary="Parking Gate Decoration Models For ItemsAdder Plugin"),
        _row("6875", title="Shell Gas Sation | SaturnStudio", summary="Shell Gas Station Models, for ItemsAdder Plugin"),
        _row("6876", title="Solar Panels | SaturnStudio", summary="Big and Small solar panels Models for ItemsAdder plugin"),
        _row("8176", title="[FREE] Donut Smp 2 TAB config", summary="configs for Tab plugin"),
        _row("416", title="Hangman DeluxeMenus Setup", summary="A Hangman game made completely using DeluxeMenus plugin"),
        _row("5456", title="AdvancedBan Configuration", summary="This is a small message modification of the AdvancedBans plugin."),
        _row("7237", title="Configuration Quetes French", summary="Menu Deluxemenu qui utilise le plugin quest, Il y a au total 177 quetes."),
    ]


def _latest_supervisor_blocker_rows():
    return [
        (_row("1753", title="BentoBox DeluxeMenu Config", summary="7 Menus | Custom Command Arg Plugin | More soon..."), foundation.PLUGIN_PRODUCT_REVIEW),
        (_row("1801", title="⚡TAB ⚡ BEST CONFIG | EN |", summary="tab, plugin, config, hub, practice, pvp"), foundation.PLUGIN_PRODUCT_REVIEW),
        (_row("2590", title="ItemsAdder Park Plus Furniture", summary="Park furniture Addon for ItemsAdder Plugin"), foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        (_row("4786", title="BoxPvP | Server Setup", summary="Villager Trades, Crates, Combat System, Tags, Custom Holograms, Clan System, Custom Plugin and More"), foundation.PLUGIN_PRODUCT_REVIEW),
        (_row("5583", title="Plugin Tab And Scoreboard", summary="A Scoreboard And TAB Config"), foundation.PLUGIN_PRODUCT_REVIEW),
        (_row("6352", title="SternalBoard Premium Config", summary="A Fancy Scoreboard Plugin for your Server | PlaceHolders, Color Gradients & More"), foundation.PLUGIN_PRODUCT_REVIEW),
        (_row("6479", title="AxTrade Config", summary="Simple plugin to handle most dangeours situations!, With AxTrade make trading more safer!"), foundation.PLUGIN_PRODUCT_REVIEW),
        (_row("6586", title="FREE Grim anticheat config", summary="Cazu Config | Prevent Falses | Easy to use | AutoBan system"), foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        (_row("7094", title="Spartan Anti Cheat Configuration", summary="plugin, anticheat, for, minecraft, server, cheat, prevention, hack, detection"), foundation.PLUGIN_PRODUCT_REVIEW),
        (_row("9302", title="Mine Setup + Plugin - English", summary="Minecraft The Movie | Custom Textures | Pickaxe | Mine Ores | Quests | Tab | Shop | Upgrades | Menus"), foundation.PLUGIN_PRODUCT_REVIEW),
    ]


def _plugin_dependent_content_fixture():
    return _row(
        "content-textures",
        title="Magical Fish",
        summary="21 fishes, 6 squids and 5 snails textures for your custom fishing plugin!",
    )


@pytest.mark.parametrize("row", _supervisor_blocker_rows(), ids=lambda row: row["canonical_identity"])
def test_supervisor_production_false_positives_are_explicitly_out_of_scope(row):
    result = foundation.classify_product_form(row, _eligible())

    assert result["product_scope_status"] == foundation.OUT_OF_SCOPE_PRODUCT_FORM
    assert any(code.startswith("OUT_OF_SCOPE_") for code in result["scope_reason_codes"])
    assert not any(item.get("reason_code") == "PLUGIN_PRODUCT_EXPLICIT_TEXT" for item in result["scope_evidence"])


@pytest.mark.parametrize(
    ("row", "expected"),
    _latest_supervisor_blocker_rows(),
    ids=lambda value: value["canonical_identity"] if isinstance(value, dict) else str(value),
)
def test_latest_supervisor_title_product_form_blockers_never_confirm(row, expected):
    result = foundation.classify_product_form(row, _eligible())

    assert result["product_scope_status"] == expected
    assert result["product_scope_status"] != foundation.PLUGIN_PRODUCT_CONFIRMED
    assert any(
        item.get("reason_code") == "TITLE_LEVEL_NON_PLUGIN_PRODUCT_FORM_CUE"
        or item.get("reason_code") == "OUT_OF_SCOPE_PLUGIN_DEPENDENT_PRODUCT"
        for item in result["scope_evidence"]
    )
    if expected == foundation.PLUGIN_PRODUCT_REVIEW:
        assert "MIXED_TITLE_PRODUCT_FORM_AND_PLUGIN_MENTION" in result["scope_reason_codes"]


@pytest.mark.parametrize(
    ("title", "summary", "expected"),
    [
        ("Planes for Cold War", "This is not a plugin; these are models used with vehicle plugins.", foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        ("Essentials configuration", "Configuration for the Essentials X Plugin.", foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        ("AdvancedBan Setup", "A setup made for AdvancedBan plugin.", foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        ("AdvancedBan Setup", "A setup using AdvancedBan plugin.", foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        ("Emotes Package", "Bundle using a player emotes plugin.", foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        ("BedWars Maps", "Maps configuration of BedWars1058 plugin.", foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        ("EconomyShopGUI Plugin Configuration", "An organized configuration for the EconomyShopGUI plugin.", foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        ("TAB Plugin config", "Custom colors and settings for TAB plugin.", foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        ("Solar Panels", "Big and small models for ItemsAdder Plugin.", foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        ("Hangman DeluxeMenus Setup", "A Hangman game made completely using DeluxeMenus plugin.", foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        ("AdvancedBan Configuration", "A small message modification of the AdvancedBans plugin.", foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        ("Configuration Quetes French", "Menu Deluxemenu qui utilise le plugin quest.", foundation.OUT_OF_SCOPE_PRODUCT_FORM),
        ("ChunkGuard", "This Paper plugin provides a config editor for server administrators.", foundation.PLUGIN_PRODUCT_CONFIRMED),
        ("Plugin Package Manager", "This Paper plugin manages downloadable plugin packages.", foundation.PLUGIN_PRODUCT_CONFIRMED),
        ("Compatible Resource", "Works with plugins like VehiclesPlus.", foundation.PLUGIN_PRODUCT_REVIEW),
    ],
)
def test_adversarial_non_plugin_context_overrides_only_dependent_plugin_mentions(title, summary, expected):
    result = foundation.classify_product_form(_row("adversarial", title=title, summary=summary), _eligible())

    assert result["product_scope_status"] == expected


def test_independent_raw_text_contradiction_scan_does_not_use_classifier_evidence():
    row = _row("contradiction", title="A Paper plugin", summary="This is not a plugin; it is a model.")
    assert foundation._independent_semantic_contradictions(row)


@pytest.mark.parametrize(
    "row",
    [
        _row("qa-negation", title="Models", summary="This is not a plugin, these are models."),
        _row("qa-forward-config", title="Essentials Configuration", summary="Configuration for the Essentials X Plugin."),
        _row("qa-reverse-config", title="EconomyShopGUI Plugin Configuration", summary="An EconomyShopGUI Plugin Configuration."),
        _row("qa-plural-model", title="Solar Panels", summary="Big and small models for ItemsAdder Plugin."),
        _row("qa-split-setup", title="Hangman DeluxeMenus Setup", summary="A Hangman game made completely using DeluxeMenus plugin."),
        _row("qa-split-modification", title="AdvancedBan Configuration", summary="A small message modification of the AdvancedBans plugin."),
        _row("qa-french-use", title="Configuration Quetes French", summary="Menu Deluxemenu qui utilise le plugin quest."),
        *[row for row, _ in _latest_supervisor_blocker_rows()],
        _plugin_dependent_content_fixture(),
    ],
)
def test_independent_qa_rejects_each_product_form_family_even_if_classifier_confirms(tmp_path, monkeypatch, row):
    input_db, input_jsonl = _write_fixture_input(tmp_path, [row])

    def false_confirmation(_row, _eligibility):
        return {
            "product_scope_status": foundation.PLUGIN_PRODUCT_CONFIRMED,
            "scope_reason_codes": ["PLUGIN_PRODUCT_EXPLICIT_SOURCE_TEXT"],
            "scope_evidence": [{"reason_code": "PLUGIN_PRODUCT_EXPLICIT_TEXT", "text_span": "plugin"}],
            "scope_confidence": "HIGH",
            "scope_method": "test_false_confirmation",
            "scope_classifier_version": foundation.SCOPE_CLASSIFIER_VERSION,
        }

    monkeypatch.setattr(foundation, "classify_product_form", false_confirmation)
    output_dir = tmp_path / "qa-detects-product-form-regression"
    with pytest.raises(foundation.CategoryFoundationError, match="independent_semantic_contradiction_check_zero"):
        foundation.build_category_foundation(input_db, input_jsonl, output_dir, enforce_pinned_inputs=False)

    qa = json.loads((output_dir / "QA_RESULT.json").read_text(encoding="utf-8"))
    assert qa["checks"]["independent_semantic_contradiction_check_zero"] is False
    assert qa["semantic_contradiction_audit"]["confirmed_contradiction_count"] == 1


@pytest.mark.parametrize(
    "row",
    [
        _row("qa-negation", title="Models", summary="This is not a plugin, these are models."),
        _row("qa-dependency", title="Essentials Configuration", summary="Configuration for the Essentials X Plugin."),
    ],
    ids=("explicit-negation", "dependent-product"),
)
def test_independent_qa_rejects_false_confirmed_scope_even_if_classifier_claims_confirmed(tmp_path, monkeypatch, row):
    input_db, input_jsonl = _write_fixture_input(tmp_path, [row])

    def false_confirmation(_row, _eligibility):
        return {
            "product_scope_status": foundation.PLUGIN_PRODUCT_CONFIRMED,
            "scope_reason_codes": ["PLUGIN_PRODUCT_EXPLICIT_SOURCE_TEXT"],
            "scope_evidence": [{"reason_code": "PLUGIN_PRODUCT_EXPLICIT_TEXT", "text_span": "plugin"}],
            "scope_confidence": "HIGH",
            "scope_method": "test_false_confirmation",
            "scope_classifier_version": foundation.SCOPE_CLASSIFIER_VERSION,
        }

    monkeypatch.setattr(foundation, "classify_product_form", false_confirmation)
    output_dir = tmp_path / "qa-detects-regression"
    with pytest.raises(foundation.CategoryFoundationError, match="independent_semantic_contradiction_check_zero"):
        foundation.build_category_foundation(input_db, input_jsonl, output_dir, enforce_pinned_inputs=False)

    qa = json.loads((output_dir / "QA_RESULT.json").read_text(encoding="utf-8"))
    assert qa["checks"]["independent_semantic_contradiction_check_zero"] is False
    assert qa["semantic_contradiction_audit"]["confirmed_contradiction_count"] == 1


def test_supervisor_false_positive_fixture_build_never_enters_category_memberships(tmp_path):
    rows = _supervisor_blocker_rows() + [row for row, _ in _latest_supervisor_blocker_rows()] + [_plugin_dependent_content_fixture()]
    input_db, input_jsonl = _write_fixture_input(tmp_path, rows)
    output_dir = tmp_path / "supervisor-fixtures"

    qa = foundation.build_category_foundation(input_db, input_jsonl, output_dir, enforce_pinned_inputs=False)

    assert qa["status"] == "PASS"
    assert qa["row_counts"]["plugin_product_confirmed"] == 0
    assert qa["row_counts"]["plugin_category_memberships"] == 0
    assert qa["semantic_contradiction_audit"]["confirmed_contradiction_count"] == 0
    assert qa["semantic_contradiction_audit"]["raw_text_contradiction_identity_count"] == len(rows)
    assert qa["scope_classifier_version"] == "yee-61-product-form-semantic-guard-v0.4"
    manifest = json.loads((output_dir / "DATASET_MANIFEST.json").read_text(encoding="utf-8"))
    assert manifest["scope_classifier_version"] == qa["scope_classifier_version"]
    assert qa["scope_classifier_version"] in (output_dir / "FINAL_REPORT.md").read_text(encoding="utf-8")
    assert (output_dir / "plugin_category_memberships.jsonl").read_text(encoding="utf-8") == ""


@pytest.mark.parametrize(
    ("title", "summary"),
    [
        ("Configurable ClaimGuard", "This Paper plugin offers configurable claim rules and lets operators configure exemptions."),
        ("Maintenance Scheduler", "This Paper plugin manages configurable maintenance windows and configurable messages."),
        ("Setup Wizard", "This Paper plugin guides administrators through setup and exposes configurable defaults."),
    ],
    ids=("configurable-claims", "configurable-maintenance", "in-product-setup"),
)
def test_real_plugin_with_configurable_behavior_is_not_misclassified_as_plugin_dependent_asset(title, summary):
    result = foundation.classify_product_form(_row("configurable-plugin", title=title, summary=summary), _eligible())

    assert result["product_scope_status"] == foundation.PLUGIN_PRODUCT_CONFIRMED
    assert not any(code == "OUT_OF_SCOPE_PLUGIN_DEPENDENT_PRODUCT" for code in result["scope_reason_codes"])


@pytest.mark.parametrize(
    "row",
    [
        _row("3450", source="hangar", title="EnderCore", summary="EnderCore is a central plugin that provides essential functions and resources for other plugins."),
        _row("7126", source="hangar", title="VertexCore", summary="Shared core plugin providing configuration, database and command infrastructure for Paper plugins."),
        _row("7426", title="HardnessControl", summary="Customise the hardness (destroyTime) of blocks through a plugin config file."),
        _row("9280", title="VortexFileSync", summary="Synchronize your plugin configurations effortlessly across multiple servers."),
        _row("9343", title="Free For All Plugin FFA", summary="The ultimate Modern FFA PvP plugin | Editable Kits | Unlimited Kits/Arenas | PAPI"),
        _row("4464", title="ChestEnergistic - FREE", summary="A plugin setup that tries to bring the style and functions of applied energistics to your server."),
    ],
    ids=("shared-plugin-resources", "paper-plugin-infrastructure", "own-config-file", "sync-plugin-configs", "free-for-all-name", "plugin-setup-behavior"),
)
def test_true_plugin_product_and_config_behavior_controls_are_not_independent_contradictions(row):
    result = foundation.classify_product_form(row, _eligible())

    assert result["product_scope_status"] == foundation.PLUGIN_PRODUCT_CONFIRMED
    assert foundation._independent_semantic_contradictions(row) == []


@pytest.mark.parametrize(
    "row",
    [
        _row("7426", title="HardnessControl", summary="Customise the hardness (destroyTime) of blocks through a plugin config file."),
        _row("9280", title="VortexFileSync", summary="Synchronize your plugin configurations effortlessly across multiple servers."),
    ],
    ids=("plugin-owned-config-file", "plugin-config-sync-feature"),
)
def test_plugin_config_features_are_not_product_form_contradictions(row):
    result = foundation.classify_product_form(row, _eligible())

    assert result["product_scope_status"] == foundation.PLUGIN_PRODUCT_CONFIRMED
    assert foundation._independent_semantic_contradictions(row) == []


def test_plugin_dependent_content_assets_are_out_of_scope_and_independently_detected():
    row = _plugin_dependent_content_fixture()
    result = foundation.classify_product_form(row, _eligible())

    assert result["product_scope_status"] == foundation.OUT_OF_SCOPE_PRODUCT_FORM
    assert foundation._independent_semantic_contradictions(row)


def test_compatibility_only_and_unclear_product_form_abstain_to_review():
    compatible_asset = _row("8", title="Vehicle Asset", summary="A resource that works with plugins like VehiclesPlus.")
    unclear = _row("9", title="A project", summary="A Minecraft server resource.")
    behavior_without_software_form = _row("10", title="Class Presets", summary="Creates preset classes for players.")
    addon_without_plugin_form = _row("11", title="Bloom License - Dashboard Addon", summary="Adds an administration panel to Bloom.")

    compatible_result = foundation.classify_product_form(compatible_asset, _eligible())
    unclear_result = foundation.classify_product_form(unclear, _eligible())
    behavior_result = foundation.classify_product_form(behavior_without_software_form, _eligible())
    addon_result = foundation.classify_product_form(addon_without_plugin_form, _eligible())

    assert compatible_result["product_scope_status"] == foundation.PLUGIN_PRODUCT_REVIEW
    assert "PLUGIN_COMPATIBILITY_IS_NOT_PRODUCT_FORM_EVIDENCE" in compatible_result["scope_reason_codes"]
    assert unclear_result["product_scope_status"] == foundation.PLUGIN_PRODUCT_REVIEW
    assert not any("plugin" in item.get("reason_code", "").lower() for item in unclear_result["scope_evidence"])
    assert behavior_result["product_scope_status"] == foundation.PLUGIN_PRODUCT_REVIEW
    assert "SERVER_BEHAVIOR_ALONE_DOES_NOT_ESTABLISH_EXECUTABLE_PLUGIN_FORM" in behavior_result["scope_reason_codes"]
    assert addon_result["product_scope_status"] == foundation.PLUGIN_PRODUCT_REVIEW


def test_scope_guard_cannot_promote_noneligible_or_external_identity():
    row = _row("external", title="This Paper plugin", summary="This plugin adds a server feature.")
    with pytest.raises(foundation.CategoryFoundationError, match="only rows with retained YEE-60 positive evidence"):
        foundation.classify_product_form(row, {**_eligible(), "plugin_eligibility": "AMBIGUOUS"})


def test_hangar_native_function_category_maps_and_uncategorized_is_explicit():
    confirmed = _row("10", source="hangar", title="Maintenance", summary="This Paper plugin schedules server maintenance.", category="admin_tools")
    confirmed_scope = foundation.classify_product_form(confirmed, _eligible())
    direct = foundation.assign_category(confirmed, confirmed_scope)

    assert direct["primary_category_id"] == "administration"
    assert direct["assignment_method"] == "SOURCE_NATIVE_DIRECT"
    assert direct["taxonomy_version"] == foundation.TAXONOMY_VERSION
    assert direct["secondary_category_ids"] == []

    unclear = _row("11", title="Something", summary="This Paper plugin does an unknown server task.")
    unclear_scope = foundation.classify_product_form(unclear, _eligible())
    # No functional category cue: a confirmed plugin stays visible as UNCATEGORIZED.
    assignment = foundation.assign_category(unclear, unclear_scope)
    assert assignment["primary_category_id"] == "uncategorized"
    assert assignment["assignment_method"] == "UNCATEGORIZED"


def test_unknown_source_category_does_not_expand_frozen_taxonomy():
    row = _row("12", source="hangar", title="Maintenance", summary="This Paper plugin maintains server operations.", category="new_unapproved_category")
    result = foundation.assign_category(row, foundation.classify_product_form(row, _eligible()))

    assert result["primary_category_id"] == "administration"  # semantic evidence, not a new taxonomy token
    assert "new_unapproved_category" not in foundation.TAXONOMY_CATEGORY_IDS
    taxonomy = foundation._taxonomy_document([])
    assert taxonomy["taxonomy_status"] == "FROZEN_BEFORE_FULL_ASSIGNMENT"


def test_primary_is_exactly_one_category_and_secondary_is_bounded_deduplicated():
    valid = {
        "primary_category_id": "administration",
        "secondary_category_ids": ["economy", "communication", "gameplay"],
        "taxonomy_version": foundation.TAXONOMY_VERSION,
    }
    foundation._validate_assignment(valid)
    with pytest.raises(foundation.CategoryFoundationError, match="zero to three"):
        foundation._validate_assignment({**valid, "secondary_category_ids": ["economy", "communication", "gameplay", "protection"]})
    with pytest.raises(foundation.CategoryFoundationError, match="deduplicated"):
        foundation._validate_assignment({**valid, "secondary_category_ids": ["economy", "economy"]})
    with pytest.raises(foundation.CategoryFoundationError, match="exclude the primary"):
        foundation._validate_assignment({**valid, "secondary_category_ids": ["administration"]})


def test_demand_price_and_popularity_changes_cannot_change_scope_or_category():
    row = _row("13", title="Base Regenerator Plugin", summary="This Paper plugin gives faction leaders the ability to regenerate griefed claims.")
    scope = foundation.classify_product_form(row, _eligible())
    assignment = foundation.assign_category(row, scope)
    altered = {
        **row,
        "demand_percentile": 1.0,
        "download_count": 999999999,
        "price_amount": 99999.0,
        "paid_state": "PAID",
        "review_count": 999999,
        "star_count": 999999,
        "freshness_age_days": 0,
    }
    altered_scope = foundation.classify_product_form(altered, _eligible())
    altered_assignment = foundation.assign_category(altered, altered_scope)

    assert altered_scope == scope
    assert altered_assignment == assignment


def test_taxonomy_retains_low_volume_roleplay_and_uncategorized_categories():
    taxonomy = foundation._taxonomy_document([])
    ids = [category["category_id"] for category in taxonomy["categories"]]

    assert "roleplay" in ids
    assert "uncategorized" in ids
    assert len(ids) == len(set(ids))
    assert taxonomy["taxonomy_version"] == foundation.TAXONOMY_VERSION


def test_full_fixture_exports_reconcile_are_read_only_and_replay_deterministically(tmp_path):
    rows = _fixture_rows()
    input_db, input_jsonl = _write_fixture_input(tmp_path, rows)
    before = {
        "db": hashlib.sha256(input_db.read_bytes()).hexdigest(),
        "jsonl": hashlib.sha256(input_jsonl.read_bytes()).hexdigest(),
    }
    output_a = tmp_path / "run-a"
    output_b = tmp_path / "run-b"

    qa_a = foundation.build_category_foundation(input_db, input_jsonl, output_a, enforce_pinned_inputs=False)
    qa_b = foundation.build_category_foundation(input_db, input_jsonl, output_b, enforce_pinned_inputs=False)

    assert qa_a["status"] == qa_b["status"] == "PASS"
    assert qa_a["replay"]["byte_identical"] is True
    assert qa_a["row_counts"]["canonical_input"] == 6
    assert qa_a["row_counts"]["plugin_category_memberships"] == qa_a["row_counts"]["plugin_product_confirmed"]
    assert qa_a["scope_status_counts"][foundation.OUT_OF_SCOPE_PRODUCT_FORM] == 3
    assert qa_a["checks"]["demand_price_and_popularity_not_used_in_decision_functions"] is True
    assert {path.name for path in output_a.iterdir()} == set(foundation.FINAL_ARTIFACTS)
    for name in foundation.CORE_ARTIFACTS:
        assert (output_a / name).read_bytes() == (output_b / name).read_bytes()
    with sqlite3.connect(output_a / "category_first_foundation.sqlite") as db:
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []
        assert db.execute("SELECT COUNT(*) FROM plugin_product_scope").fetchone()[0] == 6
        assert db.execute("SELECT COUNT(*) FROM plugin_category_memberships").fetchone()[0] == 2
        assert db.execute("SELECT COUNT(*) FROM plugin_category_inventory").fetchone()[0] == len(foundation.TAXONOMY_CATEGORY_IDS)
        assert db.execute(
            """SELECT COUNT(*) FROM plugin_category_memberships m
               JOIN plugin_product_scope s USING(source,source_resource_id)
               WHERE s.product_scope_status!='PLUGIN_PRODUCT_CONFIRMED'"""
        ).fetchone()[0] == 0
    with (output_a / "plugin_product_scope.csv").open("r", encoding="utf-8", newline="") as stream:
        scope_csv = list(csv.DictReader(stream))
    assert len(scope_csv) == 6
    assert next(row for row in scope_csv if row["canonical_identity"] == "voxel:1000")["price_amount"] == foundation.CSV_NULL
    assert {
        "db": hashlib.sha256(input_db.read_bytes()).hexdigest(),
        "jsonl": hashlib.sha256(input_jsonl.read_bytes()).hexdigest(),
    } == before


def test_builder_rejects_noneligible_row_even_when_identity_is_in_jsonl(tmp_path):
    rows = [_row("1", title="This Paper plugin", summary="This plugin adds a server feature.")]
    input_db, input_jsonl = _write_fixture_input(tmp_path, rows)
    with sqlite3.connect(input_db) as db:
        db.execute("UPDATE plugin_eligibility SET plugin_eligibility='AMBIGUOUS'")
    with pytest.raises(foundation.CategoryFoundationError, match="Non-eligible YEE-60 row leaked"):
        foundation.build_category_foundation(input_db, input_jsonl, tmp_path / "invalid", enforce_pinned_inputs=False)
