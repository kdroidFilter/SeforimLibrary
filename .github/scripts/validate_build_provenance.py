#!/usr/bin/env python3
"""Strict canonical contract for a published Seforim build provenance."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re


SHA40 = re.compile(r"[0-9a-f]{40}")
SHA64 = re.compile(r"[0-9a-f]{64}")
TAG = re.compile(r"[A-Za-z0-9._-]{1,150}")
V1_KEYS = {
    "schema_version", "correlation_id", "source_commit", "sefaria_tag",
    "sefaria_release_metadata_sha256", "sefaria_archive_sha256", "otzaria_tag",
    "otzaria_asset_sha256", "expected_links_commit", "otzaria_target_commit",
    "lineage_sha256",
    "config_sha256", "source_links_tree_sha256", "packaged_links_tree_sha256", "assets",
}
V2_KEYS = V1_KEYS | {
    "fordb_archive_sha256", "fordb_tag", "linker_payload_sha256",
    "linker_engine_fingerprint", "linker_relink_run_id", "linker_commit",
    "linker_relink_run_attempt", "linker_relink_request_id",
}
V3_KEYS = V2_KEYS | {"phase2_implementation_commit"}
# v4 publishes the physical schema of the DB this release ships: db_schema_version
# plus the sorted column list of every table. A later build reads it straight off
# the release (a few KB) to decide whether this release can serve as a patch-fan
# anchor, instead of downloading and extracting its 1.3 GB seforim.db.zst first.
# Older published provenances stay valid as v1/v2/v3 — they simply carry no such
# evidence, and the patch fan then defers to the producer exactly as before.
V4_KEYS = V3_KEYS | {"db_schema"}
# v5 stops publishing lines_snapshot.db.zst on the DB release — the identical
# bytes already live on the immutable content-addressed pre-release the build
# published before its relink. The release therefore names that pre-release
# instead: snapshot_zst_sha256 is the digest of the snapshot THIS RELEASE'S LINK
# SET was produced from — this build's own lines_snapshot.db.zst on every normal
# path, and on the recovery path the earlier attempt's snapshot, which the
# rebuild is proven line-equivalent to and which is the only one still
# resolvable — and snapshot_release_tag is the release carrying it, so a consumer
# (the LinkerToOtzaria manual relink) can resolve the snapshot and verify its
# bytes fail-closed. Older published provenances stay valid as v1..v4 — they carry the
# asset itself instead.
V5_KEYS = V4_KEYS | {"snapshot_zst_sha256", "snapshot_release_tag"}
# v6 is written only by a build that merged a private books release (repo, tag
# and asset digest); a build without one keeps publishing v5 byte-for-byte.
V6_KEYS = V5_KEYS | {"private_books"}
PRIVATE_REPO = re.compile(r"[A-Za-z0-9-]{1,39}/[A-Za-z0-9._-]{1,100}")
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
LEGACY_FULL_DB_ASSET = "seforim.db.zst"


def full_db_asset_name(db_schema_version: int) -> str:
    """Mirror of db_asset_names.sh full_db_asset_name (a test pins the two)."""
    return f"seforim-schema{db_schema_version}.db.zst" if db_schema_version >= 6 else LEGACY_FULL_DB_ASSET


def load(path: Path) -> dict:
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError(f"duplicate key {key!r}")
            value[key] = item
        return value

    raw = path.read_bytes()
    value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise ValueError("build provenance must be an object")
    version = value.get("schema_version")
    if type(version) is not int or version not in (1, 2, 3, 4, 5, 6):
        raise ValueError("schema_version must be integer 1, 2, 3, 4, 5 or 6")
    expected_keys = {1: V1_KEYS, 2: V2_KEYS, 3: V3_KEYS, 4: V4_KEYS, 5: V5_KEYS, 6: V6_KEYS}[version]
    if set(value) != expected_keys:
        raise ValueError("unknown build provenance key set")
    canonical = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode() + b"\n"
    if raw != canonical:
        raise ValueError("build provenance is not canonical JSON with one trailing LF")
    return value


def validate(value: dict) -> None:
    version = value["schema_version"]
    correlation = value["correlation_id"]
    if not isinstance(correlation, str):
        raise ValueError("correlation_id must be a string")
    match = re.fullmatch(
        r"sefaria:([1-9][0-9]*):([1-9][0-9]*):([A-Za-z0-9._-]{1,100}):([0-9a-f]{64})",
        correlation,
    )
    if not match:
        raise ValueError("invalid correlation_id")
    for field in ("source_commit", "expected_links_commit", "otzaria_target_commit"):
        if not isinstance(value[field], str) or not SHA40.fullmatch(value[field]):
            raise ValueError(f"invalid {field}")
    for field in (
        "sefaria_release_metadata_sha256", "sefaria_archive_sha256", "otzaria_asset_sha256",
        "lineage_sha256", "config_sha256", "source_links_tree_sha256",
        "packaged_links_tree_sha256",
    ):
        if not isinstance(value[field], str) or not SHA64.fullmatch(value[field]):
            raise ValueError(f"invalid {field}")
    for field in ("sefaria_tag", "otzaria_tag"):
        if not isinstance(value[field], str) or not TAG.fullmatch(value[field]):
            raise ValueError(f"invalid {field}")
    if value["sefaria_tag"] != match.group(3) or value["sefaria_release_metadata_sha256"] != match.group(4):
        raise ValueError("correlation_id disagrees with pinned Sefaria fields")
    if value["expected_links_commit"] != value["otzaria_target_commit"]:
        raise ValueError("Otzaria target differs from expected links commit")
    if version >= 2:
        if not isinstance(value["linker_commit"], str) or not SHA40.fullmatch(value["linker_commit"]):
            raise ValueError("invalid linker_commit")
        for field in (
            "fordb_archive_sha256", "linker_payload_sha256", "linker_relink_request_id",
        ):
            if not isinstance(value[field], str) or not SHA64.fullmatch(value[field]):
                raise ValueError(f"invalid {field}")
        if value["fordb_tag"] != "fordb-sha256-" + value["fordb_archive_sha256"]:
            raise ValueError("ForDB tag does not match archive digest")
        for field in ("linker_relink_run_id", "linker_relink_run_attempt"):
            if type(value[field]) is not int or value[field] < 1:
                raise ValueError(f"{field} must be a positive integer")
        fingerprint = value["linker_engine_fingerprint"]
        if not isinstance(fingerprint, str) or not re.fullmatch(r"[\x20-\x7e]{1,4096}", fingerprint):
            raise ValueError("invalid linker_engine_fingerprint")
    if version >= 3:
        phase2_commit = value["phase2_implementation_commit"]
        if not isinstance(phase2_commit, str) or not SHA40.fullmatch(phase2_commit):
            raise ValueError("invalid phase2_implementation_commit")
    if version >= 4:
        db_schema = value["db_schema"]
        if not isinstance(db_schema, dict) or set(db_schema) != {"db_schema_version", "tables"}:
            raise ValueError("db_schema must carry exactly db_schema_version and tables")
        if type(db_schema["db_schema_version"]) is not int or db_schema["db_schema_version"] < 1:
            raise ValueError("db_schema.db_schema_version must be a positive integer")
        tables = db_schema["tables"]
        if not isinstance(tables, dict) or not tables:
            raise ValueError("db_schema.tables must be a non-empty object")
        for table, columns in tables.items():
            if not IDENTIFIER.fullmatch(table):
                raise ValueError(f"invalid db_schema table name {table!r}")
            if not isinstance(columns, list) or not columns:
                raise ValueError(f"db_schema table {table!r} must list its columns")
            if not all(isinstance(column, str) and IDENTIFIER.fullmatch(column) for column in columns):
                raise ValueError(f"invalid column name in db_schema table {table!r}")
            if columns != sorted(columns) or len(columns) != len(set(columns)):
                raise ValueError(f"db_schema table {table!r} columns must be unique and sorted")
    if version >= 5:
        snapshot_sha256 = value["snapshot_zst_sha256"]
        if not isinstance(snapshot_sha256, str) or not SHA64.fullmatch(snapshot_sha256):
            raise ValueError("invalid snapshot_zst_sha256")
        if value["snapshot_release_tag"] != "lines-snapshot-sha256-" + snapshot_sha256:
            raise ValueError("snapshot release tag does not match the snapshot digest")
    if version >= 6:
        private = value["private_books"]
        if not isinstance(private, dict) or set(private) != {"repo", "tag", "asset_sha256"}:
            raise ValueError("private_books must carry exactly repo, tag and asset_sha256")
        if not isinstance(private["repo"], str) or not PRIVATE_REPO.fullmatch(private["repo"]):
            raise ValueError("invalid private_books.repo")
        if not isinstance(private["tag"], str) or not TAG.fullmatch(private["tag"]):
            raise ValueError("invalid private_books.tag")
        if not isinstance(private["asset_sha256"], str) or not SHA64.fullmatch(private["asset_sha256"]):
            raise ValueError("invalid private_books.asset_sha256")
    assets = value["assets"]
    if not isinstance(assets, list) or not assets:
        raise ValueError("assets must be a non-empty array")
    names = []
    for index, asset in enumerate(assets):
        if not isinstance(asset, dict) or set(asset) != {"name", "size", "sha256"}:
            raise ValueError(f"invalid asset descriptor {index}")
        name = asset["name"]
        if not isinstance(name, str) or not name or Path(name).name != name:
            raise ValueError(f"invalid asset name {index}")
        if type(asset["size"]) is not int or asset["size"] < 1:
            raise ValueError(f"invalid asset size {index}")
        if not isinstance(asset["sha256"], str) or not SHA64.fullmatch(asset["sha256"]):
            raise ValueError(f"invalid asset digest {index}")
        names.append(name)
    if names != sorted(names, key=lambda item: item.encode("utf-8")) or len(names) != len(set(names)):
        raise ValueError("asset names must be unique and bytewise sorted")
    # From v5 the buildstate ships compressed (~2x off a 990 MB SQLite file) and
    # the duplicate lines snapshot is not published on the DB release at all —
    # snapshot_release_tag names the immutable pre-release that carries it.
    # v1..v4 keep the asset set they were published with.
    # The full DB is named by its schema (db_asset_names.sh); a provenance without a
    # db_schema block predates schema 6, i.e. its DB is seforim.db.zst.
    full_db = full_db_asset_name(value["db_schema"]["db_schema_version"]) if version >= 4 else LEGACY_FULL_DB_ASSET
    if version >= 5:
        required = {full_db, "seforim.db.buildstate.zst"}
        forbidden = {"seforim.db.buildstate", "lines_snapshot.db.zst"}
    else:
        required = {full_db, "seforim.db.buildstate", "lines_snapshot.db.zst"}
        forbidden = set()
    # seforim.db.zst is matched by name by every released updater: never a schema-6+ DB.
    if full_db != LEGACY_FULL_DB_ASSET:
        forbidden = forbidden | set(full_db_asset_forms(LEGACY_FULL_DB_ASSET, names))
    if not required.issubset(published_names(full_db, names)):
        raise ValueError("required build assets are missing")
    if full_db in names and len(full_db_asset_forms(full_db, names)) > 1:
        raise ValueError("the full DB is published both whole and in parts")
    if forbidden & set(names):
        raise ValueError("build assets superseded by this schema version are still published")


def full_db_asset_forms(full_db: str, names) -> list:
    """Every asset of <full_db>: the file itself, or its split manifest and parts."""
    part = re.compile(re.escape(full_db) + r"\.part-[0-9]{3,}")
    return [n for n in names if n in (full_db, full_db + ".manifest.json") or part.fullmatch(n)]


def published_names(full_db: str, names) -> set:
    """<names>, counting <full_db> as present when it ships as manifest + parts only (stage_full_db)."""
    forms = set(full_db_asset_forms(full_db, names))
    split = full_db + ".manifest.json" in forms and len(forms) >= 2 and full_db not in forms
    return set(names) | ({full_db} if split else set())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    # The reconcile reuse scan runs this validator once per candidate release
    # (~27 of them), where one positive line each is noise, not evidence: that
    # loop reports its own summary. The real validation call sites — the staged
    # document this build publishes — keep the line.
    parser.add_argument("--quiet", action="store_true",
                        help="suppress the positive line; failures still report")
    args = parser.parse_args()
    try:
        value = load(Path(args.path))
        validate(value)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        print(f"build provenance contract error: {exc}", file=__import__("sys").stderr)
        return 2
    # Positive evidence, one line. Without it a green step proved nothing: a
    # silently stubbed validator and ~190 lines of contract checking looked
    # exactly alike in the log. Says which document was checked, at which schema
    # version, and how many of the two countable things it covers.
    if not args.quiet:
        print(
            f"ok: build_provenance v{value['schema_version']}, {len(value)} fields, "
            f"{len(value['assets'])} assets, "
            f"source_commit={value['source_commit'][:12]} ({args.path})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
