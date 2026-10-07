#!/usr/bin/env python3
"""Pre-download patchability verdict for one patch-fan anchor.

The release patch fan reconstitutes every anchor's DB from that release's
1.3 GB ``seforim.db.zst`` (110-135 s download plus ~7 s decompress) before
``PatchDbProducer`` ever gets to say the pair cannot be expressed as a delta at
all: run 33865604251 spent ~140 s on the v10 anchor only to be handed
``UnpatchableAnchorException``.  This module answers the same question from a
few KB of metadata.

It is strictly advisory, and asymmetric on purpose:

* ``PROCEED`` is not a patchability claim.  It means "not provably unpatchable
  from metadata"; the producer keeps being the single authority and still says
  so with exit code 3 plus the ``<out>.unpatchable`` marker.
* ``UNPATCHABLE`` costs the fan at most one anchor, which the workflow already
  treats as a warning.  A release that loses *every* anchor still fails loudly
  in the fan's final check.

Two sources, cheapest first:

1. :data:`LEGACY_UNPATCHABLE_DB_VERSIONS` - releases published before this
   metadata existed that the repository's own history proves carry a dropped
   column.
2. The anchor release's ``build_provenance.json`` ``db_schema`` block (written
   by ``dump`` below, from db_version 27 on -- v26 was published 2026-09-04
   with ``schema_version`` 3 and no such block): the exact physical column set
   of the DB that release published.  Applying the producer's own first rule to it
   - a column that exists in prev's table and no longer exists in new's - is
   exact, not heuristic.

The comparison is scoped exactly like `PatchDbProducer.planColumnMigrations`:
only the tables of the patch contract (`patch_tables_contract.json`, the same
fixture `PatchTablesContractTest` freezes) and, of those, only the ones the
producer would actually column-diff.  A physical table outside the contract, or
one *promoted* into the contract by the schema bump, is skipped there and must
be skipped here too - otherwise a dropped column in such a table would cost the
fan a patch the producer would have produced.  Whenever that scope cannot be
established, the verdict is PROCEED; it never widens to "every physical table".

``dump`` is deliberately the *only* producer of that block, so what a build
publishes about itself and what a later build compares against are the same
bytes by construction.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

# Anchors that predate the `db_schema` provenance block and are known - not
# guessed - to be unpatchable, so they can be skipped without a download:
#
#   * commit 7755902 (2026-07-14) replaced `link.isDeclaredBase` with
#     `link.baseProvenance` without bumping `db_schema_version`, so DBs
#     released before it still claim schema 1 while carrying a column the
#     current schema no longer declares - exactly the shape
#     `PatchDbProducer.planColumnMigrations` calls unpatchable;
#   * commit 849f754 ("feat(patch): add missing columns and indexes by
#     migration instead of skipping the anchor") records the measured range:
#     "Verified on real data: v9 (with its dropped link.isDeclaredBase removed)
#     to v25 produced 15 migrations and passed the apply-hash check. v9 to v13
#     remain skipped until the dropped column is handled.";
#   * run 33865604251 (building v26) re-confirmed it for the v10 anchor.
#
# The lower bound is reachability, not the start of the damage: v1-v8 predate
# 7755902 too (v8 was published 2026-07-06, v13 2026-07-09, v14 2026-07-19 --
# the first release built after the swap), but no offset in {1,2,4,8,16} can
# reach below v11 from db_version 27 on, so listing them would be dead code.
# Deliberately a closed set, not an open-ended "everything below N": v14-v25
# are known-good (v25 was produced from the same evidence) and must keep being
# offered to the producer.  The whole list can be deleted once db_version 30
# ships, when offset 16 no longer reaches v13.
LEGACY_UNPATCHABLE_DB_VERSIONS = frozenset(range(9, 14))

PROCEED = "PROCEED"
UNPATCHABLE = "UNPATCHABLE"
# Not a patch at all: the fan publishes the full-rebase barrier for the anchor.
# The line is "BARRIER <anchor db schema> <reason>".
BARRIER = "BARRIER"

# Mirrors PatchTables.kt LINE_CONTENT_SPLIT_SCHEMA_VERSION: no delta crosses it.
LINE_CONTENT_SPLIT_SCHEMA = 6


def _one_line(text: str) -> str:
    """Collapse a reason to the single line the workflow echoes."""
    return " ".join(str(text).split())


def dump(db_path: Path) -> dict:
    """The published DB's physical schema: version plus columns per table.

    Every table in seforim.db is declared in Database.sq; all but the optional
    side-channel tables are patch tables, and check() reads only the contract ones.  The
    column lists are sorted the same way PatchTableColumnContractTest sorts
    them, and the whole object is emitted as canonical JSON so it can be nested
    inside build_provenance.json without breaking that file's byte-canonical
    contract.
    """
    if not db_path.is_file():
        raise ValueError(f"{db_path} is not a file")
    try:
        conn = sqlite3.connect(f"file:{db_path.resolve().as_posix()}?mode=ro", uri=True)
        conn.execute("SELECT 1 FROM sqlite_master LIMIT 1")
    except sqlite3.Error:
        # A database left in WAL mode cannot always be opened read-only —
        # SQLite wants to create the -shm file. build/ is writable and nothing
        # below writes, so a normal connection is the safe fallback rather than
        # a failed release step.
        conn = sqlite3.connect(db_path)
    try:
        names = sorted(
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        )
        tables = {}
        for name in names:
            quoted = name.replace('"', '""')
            tables[name] = sorted(
                row[1] for row in conn.execute(f'PRAGMA table_info("{quoted}")')
            )
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key='db_schema_version'"
        ).fetchone()
    finally:
        conn.close()
    if row is None or not str(row[0]).strip():
        raise ValueError(
            f"db_schema_version row missing in {db_path} (release DB must be stamped)"
        )
    if not tables:
        raise ValueError(f"no tables found in {db_path}")
    return {"db_schema_version": int(row[0]), "tables": tables}


def _anchor_db_schema(provenance_path: Path) -> tuple[dict | None, str]:
    """The anchor's recorded db_schema block, or None plus why it is unusable."""
    if not provenance_path.exists():
        return None, f"{provenance_path.name} is not published by that release"
    value = json.loads(provenance_path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        return None, "anchor provenance is not an object"
    schema = value.get("db_schema")
    if schema is None:
        return None, "anchor provenance predates the db_schema block"
    if not isinstance(schema, dict) or not isinstance(schema.get("tables"), dict):
        return None, "anchor provenance carries an unreadable db_schema block"
    if not all(isinstance(columns, list) for columns in schema["tables"].values()):
        return None, "anchor provenance carries an unreadable db_schema block"
    return schema, ""


def _this_schema_version(path: Path) -> int | None:
    """This build's db_schema_version from its dump, or None while it does not exist yet."""
    try:
        version = json.loads(path.read_text(encoding="utf-8")).get("db_schema_version")
    except (OSError, UnicodeDecodeError, ValueError, AttributeError):
        return None
    return version if type(version) is int else None


def _contract(path: Path) -> tuple[list, int]:
    """(table names in FK order, schemaVersion) from a patch-table contract fixture.

    The fixture is `generator/common/src/jvmTest/resources/patch_tables_contract.json`
    of the payload checkout — the same commit whose PatchDbProducer runs — and
    `PatchTablesContractTest` asserts byte-for-byte that it is the serialised
    `PATCH_TABLES_IN_FK_ORDER`, so it cannot drift from the producer's list.
    """
    value = json.loads(path.read_text(encoding="utf-8"))
    tables = [entry["table"] for entry in value["fkOrder"]]
    if not tables or not all(isinstance(table, str) and table for table in tables):
        raise ValueError(f"{path.name} carries no usable fkOrder")
    version = value["schemaVersion"]
    if type(version) is not int:
        raise ValueError(f"{path.name} carries no usable schemaVersion")
    return tables, version


def _promoted_tables(
    contract_path: Path, contract_tables: list, contract_version: int, anchor_schema_version
) -> tuple[frozenset | None, str]:
    """Tables the producer refuses to column-diff because the bump promoted them.

    `PatchDbProducer` computes `promotedTables = patchTablesForSchemaVersion(to)
    - patchTablesForSchemaVersion(from)` and `continue`s on each of them, so a
    promoted table that happens to exist physically in the anchor is never
    compared there.  The same subtraction is available here from the frozen
    per-schema fixtures that sit next to the current one
    (`patch_tables_contract_schema_<N>.json`).  When the anchor's schema has no
    frozen fixture the promoted set is not derivable, and an undecidable scope
    means PROCEED rather than a comparison that might skip a good anchor.
    """
    if type(anchor_schema_version) is not int:
        return None, "the anchor's db_schema block does not state its db_schema_version"
    if anchor_schema_version == contract_version:
        return frozenset(), ""
    if anchor_schema_version > contract_version:
        return None, (
            f"the anchor claims db schema {anchor_schema_version}, newer than this build's "
            f"{contract_version}"
        )
    sibling = contract_path.with_name(
        f"patch_tables_contract_schema_{anchor_schema_version}.json"
    )
    if not sibling.is_file():
        return None, (
            f"no frozen patch-table contract for db schema {anchor_schema_version} "
            f"({sibling.name}), so the promoted-table set is not derivable"
        )
    older_tables, older_version = _contract(sibling)
    if older_version != anchor_schema_version:
        return None, f"{sibling.name} describes db schema {older_version}, not {anchor_schema_version}"
    return frozenset(contract_tables) - frozenset(older_tables), ""


def check(
    this_schema_path: Path,
    anchor_version: int,
    provenance_path: Path,
    contract_path: Path | None,
) -> tuple[str, str]:
    """Return (verdict, reason) for one anchor, without downloading its DB.

    The documented list is consulted before anything is read from disk, so a
    known-unpatchable anchor is skipped even if this build's own schema dump is
    unreadable for some reason.  A build past the line_content split asks first
    whether the anchor is on the other side of it: then no delta exists at all.
    """
    this_version = _this_schema_version(this_schema_path)
    if this_version is not None and this_version >= LINE_CONTENT_SPLIT_SCHEMA:
        anchor_schema, why = _anchor_db_schema(provenance_path)
        anchor_db_schema = anchor_schema.get("db_schema_version") if anchor_schema else None
        if type(anchor_db_schema) is not int:
            return PROCEED, (
                f"the anchor's db schema is unknown ({why or 'no db_schema_version'}) - the fan "
                "reads it from the downloaded DB"
            )
        if anchor_db_schema < LINE_CONTENT_SPLIT_SCHEMA:
            return BARRIER, (
                f"{anchor_db_schema} the anchor is db schema {anchor_db_schema}; schema "
                f"{this_version} keeps line text in line_content, so no delta reaches it - "
                "publishing the full-rebase barrier"
            )
    elif anchor_version in LEGACY_UNPATCHABLE_DB_VERSIONS:
        return UNPATCHABLE, (
            f"db_version {anchor_version} predates the db_schema provenance block and is on "
            "the documented unpatchable list (link.isDeclaredBase was dropped without a "
            "db_schema_version bump, commit 7755902)"
        )

    anchor_schema, why = _anchor_db_schema(provenance_path)
    if anchor_schema is None:
        return PROCEED, f"no cheap evidence ({why}) - deferring to the producer"
    anchor_tables = anchor_schema["tables"]

    if contract_path is None or not contract_path.is_file():
        return PROCEED, (
            "the patch-table contract fixture is unavailable, and the comparison must never "
            "widen to every physical table - deferring to the producer"
        )
    contract_tables, contract_version = _contract(contract_path)

    this_schema = json.loads(this_schema_path.read_text(encoding="utf-8"))
    if this_schema.get("db_schema_version") != contract_version:
        return PROCEED, (
            f"this build's DB is db schema {this_schema.get('db_schema_version')} but the "
            f"contract fixture freezes db schema {contract_version} - deferring to the producer"
        )
    new_tables = this_schema["tables"]

    promoted, why = _promoted_tables(
        contract_path, contract_tables, contract_version, anchor_schema.get("db_schema_version")
    )
    if promoted is None:
        return PROCEED, f"{why} - deferring to the producer"

    # Exactly the producer's own loop: contract tables only, promoted ones
    # skipped, and a table absent from either side never column-diffed (the
    # producer (re)creates a table missing from prev from a full snapshot).
    for table in contract_tables:
        if table in promoted or table not in new_tables or table not in anchor_tables:
            continue
        prev_columns = anchor_tables[table]
        dropped = [c for c in prev_columns if c not in set(new_tables[table])]
        if dropped:
            return UNPATCHABLE, (
                f"table '{table}' in the previous DB carries column(s) "
                f"{', '.join(dropped)} that the new schema no longer declares"
            )
    return PROCEED, (
        "every patch-contract column the anchor records survives in this build's schema"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    dump_parser = sub.add_parser("dump", help="print this DB's canonical schema JSON")
    dump_parser.add_argument("db")

    check_parser = sub.add_parser("check", help="verdict for one patch-fan anchor")
    check_parser.add_argument("--this-schema", required=True)
    check_parser.add_argument("--anchor-version", required=True, type=int)
    check_parser.add_argument("--anchor-provenance", required=True)
    # Optional on purpose: an absent flag degrades to PROCEED like an absent
    # file, so the fan can never be widened by forgetting it.
    check_parser.add_argument("--contract-tables")

    args = parser.parse_args(argv)

    if args.command == "dump":
        value = dump(Path(args.db))
        sys.stdout.write(
            json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        )
        return 0

    # The check must never be the reason a release fails: any surprise degrades
    # to PROCEED and the producer decides as it always did.
    try:
        verdict, reason = check(
            Path(args.this_schema),
            args.anchor_version,
            Path(args.anchor_provenance),
            Path(args.contract_tables) if args.contract_tables else None,
        )
    except (OSError, UnicodeDecodeError, ValueError, KeyError, TypeError) as exc:
        verdict, reason = PROCEED, f"pre-check error ({exc.__class__.__name__}) - deferring to the producer"
    print(f"{verdict} {_one_line(reason)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
