"""Refuses to index a DB whose line_note bodies are zstd frames with an engine that cannot read them.

Usage: check_note_body_engine.py <seforim.db> <otzaria_search_engine version from pubspec.lock>

From the release that compresses line_note.body (schema 7, compressLineContent), only
otzaria_search_engine 5.0.1 and later decode a BLOB body; 5.0.0 skips it, and the index
would hold every book without its notes, stamped with an unchanged contentHash.
"""

import re
import sqlite3
import sys

FIRST_DECODING_ENGINE = (5, 0, 1)


def blob_bodies(db_path):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'line_note'").fetchone():
            return False
        return conn.execute("SELECT EXISTS(SELECT 1 FROM line_note WHERE typeof(body) = 'blob')").fetchone()[0] == 1
    finally:
        conn.close()


def main(argv):
    if len(argv) != 3:
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 2
    db_path, version = argv[1], argv[2]
    if not blob_bodies(db_path):
        print("line_note bodies are not compressed; any engine reads them")
        return 0
    match = re.fullmatch(r"([0-9]+)\.([0-9]+)\.([0-9]+)", version)
    if not match:
        print(f"::error::otzaria_search_engine version '{version}' is not MAJOR.MINOR.PATCH; "
              "cannot prove it decodes compressed line_note bodies")
        return 1
    found = tuple(int(part) for part in match.groups())
    required = ".".join(map(str, FIRST_DECODING_ENGINE))
    if found < FIRST_DECODING_ENGINE:
        print(f"::error::the database keeps line_note bodies as zstd frames, which otzaria_search_engine "
              f"{version} skips; the index needs {required} or later")
        return 1
    print(f"line_note bodies are zstd frames; otzaria_search_engine {version} decodes them (>= {required})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
