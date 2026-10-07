#!/usr/bin/env python3
"""Lay a private books archive out as extra source folder(s) of the Otzaria extract.

The archive (``private_books.zip``) carries, at its root:

* ``אוצריא/...`` - the book files, in the same tree the public library uses;
* ``files_manifest.json`` - ``{"<SourceName>/אוצריא/<rel>": {"hash": "<sha256>"}}``
  for every one of those files, exactly like the public ``files_manifest*.json``.

Every file is copied under ``<extract root>/אוצריא`` and every manifest entry is
merged into each ``files_manifest*.json`` present at the extract root, so the
generator assigns the books to their private source like any public one.
Fail closed: a missing or extra file, a hash mismatch, a path that already
exists in the public tree, or a source name the public manifest already uses.
"""

from __future__ import annotations

import hashlib
import json
import sys
import zipfile
from pathlib import Path, PurePosixPath

LIBRARY_DIR = "אוצריא"
MANIFEST = "files_manifest.json"
PUBLIC_MANIFESTS = ("files_manifest.json", "files_manifest_new.json")


def _source_of(key: str) -> str:
    parts = key.split("/")
    if len(parts) < 3 or parts[1] != LIBRARY_DIR or not parts[0] or parts[0] == LIBRARY_DIR:
        raise ValueError(f"manifest key {key!r} is not <SourceName>/{LIBRARY_DIR}/<path>")
    return parts[0]


def _public_sources(manifest: dict) -> set:
    return {key.split("/")[0] for key in manifest if f"/{LIBRARY_DIR}/" in key}


def merge(archive: Path, extract_root: Path) -> dict:
    """Merges [archive] into [extract_root]; returns {source name: book file count}."""
    with zipfile.ZipFile(archive) as zf:
        manifest = json.loads(zf.read(MANIFEST).decode("utf-8-sig"))
        if not isinstance(manifest, dict) or not manifest:
            raise ValueError(f"{MANIFEST} must be a non-empty object")
        by_rel = {}
        counts: dict = {}
        for key, entry in manifest.items():
            source = _source_of(key)
            if not isinstance(entry, dict) or not isinstance(entry.get("hash"), str):
                raise ValueError(f"manifest entry {key!r} has no hash")
            rel = "/".join(key.split("/")[1:])
            if rel in by_rel:
                raise ValueError(f"{rel!r} is listed twice")
            by_rel[rel] = entry["hash"].lower()
            counts[source] = counts.get(source, 0) + 1

        files = {}
        for info in zf.infolist():
            if info.is_dir() or info.filename == MANIFEST:
                continue
            path = PurePosixPath(info.filename)
            if path.is_absolute() or ".." in path.parts or path.parts[0] != LIBRARY_DIR:
                raise ValueError(f"unexpected archive entry {info.filename!r}")
            files[info.filename] = info
        if set(files) != set(by_rel):
            raise ValueError(
                f"archive files and {MANIFEST} differ: "
                f"unlisted={sorted(set(files) - set(by_rel))[:5]} missing={sorted(set(by_rel) - set(files))[:5]}"
            )

        publics = [extract_root / name for name in PUBLIC_MANIFESTS if (extract_root / name).is_file()]
        if not publics:
            raise ValueError(f"no public files_manifest*.json under {extract_root}")
        loaded = {p: json.loads(p.read_text(encoding="utf-8-sig")) for p in publics}
        taken = set().union(*(_public_sources(m) for m in loaded.values()))
        clash = sorted(set(counts) & taken)
        if clash:
            raise ValueError(f"private source name(s) already used by the public library: {clash}")

        for rel, info in sorted(files.items()):
            data = zf.read(info)
            if hashlib.sha256(data).hexdigest() != by_rel[rel]:
                raise ValueError(f"hash mismatch for {rel!r}")
            target = extract_root.joinpath(*PurePosixPath(rel).parts)
            if target.exists():
                raise ValueError(f"{rel!r} already exists in the public library")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)

    for path, public in loaded.items():
        overlap = set(public) & set(manifest)
        if overlap:
            raise ValueError(f"{path.name} already lists {sorted(overlap)[:5]}")
        public.update(manifest)
        path.write_text(json.dumps(public, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return counts


def main(argv: list) -> int:
    if len(argv) != 3:
        print("usage: merge_private_books.py <private_books.zip> <otzaria extract root>", file=sys.stderr)
        return 2
    try:
        counts = merge(Path(argv[1]), Path(argv[2]))
    except (OSError, ValueError, KeyError, zipfile.BadZipFile, json.JSONDecodeError) as exc:
        print(f"private books merge failed: {exc}", file=sys.stderr)
        return 1
    print("private books merged: " + ", ".join(f"{s}={n}" for s, n in sorted(counts.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
