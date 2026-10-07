#!/usr/bin/env python3
"""merge_private_books.py lays the private archive out as an extra source, fail closed."""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import merge_private_books as mpb  # noqa: E402

LIB = "אוצריא"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class MergePrivateBooksTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.root = self.tmp / "extract"
        (self.root / LIB / "הלכה").mkdir(parents=True)
        public = self.root / LIB / "הלכה" / "ציבורי.txt"
        public.write_bytes(b"public")
        self.public_manifest = {f"MoreBooks/{LIB}/הלכה/ציבורי.txt": {"hash": sha(b"public")}}
        for name in ("files_manifest.json", "files_manifest_new.json"):
            (self.root / name).write_text(json.dumps(self.public_manifest, ensure_ascii=False), encoding="utf-8")

    def archive(self, files: dict, manifest: dict | None = None) -> Path:
        path = self.tmp / "private_books.zip"
        if manifest is None:
            manifest = {f"Private/{rel}": {"hash": sha(data)} for rel, data in files.items()}
        with zipfile.ZipFile(path, "w") as zf:
            zf.writestr("files_manifest.json", json.dumps(manifest, ensure_ascii=False))
            for rel, data in files.items():
                zf.writestr(rel, data)
        return path

    def test_files_and_manifest_entries_are_merged(self):
        counts = mpb.merge(self.archive({f"{LIB}/הלכה/פרטי.txt": b"private"}), self.root)
        self.assertEqual({"Private": 1}, counts)
        self.assertEqual(b"private", (self.root / LIB / "הלכה" / "פרטי.txt").read_bytes())
        for name in ("files_manifest.json", "files_manifest_new.json"):
            merged = json.loads((self.root / name).read_text(encoding="utf-8"))
            self.assertIn(f"Private/{LIB}/הלכה/פרטי.txt", merged)
            self.assertIn(f"MoreBooks/{LIB}/הלכה/ציבורי.txt", merged)

    def test_hash_mismatch_fails(self):
        archive = self.archive({f"{LIB}/x.txt": b"a"}, {f"Private/{LIB}/x.txt": {"hash": sha(b"b")}})
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            mpb.merge(archive, self.root)

    def test_unlisted_file_fails(self):
        archive = self.archive({f"{LIB}/x.txt": b"a", f"{LIB}/y.txt": b"b"}, {f"Private/{LIB}/x.txt": {"hash": sha(b"a")}})
        with self.assertRaisesRegex(ValueError, "differ"):
            mpb.merge(archive, self.root)

    def test_public_path_collision_fails(self):
        with self.assertRaisesRegex(ValueError, "already exists"):
            mpb.merge(self.archive({f"{LIB}/הלכה/ציבורי.txt": b"other"}), self.root)

    def test_public_source_name_fails(self):
        data = b"x"
        archive = self.archive({f"{LIB}/x.txt": data}, {f"MoreBooks/{LIB}/x.txt": {"hash": sha(data)}})
        with self.assertRaisesRegex(ValueError, "already used"):
            mpb.merge(archive, self.root)

    def test_entry_outside_the_library_fails(self):
        with self.assertRaisesRegex(ValueError, "unexpected archive entry"):
            mpb.merge(self.archive({"../evil.txt": b"x"}, {f"Private/{LIB}/x.txt": {"hash": sha(b"x")}}), self.root)


    def test_missing_public_manifest_fails(self):
        for name in ("files_manifest.json", "files_manifest_new.json"):
            (self.root / name).unlink()
        with self.assertRaisesRegex(ValueError, "no public files_manifest"):
            mpb.merge(self.archive({f"{LIB}/x.txt": b"x"}), self.root)
        self.assertFalse((self.root / LIB / "x.txt").exists())

    def test_malformed_manifest_key_fails(self):
        archive = self.archive({f"{LIB}/x.txt": b"x"}, {f"{LIB}/x.txt": {"hash": sha(b"x")}})
        with self.assertRaisesRegex(ValueError, "is not <SourceName>"):
            mpb.merge(archive, self.root)

    def test_manifest_overlap_fails_before_any_file_is_written(self):
        # A key the public manifest lists under a source it does not otherwise use.
        key = f"Private/{LIB}/x.txt"
        public = dict(self.public_manifest)
        public[key] = {"hash": sha(b"old")}
        (self.root / "files_manifest_new.json").write_text(json.dumps(public, ensure_ascii=False), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "already"):
            mpb.merge(self.archive({f"{LIB}/x.txt": b"x"}), self.root)
        self.assertFalse((self.root / LIB / "x.txt").exists())


if __name__ == "__main__":
    unittest.main()
