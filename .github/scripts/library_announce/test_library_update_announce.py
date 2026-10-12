#!/usr/bin/env python3
"""Tests for library_update_announce.py, yemot.py and their workflow."""
import hashlib
import io
import json
import os
import shutil
import subprocess
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing, redirect_stdout
from pathlib import Path
from unittest import mock

try:
    import yaml
except ImportError:  # pragma: no cover - CI installs PyYAML explicitly
    yaml = None

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import library_update_announce as lua  # noqa: E402
import yemot  # noqa: E402

WORKFLOW = ROOT / ".github" / "workflows" / "library-update-announce.yml"
RELEASE = ROOT / ".github" / "workflows" / "manual-generate-release.yml"
CI = ROOT / ".github" / "workflows" / "ci.yml"


def make_db(path, books, lines, versions=(), split=False):
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE category (id INTEGER PRIMARY KEY, parentId INTEGER, title TEXT NOT NULL);
        CREATE TABLE source (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE book (id INTEGER PRIMARY KEY, categoryId INTEGER, sourceId INTEGER, title TEXT NOT NULL);
        CREATE TABLE line (id INTEGER PRIMARY KEY, bookId INTEGER, lineIndex INTEGER, content TEXT NOT NULL);
        CREATE TABLE book_version (id INTEGER PRIMARY KEY, bookId INTEGER NOT NULL, versionTitle TEXT NOT NULL,
            heVersionTitle TEXT, hasContent INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE version_line (versionId INTEGER NOT NULL, lineId INTEGER NOT NULL, content TEXT NOT NULL);
        -- Like the real DB: every root has parentId NULL, none is its own parent.
        INSERT INTO category VALUES (1, NULL, 'תנך'), (2, 1, 'תורה'), (3, NULL, 'הלכה');
        INSERT INTO source VALUES (1, 'Sefaria'), (2, 'DictaToOtzaria');
    """)
    conn.executemany("INSERT INTO book VALUES (?, ?, ?, ?)", books)
    conn.executemany("INSERT INTO line (bookId, lineIndex, content) VALUES (?, ?, ?)", lines)
    # versions: (bookId, versionTitle, heVersionTitle, {lineIndex: content} or None for metadata-only)
    for book_id, title, he_title, texts in versions:
        version_id = conn.execute(
            "INSERT INTO book_version (bookId, versionTitle, heVersionTitle, hasContent) VALUES (?, ?, ?, ?)",
            (book_id, title, he_title, int(texts is not None))).lastrowid
        for index, content in (texts or {}).items():
            [(line_id,)] = conn.execute("SELECT id FROM line WHERE bookId = ? AND lineIndex = ?", (book_id, index))
            conn.execute("INSERT INTO version_line VALUES (?, ?, ?)", (version_id, line_id, content))
    if split:
        conn.executescript("""
            CREATE TABLE line_content (id INTEGER PRIMARY KEY, content TEXT NOT NULL);
            INSERT INTO line_content SELECT id, content FROM line;
            ALTER TABLE line DROP COLUMN content;
            ALTER TABLE version_line RENAME TO old_version_line;
            CREATE TABLE version_line (versionId INTEGER NOT NULL, lineId INTEGER NOT NULL, content TEXT);
            INSERT INTO version_line
                SELECT v.versionId, v.lineId,
                    CASE WHEN CAST(v.content AS BLOB) = CAST(lc.content AS BLOB) THEN NULL ELSE v.content END
                FROM old_version_line v JOIN line_content lc ON lc.id = v.lineId;
            DROP TABLE old_version_line;
        """)
    conn.commit()
    conn.close()


def catalog(books, lines, versions=()):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "seforim.db"
        make_db(path, books, lines, versions)
        return lua.build_catalog(path)


def names(entries):
    return [e["name"] for e in entries]


class PreviousTagTest(unittest.TestCase):
    def test_picks_the_newest_older_tag_and_ignores_others(self):
        tags = ["v27-20260906092829", "v28-20260910220310", "v30-20261001000000",
                "lines-snapshot-sha256-abc", "v9-20260706195857"]
        self.assertEqual("v28-20260910220310", lua.previous_published_tag("v29-20260927072953", tags))

    def test_first_release_has_no_previous(self):
        self.assertIsNone(lua.previous_published_tag("v1-20260625232529", ["v1-20260625232529"]))

    def test_refuses_a_tag_that_is_not_a_db_release(self):
        with self.assertRaises(ValueError):
            lua.previous_published_tag("pipeline-result-run-1-1", [])


class PlainTextTest(unittest.TestCase):
    def test_markup_entities_and_whitespace_are_not_text(self):
        self.assertEqual("א ב & ג", lua.plain_text("<b>א</b>  \n ב&nbsp;&amp; <span class='x'>ג</span> "))
        self.assertEqual("&lt;", lua.plain_text("&amp;lt;"))
        self.assertEqual("<", lua.plain_text("&lt;"))
        self.assertEqual("חסר <חסר> כאן <בד\"ה אמר>", lua.plain_text("חסר <b><חסר></b> כאן <בד\"ה אמר>"))
        self.assertEqual("x", lua.plain_text("<!-- c -->x<?php ?>"))
        self.assertEqual(lua.plain_text("נקודה: הלכה"), lua.plain_text("נקודה:<br>הלכה"))
        self.assertNotEqual(lua.plain_text("וירגה זכרונו"), lua.plain_text("וירגהזכרונו"))


class CatalogTest(unittest.TestCase):
    def test_schema_split_preserves_catalog_with_inherited_different_and_missing_edition_rows(self):
        books = [(i, 2, 1, f"book {i}") for i in range(1, 6)]
        lines = [(i, j, text) for i in range(1, 6) for j, text in ((0, "alpha"), (1, "beta"))]
        versions = [(1, "Read", None, {0: "alpha", 1: "beta"}),
                    (1, "Other", None, {0: "alpha", 1: "different"}),
                    (2, "Part A", None, {0: "alpha"}), (2, "Part B", None, {1: "beta"}),
                    (3, "Twin A", None, {0: "alpha", 1: "beta"}),
                    (3, "Twin B", None, {0: "alpha", 1: "beta"}),
                    (4, "Metadata", None, None), (5, "Empty", None, {})]
        with tempfile.TemporaryDirectory() as tmp:
            legacy, split = Path(tmp) / "legacy.db", Path(tmp) / "split.db"
            make_db(legacy, books, lines, versions)
            make_db(split, books, lines, versions, split=True)
            with closing(sqlite3.connect(split)) as conn:
                self.assertGreater(conn.execute("SELECT COUNT(*) FROM version_line WHERE content IS NULL").fetchone()[0], 0)
                self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM version_line WHERE content IS NOT NULL").fetchone()[0])
            before, after = lua.build_catalog(legacy), lua.build_catalog(split)
        self.assertEqual(before, after)
        self.assertEqual(["Read", None, None, "Metadata", None], [b["edition"] for b in after["books"]])
        self.assertFalse(any(lua.diff_catalogs(before, after).values()))

    def test_compressed_line_text_preserves_catalog(self):
        try:
            import zstandard
        except ImportError:  # pragma: no cover - CI installs zstandard explicitly
            self.skipTest("zstandard is not installed")
        dictionary = (ROOT / "generator" / "common" / "src" / "jvmMain" / "resources" / "zstd"
                      / "line_content.zdict").read_bytes()
        compressor = zstandard.ZstdCompressor(
            level=19, dict_data=zstandard.ZstdCompressionDict(dictionary), write_checksum=False)
        books = [(1, 2, 1, "book 1"), (2, 2, 1, "book 2")]
        lines = [(1, 0, "<b>אמר</b> רבא"), (1, 1, "בראשית"), (2, 0, "alpha")]
        versions = [(1, "Read", None, {0: "<b>אמר</b> רבא", 1: "בראשית"}), (2, "Other", None, {0: "beta"})]
        with tempfile.TemporaryDirectory() as tmp:
            split, compressed = Path(tmp) / "split.db", Path(tmp) / "compressed.db"
            make_db(split, books, lines, versions, split=True)
            make_db(compressed, books, lines, versions, split=True)
            with closing(sqlite3.connect(compressed)) as conn:
                conn.create_function("zframe", 1, lambda t: None if t is None else compressor.compress(t.encode()))
                conn.executescript("""
                    UPDATE line_content SET content = zframe(content);
                    UPDATE version_line SET content = zframe(content) WHERE content IS NOT NULL;
                    CREATE TABLE zstd_dict (id INTEGER PRIMARY KEY, dict BLOB NOT NULL);
                """)
                conn.execute("INSERT INTO zstd_dict VALUES (1, ?)", (dictionary,))
                conn.commit()
            self.assertEqual(lua.build_catalog(split), lua.build_catalog(compressed))

    def test_paths_walk_up_to_a_null_parent_root(self):
        value = catalog([(10, 2, 1, "בראשית")], [(10, 0, "א")])
        self.assertEqual("תנך/תורה", value["books"][0]["path"])
        self.assertEqual("Sefaria", value["books"][0]["source"])
        self.assertEqual(1, value["books"][0]["lines"])

    def test_a_category_cycle_fails_loudly(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "seforim.db"
            make_db(path, [(10, 2, 1, "בראשית")], [])
            conn = sqlite3.connect(path)
            conn.execute("UPDATE category SET parentId = 2 WHERE id = 1")
            conn.commit()
            conn.close()
            with self.assertRaisesRegex(ValueError, "broken parent chain"):
                lua.build_catalog(path)

    def test_hash_follows_line_order_not_insertion_order(self):
        a = catalog([(10, 2, 1, "בראשית")], [(10, 0, "א"), (10, 1, "ב")])
        b = catalog([(10, 2, 1, "בראשית")], [(10, 1, "ב"), (10, 0, "א")])
        c = catalog([(10, 2, 1, "בראשית")], [(10, 0, "ב"), (10, 1, "א")])
        self.assertEqual(a["books"][0]["hash"], b["books"][0]["hash"])
        self.assertNotEqual(a["books"][0]["hash"], c["books"][0]["hash"])

    def test_markup_and_line_wrapping_do_not_change_the_hash(self):
        a = catalog([(10, 2, 1, "בראשית")], [(10, 0, "בראשית ברא"), (10, 1, "אלהים")])
        b = catalog([(10, 2, 1, "בראשית")], [(10, 0, "<h1>בראשית</h1>  ברא&nbsp;אלהים"), (10, 1, "<br>")])
        c = catalog([(10, 2, 1, "בראשית")], [(10, 0, "בראשית ברא אלוהים")])
        self.assertEqual(a["books"][0]["hash"], b["books"][0]["hash"])
        self.assertNotEqual(a["books"][0]["hash"], c["books"][0]["hash"])

    def test_book_without_lines_has_no_hash(self):
        value = catalog([(10, 2, 1, "ריק")], [])
        self.assertIsNone(value["books"][0]["hash"])

    def test_edition_is_the_version_whose_text_the_book_lines_are(self):
        value = catalog(
            [(1, 2, 1, "א"), (2, 2, 1, "ב"), (3, 2, 1, "ג"), (4, 2, 2, "ד"), (5, 2, 1, "ה"), (6, 2, 1, "ו"),
             (7, 2, 1, "ז")],
            [(b, i, t) for b in range(1, 8) for i, t in ((0, "א"), (1, "ב"))],
            [(1, "Only", None, None),
             (2, "Other", "אחרת", {0: "א", 1: "שונה"}), (2, "Read", "הנקראת", {0: "א", 1: "ב"}),
             (3, "Part A", "א", {0: "א"}), (3, "Part B", "ב", {1: "ב"}),
             (5, "Blank", "  ", {0: "א", 1: "ב"}),
             (6, "Twin A", None, {0: "א", 1: "ב"}), (6, "Twin B", None, {0: "א", 1: "ב"}),
             (7, "Not the text", "לא", {0: "א", 1: "אחר"})])
        books = {b["id"]: b for b in value["books"]}
        self.assertEqual(("Only", "Only"), (books[1]["edition"], books[1]["edition_name"]))
        self.assertEqual(("Read", "הנקראת"), (books[2]["edition"], books[2]["edition_name"]))
        self.assertIsNone(books[3]["edition"], "a mosaic of editions is no one edition")
        self.assertIsNone(books[4]["edition"], "no versions at all")
        self.assertEqual("Blank", books[5]["edition_name"])
        self.assertIsNone(books[6]["edition"], "two identical editions are a tie")
        self.assertIsNone(books[7]["edition"], "the only edition is not the book's text")


class DiffTest(unittest.TestCase):
    def setUp(self):
        self.old = catalog(
            [(1, 2, 1, "בראשית"), (2, 2, 1, "שמות"), (3, 3, 2, "ישן"), (4, 3, 2, "נמחק"), (5, 2, 2, "נודד"),
             (8, 2, 1, "ויקרא"), (9, 3, 1, "משנה")],
            [(1, 0, "א"), (2, 0, "ב"), (3, 0, "תוכן ישן"), (4, 0, "ד"), (5, 0, "ה"), (8, 0, "ח"), (9, 0, "ט")],
            [(8, "Old ed", "ישנה", {0: "ח"}), (8, "New ed", "מהדורה חדשה", {0: "ח ישן"}), (9, "Same", "אותה", None)])
        self.new = catalog(
            [(1, 2, 1, "בראשית"), (2, 2, 1, "שמות"), (6, 3, 2, "חדש-בשם"), (7, 2, 1, "חדש"), (5, 3, 2, "נודד"),
             (8, 2, 1, "ויקרא"), (9, 1, 1, "משנה")],
            [(1, 0, "<b>א</b>"), (2, 0, "ב שונה"), (6, 0, "תוכן ישן"), (7, 0, "ז"), (5, 0, "ה"), (8, 0, "ח שונה"),
             (9, 0, "ט שונה")],
            [(8, "Old ed", "ישנה", {0: "ח"}), (8, "New ed", "מהדורה חדשה", {0: "ח שונה"}),
             (9, "Same", "אותה", None)])
        self.changes = lua.diff_catalogs(self.old, self.new)

    def test_classifies_every_kind_of_change(self):
        self.assertEqual(["תנך/תורה/חדש"], names(self.changes["added"]))
        self.assertEqual(["הלכה/נמחק"], names(self.changes["removed"]))
        self.assertEqual(["תנך/משנה", "תנך/תורה/ויקרא", "תנך/תורה/שמות"], names(self.changes["changed"]))
        self.assertEqual(["הלכה/חדש-בשם", "הלכה/נודד", "תנך/משנה"], names(self.changes["moved"]))

    def test_markup_only_change_is_not_a_change(self):
        self.assertNotIn("תנך/תורה/בראשית", names(self.changes["changed"]))

    def test_edition_change_is_one_changed_line(self):
        lines = lua.forum_lines(self.changes)["changed"]
        self.assertIn("* תנך/תורה/ויקרא - שינוי מהדורה ל-מהדורה חדשה", lines)
        self.assertEqual(1, sum("ויקרא" in line for line in lines))

    def test_an_edition_newly_identified_is_not_an_edition_change(self):
        old = catalog([(1, 2, 1, "א")], [(1, 0, "א")], [(1, "Listed", None, {0: "אחר"})])
        new = catalog([(1, 2, 1, "א")], [(1, 0, "א")], [(1, "Listed", None, {0: "אחר"}), (1, "Read", None, {0: "א"})])
        self.assertIsNone(old["books"][0]["edition"])
        self.assertEqual("Read", new["books"][0]["edition"])
        self.assertFalse(any(lua.diff_catalogs(old, new).values()))

    def test_moved_and_changed_is_explicit_in_both_sections(self):
        lines = lua.forum_lines(self.changes)
        self.assertIn("* תנך/משנה (לשעבר: הלכה/משנה) (השתנה גם תוכנו)", lines["moved"])
        self.assertIn("* תנך/משנה (שונה גם מיקומו/שמו)", lines["changed"])
        self.assertIn("* הלכה/חדש-בשם (לשעבר: הלכה/ישן)", lines["moved"])

    def test_identical_catalogs_have_no_changes(self):
        self.assertFalse(any(lua.diff_catalogs(self.old, self.old).values()))

    def test_ambiguous_rename_fails_loudly(self):
        old = catalog([(1, 2, 1, "א"), (2, 2, 1, "ב")], [(1, 0, "זהה"), (2, 0, "זהה")])
        new = catalog([(3, 2, 1, "ג"), (4, 2, 1, "ד")], [(3, 0, "זהה"), (4, 0, "<i>זהה</i>")])
        with self.assertRaisesRegex(ValueError, "ambiguous rename"):
            lua.diff_catalogs(old, new)

    def test_a_single_twin_on_one_side_only_is_no_rename(self):
        old = catalog([(1, 2, 1, "א"), (2, 2, 1, "ב")], [(1, 0, "זהה"), (2, 0, "זהה")])
        new = catalog([(3, 2, 1, "ג")], [(3, 0, "אחר")])
        changes = lua.diff_catalogs(old, new)
        self.assertEqual(2, len(changes["removed"]))
        self.assertEqual([], changes["moved"])


class ForumPostTest(unittest.TestCase):
    def changes(self, count, key="added"):
        value = {k: [] for k, _ in lua.SECTIONS}
        value[key] = [{"name": f"קטגוריה/ספר {i:04d}", "title": f"ספר {i:04d}"} for i in range(count)]
        return value

    def test_post_lists_only_non_empty_sections(self):
        changes = {"added": [{"name": "א/ב", "title": "ב"}], "removed": [], "moved": [],
                   "changed": [{"name": "ג/ד", "title": "ד", "edition": None, "also_moved": False},
                               {"name": "ג/ה", "title": "ה", "edition": "מהד", "also_moved": False}]}
        [post] = lua.build_forum_posts(changes, 30, "א' תשרי")
        self.assertTrue(post.startswith("# גרסת ספרייה 30\n\n**עדכון א' תשרי**\n"))
        self.assertIn("## התווספו הספרים הבאים:\n* א/ב\n", post)
        self.assertIn("## השתנו הספרים הבאים:\n* ג/ד\n* ג/ה - שינוי מהדורה ל-מהד\n", post)
        self.assertNotIn("נמחקו", post)

    def test_release_notes_hold_the_whole_list_in_one_file(self):
        changes = self.changes(3000)
        posts = lua.build_forum_posts(changes, 30, "א' תשרי")
        self.assertGreater(len(posts), 1)
        notes = lua.build_release_notes(changes, 30, "א' תשרי")
        self.assertEqual([lua.build_forum_posts(changes, 30, "א' תשרי", limit=10**9)[0]], [notes])
        self.assertEqual(3000, notes.count("\n* "))
        self.assertNotIn("(המשך)", notes)

    def test_long_list_is_split_on_line_boundaries_without_losing_a_line(self):
        changes = self.changes(1000)
        changes["removed"] = [{"name": "x/נמחק", "title": "נמחק"}]
        posts = lua.build_forum_posts(changes, 30, "x", limit=500)
        self.assertGreater(len(posts), 10)
        self.assertTrue(all(lua.post_length(p) <= 500 for p in posts))
        self.assertTrue(posts[0].startswith("# גרסת ספרייה 30\n"))
        for post in posts[1:]:
            self.assertTrue(post.startswith("# גרסת ספרייה 30 (המשך)\n\n## "))
        items = [line for post in posts for line in post.splitlines() if line.startswith("* ")]
        self.assertEqual([f"* קטגוריה/ספר {i:04d}" for i in range(1000)] + ["* x/נמחק"], items)
        self.assertEqual(posts, lua.build_forum_posts(changes, 30, "x", limit=500))

    def test_a_continuation_mid_section_repeats_its_heading(self):
        posts = lua.build_forum_posts(self.changes(100), 30, "x", limit=500)
        self.assertIn("\n\n## התווספו הספרים הבאים: (המשך)\n* ", posts[1])

    def test_default_limit_is_the_forum_maximum(self):
        posts = lua.build_forum_posts(self.changes(3000), 30, "x")
        self.assertGreater(len(posts), 1)
        self.assertTrue(all(lua.post_length(p) <= 32767 for p in posts))

    def test_length_counts_utf16_units_like_nodebb(self):
        self.assertEqual(4, lua.post_length("אב😀"))

    def test_a_line_longer_than_a_post_fails_loudly(self):
        changes = self.changes(1)
        changes["added"][0]["name"] = "א" * 600
        with self.assertRaises(ValueError):
            lua.build_forum_posts(changes, 30, "x", limit=500)


class YemotContentTest(unittest.TestCase):
    def test_reads_titles_and_the_new_name_of_a_moved_book(self):
        changes = {
            "added": [{"name": "א/ב", "title": "ב"}],
            "removed": [],
            "moved": [{"name": "a/חדש", "title": "חדש", "old_name": "c/ישן", "old_title": "ישן", "also_changed": False},
                      {"name": "a/נודד", "title": "נודד", "old_name": "c/נודד", "old_title": "נודד",
                       "also_changed": True}],
            "changed": [{"name": "ג/ד", "title": "ד", "edition": "מהד", "also_moved": False}],
        }
        self.assertEqual({
            "התווספו הספרים הבאים:": "ב",
            "שונה מיקום/שם של הספרים הבאים:": "חדש (לשעבר: ישן)\nנודד (השתנה גם תוכנו)",
            "השתנו הספרים הבאים:": "ד - שינוי מהדורה ל-מהד",
        }, lua.build_yemot_content(changes))


class Response:
    def __init__(self, body, status=200):
        self.status_code, self.body, self.text = status, body, repr(body)

    def json(self):
        if isinstance(self.body, Exception):
            raise self.body
        return self.body


class Session:
    def __init__(self, max_file="005.tts", fail_on=None, fail_tzintuk=False):
        self.max_file, self.fail_on, self.uploads, self.calls = max_file, fail_on, [], []
        self.fail_tzintuk = fail_tzintuk

    def get(self, url, params, timeout):
        self.calls.append((url, timeout))
        if url.endswith("GetIVR2DirStats"):
            return Response({"responseStatus": "OK", "maxFile": {"name": self.max_file}})
        if self.fail_tzintuk:
            return Response({"responseStatus": "ERROR", "message": "busy"})
        return Response({"responseStatus": "OK"})

    def post(self, url, data, timeout):
        self.calls.append((url, timeout))
        if self.fail_on and self.fail_on in data["what"]:
            return Response({"responseStatus": "ERROR", "message": "denied"})
        self.uploads.append((data["what"], data["contents"]))
        return Response({"responseStatus": "OK"})


class YemotTest(unittest.TestCase):
    def test_split_never_loses_or_duplicates_text(self):
        lines = [f"שורה {i}" + "x" * (i % 37) for i in range(400)]
        content = "\n".join(lines)
        parts = yemot.split_content(content, 100)
        self.assertTrue(all(len(p) <= 100 for p in parts))
        self.assertEqual(lines, "\n".join(parts).split("\n"))

    def test_a_chunk_without_a_newline_is_cut_not_dropped(self):
        content = "א" * 250 + "\nב"
        parts = yemot.split_content(content, 100)
        self.assertEqual(["א" * 100, "א" * 100, "א" * 50 + "\nב"], parts)

    def test_a_newline_at_the_chunk_start_does_not_loop(self):
        content = "\n" + "א" * 150
        self.assertEqual(["א" * 99, "א" * 51], yemot.split_content(content, 100))

    def test_uploads_continue_numbering_and_reverse_each_section(self):
        session = Session(max_file="005.tts")
        with mock.patch.object(yemot, "CHUNK_SIZE", 10):
            yemot.split_and_send({"כותרת": "אאאא\nבבבב\nגגגג"}, "עדכון\n", "t", "ivr2:/1", "list",
                                 session=session)
        self.assertEqual([
            ("ivr2:/1/006.tts", "גגגג"), ("ivr2:/1/007.tts", "אאאא\nבבבב"),
            ("ivr2:/1/007-Title.tts", "כותרת"), ("ivr2:/1/008.tts", "עדכון\n"),
        ], session.uploads)
        self.assertTrue(all(timeout for _, timeout in session.calls))
        self.assertTrue(session.calls[-1][0].endswith("RunTzintuk"))

    def test_unknown_numbering_fails_instead_of_restarting_at_000(self):
        for max_file in (None, "", "Title.tts", "005-Title.tts"):
            with self.subTest(max_file=max_file):
                with self.assertRaises(yemot.YemotError):
                    yemot.get_file_num("t", "ivr2:/1", Session(max_file=max_file))

    def test_a_refused_upload_fails_loudly(self):
        session = Session(fail_on="-Title")
        with self.assertRaisesRegex(yemot.YemotError, "UploadTextFile"):
            yemot.split_and_send({"כותרת": "א"}, "עדכון\n", "t", "ivr2:/1", "list", session=session)

    def test_an_interrupted_send_resumes_without_reuploading(self):
        content = {"כותרת": "אאאא\nבבבב\nגגגג"}
        saved = []
        progress = {}
        with mock.patch.object(yemot, "CHUNK_SIZE", 10):
            with self.assertRaises(yemot.YemotError):
                yemot.split_and_send(content, "עדכון\n", "t", "ivr2:/1", "list", session=Session(fail_on="007-Title"),
                                     progress=progress, save=lambda state: saved.append(dict(state)))
            self.assertEqual({"base": 5, "uploaded": 2, "tzintuk": False}, progress)
            # The folder grew meanwhile: a resume must keep the recorded numbering.
            second = Session(max_file="007.tts")
            yemot.split_and_send(content, "עדכון\n", "t", "ivr2:/1", "list", session=second,
                                 progress=progress, save=lambda state: saved.append(dict(state)))
        self.assertEqual([("ivr2:/1/007-Title.tts", "כותרת"), ("ivr2:/1/008.tts", "עדכון\n")], second.uploads)
        self.assertFalse(any(url.endswith("GetIVR2DirStats") for url, _ in second.calls))
        self.assertEqual({"base": 5, "uploaded": 4, "tzintuk": True}, saved[-1])

    def test_tzintuk_runs_once_after_every_upload(self):
        progress = {}
        with self.assertRaises(yemot.YemotError):
            yemot.split_and_send({"כ": "א"}, "ע\n", "t", "ivr2:/1", "list", session=Session(fail_tzintuk=True),
                                 progress=progress)
        self.assertEqual({"base": 5, "uploaded": 3, "tzintuk": False}, progress)
        again = Session()
        yemot.split_and_send({"כ": "א"}, "ע\n", "t", "ivr2:/1", "list", session=again, progress=progress)
        self.assertEqual([], again.uploads)
        self.assertEqual(["RunTzintuk"], [url.rsplit("/", 1)[-1] for url, _ in again.calls])
        done = Session()
        yemot.split_and_send({"כ": "א"}, "ע\n", "t", "ivr2:/1", "list", session=done, progress=progress)
        self.assertEqual([], done.calls)

    def test_upload_plan_is_deterministic(self):
        content = {"א": "1\n2", "ב": "3"}
        self.assertEqual([("006", "1\n2"), ("006-Title", "א"), ("007", "3"), ("007-Title", "ב"), ("008", "ד")],
                         yemot.upload_plan(content, "ד", 5))

    def test_http_errors_and_non_json_fail_loudly(self):
        with self.assertRaisesRegex(yemot.YemotError, "HTTP 500"):
            yemot._checked(Response({}, status=500), "x")
        with self.assertRaisesRegex(yemot.YemotError, "not JSON"):
            yemot._checked(Response(ValueError("bad")), "x")


class RenderAndSendTest(unittest.TestCase):
    def setUp(self):
        self.old = catalog([(1, 2, 1, "בראשית")], [(1, 0, "א")])
        self.new = catalog([(1, 2, 1, "בראשית"), (2, 2, 1, "שמות")], [(1, 0, "א"), (2, 0, "ב")])
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def render(self, old, new, argv=()):
        paths = []
        for name, value in (("old.json", old), ("new.json", new)):
            path = self.dir / name
            path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
            paths.append(str(path))
        out = io.StringIO()
        with mock.patch.dict("os.environ", {"GITHUB_OUTPUT": str(self.dir / "gh_out")}), redirect_stdout(out):
            code = lua.main(["render", "--old", paths[0], "--new", paths[1], "--tag", "v30-20261001000000",
                             "--as-of", "2026-10-01", "--out", str(self.dir / "out"), *argv])
        return code, out.getvalue()

    def test_nothing_changed_renders_nothing_to_send(self):
        code, out = self.render(self.old, self.old)
        self.assertEqual(0, code)
        self.assertIn("nothing to announce", out)
        self.assertFalse((self.dir / "out" / "forum_posts.json").exists())
        self.assertFalse((self.dir / "out" / lua.RELEASE_NOTES_ASSET).exists())
        self.assertIn("has_changes=false\n", (self.dir / "gh_out").read_text(encoding="utf-8"))

    def test_render_writes_every_channel_payload(self):
        code, out = self.render(self.old, self.new)
        self.assertEqual(0, code)
        self.assertIn("תנך/תורה/שמות", out)
        posts = json.loads((self.dir / "out" / "forum_posts.json").read_text(encoding="utf-8"))
        self.assertTrue(posts[0].startswith("# גרסת ספרייה 30\n"))
        spoken = json.loads((self.dir / "out" / "yemot.json").read_text(encoding="utf-8"))
        self.assertEqual({"התווספו הספרים הבאים:": "שמות"}, spoken["content"])
        self.assertRegex(spoken["heading"], r"^גרסת ספרייה 30\nעדכון .*תשפ.*\n$")
        self.assertEqual(posts, [(self.dir / "out" / lua.RELEASE_NOTES_ASSET).read_text(encoding="utf-8")])
        self.assertIn('channels=["forum", "yemot", "release"]\n', (self.dir / "gh_out").read_text(encoding="utf-8"))

    def test_unknown_channel_is_refused(self):
        with self.assertRaises(SystemExit):
            self.render(self.old, self.new, ["--channels", "forum,chat"])

    def send(self, channel, env, progress=None):
        argv = ["send", "--channel", channel, "--dir", str(self.dir / "out")]
        if progress:
            argv += ["--progress", str(progress)]
        with mock.patch.dict("os.environ", env), redirect_stdout(io.StringIO()):
            return lua.main(argv)

    def test_send_forum_posts_every_rendered_post_in_order(self):
        self.render(self.old, self.new)
        (self.dir / "out" / "forum_posts.json").write_text(json.dumps(["1", "2"]), encoding="utf-8")
        with mock.patch.object(lua, "send_forum") as forum:
            self.assertEqual(0, self.send("forum", {"FORUM_TOKEN": "t"}))
        self.assertEqual([mock.call("1", "t"), mock.call("2", "t")], forum.call_args_list)

    def test_an_empty_secret_fails_before_sending(self):
        self.render(self.old, self.new)
        for channel, secret in (("forum", "FORUM_TOKEN"), ("yemot", "TOKEN_YEMOT")):
            with self.subTest(channel=channel), mock.patch.object(lua, "send_forum") as forum, \
                    mock.patch.object(lua, "send_yemot") as spoken:
                with self.assertRaisesRegex(SystemExit, f"::error::{secret} is empty"):
                    self.send(channel, {secret: " "})
                forum.assert_not_called()
                spoken.assert_not_called()

    def test_a_rerun_continues_the_forum_after_the_last_accepted_post(self):
        self.render(self.old, self.new)
        (self.dir / "out" / "forum_posts.json").write_text(json.dumps(["1", "2", "3"]), encoding="utf-8")
        progress = self.dir / "progress" / "forum_progress.json"
        with mock.patch.object(lua, "send_forum", side_effect=[None, lua.ForumRefused("403: no")]):
            with self.assertRaisesRegex(RuntimeError, "post 2/3 failed; posts 1-1"):
                self.send("forum", {"FORUM_TOKEN": "t"}, progress)
        self.assertEqual(1, json.loads(progress.read_text(encoding="utf-8"))["sent"])
        with mock.patch.object(lua, "send_forum") as forum:
            self.send("forum", {"FORUM_TOKEN": "t"}, progress)
        self.assertEqual([mock.call("2", "t"), mock.call("3", "t")], forum.call_args_list)
        self.assertEqual(3, json.loads(progress.read_text(encoding="utf-8"))["sent"])

    def test_progress_for_another_announcement_is_refused(self):
        self.render(self.old, self.new)
        progress = self.dir / "forum_progress.json"
        progress.write_text(json.dumps({"payload_sha256": "0" * 64, "sent": 1}), encoding="utf-8")
        with mock.patch.object(lua, "send_forum") as forum, self.assertRaisesRegex(SystemExit, "another"):
            self.send("forum", {"FORUM_TOKEN": "t"}, progress)
        forum.assert_not_called()

    def test_a_failed_post_names_how_many_were_sent(self):
        send = mock.Mock(side_effect=[None, lua.ForumRefused("403: no")])
        with redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "post 2/3 failed; posts 1-1"):
            lua.send_forum_posts(["a", "b", "c"], "t", send=send)

    def test_send_yemot_uses_the_rendered_content_and_heading(self):
        self.render(self.old, self.new)
        with mock.patch.object(lua, "send_yemot") as spoken:
            self.send("yemot", {"TOKEN_YEMOT": "t"}, self.dir / "yemot_progress.json")
        content, heading, token, state, _save = spoken.call_args.args
        self.assertEqual({"התווספו הספרים הבאים:": "שמות"}, content)
        self.assertTrue(heading.startswith("גרסת ספרייה 30\nעדכון "))
        self.assertEqual("t", token)
        self.assertIn("payload_sha256", state)


class ForumTest(unittest.TestCase):
    def test_the_rate_limit_refusal_is_retried(self):
        post = mock.Mock(side_effect=[lua.ForumRefused("400: [[error:too-many-posts, 10]]"), None])
        with mock.patch.object(lua.time, "sleep"):
            lua.send_forum("x", "t", post=post)
        self.assertEqual(2, post.call_count)

    def test_any_other_refusal_is_not_retried(self):
        post = mock.Mock(side_effect=lua.ForumRefused("403: [[error:no-privileges]]"))
        with self.assertRaises(lua.ForumRefused):
            lua.send_forum("x", "t", post=post)
        post.assert_called_once()

    def test_the_token_is_sent_as_a_bearer_to_the_topic(self):
        session = mock.Mock()
        session.post.return_value.json.return_value = {"status": {"code": "ok"}}
        lua.post_to_forum("טקסט", "secret", session=session)
        url = session.post.call_args.args[0]
        kwargs = session.post.call_args.kwargs
        self.assertEqual(f"https://otzaria.org/forum/api/v3/topics/{lua.FORUM_TOPIC_ID}", url)
        self.assertEqual("Bearer secret", kwargs["headers"]["Authorization"])
        self.assertEqual({"content": "טקסט"}, kwargs["json"])
        self.assertTrue(kwargs["timeout"])

    def test_a_refusal_in_the_body_is_not_a_success(self):
        session = mock.Mock()
        session.post.return_value.json.return_value = {"status": {"code": "bad-request", "message": "x"}}
        with self.assertRaises(lua.ForumRefused):
            lua.post_to_forum("x", "t", session=session)


FAKE_GH = r"""#!/usr/bin/env python3
import json, os, re, subprocess, sys
fixture = json.load(open(os.environ["FAKE_GH"]))
with open(os.environ["FAKE_GH"] + ".log", "a") as log:
    log.write(" ".join(sys.argv[1:]) + "\n")
args, jq, endpoint = sys.argv[1:], None, None
if args[:2] == ["release", "download"]:
    tag, folder = args[2], args[args.index("-D") + 1]
    for name, text in fixture["assets"][tag].items():
        open(os.path.join(folder, name), "w").write(text)
    sys.exit(0)
i = 1
while i < len(args):
    if args[i] in ("-X", "-f", "-R"):
        i += 2
    elif args[i] == "--jq":
        jq = args[i + 1]; i += 2
    elif args[i] == "--paginate":
        i += 1
    else:
        endpoint = args[i]; i += 1
path = endpoint.split("?")[0]
if m := re.fullmatch(r"repos/[^/]+/[^/]+/releases/tags/(.+)", path):
    if m.group(1) not in fixture["releases"]:
        sys.stderr.write("gh: Not Found (HTTP 404)\n"); sys.exit(1)
    value = fixture["releases"][m.group(1)]
elif path.endswith("/actions/workflows/library-update-announce.yml/runs"):
    if fixture.get("runs_fail"):
        sys.stderr.write("gh: Server Error (HTTP 502)\n"); sys.exit(1)
    value = {"workflow_runs": [{"id": run} for run in fixture["jobs"]]}
elif m := re.fullmatch(r"repos/[^/]+/[^/]+/actions/runs/([0-9]+)/jobs", path):
    value = {"jobs": fixture["jobs"][m.group(1)]}
else:
    sys.stderr.write("unexpected endpoint " + endpoint + "\n"); sys.exit(2)
sys.stdout.write(subprocess.run(["jq", "-r", jq], input=json.dumps(value), text=True,
                                 capture_output=True, check=True).stdout)
"""

TAG = "v29-20260927072953"


@unittest.skipUnless(all(shutil.which(tool) for tool in ("bash", "jq", "sha256sum")), "gate needs bash, jq, sha256sum")
class GateTest(unittest.TestCase):
    GATE = Path(__file__).resolve().parent / "gate.sh"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        (self.dir / "bin").mkdir()
        gh = self.dir / "bin" / "gh"
        gh.write_text(FAKE_GH, encoding="utf-8")
        gh.chmod(0o755)
        self.fixture = {"releases": {TAG: {"tag_name": TAG, "draft": False, "prerelease": False,
                                           "created_at": "2026-09-27T07:30:00Z"}},
                        "assets": {}, "jobs": {}}

    def tearDown(self):
        self.tmp.cleanup()

    def handoff(self, status="published", run=100, attempt=1, tag=TAG, corrupt=False):
        name = f"pipeline-result-run-{run}-{attempt}"
        body = json.dumps({"status": status, "child_run_id": run, "child_run_attempt": attempt,
                           "release_tag": tag}) + "\n"
        digest = hashlib.sha256(body.encode()).hexdigest()
        self.fixture["releases"][name] = {"tag_name": name, "prerelease": True}
        self.fixture["assets"][name] = {"pipeline-result.json": body,
                                        "pipeline-result.sha256": ("0" * 64 if corrupt else digest) + "\n"}

    def gate(self, event, **env):
        fixture = self.dir / "fixture.json"
        fixture.write_text(json.dumps(self.fixture), encoding="utf-8")
        output = self.dir / "out"
        output.write_text("", encoding="utf-8")
        values = {"PATH": f"{self.dir / 'bin'}{os.pathsep}{os.environ['PATH']}", "FAKE_GH": str(fixture),
                  "EVENT_NAME": event, "GITHUB_REPOSITORY": "Otzaria/SeforimLibrary", "GITHUB_OUTPUT": str(output),
                  "WORKFLOW_RUN_ID": "100", "WORKFLOW_RUN_ATTEMPT": "1", "GITHUB_RUN_ID": "500", **env}
        result = subprocess.run(["bash", str(self.GATE)], env=values, capture_output=True, text=True)
        outputs = dict(line.split("=", 1) for line in output.read_text(encoding="utf-8").splitlines())
        log = Path(str(fixture) + ".log")
        return result, outputs, log.read_text(encoding="utf-8") if log.exists() else ""

    def test_a_release_run_that_published_nothing_is_a_clean_skip(self):
        result, out, _ = self.gate("workflow_run")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("false", out["proceed"])
        self.assertIn("published nothing", result.stdout)

    def test_a_reuse_run_is_a_clean_skip(self):
        self.handoff(status="reused")
        result, out, _ = self.gate("workflow_run")
        self.assertEqual((0, "false"), (result.returncode, out["proceed"]))
        self.assertIn("reused", result.stdout)

    def test_a_release_run_announces_the_tag_it_published(self):
        self.handoff()
        result, out, _ = self.gate("workflow_run")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual({"proceed": "true", "tag": TAG, "send": "true",
                          "channels": '["forum","yemot","release"]'}, out)

    def test_a_published_prerelease_is_a_clean_skip(self):
        self.handoff()
        self.fixture["releases"][TAG]["prerelease"] = True
        result, out, _ = self.gate("workflow_run")
        self.assertEqual((0, "false"), (result.returncode, out["proceed"]))

    def test_a_handoff_that_does_not_verify_fails(self):
        for kwargs in ({"corrupt": True}, {"run": 999}, {"status": "odd"}):
            with self.subTest(**kwargs):
                self.fixture["releases"] = {TAG: self.fixture["releases"][TAG]}
                run = kwargs.pop("run", 100)
                self.handoff(run=run, **kwargs)
                if run != 100:
                    self.fixture["releases"]["pipeline-result-run-100-1"] = self.fixture["releases"].pop(
                        f"pipeline-result-run-{run}-1")
                    self.fixture["assets"]["pipeline-result-run-100-1"] = self.fixture["assets"].pop(
                        f"pipeline-result-run-{run}-1")
                result, out, _ = self.gate("workflow_run")
                self.assertNotEqual(0, result.returncode)
                self.assertNotIn("proceed", out)

    @staticmethod
    def job(channel, conclusion, status="completed", tag=TAG):
        return {"name": f"{channel} {tag}", "status": status, "conclusion": conclusion, "html_url": "u"}

    def send_gate(self):
        return self.gate("workflow_dispatch", INPUT_TAG=TAG, DRY_RUN="false", INPUT_CHANNELS="forum,yemot")

    def test_channels_some_run_already_sent_are_dropped(self):
        self.fixture["jobs"] = {"7": [self.job("forum", "failure"), self.job("forum", "success")],
                                "8": [self.job("yemot", "success", tag="v28-20260910220310"),
                                      self.job("yemot", "skipped")]}
        result, out, _ = self.send_gate()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual('["yemot"]', out["channels"])
        self.fixture["jobs"]["9"] = [self.job("yemot", "success")]
        result, out, _ = self.send_gate()
        self.assertEqual((0, "false"), (result.returncode, out["proceed"]))
        self.assertIn("already announced", result.stdout)

    def test_a_channel_another_run_left_half_sent_fails_the_gate(self):
        for job in (self.job("yemot", "failure"), self.job("yemot", "cancelled"),
                    self.job("yemot", None, status="in_progress")):
            with self.subTest(job=job):
                self.fixture["jobs"] = {"7": [job]}
                result, out, _ = self.send_gate()
                self.assertNotEqual(0, result.returncode)
                self.assertNotIn("proceed", out)
                self.assertIn("actions/runs/7", result.stderr)
                self.assertIn('"Re-run failed jobs"', result.stderr)

    def test_a_rerun_of_the_run_that_left_it_half_sent_resumes(self):
        self.fixture["jobs"] = {"500": [self.job("yemot", "failure")]}
        result, out, _ = self.send_gate()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual('["forum","yemot"]', out["channels"])

    def test_a_failing_history_check_fails_the_gate(self):
        self.fixture["runs_fail"] = True
        result, out, _ = self.send_gate()
        self.assertNotEqual(0, result.returncode)
        self.assertNotIn("proceed", out)

    def channel_step(self, **env):
        doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
        run = next(s["run"] for s in doc["jobs"]["forum"]["steps"] if s.get("id") == "dedup")
        fixture = self.dir / "fixture.json"
        fixture.write_text(json.dumps(self.fixture), encoding="utf-8")
        output = self.dir / "step_out"
        output.write_text("", encoding="utf-8")
        values = {"PATH": f"{self.dir / 'bin'}{os.pathsep}{os.environ['PATH']}", "FAKE_GH": str(fixture),
                  "GITHUB_REPOSITORY": "Otzaria/SeforimLibrary", "GITHUB_OUTPUT": str(output), "GITHUB_RUN_ID": "500",
                  "TAG": TAG, "CHANNEL": "forum", **env}
        # GitHub runs a step as `bash -e {0}`.
        result = subprocess.run(["bash", "-e", "-c", run], cwd=ROOT, env=values, capture_output=True, text=True)
        return result, output.read_text(encoding="utf-8")

    @unittest.skipIf(yaml is None, "PyYAML is required to parse the workflow")
    def test_the_channel_step_skips_resumes_or_fails(self):
        self.fixture["jobs"] = {"7": [self.job("forum", "success")]}
        self.assertEqual((0, "sent=true\n"), (self.channel_step()[0].returncode, self.channel_step()[1]))
        self.fixture["jobs"] = {"500": [self.job("forum", None, status="in_progress")]}
        result, out = self.channel_step()
        self.assertEqual((0, "sent=false\n"), (result.returncode, out), result.stderr)
        self.fixture["jobs"] = {"7": [self.job("forum", "failure")], "500": [self.job("forum", None, "in_progress")]}
        result, out = self.channel_step()
        self.assertNotEqual(0, result.returncode)
        self.assertEqual("", out)
        self.fixture["jobs"], self.fixture["runs_fail"] = {}, True
        result, out = self.channel_step()
        self.assertNotEqual(0, result.returncode)
        self.assertEqual("", out)

    def test_a_dry_run_checks_no_history_and_sends_nothing(self):
        self.fixture["jobs"] = {"7": [self.job("forum", "success")]}
        result, out, log = self.gate("workflow_dispatch", INPUT_TAG=TAG, DRY_RUN="true", INPUT_CHANNELS="yemot, forum")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual({"proceed": "true", "tag": TAG, "send": "false", "channels": '["yemot","forum"]'}, out)
        self.assertNotIn("/runs", log)

    def test_a_manual_run_for_an_unpublished_tag_fails(self):
        self.fixture["releases"][TAG]["prerelease"] = True
        result, _, _ = self.gate("workflow_dispatch", INPUT_TAG=TAG, DRY_RUN="false", INPUT_CHANNELS="forum")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("not a published", result.stdout)

    def test_an_unknown_channel_fails(self):
        result, _, _ = self.gate("workflow_dispatch", INPUT_TAG=TAG, DRY_RUN="true", INPUT_CHANNELS="forum,chat")
        self.assertNotEqual(0, result.returncode)


@unittest.skipIf(yaml is None, "PyYAML is required to parse the workflow")
class WorkflowContractTest(unittest.TestCase):
    def setUp(self):
        self.text = WORKFLOW.read_text(encoding="utf-8")
        self.doc = yaml.safe_load(self.text)
        # PyYAML reads the bare `on:` key as boolean True.
        self.on = self.doc.get("on", self.doc.get(True))

    def test_follows_every_completed_release_run(self):
        release = yaml.safe_load(RELEASE.read_text(encoding="utf-8"))
        self.assertEqual([release["name"]], self.on["workflow_run"]["workflows"])
        self.assertEqual(["completed"], self.on["workflow_run"]["types"])
        # Whatever the run's conclusion: a publish followed by a failed step still announces.
        self.assertNotIn("workflow_run.conclusion", self.text)

    def test_only_the_release_run_and_a_manual_dispatch_trigger_it(self):
        # No `release` event: it would fire beside workflow_run every week and download twice.
        self.assertEqual({"workflow_run", "workflow_dispatch"}, set(self.on))
        self.assertNotIn("github.event.release", self.text)

    def test_the_release_workflow_does_not_depend_on_the_announcement(self):
        text = RELEASE.read_text(encoding="utf-8")
        self.assertNotIn("library-update-announce", text)
        self.assertNotIn("announce", yaml.safe_load(text)["jobs"])

    def test_nothing_ties_the_release_run_to_the_announcement(self):
        # workflow_run fires after the release run ends; hosted runners and own lock names only.
        release = yaml.safe_load(RELEASE.read_text(encoding="utf-8"))
        release_groups = {str((job.get("concurrency") or {}).get("group")) for job in release["jobs"].values()}
        release_groups.add(str((release.get("concurrency") or {}).get("group")))
        for name, job in self.doc["jobs"].items():
            self.assertEqual("ubuntu-latest", job["runs-on"], name)
            group = (job.get("concurrency") or {}).get("group")
            if group:
                self.assertTrue(group.startswith("library-update-announce-"), name)
                self.assertNotIn(group, release_groups)
        self.assertNotIn("concurrency", self.doc)

    def test_manual_run_is_a_dry_run_by_default(self):
        inputs = self.on["workflow_dispatch"]["inputs"]
        self.assertTrue(inputs["tag"]["required"])
        self.assertIs(True, inputs["dry_run"]["default"])
        self.assertEqual("forum,yemot,release", inputs["channels"]["default"])

    def test_the_gate_decides_before_anything_is_downloaded(self):
        jobs = self.doc["jobs"]
        self.assertEqual(["gate", "prepare", "forum", "yemot", "release"], list(jobs))
        self.assertEqual("gate", jobs["prepare"]["needs"])
        self.assertEqual("needs.gate.outputs.proceed == 'true'", jobs["prepare"]["if"])
        gate = next(s for s in jobs["gate"]["steps"] if s.get("id") == "gate")
        self.assertEqual("bash .github/scripts/library_announce/gate.sh", gate["run"])
        self.assertEqual("${{ github.event.workflow_run.id }}", gate["env"]["WORKFLOW_RUN_ID"])
        self.assertEqual("${{ github.event.workflow_run.run_attempt }}", gate["env"]["WORKFLOW_RUN_ATTEMPT"])

    def test_each_channel_is_its_own_locked_resumable_job(self):
        jobs = self.doc["jobs"]
        for channel in ("forum", "yemot"):
            job = jobs[channel]
            self.assertEqual(f"{channel} ${{{{ needs.gate.outputs.tag }}}}", job["name"])
            self.assertEqual(["gate", "prepare"], job["needs"])
            self.assertEqual("read", job["permissions"]["actions"])
            self.assertIn(f"contains(fromJSON(needs.gate.outputs.channels), '{channel}')", job["if"])
            self.assertIn("needs.gate.outputs.send == 'true'", job["if"])
            self.assertEqual(f"library-update-announce-{channel}-${{{{ needs.gate.outputs.tag }}}}",
                             job["concurrency"]["group"])
            self.assertIs(False, job["concurrency"]["cancel-in-progress"])
            steps = {step.get("name") or step.get("uses"): step for step in job["steps"]}
            dedup_step = steps["Skip if this tag was already announced here"]["run"]
            self.assertIn('decision="$(bash .github/scripts/library_announce/channel_decision.sh)"', dedup_step)
            self.assertNotIn("echo \"sent=$(", dedup_step)
            self.assertIn("steps.dedup.outputs.sent != 'true'", steps["Announce"]["if"])
            self.assertIn(f"-n {channel}-progress", steps["Restore progress from an earlier attempt"]["run"])
            self.assertIn("--progress", steps["Announce"]["run"])
            upload = steps["actions/upload-artifact@v6"]
            self.assertTrue(upload["if"].startswith("always()"))
            self.assertEqual(f"{channel}-progress", upload["with"]["name"])
        dedup = (Path(__file__).resolve().parent / "already_sent.sh").read_text(encoding="utf-8")
        self.assertIn("filter=all", dedup)
        self.assertIn('JOB_NAME="$CHANNEL $TAG"', dedup)

    def test_release_job_attaches_the_notes_once_under_the_name_reuse_ignores(self):
        job = self.doc["jobs"]["release"]
        self.assertEqual("release ${{ needs.gate.outputs.tag }}", job["name"])
        self.assertEqual(["gate", "prepare"], job["needs"])
        self.assertEqual({"contents": "write", "actions": "read"}, job["permissions"])
        self.assertIn("contains(fromJSON(needs.gate.outputs.channels), 'release')", job["if"])
        self.assertIn("needs.gate.outputs.send == 'true'", job["if"])
        self.assertIn("needs.prepare.outputs.has_changes == 'true'", job["if"])
        self.assertEqual("library-update-announce-release-${{ needs.gate.outputs.tag }}", job["concurrency"]["group"])
        steps = {step.get("name") or step.get("uses"): step for step in job["steps"]}
        self.assertIn('decision="$(bash .github/scripts/library_announce/channel_decision.sh)"',
                      steps["Skip if this tag was already announced here"]["run"])
        announce = steps["Announce"]
        self.assertIn("steps.dedup.outputs.sent != 'true'", announce["if"])
        self.assertIn(f"out/{lua.RELEASE_NOTES_ASSET}\"", announce["run"])
        self.assertIn("gh release upload \"$TAG\" \"$notes\" --clobber", announce["run"])
        self.assertIn(f'select(.name == "{lua.RELEASE_NOTES_ASSET}")', announce["run"])
        # Every other asset still counts, so a reuse candidate must match its provenance exactly.
        self.assertIn(f'[.assets[]|select(.name!="{lua.RELEASE_NOTES_ASSET}")]|length',
                      RELEASE.read_text(encoding="utf-8"))

    def test_no_google_chat_left(self):
        script = (Path(__file__).resolve().parent / "library_update_announce.py").read_text(encoding="utf-8")
        for text in (self.text, script):
            self.assertNotIn("chat", text.lower())

    def catalog_step(self):
        return next(s["run"] for s in self.doc["jobs"]["prepare"]["steps"] if s.get("name") == "Build both catalogs")

    def test_databases_stream_into_zstd_with_the_long_window(self):
        run = self.catalog_step()
        self.assertIn('--jq "$FULL_DB_ASSET_JQ | .name // empty"', run)
        self.assertIn('stream_full_db "$tag" "$asset" "$GITHUB_REPOSITORY"'
                      ' | zstd -d -q --long=31 --memory=2048MB -o "$db"', run)
        # Saving helpers would put the .zst (or its parts) on disk beside the DB.
        for saving in ("download_full_db_by_name", "gh release download", "--dir", "-D "):
            self.assertNotIn(saving, run)
        self.assertIn('rm -f "$db"', run)
        names = (ROOT / ".github" / "scripts" / "db_asset_names.sh").read_text(encoding="utf-8")
        stream = names[names.index("stream_full_db() {"):]
        stream = stream[:stream.index("\n}\n")]
        self.assertEqual(3, stream.count("-O -"), stream)
        self.assertNotIn("--dir", stream)

    def run_catalog_step(self, releases):
        """releases: {previous_tag: ..., current_tag: (asset, split_db, split_asset)} -> assets served."""
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        root = Path(tmp)
        binaries = root / "bin"
        binaries.mkdir()
        for tag, (name, split_db, split_asset) in releases.items():
            release = root / tag
            release.mkdir()
            (root / "built" / tag).mkdir(parents=True)
            db = root / "built" / tag / name
            make_db(db, [(1, 2, 1, "book")], [(1, 0, "text")], split=split_db)
            if split_asset:
                size = db.stat().st_size
                subprocess.run([shutil.which("bash"), str(ROOT / ".github/scripts/split_release_asset.sh"),
                                str(db), str(release), str(size // 2 + 1)], check=True, capture_output=True)
                self.assertFalse((release / name).exists())
            else:
                shutil.copy(db, release / name)
            assets = [{"name": p.name} for p in sorted(release.iterdir())]
            assets.append({"name": "patch-v4-v6.db.zst"})
            if name != "seforim.db.zst":
                assets.append({"name": "seforim.db.zst"})
            (release / "release.json").write_text(json.dumps({"assets": assets}), encoding="utf-8")
        gh = binaries / "gh"
        gh.write_text(f"#!{sys.executable}\n" + """
import os, subprocess, sys
from pathlib import Path
args = sys.argv[1:]
root = Path(os.environ['FIXTURES'])
if args[0] == 'api':
    tag = args[1].rsplit('/', 1)[-1]
    query = args[args.index('--jq') + 1]
    result = subprocess.run(['jq', '-r', query], input=(root / tag / 'release.json').read_text(), text=True)
    sys.exit(result.returncode)
if args[:2] != ['release', 'download']:
    sys.exit(f'stub gh: unexpected {args}')
tag, opts = args[2], args[3:]
opt = lambda flag: opts[opts.index(flag) + 1] if flag in opts else None
if opt('-R') != os.environ['GITHUB_REPOSITORY']:
    sys.exit(f'stub gh: download without -R $GITHUB_REPOSITORY: {args}')
if opt('-O') != '-':
    sys.exit(f'stub gh: the catalog step must stream, not save: {args}')
src = root / tag / opt('--pattern')
if not src.is_file():
    sys.exit(f'no assets match the file pattern {src.name}')
with (root / 'downloads.log').open('a') as log:
    log.write(f'{tag}/{src.name}\\n')
sys.stdout.buffer.write(src.read_bytes())
""", encoding="utf-8")
        zstd = binaries / "zstd"
        zstd.write_text(f"#!{sys.executable}\n" + """
import sys
from pathlib import Path
args = sys.argv[1:]
for flag in ('-d', '--long=31', '--memory=2048MB', '-o'):
    if flag not in args:
        sys.exit(f'stub zstd: missing {flag} in {args}')
Path(args[args.index('-o') + 1]).write_bytes(sys.stdin.buffer.read())
""", encoding="utf-8")
        python = binaries / "python"
        python.write_text(f"#!/bin/sh\nexec '{sys.executable}' \"$@\"\n", encoding="utf-8")
        for path in (gh, zstd, python):
            path.chmod(0o755)
        previous, current = releases
        result = subprocess.run([shutil.which("bash"), "-c", self.catalog_step()], cwd=ROOT,
                                env=dict(os.environ, PATH=f"{binaries}{os.pathsep}{os.environ['PATH']}",
                                         RUNNER_TEMP=tmp, PREVIOUS=previous, TAG=current,
                                         GITHUB_REPOSITORY="Otzaria/SeforimLibrary", FIXTURES=tmp),
                                capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        work = root / "announce"
        self.assertEqual(["current.json", "previous.json"], sorted(p.name for p in work.iterdir()))
        self.assertEqual(json.loads((work / "previous.json").read_text()),
                         json.loads((work / "current.json").read_text()))
        return (root / "downloads.log").read_text().splitlines()

    @unittest.skipUnless(shutil.which("bash") and shutil.which("jq"), "bash and jq are required")
    def test_catalog_workflow_downloads_the_full_asset_for_each_release_schema(self):
        downloads = self.run_catalog_step({"v5": ("seforim.db.zst", False, False),
                                           "v6": ("seforim-schema6.db.zst", True, False)})
        self.assertEqual(["v5/seforim.db.zst", "v6/seforim-schema6.db.zst"], downloads)

    @unittest.skipUnless(shutil.which("bash") and shutil.which("jq") and shutil.which("split")
                         and shutil.which("sha256sum"), "bash, jq, split and sha256sum are required")
    def test_catalog_workflow_streams_a_split_full_asset_part_by_part(self):
        downloads = self.run_catalog_step({"v5": ("seforim.db.zst", False, False),
                                           "v6": ("seforim-schema6.db.zst", True, True)})
        self.assertEqual(["v5/seforim.db.zst", "v6/seforim-schema6.db.zst.manifest.json",
                          "v6/seforim-schema6.db.zst.part-000", "v6/seforim-schema6.db.zst.part-001"],
                         downloads)

    def test_hosted_python_is_set_up_before_pip(self):
        checked = 0
        for job in self.doc["jobs"].values():
            uses = [step.get("uses", "") for step in job["steps"]]
            runs = [step.get("run", "") for step in job["steps"]]
            pips = [i for i, r in enumerate(runs) if "pip install" in r]
            if not pips:
                continue
            checked += 1
            setup = next(i for i, u in enumerate(uses) if u.startswith("actions/setup-python@"))
            self.assertLess(setup, pips[0])
        self.assertEqual(3, checked)

    def test_ci_installs_what_these_tests_import(self):
        ci = yaml.safe_load(CI.read_text(encoding="utf-8"))
        job = ci["jobs"]["announcer"]
        install = next(s["run"] for s in job["steps"] if "pip install" in s.get("run", ""))
        for package in ("requests", "pyluach", "tzdata", "pyyaml", "zstandard"):
            self.assertIn(package, install)


if __name__ == "__main__":
    unittest.main()
