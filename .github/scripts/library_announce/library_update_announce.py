#!/usr/bin/env python3
"""Announce what a published Seforim DB release changed, book by book.

The announcement compares the new release with the previous *published* one
(pre-releases never reach users, so they are skipped), and describes the two
databases themselves — Otzaria and Sefaria books alike — rather than any
source repository's file tree.

  previous-tag --current TAG            tag names on stdin -> the previous published tag
  catalog DB OUT                        seforim.db -> a small per-book JSON catalog
  render --old A --new B --tag T --out DIR   diff two catalogs -> forum posts + Yemot content + release notes
  send --channel C --dir DIR            post one rendered channel (forum / yemot)

The release channel is uploaded by the workflow itself: RELEASE_NOTES_ASSET as is.
"""
import argparse
import hashlib
import html
import json
import os
import re
import sqlite3
import sys
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Jerusalem")
TAG_RE = re.compile(r"^v([1-9][0-9]*)-[0-9]{14}$")
CATALOG_SCHEMA = 2
FORUM_URL = "https://otzaria.org/forum"
FORUM_TOPIC_ID = 20
# maximumPostLength from https://otzaria.org/forum/api/config (read 2026-09-29).
FORUM_MAX_POST_LENGTH = 32767
CHANNELS = ("forum", "yemot", "release")
SENT_CHANNELS = ("forum", "yemot")
# The reuse scan in manual-generate-release.yml leaves exactly this name out of the asset count.
RELEASE_NOTES_ASSET = "library-changes.md"
YEMOT_PATH = "ivr2:/1"
TZINTUK_LIST = "books update"

SECTIONS = (
    ("added", "התווספו הספרים הבאים:"),
    ("removed", "נמחקו הספרים הבאים:"),
    ("moved", "שונה מיקום/שם של הספרים הבאים:"),
    ("changed", "השתנו הספרים הבאים:"),
)
ALSO_CHANGED = " (השתנה גם תוכנו)"
ALSO_MOVED = " (שונה גם מיקומו/שמו)"

# Only real markup: literal text such as <חסר> or <בד"ה ...> is part of the book.
HTML_TAG_RE = re.compile(r"<[A-Za-z/!?][^>]*>")
WHITESPACE_RE = re.compile(r"\s+")


def db_version(tag):
    match = TAG_RE.fullmatch(tag)
    return int(match.group(1)) if match else None


def previous_published_tag(current, tags):
    """The newest published tag strictly older than `current`, or None."""
    current_version = db_version(current)
    if current_version is None:
        raise ValueError(f"{current!r} is not a v<N>-<timestamp> release tag")
    older = [(v, t) for t in tags if (v := db_version(t)) is not None and v < current_version]
    return max(older)[1] if older else None


def plain_text(content):
    """A line as the reader sees it: tags stripped, entities decoded, whitespace collapsed."""
    # A tag is at most a word boundary: v29 turned spaces into <br> in 855 books.
    # A space added or removed between two words, and any nikud/teamim change, IS a change.
    return WHITESPACE_RE.sub(" ", html.unescape(HTML_TAG_RE.sub(" ", content))).strip()


def category_paths(conn):
    rows = {cid: (parent, title) for cid, parent, title in conn.execute("SELECT id, parentId, title FROM category")}
    paths = {}
    for cid in rows:
        parts, seen, node = [], set(), cid
        # Roots have parentId NULL; a cycle or a dangling parent is corrupt data.
        while node is not None:
            if node in seen or node not in rows:
                raise ValueError(f"category {cid}: broken parent chain at {node!r}")
            seen.add(node)
            parent, title = rows[node]
            parts.append(title)
            node = parent
        paths[cid] = "/".join(reversed(parts))
    return paths


def book_editions(conn, split=False):
    """bookId -> (versionTitle, display title) of the edition the book's own lines are.

    A book with only metadata rows (hasContent=0) has one version, and its lines
    ARE that edition. Otherwise the edition is the one version whose every
    version_line row equals the book's line and that covers every line any of the
    book's editions covers. A mosaic of editions, or a tie, has no one edition: None.
    In the split shape, a present version_line with NULL content inherits base text.
    """
    covered = dict(conn.execute(
        "SELECT bv.bookId, COUNT(DISTINCT vl.lineId) FROM book_version bv"
        " JOIN version_line vl ON vl.versionId = bv.id GROUP BY bv.bookId"))
    content = "lc.content" if split else "l.content"
    version_content = f"COALESCE(vl.content, {content})" if split else "vl.content"
    content_join = " LEFT JOIN line_content lc ON lc.id = l.id" if split else ""
    versions = {}
    for book_id, title, he_title, has_content, rows, equal in conn.execute(
            "SELECT bv.bookId, bv.versionTitle, bv.heVersionTitle, bv.hasContent,"
            f" COUNT(vl.lineId), COALESCE(SUM({version_content} = {content}), 0)"
            " FROM book_version bv LEFT JOIN version_line vl ON vl.versionId = bv.id"
            " LEFT JOIN line l ON l.id = vl.lineId" + content_join + " GROUP BY bv.id"):
        versions.setdefault(book_id, []).append((title, he_title, has_content, rows, equal))
    editions = {}
    for book_id, rows in versions.items():
        if any(r[2] for r in rows):
            rows = [r for r in rows if r[2] and 0 < r[3] == r[4] == covered[book_id]]
        if len(rows) != 1:
            editions[book_id] = None
            continue
        title, he_title = rows[0][:2]
        # The application shows heVersionTitle when non-blank, else versionTitle.
        editions[book_id] = (title, he_title if he_title and he_title.strip() else title)
    return editions


def line_text_decoder(conn):
    """A DB with zstd_dict stores line text as zstd frames of its one dictionary."""
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='zstd_dict'").fetchone():
        return lambda value: value
    import zstandard

    [(dictionary,)] = conn.execute("SELECT dict FROM zstd_dict")
    decompressor = zstandard.ZstdDecompressor(dict_data=zstandard.ZstdCompressionDict(dictionary))
    return lambda value: decompressor.decompress(value).decode("utf-8") if isinstance(value, bytes) else value


def build_catalog(db_path):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        split = bool(conn.execute(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='line_content')"
            " AND NOT EXISTS(SELECT 1 FROM pragma_table_info('line') WHERE name='content')"
        ).fetchone()[0])
        paths = category_paths(conn)
        sources = dict(conn.execute("SELECT id, name FROM source"))
        editions = book_editions(conn, split)
        books = {}
        for book_id, title, category_id, source_id in conn.execute(
                "SELECT id, title, categoryId, sourceId FROM book"):
            edition = editions.get(book_id)
            books[book_id] = {"id": book_id, "title": title, "path": paths[category_id],
                              "source": sources[source_id], "lines": 0, "chars": 0, "hash": None,
                              "edition": edition[0] if edition else None,
                              "edition_name": edition[1] if edition else None}
        digest, current = None, None

        def flush():
            books[current]["hash"] = digest.hexdigest()

        decode = line_text_decoder(conn)
        content_sql = "lc.content" if split else "l.content"
        content_join = " JOIN line_content lc ON lc.id = l.id" if split else ""
        for book_id, content in conn.execute(
                f"SELECT l.bookId, {content_sql} FROM line l" + content_join + " ORDER BY l.bookId, l.lineIndex"):
            if book_id != current:
                if current is not None:
                    flush()
                if book_id not in books:
                    raise ValueError(f"line rows reference missing book {book_id}")
                current, digest = book_id, hashlib.sha256()
            text = plain_text(decode(content))
            # Line breaks count as whitespace, so re-wrapping lines is not a change.
            if text:
                if books[book_id]["chars"]:
                    digest.update(b" ")
                digest.update(text.encode("utf-8"))
                books[book_id]["chars"] += len(text)
            books[book_id]["lines"] += 1
        if current is not None:
            flush()
        return {"schema": CATALOG_SCHEMA, "books": sorted(books.values(), key=lambda b: b["id"])}
    finally:
        conn.close()


def display_name(book):
    return f"{book['path']}/{book['title']}"


def entry(book, **extra):
    value = {"name": display_name(book), "title": book["title"]}
    value.update(extra)
    return value


def diff_catalogs(old, new):
    """Structured changes: every list is sorted by the (new) display name."""
    old_books = {b["id"]: b for b in old["books"]}
    new_books = {b["id"]: b for b in new["books"]}
    added = [new_books[i] for i in sorted(new_books.keys() - old_books.keys())]
    removed = [old_books[i] for i in sorted(old_books.keys() - new_books.keys())]

    # A book's id is keyed by its title, so a rename surfaces as remove + add of identical text.
    def by_hash(books):
        index = {}
        for book in books:
            if book["chars"]:
                index.setdefault(book["hash"], []).append(book)
        return index

    removed_by_hash, added_by_hash = by_hash(removed), by_hash(added)
    renames = []
    for text_hash in sorted(removed_by_hash.keys() & added_by_hash.keys()):
        before, after = removed_by_hash[text_hash], added_by_hash[text_hash]
        if len(before) != 1 or len(after) != 1:
            raise ValueError("ambiguous rename: identical text in removed "
                             f"{[display_name(b) for b in before]} and added {[display_name(b) for b in after]}")
        renames.append((before[0], after[0]))
    paired = {id(book) for pair in renames for book in pair}

    moved, changed = [], []
    for before, after in renames:
        moved.append(entry(after, old_name=display_name(before), old_title=before["title"], also_changed=False))
    for book_id in sorted(old_books.keys() & new_books.keys()):
        before, after = old_books[book_id], new_books[book_id]
        was_moved = display_name(before) != display_name(after)
        text_changed = before["hash"] != after["hash"]
        # Only a known edition replacing another known one: None is a mosaic, not an edition.
        edition = (after["edition_name"] if before["edition"] and after["edition"]
                   and after["edition"] != before["edition"] else None)
        if was_moved:
            moved.append(entry(after, old_name=display_name(before), old_title=before["title"],
                               also_changed=text_changed or edition is not None))
        if text_changed or edition:
            changed.append(entry(after, edition=edition, also_moved=was_moved))

    def sort(entries):
        return sorted(entries, key=lambda e: e["name"])

    return {
        "added": sort(entry(b) for b in added if id(b) not in paired),
        "removed": sort(entry(b) for b in removed if id(b) not in paired),
        "moved": sort(moved),
        "changed": sort(changed),
    }


def describe(key, item, spoken=False):
    """One item as a line; Yemot reads titles, the forum full category paths."""
    name = item["title"] if spoken else item["name"]
    if key == "moved":
        old = item["old_title"] if spoken else item["old_name"]
        text = f"{name} (לשעבר: {old})" if old != name else name
        return text + (ALSO_CHANGED if item["also_changed"] else "")
    if key == "changed":
        text = f"{name} - שינוי מהדורה ל-{item['edition']}" if item["edition"] else name
        return text + (ALSO_MOVED if item["also_moved"] else "")
    return name


def forum_lines(changes):
    return {key: [f"* {describe(key, item)}" for item in changes[key]]
            for key, _ in SECTIONS if changes[key]}


def post_length(text):
    # NodeBB compares JavaScript string length: UTF-16 code units.
    return len(text.encode("utf-16-le")) // 2


def build_forum_posts(changes, version, date_text, limit=FORUM_MAX_POST_LENGTH):
    """The full list, split on line boundaries into consecutive posts of at most `limit`."""
    heading = f"# גרסת ספרייה {version}"
    posts, current = [], f"{heading}\n\n**עדכון {date_text}**\n"
    for key, lines in forum_lines(changes).items():
        title = dict(SECTIONS)[key]
        block = f"\n## {title}\n"
        for line in lines:
            addition = block + line + "\n"
            if post_length(current + addition) > limit:
                posts.append(current)
                current = f"{heading} (המשך)\n\n## {title}{' (המשך)' if not block else ''}\n"
                addition = line + "\n"
                if post_length(current + addition) > limit:
                    raise ValueError(f"a single line exceeds the forum limit {limit}: {line[:80]!r}")
            current += addition
            block = ""
    posts.append(current)
    return posts


def build_release_notes(changes, version, date_text):
    """The whole list as one Markdown file, worded like the forum."""
    [notes] = build_forum_posts(changes, version, date_text, limit=float("inf"))
    return notes


def build_yemot_content(changes):
    """Heading -> the book names read out, one per line; same sections and items as the forum."""
    return {title: "\n".join(describe(key, item, spoken=True) for item in changes[key])
            for key, title in SECTIONS if changes[key]}


def heb_date(day):
    from pyluach import dates
    return dates.HebrewDate.from_pydate(day).hebrew_date_string()


class ForumRefused(RuntimeError):
    # NodeBB's own error keys stay untranslated, so a locale change cannot hide the rate limit.
    RATE_LIMIT_MARKERS = ("ניתן לפרסם פוסט רק פעם ב", "too-many-posts", "still-posting")

    @property
    def retryable(self):
        return any(marker in str(self) for marker in self.RATE_LIMIT_MARKERS)


def post_to_forum(text, token, session=None):
    """One reply through NodeBB's Write API; raises ForumRefused if the forum refused it."""
    if session is None:
        import requests
        session = requests
    response = session.post(
        f"{FORUM_URL}/api/v3/topics/{FORUM_TOPIC_ID}",
        json={"content": text},
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    try:
        status = response.json().get("status") or {}
    except ValueError:
        raise ForumRefused(f"http-{response.status_code}: {response.text[:200]!r}") from None
    if status.get("code") != "ok":
        raise ForumRefused(f"{status.get('code') or response.status_code}: {status.get('message', '')}")


def send_forum(text, token, attempts=4, delay=5.0, post=post_to_forum):
    for attempt in range(1, attempts + 1):
        try:
            post(text, token)
            return
        except ForumRefused as exc:
            # Only the post-rate refusal is retried: after a transport error the
            # post may already exist, and a duplicate is worse than a miss.
            if not exc.retryable or attempt == attempts:
                raise
            time.sleep(delay * attempt)


def load_progress(path, payload):
    """A channel's progress from an earlier attempt, bound to the exact payload bytes."""
    digest = hashlib.sha256(payload).hexdigest()
    if path and Path(path).exists():
        state = json.loads(Path(path).read_text(encoding="utf-8"))
        if state.get("payload_sha256") != digest:
            raise SystemExit(f"::error::progress {path} was recorded for another announcement")
        return state
    return {"payload_sha256": digest}


def progress_saver(path):
    def save(state):
        if not path:
            return
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(state, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        temporary.replace(target)
    return save


def send_forum_posts(posts, token, state=None, save=None, send=None):
    """Posts in order, from state["sent"] on; records each post the forum accepted."""
    send = send or send_forum
    state = state if state is not None else {}
    save = save or (lambda _: None)
    start = state.get("sent", 0)
    if not 0 <= start <= len(posts):
        raise SystemExit(f"::error::progress says {start} posts were sent, but there are {len(posts)}")
    if start:
        print(f"forum: posts 1-{start} were sent by an earlier attempt")
    for index in range(start, len(posts)):
        text = posts[index]
        try:
            send(text, token)
        except Exception as exc:
            raise RuntimeError(f"forum post {index + 1}/{len(posts)} failed; posts 1-{index} were sent") from exc
        state["sent"] = index + 1
        save(state)
        print(f"forum post {index + 1}/{len(posts)} sent ({post_length(text)} chars)")


def send_yemot(content, heading, token, state=None, save=None):
    from yemot import split_and_send
    split_and_send(content, heading, token, YEMOT_PATH, TZINTUK_LIST, progress=state, save=save)


def parse_channels(text):
    channels = [c.strip() for c in text.split(",") if c.strip()]
    unknown = set(channels) - set(CHANNELS)
    if unknown or not channels:
        raise SystemExit(f"::error::unknown or empty channel list {text!r}; allowed: {', '.join(CHANNELS)}")
    return channels


def load_catalog(path):
    catalog = json.loads(Path(path).read_text(encoding="utf-8"))
    if catalog.get("schema") != CATALOG_SCHEMA:
        raise SystemExit(f"::error::unsupported catalog schema {catalog.get('schema')!r}")
    return catalog


def render(args):
    version = db_version(args.tag)
    if version is None:
        raise SystemExit(f"::error::{args.tag!r} is not a v<N>-<timestamp> release tag")
    channels = parse_channels(args.channels)
    changes = diff_catalogs(load_catalog(args.old), load_catalog(args.new))
    editions = sum(1 for item in changes["changed"] if item["edition"])
    print("changes: " + " ".join(f"{k}={len(v)}" for k, v in changes.items()) + f" edition-changed={editions}")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "changes.json").write_text(json.dumps(changes, ensure_ascii=False, indent=1), encoding="utf-8")
    has_changes = any(changes.values())
    summary = {"tag": args.tag, "channels": channels, "has_changes": has_changes,
               "counts": {k: len(v) for k, v in changes.items()}, "edition_changed": editions}
    if has_changes:
        date_text = heb_date(date.fromisoformat(args.as_of) if args.as_of else datetime.now(tz=TZ).date())
        posts = build_forum_posts(changes, version, date_text)
        yemot = {"heading": f"גרסת ספרייה {version}\nעדכון {date_text}\n", "content": build_yemot_content(changes)}
        (out / "forum_posts.json").write_text(json.dumps(posts, ensure_ascii=False, indent=1), encoding="utf-8")
        (out / "yemot.json").write_text(json.dumps(yemot, ensure_ascii=False, indent=1), encoding="utf-8")
        (out / RELEASE_NOTES_ASSET).write_text(build_release_notes(changes, version, date_text), encoding="utf-8")
        summary["forum_posts"] = [post_length(p) for p in posts]
        for index, text in enumerate(posts, 1):
            print(f"----- forum post {index}/{len(posts)} ({post_length(text)} chars) -----")
            print(text)
        print("------------------------")
    else:
        print("⏭️  No book changed since the previous published release — nothing to announce.")
    (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as sink:
            sink.write(f"has_changes={'true' if has_changes else 'false'}\n")
            sink.write(f"channels={json.dumps(channels)}\n")
    return 0


def send(args):
    secret = {"forum": "FORUM_TOKEN", "yemot": "TOKEN_YEMOT"}[args.channel]
    token = os.environ.get(secret, "")
    if not token.strip():
        raise SystemExit(f"::error::{secret} is empty — set the repository secret before announcing to {args.channel}")
    payload = (Path(args.dir) / ("forum_posts.json" if args.channel == "forum" else "yemot.json")).read_bytes()
    state, save = load_progress(args.progress, payload), progress_saver(args.progress)
    value = json.loads(payload.decode("utf-8"))
    if args.channel == "forum":
        send_forum_posts(value, token, state, save)
    else:
        send_yemot(value["content"], value["heading"], token, state, save)
    print(f"✅ {args.channel} announced")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    previous = commands.add_parser("previous-tag")
    previous.add_argument("--current", required=True)
    catalog = commands.add_parser("catalog")
    catalog.add_argument("db")
    catalog.add_argument("out")
    rendered = commands.add_parser("render")
    rendered.add_argument("--old", required=True)
    rendered.add_argument("--new", required=True)
    rendered.add_argument("--tag", required=True)
    rendered.add_argument("--as-of", default="")
    rendered.add_argument("--channels", default=",".join(CHANNELS))
    rendered.add_argument("--out", required=True)
    sender = commands.add_parser("send")
    sender.add_argument("--channel", required=True, choices=SENT_CHANNELS)
    sender.add_argument("--dir", required=True)
    sender.add_argument("--progress", default="")
    args = parser.parse_args(argv)

    if args.command == "previous-tag":
        tag = previous_published_tag(args.current, sys.stdin.read().split())
        print(tag or "")
        return 0
    if args.command == "catalog":
        value = build_catalog(args.db)
        with open(args.out, "w", encoding="utf-8") as out:
            json.dump(value, out, ensure_ascii=False, separators=(",", ":"))
        print(f"catalog: {len(value['books'])} books -> {args.out}")
        return 0
    if args.command == "render":
        return render(args)
    return send(args)


if __name__ == "__main__":
    sys.exit(main())
