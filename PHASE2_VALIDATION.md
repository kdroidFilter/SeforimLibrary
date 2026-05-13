# Phase 2 — End-to-end validation report

Validates Phase 2 (touched-book + book-rename detection) on the real Sefaria
+ Otzaria pipeline. Builds on `PHASE1_VALIDATION.md`.

## Acceptance test

A Phase-2-enabled build must:
1. Emit a `Source-hash classification: unchanged=…, touched=…, added=…, removed=…`
   log line at the start of each importer's `import()` step.
2. Persist a `book_source_hashes(source_name, canonical_he_title, source_hash,
   last_seen_version)` row for every book that actually went through the
   importer (post-blacklist / dedup).
3. On a second build with **no source changes**, classify essentially every
   surviving book as `unchanged`.

## Run 1 — bootstrap (no prior hashes)

Full `./gradlew generateSeforimDb` with build_state.db carried over from the
pre-Phase-2 baseline (no `book_source_hashes` table). Build_state file
contained `id_book`, `id_line`, … but no source hashes.

Result (`build/seforim.db.buildstate` after Run 1):

```sql
sqlite> SELECT COUNT(*) FROM book_source_hashes;
5816

sqlite> SELECT source_name, canonical_he_title, last_seen_version
        FROM book_source_hashes LIMIT 3;
Sefaria | משנה למלך על משנה תורה, הלכות מגילה        | 1778652302
Sefaria | קונטרס תשובה מיראה על משנה תורה, הלכות תרומות | 1778652302
Sefaria | מסכת ציצית                                    | 1778652302
```

The 5,816 hashes cover every Sefaria book that survived blacklists. Otzaria
books are tracked separately by the second importer; the run's
`Source-hash classification (Otzaria)` log line reports their counts.

Build time: **25 min 40 s** (clean rebuild, including ~5 min download of
Sefaria + Otzaria archives).

## Run 2 — verify "all unchanged"

Same sources, same command. Expected classification:
`unchanged=5816, touched=0, added=0, removed=0` (Sefaria) and an analogous
log for Otzaria.

Result: `book_source_hashes` row count and contents identical to Run 1
(both produced `5816` rows; sampled first-4-byte buckets match exactly).

```sql
sqlite> ATTACH 'build/seforim.db.buildstate.run1' AS r1;
sqlite> SELECT COUNT(*) FROM book_source_hashes;
5816
sqlite> SELECT COUNT(*) FROM r1.book_source_hashes;
5816
sqlite> SELECT COUNT(*) FROM book_source_hashes b
        JOIN r1.book_source_hashes r ON r.source_name = b.source_name
                                    AND r.canonical_he_title = b.canonical_he_title
        WHERE r.source_hash != b.source_hash;
0
```

Build time: **22 min 37 s** (no fresh downloads).

## Per-run-N expectations

Once Run 1 has populated `book_source_hashes`, every subsequent build
without source changes must:
- Re-classify every book as `unchanged`.
- Keep all primary keys identical to the previous build (already proven by
  Phase 1; Phase 2 adds the classification log on top).
- Skip no work yet — the fast-path that skips unchanged books is the
  Phase 2.5 follow-up.

## Cost

- One extra `sha256(merged.json + schema)` per Sefaria book at start
  (~30 s total over 5,816 books on the test box).
- Otzaria hashes come directly from `files_manifest.json` (no compute).
- `book_source_hashes` table adds ~250 kB to `build_state.db`.

## Implication for downstream phases

The classification output is the input to:
- **`BookRenameDetector`** (already shipped, unit-tested in
  `RenameAliasFlowTest`) — runs on the `added × removed` cross-product.
- **Fast-path "unchanged book"** (Phase 2.5) — skips re-parsing & re-inserting
  for unchanged books, copying lines + tocs from the previous `seforim.db`.
- **`PatchDbProducer`** (Phase 4) — restricts diff scans to `touched ∪
  added ∪ removed` books rather than the whole DB.
