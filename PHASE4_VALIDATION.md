# Phase 4 — End-to-end validation report

Validates Phase 4 (patch.db production + apply) on the real Sefaria + Otzaria
seforim.db output (the Phase-3 Test C scenario: insert a line at the top of
Genesis chapter 1).

## Pipeline

```
./gradlew :generator-common:producePatchAndVerify \
  -PprevDb=$PWD/build/seforim.db.phase3-baseline \
  -PnewDb=$PWD/build/seforim.db \
  -Pout=$PWD/build/patch-phase3.db \
  -PfromVersion=1 -PtoVersion=2
```

## Observed output

```
Info: Produced patch.db at build/patch-phase3.db
       — upserts=594,171, deletes=51 (from v1 to v2)
Info: Patch applied — migrations=0
       upserts={book=1, tocEntry=49, line=1583, link=592,538, line_toc=0,
                source=0, author=0, topic=0, pub_place=0, pub_date=0,
                connection_type=0, category=0, tocText=0}
       deletes={link=2, line=0, tocEntry=1600, tocText=0, book=0, category=0}
Warn:  DB carries 306,481 pre-existing FK violations — tolerated,
       not introduced by this patch.
```

- `patch-phase3.db` weight: **16 MB** for a delta whose underlying seforim.db
  is **7.2 GB** — ~**450× compression** ratio on this synthetic scenario.
- Patch apply runs cleanly under a single SQLite transaction, with FK checks
  showing no *new* violations introduced.

## Limitations (Phase 4.5 scope)

The producer/applier cover the **single-id tables** + `line_toc` (composite
PK, handled specially). The following tables are NOT yet diffed/applied
and account for the post-apply logical-hash mismatch:

| Table | Reason it's missing |
|-------|---------------------|
| `category_closure`        | recomputed at end of generation, not surfaced through allocator |
| `book_pub_place` / `book_pub_date` / `book_topic` / `book_author` | book-attribute junctions; rebuild required |
| `alt_toc_structure` / `alt_toc_entry` / `line_alt_toc` | per-book alt structures; ids stable but not patch-tracked |
| `book_has_links`          | derived from links — recomputable on apply |
| `book_acronym`            | small lookup; rebuild required |
| `default_commentator` / `default_targum` | metadata; rebuild required |
| `schema_meta`             | version columns; updated separately by the orchestrator |

A Phase 4.5 follow-up will extend `PatchDbSchema` / `PatchDbProducer` /
`PatchApplier` to cover these. The pattern is straightforward (the
composite-PK tables follow the same shape as `line_toc`'s special case),
but each table needs explicit configuration so columns and conflict keys
match.

## Pre-existing FK violation count

`seforim.db.phase3-baseline` carries **306,481** `tocEntry.textId → tocText`
violations — tocEntries pointing at tocText ids that were never inserted.
This is a pre-existing data-integrity issue from the generation pipeline,
unrelated to Phase 4. The applier counts violations **before vs after**
apply and accepts as long as the patch does not increase the count.
Cleaning these up is tracked separately.

## Compression / payload signal

A real release delta will be larger than this synthetic test (this test
only touches Genesis chapter 1, leaving the rest of the DB unchanged), but
the order of magnitude is encouraging:

| Metric | Value |
|--------|-------|
| Full seforim.db | 7.2 GB |
| patch-phase3.db | 16 MB |
| Compression ratio | ~450× |
| Apply duration on a 7.2 GB DB | ~30 seconds |

Once Phase 4.5 closes the junction-table gap, the `verifyApplyChain` CI
gate (DELTA_UPDATE_PLAN.md §6.8) will become a strict `prev + patch ==
new` invariant rather than a "no new FK violations" check.
