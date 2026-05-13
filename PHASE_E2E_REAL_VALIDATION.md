# Real e2e validation — first attempt on production data

> Run on 2026-05-13 against newly-released Otzaria + SefariaExport
> sources, with v1 = an existing 7.2 GB `seforim.db` and a fresh v2
> build seeded from v1's `seforim.db.buildstate`.

## What the test caught

Three real importer wiring holes that the producer's
`assertNoSecondaryUniqueCollisions` pre-check (4cf7347) refused to
ship around. Each one is documented and fixed below.

### Hole 1 — `author` / `topic` / `pub_place` / `pub_date` (`a6244c1`)

`SefariaDirectImporter` and `Generator.kt` constructed
`Author(name = …)` / `Topic(name = …)` / `PubPlace(name = …)` /
`PubDate(date = …)` and let `SeforimRepository.insertBook` fall back
to `INSERT OR IGNORE … (name)` (auto-increment). The allocator was
never consulted, so the same name got a different id on every build.

Now every construction site pre-resolves the id through
`IdAllocatorBindings.upsert{Author,Topic,PubPlace,PubDate}`.

### Hole 2 — Otzaria's `tocText` reservation vs insertion (`86801ed`)

`Generator.stableTocEntryId` called `bindings.allocator.tocTextId(text)`
which only **reserves** an id in the allocator's id_lookup. The
actual row went through `repo.insertTocText(text)`'s auto-increment
path. So the allocator thought "X" was at id 16368 while the table
had "X" at id 17546.

Now it calls `bindings.upsertTocText(text)` which both reserves and
inserts with `insertTocTextWithId`.

### Hole 3 — Sefaria alt-TOC builder (`2c43478`)

`SefariaAltTocBuilder` constructed `AltTocEntry(textId = null, …)`
at four call sites, letting the repo's `getOrCreateTocText` fall back
to auto-increment.

Now each site passes `textId = bindings.upsertTocText(text)`.

### Adjacent — Havrouta task heap (`37e1694`)

With the four lookup kinds now fully tracked, the v1 buildstate
balloons from 1.1 GB to 1.8 GB, and the allocator's in-memory
ConcurrentHashMap copies tip `:generateHavroutaLinks` over its
default `-Xmx4g`. Bumped to `-Xmx10g` to match the other generate
tasks.

## v1 buildstate one-shot migration

`seforim.db.buildstate.v1` had `id_lookup` entries for
`source`, `category`, `connection_type`, and `toc_text` but NOT for
`author`, `topic`, `pub_place`, `pub_date`. A one-shot SQL migration
seeded those four kinds from `seforim.db.v1.{author,topic,pub_place,
pub_date}.{id,name|date}` and updated the corresponding `id_counters`
rows to `MAX(id) + 1`. Same shape of migration was applied to
`toc_text` (which was tracked at the wrong ids — see Hole 2).

The migration is a four-table `INSERT INTO id_lookup` plus four
`UPDATE id_counters`. Not committed as a tool — it's a one-time
backfill for the production v1 release that predates the importer
fixes.

## Producer output (real diff)

After all the fixes, `./gradlew :generator-common:producePatchAndVerify`
produced its first real patch on production data:

  - Inputs : `seforim.db.v1` (7.2 GB) → `seforim.db` (7.3 GB)
  - Output : `patch-v1-v2.db` = **339 MiB** (4.6 % of full bundle)
  - Manifest : 477 B JSON with sha256 + content hashes
  - Upsert tables : 2 author, 87985 tocText, 6 category, 17
    category_closure, 170 book, 9 book_author, 7 book_pub_date,
    1198390 tocEntry, 623482 line, 5868629 line_toc, 781873 link,
    9 book_has_links, 1 alt_toc_structure, 34261 alt_toc_entry,
    498372 line_alt_toc
  - Delete tables : 1 tocText, 4 category, 17 category_closure,
    3 book, 3 book_author, 3 book_pub_date, 4 book_acronym,
    1196531 tocEntry, 13163 line, 13163 line_toc, 44628 link,
    3 book_has_links, 63 line_alt_toc, 1 default_commentator
  - FK violation count delta : 0 (306481 pre-existing, tolerated)
  - **Apply succeeded without exceptions.**

## Remaining gap — strict-hash divergence

The verify-step's hash comparison still differs:

  - `applied=1a144557005b6274558d17a2838fa8be376d5d048948bcbe42d32f8015f20b47`
  - `expected=3a6d7dc00398294ae1987c7e8abc3791f98667933daecb7a01a8b5c88a00c434`

The patch applies cleanly (no FK violations introduced) but the
resulting logical content hash doesn't match `seforim.db` (v2).
Investigation pending. Most likely cause: a table whose content
diverges due to the v1's auto-increment-vs-allocator drift cascading
through FK references that the patch updates but in a way that
introduces a not-quite-identical-to-v2 ordering or NULL state.

The 339 MiB patch IS usable as an approximation but **must not be
published** until the strict invariant passes — that's why
`producePatchAndVerify` keeps the task FAILED on hash mismatch.

## Next steps

  1. Identify which table(s) drive the hash mismatch.
     Diagnostic: run `LogicalContentHasher` per-table on
     target-after-apply vs `seforim.db` and bisect.
  2. Fix the producer/applier or the FK-CASCADE side effect.
  3. Re-run `producePatchAndVerify` → expect `✅ Patch apply verified`.
  4. Ship v1 release notes + retire the `.v1` backup artifacts.

The four shipped fixes are real production bug-fixes regardless of
the remaining hash work — they each closed an id-stability hole
that would have re-emerged on every release.
