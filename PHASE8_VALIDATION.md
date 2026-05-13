# Phase 8 — Snapshot-time orphan GC for build_state.db

## Goal

`build_state.db` accumulates `id_line`, `id_toc_entry`, `id_link`,
`id_alt_toc_structure`, `id_alt_toc_entry`, and `book_source_hashes`
entries over time. When a book is removed from the corpus (or rebuilt
under a different natural-key scheme — e.g. the heRef bootstrap in
Phase 3), the *old* entries linger forever, ballooning the
side-channel DB and slowing every subsequent build's load step.

Phase 8 adds an orphan GC at snapshot time: only entries reachable
from the live `books` map (and, transitively, from live `lines` /
`altTocStructures`) survive into the written snapshot.

## Implementation

`InMemoryIdAllocator.snapshotTo()`
(`generator/common/src/jvmMain/.../ids/InMemoryIdAllocator.kt`):

  1. Build `liveBookIds = books.values.toHashSet()`.
  2. Prune `lines`, `tocEntries`, `altTocStructures` whose
     `bookId !in liveBookIds`.
  3. Build `liveStructureIds = altTocStructures.values.toHashSet()`
     and prune `altTocEntries` accordingly.
  4. Build `liveLineIds = lines.values.toHashSet()` (after step 2)
     and prune `links` whose `srcLineId` or `tgtLineId` is missing.
  5. Build `liveBookKeys = books.keys.toHashSet()` and prune
     `mergedSourceHashes`.

Counters are **not** rewound — fresh ids continue from the previous
high-water mark to preserve the "never reuse an id" invariant.

## Validation

Unit suite `BuildStateGcTest` exercises four scenarios end-to-end
(load → mutate → snapshot → reload → assert):

| Scenario                                                | Assertion                                                          |
|---------------------------------------------------------|--------------------------------------------------------------------|
| Two books, both preserved across builds                 | All 5 line entries survive; both books still present after rebuild |
| Synthetic orphan line under non-existent `bookId = 999` | `lines.size` drops from 4 to 3; orphan is gone                     |
| Synthetic orphan link pointing at non-existent lineIds  | `links.size` drops from 2 to 1; good link survives                 |
| Recording a source hash for a "Ghost" book              | `sourceHashes.size` is 1 after reload; Ghost is dropped            |

Test command:

```
./gradlew :generator:common:jvmTest --tests 'BuildStateGcTest'
```

All four tests green. The wider `:generator:common:jvmTest` suite
(76 tests including `RenameAliasFlowTest` and
`InMemoryIdAllocatorTest`) remains green after the GC pass — two
pre-existing tests that relied on orphan structures
(`RenameAliasFlowTest.source hashes round-trip through snapshot reload`
and `InMemoryIdAllocatorTest.ids survive a snapshot+reload roundtrip`)
were updated to allocate their parent ids before recording dependent
state, matching the new invariant.

## Status

Shipped on `feat/delta-stable-ids` at commit `8fca38f`. The next time
the producer runs a full Sefaria + Otzaria rebuild, the stale
heRef-bootstrap line entries in `build_state.db` will be pruned in a
single snapshot pass — no separate cron task needed on the build box,
contrary to what `SHIPPING_STATUS.md` previously indicated.
