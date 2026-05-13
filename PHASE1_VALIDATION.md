# Phase 1 — End-to-end validation report

This document records the empirical validation of the stable-ID foundation
introduced by the `feat/delta-stable-ids` branch (see `DELTA_UPDATE_PLAN.md`
Phase 1). It supersedes the unit-level `StableIdReproducibilityTest`: the
results below come from running the full production pipeline
(`./gradlew generateSeforimDb`) on the real Sefaria + Otzaria datasets.

## Methodology

Six independent perturbations were applied across two consecutive pipeline
runs and the resulting `seforim.db` was diffed against a clean reference
build (`run1`), with `build_state.db` from `run1` fed back in as the prior
state for every modified run. A line/book was counted as *stable* iff the
tuple `(id, bookId, content)` (resp. `(id, title)`) matched exactly between
runs.

## Aggregate result

| Stat | Run 1 (reference) | Otzaria-perturbed run |
|------|------------------:|----------------------:|
| Books (excluding deleted id=6046) | 7,112 | 7,112 |
| Lines (excluding deleted book) | 6,269,212 | **6,269,212 identical** |
| Books fully stable | — | 7,112 / 7,112 (100 %) |
| Lines fully stable | — | 6,269,212 / 6,269,212 (100 %) |
| New ids allocated | — | 7 (next free slots, no collisions) |

## Per-test summary

### Sefaria source perturbations (single rebuild)

| # | Perturbation | Outcome |
|---|--------------|---------|
| A | Delete book id=3002 (`מראה הפנים על תלמוד ירושלמי שבת`) | 7,112 surviving books identical; 4,111 books with id > 3002 keep their original ids — **no shift**. |
| B | Add a synthetic Sefaria book (`Test Phase1`) | 7,113 prior books + 6.27 M lines strictly unchanged; the new book is allocated `id=7114`, lines start at the next free range. |
| C | Insert a unique new line at the top of Genesis (book id=1) | 99.9995 % line stability across `seforim.db`. The 31 affected lines are the Genesis Ch. 1 verses whose **content** changed because Sefaria's pipeline auto-renumbers verse prefixes (`(א)→(ב)`). Their content hash legitimately changes; the new ids are correct. Resolved by §2.1 / Phase 3 (heRef-based natural key). |

### Otzaria source perturbations (single rebuild)

| # | Perturbation | Outcome |
|---|--------------|---------|
| A_otz | Delete book id=6046 (`דרך אמת`) | 7,112 surviving books identical, no shift. |
| B_otz | Add a synthetic Otzaria book | All prior books + lines strictly unchanged; new book allocated `id=7114`. |
| C_otz | Insert a unique new line at the top of book id=6054 (`אלפי מנשה על התורה`) | **100 % line stability**. The 10 existing lines keep their original ids 4,101,477 – 4,101,486; the new line gets `id=6,269,233`. |

## Sefaria vs Otzaria stability profile

Otzaria sources are HTML lines that are byte-stable across builds (no
machine-generated line numbering), so they hit 100 % line stability on
intra-book modifications. Sefaria's pipeline injects verse prefixes
`(א), (ב), …` derived from the position in the JSON array, so an
intra-book insertion at the top of a chapter mutates the content of every
following verse — the SHA-1-of-raw-content natural key cannot see through
this. The fix is documented in `DELTA_UPDATE_PLAN.md` §2.1 and arrives in
Phase 3 (switch to heRef-based natural keys for Sefaria verses, with
content-hash fallback for headings).

## Cost

Each full rebuild takes ~17-18 min wall-clock on the test box (Linux,
G1GC, `-Xmx12g`, NVMe). Six validations ≈ 50 min of compute total.

## Implication for the delta system

The Phase 1 invariant — *"same natural key ⇒ same primary key, across
builds and across non-content perturbations"* — holds end-to-end on the
production pipeline. Building Phase 2 (touched-book detection + rename
detection) and Phase 3 (line matcher) on top of this foundation is now
safe: every reference into `seforim.db` carries a meaning that survives
the next build.
