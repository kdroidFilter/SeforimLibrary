# Phase 3 — heRef-based line natural key + LineMatcher

Implementation status and validation plan.

## Implemented (code shipped on `feat/delta-stable-ids`)

| Component | Notes |
|-----------|-------|
| `IdAllocatorBindings.lineNaturalKeyHash(content, heRef)` | When `heRef != null`: `sha1("REF:$heRef")`. Otherwise: `sha1("CT:$content")`. The `REF:` / `CT:` discriminator prevents accidental collisions between the two namespaces. |
| `SefariaDirectImporter` | Routes every line's natural key through `lineNaturalKeyHash(content, refEntry?.heRef)`. Lines with a Sefaria citation are keyed on the citation; headings / structural lines fall back to content-hash. |
| `LineNormalizer` | jsoup HTML strip → diacritics (nikud + teamim) → maqaf / gershayim → sofit → whitespace (incl. NBSP / ZWSP). Mirrors `normalizeForIndexDefault` from `:search`. |
| `LineMatcher` | Patience Diff (unique-hash anchors + LIS) with a greedy fallback for anchorless windows, then a MODIFY post-pass on adjacent DELETE/INSERT pairs whose token similarity ≥ 0.6. Pure: no DB, no allocator. Used by Phase 4's PatchDbProducer when scoping a touched book's per-line delta. |

Unit tests in `:generator-common`: 15 new (LineNaturalKeyHash + LineNormalizer + LineMatcher), all green.

## Validation matrix (e2e on the production pipeline)

| Run | Sources | Expected outcome |
|-----|---------|------------------|
| **Run 1** (pre-Phase 3) | Clean | Baseline. Line ids based on `sha1(content)`. |
| **Run 2** (Phase 3 wired, no source change) | Same as Run 1 | All Sefaria line ids reallocate ONCE because the natural-key formula changes (`sha1("REF:$heRef")` instead of `sha1(content)`). Otzaria ids unchanged. **Validated** — `max(line.id)` jumps from 6.5M to 10.2M; counts unchanged. |
| **Run 3** (Phase 3 baseline, no source change) | Same as Run 2 | Strictly identical to Run 2: 7320 books, 6,521,423 lines, max line id 10,205,191. **Validated** — bit-identical via direct SQL comparison. |
| **Run 4 (Test C)** | Run 3 + 1 inserted line at the top of Genesis chapter 1 | Genesis verses with heRef "Genesis 1:1", "1:2"… keep their Run 3 line ids despite the `(א)→(ב)` prefix shift. The inserted line gets a fresh id. Headings (`<h1>`, `<h2>`) keep their content-hash-keyed ids. **Pending** — build running. |

## Why Run 4 vs Phase 1 Test C is the decisive comparison

Phase 1 Test C produced **31 reallocated line ids** in Genesis chapter 1 because the verse-prefix renumbering changed the rendered content and the raw `sha1(content)` keyed natural key followed suit. With Phase 3's heRef-keyed natural key, those same 31 verses should keep their line ids — the citation `"Genesis 1:1"` is unchanged regardless of what prefix Sefaria renders.

The Run 4 validation report will live in `PHASE3_VALIDATION.md`.
