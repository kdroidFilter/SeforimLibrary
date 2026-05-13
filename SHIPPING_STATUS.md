# Delta-update system — shipping status

A concise, single-page summary of the delta-update system across the
SeforimLibrary + Zayit (SeforimApp) repositories. Use this as the
"one-pager" when triaging an incident, kicking off a release, or
explaining the design to a reviewer.

## What it does

  - **Producer (CI)**: turns each new `seforim.db` build into a small
    `patch.db` (typically tens of MB on a 7 GB DB), plus JSON manifests.
  - **Server**: serves `release_meta.json` + per-delta manifests + patch
    files over plain HTTP/static hosting.
  - **Client (SeforimApp)**: at user request (Settings ▸ "Check for
    updates"), downloads + applies the patch onto the live DB with
    backup, recovery, FK + content-hash checks, then updates Lucene +
    catalog.pb in lockstep.

## Acceptance criteria — all met

| Criterion | How it's satisfied |
|-----------|--------------------|
| Same input → same primary keys across builds | `IdAllocator` + `build_state.db` (Phase 1) |
| Survives Sefaria verse-prefix renumbering | heRef-based line natural key (Phase 3) |
| Server can emit minimal delta | `PatchDbProducer` + `LogicalContentHasher` (Phase 4) |
| **Strict `prev + patch == new`** invariant | Schema-introspecting all 25 tables (Phase 4.5) |
| Atomic apply with crash recovery | Single SQLite tx + file backup + marker (Phase 5) |
| Lucene index stays in step | `LuceneUpdater` sinks + `TextIndexWriter.deleteLineById` |
| Catalog stays in step | `CatalogUpdater` atomic write |
| Client retries cleanly after crash | `DbDeltaRecoveryBootstrap.runOnce()` at `main()` start |
| Operator can ship a release with one command | `./gradlew publishRelease …` (RELEASE.md) |
| User can trigger a check | Settings ▸ General ▸ Database delta updates |

## Module map

```
SeforimLibrary/
├── generator/common/                ← shared code (server + client)
│   ├── buildstate/                  ← Phase 1: BuildStateReader/Writer
│   ├── ids/                         ← Phase 1: IdAllocator + Bindings
│   ├── changes/                     ← Phase 2 & 3: Source hashes, Touched/Rename detector, LineNormalizer, LineMatcher
│   └── patch/                       ← Phase 4: PatchDbProducer, PatchApplier, LogicalContentHasher,
│                                      ReleaseManifestWriter, PatchPipelineCli
│
├── delta-updater/                   ← Phase 5: client lib (no app deps)
│   └── deltaupdater/
│       ├── DeltaApplierClient       backup + FK + hash check
│       ├── DeltaDownloader          resumable HTTP, sha256 verified
│       ├── LuceneUpdater            enumerates patch.delete_line + upsert_line
│       ├── CatalogUpdater           atomic blob write
│       ├── UpdateOrchestrator       chain of deltas
│       ├── DeltaUpdaterClient       facade
│       └── Manifest / UpdatePath    wire schemas + chooseUpdatePath
│
└── generator/sefariasqlite/, otzariasqlite/, searchindex/, …
    └── all wired to the IdAllocator (Phase 1)

Zayit/
└── SeforimApp/
    ├── main.kt                      ← DbDeltaRecoveryBootstrap.runOnce() at boot
    ├── framework/update/
    │   ├── DbDeltaUpdateService     ← facade injected via Metro
    │   └── DbDeltaRecoveryBootstrap ← boot-time rollback
    ├── framework/di/AppGraph.kt     ← dbDeltaUpdateService binding
    └── features/settings/
        ├── dbupdate/                ← Phase 5.3 ViewModel + State + Events
        └── ui/GeneralSettingsScreen ← embeds DbDeltaUpdateSection()
```

## Stats

| | Count |
|---|---:|
| Phases implemented | 9 (1, 2, 3, 4, 4.5, 5, 6, 7, 8) |
| Unit tests across all modules | ~115 |
| End-to-end pipeline runs | 7 (against real 7 GB Sefaria + Otzaria) |
| Validation reports (`PHASE*_VALIDATION.md`) | 5 |
| Adversarial-audit hardening fixes | 16 |
| Operator runbook (`RELEASE.md`) | 1 |
| Lines of new production code | ~4,000 |
| Branches PR-ready | 2 |

## Branches

  - **SeforimLibrary** : `feat/delta-stable-ids`
    https://github.com/kdroidFilter/SeforimLibrary/tree/feat/delta-stable-ids
  - **Zayit** : `feat/delta-update-client`
    https://github.com/kdroidFilter/Zayit/tree/feat/delta-update-client

## Adversarial-audit hardening (Phase 7)

Sixteen bugs caught and fixed through deliberate adversarial code review.
Each one is something that could have surfaced in production but wasn't
exercised by the happy-path tests.

| Layer | Bug class | Commit |
|-------|-----------|--------|
| Producer | Mismatched build_state.db lineage → broken patch slips through verify | `4cf7347` |
| Producer | `release_meta.json` half-written → clients see partial JSON | `3affa0f` |
| Producer | `publishRelease` without `seforim.db.buildstate` → giant patch with every id renumbered | `e0f5774` |
| Producer | `producePatchAndVerify` errors instead of skipping on first release | `00f5e25` |
| Wire | CDN returns 200 to a Range request → file corruption | `aaff468` |
| Wire | Hung CDN → client blocks forever (default JVM HTTP timeouts) | `29fc24b` |
| Wire | Server has no releases yet (404) → misleading UI error | `6d31814` |
| Applier | ENOSPC mid-backup → truncated backup → boot recovery corrupts live DB | `765db14` |
| Applier | In-process restore leaves marker → misleading "recovered" log on next boot | `9d5b813` |
| Applier | Concurrent apply / un-recovered crash trampling backup | `f97e41e` |
| Applier | Patch with newer `schema_version` silently mis-applied | `0412cd7` |
| Applier | Manifest's `fromSchemaVersion` never enforced → mixed-schema DB possible | `3aad596` |
| Applier | JDBC connection leak on pre-apply content hash check | `59fc290` |
| Orchestrator | SQLite-vs-Lucene divergence on post-commit failure → only recovered on next boot | `9d62414` |
| Orchestrator | Lucene IndexWriter never committed/closed → updates silently dropped | `c5c2d9c` + `67403882` |
| Orchestrator | Regression-coverage gap: happy-path close not asserted | `c694081` + `ef15f04b` |

All sixteen are backed by tests: localhost HTTP servers for wire bugs,
real SeforimDb fixtures for applier bugs, real producer output for the
manifest / lineage bugs.

## Open items (not blocking shipping)

| Pri | Item | Why deferred |
|-----|------|--------------|
| Med | Periodic background check (default daily) | Policy decision: opt-in vs default. Trivial to add: launch a coroutine from main() polling once per N hours. |
| Med | Phase 2.5 "fast-path" unchanged book | Big perf win on producer side (skip re-parse + copy lines from prev DB) — but build still completes in ~25 min today, well within CI budgets. |
| Low | LineMatcher integration into PatchDbProducer | Patience-Diff currently only used at acceptance-test level. Integrating it into the producer's per-touched-book diff would shrink patches further when content changes mid-book without an heRef shift. Requires extending the allocator with a line-alias mechanism (not just book aliases), which is invasive. |

## Quick triage

  - **Apply hangs at `BEGIN IMMEDIATE`** → some process has the live DB
    open. Find it (`lsof seforim.db`) and close it.
  - **"Patch introduced N new FK violations"** → the patch is bad, do
    not publish. Re-check the generator's invariants on `seforim.db`.
  - **Search shows stale results after an apply** → `luceneIndexDir`
    isn't being passed to `DbDeltaUpdateService` (default sinks are
    no-ops). Wire the directory through DI.
  - **Client picks `FullBundle` instead of `Chain`** → either the local
    version is older than `retentionWindow` or the chain bytes exceed
    70 % of the full bundle. Both are tunable on the server side.
