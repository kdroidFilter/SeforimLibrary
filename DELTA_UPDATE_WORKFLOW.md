# Delta-update workflow

End-to-end documentation of how a new `seforim.db` release reaches a
running Zayit user via a small incremental patch instead of a 7+ GB
full re-download.

---

## 0. Big picture — one diagram

```mermaid
flowchart LR
    subgraph CI ["🛠️  CI (release engineer)"]
        SRC1[(Sefaria<br/>export)]
        SRC2[(Otzaria<br/>library)]
        BS[(build_state.db<br/>v1)]
        GEN[generateSeforimDb]
        DB2[(seforim.db<br/>v2 — 7.3 GB)]
        BS2[(build_state.db<br/>v2)]
        PROD[producePatchAndVerify]
        PATCH[(patch-v1-v2.db<br/>339 MB)]
        MANI[manifest.json]
        META[release_meta.json]
        SRC1 --> GEN
        SRC2 --> GEN
        BS --> GEN
        GEN --> DB2
        GEN --> BS2
        DB1V1[(seforim.db.v1<br/>previous release)] --> PROD
        DB2 --> PROD
        PROD --> PATCH
        PROD --> MANI
        PROD --> META
    end

    subgraph CDN ["🌐  Static host (GitHub Pages / S3 / …)"]
        SMETA[release_meta.json]
        SMAN[patch-v1-v2.db.manifest.json]
        SPATCH[patch-v1-v2.db]
        META -.deploy.-> SMETA
        MANI -.deploy.-> SMAN
        PATCH -.deploy.-> SPATCH
    end

    subgraph Client ["💻  Zayit user device"]
        UI[Settings ▸<br/>Check & apply]
        CLIENT[DeltaUpdaterClient]
        ORCH[UpdateOrchestrator]
        APPLIER[PatchApplier]
        LUCENE[LuceneUpdater]
        CATALOG[CatalogUpdater]
        LIVE[(seforim.db<br/>+ Lucene index<br/>+ catalog.pb)]
        UI --> CLIENT
        CLIENT --> ORCH
        ORCH --> APPLIER
        ORCH --> LUCENE
        ORCH --> CATALOG
        APPLIER --> LIVE
        LUCENE --> LIVE
        CATALOG --> LIVE
    end

    SMETA -.HTTP poll.-> CLIENT
    SMAN -.HTTP get.-> CLIENT
    SPATCH -.HTTP get.-> CLIENT
```

The contract :

  - `seforim.db` v1 → v2 always produces the SAME `id` for the same
    row (book, line, tocEntry, link, author, topic, …). This is the
    **id-stability invariant** and is what makes a small diff possible.
  - `patch-v1-v2.db` is itself a small SQLite database that the client
    `ATTACH`es and applies. No native code, no custom protocol.
  - **Strict invariant** : `LogicalHash(apply(v1, patch)) == LogicalHash(v2)`.
    The CI verifies this before publishing; ship aborts on mismatch.

---

## 1. Producer side (CI)

### 1.1 The IdAllocator — why ids stay stable

```mermaid
flowchart TD
    subgraph build_state ["build_state.db (1.8 GB)"]
        IL[id_lookup<br/>kind, natural_key → id]
        IB[id_book<br/>sourceName, canonicalHeTitle → id]
        ILN[id_line<br/>bookId, contentHash, occurrence → id]
        ITE[id_toc_entry<br/>bookId, ancestorPath → id]
        ILK[id_link<br/>srcLineId, tgtLineId, type → id]
        IC[id_counters<br/>per-table next free id]
    end

    NEW["New build step<br/>e.g. importer encounters &quot;דוד לוריא&quot;"] --> Q{Lookup<br/>in id_lookup<br/>kind='author'}
    Q -->|hit| RID[return existing id 21]
    Q -->|miss| ALLOC[id_counters.next_id++<br/>store new mapping]
    ALLOC --> RID2[return new id]
    RID --> SNAP[snapshotTo build_state.db]
    RID2 --> SNAP
```

The allocator's natural keys are :

| Table       | Natural key                                     | Notes |
|-------------|-------------------------------------------------|-------|
| `source`    | `name`                                          | "Sefaria", "Otzaria" |
| `author`    | `name`                                          | |
| `topic`     | `name`                                          | |
| `pub_place` | `name`                                          | |
| `pub_date`  | `date` text                                     | |
| `category`  | canonical hebrew path                           | "תנ״ך/תורה/בראשית" |
| `tocText`   | display text                                    | |
| `connection_type` | `name`                                    | "commentary", "targum" |
| `book`      | `(sourceName, canonicalHeTitle)`                | survives renames via book_aliases |
| `line`      | `(bookId, "REF:"+heRef)` for Sefaria, `(bookId, contentHash, occurrenceIdx)` for Otzaria | heRef is THE killer feature — survives Sefaria's prefix renumbering |
| `tocEntry`  | `(bookId, ancestorPath@lineIndex)`              | path is a `/`-joined sequence of tocText ids |
| `link`      | `(srcLineId, tgtLineId, connectionTypeId)`      | |

> If two builds share the same `build_state.db` seed and the corpus
> contents are unchanged, all ids match. If a row's content changes,
> only that row gets a new id; everything around it stays put.

### 1.2 The full producer pipeline

```mermaid
sequenceDiagram
    autonumber
    participant Op as Release Engineer
    participant Gradle as ./gradlew publishRelease
    participant Sef as :sefariasqlite:<br/>generateSefariaSqlite
    participant Otz as :otzariasqlite:<br/>appendOtzaria
    participant Cat as :catalog:<br/>buildCatalog
    participant Lucene as :searchindex:<br/>buildLuceneIndex
    participant Stamp as :generator-common:<br/>stampSchemaVersion
    participant Patch as :generator-common:<br/>producePatchAndVerify

    Op->>Gradle: -PprevReleaseDb=…/v1.db<br/>-PdbVersion=2 -PfromVersion=1 -PtoVersion=2
    Note over Gradle: doFirst: refuse if<br/>seforim.db.buildstate is missing
    Gradle->>Sef: load v1 build_state<br/>(seed allocator)
    Sef->>Sef: import + INSERT WITH ALLOCATOR-ASSIGNED IDS
    Sef-->>Gradle: seforim.db (Sefaria tables filled)
    Gradle->>Otz: append Otzaria books<br/>(same allocator continues)
    Otz-->>Gradle: seforim.db (Otzaria appended)
    Gradle->>Cat: build catalog.pb<br/>(snapshot of category/book tree)
    Cat-->>Gradle: catalog.pb (~650 KB)
    Gradle->>Lucene: build Lucene segments<br/>(text index over line.content)
    Lucene-->>Gradle: seforim.db.lucene/ + .lookup.lucene/
    Gradle->>Stamp: INSERT OR REPLACE INTO schema_meta<br/>VALUES ('db_version', '2'),<br/>('db_schema_version', '1')
    Note over Stamp: This row is what lets the<br/>client choose the right delta
    Stamp-->>Gradle: seforim.db (stamped)
    Gradle->>Patch: produce(prev=v1, new=v2)
    Note over Patch: 1. assertNoSecondaryUniqueCollisions<br/>(refuses if id lineages diverged)<br/>2. scan upsert_* / delete_* per table<br/>3. embed catalog.pb in patch.blobs<br/>4. write manifest.json<br/>5. verify-apply: copy v1, apply, hash<br/>   == hash(v2) — refuse to ship on mismatch<br/>6. upsert release_meta.json
    Patch-->>Op: patch-v1-v2.db (339 MB)<br/>manifest.json<br/>release_meta.json
```

### 1.3 What's inside `patch.db`

```
patch.db
├── patch_meta              schema_version, from_version, to_version, …
├── migrations              optional DDL run before upserts
├── blobs                   catalog.pb (and any future binary asset)
└── for each tracked table T (25 of them):
    ├── upsert_T            same shape as T's columns + PK
    │                       rows in new that are absent or differ in prev
    └── delete_T            T's PK columns only
                            rows in prev that are absent in new
```

Per-table upsert/delete strategy (in `PatchDbProducer.scanUpserts`):

```sql
INSERT INTO upsert_T (...)
SELECT new.<cols>
FROM new.T AS new
LEFT JOIN prev.T AS prev ON <pk match>
WHERE prev.<firstPk> IS NULL
   OR new.col1 IS NOT prev.col1
   OR new.col2 IS NOT prev.col2
   …
```

The `IS NOT` (not `!=`/`<>`) is critical: it treats `NULL` as a
distinct value from `''` and from other values, so toggling a column
between null and empty triggers an upsert.

The verify-apply step at the end is the strongest guarantee:

```
copy(v1) → apply(patch) → LogicalContentHasher.compute() == hash(v2)
```

If this fails, `producePatchAndVerify` exits non-zero and the
release is NOT published.

---

## 2. CDN / static-host layout

```
https://<your-host>/
├── release_meta.json                     ← clients poll this (atomic-written)
├── patch-v1-v2.db                        ← 339 MB binary (sha256 in manifest)
├── patch-v1-v2.db.manifest.json          ← per-delta manifest (atomic-written)
├── patch-v2-v3.db
├── patch-v2-v3.db.manifest.json
└── seforim_bundle.tar.zst                ← full-bundle fallback for older clients
```

`release_meta.json` shape :

```json
{
  "latestVersion": 2,
  "retentionWindow": 30,
  "fullBundle": {
    "version": 2,
    "url": "https://…/seforim_bundle.tar.zst",
    "sha256": "…",
    "size": 8000000000
  },
  "deltas": [
    {"fromVersion": 1, "toVersion": 2, "manifestUrl": "…/patch-v1-v2.db.manifest.json", "totalSize": 354705408}
  ]
}
```

Both this file and per-delta `manifest.json` are written via
`Files.move(ATOMIC_MOVE)` from a sibling `.tmp` so a client polling
mid-write never observes a half-written JSON.

---

## 3. Client side (Zayit)

### 3.1 The big sequence — happy path

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant App as Zayit (main.kt)
    participant Boot as DbDeltaRecoveryBootstrap
    participant VM as DbDeltaUpdateViewModel
    participant Service as DbDeltaUpdateService
    participant Client as DeltaUpdaterClient
    participant Orch as UpdateOrchestrator
    participant Apply as DeltaApplierClient
    participant LU as LuceneUpdater
    participant CU as CatalogUpdater
    participant FS as <br/>filesystem
    participant CDN as remote HTTP

    Note over App,Boot: main()
    App->>Boot: runOnce()
    Boot->>FS: stat seforim.db.applying
    Boot-->>App: no marker → continue boot
    App->>App: open SQLDelight repo, init UI…

    User->>VM: click "Check & apply"
    VM->>Service: checkAndApply(progress)
    Service->>Client: checkForUpdate()
    Client->>CDN: GET /release_meta.json
    CDN-->>Client: {latestVersion: 2, deltas: [{1→2}]}
    Client->>Client: chooseUpdatePath(local=1, meta)<br/>=> Chain([1→2])
    Client-->>Service: UpdatePath.Chain
    Service->>Client: applyChain([1→2])
    Client->>Orch: applyChain(…)
    Orch->>Apply: recoverIfNeeded() → false
    Orch->>CDN: GET manifest.json
    Orch->>CDN: GET patch-v1-v2.db (339 MB, resumable)
    Orch->>Apply: apply(seforimDb, patchDb, manifest)
    Apply->>FS: assertEnoughFreeSpace()
    Apply->>FS: backup = copy(seforim.db)<br/>marker = .applying file
    Apply->>FS: ATTACH 'patch' AS patch
    Apply->>FS: PRAGMA defer_foreign_keys = ON<br/>BEGIN<br/>upserts (FK order)<br/>deletes (reverse FK order)<br/>hash check<br/>COMMIT
    Apply-->>Orch: ok
    Orch->>LU: applyTo(patchDb, sinks, liveDbPath=seforim.db)
    LU->>FS: iterate delete_line<br/>iterate upsert_line<br/>iterate upsert_book WHERE NOT IN upsert_line<br/>(re-index those books' lines)
    LU->>FS: IndexWriter.commit() + close()
    Orch->>CU: update(patchDb, catalogPb, "catalog.pb")
    CU->>FS: read blob → write to .tmp → Files.move(ATOMIC_MOVE)
    Orch->>Apply: finalizeApply() → rm marker + backup
    Orch-->>Service: done
    Service-->>VM: Outcome.Applied(1)
    VM-->>User: ✅ "Applied 1 delta"
```

### 3.2 The atomic-apply guarantees

```mermaid
flowchart TD
    START[User clicks<br/>Check & apply]

    PRE[Pre-flight checks<br/>seforim.db<br/>matches manifest.fromContentHash<br/>schema_meta.db_schema_version<br/>matches manifest.fromSchemaVersion<br/>free space ≥ size_db + size_patch + 64 MiB<br/>no .applying marker present]

    BACKUP[Copy seforim.db → seforim.db.backup<br/>Write seforim.db.applying marker]

    SQL[Open seforim.db connection<br/>autoCommit = false<br/>ATTACH patch<br/>assertPatchSchemaCompatible<br/>defer_foreign_keys = ON<br/>runUpserts in FK order<br/>runDeletes in reverse FK order<br/>FK violation count must not grow<br/>logical hash post-apply matches<br/>manifest.toContentHash<br/>COMMIT]

    LUCENE[luceneSinks .use<br/>iterate delete_line / upsert_line<br/>upsert_book WHERE NOT IN upsert_line<br/>writer.commit + close]

    CATALOG[Atomic write of catalog.pb<br/>.tmp + Files.move ATOMIC_MOVE]

    FINAL[Delete marker + backup<br/>finalizeApply]

    DONE[✅ Done]

    ROLLBACK1[Files.copy backup → seforim.db<br/>delete marker + backup<br/>throw to caller]

    ROLLBACK2[In-process<br/>recoverIfNeeded<br/>rolls SQLite back<br/>+ deletes marker + backup<br/>throw to caller]

    BOOTROLLBACK[On next launch<br/>DbDeltaRecoveryBootstrap<br/>copies backup → seforim.db<br/>deletes marker + backup]

    START --> PRE
    PRE -->|any check fails| ROLLBACK1
    PRE -->|ok| BACKUP
    BACKUP --> SQL
    SQL -->|throws| ROLLBACK1
    SQL -->|ok| LUCENE
    LUCENE -->|throws| ROLLBACK2
    LUCENE -->|ok| CATALOG
    CATALOG -->|throws| ROLLBACK2
    CATALOG -->|ok| FINAL
    FINAL --> DONE

    SQL -.JVM dies here.-> BOOTROLLBACK
    LUCENE -.JVM dies here.-> BOOTROLLBACK
    CATALOG -.JVM dies here.-> BOOTROLLBACK
```

Three failure boundaries, all returning the user to a clean
pre-apply state:

1. **Pre-flight** failure → nothing touched, just an error message.
2. **In-process** failure (SQLite/Lucene/catalog throws) → rollback
   in the same process via `recoverIfNeeded` + cleared marker, so
   the next attempt starts from a clean state without needing to
   restart the app.
3. **Hard crash** (kill -9 / power loss) → next launch's
   `DbDeltaRecoveryBootstrap.runOnce()` sees the marker + backup
   pair and restores.

### 3.3 The Lucene side

Important : `patch.db` does **NOT** contain Lucene segment files.
The client re-derives Lucene ops from the SQLite line ops:

```mermaid
flowchart LR
    PATCH[patch.db]
    PATCH -->|delete_line.id| DEL["IndexWriter.deleteDocuments<br/>line_id = X"]
    PATCH -->|upsert_line| BUILD[build Document with current<br/>book metadata from live seforim.db]
    PATCH -->|upsert_book NOT IN upsert_line| REIDX[for each book, enumerate its lines<br/>from live seforim.db and re-upsert]
    BUILD --> ADD["IndexWriter.addDocument"]
    REIDX --> ADD
    DEL --> COMMIT[on session.close → commit + close]
    ADD --> COMMIT
```

Why not ship Lucene segments :

| Strategy ✅ Current             | Strategy ❌ ship segments         |
|----------------------------------|------------------------------------|
| patch.db stays small (339 MB)    | + ~3 GB of Lucene binaries         |
| Robust to Lucene format upgrades | Lock client to producer's version  |
| Client picks its own analyzer    | Analyzer baked into segments       |
| Client pays ~10–30 s CPU on apply | Free                              |

The `upsert_book WHERE NOT IN upsert_line` re-index catches the
case where a book's metadata changed (title, categoryId, orderIndex,
isBaseBook) without any of its line content changing — without this
catch, search results would carry stale book titles.

---

## 4. Data on disk after one apply

```
~/.local/share/zayit/databases/
├── seforim.db                  ← post-apply (now at v2)
├── seforim.db.lucene/          ← updated by LuceneUpdater
├── seforim.db.lookup.lucene/
├── catalog.pb                  ← rewritten atomically from patch.blobs
├── lexical.db                  ← untouched (downloaded once)
├── release_info.txt            ← stale; client uses schema_meta.db_version
└── delta-cache/                ← per-delta dirs; cleaned only on success
    └── delta-v1-v2/            ← left if the apply failed mid-way,
        └── patch_global.db.part   so the next attempt can resume
```

If the apply succeeded, `delta-cache/delta-v1-v2/` is removed by
the orchestrator. If it failed, the partial `.part` survives so
the downloader can resume on the next retry from byte N.

---

## 5. The id-stability check

A long invariant chain holds the whole system together:

```mermaid
flowchart LR
    A[Operator runs<br/>generateSeforimDb]
    B[IdAllocator loads<br/>build_state.db<br/>previous release]
    C[Importer consults<br/>allocator before<br/>EVERY row insert]
    D[seforim.db v2 has<br/>same ids as v1 for<br/>unchanged content]
    E[Producer scans v1 vs v2<br/>tiny upsert / delete diff]
    F[Verify-apply hash<br/>matches v2]
    G[Producer publishes<br/>patch + manifest]
    H[Client local seforim.db<br/>matches manifest<br/>fromContentHash]
    I[Apply produces<br/>byte-identical-to-v2 DB]

    A --> B --> C --> D --> E --> F --> G --> H --> I
```

Any break in this chain is caught early :

  - Step C broken (importer inserts with `INSERT OR IGNORE` instead
    of allocator) → producer's secondary-UNIQUE collision pre-check
    refuses to ship.
  - Step F fails → `producePatchAndVerify` exits non-zero, nothing
    gets uploaded.
  - Step H fails → client refuses the apply with a clear error
    naming the hash mismatch.
  - Step I fails → not possible to detect on the client (no
    independent v2 to compare against), but covered by step F.

---

## 6. Operator runbook (one-liner)

```bash
./gradlew publishRelease \
    -PprevReleaseDb=$HOME/releases/v1/seforim.db \
    -PdbVersion=2 \
    -PfromVersion=1 \
    -PtoVersion=2 \
    -PreleaseMeta=$HOME/releases/release_meta.json \
    -PfullBundleUrl=https://github.com/.../v2/seforim_bundle.tar.zst \
    -PfullBundleSha=<sha256 of the full bundle> \
    -PfullBundleSize=<bytes> \
    -PmanifestBaseUrl=https://github.com/.../v2
```

Before running, copy the previous release's `build_state.db` into
`build/seforim.db.buildstate` (the operator footgun guard in
`publishRelease.doFirst` refuses to start otherwise).

Outputs that you upload to the CDN :

  - `build/seforim.db`                  the freshly-built v2 (full bundle)
  - `build/catalog.pb`                  derived metadata
  - `build/seforim.db.lucene/`          full Lucene index (for full-bundle clients)
  - `build/seforim.db.buildstate`       seed for the v3 release
  - `build/patch-v1-v2.db`              incremental patch (this is the small file)
  - `build/patch-v1-v2.db.manifest.json` per-delta manifest
  - `build/release_meta.json`           updated release index (clients poll this)

---

## 7. Tooling cheatsheet

| Task                                              | Purpose |
|---------------------------------------------------|---------|
| `:generator-common:producePatchAndVerify`         | Produce a patch + run the strict verify-apply |
| `:generator-common:stampSchemaVersion`            | Stamp `schema_meta.db_version` (chained to `generateSeforimDb`) |
| `:generator-common:diagnoseHashMismatch`          | When verify fails, identify which table(s) diverged |
| `:generator-common:compareLogicalContent`         | Hash two whole `seforim.db` files per-table and report differences |
| `:SeforimLibrary:publishRelease`                  | Umbrella: generateSeforimDb + producePatchAndVerify + release_meta |

---

## 8. Validation status (as of 2026-05-13)

  - ✅ Strict invariant `prev + patch == new` verified on real 7+ GB data
  - ✅ Live Zayit GUI applied a 339 MiB patch to a v1 DB in 3m 26s
  - ✅ Post-apply DB matches the directly-built v2 byte-for-byte on
    all 26 hashed tables (`compareLogicalContent: 0 tables diverge`)
  - ✅ Catalog.pb embedded in patch.blobs and written atomically
  - ✅ Lucene index re-derived in lock-step (with the
    `upsert_book WHERE NOT IN upsert_line` fix for metadata-only changes)
  - ✅ `schema_meta.db_version` stamped automatically by `generateSeforimDb`
  - ✅ Recovery boot validated by killing the JVM mid-apply
  - ✅ Marker-based concurrency guard prevents racing applies

The system is ready to merge to master and tag the first delta-enabled
release.
