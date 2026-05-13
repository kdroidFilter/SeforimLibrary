# Releasing a new seforim.db with a delta

This is the operator-facing flow for publishing a new release of
`seforim.db` along with the delta patch and JSON manifests that the
SeforimApp client polls.

## Prerequisites

  - The previous release's `seforim.db` and `seforim.db.buildstate` kept
    somewhere accessible (e.g. `~/releases/v124/seforim.db` and
    `~/releases/v124/seforim.db.buildstate`).
  - The Sefaria + Otzaria source archives downloaded under
    `generator/{sefariasqlite,otzariasqlite}/build/{sefaria,otzaria}/` —
    `generateSeforimDb` fetches them automatically when missing.
  - JBR 21 (or the bundled JBR via gradle toolchains).
  - ~12 GB RAM for the importer.

## Single-command release

```bash
# Seed this build's IdAllocator from the previous release's build_state
# so primary keys remain stable across releases.
cp ~/releases/v124/seforim.db.buildstate build/seforim.db.buildstate

./gradlew publishRelease \
    -PprevReleaseDb=$HOME/releases/v124/seforim.db \
    -PfromVersion=124 \
    -PtoVersion=125 \
    -PreleaseMeta=$HOME/releases/release_meta.json \
    -PfullBundleUrl=https://github.com/.../v125/seforim_bundle.tar.zst \
    -PfullBundleSha=<sha256 of the full bundle> \
    -PfullBundleSize=<bytes> \
    -PmanifestBaseUrl=https://github.com/.../v125
```

Outputs (under `build/`):

  - `seforim.db`                              — the freshly-built database
  - `seforim.db.buildstate`                   — IdAllocator snapshot for v126
  - `catalog.pb` / `seforim.db.lucene/`       — app runtime artefacts
  - `patch-v124-v125.db`                      — binary delta (~ tens of MB)
  - `patch-v124-v125.db.manifest.json`        — per-delta manifest
  - `release_meta.json`                       — release index (updated)

Wall-clock cost on a workstation with NVMe storage:

  - `generateSeforimDb`     ~25 min (or ~18 min if archives are cached)
  - `producePatchAndVerify` ~3 min (incl. apply-roundtrip verification)

The `producePatchAndVerify` step runs the **strict `prev + patch == new`**
invariant before publishing: it applies the patch onto a fresh copy of
`prevReleaseDb` and asserts the resulting logical content hash equals
the new `seforim.db`'s hash. A mismatch aborts publishing — never ship
a delta that can't be applied cleanly.

## Step-by-step (if you prefer staged commands)

```bash
# 1. Build the new release.
./gradlew generateSeforimDb

# 2. Snapshot the build outputs.
mkdir -p ~/releases/v125
cp build/seforim.db          ~/releases/v125/
cp build/seforim.db.buildstate ~/releases/v125/
cp build/catalog.pb          ~/releases/v125/
cp -r build/seforim.db.lucene ~/releases/v125/

# 3. Produce the delta against the previous release.
./gradlew :generator-common:producePatchAndVerify \
    -PprevDb=$HOME/releases/v124/seforim.db \
    -PnewDb=$HOME/releases/v125/seforim.db \
    -Pout=$HOME/releases/v125/patch-v124-v125.db \
    -PfromVersion=124 -PtoVersion=125 \
    -PreleaseMeta=$HOME/releases/release_meta.json \
    -PfullBundleUrl=… -PfullBundleSha=… -PfullBundleSize=… \
    -PmanifestBaseUrl=https://.../v125

# 4. Verify both files parse correctly on the client side.
gradle :delta-updater:test --tests "*UpdatePathTest*"

# 5. Upload (full bundle + per-delta manifest + patch + release_meta.json).
#    Typical layout under your static host:
#      https://.../v125/seforim_bundle.tar.zst        ← full bundle
#      https://.../v125/patch-v124-v125.db            ← delta payload
#      https://.../v125/patch-v124-v125.db.manifest.json
#      https://.../release_meta.json                  ← single root index
```

The client (SeforimApp) polls `release_meta.json` periodically, picks an
update path via `chooseUpdatePath(localVersion, meta)`, and either:

  - stays put (`UpToDate`),
  - downloads + applies a chain of `(N → N+1, N+1 → N+2, …)` deltas
    (`Chain`),
  - or falls back to the full bundle (`FullBundle`) when the chain would
    be too expensive (more than 70 % of the full-bundle size by default)
    or the client is outside the retention window.

## Retention

By default `release_meta.json.retentionWindow = 30`: the server can
prune deltas with `fromVersion < latestVersion - 30`. Clients on a
version older than the oldest delta are routed to the full bundle.

## Failure modes

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| `producePatchAndVerify` aborts with "Patch introduced N new FK violations" | Generator wrote an inconsistent row | Inspect `pragma_foreign_key_check` on `seforim.db` and fix the importer |
| Apply hangs at `BEGIN IMMEDIATE` | Another reader / writer holds a lock | Make sure no process has the live DB open |
| Client sees `FullBundle` when you expected `Chain` | Local version too old, or chain too big | Adjust `retentionWindow` / `fallbackRatio` |
| Lucene search returns stale snippets after apply | The Lucene index didn't get the upserts | Verify `luceneIndexDir` is passed to `DbDeltaUpdateService` |
