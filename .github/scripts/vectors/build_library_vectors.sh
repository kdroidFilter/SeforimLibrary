#!/usr/bin/env bash
# Build one library release's semantic vectors on the build machine, and publish them.
#
#   build_library_vectors.sh --tag v<N>-<YYYYMMDDHHMMSS> [--mode auto|base|dry-run]
#                            [--family all|NAME]... [--state DIR] [--work DIR] [--repo OWNER/NAME]
#
# Every family in FAMILY_CONFIGS (pins.env), in its order, or those --family names (a family is
# its config directory's name), is built from the one index and the one warehouse:
#
#   1 index     download the release's search index (split parts, sha256-checked) and expand it,
#               once its provenance names this release, this archive and this release's DB;
#               then that DB, held to the same digest: schema 5 keeps line text there
# then for each family, under <work>/<family>:
#   2 plan      export_semantic_plan v2 over it, with the family's model (its chunking_identity
#               picks the recipe), split against the warehouse: embed.jsonl holds only the texts
#               the warehouse has no vector for (every text, on a runner that has no warehouse
#               yet); the preflight has run warehouse-verify --repair on it
#   3 embed     those texts: fewer than CPU_EMBED_MAX on the CPU, with the sidecar's embed-shard
#               (ONNX Runtime and the fp32 package — the reference, so no parity certificate); more
#               on the GPU, with embed_worker.py — windows of EMBED_WINDOW records, each a v2 shard
#               with its parity certificate
#   4 warehouse warehouse-add --plan: the shards held to the plan, their vectors appended (the
#               first add creates the warehouse)
#   5 assemble  a base, then assemble --verify (the gates), then
#               the plugin's validate_semantic_vectors on the release index: G3 coverage, G4
#               resolution, G6 retrieval with the int8 query model; any exit but 0 stops the build
#   6 files     the segment compressed (and split below GitHub's asset limit), release-files
#   7 publish   (mode base or auto) a sibling release <prefix>-<tag> at the commit of <tag>, made a
#               draft first: data, then gates.json and validation.json, the manifest last;
#               once the draft holds exactly those files at their sizes it is published,
#               --latest=false; then the family's ledger is persisted — only after a publish
#
# Mode auto is mode base, and for a family of DELTA_PREFIXES also a delta from the release it
# published last, when that chain can be extended: the plan is split against that release's
# ledger, the delta is assembled and gated, and validated installed after that release's base and
# at the end of the chain of deltas a device may have applied (up to MAX_CHAIN, kept beside the
# ledger); a delta that fails a gate or a validation is dropped with a warning. The base is
# assembled in the ledger's codec epoch and alias threshold (--keep-epoch), published, and its
# ledger persisted and backed up; then the delta is published, best-effort, as the prerelease
# <prefix>-delta-<tag>. A base in a new epoch is built instead when there is no
# ledger, it is not the last published release's, or the passage package, the codec, the alias
# threshold or the plan's identity changed. Such a family keeps that base's assembly beside its
# ledger, and backs the ledger up in the prerelease vectors-ledger-<tag> for a runner that lost
# its own.
#
# A new rot192-i8i4 epoch takes its rotation from rotation/<family>.bin when ROTATIONS pins its
# SHA-256, else from the one this runner fitted before on this warehouse
# (ledger/rotations/<family>.<warehouse>.bin, also restored with a ledger backup), else
# fit-rotation fits one on the warehouse; a fitted one is kept after a publish and uploaded with
# the ledger backup, to be committed and pinned. A chain's later bases keep the ledger's epoch.
# Every validation prints G6's recall (and adds it to the job summary): a dry run of one family
# measures a new codec before anything is published.
#
# A family that fails stops the build there and the families before it stay published; retry it
# alone with --family NAME. The first family, which applications install, never waits on a later one.
#
# The warehouse (no recipe in its identity) is shared. A new family's first build can embed millions
# of texts; if needed, --family NAME --mode dry-run fills the warehouse beforehand (a dry run keeps them).
#
# LIBRARY_VECTORS_PRERELEASE (true unless "false") publishes each release as a prerelease:
# like the lines-snapshot-sha256-* and pipeline-result-* releases it then stays out of
# update-release-manifest.yml and the database history; a download by tag works the same.
#
# --mode dry-run stops after step 6: it publishes nothing and persists no ledger (the warehouse
# keeps what it embedded, as in a base). The state directory (default
# /home/runner/otzaria-vectors; see bootstrap_runner.sh) holds the binaries, the venv, the
# model cache, the warehouse and the published ledgers, and is locked for the whole build.
# Runs by hand on the build machine as it runs in the workflow. Tests replace the tools with
# OTZARIA_SEMANTIC_CLI, EXPORT_SEMANTIC_PLAN, VALIDATE_SEMANTIC_VECTORS, VECTOR_PYTHON, GH and ZSTD.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=pins.env
. "$HERE/pins.env"
# shellcheck source=../db_asset_names.sh
. "$HERE/../db_asset_names.sh"   # FULL_DB_ASSET_JQ: the release's full DB, which the index is built from

TAG=""; MODE=dry-run; STATE=/home/runner/otzaria-vectors; WORK=""; REPO=${GITHUB_REPOSITORY:-Otzaria/SeforimLibrary}
WANTED=(); ALL=""
while [ $# -gt 0 ]; do
  case "$1" in
    --tag) TAG=$2; shift 2 ;;
    --mode) MODE=$2; shift 2 ;;
    --family) [ $# -ge 2 ] || { echo "::error::--family needs a value: all, or a family's name"; exit 64; }
      if [ "$2" = all ]; then ALL=1; else WANTED+=("$2"); fi; shift 2 ;;
    --state) STATE=$2; shift 2 ;;
    --work) WORK=$2; shift 2 ;;
    --repo) REPO=$2; shift 2 ;;
    *) echo "usage: build_library_vectors.sh --tag vN-YYYYMMDDHHMMSS [--mode auto|base|dry-run] [--family all|NAME]... [--state DIR] [--work DIR] [--repo R]" >&2; exit 64 ;;
  esac
done
echo "$TAG" | grep -Eq '^v[0-9]+-[0-9]{14}$' || { echo "::error::'$TAG' is not a database release tag (v<dbVersion>-<utcTimestamp>)"; exit 64; }
case "$MODE" in auto|base|dry-run) ;; *) echo "::error::--mode is auto, base or dry-run, not '$MODE'"; exit 64 ;; esac
PRERELEASE=${LIBRARY_VECTORS_PRERELEASE:-true}
case "$PRERELEASE" in true|false) ;; *) echo "::error::LIBRARY_VECTORS_PRERELEASE is true or false, not '$PRERELEASE'"; exit 64 ;; esac
# The families built: <name>, <release prefix>, <codec> ("" for the sidecar's) and <alias
# threshold> ("" for none), in FAMILY_CONFIGS's order
NAMES=(); PREFIXES=(); CODECS=(); THRESHOLDS=()
for family in $FAMILY_CONFIGS; do
  IFS=: read -r config prefix codec threshold extra <<< "$family"
  name=${config##*/}
  [ -z "$extra" ] || { echo "::error::FAMILY_CONFIGS entry '$family' has fields past <alias threshold>"; exit 64; }
  if [ "${#WANTED[@]}" -gt 0 ] && [[ " ${WANTED[*]} " != *" $name "* ]]; then continue; fi
  NAMES+=("$name"); PREFIXES+=("$prefix"); CODECS+=("$codec"); THRESHOLDS+=("$threshold")
done
DEFAULT_CODEC=i8-sym-vec    # the sidecar's, when FAMILY_CONFIGS names none
LEDGER_BACKUPS=vectors-ledger
MAX_CHAIN=8                 # the deltas a device applies after its base (the application's findDeltaChain)
delta_family() { [ "$MODE" = auto ] && [[ " ${DELTA_PREFIXES:-} " == *" $1 "* ]]; }
if [ -n "$ALL" ] && [ "${#WANTED[@]}" -gt 0 ]; then
  echo "::error::--family all and --family ${WANTED[*]} together: give all, or the families to build"; exit 64
fi
for name in ${WANTED[@]+"${WANTED[@]}"}; do
  [[ " ${NAMES[*]} " == *" $name "* ]] || { echo "::error::--family '$name' is not a family of FAMILY_CONFIGS ($FAMILY_CONFIGS)"; exit 64; }
done
[ "${#NAMES[@]}" -gt 0 ] || { echo "::error::pins.env names no family in FAMILY_CONFIGS"; exit 64; }
VERSION=${TAG#v}; VERSION=${VERSION%%-*}
WORK=${WORK:-$STATE/work/$TAG}
CLI=${OTZARIA_SEMANTIC_CLI:-$STATE/bin/otzaria-semantic-search}
EXPORT=${EXPORT_SEMANTIC_PLAN:-$STATE/bin/export_semantic_plan}
VALIDATE=${VALIDATE_SEMANTIC_VECTORS:-$STATE/bin/validate_semantic_vectors}
PY=${VECTOR_PYTHON:-$STATE/venv/bin/python}
GH=${GH:-gh}
ZSTD=${ZSTD:-zstd}
WAREHOUSE=$STATE/warehouse/$PASSAGE_QUANTIZATION-${PASSAGE_PACKAGE_CHECKSUM:0:8}
LEDGERS=$STATE/ledger   # one ledger per family identity: ledger/<identityDigest[0:8]>
PART_SIZE=1992294400
export HSA_ENABLE_DXG_DETECTION=1

T0=$(date +%s)
group() { local t="[+$(( $(date +%s) - T0 ))s]"; if [ "${GITHUB_ACTIONS:-}" = true ]; then echo "::group::$* $t"; else echo "== $* $t"; fi; }
endgroup() { if [ "${GITHUB_ACTIONS:-}" = true ]; then echo "::endgroup::"; fi; }
die() { echo "::error::$*"; exit 1; }
need_free_space() {
  local free_kb
  free_kb=$(df -Pk "$STATE" | awk 'NR==2 {print $4}')
  [ "$free_kb" -ge $((MIN_FREE_GB * 1024 * 1024)) ] || die "only $((free_kb / 1024 / 1024)) GiB free on $STATE, fewer than $MIN_FREE_GB"
}
onnx_runtime() {  # the venv's ONNX Runtime library, which embed-shard and the validator load
  local lib
  for lib in "$STATE"/venv/lib/python3*/site-packages/onnxruntime/capi/libonnxruntime.so.*; do
    [ -f "$lib" ] && { echo "$lib"; return 0; }
  done
  return 1
}

# ─── preflight ─────────────────────────────────────────────────────────────
group "preflight ($TAG, $MODE, ${NAMES[*]})"
# Resolve symlinks and missing suffixes before touching state or clearing scratch files.
# NUL output also preserves paths ending in a newline. The build runner uses GNU coreutils.
canonical() { IFS= read -r -d '' "$2" < <(realpath -m -z -- "$1") || die "could not resolve $1 (GNU realpath is required)"; }
contains() { [ "$1" = / ] || [ "$2" = "$1" ] || [[ "$2" = "$1/"* ]]; }
canonical "$WORK" work_path
canonical "$STATE" state_path
contains "$work_path" "$state_path" && die "--work $WORK contains the persistent state $STATE"
# Scratch may be a child of state (the default), but must be disjoint from everything kept
# across builds, including symlinked caches and custom tool paths outside state.
PERSISTENT=("$STATE/bin" "$STATE/venv" "$STATE/model-cache" "$STATE/warehouse"
  "$LEDGERS" "$LEDGERS.next" "$LEDGERS.prev" "$LEDGERS.migrating" "$STATE/.lock" "$CLI" "$EXPORT" "$VALIDATE" "$PY")
# gh and zstd may be command names, or overridden executable paths.
for tool in "$GH" "$ZSTD"; do case "$tool" in */*) PERSISTENT+=("$tool");; esac; done
for persistent in "${PERSISTENT[@]}"; do
  canonical "$persistent" persistent_path
  if contains "$work_path" "$persistent_path" || contains "$persistent_path" "$work_path"; then
    die "--work $WORK overlaps persistent path $persistent"
  fi
done
mkdir -p "$STATE"
exec 9>"$STATE/.lock"; flock -n 9 || die "another vector build or bootstrap holds $STATE/.lock"
[ -x "$CLI" ] || die "the sidecar CLI is not at $CLI — run bootstrap_runner.sh"
if [ -z "${OTZARIA_SEMANTIC_CLI:-}" ]; then
  [ "$(cat "$CLI.rev" 2>/dev/null)" = "$SIDECAR_REV" ] || die "$CLI is not the pinned sidecar $SIDECAR_REV — run bootstrap_runner.sh"
  grep -q -- '--features onnx-backend' "$CLI.build" 2>/dev/null \
    || die "$CLI was built with no inference backend, so its embed-shard cannot embed — run bootstrap_runner.sh"
fi
[ -n "$PLUGIN_REV" ] || [ -n "${EXPORT_SEMANTIC_PLAN:-}" ] || die "export_semantic_plan v2 (plugin P5) is not pinned yet in pins.env: the release cannot be planned"
[ -x "$EXPORT" ] || die "export_semantic_plan is not at $EXPORT — run bootstrap_runner.sh"
[ -x "$VALIDATE" ] || die "validate_semantic_vectors is not at $VALIDATE — run bootstrap_runner.sh"
if [ -z "${EXPORT_SEMANTIC_PLAN:-}${VALIDATE_SEMANTIC_VECTORS:-}" ]; then
  for bin in "$EXPORT" "$VALIDATE"; do
    [ "$(cat "$bin.rev" 2>/dev/null)" = "$PLUGIN_REV" ] || die "$bin is not the pinned plugin $PLUGIN_REV — run bootstrap_runner.sh"
  done
  grep -qE -- '--features semantic( |$)' "$VALIDATE.build" 2>/dev/null \
    || die "$VALIDATE was built without --features semantic, so it cannot run G6 — run bootstrap_runner.sh"
fi
for name in "${NAMES[@]}"; do
  [ -f "$STATE/bin/families/$name/model.json" ] || die "$STATE/bin/families/$name/model.json is missing — run bootstrap_runner.sh"
done
[ -n "${MODEL_REVISION:-}" ] || die "pins.env pins no MODEL_REVISION: the model is fetched at a commit, never a branch"
for tool in jq sha256sum split tar; do command -v "$tool" >/dev/null || die "$tool is required"; done
MIN_FREE_GB=${VECTORS_MIN_FREE_GB:-$MIN_FREE_GB}
need_free_space
# The warehouse, or none yet: then the first family plans every text and its add creates it. A
# directory holding files but no warehouse.json is neither, and --create would empty it.
if [ ! -f "$WAREHOUSE/warehouse.json" ] && [ -n "$(ls -A "$WAREHOUSE" 2>/dev/null)" ]; then
  die "$WAREHOUSE holds files but no warehouse.json: it is not a warehouse (move it aside to start a new one)"
fi
# The single ledger a build of one family kept in ledger/ itself moves under its family's
# identity, which its published manifest's name carries.
[ -e "$LEDGERS" ] || [ ! -d "$LEDGERS.migrating" ] || mv "$LEDGERS.migrating" "$LEDGERS"
if [ -f "$LEDGERS/published.json" ]; then
  id8=$(jq -r '.manifest // empty' "$LEDGERS/published.json" | sed -En 's/^otzaria-vectors-([0-9a-f]{8})-v[0-9]+-(base|v[0-9]+)\.manifest\.json$/\1/p') \
    || die "$LEDGERS/published.json is unreadable: its family is unknown"
  [ -n "$id8" ] || die "$LEDGERS/published.json names no otzaria-vectors-<id8>-v<N>-base (or -v<M>-v<N>) manifest: its family is unknown"
  rm -rf "$LEDGERS.migrating"; mkdir "$LEDGERS.migrating"
  mv "$LEDGERS" "$LEDGERS.migrating/$id8"; mv "$LEDGERS.migrating" "$LEDGERS"
  echo "ledger: moved to $LEDGERS/$id8"
fi
echo "gh: $({ "$GH" --version 2>/dev/null || echo unavailable; } | head -n1)"
if "$GH" auth status >/dev/null 2>&1; then GH_OK=1; else GH_OK=""; fi
if [ "$MODE" != dry-run ]; then
  [ -n "$GH_OK" ] || die "mode $MODE publishes through gh, and gh is not authenticated"
  for prefix in "${PREFIXES[@]}"; do
    releases=("$prefix-$TAG")
    if delta_family "$prefix"; then releases+=("$prefix-delta-$TAG"); fi
    for release in "${releases[@]}"; do
      if "$GH" release view "$release" --repo "$REPO" >/dev/null 2>&1; then
        die "$release exists already (a draft a failed publish left counts too: delete it to retry, or build the other families with --family); a published vector release is never rebuilt in place"
      fi
    done
  done
  # Each release is tagged at the commit <tag> names (an annotated tag resolves to its commit)
  TARGET=$("$GH" api "repos/$REPO/commits/$TAG" --jq .sha) || die "could not resolve the commit of $TAG"
  echo "$TARGET" | grep -Eq '^[0-9a-f]{40}$' || die "$TAG resolves to '$TARGET', not a commit"
  echo "publish: ${PREFIXES[*]/%/-$TAG} at $TARGET, prerelease $PRERELEASE"
fi
rm -rf "$WORK"; mkdir -p "$WORK/dl" "$WORK/index"
fetch() {  # <asset name>: into $WORK/dl, through gh, or anonymously from the public release URL
  if [ -n "$GH_OK" ]; then
    "$GH" release download "$TAG" --repo "$REPO" --dir "$WORK/dl" --pattern "$1"
  else
    curl -fsSL --retry 5 --retry-delay 10 -o "$WORK/dl/$1" "https://github.com/$REPO/releases/download/$TAG/$1"
  fi
}
RELEASE_JSON=$WORK/library-release.json   # when <tag> was published, and its assets' digests
if [ -n "$GH_OK" ]; then
  "$GH" api "repos/$REPO/releases/tags/$TAG" > "$RELEASE_JSON" || die "could not read the release $TAG"
else
  curl -fsSL "https://api.github.com/repos/$REPO/releases/tags/$TAG" -o "$RELEASE_JSON" || die "could not read the release $TAG"
fi
CREATED=$(jq -r .published_at "$RELEASE_JSON")
echo "$CREATED" | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' || die "could not read when $TAG was published"
# The plan trusts the warehouse's index: bad data stops the build here, a bad index is rebuilt.
if [ -f "$WAREHOUSE/warehouse.json" ]; then
  checked=$("$CLI" warehouse-verify --warehouse "$WAREHOUSE" --repair 2>&1) \
    || die "warehouse-verify --repair, before the plan: $checked"
  echo "$checked"
fi
endgroup

# ─── 1 index ───────────────────────────────────────────────────────────────
group "1 index: the search index of $TAG"
ARCHIVE=otzaria-library-index.tar.zst
fetch otzaria-library-index.provenance.json
manifest="$WORK/dl/$ARCHIVE.manifest.json"
provenance="$WORK/dl/otzaria-library-index.provenance.json"
prov() { jq -r "$1 // empty" "$provenance"; }
# Whole when it fits one asset; parts and a manifest only above that.
if jq -e --arg n "$ARCHIVE" 'any(.assets[]; .name == $n)' "$RELEASE_JSON" >/dev/null; then
  manifest=""
  archive_sha=$(prov .indexArchiveSha256)
  echo "$archive_sha" | grep -Eqx '[0-9a-f]{64}' || die "the index provenance states no sha256 of $ARCHIVE"
else
  fetch "$ARCHIVE.manifest.json"
  archive_sha=$(jq -r '.sha256 // empty' "$manifest")
  echo "$archive_sha" | grep -Eqx '[0-9a-f]{64}' || die "the index manifest states no sha256 of its archive"
  [ "$(jq -r '.archive // empty' "$manifest")" = "$ARCHIVE" ] || die "the index manifest does not describe $ARCHIVE"
fi
# Before the archive download: the provenance names this release, the archive and
# this release's full DB (its digest, or its split manifest's sha256). A missing field fails.
[ "$(prov .schemaVersion)" = 1 ] || die "the index provenance is not a schema 1 provenance"
[ "$(prov .libraryReleaseTag)" = "$TAG" ] \
  || die "the index provenance names release '$(prov .libraryReleaseTag)': this index is another release's, not $TAG's"
[ "$(prov .indexArchive)" = "$ARCHIVE" ] && [ "$(prov .indexArchiveSha256)" = "$archive_sha" ] \
  || die "the index provenance names archive '$(prov .indexArchive)' sha256 '$(prov .indexArchiveSha256)', not the manifest's $ARCHIVE $archive_sha"
db=$(jq -c "$FULL_DB_ASSET_JQ" "$RELEASE_JSON")
db_name=$(printf '%s' "$db" | jq -r '.name // empty')
[ -n "$db_name" ] || die "$TAG carries no full DB, so the index provenance's database cannot be checked"
if [ -n "$(printf '%s' "$db" | jq -r '.manifest // empty')" ]; then
  fetch "$db_name.manifest.json"
  db_sha=$(jq -r '.sha256 // empty' "$WORK/dl/$db_name.manifest.json")
else
  db_sha=$(printf '%s' "$db" | jq -r '.digest // empty'); db_sha=${db_sha#sha256:}
fi
echo "$db_sha" | grep -Eqx '[0-9a-f]{64}' || die "$TAG publishes no sha256 of $db_name, so the index provenance's database cannot be checked"
[ "$(prov .seforimDbZstSha256)" = "$db_sha" ] \
  || die "the index provenance names database sha256 '$(prov .seforimDbZstSha256)', not $db_name's $db_sha: this index was built from another database"
# The parts' paths in an array, never split on whitespace: --state and --work may hold any character.
parts=()
if [ -z "$manifest" ]; then
  parts=("$WORK/dl/$ARCHIVE")
  fetch "$ARCHIVE"
else
  while IFS= read -r part; do
    echo "$part" | grep -Eqx 'otzaria-library-index\.tar\.zst\.part-[0-9]+' || die "the index manifest names an unexpected part '$part'"
    parts+=("$WORK/dl/$part")
  done < <(jq -r '.parts[].name' "$manifest")
  [ "${#parts[@]}" -gt 0 ] || die "the index manifest lists no parts"
  for part in "${parts[@]}"; do fetch "${part##*/}"; done
  jq -r '.parts[] | "\(.sha256)  \(.name)"' "$manifest" | (cd "$WORK/dl" && sha256sum -c --quiet -) || die "an index part does not hash to its manifest"
fi
whole=$(cat "${parts[@]}" | sha256sum | cut -d' ' -f1)
[ "$whole" = "$archive_sha" ] || die "the index archive hashes to $whole, not the $archive_sha its manifest and provenance state"
cat "${parts[@]}" | "$ZSTD" -dc --long=31 | tar -x -C "$WORK/index"
rm -f "${parts[@]}"
INDEX=$WORK/index/index
[ -f "$INDEX/meta.json" ] || die "the archive holds no index/meta.json"
echo "index: $(du -sh "$INDEX" | cut -f1), $(jq -r .searchEngineVersion "$WORK/dl/otzaria-library-index.provenance.json") engine"
# The DB the provenance named, held to that digest: the planner and the validator read official
# books' line text from it (index schema 5), as the application does.
DB=$WORK/seforim.db
if [ -n "$(printf '%s' "$db" | jq -r '.manifest // empty')" ]; then
  db_manifest="$WORK/dl/$db_name.manifest.json"
  db_parts=()
  while IFS= read -r part; do
    case "$part" in "$db_name".part-*) ;; *) die "$db_name.manifest.json names an unexpected part '$part'" ;; esac
    echo "${part#"$db_name".part-}" | grep -Eqx '[0-9]+' || die "$db_name.manifest.json names an unexpected part '$part'"
    db_parts+=("$WORK/dl/$part")
  done < <(jq -r '.parts[].name' "$db_manifest")
  [ "${#db_parts[@]}" -gt 0 ] || die "$db_name.manifest.json lists no parts"
  for part in "${db_parts[@]}"; do fetch "${part##*/}"; done
  jq -r '.parts[] | "\(.sha256)  \(.name)"' "$db_manifest" | (cd "$WORK/dl" && sha256sum -c --quiet -) || die "a part of $db_name does not hash to its manifest"
else
  db_parts=("$WORK/dl/$db_name")
  fetch "$db_name"
fi
whole=$(cat "${db_parts[@]}" | sha256sum | cut -d' ' -f1)
[ "$whole" = "$db_sha" ] || die "$db_name hashes to $whole, not the $db_sha the release and the index provenance state"
cat "${db_parts[@]}" | "$ZSTD" -dc --long=31 --memory=2048MB > "$DB" || die "could not expand $db_name"
rm -f "${db_parts[@]}"
[ -s "$DB" ] || die "$db_name expanded to nothing"
echo "database: $db_name, $(du -sh "$DB" | cut -f1)"
endgroup

no_delta() { echo "::warning::$RELEASE_TAG is published as a base alone, with no delta: $*"; }

family_of() {  # <ledger dir> <prefix>: the library version of the family's release it was published with, or nothing
  jq -r --arg p "$2-" '.published // "" | select(startswith($p)) | ltrimstr($p)
    | select(test("^v[0-9]+-[0-9]{14}$")) | ltrimstr("v") | split("-")[0]' "$1/published.json" 2>/dev/null || true
}

family_ledger() {  # <prefix>: FOUND, the ledger the family's newest publish persisted here, or ""
  local dir v best=-1
  FOUND=""
  for dir in "$LEDGERS"/*/; do
    dir=${dir%/}
    [[ ${dir##*/} =~ ^[0-9a-f]{8}$ ]] || continue
    v=$(family_of "$dir" "$1")
    if [ -n "$v" ] && [ "$v" -gt "$best" ]; then FOUND=$dir; best=$v; fi
  done
}

same_threshold() {  # <a> <b>: both none, or the same number
  if [ -z "$1" ] || [ -z "$2" ]; then [ -z "$1$2" ]; return; fi
  awk -v a="$1" -v b="$2" 'BEGIN { exit !(a + 0 == b + 0) }'
}

restore_ledger() {  # <prefix> <library tag>: the ledger backed up with that release, into $LEDGERS, and its rotation
  local d=$FW/restore archive id8 r
  rm -rf "$d"; mkdir -p "$d/x"
  "$GH" release download "$LEDGER_BACKUPS-$2" --repo "$REPO" --dir "$d" --pattern "$1.ledger-*.tar.zst" >/dev/null 2>&1 || return 1
  archive=("$d"/*.tar.zst)
  [ "${#archive[@]}" -eq 1 ] && [ -f "${archive[0]}" ] || return 1
  id8=${archive[0]##*.ledger-}; id8=${id8%%-*}
  [[ $id8 =~ ^[0-9a-f]{8}$ ]] && [ ! -e "$LEDGERS/$id8" ] || return 1
  "$ZSTD" -dc < "${archive[0]}" > "$d/ledger.tar" || return 1
  # Only <id8>/ and the regular files directly in it: no link, device, or path out of the ledger.
  tar -tvf "$d/ledger.tar" > "$d/listing" && tar -tf "$d/ledger.tar" > "$d/names" || return 1
  grep -qv '^[-d]' "$d/listing" && return 1
  grep -Evxq "$id8/?|$id8/[^/]+" "$d/names" && return 1
  tar -xf "$d/ledger.tar" -C "$d/x" --no-same-owner --no-same-permissions || return 1
  mkdir -p "$LEDGERS" && mv "$d/x/$id8" "$LEDGERS/$id8" || return 1
  echo "ledger: restored $LEDGERS/$id8 from $LEDGER_BACKUPS-$2"
  # The rotation this runner had fitted, when the backup holds one for this warehouse
  if [ ! -f "$ROTATION_KEPT" ] \
    && "$GH" release download "$LEDGER_BACKUPS-$2" --repo "$REPO" --dir "$d" --pattern "$1.rotation-$ROTATION_KEY-*.bin" >/dev/null 2>&1; then
    for r in "$d/$1.rotation-$ROTATION_KEY-"*.bin; do
      [ "$(sha256sum < "$r" | cut -c1-16)" = "$(basename "$r" .bin | sed 's/.*-//')" ] || continue
      mkdir -p "${ROTATION_KEPT%/*}" && cp "$r" "$ROTATION_KEPT" && echo "rotation: restored $ROTATION_KEPT"
    done
  fi
  rm -rf "$d"
}

previous_base() {  # PREV_RELEASE: the assembled base of $PREV_TAG, kept beside its ledger or downloaded
  local d=$FW/previous manifest sha parts=() f
  if [ "$(jq -r '"\(.kind)-\(.toLibraryVersion)"' "$PREV/release/release.json" 2>/dev/null)" = "base-$PV" ] \
    && [ -f "$PREV/release/segment.oxv" ]; then
    PREV_RELEASE=$PREV/release; return 0
  fi
  manifest=$(jq -r '.manifest // empty' "$PREV/published.json"); sha=$(jq -r '.manifest_sha256 // empty' "$PREV/published.json")
  rm -rf "$d"; mkdir -p "$d"
  # As a device installs it: the manifest held to the digest published with it, then its files.
  "$GH" release download "$PREV_TAG" --repo "$REPO" --dir "$d" --pattern "$manifest" || return 1
  [ "$(sha256sum < "$d/$manifest" | cut -d' ' -f1)" = "$sha" ] || return 1
  while IFS= read -r f; do
    echo "$f" | grep -Eqx 'otzaria-vectors-[0-9a-f]{8}-v[0-9]+-base\.oxv\.zst(\.part-[0-9]+)?' || return 1
    "$GH" release download "$PREV_TAG" --repo "$REPO" --dir "$d" --pattern "$f" || return 1
    parts+=("$d/$f")
  done < <(jq -r '.files[].file' "$d/$manifest")
  [ "${#parts[@]}" -gt 0 ] || return 1
  jq -r '.files[] | "\(.sha256)  \(.file)"' "$d/$manifest" | (cd "$d" && sha256sum -c --quiet -) || return 1
  cat "${parts[@]}" | "$ZSTD" -dc > "$d/segment.oxv" || return 1
  rm -f "${parts[@]}"
  [ "$(sha256sum < "$d/segment.oxv" | cut -d' ' -f1)" = "$(jq -r .segment.sha256 "$d/$manifest")" ] || return 1
  mv "$d/$manifest" "$d/release.json"
  PREV_RELEASE=$d
}

# PREV, PV, PREV_TAG and PREV_RELEASE of the chain a delta extends, and CHAIN, the releases a
# device that chained onto it holds; or none of them, and why. Returns 0 either way: an
# unexpected failure stops the build.
previous_chain() {  # <prefix> <codec> <alias threshold>
  local prefix=$1 tags latest pv ledger i
  tags=$("$GH" release list --repo "$REPO" --limit 200 --json tagName,isDraft --jq '.[] | select(.isDraft | not) | .tagName') \
    || { no_delta "the published releases could not be listed"; return 0; }
  latest=$(printf '%s\n' "$tags" | awk -v p="$prefix-" 'index($0, p) == 1 { print substr($0, length(p) + 1) }' \
    | { grep -Ex 'v[0-9]+-[0-9]{14}' || true; } | { grep -vxF "$TAG" || true; } | sort -V | tail -n1)
  [ -n "$latest" ] || { no_delta "no $prefix release is published yet"; return 0; }
  family_ledger "$prefix"
  if [ -z "$FOUND" ]; then
    restore_ledger "$prefix" "$latest" || { no_delta "no ledger of $prefix here, and none backed up in $LEDGER_BACKUPS-$latest"; return 0; }
    family_ledger "$prefix"
  fi
  ledger=$FOUND
  pub() { jq -r "$1 // empty" "$ledger/published.json"; }
  [ "$(pub .published)" = "$prefix-$latest" ] \
    || { no_delta "the ledger here is $(pub .published)'s, and the release published last is $prefix-$latest"; return 0; }
  pv=${latest#v}; pv=${pv%%-*}
  [ "$pv" -lt "$VERSION" ] || { no_delta "$prefix-$latest is not of a version before $VERSION"; return 0; }
  [ "$(jq -r '.library_version // empty' "$ledger/ledger-v$pv.manifest.json" 2>/dev/null)" = "$pv" ] \
    || { no_delta "$ledger holds no ledger of v$pv"; return 0; }
  [ "$(jq -r '.passage_package.checksum // empty' "$ledger/ledger-v$pv.manifest.json")" = "$PASSAGE_PACKAGE_CHECKSUM" ] \
    || { no_delta "the ledger's passages were embedded with another package"; return 0; }
  [ "$(pub .codec)" = "$2" ] || { no_delta "the ledger is in codec '$(pub .codec)', and this family's is $2"; return 0; }
  same_threshold "$(pub .alias_threshold)" "$3" \
    || { no_delta "the ledger shares short lines' vectors at '$(pub .alias_threshold)', and this family at '$3'"; return 0; }
  [ -n "$(pub .plan_identity_sha256)" ] || { no_delta "the ledger records no identity"; return 0; }
  PREV=$ledger; PV=$pv; PREV_TAG=$prefix-$latest
  if ! previous_base; then
    PREV=""; no_delta "the base of $PREV_TAG could not be had to validate the delta on"; return 0
  fi
  need_free_space   # with that base downloaded
  # A device that installed an older base and the deltas after it: the chain kept beside the ledger
  CHAIN=(); CHAIN_FROM=""
  if [ -f "$PREV/chain/base/release.json" ]; then
    CHAIN=("$PREV/chain/base"); i=1; CHAIN_FROM=$(jq -r .toLibraryVersion "$PREV/chain/base/release.json")
    while [ -f "$PREV/chain/delta-$i/release.json" ]; do CHAIN+=("$PREV/chain/delta-$i"); i=$((i + 1)); done
  fi
  echo "a delta from $PREV_TAG: ledger $PREV, its base $PREV_RELEASE${CHAIN[0]:+, and a chain of ${#CHAIN[@]} release(s) before it}"
}

epoch_rotation() {  # <name>: ROTATION, the rot192-i8i4 epoch a new base of the family starts, and ROTATION_FITTED
  local entry pinned="" printed
  for entry in ${ROTATIONS:-}; do [ "${entry%%=*}" != "$1" ] || pinned=${entry#*=}; done
  ROTATION_FITTED=""
  if [ -n "$pinned" ]; then
    ROTATION=$HERE/rotation/$1.bin
    [ "$(sha256sum < "$ROTATION" | cut -d' ' -f1)" = "$pinned" ] || die "$ROTATION is not the rotation ROTATIONS pins ($pinned)"
  elif [ -f "$ROTATION_KEPT" ]; then
    ROTATION=$ROTATION_KEPT; ROTATION_FITTED=1
    echo "rotation: the one fitted here before on this warehouse, $ROTATION"
  else
    ROTATION=$FW/rotation.bin; ROTATION_FITTED=1
    "$CLI" fit-rotation --warehouse "$WAREHOUSE" --out "$ROTATION" | tee "$FW/fit-rotation.log"
    printed=$(sed -n 's/^Epoch SHA-256: *\([0-9a-f]\{64\}\)$/\1/p' "$FW/fit-rotation.log")
    [ -n "$printed" ] || die "fit-rotation printed no epoch SHA-256"
    [ "$(sha256sum < "$ROTATION" | cut -d' ' -f1)" = "$printed" ] || die "fit-rotation wrote no rotation that hashes to the epoch it printed"
  fi
}

make_plan() {  # $FW/plan, split against the warehouse and, with one, against $PREV
  # The plugin's export_semantic_plan (PLUGIN_REV): the recipe comes from the model's
  # chunking_identity, PDF lines are left out, and keys are recomputed from the text of a
  # schema 4 index (held to its chunkKey column on schema 5).
  local split=(--warehouse "$WAREHOUSE")
  if [ -n "$FRESH" ]; then split=(); echo "no warehouse yet at $WAREHOUSE: every text is planned"; fi
  [ -z "$PREV" ] || split+=(--previous-ledger "$PREV")
  rm -rf "$FW/plan"
  "$EXPORT" --index "$INDEX" --seforim-db "$DB" --library-version "$VERSION" --release-tag "$TAG" \
    --model "$FAMILY" --passage-quantization "$PASSAGE_QUANTIZATION" \
    ${split[@]+"${split[@]}"} --created-at "$CREATED" --out "$FW/plan" | tee "$FW/plan.log"
  PLAN=$FW/plan
  jq -e '.format == "otzaria-vector-plan" and .version == 1' "$PLAN/plan-manifest.json" >/dev/null || die "the plan is not an otzaria-vector-plan version 1"
  [ "$(jq -r .library_release_tag "$PLAN/plan-manifest.json")" = "$TAG" ] || die "the plan was made for another release"
  [ "$(jq -r .parity.mismatches "$PLAN/plan-manifest.json")" = 0 ] || die "the planner's key parity (G2) failed"
  [ "$(jq -r .passage_package.checksum "$PLAN/embed-manifest.json")" = "$PASSAGE_PACKAGE_CHECKSUM" ] || die "the plan names another passage package"
  [ -z "$PREV" ] || [ "$(jq -r '.previous.library_version // empty' "$PLAN/plan-manifest.json")" = "$PV" ] \
    || die "the plan is not split against the ledger of v$PV"
  PLAN_IDENTITY=$(jq -cS .identity "$PLAN/plan-manifest.json" | sha256sum | cut -d' ' -f1)
  TO_EMBED=$(jq -r .records "$PLAN/embed-manifest.json")
  jq -c .counts "$PLAN/plan-manifest.json"
  echo "to embed: $TO_EMBED"
}

verified() {  # <release dir>: assemble --verify on it, and VERIFIED its exit
  set +e
  "$CLI" assemble --verify --plan "$PLAN" --warehouse "$WAREHOUSE" ${PREVIOUS[@]+"${PREVIOUS[@]}"} --out "$1"
  VERIFIED=$?
  set -e
  rm -rf "$1.g8"   # G8's assembly again, as large as the release
  jq -c . "$1/gates.json" 2>/dev/null || true
}

validated() {  # <what> <report> <release>...: the set those releases install, validated on the index; VALIDATED its exit
  # What assemble --verify cannot check, since it needs the index: the plugin's validator installs
  # the releases as a device installs them (into a set under the work directory), and holds it to
  # the index and the plan (G3, G4) and the recall of its scan with the int8 query model the
  # application runs against the exact f32 scan of the warehouse (G6). Its report is published.
  local what=$1 report=$2 releases=() floors=() r g6 entry
  shift 2
  for r in "$@"; do releases+=(--release "$r"); done
  for entry in ${G6_FLOORS:-}; do
    [ "${entry%%=*}" != "$name" ] || { r=${entry#*=}; floors=(--min-recall-10 "${r%,*}" --min-recall-50 "${r#*,}"); }
  done
  set +e
  TMPDIR="$FW" "$VALIDATE" --index "$INDEX" --seforim-db "$DB" "${releases[@]}" --plan "$PLAN" \
    --warehouse "$WAREHOUSE" --model "$QPKG/$QUERY_GRAPH" --model-identity "$FAMILY" --onnx-runtime "$ORT" \
    --threads "$(getconf _NPROCESSORS_ONLN)" --report "$report" ${floors[@]+"${floors[@]}"}
  VALIDATED=$?
  set -e
  jq -c '{passed, gates: [.gates[] | {gate, status}]}' "$report" 2>/dev/null || true
  # G6's recall against its floors, also when it failed: what a dry run of a new codec measures
  g6=$(jq -r '.gates[] | select(.gate == "G6") | .metrics | select(type == "object" and .recallAt10 != null) |
    "G6 recall@10 \(.recallAt10) (floor \(.minRecallAt10)), recall@50 \(.recallAt50) (floor \(.minRecallAt50)), \(.queries) queries"' \
    "$report" 2>/dev/null) || g6=""
  if [ -n "$g6" ]; then
    echo "$name, $what: $g6"
    [ -z "${GITHUB_STEP_SUMMARY:-}" ] || echo "- \`$name\` ($MODE), $what: $g6" >> "$GITHUB_STEP_SUMMARY"
  fi
  [ "$VALIDATED" -eq 0 ]
}

release_files() {  # <release dir> <log>: STEM, DATA and MANIFEST_SHA of its published files, in $FW/files
  local rel=$1/release.json size f
  STEM=$(jq -r '"otzaria-vectors-\(.identityDigest[0:8])-" + if .kind == "delta"
    then "v\(.fromLibraryVersion)-v\(.toLibraryVersion)" else "v\(.toLibraryVersion)-base" end' "$rel")
  echo "$STEM" | grep -Eqx 'otzaria-vectors-[0-9a-f]{8}-v[0-9]+-(base|v[0-9]+)' || die "$rel states no identity digest or versions"
  "$ZSTD" -q -T0 -10 "$1/segment.oxv" -o "$FW/files/$STEM.oxv.zst"
  size=$(stat -c %s "$FW/files/$STEM.oxv.zst")
  if [ "$size" -ge "$PART_SIZE" ]; then
    split -b "$PART_SIZE" -d -a 3 "$FW/files/$STEM.oxv.zst" "$FW/files/$STEM.oxv.zst.part-"
    rm "$FW/files/$STEM.oxv.zst"
  fi
  DATA=()
  while IFS= read -r -d '' f; do DATA+=("$f"); done < <(find "$FW/files" -maxdepth 1 -name "$STEM.oxv.zst*" -print0 | sort -z)
  local file_args=()
  for f in "${DATA[@]}"; do file_args+=(--files "$f"); done
  "$CLI" release-files --release "$rel" --compression zstd "${file_args[@]}" --out "$FW/files/$STEM.manifest.json" | tee "$FW/$2"
  MANIFEST_SHA=$(sed -n 's/^Manifest SHA-256 \([0-9a-f]\{64\}\)$/\1/p' "$FW/$2" | tail -n1)
  [ -n "$MANIFEST_SHA" ] || die "release-files printed no manifest SHA-256"
  [ "$(sha256sum < "$FW/files/$STEM.manifest.json" | cut -d' ' -f1)" = "$MANIFEST_SHA" ] || die "the manifest written is not the one release-files names"
  echo "manifest $STEM.manifest.json sha256 $MANIFEST_SHA; data: ${#DATA[@]} file(s)"
}

publish() {  # <release tag> <title> <notes file> <prerelease> <file>...: a draft first, published once it holds exactly the files
  local tag=$1 title=$2 notes=$3 prerelease=$4 f expected actual
  shift 4
  # A draft fires no `release` event and creates no tag: nothing is published until every
  # file is in place and checked.
  "$GH" release create "$tag" --repo "$REPO" --draft --target "$TARGET" --prerelease="$prerelease" \
    --latest=false --title "$title" --notes-file "$notes"
  for f in "$@"; do
    "$GH" release upload "$tag" "$f" --repo "$REPO"
  done
  expected=$(for f in "$@"; do printf '%s\t%s\n' "$(basename "$f")" "$(stat -c %s "$f")"; done | sort)
  actual=$("$GH" release view "$tag" --repo "$REPO" --json assets --jq '.assets[] | "\(.name)\t\(.size)"' | sort) \
    || publish_failed "could not list the assets of the draft $tag; it stays a draft"
  [ "$actual" = "$expected" ] || publish_failed "the draft $tag does not hold the files built (expected: $(printf '%s' "$expected" | tr '\t\n' '  '); found: $(printf '%s' "$actual" | tr '\t\n' '  ')); it stays a draft"
  "$GH" release edit "$tag" --repo "$REPO" --draft=false --latest=false
}
publish_failed() { echo "::${PUBLISH_LEVEL:-error}::$*"; exit 1; }

prune_family_ledgers() {  # <prefix> <ledger kept>: the family's ledgers of earlier epochs go
  local dir
  for dir in "$LEDGERS"/*/; do
    dir=${dir%/}
    [[ ${dir##*/} =~ ^[0-9a-f]{8}$ ]] || continue
    [ "$dir" != "$2" ] || continue
    if [ -n "$(family_of "$dir" "$1")" ]; then rm -rf "$dir"; echo "ledger: $dir, an earlier epoch of $1, removed"; fi
  done
}

build_family() {  # <name> <release prefix> <codec, or ""> <alias threshold, or "">: steps 2 to 7 of one family, in a subshell of its own
  local name=$1 prefix=$2 codec=$3 threshold=$4
  local FAMILY=$STATE/bin/families/$name/model.json FW=$WORK/$name RELEASE_TAG=$prefix-$TAG FRESH="" first
  first=${FAMILY_CONFIGS%% *}; first=${first%%:*}; first=${first##*/}
  need_free_space   # the families before this one hold space too
  mkdir -p "$FW/shards" "$FW/files"
  PREV=""; PV=""; PREV_TAG=""; PREV_RELEASE=""; PREVIOUS=(); CHAIN=(); ROTATION=""; ROTATION_FITTED=""
  # A fitted rotation is the warehouse's: another warehouse fits its own.
  ROTATION_KEY=$name.${WAREHOUSE##*/}; ROTATION_KEPT=$LEDGERS/rotations/$ROTATION_KEY.bin

  # ─── 2 plan ──────────────────────────────────────────────────────────────
  group "2 plan ($name)"
  [ -f "$WAREHOUSE/warehouse.json" ] || FRESH=1
  if delta_family "$prefix"; then previous_chain "$prefix" "${codec:-$DEFAULT_CODEC}" "$threshold"; fi
  make_plan
  if [ -n "$PREV" ] && [ "$PLAN_IDENTITY" != "$(jq -r '.plan_identity_sha256 // empty' "$PREV/published.json")" ]; then
    no_delta "the plan's identity is not the one the ledger of $PREV_TAG was assembled under"
    PREV=""; CHAIN=(); make_plan
  fi
  endgroup

  # ─── 3 embed + 4 warehouse ───────────────────────────────────────────────
  group "3 embed ($name): $TO_EMBED text(s) the warehouse lacks"
  EMBEDDED_BY="none: every vector came from the warehouse"
  if [ "$TO_EMBED" -eq 0 ]; then
    [ -z "$FRESH" ] || die "the plan has no text to embed and there is no warehouse: nothing to assemble a release from"
    echo "every text of the plan has its vector in the warehouse already"
    endgroup
  else
    if [ "$TO_EMBED" -lt "$CPU_EMBED_MAX" ]; then
      # Few texts: ONNX Runtime with the fp32 package on this CPU, the reference itself, which needs
      # no parity certificate (the GPU worker's could not sample 1,000 texts from fewer than 959).
      PKG=$("$PY" "$HERE/model_package.py" fetch --cache "$STATE/model-cache" --checksum "$PASSAGE_PACKAGE_CHECKSUM" \
        --graph "$MODEL_GRAPH" --repo "$MODEL_REPO" --revision "$MODEL_REVISION" --token-env OTZARIA_HF_TOKEN) \
        || die "could not fetch the passage package $PASSAGE_PACKAGE_CHECKSUM"
      ORT=$(onnx_runtime) || die "the venv holds no ONNX Runtime library for embed-shard — run bootstrap_runner.sh"
      echo "on the CPU: embed-shard in $CPU_PROCESSES process(es) of $CPU_THREADS thread(s), ONNX Runtime $ORT"
      EMBEDDED_BY="otzaria-semantic-search embed-shard (onnxruntime, cpu)"
      OTZARIA_ONNX_RUNTIME=$ORT "$CLI" embed-shard --plan "$PLAN" --model-file "$PKG/$MODEL_GRAPH" \
        --processes "$CPU_PROCESSES" --threads "$CPU_THREADS" --out "$FW/shards/cpu"
    else
      EMBEDDED_BY="seforim-gpu-worker 1.0"
      skip=0; n=0
      while [ "$skip" -lt "$TO_EMBED" ]; do
        "$PY" "$HERE/embed_worker.py" --plan "$PLAN" --out "$FW/shards/$(printf 's%03d' "$n")" \
          --skip "$skip" --take "$EMBED_WINDOW" --cache "$STATE/model-cache" \
          --repo "$MODEL_REPO" --revision "$MODEL_REVISION" --graph "$MODEL_GRAPH" --token-env OTZARIA_HF_TOKEN
        skip=$((skip + EMBED_WINDOW)); n=$((n + 1))
      done
    fi
    endgroup
    group "4 warehouse ($name): add the shards, held to the plan"
    CREATE=()
    [ -z "$FRESH" ] || CREATE=(--create --model "$FAMILY" --passage-quantization "$PASSAGE_QUANTIZATION")
    "$CLI" warehouse-add --warehouse "$WAREHOUSE" ${CREATE[@]+"${CREATE[@]}"} --plan "$PLAN" --shards "$FW/shards"
    rm -rf "$FW/shards"   # the warehouse holds their vectors now
    endgroup
  fi

  # ─── 5 assemble + gates ──────────────────────────────────────────────────
  group "5 assemble ($name), then the gates"
  BUILT_BY=$(jq -cn --arg repo "$REPO" --arg run "${GITHUB_RUN_ID:-manual}" --arg sha "${GITHUB_SHA:-$(git -C "$HERE" rev-parse HEAD 2>/dev/null || echo unknown)}" \
    --arg sidecar "$SIDECAR_REV" --arg plugin "$PLUGIN_REV" --arg worker "$EMBEDDED_BY" \
    '{repository: $repo, workflow: "build-library-vectors", runId: $run, commit: $sha, sidecar: $sidecar, plugin: $plugin, worker: $worker}')
  DELTA=""
  if [ -n "$PREV" ]; then
    PREVIOUS=(--previous "$PREV" --previous-version "$PV")
    # Every check of the chain passed, so an assembly that fails says the ledger itself is not
    # this chain's: nothing is published until someone looks at it.
    "$CLI" assemble --kind delta --plan "$PLAN" --warehouse "$WAREHOUSE" "${PREVIOUS[@]}" --out "$FW/delta" \
      --created-at "$CREATED" --built-by "$BUILT_BY" \
      || die "the delta could not be assembled against the ledger $PREV; move the ledger aside to start a new epoch"
    verified "$FW/delta"
    if [ "$VERIFIED" -eq 0 ]; then
      DELTA=1
    elif [ "$VERIFIED" -eq 2 ] && jq -e '[.gates[] | select(.passed != true) | .gate] == ["G7"]' "$FW/delta/gates.json" >/dev/null 2>&1; then
      no_delta "the delta is past G7's share of its base"
    else
      no_delta "a gate of the delta failed (assemble --verify exited $VERIFIED)"
    fi
    # Either way the base keeps the chain's epoch, so a device that holds it needs no new base.
    [ -n "$DELTA" ] || rm -rf "$FW/delta"
  fi
  if [ -n "$PREV" ]; then
    BASE_EPOCH=(--keep-epoch "${PREVIOUS[@]}")   # the chain's epoch, which its devices hold
  else
    BASE_EPOCH=()
    [ -z "$codec" ] || BASE_EPOCH=(--codec "$codec")
    if [ "$codec" = rot192-i8i4 ]; then epoch_rotation "$name"; BASE_EPOCH+=(--rotation "$ROTATION"); fi
  fi
  # A delta takes its chain's threshold; a base is given it (previous_chain held it to the chain's).
  [ -z "$threshold" ] || BASE_EPOCH+=(--alias-threshold "$threshold")
  "$CLI" assemble --kind base --plan "$PLAN" --warehouse "$WAREHOUSE" ${BASE_EPOCH[@]+"${BASE_EPOCH[@]}"} --out "$FW/release" \
    --created-at "$CREATED" --built-by "$BUILT_BY"
  verified "$FW/release"
  [ "$VERIFIED" -eq 0 ] || { echo "::error::a gate failed (assemble --verify exited $VERIFIED); nothing is published"; exit 2; }
  endgroup

  group "5 validate the release ($name) on its index: G3 coverage, G4 resolution, G6 retrieval"
  QPKG=$("$PY" "$HERE/model_package.py" fetch --cache "$STATE/model-cache" --checksum "$QUERY_PACKAGE_CHECKSUM" \
    --graph "$QUERY_GRAPH" --repo "$MODEL_REPO" --revision "$MODEL_REVISION" --token-env OTZARIA_HF_TOKEN) \
    || die "could not fetch the query package $QUERY_PACKAGE_CHECKSUM"
  ORT=$(onnx_runtime) || die "the venv holds no ONNX Runtime library for the validator — run bootstrap_runner.sh"
  validated release "$FW/release/validation.json" "$FW/release" \
    || { echo "::error::validate_semantic_vectors exited $VALIDATED: the release did not pass G3, G4 and G6; nothing is published"; exit 2; }
  if [ -n "$DELTA" ] && ! validated "delta after the base of $PREV_TAG" "$FW/delta/validation.json" "$PREV_RELEASE" "$FW/delta"; then
    no_delta "validate_semantic_vectors exited $VALIDATED on the delta installed after the base of $PREV_TAG"; DELTA=""
  fi
  # What a device that chained onto that base holds: the chain's base, its deltas, then this one
  if [ -n "$DELTA" ] && [ "${#CHAIN[@]}" -gt 0 ] \
    && ! validated "delta at the end of a chain of ${#CHAIN[@]}" "$FW/delta/validation-chain.json" "${CHAIN[@]}" "$FW/delta"; then
    no_delta "validate_semantic_vectors exited $VALIDATED on the delta at the end of the chain from $CHAIN_FROM"; DELTA=""
  fi
  [ -n "$DELTA" ] || rm -rf "$FW/delta"
  endgroup

  # ─── 6 files ─────────────────────────────────────────────────────────────
  group "6 the published files ($name)"
  ID8=$(jq -r '.identityDigest[0:8]' "$FW/release/release.json")
  echo "$ID8" | grep -Eqx '[0-9a-f]{8}' || die "release.json states no identity digest"
  if [ -n "$DELTA" ]; then
    release_files "$FW/delta" delta-files.log
    DELTA_STEM=$STEM; DELTA_DATA=("${DATA[@]}"); DELTA_SHA=$MANIFEST_SHA
  fi
  release_files "$FW/release" release-files.log
  endgroup

  if [ "$MODE" = dry-run ]; then
    # Kept for inspection: the plan, the release, the files and the logs; the rest (the validator's set) goes.
    find "$FW" -mindepth 1 -maxdepth 1 ! -name plan ! -name release ! -name files ! -name '*.log' ! -name rotation.bin -exec rm -rf {} +
    echo "dry run: built and verified $STEM ($name); nothing published, the ledger is not persisted; $(du -sh "$FW" | cut -f1) kept in $FW"
    return 0
  fi

  # ─── 7 publish the base, persist and back up the ledger, then the delta ──
  group "7 publish $RELEASE_TAG"
  notes="$FW/notes.md"
  {
    echo "Semantic vectors for library release $TAG (base)."
    echo
    if [ "$name" != "$first" ]; then
      echo "Family: \`$name\`, chunking identity \`$(jq -r .chunking_identity "$FAMILY")\`."
      echo
      jq -er '.gates[] | select(.gate == "G3") | .metrics |
        select([.standaloneTextChars, .windowedStandaloneChars, .unwindowedStandaloneChars, .partiallyWindowedStandaloneLines, .unwindowedStandaloneLines, .borrowedContextLines] |
          all(type == "number" and . >= 0)) |
        "Standalone source text: \(.windowedStandaloneChars) / \(.standaloneTextChars) normalized standalone characters inside emitted window spans; \(.unwindowedStandaloneChars) outside, \(.partiallyWindowedStandaloneLines) partially windowed standalone lines, \(.unwindowedStandaloneLines) standalone lines with no window; \(.borrowedContextLines) context-borrowing lines excluded."' \
        "$FW/release/validation.json"
      echo "G3 certifies coverage of the windows this bounded recipe emits. It does not certify complete source text coverage or semantic recall; character counts cover standalone lines only, exclude short lines which borrow neighbour context, and duplicate window suppression can make the span count conservative."
      echo
    fi
    echo "Manifest: \`$STEM.manifest.json\`, SHA-256 \`$MANIFEST_SHA\` — check it before trusting the files it lists."
    echo
    echo "Gates: \`gates.json\` (assemble --verify) and \`validation.json\` (validate_semantic_vectors on the release index: G3, G4, G6)."
  } > "$notes"
  publish "$RELEASE_TAG" "Library vectors $TAG" "$notes" "$PRERELEASE" "${DATA[@]}" "$FW/release/gates.json" \
    "$FW/release/validation.json" "$FW/files/$STEM.manifest.json"   # data first, the manifest last
  endgroup

  group "persist the ledger of $TAG ($name)"
  LEDGER=$LEDGERS/$ID8
  next="$LEDGER.next"
  rm -rf "$next"; mkdir -p "$next"
  # The base's ledger, in the epoch the delta kept: the next delta is planned against it.
  cp "$FW"/release/ledger-v"$VERSION".* "$FW"/release/pairs-v"$VERSION".bin "$next/"   # keys, pairs, manifest
  # Windowed ledgers also retain each parent's occurrence metadata for the next delta.
  # The anchor family's older ledger remains byte-compatible and has no such file.
  if [ -f "$FW/release/occurrences-v$VERSION.bin" ]; then
    cp "$FW/release/occurrences-v$VERSION.bin" "$next/"
  fi
  if [[ " ${DELTA_PREFIXES:-} " == *" $prefix "* ]]; then
    # The base the next delta is validated on, installed first, as a device holds it
    mkdir "$next/release"
    mv "$FW/release/segment.oxv" "$next/release/"; cp "$FW/release/release.json" "$next/release/"
  fi
  if [ -n "$DELTA" ]; then
    # The chain a device that installs this delta holds, for the next delta's validation: the
    # chain this one extended, or the base it was made from; it counts only once the delta is published.
    pending=$next/chain.pending
    if [ "${#CHAIN[@]}" -gt 0 ] && [ "${#CHAIN[@]}" -lt "$MAX_CHAIN" ]; then
      mv "$PREV/chain" "$pending"; steps=${#CHAIN[@]}
    else
      mkdir "$pending"; mv "$PREV_RELEASE" "$pending/base"; steps=1
    fi
    mkdir "$pending/delta-$steps"
    mv "$FW/delta/segment.oxv" "$pending/delta-$steps/"; cp "$FW/delta/release.json" "$pending/delta-$steps/"
  fi
  jq -n --arg tag "$TAG" --arg release "$RELEASE_TAG" --arg manifest "$STEM.manifest.json" --arg sha "$MANIFEST_SHA" \
    --arg family "$name" --arg codec "${codec:-$DEFAULT_CODEC}" --arg package "$PASSAGE_PACKAGE_CHECKSUM" \
    --arg identity "$PLAN_IDENTITY" --argjson to "$VERSION" --arg threshold "$threshold" \
    --arg epoch "$(jq -r '.codec.params_sha256 // empty' "$FW/release/ledger-v$VERSION.manifest.json")" \
    '{published: $release, library_release_tag: $tag, manifest: $manifest, manifest_sha256: $sha, family: $family,
      to: $to, base_tag: $release, codec: $codec, alias_threshold: $threshold, epoch_sha256: $epoch,
      passage_package: $package, plan_identity_sha256: $identity, kind: "base", from: null, deltas_since_base: 0}' > "$next/published.json"
  rm -rf "$LEDGER.prev"; [ ! -d "$LEDGER" ] || mv "$LEDGER" "$LEDGER.prev"
  mv "$next" "$LEDGER"; rm -rf "$LEDGER.prev"
  prune_family_ledgers "$prefix" "$LEDGER"
  if [ -n "$ROTATION_FITTED" ] && [ "$ROTATION" != "$ROTATION_KEPT" ]; then
    mkdir -p "${ROTATION_KEPT%/*}"; cp "$ROTATION" "$ROTATION_KEPT.next"; mv "$ROTATION_KEPT.next" "$ROTATION_KEPT"
    echo "rotation: kept in $ROTATION_KEPT, epoch $(sha256sum < "$ROTATION_KEPT" | cut -d' ' -f1)"
  fi
  echo "published $RELEASE_TAG; ledger v$VERSION persisted in $LEDGER"
  endgroup

  if [[ " ${DELTA_PREFIXES:-} " == *" $prefix "* ]]; then
    group "back the ledger of $TAG ($name) up in $LEDGER_BACKUPS-$TAG"
    # Not an application's asset: named outside otzaria-vectors-*, in a prerelease of its own.
    backup="$FW/files/$prefix.ledger-$ID8-v$VERSION.tar"
    tar -C "$LEDGERS" -cf "$backup" --exclude="$ID8/release" --exclude="$ID8/chain" --exclude="$ID8/chain.pending" "$ID8"
    "$ZSTD" -q -T0 -10 "$backup" -o "$backup.zst"
    uploads=("$backup.zst")
    if [ -f "$ROTATION_KEPT" ]; then
      # The rotation fitted on this warehouse, to restore, or to commit as rotation/<family>.bin and pin in ROTATIONS
      rotation_sha=$(sha256sum < "$ROTATION_KEPT" | cut -d' ' -f1)
      cp "$ROTATION_KEPT" "$FW/files/$prefix.rotation-$ROTATION_KEY-${rotation_sha:0:16}.bin"
      uploads+=("$FW/files/$prefix.rotation-$ROTATION_KEY-${rotation_sha:0:16}.bin")
    fi
    if ! "$GH" release view "$LEDGER_BACKUPS-$TAG" --repo "$REPO" >/dev/null 2>&1; then
      echo "The ledgers the build machine plans the next deltas against, after $TAG. Not for applications." > "$FW/ledger-notes.md"
      "$GH" release create "$LEDGER_BACKUPS-$TAG" --repo "$REPO" --target "$TARGET" --prerelease=true --latest=false \
        --title "Library vectors ledger $TAG" --notes-file "$FW/ledger-notes.md" \
        || die "could not create $LEDGER_BACKUPS-$TAG; $RELEASE_TAG stays published and the ledger is persisted here"
    fi
    for f in "${uploads[@]}"; do
      "$GH" release upload "$LEDGER_BACKUPS-$TAG" "$f" --repo "$REPO" --clobber \
        || die "could not back the ledger up in $LEDGER_BACKUPS-$TAG; $RELEASE_TAG stays published and the ledger is persisted here"
    done
    endgroup
  fi

  if [ -n "$DELTA" ]; then
    group "7 publish $prefix-delta-$TAG"
    {
      echo "Semantic vectors for library release $TAG: a delta from v$PV, on the vectors of $PREV_TAG."
      echo
      echo "Family: \`$name\`, chunking identity \`$(jq -r .chunking_identity "$FAMILY")\`."
      echo
      echo "Manifest: \`$DELTA_STEM.manifest.json\`, SHA-256 \`$DELTA_SHA\` — check it before trusting the files it lists."
      echo
      echo "Gates: \`gates.json\` (assemble --verify) and \`validation.json\` (validate_semantic_vectors on the release index, the delta installed after the base of $PREV_TAG: G3, G4, G6)."
      [ ! -f "$FW/delta/validation-chain.json" ] \
        || echo "\`validation-chain.json\`: the same, installed at the end of a chain of $((${#CHAIN[@]} + 1)) releases from the base of v$CHAIN_FROM."
    } > "$FW/delta-notes.md"
    delta_files=("${DELTA_DATA[@]}" "$FW/delta/gates.json" "$FW/delta/validation.json")
    [ ! -f "$FW/delta/validation-chain.json" ] || delta_files+=("$FW/delta/validation-chain.json")
    # Always a prerelease: a delta is never a release of the library's history. The base is
    # published and the chain persisted already, so a delta that fails to publish costs devices
    # a base download, and nothing else.
    set +e
    ( set -e; PUBLISH_LEVEL=warning
      publish "$prefix-delta-$TAG" "Library vectors delta $TAG" "$FW/delta-notes.md" true "${delta_files[@]}" \
        "$FW/files/$DELTA_STEM.manifest.json" )
    published=$?
    set -e
    if [ "$published" -eq 0 ]; then
      mv "$LEDGER/chain.pending" "$LEDGER/chain"
      jq --arg tag "$prefix-delta-$TAG" --arg manifest "$DELTA_STEM.manifest.json" --arg sha "$DELTA_SHA" --argjson from "$PV" \
        --slurpfile ledger "$FW/delta/ledger-v$VERSION.manifest.json" \
        '. + {kind: "delta", from: $from, delta_tag: $tag, delta_manifest: $manifest, delta_manifest_sha256: $sha,
              deltas_since_base: $ledger[0].deltas_since_base}' "$LEDGER/published.json" > "$LEDGER/published.json.next"
      mv "$LEDGER/published.json.next" "$LEDGER/published.json"
      echo "published $prefix-delta-$TAG"
    else
      rm -rf "$LEDGER/chain.pending"
      echo "::warning::$prefix-delta-$TAG was not published; $RELEASE_TAG is, and the next delta starts from it"
      "$GH" release delete "$prefix-delta-$TAG" --repo "$REPO" --yes >/dev/null 2>&1 \
        || echo "::warning::the draft $prefix-delta-$TAG could not be deleted: delete it by hand"
    fi
    endgroup
  fi
  rm -rf "$FW"
}

PUBLISHED=()
for i in "${!NAMES[@]}"; do
  set +e
  ( set -e; build_family "${NAMES[$i]}" "${PREFIXES[$i]}" "${CODECS[$i]}" "${THRESHOLDS[$i]}" )
  rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    [ "${#PUBLISHED[@]}" -eq 0 ] || echo "::error::the family ${NAMES[$i]} failed; ${PUBLISHED[*]} stay published — retry it alone with --family ${NAMES[$i]}"
    exit "$rc"
  fi
  [ "$MODE" = dry-run ] || PUBLISHED+=("${PREFIXES[$i]}-$TAG")
done
if [ "$MODE" = dry-run ]; then
  echo "work directory kept: $WORK"
else
  rm -rf "$WORK"
fi
