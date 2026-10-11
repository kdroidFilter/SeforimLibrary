#!/usr/bin/env bash
#
# Full from-scratch local build of the v5 Zayit bundle, on a machine WITH a GPU.
#
# Mirrors the manual-generate-release.yml CI pipeline (id-stable seed + delta fan),
# but adds the dense step: the corpus is embedded locally on the GPU AGAINST the
# freshly-built seforim.db, so the dense vectors always match the shipped corpus
# (no precomputed-vector / line-id drift risk). Produces a fused (text + v5
# vectors) Lucene index and a split .tar.zst bundle that carries the v5 model.
#
# Uses the WHOLE machine by default: DB in RAM, every core, heap sized to RAM.
#
# Prerequisites:
#   - a CUDA GPU + the SeforimEmbedding toolkit checked out (python embedder + venv)
#   - gh authenticated (models pulled from the private releases v5-fp32 / v5-int8;
#     prior-release buildstate + bundles pulled for id-stability + deltas)
#
# Usage:
#   bash scripts/build_v5_local.sh        # interactive: asks for each override + choices
#   (env vars below pre-fill the answers; a non-tty run skips the prompts and uses them)
#
# Env knobs:
#   GENERATOR_HEAP (default ~80% RAM)  INDEX_THREADS (default all cores)
#   IN_MEMORY (default true)           PATCH_OFFSETS (default "1 2 4 8 16")
#   INCLUDE_UNOFFICIAL=1               also merge Open-Otzarya-Projects/Otzarya-Unofficial-Books
#   PUBLISH / DEEP_CLEAN / SKIP_CLEAN / NO_SEED / NO_DELTAS
#
set -euo pipefail

LIB="/home/elie-gambache/IdeaProjects/SeforimApp/SeforimLibrary"
EMB="/home/elie-gambache/IdeaProjects/SeforimEmbedding"
VENV="$HOME/.venvs/seforim-embed"
DB="$LIB/build/seforim.db"
VEC="$EMB/artifacts/vector_index_v5"
EMB_MODEL_DIR="$EMB/artifacts/model_v5b_final"
PKG_DIR="$LIB/generator/packaging/build/package"
REPO="kdroidFilter/SeforimLibrary"

CORES="$(nproc)"
TOTAL_RAM_GB="$(free -g | awk '/^Mem:/{print $2}')"
HEAP="${GENERATOR_HEAP:-$(( TOTAL_RAM_GB > 16 ? TOTAL_RAM_GB * 80 / 100 : 8 ))g}"
THREADS="${INDEX_THREADS:-$CORES}"
INMEM="${IN_MEMORY:-true}"
# parallelise independent tasks; disable the configuration cache — it caches stale
# JavaExec classpaths after a recompile (ClassNotFoundException on generator tasks).
GRADLE_PAR="--parallel --max-workers=$CORES --no-configuration-cache"
PATCH_OFFSETS="${PATCH_OFFSETS:-1 2 4 8 16}"
ZSTD_LEVEL="${ZSTD_LEVEL:-22}"          # zstd compression level for the bundle + deltas (1-22)
PUBLISH="${PUBLISH:-0}"
DEEP_CLEAN="${DEEP_CLEAN:-0}"
SKIP_CLEAN="${SKIP_CLEAN:-0}"
NO_DELTAS="${NO_DELTAS:-0}"
NO_SEED="${NO_SEED:-0}"
# Optional LOCAL deps — when set, the corresponding download is skipped.
# (positional arg 1 = lexical.db shortcut)
#   LEXICAL_DB       -> staged at build/lexical.db
#   SEFARIA_EXPORT   -> staged at build/sefaria/export (extracted dir, or a .tar.zst)
#   ACRONYMIZER_DB   -> staged at build/acronymizer/acronymizer.db
#   OTZARIA_DIR      -> passed as -PsourceDir (the Otzaria library root)
# Not overridable locally: the small ForDB rules zip (always fetched; no disk hook).
LEXICAL_DB="${LEXICAL_DB:-${1:-}}"
SEFARIA_EXPORT="${SEFARIA_EXPORT:-}"
ACRONYMIZER_DB="${ACRONYMIZER_DB:-}"
OTZARIA_DIR="${OTZARIA_DIR:-}"
# INCLUDE_UNOFFICIAL=1 -> download the OFFICIAL Otzaria, clone Otzarya-Unofficial-Books
# and merge its ספרים/ categories into אוצריא/ (no source-code change needed).
INCLUDE_UNOFFICIAL="${INCLUDE_UNOFFICIAL:-0}"
UNOFFICIAL_REPO="Open-Otzarya-Projects/Otzarya-Unofficial-Books"   # owner/repo for `gh api`

log() { echo -e "\n\033[1;36m[$(date +%H:%M:%S)] $*\033[0m"; }
die() { echo -e "\n\033[1;31mFATAL: $*\033[0m" >&2; exit 1; }

# Interactive prompt for a local-override path; empty answer = download.
ask_path() { # $1=label  $2=default  $3=kind(file|dir|any)
  local label="$1" def="$2" kind="$3" ans
  while true; do
    printf '  %s [%s]: ' "$label" "${def:-↓ télécharger}" >&2
    IFS= read -r ans </dev/tty || ans=""
    ans="${ans:-$def}"; ans="${ans/#\~/$HOME}"
    [ -z "$ans" ] && { printf ''; return 0; }
    case "$kind" in
      file) [ -f "$ans" ] || { printf '    ✗ fichier introuvable\n' >&2; continue; } ;;
      dir)  [ -d "$ans" ] || { printf '    ✗ dossier introuvable\n' >&2; continue; } ;;
      any)  [ -e "$ans" ] || { printf '    ✗ introuvable\n' >&2; continue; } ;;
    esac
    printf '%s' "$ans"; return 0
  done
}
ask_yn() { # $1=label  $2=default(y/n)  -> returns 0 for yes
  local ans; printf '  %s [%s]: ' "$1" "$([ "$2" = y ] && echo Y/n || echo y/N)" >&2
  IFS= read -r ans </dev/tty || ans=""; ans="${ans:-$2}"; [[ "$ans" =~ ^[yYoO] ]]
}
ask_num() { # $1=label  $2=default  $3=min  $4=max
  local label="$1" def="$2" lo="$3" hi="$4" ans
  while true; do
    printf '  %s [%s]: ' "$label" "$def" >&2
    IFS= read -r ans </dev/tty || ans=""; ans="${ans:-$def}"
    [[ "$ans" =~ ^[0-9]+$ ]] && [ "$ans" -ge "$lo" ] && [ "$ans" -le "$hi" ] && { printf '%s' "$ans"; return 0; }
    printf '    ✗ entier %s-%s\n' "$lo" "$hi" >&2
  done
}

[ -f "$LIB/gradlew" ] || die "gradlew not found at $LIB"
[ -d "$EMB" ]  || die "SeforimEmbedding toolkit required at $EMB"
[ -d "$VENV" ] || die "python venv not found at $VENV"
for t in gh jq sqlite3 zstd; do command -v "$t" >/dev/null || die "$t not installed"; done
export GITHUB_TOKEN="${GITHUB_TOKEN:-$(gh auth token 2>/dev/null || true)}"
export GH_TOKEN="$GITHUB_TOKEN"
[ -n "$GITHUB_TOKEN" ] || die "no GitHub token — run 'gh auth login'"

# ── Interactive CLI ───────────────────────────────────────────────────────────
# Run from a terminal -> ask each override (empty = download) + a few choices.
# Env vars pre-fill the answers; a non-tty run (cron/pipe) skips prompts and uses them.
if [ -t 0 ]; then
  echo "──────── Build v5 — configuration (Entrée vide = télécharger) ────────"
  LEXICAL_DB=$(ask_path     "lexical.db locale"                          "$LEXICAL_DB"     file)
  SEFARIA_EXPORT=$(ask_path "export Sefaria (dossier extrait ou .tar.zst)" "$SEFARIA_EXPORT" any)
  ACRONYMIZER_DB=$(ask_path "acronymizer.db locale"                      "$ACRONYMIZER_DB" file)
  ask_yn "Inclure les livres NON-officiels (Otzarya-Unofficial-Books) ?" "$([ "$INCLUDE_UNOFFICIAL" = 1 ] && echo y || echo n)" && INCLUDE_UNOFFICIAL=1 || INCLUDE_UNOFFICIAL=0
  if [ "$INCLUDE_UNOFFICIAL" = 1 ]; then
    OTZARIA_DIR=""   # the script downloads the official Otzaria and merges the unofficial books in
  else
    OTZARIA_DIR=$(ask_path  "dossier source Otzaria"                     "$OTZARIA_DIR"    dir)
  fi
  ask_yn "Réutiliser les sources déjà téléchargées (non = from-scratch) ?" "$([ "$SKIP_CLEAN" = 1 ] && echo y || echo n)" && SKIP_CLEAN=1 || SKIP_CLEAN=0
  ask_yn "Produire le fan de deltas ?"                                     "$([ "$NO_DELTAS" = 1 ] && echo n || echo y)"  && NO_DELTAS=0 || NO_DELTAS=1
  ask_yn "Publier une pré-release GitHub à la fin ?"                       "$([ "$PUBLISH" = 1 ] && echo y || echo n)"    && PUBLISH=1 || PUBLISH=0
  ZSTD_LEVEL=$(ask_num "Niveau de compression zstd (1=rapide … 22=ultra/lent)" "$ZSTD_LEVEL" 1 22)
  echo "──────────────────────────────────────────────────────────────────────"
  printf "  lexical:     %s\n" "${LEXICAL_DB:-↓ download}"
  printf "  sefaria:     %s\n" "${SEFARIA_EXPORT:-↓ download}"
  printf "  otzaria:     %s\n" "$([ "$INCLUDE_UNOFFICIAL" = 1 ] && echo '↓ officiel + livres NON-officiels (merge)' || echo "${OTZARIA_DIR:-↓ download}")"
  printf "  acronymizer: %s\n" "${ACRONYMIZER_DB:-↓ download}"
  printf "  ids: %s\n" "$([ "$NO_SEED" = 1 ] && echo 'FRAÎCHES (non seedées ⚠)' || echo 'stables (seed auto depuis la dernière release)')"
  printf "  clean:%s · deltas:%s · publish:%s · zstd:%s · ram:%sg heap:%s cores:%s\n" \
    "$([ "$SKIP_CLEAN" = 1 ] && echo reuse || echo from-scratch)" \
    "$([ "$NO_DELTAS" = 1 ] && echo no || echo yes)" \
    "$([ "$PUBLISH" = 1 ] && echo yes || echo no)" "$ZSTD_LEVEL" "$TOTAL_RAM_GB" "$HEAP" "$CORES"
  ask_yn "Lancer le build ?" y || { echo "annulé."; exit 0; }
fi

[ -z "$LEXICAL_DB" ] || [ -f "$LEXICAL_DB" ] || die "lexical db not found: $LEXICAL_DB"
[ -z "$OTZARIA_DIR" ] || [ -d "$OTZARIA_DIR" ] || die "Otzaria source dir not found: $OTZARIA_DIR"
# read by PackageArtifacts + the patch compressor (Gradle JavaExec inherits the env)
export ZSTD_LEVEL

cd "$LIB"
chmod +x gradlew

# ── 0. Discover the release lineage (NON-draft v<N>- tags that carry a
#       buildstate are the real lineage; my manual drafts are ignored) ──────────
log "STEP 0/9 — discover release lineage"
PRIOR_TSV="$LIB/build-meta/prior.tsv"   # ver<TAB>tag, desc
mkdir -p "$(dirname "$PRIOR_TSV")"
gh release list -R "$REPO" --limit 100 --json tagName,isDraft \
  | jq -r '.[] | select(.isDraft|not) | .tagName
           | select(test("^v[0-9]+-"))
           | (capture("^v(?<n>[0-9]+)-").n) + "\t" + .' \
  | sort -k1,1nr > "$PRIOR_TSV" || true
if [ -s "$PRIOR_TSV" ]; then
  LAST_VER=$(head -n1 "$PRIOR_TSV" | cut -f1)
  SEED_TAG=$(head -n1 "$PRIOR_TSV" | cut -f2)
  NEXT_VER=$(( LAST_VER + 1 )); HAS_PREV=1
  log "latest lineage release: v$LAST_VER ($SEED_TAG) -> this build: v$NEXT_VER"
else
  NEXT_VER=1; HAS_PREV=0; SEED_TAG=""
  log "no prior lineage release — first build (v1), no seed / no deltas"
fi

# ── 1. Clean ─────────────────────────────────────────────────────────────────
if [ "${SKIP_CLEAN:-0}" = "1" ]; then
  log "STEP 1/9 — light clean (keep downloaded sources)"
  rm -rf "$LIB/build/seforim.db"* "$LIB/build"/*.lucene "$VEC"
else
  log "STEP 1/9 — clean (from scratch)"
  ./gradlew clean --no-daemon -q || true
  rm -rf "$LIB/build" "$LIB"/generator/*/build "$LIB/search/build" "$VEC"
  [ "$DEEP_CLEAN" = "1" ] && { log "DEEP_CLEAN — wiping ~/.gradle/caches"; rm -rf "$HOME/.gradle/caches"; }
fi
mkdir -p "$LIB/build"

# Local lexical.db: drop it where the pipeline expects it so downloadLexicalDb
# finds it present and skips the network fetch ("Using existing lexical.db").
if [ -n "$LEXICAL_DB" ]; then
  log "using LOCAL lexical.db: $LEXICAL_DB (download skipped)"
  cp -f "$LEXICAL_DB" "$LIB/build/lexical.db"
fi

# Local Sefaria export: staged at build/sefaria/export, which the fetcher reuses
# if it contains data (so the big .tar.zst download is skipped). Accept either an
# already-extracted export dir (symlinked, no multi-GB copy) or a .tar.zst archive.
if [ -n "$SEFARIA_EXPORT" ]; then
  EXP="$LIB/build/sefaria/export"
  mkdir -p "$LIB/build/sefaria"; rm -rf "$EXP"
  if [ -d "$SEFARIA_EXPORT" ]; then
    log "using LOCAL Sefaria export dir: $SEFARIA_EXPORT (download skipped)"
    ln -sfn "$(readlink -f "$SEFARIA_EXPORT")" "$EXP"
  elif [ -f "$SEFARIA_EXPORT" ]; then
    log "extracting LOCAL Sefaria export archive: $SEFARIA_EXPORT (download skipped)"
    mkdir -p "$EXP"; tar --use-compress-program=unzstd -xf "$SEFARIA_EXPORT" -C "$EXP"
  else
    die "SEFARIA_EXPORT not found: $SEFARIA_EXPORT"
  fi
fi

# Local acronymizer DB: staged where AcronymizerFetcher reuses it (skip download).
if [ -n "$ACRONYMIZER_DB" ]; then
  [ -f "$ACRONYMIZER_DB" ] || die "ACRONYMIZER_DB not found: $ACRONYMIZER_DB"
  log "using LOCAL acronymizer.db: $ACRONYMIZER_DB (download skipped)"
  mkdir -p "$LIB/build/acronymizer"; cp -f "$ACRONYMIZER_DB" "$LIB/build/acronymizer/acronymizer.db"
fi

# Optional: merge the unofficial Otzarya books into the official Otzaria source.
# Downloads the official library, clones Otzarya-Unofficial-Books, and copies its
# ספרים/<category> trees into <official>/אוצריא/ — then points -PsourceDir at the
# merged tree. No project source-code change required.
if [ "$INCLUDE_UNOFFICIAL" = "1" ]; then
  log "STEP 1b/9 — official Otzaria + merge Otzarya-Unofficial-Books"
  ./gradlew :otzariasqlite:downloadOtzaria --no-daemon --stacktrace
  # downloadOtzaria extracts to the module's build dir (CWD = the otzariasqlite project).
  # The source root holds metadata.json + אוצריא directly (or one level down). Check
  # those directly — no `find` (which choked under pipefail on the huge אוצריא tree).
  OTZ_ROOT=""
  for base in "$LIB/generator/otzariasqlite/build/otzaria/source" "$LIB/build/otzaria/source"; do
    [ -d "$base" ] || continue
    if [ -f "$base/metadata.json" ] && [ -d "$base/אוצריא" ]; then OTZ_ROOT="$base"; break; fi
    for sub in "$base"/*/; do
      if [ -f "${sub}metadata.json" ] && [ -d "${sub}אוצריא" ]; then OTZ_ROOT="${sub%/}"; break 2; fi
    done
  done
  [ -n "$OTZ_ROOT" ] || die "official Otzaria root (metadata.json + אוצריא) not found"
  UNOFF="$LIB/build/otzaria/unofficial"
  rm -rf "$UNOFF"; mkdir -p "$UNOFF"
  # Tarball, NOT git clone: the repo's LFS smudge filter aborts a normal checkout
  # mid-way (only the first category lands). A tarball pulls the whole tree (PDFs
  # come as harmless LFS pointer files, which the Otzaria parser ignores).
  log "downloading $UNOFFICIAL_REPO (tarball)"
  ok=0
  for attempt in 1 2 3; do
    if gh api "repos/$UNOFFICIAL_REPO/tarball/main" > "$UNOFF/repo.tgz" && [ -s "$UNOFF/repo.tgz" ]; then ok=1; break; fi
    log "  tarball download attempt $attempt failed, retrying…"; sleep 5
  done
  [ "$ok" = 1 ] || die "could not download the unofficial repo tarball"
  tar -xzf "$UNOFF/repo.tgz" -C "$UNOFF"
  UNOFF_SRC=$(find "$UNOFF" -maxdepth 1 -mindepth 1 -type d | head -1)
  [ -n "$UNOFF_SRC" ] && [ -d "$UNOFF_SRC/ספרים" ] || die "ספרים/ not found in the unofficial tarball"
  # Merge file-by-file (keeping the category structure), but DEDUPE BY TITLE:
  # skip any unofficial book whose title (filename) already exists. Official books
  # win, and every title stays unique — so seedGenerations' exact-title match never
  # becomes ambiguous (it aborts by design on a title matching 2 books, e.g. 'רדך').
  # The "not adapted to Otzaria" category is skipped entirely (would not parse).
  declare -A SEEN_TITLE=()
  while IFS= read -r f; do SEEN_TITLE["${f%.*}"]=1; done < <(find "$OTZ_ROOT/אוצריא" -type f -printf '%f\n')
  copied=0; skipped=0
  while IFS= read -r -d '' f; do
    rel="${f#"$UNOFF_SRC"/ספרים/}"
    title="$(basename "$f")"; title="${title%.*}"
    if [ -n "${SEEN_TITLE[$title]:-}" ]; then skipped=$((skipped + 1)); continue; fi
    SEEN_TITLE["$title"]=1
    dest="$OTZ_ROOT/אוצריא/$rel"
    mkdir -p "$(dirname "$dest")"
    cp -a "$f" "$dest"; copied=$((copied + 1))
  done < <(find "$UNOFF_SRC/ספרים" -type f -not -path "*ספרים שאינם מותאמים לאוצריא*" -print0)
  rm -rf "$UNOFF"
  OTZARIA_DIR="$OTZ_ROOT"   # feed the merged tree to the generator via -PsourceDir
  log "merged $copied unofficial books (skipped $skipped duplicate-title) -> $OTZARIA_DIR"
fi

# ── 2. Seed the IdAllocator from the previous release so unchanged content keeps
#       the SAME line ids (deltas stay small; lineage stays coherent) ───────────
if [ "$HAS_PREV" = "1" ] && [ "${NO_SEED:-0}" != "1" ]; then
  log "STEP 2/9 — seed buildstate from $SEED_TAG (id stability)"
  if gh release download "$SEED_TAG" -R "$REPO" --pattern 'seforim.db.buildstate' --dir "$LIB/build" --clobber; then
    ls -lh "$LIB/build/seforim.db.buildstate"
  else
    log "WARN: $SEED_TAG has no buildstate — building with fresh ids (deltas will be large)"
  fi
else
  log "STEP 2/9 — no seed (first build or NO_SEED=1)"
fi

# ── 3. Build seforim.db — skip the text-only index; the FUSED one is built in
#       step 5 once the vectors exist (avoids a double index build). ────────────
log "STEP 3/9 — generate seforim.db (full source pipeline, no index yet)"
SRC_ARG=()
if [ -n "$OTZARIA_DIR" ]; then
  log "using LOCAL Otzaria source: $OTZARIA_DIR (download skipped)"
  SRC_ARG=(-PsourceDir="$OTZARIA_DIR")
fi
./gradlew generateSeforimDb \
  -PreleaseName="v${NEXT_VER}-local" -PdbVersion="$NEXT_VER" \
  -PinMemoryDb="$INMEM" -PgeneratorHeap="$HEAP" "${SRC_ARG[@]}" \
  -x :searchindex:buildLuceneIndexDefault \
  --no-daemon $GRADLE_PAR --stacktrace
[ -s "$DB" ] || die "seforim.db was not produced"
DB_LINES=$(sqlite3 "$DB" "SELECT COUNT(*) FROM line WHERE charCount BETWEEN 40 AND 600;")
log "DB ready — $DB_LINES indexable lines"

# ── 4. Embed the corpus on the GPU, keyed by the line ids of THIS db ────────────
log "STEP 4/9 — download embedding model (v5-fp32) + embed corpus on GPU"
# shellcheck disable=SC1091
source "$VENV/bin/activate"
python "$EMB/scripts/download_model.py" || die "could not download the v5-fp32 model"
[ -f "$EMB_MODEL_DIR/model.safetensors" ] || die "embedding model missing after download"
SEFORIM_DB="$DB" OUT_DIR="$VEC" MODEL_DIR="$EMB_MODEL_DIR" python "$EMB/scripts/embed_corpus_bin.py"
[ -s "$VEC/vecs.f32" ] || die "embedding produced no vectors"
VEC_N=$(cut -d' ' -f1 "$VEC/meta.txt")
log "vectors: $VEC_N   (db indexable lines: $DB_LINES)"
[ "$VEC_N" -ge $(( DB_LINES * 95 / 100 )) ] || die "vectors ($VEC_N) << db lines ($DB_LINES) — mismatch"

# ── 5. Build the FUSED Lucene index (text + dense v5 vectors) ───────────────────
log "STEP 5/9 — build fused Lucene index (text + v5 vectors)"
./gradlew :searchindex:buildLuceneIndexDefault \
  -PseforimDb="$DB" -PinMemoryDb="$INMEM" -PvectorsBin="$VEC" \
  -PindexThreads="$THREADS" -PgeneratorHeap="$HEAP" \
  --no-daemon $GRADLE_PAR --stacktrace
[ -d "$DB.lucene" ] || die "fused index not produced"

# ── 6. Fetch the runtime model (v5-int8 + tokenizer) next to the DB ─────────────
log "STEP 6/9 — download runtime model (v5-int8) next to the DB"
./gradlew :packaging:downloadEmbedModel -PseforimDb="$DB" --no-daemon --stacktrace
[ -f "$LIB/build/seforim-embed-v5-int8.onnx" ] || die "runtime model not fetched next to the DB"

# ── 7. Package the split .tar.zst bundle ────────────────────────────────────────
log "STEP 7/9 — package bundle (.tar.zst, split)"
./gradlew :packaging:packageArtifacts -PseforimDb="$DB" --no-daemon $GRADLE_PAR --stacktrace
shopt -s nullglob
PARTS=("$PKG_DIR"/seforim_bundle.tar.zst.part*)
[ ${#PARTS[@]} -gt 0 ] || die "no bundle parts produced"
log "bundle parts:"; ls -lh "${PARTS[@]}"

# ── 8. Produce the delta fan against prior releases {offsets} ───────────────────
# NOTE: deltas carry the DB + TEXT index only. The dense vectors and the embedding
# model are NOT in a delta (the Lucene vector sink is a no-op client-side), so a
# client applying a delta keeps its existing vectors/model. The v4->v5 model jump
# is therefore a FULL-bundle event; deltas only ship same-generation content updates.
PATCH_STAGE="$LIB/build/patches"
if [ "$HAS_PREV" = "1" ] && [ "${NO_DELTAS:-0}" != "1" ]; then
  log "STEP 8/9 — produce + verify delta fan (offsets: $PATCH_OFFSETS)"
  mkdir -p "$PATCH_STAGE"
  for OFFSET in $PATCH_OFFSETS; do
    TARGET_VER=$(( NEXT_VER - OFFSET ))
    [ "$TARGET_VER" -ge 1 ] || { log "offset $OFFSET -> v$TARGET_VER (<1), skip"; continue; }
    TAG=$(awk -F'\t' -v v="$TARGET_VER" '$1==v{print $2; exit}' "$PRIOR_TSV" || true)
    [ -n "$TAG" ] || { log "no release at v$TARGET_VER (offset $OFFSET), skip"; continue; }
    log "patch v$TARGET_VER -> v$NEXT_VER (offset $OFFSET, $TAG)"
    PD="$LIB/build/prev-$TARGET_VER"; rm -rf "$PD"; mkdir -p "$PD/extract"
    gh release download "$TAG" -R "$REPO" --pattern 'seforim_bundle.tar.zst.part*' --dir "$PD" --clobber
    cat "$PD"/seforim_bundle.tar.zst.part* > "$PD/bundle.tar.zst"
    tar --use-compress-program=unzstd -xf "$PD/bundle.tar.zst" -C "$PD/extract"
    PREV_DB=$(find "$PD/extract" -name seforim.db -type f | head -n1)
    [ -s "$PREV_DB" ] || { log "WARN: no seforim.db in $TAG bundle, skip"; rm -rf "$PD"; continue; }
    ./gradlew :generator-common:producePatchAndVerify \
      -PprevDb="$PREV_DB" -PnewDb="$DB" \
      -Pout="$PATCH_STAGE/patch-v${TARGET_VER}-v${NEXT_VER}.db" \
      -PcatalogPb="$LIB/build/catalog.pb" \
      -PfromVersion="$TARGET_VER" -PtoVersion="$NEXT_VER" \
      --no-daemon $GRADLE_PAR --stacktrace
    rm -rf "$PD" "$PATCH_STAGE/patch-v${TARGET_VER}-v${NEXT_VER}.db" \
           "$PATCH_STAGE/verify-patch-v${TARGET_VER}-v${NEXT_VER}.db"
    df -h / | tail -1
  done
  log "delta artefacts:"; ls -lh "$PATCH_STAGE" 2>/dev/null || true
else
  log "STEP 8/9 — no deltas (first build or NO_DELTAS=1)"
fi

# ── 9. (optional) publish a GitHub pre-release ──────────────────────────────────
if [ "$PUBLISH" != "1" ]; then
  log "DONE — build complete. Bundle: $PKG_DIR  (set PUBLISH=1 to publish)"
  exit 0
fi
log "STEP 9/9 — publish pre-release"
TAG="v${NEXT_VER}-$(date -u +%Y%m%d%H%M%S)"
STAGE="$LIB/build/release-staging"; rm -rf "$STAGE"; mkdir -p "$STAGE"
cp "${PARTS[@]}" "$STAGE/"
[ -f "$LIB/build/catalog.pb" ]            && cp "$LIB/build/catalog.pb" "$STAGE/"
[ -f "$LIB/build/seforim.db.buildstate" ] && cp "$LIB/build/seforim.db.buildstate" "$STAGE/"
[ -f "$LIB/build/release_info.txt" ]      && cp "$LIB/build/release_info.txt" "$STAGE/"
if compgen -G "$PATCH_STAGE/patch-*.db.zst" > /dev/null; then
  cp "$PATCH_STAGE"/patch-*.db.zst "$PATCH_STAGE"/patch-*.db.zst.manifest.json "$STAGE/" 2>/dev/null || true
fi
find "$STAGE" -maxdepth 1 -type f -size +2147483647c | grep -q . && die "asset exceeds GitHub's 2 GiB cap"
log "publishing $TAG (db_version $NEXT_VER)"
gh release create "$TAG" -R "$REPO" --prerelease --title "$TAG" \
  --notes "Full bundle: seforim.db + fused Lucene index (text + v5 dense vectors) + v5 model + tokenizer, built from scratch on GPU. Seeded from $SEED_TAG for id stability. Delta fan included (DB+text only; dense/model require the full bundle). db_version=$NEXT_VER." \
  "$STAGE"/*
log "DONE — published $TAG"
