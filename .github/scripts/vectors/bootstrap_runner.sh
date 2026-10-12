#!/usr/bin/env bash
# Prepare the build machine for build_library_vectors.sh, once and idempotently.
#
#   bootstrap_runner.sh [--state DIR] [--seed-model-dir DIR]
#
# Into the persistent state directory (default /home/runner/otzaria-vectors, on ext4):
#   bin/otzaria-semantic-search (+ .rev, .build)   the sidecar CLI at SIDECAR_REV, a release build
#                                          with onnx-backend, so its embed-shard embeds on the CPU
#                                          (ONNX Runtime is the venv's libonnxruntime, loaded at run time)
#   bin/export_semantic_plan (+ .rev, .build)   the plugin's planner at PLUGIN_REV
#   bin/validate_semantic_vectors (+ .rev, .build)   the plugin's validator at PLUGIN_REV, with
#                                          --features semantic: G6 runs the int8 query model
#   bin/families/<name>/{model,chunking}.json   each family's identity and recipe at SIDECAR_REV
#   venv/                                  the worker's Python: PyTorch ROCm, ONNX Runtime, tokenizers
#   model-cache/<checksum>/                seeded from --seed-model-dir (checksum-verified), or
#                                          fetched by the worker on its first run
#   warehouse/                             its package's warehouse is created by the first
#                                          build, which plans and embeds every text
# Builds happen under <state>/build, which is removed afterwards.
#
# Prerequisites, which this script does not install: cargo, from rustup installed for the user
# that runs it (curl --proto '=https' -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal),
# python3 with venv, and network access to GitHub, crates.io and PyPI. pip is handed the system
# CA bundle (PIP_CERT, unless set): behind TLS inspection, as NetFree is on the build machine,
# its root is in the system bundle and not in pip's own.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=pins.env
. "$HERE/pins.env"
STATE=/home/runner/otzaria-vectors
SEED=""
while [ $# -gt 0 ]; do
  case "$1" in
    --state) STATE=$2; shift 2 ;;
    --seed-model-dir) SEED=$2; shift 2 ;;
    *) echo "usage: bootstrap_runner.sh [--state DIR] [--seed-model-dir DIR]" >&2; exit 64 ;;
  esac
done
SYSTEM_CA=${SYSTEM_CA:-/etc/ssl/certs/ca-certificates.crt}
if [ -f "$SYSTEM_CA" ]; then export PIP_CERT=${PIP_CERT:-$SYSTEM_CA}; fi
mkdir -p "$STATE/bin" "$STATE/model-cache" "$STATE/warehouse"
exec 9>"$STATE/.lock"; flock -n 9 || { echo "another build or bootstrap holds $STATE/.lock" >&2; exit 75; }

build_bin() {  # <name> <repo> <rev> <package dir in the repo> [cargo args...]
  local name=$1 repo=$2 rev=$3 sub=$4; shift 4
  local build="$*"   # the cargo arguments: a binary built with others (another feature set) is rebuilt
  if [ -x "$STATE/bin/$name" ] && [ "$(cat "$STATE/bin/$name.rev" 2>/dev/null)" = "$rev" ] \
     && [ "$(cat "$STATE/bin/$name.build" 2>/dev/null)" = "$build" ]; then
    echo "$name at $rev ($build): present"; return
  fi
  command -v cargo >/dev/null || {
    echo "cargo is needed to build $name: install rustup for this user first" \
         "(curl --proto '=https' -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal), then run this again" >&2
    exit 69
  }
  rm -rf "$STATE/build"; mkdir -p "$STATE/build"
  git clone -q --filter=blob:none "https://github.com/$repo.git" "$STATE/build/src"
  git -C "$STATE/build/src" checkout -q "$rev"
  (cd "$STATE/build/src/$sub" && CARGO_TARGET_DIR="$STATE/build/target" cargo build --release --locked "$@")
  install -m 0755 "$STATE/build/target/release/$name" "$STATE/bin/$name"
  echo "$rev" > "$STATE/bin/$name.rev"
  echo "$build" > "$STATE/bin/$name.build"
  if [ "$name" = otzaria-semantic-search ]; then
    rm -rf "$STATE/bin/families" "$STATE/bin/family-model.json" "$STATE/bin/chunking.json"
    for family in $FAMILY_CONFIGS; do
      config=${family%%:*}
      mkdir -p "$STATE/bin/families/${config##*/}"
      cp "$STATE/build/src/$config/model.json" "$STATE/build/src/$config/chunking.json" "$STATE/bin/families/${config##*/}/"
    done
  fi
  rm -rf "$STATE/build"
  echo "$name at $rev: built"
}

build_bin otzaria-semantic-search "$SIDECAR_REPO" "$SIDECAR_REV" . --features onnx-backend --bin otzaria-semantic-search
if [ -n "$PLUGIN_REV" ]; then
  build_bin export_semantic_plan "$PLUGIN_REPO" "$PLUGIN_REV" rust --features semantic-integration --bin export_semantic_plan
  build_bin validate_semantic_vectors "$PLUGIN_REPO" "$PLUGIN_REV" rust --features semantic --bin validate_semantic_vectors
else
  echo "export_semantic_plan, validate_semantic_vectors: PLUGIN_REV is not pinned — the build can neither plan nor validate"
fi
for family in $FAMILY_CONFIGS; do
  config=${family%%:*}
  [ -f "$STATE/bin/families/${config##*/}/chunking.json" ] \
    || { echo "bin/families/${config##*/}/chunking.json missing: rebuild the sidecar CLI (remove bin/otzaria-semantic-search.rev)" >&2; exit 1; }
done

if [ ! -x "$STATE/venv/bin/python" ]; then
  python3 -m venv "$STATE/venv"
fi
"$STATE/venv/bin/python" -m pip install -q --upgrade pip
# shellcheck disable=SC2086
"$STATE/venv/bin/python" -m pip install -q --find-links "$ROCM_WHEELS" "$TORCH_PIN" $PY_PINS
HSA_ENABLE_DXG_DETECTION=1 "$STATE/venv/bin/python" - <<'PY'
import torch, onnxruntime, tokenizers, onnx, numpy
print("venv:", "torch", torch.__version__, "hip", torch.version.hip, "gpu", torch.cuda.is_available() and torch.cuda.get_device_name(0),
      "| onnxruntime", onnxruntime.__version__, "| tokenizers", tokenizers.__version__, "| onnx", onnx.__version__, "| numpy", numpy.__version__)
PY

if [ -n "$SEED" ]; then
  dest="$STATE/model-cache/$PASSAGE_PACKAGE_CHECKSUM"
  mkdir -p "$dest"
  cp -f "$SEED/$MODEL_GRAPH" "$SEED/tokenizer.json" "$dest/"
  got=$("$STATE/venv/bin/python" -c 'import sys; sys.path.insert(0, sys.argv[1]); import model_package as m; print(m.package_checksum(sys.argv[2], sys.argv[3]))' \
    "$HERE" "$dest" "$MODEL_GRAPH")   # paths as arguments, never as Python source
  [ "$got" = "$PASSAGE_PACKAGE_CHECKSUM" ] || { rm -rf "$dest"; echo "the seed package hashes to $got, not $PASSAGE_PACKAGE_CHECKSUM" >&2; exit 1; }
  echo "model cache seeded: $PASSAGE_PACKAGE_CHECKSUM"
fi
echo "bootstrap: done ($STATE)"
