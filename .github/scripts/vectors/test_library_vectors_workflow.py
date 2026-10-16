"""Contract tests for build-library-vectors.yml and its driver, build_library_vectors.sh.

The workflow is thin on purpose: it decides when (after the release's index is stored), where
(the build machine, which holds the warehouse and the GPU) and in which mode; the driver does
the work, the same way by hand. Load-bearing properties, pinned here:

  * it runs only behind a successful index build of a database release (or a manual run
    naming one), and publishing is opt-in;
  * a runner with no warehouse yet plans every text and creates the warehouse on its first add;
  * a warehouse is verified before the plan reads it: bad data stops the build, and a bad
    index is rebuilt;
  * the DB the index provenance names is downloaded, held to that digest, and given to the
    planner and the validator, which read official books' line text from it (index schema 5);
  * a plan of fewer than CPU_EMBED_MAX texts is embedded on the CPU by the sidecar's embed-shard
    (the reference), so the GPU worker never gets a plan its parity certificate cannot cover;
  * a gate failure, an existing vectors release or a damaged index stops it before anything
    is published;
  * it publishes through a draft at the commit of the library release's tag: data first, the
    manifest last, and the draft is published only once it holds exactly the files built;
  * what it publishes is a prerelease unless the repository says otherwise, which keeps it out
    of update-release-manifest.yml and the database history;
  * nothing is published unless the plugin's validate_semantic_vectors passes the release on its
    index (G3 coverage, G4 resolution, G6 retrieval), and its report is published with the gates;
  * it persists the ledger only after a publish, one ledger per family;
  * it builds every family of FAMILY_CONFIGS from one index and one warehouse, in its order, each
    published as its own <prefix>-<tag>: the first, which released applications install, is
    published before a later family runs, and a later family's failure leaves it published.

The YAML assertions read the parsed workflow; the driver's are made by running it against stub
tools (gh, the planner, the sidecar CLI, the worker's Python, zstd) and reading what it called.
The stubs refuse what the real tools refuse (a missing warehouse, an add with no shards), so a
driver that depends on state the runner lacks fails here too.
"""

import hashlib
import io
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import unittest
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
WORKFLOWS = SCRIPTS.parent / "workflows"
VECTORS = WORKFLOWS / "build-library-vectors.yml"
INDEX = WORKFLOWS / "build-library-index.yml"
RELEASE_MANIFEST = WORKFLOWS / "update-release-manifest.yml"
DRIVER = HERE / "build_library_vectors.sh"
PINS = HERE / "pins.env"
DATABASE_TAG = re.compile(r"^v[0-9]+-[0-9]{14}$")
BOOTSTRAP = HERE / "bootstrap_runner.sh"
PIN = dict(line.split("=", 1) for line in PINS.read_text().splitlines() if line and not line.startswith("#") and "=" in line)
TAG = "v30-20260930165019"
TAG_COMMIT = "c66256253b25c0aa0b13d04e399647496f3b0bee"   # the commit TAG names (the stub gh's answer)
# (name, release prefix) of each family, in build order
FAMILIES = [(c.split(":")[0].rsplit("/", 1)[-1], c.split(":")[1]) for c in PIN["FAMILY_CONFIGS"].strip('"').split()]
LEGACY, WINDOWS = FAMILIES[0][0], FAMILIES[1][0]
CHUNKING_IDENTITY = {"meivin-round2-onnx": 2685558872390372738, "meivin-round2-windows-onnx": 320260894818558297}


def run_text(job):
    return "\n".join(step["run"] for step in job.get("steps") or [] if "run" in step)


def linux_tools():
    if not all(shutil.which(t) for t in ("bash", "jq", "sha256sum", "split", "flock", "tar")):
        return False
    return subprocess.run(["stat", "-c", "%s", "/"], capture_output=True).returncode == 0


@unittest.skipIf(yaml is None, "PyYAML unavailable")
class Workflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.doc = yaml.safe_load(VECTORS.read_text(encoding="utf-8"))
        cls.index = yaml.safe_load(INDEX.read_text(encoding="utf-8"))
        cls.gate = cls.doc["jobs"]["gate"]
        cls.job = cls.doc["jobs"]["vectors"]

    def test_it_runs_after_the_index_workflow_or_by_hand_and_on_nothing_else(self):
        on = self.doc[True]
        self.assertEqual(set(on), {"workflow_run", "workflow_dispatch"})
        self.assertEqual(on["workflow_run"]["workflows"], [self.index["name"]])
        self.assertEqual(on["workflow_run"]["types"], ["completed"])
        inputs = on["workflow_dispatch"]["inputs"]
        self.assertEqual(set(inputs), {"release_tag", "mode", "family"})
        self.assertTrue(inputs["release_tag"]["required"])
        self.assertEqual(inputs["mode"]["options"], ["dry-run", "base"])
        self.assertEqual(inputs["mode"]["default"], "dry-run")

    def test_the_gate_admits_a_successful_index_build_of_a_database_release_only(self):
        gate = run_text(self.gate)
        self.assertIn('[ "$RUN_CONCLUSION" != success ]', gate)
        patterns = re.findall(r"grep -Eq '(\^v[^']*\$)'", gate)
        self.assertEqual(set(patterns), {DATABASE_TAG.pattern})
        # the index workflow names its runs `library-index <tag>`; the gate reads the tag from that
        self.assertTrue(self.index["run-name"].startswith("library-index "))
        self.assertIn("${RUN_TITLE#library-index }", gate)
        self.assertEqual(self.gate["steps"][0]["env"]["RUN_CONCLUSION"], "${{ github.event.workflow_run.conclusion }}")

    def test_a_mistyped_manual_tag_fails_instead_of_skipping(self):
        self.assertIn("::error::release_tag '$TAG' is not a database release tag", run_text(self.gate))

    def test_publishing_is_opt_in(self):
        gate = run_text(self.gate)
        self.assertIn("MODE=dry-run", gate)
        self.assertIn('[ "$PUBLISH" != true ] || MODE=base', gate)
        self.assertEqual(self.gate["steps"][0]["env"]["PUBLISH"], "${{ vars.LIBRARY_VECTORS_PUBLISH }}")

    def test_a_manual_run_builds_every_family_or_one_alone(self):
        family = self.doc[True]["workflow_dispatch"]["inputs"]["family"]
        self.assertEqual(family["options"], ["all"] + [name for name, _ in FAMILIES])
        self.assertEqual(family["default"], "all")
        # a run the index workflow triggers has no inputs: every family
        self.assertEqual(self.job["env"]["FAMILY"], "${{ inputs.family }}")
        self.assertIn('--family "${FAMILY:-all}"', run_text(self.job))

    def test_the_heavy_job_runs_only_behind_the_gate_on_the_build_machine(self):
        self.assertEqual(self.job["needs"], "gate")
        self.assertEqual(self.job["if"], "needs.gate.outputs.proceed == 'true'")
        self.assertIn("otzaria-db", self.job["runs-on"])          # the index workflow's local runner
        self.assertIn('LABELS=\'["otzaria-db"]\'', run_text(self.index["jobs"]["gate"]))
        self.assertIn("amd-gpu", self.job["runs-on"])
        self.assertIn("self-hosted", self.job["runs-on"])

    def test_one_build_at_a_time_and_never_cancelled(self):
        c = self.doc["concurrency"]
        self.assertEqual(c["cancel-in-progress"], False)
        self.assertEqual(c["queue"], "max")
        self.assertNotEqual(c["group"], self.index["concurrency"]["group"])

    def test_every_job_is_bounded(self):
        for name, job in self.doc["jobs"].items():
            self.assertIsInstance(job.get("timeout-minutes"), int, name)

    def test_the_workflow_is_thin_and_the_driver_does_the_work(self):
        runs = [s for s in self.job["steps"] if "run" in s]
        self.assertEqual(len(runs), 2)
        # a changed pin is built on the machine before the driver's preflight checks it
        self.assertIn("bash .github/scripts/vectors/bootstrap_runner.sh", runs[0]["run"])
        self.assertIn("bash .github/scripts/vectors/build_library_vectors.sh", runs[1]["run"])
        self.assertTrue(DRIVER.exists() and os.access(DRIVER, os.X_OK))

    def test_untrusted_values_never_reach_the_shell_as_expressions(self):
        for name, job in self.doc["jobs"].items():
            for step in job.get("steps") or []:
                self.assertNotIn("${{", step.get("run", ""), f"{name}: an expression inside run")

    def test_the_tokens_come_from_secrets_through_the_environment(self):
        env = self.job["env"]
        self.assertEqual(env["GH_TOKEN"], "${{ secrets.PIPELINE_TOKEN }}")
        self.assertEqual(env["OTZARIA_HF_TOKEN"], "${{ secrets.OTZARIA_HF_TOKEN }}")

    def test_a_publish_is_a_prerelease_unless_the_repository_says_otherwise(self):
        self.assertEqual(self.job["env"]["LIBRARY_VECTORS_PRERELEASE"], "${{ vars.LIBRARY_VECTORS_PRERELEASE || 'true' }}")

    def test_a_prerelease_stays_out_of_the_release_manifest_and_the_history(self):
        # Why the default is a prerelease: every published release that is not one starts
        # update-release-manifest.yml, which lists it in the database history it commits.
        doc = yaml.safe_load(RELEASE_MANIFEST.read_text(encoding="utf-8"))
        self.assertEqual(doc[True]["release"]["types"], ["published"])
        (job,) = doc["jobs"].values()
        self.assertIn("github.event.release.prerelease == false", job["if"])
        self.assertIn("select((.draft|not) and (.prerelease|not))", run_text(job))


class Pins(unittest.TestCase):
    def test_the_sidecar_and_the_planner_are_pinned_to_full_commits(self):
        pins = dict(line.split("=", 1) for line in PINS.read_text().splitlines()
                    if line and not line.startswith("#") and "=" in line)
        self.assertRegex(pins["SIDECAR_REV"], r"^[0-9a-f]{40}$")
        self.assertRegex(pins["PLUGIN_REV"], r"^[0-9a-f]{40}$")
        self.assertRegex(pins["PASSAGE_PACKAGE_CHECKSUM"], r"^[0-9a-f]{64}$")
        self.assertRegex(pins["MODEL_REVISION"], r"^[0-9a-f]{7,40}$")   # the mirror at a commit, never a branch
        self.assertEqual(pins["PASSAGE_QUANTIZATION"], "fp32")
        self.assertRegex(pins["QUERY_PACKAGE_CHECKSUM"], r"^[0-9a-f]{64}$")      # G6 measures the int8 query model
        self.assertTrue(pins["QUERY_GRAPH"].endswith("-int8.onnx"))

    def test_the_family_released_applications_install_is_built_first_under_its_own_tag(self):
        # the application asks for exactly releases/tags/vectors-<tag>; the windows family
        # must not match its library-tag pattern ^v(\d+)- nor that tag
        self.assertEqual(FAMILIES, [("meivin-round2-onnx", "vectors"), ("meivin-round2-windows-onnx", "vectors-windows")])
        self.assertEqual(len({name for name, _ in FAMILIES}), len(FAMILIES))
        for _, prefix in FAMILIES[1:]:
            self.assertIsNone(re.match(r"^v(\d+)-", f"{prefix}-{TAG}"))
            self.assertNotEqual(f"{prefix}-{TAG}", f"vectors-{TAG}")
        self.assertNotIn("FAMILY_CONFIG", PIN)

    def test_a_plan_the_gpu_worker_gets_can_always_be_certified(self):
        # the GPU worker's parity certificate needs 1,000 samples, 41 of them golden: a plan of fewer
        # than 959 texts can never have one, so everything below CPU_EMBED_MAX goes to the CPU
        self.assertGreaterEqual(int(PIN["CPU_EMBED_MAX"]), 1000)
        self.assertGreaterEqual(int(PIN["CPU_PROCESSES"]), 1)
        self.assertGreaterEqual(int(PIN["CPU_THREADS"]), 1)

    def test_the_sidecar_cli_is_built_able_to_embed(self):
        # embed-shard needs a real inference backend; a default build of the sidecar has none
        line = next(l for l in BOOTSTRAP.read_text().splitlines() if l.startswith("build_bin otzaria-semantic-search "))
        self.assertIn("--features onnx-backend", line)


# The release as the API returns it, for the stub gh and the stub curl: its assets, with no
# digest under NO_DIGEST and no published_at under NO_PUBLISHED_AT.
RELEASE_JSON = r"""
release_json() {
  local f digest
  for f in "$ASSETS/$1"/*; do
    digest=""; [ -n "${NO_DIGEST:-}" ] || digest="sha256:$(sha256sum < "$f" | cut -d' ' -f1)"
    jq -n --arg name "$(basename "$f")" --argjson size "$(stat -c %s "$f")" --arg digest "$digest" \
      '{name: $name, size: $size, digest: (if $digest == "" then null else $digest end)}'
  done | jq -s --arg tag "$1" --arg at "${NO_PUBLISHED_AT:+none}" \
    '{tag_name: $tag, assets: .} + (if $at == "" then {published_at: "2026-09-30T21:38:29Z"} else {} end)'
}
"""

STUB_GH = r"""#!/usr/bin/env bash
""" + RELEASE_JSON + r"""echo "gh $*" >> "$CALLS"
if [ "$1" = api ]; then
  case "$2" in
    */commits/v*) echo "${TAG_COMMIT:-c66256253b25c0aa0b13d04e399647496f3b0bee}"; exit 0 ;;
    */releases/tags/v*) release_json "${2##*/}"; exit 0 ;;
  esac
  echo "stub gh: api $2" >&2; exit 90
fi
cmd="$1 $2"; shift 2
case "$cmd" in
  "auth status") [ -z "${GH_UNAUTHENTICATED:-}" ] ;;
  "release view")
    tag=$1
    case "$tag" in vectors-*)
      # a draft's assets: what was uploaded to it, as name<TAB>size
      case " $* " in *" --json assets "*) awk -F'\t' -v t="$tag" '$1 == t {print $2 "\t" $3}' "$UPLOADED" 2>/dev/null; exit 0 ;; esac
      # EXISTING: 1 for every release, else the tags that exist
      case "${EXISTING:-}" in 1) exit 0 ;; *" $tag "*|"$tag"|"$tag "*|*" $tag") exit 0 ;; esac; exit 1 ;;
    esac
    exit 0 ;;
  "release download")
    tag=$1; shift; dir=.; pats=()
    while [ $# -gt 0 ]; do case "$1" in --dir) dir=$2; shift 2;; --pattern) pats+=("$2"); shift 2;; *) shift;; esac; done
    for p in "${pats[@]}"; do cp "$ASSETS/$tag/$p" "$dir/$p" || exit 1; done ;;
  "release create")
    tag=$1; while [ $# -gt 0 ]; do case "$1" in --notes-file) cp "$2" "$UPLOADED.notes.$tag"; shift 2;; *) shift;; esac; done ;;
  "release upload")
    # SHORT_UPLOAD names an asset that arrives one byte short
    name=$(basename "$2"); size=$(stat -c %s "$2")
    [ "$name" != "${SHORT_UPLOAD:-}" ] || size=$((size - 1))
    printf '%s\t%s\t%s\n' "$1" "$name" "$size" >> "$UPLOADED" ;;
  "release edit") ;;
  *) echo "stub gh: $cmd" >&2; exit 90 ;;
esac
"""

# The anonymous path (gh unauthenticated), as GitHub and curl answer it: an asset URL is a 302
# only -L follows; an HTTP error (CURL_FAIL=api: the API's) exits 22 under -f, else saves its body.
STUB_CURL = r"""#!/usr/bin/env bash
""" + RELEASE_JSON + r"""echo "curl $*" >> "$CALLS"
out=/dev/stdout; url=""; opts=""
while [ $# -gt 0 ]; do case "$1" in -o) out=$2; shift 2;; --retry|--retry-delay) shift 2;; --*) shift;;
  -*) opts=$opts${1#-}; shift;; *) url=$1; shift;; esac; done
has() { case "$opts" in *"$1"*) return 0;; esac; return 1; }
http_error() {
  if has f; then { has S || ! has s; } && echo "curl: (22) The requested URL returned error: 404" >&2; exit 22; fi
  echo '{"message":"Not Found","status":"404"}' > "$out"
}
case "$url" in
  https://api.github.com/repos/*/releases/tags/*)
    if [ "${CURL_FAIL:-}" = api ]; then http_error; else release_json "${url##*/}" > "$out"; fi ;;
  https://github.com/*/releases/download/*)
    asset="$ASSETS/${url#*/releases/download/}"
    if [ ! -f "$asset" ]; then http_error
    elif has L; then cp "$asset" "$out"
    else : > "$out"; fi ;;
  *) echo "stub curl: $url" >&2; exit 90 ;;
esac
"""

STUB_EXPORT = r"""#!/usr/bin/env bash
echo "export $*" >> "$CALLS"
for o in "$@"; do out_arg=$o; done   # --out is last: the work directory holds the families before it
echo "export-work $(cd "${out_arg%/*}/.." && ls -A | tr '\n' ' ')" >> "$CALLS"
out=""; warehouse=""; db=""; model=""
while [ $# -gt 0 ]; do case "$1" in --out) out=$2; shift 2;; --warehouse) warehouse=$2; shift 2;; --seforim-db) db=$2; shift 2;;
  --model) model=$2; shift 2;; *) shift;; esac; done
[ -z "$model" ] || [ -f "$model" ] || { echo "Could not read the model identity $model" >&2; exit 1; }
# as export_semantic_plan does on a schema 5 index, which keeps official books' line text in the DB
[ -n "$db" ] || { echo "Planning failed: the document at DocAddress { segment_ord: 3, doc_id: 38119 } keeps its text in the library database, which cannot be read (pass --seforim-db)" >&2; exit 1; }
[ -s "$db" ] || { echo "Could not use the library database: $db" >&2; exit 1; }
# as export_semantic_plan does: a --warehouse is opened, and a directory without warehouse.json is none
if [ -n "$warehouse" ] && [ ! -f "$warehouse/warehouse.json" ]; then
  echo "Could not open the warehouse: $warehouse/warehouse.json: No such file or directory" >&2; exit 1
fi
# and trusts its index unverified: a bad one (index.fault) hides about half the library's vectors
[ -z "$warehouse" ] || [ ! -f "$warehouse/index.fault" ] || TO_EMBED=3173793
mkdir -p "$out"
: > "$out/embed.jsonl"
[ -z "$model" ] || cp "$model" "$out/model.json"   # what the stub assemble derives the identity digest from
jq -n --argjson n "${TO_EMBED:-0}" '{format:"otzaria-embed-plan",version:2,records:$n,plan_sha256:("a"*64),passage_package:{checksum:"4a4a2ae88a86f15ffe6069bfcefc3abd13c207cec5d7aaef52c0c59d752ade46",quantization:"fp32"}}' > "$out/embed-manifest.json"
jq -n --arg tag "${PLAN_TAG:-v30-20260930165019}" '{format:"otzaria-vector-plan",version:1,library_release_tag:$tag,parity:{checked:9,mismatches:0},counts:{records:9}}' > "$out/plan-manifest.json"
"""

STUB_CLI = r"""#!/usr/bin/env bash
echo "cli $*" >> "$CALLS"
sub=$1; shift
out=""; files=(); verify=""; warehouse=""; create=""; model=""; shards=(); repair=""; plan=""
while [ $# -gt 0 ]; do case "$1" in
  --out) out=$2; shift 2;; --plan) plan=$2; shift 2;; --files) files+=("$2"); shift 2;; --verify) verify=1; shift;;
  --warehouse) warehouse=$2; shift 2;; --create) create=1; shift;; --model) model=$2; shift 2;;
  --shards) shards+=("$2"); shift 2;; --processes) processes=$2; shift 2;; --repair) repair=1; shift;; *) shift;; esac; done
# as the sidecar does: --create makes a missing warehouse for --model; everything else needs one
need_warehouse() { [ -f "$warehouse/warehouse.json" ] || { echo "Could not open the warehouse $warehouse" >&2; exit 1; }; }
# and verifies it first: data.fault stands for a batch that fails its digests (never repaired),
# index.fault for an index that is not keys.bin's (rebuilt by an add or by --repair)
verified() {  # <repairs, or "">
  need_warehouse; rebuilt=""
  if [ -f "$warehouse/data.fault" ]; then
    echo "The warehouse failed its check: warehouse $warehouse: batch 2: vectors.f32 does not hash to its digest; data that fails its digest is not repaired: restore the warehouse from a copy, or move it aside and the next build embeds every text again" >&2; exit 1
  fi
  [ -f "$warehouse/index.fault" ] || return 0
  [ -n "$1" ] || { echo "The warehouse failed its check: warehouse $warehouse: index.bin is not the index of keys.bin; the keys are sound, so adding to the warehouse, or warehouse-verify --repair, rebuilds the index from them" >&2; exit 1; }
  rm "$warehouse/index.fault"; rebuilt=1
}
case "$sub" in
  warehouse-verify)
    verified "$repair"
    echo "Verified:        9 record(s) in 2 batch(es), 9216 bytes re-hashed"
    [ -z "$rebuilt" ] || echo "Index rebuilt:   index.bin is not the index of keys.bin"
    echo "Took:            0.0 s" ;;
  embed-shard)
    # as the sidecar does: one shard in --out, or with --processes P one per process in shard-NNN
    echo "cli-env OTZARIA_ONNX_RUNTIME=${OTZARIA_ONNX_RUNTIME:-}" >> "$CALLS"
    p=${processes:-1}; [ "$p" -le "${TO_EMBED:-1}" ] || p=${TO_EMBED:-1}
    for i in $(seq 0 $((p - 1))); do
      d=$out; [ "${processes:-1}" -le 1 ] || d=$out/$(printf 'shard-%03d' "$i")
      mkdir -p "$d"; for f in vectors.f32 keys.bin shard-manifest.json; do : > "$d/$f"; done
    done ;;
  warehouse-add)
    if [ -n "$create" ] && [ ! -f "$warehouse/warehouse.json" ]; then
      [ -f "$model" ] || { echo "--create needs --model" >&2; exit 1; }
      mkdir -p "$warehouse"; echo '{"format":"otzaria-vector-warehouse","records":0}' > "$warehouse/warehouse.json"
    fi
    found=$(for s in "${shards[@]}"; do find "$s" -name shard-manifest.json; done | wc -l)
    [ "$found" -gt 0 ] || { echo "Nothing to add: no shard-manifest.json under --shards" >&2; exit 1; }
    verified 1 ;;
  assemble)
    verified ""
    if [ -n "$verify" ]; then echo '{"gates":"stub"}' > "$out/gates.json"; exit "${VERIFY_EXIT:-0}"; fi
    mkdir -p "$out"; head -c 3000 /dev/urandom > "$out/segment.oxv"
    # the identity is the family's: its digest, the hash of the model the plan was made with
    jq -n --arg d "$(sha256sum < "$plan/model.json" | cut -c1-16)" '{identityDigest: $d, toLibraryVersion: 30}' > "$out/release.json"
    for f in ledger-v30.keys pairs-v30.bin ledger-v30.manifest.json; do echo "$f" > "$out/$f"; done
    if [ "$(jq -r .chunking_identity "$plan/model.json")" = 320260894818558297 ]; then
      echo 'windowed occurrence metadata' > "$out/occurrences-v30.bin"
    fi ;;
  release-files)
    for f in "${files[@]}"; do [ -f "$f" ] || { echo "Could not read release file $f" >&2; exit 1; }; done
    jq -cn --args '$ARGS.positional' "${files[@]}" >> "$RELEASE_FILE_ARGS"
    echo '{"files":[]}' > "$out"; echo "=== stem ==="; echo "Manifest SHA-256 $(sha256sum < "$out" | cut -d' ' -f1)" ;;
  *) echo "stub cli: $sub" >&2; exit 90 ;;
esac
"""

STUB_PY = r"""#!/usr/bin/env bash
echo "py $*" >> "$CALLS"
script=$1; shift
out=""; cache=""; checksum=""; graph=""
while [ $# -gt 0 ]; do case "$1" in --out) out=$2; shift 2;; --cache) cache=$2; shift 2;;
  --checksum) checksum=$2; shift 2;; --graph) graph=$2; shift 2;; *) shift;; esac; done
case "$script" in
  */model_package.py)   # fetch: the verified package's directory on stdout
    mkdir -p "$cache/$checksum"; : > "$cache/$checksum/$graph"; echo "$cache/$checksum" ;;
  *)                    # the GPU worker: a shard in --out
    mkdir -p "$out"; for f in vectors.f32 keys.bin shard-manifest.json; do : > "$out/$f"; done ;;
esac
"""

# As validate_semantic_vectors does: arguments that are wrong, or inputs that do not read, exit 2;
# a gate that failed or did not run (its inputs not given, and not skipped) exits 1, with the
# report written; every gate passed or skipped exits 0. VALIDATE_FAILS names gates that fail and
# VALIDATE_UNREADABLE makes the index unreadable, for the driver's tests.
STUB_VALIDATE = r"""#!/usr/bin/env bash
echo "validate $*" >> "$CALLS"
echo "validate-env TMPDIR=${TMPDIR:-}" >> "$CALLS"
wrong() { echo "$1" >&2; exit 2; }
given=" "; releases=0; skips=""
index=""; db=""; vectors=""; plan=""; warehouse=""; model=""; identity=""; ort=""; report=""; threads=""
while [ $# -gt 0 ]; do
  flag=$1
  case "$flag" in
    --release) [ $# -ge 2 ] || wrong "--release needs a value"
      { [ -f "$2/segment.oxv" ] && [ -f "$2/release.json" ]; } || wrong "the releases do not install as a device installs them: $2"
      releases=$((releases + 1)); shift 2; continue ;;
    --skip) case "${2:-}" in G3|G4|G6) skips="$skips $2"; shift 2; continue ;; *) wrong "--skip needs a gate" ;; esac ;;
    --index|--seforim-db|--vectors|--plan|--max-stale-hints|--warehouse|--model|--model-identity|--onnx-runtime|--queries|\
    --sample-queries|--min-recall-10|--min-recall-50|--threads|--report) ;;
    *) wrong "unknown argument $flag" ;;
  esac
  [ $# -ge 2 ] || wrong "$flag needs a value"
  case "$given" in *" $flag "*) wrong "$flag is given twice" ;; esac
  given="$given$flag "
  case "$flag" in
    --index) index=$2 ;; --seforim-db) db=$2 ;; --vectors) vectors=$2 ;; --plan) plan=$2 ;; --warehouse) warehouse=$2 ;; --model) model=$2 ;;
    --model-identity) identity=$2 ;; --onnx-runtime) ort=$2 ;; --report) report=$2 ;; --threads) threads=$2 ;;
  esac
  shift 2
done
n=0; for g in G3 G4 G6; do case "$skips" in *$g*) n=$((n + 1)) ;; esac; done
[ "$n" -lt 3 ] || wrong "--skip names every gate, which leaves nothing to validate"
[ -n "$index" ] || wrong "--index is required"
if [ -n "$vectors" ] && [ "$releases" -gt 0 ]; then wrong "give --vectors, or one --release or more, and not both"; fi
if [ -z "$vectors" ] && [ "$releases" -eq 0 ]; then wrong "give --vectors, or one --release or more, and not both"; fi
[ "$threads" != 0 ] || wrong "--threads is 0, and a scan needs one"
g6=""; [ -z "$warehouse" ] || g6=${g6}w; [ -z "$model" ] || g6=${g6}m; [ -z "$identity" ] || g6=${g6}i
case "$g6" in ""|wmi) ;; *) wrong "G6 needs --warehouse, --model and --model-identity together" ;; esac
{ [ -d "$index" ] && [ -z "${VALIDATE_UNREADABLE:-}" ]; } || wrong "could not validate against the index $index"
# a schema 5 index keeps official books' line text in the DB
[ -n "$db" ] || wrong "the index keeps line text in the library database: --seforim-db is required"
[ -s "$db" ] || wrong "could not use the library database $db"
[ -z "$plan" ] || [ -f "$plan/plan-manifest.json" ] || wrong "could not read the plan $plan"
if [ "$g6" = wmi ]; then
  [ -f "$warehouse/warehouse.json" ] || wrong "could not open the warehouse $warehouse"
  [ -f "$model" ] || wrong "loading the query model $model"
  [ -f "$identity" ] || wrong "could not read the model identity $identity"
  [ -z "$ort" ] || [ -f "$ort" ] || wrong "could not load ONNX Runtime from $ort"
fi
verdict() {
  case "$skips" in *$1*) echo skipped; return ;; esac
  case "${VALIDATE_FAILS:-}" in *$1*) echo failed; return ;; esac
  # VALIDATE_FAILS_FOR: the family (a part of its --model-identity path) whose G4 fails
  if [ "$1" = G4 ] && [ -n "${VALIDATE_FAILS_FOR:-}" ]; then case "$identity" in *"/${VALIDATE_FAILS_FOR}/"*) echo failed; return ;; esac; fi
  if [ "$1" = G6 ] && [ "$g6" != wmi ]; then echo notRun; return; fi
  echo passed
}
g3=$(verdict G3); g4=$(verdict G4); g6v=$(verdict G6); passed=true
for v in $g3 $g4 $g6v; do case "$v" in passed|skipped) ;; *) passed=false ;; esac; done
[ -z "$report" ] || jq -n --argjson passed "$passed" --arg g3 "$g3" --arg g4 "$g4" --arg g6 "$g6v" \
  '{tool: "validate_semantic_vectors", reportVersion: 1, passed: $passed,
    gates: [{gate: "G3", status: $g3, metrics: {sourceTextChars: 10000, windowedTextChars: 8700,
      unwindowedTextChars: 1300, partiallyWindowedLines: 2, unwindowedLines: 0}},
      {gate: "G4", status: $g4}, {gate: "G6", status: $g6}]}' > "$report"
[ "$passed" = true ]
"""

STUB_ZSTD = r"""#!/usr/bin/env bash
echo "zstd $*" >> "$CALLS"
# ZSTD_FAIL fails the DB's expansion (the one with a raised --memory), not the index's
if [ "$1" = "-dc" ]; then case " $* " in *" --memory=2048MB "*) [ -z "${ZSTD_FAIL:-}" ] || exit 1 ;; esac; cat; exit 0; fi
in=""; out=""; while [ $# -gt 0 ]; do case "$1" in -o) out=$2; shift 2;; -*) shift;; *) in=$1; shift;; esac; done
cp "$in" "$out"
"""

# df -Pk answering DF_FREE_KB's values in turn (DF_COUNT counts the calls)
STUB_DF = r"""#!/usr/bin/env bash
n=$(cat "$DF_COUNT" 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" > "$DF_COUNT"
set -- $DF_FREE_KB; shift $((n - 1)) 2>/dev/null || set --
echo "Filesystem 1024-blocks Used Available Capacity Mounted on"
echo "stub 1 1 ${1:-0} 1% /"
"""


@unittest.skipUnless(linux_tools(), "needs bash, jq, sha256sum, split, flock, tar and GNU stat")
class Driver(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        r = Path(self.tmp.name)
        self.root, self.calls = r, r / "calls.log"
        (r / "bin").mkdir()
        (r / "net").mkdir()   # on PATH, so no test reaches the network
        for path, body in ((r / "bin" / "gh", STUB_GH), (r / "bin" / "export", STUB_EXPORT), (r / "bin" / "cli", STUB_CLI),
                           (r / "bin" / "py", STUB_PY), (r / "bin" / "zstd", STUB_ZSTD), (r / "bin" / "validate", STUB_VALIDATE),
                           (r / "net" / "curl", STUB_CURL)):
            path.write_text(body)
            path.chmod(path.stat().st_mode | stat.S_IEXEC)
        # a release whose index archive is two parts of a tar (the zstd stub passes it through)
        assets = r / "assets" / TAG
        assets.mkdir(parents=True)
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            data = b'{"segments": []}'
            info = tarfile.TarInfo("index/meta.json"); info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        archive = buf.getvalue()
        half = len(archive) // 2
        parts = {"otzaria-library-index.tar.zst.part-000": archive[:half], "otzaria-library-index.tar.zst.part-001": archive[half:]}
        for name, b in parts.items():
            (assets / name).write_bytes(b)
        self.archive_sha = hashlib.sha256(archive).hexdigest()
        self.manifest = assets / "otzaria-library-index.tar.zst.manifest.json"
        self.manifest.write_text(json.dumps({
            "schemaVersion": 1, "archive": "otzaria-library-index.tar.zst", "size": len(archive), "sha256": self.archive_sha,
            "parts": [{"name": n, "size": len(b), "sha256": hashlib.sha256(b).hexdigest()} for n, b in parts.items()]}))
        # the full DB the index was built from: the provenance is held to its digest, and so is its download
        db = b"a stand-in for the release's full database"
        self.db_bytes = db
        (assets / "seforim-schema6.db.zst").write_bytes(db)
        self.db_sha = hashlib.sha256(db).hexdigest()
        self.provenance = assets / "otzaria-library-index.provenance.json"
        self.write_provenance()
        self.state = r / "state"
        (self.state / "bin").mkdir(parents=True)
        for name, _ in FAMILIES:
            family = self.state / "bin" / "families" / name
            family.mkdir(parents=True)
            (family / "model.json").write_text(json.dumps({"family_id": "stub", "chunking_identity": CHUNKING_IDENTITY[name]}))
            (family / "chunking.json").write_text("{}")
        # a runner that has built before: its warehouse (the driver's path for the fp32 package)
        self.warehouse = self.state / "warehouse" / "fp32-4a4a2ae8"
        self.warehouse.mkdir(parents=True)
        (self.warehouse / "warehouse.json").write_text("{}")
        self.ort = self.state / "venv/lib/python3.12/site-packages/onnxruntime/capi/libonnxruntime.so.1.28.0"
        self.ort.parent.mkdir(parents=True)
        self.ort.write_bytes(b"")

    def tearDown(self):
        self.tmp.cleanup()

    def write_provenance(self, **changes):
        """The index provenance, shaped as v30's (build-library-index.yml); a change to None drops the field."""
        prov = {"schemaVersion": 1, "libraryReleaseTag": TAG, "seforimDbZstSha256": self.db_sha,
                "indexArchive": "otzaria-library-index.tar.zst", "indexArchiveSha256": self.archive_sha,
                "indexUncompressedBytes": 5235054900, "catalogueBooks": 7803, "indexSegments": 11,
                "talmudBavliSha256": "c" * 64, "talmudVolumesDigest": "e" * 64, "talmudVolumes": 39,
                "includesPdfBooks": False, "searchEngineVersion": "0.8.7",
                "otzaria": {"repository": "Otzaria/otzaria", "ref": "dev", "sha": "d" * 40, "runId": None},
                "builtBy": {"repository": "Otzaria/SeforimLibrary", "runId": "36780724734"},
                "builtAt": "2026-09-30T22:37:59Z"}
        for key, value in changes.items():
            if value is None:
                prov.pop(key)
            else:
                prov[key] = value
        self.provenance.write_text(json.dumps(prov))

    def run_driver(self, mode="dry-run", tag=TAG, args=(), **env):
        e = dict(os.environ, CALLS=str(self.calls), ASSETS=str(self.root / "assets"), UPLOADED=str(self.root / "uploaded.tsv"),
                 RELEASE_FILE_ARGS=str(self.root / "release-file-args.json"),
                 OTZARIA_SEMANTIC_CLI=str(self.root / "bin" / "cli"), EXPORT_SEMANTIC_PLAN=str(self.root / "bin" / "export"),
                 VECTOR_PYTHON=str(self.root / "bin" / "py"), GH=str(self.root / "bin" / "gh"), ZSTD=str(self.root / "bin" / "zstd"),
                 VALIDATE_SEMANTIC_VECTORS=str(self.root / "bin" / "validate"),
                 VECTORS_MIN_FREE_GB="0", OTZARIA_HF_TOKEN="hf_s3cr3t", GITHUB_ACTIONS="",
                 PATH=str(self.root / "net") + os.pathsep + os.environ["PATH"])
        e.update({k: str(v) for k, v in env.items()})
        p = subprocess.run(["bash", str(DRIVER), "--tag", tag, "--mode", mode, "--state", str(self.state), "--repo", "o/r",
                            *args], capture_output=True, text=True, env=e)
        calls = self.calls.read_text().splitlines() if self.calls.exists() else []
        self.assertNotIn("hf_s3cr3t", p.stdout + p.stderr)
        return p, calls

    def index_of(self, calls, needle):
        return next(i for i, c in enumerate(calls) if needle in c)

    def model(self, name):
        return self.state / "bin" / "families" / name / "model.json"

    def id8(self, name):
        """The first 8 of the identity digest the stub assemble gives the family."""
        return hashlib.sha256(self.model(name).read_bytes()).hexdigest()[:8]

    def stem(self, name):
        return f"otzaria-vectors-{self.id8(name)}-v30-base"

    def test_a_dry_run_builds_and_verifies_and_publishes_and_persists_nothing(self):
        p, calls = self.run_driver(TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        order = [self.index_of(calls, n) for n in ("--pattern otzaria-library-index.tar.zst.manifest.json", "export --index",
                                                   "cli assemble --kind base", "cli assemble --verify", "validate --index",
                                                   "cli release-files")]
        self.assertEqual(order, sorted(order))                 # a dry run validates too
        self.assertFalse([c for c in calls if "embed_worker.py" in c or "embed-shard" in c or "warehouse-add" in c])
        self.assertFalse([c for c in calls if "release create" in c or "release upload" in c or "release edit" in c])
        self.assertFalse((self.state / "ledger").exists())

    def test_a_large_update_is_embedded_on_the_gpu_in_windows_and_then_added_to_the_warehouse(self):
        n = int(PIN["CPU_EMBED_MAX"])                    # the smallest plan the GPU worker gets
        p, calls = self.run_driver(TO_EMBED=n)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        workers = [c for c in calls if "embed_worker.py" in c]
        self.assertEqual(len(workers), len(FAMILIES) * -(-n // int(PIN["EMBED_WINDOW"])))   # each family's plan
        self.assertIn("--skip 0 --take", workers[0])
        self.assertFalse([c for c in calls if "cli embed-shard" in c
                          or ("model_package.py" in c and PIN["PASSAGE_PACKAGE_CHECKSUM"] in c)])   # not the CPU path
        self.assertIn('"worker":"seforim-gpu-worker 1.0"', calls[self.index_of(calls, "cli assemble --kind base")])
        add = self.index_of(calls, "cli warehouse-add")
        self.assertGreater(add, self.index_of(calls, "embed_worker.py"))
        self.assertIn("--plan", calls[add])
        self.assertLess(add, self.index_of(calls, "cli assemble --kind base"))

    def test_a_small_update_is_embedded_on_the_cpu_by_the_sidecar_with_no_gpu_worker(self):
        for n in (1, 100, 958, 959, int(PIN["CPU_EMBED_MAX"]) - 1):
            with self.subTest(texts=n):
                self.calls.unlink(missing_ok=True)
                p, calls = self.run_driver(TO_EMBED=n)
                self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
                self.assertFalse([c for c in calls if "embed_worker.py" in c])
                package = self.state / "model-cache" / PIN["PASSAGE_PACKAGE_CHECKSUM"]
                fetch = calls[self.index_of(calls, "model_package.py fetch")]
                for arg in ("--cache " + str(self.state / "model-cache"), "--checksum " + PIN["PASSAGE_PACKAGE_CHECKSUM"],
                            "--graph " + PIN["MODEL_GRAPH"], "--repo " + PIN["MODEL_REPO"], "--revision " + PIN["MODEL_REVISION"]):
                    self.assertIn(arg + " ", fetch + " ")
                embed = self.index_of(calls, "cli embed-shard")
                self.assertGreater(embed, self.index_of(calls, "model_package.py fetch"))
                for arg in ("--plan ", "--model-file " + str(package / PIN["MODEL_GRAPH"]),
                            "--processes " + PIN["CPU_PROCESSES"], "--threads " + PIN["CPU_THREADS"], "--out "):
                    self.assertIn(arg, calls[embed])
                self.assertEqual(calls[embed + 1], "cli-env OTZARIA_ONNX_RUNTIME=" + str(self.ort))
                self.assertGreater(self.index_of(calls, "cli warehouse-add"), embed)
                self.assertIn('"worker":"otzaria-semantic-search embed-shard', calls[self.index_of(calls, "cli assemble --kind base")])

    def test_the_cpu_path_needs_the_venvs_onnx_runtime(self):
        self.ort.unlink()
        p, calls = self.run_driver(TO_EMBED=5)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("ONNX Runtime", p.stdout)
        self.assertFalse([c for c in calls if "cli embed-shard" in c or "warehouse-add" in c])

    def test_the_stubs_refuse_what_the_real_tools_refuse(self):
        missing, env = self.root / "no-warehouse", dict(os.environ, CALLS=str(self.calls))
        db = self.root / "seforim.db"
        db.write_text("x")
        def export(*args):
            return subprocess.run([str(self.root / "bin" / "export"), *args, "--out", str(self.root / "p")],
                                  env=env, capture_output=True, text=True)
        self.assertNotEqual(export("--seforim-db", str(db), "--warehouse", str(missing)).returncode, 0)
        # a schema 5 index cannot be planned without the DB, nor with one that does not read
        no_db = export("--warehouse", str(self.warehouse))
        self.assertNotEqual(no_db.returncode, 0)
        self.assertIn("pass --seforim-db", no_db.stderr)
        self.assertNotEqual(export("--seforim-db", str(self.root / "nowhere.db"), "--warehouse", str(self.warehouse)).returncode, 0)
        self.assertEqual(export("--seforim-db", str(db), "--warehouse", str(self.warehouse)).returncode, 0)
        shard = self.root / "shard"
        shard.mkdir()
        (shard / "shard-manifest.json").write_text("{}")
        add = subprocess.run([str(self.root / "bin" / "cli"), "warehouse-add", "--warehouse", str(missing), "--shards", str(shard)],
                             env=env, capture_output=True)
        self.assertNotEqual(add.returncode, 0)
        self.assertFalse(missing.exists())
        # a bad index is refused unless repaired; bad data is refused, repair or not
        def cli(*args):
            return subprocess.run([str(self.root / "bin" / "cli"), *args, "--warehouse", str(self.warehouse)],
                                  env=env, capture_output=True).returncode
        (self.warehouse / "index.fault").write_text("")
        self.assertEqual([cli("warehouse-verify"), cli("assemble", "--kind", "base", "--out", str(self.root / "r")),
                          cli("warehouse-verify", "--repair"), cli("warehouse-verify")], [1, 1, 0, 0])
        (self.warehouse / "data.fault").write_text("")
        self.assertEqual([cli("warehouse-verify", "--repair"), cli("warehouse-add", "--shards", str(shard))], [1, 1])

    def test_a_runner_with_no_warehouse_yet_plans_every_text_and_creates_it_on_the_first_add(self):
        shutil.rmtree(self.warehouse)
        p, calls = self.run_driver(TO_EMBED=2_000_000)      # the whole library
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertNotIn("--warehouse", calls[self.index_of(calls, "export --index")])   # every text is planned
        add = self.index_of(calls, "cli warehouse-add")
        for arg in ("--create", "--model " + str(self.model(LEGACY)), "--passage-quantization fp32",
                    "--plan "):
            self.assertIn(arg, calls[add])
        self.assertLess(add, self.index_of(calls, "cli assemble --kind base"))
        self.assertTrue((self.warehouse / "warehouse.json").exists())
        # the next family plans against the warehouse the first one created, and adds to it
        exports = [c for c in calls if c.startswith("export --index")]
        self.assertEqual(len(exports), len(FAMILIES))
        for c in exports[1:]:
            self.assertIn("--warehouse " + str(self.warehouse) + " ", c)
        adds = [c for c in calls if c.startswith("cli warehouse-add")]
        self.assertEqual(len(adds), len(FAMILIES))
        self.assertFalse([c for c in adds[1:] if "--create" in c])

    def test_the_gpu_worker_fetches_the_model_at_the_pinned_revision(self):
        pins = dict(line.split("=", 1) for line in PINS.read_text().splitlines()
                    if line and not line.startswith("#") and "=" in line)
        p, calls = self.run_driver(TO_EMBED=2_000_000)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        workers = [c for c in calls if c.startswith("py ") and "embed_worker.py" in c]
        self.assertTrue(workers)
        for c in workers:
            self.assertIn(f"--repo {pins['MODEL_REPO']} --revision {pins['MODEL_REVISION']} ", c + " ")

    def test_a_runner_with_a_warehouse_plans_against_it_and_adds_to_it(self):
        p, calls = self.run_driver(TO_EMBED=2_000_000)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("--warehouse " + str(self.warehouse), calls[self.index_of(calls, "export --index")])
        self.assertNotIn("--create", calls[self.index_of(calls, "cli warehouse-add")])

    def test_a_warehouse_directory_without_its_manifest_is_refused_before_the_build(self):
        (self.warehouse / "warehouse.json").unlink()
        (self.warehouse / "vectors.f32").write_bytes(b"\0" * 1024)
        p, calls = self.run_driver(mode="base")
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("no warehouse.json", p.stdout)
        self.assertFalse([c for c in calls if "release download" in c or c.startswith(("export ", "cli ", "py "))])
        self.assertEqual((self.warehouse / "vectors.f32").stat().st_size, 1024)   # never recreated over

    def test_a_warehouse_is_verified_before_the_index_downloads_and_the_plan_reads_it(self):
        p, calls = self.run_driver(TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        checks = [c for c in calls if "warehouse-verify" in c]
        self.assertEqual(checks, [f"cli warehouse-verify --warehouse {self.warehouse} --repair"])
        check = calls.index(checks[0])
        self.assertLess(check, self.index_of(calls, "--pattern otzaria-library-index.tar.zst.part-000"))
        self.assertLess(check, self.index_of(calls, "export --index"))
        self.assertIn("\nVerified:        9 record(s)", p.stdout)

    def test_a_runner_with_no_warehouse_yet_has_none_to_verify(self):
        shutil.rmtree(self.warehouse)
        p, calls = self.run_driver(TO_EMBED=5)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertFalse([c for c in calls if "warehouse-verify" in c])
        self.assertNotIn("Verified:", p.stdout)

    def test_a_warehouse_whose_data_fails_its_digests_stops_the_build_before_the_plan(self):
        (self.warehouse / "data.fault").write_text("")
        p, calls = self.run_driver(mode="base", TO_EMBED=int(PIN["CPU_EMBED_MAX"]))
        self.assertEqual(p.returncode, 1, p.stdout + p.stderr)
        self.assertIn("::error::warehouse-verify --repair, before the plan: The warehouse failed its check: warehouse "
                      f"{self.warehouse}: batch 2: vectors.f32 does not hash to its digest; data that fails", p.stdout)
        self.assertTrue([c for c in calls if "warehouse-verify" in c])
        self.assertFalse([c for c in calls if c.startswith(("export ", "py ")) or ".part-" in c or "embed-shard" in c
                          or "warehouse-add" in c or "release create" in c])

    def test_a_warehouse_index_that_is_not_its_keys_is_rebuilt_before_the_plan_and_the_build_goes_on(self):
        (self.warehouse / "index.fault").write_text("")
        p, calls = self.run_driver(TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("\nIndex rebuilt:   ", p.stdout)
        self.assertFalse((self.warehouse / "index.fault").exists())
        self.assertLess(self.index_of(calls, "cli warehouse-verify"), self.index_of(calls, "export --index"))
        # the split read a sound index: no text the warehouse holds went to be embedded
        self.assertFalse([c for c in calls if "embed_worker.py" in c or "embed-shard" in c or "warehouse-add" in c])
        self.assertIn("dry run: built and verified", p.stdout)

    def test_an_empty_plan_on_a_runner_with_no_warehouse_stops_before_assembling(self):
        shutil.rmtree(self.warehouse)
        p, calls = self.run_driver(TO_EMBED=0)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("no warehouse", p.stdout)
        self.assertFalse([c for c in calls if "cli assemble" in c])

    def test_the_release_is_validated_on_its_index_with_every_gate_before_anything_is_published(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        work, cache = self.state / "work" / TAG, self.state / "model-cache"
        fetch = calls[self.index_of(calls, "--checksum " + PIN["QUERY_PACKAGE_CHECKSUM"])]   # the int8 query package
        for arg in ("model_package.py fetch", "--cache " + str(cache), "--graph " + PIN["QUERY_GRAPH"],
                    "--repo " + PIN["MODEL_REPO"], "--revision " + PIN["MODEL_REVISION"]):
            self.assertIn(arg + " ", fetch + " ")
        validations = [i for i, c in enumerate(calls) if c.startswith("validate --index")]
        self.assertEqual(len(validations), len(FAMILIES))
        for (name, prefix), i in zip(FAMILIES, validations):
            with self.subTest(family=name):
                fw = work / name
                args = calls[i].split()[1:]
                pairs = dict(zip(args[::2], args[1::2]))
                self.assertEqual(args[::2], ["--index", "--seforim-db", "--release", "--plan", "--warehouse", "--model",
                                             "--model-identity", "--onnx-runtime", "--threads", "--report"])
                self.assertEqual(pairs, {
                    "--index": str(work / "index" / "index"), "--seforim-db": str(work / "seforim.db"),
                    "--release": str(fw / "release"), "--plan": str(fw / "plan"),
                    "--warehouse": str(self.warehouse), "--model": str(cache / PIN["QUERY_PACKAGE_CHECKSUM"] / PIN["QUERY_GRAPH"]),
                    "--model-identity": str(self.model(name)), "--onnx-runtime": str(self.ort),
                    "--threads": pairs["--threads"], "--report": str(fw / "release" / "validation.json")})
                self.assertGreater(int(pairs["--threads"]), 0)
                self.assertEqual(calls[i + 1], "validate-env TMPDIR=" + str(fw))   # its installed set stays in the work dir
                after = calls[i:]
                self.assertTrue(self.index_of(calls[:i], "cli assemble --verify") < i
                                < i + self.index_of(after, "cli release-files") < i + self.index_of(after, "gh release create " + prefix + "-"))

    def test_a_release_the_validator_does_not_pass_is_never_published(self):
        for env, code in (({"VALIDATE_FAILS": "G6"}, 1), ({"VALIDATE_FAILS": "G3 G4"}, 1), ({"VALIDATE_UNREADABLE": "1"}, 2)):
            with self.subTest(env=env):
                self.calls.unlink(missing_ok=True)
                p, calls = self.run_driver(mode="base", TO_EMBED=0, **env)
                self.assertNotEqual(p.returncode, 0)
                self.assertIn(f"validate_semantic_vectors exited {code}", p.stdout)
                after = calls[self.index_of(calls, "validate --index"):]
                self.assertFalse([c for c in after if c.startswith("gh ") or "release-files" in c])
                self.assertFalse([c for c in calls if "gh release create" in c or "gh release upload" in c
                                  or "gh release edit" in c])
                self.assertFalse((self.state / "ledger").exists())

    def test_a_dry_run_the_validator_does_not_pass_fails_too(self):
        p, calls = self.run_driver(TO_EMBED=0, VALIDATE_FAILS="G4")
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("validate_semantic_vectors exited 1", p.stdout)
        self.assertFalse([c for c in calls if "release-files" in c])

    def test_a_missing_validator_stops_the_build_before_it_starts(self):
        p, calls = self.run_driver(VALIDATE_SEMANTIC_VECTORS=str(self.root / "bin" / "no-validator"))
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("validate_semantic_vectors is not at", p.stdout)
        self.assertFalse([c for c in calls if "release download" in c])

    def test_the_stub_validator_refuses_what_the_real_one_refuses(self):
        release, index, plan = self.root / "rel", self.root / "idx", self.root / "plan"
        release.mkdir(); index.mkdir(); plan.mkdir()
        for f in ("segment.oxv", "release.json"):
            (release / f).write_text("x")
        (plan / "plan-manifest.json").write_text("{}")
        model, ort = self.root / "q.onnx", self.root / "libonnxruntime.so"
        model.write_text("x"); ort.write_text("x")
        g6 = ["--warehouse", str(self.warehouse), "--model", str(model), "--model-identity", str(self.model(LEGACY))]
        db = self.root / "seforim.db"
        db.write_text("x")
        base = ["--index", str(index), "--seforim-db", str(db), "--release", str(release)]
        report = self.root / "report.json"
        for args, code in ((base + g6, 0), (base + g6 + ["--onnx-runtime", str(ort), "--plan", str(plan)], 0),
                           (["--index", str(index), "--release", str(release)] + g6, 2),   # no --seforim-db
                           (["--index", str(index), "--seforim-db", str(self.root / "nowhere.db"), "--release", str(release)] + g6, 2),
                           (base + ["--report", str(report)], 1),                       # G6 not given: not run
                           (base + g6[:2], 2),                                          # G6 half given
                           (base + g6 + ["--frobnicate", "1"], 2),
                           (base + ["--skip", "G3", "--skip", "G4", "--skip", "G6"], 2),
                           (base + ["--skip", "G6"], 0),
                           (["--release", str(release)] + g6, 2),                      # no --index
                           (["--index", str(index)] + g6, 2),                          # no set
                           (base + g6 + ["--threads", "0"], 2),
                           (["--index", str(index), "--release", str(self.root)] + g6, 2),   # not a release
                           (base + g6 + ["--plan", str(self.root / "nowhere")], 2)):
            with self.subTest(args=args):
                p = subprocess.run([str(self.root / "bin" / "validate")] + args, capture_output=True, text=True,
                                   env=dict(os.environ, CALLS=str(self.calls)))
                self.assertEqual(p.returncode, code, p.stderr)
        self.assertEqual(json.loads(report.read_text())["gates"][2], {"gate": "G6", "status": "notRun"})

    def test_a_publish_is_a_draft_at_the_library_commit_with_the_data_first_and_the_manifest_last(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        # the commit is resolved before the build, from the library release's tag
        resolve = self.index_of(calls, "gh api repos/o/r/commits/" + TAG)
        self.assertLess(resolve, self.index_of(calls, "gh release download"))
        create = self.index_of(calls, "gh release create vectors-" + TAG)
        for arg in ("--draft", "--target " + TAG_COMMIT, "--latest=false"):
            self.assertIn(arg, calls[create].split(" --title")[0])
        for name, prefix in FAMILIES:
            with self.subTest(family=name):
                create = self.index_of(calls, f"gh release create {prefix}-{TAG} ")
                for arg in ("--draft", "--target " + TAG_COMMIT, "--latest=false"):
                    self.assertIn(arg, calls[create].split(" --title")[0])
                uploads = [c for c in calls if c.startswith(f"gh release upload {prefix}-{TAG} ")]
                self.assertTrue(all(self.index_of(calls, u) > create for u in uploads))
                names = [Path(u.split()[4]).name for u in uploads]
                self.assertEqual(names, [self.stem(name) + ".oxv.zst", "gates.json", "validation.json",
                                         self.stem(name) + ".manifest.json"])   # data first, the manifest last

    def test_the_draft_is_published_only_once_its_files_are_checked_and_then_the_ledger_is_persisted(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        edits = [i for i, c in enumerate(calls) if "gh release edit" in c]
        self.assertEqual(len(edits), len(FAMILIES))
        for (name, prefix), edit in zip(FAMILIES, edits):
            with self.subTest(family=name):
                release = prefix + "-" + TAG
                last_upload = max(i for i, c in enumerate(calls) if c.startswith(f"gh release upload {release} "))
                check = self.index_of(calls, "gh release view " + release + " --repo o/r --json assets")
                self.assertTrue(last_upload < check < edit)
                self.assertEqual(calls[edit], "gh release edit " + release + " --repo o/r --draft=false --latest=false")
                # nothing reaches the release after it is published
                self.assertFalse([c for c in calls[edit + 1:] if f" {release} " in c + " "])
                ledger = self.state / "ledger" / self.id8(name)
                expected = ["ledger-v30.keys", "ledger-v30.manifest.json", "pairs-v30.bin", "published.json"]
                if name == WINDOWS:
                    expected.append("occurrences-v30.bin")
                    self.assertEqual((ledger / "occurrences-v30.bin").read_text(), "windowed occurrence metadata\n")
                self.assertEqual(sorted(f.name for f in ledger.iterdir()), sorted(expected))
                published = json.loads((ledger / "published.json").read_text())
                self.assertEqual(published["published"], release)
                self.assertEqual(published["manifest"], self.stem(name) + ".manifest.json")
        self.assertEqual(edits[-1], len(calls) - 1)
        self.assertEqual(sorted(p.name for p in (self.state / "ledger").iterdir()), sorted(self.id8(n) for n, _ in FAMILIES))
        self.assertFalse((self.state / "work" / TAG).exists())

    def test_a_publish_is_a_prerelease_unless_the_variable_says_false(self):
        for value, flag in ((None, "--prerelease=true"), ("", "--prerelease=true"), ("true", "--prerelease=true"),
                            ("false", "--prerelease=false")):
            self.calls.unlink(missing_ok=True)
            (self.root / "uploaded.tsv").unlink(missing_ok=True)
            shutil.rmtree(self.state / "ledger", ignore_errors=True)
            env = {} if value is None else {"LIBRARY_VECTORS_PRERELEASE": value}
            p, calls = self.run_driver(mode="base", TO_EMBED=0, **env)
            self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
            self.assertIn(" " + flag + " ", calls[self.index_of(calls, "gh release create")], value)

    def test_a_mistyped_prerelease_value_stops_before_anything_runs(self):
        p, calls = self.run_driver(mode="base", LIBRARY_VECTORS_PRERELEASE="yes")
        self.assertEqual(p.returncode, 64)
        self.assertIn("LIBRARY_VECTORS_PRERELEASE is true or false", p.stdout)
        self.assertEqual(calls, [])

    def test_a_draft_that_does_not_hold_the_files_built_is_never_published(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0, SHORT_UPLOAD="gates.json")
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("it stays a draft", p.stdout)
        self.assertFalse([c for c in calls if "release edit" in c])
        self.assertFalse((self.state / "ledger").exists())

    def test_a_library_tag_that_names_no_commit_stops_before_the_build(self):
        p, calls = self.run_driver(mode="base", TAG_COMMIT="not-a-commit")
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("not a commit", p.stdout)
        self.assertFalse([c for c in calls if "release download" in c or "release create" in c])

    def test_a_publish_needs_an_authenticated_gh(self):
        p, calls = self.run_driver(mode="base", GH_UNAUTHENTICATED=1)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("gh is not authenticated", p.stdout)
        self.assertFalse([c for c in calls if "release download" in c or "release create" in c])

    def test_a_failed_gate_stops_before_anything_is_published(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0, VERIFY_EXIT=2)
        self.assertEqual(p.returncode, 2)
        self.assertIn("a gate failed", p.stdout)
        self.assertFalse([c for c in calls if "release create" in c or "release upload" in c or "release edit" in c
                          or "release-files" in c])
        self.assertFalse((self.state / "ledger").exists())

    def test_an_existing_vectors_release_is_never_rebuilt_in_place(self):
        for existing in ("1", "vectors-" + TAG, "vectors-windows-" + TAG):
            with self.subTest(existing=existing):
                self.calls.unlink(missing_ok=True)
                p, calls = self.run_driver(mode="base", EXISTING=existing)
                self.assertNotEqual(p.returncode, 0)
                self.assertIn("exists already", p.stdout)
                self.assertFalse([c for c in calls if "release download" in c])

    def test_a_damaged_index_part_is_refused(self):
        part = self.root / "assets" / TAG / "otzaria-library-index.tar.zst.part-001"
        part.write_bytes(part.read_bytes() + b"x")
        p, calls = self.run_driver()
        self.assertNotEqual(p.returncode, 0)
        self.assertFalse([c for c in calls if c.startswith("export ")])

    def assert_refused_before_the_parts(self, p, calls, needle="provenance"):
        self.assertNotEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn(needle, p.stdout)
        self.assertFalse([c for c in calls if ".part-" in c and ("release download" in c or c.startswith("curl "))])   # nothing large is fetched
        self.assertFalse([c for c in calls if c.startswith("export ") or "release create" in c or "release edit" in c])
        self.assertFalse((self.state / "ledger").exists())

    def test_an_index_whose_provenance_is_not_this_releases_is_refused_before_its_parts_download(self):
        audit = json.dumps({"schemaVersion": 1, "libraryReleaseTag": "v29-20260801000000", "indexArchive": "unrelated.tar.zst",
                            "indexArchiveSha256": "0" * 64, "seforimDbZstSha256": "f" * 64, "searchEngineVersion": "0.8.7"})
        prov = "the index provenance"
        cases = (({"libraryReleaseTag": "v29-20260927072953"}, {}, "another release"), ({"libraryReleaseTag": None}, {}, prov),
                 ({"indexArchiveSha256": "0" * 64}, {}, prov), ({"indexArchiveSha256": None}, {}, prov),
                 ({"indexArchive": "unrelated.tar.zst"}, {}, prov), ({"indexArchive": None}, {}, prov),
                 ({"seforimDbZstSha256": "f" * 64}, {}, "another database"), ({"seforimDbZstSha256": None}, {}, prov),
                 ({"schemaVersion": 2}, {}, prov), ({"schemaVersion": None}, {}, prov),
                 (audit, {}, prov), ("not json", {}, prov), ("", {}, prov),
                 # absent on both sides is no match
                 ({"indexArchiveSha256": None}, {"sha256": None}, "the index manifest"),
                 ({"indexArchive": None}, {"archive": None}, "the index manifest"),
                 ({}, {"archive": "otzaria-library-index.tar.gz"}, "the index manifest"))
        manifest = json.loads(self.manifest.read_text())
        for changes, man, needle in cases:
            with self.subTest(provenance=changes, manifest=man):
                self.calls.unlink(missing_ok=True)
                (self.root / "uploaded.tsv").unlink(missing_ok=True)
                shutil.rmtree(self.state / "ledger", ignore_errors=True)
                if isinstance(changes, str):
                    self.provenance.write_text(changes)
                else:
                    self.write_provenance(**changes)
                self.manifest.write_text(json.dumps({k: v for k, v in {**manifest, **man}.items() if v is not None}))
                p, calls = self.run_driver(mode="base", TO_EMBED=0)
                self.assert_refused_before_the_parts(p, calls, needle)

    def assert_the_planner_and_the_validator_read_the_database(self, calls):
        db = self.state / "work" / TAG / "seforim.db"
        self.assertEqual(db.read_bytes(), self.db_bytes)   # the stub zstd passes it through
        self.assertTrue([c for c in calls if c.startswith("zstd -dc --long=31 --memory=2048MB")])
        for tool in ("export --index", "validate --index"):
            self.assertIn("--seforim-db " + str(db) + " ", calls[self.index_of(calls, tool)])
        # the compressed DB is removed once expanded; only its manifest, if split, stays
        self.assertFalse([p for p in (self.state / "work" / TAG / "dl").iterdir()
                          if p.name.startswith("seforim-schema6.db.zst") and not p.name.endswith(".manifest.json")])

    def test_an_index_whose_provenance_is_this_releases_is_built_with_the_database_held_to_that_digest(self):
        p, calls = self.run_driver(TO_EMBED=0)   # a dry run keeps its work directory
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("0.8.7 engine", p.stdout)
        self.assertTrue(self.index_of(calls, "gh api repos/o/r/releases/tags/" + TAG)
                        < self.index_of(calls, "--pattern otzaria-library-index.tar.zst.part-000"))
        db = next(i for i, c in enumerate(calls) if c.endswith("--pattern seforim-schema6.db.zst"))
        self.assertLess(self.index_of(calls, "--pattern otzaria-library-index.tar.zst.part-001"), db)
        self.assertLess(db, self.index_of(calls, "export --index"))
        self.assert_the_planner_and_the_validator_read_the_database(calls)
        p, calls = self.run_driver(mode="base", TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)

    def test_a_split_database_is_identified_by_its_manifest_and_assembled_from_its_parts_held_to_it(self):
        assets = self.root / "assets" / TAG
        (assets / "seforim-schema6.db.zst").unlink()
        half = len(self.db_bytes) // 2
        chunks = {"seforim-schema6.db.zst.part-000": self.db_bytes[:half], "seforim-schema6.db.zst.part-001": self.db_bytes[half:]}
        for name, b in chunks.items():
            (assets / name).write_bytes(b)
        parts = [{"name": n, "size": len(b), "sha256": hashlib.sha256(b).hexdigest()} for n, b in chunks.items()]
        split = {"schemaVersion": 1, "archive": "seforim-schema6.db.zst", "size": len(self.db_bytes), "sha256": self.db_sha,
                 "parts": parts}
        cases = ((split, None, None),
                 # refused on the manifest alone, before any part (the index's or the DB's) downloads
                 (dict(split, sha256="f" * 64), "provenance", "before"), (dict(split, sha256=None), "provenance", "before"),
                 # refused once its parts are read, before anything is planned
                 (dict(split, parts=[dict(parts[0], sha256="a" * 64), parts[1]]), "does not hash to its manifest", "after"),
                 (dict(split, parts=parts[::-1]), "hashes to", "after"),
                 (dict(split, parts=[dict(parts[0], name="../seforim-schema6.db.zst.part-000")]), "unexpected part", "after"),
                 (dict(split, parts=[dict(parts[0], name="seforim-schema6.db.zst.part-000 x")]), "unexpected part", "after"),
                 (dict(split, parts=[dict(parts[0], name="seforim-schema6.db.zst.part-")]), "unexpected part", "after"),
                 (dict(split, parts=[dict(parts[0], name="otzaria-library-index.tar.zst.part-000")]), "unexpected part", "after"),
                 (dict(split, parts=[]), "lists no parts", "after"))
        for manifest, needle, when in cases:
            with self.subTest(manifest=manifest):
                self.calls.unlink(missing_ok=True)
                (self.root / "uploaded.tsv").unlink(missing_ok=True)
                shutil.rmtree(self.state / "ledger", ignore_errors=True)
                (assets / "seforim-schema6.db.zst.manifest.json").write_text(
                    json.dumps({k: v for k, v in manifest.items() if v is not None}))
                p, calls = self.run_driver(TO_EMBED=0)
                self.assertTrue([c for c in calls if "--pattern seforim-schema6.db.zst.manifest.json" in c])
                if needle is None:
                    self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
                    for name in chunks:
                        self.assertTrue([c for c in calls if c.endswith("--pattern " + name)])
                    self.assert_the_planner_and_the_validator_read_the_database(calls)
                elif when == "before":
                    self.assert_refused_before_the_parts(p, calls, needle)
                else:
                    self.assertNotEqual(p.returncode, 0, p.stdout + p.stderr)
                    self.assertIn(needle, p.stdout)
                    self.assertFalse([c for c in calls if c.startswith("export ") or c.startswith("validate ")])
                    self.assertFalse((self.state / "work" / TAG / "seforim.db").exists())

    def test_a_database_that_does_not_expand_is_refused_before_the_plan(self):
        p, calls = self.run_driver(TO_EMBED=0, ZSTD_FAIL=1)
        self.assertNotEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("could not expand seforim-schema6.db.zst", p.stdout)
        self.assertFalse([c for c in calls if c.startswith("export ")])

    def test_a_release_that_states_no_digest_of_its_database_is_refused(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0, NO_DIGEST=1)
        self.assert_refused_before_the_parts(p, calls, "no sha256")
        (self.root / "assets" / TAG / "seforim-schema6.db.zst").unlink()
        self.calls.unlink()
        p, calls = self.run_driver(mode="base", TO_EMBED=0)
        self.assert_refused_before_the_parts(p, calls, "no full DB")

    def test_state_and_work_paths_with_spaces_and_odd_characters_build_and_publish(self):
        odd = self.root / "state with spaces & 'quotes' |pipes| back\\slash"
        self.state.rename(odd)
        self.state = odd
        for n in (0, 5, int(PIN["CPU_EMBED_MAX"])):   # the warehouse alone, the CPU, the GPU
            with self.subTest(texts=n):
                self.calls.unlink(missing_ok=True)
                (self.root / "uploaded.tsv").unlink(missing_ok=True)
                p, calls = self.run_driver(mode="base", TO_EMBED=n)
                self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
                self.assertTrue([c for c in calls if "gh release edit" in c])
                for name, _ in FAMILIES:
                    self.assertTrue((odd / "ledger" / self.id8(name) / "published.json").exists())
        work = self.root / "work with spaces & 'quotes' |pipes|"
        p, calls = self.run_driver(TO_EMBED=5, args=("--work", str(work)))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertTrue((work / "index" / "index" / "meta.json").exists())

    def test_newlines_in_state_and_work_reach_release_files_as_one_path(self):
        odd = self.root / "state\nwith newline"
        self.state.rename(odd)
        self.state = odd
        for work in (odd / "work" / TAG, self.root / "work\nwith newline\n"):
            with self.subTest(work=work):
                (self.root / "release-file-args.json").unlink(missing_ok=True)
                p, _ = self.run_driver(TO_EMBED=0, args=("--work", str(work)))
                self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
                files = [json.loads(l) for l in (self.root / "release-file-args.json").read_text().splitlines()]
                self.assertEqual(files, [[str(work / name / "files" / (self.stem(name) + ".oxv.zst"))] for name, _ in FAMILIES])
                self.assertTrue(all(Path(f[0]).is_file() for f in files))

    def test_work_overlapping_state_or_persistent_paths_is_refused_without_changes(self):
        alias = self.root / "state-alias"
        alias.symlink_to(self.state, target_is_directory=True)
        for persistent in ("ledger", "ledger.next", "ledger.prev", "model-cache"):
            (self.state / persistent).mkdir()
            (self.state / persistent / "keep").write_text("persistent content")
        external = self.root / "external-cache"
        external.mkdir()
        (external / "keep").write_text("symlinked content")
        shutil.rmtree(self.state / "model-cache")
        (self.state / "model-cache").symlink_to(external, target_is_directory=True)
        works = [self.root, self.state, alias, alias / "work" / ".." / "..",
                 self.warehouse.parent, self.warehouse, self.warehouse / "new-scratch",
                 alias / "bin" / "new-scratch", self.state / "venv", self.state / ".lock",
                 external, external / "new-scratch", self.root / "bin"]
        works += [self.state / name for name in ("ledger", "ledger.next", "ledger.prev")]
        before = {p: p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        for work in works:
            with self.subTest(work=work):
                p, calls = self.run_driver(args=("--work", str(work)))
                self.assertNotEqual(p.returncode, 0)
                self.assertIn("persistent", p.stdout + p.stderr)
                self.assertEqual(calls, [])
                self.assertEqual({path: path.read_bytes() for path in before}, before)
                self.assertFalse((self.state / ".lock").exists())

    def test_work_beside_persistent_paths_can_clear_old_scratch_and_build(self):
        work = self.state / "warehouse-scratch"
        work.mkdir()
        (work / "old-scratch").write_text("replace me")
        p, _ = self.run_driver(TO_EMBED=0, args=("--work", str(work)))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertFalse((work / "old-scratch").exists())
        self.assertTrue((work / LEGACY / "files" / (self.stem(LEGACY) + ".oxv.zst")).is_file())

    def test_work_cannot_delete_custom_gh_or_zstd_executables(self):
        for variable, tool in (("GH", "gh"), ("ZSTD", "zstd")):
            with self.subTest(tool=tool):
                work = self.root / (tool + "-installation")
                (work / "bin").mkdir(parents=True)
                executable = work / "bin" / tool
                shutil.copyfile(self.root / "bin" / tool, executable)
                executable.chmod(0o755)
                before = executable.read_bytes()
                p, calls = self.run_driver(args=("--work", str(work)), **{variable: executable})
                self.assertNotEqual(p.returncode, 0)
                self.assertIn("persistent", p.stdout + p.stderr)
                self.assertEqual(calls, [])
                self.assertEqual(executable.read_bytes(), before)

    def test_an_index_manifest_that_names_no_part_of_its_archive_is_refused(self):
        manifest = json.loads(self.manifest.read_text())
        part = manifest["parts"][0]
        for parts, needle in (([dict(part, name="../otzaria-library-index.tar.zst.part-000")], "unexpected part"),
                              ([dict(part, name="otzaria-library-index.tar.zst.part-000 x")], "unexpected part"),
                              ([dict(part, name="seforim-schema6.db.zst")], "unexpected part"),
                              ([], "lists no parts")):
            with self.subTest(parts=parts):
                self.calls.unlink(missing_ok=True)
                self.manifest.write_text(json.dumps(dict(manifest, parts=parts)))
                p, calls = self.run_driver()
                self.assert_refused_before_the_parts(p, calls, needle)
                self.assertFalse([c for c in calls if "release download" in c and "seforim-schema6.db.zst" in c])

    def test_without_gh_the_release_is_read_anonymously_and_held_to_its_provenance(self):
        assets = self.root / "assets" / TAG
        api = f"curl -fsSL https://api.github.com/repos/o/r/releases/tags/{TAG} -o "
        download = f"https://github.com/o/r/releases/download/{TAG}/"

        def run(**env):
            self.calls.unlink(missing_ok=True)
            p, calls = self.run_driver(GH_UNAUTHENTICATED=1, TO_EMBED=0, **env)
            self.assertEqual([c for c in calls if c.startswith("gh ")], ["gh auth status"])
            return p, calls

        for label, prov, env, needle in (("this release's index", {}, {}, None),
                                         ("the audit's v29 provenance", {"libraryReleaseTag": "v29-20260801000000"}, {}, "another release"),
                                         ("no digest", {}, {"NO_DIGEST": 1}, "no sha256"),
                                         ("the release API fails", {}, {"CURL_FAIL": "api"}, "could not read the release"),
                                         ("no published_at", {}, {"NO_PUBLISHED_AT": 1}, "could not read when")):
            with self.subTest(label):
                self.write_provenance(**prov)
                p, calls = run(**env)
                if needle:
                    self.assert_refused_before_the_parts(p, calls, needle)
                    self.assertFalse([c for c in calls if c.endswith(download + "seforim-schema6.db.zst")])
                    continue
                self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
                self.assertTrue([c for c in calls if c.startswith(api)])
                self.assertTrue([c for c in calls if c.endswith(download + "otzaria-library-index.tar.zst.part-001")])
                self.assertTrue([c for c in calls if c.endswith(download + "seforim-schema6.db.zst")])
                self.assertIn("--created-at 2026-09-30T21:38:29Z", calls[self.index_of(calls, "export --index")])
                self.assert_the_planner_and_the_validator_read_the_database(calls)
        with self.subTest("a split DB"):
            self.write_provenance()
            (assets / "seforim-schema6.db.zst").unlink()
            (assets / "seforim-schema6.db.zst.part-000").write_bytes(self.db_bytes)
            (assets / "seforim-schema6.db.zst.manifest.json").write_text(json.dumps(
                {"archive": "seforim-schema6.db.zst", "sha256": self.db_sha,
                 "parts": [{"name": "seforim-schema6.db.zst.part-000", "size": len(self.db_bytes), "sha256": self.db_sha}]}))
            p, calls = run()
            self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
            self.assertTrue([c for c in calls if c.endswith(download + "seforim-schema6.db.zst.manifest.json")])
            self.assertTrue([c for c in calls if c.endswith(download + "seforim-schema6.db.zst.part-000")])
            self.assert_the_planner_and_the_validator_read_the_database(calls)
        with self.subTest("an asset the release lacks stops the build at its download"):
            (assets / "otzaria-library-index.provenance.json").unlink()
            p, calls = run()
            self.assertEqual(p.returncode, 22, p.stdout + p.stderr)
            self.assertIn("curl: (22) The requested URL returned error: 404", p.stderr)
            self.assertTrue(calls[-1].endswith(download + "otzaria-library-index.provenance.json"))

    def test_a_plan_for_another_release_is_refused(self):
        p, calls = self.run_driver(PLAN_TAG="v29-20260927072953")
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("another release", p.stdout)
        self.assertFalse([c for c in calls if "assemble" in c])

    def test_only_database_tags_are_accepted(self):
        for tag in ("vectors-v30-20260930165019", "v30", "lines-snapshot-sha256-" + "a" * 40):
            p, calls = self.run_driver(tag=tag)
            self.assertEqual(p.returncode, 64, tag)
            self.assertEqual(calls, [])

    def test_the_planner_is_given_the_model_the_warehouse_and_the_release(self):
        p, calls = self.run_driver(TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        plan = calls[self.index_of(calls, "export --index")]
        for arg in ("--library-version 30", "--release-tag " + TAG, "--model ", "--passage-quantization fp32",
                    "--warehouse ", "--created-at 2026-09-30T21:38:29Z", "--out "):
            self.assertIn(arg, plan)
        self.assertNotIn("--chunking", plan)   # the recipe comes from the model
        # one planner for every family, each with its own model, into its own work directory
        exports = [c for c in calls if c.startswith("export --index")]
        work = self.state / "work" / TAG
        self.assertEqual(len(exports), len(FAMILIES))
        for (name, _), c in zip(FAMILIES, exports):
            self.assertIn(f"--model {self.model(name)} ", c)
            self.assertIn(f"--out {work / name / 'plan'}", c)

    def test_every_family_is_built_in_order_and_the_first_is_published_before_the_next_runs(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        # the index and the DB are fetched once, and the warehouse verified once, for every family
        self.assertEqual(len([c for c in calls if c.endswith("--pattern otzaria-library-index.tar.zst.part-000")]), 1)
        self.assertEqual(len([c for c in calls if c.endswith("--pattern seforim-schema6.db.zst")]), 1)
        self.assertEqual(len([c for c in calls if "warehouse-verify" in c]), 1)
        published = self.index_of(calls, f"gh release edit vectors-{TAG} ")
        self.assertLess(published, self.index_of(calls, f"--model {self.model(WINDOWS)} "))
        self.assertLess(published, self.index_of(calls, f"gh release create vectors-windows-{TAG} "))
        self.assertNotEqual(self.id8(LEGACY), self.id8(WINDOWS))   # their assets are told apart by the identity
        notes = [c for c in calls if c.startswith("gh release create")]
        self.assertEqual([c.split()[3] for c in notes], [prefix + "-" + TAG for _, prefix in FAMILIES])

    def test_the_first_familys_notes_are_as_before_and_a_later_ones_name_it(self):
        p, _ = self.run_driver(mode="base", TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        legacy = (self.root / f"uploaded.tsv.notes.vectors-{TAG}").read_text().splitlines()
        self.assertEqual(legacy[0], f"Semantic vectors for library release {TAG} (base).")
        self.assertEqual([l.split(":")[0] for l in legacy[1:] if l], ["Manifest", "Gates"])
        windows = (self.root / f"uploaded.tsv.notes.vectors-windows-{TAG}").read_text()
        self.assertIn(f"Family: `{WINDOWS}`, chunking identity `{CHUNKING_IDENTITY[WINDOWS]}`.", windows)
        self.assertIn("Source text: 8700 / 10000 normalized characters", windows)
        self.assertIn("1300 outside, 2 partially windowed lines, 0 nonempty lines with no window", windows)
        self.assertIn("does not certify complete source text coverage or semantic recall", windows)

    def test_a_family_leaves_only_what_it_needs_before_the_next_starts(self):
        for mode in ("base", "dry-run"):
            with self.subTest(mode=mode):
                self.calls.unlink(missing_ok=True)
                (self.root / "uploaded.tsv").unlink(missing_ok=True)
                shutil.rmtree(self.state / "ledger", ignore_errors=True)
                p, calls = self.run_driver(mode=mode, TO_EMBED=5)
                self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
                works = [c.split()[1:] for c in calls if c.startswith("export-work")]
                self.assertEqual(len(works), len(FAMILIES))
                self.assertEqual((LEGACY in works[0], WINDOWS in works[0]), (True, False))   # its own, and none after it
                # a published family's directory goes before the next family plans; a dry run's stays
                self.assertEqual(LEGACY in works[1], mode == "dry-run")
                self.assertEqual(len([c for c in calls if c.startswith("cli warehouse-add")]), len(FAMILIES))
                if mode == "dry-run":
                    for name, _ in FAMILIES:
                        kept = sorted(f.name for f in (self.state / "work" / TAG / name).iterdir())
                        self.assertEqual(kept, ["files", "plan", "plan.log", "release", "release-files.log"])   # no shards
                    self.assertIn(" kept in ", p.stdout)

    def test_each_family_needs_the_free_space_when_it_starts(self):
        # a df that answers DF_FREE_KB's values in turn: the preflight's, then each family's
        df = self.root / "df-stub"
        df.mkdir()
        (df / "df").write_text(STUB_DF)
        (df / "df").chmod(0o755)
        path = os.pathsep.join([str(df), str(self.root / "net"), os.environ["PATH"]])
        p, calls = self.run_driver(mode="base", TO_EMBED=0, PATH=path, VECTORS_MIN_FREE_GB=1,
                                   DF_COUNT=self.root / "df-count", DF_FREE_KB="2000000 2000000 10")
        self.assertEqual(p.returncode, 1, p.stdout + p.stderr)
        self.assertIn("::error::only 0 GiB free on", p.stdout)
        self.assertIn(f"vectors-{TAG} stay published", p.stdout)
        self.assertTrue([c for c in calls if c.startswith(f"gh release edit vectors-{TAG} ")])
        self.assertFalse([c for c in calls if f"--model {self.model(WINDOWS)}" in c])

    def test_a_later_family_that_fails_leaves_the_first_published(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0, VALIDATE_FAILS_FOR=WINDOWS)
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertIn(f"the family {WINDOWS} failed; vectors-{TAG} stay published — retry it alone with --family {WINDOWS}", p.stdout)
        self.assertTrue([c for c in calls if c.startswith(f"gh release edit vectors-{TAG} ")])
        self.assertFalse([c for c in calls if f"vectors-windows-{TAG}" in c and ("release create" in c or "release edit" in c)])
        self.assertEqual([p.name for p in (self.state / "ledger").iterdir()], [self.id8(LEGACY)])
        self.assertTrue((self.state / "work" / TAG / "seforim.db").exists())   # the work directory stays, as after any failure

    def test_a_first_family_that_fails_stops_before_the_next(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0, VALIDATE_FAILS_FOR=LEGACY)
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertNotIn("stay published", p.stdout)
        self.assertFalse([c for c in calls if c.startswith("gh release create") or f"--model {self.model(WINDOWS)}" in c])

    def test_one_family_is_built_alone_when_named(self):
        p, calls = self.run_driver(mode="base", TO_EMBED=0, EXISTING="vectors-" + TAG, args=("--family", WINDOWS))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertEqual([c.split()[3] for c in calls if c.startswith("gh release create")], [f"vectors-windows-{TAG}"])
        self.assertFalse([c for c in calls if f"--model {self.model(LEGACY)}" in c])
        self.assertFalse([c for c in calls if c.startswith(f"gh release view vectors-{TAG} ")])
        self.calls.unlink()
        p, calls = self.run_driver(TO_EMBED=0, args=("--family", "all"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertEqual(len([c for c in calls if c.startswith("export --index")]), len(FAMILIES))

    def test_a_family_that_is_not_pinned_is_refused_before_anything_runs(self):
        for args, needle in ((("--family", "meivin-round2"), "is not a family of FAMILY_CONFIGS"),
                             (("--family", WINDOWS, "--family", "vectors-windows"), "is not a family of FAMILY_CONFIGS"),
                             (("--family", "all", "--family", WINDOWS), "--family all and --family"),
                             (("--family", WINDOWS, "--family", "all"), "--family all and --family"),
                             (("--family",), "--family needs a value")):
            with self.subTest(args=args):
                self.calls.unlink(missing_ok=True)
                p, calls = self.run_driver(args=args)
                self.assertEqual(p.returncode, 64, p.stdout + p.stderr)
                self.assertIn(needle, p.stdout)
                self.assertEqual(calls, [])

    def test_a_family_whose_identity_the_runner_lacks_stops_the_build_before_it_starts(self):
        shutil.rmtree(self.state / "bin" / "families" / WINDOWS)
        p, calls = self.run_driver()
        self.assertNotEqual(p.returncode, 0)
        self.assertIn(f"families/{WINDOWS}/model.json is missing", p.stdout)
        self.assertFalse([c for c in calls if "release download" in c])

    def write_single_ledger(self, manifest):
        """The ledger as a build of one family persisted it: its files in ledger/ itself."""
        ledger = self.state / "ledger"
        ledger.mkdir()
        for f in ("ledger-v29.keys", "pairs-v29.bin", "ledger-v29.manifest.json"):
            (ledger / f).write_text(f)
        (ledger / "published.json").write_text(json.dumps({"published": "vectors-v29-20260927072953", "manifest": manifest}))

    def test_the_single_ledger_moves_under_its_familys_identity_once(self):
        self.write_single_ledger("otzaria-vectors-abcdef01-v29-base.manifest.json")
        p, calls = self.run_driver(mode="base", TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("ledger: moved to " + str(self.state / "ledger" / "abcdef01"), p.stdout)
        old = self.state / "ledger" / "abcdef01"
        self.assertEqual(sorted(f.name for f in old.iterdir()),
                         ["ledger-v29.keys", "ledger-v29.manifest.json", "pairs-v29.bin", "published.json"])
        self.assertEqual(sorted(p.name for p in (self.state / "ledger").iterdir()),
                         sorted(["abcdef01"] + [self.id8(n) for n, _ in FAMILIES]))
        self.assertFalse((self.state / "ledger.migrating").exists())
        p, calls = self.run_driver(TO_EMBED=0)   # once: the next build finds the per-family layout
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertNotIn("ledger: moved", p.stdout)

    def test_a_move_cut_short_is_finished_and_an_unknown_ledger_is_refused(self):
        moved = self.state / "ledger.migrating" / "abcdef01"
        moved.mkdir(parents=True)
        (moved / "published.json").write_text("{}")
        p, _ = self.run_driver(TO_EMBED=0)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertTrue((self.state / "ledger" / "abcdef01" / "published.json").exists())
        self.assertFalse((self.state / "ledger.migrating").exists())
        shutil.rmtree(self.state / "ledger")
        self.calls.unlink()
        for manifest in ("seforim-vectors.manifest.json", "otzaria-vectors-abcdef01-v-base.manifest.json",
                         "otzaria-vectors-abcdef01-v29x-base.manifest.json", None):
            with self.subTest(manifest=manifest):
                shutil.rmtree(self.state / "ledger", ignore_errors=True)
                self.calls.unlink(missing_ok=True)
                self.write_single_ledger(manifest or "")
                if manifest is None:
                    (self.state / "ledger" / "published.json").write_text("{not json")
                p, calls = self.run_driver(mode="base", TO_EMBED=0)
                self.assertEqual(p.returncode, 1, p.stdout + p.stderr)
                self.assertIn("::error::" + str(self.state / "ledger" / "published.json"), p.stdout)
                self.assertIn("its family is unknown", p.stdout)
                self.assertTrue((self.state / "ledger" / "published.json").exists())   # left as it was
                self.assertFalse([c for c in calls if "release download" in c])

STUB_GIT = r"""#!/usr/bin/env bash
echo "git $*" >> "$CALLS"
if [ "$1" = clone ]; then           # git clone -q --filter=blob:none <url> <dest>
  url=${@: -2:1}; dest=${@: -1}
  mkdir -p "$dest"
  case "$url" in *otzaria-semantic-search*)
    for family in meivin-round2-onnx meivin-round2-windows-onnx; do
      mkdir -p "$dest/config/models/$family"
      echo "{\"family_id\":\"$family\"}" > "$dest/config/models/$family/model.json"
      echo '{"chunking_version":1}' > "$dest/config/models/$family/chunking.json"
    done ;;
    *otzaria_search_engine*) mkdir -p "$dest/rust" ;;     # the plugin's crate
  esac
fi
"""

STUB_CARGO = r"""#!/usr/bin/env bash
echo "cargo $*" >> "$CALLS"
bins=(); while [ $# -gt 0 ]; do case "$1" in --bin) bins+=("$2"); shift 2;; *) shift;; esac; done
mkdir -p "$CARGO_TARGET_DIR/release"
for b in "${bins[@]}"; do printf '#!/bin/sh\n' > "$CARGO_TARGET_DIR/release/$b"; chmod +x "$CARGO_TARGET_DIR/release/$b"; done
"""

# a venv made before: its python stands in for pip and for the import check
STUB_VENV_PYTHON = r"""#!/usr/bin/env bash
echo "venv-python PIP_CERT=${PIP_CERT:-} $*" >> "$CALLS"
if [ "${1:-}" = - ]; then cat > /dev/null; echo "venv: stub"; fi
"""


@unittest.skipUnless(linux_tools(), "needs bash, flock and install")
class Bootstrap(unittest.TestCase):
    """bootstrap_runner.sh against stub git, cargo and venv: what it builds, records and hands pip."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        r = Path(self.tmp.name)
        self.root, self.calls, self.state = r, r / "calls.log", r / "state"
        (r / "bin").mkdir()
        python = self.state / "venv" / "bin" / "python"
        python.parent.mkdir(parents=True)
        for path, body in ((r / "bin" / "git", STUB_GIT), (r / "bin" / "cargo", STUB_CARGO), (python, STUB_VENV_PYTHON)):
            path.write_text(body)
            path.chmod(path.stat().st_mode | stat.S_IEXEC)
        self.ca = r / "system-ca.crt"
        self.ca.write_text("a stand-in for the system bundle")
        # the stubs, flock, and the system's tools: no other cargo
        self.path = os.pathsep.join([str(r / "bin"), os.path.dirname(shutil.which("flock")), "/usr/bin", "/bin"])

    def tearDown(self):
        self.tmp.cleanup()

    def run_bootstrap(self, args=(), **env):
        self.calls.unlink(missing_ok=True)
        e = {"PATH": self.path, "CALLS": str(self.calls), "HOME": str(self.root), "SYSTEM_CA": str(self.ca)}
        e.update({k: str(v) for k, v in env.items()})
        p = subprocess.run(["bash", str(BOOTSTRAP), "--state", str(self.state), *args], capture_output=True, text=True, env=e)
        return p, (self.calls.read_text().splitlines() if self.calls.exists() else [])

    def test_the_validator_is_built_able_to_run_g6_and_recorded_like_every_binary(self):
        p, calls = self.run_bootstrap()
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        b = self.state / "bin"
        for name, rev, build in (
                ("otzaria-semantic-search", PIN["SIDECAR_REV"], "--features onnx-backend --bin otzaria-semantic-search"),
                ("export_semantic_plan", PIN["PLUGIN_REV"], "--features semantic-integration --bin export_semantic_plan"),
                ("validate_semantic_vectors", PIN["PLUGIN_REV"], "--features semantic --bin validate_semantic_vectors")):
            self.assertTrue(os.access(b / name, os.X_OK), name)
            self.assertEqual((b / f"{name}.rev").read_text().strip(), rev, name)
            self.assertEqual((b / f"{name}.build").read_text().strip(), build, name)
        for name, _ in FAMILIES:
            self.assertEqual(json.loads((b / "families" / name / "model.json").read_text()), {"family_id": name})
            self.assertTrue((b / "families" / name / "chunking.json").exists())
        self.assertFalse((b / "family-model.json").exists() or (b / "chunking.json").exists())
        self.assertFalse((self.state / "build").exists())        # the build directory goes
        p, calls = self.run_bootstrap()                            # and a second run builds nothing
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertFalse([c for c in calls if c.startswith("cargo ")])

    def test_the_single_familys_files_make_way_for_every_familys(self):
        b = self.state / "bin"
        b.mkdir(parents=True)
        for f in ("family-model.json", "chunking.json"):
            (b / f).write_text("{}")
        p, calls = self.run_bootstrap()
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertFalse((b / "family-model.json").exists() or (b / "chunking.json").exists())
        self.assertEqual(sorted(d.name for d in (b / "families").iterdir()), sorted(n for n, _ in FAMILIES))

    def test_pip_is_handed_the_system_ca_bundle_unless_it_is_told_another(self):
        for env, expected in (({}, str(self.ca)), ({"PIP_CERT": "/elsewhere/bundle.pem"}, "/elsewhere/bundle.pem"),
                              ({"SYSTEM_CA": str(self.root / "no-bundle")}, "")):
            with self.subTest(env=env):
                p, calls = self.run_bootstrap(**env)
                self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
                pips = [c for c in calls if " -m pip install " in c]
                self.assertEqual(len(pips), 2)
                for c in pips:
                    self.assertTrue(c.startswith(f"venv-python PIP_CERT={expected} -m pip install "), c)

    def test_a_seed_package_is_hashed_whatever_the_state_path_holds(self):
        odd = self.root / "state with spaces & 'quotes'"
        self.state.rename(odd)
        self.state = odd
        python = odd / "venv" / "bin" / "python"   # the checksum runs in a real Python
        python.write_text(STUB_VENV_PYTHON + f'[ "${{1:-}}" != -c ] || exec "{sys.executable}" "$@"\n')
        seed = self.root / "seed"
        seed.mkdir()
        (seed / PIN["MODEL_GRAPH"]).write_bytes(b"not the pinned graph")
        (seed / "tokenizer.json").write_text("{}")
        p, calls = self.run_bootstrap(args=("--seed-model-dir", str(seed)))
        sys.path.insert(0, str(HERE))
        import model_package
        got = model_package.package_checksum(seed, PIN["MODEL_GRAPH"])
        self.assertEqual(p.returncode, 1, p.stdout + p.stderr)
        self.assertIn(f"the seed package hashes to {got}, not {PIN['PASSAGE_PACKAGE_CHECKSUM']}", p.stderr)
        self.assertFalse((odd / "model-cache" / PIN["PASSAGE_PACKAGE_CHECKSUM"]).exists())

    def test_with_no_cargo_it_says_to_install_rustup_first(self):
        (self.root / "bin" / "cargo").unlink()
        p, calls = self.run_bootstrap()
        self.assertEqual(p.returncode, 69)
        self.assertIn("install rustup", p.stderr)
        self.assertFalse((self.state / "bin" / "otzaria-semantic-search").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
