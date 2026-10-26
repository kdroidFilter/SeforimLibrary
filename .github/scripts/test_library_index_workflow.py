"""Contract tests for build-library-index.yml.

The library search index used to be rebuilt inside every Otzaria/otzaria
release build — 118 minutes of the 130-minute `Linux full x86_64` job of run
34779834547, with two further jobs waiting on it — although it is a pure
function of seforim.db and of the search engine. It is now built once per
database release, here, and stored on that release.

Two properties are load-bearing and invisible in a green run, so they are
pinned here:

  * this must never sit on the database release's critical path, and must never
    start on one of the per-build releases the pipeline publishes while the
    database build is still running;
  * the stored index must stay platform-neutral, which holds only while the
    bundled PDFs are kept out of it (a PDF's index key is its own absolute
    path, so indexing one on a runner would bake a runner path into an archive
    that Windows, macOS and Android clients consume).

Assertions read the parsed workflow, not its comment text.
"""

import contextlib
import importlib.util
import io
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

try:  # PyYAML ships with the ubuntu-latest image these jobs run on.
    import yaml
except ImportError:  # pragma: no cover - only on a runner without PyYAML
    yaml = None

SCRIPTS = Path(__file__).parent
WORKFLOWS = SCRIPTS.parent / "workflows"
INDEX = WORKFLOWS / "build-library-index.yml"
RELEASE = WORKFLOWS / "manual-generate-release.yml"
SPLIT = SCRIPTS / "split_release_asset.sh"
BOUNDED = SCRIPTS / "bounded_docker_run.sh"
EXTRACT = SCRIPTS / "extract_app_build_steps.py"
NOTE_ENGINE = SCRIPTS / "check_note_body_engine.py"

# manual-generate-release.yml composes the tag as v<dbVersion>-<utcTimestamp>
# ("Compute release tag"); the snapshot and hand-off releases it also publishes
# are named lines-snapshot-sha256-* and pipeline-result-*.
DATABASE_TAG = re.compile(r"^v[0-9]+-[0-9]{14}$")


def steps_of(job):
    return job.get("steps") or []


def body(job):
    return "\n".join(step["run"] for step in steps_of(job) if "run" in step)


def executed(job):
    """The job's shell, with comment lines removed."""
    return "\n".join(
        line for line in body(job).splitlines()
        if not line.lstrip().startswith("#")
    )


def commands(job):
    """One entry per logical shell command: `\\` continuations joined."""
    joined, pending = [], ""
    for line in executed(job).splitlines():
        stripped = line.strip()
        if stripped.endswith("\\"):
            pending += stripped[:-1].rstrip() + " "
            continue
        joined.append((pending + stripped).strip())
        pending = ""
    if pending:
        joined.append(pending.strip())
    return [c for c in joined if c]


@unittest.skipIf(yaml is None, "PyYAML unavailable on this runner")
class LibraryIndexWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = INDEX.read_text(encoding="utf-8")
        cls.doc = yaml.safe_load(cls.text)
        cls.gate = cls.doc["jobs"]["gate"]
        cls.index = cls.doc["jobs"]["index"]
        cls.release = yaml.safe_load(RELEASE.read_text(encoding="utf-8"))
        cls.bounded = BOUNDED.read_text(encoding="utf-8")

    # ─── when it runs ──────────────────────────────────────────────────────

    def test_it_starts_only_after_a_release_is_already_public(self):
        triggers = self.doc[True]  # YAML 1.1 reads a bare `on:` key as True
        self.assertEqual(triggers["release"], {"types": ["published"]})
        # `created` fires on a draft and `prereleased` would double up with
        # `published`; either would put this on the release's critical path.
        self.assertEqual(set(triggers), {"release", "workflow_dispatch"})

    def test_the_gate_admits_database_releases_and_nothing_else(self):
        gate = body(self.gate)
        pattern = re.search(r"grep -Eq '(\^v.*\$)'", gate)
        self.assertIsNotNone(pattern, "the gate no longer matches a tag pattern")
        self.assertEqual(pattern.group(1), DATABASE_TAG.pattern)
        for accepted in ("v1-20260910220310", "v28-20260910220310"):
            self.assertRegex(accepted, DATABASE_TAG)
        for refused in (
            "lines-snapshot-sha256-" + "a" * 40,
            "pipeline-result-run-34535234412-1",
            "v28",
            "fordb-latest",
        ):
            self.assertNotRegex(refused, DATABASE_TAG)

    def test_the_heavy_job_runs_only_behind_the_gate(self):
        self.assertEqual(self.index["needs"], "gate")
        self.assertEqual(self.index["if"], "needs.gate.outputs.proceed == 'true'")

    def test_a_mistyped_manual_tag_fails_instead_of_skipping(self):
        # A dispatch that silently decided "not a database release" would
        # report a green check over an index that was never built.
        self.assertIn(
            "::error::release_tag '$TAG' is not a database release tag",
            body(self.gate),
        )

    def test_a_queued_run_is_never_cancelled_or_superseded(self):
        concurrency = self.doc["concurrency"]
        self.assertEqual(concurrency["cancel-in-progress"], False)
        # GitHub keeps ONE pending run per group by default: a third release
        # would supersede the second, leaving that release with no index and
        # every later application build failing on it. Same durable queue the
        # release build uses.
        self.assertEqual(concurrency["queue"], "max")
        self.assertEqual(
            concurrency["queue"], self.release["concurrency"]["queue"]
        )

    # ─── where it runs ─────────────────────────────────────────────────────

    def test_it_offers_the_same_three_servers_as_the_database_build(self):
        choice = self.doc[True]["workflow_dispatch"]["inputs"]["runner_selection"]
        self.assertEqual(choice["default"], "local")
        self.assertEqual(choice["options"], ["local", "ARM64", "server-2"])
        # Same label sets, so "the machine the database was built on" means the
        # same machine in both workflows.
        release_runs_on = self.release["jobs"]["build-and-release"]["runs-on"]
        for labels in ("otzaria-db", "self-hosted", "ARM64", "server-2"):
            self.assertIn(labels, release_runs_on)
            self.assertIn(labels, body(self.gate))

    def test_the_index_job_resolves_its_runner_from_the_gate(self):
        self.assertEqual(
            self.index["runs-on"], "${{ fromJSON(needs.gate.outputs.runner_labels) }}"
        )

    def test_it_refuses_to_start_without_docker_or_disk(self):
        preflight = body(self.index)
        self.assertIn("::error::docker is required on this runner", preflight)
        self.assertIn("Refusing to start.", preflight)

    # ─── what it produces ──────────────────────────────────────────────────

    def test_the_catalogue_tree_matches_what_the_application_will_compute(self):
        run = body(self.index)
        # catalogueOrder is a POSITION in the whole tree and is baked into every
        # document id, while the application's staleness check compares only
        # kCatalogueOrderRevision — so a tree missing the bundled volumes ships
        # a silently mis-ordered index. They are laid down but never indexed
        # (IndexingRepository.isIndexableBook excludes them since 9b67127).
        self.assertIn("talmud_bavli_latest.tar.zst", run)
        self.assertIn("::error::books/תלמוד בבלי is missing", run)
        self.assertIn("::error::books/seforim.db is missing", run)
        # What is pinned is the volume NAME SET, not the archive bytes: names
        # decide the order, so a re-scan of the same tractates must not fail a
        # build and a changed set must. The two sides compare digests of the
        # same pipeline, so it has to appear here verbatim.
        self.assertIn("talmudVolumesDigest: $talmudVolumesDigest", run)
        pipeline = r"tar -tf - | sed 's#.*/##' | grep -i '\.pdf$'"
        self.assertIn(pipeline, run)
        self.assertIn("LC_ALL=C sort -u", run)
        # ...and the names in the archive are proved against what landed on
        # disk, which is the only thing that catches an extraction the
        # application would never see.
        self.assertIn('cmp -s "$WORK/volumes.txt" "$WORK/volumes-on-disk.txt"', run)
        # isTalmudBavliInstallInProgress hides the whole directory when .version
        # reads 'installing'; writing the digest makes that unreachable.
        self.assertIn('> "$WORK/books/תלמוד בבלי/.version"', run)
        self.assertIn("talmudBavliSha256: $talmudBavliSha256", run)

    def test_the_display_is_brought_up_on_an_observable_condition(self):
        # Comments are stripped: this asserts on what runs, and the step's
        # comments name the wrapper they exist to explain the absence of.
        run = executed(self.index)
        # xvfb-run blocks in `wait` for a SIGUSR1 from Xvfb and has no ceiling
        # of its own, so when that signal does not arrive it is silent forever:
        # runs 34845248197 (2h06m), 34871116986 (2h07m) and 34886226177 (killed
        # at 150s) all died that way, while the identical command exited 0 in a
        # second in probe run 34885339600. Intermittent and unobservable is the
        # one thing this step may not be, so the wrapper is gone and the
        # display is waited for on its socket.
        self.assertNotIn("xvfb-run", run)
        self.assertIn("Xvfb :99 -screen 0 1280x1024x24 -nolisten tcp", run)
        self.assertIn("[ -e /tmp/.X11-unix/X99 ] && break", run)
        self.assertIn("export DISPLAY=:99", run)
        # A dead Xvfb must end the wait at once rather than burn the full 20s,
        # and either way its own log is what the failure prints.
        self.assertIn('kill -0 "$xvfb"', run)
        self.assertIn("cat /tmp/xvfb.log", run)
        # Nothing may be retried or guessed around it.
        self.assertNotIn("SERVERNUM", run)

    def test_every_command_that_can_stall_carries_a_bound(self):
        # The hang this job had was in a container, but it is not the only
        # place one can happen: `docker build` resolves an image and installs
        # ~40 packages over the network, and the downloads and the upload move
        # gigabytes. A socket that stays open without transferring hangs any of
        # them for as long as the job's own ceiling allows, silently.
        run = executed(self.index)
        stallers = (
            "docker build",
            "docker run",
            "gh release download",
            "gh release upload",
        )
        for command in commands(self.index):
            for staller in stallers:
                if command.startswith(staller):
                    self.fail(f"unbounded: {command}")
        for staller in stallers:
            self.assertIn(staller, run + self.bounded, f"{staller} vanished from the job")
        # Every container goes through the one helper that bounds it.
        self.assertIn('timeout --kill-after=60 "$bound" docker run', self.bounded)
        self.assertNotRegex(run, r"(?m)^\s*docker run")
        self.assertEqual(run.count('timeout --kill-after=30 5m gh api "repos/$OTZARIA_REPO/'), 3)
        # curl's --retry does not fire for a connection that stays open and
        # stops transferring; only a speed floor ends that.
        self.assertIn("--speed-limit", run)
        self.assertIn("--speed-time", run)
        # zstd -19 over the whole index, and the packing it feeds.
        self.assertIn("timeout --kill-after=60 180m bash -c", run)

    def test_a_degraded_index_is_reported_but_never_refused(self):
        # It used to be a hard bound at 8. Runs 34977132429 (67 -> 10) and
        # 34978573727 (194 -> 8) came out differently on byte-identical
        # inputs — multi-threaded indexing decides the shape optimize is
        # handed — so the bound failed builds at random over a difference in
        # search speed, not in correctness. Owner's decision: report it.
        run = executed(self.index)
        self.assertIn("segments in", run)
        self.assertNotIn('exit 1', run.split("SEGMENTS=")[1].split("INDEX_SEGMENTS")[0])
        self.assertIn('::warning::the index came out in $SEGMENTS segments', run)
        # An absent optimize line must still say so, and must still leave the
        # provenance with something jq can serialise rather than an empty
        # --argjson, which would take the run down two steps later.
        self.assertIn('SEGMENTS=null', run)
        self.assertIn('::warning::the build printed no optimize line', run)
        # grep would exit 1 on no match and, under pipefail, kill the step
        # before either message could be printed.
        self.assertNotIn("SEGMENTS=$(grep", run)
        # The count is the only durable record left, so it has to ship.
        self.assertIn("indexSegments: $indexSegments", body(self.index))

    def test_a_stalled_optimize_reports_the_shape_it_stalled_on(self):
        # The count alone cannot say whether the merges optimize declined were
        # six segments worth four megabytes between them or three worth a
        # gigabyte each — which is the difference between a selector that is
        # too timid and one that is correctly protecting the index. With the
        # bound gone this listing is the only evidence a build leaves behind.
        run = executed(self.index)
        self.assertIn("segment sizes on disk", run)
        self.assertIn("-printf '%f %s", run)
        # Run 34978573727 printed eleven rows for eight segments: two sidecars
        # and an empty-named row from the .tantivy lock files. A segment id is
        # thirty-two hex characters; nothing else in that directory is.
        self.assertIn("length($1) == 32 && $1 ~ /^[0-9a-f]+$/", run)
        self.assertNotIn("! -name 'meta.json'", run)

    def test_a_long_transfer_reports_what_it_is_doing(self):
        # Step 9 of run 34893800787 took 40 minutes against 6 in the green run
        # and printed nothing at all until it was over, so a legitimately slow
        # phase could not be told from a hung one and the run was cancelled on
        # a wrong reading. gh and zstd are both silent; the growth report is
        # the only evidence either is still moving bytes.
        run = executed(self.index)
        self.assertIn("watch_growth downloading", run)
        self.assertIn("watch_growth expanding", run)
        self.assertIn("while sleep 30; do", run)
        # A reporter that outlived its step would keep writing into the next.
        self.assertIn(
            """trap 'kill "${WATCH:-}" 2>/dev/null || true' EXIT""", run
        )

    def test_a_silent_hang_is_impossible(self):
        # A bound without --kill-after is not a bound here: plain `timeout`
        # SIGTERMs the docker client, which forwards it and waits on a
        # container that is not listening. `timeout 120` on the preflight of
        # run 34871116986 did exactly that and never fired; with --kill-after
        # the same preflight failed loudly at 150s in run 34886226177.
        run = executed(self.index)
        bounds = re.findall(
            r"^\s*(?:\w+=\$\()?timeout([^\n]*?) (?:docker|gh|bash|grep)",
            run + "\n" + self.bounded, re.M,
        )
        self.assertTrue(bounds, "nothing is bounded any more")
        for bound in bounds:
            self.assertIn("--kill-after", bound, f"unbounded: timeout{bound}")
        # And the bound must reach the container, not only the client that
        # spoke to it: SIGTERM is swallowed by PID 1 and the SIGKILL that
        # follows cannot be proxied, so the container outlives the step.
        self.assertIn("--cidfile", self.bounded)
        self.assertIn("docker rm -f", self.bounded)
        self.assertEqual(run.count(". .github/scripts/bounded_docker_run.sh"), 2)
        self.assertIn("build-release-index --help", run)
        self.assertLessEqual(self.index["timeout-minutes"], 180)

    def test_the_container_runs_the_way_the_application_build_runs_it(self):
        run = body(self.index)
        # build_linux runs this binary in this image as root. An unmapped uid
        # with no /etc/passwd entry is an unproven deviation, so the index is
        # produced as root and the tree handed back afterwards.
        self.assertNotIn("--user", run)
        self.assertIn('sudo chown -R "$(id -u):$(id -g)" "$WORK"', run)
        # A tag plus docker's layer cache would otherwise freeze the image on
        # whatever the first build produced, differently on each runner.
        self.assertIn("docker build --pull", run)
        # The external catalogue is a separate screen and no part of the tree.
        self.assertNotIn("otzar-HB_catalog", run)

    def test_the_application_is_built_for_the_runner_architecture(self):
        # An x86_64 build on the ARM host is an exec format error; it is built
        # natively, so the gate has no architecture left to choose.
        run = body(self.index)
        self.assertIn('case "$(uname -m)" in', run)
        self.assertNotIn("raw_artifact", self.text)
        self.assertNotIn("RAW_ARTIFACT", self.text)

    def test_the_application_is_built_from_an_exact_revision(self):
        # No dependency on the application's CI artifacts: its source is built here.
        inputs = self.doc[True]["workflow_dispatch"]["inputs"]
        # Kept for the application's own auto-dispatch, which sends it (a 422 otherwise).
        self.assertEqual(inputs["otzaria_run_id"]["default"], "")
        self.assertEqual(inputs["otzaria_ref"]["default"], "")
        self.assertIn("release-triggered run builds", inputs["otzaria_ref"]["description"])
        run = executed(self.index)
        self.assertNotIn("gh run download", run)
        self.assertIn('REF="${REF:-dev}"', run)
        self.assertIn('fetch --depth 1 --no-tags', run)
        self.assertIn('rev-parse HEAD)" = "$OTZARIA_SHA"', run)
        # The build replays the checked-out workflow's own steps.
        self.assertIn("/scripts/extract_app_build_steps.py", run)
        self.assertIn(".github/workflows/build-and-announce.yml", run)

    def resolve(self, ref="", run_id="", run=None, branches=None, commits=None):
        """Run the resolve step against a fake `gh`; returns (rc, outputs, stderr)."""
        step = [s for s in steps_of(self.index)
                if s.get("name") == "Resolve the application revision to build"][0]
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / "fixtures.json").write_text(json.dumps(
                {"run": run, "branches": branches or {}, "commits": commits or {}}))
            fake = tmp / "gh"
            fake.write_text(f"""#!{sys.executable}
import json, sys
f = json.load(open({str(tmp / "fixtures.json")!r}))
path = sys.argv[2]
if "/actions/runs/" in path and f["run"] is not None:
    print(json.dumps(f["run"])); sys.exit(0)
for kind, key in (("/branches/", "branches"), ("/commits/", "commits")):
    if kind in path and path.split(kind, 1)[1] in f[key]:
        print(f[key][path.split(kind, 1)[1]]); sys.exit(0)
sys.exit(1)
""")
            fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
            out = tmp / "out"
            out.write_text("")
            env = dict(os.environ, PATH=f"{tmp}:{os.environ['PATH']}", WORK=str(tmp),
                       GITHUB_OUTPUT=str(out), OTZARIA_REPO="Otzaria/otzaria",
                       OTZARIA_REF_INPUT=ref, OTZARIA_RUN_ID=run_id)
            done = subprocess.run(["bash", "-c", step["run"]], env=env,
                                  capture_output=True, text=True)
            outputs = dict(line.split("=", 1) for line in out.read_text().splitlines())
            return done.returncode, outputs, done.stdout + done.stderr

    def test_a_pinned_run_builds_exactly_its_commit_or_fails(self):
        sha, other = "a" * 40, "b" * 40
        run = {"head_sha": sha, "head_branch": "dev",
               "head_repository": {"full_name": "Otzaria/otzaria"}}
        # The application's own dispatch: run id plus its branch.
        rc, out, _ = self.resolve(ref="dev", run_id="42", run=run)
        self.assertEqual((rc, out), (0, {"sha": sha, "ref": "dev"}))
        self.assertEqual(self.resolve(run_id="42", run=run)[1], {"sha": sha, "ref": "dev"})
        self.assertEqual(self.resolve(ref=sha, run_id="42", run=run)[1]["sha"], sha)
        for ref in ("main", other):
            rc, _, log = self.resolve(ref=ref, run_id="42", run=run)
            self.assertEqual(rc, 1, ref)
            self.assertIn("::error::run 42 built", log)
        fork = dict(run, head_repository={"full_name": "someone/otzaria"})
        self.assertIn("not Otzaria/otzaria", self.resolve(run_id="42", run=fork)[2])
        self.assertEqual(self.resolve(run_id="4x2", run=run)[0], 1)
        self.assertEqual(self.resolve(run_id="42")[0], 1)

    def test_without_a_run_the_ref_is_resolved_exactly(self):
        sha = "c" * 40
        self.assertEqual(self.resolve(branches={"dev": sha})[1], {"sha": sha, "ref": "dev"})
        self.assertEqual(self.resolve(ref=sha, commits={sha: sha})[1]["sha"], sha)
        self.assertEqual(self.resolve(ref="nope", branches={"dev": sha})[0], 1)
        self.assertEqual(self.resolve(ref="bad ref$")[0], 1)

    def test_the_builder_image_pins_its_toolchain(self):
        env = self.index["env"]
        self.assertRegex(env["FLUTTER_COMMIT"], r"^[0-9a-f]{40}$")
        for key in (
            "FLUTTER_SHA256_X86_64",
            "RUSTUP_SHA256_X86_64",
            "RUSTUP_SHA256_AARCH64",
            "WPE_SHA256_X86_64",
            "WPE_SHA256_AARCH64",
        ):
            self.assertRegex(env[key], r"^[0-9a-f]{64}$", key)
        for key in ("FLUTTER_VERSION", "RUST_VERSION", "RUSTUP_VERSION", "WPE_VERSION"):
            self.assertRegex(env[key], r"^[0-9]+\.[0-9]+\.[0-9]+$", key)
        run = body(self.index)
        # cargokit runs `rustup run stable`; anything but the pinned toolchain
        # under that name would make it download whatever stable is today.
        self.assertIn('"$RUSTUP_HOME/toolchains/stable-$APP_ARCH-unknown-linux-gnu"', run)
        self.assertEqual(run.count("sha256sum -c -"), 3)

    def extractor(self):
        spec = importlib.util.spec_from_file_location("extract_app_build_steps", EXTRACT)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def synthetic_job(self, x):
        """A build_linux job exactly as the extractor expects it, from its own tables."""
        steps = []
        for name, role in x.STEPS:
            if name in x.MIRRORED_USES:
                steps.append(dict(x.MIRRORED_USES[name], name=name))
            elif name == x.FLUTTER_BUILD:
                steps.append({"name": name, "shell": "bash", "env": dict(x.FLUTTER_BUILD_ENV),
                              "run": x.FLUTTER_BUILD_RUN + "\n"})
            elif name == x.WPE_DOWNLOAD:
                steps.append({"name": name, "shell": "bash", "run": 'tag="wpe-tag"\n', "env": {
                    "WPE_RUNTIME_VERSION": "9.9.9",
                    "WPE_RUNTIME_SHA256":
                        "${{ matrix.arch == 'aarch64' && '" + "1" * 64 + "' || '" + "2" * 64 + "' }}"}})
            elif name == "Set up Flutter (aarch64, from git)":
                steps.append({"name": name, "shell": "bash",
                              "run": "git clone --depth 1 --branch 3.47.2 x\n"})
            else:
                steps.append({"name": name, "shell": "bash", "run": f"echo {role}\n"})
        x.MIRRORED_RUN = {n: x.fingerprint(next(s for s in steps if s["name"] == n)["run"])
                          for n in x.MIRRORED_RUN}
        # Not pinned: a step no successful raw build runs, and everything after the raw bundle.
        steps.insert(5, {"name": "FULL only", "if": "matrix.target == 'full'", "run": "x"})
        steps.insert(6, {"name": "On failure", "if": "failure()", "run": "x"})
        steps.append({"name": "Upload", "if": "github.event_name != 'pull_request'", "run": "x"})
        return {"env": dict(x.JOB_ENV), "container": dict(x.CONTAINER), "steps": steps,
                "strategy": {"matrix": {"target": ["deb", "raw"], "arch": ["x86_64", "aarch64"]}}}

    def check(self, x, job, arch="x86_64"):
        env = dict(os.environ, FLUTTER_VERSION="3.47.2", WPE_VERSION="9.9.9", WPE_TAG="wpe-tag",
                   WPE_SHA256=("1" if arch == "aarch64" else "2") * 64)
        saved = dict(os.environ)
        os.environ.update(env)
        try:
            with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()), \
                    contextlib.redirect_stderr(io.StringIO()):
                x.check_structure(job)
                steps = {step["name"]: step for step in job["steps"]}
                x.check_mirrored(steps, arch)
                x.extract(steps, str(Path(tmp) / "out"))
            return 0
        except SystemExit as exit_:
            return exit_.code
        finally:
            os.environ.clear()
            os.environ.update(saved)

    def test_every_application_step_is_classified(self):
        x = self.extractor()
        roles = [role for _, role in x.STEPS]
        self.assertEqual(roles.count("R"), len(x.REPLAYED))
        self.assertEqual(
            [n for n, r in x.STEPS if r == "M"],
            [n for n, _ in x.STEPS if n in x.MIRRORED_RUN or n in x.MIRRORED_USES
             or n == x.FLUTTER_BUILD],
        )
        job = self.synthetic_job(x)
        self.assertEqual(self.check(x, job), 0)
        self.assertEqual(self.check(x, job, "aarch64"), 0)

    def test_the_replay_refuses_a_step_that_drifted(self):
        x = self.extractor()
        job = self.synthetic_job(x)

        def drifted(mutate):
            copy = json.loads(json.dumps(job))
            mutate(copy)
            return self.check(x, copy)

        def step(copy, name):
            return next(s for s in copy["steps"] if s["name"] == name)

        replayed = next(iter(x.REPLAYED.values()))
        cases = {
            "a new step": lambda c: c["steps"].insert(3, {"name": "New", "run": "x"}),
            "a new raw-only step": lambda c: c["steps"].insert(
                3, {"name": "New", "if": "matrix.target == 'raw'", "run": "x"}),
            "a new aarch64 step": lambda c: c["steps"].insert(
                3, {"name": "New", "if": "matrix.arch == 'aarch64'", "run": "x"}),
            "a step that now runs for raw": lambda c: step(c, "FULL only").update(
                {"if": "matrix.target != 'deb'"}),
            "an unclassifiable condition": lambda c: step(c, "FULL only").update(
                {"if": "github.event_name == 'push'"}),
            "raw dropped from the matrix": lambda c: c["strategy"]["matrix"].update(target=["deb"]),
            "a reordered step": lambda c: c["steps"].reverse(),
            "a job env value": lambda c: c["env"].update(LIBRARY_PATH="/elsewhere"),
            "a job env key": lambda c: c["env"].update(NEW="1"),
            "the container": lambda c: c.update(container={"image": "debian:trixie-slim"}),
            "a replayed step with if": lambda c: step(c, replayed).update({"if": "always()"}),
            "a replayed step with env": lambda c: step(c, replayed).update(env={"A": "1"}),
            "a replayed step with an expression": lambda c: step(c, replayed).update(run="${{ x }}"),
            "a mirrored run": lambda c: step(c, "Install Linux build dependencies").update(run="apt"),
            "a new dart-define": lambda c: step(c, x.FLUTTER_BUILD).update(
                run=x.FLUTTER_BUILD_RUN.replace("--verbose", "--verbose --dart-define=X=1")),
            "a new build env var": lambda c: step(c, x.FLUTTER_BUILD)["env"].update(RUSTFLAGS="-x"),
            "another Flutter": lambda c: step(c, "Set up Flutter")["with"].update(
                {"flutter-version": "3.48.0"}),
            "a swapped WPE digest": lambda c: step(c, x.WPE_DOWNLOAD)["env"].update(
                WPE_RUNTIME_SHA256="${{ matrix.arch == 'aarch64' && '" + "2" * 64
                                   + "' || '" + "1" * 64 + "' }}"),
        }
        for label, mutate in cases.items():
            self.assertEqual(drifted(mutate), 1, label)
        # Steps that cannot touch the raw bundle stay free to change.
        free = {
            "a new FULL-only step": lambda c: c["steps"].insert(
                3, {"name": "New", "if": "matrix.target == 'full' && matrix.arch == 'x86_64'",
                    "run": "x"}),
            "a new deb/rpm step": lambda c: c["steps"].insert(
                3, {"name": "New", "if": "matrix.target == 'deb' || matrix.target == 'rpm'",
                    "run": "x"}),
            "a removed FULL-only step": lambda c: c["steps"].remove(step(c, "FULL only")),
            "anything after the raw bundle": lambda c: c["steps"].extend(
                [{"name": "Later", "if": "steps.a.outputs.b == 'x'", "run": "x"},
                 {"name": "Later 2", "run": "x"}]),
        }
        for label, mutate in free.items():
            self.assertEqual(drifted(mutate), 0, label)

    def test_step_conditions_are_evaluated_exactly(self):
        x = self.extractor()
        table = {
            None: [True, True],
            "matrix.arch != 'aarch64'": [True, False],
            "matrix.arch == 'aarch64'": [False, True],
            "matrix.target == 'full'": [False, False],
            "matrix.target == 'deb' || matrix.target == 'rpm'": [False, False],
            "matrix.target == 'raw' && matrix.arch == 'x86_64'": [True, False],
            "${{ !(matrix.target == 'full') }}": [True, True],
            "failure()": [False, False],
            "always()": [True, True],
        }
        for condition, expected in table.items():
            self.assertEqual([x.runs_for_raw(condition, a) for a in x.ARCHES], expected, condition)
        for condition in ("github.event_name != 'pull_request'", "steps.a.outputs.b == 'x'",
                          "contains(matrix.target, 'raw')", "matrix.target ==", "matrix.target"):
            with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
                x.runs_for_raw(condition, "x86_64")

    def test_the_container_env_mirrors_the_application_job_env(self):
        x = self.extractor()
        run = body(self.index)
        for key in ("PKG_CONFIG_PATH", "LD_LIBRARY_PATH", "LIBRARY_PATH"):
            self.assertIn(f"-e {key}={x.JOB_ENV[key]} ", run)
        self.assertIn('-e FLUTTER_BUILD_DIR="build/linux/$FLUTTER_ARCH/release"', run)
        self.assertIn('-e JAVA_HOME="/usr/lib/jvm/java-17-openjdk-$JDK_ARCH"', run)
        self.assertIn("x86_64) FLUTTER_ARCH=x64; JDK_ARCH=amd64 ;;", run)
        self.assertIn("aarch64) FLUTTER_ARCH=arm64; JDK_ARCH=arm64 ;;", run)
        for key, value in x.FLUTTER_BUILD_ENV.items():
            self.assertIn(f"{key}={value}", run)

    def test_the_image_trusts_what_the_runner_trusts(self):
        # NetFree intercepts TLS on the database runner; its root is in the host store.
        self.assertEqual(self.index["env"]["HOST_CA_BUNDLE"], "/etc/ssl/certs/ca-certificates.crt")
        run = body(self.index)
        dockerfile = run.split("<<'DOCKERFILE'\n", 1)[1].split("\nDOCKERFILE", 1)[0]
        copy = dockerfile.index("COPY host-ca-bundle.crt")
        self.assertLess(dockerfile.index("ca-certificates curl"), copy)
        self.assertLess(copy, dockerfile.index("https://"))
        self.assertLess(copy, dockerfile.index("update-ca-certificates"))
        for var in ("SSL_CERT_FILE", "CURL_CA_BUNDLE", "GIT_SSL_CAINFO",
                    "REQUESTS_CA_BUNDLE", "NODE_EXTRA_CA_CERTS", "PIP_CERT"):
            self.assertIn(f"{var}=/etc/ssl/certs/ca-certificates.crt", dockerfile)
        # A dedicated context: never the work directory with the database in it.
        self.assertIn('CONTEXT="$WORK/builder-context"', run)
        self.assertIn('-t "$BUILDER_IMAGE" "$CONTEXT"', run)
        self.assertNotIn('- < "$WORK/Dockerfile"', run)
        self.assertIn("is missing or holds no certificate", run)

    def test_only_a_fired_bound_is_reported_as_one(self):
        def bounded(docker_rc, present):
            with tempfile.TemporaryDirectory() as tmp:
                tmp = Path(tmp)
                fake = tmp / "docker"
                fake.write_text(f"""#!/usr/bin/env bash
if [ "$1" = run ]; then echo cid123 > "$3"; exit {docker_rc}; fi
if [ "$1" = inspect ]; then exit {0 if present else 1}; fi
exit 0
""")
                fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
                env = dict(os.environ, PATH=f"{tmp}:{os.environ['PATH']}", WORK=str(tmp))
                done = subprocess.run(
                    ["bash", "-c", f". {BOUNDED}; bounded_run 60 t --rm img"],
                    env=env, capture_output=True, text=True)
                return done.returncode, done.stdout

        self.assertEqual(bounded(0, False), (0, ""))
        self.assertEqual(bounded(3, False), (3, ""))
        rc, out = bounded(124, False)
        self.assertEqual(rc, 124)
        self.assertIn("the bound of 60 expired", out)
        rc, out = bounded(3, True)
        self.assertEqual(rc, 3)
        self.assertIn("outlived its failed run", out)
        self.assertNotIn("expired", out)

    def test_the_index_is_built_from_the_exact_published_database(self):
        run = body(self.index)
        self.assertIn(
            "::error::$DB_ASSET downloaded as $ACTUAL but the release publishes $EXPECTED",
            run,
        )
        # The full DB of a schema 6+ release is seforim-schema<N>.db.zst: the
        # highest-schema asset is chosen through the one shared definition.
        self.assertIn(". .github/scripts/db_asset_names.sh", run)
        self.assertIn('DB_ASSET=$(jq -r "$FULL_DB_ASSET_JQ | .name // empty" "$WORK/release.json")', run)
        self.assertIn('-p "$DB_ASSET" -O "$WORK/seforim.db.zst"', run)
        self.assertIn("seforimDbZstSha256: $seforimDbZstSha256", run)

    def note_engine_check(self, version, body=None, table=True):
        """The gate script on a DB with one line_note row of [body] (None: the table is empty)."""
        import sqlite3
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "seforim.db"
            conn = sqlite3.connect(db)
            if table:
                conn.execute("CREATE TABLE line_note (lineId INTEGER, noteIndex INTEGER, body TEXT NOT NULL)")
                if body is not None:
                    conn.execute("INSERT INTO line_note VALUES (1, 0, ?)", (body,))
            conn.commit()
            conn.close()
            run = subprocess.run(
                [sys.executable, "-I", str(NOTE_ENGINE), str(db), version],
                capture_output=True, text=True,
            )
            return run.returncode, run.stdout + run.stderr

    def test_compressed_note_bodies_are_never_indexed_by_an_engine_that_skips_them(self):
        frame = b"\x28\xb5\x2f\xfd" + b"frame"
        for version, body, table, expected in [
            ("5.0.0", None, False, 0),
            ("5.0.0", None, True, 0),
            ("5.0.0", "plain", True, 0),
            ("5.0.0", frame, True, 1),
            ("4.9.9", frame, True, 1),
            ("5.0.1-dev", frame, True, 1),
            ("", frame, True, 1),
            ("5.0.1", frame, True, 0),
            ("5.1.0", frame, True, 0),
            ("6.0.0", frame, True, 0),
        ]:
            rc, out = self.note_engine_check(version, body, table)
            self.assertEqual(rc, expected, (version, body, table, out))
            if expected:
                self.assertIn("::error::", out)
        step = [s for s in steps_of(self.index) if s.get("name") == "Download the released database"][0]
        self.assertEqual(step["env"]["SEARCH_ENGINE_VERSION"], "${{ steps.engine.outputs.version }}")
        run = step["run"]
        expand = run.index('zstd -d -c --long=31 --memory=2048MB "$WORK/seforim.db.zst" > "$WORK/books/seforim.db"')
        check = run.index(
            'python3 -I .github/scripts/check_note_body_engine.py "$WORK/books/seforim.db" "$SEARCH_ENGINE_VERSION"'
        )
        self.assertLess(expand, check)

    def test_the_provenance_pins_the_engine_that_built_the_index(self):
        run = body(self.index)
        # The index schema is the search engine's; an index built by another
        # engine reaches the user as one the application rejects and rebuilds.
        self.assertIn("otzaria_search_engine:", run)
        self.assertIn("searchEngineVersion: $searchEngineVersion", run)
        # The lockfile this build's own pub get resolved; the app commits none.
        self.assertIn('LOCK="$WORK/otzaria-src/pubspec.lock"', run)
        self.assertIn("sha: $otzariaSha", run)
        self.assertIn('runId: (if $otzariaRunId == "" then null else $otzariaRunId end)', run)

    def test_nothing_caps_the_memory_the_build_may_use(self):
        # Comments are stripped first: this asserts on what is executed, and
        # the step's comments name the very flags it must not carry.
        run = executed(self.index)
        # EVERY container invocation, not the slice before the first
        # `build-release-index` — that slice ends at the --help preflight and
        # leaves the invocation that actually builds the index unchecked, so
        # --memory=32g on it used to pass this test green.
        invocations = [
            c for c in commands(self.index)
            if c.startswith("bounded_run ") or "docker run" in c or "docker build" in c
        ]
        self.assertGreaterEqual(
            len(invocations), 3, "the index is no longer built in a container"
        )
        # Owner decision: the job takes whatever the host gives and must keep
        # doing so after the host's RAM is increased — so no fixed ceiling of
        # any kind. --ipc=host is NOT the way to honour that: run 34845248197
        # carried it and hung for two hours with no output at all, and the
        # environment this command is proven in does not use it.
        #
        # Scoped to the invocations because `zstd --memory=2048MB` on the
        # database download is the opposite of a cap: it RAISES zstd's refusal
        # threshold for the 2 GiB window the asset declares.
        for invocation in invocations:
            self.assertNotIn("--ipc=host", invocation)
            for capped in ("--memory", "--shm-size", "--cpus", "--cpuset"):
                self.assertNotIn(capped, invocation, f"{capped} caps: {invocation}")
        # No RAM precondition either: the release build has one because of its
        # 20G tmpfs; this job writes to disk and must not refuse a smaller box.
        self.assertNotIn("MemAvailable", run)

    def test_it_records_that_pdfs_are_not_in_the_index(self):
        self.assertIn("includesPdfBooks: false", body(self.index))

    def test_assets_are_split_below_the_github_limit_and_reuploadable(self):
        run = body(self.index)
        self.assertIn("split_release_asset.sh", run)
        self.assertIn('-le "$PART_SIZE"', run)
        self.assertIn("--clobber", run)
        self.assertIn("gh release delete-asset", run)
        self.assertIn("1992294400", self.text)
        self.assertTrue(SPLIT.exists(), "the split helper is missing")
        split = SPLIT.read_text(encoding="utf-8")
        # Otzaria/otzaria reassembles these parts with its own
        # tool/release/assemble_split_asset.sh, which reads exactly these keys.
        for field in ("schemaVersion: 1", "archive: $archive", "parts: $parts"):
            self.assertIn(field, split)
        self.assertIn("github_limit=2147483648", split)

    def test_the_work_directory_is_released_even_when_the_build_fails(self):
        cleanup = [s for s in steps_of(self.index) if s.get("if") == "always()"]
        self.assertEqual(len(cleanup), 1)
        # sudo, or this step cannot succeed at all after a failed build: the
        # container runs as root and `sudo chown` sits on the success path, so
        # a failure leaves root-owned files in the workspace. An unprivileged
        # rm -rf then fails, the files survive, and the NEXT run of any
        # workflow on this runner dies in actions/checkout's `git clean -ffdx`.
        self.assertIn("sudo rm -rf", cleanup[0]["run"])
        prepare = [
            s for s in steps_of(self.index)
            if s.get("name", "").startswith("Prepare a clean work directory")
        ]
        self.assertEqual(len(prepare), 1)
        self.assertIn("sudo rm -rf", prepare[0]["run"])

    def test_the_portability_of_the_index_is_proved_not_declared(self):
        run = executed(self.index)
        # includesPdfBooks: false is a claim written into the provenance. What
        # makes it TRUE is IndexingRepository.isIndexableBook — in another
        # repository, on another branch, at run time. If that regresses, this
        # job would publish an index with the runner's own paths baked into the
        # document keys, and every Windows, macOS and Android client would
        # consume it while both sides reported green.
        self.assertIn('grep -rlF --binary-files=text -- "$WORK/books"', run)
        # ...and a grep that was killed or broke must never read as "found
        # nothing": its three outcomes are kept apart.
        self.assertIn("GREP_RC", run)
        self.assertIn("refusing to publish an index whose portability is unproven", run)

    def test_the_book_count_cannot_silently_become_null(self):
        run = executed(self.index)
        # The only end-to-end signal of how much of the library actually
        # reached the index. ${CATALOGUE:-null} let one reworded line in
        # ReleaseIndexBuilderCli erase it with no red check anywhere.
        self.assertIn('[ -n "$CATALOGUE" ]', run)
        self.assertIn("CATALOGUE_BOOKS=$CATALOGUE", run)
        self.assertNotIn("CATALOGUE_BOOKS=${CATALOGUE:-null}", run)

    def test_untrusted_values_never_reach_the_shell_as_expressions(self):
        # otzaria_ref is a dispatch input and git permits `"` and `$` in a ref
        # name. Interpolated into a command line on a runner whose environment
        # holds PIPELINE_TOKEN, that is a shell injection.
        packing = [
            s for s in steps_of(self.index)
            if s.get("name", "").startswith("Pack, describe and split")
        ][0]
        for step in steps_of(self.index):
            self.assertNotIn("${{", step.get("run", ""), step.get("name"))
        self.assertIn("OTZARIA_SHA", packing["env"])
        self.assertIn("OTZARIA_REF", packing["env"])

    def test_the_cross_repository_read_uses_the_pipeline_token(self):
        self.assertEqual(
            self.index["env"]["GH_TOKEN"], "${{ secrets.PIPELINE_TOKEN }}"
        )


if __name__ == "__main__":
    unittest.main()
