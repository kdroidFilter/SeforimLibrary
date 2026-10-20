"""Contract tests for the hosted CI workflows: ci.yml and contract.yml.

Both jobs used to report a green check that carried no evidence — no test count
appears anywhere in either CI log of cycle 33987355439 — while `contract`
re-ran on the machine-made refs the pipeline itself pushes and `CI - Tests` ran
on a deprecated Gradle action that reused nothing (67 of 67 tasks executed).
These tests pin the fixes.

Assertions read the parsed workflow, not its comment text, so a comment that
quotes a warning cannot satisfy or break them.
"""

import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path

try:  # PyYAML ships with the ubuntu-latest image these jobs run on.
    import yaml
except ImportError:  # pragma: no cover - only on a runner without PyYAML
    yaml = None

WORKFLOWS = Path(__file__).parents[1] / "workflows"
RELEASE = WORKFLOWS / "manual-generate-release.yml"
DELTA = WORKFLOWS / "delta-real-diff-test.yml"
CI = WORKFLOWS / "ci.yml"
CONTRACT = WORKFLOWS / "contract.yml"
TRUNK = "otzaria"

# GitHub Actions evaluates a `run:` script as ONE template expression and
# refuses to LOAD the whole workflow file when it exceeds 21,000 characters.
# The margin below is what a step body may reach here.
EXPRESSION_CEILING = 21000
RUN_CEILING = 19000

# The lowest major of each action that runs on Node 24. Every "Node.js 20 is
# deprecated … forced to run on Node.js 24" warning in cycle 33987355439 named
# an action below one of these. Read off each action's own action.yml
# (`runs.using`) at the major tag on 2026-09-08, not from memory.
NODE24_MAJOR = {
    "actions/checkout": 5,
    "actions/download-artifact": 7,
    "actions/setup-java": 5,
    "actions/setup-python": 6,
    "actions/upload-artifact": 6,
    "gradle/actions/setup-gradle": 5,
}

# Self-hosted jobs are deliberately NOT bumped: their runner version is not
# proven in this repository and a broken action on the weekly build costs a
# release. Naming them keeps that a decision rather than an oversight, and
# stops a new HOSTED job from quietly joining the exemption.
SELF_HOSTED_JOBS = {
    ("manual-generate-release.yml", "build-and-release"),
    ("delta-real-diff-test.yml", "real-diff"),
    ("build-library-index.yml", "index"),
    ("build-library-vectors.yml", "vectors"),
}


def parsed_workflows():
    for path in sorted(WORKFLOWS.glob("*.yml")):
        yield path.name, yaml.safe_load(path.read_text(encoding="utf-8"))


def steps_of(doc):
    for job_name, job in (doc.get("jobs") or {}).items():
        for step in job.get("steps") or []:
            yield job_name, job, step


def action_major(uses):
    match = re.fullmatch(r"([^@]+)@v(\d+)", uses or "")
    return (match.group(1), int(match.group(2))) if match else (None, None)


def contract_suites(body):
    """The suite names of a `for suite in \\ … ; do` loop, in their own order."""
    inner = body.split("for suite in \\\n", 1)[1].split("; do", 1)[0]
    return [
        line.strip().rstrip("\\").strip()
        for line in inner.splitlines()
        if line.strip()
    ]


def named_step(doc, step_name):
    for _, _, step in steps_of(doc):
        if step.get("name") == step_name:
            return step
    raise AssertionError(f"no step named {step_name!r}")


@unittest.skipIf(yaml is None, "PyYAML unavailable on this runner")
class CiWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ci_text = CI.read_text(encoding="utf-8")
        cls.contract_text = CONTRACT.read_text(encoding="utf-8")
        cls.ci = yaml.safe_load(cls.ci_text)
        cls.contract = yaml.safe_load(cls.contract_text)

    def run_bodies(self, doc):
        return [step["run"] for _, _, step in steps_of(doc) if "run" in step]

    def uses(self, doc):
        return [step["uses"] for _, _, step in steps_of(doc) if "uses" in step]

    # ─── ci.yml ────────────────────────────────────────────────────────────

    def test_no_workflow_still_uses_the_deprecated_gradle_action(self):
        # "##[warning]This job uses deprecated functionality from the
        # 'gradle/gradle-build-action' action" on both CI runs of the cycle.
        for name, doc in parsed_workflows():
            for uses in self.uses(doc):
                self.assertFalse(
                    uses.startswith("gradle/gradle-build-action"),
                    f"{name} still pins {uses}",
                )

    def test_ci_setup_gradle_caches_writably_only_on_the_trunk(self):
        # CI got `67 actionable tasks: 67 executed` — nothing from cache —
        # while contract's setup-gradle got 7 of 16 FROM-CACHE.
        setup = [
            step
            for _, _, step in steps_of(self.ci)
            if step.get("uses", "").startswith("gradle/actions/setup-gradle@")
        ]
        self.assertEqual(len(setup), 1)
        self.assertEqual(
            setup[0]["with"]["cache-read-only"],
            "${{ github.ref != 'refs/heads/%s' }}" % TRUNK,
        )

    def test_ci_and_contract_run_one_generation_of_the_gradle_action(self):
        # Two action generations in one repo is what split the cache.
        def majors(doc):
            return {
                uses
                for uses in self.uses(doc)
                if uses.startswith("gradle/actions/setup-gradle@")
            }

        self.assertEqual(majors(self.ci), majors(self.contract))

    def test_ci_ends_with_an_explicit_test_count(self):
        # No "tests run" line exists anywhere in either CI log of the cycle,
        # and 4 of the 11 modules are `allTests NO-SOURCE`.
        body = "\n".join(self.run_bodies(self.ci))
        self.assertIn(
            'f"tests: allTests {tests - failures - errors - skipped} passed, "', body
        )
        self.assertIn("**/build/test-results/**/*.xml", body)
        self.assertIn("::error::allTests wrote no test-result XML — no test ran", body)
        # Gradle's exit status still decides the step, count or no count.
        self.assertIn(
            "./gradlew allTests --no-daemon --continue --warning-mode all || rc=$?", body
        )
        self.assertIn('exit "$rc"', body)

    def test_exactly_one_invocation_in_the_repo_asks_for_every_deprecation(self):
        # "Deprecated Gradle features were used in this build" fired 4× per
        # release run with no way to see which. This is the one invocation that
        # shows them; the self-hosted release build keeps the quiet default.
        hits = {}
        for name, doc in parsed_workflows():
            count = sum(body.count("--warning-mode all") for body in self.run_bodies(doc))
            if count:
                hits[name] = count
        self.assertEqual(hits, {"ci.yml": 1})

    def test_ci_still_only_runs_on_the_trunk(self):
        triggers = self.ci[True]  # YAML 1.1 reads a bare `on:` key as True
        self.assertEqual(triggers["push"], {"branches": [TRUNK]})
        self.assertEqual(triggers["pull_request"], {"branches": [TRUNK]})

    # ─── contract.yml ──────────────────────────────────────────────────────

    def test_contract_ignores_the_refs_the_pipeline_pushes(self):
        # Runs 34026451036 (branch lines-snapshot-sha256-2486e499…) and
        # 34030778729 (branch v27-20260906092829) were both `push` on the same
        # already-green SHA 2aadf7a7 — 4 runs for 2 distinct trees.
        triggers = self.contract[True]
        self.assertEqual(triggers["push"], {"branches": [TRUNK]})
        self.assertIn("pull_request", triggers)
        self.assertIsNone(triggers["pull_request"], "PRs stay unfiltered on purpose")
        self.assertNotIn("release", triggers)
        self.assertNotIn("tags", triggers["push"])

    def test_contract_ends_with_an_explicit_test_count(self):
        body = "\n".join(self.run_bodies(self.contract))
        self.assertIn(
            'f"tests: PatchTablesContractTest+LogicalHashContractTest {tests - failures - errors - skipped} passed, "',
            body,
        )
        self.assertIn("generator/common/build/test-results/jvmTest/*.xml", body)
        self.assertIn(
            "::error::the contract test wrote no test-result XML — nothing ran", body
        )
        self.assertIn('exit "$rc"', body)

    def test_contract_fixture_steps_say_what_they_checked(self):
        body = "\n".join(self.run_bodies(self.contract))
        self.assertIn('echo "fixtures: 8 present"', body)
        self.assertIn('echo "fixtures: 4 pairs byte-identical"', body)

    def test_pull_requests_run_the_local_orchestration_contract_suites(self):
        # Those suites used to run in exactly one place: the release workflow's
        # `reconcile` job, i.e. only once the weekly build had already started.
        # Run 34195296928 never started a job — GitHub refused to load
        # manual-generate-release.yml — so a merged change went unchecked. The
        # PR job runs the same loop, and the two suite lists are pinned to each
        # other so neither can drift.
        release = yaml.safe_load(RELEASE.read_text(encoding="utf-8"))
        step_name = "Validate local orchestration boundary contracts"
        ci_loop = named_step(self.ci, step_name)["run"]
        release_loop = named_step(release, step_name)["run"]

        self.assertEqual(contract_suites(ci_loop), contract_suites(release_loop))
        self.assertGreater(len(contract_suites(ci_loop)), 0)
        # Same fail-closed shape: unittest's own count, and a suite that
        # collected nothing fails instead of reporting a green check.
        for pinned in (
            "set -euo pipefail",
            "ran=$(sed -n 's/^Ran \\([0-9][0-9]*\\) test.*/\\1/p'",
            "::error::$suite failed — see its output above",
            "::error::$suite reported no tests — it collected nothing",
            'echo "tests: local orchestration boundary contracts $total passed"',
        ):
            self.assertIn(pinned, ci_loop)
            self.assertIn(pinned, release_loop)

        # Cheap and hosted: no Gradle, no JVM, a bounded ceiling and read-only
        # credentials. The heavy `test` job already owns the JVM side.
        job = self.ci["jobs"]["contracts"]
        self.assertEqual(job["runs-on"], "ubuntu-latest")
        self.assertEqual(job["timeout-minutes"], 10)
        self.assertEqual(job["permissions"], {"contents": "read"})
        for step in job["steps"]:
            self.assertNotIn("gradle", (step.get("run") or "").lower())
            self.assertNotIn("gradle", step.get("uses", "").lower())
        # A missing PyYAML would skip most of those suites and still report a
        # green check, so the job says which interpreter it proved.
        self.assertIn("import sys, yaml;", "\n".join(self.run_bodies(self.ci)))

    # ─── every workflow in the repository ──────────────────────────────────

    def test_no_run_script_approaches_the_max_expression_length(self):
        """Every `run:` script stays well under GitHub's 21,000-char ceiling.

        A `run:` script is one template expression, and GitHub refuses to LOAD
        a workflow whose expression exceeds 21,000 characters. Run 34195296928
        (commit 15ef349) never started a job: "Invalid workflow file:
        .github/workflows/manual-generate-release.yml — (Line: 1974, Col: 14):
        Exceeded max expression length 21000". "Produce + verify patch fan" had
        reached 22,592 characters and the relink wait 21,230; both moved into
        .github/scripts/. Nothing in the workflow file itself reports this — the
        error only appears when GitHub tries to load the file — so it is pinned
        here, with ~2,000 characters of headroom so one more paragraph of
        comment cannot walk a step over the cliff.
        """
        # parsed_workflows() globs *.yml; a *.yaml would escape the sweep.
        self.assertEqual(sorted(WORKFLOWS.glob("*.yaml")), [])
        self.assertLess(RUN_CEILING, EXPRESSION_CEILING)
        longest = ("", 0)
        checked = 0
        for name, doc in parsed_workflows():
            for job_name, _job, step in steps_of(doc):
                run = step.get("run")
                if not isinstance(run, str):
                    continue
                checked += 1
                where = (
                    f"{name}:{job_name} step "
                    f"{step.get('name') or step.get('id') or '<unnamed>'!r}"
                )
                if len(run) > longest[1]:
                    longest = (where, len(run))
                self.assertLess(
                    len(run),
                    RUN_CEILING,
                    f"{where} has a {len(run)}-character run: script; GitHub "
                    f"refuses the whole workflow at {EXPRESSION_CEILING}. Move a "
                    "cohesive block into .github/scripts/ and call or source it.",
                )
        self.assertGreater(checked, 0, f"no run: script found (longest {longest})")

    # ─── delta-real-diff-test.yml ──────────────────────────────────────────

    def lineage_pairing(self, lineage, metadata, tag, archive_sha):
        """Run the Otzaria step's lineage block for real against fixture files."""
        doc = yaml.safe_load(DELTA.read_text(encoding="utf-8"))
        body = named_step(doc, "Stage the Otzaria source")["run"]
        start = body.index('LINEAGE="${LINEAGES[0]}"\n')
        end = body.index('OTZARIA_EXTRACT_ROOT=$(dirname "$LINEAGE")')
        for tool in ("bash", "jq", "sha256sum"):
            self.assertIsNotNone(shutil.which(tool), f"{tool} is required")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "lineage.json").write_text(json.dumps(lineage), encoding="utf-8")
            (root / "meta.json").write_text(json.dumps(metadata), encoding="utf-8")
            script = (
                "set -euo pipefail\nLINEAGES=(\"$PWD/lineage.json\")\n"
                + body[start:end]
                + 'echo PAIRED\n'
            )
            return subprocess.run(
                ["bash", "-c", script], cwd=root, capture_output=True, text=True,
                env={"PATH": os.environ["PATH"],
                     "SEFARIA_TAG": tag,
                     "SEFARIA_METADATA": str(root / "meta.json"),
                     "SEFARIA_VERIFIED_ARCHIVE_SHA": archive_sha},
            )

    def test_delta_pairs_the_otzaria_lineage_with_the_staged_sefaria_export(self):
        # manual-generate-release.yml asserts this pairing; latest mode picks
        # SefariaExport and otzaria-library independently and must fail loudly.
        archive = {"sha256": "a" * 64, "size": 3, "parts": [{"name": "p", "size": 3, "sha256": "a" * 64}]}
        metadata = {"tag": "t1", "run_id": 7, "run_attempt": 1, "archive": archive}
        meta_sha = hashlib.sha256(json.dumps(metadata).encode()).hexdigest()
        good = {"sefaria": {"tag": "t1", "release_metadata_sha256": meta_sha,
                            "run_id": 7, "run_attempt": 1, "archive": archive}}
        ok = self.lineage_pairing(good, metadata, "t1", "a" * 64)
        self.assertEqual(ok.returncode, 0, ok.stderr + ok.stdout)
        self.assertIn("PAIRED", ok.stdout)
        cases = {
            "tag": (good, "t2", "a" * 64, "'t1'", "'t2'"),
            "archive digest": (good, "t1", "b" * 64, "'" + "a" * 64 + "'", "'" + "b" * 64 + "'"),
            "metadata digest": ({"sefaria": dict(good["sefaria"], release_metadata_sha256="c" * 64)},
                                "t1", "a" * 64, "'" + "c" * 64 + "'", "'" + meta_sha + "'"),
            "run id": ({"sefaria": dict(good["sefaria"], run_id=8)}, "t1", "a" * 64,
                       '"run_id":8', '"run_id":7'),
            "run attempt": ({"sefaria": dict(good["sefaria"], run_attempt=2)}, "t1", "a" * 64,
                            '"run_attempt":2', '"run_attempt":1'),
            "archive object": ({"sefaria": dict(good["sefaria"], archive=dict(archive, parts=[
                dict(archive["parts"][0], name="q")]))}, "t1", "a" * 64,
                '"name":"q"', '"name":"p"'),
        }
        for what, (lineage, tag, sha, lineage_value, staged_value) in cases.items():
            with self.subTest(what):
                bad = self.lineage_pairing(lineage, metadata, tag, sha)
                self.assertNotEqual(bad.returncode, 0, f"{what} mismatch passed")
                self.assertNotIn("PAIRED", bad.stdout)
                self.assertIn("::error::lineage", bad.stdout)
                self.assertIn(lineage_value, bad.stdout)
                self.assertIn(staged_value, bad.stdout)

    def test_delta_sefaria_step_exports_what_it_verified(self):
        doc = yaml.safe_load(DELTA.read_text(encoding="utf-8"))
        body = named_step(doc, "Stage the Sefaria export")["run"]
        self.assertIn('echo "SEFARIA_METADATA=$META"', body)
        self.assertIn('echo "SEFARIA_VERIFIED_ARCHIVE_SHA=$EXPECTED_SHA"', body)
        self.assertIn('echo "$EXPECTED_SHA  $INPUTS/sefaria-archive.tar.zst" | sha256sum -c -', body)
        # The pairing runs in both inputs_from modes.
        self.assertNotIn("if", named_step(doc, "Stage the Otzaria source"))

    COMPRESS_STEP = "Compress v1 + v2 line text (zstd dictionary)"
    FRAMES_STEP = "Verify both DBs hold only zstd frames of one dictionary"

    def test_delta_compresses_both_builds_before_the_patch(self):
        doc = yaml.safe_load(DELTA.read_text(encoding="utf-8"))
        names = [step.get("name") for _, _, step in steps_of(doc)]
        order = [names.index(n) for n in (
            "Build v2 with the production pipeline", "Verify v2 stamp",
            self.COMPRESS_STEP, self.FRAMES_STEP,
            "producePatchAndVerify (strict invariant)")]
        self.assertEqual(order, sorted(order))
        self.assertNotIn("if", named_step(doc, self.COMPRESS_STEP))
        body = named_step(doc, self.COMPRESS_STEP)["run"]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(os.path.realpath(tmp))
            stub = root / "gradle"
            stub.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$CALLS"\n', encoding="utf-8")
            stub.chmod(0o755)
            run = subprocess.run(
                ["bash", "-c", body], cwd=root, capture_output=True, text=True,
                env={"PATH": os.environ["PATH"], "GRADLE": str(stub), "CALLS": str(root / "calls")},
            )
            self.assertEqual(run.returncode, 0, run.stderr + run.stdout)
            calls = (root / "calls").read_text(encoding="utf-8").splitlines()
        self.assertEqual(calls, [
            f":generator-common:compressLineContent -PdbPath={root}/build/{db} --no-daemon --stacktrace"
            for db in ("seforim.db.v1", "seforim.db")
        ])

    def run_delta_step(self, step_name, root, workspace_version, selected_runner, has_selector=True):
        """Runs a step body against fake toolchains and a stub git; returns $GITHUB_ENV."""
        step = named_step(yaml.safe_load(DELTA.read_text(encoding="utf-8")), step_name)
        workspace = root / f"ws-{workspace_version}"
        (workspace / "gradle/wrapper").mkdir(parents=True, exist_ok=True)
        (workspace / "gradle/wrapper/gradle-wrapper.properties").write_text(
            "distributionUrl=https\\://services.gradle.org/distributions/"
            f"gradle-{workspace_version}-bin.zip\n", encoding="utf-8")
        shutil.rmtree(workspace / ".github", ignore_errors=True)
        if has_selector:
            (workspace / ".github/scripts").mkdir(parents=True)
            shutil.copy(Path(__file__).parent / "select_gradle_toolchain.sh",
                        workspace / ".github/scripts/select_gradle_toolchain.sh")
        stubs = root / "stubs"
        stubs.mkdir(exist_ok=True)
        (stubs / "git").write_text("#!/bin/sh\necho stub\n", encoding="utf-8")
        (stubs / "git").chmod(0o755)
        for version in ("9.1.0", "9.8.0"):
            gradle = root / f"tc/gradle-{version}/bin/gradle"
            gradle.parent.mkdir(parents=True, exist_ok=True)
            gradle.write_text(f"#!/bin/sh\necho 'Gradle {version}'\n", encoding="utf-8")
            gradle.chmod(0o755)
        (root / "temp").mkdir(exist_ok=True)
        env_file = root / "github_env"
        env_file.write_text("", encoding="utf-8")
        run = subprocess.run(
            ["bash", "-c", step["run"]], cwd=workspace, capture_output=True, text=True,
            env={"PATH": f"{stubs}:{os.environ['PATH']}", "GITHUB_ENV": str(env_file),
                 "GITHUB_WORKSPACE": str(workspace), "RUNNER_TEMP": str(root / "temp"),
                 "OTZARIA_GRADLE_TOOLCHAINS": str(root / "tc"), "SELECTED_RUNNER": selected_runner,
                 "BASELINE_REF": "base", "GITHUB_SHA": "head"},
        )
        self.assertEqual(run.returncode, 0, run.stderr + run.stdout)
        if selected_runner != "ARM64":
            self.assertEqual(step["env"]["SELECTED_RUNNER"], "${{ inputs.runner_selection }}")
        return env_file.read_text(encoding="utf-8").replace(str(root), "$ROOT")

    def test_delta_reselects_gradle_after_every_checkout(self):
        # AGP 8.12.3 (old trees) fails on Gradle 9.8; AGP 9.1.1 needs >= 9.3.1.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(os.path.realpath(tmp))
            head = self.run_delta_step("Resolve the Gradle entry point", root, "9.8.0", "local")
            self.assertEqual(head, "GRADLE=$ROOT/tc/gradle-9.8.0/bin/gradle\n")
            # The baseline may lack the script; only the $RUNNER_TEMP copy runs from here on.
            base = self.run_delta_step(
                "Check out the baseline tree for v1", root, "9.1.0", "server-2", has_selector=False)
            self.assertEqual(base, "GRADLE=$ROOT/tc/gradle-9.1.0/bin/gradle\n")
            back = self.run_delta_step(
                "Restore the current tree for v2", root, "9.8.0", "local", has_selector=False)
            self.assertEqual(back, "GRADLE=$ROOT/tc/gradle-9.8.0/bin/gradle\n")
        with tempfile.TemporaryDirectory() as tmp:
            arm = self.run_delta_step("Resolve the Gradle entry point", Path(tmp), "9.8.0", "ARM64")
            self.assertEqual(arm, "GRADLE=./gradlew\n")

    def test_delta_durable_hosts_never_use_the_wrapper_or_a_path_gradle(self):
        doc = yaml.safe_load(DELTA.read_text(encoding="utf-8"))
        names = [step.get("name") for _, _, step in steps_of(doc)]
        self.assertNotIn("Verify durable Gradle 9.8.0 toolchain", names)
        for _, _, step in steps_of(doc):
            body = step.get("run") or ""
            self.assertIsNone(re.search(r"(^|[\s;(&|!])gradle\s", body, re.M), step.get("name"))
            if "uses" in step and "setup-gradle" in step["uses"]:
                self.assertEqual(step.get("if"), "inputs.runner_selection == 'ARM64'")

    def test_delta_callers_inherit_the_compressed_pipeline(self):
        # The dry run and the ARM button run this file, not their own copy.
        for caller in ("delta-pipeline-dryrun.yml", "delta-real-diff-arm.yml"):
            with self.subTest(caller):
                doc = yaml.safe_load((WORKFLOWS / caller).read_text(encoding="utf-8"))
                uses = [job.get("uses") for job in doc["jobs"].values()]
                self.assertEqual(uses, ["./.github/workflows/delta-real-diff-test.yml"])

    def frames_check(self, mutate=None):
        """Run the frames step for real on two fixture DBs; [mutate] edits v2."""
        doc = yaml.safe_load(DELTA.read_text(encoding="utf-8"))
        body = named_step(doc, self.FRAMES_STEP)["run"]
        for tool in ("bash", "sqlite3", "sha256sum"):
            self.assertIsNotNone(shutil.which(tool), f"{tool} is required")
        frame = b"\x28\xb5\x2f\xfd" + b"frame"
        with tempfile.TemporaryDirectory() as tmp:
            build = Path(tmp) / "build"
            build.mkdir()
            for name in ("seforim.db.v1", "seforim.db"):
                conn = sqlite3.connect(build / name)
                conn.executescript(
                    "CREATE TABLE zstd_dict (id INTEGER PRIMARY KEY, dict BLOB);"
                    "CREATE TABLE line_content (id INTEGER PRIMARY KEY, content TEXT);"
                    "CREATE TABLE version_line (versionId INTEGER, lineId INTEGER, content TEXT);")
                conn.execute("INSERT INTO zstd_dict VALUES (32768, ?)", (b"dict",))
                conn.executemany("INSERT INTO line_content VALUES (?, ?)", [(1, frame), (2, frame)])
                conn.executemany("INSERT INTO version_line VALUES (1, ?, ?)", [(1, None), (2, frame)])
                if mutate and name == "seforim.db":
                    mutate(conn)
                conn.commit()
                conn.close()
            return subprocess.run(
                ["bash", "-c", body], cwd=tmp, capture_output=True, text=True,
                env={"PATH": os.environ["PATH"]},
            )

    def test_delta_frames_step_accepts_two_compressed_dbs(self):
        ok = self.frames_check()
        self.assertEqual(ok.returncode, 0, ok.stderr + ok.stdout)
        self.assertIn("✓ build/seforim.db.v1: 2 line_content frames, dictionary 32768", ok.stdout)
        self.assertIn("✓ build/seforim.db: 2 line_content frames", ok.stdout)

    def test_delta_frames_step_refuses_what_the_patch_would_not_cover(self):
        cases = {
            "plain line": ("UPDATE line_content SET content = 'text' WHERE id = 2", "not zstd frames"),
            "plain edition line": ("UPDATE version_line SET content = 'text' WHERE lineId = 2", "not zstd frames"),
            "blob that is no frame": ("UPDATE line_content SET content = x'00010203' WHERE id = 1", "not zstd frames"),
            "other dictionary": ("UPDATE zstd_dict SET dict = x'00'", "different zstd dictionaries"),
            "two dictionaries": ("INSERT INTO zstd_dict VALUES (1, x'00')", "2 zstd_dict rows"),
            "no line rows": ("DELETE FROM line_content", "no line_content rows"),
        }
        for what, (sql, message) in cases.items():
            with self.subTest(what):
                bad = self.frames_check(lambda conn, sql=sql: conn.execute(sql))
                self.assertNotEqual(bad.returncode, 0, f"{what} passed")
                self.assertIn(message, bad.stdout)
        uncompressed = self.frames_check(lambda conn: conn.execute("DROP TABLE zstd_dict"))
        self.assertNotEqual(uncompressed.returncode, 0, "a DB without zstd_dict passed")

    # ─── every hosted job in the repository ────────────────────────────────

    def test_self_hosted_exemptions_name_jobs_that_have_steps(self):
        # A `uses:` caller has no steps, so its exemption would silently exempt nothing.
        docs = dict(parsed_workflows())
        checked = 0
        for name, job_name in sorted(SELF_HOSTED_JOBS):
            checked += 1
            with self.subTest(f"{name}:{job_name}"):
                self.assertIn(name, docs)
                job = (docs[name].get("jobs") or {}).get(job_name)
                self.assertIsNotNone(job, f"{name} has no job {job_name!r}")
                self.assertTrue(job.get("steps"), f"{name}:{job_name} has no steps")
        self.assertEqual(checked, len(SELF_HOSTED_JOBS))
        self.assertGreater(checked, 0)

    def test_hosted_jobs_use_action_majors_that_run_on_node_24(self):
        checked = 0
        for name, doc in parsed_workflows():
            for job_name, job, step in steps_of(doc):
                self_hosted = (name, job_name) in SELF_HOSTED_JOBS
                self.assertEqual(
                    self_hosted,
                    job.get("runs-on") != "ubuntu-latest",
                    f"{name}:{job_name} changed runner class",
                )
                if self_hosted:
                    continue
                action, major = action_major(step.get("uses"))
                if action not in NODE24_MAJOR:
                    continue
                checked += 1
                self.assertGreaterEqual(
                    major,
                    NODE24_MAJOR[action],
                    f"{name}:{job_name} pins {action}@v{major}, still Node 20",
                )
        self.assertGreater(checked, 0)


if __name__ == "__main__":
    unittest.main()
