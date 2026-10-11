import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

try:  # PyYAML ships with the ubuntu-latest image this job runs on.
    import yaml
except ImportError:  # pragma: no cover - only on a runner without PyYAML
    yaml = None

WORKFLOW = Path(__file__).parents[1] / "workflows" / "manual-generate-release.yml"
MANIFEST_WORKFLOW = Path(__file__).parents[1] / "workflows" / "update-release-manifest.yml"
HANDOFF_PUBLISHER = Path(__file__).parent / "publish_release_handoff.sh"
ZSTD_WORKERS_HELPER = Path(__file__).parent / "zstd_workers.sh"
FAILURE_RECONCILER = (
    Path(__file__).parents[1] / "workflows" / "reconcile-linker-after-failure.yml"
)
RELEASE_DRAFT = Path(__file__).parent / "release_draft.sh"
EARLY_UPLOAD = Path(__file__).parent / "upload_early_release_assets.sh"
ANCHOR_PREFETCH = Path(__file__).parent / "prefetch_patch_anchors.sh"
ANCHOR_DERIVATION = Path(__file__).parent / "patch_fan_anchors.sh"
# GitHub Actions refuses to LOAD a workflow with a `run:` template longer than
# 21,000 characters (run 34195296928). The patch fan's four shell functions and
# the relink wait loop therefore live in these two files instead of inline, and
# the assertions about them read the file that actually runs.
PATCH_FAN_LIB = Path(__file__).parent / "patch_fan_lib.sh"
RELINK_WAIT = Path(__file__).parent / "wait_for_relink_run.sh"
RELINK_TIMEOUT_CONTRACT = (
    Path(__file__).parents[1] / "contracts" / "linker_relink_timeouts_v1.json"
)
SCRIPTS_DIR = Path(__file__).parent
GENERATOR_COMMON_BUILD = (
    Path(__file__).parents[2] / "generator" / "common" / "build.gradle.kts"
)
ROOT_BUILD = Path(__file__).parents[2] / "build.gradle.kts"
VALIDATOR = Path(__file__).parent / "validate_build_provenance.py"
OTZARIA_VALIDATOR = Path(__file__).parent / "validate_otzaria_provenance.py"
SEFARIA_VALIDATOR = Path(__file__).parent / "validate-sefaria-release-metadata.py"
QA_DIR = Path(__file__).parents[2] / "scripts" / "qa"
PATCH_PIPELINE_CLI = (
    Path(__file__).parents[2] / "generator" / "common" / "src" / "jvmMain" / "kotlin"
    / "io" / "github" / "kdroidfilter" / "seforimlibrary" / "common" / "patch"
    / "PatchPipelineCli.kt"
)
PATCH_SIZE_GUARD = (
    Path(__file__).parents[2]
    / "generator/common/src/jvmMain/kotlin/io/github/kdroidfilter/seforimlibrary"
    / "common/patch/PatchSizeGuard.kt"
)


class ManualReleaseWorkflowContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")
        # Sourced by "Produce + verify patch fan" / run by the relink step: for
        # every contract below they are the same shell text as an inline body,
        # only at their own indentation.
        cls.fan_lib = PATCH_FAN_LIB.read_text(encoding="utf-8")
        cls.relink_wait = RELINK_WAIT.read_text(encoding="utf-8")

    def step(self, name):
        marker = f"      - name: {name}\n"
        self.assertEqual(self.workflow.count(marker), 1, f"step {name!r} must exist exactly once")
        return self.workflow.split(marker, 1)[1].split("\n      - ", 1)[0]

    # ── the one table the wait script derives both of its numbers from ────────
    # `wait_for_relink_run.sh` mirrors relink.yml's per-job `timeout-minutes:`
    # and its path→jobs mapping in a single `case`. Both readers below parse
    # THAT table rather than restating its numbers, so a test can assert what a
    # path costs without giving the script a second, drifting copy of it.
    def _relink_table(self):
        default_job_max = int(
            re.search(r"\nchild_job_max_min=(\d+)\n", self.relink_wait).group(1)
        )
        default_budget = re.search(
            r"\nchild_path_budget_min=\$\(\(([\d +]+)\)\)\n", self.relink_wait
        ).group(1)
        table = {"local": (default_job_max, default_budget)}
        for target, job_max, budget in re.findall(
            r"\n  (\w+)\) child_job_max_min=(\d+); child_path_budget_min=\$\(\(([\d +]+)\)\)",
            self.relink_wait,
        ):
            table[target] = (int(job_max), budget)
        self.assertEqual(sorted(table), ["kaggle", "local", "server"], table)
        return table

    def relink_path_budgets(self):
        """target -> the sum of the timeout-minutes of the jobs on that path."""
        return {
            target: sum(int(n) for n in budget.split("+"))
            for target, (_, budget) in self._relink_table().items()
        }

    def relink_job_maxima(self):
        """target -> the longest SINGLE job on that path."""
        return {target: job_max for target, (job_max, _) in self._relink_table().items()}

    def test_linker_reconciliation_is_failure_event_driven(self):
        workflow = FAILURE_RECONCILER.read_text(encoding="utf-8")
        header = workflow.split("jobs:\n", 1)[0]

        self.assertIn("workflow_run:", header)
        self.assertIn(
            "Weekly 5–6/6 · Build, link, validate and publish Seforim DB",
            header,
        )
        self.assertNotIn("schedule:", header)
        self.assertNotIn("cron:", header)
        self.assertIn(
            "if: ${{ github.event.workflow_run.conclusion != 'success' }}",
            workflow,
        )
        self.assertIn(
            "gh workflow run reconcile-pipeline.yml -R Otzaria/LinkerToOtzaria",
            workflow,
        )
        self.assertIn("GH_TOKEN: ${{ secrets.PIPELINE_TOKEN }}", workflow)

    def test_local_host_is_the_default_db_and_linker_target(self):
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        runner_input = self.workflow.split("      runner_selection:\n", 1)[1].split(
            "      prerelease:\n", 1
        )[0]

        self.assertIn("default: 'local'", runner_input)
        self.assertIn("- 'local'", runner_input)
        self.assertIn("otzaria-db", self.workflow)
        self.assertIn("vars.SERIAL_LINKER_TARGET || 'local'", relink)
        self.assertIn('local|kaggle|server)', relink)
        self.assertIn('-f library_run_id="$GITHUB_RUN_ID" -f target=local', relink)

    def test_durable_host_skips_reinstalling_existing_dependencies(self):
        root = Path(__file__).parents[2]
        installer = (root / ".github/scripts/install-db-workflow-deps.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn("for tool in gh sqlite3 zstd unzstd jq curl unzip", installer)
        self.assertIn("skipping package-manager work", installer)

    def test_disk_probe_ignores_absent_optional_wsl_drives(self):
        disk = self.step("Disk before build")

        self.assertIn("df -h /", disk)
        self.assertIn("if [ -d /mnt/c ]; then", disk)
        self.assertIn("df -h /mnt/c", disk)
        self.assertNotIn("run: df -h\n", disk)

    def test_durable_host_uses_preinstalled_java_25_without_network_setup(self):
        java = self.step("Verify durable Java 25 toolchain")

        self.assertNotIn("actions/setup-java", self.workflow)
        self.assertNotIn("gradle/actions/setup-gradle", self.workflow)
        self.assertIn("command -v java", java)
        self.assertIn("command -v javac", java)
        self.assertIn('[[ "$java_version" == 25 || "$java_version" == 25.* ]]', java)
        self.assertIn('echo "JAVA_HOME=$java_home" >> "$GITHUB_ENV"', java)

    def test_durable_host_uses_preinstalled_gradle_without_wrapper_download(self):
        select = self.step("Select the durable Gradle toolchain for source_commit")

        # From pipeline control: an old source_commit predates the selector.
        self.assertIn(
            'bash .pipeline-control/.github/scripts/select_gradle_toolchain.sh "$GITHUB_WORKSPACE"',
            select,
        )
        self.assertNotIn("./gradlew", self.workflow)
        self.assertNotIn("gradle/actions/setup-gradle", self.workflow)
        selector = (SCRIPTS_DIR / "select_gradle_toolchain.sh").read_text(encoding="utf-8")
        for download in ("gradlew", "curl", "wget", "services.gradle.org/distributions/gradle-$"):
            self.assertNotIn(download, selector)
        # Every Gradle run goes through the selected binary, never a PATH `gradle`.
        bare = re.compile(r"(^|[\s;(&|!])gradle\s", re.M)
        for name, text in (("workflow", self.workflow),
                           ("patch_fan_lib.sh", PATCH_FAN_LIB.read_text(encoding="utf-8"))):
            code = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("#"))
            self.assertIsNone(bare.search(code), f"bare gradle call in {name}")
        self.assertEqual(self.workflow.count('"$GRADLE" '), 6)
        order = [self.workflow.index(m) for m in (
            "      - name: Checkout SeforimLibrary\n",
            "      - name: Checkout immutable pipeline control scripts\n",
            "      - name: Verify durable Java 25 toolchain\n",
            "      - name: Select the durable Gradle toolchain for source_commit\n",
            '"$GRADLE" generateSeforimDb',
        )]
        self.assertEqual(order, sorted(order))

    def run_selector(self, wrapper_url, installed, reports=None):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "tree/gradle/wrapper").mkdir(parents=True)
            (root / "tree/gradle/wrapper/gradle-wrapper.properties").write_text(
                f"distributionBase=GRADLE_USER_HOME\ndistributionUrl={wrapper_url}\n", encoding="utf-8")
            for version in installed:
                bin_dir = root / f"tc/gradle-{version}/bin"
                bin_dir.mkdir(parents=True)
                (bin_dir / "gradle").write_text(
                    f"#!/bin/sh\necho\necho 'Gradle {(reports or {}).get(version, version)}'\n",
                    encoding="utf-8")
                (bin_dir / "gradle").chmod(0o755)
            env_file = root / "github_env"
            env_file.write_text("", encoding="utf-8")
            run = subprocess.run(
                ["bash", str(SCRIPTS_DIR / "select_gradle_toolchain.sh"), str(root / "tree")],
                capture_output=True, text=True,
                env={"PATH": os.environ["PATH"], "GITHUB_ENV": str(env_file),
                     "OTZARIA_GRADLE_TOOLCHAINS": str(root / "tc")},
            )
            return run, env_file.read_text(encoding="utf-8").replace(str(root), "$ROOT")

    def test_gradle_selector_follows_the_checked_out_wrapper(self):
        url = "https\\://services.gradle.org/distributions/gradle-{}-{}.zip"
        for version, flavor in (("9.1.0", "bin"), ("9.8.0", "all")):
            with self.subTest(version):
                run, env = self.run_selector(url.format(version, flavor), ["9.1.0", "9.8.0"])
                self.assertEqual(run.returncode, 0, run.stderr)
                self.assertEqual(env, f"GRADLE=$ROOT/tc/gradle-{version}/bin/gradle\n")

    def test_gradle_selector_fails_loudly(self):
        url = "https\\://services.gradle.org/distributions/gradle-{}-bin.zip"
        cases = {
            "not installed": (url.format("9.9.9"), ["9.1.0", "9.8.0"], None),
            "reports another version": (url.format("9.8.0"), ["9.8.0"], {"9.8.0": "9.1.0"}),
            "unparseable url": ("https\\://example.org/gradle-9.8.0-src.zip", ["9.8.0"], None),
        }
        for name, (wrapper_url, installed, reports) in cases.items():
            with self.subTest(name):
                run, env = self.run_selector(wrapper_url, installed, reports)
                self.assertNotEqual(run.returncode, 0)
                self.assertIn("::error::", run.stderr)
                self.assertEqual(env, "")

    def test_phase2_has_dedicated_heap_and_matching_host_headroom(self):
        mount = self.step("Mount RAM-backed build dir (tmpfs)")
        heaps = self.step("Bridle daemon heaps + generator forks for the 16 GB runner")

        # The gate must cover the tmpfs cap plus the Phase-2 heap: the atomic
        # publish holds the old DB and its candidate at once, so a 20 GiB tmpfs
        # can pin ~15.2 GiB while the 12 GiB heap is live.
        self.assertIn('size=20G', mount)
        self.assertIn('$((30*1024*1024))', mount)
        self.assertIn('(<30 GiB)', mount)
        self.assertIn('generatorHeap=8g', heaps)
        self.assertIn('linkerHeap=12g', heaps)

    def test_recovery_overlays_exact_phase2_sources_from_workflow_revision(self):
        checkout = self.step("Checkout immutable pipeline control scripts")
        overlay = self.step("Overlay pinned recovery Phase-2 implementation")
        apply_links = self.step("Apply LINKER links (Phase-2)")

        disk_allocator = (
            "generator/common/src/jvmMain/kotlin/io/github/kdroidfilter/"
            "seforimlibrary/common/ids/DiskBackedLinkIdAllocator.kt"
        )
        importer = (
            "generator/sefariasqlite/src/jvmMain/kotlin/io/github/kdroidfilter/"
            "seforimlibrary/sefariasqlite/GenerateLinkerLinks.kt"
        )
        self.assertIn(disk_allocator, checkout)
        self.assertIn(importer, checkout)
        self.assertIn("if: inputs.relink_recovery_run_id != ''", overlay)
        self.assertIn('install -m 0644 ".pipeline-control/$file" "$file"', overlay)
        self.assertIn('legacy = \'jvmArgs = listOf("-Xmx6g")\'', overlay)
        self.assertIn(
            'configured = \'jvmArgs = listOf("-Xmx$linkerHeap", "-XX:+UseG1GC")\'',
            overlay,
        )
        self.assertIn("if legacy_count == 1 and configured_count == 0", overlay)
        self.assertIn("elif legacy_count == 0 and configured_count == 1", overlay)
        self.assertIn("text = text.replace(legacy, configured)", overlay)
        self.assertIn("grep -Fxq 'linkerHeap=12g'", overlay)
        self.assertIn("DiskBackedLinkIdAllocator", overlay)
        self.assertIn("InMemoryIdAllocator", overlay)
        self.assertIn("grep -Fq", overlay)
        self.assertNotIn("rg -q", overlay)
        self.assertIn("rm -rf generator/common/build generator/sefariasqlite/build", overlay)
        self.assertLess(
            self.workflow.index("      - name: Overlay pinned recovery Phase-2 implementation\n"),
            self.workflow.index("      - name: Apply LINKER links (Phase-2)\n"),
        )
        self.assertIn('"$GRADLE" :sefariasqlite:generateLinkerLinks', apply_links)

    def test_phase2_implementation_commit_is_part_of_release_identity(self):
        lookup = self.step("Find and verify exact provenance")
        stage = self.step("Stage release assets")

        expression = "${{ inputs.relink_recovery_run_id != '' && github.sha || inputs.source_commit }}"
        self.assertGreaterEqual(self.workflow.count(f"PHASE2_IMPLEMENTATION_COMMIT: {expression}"), 3)
        self.assertIn('--arg phase2 "$PHASE2_IMPLEMENTATION_COMMIT"', lookup)
        self.assertIn(".phase2_implementation_commit==$phase2", lookup)
        self.assertIn('"schema_version": 5', stage)
        self.assertIn(
            '"phase2_implementation_commit": os.environ["PHASE2_IMPLEMENTATION_COMMIT"]',
            stage,
        )

    def test_patch_fan_skips_only_anchors_the_producer_declares_unpatchable(self):
        # A column added without a db_schema_version bump (category.heShortDesc,
        # 2026-07-16) is now handled inside PatchDbProducer, which emits an
        # ALTER TABLE … ADD COLUMN migration. The old shell pre-check that
        # compared PRAGMA table_info and dropped the anchor must be gone: the
        # producer is the single authority on what is patchable, and it says so
        # by exiting 3 and writing "<out>.unpatchable" next to the patch.
        patch_fan = self.step("Produce + verify patch fan")
        # produce_anchor moved into patch_fan_lib.sh; the old pre-check must be
        # absent from both halves of what this step runs.
        lib = self.fan_lib
        for gone in ("MISSING_COLUMNS", 'PRAGMA table_info("{t}")',
                     "patches cannot add columns"):
            self.assertNotIn(gone, patch_fan)
            self.assertNotIn(gone, lib)

        gradle_at = lib.index('"${GRADLE:?no Gradle toolchain selected}" :generator-common:producePatchAndVerify')
        # The marker is cleared before the producer runs, so a stale file from
        # an earlier anchor can never skip a good one.
        clear_at = lib.index('rm -f "$PATCH_OUT.unpatchable"')
        self.assertLess(clear_at, gradle_at)
        self.assertIn("-Pout=$PATCH_OUT", lib)
        skip_at = lib.index("producer declared the anchor unpatchable; skip anchor")
        self.assertLess(gradle_at, skip_at)
        # Run directly the CLI's exit 3 arrives in the shell; run through Gradle
        # it is already turned into a warning + a zero exit. Both reach the one
        # marker path.
        self.assertIn(
            'if [ "$PRODUCE_RC" -eq 3 ] || [ -f "$PATCH_OUT.unpatchable" ]; then',
            lib,
        )
        # A skipped anchor must leave patches/ clean: the marker, the producer's
        # half-built .tmp and any stale .db all go.
        self.assertIn(
            'rm -f "$PATCH_OUT.unpatchable" "$PATCH_OUT" "$PATCH_OUT.tmp"',
            lib[skip_at:],
        )
        self.assertIn('rm -rf "$ANCHOR_DIR"\n    return 0', lib[skip_at:])
        # Exit 3 is the only tolerated failure mode; everything else still
        # aborts the job through set -e.
        self.assertIn("exits 3", lib)
        self.assertIn("Every other non-zero exit still fails the release", lib)
        # Patch compression at L19 parallelises across cores; L22 ran single-core.
        self.assertIn('ZSTD_LEVEL: "19"', patch_fan)
        # Skips are warnings, but zero patches with prior releases must fail —
        # in the driver, after every anchor the loop produced.
        self.assertIn("patch fan produced no patch although prior releases exist", patch_fan)
        self.assertLess(
            patch_fan.index("for ROW in "),
            patch_fan.index("patch fan produced no patch although prior releases exist"),
        )

    def test_a_corpus_wide_size_guard_skip_still_publishes_a_full_only_release(self):
        # A build that churns every line legitimately loses every anchor to the
        # delta size guard (Otzaria issue #1211). That is a degraded release —
        # seforim.db.zst + buildstate, no patches — not a broken patch contract.
        patch_fan = self.step("Produce + verify patch fan")
        # produce_anchor lives in patch_fan_lib.sh, sourced into the step's shell.
        fan_lib = self.fan_lib

        # The kind of every skip outlives its subshell as a file, because the
        # anchors run in background subshells that cannot share a variable.
        self.assertIn('SKIP_DIR="$RUNNER_TEMP/patch-fan-skips"', fan_lib)
        self.assertIn("record_skip() {", fan_lib)
        # Every skip path records, so "no patch and no recorded skip" stays an error.
        self.assertEqual(fan_lib.count("record_skip "), 3)
        self.assertIn('record_skip structural "$TARGET_VER" "${PRECHECK#* }"', fan_lib)
        self.assertIn('record_skip "$SKIP_KIND" "$TARGET_VER"', fan_lib)

        # The oversized/structural split is driven by the token PatchSizeGuard
        # puts at the head of its marker — the two must not drift apart.
        token = re.search(
            r'MARKER_REASON_TOKEN: String = "([^"]+)"',
            PATCH_SIZE_GUARD.read_text(encoding="utf-8"),
        )
        self.assertIsNotNone(token, "PatchSizeGuard must expose MARKER_REASON_TOKEN")
        self.assertIn(
            f'case "$REASON" in {token.group(1)}:*) SKIP_KIND=oversized ;; esac',
            fan_lib,
        )

        # No patch + at least one size-guard skip: warn and carry on.
        warn_at = patch_fan.index("::warning::patch fan produced no patch:")
        err_at = patch_fan.index("patch fan produced no patch although prior releases exist")
        self.assertLess(warn_at, err_at)
        self.assertIn('if [ "$OVERSIZED_SKIPS" -gt 0 ]; then', patch_fan)
        self.assertNotIn("exit 1", patch_fan[warn_at:err_at])
        # No patch and no size-guard skip is still a hard failure.
        self.assertIn("exit 1", patch_fan[err_at:])

        # Staging a release with zero patches is already guarded.
        self.assertIn(
            'if compgen -G "patches/patch-*.db.zst" > /dev/null; then',
            self.step("Stage release assets"),
        )

    def test_the_fan_downloads_anchors_with_the_preflighted_release_credential(self):
        # The fan runs after the relink wait, which may outlive the job's
        # automatic GITHUB_TOKEN: a 401 there would lose the whole expensive
        # build at `gh release download` of the anchor's seforim.db.zst.
        lib = self.fan_lib
        use_at = lib.index('use_token "${RELEASE_TOKEN_KIND:-}"')
        self.assertLess(
            lib.index('. "$(dirname "${BASH_SOURCE[0]}")/release_draft.sh"'), use_at
        )
        self.assertLess(use_at, lib.index("gh release download"))
        # The preflighted credentials themselves reach the step from its env.
        patch_fan = self.step("Produce + verify patch fan")
        self.assertIn("AUTOMATIC_TOKEN: ${{ secrets.GITHUB_TOKEN }}", patch_fan)
        self.assertIn("CROSS_REPO_TOKEN: ${{ secrets.PIPELINE_TOKEN }}", patch_fan)

    def test_patch_fan_decides_unpatchable_anchors_before_downloading_them(self):
        # An anchor the producer will reject costs 110–135 s of download plus a
        # decompress before anyone learns that (run 33865604251, anchor v10).
        # The cheap verdict must therefore come BEFORE `gh release download` of
        # seforim.db.zst, and must never be able to fail the release.
        fingerprint = self.step("Fingerprint published DB schema")
        self.assertIn(
            "patch_anchor_schema.py \\\n            dump build/seforim.db > build/db_schema.json",
            fingerprint,
        )
        # It reads build/seforim.db, so it has to sit before the fan (which
        # deletes nothing) and before the compress step that supersedes it.
        self.assertLess(
            self.workflow.index("      - name: Fingerprint published DB schema\n"),
            self.workflow.index("      - name: Produce + verify patch fan\n"),
        )
        self.assertLess(
            self.workflow.index("      - name: Fingerprint published DB schema\n"),
            self.workflow.index("      - name: Compress Seforim Database (zstd)\n"),
        )

        # The per-anchor body is produce_anchor, in patch_fan_lib.sh.
        lib = self.fan_lib
        precheck_at = lib.index("patch_anchor_schema.py check")
        db_download_at = lib.index('download_full_db_by_name "$TAG" "$CANDIDATE"')
        self.assertLess(precheck_at, db_download_at)
        # Only the tiny provenance asset is fetched to decide.
        self.assertLess(
            lib.index("--pattern 'build_provenance.json'"), db_download_at
        )
        self.assertIn("--anchor-version \"$TARGET_VER\"", lib)
        self.assertIn("--this-schema build/db_schema.json", lib)
        # The column comparison is scoped to the producer's own table list, and
        # that list comes from the PAYLOAD checkout — the same commit whose
        # PatchDbProducer runs — not from .pipeline-control. Comparing more
        # tables than the producer does would skip anchors it would have patched.
        contract = "generator/common/src/jvmTest/resources/patch_tables_contract.json"
        self.assertIn(f"--contract-tables {contract}", lib)
        self.assertNotIn(f"--contract-tables .pipeline-control/{contract}", lib)
        self.assertTrue((Path(__file__).parents[2] / contract).is_file())
        # Advisory-safe: a missing asset or a crashing pre-check degrades to
        # PROCEED instead of aborting the job under `set -e`. The download is
        # inside an `if !`, so its non-zero exit is handled, not fatal — and it
        # now names the asset gh only complained about on stderr.
        self.assertIn(
            'if ! gh release download "$TAG" \\\n'
            "       --pattern 'build_provenance.json' \\\n"
            '       --dir "$META_DIR" 2>"$META_DIR/gh.err"; then',
            lib,
        )
        self.assertIn(
            "release carries no build_provenance.json"
            " ($(tr -d '\\r' < \"$META_DIR/gh.err\" | head -n1))",
            lib,
        )
        self.assertIn('|| PRECHECK="PROCEED pre-check did not run', lib)
        # A pre-check skip is announced with the same ::warning::anchor …
        # skip anchor line shape as the producer's marker path, and leaves the
        # loop exactly as that path does — nothing downloaded, nothing staged.
        skip_at = lib.index("pre-download schema check declared the anchor unpatchable; skip anchor")
        self.assertIn(
            '::warning::anchor v${TARGET_VER} ($TAG): ${PRECHECK#* } —', lib
        )
        self.assertLess(precheck_at, skip_at)
        self.assertLess(skip_at, db_download_at)
        # The producer's own marker path is untouched and still authoritative.
        self.assertLess(
            db_download_at, lib.index("producer declared the anchor unpatchable; skip anchor")
        )
        # prev-meta is run-scoped like prev-dbs.
        self.assertIn(
            "rm -rf prev-dbs prev-meta patches release-staging prior-versions.tsv",
            self.workflow,
        )

    def test_provenance_publishes_the_schema_a_future_anchor_check_needs(self):
        stage = self.step("Stage release assets")
        self.assertIn(
            'python3 - "$STAGE" "$MANUAL_LINKS_LINEAGE" build/db_schema.json', stage
        )
        self.assertIn(
            'db_schema = json.loads(pathlib.Path(sys.argv[3]).read_text(encoding="utf-8"))',
            stage,
        )
        self.assertIn('"db_schema": db_schema,', stage)
        # Published and compared blocks share one producer, so they cannot drift.
        self.assertEqual(
            self.workflow.count("patch_anchor_schema.py \\\n            dump build/seforim.db"), 1
        )

    def test_patch_fan_allows_only_the_supported_schema_transitions(self):
        # The transition gate and the producer call are both inside
        # produce_anchor, which patch_fan_lib.sh holds.
        patch_fan = self.fan_lib

        supported = (
            '[ "$PREV_SCHEMA" = 1 ] && [ "$THIS_SCHEMA" = 4 ]',
            '[ "$PREV_SCHEMA" = 2 ] && [ "$THIS_SCHEMA" = 3 ]',
            '[ "$PREV_SCHEMA" = 2 ] && [ "$THIS_SCHEMA" = 4 ]',
            '[ "$PREV_SCHEMA" = 3 ] && [ "$THIS_SCHEMA" = 4 ]',
            '[ "$PREV_SCHEMA" = 1 ] && [ "$THIS_SCHEMA" = 5 ]',
            '[ "$PREV_SCHEMA" = 2 ] && [ "$THIS_SCHEMA" = 5 ]',
            '[ "$PREV_SCHEMA" = 3 ] && [ "$THIS_SCHEMA" = 5 ]',
            '[ "$PREV_SCHEMA" = 4 ] && [ "$THIS_SCHEMA" = 5 ]',
        )
        for transition in supported:
            self.assertIn(transition, patch_fan)
            self.assertLess(
                patch_fan.index(transition),
                patch_fan.index('"${GRADLE:?no Gradle toolchain selected}" :generator-common:producePatchAndVerify'),
            )
        self.assertIn("producing the supported cross-schema delta", patch_fan)
        self.assertIn("is unsupported — skip anchor", patch_fan)
        self.assertNotIn("cross-schema delta unsupported", patch_fan)
        self.assertNotIn('[ "$PREV_SCHEMA" = 1 ] && [ "$THIS_SCHEMA" = 2 ]', patch_fan)
        self.assertNotIn('[ "$PREV_SCHEMA" = 1 ] && [ "$THIS_SCHEMA" = 3 ]', patch_fan)

    def test_patch_fan_produces_two_anchors_at_a_time(self):
        # Run 33865604251 spent 2015 s in the fan, 284–382 s per anchor, on work
        # that is single-threaded almost end to end. Anchors now run
        # PATCH_FAN_PARALLELISM at a time (default 2).
        patch_fan = self.step("Produce + verify patch fan")
        lib = self.fan_lib

        self.assertIn('FAN_PARALLELISM="${PATCH_FAN_PARALLELISM:-2}"', patch_fan)
        # A junk or zero value degrades to serial instead of running an
        # unbounded (or empty) batch.
        self.assertIn("''|*[!0-9]*|0)", patch_fan)
        self.assertIn("FAN_PARALLELISM=1", patch_fan)

        # Per-anchor isolation: every path the body touches carries the offset,
        # so two bodies never share a prev DB, a provenance dir or a log.
        self.assertIn('local ANCHOR_DIR="prev-dbs/anchor-$OFFSET"', lib)
        self.assertIn('local META_DIR="prev-meta/anchor-$OFFSET"', lib)
        self.assertIn('local PREV_DB="$ANCHOR_DIR/seforim.db"', lib)
        self.assertIn(
            'ANCHOR_LOG="$RUNNER_TEMP/patch-fan-anchor-$OFFSET.log"', patch_fan
        )
        # The shared prev-dbs/ and prev-meta/ working dirs of the serial loop
        # must be gone — one anchor wiping them would pull the DB out from
        # under its sibling.
        body = lib.split("produce_anchor() {", 1)[1]
        self.assertNotIn("rm -rf prev-dbs\n", body)
        self.assertNotIn("rm -rf prev-meta\n", body)
        # The "give up on the prefetch" verdict crosses subshells as a file,
        # since a shell variable cannot.
        self.assertIn(': > "$PREFETCH_DIR/.abandoned"', lib)
        self.assertIn('if [ -f "$PREFETCH_DIR/.abandoned" ]; then', lib)

        # Dispatch: background, own log, no shared stdin to swallow.
        self.assertIn(
            'produce_anchor "$OFFSET" "$TARGET_VER" "$TAG" > "$ANCHOR_LOG" 2>&1 '
            "< /dev/null &",
            patch_fan,
        )
        # Exit codes are collected per job — `wait <pid>` one at a time, never a
        # bare `wait` that would throw the statuses away. drain_batch is in the
        # library now, so the bare-`wait` check reads its lines rather than one
        # indentation the step no longer uses.
        self.assertIn('wait "${BATCH_PIDS[$i]}" || status=$?', lib)
        self.assertEqual(
            [line for line in lib.splitlines() if line.strip() == "wait"], []
        )
        # Logs are replayed in offset order once the batch drains, so
        # `anchor vNN timings:` (which the pipeline tracker greps) keeps its
        # exact shape on a line of its own.
        self.assertIn('cat "${BATCH_LOGS[$i]}" || true', lib)
        self.assertIn(
            'echo "anchor v${TARGET_VER} timings: download=$((T_DOWNLOADED - T_START))s '
            "extract=$((T_EXTRACTED - T_DOWNLOADED))s "
            "produce+verify+compress=$((T_DONE - T_EXTRACTED))s "
            'total=$((T_DONE - T_START))s"',
            lib,
        )
        # A genuine failure still fails the step: the sibling is drained first
        # (never killed mid-write), no further batch starts, and the step exits
        # with the anchor's own code.
        self.assertIn("drain_batch || FAN_FAILURE=$?", patch_fan)
        self.assertIn('[ "$FAN_FAILURE" -eq 0 ] || break', patch_fan)
        self.assertIn('exit "$FAN_FAILURE"', patch_fan)
        self.assertLess(
            patch_fan.index('exit "$FAN_FAILURE"'),
            patch_fan.index("wait buildstate 3600"),
        )
        # Legitimate skips and produced patches alike come back as 0.
        self.assertIn(
            'echo "::error::anchor v${TARGET_VER} ($TAG): producePatchAndVerify '
            'failed with exit code $PRODUCE_RC"',
            lib,
        )
        self.assertIn('return "$PRODUCE_RC"', lib)

        # A2 stays exactly where it was: started inside this step, waited for
        # inside this step, on the success path only (as before).
        self.assertIn(
            "upload_early_release_assets.sh \\\n            start buildstate "
            "build/seforim.db.buildstate",
            patch_fan,
        )
        self.assertIn(
            "upload_early_release_assets.sh \\\n            wait buildstate 3600",
            patch_fan,
        )
        # …and it is started before the first anchor is dispatched, which is
        # where the fan's ~30 minutes of pure CPU begin.
        self.assertLess(
            patch_fan.index("start buildstate"),
            patch_fan.index('produce_anchor "$OFFSET"'),
        )

    def test_patch_fan_java_launcher_cannot_drift_from_the_gradle_task(self):
        # Two concurrent anchors cannot both go through Gradle (one project
        # cache dir, one configuration cache, one jvmJar output), so the fan
        # runs PatchPipelineCli directly. What it runs is published by the
        # producePatchAndVerify task itself — same vals, no second source of
        # truth.
        gradle = GENERATOR_COMMON_BUILD.read_text(encoding="utf-8")
        main_class = (
            "io.github.kdroidfilter.seforimlibrary.common.patch.PatchPipelineCliKt"
        )

        self.assertIn(f'val patchPipelineMainClass = "{main_class}"', gradle)
        self.assertIn(
            "val patchPipelineJvmArgs =\n"
            '    listOf("-Xmx$generatorHeap", "-XX:+UseG1GC", '
            '"--enable-native-access=ALL-UNNAMED")',
            gradle,
        )
        self.assertIn(
            "fun patchPipelineClasspath() = files(tasks.named(\"jvmJar\")) + "
            'configurations.getByName("jvmRuntimeClasspath")',
            gradle,
        )
        # The fork's own definition reads those three and nothing else, so the
        # literals exist exactly once in the file.
        self.assertIn("mainClass.set(patchPipelineMainClass)", gradle)
        self.assertIn("classpath = patchPipelineClasspath()", gradle)
        self.assertIn("jvmArgs = patchPipelineJvmArgs", gradle)
        self.assertEqual(gradle.count(f'"{main_class}"'), 1)
        # producePatchAndVerify itself must not re-state the heap either — it
        # reads the val, so the launcher cannot publish a different one.
        produce_task = gradle.split('tasks.register<JavaExec>("producePatchAndVerify")', 1)[1]
        produce_task = produce_task.split("\ntasks.register", 1)[0]
        self.assertNotIn("-Xmx", produce_task)

        # …and the launcher spec republishes exactly them, plus the toolchain
        # the fork would have used.
        self.assertIn('tasks.register("patchPipelineLauncher")', gradle)
        for line in (
            "val launcherMainClass = patchPipelineMainClass",
            "val launcherJvmArgs = patchPipelineJvmArgs",
            "val launcherClasspath = patchPipelineClasspath()",
            "val launcherJavaVersion = libs.versions.jvmToolchain.get()",
            '"mainClass=$launcherMainClass\\n"',
            '"jvmArgs=${launcherJvmArgs.joinToString(" ")}\\n"',
            '"javaVersion=$launcherJavaVersion\\n"',
            '"classpath=${launcherClasspath.asPath}\\n"',
        ):
            self.assertIn(line, gradle)

        launcher_step = self.step("Materialise the patch-pipeline launcher")
        patch_fan = self.step("Produce + verify patch fan")
        spec = "generator/common/build/patch-pipeline-launcher.properties"

        self.assertIn("if: steps.discover.outputs.has_prev == 'true'", launcher_step)
        self.assertIn(f"LAUNCHER={spec}", launcher_step)
        self.assertIn(
            '"$GRADLE" :generator-common:patchPipelineLauncher --no-daemon', launcher_step
        )
        # Opportunistic: a payload commit without the task leaves no spec and
        # the fan falls back to its unchanged serial Gradle loop.
        self.assertIn("::warning::no :generator-common:patchPipelineLauncher", launcher_step)
        self.assertLess(
            self.workflow.index("      - name: Materialise the patch-pipeline launcher\n"),
            self.workflow.index("      - name: Produce + verify patch fan\n"),
        )

        self.assertIn(f"LAUNCHER={spec}", patch_fan)
        for key in ("mainClass", "jvmArgs", "classpath", "javaVersion"):
            self.assertIn(f"sed -n 's/^{key}=//p' \"$LAUNCHER\"", patch_fan)
        # The `java` about to be run must be the toolchain Gradle would fork.
        self.assertIn("java.specification.version", patch_fan)
        self.assertIn('[ "$JAVA_MAJOR" != "$LAUNCHER_JAVA" ]', patch_fan)
        self.assertIn("falling back to Gradle", patch_fan)
        # Same heap, same classpath, same main class, same inputs — the -P
        # properties of the Gradle task become the -D system properties it sets
        # on its fork. Both invocations are produce_anchor's, in the library the
        # step sources; the step reads the spec and exports the three values.
        lib = self.fan_lib
        self.assertIn(
            'java $PATCH_JVM_ARGS -cp "$PATCH_CLASSPATH"', lib
        )
        self.assertIn('"$PATCH_MAIN_CLASS" || PRODUCE_RC=$?', lib)
        produce = lib.split("produce_anchor() {", 1)[1]
        java_args = re.findall(r"-D(\w+)=", produce)
        gradle_args = re.findall(r"-P(\w+)=", produce)
        self.assertEqual(java_args, gradle_args)
        self.assertEqual(
            java_args, ["prevDb", "newDb", "out", "fromVersion", "toVersion"]
        )
        # The schema barrier runs the same way: launcher class when published,
        # else the Gradle task, with the same properties on both paths.
        barrier = lib.split("write_barrier() {", 1)[1].split("\n}\n", 1)[0]
        self.assertEqual(re.findall(r"-D(\w+)=", barrier), re.findall(r"-P(\w+)=", barrier))
        self.assertEqual(
            re.findall(r"-D(\w+)=", barrier),
            ["out", "fromVersion", "toVersion", "fromSchemaVersion", "toSchemaVersion"],
        )
        self.assertIn('"${GRADLE:?no Gradle toolchain selected}" :generator-common:writeSchemaBarrier', barrier)
        self.assertIn('"barrierMainClass=$launcherBarrierMainClass\\n"', gradle)
        self.assertIn("mainClass.set(schemaBarrierMainClass)", gradle)
        # ZSTD_LEVEL keeps reaching the CLI as an env var on both paths, so the
        # patch bytes do not depend on which one ran.
        self.assertIn('ZSTD_LEVEL: "19"', patch_fan)

    def test_pinned_sefaria_archive_uses_its_explicit_root_contract(self):
        extract = self.step("Verify pinned lineage and extract exact inputs")

        self.assertIn('SEFARIA_EXTRACT_ROOT="$INPUTS/sefaria-extract"', extract)
        self.assertIn('test -d "$SEFARIA_EXTRACT_ROOT/json"', extract)
        self.assertIn('test -d "$SEFARIA_EXTRACT_ROOT/schemas"', extract)
        self.assertIn("root json/ contains no JSON files", extract)
        self.assertIn("root schemas/ contains no JSON files", extract)
        self.assertNotIn("SEFARIA_DB_ROOTS", extract)

    def test_self_hosted_release_digests_are_read_through_rest(self):
        extract = self.step("Verify pinned lineage and extract exact inputs")
        apply_links = self.step("Apply LINKER links (Phase-2)")
        publish = self.step("Create draft, verify every uploaded asset, then publish")

        self.assertIn(
            'repos/otzaria/otzaria-library/releases/tags/$OTZARIA_TAG', extract
        )
        self.assertIn(
            'repos/Otzaria/LinkerToOtzaria/releases/tags/$LINKER_RELEASE_TAG',
            apply_links,
        )
        self.assertIn(
            'repos/$GITHUB_REPOSITORY/releases/tags/$RELEASE_TAG', publish
        )
        self.assertIn("release_assets_json", publish)
        for step in (extract, apply_links, publish):
            self.assertNotIn("gh release view", step)

    def test_release_write_is_probed_before_the_expensive_build(self):
        probe = self.step("Preflight release write credentials")
        self.assertLess(
            self.workflow.index("      - name: Preflight release write credentials\n"),
            self.workflow.index("      - name: Mount RAM-backed build dir (tmpfs)\n"),
        )
        self.assertIn("[ \"$code\" = 200 ]", probe)
        self.assertIn(".permissions.push == true", probe)
        self.assertNotIn("--request POST", probe)
        self.assertIn("RELEASE_AUTOMATIC_WRITABLE", probe)
        self.assertIn("RELEASE_CROSS_REPO_WRITABLE", probe)
        self.assertIn("RELEASE_TOKEN_KIND=automatic", probe)
        self.assertIn("RELEASE_TOKEN_KIND=cross-repo", probe)
        self.assertLess(
            probe.index('if [ "$cross_repo_writable" = true ]'),
            probe.index('elif [ "$automatic_writable" = true ]'),
        )

    def test_publisher_reconciles_and_falls_back_only_to_preflighted_credentials(self):
        # The create/reconcile/upload contract lives in ONE sourced script,
        # because the early upload (in the patch fan) drives the very same draft
        # release; two copies could drift apart.
        publish = self.step("Create draft, verify every uploaded asset, then publish")
        machinery = RELEASE_DRAFT.read_text(encoding="utf-8")
        self.assertIn("AUTOMATIC_TOKEN: ${{ secrets.GITHUB_TOKEN }}", publish)
        self.assertIn("CROSS_REPO_TOKEN: ${{ secrets.PIPELINE_TOKEN }}", publish)
        self.assertIn(
            "source .pipeline-control/.github/scripts/release_draft.sh", publish
        )
        self.assertIn('use_token "$RELEASE_TOKEN_KIND"', publish)
        self.assertIn('switch_token()', machinery)
        self.assertIn('export GH_TOKEN="${AUTOMATIC_TOKEN:-}"', machinery)
        self.assertIn('export GH_TOKEN="${CROSS_REPO_TOKEN:-}"', machinery)
        self.assertIn('exact-draft', machinery)
        self.assertIn('for asset_path in release-staging/*', publish)
        self.assertNotIn('gh release upload "$RELEASE_TAG" "$asset_path" --clobber', machinery)
        self.assertNotIn('gh release upload "$RELEASE_TAG" "$asset_path" --clobber', publish)
        self.assertIn("Never use --clobber.", machinery)
        self.assertIn(
            'gh api --paginate "repos/$GITHUB_REPOSITORY/releases?per_page=100"', machinery
        )
        self.assertIn("Draft releases are not", machinery)
        self.assertIn('RELEASE_ID="$(resolve_release_id)"', publish)
        self.assertIn('repos/$GITHUB_REPOSITORY/releases/$RELEASE_ID', machinery)
        self.assertNotIn('releases/tags/$RELEASE_TAG" > "$output"', machinery)
        # A draft is adopted only when its identity is exactly this build's and
        # it carries nothing but the assets this build uploads early.
        self.assertIn('EARLY_RELEASE_ASSETS="seforim.db.buildstate.zst"', machinery)
        self.assertIn("set(names) <= allowed", machinery)
        self.assertIn("len(names)==len(set(names))", machinery)

    def test_patch_fan_anchors_are_prefetched_while_the_db_is_generated(self):
        # The fan paid 110-135 s per anchor to download a 1.3 GB seforim.db.zst
        # with the CPU idle (run 33865604251). The tags are known before the DB
        # build, which spends ~36 minutes without touching the network.
        prefetch = self.step("Prefetch patch-fan anchor DBs (background)")
        fan = self.step("Produce + verify patch fan")
        cleanup = self.step(
            "Clean run-scoped disk leftovers (workspace persists on self-hosted)"
        )
        script = ANCHOR_PREFETCH.read_text(encoding="utf-8")

        self.assertLess(
            self.workflow.index("      - name: Auto-discover prior releases\n"),
            self.workflow.index(
                "      - name: Prefetch patch-fan anchor DBs (background)\n"
            ),
        )
        self.assertLess(
            self.workflow.index(
                "      - name: Prefetch patch-fan anchor DBs (background)\n"
            ),
            self.workflow.index("      - name: Generate Seforim Database\n"),
        )
        self.assertIn("if: steps.discover.outputs.has_prev == 'true'", prefetch)

        # ONE derivation of offset -> tag, used by the prefetch and by the fan:
        # a prefetch that resolved an offset differently would fetch the wrong DB.
        derivation = (
            'patch_fan_anchors.sh \\\n'
            '            "$THIS_VER" prior-versions.tsv $PATCH_OFFSETS > "$ANCHORS"'
        )
        self.assertIn(derivation, prefetch)
        self.assertIn(derivation, fan)
        self.assertTrue(ANCHOR_DERIVATION.is_file())
        self.assertNotIn("awk -F'\\t' -v v=\"$TARGET_VER\"", fan)
        for status in ("ANCHOR", "BELOW-ONE", "NO-RELEASE"):
            self.assertIn(status, ANCHOR_DERIVATION.read_text(encoding="utf-8"))
            self.assertIn(status, fan)

        # Onto the workspace disk, never onto the 16 GiB tmpfs build/.
        self.assertIn('start "$ANCHORS" prefetch', prefetch)
        self.assertNotIn("build/prefetch", self.workflow)

        # Task C's verdict still gates the download: the prefetch runs the same
        # check with the same arguments and never fetches a rejected anchor.
        self.assertIn("patch_anchor_schema.py", script)
        self.assertIn('--this-schema "$THIS_SCHEMA"', script)
        self.assertIn('--anchor-version "$version"', script)
        self.assertIn('--contract-tables "$CONTRACT_TABLES"', script)
        self.assertIn(
            "generator/common/src/jvmTest/resources/patch_tables_contract.json", script
        )
        self.assertLess(
            script.index('[ "${verdict%% *}" = UNPATCHABLE ]'),
            script.index('download_full_db_by_name "$tag" "$expected_name"'),
        )
        # Verified against the release asset's own published size and digest.
        self.assertIn("size mismatch", script)
        self.assertIn("digest mismatch", script)
        self.assertIn('sha256:$(sha256sum "$file"', script)
        self.assertNotIn("gh release view", script)

        # The fan waits for THIS tag's marker, then falls back unchanged — in
        # produce_anchor, which the step sources from patch_fan_lib.sh.
        lib = self.fan_lib
        self.assertIn('while [ ! -f "$PREFETCH_DIR/$TAG/.done" ]', lib)
        # The budget starts at PREFETCH_WAIT_SECONDS and drops to 0 for every
        # anchor after one has given up — a file, because the anchors run in
        # their own subshells now. PREFETCH_WAIT_SECONDS itself is the step's,
        # read by the library at call time.
        self.assertIn('PREFETCH_WAIT_SECONDS=900', fan)
        self.assertIn('WAIT_BUDGET="$PREFETCH_WAIT_SECONDS"', lib)
        self.assertIn('"$PREFETCH_WAITED" -lt "$WAIT_BUDGET"', lib)
        self.assertIn('if [ -f "$PREFETCH_DIR/.abandoned" ]; then\n    WAIT_BUDGET=0', lib)
        self.assertIn(
            'if [ "$PREFETCH_STATE" = ok ] && [ -s "$PREFETCH_DIR/$TAG/seforim.db.zst" ]; then',
            lib,
        )
        self.assertIn(
            'mv "$PREFETCH_DIR/$TAG/seforim.db.zst" "$ANCHOR_DIR/seforim.db.zst"', lib
        )
        self.assertIn("falling back to the serial download", lib)
        self.assertIn("prefetch_patch_anchors.sh abort", lib)
        self.assertLess(
            lib.index("PREFETCH_STATE=absent"), lib.index('download_full_db_by_name "$TAG" "$CANDIDATE"')
        )
        # A prefetch timing line per anchor, like the fan's own, and it now says
        # whether the bytes came off the network or out of the durable cache.
        self.assertIn(
            'prefetch anchor v$version ($tag) timings: precheck=$((t_checked -'
            " t_start))s download=$((t_fetched - t_checked))s verify=$((t_verified"
            " - t_fetched))s total=$((t_verified - t_start))s source=$source",
            script,
        )

        # Nothing background outlives the job; prefetch/ goes with prev-dbs.
        self.assertIn("prefetch_patch_anchors.sh abort prefetch", cleanup)
        self.assertIn(
            "rm -rf prev-dbs prev-meta patches release-staging prior-versions.tsv prefetch",
            cleanup,
        )
        # A stale pid on this weeks-old runner must never be signalled blindly.
        self.assertIn("ps -o args= -p \"$pid\"", script)

    def test_the_anchor_cache_outlives_a_failed_attempt(self):
        # Run 34021998271 downloaded 4 x 1.3 GB of anchors (237-249 s each), died
        # 45 min later on an unrelated assert, and the cleanup deleted the lot;
        # the successful retry re-downloaded the identical bytes (~16 min). The
        # assets are immutable, so the second download was pure loss.
        script = ANCHOR_PREFETCH.read_text(encoding="utf-8")
        prefetch = self.step("Prefetch patch-fan anchor DBs (background)")
        generate = self.step("Generate Seforim Database")
        cleanup = self.step(
            "Clean run-scoped disk leftovers (workspace persists on self-hosted)"
        )

        # ── where ────────────────────────────────────────────────────────────
        # The same durable per-runner cache root the image embedder already uses,
        # and NOT the workspace (deleted below) or $RUNNER_TEMP (swept below).
        cache_root = "${XDG_CACHE_HOME:-$HOME/.cache}/seforimlibrary"
        self.assertIn(f'IMAGE_CACHE_DIR="{cache_root}/textimages"', generate)
        self.assertIn(f"{cache_root}/patch-anchors", script)
        self.assertIn("PATCH_ANCHOR_CACHE_DIR", script)
        self.assertNotIn("RUNNER_TEMP", script)
        # $GITHUB_WORKSPACE appears once, and only to REFUSE a cache root under
        # the workspace's tmpfs build dir — never to build one.
        self.assertNotIn('CACHE_DIR="$GITHUB_WORKSPACE', script)
        self.assertNotIn("CACHE_DIR=${GITHUB_WORKSPACE", script)
        # build/ is a 16 GiB tmpfs: the cache must refuse it by name AND by the
        # filesystem it actually lands on.
        self.assertIn(
            "is inside the tmpfs build dir — caching disabled for this run", script
        )
        self.assertIn("tmpfs|ramfs)", script)
        self.assertIn("stat -f -c '%T'", script)

        # ── keyed by tag, verified by digest before reuse ─────────────────────
        self.assertIn('dir="$CACHE_DIR/$tag"', script)
        self.assertIn('actual="sha256:$(sha256sum "$file" | cut -d\' \' -f1)"', script)
        self.assertIn(
            "::warning::patch-fan anchor cache: digest mismatch for $tag", script
        )
        self.assertIn(
            "::warning::patch-fan anchor cache: $tag is cached at $actual_size bytes",
            script,
        )
        # Every eviction path re-downloads rather than degrading the anchor.
        self.assertEqual(
            script.count("— evicting and downloading it again"), 3, script
        )
        # One line per anchor, whichever way the bytes arrived.
        self.assertIn(
            'report+="reused $tag from cache (sha256 ok) in'
            ' $((t_fetched - t_checked))s — nothing downloaded"', script
        )
        self.assertIn(
            'report+="downloaded $tag in $((t_fetched - t_checked))s"', script
        )

        # ── bounded ──────────────────────────────────────────────────────────
        self.assertIn('CACHE_KEEP="${PATCH_ANCHOR_CACHE_KEEP:-8}"', script)
        self.assertIn(
            'CACHE_MAX_AGE_DAYS="${PATCH_ANCHOR_CACHE_MAX_AGE_DAYS:-30}"', script
        )
        self.assertIn("bound is the $CACHE_KEEP most recent", script)
        # …and a pruner killed mid-run cannot leave the bound switched off.
        self.assertIn("clearing a stale prune lock", script)

        # ── and nothing deletes it ───────────────────────────────────────────
        # The run-scoped tree still goes; the cache is only ever reported on.
        self.assertIn(
            "rm -rf prev-dbs prev-meta patches release-staging prior-versions.tsv"
            " prefetch",
            cleanup,
        )
        self.assertIn("prefetch_patch_anchors.sh cache-report", cleanup)
        self.assertIn("This step must never remove it — only report it.", cleanup)
        # …and reporting it can never fail this step. It runs `if: always()`
        # under the default `bash -e`, so on a job that died before the
        # pipeline-control checkout an unguarded `bash <missing>` exits 127 and
        # turns the cleanup red on top of the real failure. Every helper call
        # here is either wrapped by abort_background or has its own `||`.
        previous = ""
        for line in cleanup.splitlines():
            stripped = line.strip()
            if stripped.startswith("bash .pipeline-control/"):
                self.assertTrue(
                    "||" in stripped or previous.endswith("\\"),
                    f"unguarded helper call in an `if: always()` step: {stripped}",
                )
            if stripped:
                previous = stripped
        for line in cleanup.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if "rm -rf" in stripped or "rm -f" in stripped:
                self.assertNotIn("cache", stripped, line)
                self.assertNotIn("seforimlibrary", stripped, line)
        # S2's stale-temp sweep is rooted in $RUNNER_TEMP and matches two name
        # shapes; neither can reach the cache. Keep it that way.
        sweep = cleanup.split('find "$RUNNER_TEMP" -maxdepth 1 -mtime +3', 1)[1]
        self.assertEqual(
            re.findall(r"-name '([^']+)'", sweep),
            ["lines-snapshot-*-*.db", "manual-links-inputs-*-*"],
        )
        self.assertNotIn("patch-anchors", sweep)
        self.assertNotIn("$HOME", sweep)
        self.assertRegex(cleanup, r"lives under \$\{XDG_CACHE_HOME:-\$HOME/\.cache\}")

        # Warm or cold is stated before a single byte moves.
        self.assertIn("prefetch_patch_anchors.sh cache-report", prefetch)
        self.assertLess(
            prefetch.index("cache-report"), prefetch.index('start "$ANCHORS" prefetch')
        )
        self.assertIn("cache=${CACHE_DIR:-disabled}", script)

    def test_the_patch_fan_narrates_itself_instead_of_going_dark(self):
        # The two longest gaps in run 34024655297 (1318 s and 1076 s) were both
        # inside this one step, with nothing on stdout.
        fan = self.step("Produce + verify patch fan")
        # The end line per anchor, the heartbeat and the download failures all
        # live in drain_batch/produce_anchor, which the step sources.
        lib = self.fan_lib
        prefetch = self.step("Prefetch patch-fan anchor DBs (background)")

        # The five bare `ANCHOR` TSV rows are prose now, with the same
        # `anchor <tag> (offset N):` prefix the prefetch script uses.
        self.assertNotIn(
            'cat "$ANCHORS"',
            [line.strip() for line in self.workflow.splitlines()],
        )
        self.assertIn(
            'printf "anchor %s (offset %s): will patch v%s -> v%s\\n", $4, $2, $3, this',
            prefetch,
        )
        for status in ("BELOW-ONE", "NO-RELEASE"):
            self.assertIn(status, prefetch)

        # Numbered start line, then one end line per anchor carrying the elapsed
        # time, what it shipped, the verify verdict — and the producer's standing
        # "prev lacks [...] full snapshot" condition folded in, since that is what
        # explains a 454 MB offset-1 patch (report 07).
        self.assertIn(
            'ANCHOR_TOTAL=$(awk -F\'\\t\' \'$1 == "ANCHOR"\' "$ANCHORS" | wc -l', fan
        )
        self.assertIn(
            'ANCHOR_LABEL="anchor $ANCHOR_INDEX/$ANCHOR_TOTAL $TAG (offset $OFFSET,'
            ' v${TARGET_VER} → v${THIS_VER})"',
            fan,
        )
        self.assertIn('echo "$ANCHOR_LABEL: starting;', fan)
        self.assertIn(
            'echo "${BATCH_NAMES[$i]} done in ${elapsed}s: $shipped, verify=$verify'
            '${columns:+, full-snapshot columns: ${columns% }}"',
            lib,
        )
        self.assertIn("prev lacks \\[\\([^]]*\\)\\].*full snapshot", lib)
        self.assertIn(
            'echo "::error::${BATCH_NAMES[$i]} failed after ${elapsed}s with exit'
            ' code $status"',
            lib,
        )

        # A shell heartbeat, because PatchPipelineCli emits nothing to forward.
        self.assertIn('HEARTBEAT_SECONDS="${PATCH_FAN_HEARTBEAT_SECONDS:-300}"', lib)
        self.assertIn(
            'echo "still producing patch for $names (elapsed $((waited / 60))m'
            '$((waited % 60))s)"',
            lib,
        )
        # It must never claim an anchor that has already landed…
        self.assertIn('heartbeat_start "${BATCH_NAMES[@]}"', lib)
        self.assertIn(': > "$HEARTBEAT_INFLIGHT"', lib)
        self.assertIn('[ -n "$names" ] || continue', lib)
        # …and it must never outlive the batch it was started for.
        self.assertIn("heartbeat_stop", lib)
        self.assertLess(lib.index("heartbeat_start"), lib.index("heartbeat_stop"))
        self.assertIn('rm -f "$HEARTBEAT_FLAG" "$HEARTBEAT_INFLIGHT"', lib)

        # A failure names the asset it could not get, not just an exit code.
        self.assertIn(
            "::error::anchor v${TARGET_VER} ($TAG): could not download"
            " ${DB_ASSETS// / or } from that release",
            lib,
        )
        self.assertIn(
            "::error::anchor v${TARGET_VER} ($TAG): the full DB from that"
            " release could not be decompressed",
            lib,
        )

    def test_the_background_aborts_say_what_they_aborted(self):
        # `... abort ... || true` in a step that runs `if: always()` turned four
        # different outcomes — including a real failure to kill the group — into
        # one sentence, and then swallowed the abort's own exit status too.
        cleanup = self.step(
            "Clean run-scoped disk leftovers (workspace persists on self-hosted)"
        )
        prefetch_script = ANCHOR_PREFETCH.read_text(encoding="utf-8")
        uploader = EARLY_UPLOAD.read_text(encoding="utf-8")

        self.assertNotIn("abort prefetch || true", cleanup)
        self.assertNotIn("abort buildstate || true", cleanup)
        self.assertIn('abort_background() {  # <what> <command>...', cleanup)
        self.assertIn(
            '::warning::$what: the abort helper itself exited $rc', cleanup
        )
        self.assertIn(
            'abort_background "patch-fan anchor prefetch" \\\n'
            "            bash .pipeline-control/.github/scripts/"
            "prefetch_patch_anchors.sh abort prefetch",
            cleanup,
        )
        # The fan's own mid-run abort no longer swallows a failure either — it
        # is produce_anchor's, in the library the fan step sources.
        self.assertIn(
            "the prefetch abort helper itself failed — continuing with the serial"
            " download",
            self.fan_lib,
        )

        # Each helper names the branch it took, and never fails the job.
        for verdict in (
            "patch-fan anchor prefetch: nothing to abort — no background prefetch"
            " is recorded in $dest",
            "patch-fan anchor prefetch: pid $pid is no longer this script",
            "patch-fan anchor prefetch aborted: signalled process group $pid",
            "::warning::patch-fan anchor prefetch: could not signal pid $pid",
        ):
            self.assertIn(verdict, prefetch_script)
        self.assertIn(
            "$done_count anchors had finished ($ok ok, $unpatchable unpatchable,"
            " $failed failed)",
            prefetch_script,
        )
        self.assertNotIn('echo "patch-fan anchor prefetch aborted"', prefetch_script)

        self.assertNotIn("aborted (if it was still running)", uploader)
        for verdict in (
            "aborted: signalled pid $aborted_pid before it wrote a verdict",
            "nothing to abort — no upload was ever started for this label",
            "nothing to abort — it had already finished",
            "::warning::early release-asset upload '$label': could not signal pid",
        ):
            self.assertIn(verdict, uploader)
        # kill_group's exit status is what selects those, so it must report one.
        self.assertIn("0 signalled · 1 nothing was ever started ·", uploader)

    def test_final_assets_upload_while_the_job_waits_instead_of_at_publish(self):
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        fan = self.step("Produce + verify patch fan")
        publish = self.step("Create draft, verify every uploaded asset, then publish")
        uploader = EARLY_UPLOAD.read_text(encoding="utf-8")
        machinery = RELEASE_DRAFT.read_text(encoding="utf-8")

        # E2: the relink wait no longer uploads anything. lines_snapshot.db.zst
        # is not published on the DB release at all, so the ONLY early upload
        # left is the buildstate the patch fan starts — with the SHARED create.
        self.assertNotIn("upload_early_release_assets.sh", relink)
        self.assertNotIn(
            'cp build/lines_snapshot.db.zst', self.step("Stage release assets")
        )
        self.assertIn('source "$(dirname "$self")/release_draft.sh"', uploader)
        self.assertIn("ensure_draft || return 1", uploader)
        self.assertEqual(
            machinery.count(
                'gh release create "$RELEASE_TAG" --target "$SOURCE_COMMIT" '
                '--title "$RELEASE_TAG" --draft'
            ),
            2,
            "the draft create + retry pair may exist only in the shared script",
        )
        self.assertNotIn('gh release create "$RELEASE_TAG"', self.workflow)

        # The buildstate is NOT final before the relink wait: Phase-2 allocates
        # this build's fresh stable link ids straight into it, so it is
        # compressed and uploaded after "Apply LINKER links (Phase-2)". It
        # starts at the top of the fan and is reaped inside the same step, so
        # no upload is ever in flight when the publish step runs.
        self.assertNotIn("build/seforim.db.buildstate", relink)
        self.assertIn("DiskBackedLinkIdAllocator", machinery)
        start_buildstate = (
            "upload_early_release_assets.sh \\\n"
            "            start buildstate build/seforim.db.buildstate.zst"
        )
        wait_buildstate = (
            "upload_early_release_assets.sh \\\n            wait buildstate 3600"
        )
        self.assertIn(start_buildstate, fan)
        self.assertIn(wait_buildstate, fan)
        self.assertLess(fan.index(start_buildstate), fan.index(wait_buildstate))
        self.assertLess(
            self.workflow.index("      - name: Apply LINKER links (Phase-2)\n"),
            self.workflow.index("      - name: Compress buildstate for the release (zstd)\n"),
        )
        self.assertLess(
            self.workflow.index("      - name: Compress buildstate for the release (zstd)\n"),
            self.workflow.index("      - name: Produce + verify patch fan\n"),
        )
        # The abort sweep still covers every label the job can start.
        cleanup = self.step(
            "Clean run-scoped disk leftovers (workspace persists on self-hosted)"
        )
        self.assertIn(
            "upload_early_release_assets.sh abort buildstate", cleanup
        )
        self.assertNotIn("abort snapshot", self.workflow)
        self.assertNotIn("for label in snapshot buildstate", self.workflow)

        # A failed early upload is a lost optimisation, never a failed build.
        self.assertIn("never a failed build", uploader)
        self.assertIn("exit 0", uploader)

        # The publish step still uploads the rest and re-verifies everything by
        # name+size+digest against the staged bytes.
        self.assertIn(
            'for asset_path in release-staging/*; do upload_asset "$asset_path"; done',
            publish,
        )
        self.assertIn("verify_remote", publish)
        self.assertIn(
            "remote release descriptors do not exactly match staged bytes", publish
        )

    def test_a_failed_build_deletes_the_draft_release_it_created(self):
        delete = self.step("Delete this build's unpublished draft release on failure")
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        publish = self.step("Create draft, verify every uploaded asset, then publish")

        self.assertIn("if: failure() || cancelled()", delete)
        self.assertIn('[ "${DRAFT_RELEASE_CREATED:-}" = 1 ]', delete)
        self.assertIn('echo "DRAFT_RELEASE_CREATED=1" >> "$GITHUB_ENV"', relink)
        self.assertIn('echo "DRAFT_RELEASE_CREATED=1" >> "$GITHUB_ENV"', publish)
        # Only ever a DRAFT of exactly this build's identity, and never the tag.
        self.assertIn(".draft == true", delete)
        self.assertIn(".target_commitish == $target", delete)
        self.assertIn(
            'gh api -X DELETE "repos/$GITHUB_REPOSITORY/releases/$release_id"', delete
        )
        # This step deletes a DRAFT by id and must never touch a tag. (The
        # snapshot pre-release hook below does use `gh release delete
        # --cleanup-tag`, on a release that is never a DB release.)
        self.assertNotIn("--cleanup-tag", delete)
        self.assertNotIn("gh release delete", delete)
        self.assertLess(
            self.workflow.index(
                "      - name: Create draft, verify every uploaded asset, then publish\n"
            ),
            self.workflow.index(
                "      - name: Delete this build's unpublished draft release on failure\n"
            ),
        )
        # The relink orphan cleanup is untouched and still always runs.
        self.assertIn(
            "      - name: Cancel any in-flight relink for this build "
            "(no orphaned linker run)\n        if: always()\n",
            self.workflow,
        )

    def test_recovery_sets_both_cleanup_titles_and_cleanup_defaults_them(self):
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        cleanup = self.step("Cancel any in-flight relink for this build (no orphaned linker run)")
        self.assertIn('echo "KAGGLE_TITLE=kaggle-relink request=$RELINK_REQUEST_ID', relink)
        self.assertIn('echo "RELINK_DISPATCH_STARTED=1" >> "$GITHUB_ENV"', relink)
        self.assertIn('if [ "${RELINK_DISPATCH_STARTED:-}" != 1 ]; then', cleanup)
        self.assertIn(
            'actions/runs/$EXPECTED_PARENT_RUN_ID/attempts/$EXPECTED_PARENT_RUN_ATTEMPT',
            relink,
        )
        self.assertIn(': "${RELINK_TITLE:=}"', cleanup)
        self.assertIn(': "${KAGGLE_TITLE:=}"', cleanup)

    def test_large_snapshot_uses_content_addressed_release_not_actions_artifact(self):
        publish = self.step("Publish immutable snapshot release for the relink run")
        self.assertIn('tag="lines-snapshot-sha256-$SNAPSHOT_ZST_SHA256"', publish)
        self.assertIn('gh release create "$tag"', publish)
        self.assertIn('gh release upload "$tag" "$snapshot"', publish)
        self.assertIn('digest=="sha256:"+sys.argv[3]', publish)
        self.assertNotIn("actions/upload-artifact", publish)
        self.assertNotIn("Upload snapshot artifact for the relink run", self.workflow)
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        self.assertIn(
            'SNAPSHOT_RELEASE_TAG="lines-snapshot-sha256-$EXPECTED_LINKER_SNAPSHOT_ZST_SHA256"',
            relink,
        )
        self.assertIn("recovery parent snapshot release is missing or not byte-exact", relink)
        self.assertNotIn(
            "recovery parent must retain exactly one live source snapshot artifact",
            relink,
        )

    def test_recovery_inputs_are_preflighted_before_any_expensive_work(self):
        # Run 34021998271 rebuilt the database (35 min), dumped the snapshot and
        # uploaded 987 MiB before Phase-2 rejected the Linker's meta.json — a
        # file that was downloadable at second 0. The preflight runs the SAME
        # verifier on the SAME file in the reconciliation job, so the cheap gate
        # cannot drift from the expensive one and nothing is leased to lose.
        marker = "      - name: Preflight the recovery relink inputs\n"
        reconcile = self.workflow.split("  build-and-release:\n", 1)[0]
        self.assertEqual(reconcile.count(marker), 1, "the preflight must live in the first job")
        preflight = reconcile.split(marker, 1)[1]

        # Both clauses: a dispatch the reconciler answers from an existing
        # identical release never runs the build job, so its recovery inputs
        # decide nothing and must not be able to fail the dispatch.
        self.assertIn(
            "if: inputs.relink_recovery_run_id != '' "
            "&& steps.lookup.outputs.reuse != 'true'",
            preflight,
        )
        self.assertIn("GH_TOKEN: ${{ secrets.PIPELINE_TOKEN }}", preflight)
        self.assertIn(
            "verify_relink_recovery_snapshot.py --preflight --payload-meta", preflight
        )
        self.assertIn(
            'gh release download "linker-output-$REQUEST_ID-$RUN_ATTEMPT" -R Otzaria/LinkerToOtzaria',
            preflight,
        )
        self.assertIn(
            'gh release download "$LINKER_RELEASE_TAG" -R Otzaria/LinkerToOtzaria -p meta.json',
            preflight,
        )
        self.assertIn("releases/tags/lines-snapshot-sha256-$SNAPSHOT_ZST_SHA256", preflight)
        self.assertIn('"linker_commit", head', preflight)
        # One positive line, and every refusal names file, field, value and
        # expectation as a GitHub error annotation.
        self.assertEqual(preflight.count('\n          echo "recovery preflight:'), 1)
        self.assertIn(
            "expected 'relink-recovery request=<64 hex> parent=<run>:<attempt>'", preflight
        )

        for later in (
            "      - name: Mount RAM-backed build dir (tmpfs)\n",
            "      - name: Generate Seforim Database\n",
            "      - name: Dump lines snapshot for the linker\n",
            "      - name: Apply LINKER links (Phase-2)\n",
        ):
            self.assertLess(self.workflow.index(marker), self.workflow.index(later))
        # Additive only: Phase-2 still runs the full comparison itself, and the
        # cheap mode must never leak into it — --preflight there would reduce the
        # 5 GiB semantic comparison to a 759-byte schema read and still exit 0.
        phase2 = self.step("Apply LINKER links (Phase-2)")
        self.assertIn("verify_relink_recovery_snapshot.py", phase2)
        self.assertNotIn("--preflight", phase2)

    def test_recovery_verifies_semantic_snapshot_before_phase2(self):
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        apply_links = self.step("Apply LINKER links (Phase-2)")

        self.assertIn("EXPECTED_LINKER_SNAPSHOT_ZST_SHA256", relink)
        self.assertIn("relink-recovery-manifest.json", relink)
        self.assertIn(
            'EXP_SNAPSHOT_SHA="$EXPECTED_LINKER_SNAPSHOT_ZST_SHA256"',
            apply_links,
        )
        self.assertIn("verify_relink_recovery_snapshot.py", apply_links)
        self.assertIn("--original \"$ORIGINAL_SNAPSHOT_DB\"", apply_links)
        self.assertIn("--rebuilt \"$REBUILT_SNAPSHOT_DB\"", apply_links)
        self.assertLess(
            apply_links.index("verify_relink_recovery_snapshot.py"),
            apply_links.index('"$GRADLE" :sefariasqlite:generateLinkerLinks'),
        )

    def test_weekly_workflow_has_no_actions_artifact_handoffs(self):
        self.assertNotIn("actions/upload-artifact", self.workflow)
        self.assertNotIn("actions/download-artifact", self.workflow)
        self.assertNotIn("gh run download", self.workflow)
        self.assertIn("pipeline-result-run-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}", self.workflow)
        self.assertIn("linker-output-${EXPECTED_RELINK_REQUEST_ID}-${RUN_ATTEMPT}", self.workflow)

    def test_handoff_prereleases_do_not_pollute_database_manifest(self):
        refresh = self.workflow.split("  refresh-release-manifest:\n", 1)[1]
        self.assertIn("(.prerelease|not)", refresh)
        standalone = MANIFEST_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("(.prerelease|not)", standalone)
        self.assertIn("github.event.release.prerelease == false", standalone)

    def test_split_kaggle_child_releases_and_reacquires_host_lease(self):
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        release = (
            'python3 .pipeline-control/.github/scripts/host_lease.py release '
            '--state "$HOST_LEASE_STATE"'
        )
        reacquire = (
            "python3 .pipeline-control/.github/scripts/host_lease.py start \\\n"
            "            --lock /run/lock/otzaria/host-heavy.lock"
        )
        dispatch_case = 'case "$SERIAL_LINKER_TARGET" in\n            local)'
        # The wait itself is wait_for_relink_run.sh now; the lease is reacquired
        # after that call returns, which is exactly when the child is terminal.
        wait_call = "bash .pipeline-control/.github/scripts/wait_for_relink_run.sh"

        self.assertEqual(relink.count(release), 1)
        self.assertEqual(relink.count(reacquire), 1)
        self.assertLess(relink.index(release), relink.index(dispatch_case))
        self.assertLess(relink.index(wait_call), relink.index(reacquire))
        self.assertIn("completed:success)", self.relink_wait)
        self.assertNotIn(
            'if [ "$SERIAL_LINKER_TARGET" = server ]; then',
            relink,
            "the split Kaggle child also needs the Oracle host lease",
        )

    def test_parent_timeout_covers_db_build_and_complete_split_child(self):
        build = self.workflow.split("  build-and-release:\n", 1)[1]
        self.assertIn(
            "    timeout-minutes: 4320\n",
            build,
            "the self-hosted parent must outlive DB generation plus the legal split child chain",
        )
        self.assertIn("90m GPU NER + 480m CPU resolution", self.workflow)
        # The default target is local, one child job with its own 2880-minute
        # ceiling. At a 1440 PARENT ceiling that child could not be waited out —
        # this job generates the DB first, so the child always outlived it and the
        # `always()` cleanup then killed a run that was still within contract. The
        # prose has to carry that arithmetic instead of a reassurance.
        self.assertIn("target=local is ONE", self.workflow)
        self.assertIn(
            "At a 1440 parent ceiling a legal child could not be waited out",
            self.workflow,
        )
        self.assertIn("4320 − 2970 (largest wait cap) − ~55 (pre-dispatch)", self.workflow)

        # Read the four numbers back from the files that carry them, so the budget
        # cannot silently stop adding up: parent ceiling, the wait cap the step
        # passes, the path budget the wait script mirrors, and the lease TTLs.
        parent = int(re.search(r"\n    timeout-minutes: (\d+)\n", build).group(1))
        cap = int(re.search(r'\n          RELINK_WAIT_CAP_MIN: "(\d+)"\n', build).group(1))
        local_budget = self.relink_path_budgets()["local"]
        self.assertEqual(
            local_budget, 2880 + 30, "must keep tracking relink.yml's local path"
        )
        self.assertEqual(
            cap,
            local_budget + 60,
            "the wait cap is the whole local CHAIN plus an hour of grace, not one job",
        )
        # ~55 min of DB generation + snapshot publish happen BEFORE the dispatch,
        # and Phase-2 + the 5-anchor fan + ~5.1 GiB of uploads measure ~130-160 min
        # after the wait ends. Both must fit outside the cap.
        self.assertGreaterEqual(parent - cap, 55 + 160)
        self.assertLessEqual(parent, 7200, "GitHub terminates a self-hosted job at five days")

        ttls = set(re.findall(r"--ttl (\d+)\n", self.workflow))
        self.assertEqual(ttls, {"262800"})
        self.assertEqual(self.workflow.count("--ttl 262800"), 2)
        self.assertEqual(
            int(ttls.pop()) - parent * 60,
            3600,
            "both lease lives must outlive the parent ceiling by the same hour of margin",
        )

    def test_relink_wait_is_observable_and_names_a_cause(self):
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        # The loop is wait_for_relink_run.sh, run by that step: the same text,
        # only at its own indentation. The step still computes what it waits on
        # and hands the three shell locals over as environment.
        wait = self.relink_wait
        self.assertIn(
            'RELINK_RUN_ID="$RUN" RELINK_RUN_URL="$RUN_URL" \\\n'
            '            RELINK_REQUEST_ID="$RELINK_REQUEST_ID" \\\n'
            "            bash .pipeline-control/.github/scripts/wait_for_relink_run.sh",
            relink,
        )
        self.assertIn('RUN="$RELINK_RUN_ID"', wait)
        self.assertIn('RUN_URL="$RELINK_RUN_URL"', wait)

        # A heartbeat, not a per-poll line: 8h of silence reads like a hung step.
        self.assertIn('next_beat=$(( (waited / 600 + 1) * 600 ))', wait)
        self.assertIn('RUN_URL="https://github.com/Otzaria/LinkerToOtzaria/actions/runs/$RUN"', relink)
        self.assertIn('elapsed · ${state%%:*} · $(child_where) · $RUN_URL', wait)

        # Every terminal branch names the run, the cause and the way back.
        # The cancel-vs-timeout discriminator compares the longest JOB's wall
        # clock, so the number it names is the longest job's ceiling on this
        # path — never the whole path's budget.
        self.assertIn("this matches its ${child_job_max_min}-minute timeout-minutes", wait)
        self.assertNotIn("child_timeout_min", wait)
        self.assertIn("was cancelled externally", wait)
        # Three terminal conclusions plus the explicit wait cap below.
        self.assertEqual(wait.count("recovery_hint\n"), 4)
        self.assertIn(
            "recover: dispatch relink-recovery request=$RELINK_REQUEST_ID "
            "parent=${GITHUB_RUN_ID}:${GITHUB_RUN_ATTEMPT}",
            wait,
        )
        for branch in ("completed:cancelled)", "completed:failure)", "completed:*)"):
            self.assertIn(branch, wait)
        # The ::error:: line stays last in each terminal branch, and each one
        # still ends the step: `exit 1` here is a non-zero exit of the script,
        # which the step's `set -e` turns into the same failure it always was.
        for conclusion in ("cancelled", "failure", "${state#completed:}"):
            marker = f'echo "::error::relink run $RUN concluded {conclusion}"; exit 1 ;;'
            self.assertIn(marker, wait)

        # A failed poll is surfaced, capped, and must not disarm the pre-start cap.
        self.assertIn('fails=$((fails + 1))', wait)
        self.assertIn('if [ "$fails" -ge 30 ]; then', wait)
        self.assertIn('head -n 1 "$RUNNER_TEMP/wait-err.txt"', wait)
        self.assertNotIn("*) queued_for=0; sleep 60 ;;", wait)
        transient = wait.split("    transient)\n", 1)[1].split("\n    queued:", 1)[0]
        self.assertNotIn("queued_for=0", transient)

    def test_the_relink_wait_ends_at_its_own_cap_not_at_the_job_ceiling(self):
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        wait = self.relink_wait
        # The cap is configuration handed over by the step, not a literal in the
        # script: the parent's budget stays next to the parent's timeout-minutes.
        self.assertIn('RELINK_WAIT_CAP_MIN: "2970"', relink)
        self.assertIn('"${RELINK_WAIT_CAP_MIN:-}" =~ ^[1-9][0-9]*$', wait)
        self.assertNotIn("2970", wait)
        # …but the script still derives its own, per PATH, and takes the tighter
        # of the two, so kaggle (600) and server (510) are not held to the local
        # budget and a missing/garbage value cannot restore an unbounded wait.
        self.assertIn("wait_cap_min=$((child_path_budget_min + 60))", wait)
        self.assertIn('[ "$RELINK_WAIT_CAP_MIN" -lt "$wait_cap_min" ]', wait)
        self.assertLess(
            wait.index("wait_cap_min=$((child_path_budget_min + 60))"),
            wait.index('if [[ "${RELINK_WAIT_CAP_MIN:-}"'),
        )
        # The step's value must not clip the path it is written for: at 1500 a
        # legal 1470-minute local child had 30 minutes of margin, which is the
        # same defect the 150-minute kaggle cap was, one order of magnitude down.
        step_cap = int(re.search(r'RELINK_WAIT_CAP_MIN: "(\d+)"', relink).group(1))
        for target, budget in self.relink_path_budgets().items():
            self.assertGreaterEqual(
                step_cap,
                budget + 60,
                f"the step's cap clips the legal {target} child chain",
            )

        # The check runs inside the poll loop, after the heartbeat, and its branch
        # ends the wait with the same recovery recipe as the terminal branches.
        self.assertIn('if [ "$waited" -ge $((wait_cap_min * 60)) ]; then', wait)
        self.assertLess(
            wait.index("next_beat=$(( (waited / 600 + 1) * 600 ))"),
            wait.index('if [ "$waited" -ge $((wait_cap_min * 60)) ]; then'),
        )
        capped = wait.split('if [ "$waited" -ge $((wait_cap_min * 60)) ]; then', 1)[1]
        capped = capped.split("\n  fi\n", 1)[0]
        self.assertIn("\n    recovery_hint\n", capped)
        self.assertIn(
            '::error::relink run $RUN exceeded the ${wait_cap_min}-minute wait cap '
            "(the ${SERIAL_LINKER_TARGET} path's jobs may legally take "
            "${child_path_budget_min})",
            capped,
        )
        # The job's `always()` cleanup cancels the child on this failure like any
        # other, so the message says so instead of pretending it stays alive.
        self.assertIn("this job's cleanup cancels it, see $RUN_URL", capped)
        self.assertIn("exit 1", capped)
        # Cadence, heartbeat and API-error handling are untouched by the cap.
        self.assertEqual(wait.count("sleep 60"), 1)
        self.assertIn("  sleep 60\ndone\n", wait)

    def test_a_child_past_its_ceiling_fails_the_wait_with_the_recovery_recipe(self):
        """Drive the real wait loop: a child that never reaches a terminal state
        must end the wait with a named cause, not be polled in silence until the
        parent's own timeout kills the job with nothing in the log."""
        # target=local with the step's own cap: the error names both numbers.
        done = self._drive_relink_wait(cap="3")
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn(
            "relink https://github.com/Otzaria/LinkerToOtzaria/actions/runs/4242 "
            "reached no terminal state in 00:03",
            done.stdout,
        )
        self.assertIn("(last poll: in_progress: · resolve step)", done.stdout)
        self.assertIn(
            "recover: dispatch relink-recovery request=deadbeef parent=99:1 "
            "on Otzaria/LinkerToOtzaria, then re-run this build with "
            "relink_recovery_run_id=<that run id>",
            done.stdout,
        )
        self.assertIn(
            "::error::relink run 4242 exceeded the 3-minute wait cap "
            "(the local path's jobs may legally take 2910)",
            done.stdout,
        )
        self.assertIn("out of contract and no longer waited on", done.stdout)
        # It failed at the cap, not at a terminal conclusion it never saw.
        self.assertNotIn("concluded", done.stdout)

    def test_each_target_waits_out_the_whole_chain_of_jobs_on_its_path(self):
        """The cap is the SUM of the jobs on the path, not the longest one.

        The kaggle child is relink(90) + resolve(480) + publish(30) = 600 legal
        minutes; the cap derived from the single relink job was 150, so the
        parent failed at 02:30 and its `always()` cleanup then cancelled a
        healthy child 7.5 hours from the end of its own contract.
        """
        budgets = self.relink_path_budgets()
        self.assertEqual(budgets, {"kaggle": 600, "server": 510, "local": 2910})
        for target, cap in (("kaggle", 660), ("server", 570), ("local", 2970)):
            for passed in (None, "2970"):
                with self.subTest(target=target, cap=passed):
                    # Whether the step passes nothing or its local-target 2970,
                    # the path's own budget is what ends the wait.
                    run = self._drive_relink_wait(cap=passed, target=target)
                    self.assertEqual(run.returncode, 1, run.stdout + run.stderr)
                    self.assertIn(
                        f"::error::relink run 4242 exceeded the {cap}-minute wait cap "
                        f"(the {target} path's jobs may legally take {budgets[target]})",
                        run.stdout,
                    )
                    hours, minutes = divmod(cap, 60)
                    self.assertIn(
                        f"relink {hours:02d}:{minutes:02d} elapsed · in_progress",
                        run.stdout,
                    )

    def test_a_healthy_kaggle_child_is_still_waited_on_at_five_hours(self):
        """minute 300 of a kaggle child is `relink` done and `resolve` running —
        inside contract by 300 minutes. The 150-minute cap failed the parent
        here, and the cleanup cancelled the child."""
        done = self._drive_relink_wait(target="kaggle", terminal_after=301)
        self.assertEqual(done.returncode, 0, done.stdout[-2000:] + done.stderr[-2000:])
        self.assertIn("succeeded after 05:00", done.stdout)
        self.assertNotIn("wait cap", done.stdout)
        self.assertNotIn("recover: dispatch", done.stdout)
        # …and it kept saying so all the way through.
        self.assertIn("relink 02:30 elapsed · in_progress", done.stdout)
        self.assertIn("relink 04:50 elapsed · in_progress", done.stdout)

    def test_an_external_cancel_is_not_reported_as_the_childs_own_timeout(self):
        """`cancelled` is reported for an operator stop and for a job hitting its
        own timeout-minutes alike, and only the longest job's wall clock tells
        them apart — so it must be compared against the longest JOB, not the
        path. A kaggle `resolve` may legally run 200 minutes; against the old
        single-job number (90) every such cancel was misreported as a timeout."""
        maxima = self.relink_job_maxima()
        self.assertEqual(maxima, {"kaggle": 480, "server": 480, "local": 2880})
        done = self._drive_relink_wait(
            target="kaggle",
            terminal_after=1,
            terminal_state="completed:cancelled",
            job_seconds=200 * 60,
        )
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn(
            "was cancelled externally after 03:20 — well inside its 480-minute"
            " timeout-minutes",
            done.stdout,
        )
        self.assertNotIn("this matches its", done.stdout)
        self.assertIn("::error::relink run 4242 concluded cancelled", done.stdout)
        # A job that really did burn its own ceiling is still named as such.
        timed_out = self._drive_relink_wait(
            target="kaggle",
            terminal_after=1,
            terminal_state="completed:cancelled",
            job_seconds=480 * 60,
        )
        self.assertIn(
            "was cancelled after 08:00 — this matches its 480-minute timeout-minutes",
            timed_out.stdout,
        )

    # This repository owns the wait-cap calculation, so its input is a local,
    # versioned release contract. CI always reads it; it never conditionally
    # skips a cross-check because a developer happens not to have LinkerToOtzaria
    # checked out beside this repository.
    RELINK_YML_JOB_TIMEOUTS = json.loads(RELINK_TIMEOUT_CONTRACT.read_text(encoding="utf-8"))["timeouts"]

    def test_the_wait_scripts_table_mirrors_relink_ymls_own_timeouts(self):
        contract = json.loads(RELINK_TIMEOUT_CONTRACT.read_text(encoding="utf-8"))
        self.assertEqual(contract["contractVersion"], 1)
        self.assertEqual(contract["workflow"], "relink.yml")
        pinned = self.RELINK_YML_JOB_TIMEOUTS
        relink_job = pinned["relink"]
        self.assertEqual(
            self.relink_path_budgets(),
            {
                "kaggle": relink_job["kaggle"] + pinned["resolve"] + pinned["publish"],
                "server": relink_job["server"] + pinned["publish"],
                "local": relink_job["local"] + pinned["publish"],
            },
        )
        self.assertEqual(
            self.relink_job_maxima(),
            {
                "kaggle": max(relink_job["kaggle"], pinned["resolve"]),
                "server": relink_job["server"],
                "local": relink_job["local"],
            },
        )
        # The script names the versioned fixture that CI verified.
        self.assertIn("linker_relink_timeouts_v1.json", self.relink_wait)

        dispatch = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")
        digest_command = (
            "WAIT_CONTRACT_SHA256=$(sha256sum "
            ".pipeline-control/.github/contracts/linker_relink_timeouts_v1.json | cut -d' ' -f1)"
        )
        self.assertIn(digest_command, dispatch)
        self.assertIn('[[ "$WAIT_CONTRACT_SHA256" =~ ^[0-9a-f]{64}$ ]]', dispatch)
        self.assertEqual(
            3,
            dispatch.count('-f wait_contract_sha256="$WAIT_CONTRACT_SHA256"'),
            "local, split Kaggle, and server dispatches must all bind the same wait contract",
        )
        pipeline_checkout = self.step("Checkout immutable pipeline control scripts")
        self.assertIn(
            ".github/contracts",
            pipeline_checkout,
            "the immutable sparse checkout must actually contain the hashed contract",
        )

    def test_a_child_that_finishes_inside_the_cap_is_not_truncated(self):
        done = self._drive_relink_wait(cap="10", terminal_after=4)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn(
            "relink https://github.com/Otzaria/LinkerToOtzaria/actions/runs/4242 "
            "succeeded after 00:03",
            done.stdout,
        )
        self.assertNotIn("wait cap", done.stdout)
        self.assertNotIn("recover: dispatch", done.stdout)

    def _drive_relink_wait(
        self,
        cap=None,
        target="local",
        terminal_after=0,
        terminal_state="completed:success",
        job_seconds=0,
    ):
        """Run wait_for_relink_run.sh for real against a stub `gh` and a stub
        `sleep`. Both are exported shell functions, so the script needs neither
        an executable on PATH nor a real wait: assigning $SECONDS rebases the
        elapsed clock the loop reads, which is the only clock it has."""
        bash = shutil.which("bash")
        if bash is None:  # pragma: no cover - only on a host without bash
            self.skipTest("bash unavailable")
        driver = textwrap.dedent(
            """
            set -uo pipefail
            export RUNNER_TEMP="$PWD/runner-temp"
            mkdir -p "$RUNNER_TEMP"
            export GH_TOKEN=stub GITHUB_RUN_ID=99 GITHUB_RUN_ATTEMPT=1
            export RELINK_RUN_ID=4242
            export RELINK_RUN_URL=https://github.com/Otzaria/LinkerToOtzaria/actions/runs/4242
            export RELINK_REQUEST_ID=deadbeef
            export POLLS="$PWD/polls"
            : > "$POLLS"
            gh() {
              case "$*" in
                # The cancel branch asks the same endpoint for the longest job's
                # wall clock; the jq tells the two questions apart.
                *fromdateiso8601*) echo "$JOB_SECONDS"; return 0 ;;
                *"/jobs?per_page=100"*) echo "resolve step"; return 0 ;;
              esac
              if [ "$TERMINAL_AFTER" -eq 0 ]; then echo "in_progress:"; return 0; fi
              echo . >> "$POLLS"
              if [ "$(wc -l < "$POLLS" | tr -d ' ')" -ge "$TERMINAL_AFTER" ]; then
                echo "$TERMINAL_STATE"
              else
                echo "in_progress:"
              fi
            }
            sleep() {
              SECONDS=$((SECONDS + $1))
              # A wait that never ends is the exact regression these tests exist
              # to catch, so the harness must fail on it instead of inheriting
              # it: without this bound, deleting the cap branch hangs the suite
              # (unittest runs this case before the static one) until the job's
              # own timeout kills it with nothing in the log. No legitimate cap
              # can reach the parent job's own 4320-minute ceiling.
              [ "$SECONDS" -le 259200 ] || { echo "HARNESS: no cap ended the wait within 4320 simulated minutes"; exit 9; }
            }
            export -f gh sleep
            bash ./wait_for_relink_run.sh
            """
        )
        with tempfile.TemporaryDirectory() as tmp:
            shutil.copy(RELINK_WAIT, Path(tmp) / "wait_for_relink_run.sh")
            env = dict(
                os.environ,
                SERIAL_LINKER_TARGET=target,
                TERMINAL_AFTER=str(terminal_after),
                TERMINAL_STATE=terminal_state,
                JOB_SECONDS=str(job_seconds),
            )
            env.pop("RELINK_WAIT_CAP_MIN", None)
            if cap is not None:
                env["RELINK_WAIT_CAP_MIN"] = cap
            return subprocess.run(
                [bash, "-c", driver],
                cwd=tmp,
                env=env,
                capture_output=True,
                encoding="utf-8",
                errors="replace",
            )

    def test_weekly_database_releases_default_to_final(self):
        prerelease_input = self.workflow.split("      prerelease:\n", 1)[1].split(
            "      source_commit:\n", 1
        )[0]
        self.assertIn("default: false", prerelease_input)
        self.assertIn("Weekly database builds are final releases", prerelease_input)

    def test_reuse_skips_invalid_legacy_provenance_but_not_the_requested_source(self):
        lookup = self.step("Find and verify exact provenance")
        validation = (
            'if ! python3 .github/scripts/validate_build_provenance.py --quiet "$file"; then'
        )
        requested_source_guard = 'if [ "$target" = "$SOURCE_COMMIT" ]; then'
        legacy_skip = '::warning::Skipping legacy release $tag with invalid build provenance'

        self.assertIn(validation, lookup)
        self.assertIn(requested_source_guard, lookup)
        self.assertIn(legacy_skip, lookup)
        self.assertLess(lookup.index(validation), lookup.index(requested_source_guard))
        self.assertLess(lookup.index(requested_source_guard), lookup.index(legacy_skip))

    def test_zstd_steps_saturate_the_runner_without_changing_published_bytes(self):
        helper = ZSTD_WORKERS_HELPER.read_text(encoding="utf-8")
        source = ". .pipeline-control/.github/scripts/zstd_workers.sh"
        snapshot = self.step("Dump lines snapshot for the linker")
        compress = self.step("Compress Seforim Database (zstd)")
        buildstate = self.step("Compress buildstate for the release (zstd)")

        # `-T0` resolves to PHYSICAL cores (8 of the runner's 16 vCPUs), so it
        # must not survive anywhere in the workflow.
        self.assertNotIn("zstd -T0", self.workflow)
        zstd_calls = [
            line.strip()
            for line in self.workflow.splitlines()
            if "zstd " in line and not line.lstrip().startswith("#")
        ]
        self.assertTrue(zstd_calls, "the workflow must still compress with zstd")
        for call in zstd_calls:
            self.assertNotIn("-T0", call)
            # Levels above 19 need --ultra; without it `-22` was silently
            # clamped, so neither may appear in an actual invocation.
            self.assertNotIn("--ultra", call)
            self.assertNotIn("-22", call)

        # The helper is the single worker-count policy, and it must degrade to
        # zstd's own detection (0) rather than emitting an empty `-T`.
        self.assertIn("zstd_workers() {", helper)
        self.assertIn('n="$(nproc 2>/dev/null || echo 0)"', helper)
        self.assertIn("''|*[!0-9]*) n=0 ;;", helper)
        self.assertIn('if [ "$n" -gt 32 ]; then', helper)

        # Both steps source it from the pipeline-control checkout: the payload
        # checkout is pinned to source_commit and may predate the helper.
        self.assertIn(source, snapshot)
        self.assertIn(source, compress)
        self.assertIn(source, buildstate)

        # Transient snapshot: level drops to 12 (bytes change once, and only
        # this build's own sha256 — recorded here — gates the consumer).
        self.assertIn(
            'zstd -T"$(zstd_workers)" -12 -f -o build/lines_snapshot.db.zst "$RAW_SNAPSHOT"',
            snapshot,
        )
        self.assertIn(
            'SNAPSHOT_ZST_SHA256=$(sha256sum build/lines_snapshot.db.zst | cut -d\' \' -f1)',
            snapshot,
        )

        # Published DB: explicit -19 is byte-identical to the clamped -22.
        self.assertIn(
            'zstd -T"$(zstd_workers)" -19 -f -o "build/$FULL_DB_ASSET" build/seforim.db',
            compress,
        )
        # The name is the schema's: seforim.db.zst only up to schema 5.
        self.assertIn(
            'FULL_DB_ASSET=$(full_db_asset_name "$(jq -er .db_schema_version build/db_schema.json)")',
            compress,
        )

        # E1 buildstate: -10 is the measured knee (1.97x in 8.0 s on a real
        # 615.9 MB buildstate; -19 buys 6.4% for 3.5x the CPU). The worker count
        # stays byte-neutral, so the asset is deterministic for a given input.
        self.assertIn(
            'zstd -T"$(zstd_workers)" -10 -f \\\n'
            '            -o build/seforim.db.buildstate.zst build/seforim.db.buildstate',
            buildstate,
        )

        # The in-JVM patch compressor keeps its own level and is untouched.
        self.assertIn('ZSTD_LEVEL: "19"', self.workflow)

    def test_buildstate_ships_compressed_end_to_end(self):
        # E1: 990 MB of uncompressed SQLite went up every run and came back down
        # every next run ("Seed allocator from offset-1 release", 128 s in run
        # 33865604251). Publish it as .zst and expand it on the way in.
        seed = self.step("Seed allocator from offset-1 release")
        stage = self.step("Stage release assets")

        self.assertIn("--pattern 'seforim.db.buildstate.zst' \\", seed)
        self.assertIn('unzstd -f "$SEED_ZST" -o build/seforim.db.buildstate', seed)
        # …and generateSeforimDb still finds the uncompressed file where it
        # always was, never a half-written one from an earlier attempt.
        self.assertIn("rm -f build/seforim.db.buildstate", seed)
        self.assertIn("ls -lh build/seforim.db.buildstate", seed)

        # Transitional fallback: the offset-1 release of the first run after this
        # change (v26) still carries only the uncompressed asset. Documented as
        # removable once v27 is published — the comment must say so, so the
        # cleanup is not forgotten.
        self.assertIn("--pattern 'seforim.db.buildstate' \\", seed)
        self.assertIn("fallback branch once v27 is published", seed)
        self.assertIn("pre-v27 uncompressed asset", seed)

        # The release carries the compressed asset, and only that one.
        self.assertIn('cp build/seforim.db.buildstate.zst "$STAGE/"', stage)
        self.assertNotIn('cp build/seforim.db.buildstate "$STAGE/"', stage)
        self.assertIn(
            'required = {full_db, "seforim.db.buildstate.zst"}',
            VALIDATOR.read_text(encoding="utf-8"),
        )

    def test_the_duplicate_lines_snapshot_is_not_published_on_the_db_release(self):
        # E2: the identical bytes are already on the immutable content-addressed
        # pre-release, so a second 830 MB - 1 GiB copy cost ~7 min of the
        # ~2.1 MB/s uplink every week for nothing. The provenance names that
        # pre-release instead, so every consumer can still resolve it.
        stage = self.step("Stage release assets")
        publish_snapshot = self.step("Publish immutable snapshot release for the relink run")
        apply_links = self.step("Apply LINKER links (Phase-2)")
        validator = VALIDATOR.read_text(encoding="utf-8")

        # The staging step copies the buildstate and the DB, never the snapshot.
        self.assertNotIn('cp build/lines_snapshot.db.zst', stage)
        self.assertIn('"schema_version": 5,', stage)
        self.assertIn('"snapshot_zst_sha256": snapshot_sha256,', stage)
        self.assertIn(
            '"snapshot_release_tag": "lines-snapshot-sha256-" + snapshot_sha256,', stage
        )
        # The digest named here is the one the LINK SET was produced from. Off
        # the recovery path the relink step defaults it to this build's own
        # SNAPSHOT_ZST_SHA256; in recovery mode this build's rebuild is never
        # published, so naming it would publish a tag that resolves to nothing.
        self.assertIn(
            'snapshot_sha256 = os.environ["EXPECTED_LINKER_SNAPSHOT_ZST_SHA256"]', stage
        )
        self.assertIn(
            ': "${EXPECTED_LINKER_SNAPSHOT_ZST_SHA256:=$SNAPSHOT_ZST_SHA256}"',
            self.step("Run LinkerToOtzaria relink on this snapshot (and wait)"),
        )

        # The tag it names is exactly the one the pre-release step publishes.
        self.assertIn('tag="lines-snapshot-sha256-$SNAPSHOT_ZST_SHA256"', publish_snapshot)
        # And the validator re-derives it, so a consumer may trust either field.
        self.assertIn('V5_KEYS = V4_KEYS | {"snapshot_zst_sha256", "snapshot_release_tag"}', validator)
        self.assertIn(
            'value["snapshot_release_tag"] != "lines-snapshot-sha256-" + snapshot_sha256',
            validator,
        )
        self.assertIn(
            'forbidden = {"seforim.db.buildstate", "lines_snapshot.db.zst"}', validator
        )

        # The relink-recovery path already read the pre-release, never the DB
        # release, and is untouched by the removal.
        self.assertIn(
            'gh release download \\\n'
            '              "lines-snapshot-sha256-$EXPECTED_LINKER_SNAPSHOT_ZST_SHA256" \\\n'
            '              -R "$GITHUB_REPOSITORY" -p lines_snapshot.db.zst \\',
            apply_links,
        )

    def test_recovery_dumps_the_snapshot_but_publishes_no_release_for_it(self):
        # Recovery never dispatches a relink, so nothing would ever download a
        # freshly published snapshot: runs 34021998271 and 34024655297 each spent
        # 464 s uploading a 987 MiB pre-release that was orphaned on the spot.
        # The DUMP must stay — the semantic compare consumes its bytes and its
        # digest is what decides whether that compare runs at all.
        dump = self.step("Dump lines snapshot for the linker")
        publish = self.step("Publish immutable snapshot release for the relink run")
        apply_links = self.step("Apply LINKER links (Phase-2)")

        self.assertIn("if: inputs.relink_recovery_run_id == ''", publish)
        self.assertNotIn("if:", dump)
        self.assertIn(
            'echo "SNAPSHOT_ZST_SHA256=$(sha256sum build/lines_snapshot.db.zst', dump
        )
        self.assertIn('unzstd -f build/lines_snapshot.db.zst', apply_links)
        self.assertIn(
            'if [ "$EXPECTED_LINKER_SNAPSHOT_ZST_SHA256" != "$SNAPSHOT_ZST_SHA256" ]; then',
            apply_links,
        )
        self.assertLess(
            self.workflow.index("      - name: Dump lines snapshot for the linker\n"),
            self.workflow.index(
                "      - name: Publish immutable snapshot release for the relink run\n"
            ),
        )

    def test_a_failed_build_deletes_the_snapshot_prerelease_it_never_used(self):
        cleanup = self.step(
            "Delete this attempt's unconsumed snapshot pre-release on failure"
        )
        publish = self.step("Publish immutable snapshot release for the relink run")
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")

        self.assertIn("if: failure() || cancelled()", cleanup)
        # Only a release THIS attempt created (the tag is content-addressed, so a
        # byte-identical rebuild resolves to someone else's release)...
        self.assertIn('created_here=0', publish)
        self.assertIn('created_here=1', publish)
        self.assertIn(
            'echo "SNAPSHOT_RELEASE_CREATED_HERE=$created_here" >> "$GITHUB_ENV"',
            publish,
        )
        self.assertIn('[ "${SNAPSHOT_RELEASE_CREATED_HERE:-}" != 1 ]', cleanup)
        # ...and only when no relink of ours was ever dispatched against it.
        self.assertIn('echo "RELINK_DISPATCH_STARTED=1" >> "$GITHUB_ENV"', relink)
        self.assertIn('[ "${RELINK_DISPATCH_STARTED:-}" = 1 ]', cleanup)
        self.assertIn("^lines-snapshot-sha256-[0-9a-f]{64}$", cleanup)
        self.assertIn(
            'gh release delete "$TAG" -R "$GITHUB_REPOSITORY" --cleanup-tag --yes',
            cleanup,
        )
        self.assertIn("HTTP 404\\|release not found", cleanup)
        self.assertIn("exit 0", cleanup)

    def test_persistent_gradle_home_keeps_one_managed_heap_block(self):
        heaps = self.step("Bridle daemon heaps + generator forks for the 16 GB runner")
        overlay = self.step("Overlay pinned recovery Phase-2 implementation")

        # ~/.gradle is the runner's own home and survives every run, so a plain
        # append grew the file without bound and left Gradle reading whichever
        # duplicate came last.
        self.assertNotIn("cat >> ~/.gradle/gradle.properties", self.workflow)
        self.assertIn("BEGIN='# >>> manual-generate-release managed heaps >>>'", heaps)
        self.assertIn("END='# <<< manual-generate-release managed heaps <<<'", heaps)
        self.assertIn('mv "$PROPS.next" "$PROPS"', heaps)
        for line in (
            "org.gradle.jvmargs=-Xmx3g",
            "kotlin.compiler.execution.strategy=in-process",
            "generatorHeap=8g",
            "linkerHeap=12g",
        ):
            self.assertEqual(heaps.count(line + "\n"), 1, line)
        # The recovery overlay's gate still matches the emitted line verbatim.
        self.assertIn("grep -Fxq 'linkerHeap=12g' ~/.gradle/gradle.properties", overlay)

    def test_cleanup_removes_the_run_scoped_temp_trees_it_created(self):
        # 5.8 GB (raw snapshot) plus the extracted Sefaria/Otzaria inputs leaked
        # per failed attempt — both are removed only by traps inside their own
        # steps, and $RUNNER_TEMP is not run-scoped on this self-hosted runner.
        cleanup = self.step(
            "Clean run-scoped disk leftovers (workspace persists on self-hosted)"
        )
        dump = self.step("Dump lines snapshot for the linker")
        inputs = self.step("Verify pinned lineage and extract exact inputs")

        raw = '"$RUNNER_TEMP/lines-snapshot-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}.db"'
        tree = '"$RUNNER_TEMP/manual-links-inputs-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"'
        self.assertIn("RAW_SNAPSHOT=" + raw, dump)
        self.assertIn("INPUTS=" + tree, inputs)
        self.assertIn(raw, cleanup)
        self.assertIn(tree, cleanup)
        # The stale-sibling sweep stays bounded and conservative.
        self.assertIn('find "$RUNNER_TEMP" -maxdepth 1 -mtime +3', cleanup)
        self.assertIn("-name 'lines-snapshot-*-*.db'", cleanup)
        self.assertIn("-name 'manual-links-inputs-*-*'", cleanup)
        self.assertIn("du -sh", cleanup)

    def test_orphan_cleanup_reports_the_branch_it_actually_took(self):
        cleanup = self.step(
            "Cancel any in-flight relink for this build (no orphaned linker run)"
        )

        self.assertIn("RECOVERY_RUN_ID: ${{ inputs.relink_recovery_run_id }}", cleanup)
        # True in every recovery sub-case, including one whose recovery run id
        # fails validation and is therefore never actually reused.
        self.assertIn(
            "recovery mode (relink_recovery_run_id=$RECOVERY_RUN_ID): this attempt"
            " dispatches no relink of its own",
            cleanup,
        )
        self.assertIn(
            "ended before the relink dispatch boundary (RELINK_DISPATCH_STARTED is unset)",
            cleanup,
        )
        # The old wording claimed the dispatch boundary was never reached in a
        # recovery run that had deliberately skipped it.
        self.assertNotIn("never reached the relink dispatch boundary", self.workflow)
        # And the success line now names what it cancelled.
        self.assertIn('CANCELLED+="$1:$rid "', cleanup)
        self.assertIn("cancelled ${CANCELLED:-nothing", cleanup)

    # ─── generator diagnostics reports ─────────────────────────────────────
    # The generator collapses its long findings (dead priority entries, metadata
    # records that matched no book, books with no source hash, ambiguous
    # line_ref keys) into one bounded log line each plus a full JSON list on
    # disk. That trade is only honest if the JSON actually leaves the runner:
    # build/ is a tmpfs this job later unmounts.

    def test_generator_report_dir_is_pinned_to_the_root_build_dir(self):
        gradle = ROOT_BUILD.read_text(encoding="utf-8")
        self.assertIn('layout.buildDirectory.dir("generator-reports")', gradle)
        self.assertIn(
            'systemProperty("generatorReportDir", generatorReportDir)',
            gradle,
            "every report-writing JavaExec runs with its own SUBPROJECT as the "
            "working directory, so the directory must be pinned absolutely",
        )
        # Every stage that writes a report has to be wired, or its findings land
        # in generator/<module>/build/ where nothing collects them.
        for task in (
            '":sefariasqlite" to "generateSefariaSqlite"',
            '":sefariasqlite" to "seedAllMetadata"',
            '":otzariasqlite" to "appendOtzariaLines"',
            '":otzariasqlite" to "appendOtzariaLinks"',
            '":generator-common" to "buildLineRefIndex"',
        ):
            self.assertIn(task, gradle)

    @unittest.skipIf(yaml is None, "PyYAML unavailable on this runner")
    def test_generator_reports_are_not_collected_off_the_runner_yet(self):
        # Deliberate, and recorded here so it is a decision rather than an
        # oversight: this workflow collects nothing through Actions artifact
        # storage (see test_weekly_workflow_has_no_actions_artifact_handoffs),
        # and build/ is a tmpfs it unmounts, so build/generator-reports dies
        # with the run. Every finding's counts and a bounded head are in the
        # build log either way; only the tail of each list is lost. Publishing
        # them means either relaxing the no-artifact rule or adding a release
        # asset — an operator decision, not a logging fix.
        doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
        steps = doc["jobs"]["build-and-release"]["steps"]
        names = [s.get("name") for s in steps]
        self.assertIn("Generate Seforim Database", names)
        self.assertNotIn("generator-reports", WORKFLOW.read_text(encoding="utf-8"))
        # If that ever changes, the upload has to land before the RAM disk goes.
        self.assertIn("Release RAM-backed build dir (tmpfs)", names)

    def test_release_publisher_rejects_asset_names_github_would_normalize(self):
        helper = HANDOFF_PUBLISHER.read_text(encoding="utf-8")
        self.assertIn("release asset basename is unsafe or would be normalized by GitHub", helper)
        self.assertIn("^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$", helper)
        self.assertIn('repos/$GITHUB_REPOSITORY/releases/tags/$tag', helper)
        self.assertIn("targetCommitish:.target_commitish", helper)
        self.assertNotIn('gh release view "$tag" --json', helper)

    # ─── observability: a green step must not look like a skipped one ───────
    # Audit of run 34024655297 (07-build-success-nongenerate.md): the 27-minute
    # publish emitted two informational lines, all three provenance validators
    # printed nothing on success, the manifest step swallowed every gh failure
    # into `latest: null`, the cross-repo dispatch left no record, and the QA
    # step printed reference-snapshot drift and passed anyway.

    def test_publish_step_summarises_every_asset_and_the_release_total(self):
        machinery = RELEASE_DRAFT.read_text(encoding="utf-8")
        publish = self.step("Create draft, verify every uploaded asset, then publish")
        uploader = EARLY_UPLOAD.read_text(encoding="utf-8")

        # One line per asset, from the ONE shared implementation, so every
        # caller (publish step, early upload, patch fan) reports identically.
        self.assertIn(
            'echo "$verb $name $size sha256=${digest:0:12} in ${elapsed}s (${rate} MB/s)"',
            machinery,
        )
        self.assertEqual(machinery.count("_release_upload_note reused"), 2)
        self.assertEqual(machinery.count("_release_upload_note uploaded"), 4)
        # ...and one closing line naming the release and the totals.
        self.assertIn("release_upload_summary() {", machinery)
        self.assertIn(
            'echo "release ${RELEASE_TAG}: ${RELEASE_UPLOAD_ASSETS} assets, '
            '${RELEASE_UPLOAD_BYTES} bytes, ${RELEASE_UPLOAD_SECONDS}s"',
            machinery,
        )
        self.assertIn(
            'for asset_path in release-staging/*; do upload_asset "$asset_path"; done\n'
            "          release_upload_summary",
            publish,
        )
        self.assertIn("release_upload_summary", uploader)
        # The early uploader must not restate what upload_asset already printed.
        self.assertNotIn("early upload ${path##*/}", uploader)
        # Reporting only: no failure message and no retry budget moved.
        for kept in (
            "::error::Conflicting remote asset:",
            "::error::Digest metadata never settled for existing asset:",
            "::error::Unknown asset state:",
            "::warning::Upload failed for",
            "::error::Refusing to duplicate an upload whose digest metadata is still pending:",
        ):
            self.assertIn(kept, machinery)
        self.assertEqual(
            re.findall(r"gh release upload[^\n]*", machinery),
            ['gh release upload "$RELEASE_TAG" "$asset_path"; then'] * 2,
            "the upload invocations themselves must be untouched (never --clobber)",
        )

    def test_release_draft_uses_the_right_stat_dialect_on_gnu_and_bsd(self):
        """The shared uploader works on the Ubuntu runner and a stock macOS shell."""
        bash = shutil.which("bash")
        if bash is None:  # pragma: no cover - only on a host without bash
            self.skipTest("bash unavailable")
        source = RELEASE_DRAFT.read_text(encoding="utf-8")
        self.assertIn("file_size_bytes()", source)
        self.assertIn("stat --version", source)
        self.assertIn("stat --format='%s'", source)
        self.assertIn("stat -f '%z'", source)

        for flavor in ("gnu", "bsd"):
            with self.subTest(flavor=flavor), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                shutil.copy(RELEASE_DRAFT, root / "release_draft.sh")
                fake_bin = root / "bin"
                fake_bin.mkdir()
                fake_stat = fake_bin / "stat"
                fake_stat.write_text(
                    textwrap.dedent(
                        """\
                        #!/usr/bin/env bash
                        if [ "$1" = --version ]; then
                          [ "$STAT_FLAVOR" = gnu ] && exit 0
                          exit 1
                        fi
                        if [ "$STAT_FLAVOR" = gnu ] && [ "$1" = "--format=%s" ]; then
                          printf '17\\n'
                          exit 0
                        fi
                        if [ "$STAT_FLAVOR" = bsd ] && [ "$1" = -f ] && [ "$2" = %z ]; then
                          printf '17\\n'
                          exit 0
                        fi
                        echo "unexpected stat invocation: $*" >&2
                        exit 99
                        """
                    ),
                    encoding="utf-8",
                )
                fake_stat.chmod(0o755)
                result = subprocess.run(
                    [bash, "-c", "source ./release_draft.sh; file_size_bytes asset.bin"],
                    cwd=root,
                    env=dict(os.environ, PATH=f"{fake_bin}{os.pathsep}{os.environ['PATH']}", STAT_FLAVOR=flavor),
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(result.stdout.strip(), "17")

    def test_upload_asset_reports_each_asset_against_a_stub_gh(self):
        """Drive the real upload_asset in a sandbox: three assets, one reused."""
        bash = shutil.which("bash")
        if bash is None:  # pragma: no cover - only on a host without bash
            self.skipTest("bash unavailable")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shutil.copy(RELEASE_DRAFT, root / "release_draft.sh")
            (root / "assets").mkdir()
            (root / "assets" / "alpha.bin").write_bytes(b"a" * 4096)
            (root / "assets" / "beta.bin").write_bytes(b"b" * 8192)
            script = textwrap.dedent(
                """
                set -euo pipefail
                if command -v cygpath > /dev/null 2>&1; then
                  PYBIN="$(cygpath -u "$REAL_PYTHON")"
                else
                  PYBIN="$REAL_PYTHON"
                fi
                export RUNNER_TEMP="$PWD/runner-temp"
                mkdir -p "$RUNNER_TEMP"
                export GITHUB_REPOSITORY=Otzaria/SeforimLibrary
                export RELEASE_TAG=v99-sandbox
                export RELEASE_TOKEN_KIND=automatic
                export RELEASE_AUTOMATIC_WRITABLE=true
                export RELEASE_CROSS_REPO_WRITABLE=false
                export AUTOMATIC_TOKEN=stub
                export CROSS_REPO_TOKEN=stub
                STATE="$PWD/remote.json"
                echo '{"assets":[]}' > "$STATE"
                # Shell functions shadow PATH lookups, so no executable bit and no
                # real gh/python3 is needed on the host running this test.
                python3() { "$PYBIN" "$@"; }
                gh() {
                  case "$1 ${2:-}" in
                    "api "*) cat "$STATE"; return 0 ;;
                    "release upload")
                      "$PYBIN" - "$STATE" "$4" <<'PY'
                import hashlib, json, os, sys
                state, path = sys.argv[1], sys.argv[2]
                doc = json.load(open(state, encoding="utf-8"))
                doc["assets"].append({
                    "name": os.path.basename(path),
                    "size": os.path.getsize(path),
                    "digest": "sha256:" + hashlib.sha256(open(path, "rb").read()).hexdigest(),
                })
                json.dump(doc, open(state, "w", encoding="utf-8"))
                PY
                      return 0 ;;
                  esac
                  echo "stub gh: unexpected invocation: $*" >&2
                  return 90
                }
                source ./release_draft.sh
                use_token automatic
                RELEASE_ID=1
                upload_asset assets/alpha.bin
                upload_asset assets/beta.bin
                upload_asset assets/alpha.bin
                release_upload_summary
                """
            )
            env = dict(os.environ, REAL_PYTHON=sys.executable)
            result = subprocess.run(
                [bash, "-c", script], cwd=tmp, env=env, capture_output=True, text=True
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            lines = [line for line in result.stdout.splitlines() if line.strip()]

        rate = r"in \d+s \(\d+\.\d\d MB/s\)$"
        self.assertRegex(lines[0], r"^uploaded alpha\.bin 4096 sha256=[0-9a-f]{12} " + rate)
        self.assertRegex(lines[1], r"^uploaded beta\.bin 8192 sha256=[0-9a-f]{12} " + rate)
        # Third call: byte-identical asset already on the release. The dedupe
        # that the audit found sound but invisible now says so.
        self.assertRegex(lines[2], r"^reused alpha\.bin 4096 sha256=[0-9a-f]{12} " + rate)
        self.assertRegex(
            lines[3], r"^release v99-sandbox: 3 assets, 16384 bytes, \d+s$"
        )
        self.assertEqual(len(lines), 4, lines)

    # Stub server for the draft listing: a release created by the stub stays
    # out of the listing for `lag` further list calls, as GitHub's did.
    _LISTING_STUB = textwrap.dedent(
        """\
        import json, sys
        from pathlib import Path
        state = Path(sys.argv[1])
        doc = json.loads((state / "server.json").read_text())
        verb = sys.argv[2]
        if verb == "list":
            doc["list_calls"] += 1
            (state / "server.json").write_text(json.dumps(doc))
            if doc["list_mode"] == "fail":
                raise SystemExit(1)
            if doc["list_mode"] == "garbage":
                print("<html>Bad gateway</html>")
                raise SystemExit(0)
            print(json.dumps([r for r in doc["releases"] if doc["list_calls"] > r["hidden_until"]]))
        elif verb == "delete":
            doc["deleted"].append(int(sys.argv[3]))
        elif verb == "create":
            tag, target = sys.argv[3], sys.argv[4]
            doc["creates"].append(doc["list_calls"])
            plan = doc["create_plan"].pop(0)
            if plan["records"]:
                doc["releases"].append({
                    "id": 100 + len(doc["creates"]), "tag_name": tag, "name": tag,
                    "target_commitish": target, "draft": True, "assets": [],
                    "hidden_until": doc["list_calls"] + plan["lag"],
                })
            (state / "server.json").write_text(json.dumps(doc))
            raise SystemExit(plan["rc"])
        (state / "server.json").write_text(json.dumps(doc))
        """
    )

    def _run_draft_listing(self, call, releases=(), create_plan=(), lag=0,
                           list_mode="ok", step=None):
        """Run one release_draft.sh call (or a workflow step body as ./step.sh)
        against the stub; return (rc, out, err, server). server["temp"] lists
        what was left in RUNNER_TEMP."""
        bash = shutil.which("bash")
        jq = shutil.which("jq")
        if bash is None or jq is None:  # pragma: no cover - only on a bare host
            self.skipTest("bash/jq unavailable")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shutil.copy(RELEASE_DRAFT, root / "release_draft.sh")
            if step is not None:
                if yaml is None:  # pragma: no cover - only without PyYAML
                    self.skipTest("PyYAML unavailable")
                control = root / ".pipeline-control" / ".github" / "scripts"
                control.mkdir(parents=True)
                shutil.copy(RELEASE_DRAFT, control / "release_draft.sh")
                doc = yaml.safe_load(self.workflow)
                body = [
                    item["run"] for item in doc["jobs"]["build-and-release"]["steps"]
                    if item.get("name") == step
                ]
                self.assertEqual(len(body), 1, step)
                (root / "step.sh").write_text(body[0], encoding="utf-8")
            (root / "stub.py").write_text(self._LISTING_STUB, encoding="utf-8")
            (root / "server.json").write_text(json.dumps({
                "list_calls": 0, "sleeps": 0, "creates": [], "deleted": [],
                "list_mode": list_mode,
                "releases": [dict(r, hidden_until=lag) for r in releases],
                "create_plan": list(create_plan),
            }))
            script = textwrap.dedent(
                """
                set -uo pipefail
                if command -v cygpath > /dev/null 2>&1; then
                  PYBIN="$(cygpath -u "$REAL_PYTHON")"
                else
                  PYBIN="$REAL_PYTHON"
                fi
                export RUNNER_TEMP="$PWD/runner-temp"
                mkdir -p "$RUNNER_TEMP"
                export GITHUB_REPOSITORY=Otzaria/SeforimLibrary
                export RELEASE_TAG=v99-sandbox
                export SOURCE_COMMIT=0123456789abcdef0123456789abcdef01234567
                export RELEASE_AUTOMATIC_WRITABLE=true
                export RELEASE_CROSS_REPO_WRITABLE=true
                export AUTOMATIC_TOKEN=stub
                export CROSS_REPO_TOKEN=stub
                python3() { "$PYBIN" "$@"; }
                sleep() {
                  "$PYBIN" -c 'import json,sys; p=sys.argv[1]; d=json.load(open(p)); d["sleeps"]+=1; json.dump(d,open(p,"w"))' "$PWD/server.json"
                }
                gh() {
                  case "$*" in
                    "api --paginate repos/$GITHUB_REPOSITORY/releases?per_page=100")
                      "$PYBIN" "$PWD/stub.py" "$PWD" list; return ;;
                    "release create $RELEASE_TAG --target $SOURCE_COMMIT --title $RELEASE_TAG --draft")
                      "$PYBIN" "$PWD/stub.py" "$PWD" create "$RELEASE_TAG" "$SOURCE_COMMIT"; return ;;
                    "api -X DELETE repos/$GITHUB_REPOSITORY/releases/"*)
                      "$PYBIN" "$PWD/stub.py" "$PWD" delete "${4##*/}"; return ;;
                  esac
                  echo "stub gh: unexpected invocation: $*" >&2
                  return 90
                }
                source ./release_draft.sh
                use_token automatic
                """
            ) + call + "\n"
            env = dict(os.environ, REAL_PYTHON=sys.executable)
            result = subprocess.run(
                [bash, "-c", script], cwd=tmp, env=env, capture_output=True, text=True
            )
            server = json.loads((root / "server.json").read_text())
            server["temp"] = sorted(path.name for path in (root / "runner-temp").iterdir())
        return result.returncode, result.stdout, result.stderr, server

    def _draft(self, release_id=7, **overrides):
        value = {
            "id": release_id, "tag_name": "v99-sandbox", "name": "v99-sandbox",
            "target_commitish": "0123456789abcdef0123456789abcdef01234567",
            "draft": True, "assets": [],
        }
        value.update(overrides)
        return value

    def test_draft_id_lookup_waits_out_a_lagging_listing_without_relaxing(self):
        # 27/09 and 10/10: the create succeeded, the listing right after it was
        # empty. A lag inside the window now resolves the same exact draft.
        rc, out, err, server = self._run_draft_listing(
            "resolve_release_id", releases=[self._draft()], lag=3
        )
        self.assertEqual(rc, 0, out + err)
        self.assertEqual(out.strip(), "7")
        self.assertEqual((server["list_calls"], server["sleeps"]), (4, 3))
        self.assertIn("release v99-sandbox listed after 4 polls", err)
        self.assertNotIn("release-id-", " ".join(server["temp"]))

        # Never listed: fails after exactly the bound, not one poll more, and
        # says whether the listing was empty or failed.
        for mode, reason in (
            ("ok", "listing failed 0 times, was empty 12 times"),
            ("fail", "listing failed 12 times, was empty 0 times"),
            ("garbage", "listing failed 12 times, was empty 0 times"),
        ):
            with self.subTest(list_mode=mode):
                rc, out, err, server = self._run_draft_listing(
                    "resolve_release_id", list_mode=mode
                )
                self.assertNotEqual(rc, 0)
                self.assertEqual(out, "")
                self.assertEqual((server["list_calls"], server["sleeps"]), (12, 11))
                self.assertIn(
                    "release v99-sandbox not listed after 12 polls: " + reason, err
                )
                self.assertNotIn("release-id-", " ".join(server["temp"]))

        # Two drafts with this tag, or one that is not exactly ours, fail on the
        # first listing: the retry only ever waits out an EMPTY listing.
        for releases in (
            [self._draft(7), self._draft(8)],
            [self._draft(target_commitish="f" * 40)],
            [self._draft(draft=False)],
            [self._draft(name="other")],
            [self._draft(assets=[{"name": "seforim.db.zst"}])],
        ):
            with self.subTest(releases=releases):
                rc, out, err, server = self._run_draft_listing(
                    "resolve_release_id", releases=releases
                )
                self.assertNotEqual(rc, 0)
                self.assertEqual(out, "")
                self.assertEqual((server["list_calls"], server["sleeps"]), (1, 0))
                self.assertNotIn("release-id-", " ".join(server["temp"]))

        # The early asset alone is still adopted, on the first listing.
        rc, out, err, server = self._run_draft_listing(
            "resolve_release_id",
            releases=[self._draft(assets=[{"name": "seforim.db.buildstate.zst"}])],
        )
        self.assertEqual((rc, out.strip(), server["list_calls"]), (0, "7", 1))

    def test_a_lost_create_response_never_creates_a_second_draft(self):
        # The create succeeded but its response was lost, and the listing lags:
        # the draft is adopted once it shows up; no second create is sent.
        rc, out, err, server = self._run_draft_listing(
            "ensure_draft",
            create_plan=[{"rc": 1, "records": True, "lag": 4}],
        )
        self.assertEqual(rc, 0, out + err)
        self.assertEqual(server["creates"], [1])
        self.assertEqual(len(server["releases"]), 1)
        self.assertEqual((server["list_calls"], server["sleeps"]), (6, 4))

        # A create that really did nothing: "absent" is trusted only after the
        # whole 12-poll window, then the other credential creates exactly once.
        rc, out, err, server = self._run_draft_listing(
            "ensure_draft",
            create_plan=[
                {"rc": 1, "records": False, "lag": 0},
                {"rc": 0, "records": True, "lag": 0},
            ],
        )
        self.assertEqual(rc, 0, out + err)
        self.assertEqual(server["creates"], [1, 14])
        self.assertEqual(len(server["releases"]), 1)

        # Two drafts with this tag — before a create, or surfacing after a lost
        # one — still fail closed, and nothing further is created.
        rc, out, err, server = self._run_draft_listing(
            "ensure_draft", releases=[self._draft(7), self._draft(8)]
        )
        self.assertNotEqual(rc, 0)
        self.assertEqual(server["creates"], [])
        self.assertIn("conflicting identity or assets", out)
        rc, out, err, server = self._run_draft_listing(
            "ensure_draft",
            releases=[self._draft(8)],
            lag=3,
            create_plan=[{"rc": 1, "records": True, "lag": 2}],
        )
        self.assertNotEqual(rc, 0)
        self.assertEqual(server["creates"], [1])

    def test_release_state_adopts_only_this_builds_exact_draft(self):
        # Every immutable field is checked: a draft that differs in any of them
        # is a conflict, so ensure_draft neither adopts it nor creates another.
        rc, out, err, server = self._run_draft_listing(
            "release_state", releases=[self._draft()]
        )
        self.assertEqual((rc, out.strip()), (0, "exact-draft"), err)
        for overrides in (
            {"target_commitish": "f" * 40},
            {"name": "other"},
            {"draft": False},
            {"assets": [{"name": "seforim.db.zst"}]},
            {"assets": [{"name": "seforim.db.buildstate.zst"}] * 2},
        ):
            with self.subTest(overrides=overrides):
                rc, out, err, server = self._run_draft_listing(
                    "release_state", releases=[self._draft(**overrides)]
                )
                self.assertEqual((rc, out.strip()), (0, "conflict"), err)
                rc, out, err, server = self._run_draft_listing(
                    "ensure_draft", releases=[self._draft(**overrides)]
                )
                self.assertNotEqual(rc, 0)
                self.assertEqual(server["creates"], [])
                self.assertIn("conflicting identity or assets", out)

    def test_settled_release_state_waits_only_on_an_empty_listing(self):
        # A conflict or this build's exact draft is final on the first listing;
        # only "absent" (or a failed query) is polled.
        for releases, state in (
            ([self._draft(target_commitish="f" * 40)], "conflict"),
            ([self._draft(7), self._draft(8)], "conflict"),
            ([self._draft()], "exact-draft"),
        ):
            with self.subTest(releases=releases):
                rc, out, err, server = self._run_draft_listing(
                    "settled_release_state", releases=releases
                )
                self.assertEqual((rc, out.strip()), (0, state), err)
                self.assertEqual((server["list_calls"], server["sleeps"]), (1, 0))
        rc, out, err, server = self._run_draft_listing(
            "settled_release_state", releases=[self._draft()], lag=2
        )
        self.assertEqual(
            (out.strip(), server["list_calls"], server["sleeps"]), ("exact-draft", 3, 2)
        )
        rc, out, err, server = self._run_draft_listing("settled_release_state")
        self.assertEqual(
            (out.strip(), server["list_calls"], server["sleeps"]), ("absent", 12, 11)
        )

    def test_failure_cleanup_finds_a_draft_the_listing_has_not_caught_up_with(self):
        # A build that fails seconds after the early upload created its draft
        # must still delete it, not report "nothing to delete" off a lagging
        # listing. The exact-identity `.draft == true` filter is unchanged.
        step = "Delete this build's unpublished draft release on failure"
        marked = "export RELEASE_TOKEN_KIND=automatic DRAFT_RELEASE_CREATED=1\nsource ./step.sh"
        rc, out, err, server = self._run_draft_listing(
            marked, releases=[self._draft()], lag=3, step=step
        )
        self.assertEqual(rc, 0, out + err)
        self.assertEqual(server["deleted"], [7])
        self.assertEqual((server["list_calls"], server["sleeps"]), (4, 3))
        self.assertIn("deleted the unpublished draft release v99-sandbox (7).", out)
        self.assertNotIn("delete-draft-", " ".join(server["temp"]))

        # Nothing ever listed: the whole bound, then "nothing to delete", green.
        rc, out, err, server = self._run_draft_listing(marked, step=step)
        self.assertEqual(rc, 0, out + err)
        self.assertEqual(server["deleted"], [])
        self.assertEqual(server["list_calls"], 12)
        self.assertIn("nothing to delete", out)
        self.assertNotIn("delete-draft-", " ".join(server["temp"]))

        # A published release, a foreign target or two releases are never
        # deleted, and are decided on the first listing.
        for releases in (
            [self._draft(draft=False)],
            [self._draft(target_commitish="f" * 40)],
            [self._draft(7), self._draft(8)],
        ):
            with self.subTest(releases=releases):
                rc, out, err, server = self._run_draft_listing(
                    marked, releases=releases, step=step
                )
                self.assertEqual(rc, 0, out + err)
                self.assertEqual(server["deleted"], [])
                self.assertEqual(server["list_calls"], 1)

        # Without the marker the listing is never even read.
        rc, out, err, server = self._run_draft_listing(
            "export RELEASE_TOKEN_KIND=automatic\nsource ./step.sh",
            releases=[self._draft()], step=step,
        )
        self.assertEqual((rc, server["list_calls"], server["deleted"]), (0, 0, []))

    def test_provenance_validators_each_print_one_positive_line(self):
        build = VALIDATOR.read_text(encoding="utf-8")
        otzaria = OTZARIA_VALIDATOR.read_text(encoding="utf-8")
        sefaria = SEFARIA_VALIDATOR.read_text(encoding="utf-8")

        self.assertIn('f"ok: build_provenance v{value[\'schema_version\']}, ', build)
        self.assertIn("{len(value['assets'])} assets, ", build)
        self.assertIn('f"ok: otzaria_provenance v{value[\'schema_version\']}, ', otzaria)
        self.assertIn('f"ok: sefaria_release_metadata tag={checked[\'tag\']}, ', sefaria)
        # The failure contract is untouched: still stderr, still a non-zero exit.
        self.assertIn("build provenance contract error:", build)
        self.assertIn("return 2", build)
        self.assertIn("Otzaria provenance contract error:", otzaria)
        self.assertIn("raise SystemExit(str(error)) from error", sefaria)

    def test_reuse_scan_summarises_instead_of_one_ok_line_per_release(self):
        # The reuse scan validates EVERY candidate release (~27 of them), so the
        # positive line belongs to the loop, not to each iteration: --quiet there,
        # one count afterwards. The staged document this build actually publishes
        # is the real validation call site and keeps its own line.
        lookup = self.step("Find and verify exact provenance")
        stage = self.step("Stage release assets")

        self.assertIn("validate_build_provenance.py --quiet \"$file\"", lookup)
        self.assertIn("SCANNED_PROVENANCE=0", lookup)
        self.assertIn("SCANNED_PROVENANCE=$((SCANNED_PROVENANCE+1))", lookup)
        self.assertIn(
            'echo "ok: build_provenance checked on $SCANNED_PROVENANCE releases"', lookup
        )
        # The counter is bumped only for a release whose provenance validated,
        # i.e. after the legacy-skip `continue`.
        self.assertLess(
            lookup.index("::warning::Skipping legacy release"),
            lookup.index("SCANNED_PROVENANCE=$((SCANNED_PROVENANCE+1))"),
        )
        self.assertIn("validate_build_provenance.py \"$STAGE/build_provenance.json\"", stage)
        self.assertNotIn("--quiet", stage)

    def test_library_stats_are_an_advisory_staged_asset(self):
        # library_stats.json is display-only (the website's stats banner), so it
        # must never cost the release — and it must still be a STAGED asset:
        # verify_remote demands remote == staged, and the reuse scan counts
        # `provenance assets + 1`, so an asset uploaded after publish breaks both.
        stats = self.step("Compute library stats (advisory)")
        stage = self.step("Stage release assets")

        # Reads build/seforim.db, which the compress step keeps (-o, no --rm),
        # and must finish before staging hashes the directory.
        self.assertLess(
            self.workflow.index("      - name: Compress Seforim Database (zstd)\n"),
            self.workflow.index("      - name: Compute library stats (advisory)\n"),
        )
        self.assertLess(
            self.workflow.index("      - name: Compute library stats (advisory)\n"),
            self.workflow.index("      - name: Stage release assets\n"),
        )
        self.assertIn("EXPECTED_DB_VERSION: ${{ steps.discover.outputs.db_version }}", stats)

        # Advisory: GitHub runs the step with `bash -e`, so the script itself
        # never adds -e and every command that can fail is guarded by
        # `|| skip` — each failure path is skip() → warning + exit 0, and skip()
        # leaves no partial file behind for staging to pick up.
        self.assertIn("set -uo pipefail", stats)
        self.assertNotIn("set -euo pipefail", stats)
        self.assertNotIn("exit 1", stats)
        self.assertNotIn("::error::", stats)
        self.assertIn('rm -f "$OUT" "$OUT.tmp"\n', stats)
        self.assertIn('echo "::warning::library stats: $1', stats)
        # A ~/.sqliterc on the runner must not change the CLI's output format.
        self.assertIn("sqlite3 -init /dev/null -readonly -bail build/seforim.db", stats)
        self.assertIn("exit 0", stats)

        # Staged conditionally, and BEFORE the provenance hashes the stage.
        copy = 'if [ -s build/library_stats.json ]; then'
        self.assertIn(
            copy + "\n"
            '            if ! cp build/library_stats.json "$STAGE/"; then\n'
            '              echo "::warning::library stats: could not stage library_stats.json',
            stage,
        )
        self.assertIn('rm -f "$STAGE/library_stats.json" || true', stage)
        self.assertLess(stage.index(copy), stage.index('python3 - "$STAGE"'))

        # The query produces the exact published bytes, and the step's own
        # validator accepts them — and rejects a foreign db_version.
        query = re.search(r'build/seforim\.db "(select json_object\(.*?\);)"', stats).group(1)
        validator = textwrap.dedent(
            stats.split("<<'PY' || skip", 1)[1].split("\n", 1)[1].split("\n          PY\n", 1)[0]
        )
        import sqlite3  # noqa: PLC0415 - stdlib, only this test needs it

        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "seforim.db"
            with sqlite3.connect(db) as conn:
                conn.executescript(
                    "create table schema_meta(key text primary key, value text);"
                    "insert into schema_meta values ('db_version', '28');"
                    "create table book(id); insert into book values (1), (2);"
                    "create table link(id); insert into link values (1);"
                    "create table line(id); insert into line values (1), (2), (3);"
                )
                row = conn.execute(query).fetchone()[0]
            conn.close()
            out = Path(tmp) / "library_stats.json"
            out.write_bytes(row.encode() + b"\n")
            self.assertEqual(
                out.read_bytes(),
                b'{"schema_version":1,"db_version":28,"books":2,"links":1,"lines":3}\n',
            )
            script = Path(tmp) / "validate.py"
            script.write_text(validator, encoding="utf-8")
            ok = subprocess.run([sys.executable, str(script), str(out), "28"])
            self.assertEqual(ok.returncode, 0)
            wrong = subprocess.run([sys.executable, str(script), str(out), "29"])
            self.assertNotEqual(wrong.returncode, 0)
            out.write_bytes(row.encode() + b"\r\n")
            crlf = subprocess.run([sys.executable, str(script), str(out), "28"])
            self.assertNotEqual(crlf.returncode, 0)

    def test_build_provenance_quiet_only_silences_the_positive_line(self):
        sys.path.insert(0, str(Path(__file__).parent))
        import test_build_provenance  # noqa: PLC0415 - sibling fixture, not a package

        document = test_build_provenance.BuildProvenanceContractTest().value()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "build_provenance.json"
            path.write_bytes(
                (
                    json.dumps(
                        document,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                ).encode("utf-8")
            )
            loud = subprocess.run(
                [sys.executable, str(VALIDATOR), str(path)], capture_output=True, text=True
            )
            quiet = subprocess.run(
                [sys.executable, str(VALIDATOR), "--quiet", str(path)],
                capture_output=True,
                text=True,
            )
            self.assertEqual(loud.returncode, 0, loud.stderr)
            self.assertTrue(loud.stdout.startswith("ok: build_provenance "), loud.stdout)
            self.assertEqual(quiet.returncode, 0, quiet.stderr)
            self.assertEqual(quiet.stdout, "")

            document["assets"][0]["size"] = 0
            path.write_bytes(
                (
                    json.dumps(
                        document,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                ).encode("utf-8")
            )
            broken = subprocess.run(
                [sys.executable, str(VALIDATOR), "--quiet", str(path)],
                capture_output=True,
                text=True,
            )
            # --quiet silences the pass, never the failure.
            self.assertEqual(broken.returncode, 2)
            self.assertEqual(broken.stdout, "")
            self.assertIn("build provenance contract error:", broken.stderr)

    def test_sefaria_metadata_validator_prints_its_positive_line(self):
        # This one has no importable module name (the file is hyphenated), so it
        # is exercised as the workflow runs it: as a process.
        metadata = {
            "tag": "export-v1",
            "run_id": 7,
            "run_attempt": 1,
            "archive": {
                "sha256": "a" * 64,
                "size": 3,
                "parts": [
                    {"name": "part-aa", "size": 1, "sha256": "b" * 64},
                    {"name": "part-ab", "size": 2, "sha256": "c" * 64},
                ],
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "release_metadata.json"
            path.write_text(json.dumps(metadata), encoding="utf-8")
            argv = [sys.executable, str(SEFARIA_VALIDATOR), str(path), "export-v1", "a" * 64]
            done = subprocess.run(argv, capture_output=True, text=True)
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertEqual(
                done.stdout.strip(),
                "ok: sefaria_release_metadata tag=export-v1, archive 3 bytes in 2 parts, "
                "sha256=aaaaaaaaaaaa",
            )
            argv[-2] = "export-v2"
            done = subprocess.run(argv, capture_output=True, text=True)
            self.assertNotEqual(done.returncode, 0)
            self.assertEqual(done.stdout, "")

    def test_release_manifest_separates_no_release_from_a_failed_query(self):
        manifest = self.step("Regenerate and push the release manifest")
        companion = MANIFEST_WORKFLOW.read_text(encoding="utf-8")

        for body in (manifest, companion):
            # stderr is kept, not discarded, so the two cases are separable.
            self.assertNotIn(
                "gh release view --json tagName,name,publishedAt,assets 2>/dev/null", body
            )
            self.assertIn(
                'if LATEST_RAW=$(gh release view --json tagName,name,publishedAt,assets '
                '2>"$LATEST_ERR"); then',
                body,
            )
            self.assertIn(
                "elif grep -qEi 'release not found|404: not found "
                "\\(https://api\\.github\\.com/repos/[^)]*/releases' \"$LATEST_ERR\"; then",
                body,
            )
            self.assertIn(
                'echo "manifest: no release published yet (gh reported 404) '
                '— writing latest: null"',
                body,
            )
            self.assertIn("LATEST_ARG=(--argjson latest null)", body)
            # Anything else is an error that fails the step instead of shipping
            # a manifest that claims the repository has no release.
            self.assertIn("::error::gh release view failed for $GITHUB_REPOSITORY", body)
            self.assertIn("exit 1", body)
        # The weekly step used to emit zero informational lines out of 33.
        self.assertIn("manifest: latest release ", manifest)
        self.assertIn("published releases, generatedAt=", manifest)
        self.assertIn("manifest: committed $(git rev-parse --short HEAD) and pushed", manifest)
        self.assertIn("manifest: unchanged at $(git rev-parse --short HEAD)", manifest)

    def test_cross_repo_dispatches_record_what_they_asked_for(self):
        saga = self.step("Dispatch zero-trust lookup key")
        relink = self.step("Run LinkerToOtzaria relink on this snapshot (and wait)")

        # A repository dispatch answers 204 with an empty body: without these
        # lines a swallowed dispatch and a good one look identical.
        self.assertIn(
            'echo "dispatch: repos/Otzaria/otzaria-library '
            "event_type=seforim-published correlation_id=$CORRELATION_ID "
            'saga=$SAGA_RUN_ID:$SAGA_RUN_ATTEMPT child=$GITHUB_RUN_ID:$GITHUB_RUN_ATTEMPT"',
            saga,
        )
        self.assertIn("dispatch accepted by repos/Otzaria/otzaria-library", saga)
        self.assertLess(
            saga.index("echo \"dispatch: repos/Otzaria/otzaria-library"),
            saga.index("gh api repos/Otzaria/otzaria-library/dispatches"),
        )
        # The relink dispatch names the workflow, the target and every non-secret
        # input. The run URL stays with the wait loop; it is not duplicated here.
        self.assertEqual(relink.count("DISPATCHED_WORKFLOW=relink.yml"), 2)
        self.assertEqual(relink.count("DISPATCHED_WORKFLOW=kaggle-relink.yml"), 1)
        self.assertIn(
            'echo "dispatched $DISPATCHED_WORKFLOW to Otzaria/LinkerToOtzaria: '
            "target=$SERIAL_LINKER_TARGET library_run_id=$GITHUB_RUN_ID "
            "parent_run_attempt=$GITHUB_RUN_ATTEMPT relink_request_id=$RELINK_REQUEST_ID "
            "wait_contract_sha256=$WAIT_CONTRACT_SHA256 "
            'sefaria_tag=$SEFARIA_TAG snapshot_sha256=$SNAPSHOT_ZST_SHA256"',
            relink,
        )
        self.assertEqual(relink.count('RUN_URL="https://github.com/Otzaria/LinkerToOtzaria'), 1)

    def test_qa_step_gates_reference_snapshot_drift(self):
        qa = self.step("QA — run §10 checks on the full DB (required)")
        common = (QA_DIR / "common.py").read_text(encoding="utf-8")
        run_all = (QA_DIR / "run_all.py").read_text(encoding="utf-8")

        # The threshold is an env var whose default lives in the workflow, with
        # this cycle's measured drift written down next to it.
        self.assertIn('QA_DRIFT_MAX_SHRINK_PCT: "2"', qa)
        self.assertIn("12970 vs 13056 =", qa)
        self.assertIn("−0.659%", qa)
        # The gate itself: shrink past the threshold errors, everything else warns.
        self.assertIn("def gate_snapshot_drift(label, observed, snapshot):", common)
        self.assertIn("if delta < 0 and pct > limit:", common)
        self.assertIn('print(f"::error::drift {detail}', common)
        self.assertIn('print(f"::warning::drift {detail}', common)
        self.assertIn("sys.exit(1)", common)
        # Every check that carries a baseline runs it.
        for check, metric in (
            ("check1_dependence_count.py", '"dependenceType total", db_total, SNAPSHOT_TOTAL'),
            ("check2_book_base_text.py", '"book_base_text rows", len(db_pairs), SNAPSHOT_ROWS'),
            ("check7_provenance.py", '"baseProvenance=1 links", inferred_total, SNAPSHOT_INFERRED'),
        ):
            body = (QA_DIR / check).read_text(encoding="utf-8")
            self.assertIn(f"gate_snapshot_drift({metric})", body)
            # --expect-snapshot still enforces the exact baselines: the gate does
            # not replace it and does not refresh a baseline.
            self.assertIn("expect_snapshot", body)
        # A release run cannot lose the gate silently.
        self.assertIn(
            'if args.require_all and os.environ.get(DRIFT_ENV, "").strip() == "":', run_all
        )
        self.assertIn("--require-all", qa)
        # Acronyms follow the latest Acronymizer, so check9 compares to the exact file used.
        self.assertIn("--acronym-db generator/otzariasqlite/build/acronymizer/acronymizer.db", qa)

    def test_unreadable_sefaria_schemas_are_named_or_fail_the_check(self):
        common = (QA_DIR / "common.py").read_text(encoding="utf-8")

        # Sheet.json is the ONLY sanctioned skip, and it says why in one INFO line.
        self.assertIn("KNOWN_UNREADABLE_SCHEMAS = {", common)
        self.assertIn('"Sheet.json":', common)
        self.assertIn("INFO: schemas ידועים שאינם ספרים, מדולגים: ", common)
        # Everything else that fails to parse in a pinned, digest-verified export
        # is damage: it must fail rather than shrink coverage and report PASS.
        self.assertIn("if unreadable:", common)
        self.assertIn("unreadable.append((fn, str(e)))", common)
        self.assertNotIn("skipped {skipped} unreadable schema files", common)
        self.assertNotIn("אזהרה: schema לא-קריא", common)

    def test_patch_without_a_catalog_is_informational_not_a_standing_warning(self):
        cli = PATCH_PIPELINE_CLI.read_text(encoding="utf-8")
        generate = self.step("Generate Seforim Database")

        # catalog.pb is not produced on this branch at all — the build even
        # deletes any stale copy — and the manifest omits catalogBlobName when
        # nothing was embedded, so the default case is expected, not a warning.
        self.assertIn("rm -f build/catalog.pb", generate)
        self.assertIn('catalogBlobName = if (catalogEmbedded) "catalog.pb" else null', cli)
        self.assertIn("val requestedCatalogPath = System.getProperty(\"catalogPb\")", cli)
        self.assertIn("} else if (requestedCatalogPath != null) {", cli)
        self.assertIn("no catalog.pb produced by this pipeline", cli)
        self.assertNotIn(
            'logger.w { "No catalog.pb at $catalogPath — patch ships without a catalog blob" }',
            cli,
        )
        # An explicitly requested catalog that is missing is still a real gap.
        self.assertIn("No catalog.pb at the explicitly requested $catalogPath", cli)

    # ─── JDK 25 native access, CI evidence, action majors ──────────────────

    def test_every_forked_jvm_pre_approves_the_native_access_sqlite_takes(self):
        # 15 forks printed JDK 25's four-line "restricted method … System::load
        # … Restricted methods will be blocked in a future release" block in run
        # 34024655297 — 60 log lines, every one of them sqlite-jdbc.
        root = ROOT_BUILD.read_text(encoding="utf-8")
        common = GENERATOR_COMMON_BUILD.read_text(encoding="utf-8")

        self.assertIn(
            "class EnableNativeAccess : org.gradle.process.CommandLineArgumentProvider",
            root,
        )
        self.assertIn(
            "override fun asArguments(): Iterable<String> = "
            'listOf("--enable-native-access=ALL-UNNAMED")',
            root,
        )
        for wiring in (
            "tasks.withType<JavaExec>().configureEach {",
            "tasks.withType<Test>().configureEach {",
        ):
            self.assertIn(wiring, root)
        self.assertEqual(
            root.count("jvmArgumentProviders.add(EnableNativeAccess())"),
            2,
            "both JavaExec and Test forks load the sqlite native",
        )
        # It must travel as a PROVIDER: ~25 JavaExec tasks assign
        # `jvmArgs = listOf(…)` in their own blocks — including the two lines
        # "Overlay pinned recovery Phase-2 implementation" greps verbatim — and
        # an assignment silently discards anything appended to jvmArgs.
        self.assertNotIn("jvmArgs(\"--enable-native-access", root)
        self.assertIn(
            'grep -Fq \'jvmArgs = listOf("-Xmx$linkerHeap", "-XX:+UseG1GC")\'',
            self.workflow,
        )

        # The patch fan forks PatchPipelineCli with `java`, never through
        # Gradle, so its copy of the flag rides in the published launcher spec.
        self.assertIn('"--enable-native-access=ALL-UNNAMED")', common)
        self.assertIn("val launcherJvmArgs = patchPipelineJvmArgs", common)
        self.assertIn('PATCH_JVM_ARGS=$(sed -n \'s/^jvmArgs=//p\' "$LAUNCHER")', self.workflow)

    def test_the_boundary_contract_suites_report_how_many_tests_ran(self):
        step = self.step("Validate local orchestration boundary contracts")

        # Same order and the same fail-closed behaviour as the `&&` chain
        # this replaced, followed by the three suites no workflow used to run.
        suites = [
            "test_build_provenance",
            "test_otzaria_provenance",
            "test_host_safety",
            "test_manual_release_workflow",
            "test_verify_relink_recovery_snapshot",
            "test_ci_workflows",
            "test_patch_anchor_schema",
            "test_library_index_workflow",
            "test_prefetch_patch_anchors",
            "test_split_full_db",
        ]
        positions = [step.index(suite) for suite in suites]
        self.assertEqual(positions, sorted(positions))
        # EVERY suite in .github/scripts is gated by this step. Stated as an
        # equality so a new suite has to be added here on purpose and none of
        # these can quietly fall back out — test_prefetch_patch_anchors and
        # test_patch_anchor_schema had been ungated since they were written.
        on_disk = {path.stem for path in SCRIPTS_DIR.glob("test_*.py")}
        self.assertEqual(on_disk, set(suites))
        self.assertIn("set -euo pipefail", step)
        # One explicit line per suite, then one for the step.
        self.assertIn('echo "tests: $suite $passed passed', step)
        self.assertIn(
            'echo "tests: local orchestration boundary contracts $total passed"', step
        )
        # unittest\'s own summary is the only source of the number, and a suite
        # that collects nothing fails instead of reporting a green check.
        self.assertIn("ran=$(sed -n 's/^Ran \\([0-9][0-9]*\\) test.*/\\1/p'", step)
        self.assertIn('::error::$suite reported no tests', step)
        self.assertIn('::error::$suite failed', step)

    @unittest.skipIf(yaml is None, "PyYAML unavailable on this runner")
    def test_hosted_jobs_run_node24_checkout_and_the_self_hosted_one_does_not(self):
        # "Node.js 20 is deprecated … forced to run on Node.js 24:
        # actions/checkout@v4" fired once per job. v5 is the first checkout
        # major on Node 24. The self-hosted build job stays on v4 until its
        # runner is proven: a failure there costs the weekly release.
        doc = yaml.safe_load(self.workflow)
        seen = {}
        for job_name, job in doc["jobs"].items():
            hosted = job.get("runs-on") == "ubuntu-latest"
            for step in job.get("steps", []):
                uses = step.get("uses", "")
                if not uses.startswith("actions/checkout@"):
                    continue
                seen.setdefault(job_name, []).append(uses)
                self.assertEqual(
                    uses,
                    "actions/checkout@v5" if hosted else "actions/checkout@v4",
                    f"{job_name} runs on {job.get('runs-on')!r}",
                )
        self.assertEqual(
            sorted(seen),
            ["build-and-release", "reconcile", "refresh-release-manifest", "reuse-result"],
        )


if __name__ == "__main__":
    unittest.main()
