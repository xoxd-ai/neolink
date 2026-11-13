"""Tests for the estate test job and its checks (TIN-8227).

Run from the repository root:

    python3 -m unittest discover -s .github/estate-test -p 'test_*.py' -v

Each test names the guard it holds. Removing the guard from the workflow or
from a script makes the test fail.
"""

from __future__ import annotations

import json
import os
import shlex
import stat
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"
sys.path.insert(0, str(HERE))

import cargo_test_summary as cts  # noqa: E402

REQUIRED_APT = [
    "libgstrtspserver-1.0-dev",
    "libgstreamer1.0-dev",
    "libgstreamer-plugins-base1.0-dev",
    "gstreamer1.0-plugins-base",
    "gstreamer1.0-plugins-good",
    "gstreamer1.0-plugins-bad",
    "gstreamer1.0-libav",
    "protobuf-compiler",
]
REQUIRED_ELEMENTS = ["voaacenc", "avenc_aac", "faad", "aacparse", "rtpmp4gpay"]

# Output in the shape cargo and libtest print it, stdout and stderr combined.
CARGO_OK = """\
   Compiling neolink_core v0.6.3-rc.3 (/build/crates/core)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 1m 02s
     Running unittests src/main.rs (target/debug/deps/neolink-1f2e3d4c5b6a7980)

running 2 tests
test rtsp::tests::one ... ok
test rtsp::tests::two ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/neolink_core-0a1b2c3d4e5f6071)

running 3 tests
test bc::de::tests::a ... ok
test bc::de::tests::b ... ok
test bc::de::tests::c ... ignored

test result: ok. 2 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.01s

   Doc-tests neolink_core

running 1 test
test crates/core/src/lib.rs - (line 10) ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.20s

"""

CARGO_NO_CORE = """\
     Running unittests src/main.rs (target/debug/deps/neolink-1f2e3d4c5b6a7980)

running 2 tests
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/lib.rs (target/debug/deps/neolink_core-0a1b2c3d4e5f6071)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

"""

CARGO_FAILED = """\
     Running unittests src/lib.rs (target/debug/deps/neolink_core-0a1b2c3d4e5f6071)

running 3 tests
test bc::de::tests::a ... ok
test bc::de::tests::b ... FAILED

failures:

---- bc::de::tests::b stdout ----
thread 'bc::de::tests::b' panicked at crates/core/src/bc/de.rs:1:1:
assertion failed

test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s

error: test failed, to rerun pass `-p neolink_core --lib`
"""

CARGO_BUILD_ERROR = """\
   Compiling tikv-jemalloc-sys v0.5.4+5.3.0-patched
error: failed to run custom build command for `tikv-jemalloc-sys v0.5.4+5.3.0-patched`
warning: build failed, waiting for other jobs to finish...
"""


def load(name: str) -> dict:
    with open(WORKFLOWS / name, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def triggers(wf: dict) -> dict:
    # YAML 1.1 reads the bare key `on` as the boolean true.
    return wf.get("on", wf.get(True)) or {}


def job_step(job: dict, name: str) -> dict:
    for step in job["steps"]:
        if step.get("name") == name:
            return step
    raise AssertionError(f"no step named {name!r}")


def step_index(job: dict, name: str) -> int:
    for i, step in enumerate(job["steps"]):
        if step.get("name") == name:
            return i
    raise AssertionError(f"no step named {name!r}")


class ParserGrammar(unittest.TestCase):
    """The summary parser reads the real cargo and libtest grammar."""

    def test_counts_unittests_and_doctests_per_crate(self):
        s = cts.parse(CARGO_OK)
        self.assertEqual(s.results, 3)
        self.assertEqual(s.crates["neolink"].passed, 2)
        core = s.crates["neolink_core"]
        self.assertEqual((core.passed, core.failed, core.ignored, core.binaries), (3, 0, 1, 2))

    def test_windows_paths_and_exe_suffix(self):
        text = (
            "     Running unittests src\\lib.rs "
            "(target\\debug\\deps\\neolink_core-0a1b2c3d4e5f6071.exe)\r\n"
            "test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; "
            "0 filtered out; finished in 0.01s\r\n"
        )
        self.assertEqual(cts.parse(text).crates["neolink_core"].passed, 4)

    def test_integration_test_path_with_spaces_in_source(self):
        text = (
            "     Running tests/my test.rs (target/debug/deps/my_test-0a1b2c3d4e5f6071)\n"
            "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; "
            "0 filtered out; finished in 0.01s\n"
        )
        self.assertEqual(cts.parse(text).crates["my_test"].passed, 1)

    def test_strips_ansi_colour(self):
        text = (
            "\x1b[1m\x1b[32m     Running\x1b[0m unittests src/lib.rs "
            "(target/debug/deps/neolink_core-0a1b2c3d4e5f6071)\n"
            "test result: \x1b[32mok\x1b[0m. 5 passed; 0 failed; 0 ignored; "
            "0 measured; 0 filtered out; finished in 0.01s\n"
        )
        self.assertEqual(cts.parse(text).crates["neolink_core"].passed, 5)

    def test_hyphenated_doctest_crate_maps_to_lib_name(self):
        text = (
            "   Doc-tests neolink-core\n"
            "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; "
            "0 filtered out; finished in 0.01s\n"
        )
        self.assertIn("neolink_core", cts.parse(text).crates)

    def test_crate_from_path_keeps_names_with_digits(self):
        self.assertEqual(cts.crate_from_path("target/debug/deps/h264_2-0a1b2c3d4e5f6071"), "h264_2")


class SummaryChecks(unittest.TestCase):
    def test_estate_test_job_runs_core_tests(self):
        """Guard G6: a run with no passing neolink_core test fails the check."""
        self.assertEqual(cts.check(cts.parse(CARGO_OK), ["neolink_core"], 1), [])
        problems = cts.check(cts.parse(CARGO_NO_CORE), ["neolink_core"], 1)
        self.assertTrue(any("neolink_core" in p for p in problems), problems)
        problems = cts.check(cts.parse(CARGO_OK.replace("neolink_core", "other")), ["neolink_core"], 1)
        self.assertTrue(any("neolink_core" in p for p in problems), problems)

    def test_failed_result_fails(self):
        """Guard G7: a FAILED test result fails the check."""
        problems = cts.check(cts.parse(CARGO_FAILED), [], 1)
        self.assertTrue(problems)
        lying = CARGO_FAILED.replace("test result: FAILED. 2 passed; 1 failed", "test result: FAILED. 2 passed; 0 failed")
        self.assertTrue(cts.check(cts.parse(lying), [], 1))

    def test_output_without_results_fails(self):
        """Guard G7: a build error with no test result line fails the check."""
        problems = cts.check(cts.parse(CARGO_BUILD_ERROR), [], 1)
        self.assertTrue(any("no `test result:`" in p for p in problems), problems)

    def test_main_writes_summary_and_exit_code(self):
        with tempfile.TemporaryDirectory() as d:
            ok = Path(d) / "ok.log"
            bad = Path(d) / "bad.log"
            summary = Path(d) / "summary.md"
            ok.write_text(CARGO_OK)
            bad.write_text(CARGO_NO_CORE)
            env = dict(os.environ, GITHUB_STEP_SUMMARY=str(summary))
            script = str(HERE / "cargo_test_summary.py")
            r = subprocess.run(
                [sys.executable, script, str(ok), "--require", "neolink_core"],
                env=env, capture_output=True, text=True,
            )
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertIn("| `neolink_core` | 2 | 3 | 0 | 1 |", summary.read_text())
            r = subprocess.run(
                [sys.executable, script, str(bad), "--require", "neolink_core"],
                env=env, capture_output=True, text=True,
            )
            self.assertEqual(r.returncode, 1, r.stdout + r.stderr)


class RequiredTests(unittest.TestCase):
    """Guard G12: a test named with --require-test must have an ok line."""

    NAME = "rtsp::tests::one"

    def problems(self, text: str, names: list[str]) -> list[str]:
        return cts.check(cts.parse(text), [], 1, names)

    def test_ok_line_passes(self):
        self.assertEqual(self.problems(CARGO_OK, [self.NAME, "rtsp::tests::two"]), [])

    def test_missing_test_fails_and_names_it(self):
        problems = self.problems(CARGO_OK, ["rtsp::tests::three"])
        self.assertEqual(problems, ["test rtsp::tests::three did not pass: no result line"])

    def test_name_must_match_exactly(self):
        self.assertTrue(self.problems(CARGO_OK, ["rtsp::tests::on"]))
        self.assertTrue(self.problems(CARGO_OK, ["tests::one"]))
        self.assertTrue(self.problems(CARGO_OK, ["rtsp::tests::one_more"]))

    def test_failed_test_fails(self):
        text = CARGO_OK.replace(f"test {self.NAME} ... ok", f"test {self.NAME} ... FAILED")
        problems = self.problems(text, [self.NAME])
        self.assertIn(f"test {self.NAME} did not pass: FAILED", problems)

    def test_ignored_test_fails(self):
        for outcome in ("ignored", "ignored, needs a camera"):
            with self.subTest(outcome=outcome):
                text = CARGO_OK.replace(f"test {self.NAME} ... ok", f"test {self.NAME} ... {outcome}")
                self.assertIn(f"test {self.NAME} did not pass: ignored", self.problems(text, [self.NAME]))

    def test_grammar_doc_test_names_ansi_and_crlf(self):
        text = CARGO_OK.replace("\n", "\r\n").replace(
            f"test {self.NAME} ... ok", f"\x1b[32mtest {self.NAME} ... ok\x1b[0m"
        )
        summary = cts.parse(text)
        self.assertEqual(summary.tests[self.NAME], ["ok"])
        self.assertEqual(summary.tests["crates/core/src/lib.rs - (line 10)"], ["ok"])
        self.assertEqual(summary.tests["bc::de::tests::c"], ["ignored"])
        # The run summary is not a test.
        self.assertFalse(any(n.startswith("result") for n in summary.tests))

    def test_main_exit_code(self):
        with tempfile.TemporaryDirectory() as d:
            log = Path(d) / "ok.log"
            log.write_text(CARGO_OK)
            script = str(HERE / "cargo_test_summary.py")
            env = {k: v for k, v in os.environ.items() if k != "GITHUB_STEP_SUMMARY"}
            for name, want in ((self.NAME, 0), ("rtsp::tests::three", 1)):
                with self.subTest(name=name):
                    r = subprocess.run(
                        [sys.executable, script, str(log), "--require-test", name],
                        env=env, capture_output=True, text=True,
                    )
                    self.assertEqual(r.returncode, want, r.stdout + r.stderr)

    def test_workflow_names_every_resubscribe_test(self):
        """The step names the five tests in src/rtsp/factory.rs, and each exists there."""
        source = (ROOT / "src/rtsp/factory.rs").read_text()
        self.assertIn("mod resubscribe_tests {", source)
        for t in RESUBSCRIBE_TESTS:
            self.assertIn(f"fn {t.rsplit('::', 1)[1]}()", source)

    def test_workflow_names_every_discovery_tid_and_pusher_end_test(self):
        """Each test the two steps name exists in its source file, in its module."""
        for path, module, tests in (
            ("crates/core/src/bc_protocol/connection/discovery.rs", "mod tests {", DISCOVERY_TID_TESTS),
            ("src/rtsp/factory.rs", "mod pusher_end_tests {", PUSHER_END_TESTS),
        ):
            source = (ROOT / path).read_text()
            self.assertIn(module, source)
            for t in tests:
                self.assertIn(f"fn {t.rsplit('::', 1)[1]}()", source)


class GstElementCheck(unittest.TestCase):
    """Guard G5: the element check fails when a named element is missing."""

    def run_check(self, present: list[str], args: list[str]) -> subprocess.CompletedProcess:
        with tempfile.TemporaryDirectory() as d:
            fake = Path(d) / "fake-inspect"
            # The shape gst-inspect-1.0 prints for one element.
            cases = "\n".join(
                f'  {e}) printf "Factory Details:\\n  Rank                     primary (256)\\n'
                f'  Long-name                {e} element\\n\\nPlugin Details:\\n'
                f'  Name                     {e}plug\\n  Description              test\\n'
                f'  Version                  1.24.2\\n  License                  LGPL\\n"; exit 0 ;;'
                for e in present
            )
            fake.write_text(f'#!/usr/bin/env bash\ncase "$1" in\n{cases}\n  *) exit 1 ;;\nesac\n')
            fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
            summary = Path(d) / "summary.md"
            env = dict(os.environ, GST_INSPECT=str(fake), GITHUB_STEP_SUMMARY=str(summary))
            r = subprocess.run(
                ["bash", str(HERE / "check_gst_elements.sh"), *args],
                env=env, capture_output=True, text=True,
            )
            r.summary = summary.read_text() if summary.exists() else ""
            return r

    def test_all_present_passes(self):
        r = self.run_check(REQUIRED_ELEMENTS, REQUIRED_ELEMENTS)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("| `voaacenc` | voaacencplug | 1.24.2 |", r.summary)

    def test_missing_element_fails_and_names_it(self):
        r = self.run_check(["avenc_aac", "faad", "aacparse", "rtpmp4gpay"], REQUIRED_ELEMENTS)
        self.assertEqual(r.returncode, 1)
        self.assertIn("missing voaacenc", r.stderr)
        self.assertIn("| `voaacenc` | missing | |", r.summary)

    def test_no_element_named_fails(self):
        self.assertEqual(self.run_check([], []).returncode, 2)


TREE_ARGS = (
    "tree --locked --all-features --target all -e normal,build "
    "-i neolink_core --depth 0 --format {p} [{f}]"
)


class NoTestUtilCheck(unittest.TestCase):
    """Guard G12: a normal build never compiles neolink_core with test-util."""

    def run_check(self, output: str) -> subprocess.CompletedProcess:
        with tempfile.TemporaryDirectory() as d:
            fake = Path(d) / "fake-cargo"
            args = Path(d) / "args"
            fake.write_text(
                "#!/usr/bin/env bash\n"
                f'printf "%s\\n" "$*" > {shlex.quote(str(args))}\n'
                f"printf '%s\\n' {shlex.quote(output.rstrip())}\n"
            )
            fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
            env = dict(os.environ, ESTATE_CARGO=str(fake))
            r = subprocess.run(
                ["bash", str(HERE / "check_no_test_util.sh")],
                env=env, capture_output=True, text=True,
            )
            r.args_seen = args.read_text().strip() if args.exists() else ""
            return r

    def test_no_feature_passes(self):
        r = self.run_check("neolink_core v0.6.3-rc.3 (/build/crates/core) []\n")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.args_seen, TREE_ARGS)

    def test_other_features_pass(self):
        r = self.run_check("neolink_core v0.6.3-rc.3 (/build/crates/core) [default,test-utils]\n")
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_test_util_fails(self):
        # The line cargo tree printed for this workspace under resolver 1.
        r = self.run_check("neolink_core v0.6.3-rc.3 (/build/crates/core) [test-util]\n")
        self.assertEqual(r.returncode, 1)
        self.assertIn("neolink_core/test-util", r.stderr)

    def test_test_util_among_others_fails(self):
        r = self.run_check("neolink_core v0.6.3-rc.3 (/build/crates/core) [default,test-util]\n")
        self.assertEqual(r.returncode, 1)

    def test_output_without_feature_list_fails(self):
        self.assertEqual(self.run_check("warning: nothing to print\n").returncode, 2)

    def test_root_uses_resolver_2(self):
        with open(ROOT / "Cargo.toml", "rb") as fh:
            manifest = tomllib.load(fh)
        self.assertEqual(manifest["workspace"].get("resolver"), "2")
        self.assertEqual(
            manifest["dev-dependencies"]["neolink_core"].get("features"), ["test-util"]
        )
        self.assertNotIn("test-util", manifest["dependencies"]["neolink_core"].get("features", []))

    def test_test_util_module_is_gated(self):
        """The fake camera module compiles only with the test-util feature."""
        lines = [
            line.strip()
            for line in (ROOT / "crates/core/src/bc_protocol.rs").read_text().splitlines()
        ]
        self.assertEqual(lines.count("pub mod test_util;"), 1)
        i = lines.index("pub mod test_util;")
        self.assertEqual(lines[i - 1], '#[cfg(feature = "test-util")]')
        with open(ROOT / "crates/core/Cargo.toml", "rb") as fh:
            features = tomllib.load(fh)["features"]
        self.assertEqual(features["test-util"], [])
        self.assertNotIn("test-util", features.get("default", []))


# The tests that drive the RTSP frame pusher's resubscribe from a plain thread.
RESUBSCRIBE_TESTS = [
    "rtsp::factory::resubscribe_tests::" + name
    for name in (
        "resubscribe_on_a_plain_thread_returns_the_new_stream",
        "resubscribe_on_a_plain_thread_times_out_a_hung_attempt",
        "resubscribe_stops_once_the_client_has_gone",
        "resubscribe_to_a_gone_camera_retries_until_the_client_leaves",
        "resubscribe_backoff_stops_at_its_max",
    )
]

# The tests that give each open discovery destination its own tid.
DISCOVERY_TID_TESTS = [
    "bc_protocol::connection::discovery::tests::" + name
    for name in (
        "every_open_destination_gets_its_own_tid",
        "new_tid_is_refused_while_all_are_held",
    )
]

# The tests that read the warn line a frame pusher leaves when it ends.
PUSHER_END_TESTS = [
    "rtsp::factory::pusher_end_tests::" + name
    for name in (
        "pusher_error_logs_camera_stream_and_cause_at_warn",
        "pusher_panic_logs_camera_stream_and_cause_at_warn",
        "pusher_literal_panic_logs_its_message_at_warn",
        "pusher_starts_for_a_camera_name_with_a_nul",
        "pusher_clean_stop_logs_no_warn",
    )
]

# Every step whose exit status decides whether estate-test passes, with the exact
# keys it may carry. A run is compared after collapsing whitespace, so only
# layout may change. An exact match, not a deny list, is what stops a later
# edit from appending `|| true`, `|| :`, a `set +` line or a narrowing flag.
GATING_STEPS = {
    ("estate-test", "Estate CI self-test"): {
        "run": "python3 -m unittest discover -s .github/estate-test -p 'test_*.py' -v",
    },
    ("estate-test", "GStreamer elements"): {
        "run": "bash .github/estate-test/check_gst_elements.sh voaacenc avenc_aac faad aacparse rtpmp4gpay",
    },
    ("estate-test", "release_build_has_no_test_util"): {
        "run": "bash .github/estate-test/check_no_test_util.sh",
    },
    ("estate-test", "cargo test"): {
        # An explicit bash shell runs with -o pipefail, so tee cannot hide a cargo failure.
        "shell": "bash",
        "run": "cargo test --workspace --locked 2>&1 | tee cargo-test.log",
    },
    ("estate-test", "estate_test_job_runs_core_tests"): {
        # Runs after a failed cargo step too, so the summary table is written.
        "if": "${{ !cancelled() }}",
        "run": "python3 .github/estate-test/cargo_test_summary.py cargo-test.log "
               "--require neolink_core --min 1",
    },
    ("estate-test", "rtsp_resubscribe_tests_ran"): {
        "if": "${{ !cancelled() }}",
        "run": "python3 .github/estate-test/cargo_test_summary.py cargo-test.log "
               + " ".join(f"--require-test {t}" for t in RESUBSCRIBE_TESTS),
    },
    ("estate-test", "discovery_tid_tests_ran"): {
        "if": "${{ !cancelled() }}",
        "run": "python3 .github/estate-test/cargo_test_summary.py cargo-test.log "
               + " ".join(f"--require-test {t}" for t in DISCOVERY_TID_TESTS),
    },
    ("estate-test", "pusher_end_tests_ran"): {
        "if": "${{ !cancelled() }}",
        "run": "python3 .github/estate-test/cargo_test_summary.py cargo-test.log "
               + " ".join(f"--require-test {t}" for t in PUSHER_END_TESTS),
    },
    ("actionlint", "actionlint estate-test.yml"): {
        "run": "./actionlint -color .github/workflows/estate-test.yml",
    },
    ("actionlint", "actionlint build.yml"): {
        "run": "./actionlint -color -shellcheck= .github/workflows/build.yml",
    },
}


def squash(text: str) -> str:
    return " ".join(str(text).split())


class EstateTestWorkflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.wf = load("estate-test.yml")
        cls.job = cls.wf["jobs"]["estate-test"]

    def test_triggers(self):
        """Guard G1: runs on every pull request and on push to master, with no filter."""
        on = triggers(self.wf)
        self.assertEqual(set(on), {"pull_request", "push", "workflow_dispatch"})
        # Any key under pull_request (branches, paths, paths-ignore, types and the
        # rest) narrows which pull requests run the job.
        self.assertIn(on["pull_request"], (None, {}))
        self.assertEqual(on["push"], {"branches": ["master"]})
        self.assertIn(on["workflow_dispatch"], (None, {}))

    def test_jobs_are_never_skipped(self):
        """Guard G1: GitHub counts a skipped job as a passed required check, so no job has an if."""
        for name, job in self.wf["jobs"].items():
            self.assertNotIn("if", job, name)
            self.assertNotIn("needs", job, name)

    def test_concurrency_never_cancels_master(self):
        """Guard G11: a newer push cancels an older pull request run, never a master run."""
        self.assertEqual(
            self.wf["concurrency"],
            {
                "group": "estate-test-${{ github.ref }}",
                "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
            },
        )

    def test_runner_and_timeout(self):
        """Guard G2: one job on ubuntu-24.04 with a 45 minute limit."""
        self.assertEqual(self.job["name"], "estate-test")
        self.assertEqual(self.job["runs-on"], "ubuntu-24.04")
        self.assertEqual(self.job["timeout-minutes"], 45)
        for name, job in self.wf["jobs"].items():
            self.assertNotIn("continue-on-error", job, name)
            # A defaults block could swap the shell of every step for one without -e.
            self.assertNotIn("defaults", job, name)
        self.assertNotIn("defaults", self.wf)

    def test_gating_steps_are_exact_and_fatal(self):
        """Guards G2, G4, G5, G6, G9 and G10: each gating step runs its exact command and can fail the job."""
        for (job_name, step_name), expected in GATING_STEPS.items():
            with self.subTest(job=job_name, step=step_name):
                step = job_step(self.wf["jobs"][job_name], step_name)
                self.assertEqual(set(step), {"name", *expected})
                run = squash(step["run"])
                self.assertEqual(run, squash(expected["run"]))
                self.assertNotIn("||", run)
                self.assertNotIn("set +", run)
                for key in set(expected) - {"run"}:
                    self.assertEqual(step[key], expected[key])

    def test_apt_packages(self):
        """Guard G3: the apt step installs every package the spec lists."""
        words = job_step(self.job, "apt install GStreamer and build deps")["run"].replace("\\", " ").split()
        missing = [p for p in REQUIRED_APT if p not in words]
        self.assertEqual(missing, [])
        self.assertIn("gstreamer1.0-tools", words)  # gst-inspect-1.0

    def test_cargo_test_default_features(self):
        """Guard G4: the default features the cargo step builds with include gstreamer."""
        with open(ROOT / "Cargo.toml", "rb") as fh:
            default = tomllib.load(fh)["features"]["default"]
        self.assertIn("gstreamer", default)

    def test_step_order(self):
        """Guards G6 and G10: self-test and element check before cargo, the summary check after it."""
        order = [step_index(self.job, n) for n in (
            "Estate CI self-test",
            "GStreamer elements",
            "Install Rust ${{ env.rust_version }}",
            "release_build_has_no_test_util",
            "cargo test",
            "estate_test_job_runs_core_tests",
            "rtsp_resubscribe_tests_ran",
            "discovery_tid_tests_ran",
            "pusher_end_tests_ran",
        )]
        self.assertEqual(order, sorted(order))

    def test_gst_elements_named(self):
        """Guard G5: the element check names every element the audio work needs."""
        run = job_step(self.job, "GStreamer elements")["run"]
        for e in REQUIRED_ELEMENTS:
            self.assertIn(e, run.split())


class BuildWorkflow(unittest.TestCase):
    def test_native_matrix(self):
        """Guard G8: macos-12 is gone and only the Windows and macOS native jobs may fail."""
        text = (WORKFLOWS / "build.yml").read_text()
        build = load("build.yml")
        # Comments drop out of the parsed YAML, so this sees jobs, steps and artifact names only.
        self.assertNotIn("macos-12", json.dumps(build))
        native = build["jobs"]["native"]
        oses = native["strategy"]["matrix"]["os"]
        self.assertIn("ubuntu-22.04", oses)
        expr = str(native.get("continue-on-error", ""))
        allowed = {os_ for os_ in oses if f"'{os_}'" in expr}
        self.assertEqual(allowed, {"windows-2022", "macos-14"})
        self.assertIn("TIN-8227", text)


if __name__ == "__main__":
    unittest.main()
