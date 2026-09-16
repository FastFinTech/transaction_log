#!/usr/bin/env python3
"""Run, archive, render and compare the opt-in Rust benchmark suite.

Python standard library only. See scripts/README.md for the versioned result
schema, CI usage, comparison rules and why measurements run serially.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tomllib
import uuid


ROOT = Path(__file__).resolve().parents[1]
TARGETS = ("record_reader", "record_writer", "record_io")
SCHEMA_VERSION = 1
MACHINE_KEYS = (
    "cpu_model", "physical_cores", "logical_processors", "available_parallelism",
    "memory_bytes", "os_name", "os_version", "architecture", "platform", "power_plan",
)
CONFIG_KEYS = (
    "records", "connections", "payload_bytes", "batch_records", "warmup_records",
    "write_chunk_bytes", "retain_records",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_json(path: Path, value: dict) -> None:
    """Publish checkpoints atomically, including failed/incomplete run metadata."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temporary.replace(path)


def capture(command: list[str]) -> str:
    return subprocess.check_output(command, cwd=ROOT, text=True, encoding="utf-8", errors="replace").strip()


def source_identity() -> dict:
    """A dirty checkout must not be presented as if it were the committed source.

    Hash source/config files, including untracked additions. Documentation and
    result artifacts are excluded so publishing a table doesn't change its code ID.
    Never store the source contents or an unrestricted environment dump.
    """
    status = capture(["git", "status", "--porcelain=v1"])
    paths = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT
    ).decode("utf-8").split("\0")
    digest = hashlib.sha256()
    for name in sorted(set(filter(None, paths))):
        path = ROOT / name
        if path.suffix not in (".rs", ".toml", ".lock", ".py"):
            continue
        digest.update(name.encode("utf-8") + b"\0")
        digest.update(path.read_bytes() if path.is_file() else b"<deleted>")
        digest.update(b"\0")
    return {
        "commit": capture(["git", "rev-parse", "HEAD"]),
        "dirty": bool(status),
        "status": status.splitlines(),
        "source_sha256": digest.hexdigest(),
    }


def toolchain_environment() -> dict:
    names = {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC", "RUSTC_WRAPPER",
             "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_TARGET", "CARGO_BUILD_RUSTFLAGS"}
    settings = {
        key: value for key, value in sorted(os.environ.items())
        if key in names or key.startswith(("CARGO_PROFILE_BENCH_", "CARGO_PROFILE_RELEASE_"))
        or (key.startswith("CARGO_TARGET_") and key.endswith("_RUSTFLAGS"))
    }
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
    configs = {}
    for label, directory in (("workspace", ROOT / ".cargo"), ("user", cargo_home)):
        for name in ("config", "config.toml"):
            path = directory / name
            if path.is_file():
                configs[f"{label}/{name}"] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {
        "rustc_verbose": capture([os.environ.get("RUSTC", "rustc"), "-vV"]),
        "cargo_version": capture(["cargo", "--version"]),
        "profile": "bench",
        "workspace_profiles": tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8")).get("profile", {}),
        "build_environment": settings,
        "cargo_config_sha256": configs,
    }


def execute(command: list[str], log: Path, timeout: int) -> tuple[int, str]:
    """Use argv directly, without shell quoting or pipelines masking exit codes."""
    try:
        result = subprocess.run(
            command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", timeout=timeout or None,
        )
        output = result.stdout
        code = result.returncode
    except subprocess.TimeoutExpired as error:
        output = error.stdout or b""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        output += f"\nRunner timeout after {timeout} seconds.\n"
        code = 124
    log.write_text(output, encoding="utf-8")
    return code, output


def fields(line: str) -> dict:
    values = {}
    for token in line.split()[1:]:
        key, value = token.split("=", 1)
        if key in values:
            raise ValueError(f"duplicate result field: {key}")
        try:
            values[key] = int(value)
        except ValueError:
            try:
                values[key] = float(value)
            except ValueError:
                values[key] = value
    return values


def parse_output(text: str, target: str, workload: str, config: dict) -> dict:
    """Reject truncated, mismatched or nonfinite output before publishing success."""
    lines = text.splitlines()
    machines = [json.loads(line.removeprefix("ENVIRONMENT ")) for line in lines if line.startswith("ENVIRONMENT ")]
    if len(machines) != 1 or machines[0].get("schema_version") != 1:
        raise ValueError("expected exactly one supported ENVIRONMENT record")
    results = [fields(line) for line in lines if line.startswith("RESULT ")]
    connections = [fields(line) for line in lines if line.startswith("CONNECTION ")]
    if sorted(row.get("run", 0) for row in results) != list(range(1, config["runs"] + 1)):
        raise ValueError("missing, duplicate or unexpected measured runs")
    expected_connection_rows = 0 if config["connections"] == 1 else config["connections"] * config["runs"]
    if len(connections) != expected_connection_rows:
        raise ValueError("unexpected per-connection result count")
    record_bytes = config["payload_bytes"] + 16
    interval = "read_seconds" if target == "record_reader" else "elapsed_seconds"
    for row in results + connections:
        for value in row.values():
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError("nonfinite benchmark metric")
        if row.get(interval, 0) <= 0:
            raise ValueError("benchmark duration must be positive")
        for key in ("payload_bytes", "batch_records", "retain_records"):
            if row.get(key) != config[key]:
                raise ValueError(f"unexpected {key}")
        if target != "record_reader":
            if row.get("benchmark") != target or row.get("workload") != workload:
                raise ValueError("benchmark/workload mismatch")
            if workload == "serialize" and row.get("write_chunk_bytes") != config["write_chunk_bytes"]:
                raise ValueError("write chunk size mismatch")
            if abs(row[interval] - max(row["sender_seconds_max"], row["receiver_seconds_max"])) > 0.0000011:
                raise ValueError("duration is not the last sender/receiver completion")
    samples = []
    for row in sorted(results, key=lambda item: item["run"]):
        if row.get("records") != config["records"] or row.get("bytes") != config["records"] * record_bytes:
            raise ValueError("aggregate record/byte count mismatch")
        if row.get("connections") != config["connections"]:
            raise ValueError("aggregate connection count mismatch")
        peers = [peer for peer in connections if peer["run"] == row["run"]]
        if len(peers) != (config["connections"] if config["connections"] > 1 else 0):
            raise ValueError("missing per-connection results for a measured run")
        if peers:
            if sorted(peer["connection"] for peer in peers) != list(range(1, config["connections"] + 1)):
                raise ValueError("duplicate or missing connection")
            for peer in peers:
                count = config["records"] // config["connections"] + int(peer["connection"] <= config["records"] % config["connections"])
                if peer["records"] != count or peer["bytes"] != count * record_bytes:
                    raise ValueError("per-connection count mismatch")
                if target != "record_reader" and peer.get("sent_batches") != (count + config["batch_records"] - 1) // config["batch_records"]:
                    raise ValueError("per-connection batch count mismatch")
            if abs(row[interval] - max(peer[interval] for peer in peers)) > 0.0000011:
                raise ValueError("aggregate duration is not the last worker completion")
        if target != "record_reader":
            expected_batches = sum(
                ((config["records"] // config["connections"] + int(index < config["records"] % config["connections"])) + config["batch_records"] - 1) // config["batch_records"]
                for index in range(config["connections"])
            )
            if row.get("sent_batches") != expected_batches:
                raise ValueError("aggregate batch count mismatch")
        samples.append({"run": row["run"], "elapsed_seconds": row[interval],
                        "million_records_per_minute": row["million_records_per_minute"],
                        "mib_per_second": row["mib_per_second"], "metrics": row, "connections": peers})
    return {"machine": machines[0], "samples": samples, "summary": {
        "runs": len(samples),
        **{f"{metric}_{stat}": function(sample[metric] for sample in samples)
           for metric in ("elapsed_seconds", "million_records_per_minute", "mib_per_second")
           for stat, function in (("median", statistics.median), ("min", min), ("max", max))},
    }}


def markdown(report: dict) -> str:
    passed = [case for case in report["cases"] if case["status"] == "passed"]
    lines = [f"Benchmark suite: **{report['status']}**. Started {report['started_at_utc']}.", ""]
    source = report["source"]
    lines += [f"Revision: `{source['commit']}`; dirty checkout: `{str(source['dirty']).lower()}`.",
              f"Source SHA-256: `{source['source_sha256']}`.", ""]
    if passed:
        machine = passed[0]["machine"]
        ram = machine.get("memory_bytes")
        ram_text = f"{ram / 2**30:.2f} GiB RAM" if ram else "RAM unavailable"
        lines += [f"Machine: {machine.get('cpu_model', 'unknown CPU')}; "
                  f"{machine.get('physical_cores', 'unknown')} physical / "
                  f"{machine.get('logical_processors', 'unknown')} logical CPUs; "
                  f"{ram_text}.",
                  f"OS: {machine.get('os_name', 'unknown')} {machine.get('os_version', '')}; "
                  f"architecture: {machine.get('architecture', 'unknown')}.",
                  f"Power plan/governor: {machine.get('power_plan') or 'unavailable'}.", ""]
    config = report["config"]
    lines += [f"Per sample: {config['records']:,} records, {config['connections']} connections, "
              f"{config['payload_bytes']:,}-byte payloads, {config['batch_records']:,} records/batch, "
              f"{config['warmup_records']:,} warmup records.", "",
              "| Benchmark | Workload | Runs | Median seconds | Million records/min (median) | MiB/s (median) |",
              "| --- | --- | ---: | ---: | ---: | ---: |"]
    for case in passed:
        summary = case["summary"]
        lines.append(f"| {case['target']} | {case['workload']} | {summary['runs']} | "
                     f"{summary['elapsed_seconds_median']:.3f} | "
                     f"{summary['million_records_per_minute_median']:.3f} | "
                     f"{summary['mib_per_second_median']:,.3f} |")
    if report["status"] != "passed":
        lines += ["", "Incomplete/failed suite: successful rows above are partial observations."]
    return "\n".join(lines) + "\n"


def artifacts(directory: Path, report: dict) -> None:
    write_json(directory / "results.json", report)
    (directory / "summary.md").write_text(markdown(report), encoding="utf-8")
    columns = ("target", "workload", "run", "records", "connections", "payload_bytes",
               "elapsed_seconds", "million_records_per_minute", "mib_per_second")
    with (directory / "samples.csv").open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        for case in report["cases"]:
            for sample in case.get("samples", []):
                writer.writerow({"target": case["target"], "workload": case["workload"],
                                 **{key: report["config"][key] for key in ("records", "connections", "payload_bytes")},
                                 **{key: sample[key] for key in ("run", "elapsed_seconds", "million_records_per_minute", "mib_per_second")}})


def run_suite(args: argparse.Namespace) -> int:
    config = {key: getattr(args, key) for key in (*CONFIG_KEYS, "runs")}
    if config["records"] < config["connections"] or (config["warmup_records"] and config["warmup_records"] < config["connections"]):
        raise ValueError("record/warmup totals must cover every connection (zero warmup disables it)")
    if config["payload_bytes"] > 65519:
        raise ValueError("maximum payload is 65519 bytes")
    if "record_writer" in args.targets and config["retain_records"]:
        raise ValueError("record_writer cannot retain records; select reader/io targets")
    identity = source_identity()
    toolchain = toolchain_environment()
    directory = args.output or ROOT / "benchmark-results" / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + identity["commit"][:12] + "-" + uuid.uuid4().hex[:8]
    )
    directory = directory.resolve()
    directory.mkdir(parents=True, exist_ok=False)  # Never overwrite a prior run.
    report = {"schema_version": SCHEMA_VERSION, "started_at_utc": utc_now(), "status": "running",
              "label": args.label, "source": identity, "toolchain": toolchain,
              "runner_python": sys.version, "config": config, "cases": []}
    print(f"Results: {directory}", flush=True)
    try:
        targets = list(dict.fromkeys(args.targets))
        command = ["cargo", "bench", "-p", "transaction-log-exports", "--no-run", "--locked", "--message-format=json"]
        for target in targets:
            command += ["--bench", target]
        report["build"] = {"command": command, "log": "build.log"}
        artifacts(directory, report)
        print("Building all selected executables before measurement...", flush=True)
        code, output = execute(command, directory / "build.log", args.timeout_seconds)
        report["build"]["exit_code"] = code
        if code:
            raise RuntimeError(f"build failed ({code}); see {directory / 'build.log'}")
        executables = {}
        for line in output.splitlines():
            if not line.startswith("{"):
                continue
            item = json.loads(line)
            if item.get("reason") == "compiler-artifact" and item.get("executable") and item["target"]["name"] in targets:
                executables[item["target"]["name"]] = item["executable"]
        if set(executables) != set(targets):
            raise RuntimeError("Cargo did not report every benchmark executable")
        for target in targets:
            workloads = ["prebuilt"] if target == "record_reader" else ["serialize", "copy"] if args.include_copy else ["serialize"]
            for workload in workloads:
                command = [executables[target], "--bench"]
                for key in ("records", "connections", "payload_bytes", "batch_records", "warmup_records", "runs", "retain_records"):
                    command += ["--" + key.replace("_", "-"), str(config[key])]
                if target != "record_reader":
                    command += ["--workload", workload]
                    if workload == "serialize":
                        command += ["--write-chunk-bytes", str(config["write_chunk_bytes"])]
                log_name = f"{target}-{workload}.log"
                case = {"target": target, "workload": workload, "status": "running", "command": command, "log": log_name}
                report["cases"].append(case)
                artifacts(directory, report)
                print(f"Running {target}/{workload}: {config['records']:,} records x {config['runs']} sample(s)...", flush=True)
                code, output = execute(command, directory / log_name, args.timeout_seconds)
                case["exit_code"] = code
                if code:
                    case["status"] = "failed"
                    raise RuntimeError(f"{target}/{workload} failed ({code}); see {directory / log_name}")
                case.update(parse_output(output, target, workload, config))
                case["status"] = "passed"
                print(next(line for line in output.splitlines() if line.startswith("ENVIRONMENT ")), flush=True)
                for line in output.splitlines():
                    if line.startswith("RESULT "):
                        print(line, flush=True)
                artifacts(directory, report)
        if source_identity()["source_sha256"] != identity["source_sha256"]:
            raise RuntimeError("source/config changed during the suite; results cannot identify a single build")
        if toolchain_environment() != toolchain:
            raise RuntimeError("toolchain/build configuration changed during the suite")
        report["status"] = "passed"
    except (Exception, KeyboardInterrupt) as error:
        report["status"] = "interrupted" if isinstance(error, KeyboardInterrupt) else "failed"
        report["error"] = str(error) or "interrupted"
        for case in report["cases"]:
            if case["status"] == "running":
                case["status"] = report["status"]
        print(f"Suite {report['status']}: {report['error']}", file=sys.stderr, flush=True)
    finally:
        report["finished_at_utc"] = utc_now()
        artifacts(directory, report)
    print(f"Saved JSON, CSV, Markdown and raw logs to {directory}", flush=True)
    return 0 if report["status"] == "passed" else 1


def load_report(path: Path) -> dict:
    report = json.loads(path.read_text(encoding="utf-8"))
    if report.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported result schema version")
    return report


def compare(baseline: dict, current: dict, threshold: float, allow_environment_change: bool) -> tuple[str, bool]:
    """Compare like-for-like median throughput, never timings from different tests."""
    if baseline["status"] != "passed" or current["status"] != "passed":
        raise ValueError("only completed successful suites can be compared")
    if any(baseline["config"][key] != current["config"][key] for key in CONFIG_KEYS):
        raise ValueError("workload settings differ; select a matching baseline")
    before = {(case["target"], case["workload"]): case for case in baseline["cases"]}
    after = {(case["target"], case["workload"]): case for case in current["cases"]}
    if any(not report["cases"] or any(case["status"] != "passed" for case in report["cases"])
           for report in (baseline, current)):
        raise ValueError("a successful suite must contain successful benchmark cases")
    if len(before) != len(baseline["cases"]) or len(after) != len(current["cases"]):
        raise ValueError("duplicate benchmark/workload cases")
    if before.keys() != after.keys():
        raise ValueError("benchmark/workload sets differ")
    environment_change = baseline["toolchain"] != current["toolchain"] or baseline.get("label") != current.get("label")
    for key in before:
        left, right = before[key]["machine"], after[key]["machine"]
        if not all(machine.get("cpu_model") and machine.get("memory_bytes") and machine.get("os_version") for machine in (left, right)):
            environment_change = True
        if any(left.get(field) != right.get(field) for field in MACHINE_KEYS):
            environment_change = True
    if environment_change and not allow_environment_change:
        raise ValueError("machine/toolchain/runner label differs or is incomplete; use a matching runner, or explicitly pass --allow-environment-change")
    lines = []
    if environment_change:
        lines += ["Environment difference explicitly allowed; this is not a controlled regression comparison.", ""]
    lines += ["| Benchmark | Workload | Baseline M records/min | Current M records/min | Change | Result |",
              "| --- | --- | ---: | ---: | ---: | --- |"]
    failed = False
    for key in before:
        previous = before[key]["summary"]["million_records_per_minute_median"]
        measured = after[key]["summary"]["million_records_per_minute_median"]
        if not all(math.isfinite(value) and value > 0 for value in (previous, measured)):
            raise ValueError("invalid throughput in results")
        change = (measured / previous - 1) * 100
        regression = change < -threshold - 1e-9
        failed |= regression
        lines.append(f"| {key[0]} | {key[1]} | {previous:.3f} | {measured:.3f} | {change:+.2f}% | {'REGRESSION' if regression else 'pass'} |")
    return "\n".join(lines) + "\n", failed


def positive(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def nonnegative(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return number


def self_test() -> int:
    """Small same-file correctness tests; never build Rust or transfer records."""
    import copy
    import tempfile
    import unittest

    class ReportTests(unittest.TestCase):
        def setUp(self):
            self.config = dict(records=5, connections=2, payload_bytes=0, batch_records=2,
                               warmup_records=0, runs=1, retain_records=0, write_chunk_bytes=8)
            self.machine = dict(schema_version=1, cpu_model="test CPU", physical_cores=2,
                                logical_processors=4, available_parallelism=4, memory_bytes=8192,
                                available_memory_bytes=4096, os_name="test OS", os_version="1", architecture="test")
            prefix = "benchmark=record_io workload=serialize payload_bytes=0 batch_records=2 retain_records=0 write_chunk_bytes=8"
            self.text = "\n".join([
                "ENVIRONMENT " + json.dumps(self.machine),
                f"CONNECTION run=1 connection=1 {prefix} records=3 bytes=48 elapsed_seconds=2.0 sender_seconds_max=1.9 receiver_seconds_max=2.0 sent_batches=2",
                f"CONNECTION run=1 connection=2 {prefix} records=2 bytes=32 elapsed_seconds=1.0 sender_seconds_max=0.9 receiver_seconds_max=1.0 sent_batches=1",
                f"RESULT run=1 connections=2 {prefix} records=5 bytes=80 elapsed_seconds=2.0 sender_seconds_max=1.9 receiver_seconds_max=2.0 sent_batches=3 million_records_per_minute=0.00015 mib_per_second=0.000038",
            ])
            case = dict(target="record_io", workload="serialize", status="passed", **parse_output(self.text, "record_io", "serialize", self.config))
            self.report = dict(schema_version=1, status="passed", started_at_utc="test", label=None,
                               source=dict(commit="test", dirty=True, source_sha256="test"),
                               toolchain={}, config=self.config, cases=[case])

        def test_output_requires_complete_matching_records_and_timing(self):
            invalid = [self.text.replace("RESULT ", "MISSING "), self.text + "\n" + self.text.splitlines()[-1],
                       self.text.replace("bytes=80", "bytes=81"), self.text.replace("records=3", "records=2"),
                       self.text.replace("sent_batches=3", "sent_batches=2"), self.text.replace("connection=2", "connection=1"),
                       self.text.replace("CONNECTION run=1", "CONNECTION run=2"),
                       self.text.replace("elapsed_seconds=2.0", "elapsed_seconds=3.0"),
                       self.text.replace("million_records_per_minute=0.00015", "million_records_per_minute=nan"),
                       self.text.replace("ENVIRONMENT ", "MISSING ")]
            for output in invalid:
                with self.subTest(output=output), self.assertRaises(ValueError):
                    parse_output(output, "record_io", "serialize", self.config)

        def test_regression_threshold_uses_median_and_returns_failure(self):
            current = copy.deepcopy(self.report)
            current["cases"][0]["summary"]["million_records_per_minute_median"] *= 0.9
            self.assertTrue(compare(self.report, current, 5, False)[1])
            self.assertFalse(compare(self.report, current, 10, False)[1])

        def test_volatile_free_memory_does_not_block_comparison(self):
            current = copy.deepcopy(self.report)
            current["cases"][0]["machine"]["available_memory_bytes"] = 1
            self.assertFalse(compare(self.report, current, 0, False)[1])

        def test_incompatible_or_failed_runs_cannot_silently_compare(self):
            for change in ("cpu", "missing_cpu", "toolchain", "workload", "cases", "duplicate", "failed_case", "failed"):
                current = copy.deepcopy(self.report)
                if change == "cpu":
                    current["cases"][0]["machine"]["cpu_model"] = "other CPU"
                elif change == "missing_cpu":
                    current["cases"][0]["machine"]["cpu_model"] = None
                elif change == "toolchain":
                    current["toolchain"]["rustc_verbose"] = "other compiler"
                elif change == "workload":
                    current["config"]["batch_records"] = 3
                elif change == "cases":
                    current["cases"] = []
                elif change == "duplicate":
                    current["cases"].append(copy.deepcopy(current["cases"][0]))
                elif change == "failed_case":
                    current["cases"][0]["status"] = "failed"
                else:
                    current["status"] = "failed"
                with self.subTest(change=change), self.assertRaises(ValueError):
                    compare(self.report, current, 5, False)
                if change in ("cpu", "missing_cpu", "toolchain"):
                    self.assertIn("not a controlled", compare(self.report, current, 5, True)[0])

        def test_artifact_roundtrip_and_partial_status_are_visible(self):
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory)
                artifacts(path, self.report)
                restored = load_report(path / "results.json")
                self.assertEqual(restored, self.report)
                self.assertEqual((path / "summary.md").read_text(encoding="utf-8"), markdown(restored))
                with (path / "samples.csv").open(encoding="utf-8", newline="") as csv_file:
                    self.assertEqual(len(list(csv.DictReader(csv_file))), 1)
                self.report["status"] = "failed"
                self.assertIn("partial observations", markdown(self.report))

    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(ReportTests))
    return 0 if result.wasSuccessful() else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    commands.add_parser("self-test", help="test parsing/reporting/comparison without running benchmarks")
    run = commands.add_parser("run", help="build once, run serially, preserve a result bundle")
    run.add_argument("--records", type=positive, default=100_000_000)
    run.add_argument("--connections", type=positive, default=8)
    run.add_argument("--payload-bytes", type=nonnegative, default=2048)
    run.add_argument("--batch-records", type=positive, default=8192)
    run.add_argument("--write-chunk-bytes", type=positive, default=8)
    run.add_argument("--warmup-records", type=nonnegative, default=1_000_000)
    run.add_argument("--retain-records", type=nonnegative, default=0)
    run.add_argument("--runs", type=positive, default=3)
    run.add_argument("--targets", nargs="+", choices=TARGETS, default=list(TARGETS))
    run.add_argument("--include-copy", action="store_true", help="also measure existing-record copies")
    run.add_argument("--output", type=Path, help="new directory; existing directories are never overwritten")
    run.add_argument("--label", help="stable benchmark-runner label, recorded and compared")
    run.add_argument("--timeout-seconds", type=nonnegative, default=3600, help="per build/case timeout; 0 disables")
    render = commands.add_parser("report", help="regenerate Markdown from recorded JSON without rerunning")
    render.add_argument("results", type=Path)
    render.add_argument("--output", type=Path)
    diff = commands.add_parser("compare", help="compare median throughput with an explicit regression budget")
    diff.add_argument("baseline", type=Path)
    diff.add_argument("current", type=Path)
    diff.add_argument("--max-regression-percent", type=float, required=True)
    diff.add_argument("--allow-environment-change", action="store_true")
    diff.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.action == "self-test":
            return self_test()
        if args.action == "run":
            return run_suite(args)
        if args.action == "report":
            output, code = markdown(load_report(args.results)), 0
        else:
            if not math.isfinite(args.max_regression_percent) or not 0 <= args.max_regression_percent <= 100:
                raise ValueError("regression percentage must be finite and between 0 and 100")
            output, failed = compare(load_report(args.baseline), load_report(args.current), args.max_regression_percent, args.allow_environment_change)
            code = 2 if failed else 0
        if args.output:
            args.output.write_text(output, encoding="utf-8")
        print(output, end="")
        return code
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
