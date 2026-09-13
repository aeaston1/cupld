#!/usr/bin/env python3
"""Reproducible core graph measurements using Python's standard library only."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
QUERIES = ("count", "lookup", "limited_scan", "traversal")


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def nonnegative(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return number


def command_text(command, cwd=ROOT):
    try:
        return subprocess.check_output(command, cwd=cwd, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def fingerprint(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def machine_metadata():
    metadata = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "logical_cpus": os.cpu_count(),
    }
    for name, path in {
        "memory_total": "/proc/meminfo",
        "cpu": "/proc/cpuinfo",
        "cgroup_memory_max": "/sys/fs/cgroup/memory.max",
        "cgroup_cpu_max": "/sys/fs/cgroup/cpu.max",
    }.items():
        try:
            contents = Path(path).read_text()
        except OSError:
            continue
        if name == "memory_total":
            metadata[name] = next((line for line in contents.splitlines() if line.startswith("MemTotal:")), None)
        elif name == "cpu":
            metadata[name] = next((line.split(":", 1)[1].strip() for line in contents.splitlines()
                                   if line.startswith(("model name", "Hardware", "CPU implementer"))), None)
        else:
            metadata[name] = contents.strip()
    return metadata


def gnu_time_available():
    if platform.system() != "Linux" or not Path("/usr/bin/time").is_file():
        return False
    version = command_text(["/usr/bin/time", "--version"])
    return version is not None and "GNU" in version


def measure(command, scratch, environment, use_gnu_time, timeout):
    # Each workload gets a new process; ru_maxrss is never accumulated across siblings.
    rss_file = scratch / "peak-rss.txt"
    wrapped = command
    if use_gnu_time:
        wrapped = ["/usr/bin/time", "-f", "%M", "-o", str(rss_file), "--", *command]
    started = time.perf_counter()
    child = subprocess.Popen(wrapped, cwd=scratch, env=environment, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True, start_new_session=os.name == "posix")
    try:
        stdout, stderr = child.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        if os.name == "posix":
            os.killpg(child.pid, signal.SIGKILL)
        else:
            child.kill()
        child.communicate()
        raise
    wall_seconds = time.perf_counter() - started
    if child.returncode:
        raise RuntimeError(f"command failed ({child.returncode}): {command}\n{stderr}")
    try:
        result = json.loads(stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"command did not return JSON: {command}\n{stdout}") from error
    rss_bytes = int(rss_file.read_text().strip()) * 1024 if use_gnu_time else None
    return {
        "wall_seconds": wall_seconds,
        "peak_rss_bytes": rss_bytes,
        "stderr": stderr.strip(),
        "result": result,
    }


def checked_worker(measurement):
    if measurement["result"].get("verified") is not True:
        raise RuntimeError("worker did not verify its result")
    return measurement


def checked_count(measurement, expected):
    result = measurement["result"]
    if result.get("ok") is not True or len(result.get("results", [])) != 1:
        raise RuntimeError(f"unexpected CLI query envelope: {result}")
    rows = result["results"][0].get("rows")
    if rows != [{"total": expected}]:
        raise RuntimeError(f"incorrect CLI count result: expected {expected}, got {rows}")
    return measurement


def checked_context(measurement, seed, degree):
    result = measurement["result"]
    if result.get("ok") is not True:
        raise RuntimeError(f"context failed: {result}")
    nodes = result.get("nodes", [])
    ordinals = {node["properties"]["ordinal"] for node in nodes}
    if len(nodes) != degree + 1 or ordinals != set(range(degree + 1)):
        raise RuntimeError(f"context returned incorrect ring neighborhood: {ordinals}")
    edges = result.get("edges", [])
    expected_targets = {node["node_id"] for node in nodes if node["node_id"] != seed}
    if (len(edges) != degree or {edge["target_node_id"] for edge in edges} != expected_targets
            or any(edge["source_node_id"] != seed or edge["edge_type"] != "LINKS" for edge in edges)):
        raise RuntimeError("context returned incorrect seed edges")
    return measurement


def summary(case):
    return {
        "nodes": case["nodes"],
        "edges": case["edges"],
        "initial_database_bytes": case["seed"]["result"]["database_bytes"],
        "after_edits_database_bytes": case["mutate"]["result"]["edits"][-1]["database_bytes"],
        "after_compact_database_bytes": case["compact"]["result"]["after_database_bytes"],
        "open_median_seconds": statistics.median(item["result"]["open_seconds"] for item in case["open"]),
        "cli_count_median_wall_seconds": statistics.median(item["wall_seconds"] for item in case["cli_count"]),
        "session_query_median_seconds": {
            name: statistics.median(case["session_queries"][name]["result"]["query_seconds"])
            for name in QUERIES
        },
        "context_median_wall_seconds": statistics.median(item["wall_seconds"] for item in case["cli_context"]),
        "commit_median_seconds": statistics.median(edit["commit_seconds"] for edit in case["mutate"]["result"]["edits"]),
        "compact_seconds": case["compact"]["result"]["compact_seconds"],
        "phase_peak_rss_bytes": {
            "seed": case["seed"]["peak_rss_bytes"],
            "open": max((item["peak_rss_bytes"] for item in case["open"] if item["peak_rss_bytes"] is not None), default=None),
            **{f"session_{name}": case["session_queries"][name]["peak_rss_bytes"] for name in QUERIES},
            "cli_count": max((item["peak_rss_bytes"] for item in case["cli_count"] if item["peak_rss_bytes"] is not None), default=None),
            "cli_context": max((item["peak_rss_bytes"] for item in case["cli_context"] if item["peak_rss_bytes"] is not None), default=None),
            "mutate": case["mutate"]["peak_rss_bytes"],
            "compact": case["compact"]["peak_rss_bytes"],
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--smoke", action="store_true", help="default to 32,128 nodes, 2 repeats and 2 edits")
    parser.add_argument("--sizes", help="comma-separated node counts; default 100,1000,10000")
    parser.add_argument("--degree", type=nonnegative, default=3, help="outgoing ring edges per node")
    parser.add_argument("--payload-bytes", type=nonnegative, default=256, help="ASCII payload bytes per node")
    parser.add_argument("--repeats", type=positive, help="open, CLI and in-session samples; default 5")
    parser.add_argument("--edits", type=positive, help="single-node transactions per graph; default 5")
    parser.add_argument("--timeout", type=positive, default=180, help="seconds per child process")
    parser.add_argument("--output", required=True, type=Path, help="new JSON report; refuses existing files")
    parser.add_argument("--skip-build", action="store_true", help="use previously built release binaries")
    parser.add_argument("--binary", type=Path, help="CLI path (requires --skip-build)")
    parser.add_argument("--worker", type=Path, help="core_bench path (requires --skip-build)")
    parser.add_argument("--temp-dir", type=Path, help="parent for disposable databases; choose storage explicitly")
    args = parser.parse_args()
    if (args.binary or args.worker) and not args.skip_build:
        parser.error("--binary and --worker require --skip-build")
    try:
        sizes = [int(value) for value in (args.sizes or ("32,128" if args.smoke else "100,1000,10000")).split(",")]
    except ValueError:
        parser.error("--sizes must be comma-separated integers")
    if any(size < 2 or size <= args.degree for size in sizes) or len(set(sizes)) != len(sizes):
        parser.error("sizes must be distinct, at least 2, and larger than degree")
    sizes.sort()
    repeats = args.repeats or (2 if args.smoke else 5)
    edits = args.edits or (2 if args.smoke else 5)
    output = args.output.absolute()
    # Reserve only the requested new artifact. No existing output or database is replaced.
    with output.open("x", encoding="utf-8") as report_file:
        report = {"format_version": 1, "status": "running", "cases": []}

        def save():
            report_file.seek(0)
            json.dump(report, report_file, indent=2)
            report_file.write("\n")
            report_file.truncate()
            report_file.flush()

        try:
            tool_prefix = ["mise", "exec", "--"] if shutil.which("mise") else []
            if not args.skip_build:
                subprocess.run([*tool_prefix, "cargo", "build", "--locked", "--offline", "--release", "--bin", "cupld", "--example", "core_bench"], cwd=ROOT, check=True)
            target_dir = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
            if not target_dir.is_absolute():
                target_dir = ROOT / target_dir
            suffix = ".exe" if os.name == "nt" else ""
            cli = (args.binary or target_dir / "release" / f"cupld{suffix}").resolve()
            worker = (args.worker or target_dir / "release" / "examples" / f"core_bench{suffix}").resolve()
            use_gnu_time = gnu_time_available()
            status = command_text(["git", "status", "--porcelain=v1", "--untracked-files=normal"])
            report.update({
                "started_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "git": {"revision": command_text(["git", "rev-parse", "HEAD"]), "dirty": bool(status) if status is not None else None, "status": status},
                "machine": machine_metadata(),
                "rustc": command_text([*tool_prefix, "rustc", "--version", "--verbose"]),
                "build": {"profile": "release", "skipped": args.skip_build, "rustflags": os.environ.get("RUSTFLAGS")},
                "binaries": {"cli": fingerprint(cli), "worker": fingerprint(worker)},
                "harness": {"driver": fingerprint(Path(__file__).resolve()), "worker_source": fingerprint(ROOT / "examples" / "core_bench.rs")},
                "config": {"sizes": sizes, "degree": args.degree, "payload_bytes": args.payload_bytes, "repeats": repeats, "edits": edits, "timeout_seconds": args.timeout},
                "measurement": {
                    "rss_source": "GNU time %M, KiB converted to bytes" if use_gnu_time else None,
                    "rss_scope": "individual child lifetime including open, operation, validation and shutdown; no baseline subtraction",
                    "cache_policy": "OS caches are not cleared; first and repeated samples retained, no warmup discarded",
                    "binary_identity": "hashes identify executables; --skip-build does not prove they match the recorded source revision",
                },
            })
            save()
            with tempfile.TemporaryDirectory(prefix="cupld-core-bench-", dir=args.temp_dir) as temporary:
                scratch = Path(temporary).resolve()
                environment = os.environ.copy()
                # Keep inherited HOME untouched. The CLI's isolated cwd and config path are disposable.
                environment.update({"CUPLD_NO_INSTALL_PROMPT": "1", "CUPLD_NO_UPGRADE_CHECK": "1",
                                    "XDG_CONFIG_HOME": str(scratch / "config")})
                report["measurement"]["database_temp_parent"] = str(scratch.parent)
                report["measurement"]["filesystem"] = command_text(["df", "-T", str(scratch)], cwd=scratch)
                for nodes in sizes:
                    print(f"Benchmarking {nodes} nodes / {nodes * args.degree} edges", file=sys.stderr)
                    database = scratch / f"graph-{nodes}.cupld"

                    def run_worker(phase, iterations=1, query="count"):
                        command = [str(worker), phase, str(database), str(nodes), str(args.degree),
                                   str(args.payload_bytes), str(iterations), query]
                        return checked_worker(measure(command, scratch, environment, use_gnu_time, args.timeout))

                    case = {"nodes": nodes, "edges": nodes * args.degree}
                    report["cases"].append(case)
                    case["seed"] = run_worker("seed")
                    case["open"] = [run_worker("open") for _ in range(repeats)]
                    case["session_queries"] = {name: run_worker("query", repeats, name) for name in QUERIES}
                    cli_query = [str(cli), "query", "--db", str(database), "--output", "json", "--max-rows", "1",
                                 "MATCH (n:Entity) RETURN count(n) AS total"]
                    case["cli_count"] = [checked_count(measure(cli_query, scratch, environment, use_gnu_time, args.timeout), nodes)
                                         for _ in range(repeats)]
                    seed = case["seed"]["result"]["seed_node_id"]
                    context = [str(cli), "context", "--db", str(database), "--node", str(seed), "--depth", "1",
                               "--direction", "out", "--edge-type", "LINKS", "--max-nodes", str(args.degree + 1),
                               "--max-edges", str(max(1, args.degree)), "--output", "json"]
                    case["cli_context"] = [checked_context(measure(context, scratch, environment, use_gnu_time, args.timeout), seed, args.degree)
                                           for _ in range(repeats)]
                    case["mutate"] = run_worker("mutate", edits)
                    case["verify_after_edits"] = run_worker("verify", edits)
                    case["compact"] = run_worker("compact")
                    case["verify_after_compact"] = run_worker("verify", edits)
                    case["summary"] = summary(case)
                    save()
            report["status"] = "complete"
            report["finished_at_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            save()
            print(f"Verified report: {output}", file=sys.stderr)
        except Exception as error:
            report["status"] = "failed"
            report["error"] = str(error)
            save()
            raise


if __name__ == "__main__":
    main()
