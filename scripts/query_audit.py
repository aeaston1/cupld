#!/usr/bin/env python3
"""Linux baseline runner using Python's stdlib and GNU time, never production DBs.

Build first: mise exec -- cargo build --locked --offline --release --bin cupld \
    --example query_audit_bench --target-dir target/core-audit
Run: python3 scripts/query_audit.py --output /tmp/cupld-query-audit.json
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, default=Path("target/core-audit/release"))
    parser.add_argument("--sizes", type=int, nargs="+", default=[1000, 10000, 50000])
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--edits", type=int, default=10)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.sizes + [args.samples, args.edits]) <= 0:
        parser.error("sizes, samples and edits must be positive")
    binary = (args.bin_dir / "cupld").resolve()
    driver = (args.bin_dir / "examples/query_audit_bench").resolve()
    env = dict(os.environ, CUPLD_NO_INSTALL_PROMPT="1", CUPLD_NO_UPGRADE_CHECK="1")
    report = {
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "platform": platform.platform(),
        "rustc": subprocess.check_output(["mise", "exec", "--", "rustc", "-V"], text=True).strip(),
        "binary_bytes": binary.stat().st_size,
        "profile": "release (not dist; not stripped)",
        "samples": args.samples, "edits": args.edits,
        "fixture": "N Item nodes, 256 ASCII body bytes and integer key; N-1 NEXT chain edges; equality index on key",
        "cache_policy": "warm OS cache; no cache eviction; fresh process for CLI, one warmup for open-session queries",
        "rss_scope": "GNU time maximum resident KiB per child process including open/setup; not per-operation allocation",
        "graphs": [],
    }
    with tempfile.TemporaryDirectory(prefix="cupld-core-audit-") as temporary:
        root = Path(temporary)

        def run(command, input_text=None):
            rss_file = root / "rss.txt"
            start = time.perf_counter()
            result = subprocess.run(
                ["/usr/bin/time", "-f", "%M", "-o", str(rss_file), *map(str, command)],
                cwd=root, env=env, input=input_text, text=True, capture_output=True,
                timeout=180,
            )
            elapsed = (time.perf_counter() - start) * 1000
            if result.returncode:
                raise RuntimeError(f"{command}: {result.returncode}: {result.stderr}")
            return {"wall_ms": elapsed, "peak_rss_kib": int(rss_file.read_text().strip()),
                    "stdout": result.stdout}

        def samples(command):
            values = [run(command) for _ in range(args.samples)]
            return {"median_wall_ms": statistics.median(v["wall_ms"] for v in values),
                    "max_peak_rss_kib": max(v["peak_rss_kib"] for v in values),
                    "wall_ms_samples": [v["wall_ms"] for v in values],
                    "peak_rss_kib_samples": [v["peak_rss_kib"] for v in values]}

        def measured_driver(mode, db, count, *extra):
            result = run([driver, mode, db, count, *extra])
            result["metrics"] = [json.loads(line) for line in result.pop("stdout").splitlines()]
            return result

        report["cli_version_startup"] = samples([binary, "--version"])
        for size in args.sizes:
            print(f"Measuring {size} nodes", flush=True)
            db = root / f"graph-{size}.cupld"
            run([driver, "seed", db, size])
            row = {"nodes": size, "edges": size - 1, "initial_db_bytes": db.stat().st_size,
                   "queries": {}}
            queries = {
                "constant": "RETURN 1 AS value",
                "indexed_lookup": f"MATCH (n:Item {{key: {size // 2}}}) RETURN n.key",
                "scan_limit_10": "MATCH (n:Item) RETURN n.key LIMIT 10",
                "scan_output_cap_10": "MATCH (n:Item) RETURN n.key",
                "one_hop": f"MATCH (n:Item {{key: {size // 2}}})-[:NEXT]->(m) RETURN m.key",
            }
            for name, query in queries.items():
                row["queries"][name] = {
                    "query": query,
                    "cli": samples([binary, "query", "--db", db, "--output", "json", "--max-rows", "10", query]),
                    "open_session": measured_driver("read", db, args.samples, query),
                }
            row["edits"] = measured_driver("edit", db, args.edits)
            row["after_edits_db_bytes"] = db.stat().st_size
            row["cli_after_edits"] = samples([binary, "query", "--db", db, "--output", "json", "RETURN 1"])
            row["compaction"] = measured_driver("compact", db, 1)
            row["compacted_db_bytes"] = db.stat().st_size
            # Reopening after compaction must still see the final edit.
            out = run([binary, "query", "--db", db, "--output", "json",
                       "MATCH (n:Item {key: 0}) RETURN n.edit AS edit"])
            assert json.loads(out["stdout"])["results"][0]["rows"] == [{"edit": args.edits - 1}]
            report["graphs"].append(row)

        # Generic workflow: no markdown root, installer, or MCP involved.
        db = root / "workflow.cupld"
        run([binary, db], "")  # Current creation workaround; no structured receipt.

        def query(text, *extra):
            result = run([binary, "query", "--db", db, "--output", "json", *extra, text])
            envelope = json.loads(result["stdout"])
            assert envelope["ok"] is True
            return envelope

        query("CREATE (a:Person {name: 'Ada', score: 1})-[:KNOWS]->(b:Person {name: 'Grace', score: 2})")
        schema = query("SHOW SCHEMA")
        assert schema["results"][0]["row_count"] >= 2
        query("MATCH (n:Person {name: $name}) SET n.score = 3", "--params-json", '{"name":"Ada"}')
        rows = query("MATCH (n:Person) RETURN n.name AS name, n.score AS score ORDER BY n.name")["results"][0]["rows"]
        assert rows == [{"name": "Ada", "score": 3}, {"name": "Grace", "score": 2}]
        seed = query("MATCH (n:Person {name: 'Ada'}) RETURN id(n) AS id")["results"][0]["rows"][0]["id"]
        context = json.loads(run([binary, "context", "--db", db, "--node", seed, "--depth", "1", "--output", "json"])["stdout"])
        assert context["ok"] and len(context["nodes"]) == 2 and len(context["edges"]) == 1
        export = run([binary, "query", "--db", db, "--output", "ndjson", "MATCH (n:Person) RETURN n.name ORDER BY n.name"])
        records = [json.loads(line) for line in export["stdout"].splitlines()]
        assert sum(record["kind"] == "query_row" for record in records) == 2
        report["generic_workflow"] = {"passed": True, "creation": "REPL with EOF", "schema": schema,
                                      "updated_rows": rows, "context_nodes": 2, "context_edges": 1,
                                      "export": "query rows only; not lossless graph export"}

        # Record correctness observations without enshrining known defects as tests.
        report["semantic_probes"] = []
        for name, text, expected in [
            ("aggregate_limit", "MATCH (n:Person) RETURN count(n) AS total LIMIT 1", [{"total": 2}]),
            ("projected_alias_order", "MATCH (n:Person) RETURN n.score AS score ORDER BY score", [{"score": 2}, {"score": 3}]),
        ]:
            actual = query(text)["results"][0]["rows"]
            report["semantic_probes"].append({"name": name, "query": text, "expected": expected,
                                               "actual": actual, "matches_expected": actual == expected})
        report["cli_probes"] = []
        for command, input_text in [
            (["query", "--db", str(db), "--output", "json", "RETURN ("], None),
            (["query", "--output", "json", "RETURN 1"], None),
            (["schema", "--db", str(db), "--output", "json"], None),
            (["check", "--db", str(db), "--output", "json"], None),
            ([str(db)], "RETURN (\n"),
        ]:
            result = subprocess.run([str(binary), *command], cwd=root, env=env, input=input_text,
                                    text=True, capture_output=True, timeout=30)
            report["cli_probes"].append({"args": [s.replace(str(db), "<db>") for s in command],
                                         "stdin": input_text, "exit_code": result.returncode,
                                         "stdout": result.stdout, "stderr": result.stderr})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
