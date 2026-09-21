#!/usr/bin/env python3
"""Measure sustained fuzz CPU separately from builds; validate all campaign shards."""

import argparse
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import time

from run import HERE, ROOT, TARGETS, corpus_state, digest

DECODERS = ("cif_document", "fcz_decode")


def write(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def sources():
    files = [
        HERE / "Cargo.toml",
        HERE / "Cargo.lock",
        HERE / "campaign.py",
        HERE / "run.py",
        HERE / "seed.py",
        ROOT / "datafusion/bio-format-structure/src/options.rs",
        ROOT / "datafusion/bio-format-structure/src/model.rs",
        ROOT / "datafusion/bio-format-foldcomp/src/index.rs",
    ]
    for directory in [
        HERE / "src",
        HERE / "fuzz_targets",
        ROOT / "datafusion/bio-format-structure/src/cif",
        ROOT / "datafusion/bio-format-foldcomp/src/fcz",
    ]:
        files.extend(directory.glob("*.rs"))
    return {str(p.relative_to(ROOT)): digest(p.read_bytes()) for p in sorted(files)}


def campaign(args):
    if os.name != "posix":
        raise RuntimeError("CPU accounting requires a Unix host")
    compiler = subprocess.check_output(
        ["rustc", f"+{args.toolchain}", "-vV"], text=True
    )
    host = re.search(r"^host: (.+)$", compiler, re.MULTILINE)[1]
    binary = (args.binary or HERE / "target" / host / "release" / args.target).resolve()
    if not binary.is_file():
        raise RuntimeError(f"build the cargo-fuzz target first: {binary}")
    args.output.mkdir(parents=True, exist_ok=True)
    artifacts = args.output / "artifacts"
    artifacts.mkdir(exist_ok=True)
    corpus = HERE / "corpus" / args.target
    if not corpus.is_dir() or not any(corpus.iterdir()):
        raise RuntimeError("run seed.py before starting the campaign")
    revision = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    manifest = sources()
    metadata = {
        "revision": revision,
        "platform": platform.platform(),
        "rustc": compiler,
        "target": args.target,
        "shard": args.shard,
        "binary_sha256": digest(binary.read_bytes()),
        "sources": manifest,
        "sources_sha256": digest(json.dumps(manifest, sort_keys=True).encode()),
        "required_cpu_seconds": args.cpu_seconds,
        "cpu_seconds": 0.0,
        "executions": 0,
        "complete": False,
        "runs": [],
    }
    env = os.environ.copy()
    env["ASAN_OPTIONS"] = ":".join(
        filter(None, [env.get("ASAN_OPTIONS"), "detect_odr_violation=0"])
    )
    path = args.output / "campaign.json"
    write(path, metadata)
    while metadata["cpu_seconds"] < args.cpu_seconds:
        index = len(metadata["runs"])
        remaining = args.cpu_seconds - metadata["cpu_seconds"]
        seconds = max(1, math.ceil(min(args.chunk_seconds, remaining * 1.2)))
        command = [
            str(binary),
            str(corpus),
            f"-artifact_prefix={artifacts}{os.sep}",
            f"-max_total_time={seconds}",
            f"-max_len={TARGETS[args.target]}",
            f"-seed={args.seed + args.shard * 10000 + index}",
            "-timeout=10",
            "-rss_limit_mb=2048",
            "-print_final_stats=1",
        ]
        initial = corpus_state(args.target)
        log = args.output / f"segment-{index:04}.log"
        # Only this already-built fuzzer is a child between these samples.
        # Compiler/Cargo, source hashing and corpus bookkeeping are excluded.
        with log.open("w") as stream:
            cpu_before = os.times()
            started = time.monotonic()
            result = subprocess.run(
                command, env=env, stdout=stream, stderr=subprocess.STDOUT
            )
            wall = time.monotonic() - started
            cpu_after = os.times()
        cpu = (
            cpu_after.children_user
            + cpu_after.children_system
            - cpu_before.children_user
            - cpu_before.children_system
        )
        text = log.read_text()
        executions = re.search(r"stat::number_of_executed_units:\s*(\d+)", text)
        count = int(executions[1]) if executions else 0
        metadata["runs"].append(
            {
                "command": command,
                "cpu_seconds": cpu,
                "wall_seconds": wall,
                "exit_code": result.returncode,
                "executions": count,
                "initial_corpus": initial,
                "final_corpus": corpus_state(args.target),
                "log_sha256": digest(log.read_bytes()),
            }
        )
        metadata["cpu_seconds"] += cpu
        metadata["executions"] += count
        metadata["complete"] = (
            result.returncode == 0
            and count > 0
            and metadata["cpu_seconds"] >= args.cpu_seconds
        )
        write(path, metadata)
        print(
            f"{args.target}/{args.shard}: {metadata['cpu_seconds']:.2f} CPU seconds, {metadata['executions']} executions",
            flush=True,
        )
        if result.returncode or not count:
            raise RuntimeError(f"fuzzer failed or omitted statistics; inspect {log}")


def summarize(args):
    records = [
        json.loads(p.read_text()) for p in sorted(args.directory.rglob("campaign.json"))
    ]
    expected = {(target, shard) for target in DECODERS for shard in range(args.shards)}
    found = {(r["target"], r["shard"]) for r in records}
    if found != expected or len(records) != len(expected):
        raise RuntimeError(
            f"missing, duplicate or unexpected shards: {found ^ expected}"
        )
    if (
        len({r["sources_sha256"] for r in records}) != 1
        or len({r["rustc"] for r in records}) != 1
    ):
        raise RuntimeError("shards used different source/toolchain inputs")
    for record in records:
        if (
            record["revision"] != args.revision
            or not record["complete"]
            or record["cpu_seconds"] < args.cpu_seconds
            or not record["runs"]
            or any(r["exit_code"] or not r["executions"] for r in record["runs"])
        ):
            raise RuntimeError(
                f"unmet campaign budget or failed shard: {record['target']}/{record['shard']}"
            )
    result = {
        "revision": args.revision,
        "sources_sha256": records[0]["sources_sha256"],
        "shards_per_target": args.shards,
        "required_cpu_seconds_per_shard": args.cpu_seconds,
        "targets": {
            target: {
                "cpu_seconds": sum(
                    r["cpu_seconds"] for r in records if r["target"] == target
                ),
                "executions": sum(
                    r["executions"] for r in records if r["target"] == target
                ),
            }
            for target in DECODERS
        },
    }
    write(args.output, result)
    print(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    run = modes.add_parser("run")
    run.add_argument("--target", choices=DECODERS, required=True)
    run.add_argument("--shard", type=int, default=0)
    run.add_argument("--seed", type=int, default=20260920)
    run.add_argument("--toolchain", default="nightly-2026-02-02")
    run.add_argument("--binary", type=Path)
    run.add_argument("--chunk-seconds", type=int, default=900)
    run.add_argument("--cpu-seconds", type=float, required=True)
    run.add_argument("--output", type=Path, required=True)
    check = modes.add_parser("summarize")
    check.add_argument("--directory", type=Path, required=True)
    check.add_argument("--shards", type=int, required=True)
    check.add_argument("--cpu-seconds", type=float, required=True)
    check.add_argument("--revision", required=True)
    check.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not math.isfinite(args.cpu_seconds) or args.cpu_seconds <= 0:
        parser.error("cpu-seconds must be finite and positive")
    if args.mode == "run":
        if args.shard < 0 or args.chunk_seconds < 1:
            parser.error("shard must be nonnegative and chunk-seconds positive")
        campaign(args)
    else:
        if args.shards < 1:
            parser.error("shards must be positive")
        summarize(args)


if __name__ == "__main__":
    main()
