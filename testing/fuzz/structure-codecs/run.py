#!/usr/bin/env python3
"""Run bounded ASan/libFuzzer smoke campaigns and retain reproducibility metadata."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
TARGETS = {"cif_document": 262144, "fcz_decode": 65536, "selected_range": 4096}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def corpus_state(target):
    paths = sorted((HERE / "corpus" / target).glob("*"))
    hashes = sorted(digest(path.read_bytes()) for path in paths if path.is_file())
    return {"count": len(hashes), "sha256": digest("\n".join(hashes).encode())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--seed", type=int, default=20260920)
    parser.add_argument("--target", choices=["all", *TARGETS], default="all")
    parser.add_argument("--toolchain", default="nightly")
    parser.add_argument("--output", type=Path, default=HERE / "logs")
    args = parser.parse_args()
    if args.seconds < 1:
        parser.error("seconds must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    local_tools = ROOT / "target/structure-codecs-tools/bin"
    if local_tools.exists():
        env["PATH"] = str(local_tools) + os.pathsep + env["PATH"]
    cargo = ["cargo", f"+{args.toolchain}"]
    metadata = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "toolchain": subprocess.check_output(
            ["rustc", f"+{args.toolchain}", "--version"], text=True
        ).strip(),
        "cargo_fuzz": subprocess.check_output(
            [*cargo, "fuzz", "--version"], env=env, text=True
        ).strip(),
        "revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "diff_sha256": digest(
            subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT)
        ),
        "harness_sha256": digest((HERE / "src/lib.rs").read_bytes()),
        "runs": [],
    }
    failed = False
    for target in TARGETS if args.target == "all" else [args.target]:
        before = corpus_state(target)
        log = args.output / f"{target}.log"
        command = [
            *cargo,
            "fuzz",
            "run",
            "--fuzz-dir",
            str(HERE),
            target,
            "--",
            f"-max_total_time={args.seconds}",
            f"-max_len={TARGETS[target]}",
            f"-seed={args.seed}",
            "-timeout=10",
            "-rss_limit_mb=2048",
            "-print_final_stats=1",
        ]
        started = time.monotonic()
        cpu_before = os.times()
        with log.open("w") as stream:
            result = subprocess.run(
                command,
                cwd=ROOT,
                env=env,
                stdout=stream,
                stderr=subprocess.STDOUT,
                check=False,
            )
        cpu_after = os.times()
        text = log.read_text()
        executions = re.search(r"stat::number_of_executed_units:\s*(\d+)", text)
        record = {
            "target": target,
            "command": command,
            "seed_corpus": before,
            "exit_code": result.returncode,
            "wall_seconds": time.monotonic() - started,
            "child_cpu_seconds": cpu_after.children_user
            + cpu_after.children_system
            - cpu_before.children_user
            - cpu_before.children_system,
            "executions": int(executions[1]) if executions else None,
            "final_corpus": corpus_state(target),
            "log_sha256": digest(log.read_bytes()),
        }
        metadata["runs"].append(record)
        (args.output / "results.json").write_text(json.dumps(metadata, indent=2) + "\n")
        print(
            f"{target}: exit={result.returncode}, executions={record['executions']}, log={log}",
            flush=True,
        )
        if result.returncode:
            print(text[-6000:], flush=True)
            failed = True
            break
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
