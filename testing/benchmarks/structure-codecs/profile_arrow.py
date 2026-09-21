#!/usr/bin/env python3
"""Sample common residue/Arrow work; separate from uninstrumented acceptance data."""

import argparse
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

import run as benchmark


def perf_binary():
    candidates = [shutil.which("perf"), *Path("/usr/lib/linux-tools").glob("*/perf")]
    for path in candidates:
        if (
            path
            and subprocess.run([str(path), "--version"], capture_output=True).returncode
            == 0
        ):
            return str(path)
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=float, default=30)
    args = parser.parse_args()
    if not math.isfinite(args.seconds) or args.seconds <= 0:
        parser.error("seconds must be finite and positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    perf = perf_binary()
    report = {
        "scope": "instrumented diagnostic only; no acceptance ratios",
        "perf": perf,
        "results": [],
    }
    if not perf:
        report["unavailable"] = "no working perf executable"
        (output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
        return
    env = {**os.environ, "CARGO_PROFILE_RELEASE_DEBUG": "0"}
    binaries = benchmark.build_pair(env)
    report["binary_sha256"] = {
        backend: benchmark.digest(Path(values["foldcomp"]).read_bytes())
        for backend, values in binaries.items()
    }
    report["revision"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=benchmark.ROOT, text=True
    ).strip()
    with tempfile.TemporaryDirectory(prefix="codec-profile-") as temp:
        directory = Path(temp)
        inputs = benchmark.datasets(directory)["fcz_long_mixed"][1]
        for stage in ["arrow", "residue"]:
            config = {
                "stage": stage,
                "level": "residue",
                "workers": 1,
                "copies": 1,
                "iterations": 100,
                "inputs": inputs,
                "backend": "native",
            }
            trial = benchmark.measure(
                binaries["native"]["foldcomp"], config, directory, env
            )
            config["iterations"] = max(
                1, min(100000, math.ceil(args.seconds * 100 / trial["seconds"]))
            )
            for backend in ["native", "rust"]:
                binary = binaries[backend]["foldcomp"]
                name = f"{stage}-{backend}"
                config_path = output / f"{name}.json"
                config_path.write_text(json.dumps({**config, "backend": backend}))
                data_path = output / f"{name}.data"
                command = [
                    perf,
                    "record",
                    "-F",
                    "199",
                    "-e",
                    "cpu-clock",
                    "-g",
                    "--call-graph",
                    "dwarf,8192",
                    "-o",
                    str(data_path),
                    "--",
                    binary,
                    "--ignored",
                    "--exact",
                    "migration_benchmarks::worker",
                    "--nocapture",
                    "--test-threads=1",
                ]
                process = subprocess.run(
                    command,
                    cwd=benchmark.ROOT,
                    env={**env, "BIO_CODEC_BENCH_CONFIG": str(config_path)},
                    capture_output=True,
                    text=True,
                )
                (output / f"{name}.log").write_text(process.stdout + process.stderr)
                item = {
                    "name": name,
                    "command": command,
                    "returncode": process.returncode,
                }
                # Profiler permission/support failures are retained explicitly, never
                # interpreted as a successful profile or a performance acceptance pass.
                if process.returncode == 0:
                    profile = subprocess.run(
                        [
                            perf,
                            "report",
                            "--stdio",
                            "--no-children",
                            "--percent-limit",
                            "0.5",
                            "-i",
                            str(data_path),
                        ],
                        capture_output=True,
                        text=True,
                    )
                    (output / f"{name}.txt").write_text(profile.stdout + profile.stderr)
                    item["report_returncode"] = profile.returncode
                report["results"].append(item)
                (output / "summary.json").write_text(
                    json.dumps(report, indent=2) + "\n"
                )
                print(f"{name}: perf exit {process.returncode}", flush=True)


if __name__ == "__main__":
    main()
