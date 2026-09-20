#!/usr/bin/env python3
"""Run native-vs-Rust array checks in an isolated Linux container (no wheel claim)."""

import argparse
import json
from pathlib import Path
import shlex
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def run(command, **kwargs):
    return subprocess.run(command, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arch", choices=["arm64", "amd64"], default="arm64")
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="also run the full paired release benchmark",
    )
    parser.add_argument(
        "--datasets", help="restrict the benchmark to diagnostic datasets"
    )
    parser.add_argument("--stages", default="raw,decode,arrow,pipeline,query")
    parser.add_argument("--seconds", type=float, default=0.2)
    parser.add_argument(
        "--report", default="benchmark.json", help="benchmark report filename"
    )
    args = parser.parse_args()
    if Path(args.report).name != args.report or args.seconds <= 0:
        parser.error("report must be a filename and seconds must be positive")
    image = f"polars-bio-codec-check:rust1.91-{args.arch}"
    output = ROOT / f"target/codec-linux-{args.arch}"
    output.mkdir(parents=True, exist_ok=True)
    common = subprocess.check_output(
        ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
        cwd=ROOT,
        text=True,
    ).strip()
    run(
        [
            "docker",
            "build",
            "--platform",
            f"linux/{args.arch}",
            "-t",
            image,
            "-f",
            str(HERE / "Dockerfile"),
            str(HERE),
        ]
    )
    image_info = subprocess.check_output(
        ["docker", "image", "inspect", image], text=True
    )
    (output / "image.json").write_text(image_info)
    # Source/Git metadata are read-only; generated build/reference output goes
    # exclusively into the dedicated host directory mounted over target/.
    command = "\n".join(
        [
            "set -e",
            'git config --global --add safe.directory "$PWD"',
            "cargo build --locked --manifest-path testing/fuzz/structure-codecs/Cargo.toml --release --no-default-features --features probe --bin codec_probe",
            f"python3 testing/oracles/structure-codecs/compare_candidate.py --candidate target/probe-build/release/codec_probe --output target/comparison.json --label linux-{args.arch}-container",
        ]
    )
    if args.benchmark:
        benchmark = [
            "python3",
            "testing/benchmarks/structure-codecs/run.py",
            "--output",
            f"target/{args.report}",
            "--samples",
            "9",
            "--seconds",
            str(args.seconds),
            "--stages",
            args.stages,
        ]
        if args.datasets:
            benchmark.extend(["--datasets", args.datasets])
        command += "\n" + shlex.join(benchmark)
    run(
        [
            "docker",
            "run",
            "--rm",
            "--platform",
            f"linux/{args.arch}",
            "--mount",
            f"type=bind,src={ROOT},dst={ROOT},readonly",
            "--mount",
            f"type=bind,src={common},dst={common},readonly",
            "--mount",
            f"type=bind,src={output},dst={ROOT / 'target'}",
            "--workdir",
            str(ROOT),
            "--env",
            f"CARGO_TARGET_DIR={ROOT / 'target/probe-build'}",
            "--env",
            "CARGO_BUILD_JOBS=4",
            image,
            "sh",
            "-c",
            command,
        ]
    )
    print(json.dumps({"output": str(output), "image": image}))


if __name__ == "__main__":
    main()
