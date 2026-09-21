#!/usr/bin/env python3
"""Paired release codec/Arrow/query measurements in isolated worker processes."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
DATA = ROOT / "testing/data/structure"
STRESS = ROOT / "testing/oracles/structure-codecs/fcz-stress"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def build(env):
    command = [
        "cargo",
        "test",
        "--release",
        "--lib",
        "--no-run",
        "--message-format=json",
        "-p",
        "datafusion-bio-format-structure",
        "-p",
        "datafusion-bio-format-foldcomp",
    ]
    result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    binaries = {}
    for line in result.stdout.splitlines():
        item = json.loads(line)
        if item.get("executable"):
            for kind in ["structure", "foldcomp"]:
                if item["target"]["name"] == f"datafusion_bio_format_{kind}":
                    binaries[kind] = item["executable"]
    if len(binaries) != 2:
        raise RuntimeError(
            f"missing benchmark binaries: {result.stdout}\n{result.stderr}"
        )
    return binaries


def datasets(directory):
    original = (DATA / "1ubq.cif").read_bytes()
    if b"data_1UBQ" not in original:
        raise RuntimeError("unexpected base CIF block name")
    expanded = directory / "1ubq_64_blocks.cif"
    expanded.write_bytes(
        b"\n".join(
            original.replace(b"data_1UBQ", f"data_copy_{i}".encode(), 1)
            for i in range(64)
        )
    )
    result = {
        "cif_1ubq": ("structure", [{"path": str(DATA / "1ubq.cif")}]),
        "cif_64_blocks": ("structure", [{"path": str(expanded)}]),
        "fcz_1ubq": ("foldcomp", [{"path": str(DATA / "1ubq.fcz")}]),
        "fcz_long_mixed": ("foldcomp", [{"path": str(STRESS / "long_mixed.fcz")}]),
        "fcz_long_segment": ("foldcomp", [{"path": str(STRESS / "long_segment.fcz")}]),
    }
    rows = [
        list(map(int, line.split()))
        for line in (DATA / "example_db.index").read_text().splitlines()
    ]
    for name, keys in [
        ("fcz_subset_2", {0, 7}),
        ("fcz_database_24", {row[0] for row in rows}),
    ]:
        result[name] = (
            "foldcomp",
            [
                {"path": str(DATA / "example_db"), "offset": offset, "length": size - 1}
                for key, offset, size in rows
                if key in keys
            ],
        )
    return result


def measure(binary, config, directory, env):
    path = directory / "config.json"
    path.write_text(json.dumps(config))
    env = {**env, "BIO_CODEC_BENCH_CONFIG": str(path)}
    rss_path = directory / "rss.txt"
    if platform.system() == "Darwin":
        prefix = ["/usr/bin/time", "-l", "-o", str(rss_path)]
    elif platform.system() == "Linux":
        prefix = ["/usr/bin/time", "-v", "-o", str(rss_path)]
    else:
        raise RuntimeError(
            "RSS runner currently supports macOS/Linux; Windows remains a separate gate"
        )
    command = [
        *prefix,
        binary,
        "--ignored",
        "--exact",
        "migration_benchmarks::worker",
        "--nocapture",
        "--test-threads=1",
    ]
    process = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True)
    if process.returncode:
        raise RuntimeError(process.stdout + process.stderr)
    record = json.loads(
        next(
            line.split("CODEC_BENCH ", 1)[1]
            for line in process.stdout.splitlines()
            if "CODEC_BENCH " in line
        )
    )
    text = rss_path.read_text()
    if platform.system() == "Darwin":
        record["peak_rss_bytes"] = int(
            re.search(r"(\d+)\s+maximum resident set size", text)[1]
        )
    else:
        record["peak_rss_bytes"] = (
            int(re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", text)[1])
            * 1024
        )
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=9)
    parser.add_argument(
        "--seconds",
        type=float,
        default=0.2,
        help="native calibration target per timed sample",
    )
    parser.add_argument("--datasets", help="comma-separated subset")
    parser.add_argument("--stages", default="raw,decode,arrow,pipeline,query")
    parser.add_argument(
        "--inspect-layout",
        action="store_true",
        help="diagnostic: inspect decoded allocations before timing",
    )
    parser.add_argument("--structure-bin", type=Path)
    parser.add_argument("--foldcomp-bin", type=Path)
    args = parser.parse_args()
    if args.samples < 3 or args.seconds <= 0:
        parser.error("at least three samples and positive seconds are required")
    if bool(args.structure_bin) != bool(args.foldcomp_bin):
        parser.error("supply both binaries or neither")
    env = {**os.environ, "CARGO_PROFILE_RELEASE_DEBUG": "0"}
    binaries = (
        {
            "structure": str(args.structure_bin.resolve()),
            "foldcomp": str(args.foldcomp_bin.resolve()),
        }
        if args.structure_bin
        else build(env)
    )
    metadata = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "rustc": subprocess.check_output(
            ["rustc", "--version"], cwd=ROOT, text=True
        ).strip(),
        "revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "diff_sha256": digest(
            subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT)
        ),
        "cargo_lock_sha256": digest((ROOT / "Cargo.lock").read_bytes()),
        "binaries": {
            kind: {"sha256": digest(Path(path).read_bytes()), "path": path}
            for kind, path in binaries.items()
        },
        "samples": args.samples,
        "calibration_seconds": args.seconds,
        "method": "alternating paired fresh processes; 2 untimed warmups; preloaded input; no cache flush; query uses shared EntrySource harness; complete materialization",
        "threshold_ratio": 1.10,
        "noise_limit_mad_ratio": 0.05,
        "diagnostic_layout_inspection": args.inspect_layout,
        "inputs": {},
        "results": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="codec-benchmark-") as temp:
        directory = Path(temp)
        for name, (kind, inputs) in datasets(directory).items():
            if args.datasets and name not in args.datasets.split(","):
                continue
            metadata["inputs"][name] = []
            for item in inputs:
                data = Path(item["path"]).read_bytes()
                start = item.get("offset", 0)
                payload = (
                    data[start : start + item["length"]] if "length" in item else data
                )
                metadata["inputs"][name].append(
                    {
                        "file": Path(item["path"]).name,
                        "offset": start,
                        "length": len(payload),
                        "sha256": digest(payload),
                    }
                )
            for stage in args.stages.split(","):
                if stage == "raw" and kind != "structure":
                    continue
                levels = (
                    ["atom", "residue"]
                    if stage in ["arrow", "arrow_clone", "pipeline", "query"]
                    else ["residue"]
                    if stage == "residue"
                    else ["atom"]
                )
                for level in levels:
                    for workers in [1, 2, 4, 8] if stage == "query" else [1]:
                        config = {
                            "stage": stage,
                            "level": level,
                            "workers": workers,
                            "copies": 8
                            if stage == "query" and name == "cif_1ubq"
                            else 1,
                            "iterations": 1,
                            "inputs": inputs,
                            "backend": "native",
                            "inspect_layout": args.inspect_layout,
                        }
                        trial = measure(binaries[kind], config, directory, env)
                        config["iterations"] = max(
                            1, min(10000, math.ceil(args.seconds / trial["seconds"]))
                        )
                        samples = {"native": [], "rust": []}
                        for sample in range(args.samples):
                            order = (
                                ["native", "rust"]
                                if sample % 2 == 0
                                else ["rust", "native"]
                            )
                            for backend in order:
                                samples[backend].append(
                                    measure(
                                        binaries[kind],
                                        {**config, "backend": backend},
                                        directory,
                                        env,
                                    )
                                )
                            for field in [
                                "rows",
                                "decode_calls",
                                "input_bytes",
                                "iterations",
                            ]:
                                if (
                                    samples["native"][-1][field]
                                    != samples["rust"][-1][field]
                                ):
                                    raise RuntimeError(
                                        f"unequal work: {name}/{stage}/{field}"
                                    )
                            if args.inspect_layout and (
                                samples["native"][-1]["allocation_layout"][
                                    "normalized_debug_hash"
                                ]
                                != samples["rust"][-1]["allocation_layout"][
                                    "normalized_debug_hash"
                                ]
                            ):
                                raise RuntimeError(
                                    f"unequal normalized values: {name}/{stage}"
                                )
                        medians = {}
                        for backend in samples:
                            times = [
                                v["seconds"] / v["iterations"] for v in samples[backend]
                            ]
                            median = statistics.median(times)
                            rss = [v["peak_rss_bytes"] for v in samples[backend]]
                            rss_median = statistics.median(rss)
                            medians[backend] = {
                                "seconds": median,
                                "mad_ratio": statistics.median(
                                    abs(v - median) for v in times
                                )
                                / median,
                                "peak_rss_bytes": rss_median,
                                "rss_mad_ratio": statistics.median(
                                    abs(v - rss_median) for v in rss
                                )
                                / rss_median,
                                "first_batch_seconds": statistics.median(
                                    v["first_batch_seconds"] / v["iterations"]
                                    for v in samples[backend]
                                ),
                            }
                        time_ratio = (
                            medians["rust"]["seconds"] / medians["native"]["seconds"]
                        )
                        rss_ratio = (
                            medians["rust"]["peak_rss_bytes"]
                            / medians["native"]["peak_rss_bytes"]
                        )
                        noisy = any(
                            (v["mad_ratio"] > 0.05 or v["rss_mad_ratio"] > 0.05)
                            for v in medians.values()
                        )
                        status = (
                            "noisy"
                            if noisy
                            else "regression"
                            if max(time_ratio, rss_ratio) > 1.10
                            else "within_local_budget"
                        )
                        record = {
                            "dataset": name,
                            "stage": stage,
                            "level": level,
                            "workers": workers,
                            "iterations": config["iterations"],
                            "copies": config["copies"],
                            "samples": samples,
                            "medians": medians,
                            "rust_native_time_ratio": time_ratio,
                            "rust_native_rss_ratio": rss_ratio,
                            "status": status,
                        }
                        metadata["results"].append(record)
                        args.output.write_text(json.dumps(metadata, indent=2) + "\n")
                        print(
                            f"{name}/{stage}/{level}/{workers}: time={time_ratio:.3f} RSS={rss_ratio:.3f} {status}",
                            flush=True,
                        )


if __name__ == "__main__":
    main()
