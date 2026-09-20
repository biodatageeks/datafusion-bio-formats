#!/usr/bin/env python3
"""Compare installed native/Rust wheels through real local storage and Python."""

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import resource
import statistics
import subprocess
import sys
import tempfile
import time
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
DATA = ROOT / "testing/data/structure"
STRESS = ROOT / "testing/oracles/structure-codecs/fcz-stress"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def worker(config):
    import polars_bio as pb
    import polars_bio.polars_bio as extension

    package = Path(pb.__file__).resolve()
    if "site-packages" not in package.parts:
        raise RuntimeError(f"expected an installed wheel, imported {package}")
    if config.get("inspect"):
        return {
            "package": str(package),
            "extension": str(Path(extension.__file__).resolve()),
            "extension_sha256": digest(Path(extension.__file__).read_bytes()),
            "python": sys.version,
            "packages": dict(
                sorted(
                    (dist.metadata["Name"], dist.version)
                    for dist in importlib.metadata.distributions()
                )
            ),
        }
    workers = config["workers"]
    pb.set_option("datafusion.execution.target_partitions", str(workers))
    reader = getattr(pb, "read_" + config["format"])

    def collect():
        return reader(config["path"], level=config["level"], **config["options"])

    start = time.perf_counter()
    frame = collect()
    first_collection = time.perf_counter() - start
    expected_rows = frame.height
    del frame
    # A second untimed collection warms code/allocator and local filesystem caches.
    frame = collect()
    del frame
    rows = 0
    start = time.perf_counter()
    for iteration in range(config["iterations"]):
        frame = collect()
        if frame.height != expected_rows:
            raise RuntimeError("row count changed across repeated collections")
        rows += frame.height
        if iteration + 1 < config["iterations"]:
            del frame
    seconds = time.perf_counter() - start
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if platform.system() == "Linux":
        peak_rss *= 1024
    elif platform.system() != "Darwin":
        raise RuntimeError("RSS units are only validated on Linux/macOS")
    # Validate every column after timing/RSS capture. Identical installed Polars
    # versions make seeded row hashes comparable; sorting removes query ordering.
    row_hashes = frame.hash_rows(seed=20260920).sort().to_numpy().tobytes()
    return {
        "seconds": seconds,
        "iterations": config["iterations"],
        "rows": rows,
        "rows_per_collection": expected_rows,
        "schema": {name: str(dtype) for name, dtype in frame.schema.items()},
        "all_columns_sha256": digest(row_hashes),
        "peak_rss_bytes": peak_rss,
        "first_collection_seconds": first_collection,
        "target_partitions": pb.get_option("datafusion.execution.target_partitions"),
        "package": str(package),
    }


def measure(python, config, directory, workers=1):
    config_path = directory / "config.json"
    config_path.write_text(json.dumps(config))
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    env.update(
        POLARS_MAX_THREADS=str(workers),
        RAYON_NUM_THREADS=str(workers),
        OMP_NUM_THREADS="1",
        OPENBLAS_NUM_THREADS="1",
    )
    result = subprocess.run(
        [
            str(python),
            "-I",
            str(Path(__file__).resolve()),
            "--worker",
            str(config_path),
        ],
        cwd=directory,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return json.loads(
        next(
            line.removeprefix("CODEC_CONSUMER ")
            for line in result.stdout.splitlines()
            if line.startswith("CODEC_CONSUMER ")
        )
    )


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
    return {
        "cif_1ubq": ("mmcif", DATA / "1ubq.cif", {}),
        "cif_64_blocks": ("mmcif", expanded, {}),
        "fcz_1ubq": ("foldcomp", DATA / "1ubq.fcz", {}),
        "fcz_long_mixed": ("foldcomp", STRESS / "long_mixed.fcz", {}),
        "fcz_long_segment": ("foldcomp", STRESS / "long_segment.fcz", {}),
        "fcz_subset_2": ("foldcomp", DATA / "example_db", {"entry_keys": [0, 7]}),
        "fcz_database_24": ("foldcomp", DATA / "example_db", {}),
        "fcz_subset_empty": ("foldcomp", DATA / "example_db", {"entry_keys": []}),
    }


def summarize(samples):
    result = {}
    for field, output in [
        ("seconds", "seconds"),
        ("peak_rss_bytes", "peak_rss_bytes"),
        ("first_collection_seconds", "first_collection_seconds"),
    ]:
        values = [
            s[field] / s["iterations"] if field == "seconds" else s[field]
            for s in samples
        ]
        median = statistics.median(values)
        result[output] = median
        result[output + "_mad_ratio"] = (
            statistics.median(abs(v - median) for v in values) / median
        )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-python", type=Path)
    parser.add_argument("--rust-python", type=Path)
    parser.add_argument("--native-wheel", type=Path)
    parser.add_argument("--rust-wheel", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--samples", type=int, default=9)
    parser.add_argument("--seconds", type=float, default=0.5)
    parser.add_argument("--datasets", help="comma-separated diagnostic subset")
    parser.add_argument("--workers", default="1,2,4,8")
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        print(
            "CODEC_CONSUMER " + json.dumps(worker(json.loads(args.worker.read_text())))
        )
        return
    if not all(
        [
            args.native_python,
            args.rust_python,
            args.native_wheel,
            args.rust_wheel,
            args.output,
        ]
    ):
        parser.error(
            "both installed interpreters, both wheel files and --output are required"
        )
    workers = [int(value) for value in args.workers.split(",")]
    if (
        args.samples < 3
        or args.seconds <= 0
        or any(value not in [1, 2, 4, 8] for value in workers)
    ):
        parser.error(
            "use at least three samples, positive seconds, and workers from 1,2,4,8"
        )
    # Keep virtualenv executable paths: resolving their symlinks selects base Python.
    pythons = {
        "native": args.native_python.absolute(),
        "rust": args.rust_python.absolute(),
    }
    wheels = {"native": args.native_wheel, "rust": args.rust_wheel}
    report = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "harness_sha256": digest(Path(__file__).read_bytes()),
        "samples": args.samples,
        "calibration_seconds": args.seconds,
        "method": "alternating paired fresh installed-wheel processes outside checkout; full public read including scan/selection/storage/collection; two untimed warmups; all columns; warm filesystem cache; no cache flush",
        "first_collection_scope": "first collection in a fresh process, excluding import; NOT cold filesystem or first batch",
        "rss_scope": "process peak through timed collection, including imports and warmups; excludes output hashing",
        "unavailable_metrics": [
            "actual storage bytes read",
            "decoder calls",
            "first batch latency",
        ],
        "threshold_ratio": 1.10,
        "noise_limit_mad_ratio": 0.05,
        "installations": {},
        "inputs": {},
        "results": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="codec-consumer-") as temporary:
        directory = Path(temporary)
        for backend, python in pythons.items():
            installed = measure(python, {"inspect": True}, directory)
            with zipfile.ZipFile(wheels[backend]) as archive:
                binaries = [
                    name
                    for name in archive.namelist()
                    if name.endswith((".so", ".pyd"))
                ]
                if (
                    len(binaries) != 1
                    or digest(archive.read(binaries[0]))
                    != installed["extension_sha256"]
                ):
                    raise RuntimeError(
                        f"{backend}: installed extension does not match supplied wheel"
                    )
            installed["wheel_sha256"] = digest(wheels[backend].read_bytes())
            installed["wheel"] = wheels[backend].name
            report["installations"][backend] = installed
        for field in ["packages", "python"]:
            if (
                report["installations"]["native"][field]
                != report["installations"]["rust"][field]
            ):
                raise RuntimeError(f"installed {field} differ between backends")
        available = datasets(directory)
        selected = args.datasets.split(",") if args.datasets else list(available)
        if set(selected) - set(available):
            parser.error("unknown dataset")
        for name in selected:
            kind, path, options = available[name]
            inputs = [path]
            if path.name == "example_db":
                inputs += [
                    Path(str(path) + suffix)
                    for suffix in [".index", ".lookup", ".dbtype"]
                ]
            report["inputs"][name] = [
                {
                    "file": p.name,
                    "bytes": p.stat().st_size,
                    "sha256": digest(p.read_bytes()),
                }
                for p in inputs
            ]
            for level in ["atom", "residue"]:
                for count in workers:
                    config = {
                        "format": kind,
                        "path": str(path),
                        "options": options,
                        "level": level,
                        "workers": count,
                        "iterations": 1,
                    }
                    trial = measure(pythons["native"], config, directory, count)
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
                                measure(pythons[backend], config, directory, count)
                            )
                        for field in [
                            "rows",
                            "iterations",
                            "schema",
                            "all_columns_sha256",
                            "target_partitions",
                        ]:
                            if (
                                samples["native"][-1][field]
                                != samples["rust"][-1][field]
                            ):
                                raise RuntimeError(
                                    f"unequal output/work: {name}/{level}/{count}/{field}"
                                )
                    medians = {
                        backend: summarize(values)
                        for backend, values in samples.items()
                    }
                    time_ratio = (
                        medians["rust"]["seconds"] / medians["native"]["seconds"]
                    )
                    rss_ratio = (
                        medians["rust"]["peak_rss_bytes"]
                        / medians["native"]["peak_rss_bytes"]
                    )
                    noisy = any(
                        m[key] > 0.05
                        for m in medians.values()
                        for key in ["seconds_mad_ratio", "peak_rss_bytes_mad_ratio"]
                    )
                    status = (
                        "noisy"
                        if noisy
                        else "regression"
                        if max(time_ratio, rss_ratio) > 1.10
                        else "within_local_budget"
                    )
                    report["results"].append(
                        {
                            "dataset": name,
                            "level": level,
                            "workers": count,
                            "iterations": config["iterations"],
                            "options": options,
                            "samples": samples,
                            "medians": medians,
                            "rust_native_time_ratio": time_ratio,
                            "rust_native_rss_ratio": rss_ratio,
                            "status": status,
                        }
                    )
                    args.output.write_text(json.dumps(report, indent=2) + "\n")
                    print(
                        f"{name}/{level}/{count}: time={time_ratio:.3f} RSS={rss_ratio:.3f} {status}",
                        flush=True,
                    )


if __name__ == "__main__":
    main()
