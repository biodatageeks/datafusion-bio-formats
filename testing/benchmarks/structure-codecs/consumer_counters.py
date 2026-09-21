#!/usr/bin/env python3
"""Check installed-wheel SQL execution counters separately from timing samples."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent
FIELDS = ["sources_opened", "entries_decoded", "encoded_bytes_read", "output_rows"]


def worker(config):
    import polars_bio as pb

    if "site-packages" not in Path(pb.__file__).resolve().parts:
        raise RuntimeError("expected installed wheel")
    pb.set_option("datafusion.execution.target_partitions", str(config["workers"]))
    records = []
    for item in config["inputs"]:
        for level in ["atom", "residue"]:
            table = "codec_counters_" + item["name"] + "_" + level
            options = {"level": level, **item["options"]}
            if item["kind"] == "mmcif":
                pb.register_structure(table, item["path"], format="mmcif", **options)
            else:
                pb.register_foldcomp(table, item["path"], **options)
            rows = pb.sql(f"EXPLAIN ANALYZE SELECT * FROM {table}").collect().to_dicts()
            plans = [str(value) for row in rows for value in row.values()]
            leaves = [
                line
                for plan in plans
                for line in plan.splitlines()
                if "StructureExec" in line
            ]
            if not leaves:
                raise RuntimeError(f"missing structure execution metrics: {rows}")
            counters = {}
            for field in FIELDS:
                values = [
                    value.strip()
                    for leaf in leaves
                    for value in re.findall(rf"\b{field}=([^,\]]+)", leaf)
                ]
                if len(values) != 1:
                    raise RuntimeError(f"expected one aggregate {field} in {rows}")
                counters[field] = values[0]
            records.append(
                {
                    "dataset": item["name"],
                    "level": level,
                    "workers": config["workers"],
                    "counters": counters,
                    "explain": rows,
                }
            )
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-python", type=Path)
    parser.add_argument("--rust-python", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        print(
            "CODEC_COUNTERS " + json.dumps(worker(json.loads(args.worker.read_text())))
        )
        return
    if not all([args.native_python, args.rust_python, args.output]):
        parser.error("both installed interpreters and --output are required")
    spec = importlib.util.spec_from_file_location(
        "consumer_bench", HERE / "consumer.py"
    )
    benchmark = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(benchmark)
    report = {
        "scope": "separate untimed EXPLAIN ANALYZE checks; exact small source/entry counts, rounded displayed row/payload counts; logical encoded bytes exclude sidecars and physical filesystem I/O; expected_counts are fixture-derived exact totals",
        "results": [],
    }
    with tempfile.TemporaryDirectory(prefix="codec-counter-check-") as temporary:
        directory = Path(temporary)
        report["installations"] = {
            backend: benchmark.measure(python.absolute(), {"inspect": True}, directory)
            for backend, python in [
                ("native", args.native_python),
                ("rust", args.rust_python),
            ]
        }
        inputs = [
            {"name": name, "kind": kind, "path": str(path), "options": options}
            for name, (kind, path, options) in benchmark.datasets(directory).items()
        ]
        expected_counts = {}
        for item in inputs:
            path = Path(item["path"])
            if path.name == "example_db":
                selected = item["options"].get("entry_keys")
                records = [
                    list(map(int, line.split()))
                    for line in Path(str(path) + ".index").read_text().splitlines()
                ]
                records = [
                    row for row in records if selected is None or row[0] in selected
                ]
                expected_counts[item["name"]] = {
                    "sources_opened": len(records),
                    "entries_decoded": len(records),
                    "encoded_bytes_read": sum(row[2] for row in records),
                }
            else:
                expected_counts[item["name"]] = {
                    "sources_opened": 1,
                    "entries_decoded": 64 if item["name"] == "cif_64_blocks" else 1,
                    "encoded_bytes_read": path.stat().st_size,
                }
        report["expected_counts"] = expected_counts
        report["inputs"] = [
            {
                "dataset": item["name"],
                "sha256": hashlib.sha256(Path(item["path"]).read_bytes()).hexdigest(),
            }
            for item in inputs
        ]
        for workers in [1, 2, 4, 8]:
            config = directory / "config.json"
            config.write_text(json.dumps({"inputs": inputs, "workers": workers}))
            results = {}
            for backend, python in [
                ("native", args.native_python),
                ("rust", args.rust_python),
            ]:
                env = {
                    key: value
                    for key, value in os.environ.items()
                    if key != "PYTHONPATH"
                }
                env.update(
                    POLARS_MAX_THREADS=str(workers), RAYON_NUM_THREADS=str(workers)
                )
                result = subprocess.run(
                    [
                        str(python.absolute()),
                        "-I",
                        str(Path(__file__).resolve()),
                        "--worker",
                        str(config),
                    ],
                    cwd=directory,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=300,
                )
                if result.returncode:
                    raise RuntimeError(result.stdout + result.stderr)
                results[backend] = json.loads(
                    next(
                        line.removeprefix("CODEC_COUNTERS ")
                        for line in result.stdout.splitlines()
                        if line.startswith("CODEC_COUNTERS ")
                    )
                )
            for native, rust in zip(results["native"], results["rust"], strict=True):
                for field in ["dataset", "level", "workers", "counters"]:
                    if native[field] != rust[field]:
                        raise RuntimeError(f"unequal counters: {native} versus {rust}")
                for field, expected in expected_counts[native["dataset"]].items():
                    reported = native["counters"][field]
                    if field != "encoded_bytes_read":
                        matches = reported == str(expected)
                    else:
                        # DataFusion 53 rounds large counts to one or two decimals.
                        # Preserve that string; never claim it is an exact byte counter.
                        parts = reported.split()
                        scale = {
                            "K": 1_000,
                            "M": 1_000_000,
                            "B": 1_000_000_000,
                            "T": 1_000_000_000_000,
                        }
                        if len(parts) == 1:
                            matches = int(parts[0]) == expected
                        else:
                            unit = scale[parts[1]]
                            decimals = len(parts[0].partition(".")[2])
                            matches = abs(float(parts[0]) * unit - expected) <= unit / (
                                2 * 10**decimals
                            )
                    if not matches:
                        raise RuntimeError(f"unexpected {field}: {native}")
            report["results"].append({"workers": workers, **results})
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
            print(
                f"{workers} workers: all 16 atom/residue cases have matching counters",
                flush=True,
            )


if __name__ == "__main__":
    main()
