#!/usr/bin/env python3
"""Compare the standalone Rust candidate with the pinned native process."""

import argparse
import json
import math
from pathlib import Path
import platform
import struct
import tempfile

from cif_probes import inputs as cif_inputs
from corpus import all_cases
from reference import HERE, build, canonical, query, run, sha256


def number(bits):
    return struct.unpack("<f", struct.pack("<I", bits))[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--label", required=True, help="actual executable target, including emulation"
    )
    args = parser.parse_args()
    native, provenance = build()
    candidate = args.candidate.resolve()
    report = {
        "label": args.label,
        "host": platform.platform(),
        "reference": provenance,
        "candidate_sha256": sha256(candidate.read_bytes()),
        "reference_binary_sha256": sha256(native.read_bytes()),
        "coordinate_ceiling": 1e-4,
        "bfactor_ceiling": 0.0,
        "cases": [],
        "failures": [],
    }
    cases = all_cases()
    cases.extend(
        {"name": f"lexical_{name}", "mode": "cif", "data": data, "max_atoms": 5_000_000}
        for name, data in cif_inputs()
    )
    cases.extend(
        {
            "name": path.stem,
            "mode": "fcz",
            "data": path.read_bytes(),
            "max_atoms": 5_000_000,
        }
        for path in sorted((HERE / "fcz-stress").glob("*.fcz"))
    )
    errors = []
    max_bfactor = 0.0
    for case in cases:
        expected = query(native, case["mode"], case["data"], case["max_atoms"])
        with tempfile.TemporaryDirectory(prefix="candidate-input-") as temp:
            source = Path(temp) / "input"
            source.write_bytes(case["data"])
            actual = json.loads(
                run(
                    [str(candidate), case["mode"], str(source), str(case["max_atoms"])],
                    timeout=20,
                ).stdout
            )
        failure = None
        worst = 0.0
        worst_b = 0.0
        worst_atom = None
        if expected["status"] != actual["status"]:
            failure = "acceptance differs"
        elif actual["status"] == "ok":
            if case["mode"] == "cif":
                if actual != expected:
                    failure = "raw CIF blocks/columns/values differ"
            elif actual["decoded_title_hex"] != expected["decoded_title_hex"] or len(
                actual["atoms"]
            ) != len(expected["atoms"]):
                failure = "title/atom count differs"
            else:
                for index, (a, b) in enumerate(
                    zip(actual["atoms"], expected["atoms"], strict=True)
                ):
                    if a[:5] != b[:5]:
                        failure = f"atom identity differs at {index}"
                    for axis in range(5, 8):
                        delta = abs(number(a[axis]) - number(b[axis]))
                        if not math.isfinite(delta):
                            raise RuntimeError(
                                f"non-finite coordinate in {case['name']}"
                            )
                        errors.append(delta)
                        if delta > worst:
                            worst = delta
                            worst_atom = {
                                "index": index,
                                "axis": axis - 5,
                                "rust": number(a[axis]),
                                "native": number(b[axis]),
                            }
                    b_delta = abs(number(a[8]) - number(b[8]))
                    if not math.isfinite(b_delta):
                        raise RuntimeError(f"non-finite B factor in {case['name']}")
                    worst_b = max(worst_b, b_delta)
                if worst > 1e-4 or worst_b > 0.0:
                    failure = "numerical ceiling exceeded"
        record = {
            "name": case["name"],
            "mode": case["mode"],
            "input_sha256": sha256(case["data"]),
            "native_sha256": sha256(canonical(expected)),
            "rust_sha256": sha256(canonical(actual)),
            "max_coordinate_error": worst,
            "max_bfactor_error": worst_b,
            "worst_atom": worst_atom,
        }
        report["cases"].append(record)
        max_bfactor = max(max_bfactor, worst_b)
        if failure:
            report["failures"].append(
                {
                    "name": case["name"],
                    "reason": failure,
                    "max_coordinate_error": worst,
                    "max_bfactor_error": worst_b,
                    "worst_atom": worst_atom,
                }
            )
    errors.sort()
    report["summary"] = {
        "cases": len(cases),
        "components": len(errors),
        "max_coordinate_error": max(errors, default=0),
        "p50": errors[len(errors) // 2],
        "p95": errors[len(errors) * 95 // 100],
        "p99": errors[len(errors) * 99 // 100],
        "max_bfactor_error": max_bfactor,
        "failures": len(report["failures"]),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical(report))
    print(json.dumps(report["summary"]), flush=True)
    if report["failures"]:
        print(json.dumps(report["failures"][:10], indent=2), flush=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
