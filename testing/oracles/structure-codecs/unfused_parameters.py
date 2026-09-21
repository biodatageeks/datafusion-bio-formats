#!/usr/bin/env python3
"""Freeze the x86-64 reference's separately rounded FCZ parameter/B-factor bits."""

import argparse
import json

from corpus import all_cases
from reference import HERE, build, canonical, query, sha256

FIELDS = (
    "backbone_parameter_bits",
    "torsion_bits",
    "bond_angle_bits",
    "sidechain_angle_bits",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--record", action="store_true")
    modes.add_argument("--check", action="store_true")
    args = parser.parse_args()
    executable, metadata = build()
    baseline_bytes = (HERE / "golden.json").read_bytes()
    baseline = {row["name"]: row["expected"] for row in json.loads(baseline_bytes)}
    overrides = []
    for case in all_cases():
        if case["mode"] != "fcz" or case["name"].startswith("database_"):
            continue
        expected = baseline[case["name"]]
        actual = query(executable, "fcz", case["data"], case["max_atoms"])
        if expected["status"] != actual["status"]:
            raise RuntimeError(f"acceptance differs for {case['name']}")
        if actual["status"] != "ok":
            continue
        fields = {
            name: actual[name] for name in FIELDS if actual[name] != expected[name]
        }
        bfactors = [row[8] for row in actual["atoms"]]
        if bfactors != [row[8] for row in expected["atoms"]]:
            fields["bfactor_bits"] = bfactors
        if fields:
            overrides.append({"name": case["name"], "fields": fields})
    # Handwritten code 93 proves this process is using the intended profile.
    varied = next(row for row in overrides if row["name"] == "residue_code_01")
    if 3259159248 not in [
        v for row in varied["fields"]["sidechain_angle_bits"] for v in row
    ]:
        raise RuntimeError("reference is not the unfused arithmetic profile")
    result = {
        "baseline_sha256": sha256(baseline_bytes),
        "generator_sha256": sha256((HERE / "unfused_parameters.py").read_bytes()),
        "capture": metadata,
        "overrides": overrides,
    }
    path = HERE / "unfused-parameters.json"
    if args.record:
        path.write_bytes(canonical(result))
    else:
        frozen = json.loads(path.read_bytes())
        for key in ["baseline_sha256", "generator_sha256", "overrides"]:
            if frozen[key] != result[key]:
                parser.exit(1, f"unfused parameter contract changed: {key}\n")
        for key in ["formats_revision", "native_archive_sha256", "driver_sha256"]:
            if frozen["capture"][key] != metadata[key]:
                parser.exit(1, f"reference provenance changed: {key}\n")
    print(
        f"{'Recorded' if args.record else 'Verified'} {len(overrides)} separately rounded parameter cases"
    )


if __name__ == "__main__":
    main()
