#!/usr/bin/env python3
"""Record or verify byte-exact observations from the pinned legacy process."""

import argparse
from collections import Counter
import json
import sys

from corpus import DATA, all_cases
from check_contract import check
from reference import HERE, build, canonical, query, sha256


def capture():
    executable, metadata = build()
    cases = all_cases()
    if len({item["name"] for item in cases}) != len(cases):
        raise ValueError("duplicate corpus case name")
    observations = []
    manifest_cases = []
    inputs = []
    for item in cases:
        name = item["name"]
        try:
            result = query(executable, item["mode"], item["data"], item["max_atoms"])
        except Exception as error:
            raise RuntimeError(f"reference failed for {name}: {error}") from error
        # Large existing database records remain in their original fixture.
        # Freeze full arrays by hash; retain full arrays for the small probes and
        # 1UBQ so a future Rust decoder can compare individual fields offline.
        expected = result
        if name.startswith("database_") and result["status"] == "ok":
            expected = {
                "status": "ok",
                "sha256": sha256(canonical(result)),
                "header": result["header"],
                "anchors": result["anchors"],
                "residue_codes": sorted({row[0] for row in result["backbone"]}),
                "has_oxt": result["has_oxt"],
                "atom_count": len(result["atoms"]),
            }
        observations.append({"name": name, "expected": expected})
        manifest_cases.append(
            {
                "name": name,
                "mode": item["mode"],
                "coverage": item["coverage"],
                "input_sha256": sha256(item["data"]),
                "input_bytes": len(item["data"]),
                "max_atoms": item["max_atoms"],
                "output_sha256": sha256(canonical(result)),
            }
        )
        if not name.startswith("database_") and name not in ("1ubq", "1ubq_raw"):
            inputs.append(
                {
                    "name": name,
                    "mode": item["mode"],
                    "input_hex": item["data"].hex(),
                    "max_atoms": item["max_atoms"],
                }
            )
    tables = query(executable, "tables")
    checks = check(observations, tables)
    outputs = {
        "golden.json": canonical(observations),
        "inputs.json": canonical(inputs),
        "residue-codes.json": canonical(tables),
    }
    manifest = {
        "schema_version": 1,
        "baseline": metadata,
        "generator_sha256": sha256((HERE / "corpus.py").read_bytes()),
        "capture_sha256": sha256((HERE / "capture.py").read_bytes()),
        "reference_sha256": sha256((HERE / "reference.py").read_bytes()),
        "contract_checks_sha256": sha256((HERE / "check_contract.py").read_bytes()),
        "independent_checks": checks,
        "fixture_sources": {
            "1ubq.cif": sha256((DATA / "1ubq.cif").read_bytes()),
            "1ubq.fcz": sha256((DATA / "1ubq.fcz").read_bytes()),
            "example_db": sha256((DATA / "example_db").read_bytes()),
            "example_db.index": sha256((DATA / "example_db.index").read_bytes()),
        },
        "outputs": {name: sha256(data) for name, data in outputs.items()},
        "cases": manifest_cases,
    }
    counts = Counter(
        (item["mode"], row["expected"]["status"])
        for item, row in zip(cases, observations, strict=True)
    )
    print(f"Captured {len(cases)} cases: {dict(counts)}", file=sys.stderr)
    return outputs, manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--record",
        action="store_true",
        help="replace the frozen observations; review resulting diffs",
    )
    mode.add_argument(
        "--check",
        action="store_true",
        help="verify input/output hashes and observed values without writing",
    )
    args = parser.parse_args()
    outputs, manifest = capture()
    if args.record:
        for name, data in outputs.items():
            (HERE / name).write_bytes(data)
        (HERE / "manifest.json").write_bytes(canonical(manifest))
        print(
            "Recorded baseline; review acceptance/error observations before using them as a contract."
        )
        return
    frozen = json.loads((HERE / "manifest.json").read_bytes())
    # Compiler/platform provenance is retained from the original recording. A
    # different environment must still reproduce the same strict observations;
    # this check is not a numerical tolerance or cross-platform acceptance test.
    failures = []
    for key in (
        "schema_version",
        "generator_sha256",
        "capture_sha256",
        "reference_sha256",
        "contract_checks_sha256",
        "independent_checks",
        "fixture_sources",
        "outputs",
        "cases",
    ):
        if frozen[key] != manifest[key]:
            failures.append(f"manifest {key}")
    for key in ("formats_revision", "native_archive_sha256", "driver_sha256", "flags"):
        if frozen["baseline"][key] != manifest["baseline"][key]:
            failures.append(f"baseline {key}")
    for name, data in outputs.items():
        if (HERE / name).read_bytes() != data:
            failures.append(name)
    if failures:
        parser.exit(1, "Baseline drift: " + ", ".join(failures) + "\n")
    print(
        f"Verified {len(manifest['cases'])} cases and all input/output/source hashes."
    )


if __name__ == "__main__":
    main()
