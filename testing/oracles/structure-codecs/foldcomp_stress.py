#!/usr/bin/env python3
"""Freeze two bounded long-chain FCZ probes from the handwritten corpus packer."""

import argparse
import json

from corpus import synthetic_fcz
from reference import HERE, build, canonical, query, sha256


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--record", action="store_true")
    modes.add_argument("--check", action="store_true")
    args = parser.parse_args()
    executable, metadata = build()
    inputs = {
        "long_mixed": synthetic_fcz(
            [i % 20 for i in range(1040)],
            anchors=[*range(0, 1040, 64), 1039],
            oxt=True,
            varied=True,
        ),
        "long_segment": synthetic_fcz([7] * 4096, varied=True),
    }
    directory = HERE / "fcz-stress"
    observations = []
    for name, data in inputs.items():
        result = query(executable, "fcz", data)
        if result["status"] != "ok":
            raise RuntimeError(f"{name}: {result}")
        path = directory / f"{name}.fcz"
        if args.record:
            directory.mkdir(exist_ok=True)
            path.write_bytes(data)
        elif path.read_bytes() != data:
            parser.exit(1, f"{name}: fixture bytes changed\n")
        observations.append(
            {
                "name": name,
                "input_sha256": sha256(data),
                "output_sha256": sha256(canonical(result)),
                "atoms_sha256": sha256(canonical(result["atoms"])),
                "residues": result["header"][0],
                "anchors": result["anchors"],
                "atoms": len(result["atoms"]),
            }
        )
    captured = {
        "formats_revision": metadata["formats_revision"],
        "native_archive_sha256": metadata["native_archive_sha256"],
        "driver_sha256": metadata["driver_sha256"],
        "generator_sha256": sha256((HERE / "foldcomp_stress.py").read_bytes()),
        "corpus_sha256": sha256((HERE / "corpus.py").read_bytes()),
        "cases": observations,
    }
    path = directory / "manifest.json"
    if args.record:
        path.write_bytes(canonical(captured))
    elif json.loads(path.read_bytes()) != captured:
        parser.exit(1, "Long-chain FCZ reference observations or hashes changed\n")
    print(
        "Verified long-chain FCZ fixtures"
        if args.check
        else "Recorded long-chain FCZ fixtures"
    )


if __name__ == "__main__":
    main()
