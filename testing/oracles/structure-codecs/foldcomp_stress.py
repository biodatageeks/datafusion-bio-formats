#!/usr/bin/env python3
"""Freeze two bounded long-chain FCZ probes from the handwritten corpus packer."""

import argparse
import json
from pathlib import Path

from corpus import synthetic_fcz
from reference import HERE, build, canonical, query, sha256


def source_sha256(path):
    # Windows Git checkouts can use CRLF without changing the source program.
    return sha256(path.read_bytes().replace(b"\r\n", b"\n"))


def arithmetic_profile(metadata):
    """Name only the native arithmetic/toolchain combinations we have measured."""
    machine = metadata["machine"].lower()
    architecture = {"arm64": "aarch64", "amd64": "x86_64"}.get(machine, machine)
    compiler = metadata["compiler"]
    system = metadata["platform"]
    if system.startswith("macOS-") and "Apple clang" in compiler:
        family = "macos-apple-clang"
    elif (
        system.startswith("Linux-")
        and "with-glibc" in system
        and "Free Software Foundation" in compiler
    ):
        family = "linux-gnu-gcc"
    elif system.startswith("Windows-") and Path(
        metadata["compiler_command"][0]
    ).name.lower() in ("cl", "cl.exe"):
        family = "windows-msvc"
    else:
        raise ValueError(
            "uncharacterized native toolchain; characterize it before adding a profile"
        )
    return f"{family}-{architecture}"


def check_observations(manifest, captured, profile, outputs):
    expected = {key: value for key, value in manifest.items() if key != "profiles"}
    if expected != captured:
        raise ValueError("Long-chain FCZ fixture/provenance observations changed")
    if profile not in manifest["profiles"]:
        raise ValueError(
            f"No frozen stress outputs for {profile}; record and review this profile"
        )
    if manifest["profiles"][profile]["outputs"] != outputs:
        raise ValueError(
            f"Long-chain FCZ reference output hashes changed for {profile}"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--record", action="store_true")
    modes.add_argument("--check", action="store_true")
    args = parser.parse_args()
    executable, metadata = build()
    profile = arithmetic_profile(metadata)
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
    outputs = []
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
                "residues": result["header"][0],
                "anchors": result["anchors"],
                "atoms": len(result["atoms"]),
            }
        )
        outputs.append({"name": name, "output_sha256": sha256(canonical(result))})
    captured = {
        "formats_revision": metadata["formats_revision"],
        # The immutable revision identifies native source independently of Git's
        # platform-specific archive bytes. Raw archive/compiler provenance is
        # retained inside each measured profile, never treated as a global hash.
        "driver_sha256": source_sha256(HERE / "reference.cpp"),
        "generator_sha256": source_sha256(HERE / "foldcomp_stress.py"),
        "corpus_sha256": source_sha256(HERE / "corpus.py"),
        "cases": observations,
    }
    path = directory / "manifest.json"
    manifest = json.loads(path.read_bytes()) if path.exists() else {}
    if args.record:
        # Preserve other measured profiles only when their shared inputs and
        # reference source still match. A script edit alone need not erase them.
        shared = {
            key: value for key, value in captured.items() if key != "generator_sha256"
        }
        previous = {key: manifest.get(key) for key in shared}
        profiles = manifest.get("profiles", {}) if previous == shared else {}
        profiles[profile] = {"reference": metadata, "outputs": outputs}
        captured["profiles"] = profiles
        path.write_bytes(canonical(captured))
    else:
        try:
            check_observations(manifest, captured, profile, outputs)
        except ValueError as error:
            parser.exit(1, f"{error}\n")
    print(
        f"Verified long-chain FCZ fixtures ({profile})"
        if args.check
        else "Recorded long-chain FCZ fixtures"
    )


if __name__ == "__main__":
    main()
