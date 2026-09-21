#!/usr/bin/env python3
"""Recreate deterministic fuzz seeds from the frozen offline codec corpus."""

import hashlib
import json
from pathlib import Path
import struct

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
ORACLE = ROOT / "testing/oracles/structure-codecs"


def save(target, data):
    folder = HERE / "corpus" / target
    folder.mkdir(parents=True, exist_ok=True)
    (folder / hashlib.sha256(data).hexdigest()).write_bytes(data)


def main():
    for case in json.loads((ORACLE / "inputs.json").read_bytes()):
        save(
            "cif_document" if case["mode"] == "cif" else "fcz_decode",
            bytes.fromhex(case["input_hex"]),
        )
    for case in json.loads((ORACLE / "cif-probes.json").read_bytes())["cases"]:
        save("cif_document", bytes.fromhex(case["input_hex"]))
    for suffix, target in [("cif", "cif_document"), ("fcz", "fcz_decode")]:
        save(target, (ROOT / f"testing/data/structure/1ubq.{suffix}").read_bytes())
    data = (ROOT / "testing/data/structure/example_db").read_bytes()
    for line in (
        (ROOT / "testing/data/structure/example_db.index").read_text().splitlines()
    ):
        _, offset, length = map(int, line.split())
        save("fcz_decode", data[offset : offset + length - 1])
    for path in (ORACLE / "fcz-stress").glob("*.fcz"):
        save("fcz_decode", path.read_bytes())
    for previous in [0, 1, 2**64 - 1]:
        for row in [
            b"1 0 2",
            b"2 0 100",
            b"1 18446744073709551615 2",
            b"0 0 0",
            b"-1 0 2",
            b"1 2 3 4",
            b"18446744073709551616 0 2",
        ]:
            save("selected_range", struct.pack("<3Q", previous, 100, 100) + row)
    for target in ["cif_document", "fcz_decode", "selected_range"]:
        files = sorted((HERE / "corpus" / target).iterdir())
        digest = hashlib.sha256(
            b"".join(path.name.encode() + b"\n" for path in files)
        ).hexdigest()
        print(f"{target}: {len(files)} seeds, name-list sha256={digest}")


if __name__ == "__main__":
    main()
