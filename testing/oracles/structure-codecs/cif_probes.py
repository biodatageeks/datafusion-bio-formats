#!/usr/bin/env python3
"""Frozen CIF lexical boundary matrix, using the separate pinned reference."""

import argparse
import json

from reference import HERE, build, canonical, query, sha256


def inputs():
    values = [
        b"plain",
        b"a#b",
        b".",
        b"?",
        b"'.'",
        b'"?"',
        b"''",
        b'""',
        b"'can't'",
        b'"a"b"',
        b"'space inside'",
        b"'\xc3\xa9'",
        b"'\xff'",
        b"'a\x00b'",
        b"loop_",
        b"loop_more",
        b"stop_",
        b"stop_more",
        b"global_",
        b"global_more",
        b"data_x",
        b"data_",
        b"save_x",
        b"save_",
        b"_tag",
        b"$ref",
        b"[list]",
        b";bare",
        b"a_b",
        b"a\x0bb",
        b"a\x0cb",
        b"a\xff",
        b"a\x00",
    ]
    for position, template in enumerate(
        [
            b"data_x\n_a %s%s\n",
            b"data_x\nloop_\n_a\n%s%s\n",
            b"data_x\nsave_f\n_a %s%s\nsave_\n",
        ]
    ):
        for index, value in enumerate(values):
            for suffix_index, suffix in enumerate([b"", b" ", b"#comment", b"\t#\xff"]):
                yield (
                    f"value_{position}_{index}_{suffix_index}",
                    template % (value, suffix),
                )
    for index, text in enumerate(
        [
            b"data_x\nloop_#comment\n_a\n1 2\n",
            b"global_#comment\n_a 1\n",
            b"data_x\nloop_\n_a\n1 stop_#comment\n_b 2\n",
            b"data_x\nsave_f\n_a 1\nsave_#comment words\n_b 2\n",
            b"data_x\nsave_f\n_a 1\nsave_#\xff\n_b 2\n",
            b"data_x\nsave_#name\n_a 1\nsave_\n",
            b"data_x\nsave_f\n_a 1\nsave_more\n",
            b"data_x\nloop_\n_a\nstop_\n",
            b"data_x\n_a\n;.\n;\n",
            b"data_x\n_a\n;txt\n;#comment\n",
            b"data_x\nsave_f\nloop_\n_a\n_A\n1 2\nsave_\n",
            b"data_x\nloop_\n_a\n1\nsave_f\n_b 2\nsave_\n",
            b"DaTa_X\nLoOp_\n_A\n1\nStOp_\n",
            b"data_x\n_a 1\ndata_y\n_b 2\r\n",
            b"data_x\n_a\n;first\r\nsecond\r\n;\n",
            b"data_x\n_a 'has\rreturn'\n",
        ]
    ):
        yield f"boundary_{index}", text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--record", action="store_true")
    modes.add_argument("--check", action="store_true")
    args = parser.parse_args()
    executable, metadata = build()
    cases = [
        {
            "name": name,
            "input_hex": data.hex(),
            "input_sha256": sha256(data),
            "expected": query(executable, "cif", data),
        }
        for name, data in inputs()
    ]
    captured = {
        "formats_revision": metadata["formats_revision"],
        "native_archive_sha256": metadata["native_archive_sha256"],
        "driver_sha256": metadata["driver_sha256"],
        "reference_sha256": sha256((HERE / "reference.py").read_bytes()),
        "generator_sha256": sha256((HERE / "cif_probes.py").read_bytes()),
        "cases": cases,
    }
    path = HERE / "cif-probes.json"
    if args.record:
        path.write_bytes(canonical(captured))
    elif json.loads(path.read_bytes()) != captured:
        parser.exit(1, "CIF probe observations or hashes changed\n")
    print(
        f"{'Recorded' if args.record else 'Verified'} {len(cases)} CIF boundary probes"
    )


if __name__ == "__main__":
    main()
