"""Independent checks so recording cannot silently bless a broken driver."""

import json
import math
import struct

from corpus import LETTERS, SIDECHAINS
from reference import ROOT


def number(bits):
    return struct.unpack("<f", struct.pack("<I", bits))[0]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def check(observations, tables):
    results = {item["name"]: item["expected"] for item in observations}
    require(
        results["raw_nulls"]["blocks"]
        == [
            {
                "name": "Mixed",
                "columns": {"_x.value": [None, None, ".", "?", "", ""]},
            }
        ],
        "null/quoted-string oracle disagrees with handwritten values",
    )
    require(
        results["text_field_crlf"]["blocks"][0]["columns"]["_a"] == ["first\r\nsecond"],
        "text field must preserve interior CRLF",
    )
    for code, table in enumerate(tables):
        supported = code not in (20, 21, 22)
        require(
            table["code"] == code and table["letter"] == ord(LETTERS[code]),
            "residue code mapping drift",
        )
        require(table["supported"] == supported, "residue support drift")
        if supported:
            require(
                table["sidechain_count"] == SIDECHAINS[code],
                "handwritten sidechain count mismatch",
            )
        result = results[f"residue_code_{code:02}"]
        require(
            (result["status"] == "ok") == supported,
            f"unexpected residue acceptance for {code}",
        )
        if not supported:
            continue
        count = table["atom_count"]
        require(
            len(result["atoms"]) == 2 * count,
            f"wrong reconstructed atom count for {code}",
        )
        for index, atom in enumerate(result["atoms"]):
            require(
                atom[1] == table["name_hex"] and atom[2] == "5a",
                "residue/chain identity mismatch",
            )
            require(
                atom[3] == index + 10 and atom[4] == 5 + index // count,
                "atom/residue numbering mismatch",
            )
            require(
                number(atom[8]) == (7.5 if index < count else 39.375),
                "analytical B-factor mismatch",
            )
            require(
                all(math.isfinite(number(bits)) for bits in atom[5:9]),
                "non-finite output accepted",
            )
    packed = results["packed_bits"]
    require(
        packed["backbone"]
        == [[code, 0xABC, 0xDEF, 0x567, 0x89, 0xAB, 0xCD] for code in (0, 7)],
        "packed bit fields disagree with handwritten integers",
    )
    require(
        results["multi_anchor"]["anchors"] == [0, 2, 4],
        "multi-anchor input not exercised",
    )
    require(results["oxt"]["atoms"][-1][0] == b"OXT".hex(), "terminal OXT missing")
    require(results["oxt"]["atoms"][-1][4] == 2, "legacy OXT numbering changed")
    require(
        bytes.fromhex(results["title_nul"]["decoded_title_hex"]) == b"before",
        "CStr title truncation changed",
    )
    require(
        results["chain_nul"]["atoms"][0][2] == "", "CStr NUL chain conversion changed"
    )
    for name, result in results.items():
        if name.startswith("truncated_"):
            require(result["status"] == "error", f"truncated input accepted: {name}")
    for name in (
        "declared_atom_limit",
        "reconstructed_atom_limit",
        "sidechain_count_valid_length",
        "last_anchor_not_final",
        "chain_invalid_utf8",
    ):
        require(
            results[name]["status"] == "error", f"invalid FCZ probe accepted: {name}"
        )
    # Existing independent Python Foldcomp oracle, derived from the same encoded
    # bytes but not this new driver. Compare decoded arrays (no PDB rounding).
    legacy = json.loads((ROOT / "testing/oracles/structure/1ubq.fcz.json").read_text())
    actual = results["1ubq"]["atoms"]
    require(
        len(actual) == len(legacy["atoms"]),
        "1UBQ independent oracle atom count mismatch",
    )
    max_error = 0.0
    for row, expected in zip(actual, legacy["atoms"], strict=True):
        require(
            bytes.fromhex(row[0]).decode() == expected["atom_name"],
            "1UBQ atom name mismatch",
        )
        require(
            bytes.fromhex(row[1]).decode() == expected["residue_name"],
            "1UBQ residue name mismatch",
        )
        require(
            bytes.fromhex(row[2]).decode() == expected["chain_id"],
            "1UBQ chain mismatch",
        )
        require(str(row[4]) == expected["auth_seq_id"], "1UBQ residue index mismatch")
        max_error = max(
            max_error,
            *(
                abs(number(bits) - value)
                for bits, value in zip(row[5:8], expected["position"], strict=True)
            ),
        )
    require(
        max_error <= 1e-4, f"1UBQ coordinate error {max_error} exceeds existing ceiling"
    )
    return {
        "1ubq_max_coordinate_error_angstrom": max_error,
        "synthetic_bfactor_max_error": 0.0,
    }
