"""Small hand-authored syntax/byte probes, independent of the native decoder.

FCZ residue IDs/counts follow the pinned Foldcomp format (MIT); see README.md.
These synthetic coordinates exercise decoding, not biological plausibility.
"""

import struct

from reference import ROOT

DATA = ROOT / "testing" / "data" / "structure"
LETTERS = "ARNDCQEGHILKMFPSTWYVBZ*X" + "X" * 8
SIDECHAINS = [2, 8, 5, 5, 3, 6, 6, 1, 7, 5, 5, 6, 5, 8, 4, 3, 4, 11, 9, 4] + [0] * 12


def case(name, mode, data, coverage, max_atoms=5_000_000):
    return {
        "name": name,
        "mode": mode,
        "data": data,
        "coverage": coverage,
        "max_atoms": max_atoms,
    }


def cif_cases():
    probes = {
        "empty": "",
        "comments_only": "# ignored\n \t\r\n# EOF",
        "raw_nulls": "data_Mixed\nloop_\n_X.Value\n. ? '.' \"?\" '' \"\"\n",
        "embedded_quotes": "data_x\n_a 'can't'\n_b \"say\"hello\"\n",
        "quote_before_comment": "data_x\n_a 'value'# comment\n_b 2\n",
        "bare_hash": "data_x\n_a alpha#beta\n_b a_b\n",
        "quoted_reserved_words": "data_x\nloop_\n_a\n'data_x' 'loop_' 'save_x' 'stop_' '_tag' '$ref'\n",
        "keyword_prefix_values": "data_x\nloop_\n_a\nloop_more stop_more global_more\n",
        "text_field_lf": "data_x\n_a\n;first\nsecond\n;\n",
        "text_field_crlf": "data_x\r\n_a\r\n;first\r\nsecond\r\n;\r\n",
        "text_field_leading_newline": "data_x\n_a\n;\nfirst\n\n;\n",
        "text_field_empty": "data_x\n_a\n;\n;\n",
        "semicolon_not_column_one": "data_x\n_a ;literal\n",
        "semicolon_bad_close": "data_x\n_a\n;first\n;tail\n",
        "multiline_quote": "data_x\n_a 'first\nsecond'\n",
        "quote_carriage_return": "data_x\n_a 'first\rsecond'\n",
        "unterminated_single_quote": "data_x\n_a 'no end\n",
        "unterminated_double_quote": 'data_x\n_a "no end',
        "unterminated_text_field": "data_x\n_a\n;no end\n",
        "loop_physical_lines": "data_x\nloop_\n_a\n_b\n1\n2 3 # comment\n4\n",
        "loop_stop": "data_x\nloop_\n_a\n1 2 stop_\n_b 3\n",
        "empty_loop_at_eof": "data_x\nloop_\n_a\n",
        "empty_loop_before_block": "data_x\nloop_\n_a\ndata_y\n_b 1\n",
        "loop_no_tags": "data_x\nloop_\n1\n",
        "loop_incomplete_row": "data_x\nloop_\n_a\n_b\n1 2 3\n",
        "loop_tag_no_whitespace": "data_x\nloop_\n_a",
        "missing_value": "data_x\n_a\n_b 1\n",
        "tag_no_name": "data_x\n_ 1\n",
        "duplicate_scalar": "data_x\n_a 1\n_A 2\n",
        "duplicate_loop_tag": "data_x\nloop_\n_a\n_A\n1 2\n",
        "duplicate_scalar_loop": "data_x\n_a 1\nloop_\n_A\n2\n",
        "multiple_blocks": "data_Case\n_a 1\ndata_Other\n_a 2\n",
        "duplicate_block": "data_Case\n_a 1\ndata_case\n_b 2\n",
        "bare_data_heading": "data_\n_a 1\n",
        "global_blocks": "global_\n_a 1\ndata_x\n_b 2\nglobal_\n_c 3\n",
        "save_frame_ignored": "data_x\n_a before\nsave_frame\n_b hidden\nsave_\n_c after\n",
        "save_frame_duplicate_tags": "data_x\nsave_frame\n_b first\n_B second\nsave_\n",
        "save_frame_duplicate_names": "data_x\nsave_frame\n_b 1\nsave_\nsave_FRAME\n_b 2\nsave_\n",
        "save_frame_missing_value": "data_x\nsave_frame\n_b\nsave_\n",
        "nested_save_frame": "data_x\nsave_outer\nsave_inner\n_b 1\nsave_\nsave_\n",
        "standalone_stop": "data_x\nstop_\n",
        "bare_dollar": "data_x\n_a $ref\n",
        "no_header": "_a value\n",
        "unicode_quoted": "data_x\n_a 'caf\u00e9'\n",
        "unicode_unquoted": "data_x\n_a caf\u00e9\n",
        "unicode_comment": "# caf\u00e9\ndata_x\n_a ok\n",
        "numeric_uncertainty_raw": "data_x\n_a 12.3(4)\n_b nan\n_c inf\n",
        "whitespace_formfeed": "data_x\n_a\f1\n",
        "whitespace_vertical_tab": "data_x\n_a\v1\n",
        "metadata_after_atoms": (
            "data_RawIds\nloop_\n_atom_site.id\n_atom_site.auth_seq_id\n"
            "_atom_site.label_seq_id\n_atom_site.auth_asym_id\n_atom_site.pdbx_PDB_model_num\n"
            "2 X1 1 'Mixed Chain' 2\n1 -1 2 A 1\n"
            "_entry.id 'preserved entry'\n_struct_asym.id A\n_struct_asym.entity_id 7\n"
            "_entity_poly.entity_id 7\n_entity_poly.type 'polypeptide(L)'\n"
            "_chem_comp.id MSE\n_chem_comp.mon_nstd_parent_comp_id MET\n"
            "_pdbx_struct_mod_residue.auth_seq_id X1\n"
            "_pdbx_struct_mod_residue.parent_comp_id MET\n"
        ),
    }
    for name, text in probes.items():
        yield case(name, "cif", text.encode(), name.replace("_", " "))
    for name, data in {
        "invalid_utf8_quoted": b"data_x\n_a '\xff'\n",
        "invalid_utf8_comment": b"# \xff\ndata_x\n_a ok\n",
        "invalid_utf8_ignored_frame": b"data_x\nsave_f\n_a '\xff'\nsave_\n",
        "nul_quoted": b"data_x\n_a 'a\x00b'\n",
        "utf8_bom": b"\xef\xbb\xbfdata_x\n_a 1\n",
    }.items():
        yield case(name, "cif", data, name.replace("_", " "))
    yield case(
        "1ubq_raw",
        "cif",
        (DATA / "1ubq.cif").read_bytes(),
        "all real 1UBQ categories, values and rows",
    )


def packed_record(code, phi=0, psi=0, omega=0, n_ca_c=0, ca_c_n=0, c_n_ca=0):
    return bytes(
        [
            (code << 3) | (omega >> 8),
            omega & 255,
            psi >> 4,
            ((psi & 15) << 4) | (phi >> 8),
            phi & 255,
            ca_c_n,
            c_n_ca,
            n_ca_c,
        ]
    )


def synthetic_fcz(
    codes, anchors=None, oxt=False, title=b"synthetic", indices=(5, 10), varied=False
):
    count = len(codes)
    anchors = [0, count - 1] if anchors is None else anchors
    sidechains = sum(SIDECHAINS[code] for code in codes)
    header = struct.pack(
        "<4HBB2xIBB2xI6f6f",
        count,
        count * 3,
        *indices,
        len(anchors),
        ord("Z"),
        sidechains,
        ord(LETTERS[codes[0]]),
        ord(LETTERS[codes[-1]]),
        len(title),
        -60.0,
        -45.0,
        180.0,
        111.0,
        116.0,
        121.0,
        *([0.0] * 6),
    )
    assert len(header) == 72
    output = bytearray(b"FCMP" + header)
    output.extend(struct.pack(f"<{len(anchors)}i", *anchors))
    output.extend(title)
    for index in anchors:
        # Non-collinear N/CA/C anchor positions with nonzero starting indices.
        x = index * 3.5
        output.extend(struct.pack("<9f", x, 0, 0, x + 1.45, 0, 0, x + 1.95, 1.4, 0.2))
    output.extend(struct.pack("<B3f", oxt, 9.0, 8.0, 7.0))
    for code in codes:
        output.extend(
            packed_record(code, 0xABC, 0xDEF, 0x567, 0x89, 0xAB, 0xCD)
            if varied
            else packed_record(code)
        )
    output.extend(bytes((i * 31) % 256 for i in range(sidechains)))
    output.extend(struct.pack("<2f", 7.5, 0.125))
    output.extend(bytes((i * 255) // (count - 1) for i in range(count)))
    return bytes(output)


def fcz_cases():
    for code in range(32):
        yield case(
            f"residue_code_{code:02}",
            "fcz",
            synthetic_fcz([code, code]),
            f"5-bit residue code {code}, sidechain count, full atoms, numbering, B factors",
        )
    for name, payload, coverage in [
        ("oxt", synthetic_fcz([0, 7], oxt=True), "terminal OXT identity/numbering"),
        (
            "packed_bits",
            synthetic_fcz([0, 7], varied=True),
            "nonzero packed angle bit fields",
        ),
        (
            "multi_anchor",
            synthetic_fcz([0, 7, 14, 17, 19], anchors=[0, 2, 4]),
            "three anchors, two corrected segments",
        ),
        (
            "adjacent_anchors",
            synthetic_fcz([7, 0, 17], anchors=[0, 1, 2]),
            "adjacent segments",
        ),
        (
            "title_nul",
            synthetic_fcz([0, 7], title=b"before\0after"),
            "CStr title truncation",
        ),
        (
            "title_utf8",
            synthetic_fcz([0, 7], title="caf\u00e9".encode()),
            "UTF-8 title",
        ),
        (
            "title_invalid_utf8",
            synthetic_fcz([0, 7], title=b"\xff"),
            "invalid title UTF-8",
        ),
        ("title_empty", synthetic_fcz([0, 7], title=b""), "empty title"),
        (
            "1ubq",
            (DATA / "1ubq.fcz").read_bytes(),
            "real multi-anchor 1UBQ, full atoms and B factors",
        ),
    ]:
        yield case(name, "fcz", payload, coverage)
    good = synthetic_fcz([0, 7])
    coordinate = 76 + 8 + len(b"synthetic")
    backbone = coordinate + 72 + 13
    mutations = {
        "bad_magic": (0, b"NOPE"),
        "one_residue": (4, struct.pack("<H", 1)),
        "too_few_declared_atoms": (6, struct.pack("<H", 5)),
        "anchor_count_one": (12, b"\x01"),
        "anchor_count_too_large": (12, b"\x03"),
        "sidechain_count": (16, struct.pack("<I", 0)),
        "first_residue_mismatch": (20, b"V"),
        "last_residue_mismatch": (21, b"V"),
        "title_length_overflow": (24, struct.pack("<I", 0xFFFFFFFF)),
        "nonfinite_min": (28, struct.pack("<f", float("nan"))),
        "nonfinite_factor": (52, struct.pack("<f", float("inf"))),
        "first_anchor_nonzero": (76, struct.pack("<i", 1)),
        "first_anchor_negative": (76, struct.pack("<i", -1)),
        "anchors_duplicate": (80, struct.pack("<i", 0)),
        "last_anchor_out_of_range": (80, struct.pack("<i", 2)),
        "nonfinite_anchor": (coordinate, struct.pack("<f", float("nan"))),
        "invalid_oxt_flag": (coordinate + 72, b"\x02"),
        "nonfinite_oxt": (coordinate + 73, struct.pack("<f", float("inf"))),
        "nonfinite_bfactor_min": (backbone + 16 + 3, struct.pack("<f", float("nan"))),
        "nonfinite_bfactor_factor": (
            backbone + 16 + 3 + 4,
            struct.pack("<f", float("inf")),
        ),
    }
    for name, (offset, value) in mutations.items():
        mutated = bytearray(good)
        mutated[offset : offset + len(value)] = value
        yield case(name, "fcz", bytes(mutated), "malformed " + name.replace("_", " "))
    # Every byte boundary of a small valid record, including zero length.
    for size in range(len(good)):
        yield case(
            f"truncated_{size:03}",
            "fcz",
            good[:size],
            "truncation at every byte boundary",
        )
    yield case("trailing_byte", "fcz", good + b"\0", "exact section exhaustion")
    fewer_sidechains = bytearray(good)
    struct.pack_into("<I", fewer_sidechains, 16, 2)
    del fewer_sidechains[backbone + 16]
    yield case(
        "sidechain_count_valid_length",
        "fcz",
        bytes(fewer_sidechains),
        "length fits but residue codes require more sidechain bytes",
    )
    for name, chain in [("chain_nul", 0), ("chain_invalid_utf8", 255)]:
        modified = bytearray(good)
        modified[13] = chain
        yield case(name, "fcz", bytes(modified), "CStr chain byte interpretation")
    premature = bytearray(synthetic_fcz([0, 7, 0]))
    struct.pack_into("<i", premature, 80, 1)
    yield case(
        "last_anchor_not_final",
        "fcz",
        bytes(premature),
        "increasing in-range last anchor must equal final residue",
    )
    yield case(
        "declared_atom_limit",
        "fcz",
        good,
        "header count exceeds max_atoms",
        max_atoms=5,
    )
    yield case(
        "reconstructed_atom_limit",
        "fcz",
        good,
        "header fits but reconstructed full atoms exceed max_atoms",
        max_atoms=6,
    )


def database_cases():
    database = (DATA / "example_db").read_bytes()
    for line in (DATA / "example_db.index").read_text().splitlines():
        key, offset, size = map(int, line.split())
        payload = database[offset : offset + size]
        assert payload[-1] == 0
        yield case(
            f"database_{key}",
            "fcz",
            payload[:-1],
            "official database record, selected range excluding terminator",
        )


def all_cases():
    return [*cif_cases(), *fcz_cases(), *database_cases()]
