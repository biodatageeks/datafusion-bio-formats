"""Regenerate offline structure goldens using pinned independent implementations.

Run with --check to reject input, output, or dependency drift. Install requirements.txt
in a separate venv; the production extension is deliberately never imported here.
"""
import argparse
import hashlib
import importlib.metadata
import json
import math
from pathlib import Path

import foldcomp
import gemmi
import numpy as np
from Bio.PDB.vectors import Vector, calc_angle, calc_dihedral

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "testing/data/structure"
OUTPUT = Path(__file__).resolve().parent
VERSIONS = {"gemmi": "0.7.5", "biopython": "1.88", "foldcomp": "1.0.0", "numpy": "2.5.3"}
for package, version in VERSIONS.items():
    assert importlib.metadata.version(package) == version, (package, version)


def atoms(structure):
    return [
        {"model_id": m.num, "chain_id": c.name, "auth_seq_id": str(r.seqid.num),
         "insertion_code": r.seqid.icode.strip() or None, "residue_name": r.name,
         "atom_name": a.name, "alt_id": a.altloc if a.altloc != "\x00" else None,
         "position": [a.pos.x, a.pos.y, a.pos.z]}
        for m in structure for c in m for r in c for a in r
    ]


def geometry(structure):
    rows = []
    for model in structure:
        for chain in model:
            residues = [r for r in chain if gemmi.find_tabulated_residue(r.name).is_amino_acid()]
            for i, r in enumerate(residues):
                def pos(res, name):
                    return next((a.pos for a in res if a.name == name), None)
                n, ca, c = [pos(r, name) for name in ["N", "CA", "C"]]
                prev = pos(residues[i - 1], "C") if i else None
                nxt = pos(residues[i + 1], "N") if i + 1 < len(residues) else None
                next_ca = pos(residues[i + 1], "CA") if i + 1 < len(residues) else None
                if prev is not None and prev.dist(n) > 1.8:
                    prev = None
                if nxt is not None and c.dist(nxt) > 1.8:
                    nxt = next_ca = None
                def measure(points):
                    if any(p is None for p in points):
                        return None
                    if len(points) == 4:
                        val = math.degrees(gemmi.calculate_dihedral(*points))
                        bio = math.degrees(calc_dihedral(*[Vector(p.x, p.y, p.z) for p in points]))
                        val = (val + 180) % 360 - 180
                        assert abs((val - bio + 180) % 360 - 180) < 1e-6
                    else:
                        val = math.degrees(gemmi.calculate_angle(*points))
                        bio = math.degrees(calc_angle(*[Vector(p.x, p.y, p.z) for p in points]))
                        assert abs(val - bio) < 1e-6
                    return val
                rows.append({"auth_seq_id": str(r.seqid.num), "residue_name": r.name,
                    "angles": [measure(p) for p in [(prev,n,ca,c), (n,ca,c,nxt), (ca,c,nxt,next_ca), (n,ca,c), (ca,c,nxt), (prev,n,ca)]],
                    "backbone": [[p.x,p.y,p.z] if p is not None else None for p in [n,ca,c,pos(r,"O")]]})
    return rows


pdb = gemmi.read_structure(str(DATA / "1ubq.pdb"))
cif = gemmi.read_structure(str(DATA / "1ubq.cif"))
assert atoms(pdb) == atoms(cif)
outputs = {"1ubq.atoms.json": atoms(pdb), "1ubq.residues.json": geometry(pdb)}
for filename in ["1ubq.fcz"]:
    payload = (DATA / filename).read_bytes()
    title, decoded = foldcomp.decompress(payload)
    structure = gemmi.read_pdb_string(decoded)
    expected = atoms(structure)
    raw = foldcomp.get_data(payload)["coordinates"]
    assert len(raw) == len(expected)
    for row, coordinate in zip(expected, raw):
        assert np.max(np.abs(np.array(row["position"]) - coordinate)) <= 0.00051
        row["position"] = list(coordinate)
    # Assign the raw coordinates before the independent geometry computation.
    for atom, xyz in zip((a for m in structure for c in m for r in c for a in r), raw):
        atom.pos = gemmi.Position(*xyz)
    outputs[filename + ".json"] = {"title": title, "atoms": expected, "residues": geometry(structure)}
def encode_rows(value):
    if isinstance(value, list):
        return "[\n" + ",\n".join("  " + json.dumps(row, allow_nan=False) for row in value) + "\n]"
    return "{\n" + ",\n".join("  " + json.dumps(key) + ": " + (encode_rows(val) if isinstance(val, list) else json.dumps(val)) for key, val in value.items()) + "\n}"

encoded = {name: (encode_rows(value) + "\n").encode() for name,value in outputs.items()}
sha = lambda data: hashlib.sha256(data).hexdigest()
manifest = {"schema_version": 1, "versions": VERSIONS,
    "sources": {"1ubq": "https://files.rcsb.org/download/1UBQ.pdb", "foldcomp_fixture_commit": "89e37195d3c8ade8d40ead91ad82e6cd2964a967"},
    "inputs": {f.name: sha(f.read_bytes()) for f in sorted(DATA.iterdir()) if f.is_file()},
    "outputs": {name: sha(data) for name,data in encoded.items()},
    "tolerances": {"text_coordinates_angstrom": 1e-9, "angles_circular_degrees": 1e-6, "foldcomp_coordinates_angstrom": 2e-5, "foldcomp_angles_degrees": 0.01}}
encoded["manifest.json"] = (json.dumps(manifest,indent=2) + "\n").encode()
check = argparse.ArgumentParser()
check.add_argument("--check",action="store_true")
args = check.parse_args()
for name,data in encoded.items():
    target = OUTPUT/name
    if args.check:
        assert target.read_bytes() == data, f"oracle drift: {name}"
    else:
        target.write_bytes(data)
print(f"{'Verified' if args.check else 'Wrote'} {len(encoded)} pinned oracle files")
