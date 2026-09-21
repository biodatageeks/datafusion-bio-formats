#!/usr/bin/env python3
"""Isolated, pinned legacy CIF/FCZ oracle. Uses only Python's standard library."""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile

BASELINE = "fd17754c55c63394717967c18b7a45cf8aeb48ee"
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
NATIVE = (
    "datafusion/bio-format-structure/native",
    "datafusion/bio-format-foldcomp/native",
)
SOURCES = (
    "amino_acid.cpp",
    "atom_coordinate.cpp",
    "discretizer.cpp",
    "foldcomp.cpp",
    "nerf.cpp",
    "sidechain.cpp",
    "torsion_angle.cpp",
    "utility.cpp",
)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return (
        json.dumps(value, indent=2, ensure_ascii=True, allow_nan=False) + "\n"
    ).encode()


def run(argv, **kwargs):
    return subprocess.run(argv, check=True, capture_output=True, **kwargs)


def build():
    """Build immutable source objects, never files from a mutable working tree."""
    compiler = shlex.split(os.environ.get("CXX", "c++"))
    if not compiler or shutil.which(compiler[0]) is None:
        raise RuntimeError("set CXX to an available C++17 compiler")
    msvc = Path(compiler[0]).name.lower() in ("cl", "cl.exe")
    if msvc:
        # cl may return an error for the missing source while printing its
        # version. Actual compilation below still requires a successful exit.
        banner = subprocess.run(compiler, capture_output=True, check=False)
        version = (banner.stdout + banner.stderr).decode().strip()
        flags = [
            "/nologo",
            "/std:c++17",
            "/O2",
            "/EHsc",
            "/D_USE_MATH_DEFINES",
            "/FIexception",
        ]
    else:
        version = run([*compiler, "--version"]).stdout.decode().strip()
        flags = ["-std=c++17", "-O2", "-D_USE_MATH_DEFINES", "-include", "exception"]
    archive = run(["git", "archive", BASELINE, *NATIVE], cwd=ROOT).stdout
    driver = (HERE / "reference.cpp").read_bytes()
    metadata = {
        "formats_revision": BASELINE,
        "native_archive_sha256": sha256(archive),
        "driver_sha256": sha256(driver),
        "compiler": version,
        "compiler_command": compiler,
        "flags": flags,
        "platform": platform.platform(),
        "machine": platform.machine(),
    }
    cache = ROOT / "target" / "structure-codec-reference" / sha256(canonical(metadata))
    executable = cache / ("reference.exe" if os.name == "nt" else "reference")
    if executable.is_file():
        return executable, metadata
    cache.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="build-", dir=cache.parent) as temporary:
        checkout = Path(temporary)
        with tarfile.open(fileobj=io.BytesIO(archive)) as source:
            source.extractall(checkout, filter="data")
        (checkout / "reference.cpp").write_bytes(driver)
        vendor = checkout / NATIVE[1] / "vendor"
        includes = [checkout, checkout / NATIVE[0] / "vendor", vendor]
        if os.name == "nt":
            includes.append(vendor / "windows")
        output = checkout / executable.name
        command = [*compiler, *flags]
        for include in includes:
            command.extend(["/I" if msvc else "-I", str(include)])
        command.extend(
            [
                str(checkout / "reference.cpp"),
                *(str(vendor / source) for source in SOURCES),
            ]
        )
        command.extend([f"/Fe{output}"] if msvc else ["-o", str(output)])
        print(f"Building legacy reference at {BASELINE[:12]}", file=sys.stderr)
        try:
            run(command, cwd=checkout, timeout=180)
        except subprocess.CalledProcessError as error:
            sys.stderr.buffer.write(error.stderr)
            raise
        cache.mkdir(exist_ok=True)
        shutil.copy2(output, executable)
        (cache / "build.json").write_bytes(canonical(metadata))
    return executable, metadata


def query(executable, mode, data=None, max_atoms=5_000_000):
    if mode == "tables":
        command = [str(executable), mode]
        return json.loads(run(command, timeout=20).stdout)
    with tempfile.TemporaryDirectory(prefix="structure-reference-input-") as temporary:
        source = Path(temporary) / f"input.{mode}"
        source.write_bytes(data)
        command = [str(executable), mode, str(source)]
        if mode == "fcz":
            command.append(str(max_atoms))
        result = json.loads(run(command, timeout=20).stdout)
    # JSON contains byte-exact raw strings. Match the public adapters' UTF-8
    # conversion separately so invalid exposed strings are not called successes.
    if "error_hex" in result:
        return {
            "status": "error",
            "stage": "parse",
            "message": bytes.fromhex(result["error_hex"]).decode(),
        }
    try:
        if mode == "cif":
            blocks = []
            for block in result["blocks"]:
                blocks.append(
                    {
                        "name": bytes.fromhex(block["name_hex"]).decode(),
                        "columns": {
                            bytes.fromhex(column["name_hex"]).decode(): [
                                None if cell is None else bytes.fromhex(cell).decode()
                                for cell in column["values_hex"]
                            ]
                            for column in block["columns"]
                        },
                    }
                )
            return {"status": "ok", "blocks": blocks}
        bytes.fromhex(result["decoded_title_hex"]).decode()
        for atom in result["atoms"]:
            for value in atom[:3]:
                bytes.fromhex(value).decode()
    except UnicodeDecodeError:
        return {"status": "error", "stage": "view", "message": "invalid UTF-8"}
    return {"status": "ok", **result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("cif", "fcz", "tables", "build"))
    parser.add_argument("input", type=Path, nargs="?")
    parser.add_argument("--max-atoms", type=int, default=5_000_000)
    args = parser.parse_args()
    if args.mode in ("cif", "fcz") and args.input is None:
        parser.error("cif/fcz modes require an input file")
    executable, metadata = build()
    result = (
        metadata
        if args.mode == "build"
        else query(
            executable,
            args.mode,
            args.input.read_bytes() if args.input else None,
            args.max_atoms,
        )
    )
    sys.stdout.buffer.write(canonical(result))


if __name__ == "__main__":
    main()
