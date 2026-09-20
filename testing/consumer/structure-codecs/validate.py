#!/usr/bin/env python3
"""Prepare isolated consumer sources and test an installed candidate wheel."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
CONSUMER = "ea24d4a2a59d7c73e2d6c36b3ef0d5168276e0fa"
BASELINE = "fd17754c55c63394717967c18b7a45cf8aeb48ee"
TESTS = [
    "test_io_structure.py",
    "test_io_foldcomp.py",
    "test_msa_io.py",
    "test_source_metadata.py",
    "test_comprehensive_metadata.py",
    "test_coordinate_system_metadata.py",
    "test_lazyframe_partitioning.py",
    "test_pushdown_equivalence.py",
]


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args])


def sha(data):
    return hashlib.sha256(data).hexdigest()


def prepare(args):
    consumer, formats = args.consumer.resolve(), args.formats.resolve()
    if git(consumer, "rev-parse", "HEAD").decode().strip() != CONSUMER:
        raise RuntimeError("consumer must be the frozen validation revision")
    for root in [consumer, formats]:
        if git(root, "diff", "HEAD", "--"):
            raise RuntimeError(
                f"prepare requires an unmodified isolated checkout: {root}"
            )
    before = git(formats, "rev-parse", "HEAD").decode().strip()
    if args.candidate:
        patch = HERE / "candidate-routing.patch"
        subprocess.run(
            ["git", "-C", str(formats), "apply", "--check", str(patch)], check=True
        )
        subprocess.run(["git", "-C", str(formats), "apply", str(patch)], check=True)
    path = consumer / "Cargo.toml"
    text = path.read_text()
    pattern = re.compile(
        r'(datafusion-bio-format-([a-z]+)) = \{ git = "https://github.com/biodatageeks/datafusion-bio-formats.git", rev = "'
        + BASELINE
        + r'" \}'
    )
    text, count = pattern.subn(
        lambda match: (
            f"{match[1]} = {{ path = {json.dumps((formats / 'datafusion' / ('bio-format-' + match[2])).as_posix())} }}"
        ),
        text,
    )
    if count != 17:
        raise RuntimeError(f"expected 17 coordinated formats overrides, got {count}")
    path.write_text(text)
    lock = consumer / "Cargo.lock"
    source = f'source = "git+https://github.com/biodatageeks/datafusion-bio-formats.git?rev={BASELINE}#{BASELINE}"\n'
    text = lock.read_text()
    if text.count(source) != 17:
        raise RuntimeError("unexpected frozen formats lock entries")
    lock.write_text(text.replace(source, ""))
    args.output.mkdir(parents=True, exist_ok=True)
    report = {
        "consumer_revision": CONSUMER,
        "formats_revision": before,
        "backend": "rust-candidate" if args.candidate else "native",
        "candidate_routing_patch_sha256": sha(
            (HERE / "candidate-routing.patch").read_bytes()
        )
        if args.candidate
        else None,
        "cargo_lock_sha256": sha(lock.read_bytes()),
        "consumer_diff_sha256": sha(git(consumer, "diff", "HEAD", "--")),
        "formats_diff_sha256": sha(git(formats, "diff", "HEAD", "--")),
        "build_scope": "runtime validation only; native build inputs remain; not a C++-free distribution",
    }
    (args.output / "source-provenance.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )


def installed_tests(args):
    import polars_bio
    import polars_bio.polars_bio as extension

    package = Path(polars_bio.__file__).resolve()
    if "site-packages" not in package.parts:
        raise RuntimeError(f"tests must import installed wheel: {package}")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="codec-installed-") as directory:
        destination = Path(directory) / "tests"
        destination.mkdir()
        source = args.consumer.resolve() / "tests"
        for path in source.glob("*.py"):
            shutil.copy2(path, destination / path.name)
        shutil.copytree(source / "data", destination / "data")
        command = [
            sys.executable,
            "-I",
            "-m",
            "pytest",
            "-q",
            "-ra",
            *[str(destination / test) for test in TESTS],
            "--junitxml",
            str(output / "installed-tests.xml"),
        ]
        with (output / "installed-tests.log").open("w") as log:
            result = subprocess.run(
                command, cwd=directory, stdout=log, stderr=subprocess.STDOUT
            )
        cases = list(ET.parse(output / "installed-tests.xml").iter("testcase"))
        report = {
            "package": str(package),
            "extension": str(Path(extension.__file__).resolve()),
            "extension_sha256": sha(Path(extension.__file__).read_bytes()),
            "outside_checkout": directory,
            "test_files": TESTS,
            "exit_code": result.returncode,
            "cases": len(cases),
            "skips": [
                {
                    "test": case.get("name"),
                    "reason": case.find("skipped").get("message"),
                }
                for case in cases
                if case.find("skipped") is not None
            ],
        }
        (output / "installed-tests.json").write_text(
            json.dumps(report, indent=2) + "\n"
        )
        print((output / "installed-tests.log").read_text())
        if result.returncode:
            raise SystemExit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["prepare", "test"])
    parser.add_argument("--consumer", type=Path, required=True)
    parser.add_argument("--formats", type=Path)
    parser.add_argument("--candidate", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        if not args.formats:
            parser.error("prepare requires --formats")
        prepare(args)
    else:
        installed_tests(args)


if __name__ == "__main__":
    main()
