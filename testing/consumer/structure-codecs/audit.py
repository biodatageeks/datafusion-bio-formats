#!/usr/bin/env python3
"""Audit codec crate packages and built Python distributions after native removal."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import zipfile


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--formats", type=Path, required=True)
    parser.add_argument("--distribution", type=Path, action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    formats = args.formats.resolve()
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--locked", "--no-deps", "--format-version=1"],
            cwd=formats,
        )
    )
    report = {"crates": [], "distributions": []}
    for kind in ("structure", "foldcomp"):
        name = f"datafusion-bio-format-{kind}"
        package = next(p for p in metadata["packages"] if p["name"] == name)
        assert not any("custom-build" in t["kind"] for t in package["targets"]), name
        assert not any(d["kind"] == "build" for d in package["dependencies"]), name
        files = subprocess.check_output(
            ["cargo", "package", "--list", "--allow-dirty", "-p", name],
            cwd=formats,
            text=True,
        ).splitlines()
        forbidden = [
            p
            for p in files
            if p.startswith("native/")
            or Path(p).suffix in (".cpp", ".hpp", ".h")
            or p == "build.rs"
        ]
        assert not forbidden, (name, forbidden)
        assert "LICENSE-APACHE" in files, name
        if kind == "foldcomp":
            assert "src/fcz/LICENSE-FOLDCOMP" in files and "NOTICE" in files
            assert package["license"] == "Apache-2.0 AND MIT"
        report["crates"].append(
            {"name": name, "license": package["license"], "package_files": files}
        )
    expected_mit = (
        formats / "datafusion/bio-format-foldcomp/src/fcz/LICENSE-FOLDCOMP"
    ).read_bytes()
    for path in args.distribution:
        if path.suffix == ".whl":
            with zipfile.ZipFile(path) as archive:
                entries = {
                    name: archive.read(name)
                    for name in archive.namelist()
                    if not name.endswith("/")
                }
        else:
            with tarfile.open(path) as archive:
                entries = {
                    member.name: archive.extractfile(member).read()
                    for member in archive.getmembers()
                    if member.isfile()
                }
        notices = {
            name: content
            for name, content in entries.items()
            if "/licenses/structure/" in name
        }
        assert {Path(name).name for name in notices} == {
            "README.md",
            "FOLDCOMP-LICENSE.txt",
        }, notices.keys()
        assert (
            next(
                content
                for name, content in notices.items()
                if name.endswith("FOLDCOMP-LICENSE.txt")
            )
            == expected_mit
        )
        forbidden = [
            name
            for name in entries
            if any(
                f"bio-format-{kind}/native/" in name
                or name.endswith(f"bio-format-{kind}/build.rs")
                for kind in ("structure", "foldcomp")
            )
        ]
        assert not forbidden, forbidden
        report["distributions"].append(
            {
                "file": path.name,
                "sha256": digest(path.read_bytes()),
                "notices": {name: digest(content) for name, content in notices.items()},
                "codec_native_inputs": forbidden,
            }
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        f"Audited {len(report['crates'])} codec packages and {len(report['distributions'])} distributions"
    )


if __name__ == "__main__":
    main()
