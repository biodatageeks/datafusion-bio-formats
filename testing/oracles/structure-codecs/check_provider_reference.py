#!/usr/bin/env python3
"""Run explicitly selected provider comparisons against an external legacy process."""

import os
import subprocess

from reference import ROOT, build


def main():
    executable, _ = build()
    subprocess.run(
        [
            "cargo",
            "test",
            "--locked",
            "--release",
            "--lib",
            "-p",
            "datafusion-bio-format-structure",
            "-p",
            "datafusion-bio-format-foldcomp",
            "external_reference",
            "--",
            "--ignored",
        ],
        cwd=ROOT,
        env={**os.environ, "BIO_CODEC_REFERENCE": str(executable)},
        check=True,
    )


if __name__ == "__main__":
    main()
