"""Regression checks for selecting platform-specific frozen stress outputs."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

from foldcomp_stress import arithmetic_profile, check_observations, source_sha256
from reference import HERE


class StressProfiles(unittest.TestCase):
    def setUp(self):
        self.manifest = json.loads((HERE / "fcz-stress/manifest.json").read_bytes())
        self.shared = {
            key: value for key, value in self.manifest.items() if key != "profiles"
        }

    def test_each_measured_profile_accepts_its_own_output(self):
        for profile, observed in self.manifest["profiles"].items():
            with self.subTest(profile=profile):
                self.assertEqual(arithmetic_profile(observed["reference"]), profile)
                check_observations(
                    self.manifest, self.shared, profile, observed["outputs"]
                )

    def test_linux_does_not_accept_macos_hashes(self):
        outputs = self.manifest["profiles"]["macos-apple-clang-aarch64"]["outputs"]
        with self.assertRaisesRegex(ValueError, "output hashes changed"):
            check_observations(
                self.manifest, self.shared, "linux-gnu-gcc-x86_64", outputs
            )

    def test_input_changes_still_fail_with_matching_platform_outputs(self):
        changed = copy.deepcopy(self.shared)
        changed["cases"][0]["input_sha256"] = "0" * 64
        profile = "macos-apple-clang-aarch64"
        with self.assertRaisesRegex(ValueError, "fixture/provenance"):
            check_observations(
                self.manifest,
                changed,
                profile,
                self.manifest["profiles"][profile]["outputs"],
            )

    def test_unknown_profile_cannot_fall_back_to_a_measured_one(self):
        with self.assertRaisesRegex(ValueError, "No frozen stress outputs"):
            check_observations(self.manifest, self.shared, "unknown", [])
        metadata = copy.deepcopy(
            self.manifest["profiles"]["linux-gnu-gcc-x86_64"]["reference"]
        )
        metadata["platform"] = "Linux-x86_64-with-musl"
        with self.assertRaisesRegex(ValueError, "uncharacterized native toolchain"):
            arithmetic_profile(metadata)

    def test_source_line_endings_do_not_change_fingerprints(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source"
            path.write_bytes(b"first\nsecond\n")
            expected = source_sha256(path)
            path.write_bytes(b"first\r\nsecond\r\n")
            self.assertEqual(source_sha256(path), expected)


if __name__ == "__main__":
    unittest.main()
