#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
BUILD_DIR=$(mktemp -d)
trap 'rm -rf "$BUILD_DIR"' EXIT
CODEC=datafusion/bio-format-foldcomp/native
"${CXX:-c++}" -std=c++17 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer \
  -I "$CODEC/vendor" "$CODEC/codec_bridge.cpp" \
  "$CODEC"/vendor/{amino_acid,atom_coordinate,discretizer,foldcomp,nerf,sidechain,torsion_angle,utility}.cpp \
  testing/native/codec_sanitizer.cpp -o "$BUILD_DIR/codec-check"
"$BUILD_DIR/codec-check" testing/data/structure/1ubq.fcz
