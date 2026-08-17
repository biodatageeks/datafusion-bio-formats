#!/usr/bin/env python3
"""Generate and benchmark the pinned PGEN parity fixture.

The production Rust crate does not import this file or link pgenlib. It is an
external LGPL/BSD oracle harness used only for conformance and release timing.
"""

from __future__ import annotations

import argparse
import gc
import importlib.metadata
from pathlib import Path
import statistics
import time

import numpy as np

DEFAULT_VARIANTS = 16_384
DEFAULT_SAMPLES = 1_024
DEFAULT_SEED = 2_026_08_16
DEFAULT_MISSING_RATE = 0.005


def generate(prefix: Path, variants: int, samples: int, seed: int) -> None:
    import pgenlib

    prefix.parent.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    with pgenlib.PgenWriter(
        str(prefix.with_suffix(".pgen")).encode(),
        sample_ct=samples,
        variant_ct=variants,
        nonref_flags=False,
        hardcall_phase_present=True,
    ) as writer:
        for start in range(0, variants, 512):
            count = min(512, variants - start)
            alleles = rng.integers(
                0,
                2,
                size=(count, samples * 2),
                dtype=np.int32,
            )
            missing = rng.random((count, samples)) < DEFAULT_MISSING_RATE
            allele_rows = alleles.reshape(count, samples, 2)
            allele_rows[missing] = -9
            writer.append_alleles_batch(alleles, all_phased=True)

    pvar = prefix.with_suffix(".pvar")
    with pvar.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write("#CHROM\tPOS\tID\tREF\tALT\n")
        for variant in range(variants):
            handle.write(f"1\t{variant + 1}\tv{variant}\tA\tC\n")

    psam = prefix.with_suffix(".psam")
    with psam.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write("#IID\n")
        for sample in range(samples):
            handle.write(f"sample{sample}\n")

    print(f"prefix={prefix}")
    print(f"variants={variants}")
    print(f"samples={samples}")
    print(f"seed={seed}")


def benchmark(prefix: Path, iterations: int) -> None:
    import snputils

    if iterations < 10:
        raise ValueError("release parity requires at least 10 iterations")
    path = str(prefix)

    expected_shape = None
    expected_bytes = None
    digest = None
    for _ in range(2):
        result = snputils.read_pgen(
            path,
            genotype_mode="phased",
            fields=["GT"],
            chromosome_ploidy="autosomal",
        ).genotypes
        expected_shape = result.shape
        expected_bytes = result.nbytes
        digest = genotype_digest(result)
        del result
        gc.collect()

    timings = []
    for _ in range(iterations):
        started = time.perf_counter_ns()
        result = snputils.read_pgen(
            path,
            genotype_mode="phased",
            fields=["GT"],
            chromosome_ploidy="autosomal",
        ).genotypes
        elapsed = time.perf_counter_ns() - started
        if result.shape != expected_shape or result.nbytes != expected_bytes:
            raise RuntimeError("oracle output changed across iterations")
        timings.append(elapsed)
        del result
        gc.collect()

    print(f"snputils_version={importlib.metadata.version('snputils')}")
    print(f"pgenlib_version={importlib.metadata.version('pgenlib')}")
    print(f"shape={'x'.join(map(str, expected_shape))}")
    print(f"numpy_bytes={expected_bytes}")
    print(f"digest={digest}")
    print(f"iterations={iterations}")
    print(f"scan_median_ns={int(statistics.median(timings))}")


def genotype_digest(genotypes: np.ndarray) -> str:
    pairs = genotypes.reshape(-1, 2)
    valid = np.all(pairs >= 0, axis=1)
    valid_count = int(np.count_nonzero(valid))
    left_sum = int(np.sum(pairs[valid, 0], dtype=np.uint64))
    right_sum = int(np.sum(pairs[valid, 1], dtype=np.uint64))
    weighted_sum = np.uint64(0)
    for start in range(0, pairs.shape[0], 1_048_576):
        end = min(start + 1_048_576, pairs.shape[0])
        called = valid[start:end]
        values = pairs[start:end].astype(np.uint64, copy=True)
        values[~called] = 0
        weights = np.arange(start + 1, end + 1, dtype=np.uint64)
        weighted_sum += np.sum(
            weights * (values[:, 0] * np.uint64(3) + values[:, 1] * np.uint64(5)),
            dtype=np.uint64,
        )
    return f"{valid_count}:{left_sum}:{right_sum}:{int(weighted_sum)}"


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate")
    generate_parser.add_argument("prefix", type=Path)
    generate_parser.add_argument("--variants", type=int, default=DEFAULT_VARIANTS)
    generate_parser.add_argument("--samples", type=int, default=DEFAULT_SAMPLES)
    generate_parser.add_argument("--seed", type=int, default=DEFAULT_SEED)

    benchmark_parser = subparsers.add_parser("benchmark")
    benchmark_parser.add_argument("prefix", type=Path)
    benchmark_parser.add_argument("--iterations", type=int, default=11)

    arguments = parser.parse_args()
    if arguments.command == "generate":
        generate(arguments.prefix, arguments.variants, arguments.samples, arguments.seed)
    else:
        benchmark(arguments.prefix, arguments.iterations)


if __name__ == "__main__":
    main()
