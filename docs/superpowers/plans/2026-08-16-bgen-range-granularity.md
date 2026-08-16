# BGEN payload range granularity

Follow-up to [#226](https://github.com/biodatageeks/datafusion-bio-formats/pull/226),
which measured this limitation and deliberately left it alone.

## The problem

`plan_payload_partitions` capped each coalesced payload range at
`payload_bytes / target_partitions` — one partition's byte share. A variant's
payload is indivisible, so that cap hands the scan `target_partitions + 1`
chunks, and `target + 1` chunks never divide evenly into `target` partitions:
one partition always takes two and becomes the bottleneck.

Planned byte shares before, on `chr22.first-25000.unphased.bgen`:

| Target | Shares | Busiest vs fair |
| --- | --- | --- |
| 2 | **87.2%**, 12.8% | 1.74x |
| 4 | 43.6%, 21.8%, 21.8%, 12.9% | 1.74x |
| 8 | 21.8%, 10.9% x5, 21.8%, 2.0% | 1.74x |

1 / 0.872 = 1.15, which was exactly the measured two-partition speedup.

## The change

Aim for `PAYLOAD_RANGES_PER_PARTITION = 4` ranges per partition instead of one,
so the busiest partition carries at most one extra range — `(k + 1) / k`, a 1.25x
share rather than 1.74x.

`MIN_PAYLOAD_RANGE_BYTES = 256 KiB` stops the split asking for object reads far
under a useful size. It is capped at one partition's share so it can never
starve a partition: a file smaller than the floor is still divided across the
requested partitions rather than collapsing into a single range, which is the
collapse the original cap existed to prevent and which
`coalescing_bridges_metadata_gaps_without_collapsing_partitions` guards.

No new public API. A caller who wants something else still has
`max_range_bytes`, and an explicit value below the floor still wins.

### Why 256 KiB

Measured on the 4.9 MB slice at eight partitions, Rust scan only, varying only
the cap:

| Cap | Time | Speedup vs 1 partition |
| --- | --- | --- |
| 128 KiB | 147.76 ms | 5.09x |
| **256 KiB** | 166.86 ms | **4.65x** |
| 512 KiB | 209.94 ms | 3.68x |
| 1 MiB | 208.32 ms | 3.71x |
| one partition's share (before) | 204.37 ms | 3.64x |

256 KiB keeps 91% of the available balance at half the requests of 128 KiB.

## Result

Rust scan, `chr22.first-25000.unphased.bgen`, probability output, fixed layout:

| Partitions | Before | After | Speedup before | Speedup after |
| --- | --- | --- | --- | --- |
| 1 | 743.97 ms | 755.58 ms | 1.00x | 1.00x |
| 2 | 641.80 ms | **437.36 ms** | 1.16x | **1.73x** |
| 4 | 354.23 ms | **235.96 ms** | 2.10x | **3.20x** |
| 8 | 204.37 ms | **163.09 ms** | 3.64x | **4.63x** |

Dosage on the same file improves in step: 157.76 ms to 132.82 ms at eight
partitions, a 4.54x speedup against 3.75x. The phased fixture's fixed-layout
probability read goes from 222.67 ms to 167.94 ms.

One partition is unchanged, which is the point: this is balance, not throughput.

## Cost

More object reads. For the 4.9 MB slice at eight partitions the plan goes from 9
coalesced ranges to about 19; for the 160 MB chromosome, from about 10 to about
32. Each is still a bulk sequential read of at least 256 KiB, and the count stays
bounded at roughly four per partition rather than growing with the variant count.
That trade is why the floor exists and why it is not smaller.

## Correctness

Element-wise against the `bgen` package, no tolerance, after the change:

| Comparison | Cells | Differing |
| --- | --- | --- |
| probabilities, phased fixture, fixed layout, 1 / 2 / 4 / 8 partitions | 254,800,000 each | 0 |
| dosage, unphased fixture, 8 partitions | 63,700,000 | 0 |

`bitwise_differences` is 0 in every run as well. Re-planning which bytes each
partition reads changes no emitted value.
