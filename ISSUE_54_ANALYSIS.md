# Issue #54: CRAM Reader Fails to Expose MD and NM Alignment Tags

## Root Cause Analysis

### The Problem

The CRAM table provider cannot read MD (mismatch string) and NM (edit distance) tags from CRAM files, even though:
1. The tags are present in the file (verified with `samtools view`)
2. Other tags (RG, MQ, AM, XT, etc.) read correctly
3. The same code works fine for BAM files

### Why This Happens

**CRAM handles MD and NM tags differently than other optional tags:**

According to the [SAM specification](https://samtools.github.io/hts-specs/SAMtags.pdf) and [htslib documentation](http://www.htslib.org/doc/samtools-calmd.html):

1. **MD and NM are "calculated" tags** - they describe differences between reads and reference sequences
2. **CRAM is a reference-based compression format** - it stores features (substitutions, insertions, deletions) rather than full sequences
3. **CRAM recalculates MD/NM on-the-fly by default** - these tags can be derived from alignment features + reference, so storing them explicitly is redundant

From the [samtools-view manual](http://www.htslib.org/doc/samtools-view.html):
> "CRAM recalculates MD and NM tags on the fly, which means in most cases you don't need to store these tags explicitly in CRAM files."

### Library Behavior Differences

Different CRAM implementations handle this differently:

- **samtools/htslib (C)**: Calculates MD/NM on-the-fly during decode if not stored
- **htsjdk (Java)**: Only returns MD/NM if explicitly stored ([Issue #1350](https://github.com/samtools/htsjdk/issues/1350))
- **noodles (Rust)**: Follows htsjdk approach - only returns stored tags

The noodles-cram library (used by this project) does NOT calculate MD/NM tags on-the-fly. It only returns them if they were stored explicitly using:
```bash
samtools view -C --output-fmt-option store_md=1 --output-fmt-option store_nm=1
```

### Why `record.data().get(tag)` Returns None

The `noodles_sam::alignment::Record::data()` interface provides access to optional fields stored in the alignment record. For CRAM:
- Tags like RG, MQ, AM are stored as explicit optional fields → accessible via `data().get()`
- Tags like MD, NM are typically NOT stored → calculated from features + reference
- noodles-cram doesn't provide automatic calculation → `data().get()` returns None

## Solution Implemented

### Phase 1: Calculate MD/NM Tags (COMPLETED)

Created `/datafusion/bio-format-core/src/calculated_tags.rs` with functions to calculate:

1. **`calculate_nm_tag()`** - Calculate edit distance from CIGAR and reference
   - Counts mismatches, insertions, and deletions
   - Works with or without reference sequence (limited accuracy without reference)

2. **`calculate_md_tag()`** - Calculate mismatch descriptor from CIGAR and reference
   - Requires reference sequence
   - Generates format like "10A5^ACG3" (matches, mismatches, deletions)

These implementations follow the [SAM specification](https://samtools.github.io/hts-specs/SAMtags.pdf) for NM and MD tag calculation.

### Phase 2: Integrate with CRAM Reader (PARTIALLY COMPLETED)

Modified `/datafusion/bio-format-cram/src/physical_exec.rs`:

1. Added `reference_seq` parameter to `load_tags()` function
2. Added logic to detect MD/NM tag requests
3. Falls back to calculation when tags not found in `record.data()`

**Current Limitation:** The `load_tags()` function is called with `None` for reference_seq because:
- We need to extract the appropriate reference region for each alignment
- This requires knowing the chromosome and alignment position
- Implementation requires fetching from the reference repository

### Phase 3: Reference Sequence Integration (TODO)

To fully enable MD/NM calculation, we need to:

1. **Pass reference repository to tag loading:**
   ```rust
   // In get_remote_cram_stream and get_local_cram
   load_tags(
       &record,
       &mut tag_builders,
       Some(&reference_region)  // Need to fetch this
   )?;
   ```

2. **Fetch appropriate reference region:**
   ```rust
   // Before loading tags, extract reference sequence
   let ref_seq_id = record.reference_sequence_id();
   let alignment_start = record.alignment_start();
   let alignment_end = record.alignment_end();

   let reference_region = reader.fetch_reference_region(
       ref_seq_id?,
       alignment_start?,
       alignment_end?
   )?;
   ```

3. **Expose reference repository in CramReader:**
   - Add method to fetch reference sequences by region
   - Cache frequently accessed regions for performance

## Testing

### Diagnostic Tool

Created `/datafusion/bio-format-cram/examples/debug_tags.rs` to diagnose tag availability:
```bash
cargo run --example debug_tags --package datafusion-bio-format-cram \
    /path/to/test.cram /path/to/reference.fa
```

This tool:
- Lists all tags available via `record.data()`
- Specifically checks for MD and NM presence
- Helps determine if tags are stored or need calculation

### Expected Behavior After Full Implementation

Once reference integration is complete:

1. **Tags stored in file** → Read directly from `record.data()`
2. **Tags not stored** → Calculate on-the-fly from features + reference
3. **No reference available** → Return NULL (or partial calculation for NM)

## References

- [SAM Optional Fields Specification](https://samtools.github.io/hts-specs/SAMtags.pdf)
- [samtools-calmd Manual](http://www.htslib.org/doc/samtools-calmd.html)
- [samtools-view Manual](http://www.htslib.org/doc/samtools-view.html)
- [htsjdk Issue #1350](https://github.com/samtools/htsjdk/issues/1350) - CRAM MD/NM tags
- [hts-specs Issue #453](https://github.com/samtools/hts-specs/issues/453) - Clarify NM/MD in CRAM
- [CRAM 3.1 Specification](https://samtools.github.io/hts-specs/CRAMv3.pdf)

## Next Steps

1. **Implement reference region fetching** in `CramReader`
2. **Pass reference sequences** to `load_tags()` calls
3. **Add integration tests** with CRAM files that:
   - Have MD/NM stored explicitly
   - Omit MD/NM (requiring calculation)
4. **Document limitations** when reference is unavailable
5. **Consider caching** for performance with large files

## Alternative Approaches

If reference integration proves complex, consider:

1. **Document the limitation** - make it clear MD/NM require stored tags
2. **Provide helper utility** - tool to add MD/NM to CRAM files before querying
3. **Support BAM as workaround** - BAM always stores optional tags explicitly

## Build Status

✅ Core module compiles successfully
✅ CRAM module compiles successfully
⚠️ Reference integration pending
⚠️ Integration tests needed
