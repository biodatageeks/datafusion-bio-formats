"""Deterministic multi-member gzip FASTQ fixtures for regression tests.
Run once; commit the .fastq and .fastq.gz outputs. Reproducible (no randomness).

    python3 datafusion/bio-format-fastq/data/gen_multimember_fixtures.py
"""
import gzip, os

N = 100
SEQ = "ACGTACGTACGTACGTACGT"   # 20 bp
QUAL = "IIIIIIIIIIIIIIIIIIII"  # 20 q
HERE = os.path.dirname(os.path.abspath(__file__))

reads = [f"@read{i} simulated\n{SEQ}\n+\n{QUAL}\n" for i in range(N)]
plain = "".join(reads).encode()

# (1) clean: member boundary BETWEEN records -> pre-fix returns only first member (40)
cut_clean = plain.index(b"@read40")
open(os.path.join(HERE, "multimember_clean.fastq.gz"), "wb").write(
    gzip.compress(plain[:cut_clean]) + gzip.compress(plain[cut_clean:]))

# (2) split: member boundary MID-RECORD (inside read40 sequence) -> pre-fix CRASHES (UnexpectedEof)
cut_mid = plain.index(b"@read40") + len(b"@read40 simulated\nACGTAC")
open(os.path.join(HERE, "multimember_split.fastq.gz"), "wb").write(
    gzip.compress(plain[:cut_mid]) + gzip.compress(plain[cut_mid:]))

# (3) real tool-produced multi-member gzip: concatenate pigz-compressed, record-aligned
# chunks -- mirrors the common `cat lane1.fastq.gz lane2.fastq.gz ...` pattern.
# pigz -n strips name/mtime for reproducible bytes. Requires pigz on PATH; skipped otherwise.
import shutil, subprocess
if shutil.which("pigz"):
    PIGZ_N = 2000          # reads in the pigz fixture
    PIGZ_CHUNKS = 4        # -> 4 gzip members of 500 reads each
    preads = [f"@read{i} simulated\n{SEQ}\n+\n{QUAL}\n" for i in range(PIGZ_N)]
    per = PIGZ_N // PIGZ_CHUNKS
    with open(os.path.join(HERE, "multimember_pigz.fastq.gz"), "wb") as out:
        for k in range(PIGZ_CHUNKS):
            chunk = "".join(preads[k * per:(k + 1) * per]).encode()
            out.write(subprocess.run(["pigz", "-n", "-c"], input=chunk,
                                     capture_output=True, check=True).stdout)
    print(f"wrote multimember_pigz.fastq.gz ({PIGZ_CHUNKS} pigz members, {PIGZ_N} reads)")
else:
    print("pigz not found -- skipped multimember_pigz.fastq.gz (committed binary is used by tests)")

print(f"wrote synthetic fixtures with {N} reads")
