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
open(os.path.join(HERE, "multimember.fastq"), "wb").write(plain)

# (1) clean: member boundary BETWEEN records -> pre-fix returns only first member (40)
cut_clean = plain.index(b"@read40")
open(os.path.join(HERE, "multimember_clean.fastq.gz"), "wb").write(
    gzip.compress(plain[:cut_clean]) + gzip.compress(plain[cut_clean:]))

# (2) split: member boundary MID-RECORD (inside read40 sequence) -> pre-fix CRASHES (UnexpectedEof)
cut_mid = plain.index(b"@read40") + len(b"@read40 simulated\nACGTAC")
open(os.path.join(HERE, "multimember_split.fastq.gz"), "wb").write(
    gzip.compress(plain[:cut_mid]) + gzip.compress(plain[cut_mid:]))

print(f"wrote fixtures with {N} reads")
