"""Deterministic multi-member gzip fixtures for the non-FASTQ readers (issue #207).

Mirrors datafusion/bio-format-fastq/data/gen_multimember_fixtures.py but emits one
set of fixtures per format crate (bed/fasta/gff/vcf/gtf/pairs) into that crate's
`tests/` directory. Run once; commit the generated `.gz` files.

    python3 scripts/gen_multimember_gz_fixtures.py

For each format three fixtures are written, all of which decode to the SAME full
content once every gzip member is read:

  * multimember_clean.<ext>.gz  -- member boundary BETWEEN records.
        Pre-fix (first-member-only decode) returns just the first member.
  * multimember_split.<ext>.gz  -- member boundary MID-record.
        Pre-fix crashes (UnexpectedEof) or truncates.
  * multimember_pigz.<ext>.gz   -- real pigz output, 4 concatenated members
        (the `cat a.gz b.gz` pattern). Skipped if pigz is not on PATH.

gzip.compress(..., mtime=0) and `pigz -n` keep the bytes reproducible.
"""

import gzip
import os
import shutil
import subprocess

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

N = 100              # records in the clean/split fixtures
FIRST_MEMBER = 40    # records before the member boundary
PIGZ_N = 2000        # records in the pigz fixture
PIGZ_CHUNKS = 4      # -> 4 pigz members of 500 records each

SEQ = "ACGTACGTACGTACGTACGT"  # 20 bp, reused for fasta


def bed_records(n):
    # BED4 (chrom, start, end, name) -- the variant the reader implements.
    return [f"chr1\t{i * 100}\t{i * 100 + 50}\tfeature{i}\n" for i in range(n)]


def fasta_records(n):
    return [f">seq{i} desc{i}\n{SEQ}\n" for i in range(n)]


def gff_records(n):
    return [
        f"chr1\tsrc\tgene\t{1000 + i * 100}\t{1050 + i * 100}\t.\t+\t.\tID=gene{i}\n"
        for i in range(n)
    ]


def vcf_records(n):
    return [f"chr1\t{100 + i}\t.\tA\tT\t50.0\tPASS\t.\n" for i in range(n)]


def gtf_records(n):
    return [
        f'chr1\ttest\tgene\t{100 + i * 100}\t{150 + i * 100}\t.\t+\t.\tgene_id "G{i}";\n'
        for i in range(n)
    ]


def pairs_records(n):
    return [f"read{i}\tchr1\t{1000 + i}\tchr1\t{5000 + i}\t+\t-\n" for i in range(n)]


# (crate dir, file extension, header text emitted once before the first record, record fn)
FORMATS = [
    ("bio-format-bed", "bed", "", bed_records),
    ("bio-format-fasta", "fasta", "", fasta_records),
    ("bio-format-gff", "gff3", "##gff-version 3\n", gff_records),
    (
        "bio-format-vcf",
        "vcf",
        "##fileformat=VCFv4.3\n"
        "##contig=<ID=chr1,length=249250621>\n"
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n",
        vcf_records,
    ),
    ("bio-format-gtf", "gtf", "", gtf_records),
    (
        "bio-format-pairs",
        "pairs",
        "## pairs format v1.0\n#columns: readID chr1 pos1 chr2 pos2 strand1 strand2\n",
        pairs_records,
    ),
]


def write_multimember(path, *members):
    """Concatenate independently gzip-compressed members (reproducible mtime=0)."""
    with open(path, "wb") as out:
        for m in members:
            out.write(gzip.compress(m, mtime=0))


def main():
    have_pigz = shutil.which("pigz") is not None
    for crate, ext, header, rec_fn in FORMATS:
        tests_dir = os.path.join(ROOT, "datafusion", crate, "tests")
        os.makedirs(tests_dir, exist_ok=True)

        records = rec_fn(N)
        plain = (header + "".join(records)).encode()
        cut_clean = len((header + "".join(records[:FIRST_MEMBER])).encode())
        # mid-record: halfway into the record at the boundary
        rec_bytes = records[FIRST_MEMBER].encode()
        cut_split = cut_clean + max(1, len(rec_bytes) // 2)

        clean_path = os.path.join(tests_dir, f"multimember_clean.{ext}.gz")
        split_path = os.path.join(tests_dir, f"multimember_split.{ext}.gz")
        write_multimember(clean_path, plain[:cut_clean], plain[cut_clean:])
        write_multimember(split_path, plain[:cut_split], plain[cut_split:])

        # By construction the first member holds only FIRST_MEMBER records, so a
        # first-member-only decoder truncates to that and a multi-member decoder
        # recovers all N. (Python's gzip.decompress reads every member, so it is
        # not a faithful stand-in for the buggy single-member decoders.)
        assert FIRST_MEMBER < N

        if have_pigz:
            records2 = rec_fn(PIGZ_N)
            per = PIGZ_N // PIGZ_CHUNKS
            pigz_path = os.path.join(tests_dir, f"multimember_pigz.{ext}.gz")
            with open(pigz_path, "wb") as out:
                for k in range(PIGZ_CHUNKS):
                    chunk = (header if k == 0 else "") + "".join(
                        records2[k * per:(k + 1) * per]
                    )
                    out.write(
                        subprocess.run(
                            ["pigz", "-n", "-c"],
                            input=chunk.encode(),
                            capture_output=True,
                            check=True,
                        ).stdout
                    )
            print(f"{crate}: wrote clean/split (first member {FIRST_MEMBER} recs) + pigz ({PIGZ_N})")
        else:
            print(f"{crate}: wrote clean/split (first member {FIRST_MEMBER} recs); pigz SKIPPED")

    if not have_pigz:
        print("pigz not found -- committed pigz fixtures are reused by tests")


if __name__ == "__main__":
    main()
