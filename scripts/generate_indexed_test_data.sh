#!/usr/bin/env bash
#
# Generate small, multi-chromosome indexed test files for integration tests.
# Source: WES dataset (NA12878) and Ensembl VCF.
# Target: <500KB each, committed to git.
#
set -euo pipefail

# --- Configuration ---
WES_BAM="/Users/mwiewior/research/data/WES/NA12878.proper.wes.md.bam"
WES_CRAM="/Users/mwiewior/research/data/WES/NA12878.proper.wes.md.cram"
REFERENCE="/Users/mwiewior/research/data/references/Homo_sapiens_assembly38.fasta"

SAMTOOLS="/opt/homebrew/bin/samtools"
BCFTOOLS="/opt/homebrew/bin/bcftools"
TABIX="/opt/homebrew/bin/tabix"

# Regions: small WES target regions across 3 chromosomes
REGIONS="chr1:100000000-100005000 chr2:100000000-100005000 chrX:100000000-100000500"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BAM_TESTS="${REPO_ROOT}/datafusion/bio-format-bam/tests"
CRAM_TESTS="${REPO_ROOT}/datafusion/bio-format-cram/tests"
VCF_TESTS="${REPO_ROOT}/datafusion/bio-format-vcf/tests"

mkdir -p "$BAM_TESTS" "$CRAM_TESTS" "$VCF_TESTS"

echo "=== Generating BAM test data ==="
# Extract reads for 3 regions with a filtered header (only chr1, chr2, chrX as @SQ).
# This prevents empty partitions for unused contigs in indexed CRAM reads.
{
  $SAMTOOLS view -H "$WES_BAM" | \
    awk -F'\t' 'BEGIN{OFS="\t"} /^@SQ/{if($2=="SN:chr1" || $2=="SN:chr2" || $2=="SN:chrX") print; next} {print}'
  $SAMTOOLS view "$WES_BAM" $REGIONS
} | $SAMTOOLS sort -o "${BAM_TESTS}/multi_chrom.bam" -
$SAMTOOLS index "${BAM_TESTS}/multi_chrom.bam"

echo "BAM file:"
ls -lh "${BAM_TESTS}/multi_chrom.bam" "${BAM_TESTS}/multi_chrom.bam.bai"
echo "Per-chromosome counts:"
for region in $REGIONS; do
  chrom="${region%%:*}"
  count=$($SAMTOOLS view -c "${BAM_TESTS}/multi_chrom.bam" "$region")
  echo "  $chrom: $count reads"
done
total=$($SAMTOOLS view -c "${BAM_TESTS}/multi_chrom.bam")
echo "  Total: $total reads"

echo ""
echo "=== Generating CRAM test data (no external reference) ==="
# Convert BAM subset to CRAM using no_ref mode (reference-free)
# This avoids reference mismatch issues and makes tests portable
$SAMTOOLS view -C --output-fmt-option no_ref=1 \
  -o "${CRAM_TESTS}/multi_chrom.cram" \
  "${BAM_TESTS}/multi_chrom.bam"
$SAMTOOLS index "${CRAM_TESTS}/multi_chrom.cram"

echo "CRAM file:"
ls -lh "${CRAM_TESTS}/multi_chrom.cram" "${CRAM_TESTS}/multi_chrom.cram.crai"

echo ""
echo "=== Generating VCF test data ==="
# Generate synthetic multi-chromosome VCF with proper QUAL values
# Uses bare chromosome names (21, 22) to test non-chr-prefixed naming

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

cat > "${TMPDIR}/multi_chrom.vcf" << 'VCFEOF'
##fileformat=VCFv4.3
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total read depth">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##INFO=<ID=DB,Number=0,Type=Flag,Description="dbSNP membership">
##FILTER=<ID=PASS,Description="All filters passed">
##FILTER=<ID=LowQual,Description="Low quality">
##contig=<ID=21,length=46709983>
##contig=<ID=22,length=50818468>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
VCFEOF

# Generate 50 variants on chromosome 21
for i in $(seq 1 50); do
  pos=$((5000000 + i * 1000))
  qual=$((10 + (i * 3) % 90))
  dp=$((20 + i % 30))
  af=$(echo "scale=2; ($i % 50) / 100 + 0.01" | bc)
  ref="A"; alt="T"
  if (( i % 5 == 0 )); then ref="G"; alt="C"; fi
  if (( i % 7 == 0 )); then ref="AT"; alt="A"; fi
  filter="PASS"
  if (( qual < 30 )); then filter="LowQual"; fi
  info="DP=${dp};AF=${af}"
  if (( i % 3 == 0 )); then info="${info};DB"; fi
  echo -e "21\t${pos}\trs${i}\t${ref}\t${alt}\t${qual}\t${filter}\t${info}"
done >> "${TMPDIR}/multi_chrom.vcf"

# Generate 50 variants on chromosome 22
for i in $(seq 1 50); do
  pos=$((16000000 + i * 1000))
  qual=$((15 + (i * 7) % 85))
  dp=$((25 + i % 25))
  af=$(echo "scale=2; ($i % 40) / 100 + 0.05" | bc)
  ref="C"; alt="G"
  if (( i % 4 == 0 )); then ref="T"; alt="A"; fi
  if (( i % 6 == 0 )); then ref="CGA"; alt="C"; fi
  filter="PASS"
  if (( qual < 30 )); then filter="LowQual"; fi
  info="DP=${dp};AF=${af}"
  if (( i % 4 == 0 )); then info="${info};DB"; fi
  echo -e "22\t${pos}\trs$((100+i))\t${ref}\t${alt}\t${qual}\t${filter}\t${info}"
done >> "${TMPDIR}/multi_chrom.vcf"

$BCFTOOLS sort -Oz -o "${VCF_TESTS}/multi_chrom.vcf.gz" "${TMPDIR}/multi_chrom.vcf"
$TABIX -p vcf "${VCF_TESTS}/multi_chrom.vcf.gz"

echo "VCF file:"
ls -lh "${VCF_TESTS}/multi_chrom.vcf.gz" "${VCF_TESTS}/multi_chrom.vcf.gz.tbi"
echo "Per-chromosome counts:"
for chrom in 21 22; do
  count=$($BCFTOOLS view -H "${VCF_TESTS}/multi_chrom.vcf.gz" "$chrom" | wc -l | tr -d ' ')
  echo "  $chrom: $count variants"
done
total=$($BCFTOOLS view -H "${VCF_TESTS}/multi_chrom.vcf.gz" | wc -l | tr -d ' ')
echo "  Total: $total variants"

echo ""
echo "=== Summary ==="
echo "Files generated:"
ls -lh "${BAM_TESTS}/multi_chrom.bam" "${BAM_TESTS}/multi_chrom.bam.bai"
ls -lh "${CRAM_TESTS}/multi_chrom.cram" "${CRAM_TESTS}/multi_chrom.cram.crai"
ls -lh "${VCF_TESTS}/multi_chrom.vcf.gz" "${VCF_TESTS}/multi_chrom.vcf.gz.tbi"
echo "Done!"
