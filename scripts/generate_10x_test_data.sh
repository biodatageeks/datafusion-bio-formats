#!/usr/bin/env bash
#
# Generate 10x larger multi-chromosome indexed test files for integration tests.
# Roughly 400 reads (BAM/CRAM), 1000 variants (VCF), 480 features (GFF).
#
set -euo pipefail

# --- Configuration ---
WES_BAM="/Users/mwiewior/research/data/WES/NA12878.proper.wes.md.bam"
SAMTOOLS="/opt/homebrew/bin/samtools"
BCFTOOLS="/opt/homebrew/bin/bcftools"
TABIX="/opt/homebrew/bin/tabix"
BGZIP="/opt/homebrew/bin/bgzip"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BAM_TESTS="${REPO_ROOT}/datafusion/bio-format-bam/tests"
CRAM_TESTS="${REPO_ROOT}/datafusion/bio-format-cram/tests"
VCF_TESTS="${REPO_ROOT}/datafusion/bio-format-vcf/tests"
GFF_TESTS="${REPO_ROOT}/datafusion/bio-format-gff/tests"

mkdir -p "$BAM_TESTS" "$CRAM_TESTS" "$VCF_TESTS" "$GFF_TESTS"

# =====================================================
# BAM: ~400 reads across 3 chromosomes
# =====================================================
echo "=== Generating 10x BAM test data ==="
{
  $SAMTOOLS view -H "$WES_BAM" | \
    awk -F'\t' 'BEGIN{OFS="\t"} /^@SQ/{if($2=="SN:chr1" || $2=="SN:chr2" || $2=="SN:chrX") print; next} {print}'
  $SAMTOOLS view "$WES_BAM" chr1:55000000-55020000 chr2:25000000-25020000 chrX:48000000-48001550
} | $SAMTOOLS sort -o "${BAM_TESTS}/multi_chrom_10x.bam" -
$SAMTOOLS index "${BAM_TESTS}/multi_chrom_10x.bam"

echo "BAM 10x:"
ls -lh "${BAM_TESTS}/multi_chrom_10x.bam" "${BAM_TESTS}/multi_chrom_10x.bam.bai"
echo "  Total: $($SAMTOOLS view -c "${BAM_TESTS}/multi_chrom_10x.bam") reads"
for chrom in chr1 chr2 chrX; do
  echo "  $chrom: $($SAMTOOLS view -c "${BAM_TESTS}/multi_chrom_10x.bam" "$chrom") reads"
done

# =====================================================
# CRAM: convert BAM 10x to CRAM (no_ref mode)
# =====================================================
echo ""
echo "=== Generating 10x CRAM test data ==="
$SAMTOOLS view -C --output-fmt-option no_ref=1 \
  -o "${CRAM_TESTS}/multi_chrom_10x.cram" \
  "${BAM_TESTS}/multi_chrom_10x.bam"
$SAMTOOLS index "${CRAM_TESTS}/multi_chrom_10x.cram"

echo "CRAM 10x:"
ls -lh "${CRAM_TESTS}/multi_chrom_10x.cram" "${CRAM_TESTS}/multi_chrom_10x.cram.crai"

# =====================================================
# VCF: 1000 variants across 2 chromosomes (500 each)
# =====================================================
echo ""
echo "=== Generating 10x VCF test data ==="
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

cat > "${TMPDIR}/multi_chrom_10x.vcf" << 'VCFEOF'
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

# 500 variants on chromosome 21
for i in $(seq 1 500); do
  pos=$((5000000 + i * 100))
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
  printf "21\t%d\trs%d\t%s\t%s\t%d\t%s\t%s\n" "$pos" "$i" "$ref" "$alt" "$qual" "$filter" "$info"
done >> "${TMPDIR}/multi_chrom_10x.vcf"

# 500 variants on chromosome 22
for i in $(seq 1 500); do
  pos=$((16000000 + i * 100))
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
  printf "22\t%d\trs%d\t%s\t%s\t%d\t%s\t%s\n" "$pos" "$((1000+i))" "$ref" "$alt" "$qual" "$filter" "$info"
done >> "${TMPDIR}/multi_chrom_10x.vcf"

$BCFTOOLS sort -Oz -o "${VCF_TESTS}/multi_chrom_10x.vcf.gz" "${TMPDIR}/multi_chrom_10x.vcf"
$TABIX -p vcf "${VCF_TESTS}/multi_chrom_10x.vcf.gz"

echo "VCF 10x:"
ls -lh "${VCF_TESTS}/multi_chrom_10x.vcf.gz" "${VCF_TESTS}/multi_chrom_10x.vcf.gz.tbi"
total=$($BCFTOOLS view -H "${VCF_TESTS}/multi_chrom_10x.vcf.gz" | wc -l | tr -d ' ')
echo "  Total: $total variants"

# =====================================================
# GFF: ~480 features across 3 chromosomes
# =====================================================
echo ""
echo "=== Generating 10x GFF test data ==="

GFFTMP="${TMPDIR}/multi_chrom_10x.gff3"
echo "##gff-version 3" > "$GFFTMP"

gene_id=1
# Function to emit a gene locus with transcript + exons + optional CDS
emit_gene() {
  local chr=$1 start=$2 end=$3 strand=$4 gene_type=$5 num_exons=$6 has_cds=$7
  local gene_name="GENE_${gene_id}"
  local tx_start=$start tx_end=$end

  printf "%s\tGENCODE\tgene\t%d\t%d\t.\t%s\t.\tID=gene%d;gene_id=ENSG%08d;gene_name=%s;gene_type=%s\n" \
    "$chr" "$start" "$end" "$strand" "$gene_id" "$gene_id" "$gene_name" "$gene_type" >> "$GFFTMP"
  printf "%s\tGENCODE\ttranscript\t%d\t%d\t.\t%s\t.\tID=tx%d;Parent=gene%d;transcript_id=ENST%08d;transcript_type=%s\n" \
    "$chr" "$tx_start" "$tx_end" "$strand" "$gene_id" "$gene_id" "$gene_id" "$gene_type" >> "$GFFTMP"

  local span=$(( (end - start) / num_exons ))
  local exon_len=$(( span * 2 / 3 ))
  local phase=0
  for e in $(seq 1 "$num_exons"); do
    local estart=$(( start + (e - 1) * span ))
    local eend=$(( estart + exon_len ))
    if [ "$eend" -gt "$end" ]; then eend=$end; fi
    printf "%s\tGENCODE\texon\t%d\t%d\t.\t%s\t.\tID=exon%d_%d;Parent=tx%d;exon_number=%d\n" \
      "$chr" "$estart" "$eend" "$strand" "$gene_id" "$e" "$gene_id" "$e" >> "$GFFTMP"
    if [ "$has_cds" = "1" ]; then
      local cstart=$(( estart + 50 ))
      if [ "$cstart" -lt "$eend" ]; then
        printf "%s\tGENCODE\tCDS\t%d\t%d\t.\t%s\t%d\tID=cds%d_%d;Parent=tx%d;protein_id=ENSP%08d\n" \
          "$chr" "$cstart" "$eend" "$strand" "$phase" "$gene_id" "$e" "$gene_id" "$gene_id" >> "$GFFTMP"
        phase=$(( (phase + 1) % 3 ))
      fi
    fi
  done
  gene_id=$((gene_id + 1))
}

# chr1: 20 genes (mix of protein_coding with CDS and lncRNA without)
for i in $(seq 0 19); do
  gstart=$((1000 + i * 5000))
  gend=$((gstart + 3000))
  if (( i % 3 == 0 )); then
    emit_gene chr1 $gstart $gend "+" "protein_coding" 4 1
  elif (( i % 3 == 1 )); then
    emit_gene chr1 $gstart $gend "-" "protein_coding" 3 1
  else
    emit_gene chr1 $gstart $gend "+" "lncRNA" 2 0
  fi
done

# chr2: 28 genes
for i in $(seq 0 27); do
  gstart=$((5000 + i * 4000))
  gend=$((gstart + 2500))
  if (( i % 3 == 0 )); then
    emit_gene chr2 $gstart $gend "+" "protein_coding" 4 1
  elif (( i % 3 == 1 )); then
    emit_gene chr2 $gstart $gend "-" "protein_coding" 3 1
  else
    emit_gene chr2 $gstart $gend "-" "lncRNA" 2 0
  fi
done

# chrX: 12 genes
for i in $(seq 0 11); do
  gstart=$((8000 + i * 6000))
  gend=$((gstart + 4000))
  if (( i % 2 == 0 )); then
    emit_gene chrX $gstart $gend "+" "protein_coding" 4 1
  else
    emit_gene chrX $gstart $gend "-" "lncRNA" 3 0
  fi
done

# Sort, compress, index
grep -v "^#" "$GFFTMP" | sort -t'	' -k1,1 -k4,4n > "${TMPDIR}/sorted.gff3"
(echo "##gff-version 3"; cat "${TMPDIR}/sorted.gff3") | $BGZIP -c > "${GFF_TESTS}/multi_chrom_10x.gff3.gz"
$TABIX -p gff "${GFF_TESTS}/multi_chrom_10x.gff3.gz"

echo "GFF 10x:"
ls -lh "${GFF_TESTS}/multi_chrom_10x.gff3.gz" "${GFF_TESTS}/multi_chrom_10x.gff3.gz.tbi"
total=$(bgzip -d -c "${GFF_TESTS}/multi_chrom_10x.gff3.gz" | grep -v "^#" | wc -l | tr -d ' ')
echo "  Total: $total features"
for chrom in chr1 chr2 chrX; do
  count=$($TABIX "${GFF_TESTS}/multi_chrom_10x.gff3.gz" "$chrom" | wc -l | tr -d ' ')
  echo "  $chrom: $count features"
done

echo ""
echo "=== Summary ==="
echo "All 10x test files:"
ls -lh "${BAM_TESTS}/multi_chrom_10x.bam" "${BAM_TESTS}/multi_chrom_10x.bam.bai"
ls -lh "${CRAM_TESTS}/multi_chrom_10x.cram" "${CRAM_TESTS}/multi_chrom_10x.cram.crai"
ls -lh "${VCF_TESTS}/multi_chrom_10x.vcf.gz" "${VCF_TESTS}/multi_chrom_10x.vcf.gz.tbi"
ls -lh "${GFF_TESTS}/multi_chrom_10x.gff3.gz" "${GFF_TESTS}/multi_chrom_10x.gff3.gz.tbi"
echo "Done!"
