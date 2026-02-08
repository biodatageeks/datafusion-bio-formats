#!/usr/bin/env bash
#
# Generate multi-chromosome indexed test files for integration tests.
#
# Two tiers:
#   base  – moderate size for fast CI (~400 reads / ~1000 variants / ~450 features)
#   large – 10x of base for thorough testing (~4000 reads / ~10000 variants / ~4500 features)
#
# Usage:
#   ./scripts/generate_test_data.sh          # generate both tiers
#   ./scripts/generate_test_data.sh base     # generate base only
#   ./scripts/generate_test_data.sh large    # generate large only
#
set -euo pipefail

TIER="${1:-all}"  # "base", "large", or "all"

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

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# =============================================================================
# Shared function: emit a GFF gene locus with transcript + exons + optional CDS
# =============================================================================
gene_id=1
emit_gene() {
  local gff_file=$1 chr=$2 start=$3 end=$4 strand=$5 gene_type=$6 num_exons=$7 has_cds=$8
  local gene_name="GENE_${gene_id}"

  printf "%s\tGENCODE\tgene\t%d\t%d\t.\t%s\t.\tID=gene%d;gene_id=ENSG%08d;gene_name=%s;gene_type=%s\n" \
    "$chr" "$start" "$end" "$strand" "$gene_id" "$gene_id" "$gene_name" "$gene_type" >> "$gff_file"
  printf "%s\tGENCODE\ttranscript\t%d\t%d\t.\t%s\t.\tID=tx%d;Parent=gene%d;transcript_id=ENST%08d;transcript_type=%s\n" \
    "$chr" "$start" "$end" "$strand" "$gene_id" "$gene_id" "$gene_id" "$gene_type" >> "$gff_file"

  local span=$(( (end - start) / num_exons ))
  local exon_len=$(( span * 2 / 3 ))
  local phase=0
  for e in $(seq 1 "$num_exons"); do
    local estart=$(( start + (e - 1) * span ))
    local eend=$(( estart + exon_len ))
    if [ "$eend" -gt "$end" ]; then eend=$end; fi
    printf "%s\tGENCODE\texon\t%d\t%d\t.\t%s\t.\tID=exon%d_%d;Parent=tx%d;exon_number=%d\n" \
      "$chr" "$estart" "$eend" "$strand" "$gene_id" "$e" "$gene_id" "$e" >> "$gff_file"
    if [ "$has_cds" = "1" ]; then
      local cstart=$(( estart + 50 ))
      if [ "$cstart" -lt "$eend" ]; then
        printf "%s\tGENCODE\tCDS\t%d\t%d\t.\t%s\t%d\tID=cds%d_%d;Parent=tx%d;protein_id=ENSP%08d\n" \
          "$chr" "$cstart" "$eend" "$strand" "$phase" "$gene_id" "$e" "$gene_id" "$gene_id" >> "$gff_file"
        phase=$(( (phase + 1) % 3 ))
      fi
    fi
  done
  gene_id=$((gene_id + 1))
}

# =============================================================================
# Shared function: generate VCF variants
# =============================================================================
generate_vcf() {
  local vcf_file=$1 n_per_chrom=$2 suffix=$3

  cat > "$vcf_file" << 'VCFEOF'
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

  for i in $(seq 1 "$n_per_chrom"); do
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
  done >> "$vcf_file"

  for i in $(seq 1 "$n_per_chrom"); do
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
    printf "22\t%d\trs%d\t%s\t%s\t%d\t%s\t%s\n" "$pos" "$((10000+i))" "$ref" "$alt" "$qual" "$filter" "$info"
  done >> "$vcf_file"
}

# =============================================================================
# Shared function: generate GFF features
# =============================================================================
generate_gff() {
  local gff_file=$1 n_chr1=$2 n_chr2=$3 n_chrx=$4

  echo "##gff-version 3" > "$gff_file"

  for i in $(seq 0 $((n_chr1 - 1))); do
    gstart=$((1000 + i * 5000))
    gend=$((gstart + 3000))
    if (( i % 3 == 0 )); then
      emit_gene "$gff_file" chr1 $gstart $gend "+" "protein_coding" 4 1
    elif (( i % 3 == 1 )); then
      emit_gene "$gff_file" chr1 $gstart $gend "-" "protein_coding" 3 1
    else
      emit_gene "$gff_file" chr1 $gstart $gend "+" "lncRNA" 2 0
    fi
  done

  for i in $(seq 0 $((n_chr2 - 1))); do
    gstart=$((5000 + i * 4000))
    gend=$((gstart + 2500))
    if (( i % 3 == 0 )); then
      emit_gene "$gff_file" chr2 $gstart $gend "+" "protein_coding" 4 1
    elif (( i % 3 == 1 )); then
      emit_gene "$gff_file" chr2 $gstart $gend "-" "protein_coding" 3 1
    else
      emit_gene "$gff_file" chr2 $gstart $gend "-" "lncRNA" 2 0
    fi
  done

  for i in $(seq 0 $((n_chrx - 1))); do
    gstart=$((8000 + i * 6000))
    gend=$((gstart + 4000))
    if (( i % 2 == 0 )); then
      emit_gene "$gff_file" chrX $gstart $gend "+" "protein_coding" 4 1
    else
      emit_gene "$gff_file" chrX $gstart $gend "-" "lncRNA" 3 0
    fi
  done
}

# =============================================================================
# Shared function: sort, compress, and index a GFF file
# =============================================================================
finalize_gff() {
  local gff_raw=$1 output_gz=$2
  grep -v "^#" "$gff_raw" | sort -t'	' -k1,1 -k4,4n > "${TMPDIR}/sorted.gff3"
  (echo "##gff-version 3"; cat "${TMPDIR}/sorted.gff3") | $BGZIP -c > "$output_gz"
  $TABIX -p gff "$output_gz"
}

# #############################################################################
# BASE tier: ~400 reads, 1000 variants, ~450 features
# #############################################################################
if [ "$TIER" = "base" ] || [ "$TIER" = "all" ]; then
  echo "====================================================================="
  echo "=== Generating BASE test data ==="
  echo "====================================================================="

  # --- BAM ---
  echo ""
  echo "--- BAM (base) ---"
  {
    $SAMTOOLS view -H "$WES_BAM" | \
      awk -F'\t' 'BEGIN{OFS="\t"} /^@SQ/{if($2=="SN:chr1" || $2=="SN:chr2" || $2=="SN:chrX") print; next} {print}'
    $SAMTOOLS view "$WES_BAM" chr1:55000000-55020000 chr2:25000000-25020000 chrX:48000000-48001550
  } | $SAMTOOLS sort -o "${BAM_TESTS}/multi_chrom.bam" -
  $SAMTOOLS index "${BAM_TESTS}/multi_chrom.bam"
  ls -lh "${BAM_TESTS}/multi_chrom.bam" "${BAM_TESTS}/multi_chrom.bam.bai"
  echo "  Total: $($SAMTOOLS view -c "${BAM_TESTS}/multi_chrom.bam") reads"
  for chrom in chr1 chr2 chrX; do
    echo "  $chrom: $($SAMTOOLS view -c "${BAM_TESTS}/multi_chrom.bam" "$chrom") reads"
  done

  # --- CRAM ---
  echo ""
  echo "--- CRAM (base) ---"
  $SAMTOOLS view -C --output-fmt-option no_ref=1 \
    -o "${CRAM_TESTS}/multi_chrom.cram" \
    "${BAM_TESTS}/multi_chrom.bam"
  $SAMTOOLS index "${CRAM_TESTS}/multi_chrom.cram"
  ls -lh "${CRAM_TESTS}/multi_chrom.cram" "${CRAM_TESTS}/multi_chrom.cram.crai"

  # --- VCF ---
  echo ""
  echo "--- VCF (base) ---"
  generate_vcf "${TMPDIR}/base.vcf" 500 "base"
  $BCFTOOLS sort -Oz -o "${VCF_TESTS}/multi_chrom.vcf.gz" "${TMPDIR}/base.vcf"
  $TABIX -p vcf "${VCF_TESTS}/multi_chrom.vcf.gz"
  ls -lh "${VCF_TESTS}/multi_chrom.vcf.gz" "${VCF_TESTS}/multi_chrom.vcf.gz.tbi"
  echo "  Total: $($BCFTOOLS view -H "${VCF_TESTS}/multi_chrom.vcf.gz" | wc -l | tr -d ' ') variants"

  # --- GFF ---
  echo ""
  echo "--- GFF (base) ---"
  gene_id=1
  generate_gff "${TMPDIR}/base.gff3" 20 28 12
  finalize_gff "${TMPDIR}/base.gff3" "${GFF_TESTS}/multi_chrom.gff3.gz"
  ls -lh "${GFF_TESTS}/multi_chrom.gff3.gz" "${GFF_TESTS}/multi_chrom.gff3.gz.tbi"
  total=$($BGZIP -d -c "${GFF_TESTS}/multi_chrom.gff3.gz" | grep -v "^#" | wc -l | tr -d ' ')
  echo "  Total: $total features"
  for chrom in chr1 chr2 chrX; do
    count=$($TABIX "${GFF_TESTS}/multi_chrom.gff3.gz" "$chrom" | wc -l | tr -d ' ')
    echo "  $chrom: $count features"
  done
fi

# #############################################################################
# LARGE tier: ~4000 reads, 10000 variants, ~4500 features
# #############################################################################
if [ "$TIER" = "large" ] || [ "$TIER" = "all" ]; then
  echo ""
  echo "====================================================================="
  echo "=== Generating LARGE test data ==="
  echo "====================================================================="

  # --- BAM ---
  echo ""
  echo "--- BAM (large) ---"
  {
    $SAMTOOLS view -H "$WES_BAM" | \
      awk -F'\t' 'BEGIN{OFS="\t"} /^@SQ/{if($2=="SN:chr1" || $2=="SN:chr2" || $2=="SN:chrX") print; next} {print}'
    $SAMTOOLS view "$WES_BAM" chr1:55000000-55030000 chr2:25000000-25025000 chrX:48000000-48002200
  } | $SAMTOOLS sort -o "${BAM_TESTS}/multi_chrom_large.bam" -
  $SAMTOOLS index "${BAM_TESTS}/multi_chrom_large.bam"
  ls -lh "${BAM_TESTS}/multi_chrom_large.bam" "${BAM_TESTS}/multi_chrom_large.bam.bai"
  echo "  Total: $($SAMTOOLS view -c "${BAM_TESTS}/multi_chrom_large.bam") reads"
  for chrom in chr1 chr2 chrX; do
    echo "  $chrom: $($SAMTOOLS view -c "${BAM_TESTS}/multi_chrom_large.bam" "$chrom") reads"
  done

  # --- CRAM ---
  echo ""
  echo "--- CRAM (large) ---"
  $SAMTOOLS view -C --output-fmt-option no_ref=1 \
    -o "${CRAM_TESTS}/multi_chrom_large.cram" \
    "${BAM_TESTS}/multi_chrom_large.bam"
  $SAMTOOLS index "${CRAM_TESTS}/multi_chrom_large.cram"
  ls -lh "${CRAM_TESTS}/multi_chrom_large.cram" "${CRAM_TESTS}/multi_chrom_large.cram.crai"

  # --- VCF ---
  echo ""
  echo "--- VCF (large) ---"
  generate_vcf "${TMPDIR}/large.vcf" 5000 "large"
  $BCFTOOLS sort -Oz -o "${VCF_TESTS}/multi_chrom_large.vcf.gz" "${TMPDIR}/large.vcf"
  $TABIX -p vcf "${VCF_TESTS}/multi_chrom_large.vcf.gz"
  ls -lh "${VCF_TESTS}/multi_chrom_large.vcf.gz" "${VCF_TESTS}/multi_chrom_large.vcf.gz.tbi"
  echo "  Total: $($BCFTOOLS view -H "${VCF_TESTS}/multi_chrom_large.vcf.gz" | wc -l | tr -d ' ') variants"

  # --- GFF ---
  echo ""
  echo "--- GFF (large) ---"
  gene_id=1
  generate_gff "${TMPDIR}/large.gff3" 200 280 120
  finalize_gff "${TMPDIR}/large.gff3" "${GFF_TESTS}/multi_chrom_large.gff3.gz"
  ls -lh "${GFF_TESTS}/multi_chrom_large.gff3.gz" "${GFF_TESTS}/multi_chrom_large.gff3.gz.tbi"
  total=$($BGZIP -d -c "${GFF_TESTS}/multi_chrom_large.gff3.gz" | grep -v "^#" | wc -l | tr -d ' ')
  echo "  Total: $total features"
  for chrom in chr1 chr2 chrX; do
    count=$($TABIX "${GFF_TESTS}/multi_chrom_large.gff3.gz" "$chrom" | wc -l | tr -d ' ')
    echo "  $chrom: $count features"
  done
fi

echo ""
echo "=== Done ==="
