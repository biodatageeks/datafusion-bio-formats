#!/bin/bash
# Download VCF files from 1000 Genomes project for performance benchmarking
# 
# This script downloads 4 different sizes of VCF files:
# 1. SMALL: ~50KB - Y chromosome trio (quick tests)
# 2. SMALL-MEDIUM: ~800KB - CEU trio sites
# 3. MEDIUM: ~2-3MB - CEU trio with genotypes
# 4. LARGE: ~200-300MB - Chromosome 22 (full Phase 3)
#
# Usage:
#   ./download_vcf_benchmark_files.sh [output_dir]
#   Default output directory: /tmp

set -e

OUTPUT_DIR="${1:-/tmp}"
cd "$OUTPUT_DIR"

echo "=========================================="
echo "Downloading 4 VCF files for benchmarks"
echo "Output directory: $OUTPUT_DIR"
echo "=========================================="

# 1. SMALL: ~50KB - Y chromosome trio (very small for quick tests)
echo -e "\n[1/4] Downloading SMALL file (~50KB)..."
wget -q --show-progress \
  ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/snps/trio.2010_06.ychr.sites.vcf.gz \
  -O small_trio_ychr.vcf.gz || {
    echo "Warning: Failed to download small file, continuing..."
  }

# 2. SMALL-MEDIUM: ~800KB - CEU trio sites
echo -e "\n[2/4] Downloading SMALL-MEDIUM file (~800KB)..."
wget -q --show-progress \
  ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/snps/CEU.trio.2010_03.sites.vcf.gz \
  -O small_medium_ceu_trio.vcf.gz || {
    echo "Warning: Failed to download small-medium file, continuing..."
  }

# 3. MEDIUM: ~2-3MB - CEU trio with genotypes
echo -e "\n[3/4] Downloading MEDIUM file (~2-3MB)..."
wget -q --show-progress \
  ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/release/2010_07/trio/snps/CEU.trio.2010_03.genotypes.vcf.gz \
  -O medium_ceu_genotypes.vcf.gz || {
    echo "Warning: Failed to download medium file, continuing..."
  }

# 4. LARGE: ~200-300MB - Chromosome 22 (smallest autosome, full Phase 3)
echo -e "\n[4/4] Downloading LARGE file (~200-300MB - this will take a few minutes)..."
wget -q --show-progress \
  ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz \
  -O large_chr22_phase3.vcf.gz || {
    echo "Warning: Failed to download large file, continuing..."
  }

# Download index for the large file (needed for fast access)
echo -e "\nDownloading index for large file..."
wget -q --show-progress \
  ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz.tbi \
  -O large_chr22_phase3.vcf.gz.tbi || {
    echo "Warning: Failed to download index file, continuing..."
  }

echo -e "\n=========================================="
echo "Download complete! File summary:"
echo "=========================================="
ls -lh "$OUTPUT_DIR"/*vcf.gz* 2>/dev/null | awk '{printf "%-40s %10s\n", $9, $5}' || echo "No VCF files found"

echo -e "\n=========================================="
echo "File details:"
echo "=========================================="
echo "1. small_trio_ychr.vcf.gz        - ~50KB (Y chromosome, 3 samples)"
echo "2. small_medium_ceu_trio.vcf.gz  - ~800KB (CEU population, sites only)"
echo "3. medium_ceu_genotypes.vcf.gz   - ~2-3MB (CEU population with genotypes)"
echo "4. large_chr22_phase3.vcf.gz     - ~200-300MB (Chromosome 22, 2504 samples)"
echo "=========================================="
echo ""
echo "To run VCF benchmarks locally, use:"
echo "  ./target/release/benchmark-runner benchmarks/configs/vcf_local.yml --output-dir results"
echo ""

