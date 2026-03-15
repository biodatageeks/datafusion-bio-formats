use crate::errors::Result;
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::util::{
    BatchBuilder, ColumnMap, ProvenanceWriter, normalize_genomic_end, normalize_genomic_start,
    normalize_nullable_ref, parse_f64_ref, parse_i8_ref, parse_i64_ref,
};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// VariationColumnIndices – pre-computed builder indices from ColumnMap
// ---------------------------------------------------------------------------

pub(crate) struct VariationColumnIndices {
    // Required columns
    chrom: Option<usize>,
    start: Option<usize>,
    end: Option<usize>,
    variation_name: Option<usize>,
    allele_string: Option<usize>,
    region_bin: Option<usize>,
    // Optional canonical columns
    failed: Option<usize>,
    somatic: Option<usize>,
    strand: Option<usize>,
    minor_allele: Option<usize>,
    minor_allele_freq: Option<usize>,
    clin_sig: Option<usize>,
    phenotype_or_disease: Option<usize>,
    clinical_impact: Option<usize>,
    pubmed: Option<usize>,
    var_synonyms: Option<usize>,
    // Extra columns: (tab_idx, builder_idx)
    extras: Vec<(usize, usize)>,
}

impl VariationColumnIndices {
    pub fn new(col_map: &ColumnMap, ctx: &VariationContext) -> Self {
        let extras: Vec<(usize, usize)> = ctx
            .extra_columns
            .iter()
            .filter_map(|(tab_idx, col_name)| {
                col_map
                    .get(col_name)
                    .map(|builder_idx| (*tab_idx, builder_idx))
            })
            .collect();

        Self {
            chrom: col_map.get("chrom"),
            start: col_map.get("start"),
            end: col_map.get("end"),
            variation_name: col_map.get("variation_name"),
            allele_string: col_map.get("allele_string"),
            region_bin: col_map.get("region_bin"),
            failed: col_map.get("failed"),
            somatic: col_map.get("somatic"),
            strand: col_map.get("strand"),
            minor_allele: col_map.get("minor_allele"),
            minor_allele_freq: col_map.get("minor_allele_freq"),
            clin_sig: col_map.get("clin_sig"),
            phenotype_or_disease: col_map.get("phenotype_or_disease"),
            clinical_impact: col_map.get("clinical_impact"),
            pubmed: col_map.get("pubmed"),
            var_synonyms: col_map.get("var_synonyms"),
            extras,
        }
    }
}

// Maximum number of tab-separated fields we support per variation line.
// VEP 115 GRCh38 has ~39 columns (core + 1000G + gnomAD sub-populations).
// Using 96 provides headroom for future cache versions with additional
// population-specific frequency columns.
const MAX_VARIATION_FIELDS: usize = 96;

// ---------------------------------------------------------------------------
// VariationContext – pre-computed per-partition field mapping
// ---------------------------------------------------------------------------

pub(crate) struct VariationContext {
    // Tab field index for required fields
    chrom_tab: Option<usize>,
    start_tab: Option<usize>,
    end_tab: Option<usize>,
    var_name_tab: Option<usize>,
    allele_tab: Option<usize>,
    // Tab field index for optional canonical columns
    failed_tab: Option<usize>,
    somatic_tab: Option<usize>,
    strand_tab: Option<usize>,
    minor_allele_tab: Option<usize>,
    minor_allele_freq_tab: Option<usize>,
    clin_sig_tab: Option<usize>,
    phenotype_or_disease_tab: Option<usize>,
    clinical_impact_tab: Option<usize>,
    pubmed_tab: Option<usize>,
    var_synonyms_tab: Option<usize>,
    // Extra columns: (tab_idx, output_column_name)
    extra_columns: Vec<(usize, String)>,
    // Pre-computed source ID mapping: source_key -> ids_column_name
    source_to_id_column: HashMap<String, String>,
}

impl VariationContext {
    pub fn source_to_id_column(&self) -> &HashMap<String, String> {
        &self.source_to_id_column
    }

    pub fn new(cache_info: &CacheInfo) -> Self {
        let cols = &cache_info.variation_cols;

        let find = |candidates: &[&str]| -> Option<usize> {
            for candidate in candidates {
                if let Some(idx) = cols.iter().position(|c| c == candidate) {
                    return Some(idx);
                }
            }
            None
        };

        let chrom_tab = find(&["chr", "chrom", "seq_region_name"]);
        let start_tab = find(&["start", "pos", "position"]);
        let end_tab = find(&["end"]);
        let var_name_tab = find(&["variation_name", "id"]);
        let allele_tab = find(&["allele_string", "alleles"]);

        let find_exact = |name: &str| -> Option<usize> { cols.iter().position(|c| c == name) };

        let failed_tab = find_exact("failed");
        let somatic_tab = find_exact("somatic");
        let strand_tab = find_exact("strand");
        let minor_allele_tab = find_exact("minor_allele");
        let minor_allele_freq_tab = find_exact("minor_allele_freq");
        let clin_sig_tab = find_exact("clin_sig");
        let phenotype_or_disease_tab = find_exact("phenotype_or_disease");
        let clinical_impact_tab = find_exact("clinical_impact");
        let pubmed_tab = find_exact("pubmed");
        let var_synonyms_tab = find_exact("var_synonyms");

        // Build known set for filtering extras
        let mut known: std::collections::HashSet<&str> = [
            "chr",
            "chrom",
            "seq_region_name",
            "start",
            "pos",
            "position",
            "end",
            "variation_name",
            "id",
            "allele_string",
            "alleles",
            "failed",
            "somatic",
            "strand",
            "minor_allele",
            "minor_allele_freq",
            "clin_sig",
            "phenotype_or_disease",
            "clinical_impact",
            "pubmed",
            "var_synonyms",
        ]
        .into_iter()
        .collect();
        for source in &cache_info.source_descriptors {
            known.insert(&source.ids_column);
        }

        let extra_columns: Vec<(usize, String)> = cols
            .iter()
            .enumerate()
            .filter(|(_, name)| !known.contains(name.as_str()))
            .map(|(idx, name)| (idx, name.clone()))
            .collect();

        let source_to_id_column: HashMap<String, String> = cache_info
            .source_descriptors
            .iter()
            .map(|s| (s.source_key.clone(), s.ids_column.clone()))
            .collect();

        Self {
            chrom_tab,
            start_tab,
            end_tab,
            var_name_tab,
            allele_tab,
            failed_tab,
            somatic_tab,
            strand_tab,
            minor_allele_tab,
            minor_allele_freq_tab,
            clin_sig_tab,
            phenotype_or_disease_tab,
            clinical_impact_tab,
            pubmed_tab,
            var_synonyms_tab,
            extra_columns,
            source_to_id_column,
        }
    }
}

// ---------------------------------------------------------------------------
// SourceIdWriter – reusable, allocation-free source ID accumulator
// ---------------------------------------------------------------------------

struct SourceIdSlot {
    builder_idx: usize,
    buffer: String,
    /// (start_byte, len_bytes) into `buffer` for each value, enabling O(n) dedup
    /// without re-splitting the comma-separated buffer.
    offsets: Vec<(usize, usize)>,
}

pub(crate) struct SourceIdWriter {
    key_to_slot: HashMap<String, usize>,
    slots: Vec<SourceIdSlot>,
    normalize_buf: String,
}

impl SourceIdWriter {
    pub fn new(col_map: &ColumnMap, source_to_id_column: &HashMap<String, String>) -> Self {
        let mut key_to_slot = HashMap::new();
        let mut slots = Vec::new();
        for (source_key, ids_column) in source_to_id_column {
            if let Some(builder_idx) = col_map.get(ids_column) {
                let slot_idx = slots.len();
                slots.push(SourceIdSlot {
                    builder_idx,
                    buffer: String::with_capacity(64),
                    offsets: Vec::with_capacity(4),
                });
                key_to_slot.insert(source_key.clone(), slot_idx);
            }
        }
        Self {
            key_to_slot,
            slots,
            normalize_buf: String::with_capacity(32),
        }
    }

    #[inline]
    pub fn is_active(&self) -> bool {
        !self.slots.is_empty()
    }

    pub fn clear(&mut self) {
        for slot in &mut self.slots {
            slot.buffer.clear();
            slot.offsets.clear();
        }
    }

    pub fn populate(&mut self, variation_name: Option<&str>, var_synonyms: Option<&str>) {
        if let Some(name) = variation_name {
            self.assign_by_pattern(name);
        }
        if let Some(synonyms) = var_synonyms {
            self.parse_var_synonyms(synonyms);
        }
    }

    fn parse_var_synonyms(&mut self, synonyms: &str) {
        let mut current_slot: Option<usize> = None;
        for token in synonyms.split(',') {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }

            if let Some((source, value)) = token.split_once("::") {
                current_slot = self.lookup_source(source);
                if let Some(slot_idx) = current_slot {
                    self.push_value(slot_idx, value.trim());
                } else {
                    self.assign_by_pattern(token);
                }
                continue;
            }

            if let Some((source, value)) = token.split_once(':') {
                let slot = self.lookup_source(source);
                if let Some(slot_idx) = slot {
                    current_slot = Some(slot_idx);
                    self.push_value(slot_idx, value.trim());
                    continue;
                }
            }

            if let Some(slot_idx) = current_slot {
                self.push_value(slot_idx, token);
            } else {
                self.assign_by_pattern(token);
            }
        }
    }

    fn lookup_source(&mut self, raw: &str) -> Option<usize> {
        // Normalize source name into reusable buffer (zero allocation)
        self.normalize_buf.clear();
        let mut previous_is_underscore = true; // skip leading underscores
        for ch in raw.chars() {
            if ch.is_ascii_alphanumeric() {
                self.normalize_buf.push(ch.to_ascii_lowercase());
                previous_is_underscore = false;
            } else if !previous_is_underscore {
                self.normalize_buf.push('_');
                previous_is_underscore = true;
            }
        }
        // Trim trailing underscore
        while self.normalize_buf.ends_with('_') {
            self.normalize_buf.pop();
        }
        if self.normalize_buf.is_empty() {
            return None;
        }

        // Try lookup as-is
        if let Some(idx) = self.key_to_slot.get(self.normalize_buf.as_str()).copied() {
            return Some(idx);
        }
        // If starts with digit, try with "src_" prefix
        if self.normalize_buf.as_bytes()[0].is_ascii_digit() {
            self.normalize_buf.insert_str(0, "src_");
            return self.key_to_slot.get(self.normalize_buf.as_str()).copied();
        }
        None
    }

    fn push_value(&mut self, slot_idx: usize, value: &str) {
        let value = value.trim();
        if value.is_empty() || value == "." {
            return;
        }
        let slot = &mut self.slots[slot_idx];
        // Dedup: check tracked offsets instead of re-splitting
        for &(start, len) in &slot.offsets {
            if &slot.buffer[start..start + len] == value {
                return;
            }
        }
        let start = if slot.offsets.is_empty() {
            0
        } else {
            slot.buffer.push(',');
            slot.buffer.len()
        };
        slot.buffer.push_str(value);
        slot.offsets.push((start, value.len()));
    }

    fn assign_by_pattern(&mut self, value: &str) {
        let value = value.trim();
        if value.is_empty() || value == "." {
            return;
        }

        let slot_idx = if value.starts_with("rs") && value[2..].chars().all(|c| c.is_ascii_digit())
        {
            self.key_to_slot.get("dbsnp").copied()
        } else if value.starts_with("COSM") || value.starts_with("COSV") {
            self.key_to_slot.get("cosmic").copied()
        } else if value.starts_with("CM") && value[2..].chars().any(|c| c.is_ascii_digit()) {
            self.key_to_slot.get("hgmd_public").copied()
        } else if value.starts_with("RCV") || value.starts_with("VCV") {
            self.key_to_slot.get("clinvar").copied()
        } else {
            None
        };

        if let Some(idx) = slot_idx {
            self.push_value(idx, value);
        }
    }

    pub fn flush(&self, batch: &mut BatchBuilder) {
        for slot in &self.slots {
            if !slot.offsets.is_empty() {
                batch.set_utf8(slot.builder_idx, &slot.buffer);
            } else {
                batch.set_null(slot.builder_idx);
            }
        }
    }

    #[cfg(test)]
    fn slot_buffer(&self, source_key: &str) -> Option<&str> {
        self.key_to_slot.get(source_key).and_then(|idx| {
            let slot = &self.slots[*idx];
            if slot.offsets.is_empty() {
                None
            } else {
                Some(slot.buffer.as_str())
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Public helpers
// ---------------------------------------------------------------------------

pub(crate) fn detect_region_size(
    cache_info: &CacheInfo,
    variation_files: &[std::path::PathBuf],
) -> i64 {
    if let Some(size) = cache_info.cache_region_size
        && size > 0
    {
        return size;
    }

    for path in variation_files {
        let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
            continue;
        };
        if let Some(size) = parse_region_from_filename(name) {
            return size;
        }
    }

    1_000_000
}

pub(crate) fn parse_region_from_filename(name: &str) -> Option<i64> {
    let marker = name.find('_')?;
    let suffix = &name[marker + 1..];
    let (start_raw, rest) = suffix.split_once('-')?;
    let end_raw: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if end_raw.is_empty() {
        return None;
    }

    let start = start_raw.parse::<i64>().ok()?;
    let end = end_raw.parse::<i64>().ok()?;
    if end >= start {
        Some(end - start + 1)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Parse result
// ---------------------------------------------------------------------------

/// Outcome of parsing a single variation line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VariationParseResult {
    /// Row was added to the batch.
    Added,
    /// Row was skipped (blank, comment, or predicate mismatch).
    Skipped,
    /// Row was dropped because a required field could not be parsed.
    /// The caller should count this for diagnostics.
    Malformed,
}

// ---------------------------------------------------------------------------
// Direct builder parser
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_variation_line_into(
    line: &str,
    source_file_str: &str,
    predicate: &SimplePredicate,
    cache_region_size: i64,
    coordinate_system_zero_based: bool,
    batch: &mut BatchBuilder,
    col_idx: &VariationColumnIndices,
    ctx: &VariationContext,
    provenance: &ProvenanceWriter,
    source_id_writer: &mut SourceIdWriter,
) -> Result<VariationParseResult> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(VariationParseResult::Skipped);
    }

    // Stack-allocated field splitting – avoids per-line heap allocation
    let mut fields = [""; MAX_VARIATION_FIELDS];
    let mut field_count = 0;
    for (i, f) in trimmed.split('\t').enumerate() {
        if i >= MAX_VARIATION_FIELDS {
            break;
        }
        fields[i] = f;
        field_count += 1;
    }

    // Helper to get a tab field by pre-computed index
    let field_at =
        |tab_idx: Option<usize>| -> Option<&str> { tab_idx.and_then(|i| fields.get(i).copied()) };

    // Extract required fields — return Malformed instead of Err so a single
    // bad line does not abort the entire partition and silently drop all
    // subsequent records.
    let Some(chrom_ref) = field_at(ctx.chrom_tab).and_then(normalize_nullable_ref) else {
        return Ok(VariationParseResult::Malformed);
    };

    let Some(source_start) = parse_i64_ref(field_at(ctx.start_tab)) else {
        return Ok(VariationParseResult::Malformed);
    };

    let source_end = parse_i64_ref(field_at(ctx.end_tab)).unwrap_or(source_start);

    let start = normalize_genomic_start(source_start, coordinate_system_zero_based);
    let end = normalize_genomic_end(source_end, coordinate_system_zero_based);

    if !predicate.matches(chrom_ref, start, end) {
        return Ok(VariationParseResult::Skipped);
    }

    let Some(variation_name_ref) = field_at(ctx.var_name_tab).and_then(normalize_nullable_ref)
    else {
        return Ok(VariationParseResult::Malformed);
    };

    let Some(allele_string_ref) = field_at(ctx.allele_tab).and_then(normalize_nullable_ref) else {
        return Ok(VariationParseResult::Malformed);
    };

    let region_bin = ((source_start.saturating_sub(1)) / cache_region_size.max(1)).max(0);

    // Write required columns (direct index access, no HashMap lookups)
    if let Some(idx) = col_idx.chrom {
        batch.set_utf8(idx, chrom_ref);
    }
    if let Some(idx) = col_idx.start {
        batch.set_i64(idx, start);
    }
    if let Some(idx) = col_idx.end {
        batch.set_i64(idx, end);
    }
    if let Some(idx) = col_idx.variation_name {
        batch.set_utf8(idx, variation_name_ref);
    }
    if let Some(idx) = col_idx.allele_string {
        batch.set_utf8(idx, allele_string_ref);
    }
    if let Some(idx) = col_idx.region_bin {
        batch.set_i64(idx, region_bin);
    }

    // Write optional canonical columns (only if projected)
    if let Some(idx) = col_idx.failed {
        batch.set_opt_i8(idx, parse_i8_ref(field_at(ctx.failed_tab)));
    }
    if let Some(idx) = col_idx.somatic {
        batch.set_opt_i8(idx, parse_i8_ref(field_at(ctx.somatic_tab)));
    }
    if let Some(idx) = col_idx.strand {
        batch.set_opt_i8(idx, parse_i8_ref(field_at(ctx.strand_tab)));
    }
    if let Some(idx) = col_idx.minor_allele {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.minor_allele_tab).and_then(normalize_nullable_ref),
        );
    }
    if let Some(idx) = col_idx.minor_allele_freq {
        batch.set_opt_f64(idx, parse_f64_ref(field_at(ctx.minor_allele_freq_tab)));
    }
    if let Some(idx) = col_idx.clin_sig {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.clin_sig_tab).and_then(normalize_nullable_ref),
        );
    }
    if let Some(idx) = col_idx.phenotype_or_disease {
        batch.set_opt_i8(idx, parse_i8_ref(field_at(ctx.phenotype_or_disease_tab)));
    }
    if let Some(idx) = col_idx.clinical_impact {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.clinical_impact_tab).and_then(normalize_nullable_ref),
        );
    }
    if let Some(idx) = col_idx.pubmed {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.pubmed_tab).and_then(normalize_nullable_ref),
        );
    }
    if let Some(idx) = col_idx.var_synonyms {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.var_synonyms_tab).and_then(normalize_nullable_ref),
        );
    }

    // Source IDs: only compute if any source ID column is projected (checked once at init)
    if source_id_writer.is_active() {
        let var_synonyms_ref = field_at(ctx.var_synonyms_tab).and_then(normalize_nullable_ref);
        source_id_writer.clear();
        source_id_writer.populate(Some(variation_name_ref), var_synonyms_ref);
        source_id_writer.flush(batch);
    }

    // Extra columns (pre-computed builder indices, no HashMap lookups)
    for &(tab_idx, builder_idx) in &col_idx.extras {
        if tab_idx < field_count {
            batch.set_opt_utf8(builder_idx, normalize_nullable_ref(fields[tab_idx]));
        }
    }

    // Provenance columns (pre-computed indices, no HashMap lookups)
    provenance.write(batch, source_file_str);

    batch.finish_row();
    Ok(VariationParseResult::Added)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::info::{CacheInfo, SourceDescriptor};
    use crate::schema::variation_schema;
    use crate::util::{BatchBuilder, ColumnMap, ProvenanceWriter};
    use std::path::PathBuf;

    fn make_cache_info(variation_cols: Vec<String>, sources: Vec<SourceDescriptor>) -> CacheInfo {
        CacheInfo {
            cache_root: PathBuf::from("/tmp/test"),
            source_cache_path: "/tmp/test".to_string(),
            species: "homo_sapiens".to_string(),
            assembly: "GRCh38".to_string(),
            cache_version: "115".to_string(),
            serializer_type: Some("storable".to_string()),
            var_type: Some("region".to_string()),
            cache_region_size: Some(1_000_000),
            variation_cols,
            source_descriptors: sources,
        }
    }

    fn standard_cols() -> Vec<String> {
        vec![
            "chr",
            "start",
            "end",
            "variation_name",
            "allele_string",
            "failed",
            "somatic",
            "strand",
            "minor_allele",
            "minor_allele_freq",
            "clin_sig",
            "phenotype_or_disease",
            "clinical_impact",
            "pubmed",
            "var_synonyms",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    }

    fn standard_sources() -> Vec<SourceDescriptor> {
        vec![
            SourceDescriptor {
                source_key: "dbsnp".to_string(),
                source_column: "source_dbsnp".to_string(),
                ids_column: "dbsnp_ids".to_string(),
                value: "156".to_string(),
            },
            SourceDescriptor {
                source_key: "cosmic".to_string(),
                source_column: "source_cosmic".to_string(),
                ids_column: "cosmic_ids".to_string(),
                value: "101".to_string(),
            },
            SourceDescriptor {
                source_key: "clinvar".to_string(),
                source_column: "source_clinvar".to_string(),
                ids_column: "clinvar_ids".to_string(),
                value: "202502".to_string(),
            },
            SourceDescriptor {
                source_key: "hgmd_public".to_string(),
                source_column: "source_hgmd_public".to_string(),
                ids_column: "hgmd_public_ids".to_string(),
                value: "20204".to_string(),
            },
        ]
    }

    // -----------------------------------------------------------------------
    // parse_region_from_filename
    // -----------------------------------------------------------------------

    #[test]
    fn parse_region_typical() {
        assert_eq!(
            parse_region_from_filename("1_1-1000000_var.gz"),
            Some(1_000_000)
        );
    }

    #[test]
    fn parse_region_large() {
        assert_eq!(
            parse_region_from_filename("22_15000001-16000000_var.gz"),
            Some(1_000_000)
        );
    }

    #[test]
    fn parse_region_no_underscore() {
        assert_eq!(parse_region_from_filename("all_vars.gz"), None);
    }

    #[test]
    fn parse_region_no_dash() {
        assert_eq!(parse_region_from_filename("1_var.gz"), None);
    }

    // -----------------------------------------------------------------------
    // detect_region_size
    // -----------------------------------------------------------------------

    #[test]
    fn detect_from_cache_info() {
        let info = make_cache_info(standard_cols(), vec![]);
        assert_eq!(detect_region_size(&info, &[]), 1_000_000);
    }

    #[test]
    fn detect_from_filename() {
        let mut info = make_cache_info(standard_cols(), vec![]);
        info.cache_region_size = None;
        let files = vec![PathBuf::from("1_1-500000_var.gz")];
        assert_eq!(detect_region_size(&info, &files), 500_000);
    }

    #[test]
    fn detect_defaults_to_1m() {
        let mut info = make_cache_info(standard_cols(), vec![]);
        info.cache_region_size = None;
        assert_eq!(detect_region_size(&info, &[]), 1_000_000);
    }

    // -----------------------------------------------------------------------
    // VariationContext
    // -----------------------------------------------------------------------

    #[test]
    fn variation_context_maps_standard_cols() {
        let info = make_cache_info(standard_cols(), vec![]);
        let ctx = VariationContext::new(&info);
        assert_eq!(ctx.chrom_tab, Some(0));
        assert_eq!(ctx.start_tab, Some(1));
        assert_eq!(ctx.end_tab, Some(2));
        assert_eq!(ctx.var_name_tab, Some(3));
        assert_eq!(ctx.allele_tab, Some(4));
        assert_eq!(ctx.failed_tab, Some(5));
        assert_eq!(ctx.var_synonyms_tab, Some(14));
    }

    #[test]
    fn variation_context_extra_columns() {
        let mut cols = standard_cols();
        cols.push("AFR".to_string());
        cols.push("AF".to_string());
        let info = make_cache_info(cols, vec![]);
        let ctx = VariationContext::new(&info);
        assert_eq!(ctx.extra_columns.len(), 2);
        assert_eq!(ctx.extra_columns[0].1, "AFR");
        assert_eq!(ctx.extra_columns[1].1, "AF");
    }

    #[test]
    fn variation_context_alternate_names() {
        let cols: Vec<String> = vec!["seq_region_name", "pos", "end", "id", "alleles"]
            .into_iter()
            .map(String::from)
            .collect();
        let info = make_cache_info(cols, vec![]);
        let ctx = VariationContext::new(&info);
        assert_eq!(ctx.chrom_tab, Some(0));
        assert_eq!(ctx.start_tab, Some(1));
        assert_eq!(ctx.var_name_tab, Some(3));
        assert_eq!(ctx.allele_tab, Some(4));
    }

    // -----------------------------------------------------------------------
    // SourceIdWriter
    // -----------------------------------------------------------------------

    fn make_source_id_writer() -> SourceIdWriter {
        let sources = standard_sources();
        let source_to_id: HashMap<String, String> = sources
            .iter()
            .map(|s| (s.source_key.clone(), s.ids_column.clone()))
            .collect();
        // Create a dummy column map that has dbsnp_ids=0, cosmic_ids=1, clinvar_ids=2, hgmd_public_ids=3
        let mut map_entries = HashMap::new();
        map_entries.insert("dbsnp_ids".to_string(), 0usize);
        map_entries.insert("cosmic_ids".to_string(), 1usize);
        map_entries.insert("clinvar_ids".to_string(), 2usize);
        map_entries.insert("hgmd_public_ids".to_string(), 3usize);
        let col_map = crate::util::ColumnMap::from_map(map_entries);
        SourceIdWriter::new(&col_map, &source_to_id)
    }

    #[test]
    fn source_id_writer_rs_pattern() {
        let mut writer = make_source_id_writer();
        writer.clear();
        writer.populate(Some("rs12345"), None);
        assert_eq!(writer.slot_buffer("dbsnp"), Some("rs12345"));
    }

    #[test]
    fn source_id_writer_cosm_pattern() {
        let mut writer = make_source_id_writer();
        writer.clear();
        writer.populate(Some("COSM12345"), None);
        assert_eq!(writer.slot_buffer("cosmic"), Some("COSM12345"));
    }

    #[test]
    fn source_id_writer_cosv_pattern() {
        let mut writer = make_source_id_writer();
        writer.clear();
        writer.populate(Some("COSV12345"), None);
        assert_eq!(writer.slot_buffer("cosmic"), Some("COSV12345"));
    }

    #[test]
    fn source_id_writer_cm_pattern() {
        let mut writer = make_source_id_writer();
        writer.clear();
        writer.populate(Some("CM001234"), None);
        assert_eq!(writer.slot_buffer("hgmd_public"), Some("CM001234"));
    }

    #[test]
    fn source_id_writer_rcv_pattern() {
        let mut writer = make_source_id_writer();
        writer.clear();
        writer.populate(Some("RCV000012345"), None);
        assert_eq!(writer.slot_buffer("clinvar"), Some("RCV000012345"));
    }

    #[test]
    fn source_id_writer_var_synonyms_double_colon() {
        let mut writer = make_source_id_writer();
        writer.clear();
        writer.populate(None, Some("dbSNP::rs111,dbSNP::rs222"));
        assert_eq!(writer.slot_buffer("dbsnp"), Some("rs111,rs222"));
    }

    #[test]
    fn source_id_writer_var_synonyms_single_colon() {
        let mut writer = make_source_id_writer();
        writer.clear();
        writer.populate(None, Some("COSMIC:COSM100,COSM200"));
        assert_eq!(writer.slot_buffer("cosmic"), Some("COSM100,COSM200"));
    }

    #[test]
    fn source_id_writer_dedup() {
        let mut writer = make_source_id_writer();
        writer.clear();
        writer.populate(Some("rs123"), Some("dbSNP::rs123"));
        assert_eq!(writer.slot_buffer("dbsnp"), Some("rs123"));
    }

    #[test]
    fn source_id_writer_mixed_sources() {
        let mut writer = make_source_id_writer();
        writer.clear();
        writer.populate(Some("rs123"), Some("COSMIC::COSM456,ClinVar::RCV789"));
        assert_eq!(writer.slot_buffer("dbsnp"), Some("rs123"));
        assert_eq!(writer.slot_buffer("cosmic"), Some("COSM456"));
        assert_eq!(writer.slot_buffer("clinvar"), Some("RCV789"));
    }

    #[test]
    fn source_id_writer_empty_values_skipped() {
        let mut writer = make_source_id_writer();
        writer.clear();
        writer.populate(None, Some("dbSNP::.,dbSNP::"));
        assert_eq!(writer.slot_buffer("dbsnp"), None);
    }

    #[test]
    fn source_id_writer_clear_resets() {
        let mut writer = make_source_id_writer();
        writer.clear();
        writer.populate(Some("rs123"), None);
        assert_eq!(writer.slot_buffer("dbsnp"), Some("rs123"));
        writer.clear();
        assert_eq!(writer.slot_buffer("dbsnp"), None);
    }

    #[test]
    fn source_id_writer_is_active() {
        let writer = make_source_id_writer();
        assert!(writer.is_active());
    }

    #[test]
    fn source_id_writer_inactive_when_no_columns() {
        let source_to_id: HashMap<String, String> = HashMap::new();
        let col_map = crate::util::ColumnMap::from_map(HashMap::new());
        let writer = SourceIdWriter::new(&col_map, &source_to_id);
        assert!(!writer.is_active());
    }

    // -----------------------------------------------------------------------
    // parse_variation_line_into
    // -----------------------------------------------------------------------

    fn setup_variation_parser() -> (
        BatchBuilder,
        VariationColumnIndices,
        VariationContext,
        ProvenanceWriter,
        SourceIdWriter,
    ) {
        let info = make_cache_info(standard_cols(), standard_sources());
        let schema = variation_schema(&info, false).unwrap();
        let col_map = ColumnMap::from_schema(&schema);
        let ctx = VariationContext::new(&info);
        let col_idx = VariationColumnIndices::new(&col_map, &ctx);
        let provenance = ProvenanceWriter::new(&col_map, &info);
        let source_id_writer = SourceIdWriter::new(&col_map, ctx.source_to_id_column());
        let builder = BatchBuilder::new(schema, 64).unwrap();
        (builder, col_idx, ctx, provenance, source_id_writer)
    }

    #[test]
    fn parse_valid_line() {
        let (mut batch, col_idx, ctx, prov, mut sid) = setup_variation_parser();
        let predicate = SimplePredicate::default();
        let line = "1\t100\t100\trs12345\tA/G\t0\t0\t1\tG\t0.45\t.\t0\t.\t.\t.";
        let result = parse_variation_line_into(
            line, "test.gz", &predicate, 1_000_000, false, &mut batch, &col_idx, &ctx, &prov,
            &mut sid,
        )
        .unwrap();
        assert_eq!(result, VariationParseResult::Added);
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn parse_blank_line_skipped() {
        let (mut batch, col_idx, ctx, prov, mut sid) = setup_variation_parser();
        let predicate = SimplePredicate::default();
        let result = parse_variation_line_into(
            "", "test.gz", &predicate, 1_000_000, false, &mut batch, &col_idx, &ctx, &prov,
            &mut sid,
        )
        .unwrap();
        assert_eq!(result, VariationParseResult::Skipped);
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn parse_comment_line_skipped() {
        let (mut batch, col_idx, ctx, prov, mut sid) = setup_variation_parser();
        let predicate = SimplePredicate::default();
        let result = parse_variation_line_into(
            "# header comment",
            "test.gz",
            &predicate,
            1_000_000,
            false,
            &mut batch,
            &col_idx,
            &ctx,
            &prov,
            &mut sid,
        )
        .unwrap();
        assert_eq!(result, VariationParseResult::Skipped);
    }

    #[test]
    fn parse_missing_chrom_malformed() {
        let (mut batch, col_idx, ctx, prov, mut sid) = setup_variation_parser();
        let predicate = SimplePredicate::default();
        let line = ".\t100\t100\trs12345\tA/G";
        let result = parse_variation_line_into(
            line, "test.gz", &predicate, 1_000_000, false, &mut batch, &col_idx, &ctx, &prov,
            &mut sid,
        )
        .unwrap();
        assert_eq!(result, VariationParseResult::Malformed);
    }

    #[test]
    fn parse_missing_start_malformed() {
        let (mut batch, col_idx, ctx, prov, mut sid) = setup_variation_parser();
        let predicate = SimplePredicate::default();
        let line = "1\t.\t100\trs12345\tA/G";
        let result = parse_variation_line_into(
            line, "test.gz", &predicate, 1_000_000, false, &mut batch, &col_idx, &ctx, &prov,
            &mut sid,
        )
        .unwrap();
        assert_eq!(result, VariationParseResult::Malformed);
    }

    #[test]
    fn parse_predicate_filters_chrom() {
        let (mut batch, col_idx, ctx, prov, mut sid) = setup_variation_parser();
        let predicate = SimplePredicate {
            chrom: Some("2".to_string()),
            ..Default::default()
        };
        let line = "1\t100\t100\trs12345\tA/G\t0\t0\t1\tG\t0.45\t.\t0\t.\t.\t.";
        let result = parse_variation_line_into(
            line, "test.gz", &predicate, 1_000_000, false, &mut batch, &col_idx, &ctx, &prov,
            &mut sid,
        )
        .unwrap();
        assert_eq!(result, VariationParseResult::Skipped);
    }

    #[test]
    fn parse_zero_based_coordinate() {
        let (mut batch, col_idx, ctx, prov, mut sid) = setup_variation_parser();
        let predicate = SimplePredicate::default();
        let line = "1\t100\t100\trs12345\tA/G\t0\t0\t1\tG\t0.45\t.\t0\t.\t.\t.";
        parse_variation_line_into(
            line, "test.gz", &predicate, 1_000_000, true, // zero-based
            &mut batch, &col_idx, &ctx, &prov, &mut sid,
        )
        .unwrap();
        // Start should be 99 (100 - 1) in zero-based
        let rb = batch.finish().unwrap();
        let starts = rb
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(starts.value(0), 99);
    }
}
