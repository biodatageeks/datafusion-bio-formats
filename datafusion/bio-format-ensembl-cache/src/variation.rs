use crate::errors::{Result, exec_err};
use crate::filter::SimplePredicate;
use crate::info::CacheInfo;
use crate::util::{
    BatchBuilder, ColumnMap, ProvenanceWriter, normalize_genomic_end, normalize_genomic_start,
    normalize_nullable_ref, parse_f64_ref, parse_i8_ref, parse_i64_ref,
};
use std::collections::HashMap;

// Maximum number of tab-separated fields we support per variation line.
// Ensembl VEP variation lines typically have 20-30 fields.
const MAX_VARIATION_FIELDS: usize = 48;

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
    count: usize,
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
                    count: 0,
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
            slot.count = 0;
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
        // Dedup: check if value already present in comma-separated buffer
        if slot.count > 0 && slot.buffer.split(',').any(|v| v == value) {
            return;
        }
        if slot.count > 0 {
            slot.buffer.push(',');
        }
        slot.buffer.push_str(value);
        slot.count += 1;
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
            if slot.count > 0 {
                batch.set_utf8(slot.builder_idx, &slot.buffer);
            } else {
                batch.set_null(slot.builder_idx);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public helpers
// ---------------------------------------------------------------------------

pub(crate) fn detect_region_size(
    cache_info: &CacheInfo,
    variation_files: &[std::path::PathBuf],
) -> i64 {
    if let Some(size) = cache_info.cache_region_size {
        if size > 0 {
            return size;
        }
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
    col_map: &ColumnMap,
    ctx: &VariationContext,
    provenance: &ProvenanceWriter,
    source_id_writer: &mut SourceIdWriter,
) -> Result<bool> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(false);
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

    // Extract required fields using zero-copy refs
    let chrom_ref = field_at(ctx.chrom_tab)
        .and_then(normalize_nullable_ref)
        .ok_or_else(|| {
            exec_err(format!(
                "Variation row missing required chrom in {}: {}",
                source_file_str, trimmed
            ))
        })?;

    let source_start = parse_i64_ref(field_at(ctx.start_tab)).ok_or_else(|| {
        exec_err(format!(
            "Variation row missing required start in {}: {}",
            source_file_str, trimmed
        ))
    })?;

    let source_end = parse_i64_ref(field_at(ctx.end_tab)).unwrap_or(source_start);

    let start = normalize_genomic_start(source_start, coordinate_system_zero_based);
    let end = normalize_genomic_end(source_end, coordinate_system_zero_based);

    if !predicate.matches(chrom_ref, start, end) {
        return Ok(false);
    }

    let variation_name_ref = field_at(ctx.var_name_tab)
        .and_then(normalize_nullable_ref)
        .ok_or_else(|| {
            exec_err(format!(
                "Variation row missing required variation_name in {}: {}",
                source_file_str, trimmed
            ))
        })?;

    let allele_string_ref = field_at(ctx.allele_tab)
        .and_then(normalize_nullable_ref)
        .ok_or_else(|| {
            exec_err(format!(
                "Variation row missing required allele_string in {}: {}",
                source_file_str, trimmed
            ))
        })?;

    let region_bin = ((source_start.saturating_sub(1)) / cache_region_size.max(1)).max(0);

    // Write required columns
    if let Some(idx) = col_map.get("chrom") {
        batch.set_utf8(idx, chrom_ref);
    }
    if let Some(idx) = col_map.get("start") {
        batch.set_i64(idx, start);
    }
    if let Some(idx) = col_map.get("end") {
        batch.set_i64(idx, end);
    }
    if let Some(idx) = col_map.get("variation_name") {
        batch.set_utf8(idx, variation_name_ref);
    }
    if let Some(idx) = col_map.get("allele_string") {
        batch.set_utf8(idx, allele_string_ref);
    }
    if let Some(idx) = col_map.get("region_bin") {
        batch.set_i64(idx, region_bin);
    }

    // Write optional canonical columns (only if projected)
    if let Some(idx) = col_map.get("failed") {
        batch.set_opt_i8(idx, parse_i8_ref(field_at(ctx.failed_tab)));
    }
    if let Some(idx) = col_map.get("somatic") {
        batch.set_opt_i8(idx, parse_i8_ref(field_at(ctx.somatic_tab)));
    }
    if let Some(idx) = col_map.get("strand") {
        batch.set_opt_i8(idx, parse_i8_ref(field_at(ctx.strand_tab)));
    }
    if let Some(idx) = col_map.get("minor_allele") {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.minor_allele_tab).and_then(normalize_nullable_ref),
        );
    }
    if let Some(idx) = col_map.get("minor_allele_freq") {
        batch.set_opt_f64(idx, parse_f64_ref(field_at(ctx.minor_allele_freq_tab)));
    }
    if let Some(idx) = col_map.get("clin_sig") {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.clin_sig_tab).and_then(normalize_nullable_ref),
        );
    }
    if let Some(idx) = col_map.get("phenotype_or_disease") {
        batch.set_opt_i8(idx, parse_i8_ref(field_at(ctx.phenotype_or_disease_tab)));
    }
    if let Some(idx) = col_map.get("clinical_impact") {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.clinical_impact_tab).and_then(normalize_nullable_ref),
        );
    }
    if let Some(idx) = col_map.get("pubmed") {
        batch.set_opt_utf8(
            idx,
            field_at(ctx.pubmed_tab).and_then(normalize_nullable_ref),
        );
    }
    if let Some(idx) = col_map.get("var_synonyms") {
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

    // Extra columns (dynamic, not in known set)
    for (tab_idx, col_name) in &ctx.extra_columns {
        if let Some(builder_idx) = col_map.get(col_name) {
            if *tab_idx < field_count {
                batch.set_opt_utf8(builder_idx, normalize_nullable_ref(fields[*tab_idx]));
            }
        }
    }

    // Provenance columns (pre-computed indices, no HashMap lookups)
    provenance.write(batch, source_file_str);

    batch.finish_row();
    Ok(true)
}
