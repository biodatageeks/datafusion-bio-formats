//! VCF header builder for constructing VCF headers from Arrow schemas
//!
//! This module provides functionality for building VCF header lines from Arrow schemas,
//! enabling round-trip VCF read/write operations. Header information is reconstructed from:
//! - Schema-level metadata: file format version, FILTER, CONTIG, and ALT definitions (stored as JSON)
//! - Field-level metadata: INFO/FORMAT field descriptions, types, and numbers (using `bio.vcf.field.*` keys)
//!
//! When metadata is not available, sensible defaults are generated from Arrow types.

use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::common::Result;
use datafusion_bio_format_core::metadata::{
    AltAlleleMetadata, ContigMetadata, FilterMetadata, VCF_ALTERNATIVE_ALLELES_KEY,
    VCF_CONTIGS_KEY, VCF_FIELD_DESCRIPTION_KEY, VCF_FIELD_FORMAT_ID_KEY, VCF_FIELD_NUMBER_KEY,
    VCF_FIELD_TYPE_KEY, VCF_FILE_FORMAT_KEY, VCF_FILTERS_KEY, VCF_HEADER_RAW_LINES_KEY,
    from_json_string,
};
use std::collections::HashSet;

/// Index of the CHROM column in VCF schema
pub const CHROM_IDX: usize = 0;
/// Index of the START (POS) column in VCF schema
pub const START_IDX: usize = 1;
/// Index of the END column in VCF schema
pub const END_IDX: usize = 2;
/// Index of the ID column in VCF schema
pub const ID_IDX: usize = 3;
/// Index of the REF column in VCF schema
pub const REF_IDX: usize = 4;
/// Index of the ALT column in VCF schema
pub const ALT_IDX: usize = 5;
/// Index of the QUAL column in VCF schema
pub const QUAL_IDX: usize = 6;
/// Index of the FILTER column in VCF schema
pub const FILTER_IDX: usize = 7;
/// Number of core VCF columns (before INFO fields)
pub const CORE_FIELD_COUNT: usize = 8;

/// Builds VCF header lines from an Arrow schema with INFO and FORMAT field definitions
///
/// Reconstructs VCF header lines from schema metadata:
/// - File format version from `bio.vcf.file_format` (defaults to VCFv4.3)
/// - FILTER definitions from `bio.vcf.filters` (JSON array)
/// - CONTIG definitions from `bio.vcf.contigs` (JSON array)
/// - ALT allele definitions from `bio.vcf.alternative_alleles` (JSON array)
/// - INFO/FORMAT field metadata from `bio.vcf.field.*` keys
///
/// When metadata is absent, defaults are inferred from Arrow types.
///
/// # Arguments
///
/// * `schema` - The Arrow schema containing field definitions and metadata
/// * `info_fields` - List of INFO field names to include
/// * `format_fields` - List of FORMAT field names per sample
/// * `sample_names` - List of sample names from the original VCF
///
/// # Returns
///
/// A vector of VCF header lines (without the column header line)
pub fn build_vcf_header_lines(
    schema: &SchemaRef,
    info_fields: &[String],
    format_fields: &[String],
    sample_names: &[String],
) -> Result<Vec<String>> {
    let mut lines = Vec::new();
    let schema_metadata = schema.metadata();

    // Verbatim passthrough: when the source header was captured as text, re-emit
    // it unchanged. Reconstruction below can only express what the typed metadata
    // carries, which silently drops ##fileDate, ##source, tool provenance such as
    // ##bcftools_*, contig attributes beyond ID and length, the implicit PASS
    // filter, and the original ordering of all of them.
    if let Some(raw_json) = schema_metadata.get(VCF_HEADER_RAW_LINES_KEY)
        && let Some(raw_lines) = from_json_string::<Vec<String>>(raw_json)
    {
        return Ok(passthrough_header_lines(
            raw_lines,
            schema,
            info_fields,
            format_fields,
            sample_names,
        ));
    }

    // Get file format from schema metadata (default to VCFv4.3)
    let file_format = schema_metadata
        .get(VCF_FILE_FORMAT_KEY)
        .map(|s| s.as_str())
        .unwrap_or("VCFv4.3");
    lines.push(format!("##fileformat={file_format}"));

    // Add FILTER definitions from metadata using shared utilities
    if let Some(filters_json) = schema_metadata.get(VCF_FILTERS_KEY)
        && let Some(filters) = from_json_string::<Vec<FilterMetadata>>(filters_json)
    {
        for filter in filters {
            if filter.id != "PASS" {
                // PASS is implicit
                lines.push(format!(
                    "##FILTER=<ID={},Description=\"{}\">",
                    filter.id, filter.description
                ));
            }
        }
    }

    // Add CONTIG definitions from metadata using shared utilities
    if let Some(contigs_json) = schema_metadata.get(VCF_CONTIGS_KEY)
        && let Some(contigs) = from_json_string::<Vec<ContigMetadata>>(contigs_json)
    {
        for contig in contigs {
            let mut line = format!("##contig=<ID={}", contig.id);
            if let Some(length) = contig.length {
                line.push_str(&format!(",length={length}"));
            }
            line.push('>');
            lines.push(line);
        }
    }

    // Add ALT definitions from metadata using shared utilities
    if let Some(alts_json) = schema_metadata.get(VCF_ALTERNATIVE_ALLELES_KEY)
        && let Some(alts) = from_json_string::<Vec<AltAlleleMetadata>>(alts_json)
    {
        for alt in alts {
            lines.push(format!(
                "##ALT=<ID={},Description=\"{}\">",
                alt.id, alt.description
            ));
        }
    }

    // Add INFO field definitions
    for info_name in info_fields {
        // Look up field by name to support any column order
        if let Ok(field_idx) = schema.index_of(info_name) {
            let field = schema.field(field_idx);
            let (vcf_type, number, description) = get_info_field_metadata(field, info_name);

            lines.push(format!(
                "##INFO=<ID={info_name},Number={number},Type={vcf_type},Description=\"{description}\">"
            ));
        }
    }

    // Add FORMAT field definitions. Deduplicate by name while preserving
    // first-occurrence order: iterating a HashSet directly would emit the
    // ##FORMAT lines in a per-process-randomized order, making VCF output
    // non-byte-reproducible run to run.
    let mut seen = HashSet::new();
    for format_name in format_fields
        .iter()
        .filter(|name| seen.insert(name.as_str()))
    {
        // Find the first occurrence to get the type (try both naming conventions)
        if let Some(field) = find_format_field(schema, format_name, sample_names) {
            let (vcf_type, number, description) = get_format_field_metadata(field, format_name);

            lines.push(format!(
                "##FORMAT=<ID={format_name},Number={number},Type={vcf_type},Description=\"{description}\">"
            ));
        }
    }

    Ok(lines)
}

/// A structured header declaration the current schema would emit.
struct CurrentDeclaration {
    /// `"##INFO="`, `"##FORMAT="`, `"##FILTER="`, `"##contig="` or `"##ALT="`.
    kind: &'static str,
    id: String,
    /// The attributes the typed schema can represent, compared against a raw
    /// line to decide whether it was redefined. `None` means the schema
    /// expects the attribute to be absent, which is a real expectation and not
    /// "skip this check". Attributes not listed here are invisible to the
    /// schema and must not count as evidence of a change.
    compare: Vec<(&'static str, Option<String>)>,
    rendered: String,
}

/// Parses the `key=value` attributes of a `##KEY=<...>` header line.
///
/// Quoted values may contain commas and `\\`-escaped characters, so this cannot
/// be a plain `split(',')`. Returns `None` when the line is not a structured
/// declaration, which callers treat as "leave it alone".
fn parse_structured_attributes(line: &str) -> Option<Vec<(String, String)>> {
    let body = line.split_once('<')?.1.strip_suffix('>')?;
    let mut attributes = Vec::new();
    let mut chars = body.chars().peekable();

    loop {
        let mut key = String::new();
        for c in chars.by_ref() {
            if c == '=' {
                break;
            }
            key.push(c);
        }
        if key.is_empty() {
            return None;
        }

        let mut value = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            let mut closed = false;
            while let Some(c) = chars.next() {
                match c {
                    '\\' => value.push(chars.next()?),
                    '"' => {
                        closed = true;
                        break;
                    }
                    _ => value.push(c),
                }
            }
            if !closed {
                return None;
            }
            // Consume the separator after a quoted value.
            match chars.next() {
                None => {
                    attributes.push((key, value));
                    break;
                }
                Some(',') => {}
                Some(_) => return None,
            }
        } else {
            let mut ended = false;
            for c in chars.by_ref() {
                if c == ',' {
                    ended = true;
                    break;
                }
                value.push(c);
            }
            if !ended {
                attributes.push((key, value));
                break;
            }
        }
        attributes.push((key, value));
    }

    Some(attributes)
}

impl CurrentDeclaration {
    /// True when `raw` declares this field with the same Number, Type and
    /// Description — the only attributes the typed schema retains.
    ///
    /// Everything else on the line (the optional `Source`/`Version` attributes,
    /// the exact escaping of the description, attribute order) is invisible to
    /// the schema, so it must not count as evidence of a redefinition: comparing
    /// the rendered line byte for byte would discard valid metadata this module
    /// exists to preserve. A line that cannot be parsed is left alone.
    fn matches_raw(&self, raw: &str) -> bool {
        let Some(attributes) = parse_structured_attributes(raw) else {
            return true;
        };
        self.compare.iter().all(|(key, expected)| {
            let actual = attributes
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.as_str());
            actual == expected.as_deref()
        })
    }

    /// True when `raw` is a declaration of this same kind and ID.
    fn declares(&self, raw: &str) -> bool {
        raw.strip_prefix(self.kind)
            .and_then(|rest| rest.strip_prefix("<ID="))
            .is_some_and(|rest| {
                rest.strip_prefix(self.id.as_str())
                    .is_some_and(|tail| tail.starts_with(',') || tail.starts_with('>'))
            })
    }
}

/// True when `raw`'s `ID=` attribute is present in `ids`.
///
/// A declaration that cannot be parsed, or that carries no `ID`, is treated as
/// present so the conservative default stays "keep what the source said".
fn declaration_id_is_in(raw: &str, ids: &HashSet<String>) -> bool {
    let Some(attributes) = parse_structured_attributes(raw) else {
        return true;
    };
    match attributes.iter().find(|(k, _)| k == "ID") {
        Some((_, id)) => ids.contains(id),
        None => true,
    }
}

/// Re-emits a captured source header, reconciled against the current schema.
///
/// Raw lines pass through untouched — including everything the typed model
/// cannot represent — with one exception: a raw `##INFO`/`##FORMAT` declaration
/// whose Number, Type or Description disagrees with the current schema was
/// redefined downstream (as an annotator does when it rewrites `CSQ`). Such a
/// line is dropped from its original position and the current definition is
/// appended, alongside definitions for fields the source header never declared.
///
/// Agreement on those three attributes is enough to keep the source line exactly
/// where it was, even if this module would have rendered it differently.
fn passthrough_header_lines(
    raw_lines: Vec<String>,
    schema: &SchemaRef,
    info_fields: &[String],
    format_fields: &[String],
    sample_names: &[String],
) -> Vec<String> {
    // Deduplicate by name in first-occurrence order so output stays
    // byte-reproducible run to run.
    let mut seen = HashSet::new();
    let mut current: Vec<CurrentDeclaration> = Vec::new();
    let schema_metadata = schema.metadata();

    // Kinds whose typed key, when present, is an authoritative statement of which
    // declarations the header should carry. A raw declaration of such a kind
    // whose ID is absent from the list was removed downstream and must not be
    // re-emitted.
    //
    // INFO and FORMAT are deliberately excluded: `info_fields`/`format_fields`
    // are a projection, not a statement about the header. Reading only DP must
    // not strip AF's declaration — an unused declaration is valid VCF, and both
    // bcftools and Ensembl VEP keep the whole input header.
    let mut authoritative: Vec<(&'static str, HashSet<String>)> = Vec::new();

    // Schema-level declarations. Only the attributes the typed model carries are
    // compared: ContigMetadata holds id and length, so a contig whose length
    // still agrees keeps its raw line — and with it `assembly`, `md5` and
    // anything else the model cannot represent.
    if let Some(json) = schema_metadata.get(VCF_FILTERS_KEY)
        && let Some(filters) = from_json_string::<Vec<FilterMetadata>>(json)
    {
        authoritative.push(("##FILTER=", filters.iter().map(|f| f.id.clone()).collect()));
        for filter in filters {
            if !seen.insert(("FILTER", filter.id.clone())) {
                continue;
            }
            current.push(CurrentDeclaration {
                rendered: format!(
                    "##FILTER=<ID={},Description=\"{}\">",
                    filter.id, filter.description
                ),
                kind: "##FILTER=",
                compare: vec![("Description", Some(filter.description))],
                id: filter.id,
            });
        }
    }

    if let Some(json) = schema_metadata.get(VCF_CONTIGS_KEY)
        && let Some(contigs) = from_json_string::<Vec<ContigMetadata>>(json)
    {
        authoritative.push(("##contig=", contigs.iter().map(|c| c.id.clone()).collect()));
        for contig in contigs {
            if !seen.insert(("contig", contig.id.clone())) {
                continue;
            }
            let mut rendered = format!("##contig=<ID={}", contig.id);
            if let Some(length) = contig.length {
                rendered.push_str(&format!(",length={length}"));
            }
            rendered.push('>');
            // `None` here means "the schema declares no length", so a raw line
            // that still carries one has been superseded.
            let compare = vec![("length", contig.length.map(|l| l.to_string()))];
            current.push(CurrentDeclaration {
                kind: "##contig=",
                id: contig.id,
                compare,
                rendered,
            });
        }
    }

    if let Some(json) = schema_metadata.get(VCF_ALTERNATIVE_ALLELES_KEY)
        && let Some(alts) = from_json_string::<Vec<AltAlleleMetadata>>(json)
    {
        authoritative.push(("##ALT=", alts.iter().map(|a| a.id.clone()).collect()));
        for alt in alts {
            if !seen.insert(("ALT", alt.id.clone())) {
                continue;
            }
            current.push(CurrentDeclaration {
                rendered: format!("##ALT=<ID={},Description=\"{}\">", alt.id, alt.description),
                kind: "##ALT=",
                compare: vec![("Description", Some(alt.description))],
                id: alt.id,
            });
        }
    }

    for name in info_fields {
        if !seen.insert(("INFO", name.clone())) {
            continue;
        }
        let Ok(idx) = schema.index_of(name) else {
            continue;
        };
        let (vcf_type, number, description) = get_info_field_metadata(schema.field(idx), name);
        current.push(CurrentDeclaration {
            rendered: format!(
                "##INFO=<ID={name},Number={number},Type={vcf_type},Description=\"{description}\">"
            ),
            kind: "##INFO=",
            id: name.clone(),
            compare: vec![
                ("Number", Some(number)),
                ("Type", Some(vcf_type)),
                ("Description", Some(description)),
            ],
        });
    }

    for name in format_fields {
        if !seen.insert(("FORMAT", name.clone())) {
            continue;
        }
        let Some(field) = find_format_field(schema, name, sample_names) else {
            continue;
        };
        let (vcf_type, number, description) = get_format_field_metadata(field, name);
        current.push(CurrentDeclaration {
            rendered: format!(
                "##FORMAT=<ID={name},Number={number},Type={vcf_type},Description=\"{description}\">"
            ),
            kind: "##FORMAT=",
            id: name.clone(),
            compare: vec![
                ("Number", Some(number)),
                ("Type", Some(vcf_type)),
                ("Description", Some(description)),
            ],
        });
    }

    // `##fileformat` is unstructured and must stay the first line, so a change is
    // substituted in place rather than dropped and re-appended.
    let file_format_line = schema_metadata
        .get(VCF_FILE_FORMAT_KEY)
        .map(|ff| format!("##fileformat={ff}"));

    let mut lines: Vec<String> = Vec::with_capacity(raw_lines.len() + current.len());
    let mut satisfied: HashSet<usize> = HashSet::new();

    for raw in raw_lines {
        if raw.starts_with("##fileformat=")
            && let Some(current_line) = &file_format_line
        {
            lines.push(current_line.clone());
            continue;
        }
        if authoritative
            .iter()
            .any(|(kind, ids)| raw.starts_with(kind) && !declaration_id_is_in(&raw, ids))
        {
            continue;
        }
        match current.iter().position(|d| d.declares(&raw)) {
            // Redefined downstream: drop here, re-emit at the end.
            Some(i) if !current[i].matches_raw(&raw) => continue,
            // Semantically unchanged: keep the source line exactly as it was.
            Some(i) => {
                satisfied.insert(i);
                lines.push(raw);
            }
            None => lines.push(raw),
        }
    }

    for (i, declaration) in current.iter().enumerate() {
        if !satisfied.contains(&i) {
            lines.push(declaration.rendered.clone());
        }
    }

    lines
}

/// Extracts VCF metadata from an INFO field, using stored metadata if available
///
/// Reads metadata from `bio.vcf.field.*` keys. If not present, generates defaults
/// from the Arrow data type.
fn get_info_field_metadata(field: &Field, field_name: &str) -> (String, String, String) {
    let metadata = field.metadata();

    // Get stored VCF metadata using bio.vcf.field.* keys
    let vcf_type = metadata
        .get(VCF_FIELD_TYPE_KEY)
        .cloned()
        .unwrap_or_else(|| arrow_type_to_vcf_type(field.data_type()).to_string());

    let number = metadata
        .get(VCF_FIELD_NUMBER_KEY)
        .cloned()
        .unwrap_or_else(|| arrow_type_to_vcf_number(field.data_type()).to_string());

    let description = metadata
        .get(VCF_FIELD_DESCRIPTION_KEY)
        .cloned()
        .unwrap_or_else(|| format!("{field_name} field"));

    (vcf_type, number, description)
}

/// Extracts VCF metadata from a FORMAT field, using stored metadata if available
///
/// Reads metadata from `bio.vcf.field.*` keys. If not present, generates defaults
/// from the Arrow data type (with special handling for GT fields).
/// In the columnar multi-sample schema the field type is `List<T>`; the function
/// unwraps one List level to derive the scalar VCF type for the header.
fn get_format_field_metadata(field: &Field, format_name: &str) -> (String, String, String) {
    let metadata = field.metadata();

    // For columnar multi-sample fields, unwrap List<T>/LargeList<T> → T for type inference.
    let scalar_type = match field.data_type() {
        DataType::List(inner) | DataType::LargeList(inner) => inner.data_type(),
        other => other,
    };

    // Get stored VCF metadata using bio.vcf.field.* keys
    let vcf_type = metadata
        .get(VCF_FIELD_TYPE_KEY)
        .cloned()
        .unwrap_or_else(|| {
            // GT is always a string
            if format_name == "GT" {
                "String".to_string()
            } else {
                arrow_type_to_vcf_type(scalar_type).to_string()
            }
        });

    let number = metadata
        .get(VCF_FIELD_NUMBER_KEY)
        .cloned()
        .unwrap_or_else(|| arrow_type_to_vcf_number(scalar_type).to_string());

    let description = metadata
        .get(VCF_FIELD_DESCRIPTION_KEY)
        .cloned()
        .unwrap_or_else(|| format!("{format_name} format field"));

    (vcf_type, number, description)
}

/// Finds a FORMAT field in the schema by name (handles both single and multi-sample naming).
///
/// For single-sample VCFs, FORMAT columns may be prefixed with "fmt_" when their name
/// collides with an INFO column. This function checks `bio.vcf.field.format_id` metadata
/// as a fallback when direct name lookup fails.
fn find_format_field<'a>(
    schema: &'a SchemaRef,
    format_name: &str,
    _sample_names: &[String],
) -> Option<&'a Field> {
    // First try direct name lookup (single sample case, no collision).
    // Only return early if the field has matching format_id metadata, confirming it's
    // actually a FORMAT field. Without this check, an INFO field with the same name
    // (e.g., INFO "DP") would be returned instead of the renamed FORMAT "fmt_DP".
    if let Ok(idx) = schema.index_of(format_name) {
        let field = schema.field(idx);
        if field
            .metadata()
            .get(VCF_FIELD_FORMAT_ID_KEY)
            .is_some_and(|id| id == format_name)
        {
            return Some(field);
        }
    }

    // Check for renamed FORMAT columns (e.g., "fmt_DP") via format_id metadata
    for field in schema.fields() {
        if field
            .metadata()
            .get(VCF_FIELD_FORMAT_ID_KEY)
            .is_some_and(|id| id == format_name)
        {
            return Some(field.as_ref());
        }
    }

    // Fallback: check for renamed column by convention when metadata was stripped
    // (e.g., Polars → Arrow conversion drops field-level metadata)
    for prefix in &["fmt_", "format_"] {
        let prefixed = format!("{prefix}{format_name}");
        if let Ok(idx) = schema.index_of(&prefixed) {
            return Some(schema.field(idx));
        }
    }

    // Columnar multisample schema: genotypes: Struct<GT: List<T>, GQ: List<T>, ...>
    if let Ok(idx) = schema.index_of("genotypes") {
        let genotypes_field = schema.field(idx);
        if let DataType::Struct(struct_fields) = genotypes_field.data_type()
            && let Some(field) = struct_fields.iter().find(|f| f.name() == format_name)
        {
            return Some(field.as_ref());
        }
    }

    None
}

/// Builds the VCF column header line
///
/// # Arguments
///
/// * `sample_names` - List of sample names
///
/// # Returns
///
/// The column header line (starting with #CHROM)
pub fn build_vcf_column_header(sample_names: &[String]) -> String {
    let mut header = "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO".to_string();

    if !sample_names.is_empty() {
        header.push_str("\tFORMAT");
        for sample in sample_names {
            header.push('\t');
            header.push_str(sample);
        }
    }

    header
}

/// Converts Arrow DataType to VCF type string
fn arrow_type_to_vcf_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int32 | DataType::Int64 => "Integer",
        DataType::Float32 | DataType::Float64 => "Float",
        DataType::Boolean => "Flag",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "String",
        DataType::List(inner) | DataType::LargeList(inner) => match inner.data_type() {
            DataType::Int32 | DataType::Int64 => "Integer",
            DataType::Float32 | DataType::Float64 => "Float",
            _ => "String",
        },
        _ => "String",
    }
}

/// Converts Arrow DataType to VCF Number string
fn arrow_type_to_vcf_number(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "0",                          // Flag type
        DataType::List(_) | DataType::LargeList(_) => ".", // Variable length
        _ => "1",                                          // Single value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_bio_format_core::metadata::VCF_FIELD_FIELD_TYPE_KEY;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn format_header_lines_preserve_input_order_and_dedup() {
        // FORMAT header lines must be emitted in the deterministic first-occurrence
        // order of `format_fields`, not HashSet iteration order (randomized per
        // process), so VCF output is byte-reproducible run to run. Duplicates
        // collapse to their first occurrence.
        let format_in = [
            "GT", "GQ", "DP", "GT", "AD", "ADALL", "DP", "PS", "PL", "MIN_DP", "SB",
        ];
        let expected = ["GT", "GQ", "DP", "AD", "ADALL", "PS", "PL", "MIN_DP", "SB"];

        let mut fields = vec![Field::new("chrom", DataType::Utf8, false)];
        for id in ["GT", "GQ", "DP", "AD", "ADALL", "PS", "PL", "MIN_DP", "SB"] {
            let mut md = HashMap::new();
            md.insert(VCF_FIELD_FORMAT_ID_KEY.to_string(), id.to_string());
            fields.push(Field::new(id, DataType::Utf8, true).with_metadata(md));
        }
        let schema = Arc::new(Schema::new(fields));
        let format_fields: Vec<String> = format_in.iter().map(|s| s.to_string()).collect();

        let lines =
            build_vcf_header_lines(&schema, &[], &format_fields, &["S".to_string()]).unwrap();
        let got: Vec<&str> = lines
            .iter()
            .filter_map(|l| l.strip_prefix("##FORMAT=<ID="))
            .map(|l| l.split(',').next().unwrap())
            .collect();
        assert_eq!(got, expected);
    }

    /// Builds a schema carrying `raw` as the verbatim source header, plus one
    /// INFO field per `(name, number, type, description)` tuple.
    fn schema_with_raw_header(raw: &[&str], info: &[(&str, &str, &str, &str)]) -> Arc<Schema> {
        let mut fields = vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
        ];
        for (name, number, ty, description) in info {
            let mut md = HashMap::new();
            md.insert(VCF_FIELD_FIELD_TYPE_KEY.to_string(), "INFO".to_string());
            md.insert(VCF_FIELD_NUMBER_KEY.to_string(), number.to_string());
            md.insert(VCF_FIELD_TYPE_KEY.to_string(), ty.to_string());
            md.insert(
                VCF_FIELD_DESCRIPTION_KEY.to_string(),
                description.to_string(),
            );
            fields.push(Field::new(*name, DataType::Utf8, true).with_metadata(md));
        }
        let mut schema_md = HashMap::new();
        schema_md.insert(
            VCF_HEADER_RAW_LINES_KEY.to_string(),
            serde_json::to_string(raw).unwrap(),
        );
        Arc::new(Schema::new(fields).with_metadata(schema_md))
    }

    const SAMPLE_RAW: [&str; 6] = [
        "##fileformat=VCFv4.2",
        "##FILTER=<ID=PASS,Description=\"All filters passed\">",
        "##fileDate=20160824",
        "##contig=<ID=chr1,length=248956422,assembly=GRCh38>",
        "##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Depth\">",
        "##bcftools_normVersion=1.21+htslib-1.21",
    ];

    /// Matches SAMPLE_RAW's DP declaration exactly.
    const DP_INFO: (&str, &str, &str, &str) = ("DP", "1", "Integer", "Depth");
    const CSQ_INFO: (&str, &str, &str, &str) = (
        "CSQ",
        ".",
        "String",
        "Consequence annotations from Ensembl VEP",
    );

    /// Adds typed schema-level metadata on top of a raw-header schema.
    fn with_typed(
        schema: Arc<Schema>,
        file_format: Option<&str>,
        filters: &[(&str, &str)],
        contigs: &[(&str, Option<u64>)],
    ) -> Arc<Schema> {
        let mut md = schema.metadata().clone();
        if let Some(ff) = file_format {
            md.insert(VCF_FILE_FORMAT_KEY.to_string(), ff.to_string());
        }
        let filters: Vec<FilterMetadata> = filters
            .iter()
            .map(|(id, description)| FilterMetadata {
                id: id.to_string(),
                description: description.to_string(),
            })
            .collect();
        md.insert(
            VCF_FILTERS_KEY.to_string(),
            serde_json::to_string(&filters).unwrap(),
        );
        let contigs: Vec<ContigMetadata> = contigs
            .iter()
            .map(|(id, length)| ContigMetadata {
                id: id.to_string(),
                length: *length,
                metadata: HashMap::new(),
            })
            .collect();
        md.insert(
            VCF_CONTIGS_KEY.to_string(),
            serde_json::to_string(&contigs).unwrap(),
        );
        Arc::new(Schema::new(schema.fields().clone()).with_metadata(md))
    }

    #[test]
    fn typed_filter_absent_from_the_raw_header_is_declared() {
        // A FILTER carried on records must be declared, or the output is invalid
        // VCF. Passthrough must not silently omit one the schema knows about.
        let raw = ["##fileformat=VCFv4.2", "##contig=<ID=chr1,length=100>"];
        let schema = with_typed(
            schema_with_raw_header(&raw, &[]),
            None,
            &[("LowQual", "Low quality")],
            &[("chr1", Some(100))],
        );
        let lines = build_vcf_header_lines(&schema, &[], &[], &[]).unwrap();
        assert!(
            lines
                .iter()
                .any(|l| l == "##FILTER=<ID=LowQual,Description=\"Low quality\">"),
            "{lines:#?}"
        );
        assert_eq!(&lines[..2], &raw[..], "raw lines must keep their position");
    }

    #[test]
    fn typed_contig_absent_from_the_raw_header_is_declared() {
        let raw = ["##fileformat=VCFv4.2", "##contig=<ID=chr1,length=100>"];
        let schema = with_typed(
            schema_with_raw_header(&raw, &[]),
            None,
            &[],
            &[("chr1", Some(100)), ("chr2", Some(200))],
        );
        let lines = build_vcf_header_lines(&schema, &[], &[], &[]).unwrap();
        assert!(
            lines.iter().any(|l| l == "##contig=<ID=chr2,length=200>"),
            "{lines:#?}"
        );
    }

    #[test]
    fn typed_file_format_change_supersedes_the_raw_line() {
        let raw = ["##fileformat=VCFv4.2", "##contig=<ID=chr1,length=100>"];
        let schema = with_typed(
            schema_with_raw_header(&raw, &[]),
            Some("VCFv4.3"),
            &[],
            &[("chr1", Some(100))],
        );
        let lines = build_vcf_header_lines(&schema, &[], &[], &[]).unwrap();
        assert!(
            !lines.iter().any(|l| l == "##fileformat=VCFv4.2"),
            "{lines:#?}"
        );
        assert_eq!(lines[0], "##fileformat=VCFv4.3");
    }

    #[test]
    fn removing_a_typed_filter_drops_its_raw_declaration() {
        // `bio.vcf.filters` is an authoritative list: when it is present, it says
        // which FILTERs the header declares. A raw declaration whose ID is no
        // longer listed was deliberately removed downstream.
        let raw = [
            "##fileformat=VCFv4.2",
            "##FILTER=<ID=PASS,Description=\"All filters passed\">",
            "##FILTER=<ID=LowQual,Description=\"Low quality\">",
        ];
        let schema = with_typed(
            schema_with_raw_header(&raw, &[]),
            None,
            &[("PASS", "All filters passed")],
            &[],
        );
        let lines = build_vcf_header_lines(&schema, &[], &[], &[]).unwrap();
        assert_eq!(
            lines,
            vec![
                "##fileformat=VCFv4.2".to_string(),
                "##FILTER=<ID=PASS,Description=\"All filters passed\">".to_string(),
            ]
        );
    }

    #[test]
    fn removing_a_typed_contig_drops_its_raw_declaration() {
        let raw = [
            "##fileformat=VCFv4.2",
            "##contig=<ID=chr1,length=100>",
            "##contig=<ID=chr2,length=200>",
        ];
        let schema = with_typed(
            schema_with_raw_header(&raw, &[]),
            None,
            &[],
            &[("chr1", Some(100))],
        );
        let lines = build_vcf_header_lines(&schema, &[], &[], &[]).unwrap();
        assert!(!lines.iter().any(|l| l.contains("ID=chr2")), "{lines:#?}");
        assert!(lines.iter().any(|l| l == "##contig=<ID=chr1,length=100>"));
    }

    #[test]
    fn raw_declarations_survive_when_no_authoritative_list_exists() {
        // Without the typed key there is no statement about which declarations
        // should exist, so the source header stands.
        let raw = [
            "##fileformat=VCFv4.2",
            "##FILTER=<ID=LowQual,Description=\"Low quality\">",
            "##contig=<ID=chr9,length=900>",
        ];
        let schema = schema_with_raw_header(&raw, &[]); // no filters/contigs keys
        let lines = build_vcf_header_lines(&schema, &[], &[], &[]).unwrap();
        assert_eq!(lines, raw.to_vec());
    }

    #[test]
    fn unprojected_info_declarations_are_not_dropped() {
        // `info_fields` is a projection, not an authoritative list of what the
        // header declares. Reading only DP must not strip AF from the header —
        // an unused declaration is valid VCF, and VEP keeps the whole header.
        let raw = [
            "##fileformat=VCFv4.2",
            "##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Depth\">",
            "##INFO=<ID=AF,Number=A,Type=Float,Description=\"Allele frequency\">",
        ];
        let schema = schema_with_raw_header(&raw, &[DP_INFO]);
        let lines = build_vcf_header_lines(&schema, &["DP".to_string()], &[], &[]).unwrap();
        assert_eq!(lines, raw.to_vec());
    }

    #[test]
    fn dropping_a_contig_length_replaces_the_raw_declaration() {
        // A typed contig with no length renders as `##contig=<ID=chr1>`. Absence
        // is a real expectation, not "nothing to compare": keeping the raw line
        // would re-emit a length the schema no longer declares.
        let raw = ["##fileformat=VCFv4.2", "##contig=<ID=chr1,length=100>"];
        let schema = with_typed(
            schema_with_raw_header(&raw, &[]),
            None,
            &[],
            &[("chr1", None)],
        );
        let lines = build_vcf_header_lines(&schema, &[], &[], &[]).unwrap();
        assert_eq!(
            lines,
            vec![
                "##fileformat=VCFv4.2".to_string(),
                "##contig=<ID=chr1>".to_string(),
            ]
        );
    }

    #[test]
    fn raw_contig_attributes_survive_when_id_and_length_agree() {
        // Regression guard: ContigMetadata holds only id and length, so a
        // reconciliation that compared rendered lines would strip assembly= and
        // md5= from every contig — the exact loss this module exists to prevent.
        let raw = [
            "##fileformat=VCFv4.2",
            "##contig=<ID=chr1,length=100,assembly=GRCh38,md5=abc>",
        ];
        let schema = with_typed(
            schema_with_raw_header(&raw, &[]),
            None,
            &[],
            &[("chr1", Some(100))],
        );
        let lines = build_vcf_header_lines(&schema, &[], &[], &[]).unwrap();
        assert_eq!(lines, raw.to_vec());
    }

    #[test]
    fn raw_header_lines_are_emitted_verbatim_and_in_order() {
        // The typed model cannot represent ##fileDate, ##bcftools_*, the implicit
        // PASS filter, or a contig's assembly= attribute. Reconstruction drops all
        // of them; passthrough is the only way to keep a header byte-identical.
        let schema = schema_with_raw_header(&SAMPLE_RAW, &[DP_INFO]);
        let lines = build_vcf_header_lines(&schema, &["DP".to_string()], &[], &[]).unwrap();
        assert_eq!(lines, SAMPLE_RAW.to_vec());
    }

    #[test]
    fn raw_header_passthrough_appends_only_undeclared_fields() {
        // CSQ is added by the annotator and is not in the source header, so it is
        // appended after the raw block. DP is already declared and must not be
        // emitted twice.
        let schema = schema_with_raw_header(&SAMPLE_RAW, &[DP_INFO, CSQ_INFO]);
        let lines =
            build_vcf_header_lines(&schema, &["DP".to_string(), "CSQ".to_string()], &[], &[])
                .unwrap();

        let dp_count = lines.iter().filter(|l| l.contains("<ID=DP,")).count();
        assert_eq!(dp_count, 1, "DP declared twice: {lines:#?}");

        assert_eq!(&lines[..SAMPLE_RAW.len()], &SAMPLE_RAW[..]);
        assert_eq!(
            lines.last().unwrap(),
            "##INFO=<ID=CSQ,Number=.,Type=String,Description=\"Consequence annotations from Ensembl VEP\">"
        );
    }

    #[test]
    fn raw_declaration_is_superseded_only_when_the_schema_differs() {
        // Re-annotating a file that already carries CSQ must not keep the stale
        // definition. The rule is generic: a raw declaration is kept in place
        // when it matches what the schema would emit, and replaced (dropped from
        // its original position, re-emitted at the end) when it differs.
        let raw = [
            "##fileformat=VCFv4.2",
            "##INFO=<ID=CSQ,Number=.,Type=String,Description=\"stale\">",
            "##contig=<ID=chr1,length=248956422>",
            "##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Depth\">",
        ];
        let schema = schema_with_raw_header(&raw, &[CSQ_INFO, DP_INFO]);
        let lines =
            build_vcf_header_lines(&schema, &["CSQ".to_string(), "DP".to_string()], &[], &[])
                .unwrap();

        assert_eq!(
            lines,
            vec![
                // stale CSQ dropped from position 1
                "##fileformat=VCFv4.2".to_string(),
                "##contig=<ID=chr1,length=248956422>".to_string(),
                // DP matches what the schema would emit, so it stays put
                "##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Depth\">".to_string(),
                // the current CSQ definition is appended
                "##INFO=<ID=CSQ,Number=.,Type=String,Description=\"Consequence annotations from Ensembl VEP\">"
                    .to_string(),
            ]
        );
    }

    #[test]
    fn raw_declaration_with_extra_attributes_is_kept_in_place() {
        // Source and Version are valid optional INFO attributes that the typed
        // schema does not retain. Re-rendering would drop them, so a declaration
        // whose Number/Type/Description still agree must be left untouched.
        let raw = [
            "##fileformat=VCFv4.2",
            "##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Depth\",Source=\"caller\",Version=\"1.2\">",
            "##contig=<ID=chr1,length=100>",
        ];
        let schema = schema_with_raw_header(&raw, &[DP_INFO]);
        let lines = build_vcf_header_lines(&schema, &["DP".to_string()], &[], &[]).unwrap();
        assert_eq!(lines, raw.to_vec());
    }

    #[test]
    fn raw_declaration_with_escaped_description_is_kept_in_place() {
        // The reader unescapes the description, so a naive re-render would not be
        // byte-equal to the source line even though nothing changed.
        let raw = [
            "##fileformat=VCFv4.2",
            "##INFO=<ID=NOTE,Number=1,Type=String,Description=\"say \\\"hi\\\", ok\">",
        ];
        let schema = schema_with_raw_header(&raw, &[("NOTE", "1", "String", "say \"hi\", ok")]);
        let lines = build_vcf_header_lines(&schema, &["NOTE".to_string()], &[], &[]).unwrap();
        assert_eq!(lines, raw.to_vec());
    }

    #[test]
    fn raw_declaration_that_cannot_be_parsed_is_kept_in_place() {
        // Conservative default: without positive evidence of a redefinition, the
        // source line wins.
        let raw = ["##fileformat=VCFv4.2", "##INFO=<ID=DP,malformed"];
        let schema = schema_with_raw_header(&raw, &[DP_INFO]);
        let lines = build_vcf_header_lines(&schema, &["DP".to_string()], &[], &[]).unwrap();
        assert_eq!(lines[0], "##fileformat=VCFv4.2");
        assert_eq!(lines[1], "##INFO=<ID=DP,malformed");
    }

    #[test]
    fn raw_passthrough_keeps_lines_the_typed_model_cannot_represent() {
        // bio-formats is format-generic: it has no opinion about ##VEP or any
        // other tool's provenance, and must not silently drop it. Suppressing
        // tool-specific lines is the caller's business.
        let raw = [
            "##fileformat=VCFv4.2",
            "##VEP=\"v115\" time=\"old\"",
            "##VEP-command-line='vep --old'",
            "##source=myCaller",
            "##reference=file:///GRCh38.fa",
        ];
        let schema = schema_with_raw_header(&raw, &[]);
        let lines = build_vcf_header_lines(&schema, &[], &[], &[]).unwrap();
        assert_eq!(lines, raw.to_vec());
    }

    #[test]
    fn without_raw_header_lines_reconstruction_still_applies() {
        // Parquet and Polars sources have no raw header; that path is unchanged.
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("DP", DataType::Int32, true),
        ]));
        let lines = build_vcf_header_lines(&schema, &["DP".to_string()], &[], &[]).unwrap();
        assert!(lines.iter().any(|l| l.starts_with("##fileformat=")));
        assert!(lines.iter().any(|l| l.contains("##INFO=<ID=DP")));
    }

    #[test]
    fn test_build_vcf_header_lines_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new("DP", DataType::Int32, true),
        ]));

        let info_fields = vec!["DP".to_string()];
        let format_fields = vec![];
        let sample_names = vec![];

        let lines =
            build_vcf_header_lines(&schema, &info_fields, &format_fields, &sample_names).unwrap();

        assert!(lines.iter().any(|l| l.contains("##fileformat=")));
        assert!(lines.iter().any(|l| l.contains("##INFO=<ID=DP")));
    }

    #[test]
    fn test_build_vcf_header_lines_with_metadata() {
        // Create field with VCF metadata using new bio.vcf.field.* keys
        let mut dp_metadata = HashMap::new();
        dp_metadata.insert(
            VCF_FIELD_DESCRIPTION_KEY.to_string(),
            "Read Depth".to_string(),
        );
        dp_metadata.insert(VCF_FIELD_NUMBER_KEY.to_string(), "1".to_string());
        dp_metadata.insert(VCF_FIELD_TYPE_KEY.to_string(), "Integer".to_string());

        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new("DP", DataType::Int32, true).with_metadata(dp_metadata),
        ]));

        let info_fields = vec!["DP".to_string()];
        let format_fields = vec![];
        let sample_names = vec![];

        let lines =
            build_vcf_header_lines(&schema, &info_fields, &format_fields, &sample_names).unwrap();

        // Should use the original description from metadata
        assert!(
            lines
                .iter()
                .any(|l| l.contains("Description=\"Read Depth\""))
        );
    }

    #[test]
    fn test_build_vcf_column_header_no_samples() {
        let header = build_vcf_column_header(&[]);
        assert_eq!(header, "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO");
    }

    #[test]
    fn test_build_vcf_column_header_with_samples() {
        let header = build_vcf_column_header(&["SAMPLE1".to_string(), "SAMPLE2".to_string()]);
        assert!(header.contains("FORMAT"));
        assert!(header.contains("SAMPLE1"));
        assert!(header.contains("SAMPLE2"));
    }

    #[test]
    fn test_find_format_field_single_sample() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(VCF_FIELD_FORMAT_ID_KEY.to_string(), "GT".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("GT", DataType::Utf8, true).with_metadata(metadata),
        ]));

        let field = find_format_field(&schema, "GT", &["SAMPLE1".to_string()]);
        assert!(field.is_some());
        assert_eq!(field.unwrap().name(), "GT");
    }

    #[test]
    fn test_find_format_field_multi_sample() {
        // Columnar schema: genotypes: Struct<GT: List<Utf8>, DP: List<Int32>>
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new(
                "genotypes",
                DataType::Struct(
                    vec![
                        Field::new(
                            "GT",
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        ),
                        Field::new(
                            "DP",
                            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        let field = find_format_field(
            &schema,
            "GT",
            &["SAMPLE1".to_string(), "SAMPLE2".to_string()],
        );
        assert!(field.is_some());
        assert_eq!(field.unwrap().name(), "GT");
    }

    #[test]
    fn test_find_format_field_columnar_struct() {
        // Columnar schema with multiple FORMAT fields
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new(
                "genotypes",
                DataType::Struct(
                    vec![
                        Field::new(
                            "GT",
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        ),
                        Field::new(
                            "GQ",
                            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        let field = find_format_field(
            &schema,
            "GQ",
            &["SAMPLE1".to_string(), "SAMPLE2".to_string()],
        );
        assert!(field.is_some());
        assert_eq!(field.unwrap().name(), "GQ");

        // Non-existent field returns None
        let missing = find_format_field(
            &schema,
            "AD",
            &["SAMPLE1".to_string(), "SAMPLE2".to_string()],
        );
        assert!(missing.is_none());
    }

    #[test]
    fn test_format_type_inference_from_large_list() {
        // Columnar schema with LargeList children (DataFusion default from named_struct)
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new(
                "genotypes",
                DataType::Struct(
                    vec![
                        Field::new(
                            "GT",
                            DataType::LargeList(Arc::new(Field::new(
                                "item",
                                DataType::Utf8View,
                                true,
                            ))),
                            true,
                        ),
                        Field::new(
                            "DP",
                            DataType::LargeList(Arc::new(Field::new(
                                "item",
                                DataType::Int32,
                                true,
                            ))),
                            true,
                        ),
                        Field::new(
                            "GQ",
                            DataType::LargeList(Arc::new(Field::new(
                                "item",
                                DataType::Int32,
                                true,
                            ))),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        let samples = vec!["S1".to_string(), "S2".to_string()];
        let format_fields = vec!["GT".to_string(), "DP".to_string(), "GQ".to_string()];
        let lines = build_vcf_header_lines(&schema, &[], &format_fields, &samples).unwrap();

        // GT should be Type=String
        assert!(
            lines
                .iter()
                .any(|l| l.contains("ID=GT") && l.contains("Type=String")),
            "GT should be Type=String. Lines: {lines:?}"
        );
        // DP should be Type=Integer (not Type=String)
        assert!(
            lines
                .iter()
                .any(|l| l.contains("ID=DP") && l.contains("Type=Integer")),
            "DP should be Type=Integer. Lines: {lines:?}"
        );
        // GQ should be Type=Integer (not Type=String)
        assert!(
            lines
                .iter()
                .any(|l| l.contains("ID=GQ") && l.contains("Type=Integer")),
            "GQ should be Type=Integer. Lines: {lines:?}"
        );
    }
}
