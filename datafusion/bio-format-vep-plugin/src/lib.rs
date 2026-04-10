//! VEP plugin source support for Apache DataFusion.
//!
//! This crate owns source-format-specific logic for external VEP plugin inputs:
//! registering raw TSV/VCF files as DataFusion tables, source-specific select
//! queries, and source-specific batch/schema normalization.

#![warn(missing_docs)]

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{
    Array, ArrayRef, Float32Array, Float32Builder, Int32Array, Int32Builder, StringArray,
    StringBuilder, UInt32Array, UInt32Builder,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use flate2::Compression as GzCompression;
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use once_cell::sync::Lazy;
use tempfile::{Builder, NamedTempFile};

static SANITIZED_TSVS: Lazy<Mutex<Vec<NamedTempFile>>> = Lazy::new(|| Mutex::new(Vec::new()));

/// Supported raw plugin source kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginSourceKind {
    /// ClinVar VCF.
    ClinVar,
    /// CADD TSV.
    Cadd,
    /// SpliceAI VCF.
    SpliceAI,
    /// AlphaMissense TSV.
    AlphaMissense,
    /// dbNSFP TSV.
    DbNSFP,
}

impl PluginSourceKind {
    /// Parse a plugin kind from the public plugin name.
    pub fn from_name(plugin_name: &str) -> Result<Self> {
        match plugin_name {
            "clinvar" => Ok(Self::ClinVar),
            "cadd" => Ok(Self::Cadd),
            "spliceai" => Ok(Self::SpliceAI),
            "alphamissense" => Ok(Self::AlphaMissense),
            "dbnsfp" => Ok(Self::DbNSFP),
            other => Err(DataFusionError::Execution(format!(
                "Unknown plugin source kind: {other}"
            ))),
        }
    }

    /// Canonical plugin name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ClinVar => "clinvar",
            Self::Cadd => "cadd",
            Self::SpliceAI => "spliceai",
            Self::AlphaMissense => "alphamissense",
            Self::DbNSFP => "dbnsfp",
        }
    }

    /// Chromosome column name as exposed by the registered source table.
    pub fn chrom_column(self) -> &'static str {
        match self {
            Self::DbNSFP => "chr",
            _ => "chrom",
        }
    }

    /// SQL projection used to materialize issue-style parquet rows.
    pub fn select_query(self) -> &'static str {
        match self {
            Self::ClinVar => {
                "SELECT chrom, CAST(start AS INTEGER) AS pos, ref AS ref, alt AS alt, array_to_string(\"CLNSIG\", '|') AS clnsig, array_to_string(\"CLNREVSTAT\", '|') AS clnrevstat, array_to_string(\"CLNDN\", '|') AS clndn, \"CLNVC\" AS clnvc, array_to_string(\"CLNVI\", '|') AS clnvi, CAST(\"AF_ESP\" AS FLOAT) AS af_esp, CAST(\"AF_EXAC\" AS FLOAT) AS af_exac, CAST(\"AF_TGP\" AS FLOAT) AS af_tgp FROM source"
            }
            Self::Cadd => {
                "SELECT chrom AS chrom, CAST(pos AS INTEGER) AS pos, ref AS ref, alt AS alt, CAST(rawscore AS FLOAT) AS raw_score, CAST(phred AS FLOAT) AS phred_score FROM source"
            }
            Self::AlphaMissense => {
                "SELECT chrom AS chrom, CAST(pos AS INTEGER) AS pos, ref AS ref, alt AS alt, genome, uniprot_id, transcript_id, protein_variant, CAST(am_pathogenicity AS FLOAT) AS am_pathogenicity, am_class FROM source"
            }
            Self::SpliceAI => {
                "SELECT chrom, CAST(start AS INTEGER) AS pos, ref AS ref, alt AS alt, split_part(\"SpliceAI\", '|', 2) AS symbol, CAST(split_part(\"SpliceAI\", '|', 3) AS FLOAT) AS ds_ag, CAST(split_part(\"SpliceAI\", '|', 4) AS FLOAT) AS ds_al, CAST(split_part(\"SpliceAI\", '|', 5) AS FLOAT) AS ds_dg, CAST(split_part(\"SpliceAI\", '|', 6) AS FLOAT) AS ds_dl, CAST(NULLIF(regexp_replace(split_part(\"SpliceAI\", '|', 7), '[^0-9-]', ''), '') AS INTEGER) AS dp_ag, CAST(NULLIF(regexp_replace(split_part(\"SpliceAI\", '|', 8), '[^0-9-]', ''), '') AS INTEGER) AS dp_al, CAST(NULLIF(regexp_replace(split_part(\"SpliceAI\", '|', 9), '[^0-9-]', ''), '') AS INTEGER) AS dp_dg, CAST(NULLIF(regexp_replace(split_part(\"SpliceAI\", '|', 10), '[^0-9-]', ''), '') AS INTEGER) AS dp_dl FROM source"
            }
            Self::DbNSFP => {
                "SELECT chr AS chrom, CAST(\"pos(1-based)\" AS INTEGER) AS pos, ref AS ref, alt AS alt, sift4g_score, sift4g_pred, polyphen2_hdiv_score, polyphen2_hvar_score, lrt_score, lrt_pred, mutationtaster_score, mutationtaster_pred, fathmm_score, fathmm_pred, provean_score, provean_pred, vest4_score, metasvm_score, metasvm_pred, metalr_score, metalr_pred, CAST(revel_score AS FLOAT) AS revel_score, CAST(\"gerp++_rs\" AS FLOAT) AS gerp_rs, CAST(phyloP100way_vertebrate AS FLOAT) AS phylop100way, CAST(phyloP30way_mammalian AS FLOAT) AS phylop30way, CAST(phastCons100way_vertebrate AS FLOAT) AS phastcons100way, CAST(phastCons30way_mammalian AS FLOAT) AS phastcons30way, CAST(\"siphy_29way_logodds\" AS FLOAT) AS siphy_29way, CAST(cadd_raw AS FLOAT) AS cadd_raw, CAST(cadd_phred AS FLOAT) AS cadd_phred FROM source"
            }
        }
    }

    /// SQL projection used to materialize Fjall rows from plugin parquet.
    pub fn fjall_projection(self) -> &'static str {
        match self {
            Self::ClinVar => {
                "chrom, CAST(pos AS BIGINT) AS start, CAST(pos AS BIGINT) AS end, concat(ref, '/', alt) AS allele_string, clnsig, clnrevstat, clndn, clnvc, clnvi, af_esp, af_exac, af_tgp"
            }
            Self::Cadd => {
                "chrom, CAST(pos AS BIGINT) AS start, CAST(pos AS BIGINT) AS end, concat(ref, '/', alt) AS allele_string, raw_score, phred_score"
            }
            Self::SpliceAI => {
                "chrom, CAST(pos AS BIGINT) AS start, CAST(pos AS BIGINT) AS end, concat(ref, '/', alt) AS allele_string, symbol, ds_ag, ds_al, ds_dg, ds_dl, dp_ag, dp_al, dp_dg, dp_dl"
            }
            Self::AlphaMissense => {
                "chrom, CAST(pos AS BIGINT) AS start, CAST(pos AS BIGINT) AS end, concat(ref, '/', alt) AS allele_string, genome, uniprot_id, transcript_id, protein_variant, am_pathogenicity, am_class"
            }
            Self::DbNSFP => {
                "chrom, CAST(pos AS BIGINT) AS start, CAST(pos AS BIGINT) AS end, concat(ref, '/', alt) AS allele_string, sift4g_score, sift4g_pred, polyphen2_hdiv_score, polyphen2_hvar_score, lrt_score, lrt_pred, mutationtaster_score, mutationtaster_pred, fathmm_score, fathmm_pred, provean_score, provean_pred, vest4_score, metasvm_score, metasvm_pred, metalr_score, metalr_pred, revel_score, gerp_rs, phylop100way, phylop30way, phastcons100way, phastcons30way, siphy_29way, cadd_raw, cadd_phred"
            }
        }
    }
}

/// Register a raw plugin source file as a DataFusion table.
pub async fn register_plugin_source(
    ctx: &SessionContext,
    kind: PluginSourceKind,
    source_path: &str,
    table_name: &str,
) -> Result<()> {
    match kind {
        PluginSourceKind::ClinVar | PluginSourceKind::SpliceAI => {
            let provider = VcfTableProvider::new(source_path.to_string(), None, None, None, false)?;
            ctx.register_table(table_name, Arc::new(provider))?;
            Ok(())
        }
        PluginSourceKind::Cadd | PluginSourceKind::AlphaMissense | PluginSourceKind::DbNSFP => {
            let (table_path, is_gzip) = sanitize_plugin_source(source_path)?;
            let mut options = CsvReadOptions::new()
                .delimiter(b'\t')
                .has_header(true)
                .file_extension(".gz");
            if is_gzip {
                options = options.file_compression_type(FileCompressionType::GZIP);
            }
            ctx.register_csv(table_name, &table_path, options).await?;
            Ok(())
        }
    }
}

/// Normalize a plugin output batch to the issue-style schema.
pub fn normalize_plugin_batch(kind: PluginSourceKind, batch: &RecordBatch) -> Result<RecordBatch> {
    let batch = if kind == PluginSourceKind::ClinVar {
        decompose_multi_allelic_batch(batch)?
    } else {
        batch.clone()
    };
    coerce_plugin_batch_types(&batch)
}

/// Normalize the output schema to the issue-style schema.
pub fn normalize_plugin_schema(schema: &SchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "pos" && field.data_type() != &DataType::UInt32 {
                Arc::new(Field::new("pos", DataType::UInt32, field.is_nullable()))
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

/// SQL used to merge two CADD sources into one logical output.
pub fn cadd_union_query(raw_chroms: &[String]) -> String {
    let snv_where = build_where_clause("chrom", raw_chroms);
    let indel_where = build_where_clause("chrom", raw_chroms);
    format!(
        "SELECT * FROM (\
         SELECT chrom AS chrom, CAST(pos AS INTEGER) AS pos, ref AS ref, alt AS alt, CAST(rawscore AS FLOAT) AS raw_score, CAST(phred AS FLOAT) AS phred_score FROM source_snv{snv_where} \
         UNION ALL \
         SELECT chrom AS chrom, CAST(pos AS INTEGER) AS pos, ref AS ref, alt AS alt, CAST(rawscore AS FLOAT) AS raw_score, CAST(phred AS FLOAT) AS phred_score FROM source_indel{indel_where}\
         ) ORDER BY chrom, pos, ref, alt"
    )
}

/// Build a simple chromosome filter for plugin source SQL.
pub fn build_where_clause(chrom_col: &str, raw_chroms: &[String]) -> String {
    if raw_chroms.len() == 1 {
        format!(
            " WHERE {chrom_col} = '{}'",
            raw_chroms[0].replace('\'', "''")
        )
    } else {
        let in_list = raw_chroms
            .iter()
            .map(|value| format!("'{}'", value.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" WHERE {chrom_col} IN ({in_list})")
    }
}

fn sanitize_plugin_source(source_path: &str) -> Result<(String, bool)> {
    let mut source_file = File::open(source_path).map_err(|e| {
        DataFusionError::Execution(format!("Failed to open plugin source {source_path}: {e}"))
    })?;

    let mut signature = [0u8; 2];
    let is_gzip = match source_file.read_exact(&mut signature) {
        Ok(_) => {
            source_file.seek(SeekFrom::Start(0)).map_err(|e| {
                DataFusionError::Execution(format!("Failed to seek plugin source: {e}"))
            })?;
            signature == [0x1f, 0x8b]
        }
        Err(_) => {
            source_file.seek(SeekFrom::Start(0)).map_err(|e| {
                DataFusionError::Execution(format!("Failed to seek plugin source: {e}"))
            })?;
            false
        }
    };

    if !is_gzip {
        return Ok((source_path.to_string(), false));
    }

    let temp = Builder::new()
        .prefix("vepyr_plugin_tsv_")
        .suffix(".sanitized.tsv.gz")
        .tempfile()
        .map_err(|e| {
            DataFusionError::Execution(format!("Failed to create sanitized plugin source: {e}"))
        })?;
    let mut reader = BufReader::new(MultiGzDecoder::new(source_file));
    let writer = temp.reopen().map_err(|e| {
        DataFusionError::Execution(format!("Failed to open temp file for writing: {e}"))
    })?;
    let mut encoder = GzEncoder::new(writer, GzCompression::default());

    let mut buffer = String::new();
    let mut header_written = false;

    while reader.read_line(&mut buffer)? != 0 {
        let line = buffer.trim_end_matches(&['\r', '\n'][..]);
        if line.is_empty() {
            buffer.clear();
            continue;
        }
        if !header_written {
            if line.starts_with("##") || !line.contains('\t') {
                buffer.clear();
                continue;
            }
            let header_line = line.trim_start_matches('#').to_lowercase();
            encoder.write_all(header_line.as_bytes())?;
            encoder.write_all(b"\n")?;
            header_written = true;
        } else {
            encoder.write_all(line.as_bytes())?;
            encoder.write_all(b"\n")?;
        }
        buffer.clear();
    }

    if !header_written {
        return Err(DataFusionError::Execution(format!(
            "Plugin source {source_path} missing header row"
        )));
    }

    encoder.finish().map_err(|e| {
        DataFusionError::Execution(format!("Failed to finish sanitized source: {e}"))
    })?;

    let sanitized_path = temp.path().to_string_lossy().into_owned();
    let mut storage = SANITIZED_TSVS
        .lock()
        .map_err(|e| DataFusionError::Execution(format!("Sanitized path cache poisoned: {e}")))?;
    storage.push(temp);
    Ok((sanitized_path, true))
}

fn coerce_plugin_batch_types(batch: &RecordBatch) -> Result<RecordBatch> {
    let Some((pos_idx, _)) = batch.schema().column_with_name("pos") else {
        return Ok(batch.clone());
    };
    if batch.column(pos_idx).data_type() == &DataType::UInt32 {
        return Ok(batch.clone());
    }

    let casted = cast(batch.column(pos_idx), &DataType::UInt32).map_err(|e| {
        DataFusionError::Execution(format!("Failed to cast plugin column 'pos' to UInt32: {e}"))
    })?;

    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields[pos_idx] = Arc::new(Field::new("pos", DataType::UInt32, true));
    let schema = Arc::new(Schema::new(fields));

    let mut columns = batch.columns().to_vec();
    columns[pos_idx] = casted;
    RecordBatch::try_new(schema, columns)
        .map_err(|e| DataFusionError::Execution(format!("Failed to rebuild plugin batch: {e}")))
}

fn decompose_multi_allelic_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let Some((alt_idx, _)) = batch.schema().column_with_name("alt") else {
        return Ok(batch.clone());
    };
    let alt_array = batch
        .column(alt_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("ClinVar alt column must be StringArray".to_string())
        })?;
    if !(0..alt_array.len())
        .any(|row| !alt_array.is_null(row) && alt_array.value(row).contains([',', '|']))
    {
        return Ok(batch.clone());
    }

    let mut expanded_indices = Vec::new();
    let mut expanded_alts = Vec::new();
    for row in 0..batch.num_rows() {
        if alt_array.is_null(row) {
            expanded_indices.push(row);
            expanded_alts.push(None);
            continue;
        }
        let value = alt_array.value(row);
        let mut seen_split = false;
        for alt in value
            .split([',', '|'])
            .map(str::trim)
            .filter(|alt| !alt.is_empty())
        {
            expanded_indices.push(row);
            expanded_alts.push(Some(alt.to_string()));
            seen_split = true;
        }
        if !seen_split {
            expanded_indices.push(row);
            expanded_alts.push(Some(value.to_string()));
        }
    }

    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (col_idx, field) in schema.fields().iter().enumerate() {
        if col_idx == alt_idx {
            let mut builder =
                StringBuilder::with_capacity(expanded_alts.len(), expanded_alts.len() * 8);
            for value in &expanded_alts {
                if let Some(value) = value {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            columns.push(Arc::new(builder.finish()));
            continue;
        }
        columns.push(expand_column(
            batch.column(col_idx),
            field.data_type(),
            &expanded_indices,
        )?);
    }

    RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to rebuild ClinVar batch after multi-allelic decomposition: {e}"
        ))
    })
}

fn expand_column(
    source: &ArrayRef,
    data_type: &DataType,
    row_indices: &[usize],
) -> Result<ArrayRef> {
    match data_type {
        DataType::Utf8 => {
            let array = source
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Expected Utf8 column during ClinVar decomposition".into(),
                    )
                })?;
            let mut builder =
                StringBuilder::with_capacity(row_indices.len(), row_indices.len() * 16);
            for &row in row_indices {
                if array.is_null(row) {
                    builder.append_null();
                } else {
                    builder.append_value(array.value(row));
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt32 => {
            let array = source
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Expected UInt32 column during ClinVar decomposition".into(),
                    )
                })?;
            let mut builder = UInt32Builder::with_capacity(row_indices.len());
            for &row in row_indices {
                if array.is_null(row) {
                    builder.append_null();
                } else {
                    builder.append_value(array.value(row));
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let array = source
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Expected Int32 column during ClinVar decomposition".into(),
                    )
                })?;
            let mut builder = Int32Builder::with_capacity(row_indices.len());
            for &row in row_indices {
                if array.is_null(row) {
                    builder.append_null();
                } else {
                    builder.append_value(array.value(row));
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let array = source
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Expected Float32 column during ClinVar decomposition".into(),
                    )
                })?;
            let mut builder = Float32Builder::with_capacity(row_indices.len());
            for &row in row_indices {
                if array.is_null(row) {
                    builder.append_null();
                } else {
                    builder.append_value(array.value(row));
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(DataFusionError::Execution(format!(
            "Unsupported ClinVar decomposition column type: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{PluginSourceKind, build_where_clause, cadd_union_query, normalize_plugin_schema};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn queries_use_expected_shapes() {
        assert!(
            PluginSourceKind::SpliceAI
                .select_query()
                .contains(" AS ds_ag")
        );
        assert!(
            PluginSourceKind::ClinVar
                .select_query()
                .contains("\"CLNSIG\"")
        );
        assert!(
            PluginSourceKind::AlphaMissense
                .select_query()
                .contains("protein_variant")
        );
        assert!(
            PluginSourceKind::DbNSFP
                .select_query()
                .contains("cadd_phred")
        );
    }

    #[test]
    fn cadd_union_uses_both_sources() {
        let query = cadd_union_query(&["chr1".to_string()]);
        assert!(query.contains("FROM source_snv"));
        assert!(query.contains("FROM source_indel"));
        assert!(query.contains("ORDER BY chrom, pos, ref, alt"));
    }

    #[test]
    fn normalize_schema_forces_uint32_pos() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, true),
            Field::new("pos", DataType::Int32, true),
        ]));
        let normalized = normalize_plugin_schema(&schema);
        assert_eq!(
            normalized.field_with_name("pos").expect("pos").data_type(),
            &DataType::UInt32
        );
    }

    #[test]
    fn where_clause_supports_multi_values() {
        let clause = build_where_clause("chrom", &["1".to_string(), "chr1".to_string()]);
        assert!(clause.contains("IN"));
    }
}
