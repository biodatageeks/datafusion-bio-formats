use crate::entity::EnsemblEntityKind;
use datafusion::arrow::datatypes::Schema;

/// VEP cache region size used for source-file preference during deduplication.
///
/// Ensembl VEP stores transcript features in 1 Mb cache regions. The export
/// query uses this to prefer the copy from the region containing the transcript
/// start when deduplicating cross-boundary entries.
pub const VEP_CACHE_REGION_SIZE_BP: i64 = 1_000_000;

fn transcript_region_start_expr(start_col: &str) -> String {
    format!(
        "(CAST(FLOOR(({start_col} - 1) / {VEP_CACHE_REGION_SIZE_BP}.0) AS BIGINT) * {VEP_CACHE_REGION_SIZE_BP} + 1)"
    )
}

fn source_region_preference_expr(start_col: &str, source_file_col: &str) -> String {
    let region_start = transcript_region_start_expr(start_col);
    let region_end = format!("({region_start} + {} - 1)", VEP_CACHE_REGION_SIZE_BP);
    format!(
        "CASE WHEN {source_file_col} LIKE CONCAT('%/', CAST({region_start} AS VARCHAR), '-', CAST({region_end} AS VARCHAR), '.gz') THEN 0 ELSE 1 END"
    )
}

/// Numeric start of the 1 Mb cache region a row was read from, parsed out of the
/// `<start>-<end>.gz` source file name.
///
/// Lexicographic ordering on `source_file` is unsafe here: `10000001-11000000`
/// sorts before `9000001-10000000` as text. `TRY_CAST` yields NULL when the name
/// does not match, so a non-region source degrades to the following order terms
/// rather than erroring.
fn source_region_start_expr(source_file_col: &str) -> String {
    format!(
        "TRY_CAST(REGEXP_REPLACE({source_file_col}, '^.*/([0-9]+)-[0-9]+\\.gz$', '$1') AS BIGINT)"
    )
}

fn transcript_select_list(schema: &Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn schema_has_column(schema: &Schema, name: &str) -> bool {
    schema.fields().iter().any(|field| field.name() == name)
}

fn transcript_dedup_order_expr(schema: &Schema) -> String {
    let mut order = Vec::new();
    // Mirror Ensembl VEP's RefSeq/merged duplicate handling where possible:
    // prefer Ensembl-source rows, then choose the lowest dbID when resolving
    // stable-id duplicates. The following source-region/source-file terms only
    // stabilize cache boundary duplicates.
    if schema_has_column(schema, "source") {
        order.push("CASE WHEN source = 'Ensembl' THEN 0 ELSE 1 END".to_string());
    }
    if schema_has_column(schema, "db_id") {
        order.push("db_id NULLS LAST".to_string());
    }
    order.push(source_region_preference_expr("start", "source_file"));
    order.push("cds_start NULLS LAST".to_string());
    order.push("source_file".to_string());
    order.join(", ")
}

fn build_export_query_with_where_clause(
    kind: EnsemblEntityKind,
    table_name: &str,
    where_clause: &str,
    schema: Option<&Schema>,
) -> String {
    match kind {
        EnsemblEntityKind::Transcript => {
            let schema = schema.expect("Transcript requires schema for HGNC propagation");
            let select_list = transcript_select_list(schema);
            let dedup_order = transcript_dedup_order_expr(schema);
            format!(
                "SELECT {select_list} FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY chrom, stable_id \
                        ORDER BY {dedup_order}\
                    ) AS _rn \
                    FROM {table_name}{where_clause}\
                ) WHERE _rn = 1 \
                ORDER BY chrom, start"
            )
        }
        EnsemblEntityKind::Translation => unreachable!("use translation split export instead"),
        EnsemblEntityKind::Exon => {
            // An exon of a transcript that straddles a 1 Mb cache-region boundary
            // is stored in both region files, and both copies carry the same
            // `stable_id` — so `stable_id` alone leaves the duplicates fully tied
            // and `ROW_NUMBER()` keeps whichever copy the scan happened to emit
            // first. The remaining terms mirror the transcript and translation
            // dedup rules: Ensembl VEP's `merge_features` keeps the FIRST copy
            // over a region-ordered feature list, so the lowest region wins, with
            // the start-region preference as a fallback for source names that do
            // not parse and `source_file` as the final total-order tie-break.
            let region_start = source_region_start_expr("source_file");
            let source_pref = source_region_preference_expr("start", "source_file");
            format!(
                "SELECT * FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY chrom, transcript_id, exon_number \
                        ORDER BY stable_id NULLS LAST, {region_start} NULLS LAST, {source_pref}, source_file\
                    ) AS _rn \
                    FROM {table_name}{where_clause}\
                ) WHERE _rn = 1 \
                ORDER BY transcript_id, start"
            )
        }
        _ => {
            format!("SELECT * FROM {table_name}{where_clause} ORDER BY chrom, start")
        }
    }
}

fn build_translation_dedup_query_with_where_clause(table_name: &str, where_clause: &str) -> String {
    let source_pref = source_region_preference_expr("start", "source_file");
    format!(
        "SELECT * FROM (\
            SELECT *, ROW_NUMBER() OVER (\
                PARTITION BY chrom, transcript_id \
                ORDER BY {source_pref}, cdna_coding_start NULLS LAST, source_file\
            ) AS _rn \
            FROM {table_name}{where_clause}\
        ) WHERE _rn = 1"
    )
}

/// Build the export SQL query for one entity with an optional single-chromosome filter.
pub fn build_export_query(
    kind: EnsemblEntityKind,
    table_name: &str,
    chrom_filter: Option<&str>,
    schema: Option<&Schema>,
) -> String {
    let where_clause = chrom_filter
        .map(|chrom| format!(" WHERE chrom = '{chrom}'"))
        .unwrap_or_default();
    build_export_query_with_where_clause(kind, table_name, &where_clause, schema)
}

/// Build the translation dedup SQL query with an optional single-chromosome filter.
///
/// Translation rows are duplicated across 1 Mb cache region files for transcripts
/// that span region boundaries. VEP's observed DOMAINS order matches the copy from
/// the region containing the transcript start, so the export query prefers that
/// source file before falling back to `cdna_coding_start`.
pub fn build_translation_dedup_query(table_name: &str, chrom_filter: Option<&str>) -> String {
    let where_clause = chrom_filter
        .map(|chrom| format!(" WHERE chrom = '{chrom}'"))
        .unwrap_or_default();
    build_translation_dedup_query_with_where_clause(table_name, &where_clause)
}

/// Build the export SQL query for one entity filtered to multiple chromosomes/contigs.
pub fn build_export_query_multi_chrom(
    kind: EnsemblEntityKind,
    table_name: &str,
    chroms: &[&str],
    schema: Option<&Schema>,
) -> String {
    let list = chroms
        .iter()
        .map(|chrom| format!("'{chrom}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = format!(" WHERE chrom IN ({list})");
    build_export_query_with_where_clause(kind, table_name, &where_clause, schema)
}

/// Build the translation dedup SQL query filtered to multiple chromosomes/contigs.
pub fn build_translation_dedup_query_multi_chrom(table_name: &str, chroms: &[&str]) -> String {
    let list = chroms
        .iter()
        .map(|chrom| format!("'{chrom}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = format!(" WHERE chrom IN ({list})");
    build_translation_dedup_query_with_where_clause(table_name, &where_clause)
}

#[cfg(test)]
mod tests {
    use super::{
        build_export_query, build_export_query_multi_chrom, build_translation_dedup_query,
        build_translation_dedup_query_multi_chrom,
    };
    use crate::entity::EnsemblEntityKind;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn test_transcript_schema() -> Schema {
        Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::Int64, false),
            Field::new("end", DataType::Int64, false),
            Field::new("stable_id", DataType::Utf8, false),
            Field::new("db_id", DataType::Int64, true),
            Field::new("source", DataType::Utf8, true),
            Field::new("cds_start", DataType::Int64, true),
            Field::new("gene_symbol", DataType::Utf8, true),
            Field::new("gene_hgnc_id", DataType::Utf8, true),
            Field::new("gene_hgnc_id_native", DataType::Utf8, true),
        ])
    }

    #[test]
    fn build_export_query_variation_no_filter() {
        let q = build_export_query(EnsemblEntityKind::Variation, "var", None, None);
        assert_eq!(q, "SELECT * FROM var ORDER BY chrom, start");
    }

    #[test]
    fn build_export_query_variation_with_filter() {
        let q = build_export_query(EnsemblEntityKind::Variation, "var", Some("1"), None);
        assert_eq!(
            q,
            "SELECT * FROM var WHERE chrom = '1' ORDER BY chrom, start"
        );
    }

    #[test]
    fn build_export_query_transcript_dedup() {
        let schema = test_transcript_schema();
        let q = build_export_query(
            EnsemblEntityKind::Transcript,
            "tx",
            Some("X"),
            Some(&schema),
        );
        assert!(q.contains("ROW_NUMBER()"));
        assert!(q.contains("PARTITION BY chrom, stable_id"));
        assert!(q.contains("CASE WHEN source = 'Ensembl' THEN 0 ELSE 1 END"));
        assert!(q.contains("db_id NULLS LAST"));
        assert!(q.contains("WHERE _rn = 1"));
        assert!(q.contains("ORDER BY chrom, start"));
        assert!(q.contains("WHERE chrom = 'X'"));
        assert!(q.contains("source_file LIKE CONCAT('%/'"));
    }

    #[test]
    fn build_export_query_transcript_no_hgnc_propagation() {
        let schema = test_transcript_schema();
        let q = build_export_query(
            EnsemblEntityKind::Transcript,
            "tx",
            Some("9"),
            Some(&schema),
        );
        // gene_hgnc_id should pass through without propagation
        assert!(
            !q.contains("COALESCE(gene_hgnc_id"),
            "gene_hgnc_id should not be propagated"
        );
        assert!(
            !q.contains("FIRST_VALUE(gene_hgnc_id)"),
            "no window-based HGNC fill"
        );
        // Both columns should appear as plain quoted names
        assert!(q.contains("\"gene_hgnc_id\""));
        assert!(q.contains("\"gene_hgnc_id_native\""));
        // Still uses explicit column list (not SELECT *)
        assert!(!q.starts_with("SELECT *"));
    }

    #[test]
    fn build_export_query_exon_dedup() {
        let q = build_export_query(EnsemblEntityKind::Exon, "exon", None, None);
        assert!(q.contains("PARTITION BY chrom, transcript_id, exon_number"));
        assert!(q.contains("ORDER BY transcript_id, start"));
    }

    #[test]
    fn exon_dedup_breaks_the_stable_id_tie_deterministically() {
        let q = build_export_query(EnsemblEntityKind::Exon, "exon", None, None);
        let window_order = q
            .split("ORDER BY ")
            .nth(1)
            .expect("window ORDER BY present");
        // `stable_id` stays the leading term, but it is identical for the two
        // copies of a boundary-straddling exon, so the tie must be resolved by
        // the region the row was read from and finally by `source_file`.
        assert!(window_order.starts_with("stable_id NULLS LAST,"));
        assert!(window_order.contains("REGEXP_REPLACE(source_file"));
        assert!(window_order.contains("source_file LIKE CONCAT('%/'"));
        let terms: Vec<&str> = window_order.split(") AS _rn").collect();
        assert!(
            terms[0].trim_end().ends_with("source_file"),
            "window ORDER BY must end with source_file as the total-order tie-break, got: {}",
            terms[0]
        );
    }

    #[test]
    fn exon_dedup_prefers_the_lowest_source_region() {
        let q = build_export_query(EnsemblEntityKind::Exon, "exon", None, None);
        let window_order = q
            .split("ORDER BY ")
            .nth(1)
            .expect("window ORDER BY present");
        let region_start = window_order
            .find("REGEXP_REPLACE(source_file")
            .expect("region-start term present");
        let start_region_pref = window_order
            .find("source_file LIKE CONCAT('%/'")
            .expect("start-region preference term present");
        // Ensembl VEP's merge_features keeps the first copy over a region-ordered
        // list, so the lowest region must outrank the start-region fallback.
        assert!(region_start < start_region_pref);
    }

    #[test]
    fn build_export_query_multi_chrom_variation() {
        let q = build_export_query_multi_chrom(
            EnsemblEntityKind::Variation,
            "var",
            &["MT", "GL000220"],
            None,
        );
        assert!(q.contains("WHERE chrom IN ('MT', 'GL000220')"));
        assert!(q.contains("ORDER BY chrom, start"));
    }

    #[test]
    fn build_export_query_multi_chrom_transcript() {
        let schema = test_transcript_schema();
        let q = build_export_query_multi_chrom(
            EnsemblEntityKind::Transcript,
            "tx",
            &["1", "2"],
            Some(&schema),
        );
        assert!(q.contains("WHERE chrom IN ('1', '2')"));
        assert!(q.contains("ROW_NUMBER()"));
        assert!(q.contains("WHERE _rn = 1"));
        assert!(q.contains("PARTITION BY chrom, stable_id"));
        assert!(q.contains("source_file LIKE CONCAT('%/'"));
    }

    #[test]
    fn build_translation_dedup_query_prefers_transcript_start_region() {
        let q = build_translation_dedup_query("tl", Some("2"));
        assert!(q.contains("PARTITION BY chrom, transcript_id"));
        assert!(q.contains("source_file LIKE CONCAT('%/'"));
        assert!(q.contains("cdna_coding_start NULLS LAST"));
        assert!(q.contains("WHERE chrom = '2'"));
    }

    #[test]
    fn build_translation_dedup_query_multi_chrom_prefers_transcript_start_region() {
        let q = build_translation_dedup_query_multi_chrom("tl", &["2", "X"]);
        assert!(q.contains("PARTITION BY chrom, transcript_id"));
        assert!(q.contains("source_file LIKE CONCAT('%/'"));
        assert!(q.contains("WHERE chrom IN ('2', 'X')"));
    }
}
