use crate::entity::EnsemblEntityKind;
use datafusion::arrow::datatypes::Schema;

/// VEP cache region size used for region-local feature merging.
///
/// Ensembl VEP loads transcript features in 1 Mb cache regions. The transcript
/// export query mirrors that locality when propagating `gene_hgnc_id`.
pub const VEP_CACHE_REGION_SIZE_BP: i64 = 1_000_000;

fn transcript_select_list(schema: &Schema) -> String {
    let region_expr = format!("CAST(FLOOR((start - 1) / {VEP_CACHE_REGION_SIZE_BP}.0) AS BIGINT)");
    schema
        .fields()
        .iter()
        .map(|f| {
            if f.name() == "gene_hgnc_id" {
                format!(
                    "COALESCE(gene_hgnc_id, \
                         CASE WHEN gene_symbol IS NOT NULL \
                              THEN FIRST_VALUE(gene_hgnc_id) IGNORE NULLS \
                                   OVER (PARTITION BY chrom, gene_symbol, {region_expr} \
                                         ORDER BY gene_hgnc_id NULLS LAST) \
                              ELSE NULL END) AS gene_hgnc_id"
                )
            } else {
                format!("\"{}\"", f.name())
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
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
            format!(
                "SELECT {select_list} FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY stable_id \
                        ORDER BY cds_start NULLS LAST\
                    ) AS _rn \
                    FROM {table_name}{where_clause}\
                ) WHERE _rn = 1 \
                ORDER BY chrom, start"
            )
        }
        EnsemblEntityKind::Translation => unreachable!("use translation split export instead"),
        EnsemblEntityKind::Exon => {
            format!(
                "SELECT * FROM (\
                    SELECT *, ROW_NUMBER() OVER (\
                        PARTITION BY transcript_id, exon_number \
                        ORDER BY stable_id NULLS LAST\
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

#[cfg(test)]
mod tests {
    use super::{VEP_CACHE_REGION_SIZE_BP, build_export_query, build_export_query_multi_chrom};
    use crate::entity::EnsemblEntityKind;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn test_transcript_schema() -> Schema {
        Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::Int64, false),
            Field::new("end", DataType::Int64, false),
            Field::new("stable_id", DataType::Utf8, false),
            Field::new("cds_start", DataType::Int64, true),
            Field::new("gene_symbol", DataType::Utf8, true),
            Field::new("gene_hgnc_id", DataType::Utf8, true),
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
        assert!(q.contains("PARTITION BY stable_id"));
        assert!(q.contains("WHERE _rn = 1"));
        assert!(q.contains("ORDER BY chrom, start"));
        assert!(q.contains("WHERE chrom = 'X'"));
    }

    #[test]
    fn build_export_query_transcript_hgnc_propagation_is_local() {
        let schema = test_transcript_schema();
        let q = build_export_query(
            EnsemblEntityKind::Transcript,
            "tx",
            Some("9"),
            Some(&schema),
        );
        assert!(q.contains("COALESCE(gene_hgnc_id"));
        assert!(q.contains("FIRST_VALUE(gene_hgnc_id) IGNORE NULLS"));
        assert!(q.contains("PARTITION BY chrom, gene_symbol"));
        assert!(q.contains(&format!(
            "CAST(FLOOR((start - 1) / {VEP_CACHE_REGION_SIZE_BP}.0) AS BIGINT)"
        )));
        assert!(!q.starts_with("SELECT *"));
    }

    #[test]
    fn build_export_query_exon_dedup() {
        let q = build_export_query(EnsemblEntityKind::Exon, "exon", None, None);
        assert!(q.contains("PARTITION BY transcript_id, exon_number"));
        assert!(q.contains("ORDER BY transcript_id, start"));
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
        assert!(q.contains("PARTITION BY chrom, gene_symbol"));
    }
}
