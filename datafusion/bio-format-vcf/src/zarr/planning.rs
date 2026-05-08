use std::collections::BTreeSet;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion_bio_format_core::metadata::{VCF_FIELD_FIELD_TYPE_KEY, VCF_FIELD_FORMAT_ID_KEY};

/// Logical projection converted into the raw VCF Zarr arrays needed to satisfy it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProjectionPlan {
    /// Projected schema indices supplied by DataFusion.
    pub projected_indices: Option<Vec<usize>>,
    /// Logical columns requested by the query.
    pub projected_columns: BTreeSet<String>,
    /// Raw VCF Zarr array names required by the logical projection.
    pub raw_arrays: BTreeSet<String>,
}

impl ProjectionPlan {
    /// Builds a projection plan from a DataFusion projection.
    pub fn from_projection(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Self {
        let projected_indices = projection.cloned();
        let field_indices: Vec<usize> = match projection {
            Some(indices) => indices.clone(),
            None => (0..schema.fields().len()).collect(),
        };

        let mut projected_columns = BTreeSet::new();
        let mut raw_arrays = BTreeSet::new();

        for index in field_indices {
            let field = schema.field(index);
            projected_columns.insert(field.name().clone());
            add_field_dependencies(
                field.name(),
                field.data_type(),
                field.metadata(),
                &mut raw_arrays,
            );
        }

        Self {
            projected_indices,
            projected_columns,
            raw_arrays,
        }
    }
}

fn add_field_dependencies(
    column: &str,
    data_type: &DataType,
    metadata: &std::collections::HashMap<String, String>,
    raw_arrays: &mut BTreeSet<String>,
) {
    match column {
        "chrom" => {
            raw_arrays.insert("variant_contig".to_string());
            raw_arrays.insert("contig_id".to_string());
        }
        "start" => {
            raw_arrays.insert("variant_position".to_string());
        }
        "end" => {
            raw_arrays.insert("variant_position".to_string());
            raw_arrays.insert("variant_length".to_string());
            raw_arrays.insert("variant_allele".to_string());
        }
        "id" => {
            raw_arrays.insert("variant_id".to_string());
        }
        "ref" | "alt" => {
            raw_arrays.insert("variant_allele".to_string());
        }
        "qual" => {
            raw_arrays.insert("variant_quality".to_string());
        }
        "filter" => {
            raw_arrays.insert("variant_filter".to_string());
            raw_arrays.insert("filter_id".to_string());
        }
        "genotypes" => {
            if let DataType::Struct(children) = data_type {
                for child in children {
                    let id = child
                        .metadata()
                        .get(VCF_FIELD_FORMAT_ID_KEY)
                        .map(String::as_str)
                        .unwrap_or_else(|| child.name().as_str());
                    raw_arrays.insert(format!("call_{id}"));
                }
            }
        }
        other => {
            if metadata
                .get(VCF_FIELD_FIELD_TYPE_KEY)
                .is_some_and(|field_type| field_type == "INFO")
            {
                raw_arrays.insert(format!("variant_{other}"));
            }
        }
    }
}
