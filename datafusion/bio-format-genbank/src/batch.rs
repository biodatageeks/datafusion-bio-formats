use crate::location::Location;
use crate::reader::{Feature, Record};
use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, Int64Builder, LargeStringBuilder, ListBuilder, MapBuilder,
    MapFieldNames, RecordBatch, RecordBatchOptions, StringBuilder, StructBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use std::collections::HashSet;
use std::sync::Arc;

pub(crate) const RECORDS: &[&str] = &[
    "name",
    "length",
    "molecule_type",
    "topology",
    "division",
    "date",
    "definition",
    "accession",
    "version",
    "organism",
    "taxonomy",
    "sequence",
];
pub(crate) const FEATURES: &[&str] = &[
    "record_id",
    "type",
    "start",
    "end",
    "strand",
    "location",
    "parts",
];

type Values = ListBuilder<StringBuilder>;
type Qualifiers = MapBuilder<StringBuilder, Values>;

fn qualifier_builder() -> Qualifiers {
    let names = MapFieldNames {
        entry: "entries".into(),
        key: "key".into(),
        value: "value".into(),
    };
    MapBuilder::new(
        Some(names),
        StringBuilder::new(),
        Values::new(StringBuilder::new()),
    )
}

fn part_fields() -> Fields {
    Fields::from(vec![
        Field::new("start", DataType::Int64, true),
        Field::new("end", DataType::Int64, true),
        Field::new("strand", DataType::Utf8, true),
    ])
}

pub(crate) fn schema(
    features: bool,
    qualifiers: Option<&[String]>,
    zero_based: bool,
) -> Result<SchemaRef> {
    let mut fields = Vec::new();
    let fixed = if features { FEATURES } else { RECORDS };
    for &col in fixed {
        let ty = match col {
            "length" | "start" | "end" => DataType::Int64,
            "sequence" => DataType::LargeUtf8,
            "taxonomy" => DataType::new_list(DataType::Utf8, true),
            "parts" => DataType::new_list(DataType::Struct(part_fields()), true),
            _ => DataType::Utf8,
        };
        fields.push(Field::new(
            col,
            ty,
            !matches!(col, "name" | "length" | "record_id" | "type" | "location"),
        ));
    }
    if features {
        let default = ["qualifiers".to_owned()];
        let mut used: HashSet<&str> = fixed.iter().copied().collect();
        for key in qualifiers.unwrap_or(&default) {
            if key.is_empty() || !used.insert(key) {
                return Err(DataFusionError::Plan(format!(
                    "invalid or duplicate qualifier column {key:?}"
                )));
            }
            let ty = if key == "qualifiers" {
                qualifier_builder().finish().data_type().clone()
            } else {
                DataType::Utf8
            };
            fields.push(Field::new(key, ty, true));
        }
    } else if qualifiers.is_some() {
        return Err(DataFusionError::Plan(
            "qualifier_fields requires view='features'".into(),
        ));
    }
    let mut schema = Schema::new(fields);
    if features {
        schema.metadata.insert(
            datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY.to_string(),
            zero_based.to_string(),
        );
    }
    Ok(Arc::new(schema))
}

enum Builder {
    Text(StringBuilder),
    Sequence(LargeStringBuilder),
    Number(Int64Builder),
    Taxonomy(Values),
    Parts(ListBuilder<StructBuilder>),
    Qualifiers(Box<Qualifiers>),
}

pub(crate) struct Batch {
    schema: SchemaRef,
    columns: Vec<Builder>,
    pub rows: usize,
    pub bytes: usize,
}

impl Batch {
    pub fn new(schema: SchemaRef) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|f| match f.name().as_str() {
                "sequence" if f.data_type() == &DataType::LargeUtf8 => {
                    Builder::Sequence(LargeStringBuilder::new())
                }
                "taxonomy" if matches!(f.data_type(), DataType::List(_)) => {
                    Builder::Taxonomy(Values::new(StringBuilder::new()))
                }
                "qualifiers" => Builder::Qualifiers(Box::new(qualifier_builder())),
                "parts" => Builder::Parts(ListBuilder::new(StructBuilder::from_fields(
                    part_fields(),
                    0,
                ))),
                _ if f.data_type() == &DataType::Int64 => Builder::Number(Int64Builder::new()),
                _ => Builder::Text(StringBuilder::new()),
            })
            .collect();
        Self {
            schema,
            columns,
            rows: 0,
            bytes: 0,
        }
    }

    pub fn push(&mut self, rec: &Record, feat: Option<&Feature>, loc: &Location) -> Result<()> {
        self.rows += 1;
        for (field, b) in self.schema.fields().iter().zip(&mut self.columns) {
            let name = field.name().as_str();
            match b {
                Builder::Text(out) => {
                    let text = match feat {
                        Some(f) => match name {
                            "type" => Some(f.kind.as_str()),
                            "record_id" => Some(rec.name.as_str()),
                            "location" => Some(f.location.as_str()),
                            "strand" => loc.reverse.map(strand),
                            _ => f
                                .qualifiers
                                .get(name)
                                .and_then(|values| values.first()?.as_deref()),
                        },
                        None if name == "name" => Some(rec.name.as_str()),
                        None => rec.header.get(name).map(String::as_str),
                    };
                    self.bytes += text.map_or(0, str::len);
                    out.append_option(text);
                }
                Builder::Sequence(out) => {
                    self.bytes += rec.sequence.as_ref().map_or(0, String::len);
                    out.append_option(rec.sequence.as_deref());
                }
                Builder::Number(out) => out.append_option(match name {
                    "length" => Some(rec.length),
                    "start" => loc.bounds.map(|p| p.0),
                    _ => loc.bounds.map(|p| p.1),
                }),
                Builder::Taxonomy(out) => {
                    for item in rec
                        .taxonomy
                        .trim_end_matches('.')
                        .split(';')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        self.bytes += item.len();
                        out.values().append_value(item);
                    }
                    out.append(!rec.taxonomy.is_empty());
                }
                Builder::Parts(out) => {
                    if let Some(parts) = &loc.parts {
                        self.bytes += parts.len() * 24;
                        let st = out.values();
                        for p in parts {
                            st.field_builder::<Int64Builder>(0)
                                .unwrap()
                                .append_option(p.start);
                            st.field_builder::<Int64Builder>(1)
                                .unwrap()
                                .append_option(p.end);
                            st.field_builder::<StringBuilder>(2)
                                .unwrap()
                                .append_value(strand(p.reverse));
                            st.append(true);
                        }
                    }
                    out.append(loc.parts.is_some());
                }
                Builder::Qualifiers(out) => {
                    for (key, values) in &feat.expect("feature schema").qualifiers {
                        out.keys().append_value(key);
                        self.bytes += key.len();
                        for value in values {
                            self.bytes += value.as_ref().map_or(0, String::len);
                            out.values().values().append_option(value.as_deref());
                        }
                        out.values().append(true);
                    }
                    out.append(true)?;
                }
            }
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = self
            .columns
            .iter_mut()
            .map(|b| {
                let b: &mut dyn ArrayBuilder = match b {
                    Builder::Text(v) => v,
                    Builder::Number(v) => v,
                    Builder::Sequence(v) => v,
                    Builder::Qualifiers(v) => v.as_mut(),
                    Builder::Parts(v) => v,
                    Builder::Taxonomy(v) => v,
                };
                b.finish()
            })
            .collect();
        let opts = RecordBatchOptions::new().with_row_count(Some(self.rows));
        self.rows = 0;
        self.bytes = 0;
        Ok(RecordBatch::try_new_with_options(
            self.schema.clone(),
            arrays,
            &opts,
        )?)
    }
}
fn strand(reverse: bool) -> &'static str {
    if reverse { "-" } else { "+" }
}
