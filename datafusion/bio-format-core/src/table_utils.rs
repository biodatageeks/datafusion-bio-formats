use datafusion::arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanBuilder, Float32Builder, Int32Builder, ListBuilder,
    StringBuilder, StructBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::arrow::error::ArrowError;
use std::sync::Arc;

pub struct Attribute {
    pub tag: String,
    pub value: Option<String>,
}

#[derive(Debug)]
pub enum OptionalField {
    Int32Builder(Int32Builder),
    ArrayInt32Builder(ListBuilder<Int32Builder>),
    Float32Builder(Float32Builder),
    ArrayFloat32Builder(ListBuilder<Float32Builder>),
    BooleanBuilder(BooleanBuilder),
    ArrayBooleanBuilder(ListBuilder<BooleanBuilder>),
    Utf8Builder(StringBuilder),
    ArrayUtf8Builder(ListBuilder<StringBuilder>),
    ArrayStructBuilder(ListBuilder<StructBuilder>),
}

impl OptionalField {
    pub fn new(data_type: &DataType, batch_size: usize) -> Result<OptionalField, ArrowError> {
        match data_type {
            DataType::Int32 => Ok(OptionalField::Int32Builder(Int32Builder::with_capacity(
                batch_size,
            ))),
            DataType::Float32 => Ok(OptionalField::Float32Builder(
                Float32Builder::with_capacity(batch_size),
            )),
            DataType::Utf8 => Ok(OptionalField::Utf8Builder(StringBuilder::with_capacity(
                batch_size,
                batch_size * 10,
            ))),
            DataType::Boolean => Ok(OptionalField::BooleanBuilder(
                BooleanBuilder::with_capacity(batch_size),
            )),

            DataType::List(f) => match f.data_type() {
                DataType::Int32 => Ok(OptionalField::ArrayInt32Builder(
                    ListBuilder::with_capacity(Int32Builder::with_capacity(batch_size), batch_size),
                )),
                DataType::Float32 => Ok(OptionalField::ArrayFloat32Builder(
                    ListBuilder::with_capacity(
                        Float32Builder::with_capacity(batch_size),
                        batch_size,
                    ),
                )),
                DataType::Utf8 => Ok(OptionalField::ArrayUtf8Builder(ListBuilder::with_capacity(
                    StringBuilder::with_capacity(batch_size, batch_size * 10),
                    batch_size,
                ))),
                DataType::Boolean => Ok(OptionalField::ArrayBooleanBuilder(
                    ListBuilder::with_capacity(
                        BooleanBuilder::with_capacity(batch_size),
                        batch_size,
                    ),
                )),
                DataType::Struct(_) => {
                    let tag_builder = StringBuilder::with_capacity(batch_size, batch_size);
                    let value_builder = StringBuilder::with_capacity(batch_size, batch_size);

                    let struct_fields = Fields::from(vec![
                        Field::new("tag", DataType::Utf8, false),  // non-null
                        Field::new("value", DataType::Utf8, true), // nullable
                    ]);
                    let struct_builder = StructBuilder::new(
                        struct_fields,
                        vec![
                            Box::new(tag_builder) as Box<dyn ArrayBuilder>,
                            Box::new(value_builder) as Box<dyn ArrayBuilder>,
                        ],
                    );

                    // 3) wrap StructBuilder in a ListBuilder
                    Ok(OptionalField::ArrayStructBuilder(
                        ListBuilder::with_capacity(struct_builder, batch_size),
                    ))
                }

                _ => Err(ArrowError::SchemaError(
                    "Unsupported list inner data type".into(),
                )),
            },

            _ => Err(ArrowError::SchemaError("Unsupported data type".into())),
        }
    }

    pub fn append_array_struct(&mut self, items: Vec<Attribute>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayStructBuilder(list_builder) => {
                // 1) start a new list slot

                // 2) grab the StructBuilder inside
                let struct_builder = list_builder.values();

                // 3) push each Attribute
                for Attribute { tag, value } in items {
                    // field 0: tag (non-null)
                    struct_builder
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_value(&tag);
                    // field 1: value (nullable)
                    struct_builder
                        .field_builder::<StringBuilder>(1)
                        .unwrap()
                        .append_option(value.as_deref());
                    struct_builder.append(true);
                }

                list_builder.append(true);
                Ok(())
            }
            other => Err(ArrowError::SchemaError(format!(
                "Expected ArrayStructBuilder, found {:?}",
                other
            ))),
        }
    }
    pub fn append_int(&mut self, value: i32) -> Result<(), ArrowError> {
        match self {
            OptionalField::Int32Builder(builder) => Ok(builder.append_value(value)),
            _ => Err(ArrowError::SchemaError("Invalid builder".into())),
        }
    }

    pub fn append_boolean(&mut self, value: bool) -> Result<(), ArrowError> {
        match self {
            OptionalField::BooleanBuilder(builder) => Ok(builder.append_value(value)),
            _ => Err(ArrowError::SchemaError("Expected BooleanBuilder".into())),
        }
    }

    pub fn append_array_int(&mut self, value: Vec<i32>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayInt32Builder(builder) => {
                builder.values().append_slice(&value);
                Ok(builder.append(true))
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayInt32Builder".into())),
        }
    }

    pub fn append_float(&mut self, value: f32) -> Result<(), ArrowError> {
        match self {
            OptionalField::Float32Builder(builder) => Ok(builder.append_value(value)),
            _ => Err(ArrowError::SchemaError("Expected Float32Builder".into())),
        }
    }

    pub fn append_array_float(&mut self, value: Vec<f32>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayFloat32Builder(builder) => {
                builder.values().append_slice(&value);
                Ok(builder.append(true))
            }
            _ => Err(ArrowError::SchemaError(
                "Expected ArrayFloat32Builder".into(),
            )),
        }
    }

    pub fn append_string(&mut self, value: &str) -> Result<(), ArrowError> {
        match self {
            OptionalField::Utf8Builder(builder) => Ok(builder.append_value(value)),
            _ => Err(ArrowError::SchemaError("Expected Utf8Builder".into())),
        }
    }

    pub fn append_array_string(&mut self, value: Vec<String>) -> Result<(), ArrowError> {
        match self {
            OptionalField::ArrayUtf8Builder(builder) => {
                for v in value {
                    builder.values().append_value(&v);
                }
                Ok(builder.append(true))
            }
            _ => Err(ArrowError::SchemaError("Expected ArrayUtf8Builder".into())),
        }
    }

    pub fn append_null(&mut self) -> Result<(), ArrowError> {
        match self {
            OptionalField::Int32Builder(builder) => Ok(builder.append_null()),
            OptionalField::ArrayInt32Builder(builder) => Ok(builder.append_null()),
            OptionalField::Utf8Builder(builder) => Ok(builder.append_null()),
            OptionalField::ArrayUtf8Builder(builder) => Ok(builder.append_null()),
            OptionalField::Float32Builder(builder) => Ok(builder.append_null()),
            OptionalField::ArrayFloat32Builder(builder) => Ok(builder.append_null()),
            OptionalField::BooleanBuilder(builder) => Ok(builder.append_null()),
            OptionalField::ArrayBooleanBuilder(builder) => Ok(builder.append_null()),
            OptionalField::ArrayStructBuilder(builder) => {
                builder.append_null();
                Ok(())
            }
        }
    }

    pub fn finish(&mut self) -> Result<ArrayRef, ArrowError> {
        match self {
            OptionalField::Int32Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayInt32Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::Utf8Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayUtf8Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::Float32Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayFloat32Builder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::BooleanBuilder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayBooleanBuilder(builder) => Ok(Arc::new(builder.finish())),
            OptionalField::ArrayStructBuilder(builder) => {
                let struct_array = builder.finish();
                Ok(Arc::new(struct_array))
            }
        }
    }
}

pub fn builders_to_arrays(builders: &mut Vec<OptionalField>) -> Vec<Arc<dyn Array>> {
    builders
        .iter_mut()
        .map(|f| f.finish())
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}
