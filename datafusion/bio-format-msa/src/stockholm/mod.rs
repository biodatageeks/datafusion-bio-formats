//! Stockholm (`.sto` / `.stk`) support: parser, table provider, execution plan
//! and alignment-level annotation reader.

pub mod annotations;
pub mod physical_exec;
pub mod reader;
pub mod table_provider;

pub use annotations::{annotations_schema, read_stockholm_annotations};
pub use physical_exec::{PartitionRange, StockholmExec};
pub use reader::{Alignment, AnnotationKind, FileAnnotation, SequenceRecord, StockholmReader};
pub use table_provider::{
    ColumnKind, GS_BAG_SENTINEL, StockholmTableProvider, annotation_bag_type,
};
