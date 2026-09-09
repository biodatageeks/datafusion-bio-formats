//! A2M, A3M and Stockholm multiple-sequence-alignment support for Apache DataFusion.
//!
//! Three read-only table providers over the same storage plumbing as the other
//! `datafusion-bio-format-*` crates (local files and object stores, `gz` and
//! `bgz` compression):
//!
//! * [`FastaLikeTableProvider`] for A2M (`.a2m`) and A3M (`.a3m`). These are
//!   FASTA at the byte level; rows are returned **verbatim** (case, `.` and `-`
//!   preserved, ragged rows allowed) with the FASTA schema
//!   `name`, `description`, `sequence`. Lines starting with `#` before the
//!   first `>` (hh-suite's `#A3M#` marker) are skipped.
//! * [`StockholmTableProvider`] for Stockholm (`.sto`, `.stk`): one row per
//!   sequence per alignment with `alignment_id`, `name`, `sequence`, and the
//!   `#=GS` / `#=GR` annotations as `gs` / `gr` list-of-struct bags (the same
//!   Arrow shape as GFF `attributes`). Interleaved blocks are concatenated,
//!   multi-alignment files are supported and, for local uncompressed files,
//!   split across partitions on `//` boundaries.
//! * [`read_stockholm_annotations`] for the alignment-level `#=GF` / `#=GC`
//!   lines in long format, without materialising sequences.
//!
//! # Example
//!
//! ```rust,no_run
//! use datafusion::prelude::*;
//! use datafusion_bio_format_msa::{FastaLikeTableProvider, MsaFlavor, StockholmTableProvider};
//! use std::sync::Arc;
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! let ctx = SessionContext::new();
//! ctx.register_table(
//!     "msa",
//!     Arc::new(FastaLikeTableProvider::new("query.a3m".into(), MsaFlavor::A3m, None)?),
//! )?;
//! ctx.register_table(
//!     "pfam",
//!     Arc::new(StockholmTableProvider::new("PF00001.sto".into(), None, None)?),
//! )?;
//! ctx.sql("SELECT name, length(sequence) FROM msa").await?.show().await?;
//! ctx.sql("SELECT alignment_id, count(*) FROM pfam GROUP BY alignment_id").await?.show().await?;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

// Each module carries its own `//!` docs; an outer `///` here would be merged
// into them and resolved in this scope, breaking their intra-doc links.
pub mod fastalike;
pub mod stockholm;
pub mod storage;

pub use fastalike::{FastaLikeExec, FastaLikeTableProvider, MsaFlavor, fasta_like_schema};
pub use stockholm::{
    Alignment, AnnotationKind, Collect, ColumnKind, FileAnnotation, GS_BAG_SENTINEL,
    PartitionRange, SequenceRecord, StockholmExec, StockholmReader, StockholmTableProvider,
    annotation_bag_type, annotations_schema, read_stockholm_annotations,
};
