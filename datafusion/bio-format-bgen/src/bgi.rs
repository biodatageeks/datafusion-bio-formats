use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{Expr, Operator, expr::InList};
use datafusion_bio_format_core::companion::{CompanionRule, resolve_companion, sanitize_location};
use datafusion_bio_format_core::genotype::CoordinateSystem;
use log::debug;
use rusqlite::types::Value;
use rusqlite::{Connection, OpenFlags};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::catalog::IndexedVariant;
use crate::header::BgenHeader;
use crate::source::ObjectAccess;
use crate::table_provider::{BgenReadOptions, IndexReadCost, StaleBgiPolicy};

static CACHE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Host parameters a single index lookup may bind.
///
/// SQLite's compiled `SQLITE_MAX_VARIABLE_NUMBER` is 999 in older builds, so
/// staying under that keeps the lookup portable across the SQLite a host
/// provides.
const MAX_SQLITE_PARAMETERS: usize = 900;

/// Bytes of the BGEN object a BGI stores to identify the file it describes.
const IDENTITY_PREFIX_BYTES: u64 = 1000;

#[derive(Clone)]
pub(crate) struct BgiIndex {
    pub(crate) row_indices: Arc<Vec<usize>>,
    /// The index's own record of every variant, which becomes the catalog.
    pub(crate) variants: Arc<Vec<IndexedVariant>>,
    /// Connection held open for the provider's lifetime.
    ///
    /// A cached remote BGI can be evicted by a later provider that exceeds
    /// `max_bgi_cache_bytes`. Reopening by path after that would fail, so the
    /// connection is opened once and retained: on POSIX the unlinked file stays
    /// readable through it.
    connection: Arc<std::sync::Mutex<Connection>>,
    sqlite_path: Arc<PathBuf>,
    offset_to_index: Arc<HashMap<u64, usize>>,
}

impl std::fmt::Debug for BgiIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BgiIndex")
            .field("rows", &self.row_indices.len())
            .field("path", &self.sqlite_path)
            .finish_non_exhaustive()
    }
}

impl BgiIndex {
    pub(crate) fn candidate_indices(
        &self,
        filters: &[&Expr],
        coordinate_system: CoordinateSystem,
    ) -> Result<Vec<usize>> {
        let mut clauses = Vec::new();
        let mut parameters = Vec::new();
        for filter in filters {
            append_sql_predicate(filter, coordinate_system, &mut clauses, &mut parameters);
        }
        // Every predicate pushed here is also evaluated against the catalog, so
        // returning all candidates stays correct and only costs a scan of the
        // metadata. That is the right trade when the index cannot answer the
        // query: SQLite refuses to prepare a statement with more host
        // parameters than its compiled limit, and a large `IN` list would
        // otherwise fail a query the catalog can answer.
        if clauses.is_empty() || parameters.len() > MAX_SQLITE_PARAMETERS {
            return Ok(self.row_indices.as_ref().clone());
        }

        let sql = format!(
            "SELECT file_start_position FROM Variant WHERE {} ORDER BY file_start_position, rowid",
            clauses.join(" AND ")
        );
        let connection = self.connection.lock().map_err(|error| {
            DataFusionError::Plan(format!("BGI connection is poisoned: {error}"))
        })?;
        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| DataFusionError::Plan(format!("prepare BGI pushdown: {error}")))?;
        statement
            .query_map(rusqlite::params_from_iter(parameters), |row| {
                row.get::<_, i64>(0)
            })
            .map_err(|error| DataFusionError::Plan(format!("query BGI pushdown: {error}")))?
            .enumerate()
            .map(|(row, offset)| {
                let offset = offset.map_err(|error| {
                    DataFusionError::Plan(format!("decode BGI pushdown row {row}: {error}"))
                })?;
                let offset = u64::try_from(offset).map_err(|_| {
                    DataFusionError::Plan(format!("BGI pushdown row {row} has a negative offset"))
                })?;
                self.offset_to_index.get(&offset).copied().ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "BGI pushdown returned unvalidated offset {offset}"
                    ))
                })
            })
            .collect()
    }
}

#[derive(Debug)]
struct BgiMetadata {
    file_size: u64,
    first_bytes: Vec<u8>,
}

#[derive(Debug)]
struct BgiRow {
    chrom: String,
    position: u32,
    rsid: Option<String>,
    allele_count: usize,
    allele1: Option<String>,
    allele2: Option<String>,
    offset: u64,
    size: u64,
}

/// Opens the index a BGEN object should use, if there is a usable one.
///
/// Returns the index alongside what a *rejected* index cost: an index that was
/// read and then found stale is dropped, but its bytes were still fetched, and a
/// scan that omits them reports less I/O than it performed. The cost is zero
/// when an index is returned, because the index carries its own.
pub(crate) async fn open_optional_bgi(
    primary_path: &str,
    primary_source: &ObjectAccess,
    header: &BgenHeader,
    options: &BgenReadOptions,
) -> Result<(Option<BgiIndex>, IndexReadCost)> {
    let storage_options = options.object_storage_options.clone().unwrap_or_default();
    let mut cost = IndexReadCost::default();
    // Probing for a companion stats each candidate, which is a physical request
    // against remote storage and belongs in the index's cost like any other.
    // `exists` opens its own handle, so it is counted here rather than by one.
    let probes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let probe_counter = Arc::clone(&probes);
    let bgi_path = resolve_companion(
        primary_path,
        "BGI",
        options.bgi_path.as_deref(),
        &[CompanionRule::AppendSuffix(".bgi".to_string())],
        false,
        |candidate| {
            let storage_options = storage_options.clone();
            let probe_counter = Arc::clone(&probe_counter);
            async move {
                probe_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                ObjectAccess::exists(&candidate, &storage_options).await
            }
        },
    )
    .await?;
    cost.requests = probes.load(std::sync::atomic::Ordering::Relaxed);
    let Some(bgi_path) = bgi_path else {
        return Ok((None, cost));
    };

    let explicit = options.bgi_path.is_some();
    let result = open_and_validate(
        primary_path,
        primary_source,
        &bgi_path,
        header,
        options,
        &mut cost,
    )
    .await;
    match result {
        Ok(index) => Ok((Some(index), cost)),
        Err(error) if !explicit && options.stale_bgi_policy == StaleBgiPolicy::Ignore => {
            debug!(
                "BGI {}: ignoring a stale index: {error}",
                sanitize_location(&bgi_path)
            );
            Ok((None, cost))
        }
        Err(error) => Err(error),
    }
}

async fn open_and_validate(
    primary_path: &str,
    primary_source: &ObjectAccess,
    bgi_path: &str,
    header: &BgenHeader,
    options: &BgenReadOptions,
    cost: &mut IndexReadCost,
) -> Result<BgiIndex> {
    let storage_options = options.object_storage_options.clone().unwrap_or_default();
    let bgi_source = ObjectAccess::open(bgi_path, &storage_options).await?;
    let result = open_validated_index(
        primary_path,
        primary_source,
        bgi_path,
        header,
        options,
        cost,
        &bgi_source,
    )
    .await;
    // Counted however the attempt ended: a rejected index was read all the same.
    // The primary object's own requests belong to its handle, which the provider
    // snapshots separately, so they are not added here.
    // Requests come from the handle, which counts them wherever they are made.
    // Bytes do not: a local index is read by SQLite rather than through the
    // handle, so the handle would report none for it. The index is read in full
    // either way — downloaded, or read where it lies — so its size is what is
    // counted, as it was before the handle existed.
    cost.requests = cost.requests.saturating_add(bgi_source.requests());
    result
}

#[allow(clippy::too_many_arguments)]
async fn open_validated_index(
    primary_path: &str,
    primary_source: &ObjectAccess,
    bgi_path: &str,
    header: &BgenHeader,
    options: &BgenReadOptions,
    cost: &mut IndexReadCost,
    bgi_source: &ObjectAccess,
) -> Result<BgiIndex> {
    // One stat yields both the size the limits are checked against and the
    // validator the cache is keyed on, so a cached index costs no extra request.
    let (bgi_size, bgi_validator) = bgi_source.identity(bgi_path).await?;
    if bgi_size > options.max_bgi_bytes as u64 {
        return Err(index_error(
            bgi_path,
            &format!(
                "index size {bgi_size} exceeds max_bgi_bytes {}",
                options.max_bgi_bytes
            ),
        ));
    }
    let (sqlite_path, connection) = if let Some(path) = bgi_source.local_path() {
        let path = PathBuf::from(path);
        let connection = open_retained_index(&path)?;
        (path, connection)
    } else {
        // The index is about to be cached, so an index the cache cannot hold is
        // rejected before it is downloaded. Otherwise a stale-BGI policy that
        // ignores the failure would re-download it on every open.
        if bgi_size > options.max_bgi_cache_bytes as u64 {
            return Err(index_error(
                bgi_path,
                &format!(
                    "index size {bgi_size} exceeds max_bgi_cache_bytes {}",
                    options.max_bgi_cache_bytes
                ),
            ));
        }
        cache_remote_index(
            bgi_path,
            bgi_validator.as_deref(),
            bgi_size,
            options.max_bgi_cache_bytes,
            options.bgi_cache_directory.as_deref(),
            || bgi_source.read_all_bounded(bgi_path, options.max_bgi_bytes),
        )
        .await?
    };
    // Counted only now. An index rejected by a size limit above was never read —
    // only stated — so charging its advertised size there would report gigabytes
    // of I/O that never happened. From here it has been downloaded, or opened
    // where it lies for SQLite to read, so it is read in full either way.
    cost.companion_bytes = bgi_size;

    // Validation runs through the connection opened under the cache lease, so an
    // entry evicted by a concurrent provider cannot make it fail on a path that
    // no longer exists.
    let connection = Arc::new(std::sync::Mutex::new(connection));
    let display_path = bgi_path.to_string();
    let validation_connection = connection.clone();
    let (metadata, rows) = tokio::task::spawn_blocking(move || {
        let guard = validation_connection
            .lock()
            .map_err(|error| index_error(&display_path, &format!("poisoned index: {error}")))?;
        read_sqlite(&guard, &display_path)
    })
    .await
    .map_err(|error| index_error(bgi_path, &format!("SQLite validation task failed: {error}")))??;
    validate_identity(
        primary_path,
        primary_source,
        header,
        &metadata,
        &rows,
        bgi_path,
    )
    .await?;

    let offset_to_index = rows
        .iter()
        .enumerate()
        .map(|(index, row)| (row.offset, index))
        .collect();
    let variants = rows
        .into_iter()
        .map(|row| IndexedVariant {
            chrom: row.chrom,
            position: row.position,
            // A parsed record reports an empty RS identifier as absent, so an
            // index that spells the same thing as an empty string has to agree.
            rsid: row.rsid.filter(|value| !value.is_empty()),
            allele_count: row.allele_count,
            // Only a leading run is usable: these are compared against the
            // record's alleles by position, so a null `allele1` beside a
            // present `allele2` would line the second allele up with the first.
            alleles: [row.allele1, row.allele2]
                .into_iter()
                .take_while(Option::is_some)
                .flatten()
                .collect(),
            record_offset: row.offset,
            record_size: row.size,
        })
        .collect::<Vec<_>>();
    Ok(BgiIndex {
        row_indices: Arc::new((0..variants.len()).collect()),
        variants: Arc::new(variants),
        connection,
        sqlite_path: Arc::new(sqlite_path),
        offset_to_index: Arc::new(offset_to_index),
    })
}

fn append_sql_predicate(
    expression: &Expr,
    coordinate_system: CoordinateSystem,
    clauses: &mut Vec<String>,
    parameters: &mut Vec<Value>,
) {
    match expression {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            append_sql_predicate(&binary.left, coordinate_system, clauses, parameters);
            append_sql_predicate(&binary.right, coordinate_system, clauses, parameters);
        }
        Expr::BinaryExpr(binary) => {
            let (Expr::Column(column), Expr::Literal(literal, _)) = (&*binary.left, &*binary.right)
            else {
                return;
            };
            let operator = match binary.op {
                Operator::Eq => "=",
                Operator::NotEq => "!=",
                Operator::Lt => "<",
                Operator::LtEq => "<=",
                Operator::Gt => ">",
                Operator::GtEq => ">=",
                _ => return,
            };
            match column.name.as_str() {
                "chrom" | "rsid" => {
                    if let Some(value) = string_value(literal) {
                        let column_name = if column.name == "chrom" {
                            "chromosome"
                        } else {
                            "rsid"
                        };
                        clauses.push(format!("{column_name} {operator} ?"));
                        parameters.push(Value::Text(value.to_string()));
                    }
                }
                "start" | "end" => {
                    if let Some(value) =
                        bgi_position(integer_value(literal), &column.name, coordinate_system)
                    {
                        clauses.push(format!("position {operator} ?"));
                        parameters.push(Value::Integer(value));
                    }
                }
                _ => {}
            }
        }
        Expr::Between(between) => {
            let (Expr::Column(column), Expr::Literal(low, _), Expr::Literal(high, _)) =
                (&*between.expr, &*between.low, &*between.high)
            else {
                return;
            };
            if !matches!(column.name.as_str(), "start" | "end") {
                return;
            }
            let Some(low) = bgi_position(integer_value(low), &column.name, coordinate_system)
            else {
                return;
            };
            let Some(high) = bgi_position(integer_value(high), &column.name, coordinate_system)
            else {
                return;
            };
            clauses.push(format!(
                "position {}BETWEEN ? AND ?",
                if between.negated { "NOT " } else { "" }
            ));
            parameters.extend([Value::Integer(low), Value::Integer(high)]);
        }
        Expr::InList(in_list) => {
            append_in_list(in_list, coordinate_system, clauses, parameters);
        }
        _ => {}
    }
}

fn append_in_list(
    in_list: &InList,
    coordinate_system: CoordinateSystem,
    clauses: &mut Vec<String>,
    parameters: &mut Vec<Value>,
) {
    let Expr::Column(column) = &*in_list.expr else {
        return;
    };
    let values = match column.name.as_str() {
        "chrom" | "rsid" => in_list
            .list
            .iter()
            .map(|expression| {
                let Expr::Literal(value, _) = expression else {
                    return None;
                };
                string_value(value).map(|value| Value::Text(value.to_string()))
            })
            .collect::<Option<Vec<_>>>(),
        "start" | "end" => in_list
            .list
            .iter()
            .map(|expression| {
                let Expr::Literal(value, _) = expression else {
                    return None;
                };
                bgi_position(integer_value(value), &column.name, coordinate_system)
                    .map(Value::Integer)
            })
            .collect::<Option<Vec<_>>>(),
        _ => None,
    };
    let Some(values) = values else {
        return;
    };
    if values.is_empty() {
        return;
    }
    let column_name = match column.name.as_str() {
        "start" | "end" => "position",
        "chrom" => "chromosome",
        _ => &column.name,
    };
    clauses.push(format!(
        "{column_name} {}IN ({})",
        if in_list.negated { "NOT " } else { "" },
        std::iter::repeat_n("?", values.len())
            .collect::<Vec<_>>()
            .join(",")
    ));
    parameters.extend(values);
}

fn bgi_position(
    value: Option<u64>,
    column: &str,
    coordinate_system: CoordinateSystem,
) -> Option<i64> {
    let value = value?;
    let one_based = match (coordinate_system, column) {
        (CoordinateSystem::ZeroBasedHalfOpen, "start") => value.checked_add(1)?,
        _ => value,
    };
    i64::try_from(one_based).ok()
}

fn string_value(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => Some(value),
        _ => None,
    }
}

fn integer_value(value: &ScalarValue) -> Option<u64> {
    match value {
        ScalarValue::UInt8(Some(value)) => Some((*value).into()),
        ScalarValue::UInt16(Some(value)) => Some((*value).into()),
        ScalarValue::UInt32(Some(value)) => Some((*value).into()),
        ScalarValue::UInt64(Some(value)) => Some(*value),
        ScalarValue::Int8(Some(value)) => u64::try_from(*value).ok(),
        ScalarValue::Int16(Some(value)) => u64::try_from(*value).ok(),
        ScalarValue::Int32(Some(value)) => u64::try_from(*value).ok(),
        ScalarValue::Int64(Some(value)) => u64::try_from(*value).ok(),
        _ => None,
    }
}

fn read_sqlite(connection: &Connection, display_path: &str) -> Result<(BgiMetadata, Vec<BgiRow>)> {
    connection
        .pragma_update(None, "query_only", true)
        .map_err(|error| index_error(display_path, &format!("enable query-only mode: {error}")))?;

    let mut metadata_statement = connection
        .prepare("SELECT file_size, first_1000_bytes FROM Metadata")
        .map_err(|error| index_error(display_path, &format!("read Metadata schema: {error}")))?;
    let metadata_rows = metadata_statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(|error| index_error(display_path, &format!("query Metadata: {error}")))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| index_error(display_path, &format!("decode Metadata: {error}")))?;
    if metadata_rows.len() != 1 {
        return Err(index_error(
            display_path,
            &format!(
                "Metadata must contain exactly one row, found {}",
                metadata_rows.len()
            ),
        ));
    }
    let mut metadata_rows = metadata_rows.into_iter();
    let (file_size, first_bytes) = metadata_rows.next().ok_or_else(|| {
        index_error(
            display_path,
            "Metadata row disappeared during SQLite validation",
        )
    })?;
    let file_size = u64::try_from(file_size)
        .map_err(|_| index_error(display_path, "Metadata.file_size is negative"))?;

    let mut variant_statement = connection
        .prepare(
            "SELECT chromosome, position, rsid, number_of_alleles, allele1, allele2, \
             file_start_position, size_in_bytes \
             FROM Variant ORDER BY file_start_position, rowid",
        )
        .map_err(|error| index_error(display_path, &format!("read Variant schema: {error}")))?;
    let rows = variant_statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })
        .map_err(|error| index_error(display_path, &format!("query Variant: {error}")))?
        .enumerate()
        .map(|(index, row)| {
            let (chrom, position, rsid, allele_count, allele1, allele2, offset, size) = row
                .map_err(|error| {
                    index_error(
                        display_path,
                        &format!("decode Variant row {index}: {error}"),
                    )
                })?;
            Ok(BgiRow {
                chrom,
                position: u32::try_from(position).map_err(|_| {
                    index_error(
                        display_path,
                        &format!("Variant row {index} has invalid position"),
                    )
                })?,
                rsid: rsid.filter(|value| !value.is_empty()),
                allele_count: usize::try_from(allele_count).map_err(|_| {
                    index_error(
                        display_path,
                        &format!("Variant row {index} has invalid allele count"),
                    )
                })?,
                allele1,
                allele2,
                offset: u64::try_from(offset).map_err(|_| {
                    index_error(
                        display_path,
                        &format!("Variant row {index} has negative offset"),
                    )
                })?,
                size: u64::try_from(size).map_err(|_| {
                    index_error(
                        display_path,
                        &format!("Variant row {index} has negative size"),
                    )
                })?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok((
        BgiMetadata {
            file_size,
            first_bytes,
        },
        rows,
    ))
}

/// Checks that an index describes this object and covers it consistently.
///
/// This is the part of validation that can be done without reading the
/// variants: the object's size and its first bytes identify the file, the
/// declared variant count fixes how many records the index must describe, and
/// the row ranges must tile the variant region in order without gaps, overlaps,
/// or reads that fall outside the object.
///
/// Each row's contents are checked against the record it points at when a scan
/// reads that record — see `resolve_variant`. Rebuilding every record's
/// metadata here to compare it would mean walking the whole object, which is
/// exactly the work the index exists to avoid, and it would happen on every
/// open whether or not the query touches those variants.
async fn validate_identity(
    primary_path: &str,
    primary_source: &ObjectAccess,
    header: &BgenHeader,
    metadata: &BgiMetadata,
    rows: &[BgiRow],
    bgi_path: &str,
) -> Result<()> {
    if metadata.file_size != header.object_size {
        return Err(index_error(
            bgi_path,
            &format!(
                "Metadata.file_size {} differs from BGEN size {}",
                metadata.file_size, header.object_size
            ),
        ));
    }
    let identity_length = header.object_size.min(IDENTITY_PREFIX_BYTES);
    let primary_prefix = primary_source
        .read_range(primary_path, 0..identity_length)
        .await?;
    if metadata.first_bytes != primary_prefix.as_ref() {
        return Err(index_error(
            bgi_path,
            "Metadata.first_1000_bytes does not match the BGEN object",
        ));
    }
    if rows.len() != header.variant_count as usize {
        return Err(index_error(
            bgi_path,
            &format!(
                "Variant row count {} differs from the BGEN header's variant count {}",
                rows.len(),
                header.variant_count
            ),
        ));
    }

    // Records are laid out end to end from the first variant offset to the end
    // of the object, so a set of rows that describes this object has to do the
    // same. Anything else means the index is describing different records, and
    // catching it here keeps a scan from reading a range that begins mid-record.
    let mut expected_offset = header.first_variant_offset;
    for (index, row) in rows.iter().enumerate() {
        let end = row.offset.checked_add(row.size).ok_or_else(|| {
            index_error(bgi_path, &format!("Variant row {index} range overflowed"))
        })?;
        if row.size == 0 || end > header.object_size {
            return Err(index_error(
                bgi_path,
                &format!(
                    "Variant row {index} range {}..{end} is out of bounds",
                    row.offset
                ),
            ));
        }
        if row.offset != expected_offset {
            return Err(index_error(
                bgi_path,
                &format!(
                    "Variant row {index} starts at {} but the preceding rows end at \
                     {expected_offset}",
                    row.offset
                ),
            ));
        }
        expected_offset = end;
    }
    if expected_offset != header.object_size {
        return Err(index_error(
            bgi_path,
            &format!(
                "Variant rows end at {expected_offset}, but the object is {} bytes",
                header.object_size
            ),
        ));
    }
    Ok(())
}

/// Returns a local copy of a remote index, downloading it only on a miss.
///
/// The key comes from the object's identity rather than its bytes, so the cache
/// can be consulted before anything is fetched — keying on content would mean
/// downloading a multi-gigabyte index to discover it was already cached. The
/// download runs under the cache lease, so concurrent opens of the same index
/// wait for the first rather than each fetching their own copy.
async fn cache_remote_index<F, Fut>(
    path: &str,
    validator: Option<&str>,
    expected_size: u64,
    max_cache_bytes: usize,
    configured_directory: Option<&str>,
    download: F,
) -> Result<(PathBuf, Connection)>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Bytes>>,
{
    if expected_size > max_cache_bytes as u64 {
        return Err(index_error(
            path,
            &format!(
                "remote index size {expected_size} exceeds max_bgi_cache_bytes {max_cache_bytes}"
            ),
        ));
    }
    let incoming = usize::try_from(expected_size)
        .map_err(|_| index_error(path, "remote index size does not fit usize"))?;
    let cache_root = configured_directory
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("DATAFUSION_BIO_BGI_CACHE_DIR").map(PathBuf::from))
        .unwrap_or_else(|| {
            std::env::temp_dir()
                .join("datafusion-bio-formats")
                .join("bgi")
        });
    tokio::fs::create_dir_all(&cache_root)
        .await
        .map_err(|error| index_error(path, &format!("create BGI cache: {error}")))?;

    let lock = CACHE_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    // A validator identifies *this version* of the object, so an entry keyed on
    // one can be reused without reading anything. Length alone is not a
    // validator: a replacement of the same length would be served from the cache
    // for ever, and open-time validation cannot catch it — that compares the
    // index against the BGEN object, which has not changed, and the rows' own
    // chromosomes, positions and identifiers are only checked against records a
    // scan actually reads. A stale row would prune matching variants first.
    //
    // So without one, the object's bytes are its only identity, and they have to
    // be fetched before the cache can be consulted. That is slower and always
    // correct.
    let mut download = Some(download);
    let (key, downloaded) = match validator {
        Some(validator) => {
            let mut hasher = blake3::Hasher::new();
            hasher.update(path.as_bytes());
            hasher.update(validator.as_bytes());
            (hasher.finalize().to_hex(), None)
        }
        None => {
            let fetch = download.take().expect("the index is downloaded once");
            let bytes = fetch().await?;
            let mut hasher = blake3::Hasher::new();
            hasher.update(path.as_bytes());
            hasher.update(&bytes);
            (hasher.finalize().to_hex(), Some(bytes))
        }
    };
    let destination = cache_root.join(format!("{key}.bgi"));
    if tokio::fs::metadata(&destination)
        .await
        .is_ok_and(|metadata| metadata.len() == expected_size)
    {
        // Reusing an entry still has to respect this provider's limit: entries
        // written under a larger limit would otherwise keep the shared cache
        // above the configured maximum indefinitely. The destination itself is
        // counted as incoming and excluded from eviction, matching the miss path.
        evict_cache_entries(&cache_root, incoming, max_cache_bytes, &destination).await?;
        // Opened before the lease is released so a concurrent provider cannot
        // evict this entry between publishing it and opening it.
        let connection = open_retained_index(&destination)?;
        drop(lock);
        return Ok((destination, connection));
    }

    evict_cache_entries(&cache_root, incoming, max_cache_bytes, &destination).await?;
    // Fetched only now that the entry is known to be missing, and under the
    // lease, so concurrent opens of the same index do not each download it.
    let bytes = match downloaded {
        Some(bytes) => bytes,
        None => {
            let fetch = download.take().expect("the index is downloaded once");
            fetch().await?
        }
    };
    let temporary = cache_root.join(format!(".{key}.{}.tmp", std::process::id()));
    if tokio::fs::try_exists(&temporary).await.unwrap_or(false) {
        tokio::fs::remove_file(&temporary)
            .await
            .map_err(|error| index_error(path, &format!("remove stale cached BGI: {error}")))?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .await
        .map_err(|error| index_error(path, &format!("create cached BGI: {error}")))?;
    file.write_all(&bytes)
        .await
        .map_err(|error| index_error(path, &format!("write cached BGI: {error}")))?;
    file.sync_all()
        .await
        .map_err(|error| index_error(path, &format!("sync cached BGI: {error}")))?;
    drop(file);
    tokio::fs::rename(&temporary, &destination)
        .await
        .map_err(|error| index_error(path, &format!("publish cached BGI: {error}")))?;
    let connection = open_retained_index(&destination)?;
    drop(lock);
    Ok((destination, connection))
}

/// Opens the long-lived read-only connection kept by a [`BgiIndex`].
fn open_retained_index(path: &Path) -> Result<Connection> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| {
        index_error(
            &path.to_string_lossy(),
            &format!("open for pushdown: {error}"),
        )
    })
}

async fn evict_cache_entries(
    cache_root: &Path,
    incoming: usize,
    max_cache_bytes: usize,
    destination: &Path,
) -> Result<()> {
    let mut entries = Vec::new();
    let mut total = 0_u64;
    let mut directory = tokio::fs::read_dir(cache_root)
        .await
        .map_err(|error| index_error("BGI cache", &format!("list cache: {error}")))?;
    while let Some(entry) = directory
        .next_entry()
        .await
        .map_err(|error| index_error("BGI cache", &format!("read cache entry: {error}")))?
    {
        let path = entry.path();
        if path == destination || path.extension().and_then(|value| value.to_str()) != Some("bgi") {
            continue;
        }
        let metadata = entry
            .metadata()
            .await
            .map_err(|error| index_error("BGI cache", &format!("stat cache entry: {error}")))?;
        if !metadata.is_file() {
            continue;
        }
        total = total.saturating_add(metadata.len());
        entries.push((
            metadata
                .modified()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
            metadata.len(),
            path,
        ));
    }
    entries.sort_by_key(|entry| entry.0);
    let limit = max_cache_bytes as u64;
    let incoming = incoming as u64;
    for (_, size, path) in entries {
        if total.saturating_add(incoming) <= limit {
            break;
        }
        // An entry another provider still holds open cannot be removed on
        // Windows, which locks open files, and a shared cache is exactly where
        // that happens. Failing the open over it would make an unrelated
        // concurrent reader break this one, so an entry that will not go is
        // left in place and its bytes stay counted; the shortfall is reported
        // below only if the cache genuinely cannot fit the incoming index.
        match tokio::fs::remove_file(&path).await {
            Ok(()) => total = total.saturating_sub(size),
            Err(error) => debug!(
                "BGI cache: leaving {} in place: {error}",
                sanitize_location(&path.to_string_lossy())
            ),
        }
    }
    if total.saturating_add(incoming) > limit {
        return Err(index_error(
            "BGI cache",
            "cache cannot satisfy max_bgi_cache_bytes after eviction",
        ));
    }
    Ok(())
}

fn index_error(path: &str, message: &str) -> DataFusionError {
    DataFusionError::Plan(format!("BGI {}: {message}", sanitize_location(path)))
}
