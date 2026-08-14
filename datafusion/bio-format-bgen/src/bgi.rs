use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{Expr, Operator, expr::InList};
use datafusion_bio_format_core::companion::{CompanionRule, resolve_companion, sanitize_location};
use datafusion_bio_format_core::genotype::CoordinateSystem;
use rusqlite::types::Value;
use rusqlite::{Connection, OpenFlags};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::catalog::BgenCatalog;
use crate::header::BgenHeader;
use crate::source::ObjectAccess;
use crate::table_provider::{BgenReadOptions, StaleBgiPolicy};

static CACHE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Clone)]
pub(crate) struct BgiIndex {
    pub(crate) row_indices: Arc<Vec<usize>>,
    pub(crate) bytes_read: u64,
    pub(crate) primary_bytes_read: u64,
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
        if clauses.is_empty() {
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

pub(crate) async fn open_optional_bgi(
    primary_path: &str,
    primary_source: &ObjectAccess,
    header: &BgenHeader,
    catalog: &BgenCatalog,
    options: &BgenReadOptions,
) -> Result<Option<BgiIndex>> {
    let storage_options = options.object_storage_options.clone().unwrap_or_default();
    let bgi_path = resolve_companion(
        primary_path,
        "BGI",
        options.bgi_path.as_deref(),
        &[CompanionRule::AppendSuffix(".bgi".to_string())],
        false,
        |candidate| {
            let storage_options = storage_options.clone();
            async move { ObjectAccess::exists(&candidate, &storage_options).await }
        },
    )
    .await?;
    let Some(bgi_path) = bgi_path else {
        return Ok(None);
    };

    let explicit = options.bgi_path.is_some();
    let result = open_and_validate(
        primary_path,
        primary_source,
        &bgi_path,
        header,
        catalog,
        options,
    )
    .await;
    match result {
        Ok(index) => Ok(Some(index)),
        Err(_) if !explicit && options.stale_bgi_policy == StaleBgiPolicy::Ignore => Ok(None),
        Err(error) => Err(error),
    }
}

async fn open_and_validate(
    primary_path: &str,
    primary_source: &ObjectAccess,
    bgi_path: &str,
    header: &BgenHeader,
    catalog: &BgenCatalog,
    options: &BgenReadOptions,
) -> Result<BgiIndex> {
    let storage_options = options.object_storage_options.clone().unwrap_or_default();
    let bgi_source = ObjectAccess::open(bgi_path, &storage_options).await?;
    let bgi_size = bgi_source.size(bgi_path).await?;
    if bgi_size > options.max_bgi_bytes as u64 {
        return Err(index_error(
            bgi_path,
            &format!(
                "index size {bgi_size} exceeds max_bgi_bytes {}",
                options.max_bgi_bytes
            ),
        ));
    }
    let sqlite_path = if let Some(path) = bgi_source.local_path() {
        PathBuf::from(path)
    } else {
        let bytes = bgi_source
            .read_all_bounded(bgi_path, options.max_bgi_bytes)
            .await?;
        cache_remote_index(
            bgi_path,
            &bytes,
            options.max_bgi_cache_bytes,
            options.bgi_cache_directory.as_deref(),
        )
        .await?
    };

    let display_path = bgi_path.to_string();
    let validation_path = sqlite_path.clone();
    let (metadata, rows) =
        tokio::task::spawn_blocking(move || read_sqlite(&validation_path, &display_path))
            .await
            .map_err(|error| {
                index_error(bgi_path, &format!("SQLite validation task failed: {error}"))
            })??;
    validate_identity(
        primary_path,
        primary_source,
        header,
        catalog,
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
    // Opened here, before any later provider can evict this cache entry, and
    // retained so pushdown never has to reopen the path.
    let connection = Connection::open_with_flags(
        &sqlite_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| {
        index_error(
            &sqlite_path.to_string_lossy(),
            &format!("open for pushdown: {error}"),
        )
    })?;

    Ok(BgiIndex {
        row_indices: Arc::new((0..rows.len()).collect()),
        bytes_read: bgi_size,
        primary_bytes_read: header.object_size.min(1000),
        connection: Arc::new(std::sync::Mutex::new(connection)),
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

fn read_sqlite(path: &Path, display_path: &str) -> Result<(BgiMetadata, Vec<BgiRow>)> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| index_error(display_path, &format!("open SQLite index: {error}")))?;
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

async fn validate_identity(
    primary_path: &str,
    primary_source: &ObjectAccess,
    header: &BgenHeader,
    catalog: &BgenCatalog,
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
    let identity_length = header.object_size.min(1000);
    let primary_prefix = primary_source
        .read_range(primary_path, 0..identity_length)
        .await?;
    if metadata.first_bytes != primary_prefix.as_ref() {
        return Err(index_error(
            bgi_path,
            "Metadata.first_1000_bytes does not match the BGEN object",
        ));
    }
    if rows.len() != catalog.variants.len() {
        return Err(index_error(
            bgi_path,
            &format!(
                "Variant row count {} differs from BGEN count {}",
                rows.len(),
                catalog.variants.len()
            ),
        ));
    }

    for (index, (row, variant)) in rows.iter().zip(catalog.variants.iter()).enumerate() {
        let end = row.offset.checked_add(row.size).ok_or_else(|| {
            index_error(bgi_path, &format!("Variant row {index} range overflowed"))
        })?;
        if row.size == 0 || row.offset < header.first_variant_offset || end > header.object_size {
            return Err(index_error(
                bgi_path,
                &format!(
                    "Variant row {index} range {}..{end} is out of bounds",
                    row.offset
                ),
            ));
        }
        if row.offset != variant.record_offset
            || row.size != variant.record_size
            || row.chrom != variant.chrom
            || row.position != variant.position
            || row.rsid.as_deref() != variant.rsid.as_deref()
            || row.allele_count != variant.alleles.len()
            || row.allele1.as_deref() != variant.alleles.first().map(String::as_str)
            || row.allele2.as_deref() != variant.alleles.get(1).map(String::as_str)
        {
            return Err(index_error(
                bgi_path,
                &format!("Variant row {index} does not match BGEN metadata"),
            ));
        }
    }
    Ok(())
}

async fn cache_remote_index(
    path: &str,
    bytes: &[u8],
    max_cache_bytes: usize,
    configured_directory: Option<&str>,
) -> Result<PathBuf> {
    if bytes.len() > max_cache_bytes {
        return Err(index_error(
            path,
            &format!(
                "remote index size {} exceeds max_bgi_cache_bytes {max_cache_bytes}",
                bytes.len()
            ),
        ));
    }
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

    let mut hasher = blake3::Hasher::new();
    hasher.update(path.as_bytes());
    hasher.update(bytes);
    let key = hasher.finalize().to_hex();
    let destination = cache_root.join(format!("{key}.bgi"));
    let lock = CACHE_LOCK.get_or_init(|| Mutex::new(())).lock().await;
    if tokio::fs::metadata(&destination)
        .await
        .is_ok_and(|metadata| metadata.len() == bytes.len() as u64)
    {
        drop(lock);
        return Ok(destination);
    }

    evict_cache_entries(&cache_root, bytes.len(), max_cache_bytes, &destination).await?;
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
    file.write_all(bytes)
        .await
        .map_err(|error| index_error(path, &format!("write cached BGI: {error}")))?;
    file.sync_all()
        .await
        .map_err(|error| index_error(path, &format!("sync cached BGI: {error}")))?;
    drop(file);
    tokio::fs::rename(&temporary, &destination)
        .await
        .map_err(|error| index_error(path, &format!("publish cached BGI: {error}")))?;
    drop(lock);
    Ok(destination)
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
        tokio::fs::remove_file(&path)
            .await
            .map_err(|error| index_error("BGI cache", &format!("evict cache entry: {error}")))?;
        total = total.saturating_sub(size);
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
