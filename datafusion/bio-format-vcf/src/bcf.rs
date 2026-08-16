use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::{self, Cursor, Read};
use std::sync::Arc;

use async_stream::try_stream;
use datafusion::arrow::array::{Array, Int8Array, ListArray, NullBufferBuilder, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::object_storage::{
    ObjectStorageOptions, RemoteObject, StorageType, get_remote_stream_bgzf_head_tolerant,
    get_remote_stream_bgzf_single_request, get_storage_type,
};
use datafusion_bio_format_core::partition_balancer::PartitionAssignment;
use datafusion_bio_format_core::partition_balancer::RegionSizeEstimate;
use datafusion_bio_format_core::record_filter::{RecordFieldAccessor, evaluate_record_filters};
use datafusion_bio_format_core::table_utils::{OptionalField, builders_to_arrays};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::{TryStream, TryStreamExt};
use log::info;
use noodles_bcf::{self as bcf, Record as BcfRecord};
use noodles_vcf::Header;
use noodles_vcf::header::record::value::map::format::{Number as FormatNumber, Type as FormatType};
use noodles_vcf::header::record::value::map::info::{Number as InfoNumber, Type as InfoType};
use noodles_vcf::variant::Record as VariantRecord;
use noodles_vcf::variant::record::Samples as _;
use noodles_vcf::variant::record::info::field::Value as InfoValue;
use noodles_vcf::variant::record::{AlternateBases, Filters, Info as _};
use smallvec::SmallVec;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

use crate::physical_exec::{
    CoreBatchBuilders, FormatMode, ProjectionFlags,
    adjust_effective_batch_size_by_observed_format_bytes, build_noodles_region,
    build_record_batch_from_builders, choose_dosage_effective_batch_size,
    choose_effective_batch_size, choose_initial_builder_batch_size, init_format_mode,
    is_missing_info_value_error, load_infos_single_pass, resolve_selected_sample_indices,
    set_info_builders,
};
use crate::table_provider::GenotypeOutputMode;

const SUPPORTED_BCF_VERSION: (u8, u8) = (2, 2);
const BCF_HEADER_PREFIX_SIZE: usize = 9;
const BCF_RECORD_LENGTH_PREFIX_SIZE: usize = 8;
const BCF_FIXED_SITE_PREFIX_SIZE: usize = 24;
const MIN_BCF_SHARED_LENGTH: u64 = 24;
// Like record bodies, the BCF header text length is an untrusted u32. Bound it
// before noodles reads the text into growable buffers.
const MAX_BCF_HEADER_TEXT_SIZE: u64 = 256 * 1024 * 1024;
// This is a hard safety ceiling, not a tuning target. Real records are normally
// orders of magnitude smaller, while the on-disk u32 fields can declare nearly
// 8 GiB in aggregate and make the decoder reserve that memory before parsing.
const MAX_BCF_RECORD_BODY_SIZE: u64 = 256 * 1024 * 1024;
// CSI companions are normally much smaller than the BCF itself. Bound their
// compressed bytes before parsing so an untrusted remote object cannot make
// provider planning buffer an arbitrary response.
const MAX_BCF_CSI_INDEX_SIZE: usize = 256 * 1024 * 1024;
// A single CSI chunk may span a large part of a remote BCF. Decode the logical
// range incrementally with one bounded compressed chunk in flight per scan
// partition instead of allocating the whole span.
const MAX_REMOTE_BCF_STREAM_CHUNK_SIZE: usize = 8 * 1024 * 1024;

fn validate_bcf_header_prefix(prefix: [u8; BCF_HEADER_PREFIX_SIZE]) -> Result<u64> {
    if &prefix[..3] != b"BCF" {
        return Err(DataFusionError::Execution(format!(
            "invalid BCF magic: expected BCF, found {:02x?}",
            &prefix[..3]
        )));
    }

    validate_version((prefix[3], prefix[4]))?;

    let header_len = u64::from(u32::from_le_bytes(prefix[5..].try_into().unwrap()));
    if header_len > MAX_BCF_HEADER_TEXT_SIZE {
        return Err(DataFusionError::Execution(format!(
            "invalid BCF header length: l_text is {header_len}, exceeding the \
             {MAX_BCF_HEADER_TEXT_SIZE}-byte safety limit"
        )));
    }

    Ok(header_len)
}

fn read_bcf_header_bounded<R>(inner: &mut R) -> Result<Header>
where
    R: Read,
{
    let mut prefix = [0; BCF_HEADER_PREFIX_SIZE];
    inner
        .read_exact(&mut prefix)
        .map_err(|e| execution_error("failed to read BCF header prefix", e))?;
    let header_len = validate_bcf_header_prefix(prefix)?;

    let body = (&mut *inner).take(header_len);
    let bounded = Cursor::new(prefix).chain(body);
    let header = bcf::io::Reader::from(bounded)
        .read_header()
        .map_err(|e| execution_error("failed to parse BCF header", e))?;
    validate_bcf_header(&header)?;
    Ok(header)
}

async fn read_bcf_header_bounded_async<R>(inner: &mut R) -> Result<Header>
where
    R: AsyncRead + Unpin,
{
    let mut prefix = [0; BCF_HEADER_PREFIX_SIZE];
    tokio::io::AsyncReadExt::read_exact(&mut *inner, &mut prefix)
        .await
        .map_err(|e| execution_error("failed to read BCF header prefix", e))?;
    let header_len = validate_bcf_header_prefix(prefix)?;

    let body = tokio::io::AsyncReadExt::take(&mut *inner, header_len);
    let bounded = tokio::io::AsyncReadExt::chain(Cursor::new(prefix), body);
    let header = bcf::r#async::io::Reader::from(bounded)
        .read_header()
        .await
        .map_err(|e| execution_error("failed to parse BCF header", e))?;
    validate_bcf_header(&header)?;
    Ok(header)
}

fn validate_bcf_header(header: &Header) -> Result<()> {
    let Some(gt) = header.formats().get("GT") else {
        return Ok(());
    };

    // GT is physically encoded as an integer vector in BCF, but its logical
    // VCF header declaration remains reserved as Number=1,Type=String.
    if gt.number() != FormatNumber::Count(1) || gt.ty() != FormatType::String {
        return Err(DataFusionError::Execution(
            "invalid BCF GT header declaration: expected Number=1,Type=String".into(),
        ));
    }

    Ok(())
}

fn read_bcf_record_lengths(prefix: [u8; BCF_RECORD_LENGTH_PREFIX_SIZE]) -> io::Result<(u64, u64)> {
    let shared = u64::from(u32::from_le_bytes(prefix[..4].try_into().unwrap()));
    let individual = u64::from(u32::from_le_bytes(prefix[4..].try_into().unwrap()));

    if shared < MIN_BCF_SHARED_LENGTH {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "invalid BCF record length: l_shared is {shared}, expected at least \
                 {MIN_BCF_SHARED_LENGTH} bytes"
            ),
        ));
    }

    let body_size = shared.checked_add(individual).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid BCF record length: l_shared + l_indiv overflowed",
        )
    })?;
    if body_size > MAX_BCF_RECORD_BODY_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "invalid BCF record length: declared {body_size} body bytes \
                 (l_shared={shared}, l_indiv={individual}) exceeds the \
                 {MAX_BCF_RECORD_BODY_SIZE}-byte safety limit"
            ),
        ));
    }

    Ok((shared, individual))
}

fn remaining_bcf_record_body_size(shared: u64, individual: u64) -> io::Result<u64> {
    let body_size = shared.checked_add(individual).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid BCF record length: l_shared + l_indiv overflowed",
        )
    })?;

    body_size
        .checked_sub(BCF_FIXED_SITE_PREFIX_SIZE as u64)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid BCF record length: body is shorter than the fixed site prefix",
            )
        })
}

fn validate_bcf_fixed_site(prefix: &[u8; BCF_FIXED_SITE_PREFIX_SIZE]) -> io::Result<()> {
    let span = i32::from_le_bytes(prefix[8..12].try_into().unwrap());
    if span <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid BCF record span: rlen is {span}, expected a positive value"),
        ));
    }

    let allele_count = u16::from_le_bytes(prefix[18..20].try_into().unwrap());
    if allele_count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid BCF allele count: expected at least one reference allele",
        ));
    }

    Ok(())
}

fn read_bcf_record_bounded<R>(
    reader: &mut bcf::io::Reader<R>,
    record: &mut BcfRecord,
) -> io::Result<usize>
where
    R: Read,
{
    let inner = reader.get_mut();
    let mut length_prefix = [0; BCF_RECORD_LENGTH_PREFIX_SIZE];
    if inner.read(&mut length_prefix[..1])? == 0 {
        return Ok(0);
    }
    inner.read_exact(&mut length_prefix[1..])?;

    let (shared, individual) = read_bcf_record_lengths(length_prefix)?;
    let mut fixed_prefix = [0; BCF_FIXED_SITE_PREFIX_SIZE];
    inner.read_exact(&mut fixed_prefix)?;
    validate_bcf_fixed_site(&fixed_prefix)?;

    let remaining = remaining_bcf_record_body_size(shared, individual)?;
    let remainder = (&mut *inner).take(remaining);
    let bounded = Cursor::new(length_prefix)
        .chain(Cursor::new(fixed_prefix))
        .chain(remainder);
    bcf::io::Reader::from(bounded).read_record(record)
}

async fn read_bcf_record_bounded_async<R>(
    reader: &mut bcf::r#async::io::Reader<R>,
    record: &mut BcfRecord,
) -> io::Result<usize>
where
    R: AsyncRead + Unpin,
{
    let inner = reader.get_mut();
    let mut length_prefix = [0; BCF_RECORD_LENGTH_PREFIX_SIZE];
    if tokio::io::AsyncReadExt::read(inner, &mut length_prefix[..1]).await? == 0 {
        return Ok(0);
    }
    tokio::io::AsyncReadExt::read_exact(inner, &mut length_prefix[1..]).await?;

    let (shared, individual) = read_bcf_record_lengths(length_prefix)?;
    let mut fixed_prefix = [0; BCF_FIXED_SITE_PREFIX_SIZE];
    tokio::io::AsyncReadExt::read_exact(inner, &mut fixed_prefix).await?;
    validate_bcf_fixed_site(&fixed_prefix)?;

    let remaining = remaining_bcf_record_body_size(shared, individual)?;
    let remainder = tokio::io::AsyncReadExt::take(&mut *inner, remaining);
    let prefixes =
        tokio::io::AsyncReadExt::chain(Cursor::new(length_prefix), Cursor::new(fixed_prefix));
    let bounded = tokio::io::AsyncReadExt::chain(prefixes, remainder);
    bcf::r#async::io::Reader::from(bounded)
        .read_record(record)
        .await
}

fn execution_error(context: &str, error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!("{context}: {error}"))
}

fn validate_version(version: (u8, u8)) -> Result<()> {
    if version == SUPPORTED_BCF_VERSION {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "unsupported BCF version {}.{}; expected 2.2; transcode the input first (for \
             example, with `bcftools view -Ob input.bcf -o output.bcf`)",
            version.0, version.1
        )))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BcfEncodedType {
    Null,
    Int8,
    Int16,
    Int32,
    Float,
    String,
}

impl BcfEncodedType {
    fn width(self) -> usize {
        match self {
            Self::Null => 0,
            Self::Int8 | Self::String => 1,
            Self::Int16 => 2,
            Self::Int32 | Self::Float => 4,
        }
    }
}

fn take_bcf_bytes<'a>(src: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    if src.len() < len {
        return Err(DataFusionError::Execution(
            "invalid BCF FORMAT encoding: unexpected end of record".into(),
        ));
    }

    let (value, rest) = src.split_at(len);
    *src = rest;
    Ok(value)
}

fn read_bcf_encoded_type(src: &mut &[u8]) -> Result<(BcfEncodedType, usize)> {
    let descriptor = take_bcf_bytes(src, 1)?[0];
    let mut len = usize::from(descriptor >> 4);
    if len == 0x0f {
        let extended_len = read_bcf_typed_integer(src)?;
        len = usize::try_from(extended_len).map_err(|_| {
            DataFusionError::Execution(format!(
                "invalid BCF FORMAT encoding: negative extended length {extended_len}"
            ))
        })?;
    }

    let encoded_type = match descriptor & 0x0f {
        0 => BcfEncodedType::Null,
        1 => BcfEncodedType::Int8,
        2 => BcfEncodedType::Int16,
        3 => BcfEncodedType::Int32,
        5 => BcfEncodedType::Float,
        7 => BcfEncodedType::String,
        code => {
            return Err(DataFusionError::Execution(format!(
                "invalid BCF FORMAT encoding: unsupported type code {code}"
            )));
        }
    };

    Ok((encoded_type, len))
}

fn read_bcf_typed_integer(src: &mut &[u8]) -> Result<i32> {
    let (encoded_type, len) = read_bcf_encoded_type(src)?;
    if len != 1 {
        return Err(DataFusionError::Execution(format!(
            "invalid BCF FORMAT encoding: expected one integer, found {len} values"
        )));
    }

    let value = match encoded_type {
        BcfEncodedType::Int8 => i32::from(take_bcf_bytes(src, 1)?[0] as i8),
        BcfEncodedType::Int16 => i32::from(i16::from_le_bytes(
            take_bcf_bytes(src, 2)?.try_into().unwrap(),
        )),
        BcfEncodedType::Int32 => i32::from_le_bytes(take_bcf_bytes(src, 4)?.try_into().unwrap()),
        BcfEncodedType::Null | BcfEncodedType::Float | BcfEncodedType::String => {
            return Err(DataFusionError::Execution(
                "invalid BCF FORMAT encoding: expected integer value".into(),
            ));
        }
    };

    Ok(value)
}

fn info_type_accepts_encoding(info_type: InfoType, encoded_type: BcfEncodedType) -> bool {
    match info_type {
        InfoType::Integer => matches!(
            encoded_type,
            BcfEncodedType::Null
                | BcfEncodedType::Int8
                | BcfEncodedType::Int16
                | BcfEncodedType::Int32
        ),
        InfoType::Float => matches!(encoded_type, BcfEncodedType::Null | BcfEncodedType::Float),
        InfoType::Flag => matches!(encoded_type, BcfEncodedType::Null | BcfEncodedType::Int8),
        InfoType::Character | InfoType::String => {
            matches!(encoded_type, BcfEncodedType::Null | BcfEncodedType::String)
        }
    }
}

fn format_type_accepts_encoding(
    key: &str,
    format_type: FormatType,
    encoded_type: BcfEncodedType,
) -> bool {
    if key == "GT" {
        return matches!(
            encoded_type,
            BcfEncodedType::Int8 | BcfEncodedType::Int16 | BcfEncodedType::Int32
        );
    }

    // Unlike INFO, FORMAT vectors reserve a fixed-width slot for every sample.
    // Missing FORMAT values therefore use the declared type's sentinel (or "."
    // for strings), rather than a standalone BCF Null descriptor.
    match format_type {
        FormatType::Integer => matches!(
            encoded_type,
            BcfEncodedType::Int8 | BcfEncodedType::Int16 | BcfEncodedType::Int32
        ),
        FormatType::Float => matches!(encoded_type, BcfEncodedType::Float),
        FormatType::Character | FormatType::String => {
            matches!(encoded_type, BcfEncodedType::String)
        }
    }
}

fn bcf_scalar_value_is_missing(encoded_type: BcfEncodedType, payload: &[u8]) -> bool {
    match encoded_type {
        BcfEncodedType::Null => payload.is_empty(),
        BcfEncodedType::Int8 => payload == [0x80],
        BcfEncodedType::Int16 => payload == i16::MIN.to_le_bytes(),
        BcfEncodedType::Int32 => payload == i32::MIN.to_le_bytes(),
        BcfEncodedType::Float => payload == 0x7f80_0001_u32.to_le_bytes(),
        BcfEncodedType::String => payload == b".",
    }
}

fn bcf_numeric_value_is_vector_end(encoded_type: BcfEncodedType, value: &[u8]) -> bool {
    match encoded_type {
        BcfEncodedType::Int8 => value == [(i8::MIN + 1) as u8],
        BcfEncodedType::Int16 => value == (i16::MIN + 1).to_le_bytes(),
        BcfEncodedType::Int32 => value == (i32::MIN + 1).to_le_bytes(),
        BcfEncodedType::Float => value == 0x7f80_0002_u32.to_le_bytes(),
        BcfEncodedType::Null | BcfEncodedType::String => false,
    }
}

fn bcf_numeric_value_is_reserved(encoded_type: BcfEncodedType, value: &[u8]) -> bool {
    match encoded_type {
        BcfEncodedType::Int8 => ((i8::MIN + 2)..=(i8::MIN + 7)).contains(&(value[0] as i8)),
        BcfEncodedType::Int16 => {
            let value = i16::from_le_bytes(value.try_into().unwrap());
            ((i16::MIN + 2)..=(i16::MIN + 7)).contains(&value)
        }
        BcfEncodedType::Int32 => {
            let value = i32::from_le_bytes(value.try_into().unwrap());
            ((i32::MIN + 2)..=(i32::MIN + 7)).contains(&value)
        }
        BcfEncodedType::Float => {
            let bits = u32::from_le_bytes(value.try_into().unwrap());
            (0x7f80_0003..=0x7f80_0007).contains(&bits)
        }
        BcfEncodedType::Null | BcfEncodedType::String => false,
    }
}

fn bcf_numeric_vector_is_missing(encoded_type: BcfEncodedType, payload: &[u8]) -> bool {
    let mut values = payload.chunks_exact(encoded_type.width());
    values
        .next()
        .is_some_and(|value| bcf_scalar_value_is_missing(encoded_type, value))
        && values.all(|value| bcf_numeric_value_is_vector_end(encoded_type, value))
}

#[derive(Clone, Copy)]
enum BcfFieldContext {
    Info,
    FormatSample(usize),
}

impl std::fmt::Display for BcfFieldContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Info => f.write_str("INFO"),
            Self::FormatSample(sample_index) => write!(f, "FORMAT sample {sample_index}"),
        }
    }
}

fn bcf_numeric_logical_value_count(
    context: BcfFieldContext,
    key: &str,
    encoded_type: BcfEncodedType,
    payload: &[u8],
) -> Result<usize> {
    let width = encoded_type.width();
    debug_assert!(width > 0);
    let mut logical_count = 0;
    let mut reached_vector_end = false;

    for (value_offset, value) in payload.chunks_exact(width).enumerate() {
        if bcf_numeric_value_is_vector_end(encoded_type, value) {
            reached_vector_end = true;
        } else if bcf_numeric_value_is_reserved(encoded_type, value) {
            return Err(DataFusionError::Execution(format!(
                "invalid BCF {context} field '{key}': reserved numeric value at offset \
                 {value_offset}"
            )));
        } else if reached_vector_end {
            return Err(DataFusionError::Execution(format!(
                "invalid BCF {context} field '{key}': value after vector-end at offset \
                 {value_offset}"
            )));
        } else {
            // Missing sentinels are logical null elements and still occupy one
            // position in a fixed-cardinality vector.
            logical_count += 1;
        }
    }

    Ok(logical_count)
}

fn validate_bcf_string_payload<'a>(
    key: &str,
    sample_index: Option<usize>,
    payload: &'a [u8],
) -> Result<&'a str> {
    let end = payload
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(payload.len());
    if end < payload.len()
        && let Some(trailing_offset) = payload[end + 1..].iter().position(|&byte| byte != 0)
    {
        let offset = end + 1 + trailing_offset;
        let context = sample_index
            .map(|sample_index| format!("FORMAT string for field '{key}', sample {sample_index}"))
            .unwrap_or_else(|| format!("INFO string for field '{key}'"));
        return Err(DataFusionError::Execution(format!(
            "invalid BCF {context}: value after vector-end at offset {offset}"
        )));
    }

    std::str::from_utf8(&payload[..end]).map_err(|error| {
        let context = sample_index
            .map(|sample_index| format!("FORMAT string for field '{key}', sample {sample_index}"))
            .unwrap_or_else(|| format!("INFO string for field '{key}'"));
        execution_error(&format!("invalid BCF {context}"), error)
    })
}

fn validate_bcf_info_fixed_cardinality(
    key: &str,
    info_number: InfoNumber,
    info_type: InfoType,
    encoded_type: BcfEncodedType,
    value_count: usize,
    payload: &[u8],
    allele_count: usize,
) -> Result<()> {
    let expected_count = match info_number {
        InfoNumber::Count(count) => Some(count),
        InfoNumber::AlternateBases => Some(allele_count - 1),
        InfoNumber::ReferenceAlternateBases => Some(allele_count),
        InfoNumber::Samples | InfoNumber::Unknown => None,
    };

    if matches!(info_type, InfoType::Flag) {
        if expected_count.is_some_and(|count| count != 0) {
            return Err(DataFusionError::Execution(format!(
                "INFO flag '{key}' declares invalid fixed cardinality {}",
                expected_count.unwrap()
            )));
        }
        return Ok(());
    }

    let string_value = matches!(info_type, InfoType::Character | InfoType::String)
        .then(|| validate_bcf_string_payload(key, None, payload))
        .transpose()?;

    // A whole-field missing sentinel is not a zero- or one-element biological
    // vector, and is valid for any fixed cardinality.
    let is_missing = value_count == 0
        || string_value.is_some_and(|value| value == ".")
        || (value_count == 1 && bcf_scalar_value_is_missing(encoded_type, payload));
    if is_missing {
        return Ok(());
    }

    let actual_count = match info_type {
        InfoType::Character | InfoType::String => {
            let value = string_value.expect("string INFO payload was validated above");
            if info_type == InfoType::Character {
                validate_bcf_character_elements(BcfFieldContext::Info, key, value)?;
            }
            value.bytes().filter(|&byte| byte == b',').count() + 1
        }
        InfoType::Integer | InfoType::Float => {
            bcf_numeric_logical_value_count(BcfFieldContext::Info, key, encoded_type, payload)?
        }
        InfoType::Flag => unreachable!("flags were handled above"),
    };

    let Some(expected_count) = expected_count else {
        return Ok(());
    };

    if actual_count != expected_count {
        let message = match info_number {
            InfoNumber::Count(1) => format!(
                "INFO field '{key}' is declared scalar but the BCF record encodes \
                 {actual_count} values"
            ),
            InfoNumber::Count(_) => format!(
                "INFO field '{key}' declares {expected_count} values but the BCF record encodes \
                 {actual_count} values"
            ),
            InfoNumber::AlternateBases => format!(
                "INFO field '{key}' declares Number=A ({expected_count} expected) but the BCF \
                 record encodes {actual_count} values"
            ),
            InfoNumber::ReferenceAlternateBases => format!(
                "INFO field '{key}' declares Number=R ({expected_count} expected) but the BCF \
                 record encodes {actual_count} values"
            ),
            InfoNumber::Samples | InfoNumber::Unknown => {
                unreachable!("dynamic cardinalities returned above")
            }
        };
        return Err(DataFusionError::Execution(message));
    }

    Ok(())
}

fn validate_bcf_character_elements(context: BcfFieldContext, key: &str, value: &str) -> Result<()> {
    for (element_index, element) in value.split(',').enumerate() {
        let character_count = element.chars().count();
        if character_count != 1 {
            return Err(DataFusionError::Execution(format!(
                "invalid BCF {context} Character field '{key}': element {element_index} contains \
                 {character_count} characters"
            )));
        }
    }
    Ok(())
}

fn validate_bcf_info_encoding(
    info: &bcf::record::Info<'_>,
    header: &Header,
    allele_count: usize,
) -> Result<()> {
    let mut src = info.as_ref();
    let mut seen_key_indices = SmallVec::<[usize; 16]>::new();

    for _ in 0..info.len() {
        let key_index = usize::try_from(read_bcf_typed_integer(&mut src)?).map_err(|_| {
            DataFusionError::Execution("invalid BCF INFO string-map index in record".into())
        })?;
        let key = header
            .string_maps()
            .strings()
            .get_index(key_index)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "invalid BCF INFO dictionary index {key_index} in record"
                ))
            })?;
        if seen_key_indices.contains(&key_index) {
            return Err(DataFusionError::Execution(format!(
                "BCF record contains duplicate INFO field '{key}'"
            )));
        }
        seen_key_indices.push(key_index);
        let info = header.infos().get(key).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "BCF INFO dictionary index {key_index} resolves to '{key}', which has no INFO \
                 header definition"
            ))
        })?;

        let (encoded_type, value_count) = read_bcf_encoded_type(&mut src)?;
        if encoded_type == BcfEncodedType::Null && value_count != 0 {
            return Err(DataFusionError::Execution(format!(
                "INFO field '{key}' has an invalid null encoding length {value_count}"
            )));
        }
        if !info_type_accepts_encoding(info.ty(), encoded_type) {
            return Err(DataFusionError::Execution(format!(
                "INFO field '{key}' is declared as {} but the BCF record uses an incompatible \
                 {encoded_type:?} encoding",
                info.ty()
            )));
        }
        let payload_len = encoded_type
            .width()
            .checked_mul(value_count)
            .ok_or_else(|| DataFusionError::Execution("BCF INFO payload length overflow".into()))?;
        let payload = take_bcf_bytes(&mut src, payload_len)?;
        if matches!(info.ty(), InfoType::Flag)
            && encoded_type == BcfEncodedType::Int8
            && (value_count != 1 || payload != [1])
        {
            return Err(DataFusionError::Execution(format!(
                "INFO flag '{key}' has an invalid encoded value"
            )));
        }
        validate_bcf_info_fixed_cardinality(
            key,
            info.number(),
            info.ty(),
            encoded_type,
            value_count,
            payload,
            allele_count,
        )?;
    }

    if !src.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "invalid BCF INFO encoding: {} trailing bytes",
            src.len()
        )));
    }

    Ok(())
}

fn validate_bcf_gt_payload(
    payload: &[u8],
    encoded_type: BcfEncodedType,
    value_count: usize,
    allele_count: usize,
) -> Result<()> {
    if value_count == 0 {
        return Err(DataFusionError::Execution(
            "invalid BCF GT encoding: expected at least one value per sample".into(),
        ));
    }
    let (width, vector_end): (usize, i64) = match encoded_type {
        BcfEncodedType::Int8 => (1, i64::from(i8::MIN) + 1),
        BcfEncodedType::Int16 => (2, i64::from(i16::MIN) + 1),
        BcfEncodedType::Int32 => (4, i64::from(i32::MIN) + 1),
        BcfEncodedType::Null | BcfEncodedType::Float | BcfEncodedType::String => {
            return Err(DataFusionError::Execution(
                "invalid non-integer BCF GT encoding".into(),
            ));
        }
    };
    let sample_width = width
        .checked_mul(value_count)
        .ok_or_else(|| DataFusionError::Execution("BCF GT payload length overflow".into()))?;

    for (sample_index, genotype) in payload.chunks_exact(sample_width).enumerate() {
        let mut reached_vector_end = false;
        let mut ploidy = 0;

        for (allele_offset, raw_allele) in genotype.chunks_exact(width).enumerate() {
            let encoded_allele = match encoded_type {
                BcfEncodedType::Int8 => i64::from(raw_allele[0] as i8),
                BcfEncodedType::Int16 => {
                    i64::from(i16::from_le_bytes(raw_allele.try_into().unwrap()))
                }
                BcfEncodedType::Int32 => {
                    i64::from(i32::from_le_bytes(raw_allele.try_into().unwrap()))
                }
                BcfEncodedType::Null | BcfEncodedType::Float | BcfEncodedType::String => {
                    unreachable!("non-integer GT encodings were rejected above")
                }
            };

            if encoded_allele == vector_end {
                reached_vector_end = true;
                continue;
            }
            if reached_vector_end {
                return Err(DataFusionError::Execution(format!(
                    "invalid BCF GT encoding for sample {sample_index}: value after vector-end at \
                     allele offset {allele_offset}"
                )));
            }
            if encoded_allele < 0 {
                return Err(DataFusionError::Execution(format!(
                    "invalid BCF GT encoding for sample {sample_index}: reserved or invalid \
                     value {encoded_allele} at allele offset {allele_offset}"
                )));
            }
            ploidy += 1;

            // Encoded values 0 and 1 are the unphased/phased missing allele.
            if encoded_allele >= 2 {
                let allele_index = usize::try_from((encoded_allele >> 1) - 1).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "invalid BCF GT allele encoding {encoded_allele} for sample {sample_index}"
                    ))
                })?;
                if allele_index >= allele_count {
                    return Err(DataFusionError::Execution(format!(
                        "invalid BCF GT allele index {allele_index} for sample {sample_index}; \
                         record has {allele_count} REF/ALT alleles"
                    )));
                }
            }
        }
        if ploidy == 0 {
            return Err(DataFusionError::Execution(format!(
                "invalid BCF GT encoding for sample {sample_index}: genotype has zero ploidy"
            )));
        }
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct BcfGtEncoding<'a> {
    payload: &'a [u8],
    encoded_type: BcfEncodedType,
    value_count: usize,
}

#[derive(Clone, Copy)]
struct BcfFormatEncoding<'a> {
    encoded_type: BcfEncodedType,
    value_count: usize,
    payload: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BcfGtDependentNumber {
    Genotypes,
    Ploidy,
}

impl BcfGtDependentNumber {
    fn label(self) -> char {
        match self {
            Self::Genotypes => 'G',
            Self::Ploidy => 'P',
        }
    }
}

struct BcfGtDependentFormatEncoding<'header, 'payload> {
    key: &'header str,
    number: BcfGtDependentNumber,
    format_type: FormatType,
    encoding: BcfFormatEncoding<'payload>,
}

struct BcfFormatValidation<'payload> {
    gt_encoding: Option<BcfGtEncoding<'payload>>,
    gt_values_validated: bool,
}

fn bcf_gt_sample_ploidy(gt: &BcfGtEncoding<'_>, sample_index: usize) -> Result<usize> {
    let width = gt.encoded_type.width();
    if width == 0 {
        return Err(DataFusionError::Execution(
            "invalid non-integer BCF GT encoding".into(),
        ));
    }
    let sample_width = width
        .checked_mul(gt.value_count)
        .ok_or_else(|| DataFusionError::Execution("BCF GT sample width overflow".into()))?;
    let start = sample_width
        .checked_mul(sample_index)
        .ok_or_else(|| DataFusionError::Execution("BCF GT sample offset overflow".into()))?;
    let end = start
        .checked_add(sample_width)
        .ok_or_else(|| DataFusionError::Execution("BCF GT sample offset overflow".into()))?;
    let genotype = gt.payload.get(start..end).ok_or_else(|| {
        DataFusionError::Execution(format!("BCF GT payload is missing sample {sample_index}"))
    })?;
    let ploidy = genotype
        .chunks_exact(width)
        .take_while(|value| !bcf_numeric_value_is_vector_end(gt.encoded_type, value))
        .count();
    if ploidy == 0 {
        return Err(DataFusionError::Execution(format!(
            "invalid BCF GT encoding for sample {sample_index}: genotype has zero ploidy"
        )));
    }

    Ok(ploidy)
}

fn bcf_genotype_cardinality(allele_count: usize, ploidy: usize) -> Result<usize> {
    if allele_count == 0 || ploidy == 0 {
        return Err(DataFusionError::Execution(
            "cannot calculate BCF genotype cardinality with zero alleles or ploidy".into(),
        ));
    }

    // Number=G is the number of unordered genotypes with repetition:
    // C(allele_count + ploidy - 1, ploidy). Use the smaller side of the
    // binomial and a checked wide accumulator so corrupt declarations cannot
    // overflow while being validated.
    let n = allele_count
        .checked_add(ploidy - 1)
        .ok_or_else(|| DataFusionError::Execution("BCF genotype cardinality overflow".into()))?;
    let k = ploidy.min(allele_count - 1);
    let mut result = 1u128;
    for i in 1..=k {
        let factor = n - k + i;
        result = result.checked_mul(factor as u128).ok_or_else(|| {
            DataFusionError::Execution("BCF genotype cardinality overflow".into())
        })? / i as u128;
        if result > usize::MAX as u128 {
            return Err(DataFusionError::Execution(
                "BCF genotype cardinality exceeds the supported size".into(),
            ));
        }
    }

    Ok(result as usize)
}

fn validate_bcf_format_cardinality(
    key: &str,
    format_number: FormatNumber,
    format_type: FormatType,
    encoded_type: BcfEncodedType,
    value_count: usize,
    payload: &[u8],
    allele_count: usize,
) -> Result<()> {
    if key == "GT" {
        return Ok(());
    }

    let expected_count = match format_number {
        FormatNumber::Count(count) => Some(count),
        FormatNumber::AlternateBases => Some(allele_count - 1),
        FormatNumber::ReferenceAlternateBases => Some(allele_count),
        FormatNumber::Samples
        | FormatNumber::LocalAlternateBases
        | FormatNumber::LocalReferenceAlternateBases
        | FormatNumber::LocalSamples
        | FormatNumber::Ploidy
        | FormatNumber::BaseModifications
        | FormatNumber::Unknown => None,
    };

    match format_type {
        FormatType::Integer | FormatType::Float => {
            let sample_width = encoded_type
                .width()
                .checked_mul(value_count)
                .ok_or_else(|| {
                    DataFusionError::Execution("BCF FORMAT sample width overflow".into())
                })?;
            for (sample_index, sample) in payload.chunks_exact(sample_width).enumerate() {
                let actual_count = bcf_numeric_logical_value_count(
                    BcfFieldContext::FormatSample(sample_index),
                    key,
                    encoded_type,
                    sample,
                )?;
                let Some(expected_count) = expected_count else {
                    continue;
                };
                if bcf_numeric_vector_is_missing(encoded_type, sample)
                    || actual_count == expected_count
                {
                    continue;
                }
                let message = match format_number {
                    FormatNumber::Count(1) => format!(
                        "FORMAT field '{key}' is declared scalar but the BCF record encodes \
                         {actual_count} values per sample"
                    ),
                    FormatNumber::Count(_) => format!(
                        "FORMAT field '{key}' declares {expected_count} values but the BCF record \
                         encodes {actual_count} values per sample"
                    ),
                    FormatNumber::AlternateBases => format!(
                        "FORMAT field '{key}' declares Number=A ({expected_count} expected) but \
                         the BCF record encodes {actual_count} values per sample"
                    ),
                    FormatNumber::ReferenceAlternateBases => format!(
                        "FORMAT field '{key}' declares Number=R ({expected_count} expected) but \
                         the BCF record encodes {actual_count} values per sample"
                    ),
                    FormatNumber::Samples
                    | FormatNumber::LocalAlternateBases
                    | FormatNumber::LocalReferenceAlternateBases
                    | FormatNumber::LocalSamples
                    | FormatNumber::Ploidy
                    | FormatNumber::BaseModifications
                    | FormatNumber::Unknown => {
                        unreachable!("dynamic cardinalities returned above")
                    }
                };
                return Err(DataFusionError::Execution(message));
            }
        }
        FormatType::Character | FormatType::String => {
            for (sample_index, raw_value) in payload.chunks_exact(value_count).enumerate() {
                let value = validate_bcf_string_payload(key, Some(sample_index), raw_value)?;
                if value.is_empty() || value == "." {
                    continue;
                }
                if format_type == FormatType::Character {
                    validate_bcf_character_elements(
                        BcfFieldContext::FormatSample(sample_index),
                        key,
                        value,
                    )?;
                }
                let Some(expected_count) = expected_count else {
                    continue;
                };
                let actual_count = value.bytes().filter(|&byte| byte == b',').count() + 1;
                if actual_count != expected_count {
                    return Err(DataFusionError::Execution(format!(
                        "FORMAT field '{key}' declares {expected_count} values but sample \
                         {sample_index} encodes {actual_count} values"
                    )));
                }
            }
        }
    }

    Ok(())
}

fn bcf_gt_dependent_cardinality_error(
    key: &str,
    number: BcfGtDependentNumber,
    expected_count: usize,
    actual_count: usize,
    sample_index: usize,
    ploidy: usize,
) -> DataFusionError {
    let label = number.label();
    DataFusionError::Execution(format!(
        "FORMAT field '{key}' declares Number={label} ({expected_count} expected for sample \
         {sample_index} with ploidy {ploidy}) but the sample encodes {actual_count} values"
    ))
}

#[derive(Clone, Copy)]
enum BcfGtDependentPloidy<'gt, 'payload> {
    AssumedDiploid,
    Encoded(&'gt BcfGtEncoding<'payload>),
}

impl BcfGtDependentPloidy<'_, '_> {
    fn for_sample(self, sample_index: usize) -> Result<usize> {
        match self {
            Self::AssumedDiploid => Ok(2),
            Self::Encoded(gt) => bcf_gt_sample_ploidy(gt, sample_index),
        }
    }
}

fn bcf_gt_dependent_ploidy<'gt, 'payload>(
    key: &str,
    number: BcfGtDependentNumber,
    gt: Option<&'gt BcfGtEncoding<'payload>>,
) -> Result<BcfGtDependentPloidy<'gt, 'payload>> {
    match (number, gt) {
        (_, Some(gt)) => Ok(BcfGtDependentPloidy::Encoded(gt)),
        // VCF specifies that Number=G values are diploid when GT is absent.
        (BcfGtDependentNumber::Genotypes, None) => Ok(BcfGtDependentPloidy::AssumedDiploid),
        (BcfGtDependentNumber::Ploidy, None) => Err(DataFusionError::Execution(format!(
            "FORMAT field '{key}' declares Number=P but the record has no GT field"
        ))),
    }
}

fn validate_bcf_gt_dependent_payload(
    key: &str,
    number: BcfGtDependentNumber,
    format_type: FormatType,
    encoding: BcfFormatEncoding<'_>,
    allele_count: usize,
    gt: Option<&BcfGtEncoding<'_>>,
) -> Result<()> {
    let ploidy = bcf_gt_dependent_ploidy(key, number, gt)?;

    let BcfFormatEncoding {
        encoded_type,
        value_count,
        payload,
    } = encoding;

    match format_type {
        FormatType::Integer | FormatType::Float => {
            let sample_width = encoded_type
                .width()
                .checked_mul(value_count)
                .ok_or_else(|| {
                    DataFusionError::Execution("BCF FORMAT sample width overflow".into())
                })?;
            for (sample_index, sample) in payload.chunks_exact(sample_width).enumerate() {
                let actual_count = bcf_numeric_logical_value_count(
                    BcfFieldContext::FormatSample(sample_index),
                    key,
                    encoded_type,
                    sample,
                )?;
                if bcf_numeric_vector_is_missing(encoded_type, sample) {
                    continue;
                }
                let sample_ploidy = ploidy.for_sample(sample_index)?;
                let expected_count = match number {
                    BcfGtDependentNumber::Genotypes => {
                        bcf_genotype_cardinality(allele_count, sample_ploidy)?
                    }
                    BcfGtDependentNumber::Ploidy => sample_ploidy,
                };
                if actual_count != expected_count {
                    return Err(bcf_gt_dependent_cardinality_error(
                        key,
                        number,
                        expected_count,
                        actual_count,
                        sample_index,
                        sample_ploidy,
                    ));
                }
            }
        }
        FormatType::Character | FormatType::String => {
            for (sample_index, raw_value) in payload.chunks_exact(value_count).enumerate() {
                let value = validate_bcf_string_payload(key, Some(sample_index), raw_value)?;
                if value.is_empty() || value == "." {
                    continue;
                }
                if format_type == FormatType::Character {
                    validate_bcf_character_elements(
                        BcfFieldContext::FormatSample(sample_index),
                        key,
                        value,
                    )?;
                }
                let actual_count = value.bytes().filter(|&byte| byte == b',').count() + 1;
                let sample_ploidy = ploidy.for_sample(sample_index)?;
                let expected_count = match number {
                    BcfGtDependentNumber::Genotypes => {
                        bcf_genotype_cardinality(allele_count, sample_ploidy)?
                    }
                    BcfGtDependentNumber::Ploidy => sample_ploidy,
                };
                if actual_count != expected_count {
                    return Err(bcf_gt_dependent_cardinality_error(
                        key,
                        number,
                        expected_count,
                        actual_count,
                        sample_index,
                        sample_ploidy,
                    ));
                }
            }
        }
    }

    Ok(())
}

fn validate_bcf_format_encoding<'a>(
    samples: &'a bcf::record::Samples<'_>,
    header: &Header,
    allele_count: usize,
    validate_gt_values: bool,
) -> Result<BcfFormatValidation<'a>> {
    let sample_count = samples.len();
    let mut src = samples.as_ref();
    let mut gt_encoding = None;
    let mut gt_values_validated = false;
    let mut gt_dependent_encodings = SmallVec::<[BcfGtDependentFormatEncoding<'_, 'a>; 4]>::new();
    let mut seen_key_indices = SmallVec::<[usize; 8]>::new();

    for _ in 0..samples.format_count() {
        let key_index = usize::try_from(read_bcf_typed_integer(&mut src)?).map_err(|_| {
            DataFusionError::Execution("invalid BCF FORMAT dictionary index in record".into())
        })?;
        let key = header
            .string_maps()
            .strings()
            .get_index(key_index)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "invalid BCF FORMAT dictionary index {key_index} in record"
                ))
            })?;
        if seen_key_indices.contains(&key_index) {
            return Err(DataFusionError::Execution(format!(
                "BCF record contains duplicate FORMAT field '{key}'"
            )));
        }
        seen_key_indices.push(key_index);
        let format = header.formats().get(key).ok_or_else(|| {
            DataFusionError::Execution(format!("BCF FORMAT field '{key}' has no header definition"))
        })?;
        let (encoded_type, value_count) = read_bcf_encoded_type(&mut src)?;
        if value_count == 0 {
            return Err(DataFusionError::Execution(format!(
                "FORMAT field '{key}' has an invalid zero-length encoding"
            )));
        }
        if !format_type_accepts_encoding(key, format.ty(), encoded_type) {
            return Err(DataFusionError::Execution(format!(
                "FORMAT field '{key}' is declared as {} but the BCF record uses an incompatible \
                 {encoded_type:?} encoding",
                format.ty()
            )));
        }

        let payload_len = encoded_type
            .width()
            .checked_mul(value_count)
            .and_then(|len| len.checked_mul(sample_count))
            .ok_or_else(|| {
                DataFusionError::Execution("BCF FORMAT payload length overflow".into())
            })?;
        let payload = take_bcf_bytes(&mut src, payload_len)?;
        let encoding = BcfFormatEncoding {
            encoded_type,
            value_count,
            payload,
        };
        let gt_dependent_number = match format.number() {
            FormatNumber::Samples => Some(BcfGtDependentNumber::Genotypes),
            FormatNumber::Ploidy => Some(BcfGtDependentNumber::Ploidy),
            _ => None,
        };
        if let Some(number) = gt_dependent_number {
            // GT can occur after dependent fields, so retain only their small
            // borrowed descriptors and validate them once GT is known.
            gt_dependent_encodings.push(BcfGtDependentFormatEncoding {
                key,
                number,
                format_type: format.ty(),
                encoding,
            });
        } else {
            validate_bcf_format_cardinality(
                key,
                format.number(),
                format.ty(),
                encoded_type,
                value_count,
                payload,
                allele_count,
            )?;
        }
        if key == "GT" {
            if validate_gt_values {
                validate_bcf_gt_payload(payload, encoded_type, value_count, allele_count)?;
                gt_values_validated = true;
            }
            gt_encoding = Some(BcfGtEncoding {
                payload,
                encoded_type,
                value_count,
            });
        }
    }

    if !src.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "invalid BCF FORMAT encoding: {} trailing bytes",
            src.len()
        )));
    }

    if !gt_dependent_encodings.is_empty() {
        // FORMAT series may appear in any order, so validate Number=G/P only
        // after the first pass has located and validated GT. Retaining borrowed
        // descriptors avoids reparsing the entire FORMAT byte slice. Number=G
        // assumes diploidy when GT is absent; Number=P requires GT.
        if !validate_gt_values && let Some(gt) = gt_encoding.as_ref() {
            validate_bcf_gt_payload(gt.payload, gt.encoded_type, gt.value_count, allele_count)?;
            gt_values_validated = true;
        }
        for dependent in gt_dependent_encodings {
            validate_bcf_gt_dependent_payload(
                dependent.key,
                dependent.number,
                dependent.format_type,
                dependent.encoding,
                allele_count,
                gt_encoding.as_ref(),
            )?;
        }
    }

    Ok(BcfFormatValidation {
        gt_encoding,
        gt_values_validated,
    })
}

fn local_path(path: &str) -> &str {
    path.strip_prefix("file://").unwrap_or(path)
}

fn read_local_header(path: &str) -> Result<Header> {
    let path = local_path(path);
    let file = File::open(path).map_err(|e| execution_error("failed to open BCF", e))?;
    let mut reader = bcf::io::Reader::new(file);
    read_bcf_header_bounded(reader.get_mut())
}

async fn read_remote_header(
    path: &str,
    object_storage_options: ObjectStorageOptions,
) -> Result<Header> {
    // A bounded header read needs no object length, so it goes through a single
    // request: the chunked whole-object path asks the backend for the size and
    // so issues a HEAD, which a pre-signed GET/range URL can reject even though
    // every read this provider actually makes would succeed.
    let mut inner = get_remote_stream_bgzf_single_request(path.to_string(), object_storage_options)
        .await
        .map_err(|e| execution_error("failed to open remote BCF", e))?;
    read_bcf_header_bounded_async(&mut inner).await
}

pub(crate) async fn read_header(
    path: &str,
    object_storage_options: Option<ObjectStorageOptions>,
) -> Result<Header> {
    match get_storage_type(path.to_string()) {
        StorageType::LOCAL => read_local_header(path),
        _ => read_remote_header(path, object_storage_options.unwrap_or_default()).await,
    }
}

async fn read_csi_index(
    index_path: &str,
    object_storage_options: Option<ObjectStorageOptions>,
) -> Result<noodles_csi::Index> {
    match get_storage_type(index_path.to_string()) {
        StorageType::LOCAL => read_local_csi_index(index_path),
        _ => {
            let object = RemoteObject::open(
                index_path.to_string(),
                object_storage_options.unwrap_or_default(),
            )
            .await
            .map_err(|error| execution_error("failed to open remote BCF CSI index", error))?;
            let stream = object
                .stream_single_request()
                .await
                .map_err(|error| execution_error("failed to stream remote BCF CSI index", error))?;
            let bytes = collect_remote_csi_bytes(stream, MAX_BCF_CSI_INDEX_SIZE).await?;
            let mut reader = noodles_csi::io::Reader::new(Cursor::new(bytes));
            reader
                .read_index()
                .map_err(|error| execution_error("failed to parse remote BCF CSI index", error))
        }
    }
}

fn read_local_csi_index(index_path: &str) -> Result<noodles_csi::Index> {
    let path = local_path(index_path);
    let file =
        File::open(path).map_err(|error| execution_error("failed to open BCF CSI index", error))?;
    let size = file
        .metadata()
        .map_err(|error| execution_error("failed to inspect BCF CSI index", error))?
        .len();
    if size > MAX_BCF_CSI_INDEX_SIZE as u64 {
        return Err(DataFusionError::Execution(format!(
            "BCF CSI index is {size} bytes, exceeding the {MAX_BCF_CSI_INDEX_SIZE}-byte safety \
             limit"
        )));
    }
    let mut reader = noodles_csi::io::Reader::new(file);
    reader
        .read_index()
        .map_err(|error| execution_error("failed to read BCF CSI index", error))
}

async fn collect_remote_csi_bytes<S, E>(mut stream: S, max_size: usize) -> Result<Vec<u8>>
where
    S: TryStream<Ok = bytes::Bytes, Error = E> + Unpin,
    E: std::fmt::Display,
{
    let mut bytes = Vec::new();
    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|error| execution_error("failed to stream remote BCF CSI index", error))?
    {
        let next_len = bytes.len().checked_add(chunk.len()).ok_or_else(|| {
            DataFusionError::Execution("remote BCF CSI index length overflow".into())
        })?;
        if next_len > max_size {
            return Err(DataFusionError::Execution(format!(
                "remote BCF CSI index exceeds the {max_size}-byte safety limit"
            )));
        }
        bytes.try_reserve_exact(chunk.len()).map_err(|error| {
            execution_error("failed to allocate remote BCF CSI index buffer", error)
        })?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Returns the exclusive compressed end of the BGZF block starting at `start`
/// using only ranged GETs. This avoids a metadata/HEAD request for signed HTTP
/// URLs while still requesting an exact range for the indexed chunk.
async fn remote_bgzf_block_end(object: &RemoteObject, start: u64) -> Result<u64> {
    const GZIP_FIXED_HEADER_LEN: u64 = 12;
    const GZIP_TRAILER_LEN: u64 = 8;

    let fixed_end = start.checked_add(GZIP_FIXED_HEADER_LEN).ok_or_else(|| {
        DataFusionError::Execution("remote BCF BGZF header offset overflow".into())
    })?;
    let fixed = object.read_range(start..fixed_end).await.map_err(|error| {
        execution_error(
            "failed to read remote BCF BGZF block header; the CSI index does not match the file",
            error,
        )
    })?;
    if fixed.len() != GZIP_FIXED_HEADER_LEN as usize
        || fixed[0..3] != [0x1f, 0x8b, 0x08]
        || fixed[3] & 0x04 == 0
    {
        return Err(DataFusionError::Execution(format!(
            "remote BCF CSI offset {start} does not point to a BGZF block; the index does not \
             match the file"
        )));
    }

    let extra_len = u16::from_le_bytes([fixed[10], fixed[11]]) as u64;
    let extra_end = fixed_end.checked_add(extra_len).ok_or_else(|| {
        DataFusionError::Execution("remote BCF BGZF extra-header offset overflow".into())
    })?;
    let extra = object
        .read_range(fixed_end..extra_end)
        .await
        .map_err(|error| {
            execution_error(
                "failed to read remote BCF BGZF extra header; the CSI index does not match the \
                 file",
                error,
            )
        })?;

    let mut offset = 0usize;
    let mut block_size = None;
    while offset + 4 <= extra.len() {
        let subfield_len = u16::from_le_bytes([extra[offset + 2], extra[offset + 3]]) as usize;
        let payload_end = offset + 4 + subfield_len;
        if payload_end > extra.len() {
            break;
        }
        if &extra[offset..offset + 2] == b"BC" && subfield_len == 2 {
            block_size =
                Some(u16::from_le_bytes([extra[offset + 4], extra[offset + 5]]) as u64 + 1);
            break;
        }
        offset = payload_end;
    }

    let block_size = block_size.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "remote BCF BGZF block at offset {start} has no BC size subfield; the CSI index \
             does not match the file"
        ))
    })?;
    let minimum_size = GZIP_FIXED_HEADER_LEN + extra_len + GZIP_TRAILER_LEN;
    if block_size < minimum_size {
        return Err(DataFusionError::Execution(format!(
            "remote BCF BGZF block at offset {start} reports invalid size {block_size}; the CSI \
             index does not match the file"
        )));
    }
    start.checked_add(block_size).ok_or_else(|| {
        DataFusionError::Execution("remote BCF BGZF block-end offset overflow".into())
    })
}

/// Reads and parses the CSI once at planning time so that scan partitions can
/// share it instead of each re-downloading the full index.
pub(crate) async fn load_csi_index(
    index_path: &str,
    object_storage_options: Option<ObjectStorageOptions>,
) -> Result<Arc<noodles_csi::Index>> {
    read_csi_index(index_path, object_storage_options)
        .await
        .map(Arc::new)
}

/// Validates every reference-dictionary property represented by CSI.
///
/// Standard BCF CSI files do not carry reference names, so their reference
/// count is the strongest planning-time dictionary check available. If an
/// auxiliary CSI header does carry names, require exact name and order parity
/// with the BCF header as well.
pub(crate) fn validate_csi_reference_dictionary(
    index: &noodles_csi::Index,
    bcf_header: &Header,
) -> Result<()> {
    use noodles_csi::BinningIndex;

    let bcf_contigs = bcf_header.string_maps().contigs();
    let mut bcf_names = Vec::with_capacity(bcf_header.contigs().len());
    for reference_sequence_id in 0..bcf_header.contigs().len() {
        let name = bcf_contigs
            .get_index(reference_sequence_id)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "invalid BCF reference dictionary: missing contig ID {reference_sequence_id}"
                ))
            })?;
        bcf_names.push(name);
    }
    let index_reference_count = index.reference_sequences().len();
    if index_reference_count != bcf_names.len() {
        return Err(DataFusionError::Execution(format!(
            "BCF CSI reference dictionary mismatch: BCF header has {} contigs, but CSI has {}",
            bcf_names.len(),
            index_reference_count
        )));
    }

    let Some(index_header) = index.header() else {
        return Ok(());
    };
    let index_names = index_header.reference_sequence_names();
    if index_names.is_empty() {
        return Ok(());
    }

    let dictionaries_match = index_names.len() == bcf_names.len()
        && index_names
            .iter()
            .zip(bcf_names.iter())
            .all(|(index_name, bcf_name)| index_name.as_slice() == bcf_name.as_bytes());
    if !dictionaries_match {
        return Err(DataFusionError::Execution(
            "BCF CSI reference dictionary mismatch: CSI names or ordering differ from the BCF \
             header"
                .into(),
        ));
    }

    Ok(())
}

/// Returns the 1-based inclusive coordinate interval represented by a CSI bin.
///
/// CSI numbers bins breadth-first. Level 0 is the root, and level `depth`
/// contains the smallest bins whose width is `1 << min_shift` bases.
fn csi_bin_interval(bin_id: usize, min_shift: u8, depth: u8) -> Option<(u64, u64, u8)> {
    for level in 0..=depth {
        let level_shift = u32::from(level).checked_mul(3)?;
        let bin_count = 1usize.checked_shl(level_shift)?;
        let first_bin = bin_count.checked_sub(1)?.checked_div(7)?;
        let bins_end = first_bin.checked_add(bin_count)?;
        if !(first_bin..bins_end).contains(&bin_id) {
            continue;
        }

        let span_shift = u32::from(min_shift)
            .checked_add(u32::from(depth.checked_sub(level)?).checked_mul(3)?)?;
        let span = 1u64.checked_shl(span_shift)?;
        let bin_offset = u64::try_from(bin_id.checked_sub(first_bin)?).ok()?;
        let start = bin_offset.checked_mul(span)?.checked_add(1)?;
        let end = start.checked_add(span.checked_sub(1)?)?;
        return Some((start, end, level));
    }

    None
}

pub(crate) fn estimate_region_sizes(
    index: Option<&noodles_csi::Index>,
    regions: &[datafusion_bio_format_core::genomic_filter::GenomicRegion],
    contig_names: &[String],
    contig_lengths: &[u64],
) -> Vec<RegionSizeEstimate> {
    use noodles_csi::BinningIndex;

    let Some(index) = index else {
        return regions
            .iter()
            .cloned()
            .map(|region| RegionSizeEstimate {
                region,
                estimated_bytes: 1,
                contig_length: None,
                unmapped_count: 0,
                nonempty_bin_positions: Vec::new(),
                leaf_bin_span: 0,
            })
            .collect();
    };

    let name_to_index: HashMap<&str, usize> = contig_names
        .iter()
        .enumerate()
        .map(|(index, name)| (name.as_str(), index))
        .collect();

    regions
        .iter()
        .cloned()
        .map(|region| {
            let reference_index = name_to_index.get(region.chrom.as_str()).copied();
            let reference = reference_index
                .and_then(|index_value| index.reference_sequences().get(index_value));
            let declared_contig_length = reference_index
                .and_then(|index_value| contig_lengths.get(index_value))
                .copied()
                .filter(|length| *length > 0);

            // A valid VCF/BCF header may omit contig lengths. Infer a safe upper
            // coordinate bound from populated CSI bins so a single large contig
            // remains splittable. Finest-level bin starts also let the shared
            // balancer place boundaries near actual data instead of empty space.
            let min_shift = index.min_shift();
            let depth = index.depth();
            let leaf_span = 1u64.checked_shl(u32::from(min_shift)).unwrap_or(0);
            let mut inferred_contig_length = None;
            let mut nonempty_bin_positions = Vec::new();
            let mut min_offset = u64::MAX;
            let mut max_offset = 0;
            if let Some(reference) = reference {
                for (&bin_id, bin) in reference.bins() {
                    if bin.chunks().is_empty() {
                        continue;
                    }
                    let Some((start, end, level)) = csi_bin_interval(bin_id, min_shift, depth)
                    else {
                        continue;
                    };
                    inferred_contig_length =
                        Some(inferred_contig_length.map_or(end, |current: u64| current.max(end)));
                    let intersects_region =
                        region.start.is_none_or(|start_bound| end >= start_bound)
                            && region.end.is_none_or(|end_bound| start <= end_bound);
                    if !intersects_region {
                        continue;
                    }
                    for chunk in bin.chunks() {
                        min_offset = min_offset.min(chunk.start().compressed());
                        max_offset = max_offset.max(chunk.end().compressed());
                    }
                    if level == depth {
                        nonempty_bin_positions.push(start);
                    }
                }
            }
            nonempty_bin_positions.sort_unstable();
            let estimated_bytes = if min_offset == u64::MAX {
                1
            } else {
                max_offset.saturating_sub(min_offset).max(1)
            };

            RegionSizeEstimate {
                region,
                estimated_bytes,
                contig_length: declared_contig_length.or(inferred_contig_length),
                unmapped_count: 0,
                leaf_bin_span: if nonempty_bin_positions.is_empty() {
                    0
                } else {
                    leaf_span
                },
                nonempty_bin_positions,
            }
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
struct RemoteChunkSpan {
    start: noodles_bgzf::VirtualPosition,
    end: noodles_bgzf::VirtualPosition,
}

fn plan_remote_chunks(
    index: &noodles_csi::Index,
    header: &Header,
    regions: &[datafusion_bio_format_core::genomic_filter::GenomicRegion],
) -> Result<Vec<RemoteChunkSpan>> {
    use noodles_csi::BinningIndex;

    const MAX_COALESCED_BYTES: u64 = 8 * 1024 * 1024;
    const MAX_COALESCING_GAP: u64 = 64 * 1024;

    let mut chunks = Vec::new();
    for region in regions {
        let query_region = build_noodles_region(region)?;
        // A filter such as `chrom = 'chrMissing'` legitimately produces a region
        // for a contig absent from this file; that matches no rows rather than
        // being an error (the indexed text-VCF path skips these the same way).
        let Some(reference_id) = header
            .string_maps()
            .contigs()
            .get_index_of(region.chrom.as_str())
        else {
            log::debug!(
                "skipping BCF region {}: contig not present in header dictionary",
                region.chrom
            );
            continue;
        };
        chunks.extend(
            index
                .query(reference_id, query_region.interval())
                .map_err(|error| execution_error("failed to query remote BCF CSI index", error))?
                .into_iter()
                .map(|chunk| RemoteChunkSpan {
                    start: chunk.start(),
                    end: chunk.end(),
                }),
        );
    }

    chunks.sort_unstable_by_key(|chunk| chunk.start);
    let mut merged: Vec<RemoteChunkSpan> = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        if let Some(current) = merged.last_mut() {
            let overlaps = chunk.start <= current.end;
            let gap = chunk
                .start
                .compressed()
                .saturating_sub(current.end.compressed());
            let merged_bytes = chunk
                .end
                .compressed()
                .saturating_sub(current.start.compressed());
            if overlaps || (gap <= MAX_COALESCING_GAP && merged_bytes <= MAX_COALESCED_BYTES) {
                current.end = current.end.max(chunk.end);
                continue;
            }
        }
        merged.push(chunk);
    }

    Ok(merged)
}

/// Returns true when the record's start position falls inside one of `regions`.
///
/// Ownership is decided by variant start (not interval overlap) so that a record
/// spanning the boundary between two adjacent partition sub-regions is emitted by
/// exactly one partition. This mirrors the indexed VCF path, which applies the
/// same start-containment check after each region query.
fn record_starts_in_regions(
    record: &BcfRecord,
    header: &Header,
    regions: &[datafusion_bio_format_core::genomic_filter::GenomicRegion],
) -> Result<bool> {
    let chrom = VariantRecord::reference_sequence_name(record, header)
        .map_err(|error| execution_error("invalid BCF contig dictionary index", error))?;
    let start = validate_bcf_position(record)?;

    Ok(regions.iter().any(|region| {
        !region.unmapped_tail
            && region.chrom == chrom
            && u64::from(start) >= region.start.unwrap_or(1)
            && u64::from(start) <= region.end.unwrap_or(u64::MAX)
    }))
}

fn validate_bcf_position(record: &BcfRecord) -> Result<u32> {
    let position = record
        .variant_start()
        .transpose()
        .map_err(|error| execution_error("invalid BCF position", error))?
        .ok_or_else(|| DataFusionError::Execution("BCF record has no position".into()))?;
    u32::try_from(position.get())
        .map_err(|_| DataFusionError::Execution("BCF position exceeds UInt32 range".into()))
}

enum BcfDosageValues {
    Single {
        values: Vec<i8>,
        validity: NullBufferBuilder,
    },
    Multi {
        values: Vec<i8>,
        validity: NullBufferBuilder,
        offsets: Vec<i32>,
        gt_field: Field,
    },
}

fn validate_bcf_dosage_allele_count(allele_count: usize) -> Result<()> {
    if allele_count == 2 {
        Ok(())
    } else {
        Err(DataFusionError::Execution(format!(
            "BCF GT dosage supports exactly one ALT allele; record has {} ALT alleles",
            allele_count.saturating_sub(1)
        )))
    }
}

/// Direct typed sink for biallelic GT dosage.
///
/// The sink consumes the raw, already structurally validated BCF FORMAT series.
/// It deliberately avoids noodles sample iterators, dynamic FORMAT values, GT
/// strings, and per-cell heap allocations.
struct BcfDosageBuilder {
    values: BcfDosageValues,
    selected_sample_indices: Vec<usize>,
    all_samples_selected_in_order: bool,
    batch_size: usize,
}

impl BcfDosageBuilder {
    fn new(
        schema: &SchemaRef,
        batch_size: usize,
        sample_names: &[String],
        source_sample_names: &[String],
    ) -> Result<Self> {
        let selected_sample_indices =
            resolve_selected_sample_indices(sample_names, source_sample_names);
        if selected_sample_indices.is_empty() {
            return Err(DataFusionError::Plan(
                "BCF dosage requires at least one selected sample".into(),
            ));
        }
        let all_samples_selected_in_order = selected_sample_indices.len()
            == source_sample_names.len()
            && selected_sample_indices
                .iter()
                .copied()
                .eq(0..source_sample_names.len());
        let inner_capacity = batch_size
            .checked_mul(selected_sample_indices.len())
            .ok_or_else(|| DataFusionError::Plan("BCF dosage batch capacity overflow".into()))?;

        let values = if source_sample_names.len() == 1 {
            BcfDosageValues::Single {
                values: Vec::with_capacity(batch_size),
                validity: NullBufferBuilder::new(batch_size),
            }
        } else {
            let genotypes = schema.field_with_name("genotypes").map_err(|error| {
                DataFusionError::Plan(format!(
                    "BCF dosage schema is missing the genotypes field: {error}"
                ))
            })?;
            let DataType::Struct(children) = genotypes.data_type() else {
                return Err(DataFusionError::Plan(
                    "BCF dosage genotypes field must be a struct".into(),
                ));
            };
            let gt_field = children
                .iter()
                .find(|field| field.name() == "GT")
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "BCF dosage genotypes struct is missing its GT child".into(),
                    )
                })?
                .as_ref()
                .clone();
            let mut offsets = Vec::with_capacity(batch_size + 1);
            offsets.push(0);
            BcfDosageValues::Multi {
                values: Vec::with_capacity(inner_capacity),
                validity: NullBufferBuilder::new(inner_capacity),
                offsets,
                gt_field,
            }
        };

        Ok(Self {
            values,
            selected_sample_indices,
            all_samples_selected_in_order,
            batch_size,
        })
    }

    #[inline(always)]
    fn finish_dosage(dosage: usize, has_allele: bool, missing: bool) -> Result<Option<i8>> {
        if !has_allele {
            return Err(DataFusionError::Execution(
                "invalid BCF GT encoding: genotype has zero ploidy".into(),
            ));
        }
        if missing {
            return Ok(None);
        }
        let dosage = i8::try_from(dosage).map_err(|_| {
            DataFusionError::Execution(
                "BCF GT alternate dosage exceeds the signed 8-bit output range".into(),
            )
        })?;
        Ok(Some(dosage))
    }

    #[inline(always)]
    fn decode_i8(genotype: &[u8]) -> Result<Option<i8>> {
        let mut dosage = 0usize;
        let mut has_allele = false;
        let mut missing = false;
        let mut reached_vector_end = false;
        for (allele_offset, &raw) in genotype.iter().enumerate() {
            let allele = raw as i8;
            if allele == i8::MIN + 1 {
                reached_vector_end = true;
                continue;
            }
            if reached_vector_end {
                return Err(DataFusionError::Execution(format!(
                    "invalid BCF GT encoding: value after vector-end at allele offset \
                     {allele_offset}"
                )));
            }
            if allele < 0 {
                return Err(DataFusionError::Execution(format!(
                    "invalid BCF GT reserved value {allele} at allele offset {allele_offset}"
                )));
            }
            has_allele = true;
            if allele <= 1 {
                missing = true;
            } else if allele >= 4 {
                if allele > 5 {
                    return Err(DataFusionError::Execution(format!(
                        "invalid BCF GT allele index {} for biallelic dosage",
                        (allele >> 1) - 1
                    )));
                }
                dosage += 1;
            }
        }
        Self::finish_dosage(dosage, has_allele, missing)
    }

    #[inline(always)]
    fn decode_i16(genotype: &[u8]) -> Result<Option<i8>> {
        let mut dosage = 0usize;
        let mut has_allele = false;
        let mut missing = false;
        let mut reached_vector_end = false;
        for (allele_offset, raw) in genotype.chunks_exact(2).enumerate() {
            let allele = i16::from_le_bytes(raw.try_into().unwrap());
            if allele == i16::MIN + 1 {
                reached_vector_end = true;
                continue;
            }
            if reached_vector_end {
                return Err(DataFusionError::Execution(format!(
                    "invalid BCF GT encoding: value after vector-end at allele offset \
                     {allele_offset}"
                )));
            }
            if allele < 0 {
                return Err(DataFusionError::Execution(format!(
                    "invalid BCF GT reserved value {allele} at allele offset {allele_offset}"
                )));
            }
            has_allele = true;
            if allele <= 1 {
                missing = true;
            } else if allele >= 4 {
                if allele > 5 {
                    return Err(DataFusionError::Execution(format!(
                        "invalid BCF GT allele index {} for biallelic dosage",
                        (allele >> 1) - 1
                    )));
                }
                dosage += 1;
            }
        }
        Self::finish_dosage(dosage, has_allele, missing)
    }

    #[inline(always)]
    fn decode_i32(genotype: &[u8]) -> Result<Option<i8>> {
        let mut dosage = 0usize;
        let mut has_allele = false;
        let mut missing = false;
        let mut reached_vector_end = false;
        for (allele_offset, raw) in genotype.chunks_exact(4).enumerate() {
            let allele = i32::from_le_bytes(raw.try_into().unwrap());
            if allele == i32::MIN + 1 {
                reached_vector_end = true;
                continue;
            }
            if reached_vector_end {
                return Err(DataFusionError::Execution(format!(
                    "invalid BCF GT encoding: value after vector-end at allele offset \
                     {allele_offset}"
                )));
            }
            if allele < 0 {
                return Err(DataFusionError::Execution(format!(
                    "invalid BCF GT reserved value {allele} at allele offset {allele_offset}"
                )));
            }
            has_allele = true;
            if allele <= 1 {
                missing = true;
            } else if allele >= 4 {
                if allele > 5 {
                    return Err(DataFusionError::Execution(format!(
                        "invalid BCF GT allele index {} for biallelic dosage",
                        (allele >> 1) - 1
                    )));
                }
                dosage += 1;
            }
        }
        Self::finish_dosage(dosage, has_allele, missing)
    }

    #[inline]
    fn append_optional(values: &mut Vec<i8>, validity: &mut NullBufferBuilder, value: Option<i8>) {
        match value {
            Some(value) => {
                values.push(value);
                validity.append_non_null();
            }
            None => {
                values.push(0);
                validity.append_null();
            }
        }
    }

    /// Decodes the overwhelmingly common fixed-diploid Int8 representation in
    /// bulk. A separate validation pass makes the materialization loop
    /// branch-free and gives LLVM room to vectorize it. Records containing a
    /// missing allele, variable ploidy, or malformed value use the fully
    /// checked fallback below.
    #[inline]
    fn append_all_diploid_i8(
        values: &mut Vec<i8>,
        validity: &mut NullBufferBuilder,
        payload: &[u8],
    ) -> Result<()> {
        debug_assert_eq!(payload.len() % 2, 0);
        let sample_count = payload.len() / 2;

        if payload.iter().all(|&raw| (2..=5).contains(&raw)) {
            values.reserve(sample_count);
            for genotype in payload.chunks_exact(2) {
                values.push(((genotype[0] >> 2) + (genotype[1] >> 2)) as i8);
            }
            validity.append_n_non_nulls(sample_count);
            return Ok(());
        }

        // Buffer validity in runs so the lazy null builder is touched once per
        // run rather than once for every one of billions of genotype cells.
        let mut non_null_run = 0;
        for genotype in payload.chunks_exact(2) {
            match Self::decode_i8(genotype)? {
                Some(dosage) => {
                    values.push(dosage);
                    non_null_run += 1;
                }
                None => {
                    values.push(0);
                    validity.append_n_non_nulls(non_null_run);
                    validity.append_null();
                    non_null_run = 0;
                }
            }
        }
        validity.append_n_non_nulls(non_null_run);
        Ok(())
    }

    #[inline]
    fn gt_sample_payload(
        payload: &[u8],
        sample_index: usize,
        sample_width: usize,
    ) -> Result<&[u8]> {
        let start = sample_index
            .checked_mul(sample_width)
            .ok_or_else(|| DataFusionError::Execution("BCF GT sample offset overflow".into()))?;
        let end = start
            .checked_add(sample_width)
            .ok_or_else(|| DataFusionError::Execution("BCF GT sample offset overflow".into()))?;
        payload
            .get(start..end)
            .ok_or_else(|| DataFusionError::Execution("BCF GT sample payload is truncated".into()))
    }

    fn append_gt(&mut self, gt: Option<BcfGtEncoding<'_>>, allele_count: usize) -> Result<()> {
        debug_assert_eq!(allele_count, 2, "dosage allele count is prevalidated");

        match (&mut self.values, gt) {
            (BcfDosageValues::Single { values, validity }, Some(gt)) => {
                let sample_width = gt
                    .encoded_type
                    .width()
                    .checked_mul(gt.value_count)
                    .ok_or_else(|| {
                        DataFusionError::Execution("BCF GT sample width overflow".into())
                    })?;
                let sample_index = self.selected_sample_indices[0];
                let sample = Self::gt_sample_payload(gt.payload, sample_index, sample_width)?;
                let dosage = match gt.encoded_type {
                    BcfEncodedType::Int8 => Self::decode_i8(sample)?,
                    BcfEncodedType::Int16 => Self::decode_i16(sample)?,
                    BcfEncodedType::Int32 => Self::decode_i32(sample)?,
                    _ => unreachable!("GT encoding was validated before dosage decode"),
                };
                Self::append_optional(values, validity, dosage);
            }
            (BcfDosageValues::Single { values, validity }, None) => {
                Self::append_optional(values, validity, None)
            }
            (
                BcfDosageValues::Multi {
                    values,
                    validity,
                    offsets,
                    ..
                },
                Some(gt),
            ) => {
                let sample_width = gt
                    .encoded_type
                    .width()
                    .checked_mul(gt.value_count)
                    .ok_or_else(|| {
                        DataFusionError::Execution("BCF GT sample width overflow".into())
                    })?;
                macro_rules! append_sample {
                    ($sample:expr, $decoder:ident) => {{
                        Self::append_optional(values, validity, Self::$decoder($sample)?);
                    }};
                }
                macro_rules! append_indexed_sample {
                    ($sample_index:expr, $decoder:ident) => {{
                        let sample =
                            Self::gt_sample_payload(gt.payload, $sample_index, sample_width)?;
                        append_sample!(sample, $decoder);
                    }};
                }
                match gt.encoded_type {
                    BcfEncodedType::Int8 => {
                        if self.all_samples_selected_in_order && gt.value_count == 2 {
                            Self::append_all_diploid_i8(values, validity, gt.payload)?;
                        } else if self.all_samples_selected_in_order {
                            for sample in gt.payload.chunks_exact(sample_width) {
                                append_sample!(sample, decode_i8);
                            }
                        } else {
                            for &sample_index in &self.selected_sample_indices {
                                append_indexed_sample!(sample_index, decode_i8);
                            }
                        }
                    }
                    BcfEncodedType::Int16 => {
                        if self.all_samples_selected_in_order {
                            for sample in gt.payload.chunks_exact(sample_width) {
                                append_sample!(sample, decode_i16);
                            }
                        } else {
                            for &sample_index in &self.selected_sample_indices {
                                append_indexed_sample!(sample_index, decode_i16);
                            }
                        }
                    }
                    BcfEncodedType::Int32 => {
                        if self.all_samples_selected_in_order {
                            for sample in gt.payload.chunks_exact(sample_width) {
                                append_sample!(sample, decode_i32);
                            }
                        } else {
                            for &sample_index in &self.selected_sample_indices {
                                append_indexed_sample!(sample_index, decode_i32);
                            }
                        }
                    }
                    _ => unreachable!("GT encoding was validated before dosage decode"),
                }
                offsets.push(i32::try_from(values.len()).map_err(|_| {
                    DataFusionError::Execution(
                        "BCF dosage batch exceeds the Arrow List offset range".into(),
                    )
                })?);
            }
            (
                BcfDosageValues::Multi {
                    values,
                    validity,
                    offsets,
                    ..
                },
                None,
            ) => {
                let sample_count = self.selected_sample_indices.len();
                values.resize(values.len() + sample_count, 0);
                validity.append_n_nulls(sample_count);
                offsets.push(i32::try_from(values.len()).map_err(|_| {
                    DataFusionError::Execution(
                        "BCF dosage batch exceeds the Arrow List offset range".into(),
                    )
                })?);
            }
        }
        Ok(())
    }

    fn finish_arrays(&mut self) -> Result<Vec<Arc<dyn Array>>> {
        match &mut self.values {
            BcfDosageValues::Single { values, validity } => {
                let values = std::mem::replace(values, Vec::with_capacity(self.batch_size));
                let array =
                    Arc::new(Int8Array::new(values.into(), validity.finish())) as Arc<dyn Array>;
                Ok(vec![array])
            }
            BcfDosageValues::Multi {
                values,
                validity,
                offsets,
                gt_field,
            } => {
                let DataType::List(item_field) = gt_field.data_type() else {
                    return Err(DataFusionError::Execution(
                        "BCF dosage GT field must be an Arrow List".into(),
                    ));
                };
                let inner_capacity = self
                    .batch_size
                    .checked_mul(self.selected_sample_indices.len())
                    .ok_or_else(|| {
                        DataFusionError::Execution("BCF dosage batch capacity overflow".into())
                    })?;
                let values = std::mem::replace(values, Vec::with_capacity(inner_capacity));
                let child =
                    Arc::new(Int8Array::new(values.into(), validity.finish())) as Arc<dyn Array>;
                let finished_offsets =
                    std::mem::replace(offsets, Vec::with_capacity(self.batch_size + 1));
                offsets.push(0);
                let list = Arc::new(ListArray::new(
                    Arc::clone(item_field),
                    OffsetBuffer::new(finished_offsets.into()),
                    child,
                    None,
                )) as Arc<dyn Array>;
                let array =
                    StructArray::try_new(vec![Arc::new(gt_field.clone())].into(), vec![list], None)
                        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
                Ok(vec![Arc::new(array)])
            }
        }
    }
}

enum BcfFormatMode {
    Generic(FormatMode),
    Dosage(BcfDosageBuilder),
}

impl BcfFormatMode {
    fn is_direct_dosage(&self) -> bool {
        matches!(self, Self::Dosage(_))
    }

    fn fuses_complete_gt_validation(&self) -> bool {
        matches!(
            self,
            Self::Dosage(builder) if builder.all_samples_selected_in_order
        )
    }

    fn has_fields(&self) -> bool {
        match self {
            Self::Generic(mode) => mode.has_fields(),
            Self::Dosage(_) => true,
        }
    }

    fn append_record(
        &mut self,
        record: &BcfRecord,
        header: &Header,
        gt: Option<BcfGtEncoding<'_>>,
        allele_count: usize,
    ) -> Result<()> {
        match self {
            Self::Generic(mode) => mode
                .append_record(record, header)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None)),
            Self::Dosage(builder) => builder.append_gt(gt, allele_count),
        }
    }

    fn finish_arrays(&mut self) -> Result<Vec<Arc<dyn Array>>> {
        match self {
            Self::Generic(mode) => mode
                .finish_arrays()
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None)),
            Self::Dosage(builder) => builder.finish_arrays(),
        }
    }
}

enum BcfInfoFilterValue {
    String(String),
    F64(f64),
}

struct BcfRecordFilterFields {
    chrom: Option<String>,
    start: Option<u32>,
    end: Option<u32>,
    id: Option<String>,
    reference: Option<String>,
    alternate: Option<String>,
    quality: Option<Option<f64>>,
    filter: Option<String>,
    info_values: HashMap<String, BcfInfoFilterValue>,
    null_info_fields: HashSet<String>,
}

struct BcfCoreFilterValues<'a> {
    chrom: &'a str,
    start: u32,
    end: u32,
    ids: &'a str,
    reference_bases: &'a str,
    quality_score: Option<f64>,
}

impl RecordFieldAccessor for BcfRecordFilterFields {
    fn is_null_field(&self, name: &str) -> bool {
        (name == "qual" && matches!(self.quality, Some(None)))
            || self.null_info_fields.contains(name)
    }

    fn get_string_field(&self, name: &str) -> Option<String> {
        match name {
            "chrom" => self.chrom.clone(),
            "id" => self.id.clone(),
            "ref" => self.reference.clone(),
            "alt" => self.alternate.clone(),
            "filter" => self.filter.clone(),
            _ => match self.info_values.get(name) {
                Some(BcfInfoFilterValue::String(value)) => Some(value.clone()),
                _ => None,
            },
        }
    }

    fn get_u32_field(&self, name: &str) -> Option<u32> {
        match name {
            "start" => self.start,
            "end" => self.end,
            _ => None,
        }
    }

    fn get_f32_field(&self, _name: &str) -> Option<f32> {
        None
    }

    fn get_f64_field(&self, name: &str) -> Option<f64> {
        if name == "qual" {
            return self.quality.flatten();
        }
        match self.info_values.get(name) {
            Some(BcfInfoFilterValue::F64(value)) => Some(*value),
            _ => None,
        }
    }
}

fn bcf_record_filter_fields(
    record: &BcfRecord,
    header: &Header,
    filter_columns: &HashSet<String>,
    core: BcfCoreFilterValues<'_>,
) -> Result<BcfRecordFilterFields> {
    let mut alternate = None;
    if filter_columns.contains("alt") {
        let mut value = String::new();
        for result in record.alternate_bases().iter() {
            if !value.is_empty() {
                value.push('|');
            }
            value.push_str(
                result.map_err(|error| execution_error("invalid BCF alternate allele", error))?,
            );
        }
        alternate = Some(value);
    }

    let mut filter = None;
    if filter_columns.contains("filter") {
        let mut value = String::new();
        for result in record.filters().iter(header) {
            if !value.is_empty() {
                value.push(';');
            }
            value.push_str(
                result.map_err(|error| {
                    execution_error("invalid BCF filter dictionary index", error)
                })?,
            );
        }
        filter = Some(value);
    }

    let mut info_values = HashMap::new();
    let mut null_info_fields = filter_columns
        .iter()
        .filter(|column| header.infos().contains_key(column.as_str()))
        .cloned()
        .collect::<HashSet<_>>();

    if !null_info_fields.is_empty() {
        // Parse INFO once per record regardless of how many scalar INFO
        // predicates are present. Absent and explicitly missing fields remain
        // in `null_info_fields` and therefore follow SQL WHERE null semantics.
        for result in record.info().iter(header) {
            let (info_name, info_value) = match result {
                Ok(field) => field,
                Err(error) if is_missing_info_value_error(&error) => continue,
                Err(error) => {
                    return Err(execution_error(
                        "invalid BCF INFO field during filter evaluation",
                        error,
                    ));
                }
            };
            if !null_info_fields.contains(info_name) {
                continue;
            }
            let Some(info_value) = info_value else {
                continue;
            };
            let value = match info_value {
                InfoValue::Integer(value) => BcfInfoFilterValue::F64(f64::from(value)),
                InfoValue::Float(value) => BcfInfoFilterValue::F64(f64::from(value)),
                InfoValue::Character(value) => BcfInfoFilterValue::String(value.to_string()),
                InfoValue::String(value) => BcfInfoFilterValue::String(value.into_owned()),
                InfoValue::Flag | InfoValue::Array(_) => {
                    // Boolean and list-valued INFO fields are not admitted by
                    // `can_push_down_record_filter`.
                    continue;
                }
            };
            null_info_fields.remove(info_name);
            info_values.insert(info_name.to_string(), value);
        }
    }

    Ok(BcfRecordFilterFields {
        chrom: filter_columns
            .contains("chrom")
            .then(|| core.chrom.to_string()),
        start: filter_columns.contains("start").then_some(core.start),
        end: filter_columns.contains("end").then_some(core.end),
        id: filter_columns.contains("id").then(|| core.ids.to_string()),
        reference: filter_columns
            .contains("ref")
            .then(|| core.reference_bases.to_string()),
        alternate,
        quality: filter_columns
            .contains("qual")
            .then_some(core.quality_score),
        filter,
        info_values,
        null_info_fields,
    })
}

struct BcfBatchDecoder {
    schema: SchemaRef,
    requested_batch_size: usize,
    effective_batch_size: usize,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    filter_columns: HashSet<String>,
    source_sample_names: Vec<String>,
    flags: ProjectionFlags,
    core_builders: CoreBatchBuilders,
    info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>),
    info_name_to_index: HashMap<String, usize>,
    info_populated: Vec<bool>,
    format_mode: BcfFormatMode,
    genotype_output_mode: GenotypeOutputMode,
    has_format_fields: bool,
    batch_row_count: usize,
    join_buf: String,
}

impl BcfBatchDecoder {
    #[allow(clippy::too_many_arguments)]
    fn new(
        header: &Header,
        schema: SchemaRef,
        batch_size: usize,
        info_fields: Option<Vec<String>>,
        format_fields: Option<Vec<String>>,
        sample_names: &[String],
        source_sample_names: Vec<String>,
        projection: Option<Vec<usize>>,
        coordinate_system_zero_based: bool,
        residual_filters: Vec<Expr>,
        genotype_output_mode: GenotypeOutputMode,
    ) -> Result<Self> {
        // First pass exists only to learn the INFO field count for
        // `ProjectionFlags`; the builders are rebuilt below once the
        // batch-size heuristics have settled on `initial_builder_batch_size`.
        let mut info_builders = (Vec::new(), Vec::new(), Vec::new());
        set_info_builders(
            batch_size,
            info_fields.clone(),
            header.infos(),
            &mut info_builders,
        );
        let flags = ProjectionFlags::new(&projection, info_builders.0.len());
        let effective_batch_size =
            if genotype_output_mode == GenotypeOutputMode::Dosage && flags.any_format {
                let selected_sample_count =
                    resolve_selected_sample_indices(sample_names, &source_sample_names).len();
                choose_dosage_effective_batch_size(batch_size, selected_sample_count)
            } else {
                choose_effective_batch_size(
                    batch_size,
                    flags.any_format,
                    &format_fields,
                    sample_names,
                    &source_sample_names,
                    header.formats(),
                )
            };
        let initial_builder_batch_size =
            if genotype_output_mode == GenotypeOutputMode::Dosage && flags.any_format {
                effective_batch_size
            } else {
                choose_initial_builder_batch_size(
                    effective_batch_size,
                    flags.any_format,
                    &source_sample_names,
                )
            };

        info_builders = (Vec::new(), Vec::new(), Vec::new());
        set_info_builders(
            initial_builder_batch_size,
            info_fields,
            header.infos(),
            &mut info_builders,
        );
        let info_name_to_index = info_builders
            .0
            .iter()
            .enumerate()
            .map(|(index, name)| (name.clone(), index))
            .collect();
        let info_populated = vec![false; info_builders.0.len()];
        let mut filter_columns = HashSet::new();
        for filter in &residual_filters {
            for column in filter.column_refs() {
                filter_columns.insert(column.name.clone());
            }
        }

        let format_mode = if !flags.any_format {
            BcfFormatMode::Generic(FormatMode::None)
        } else if genotype_output_mode == GenotypeOutputMode::Dosage {
            BcfFormatMode::Dosage(BcfDosageBuilder::new(
                &schema,
                initial_builder_batch_size,
                sample_names,
                &source_sample_names,
            )?)
        } else {
            BcfFormatMode::Generic(
                init_format_mode(
                    initial_builder_batch_size,
                    format_fields,
                    sample_names,
                    &source_sample_names,
                    header.formats(),
                )
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            )
        };
        let has_format_fields = format_mode.has_fields();
        let core_builders = CoreBatchBuilders::new(&flags, initial_builder_batch_size);

        Ok(Self {
            schema,
            requested_batch_size: batch_size,
            effective_batch_size,
            projection,
            coordinate_system_zero_based,
            residual_filters,
            filter_columns,
            source_sample_names,
            flags,
            core_builders,
            info_builders,
            info_name_to_index,
            info_populated,
            format_mode,
            genotype_output_mode,
            has_format_fields,
            batch_row_count: 0,
            join_buf: String::with_capacity(64),
        })
    }

    fn append_record(
        &mut self,
        record: &BcfRecord,
        header: &Header,
    ) -> Result<Option<RecordBatch>> {
        let samples = record
            .samples()
            .map_err(|e| execution_error("invalid BCF sample count", e))?;
        let record_sample_count = samples.len();
        if record_sample_count != self.source_sample_names.len() {
            return Err(DataFusionError::Execution(format!(
                "BCF record sample count {record_sample_count} does not match header sample count {}",
                self.source_sample_names.len()
            )));
        }
        let reference_sequence_name = VariantRecord::reference_sequence_name(record, header)
            .map_err(|e| execution_error("invalid BCF contig dictionary index in record", e))?;
        let record_ids = record.ids();
        let ids = std::str::from_utf8(record_ids.as_ref())
            .map_err(|e| execution_error("invalid BCF ID", e))?;
        let record_reference_bases = record.reference_bases();
        let reference_bases = std::str::from_utf8(record_reference_bases.as_ref())
            .map_err(|e| execution_error("invalid BCF reference allele", e))?;
        if reference_bases.is_empty() {
            return Err(DataFusionError::Execution(
                "invalid BCF reference allele: expected a nonempty value".into(),
            ));
        }
        let alternate_bases = record.alternate_bases();
        for result in alternate_bases.iter() {
            let alternate_base =
                result.map_err(|e| execution_error("invalid BCF alternate allele", e))?;
            if alternate_base.is_empty() {
                return Err(DataFusionError::Execution(
                    "invalid BCF alternate allele: expected a nonempty value".into(),
                ));
            }
        }
        let allele_count = alternate_bases.len() + 1;
        let quality_score = record
            .quality_score()
            .map_err(|e| execution_error("invalid BCF quality score", e))?
            .map(f64::from);
        let mut saw_pass_filter = false;
        let mut saw_failing_filter = false;
        for result in record.filters().iter(header) {
            let filter = result
                .map_err(|e| execution_error("invalid BCF FILTER dictionary index in record", e))?;
            if filter == "PASS" {
                saw_pass_filter = true;
            } else {
                saw_failing_filter = true;
            }
            if saw_pass_filter && saw_failing_filter {
                return Err(DataFusionError::Execution(
                    "BCF FILTER contains PASS together with a failing filter".into(),
                ));
            }
            if filter != "PASS" && !header.filters().contains_key(filter) {
                return Err(DataFusionError::Execution(format!(
                    "BCF FILTER dictionary entry '{filter}' has no FILTER header definition"
                )));
            }
        }
        validate_bcf_info_encoding(&record.info(), header, allele_count)?;
        let direct_dosage = self.format_mode.is_direct_dosage();
        // The direct sink validates every cell while decoding only when all
        // source samples are selected in source order. A subset scan still
        // validates the complete untrusted GT payload before materializing the
        // requested cells, so integrity never depends on sample projection.
        let validate_gt_values = !self.format_mode.fuses_complete_gt_validation();
        let format_validation =
            validate_bcf_format_encoding(&samples, header, allele_count, validate_gt_values)?;
        let gt_encoding = format_validation.gt_encoding;

        let has_filters = !self.residual_filters.is_empty();

        let position = validate_bcf_position(record)?;
        // BCF stores the record span independently from the projected columns.
        // Validate it for every record so an empty projection (e.g. COUNT(*))
        // cannot accept a record that projecting `end` would reject.
        record
            .end()
            .map_err(|e| execution_error("invalid BCF record span", e))?;
        let output_start = if self.coordinate_system_zero_based {
            position - 1
        } else {
            position
        };
        let start = self.flags.start.then_some(output_start);

        let chrom = if self.flags.chrom {
            Some(reference_sequence_name.to_string())
        } else {
            None
        };

        // INFO/END participates in the logical span and must be valid even for
        // metadata-only projections such as COUNT(*).
        let variant_end = record
            .variant_end(header)
            .map_err(|e| execution_error("invalid BCF variant span", e))?;
        let variant_end = u32::try_from(variant_end.get()).map_err(|_| {
            DataFusionError::Execution("BCF end position exceeds UInt32 range".into())
        })?;
        let end = self.flags.end.then_some(variant_end);

        if has_filters {
            let fields = bcf_record_filter_fields(
                record,
                header,
                &self.filter_columns,
                BcfCoreFilterValues {
                    chrom: reference_sequence_name,
                    start: output_start,
                    end: variant_end,
                    ids,
                    reference_bases,
                    quality_score,
                },
            )?;
            if !evaluate_record_filters(&fields, &self.residual_filters) {
                if direct_dosage
                    && !format_validation.gt_values_validated
                    && let Some(gt) = gt_encoding.as_ref()
                {
                    // Direct materialization performs GT validation for accepted
                    // records. A filtered record without a GT-dependent FORMAT
                    // field still needs the same integrity checks even though no
                    // Arrow value is constructed. Number=G/P validation already
                    // validated the complete GT payload in the first pass.
                    validate_bcf_gt_payload(
                        gt.payload,
                        gt.encoded_type,
                        gt.value_count,
                        allele_count,
                    )?;
                }
                return Ok(None);
            }
        }

        if self.genotype_output_mode == GenotypeOutputMode::Dosage {
            // Dosage is a biallelic table contract, not merely a property of a
            // projected column. Enforce it for every selected record, including
            // metadata-only and COUNT(*) scans, but do not reject an unrelated
            // multiallelic record that provider-owned predicates filtered out.
            validate_bcf_dosage_allele_count(allele_count)?;
        }

        if self.flags.chrom {
            self.core_builders
                .append_chrom(chrom.as_deref().expect("chrom was requested"));
        }
        if self.flags.start {
            self.core_builders
                .append_start(start.expect("start was requested"));
        }
        if self.flags.end {
            self.core_builders
                .append_end(end.expect("end was requested"));
        }
        if self.flags.id {
            self.core_builders.append_id(ids);
        }
        if self.flags.reference {
            self.core_builders.append_ref(reference_bases);
        }
        if self.flags.alt {
            self.join_buf.clear();
            let mut first = true;
            for result in record.alternate_bases().iter() {
                if !first {
                    self.join_buf.push('|');
                }
                first = false;
                self.join_buf.push_str(
                    result.map_err(|e| execution_error("invalid BCF alternate allele", e))?,
                );
            }
            self.core_builders.append_alt(&self.join_buf);
        }
        if self.flags.qual {
            self.core_builders.append_qual(quality_score);
        }
        if self.flags.filter {
            self.join_buf.clear();
            let mut first = true;
            for result in record.filters().iter(header) {
                if !first {
                    self.join_buf.push(';');
                }
                first = false;
                self.join_buf.push_str(
                    result
                        .map_err(|e| execution_error("invalid BCF filter dictionary index", e))?,
                );
            }
            self.core_builders.append_filter(&self.join_buf);
        }
        if self.flags.any_info {
            load_infos_single_pass(
                record,
                header,
                &self.info_builders.1,
                &mut self.info_builders.2,
                &self.info_name_to_index,
                &mut self.info_populated,
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        }
        if self.has_format_fields && self.flags.any_format {
            self.format_mode
                .append_record(record, header, gt_encoding, allele_count)?;
        }

        self.batch_row_count += 1;
        if self.batch_row_count == self.effective_batch_size {
            self.finish_batch().map(Some)
        } else {
            Ok(None)
        }
    }

    fn finish_batch(&mut self) -> Result<RecordBatch> {
        let info_arrays = if self.flags.any_info {
            Some(builders_to_arrays(&mut self.info_builders.2))
        } else {
            None
        };
        let format_arrays = if self.has_format_fields && self.flags.any_format {
            Some(self.format_mode.finish_arrays()?)
        } else {
            None
        };

        if self.genotype_output_mode != GenotypeOutputMode::Dosage {
            self.effective_batch_size = adjust_effective_batch_size_by_observed_format_bytes(
                self.requested_batch_size,
                self.effective_batch_size,
                self.flags.any_format,
                &self.source_sample_names,
                self.batch_row_count,
                format_arrays.as_ref(),
            );
        }
        let row_count = self.batch_row_count;
        self.batch_row_count = 0;

        build_record_batch_from_builders(
            self.schema.clone(),
            self.core_builders.finish(),
            info_arrays.as_ref(),
            format_arrays.as_ref(),
            self.info_builders.0.len(),
            &self.projection,
            row_count,
        )
    }

    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.batch_row_count == 0 {
            Ok(None)
        } else {
            self.finish_batch().map(Some)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn full_local_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    source_sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    genotype_output_mode: GenotypeOutputMode,
    limit: Option<usize>,
) -> SendableRecordBatchStream {
    let output_schema = schema.clone();
    let stream = try_stream! {
        let file = File::open(local_path(&file_path))
            .map_err(|e| execution_error("failed to open BCF", e))?;
        let mut reader = bcf::io::Reader::new(file);
        let header = read_bcf_header_bounded(reader.get_mut())?;
        let mut decoder = BcfBatchDecoder::new(
            &header,
            schema,
            batch_size,
            info_fields,
            format_fields,
            &sample_names,
            source_sample_names,
            projection,
            coordinate_system_zero_based,
            residual_filters,
            genotype_output_mode,
        )?;
        let mut record = BcfRecord::default();
        let mut emitted = 0usize;

        loop {
            let record_size = read_bcf_record_bounded(&mut reader, &mut record)
                .map_err(|e| execution_error("failed to decode BCF record", e))?;
            if record_size == 0 {
                break;
            }

            if let Some(batch) = decoder.append_record(&record, &header)? {
                emitted += batch.num_rows();
                yield batch;
            }

            let accepted = emitted + decoder.batch_row_count;
            if limit.is_some_and(|value| accepted >= value) {
                break;
            }
        }

        if let Some(batch) = decoder.finish()? {
            yield batch;
        }
    };

    Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
}

#[allow(clippy::too_many_arguments)]
async fn full_remote_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    source_sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    genotype_output_mode: GenotypeOutputMode,
    limit: Option<usize>,
) -> Result<SendableRecordBatchStream> {
    let output_schema = schema.clone();
    // The whole object is read here, so the chunked concurrent reader stays the
    // default; it falls back to one sequential request only when the backend
    // refuses the size preflight the chunking needs, as a pre-signed URL scoped
    // to GET and range requests does.
    let mut inner =
        get_remote_stream_bgzf_head_tolerant(file_path, object_storage_options.unwrap_or_default())
            .await
            .map_err(|e| execution_error("failed to open remote BCF", e))?;
    let header = read_bcf_header_bounded_async(&mut inner).await?;
    let mut reader = bcf::r#async::io::Reader::from(inner);

    let stream = try_stream! {
        let mut decoder = BcfBatchDecoder::new(
            &header,
            schema,
            batch_size,
            info_fields,
            format_fields,
            &sample_names,
            source_sample_names,
            projection,
            coordinate_system_zero_based,
            residual_filters,
            genotype_output_mode,
        )?;
        let mut record = BcfRecord::default();
        let mut emitted = 0usize;

        loop {
            let record_size = read_bcf_record_bounded_async(&mut reader, &mut record)
                .await
                .map_err(|e| execution_error("failed to decode remote BCF record", e))?;
            if record_size == 0 {
                break;
            }

            if let Some(batch) = decoder.append_record(&record, &header)? {
                emitted += batch.num_rows();
                yield batch;
            }

            let accepted = emitted + decoder.batch_row_count;
            if limit.is_some_and(|value| accepted >= value) {
                break;
            }
        }

        if let Some(batch) = decoder.finish()? {
            yield batch;
        }
    };

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        stream,
    )))
}

#[allow(clippy::too_many_arguments)]
fn indexed_local_stream(
    file_path: String,
    index_path: String,
    shared_index: Option<Arc<noodles_csi::Index>>,
    regions: Vec<datafusion_bio_format_core::genomic_filter::GenomicRegion>,
    schema: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    source_sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    genotype_output_mode: GenotypeOutputMode,
    limit: Option<usize>,
) -> SendableRecordBatchStream {
    let output_schema = schema.clone();
    let stream = try_stream! {
        let file = File::open(local_path(&file_path))
            .map_err(|e| execution_error("failed to open indexed BCF", e))?;
        let mut reader = bcf::io::Reader::new(file);
        let header = read_bcf_header_bounded(reader.get_mut())?;
        let index = match shared_index {
            Some(index) => index,
            None => Arc::new(read_local_csi_index(&index_path)?),
        };
        validate_csi_reference_dictionary(index.as_ref(), &header)?;
        let mut decoder = BcfBatchDecoder::new(
            &header,
            schema,
            batch_size,
            info_fields,
            format_fields,
            &sample_names,
            source_sample_names,
            projection,
            coordinate_system_zero_based,
            residual_filters,
            genotype_output_mode,
        )?;
        let mut emitted = 0usize;

        'regions: for region in regions {
            // A filter on a contig absent from this file matches no rows; skip it
            // instead of letting the query fail (the indexed text-VCF path skips
            // unknown contigs the same way).
            let Some(reference_sequence_id) = header
                .string_maps()
                .contigs()
                .get_index_of(region.chrom.as_str())
            else {
                log::debug!(
                    "skipping BCF region {}: contig not present in header dictionary",
                    region.chrom
                );
                continue;
            };
            let noodles_region = build_noodles_region(&region)?;
            let chunks = noodles_csi::BinningIndex::query(
                index.as_ref(),
                reference_sequence_id,
                noodles_region.interval(),
            )
                .map_err(|e| execution_error("failed to query BCF CSI index", e))?;
            // Build the CSI byte-range reader explicitly so record lengths are
            // checked before the BCF decoder allocates its site/sample buffers.
            let query = noodles_csi::io::Query::new(reader.get_mut(), chunks);
            let mut query_reader = bcf::io::Reader::from(query);
            let mut record = BcfRecord::default();

            loop {
                let record_size = read_bcf_record_bounded(&mut query_reader, &mut record)
                    .map_err(|e| execution_error("failed to decode indexed BCF record", e))?;
                if record_size == 0 {
                    break;
                }
                // The CSI query matches by interval overlap; keep only records that
                // start inside this partition's sub-region so records spanning a
                // partition boundary are not emitted twice.
                if !record_starts_in_regions(&record, &header, std::slice::from_ref(&region))? {
                    continue;
                }
                if let Some(batch) = decoder.append_record(&record, &header)? {
                    emitted += batch.num_rows();
                    yield batch;
                }

                let accepted = emitted + decoder.batch_row_count;
                if limit.is_some_and(|value| accepted >= value) {
                    break 'regions;
                }
            }
        }

        if let Some(batch) = decoder.finish()? {
            yield batch;
        }
    };

    Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
}

#[allow(clippy::too_many_arguments)]
async fn indexed_remote_stream(
    file_path: String,
    index_path: String,
    shared_index: Option<Arc<noodles_csi::Index>>,
    shared_header: Option<Arc<Header>>,
    regions: Vec<datafusion_bio_format_core::genomic_filter::GenomicRegion>,
    schema: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    source_sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    genotype_output_mode: GenotypeOutputMode,
    limit: Option<usize>,
) -> Result<SendableRecordBatchStream> {
    let options = object_storage_options.unwrap_or_default();
    // Reuse the header parsed at provider construction; re-reading it here
    // would download and parse it once per partition.
    let header = match shared_header {
        Some(header) => header,
        None => Arc::new(read_header(&file_path, Some(options.clone())).await?),
    };
    // Reuse the CSI parsed at planning time; each partition re-downloading the
    // full index would transfer it N+1 times for an N-partition scan.
    let index = match shared_index {
        Some(index) => index,
        None => Arc::new(read_csi_index(&index_path, Some(options.clone())).await?),
    };
    validate_csi_reference_dictionary(index.as_ref(), &header)?;
    let chunks = plan_remote_chunks(index.as_ref(), &header, &regions)?;
    let object = RemoteObject::open(file_path, options)
        .await
        .map_err(|error| execution_error("failed to open indexed remote BCF", error))?;
    let output_schema = schema.clone();

    let stream = try_stream! {
        let mut decoder = BcfBatchDecoder::new(
            &header,
            schema,
            batch_size,
            info_fields,
            format_fields,
            &sample_names,
            source_sample_names,
            projection,
            coordinate_system_zero_based,
            residual_filters,
            genotype_output_mode,
        )?;
        let mut emitted = 0usize;

        'chunks: for chunk in chunks {
            let compressed_start = chunk.start.compressed();
            // Resolve the exact end of a partial BGZF block with range GETs.
            // Do not stat/HEAD the object first: signed HTTP URLs are often
            // authorized for GET (including ranges) only.
            let compressed_end = if chunk.end.uncompressed() == 0 {
                chunk.end.compressed()
            } else {
                remote_bgzf_block_end(&object, chunk.end.compressed()).await?
            };
            if compressed_end <= compressed_start {
                continue;
            }

            let remote_stream = object
                .stream_range_bounded(
                    compressed_start..compressed_end,
                    MAX_REMOTE_BCF_STREAM_CHUNK_SIZE,
                )
                .await
                .map_err(|error| {
                    let context = if error.kind() == opendal::ErrorKind::Unexpected {
                        "failed to stream the complete remote BCF CSI range; the index does not \
                         match the file"
                    } else {
                        "failed to stream remote BCF CSI range"
                    };
                    execution_error(context, error)
                })?;
            // OpenDAL splits this explicit range at the hard ceiling (or a
            // smaller configured object-store chunk size). Stream those chunks
            // directly through BGZF instead of materializing an arbitrarily
            // large CSI span in one Bytes allocation.
            let inner = StreamReader::new(remote_stream);
            let mut bgzf_reader = noodles_bgzf::r#async::io::Reader::new(inner);
            let local_start =
                noodles_bgzf::VirtualPosition::new(0, chunk.start.uncompressed())
                    .expect("zero compressed offset is valid");
            let local_end = noodles_bgzf::VirtualPosition::new(
                chunk.end.compressed().saturating_sub(compressed_start),
                chunk.end.uncompressed(),
            )
            .ok_or_else(|| {
                DataFusionError::Execution("remote BCF CSI virtual offset overflow".into())
            })?;

            let mut prefix = tokio::io::AsyncReadExt::take(
                &mut bgzf_reader,
                u64::from(chunk.start.uncompressed()),
            );
            let skipped = tokio::io::copy(&mut prefix, &mut tokio::io::sink())
                .await
                .map_err(|error| {
                    execution_error(
                        "failed to seek streamed remote BCF CSI range; the index does not match \
                         the file",
                        error,
                    )
                })?;
            if skipped != u64::from(chunk.start.uncompressed())
                || bgzf_reader.virtual_position() != local_start
            {
                Err(DataFusionError::Execution(
                    "streamed remote BCF CSI range ended before its virtual start; the index does \
                     not match the file"
                        .into(),
                ))?;
            }

            let mut reader = bcf::r#async::io::Reader::from(bgzf_reader);
            let mut record = BcfRecord::default();

            while reader.get_ref().virtual_position() < local_end {
                let record_size = read_bcf_record_bounded_async(&mut reader, &mut record)
                    .await
                    .map_err(|error| {
                        execution_error(
                            "failed to decode streamed remote indexed BCF record; the index does \
                             not match the file",
                            error,
                        )
                    })?;
                if record_size == 0 {
                    break;
                }
                if !record_starts_in_regions(&record, &header, &regions)? {
                    continue;
                }
                if let Some(batch) = decoder.append_record(&record, &header)? {
                    emitted += batch.num_rows();
                    yield batch;
                }
                let accepted = emitted + decoder.batch_row_count;
                if limit.is_some_and(|value| accepted >= value) {
                    break 'chunks;
                }
            }
        }

        if let Some(batch) = decoder.finish()? {
            yield batch;
        }
    };

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        stream,
    )))
}

pub(crate) struct BcfExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) info_fields: Option<Vec<String>>,
    pub(crate) format_fields: Option<Vec<String>>,
    pub(crate) sample_names: Vec<String>,
    pub(crate) source_sample_names: Vec<String>,
    pub(crate) cache: Arc<PlanProperties>,
    pub(crate) limit: Option<usize>,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
    pub(crate) coordinate_system_zero_based: bool,
    pub(crate) partition_assignments: Option<Vec<PartitionAssignment>>,
    pub(crate) index_path: Option<String>,
    /// CSI parsed once at planning time and shared by all scan partitions.
    pub(crate) index: Option<Arc<noodles_csi::Index>>,
    /// Header parsed once at provider construction and shared by all scan
    /// partitions (avoids re-downloading it per partition on remote scans).
    pub(crate) header: Option<Arc<Header>>,
    pub(crate) residual_filters: Vec<Expr>,
    pub(crate) genotype_output_mode: GenotypeOutputMode,
}

impl Debug for BcfExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BcfExec")
            .field("file_path", &self.file_path)
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for BcfExec {
    fn fmt_as(&self, _display_type: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "BcfExec: projection={:?}", self.projection)
    }
}

impl ExecutionPlan for BcfExec {
    fn name(&self) -> &str {
        "BcfExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        info!(
            "BCF scan partition={partition}, projection={:?}",
            self.projection
        );
        let batch_size = context.session_config().batch_size();

        if let (Some(assignments), Some(index_path)) =
            (&self.partition_assignments, &self.index_path)
            && let Some(assignment) = assignments.get(partition)
        {
            if matches!(get_storage_type(self.file_path.clone()), StorageType::LOCAL) {
                return Ok(indexed_local_stream(
                    self.file_path.clone(),
                    index_path.clone(),
                    self.index.clone(),
                    assignment.regions.clone(),
                    self.schema.clone(),
                    batch_size,
                    self.info_fields.clone(),
                    self.format_fields.clone(),
                    self.sample_names.clone(),
                    self.source_sample_names.clone(),
                    self.projection.clone(),
                    self.coordinate_system_zero_based,
                    self.residual_filters.clone(),
                    self.genotype_output_mode,
                    self.limit,
                ));
            }

            let future = indexed_remote_stream(
                self.file_path.clone(),
                index_path.clone(),
                self.index.clone(),
                self.header.clone(),
                assignment.regions.clone(),
                self.schema.clone(),
                batch_size,
                self.info_fields.clone(),
                self.format_fields.clone(),
                self.sample_names.clone(),
                self.source_sample_names.clone(),
                self.projection.clone(),
                self.object_storage_options.clone(),
                self.coordinate_system_zero_based,
                self.residual_filters.clone(),
                self.genotype_output_mode,
                self.limit,
            );
            let stream = futures::stream::once(future).try_flatten();
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                stream,
            )));
        }

        match get_storage_type(self.file_path.clone()) {
            StorageType::LOCAL => Ok(full_local_stream(
                self.file_path.clone(),
                self.schema.clone(),
                batch_size,
                self.info_fields.clone(),
                self.format_fields.clone(),
                self.sample_names.clone(),
                self.source_sample_names.clone(),
                self.projection.clone(),
                self.coordinate_system_zero_based,
                self.residual_filters.clone(),
                self.genotype_output_mode,
                self.limit,
            )),
            _ => {
                let future = full_remote_stream(
                    self.file_path.clone(),
                    self.schema.clone(),
                    batch_size,
                    self.info_fields.clone(),
                    self.format_fields.clone(),
                    self.sample_names.clone(),
                    self.source_sample_names.clone(),
                    self.projection.clone(),
                    self.object_storage_options.clone(),
                    self.coordinate_system_zero_based,
                    self.residual_filters.clone(),
                    self.genotype_output_mode,
                    self.limit,
                );
                let stream = futures::stream::once(future).try_flatten();
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    self.schema.clone(),
                    stream,
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_bio_format_core::genomic_filter::GenomicRegion;
    use noodles_vcf as vcf;
    use noodles_vcf::variant::io::Write as _;

    #[test]
    fn validates_only_bcf_2_2() {
        assert!(validate_version((2, 2)).is_ok());
        let error = validate_version((2, 1)).unwrap_err().to_string();
        assert!(error.contains("bcftools view -Ob"));
        assert!(validate_version((3, 0)).is_err());
    }

    #[test]
    fn validates_reserved_gt_header_declaration() {
        let text = "##fileformat=VCFv4.3\n\
                    ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                    #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n";
        let mut reader = vcf::io::Reader::new(text.as_bytes());
        let header = reader.read_header().unwrap();
        assert!(validate_bcf_header(&header).is_ok());

        let mut invalid_number = header.clone();
        *invalid_number
            .formats_mut()
            .get_mut("GT")
            .unwrap()
            .number_mut() = FormatNumber::Count(2);
        let error = validate_bcf_header(&invalid_number)
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Execution error: invalid BCF GT header declaration: expected Number=1,Type=String"
        );

        let mut invalid_type = header;
        *invalid_type.formats_mut().get_mut("GT").unwrap().type_mut() = FormatType::Integer;
        let error = validate_bcf_header(&invalid_type).unwrap_err().to_string();
        assert_eq!(
            error,
            "Execution error: invalid BCF GT header declaration: expected Number=1,Type=String"
        );
    }

    #[test]
    fn checks_remaining_bcf_record_body_arithmetic() {
        assert_eq!(remaining_bcf_record_body_size(24, 10).unwrap(), 10);
        let underflow = remaining_bcf_record_body_size(23, 0)
            .unwrap_err()
            .to_string();
        assert!(underflow.contains("shorter than the fixed site prefix"));

        let overflow = remaining_bcf_record_body_size(u64::MAX, 1)
            .unwrap_err()
            .to_string();
        assert!(overflow.contains("l_shared + l_indiv overflowed"));
    }

    #[test]
    fn maps_general_csi_bins_to_coordinate_intervals() {
        assert_eq!(csi_bin_interval(0, 14, 5), Some((1, 536_870_912, 0)));
        assert_eq!(csi_bin_interval(4681, 14, 5), Some((1, 16_384, 5)));
        assert_eq!(csi_bin_interval(4682, 14, 5), Some((16_385, 32_768, 5)));
        assert_eq!(
            csi_bin_interval(37_448, 14, 5),
            Some((536_854_529, 536_870_912, 5))
        );
        assert_eq!(csi_bin_interval(9, 10, 2), Some((1, 1024, 2)));
        assert_eq!(csi_bin_interval(37_450, 14, 5), None);
    }

    #[test]
    fn estimates_only_csi_bins_intersecting_the_requested_region() {
        use noodles_csi::binning_index::index::ReferenceSequence;
        use noodles_csi::binning_index::index::reference_sequence::{
            Bin, bin::Chunk, index::BinnedIndex,
        };

        let virtual_position = |compressed| {
            noodles_bgzf::VirtualPosition::new(compressed, 0)
                .expect("test compressed positions must be representable")
        };
        let bins = [
            (
                4681,
                Bin::new(vec![Chunk::new(
                    virtual_position(100),
                    virtual_position(200),
                )]),
            ),
            (
                4682,
                Bin::new(vec![Chunk::new(
                    virtual_position(10_000),
                    virtual_position(20_000),
                )]),
            ),
        ]
        .into_iter()
        .collect();
        let reference: ReferenceSequence<BinnedIndex> =
            ReferenceSequence::new(bins, Default::default(), None);
        let index = noodles_csi::Index::builder()
            .set_reference_sequences(vec![reference])
            .build();
        let contig_names = vec!["chr1".to_string()];
        let contig_lengths = vec![32_768];

        let targeted = estimate_region_sizes(
            Some(&index),
            &[region(Some(1), Some(16_000))],
            &contig_names,
            &contig_lengths,
        );
        assert_eq!(targeted[0].estimated_bytes, 100);

        let full_contig = estimate_region_sizes(
            Some(&index),
            &[region(None, None)],
            &contig_names,
            &contig_lengths,
        );
        assert_eq!(full_contig[0].estimated_bytes, 19_900);
    }

    #[test]
    fn rejects_named_csi_dictionary_with_different_order() {
        let mut bcf_header = vcf::Header::builder()
            .add_contig("chr1", Default::default())
            .add_contig("chr2", Default::default())
            .build();
        *bcf_header.string_maps_mut() = vcf::header::StringMaps::try_from(&bcf_header).unwrap();
        let csi_header = noodles_csi::binning_index::index::Header::builder()
            .set_reference_sequence_names(["chr2", "chr1"].into_iter().map(Into::into).collect())
            .build();
        let reference_sequences = (0..2)
            .map(|_| {
                noodles_csi::binning_index::index::ReferenceSequence::new(
                    Default::default(),
                    Default::default(),
                    None,
                )
            })
            .collect();
        let index = noodles_csi::Index::builder()
            .set_header(csi_header)
            .set_reference_sequences(reference_sequences)
            .build();

        let error = validate_csi_reference_dictionary(&index, &bcf_header)
            .expect_err("different CSI name order must be rejected")
            .to_string();
        assert!(error.contains("names or ordering differ"));
    }

    #[tokio::test]
    async fn bounds_remote_csi_bytes_while_streaming() {
        let accepted = futures::stream::iter([
            Ok::<_, io::Error>(bytes::Bytes::from_static(b"abc")),
            Ok(bytes::Bytes::from_static(b"de")),
        ]);
        assert_eq!(
            collect_remote_csi_bytes(accepted, 5).await.unwrap(),
            b"abcde"
        );

        let oversized = futures::stream::iter([
            Ok::<_, io::Error>(bytes::Bytes::from_static(b"abc")),
            Ok(bytes::Bytes::from_static(b"def")),
        ]);
        let error = collect_remote_csi_bytes(oversized, 5)
            .await
            .expect_err("the stream must stop before buffering more than the ceiling")
            .to_string();
        assert!(error.contains("exceeds the 5-byte safety limit"));
    }

    #[test]
    fn rejects_oversized_local_csi_before_parsing() {
        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file()
            .set_len(MAX_BCF_CSI_INDEX_SIZE as u64 + 1)
            .unwrap();
        let error = read_local_csi_index(file.path().to_str().unwrap())
            .expect_err("an oversized local CSI must be rejected from metadata")
            .to_string();
        assert!(error.contains("exceeding the 268435456-byte safety limit"));
    }

    #[test]
    fn validates_gt_payload_for_each_integer_width() {
        let error = validate_bcf_gt_payload(&[], BcfEncodedType::Int8, 0, 2)
            .expect_err("zero-width GT must return an error rather than panic")
            .to_string();
        assert!(
            error.contains("at least one value per sample"),
            "unexpected error: {error}"
        );

        assert!(validate_bcf_gt_payload(&[2, 4], BcfEncodedType::Int8, 2, 2).is_ok());

        let mut int16 = Vec::new();
        int16.extend_from_slice(&2i16.to_le_bytes());
        int16.extend_from_slice(&4i16.to_le_bytes());
        assert!(validate_bcf_gt_payload(&int16, BcfEncodedType::Int16, 2, 2).is_ok());

        let mut int32 = Vec::new();
        int32.extend_from_slice(&2i32.to_le_bytes());
        int32.extend_from_slice(&6i32.to_le_bytes());
        let error = validate_bcf_gt_payload(&int32, BcfEncodedType::Int32, 2, 2)
            .expect_err("allele index 2 must be rejected for a biallelic record")
            .to_string();
        assert!(
            error.contains("GT allele index 2"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn bounds_selected_gt_sample_offsets() {
        let payload = [2, 2, 2, 4];
        assert_eq!(
            BcfDosageBuilder::gt_sample_payload(&payload, 1, 2).unwrap(),
            &[2, 4]
        );
        assert!(BcfDosageBuilder::gt_sample_payload(&payload, 2, 2).is_err());
        assert!(BcfDosageBuilder::gt_sample_payload(&payload, usize::MAX, 2).is_err());
    }

    #[test]
    fn decodes_biallelic_dosage_for_each_integer_width() {
        assert_eq!(BcfDosageBuilder::decode_i8(&[2, 4]).unwrap(), Some(1));
        assert_eq!(BcfDosageBuilder::decode_i8(&[5, 5]).unwrap(), Some(2));
        assert_eq!(BcfDosageBuilder::decode_i8(&[3, 3]).unwrap(), Some(0));
        assert_eq!(BcfDosageBuilder::decode_i8(&[2, 0]).unwrap(), None);
        assert_eq!(
            BcfDosageBuilder::decode_i8(&[4, (i8::MIN + 1) as u8]).unwrap(),
            Some(1)
        );

        let int16 = [2i16, 5i16]
            .into_iter()
            .flat_map(i16::to_le_bytes)
            .collect::<Vec<_>>();
        assert_eq!(BcfDosageBuilder::decode_i16(&int16).unwrap(), Some(1));

        let int32 = [4i32, 4i32]
            .into_iter()
            .flat_map(i32::to_le_bytes)
            .collect::<Vec<_>>();
        assert_eq!(BcfDosageBuilder::decode_i32(&int32).unwrap(), Some(2));
    }

    #[test]
    fn dosage_range_is_independent_from_gt_ploidy() {
        let reference_i8 = vec![2; 128];
        assert_eq!(BcfDosageBuilder::decode_i8(&reference_i8).unwrap(), Some(0));

        let reference_i16 = std::iter::repeat_n(2i16, 128)
            .flat_map(i16::to_le_bytes)
            .collect::<Vec<_>>();
        assert_eq!(
            BcfDosageBuilder::decode_i16(&reference_i16).unwrap(),
            Some(0)
        );

        let reference_i32 = std::iter::repeat_n(2i32, 128)
            .flat_map(i32::to_le_bytes)
            .collect::<Vec<_>>();
        assert_eq!(
            BcfDosageBuilder::decode_i32(&reference_i32).unwrap(),
            Some(0)
        );

        let alternate_i8 = vec![4; 128];
        let error = BcfDosageBuilder::decode_i8(&alternate_i8)
            .expect_err("alternate dosage 128 must not fit in signed 8-bit output")
            .to_string();
        assert!(
            error.contains("alternate dosage exceeds"),
            "unexpected error: {error}"
        );

        let mut partially_missing_i8 = vec![4; 128];
        partially_missing_i8.push(0);
        assert_eq!(
            BcfDosageBuilder::decode_i8(&partially_missing_i8).unwrap(),
            None
        );

        let partially_missing_i16 = std::iter::repeat_n(4i16, 128)
            .chain(std::iter::once(0))
            .flat_map(i16::to_le_bytes)
            .collect::<Vec<_>>();
        assert_eq!(
            BcfDosageBuilder::decode_i16(&partially_missing_i16).unwrap(),
            None
        );

        let partially_missing_i32 = std::iter::repeat_n(4i32, 128)
            .chain(std::iter::once(0))
            .flat_map(i32::to_le_bytes)
            .collect::<Vec<_>>();
        assert_eq!(
            BcfDosageBuilder::decode_i32(&partially_missing_i32).unwrap(),
            None
        );

        let error = BcfDosageBuilder::decode_i8(&[0, 6])
            .expect_err("a missing allele must not hide a later invalid allele")
            .to_string();
        assert!(
            error.contains("allele index 2"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn calculates_number_g_cardinality_from_alleles_and_ploidy() {
        assert_eq!(bcf_genotype_cardinality(2, 1).unwrap(), 2);
        assert_eq!(bcf_genotype_cardinality(2, 2).unwrap(), 3);
        assert_eq!(bcf_genotype_cardinality(3, 2).unwrap(), 6);
        assert_eq!(bcf_genotype_cardinality(2, 3).unwrap(), 4);
        assert!(bcf_genotype_cardinality(0, 2).is_err());
        assert!(bcf_genotype_cardinality(2, 0).is_err());

        let gt = BcfGtEncoding {
            payload: &[2, 4, 4, 0x81],
            encoded_type: BcfEncodedType::Int8,
            value_count: 2,
        };
        assert_eq!(bcf_gt_sample_ploidy(&gt, 0).unwrap(), 2);
        assert_eq!(bcf_gt_sample_ploidy(&gt, 1).unwrap(), 1);
    }

    #[test]
    fn validates_number_p_cardinality_against_gt_ploidy() {
        let vcf_text = "##fileformat=VCFv4.3\n\
                        ##contig=<ID=chr1,length=1000>\n\
                        ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                        ##FORMAT=<ID=XP,Number=1,Type=Integer,Description=\"Ploidy values\">\n\
                        #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                        chr1\t10\trs1\tA\tC\t50\tPASS\t.\tXP:GT\t7:0/1\n";
        let mut vcf_reader = vcf::io::Reader::new(vcf_text.as_bytes());
        let vcf_header = vcf_reader.read_header().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("number-p.bcf");
        let mut writer = bcf::io::Writer::new(File::create(&path).unwrap());
        writer.write_header(&vcf_header).unwrap();
        for result in vcf_reader.records() {
            writer
                .write_variant_record(&vcf_header, &result.unwrap())
                .unwrap();
        }
        writer.try_finish().unwrap();

        let mut reader = bcf::io::Reader::new(File::open(path).unwrap());
        let mut header = reader.read_header().unwrap();
        *header
            .formats_mut()
            .get_mut("XP")
            .expect("XP must be defined")
            .number_mut() = FormatNumber::Ploidy;
        let mut record = BcfRecord::default();
        assert!(reader.read_record(&mut record).unwrap() > 0);

        let samples = record.samples().unwrap();
        let error = validate_bcf_format_encoding(&samples, &header, 2, true)
            .err()
            .expect("one XP value must not satisfy diploid Number=P")
            .to_string();
        assert!(
            error.contains("Number=P (2 expected for sample 0 with ploidy 2)"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validates_number_g_as_diploid_when_gt_is_absent() {
        let vcf_text = "##fileformat=VCFv4.3\n\
                        ##contig=<ID=chr1,length=1000>\n\
                        ##FORMAT=<ID=PL,Number=2,Type=Integer,Description=\"Likelihoods\">\n\
                        #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                        chr1\t10\trs1\tA\tC\t50\tPASS\t.\tPL\t0,10\n";
        let mut vcf_reader = vcf::io::Reader::new(vcf_text.as_bytes());
        let vcf_header = vcf_reader.read_header().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("number-g-without-gt.bcf");
        let mut writer = bcf::io::Writer::new(File::create(&path).unwrap());
        writer.write_header(&vcf_header).unwrap();
        for result in vcf_reader.records() {
            writer
                .write_variant_record(&vcf_header, &result.unwrap())
                .unwrap();
        }
        writer.try_finish().unwrap();

        let mut reader = bcf::io::Reader::new(File::open(path).unwrap());
        let mut header = reader.read_header().unwrap();
        *header
            .formats_mut()
            .get_mut("PL")
            .expect("PL must be defined")
            .number_mut() = FormatNumber::Samples;
        let mut record = BcfRecord::default();
        assert!(reader.read_record(&mut record).unwrap() > 0);

        let samples = record.samples().unwrap();
        let error = match validate_bcf_format_encoding(&samples, &header, 2, true) {
            Ok(_) => panic!("two PL values must not satisfy diploid Number=G"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("Number=G (3 expected for sample 0 with ploidy 2)"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validates_number_g_character_payloads_in_the_deferred_path() {
        fn read_character_record(value: &str) -> (Header, BcfRecord) {
            let vcf_text = format!(
                "##fileformat=VCFv4.3\n\
                 ##contig=<ID=chr1,length=1000>\n\
                 ##FORMAT=<ID=XC,Number=1,Type=String,Description=\"Characters\">\n\
                 #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                 chr1\t10\trs1\tA\tC\t50\tPASS\t.\tXC\t{value}\n"
            );
            let mut vcf_reader = vcf::io::Reader::new(vcf_text.as_bytes());
            let vcf_header = vcf_reader.read_header().unwrap();

            let file = tempfile::NamedTempFile::new().unwrap();
            let mut writer = bcf::io::Writer::new(file.reopen().unwrap());
            writer.write_header(&vcf_header).unwrap();
            for result in vcf_reader.records() {
                writer
                    .write_variant_record(&vcf_header, &result.unwrap())
                    .unwrap();
            }
            writer.try_finish().unwrap();
            drop(writer);

            let mut reader = bcf::io::Reader::new(file.reopen().unwrap());
            let mut header = reader.read_header().unwrap();
            let format = header.formats_mut().get_mut("XC").unwrap();
            *format.number_mut() = FormatNumber::Samples;
            *format.type_mut() = FormatType::Character;
            let mut record = BcfRecord::default();
            assert!(reader.read_record(&mut record).unwrap() > 0);
            (header, record)
        }

        let (valid_header, valid_record) = read_character_record("a,b,c");
        let valid_samples = valid_record.samples().unwrap();
        assert!(validate_bcf_format_encoding(&valid_samples, &valid_header, 2, true).is_ok());

        let (invalid_header, invalid_record) = read_character_record("a,bb,c");
        let invalid_samples = invalid_record.samples().unwrap();
        let error = match validate_bcf_format_encoding(&invalid_samples, &invalid_header, 2, true) {
            Ok(_) => panic!("Number=G Character elements must remain single characters"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("Character field 'XC': element 1 contains 2 characters"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validates_number_g_and_number_p_fields_in_the_same_format_series() {
        let vcf_text = "##fileformat=VCFv4.3\n\
                        ##contig=<ID=chr1,length=1000>\n\
                        ##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n\
                        ##FORMAT=<ID=XG,Number=3,Type=Integer,Description=\"Genotype values\">\n\
                        ##FORMAT=<ID=XP,Number=1,Type=Integer,Description=\"Ploidy values\">\n\
                        #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                        chr1\t10\trs1\tA\tC\t50\tPASS\t.\tXG:GT:XP\t5,7,9:0/1:7\n";
        let mut vcf_reader = vcf::io::Reader::new(vcf_text.as_bytes());
        let vcf_header = vcf_reader.read_header().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("number-g-and-p.bcf");
        let mut writer = bcf::io::Writer::new(File::create(&path).unwrap());
        writer.write_header(&vcf_header).unwrap();
        for result in vcf_reader.records() {
            writer
                .write_variant_record(&vcf_header, &result.unwrap())
                .unwrap();
        }
        writer.try_finish().unwrap();

        let mut reader = bcf::io::Reader::new(File::open(path).unwrap());
        let mut header = reader.read_header().unwrap();
        *header.formats_mut().get_mut("XG").unwrap().number_mut() = FormatNumber::Samples;
        *header.formats_mut().get_mut("XP").unwrap().number_mut() = FormatNumber::Ploidy;
        let mut record = BcfRecord::default();
        assert!(reader.read_record(&mut record).unwrap() > 0);

        let samples = record.samples().unwrap();
        let error = match validate_bcf_format_encoding(&samples, &header, 2, true) {
            Ok(_) => panic!("valid Number=G must not hide invalid Number=P"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("FORMAT field 'XP' declares Number=P (2 expected"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_number_p_when_gt_is_absent() {
        let vcf_text = "##fileformat=VCFv4.3\n\
                        ##contig=<ID=chr1,length=1000>\n\
                        ##FORMAT=<ID=XP,Number=1,Type=Integer,Description=\"Ploidy values\">\n\
                        #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\n\
                        chr1\t10\trs1\tA\tC\t50\tPASS\t.\tXP\t7\n";
        let mut vcf_reader = vcf::io::Reader::new(vcf_text.as_bytes());
        let vcf_header = vcf_reader.read_header().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("number-p-without-gt.bcf");
        let mut writer = bcf::io::Writer::new(File::create(&path).unwrap());
        writer.write_header(&vcf_header).unwrap();
        for result in vcf_reader.records() {
            writer
                .write_variant_record(&vcf_header, &result.unwrap())
                .unwrap();
        }
        writer.try_finish().unwrap();

        let mut reader = bcf::io::Reader::new(File::open(path).unwrap());
        let mut header = reader.read_header().unwrap();
        *header
            .formats_mut()
            .get_mut("XP")
            .expect("XP must be defined")
            .number_mut() = FormatNumber::Ploidy;
        let mut record = BcfRecord::default();
        assert!(reader.read_record(&mut record).unwrap() > 0);

        let samples = record.samples().unwrap();
        let error = match validate_bcf_format_encoding(&samples, &header, 2, true) {
            Ok(_) => panic!("Number=P is defined by GT and must require it"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("FORMAT field 'XP' declares Number=P but the record has no GT field"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn counts_numeric_values_before_vector_end_for_each_width() {
        assert_eq!(
            bcf_numeric_logical_value_count(
                BcfFieldContext::Info,
                "DP",
                BcfEncodedType::Int8,
                &[5, 0x81],
            )
            .unwrap(),
            1
        );

        let mut int16 = Vec::new();
        int16.extend_from_slice(&5i16.to_le_bytes());
        int16.extend_from_slice(&(i16::MIN + 1).to_le_bytes());
        assert_eq!(
            bcf_numeric_logical_value_count(
                BcfFieldContext::Info,
                "DP",
                BcfEncodedType::Int16,
                &int16,
            )
            .unwrap(),
            1
        );

        let mut int32 = Vec::new();
        int32.extend_from_slice(&5i32.to_le_bytes());
        int32.extend_from_slice(&(i32::MIN + 1).to_le_bytes());
        assert_eq!(
            bcf_numeric_logical_value_count(
                BcfFieldContext::Info,
                "DP",
                BcfEncodedType::Int32,
                &int32,
            )
            .unwrap(),
            1
        );

        let mut float = Vec::new();
        float.extend_from_slice(&5.0f32.to_le_bytes());
        float.extend_from_slice(&0x7f80_0002_u32.to_le_bytes());
        assert_eq!(
            bcf_numeric_logical_value_count(
                BcfFieldContext::Info,
                "DP",
                BcfEncodedType::Float,
                &float,
            )
            .unwrap(),
            1
        );

        let error = bcf_numeric_logical_value_count(
            BcfFieldContext::Info,
            "DP",
            BcfEncodedType::Int8,
            &[5, 0x81, 7],
        )
        .expect_err("a value after vector-end must be rejected")
        .to_string();
        assert!(error.contains("value after vector-end"));

        let error = bcf_numeric_logical_value_count(
            BcfFieldContext::Info,
            "DP",
            BcfEncodedType::Int8,
            &[0x82],
        )
        .expect_err("a reserved numeric sentinel must be rejected")
        .to_string();
        assert!(error.contains("reserved numeric value"));

        assert!(bcf_numeric_vector_is_missing(
            BcfEncodedType::Int8,
            &[0x80, 0x81]
        ));
    }

    fn region(start: Option<u64>, end: Option<u64>) -> GenomicRegion {
        GenomicRegion {
            chrom: "chr1".to_string(),
            start,
            end,
            unmapped_tail: false,
        }
    }

    #[test]
    fn spanning_record_is_owned_by_exactly_one_sub_region() {
        // One record at chr1:400 with a 201 bp REF, i.e. spanning [400, 600].
        let long_ref = "A".repeat(201);
        let vcf_text = format!(
            "##fileformat=VCFv4.3\n##contig=<ID=chr1,length=10000>\n\
             #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
             chr1\t400\t.\t{long_ref}\tA\t.\tPASS\t.\n"
        );
        let mut vcf_reader = vcf::io::Reader::new(vcf_text.as_bytes());
        let vcf_header = vcf_reader.read_header().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("span.bcf");
        let mut writer = bcf::io::Writer::new(File::create(&path).unwrap());
        writer.write_header(&vcf_header).unwrap();
        for result in vcf_reader.records() {
            writer
                .write_variant_record(&vcf_header, &result.unwrap())
                .unwrap();
        }
        writer.try_finish().unwrap();

        let mut reader = bcf::io::Reader::new(File::open(&path).unwrap());
        let header = reader.read_header().unwrap();
        let mut record = BcfRecord::default();
        assert!(reader.read_record(&mut record).unwrap() > 0);

        // Splitting chr1 at 500 assigns the record to the first sub-region only,
        // even though its interval overlaps both.
        assert!(record_starts_in_regions(&record, &header, &[region(Some(1), Some(500))]).unwrap());
        assert!(
            !record_starts_in_regions(&record, &header, &[region(Some(501), Some(1000))]).unwrap()
        );
        // Overlap without start containment does not confer ownership.
        assert!(
            !record_starts_in_regions(&record, &header, &[region(Some(450), Some(460))]).unwrap()
        );
        // Open-ended bounds contain every start on the contig.
        assert!(record_starts_in_regions(&record, &header, &[region(None, None)]).unwrap());
        // Boundary inclusivity: 1-based start 400 is inside [400, 400].
        assert!(
            record_starts_in_regions(&record, &header, &[region(Some(400), Some(400))]).unwrap()
        );
    }
}
