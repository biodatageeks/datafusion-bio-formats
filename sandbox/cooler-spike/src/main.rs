//! Spike (tasks 1.2/1.3/1.4): read cooler .cool/.mcool via statically linked HDF5.
//!
//! Exercises everything the future provider needs:
//! - root attributes (format, format-version, bin-size, nnz)
//! - chroms (fixed-length ASCII names + lengths)
//! - bins (enum-typed `chrom`, start/end, optional `weight`)
//! - indexes (chrom_offset, bin1_offset)
//! - chunked hyperslab reads of `pixels`
//! - .mcool resolution discovery and per-resolution access
//! - count dtype detection (int vs float)

use anyhow::{bail, Context, Result};
use hdf5_metno as hdf5;
use hdf5::types::{FixedAscii, TypeDescriptor, VarLenAscii, VarLenUnicode};
use hdf5::{Dataset, File, Group};
use ndarray::s;

fn read_names(ds: &Dataset) -> Result<Vec<String>> {
    // cooler writes chrom names as fixed-length ASCII (h5py 'S{n}'); accept var-len too.
    let td = ds.dtype()?.to_descriptor()?;
    let names = match td {
        TypeDescriptor::FixedAscii(_) | TypeDescriptor::FixedUnicode(_) => ds
            .read_1d::<FixedAscii<256>>()?
            .iter()
            .map(|s| s.to_string())
            .collect(),
        TypeDescriptor::VarLenAscii => ds
            .read_1d::<VarLenAscii>()?
            .iter()
            .map(|s| s.to_string())
            .collect(),
        TypeDescriptor::VarLenUnicode => ds
            .read_1d::<VarLenUnicode>()?
            .iter()
            .map(|s| s.to_string())
            .collect(),
        other => bail!("unsupported chrom name dtype: {other:?}"),
    };
    Ok(names)
}

fn scalar_attr_i64(g: &Group, name: &str) -> Result<i64> {
    Ok(g.attr(name)?.read_scalar::<i64>()?)
}

fn attr_str(g: &Group, name: &str) -> Result<String> {
    let a = g.attr(name)?;
    let td = a.dtype()?.to_descriptor()?;
    Ok(match td {
        TypeDescriptor::VarLenUnicode => a.read_scalar::<VarLenUnicode>()?.to_string(),
        TypeDescriptor::VarLenAscii => a.read_scalar::<VarLenAscii>()?.to_string(),
        TypeDescriptor::FixedAscii(_) | TypeDescriptor::FixedUnicode(_) => {
            a.read_scalar::<FixedAscii<256>>()?.to_string()
        },
        other => bail!("unsupported attr dtype: {other:?}"),
    })
}

fn dump_collection(root: &Group, label: &str) -> Result<()> {
    println!("=== {label} ===");
    println!(
        "format={} version={} bin-size={:?} nnz={:?}",
        attr_str(root, "format")?,
        scalar_attr_i64(root, "format-version")?,
        scalar_attr_i64(root, "bin-size").ok(),
        scalar_attr_i64(root, "nnz").ok(),
    );

    // chroms
    let chroms = root.group("chroms")?;
    let names = read_names(&chroms.dataset("name")?)?;
    let lengths = chroms.dataset("length")?.read_1d::<i64>()?;
    println!("chroms: {:?}", names.iter().zip(lengths.iter()).collect::<Vec<_>>());

    // bins: chrom is enum-typed (h5py categorical) -> read via soft conversion to i32
    let bins = root.group("bins")?;
    let chrom_ds = bins.dataset("chrom")?;
    let chrom_td = chrom_ds.dtype()?.to_descriptor()?;
    let bin_chrom: Vec<i32> = chrom_ds
        .as_reader()
        .conversion(hdf5::Conversion::Soft)
        .read_1d::<i32>()
        .context("reading enum-typed bins/chrom as i32")?
        .to_vec();
    let starts = bins.dataset("start")?.read_1d::<i32>()?;
    let ends = bins.dataset("end")?.read_1d::<i32>()?;
    let has_weight = bins.link_exists("weight");
    println!(
        "bins: n={} chrom_dtype={chrom_td:?} first=({},{},{}) weight_col={has_weight}",
        bin_chrom.len(),
        bin_chrom[0],
        starts[0],
        ends[0]
    );
    if has_weight {
        let w = bins.dataset("weight")?.read_1d::<f64>()?;
        let finite = w.iter().filter(|x| x.is_finite()).count();
        println!("weights: n={} finite={} first_finite={:?}", w.len(), finite, w.iter().find(|x| x.is_finite()));
    }

    // indexes
    let indexes = root.group("indexes")?;
    let chrom_offset = indexes.dataset("chrom_offset")?.read_1d::<i64>()?;
    let bin1_offset = indexes.dataset("bin1_offset")?.read_1d::<i64>()?;
    println!(
        "indexes: chrom_offset={:?} bin1_offset[..5]={:?} len={}",
        chrom_offset.to_vec(),
        &bin1_offset.to_vec()[..5.min(bin1_offset.len())],
        bin1_offset.len()
    );

    // pixels: chunked hyperslab reads
    let pixels = root.group("pixels")?;
    let count_ds = pixels.dataset("count")?;
    let count_td = count_ds.dtype()?.to_descriptor()?;
    let n = count_ds.shape()[0];
    let chunk = 1000usize.min(n);
    let b1 = pixels.dataset("bin1_id")?.read_slice_1d::<i64, _>(s![0..chunk])?;
    let b2 = pixels.dataset("bin2_id")?.read_slice_1d::<i64, _>(s![0..chunk])?;
    println!("pixels: n={n} count_dtype={count_td:?}");
    let is_float = matches!(count_td, TypeDescriptor::Float(_));
    if is_float {
        let c = count_ds.read_slice_1d::<f64, _>(s![0..chunk])?;
        println!("first pixels (float): {:?}", (0..3).map(|i| (b1[i], b2[i], c[i])).collect::<Vec<_>>());
    } else {
        let c = count_ds.read_slice_1d::<i32, _>(s![0..chunk])?;
        println!("first pixels (int): {:?}", (0..3).map(|i| (b1[i], b2[i], c[i])).collect::<Vec<_>>());
        // tail slice too, to prove offset hyperslabs work
        let tail = count_ds.read_slice_1d::<i32, _>(s![n - 3..n])?;
        println!("tail counts: {:?}", tail.to_vec());
    }

    // join spot-check: pixel 0 -> genomic coords (0-based half-open, as stored)
    let (p1, p2) = (b1[0] as usize, b2[0] as usize);
    println!(
        "pixel0 joined: {}:{}-{} x {}:{}-{}",
        names[bin_chrom[p1] as usize], starts[p1], ends[p1],
        names[bin_chrom[p2] as usize], starts[p2], ends[p2]
    );
    Ok(())
}

fn main() -> Result<()> {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "../fixtures".into());
    println!("HDF5 library version: {:?}", hdf5::library_version());

    // .cool at root
    let f = File::open(format!("{dir}/test.cool"))?;
    dump_collection(&f.group("/")?, "test.cool")?;

    // .mcool: discover resolutions, open one collection
    let m = File::open(format!("{dir}/test.mcool"))?;
    let res_group = m.group("resolutions")?;
    let mut resolutions = res_group.member_names()?;
    resolutions.sort_by_key(|r| r.parse::<u64>().unwrap_or(u64::MAX));
    println!("\nmcool resolutions: {resolutions:?}");
    dump_collection(&m.group("/resolutions/2000")?, "test.mcool::/resolutions/2000")?;

    // float-count variant
    let ff = File::open(format!("{dir}/test_float.cool"))?;
    dump_collection(&ff.group("/")?, "test_float.cool")?;

    // task 1.4 smoke: datafusion + arrow co-compile and link alongside static hdf5
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let ctx = datafusion::prelude::SessionContext::new();
        let df = ctx.sql("SELECT 1 + 1 AS two").await?;
        let batches = df.collect().await?;
        println!("\ndatafusion co-build OK: {:?}", batches[0].column(0));
        Ok::<_, datafusion::error::DataFusionError>(())
    })?;

    println!("\nSPIKE OK");
    Ok(())
}
