//! Repository-owned FCZ decoding candidate. Production cutover follows the
//! compatibility, robustness and platform gates in the migration plan.
//!
//! Packing, residue geometry, reconstruction and discretization are adapted from
//! Foldcomp 89e37195d3c8ade8d40ead91ad82e6cd2964a967 (MIT). See LICENSE-FOLDCOMP.
//! Source mapping: foldcomp.{h,cpp}, utility.h, discretizer.cpp, float3d.h,
//! nerf.{h,cpp}, atom_coordinate.cpp and amino_acid.h. Upstream authorship:
//! Hyunbin Kim; NeRF contributor Milot Mirdita. Geometry tables are regenerated
//! by testing/oracles/structure-codecs/foldcomp_tables.py from the pinned header.
mod backbone;
mod bitstream;
mod decode;
mod geometry;
mod tables;

pub(crate) use decode::decode;
mod discretize;
mod header;
mod residue;

#[cfg(test)]
mod tests;
