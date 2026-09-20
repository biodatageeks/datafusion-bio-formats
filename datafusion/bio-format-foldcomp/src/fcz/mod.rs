//! Repository-owned FCZ decoding candidate. Production cutover follows the
//! compatibility, robustness and platform gates in the migration plan.
//!
//! Packing, residue constants and inverse discretization are adapted from
//! Foldcomp 89e37195d3c8ade8d40ead91ad82e6cd2964a967 (MIT). See LICENSE-FOLDCOMP.
mod bitstream;
mod discretize;
mod header;
mod residue;

#[cfg(test)]
mod tests;
