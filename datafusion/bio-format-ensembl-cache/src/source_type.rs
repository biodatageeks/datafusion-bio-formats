use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// Arrow schema metadata key declaring the VEP cache source mode.
pub const VEP_CACHE_SOURCE_TYPE_METADATA_KEY: &str = "bio.vep.cache_source_type";

/// Explicit source mode for an Ensembl VEP cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheSourceType {
    /// Ensembl-only cache.
    Ensembl,
    /// Merged Ensembl + RefSeq cache.
    Merged,
    /// RefSeq-only cache.
    RefSeq,
}

impl CacheSourceType {
    /// Returns the canonical lowercase metadata value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ensembl => "ensembl",
            Self::Merged => "merged",
            Self::RefSeq => "refseq",
        }
    }
}

impl Display for CacheSourceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for CacheSourceType {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "ensembl" => Ok(Self::Ensembl),
            "merged" => Ok(Self::Merged),
            "refseq" => Ok(Self::RefSeq),
            other => Err(format!(
                "unknown VEP cache source type {other:?}; expected one of ensembl, merged, refseq"
            )),
        }
    }
}
