/// Entity kinds available in Ensembl VEP cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnsemblEntityKind {
    /// Variation cache rows.
    Variation,
    /// Transcript cache objects.
    Transcript,
    /// Regulatory feature cache objects.
    RegulatoryFeature,
    /// Motif feature cache objects.
    MotifFeature,
    /// Individual exon objects (one row per exon, extracted from transcript cache).
    Exon,
    /// Translation objects (one row per coding transcript, extracted from transcript cache).
    Translation,
}
