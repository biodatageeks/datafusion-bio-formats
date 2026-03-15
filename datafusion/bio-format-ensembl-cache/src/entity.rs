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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_kind_eq() {
        assert_eq!(EnsemblEntityKind::Variation, EnsemblEntityKind::Variation);
        assert_ne!(EnsemblEntityKind::Variation, EnsemblEntityKind::Transcript);
    }

    #[test]
    fn entity_kind_clone() {
        let kind = EnsemblEntityKind::Exon;
        let cloned = kind;
        assert_eq!(kind, cloned);
    }

    #[test]
    fn entity_kind_debug() {
        let s = format!("{:?}", EnsemblEntityKind::RegulatoryFeature);
        assert_eq!(s, "RegulatoryFeature");
    }

    #[test]
    fn all_variants_distinct() {
        let all = [
            EnsemblEntityKind::Variation,
            EnsemblEntityKind::Transcript,
            EnsemblEntityKind::RegulatoryFeature,
            EnsemblEntityKind::MotifFeature,
            EnsemblEntityKind::Exon,
            EnsemblEntityKind::Translation,
        ];
        for (i, a) in all.iter().enumerate() {
            for (j, b) in all.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b);
                }
            }
        }
    }
}
