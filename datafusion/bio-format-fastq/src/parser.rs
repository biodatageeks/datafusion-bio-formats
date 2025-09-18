#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FastqParser {
    Noodles,
    Needletail,
}

impl Default for FastqParser {
    fn default() -> Self {
        FastqParser::Noodles
    }
}

impl std::str::FromStr for FastqParser {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "noodles" => Ok(FastqParser::Noodles),
            "needletail" => Ok(FastqParser::Needletail),
            _ => Err(format!(
                "Unknown parser: {}. Valid options are: noodles, needletail",
                s
            )),
        }
    }
}

impl std::fmt::Display for FastqParser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FastqParser::Noodles => write!(f, "noodles"),
            FastqParser::Needletail => write!(f, "needletail"),
        }
    }
}
