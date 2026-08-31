use serde::Deserialize;
use std::fmt;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum CoverageStatus {
    Executable,
    Partial,
    Gap,
    OutOfScope,
}

impl fmt::Display for CoverageStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Executable => "executable",
            Self::Partial => "partial",
            Self::Gap => "gap",
            Self::OutOfScope => "out_of_scope",
        };
        formatter.write_str(value)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoverageDocument {
    pub(crate) schema_version: u32,
    #[serde(rename = "case")]
    pub(crate) cases: Vec<CoverageCase>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoverageCase {
    pub(crate) id: String,
    pub(crate) status: CoverageStatus,
    #[serde(default)]
    pub(crate) rust: Vec<String>,
    #[serde(default)]
    pub(crate) reason: String,
}

pub(crate) fn parse(source: &str) -> Result<CoverageDocument, Vec<String>> {
    toml::from_str(source).map_err(|error| vec![format!("coverage TOML parse error: {error}")])
}
