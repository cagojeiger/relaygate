use thiserror::Error;

/// A destination failed canonical validation.
///
/// Invalid input is deliberately omitted so it cannot inject log content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DestinationError {
    #[error("destination must be namespace/name")]
    InvalidDestination,
    #[error("namespace must contain exactly one label")]
    InvalidNamespace,
    #[error("destination exceeds the byte limit")]
    DestinationNameLength,
    #[error("labels must contain 1..=63 bytes")]
    LabelLength,
    #[error("labels must start and end with a lowercase ASCII letter or digit")]
    LabelBoundary,
    #[error("labels may contain only lowercase ASCII letters, digits and hyphens")]
    LabelCharacter,
}
