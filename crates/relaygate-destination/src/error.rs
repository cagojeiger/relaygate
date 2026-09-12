use thiserror::Error;

/// A destination failed canonical validation.
///
/// Invalid input is deliberately omitted so it cannot inject log content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DestinationError {
    /// The value does not have the exact `namespace/name` form.
    #[error("destination must be namespace/name")]
    InvalidDestination,
    /// The namespace contains a dot and therefore more than one label.
    #[error("namespace must contain exactly one label")]
    InvalidNamespace,
    /// The complete destination name exceeds [`crate::MAX_DESTINATION_NAME_BYTES`].
    #[error("destination exceeds the byte limit")]
    DestinationNameLength,
    /// A label is empty or exceeds [`crate::MAX_LABEL_BYTES`].
    #[error("labels must contain 1..=63 bytes")]
    LabelLength,
    /// A label starts or ends with a character not permitted at its boundary.
    #[error("labels must start and end with a lowercase ASCII letter or digit")]
    LabelBoundary,
    /// A label contains a character outside the canonical lowercase ASCII set.
    #[error("labels may contain only lowercase ASCII letters, digits and hyphens")]
    LabelCharacter,
}
