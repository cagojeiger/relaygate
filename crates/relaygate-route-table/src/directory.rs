use std::{collections::HashSet, sync::Arc};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{DestinationId, RouteTableError, ShardDirectoryGeneration, ShardEndpoint, ShardId};

pub const AUTHORITY_HASH_SHA256_MODULO_V1: &str = "sha256-modulo-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRecord {
    id: ShardId,
    endpoint: ShardEndpoint,
}

impl ShardRecord {
    #[must_use]
    pub fn id(&self) -> &ShardId {
        &self.id
    }

    #[must_use]
    pub fn endpoint(&self) -> &ShardEndpoint {
        &self.endpoint
    }
}

/// Immutable, ordered shard directory loaded from exact JSON artifact bytes.
#[derive(Debug, Clone)]
pub struct ShardDirectory {
    artifact: Arc<[u8]>,
    generation: ShardDirectoryGeneration,
    shards: Arc<[ShardRecord]>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryDocument {
    format_version: u32,
    authority_hash: String,
    shards: Vec<ShardRecordDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShardRecordDocument {
    id: String,
    endpoint: String,
}

impl ShardDirectory {
    pub fn from_json_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, RouteTableError> {
        let bytes = bytes.as_ref();
        let document: DirectoryDocument = serde_json::from_slice(bytes).map_err(|error| {
            RouteTableError::InvalidArgument(format!("invalid ShardDirectory JSON: {error}"))
        })?;

        if document.format_version != 1 {
            return Err(RouteTableError::InvalidArgument(
                "ShardDirectory format_version must be 1".to_owned(),
            ));
        }
        if document.authority_hash != AUTHORITY_HASH_SHA256_MODULO_V1 {
            return Err(RouteTableError::InvalidArgument(format!(
                "ShardDirectory authority_hash must be {AUTHORITY_HASH_SHA256_MODULO_V1}"
            )));
        }
        if document.shards.is_empty() {
            return Err(RouteTableError::InvalidArgument(
                "ShardDirectory must contain at least one shard".to_owned(),
            ));
        }

        let mut seen = HashSet::with_capacity(document.shards.len());
        let mut shards = Vec::with_capacity(document.shards.len());
        for record in document.shards {
            let id = ShardId::new(record.id)?;
            if !seen.insert(id.clone()) {
                return Err(RouteTableError::InvalidArgument(
                    "ShardDirectory contains a duplicate ShardId".to_owned(),
                ));
            }
            shards.push(ShardRecord {
                id,
                endpoint: ShardEndpoint::new(record.endpoint)?,
            });
        }

        let digest: [u8; 32] = Sha256::digest(bytes).into();
        Ok(Self {
            artifact: Arc::from(bytes),
            generation: ShardDirectoryGeneration::from_bytes(digest),
            shards: Arc::from(shards),
        })
    }

    #[must_use]
    pub const fn generation(&self) -> ShardDirectoryGeneration {
        self.generation
    }

    #[must_use]
    pub fn artifact_bytes(&self) -> &[u8] {
        &self.artifact
    }

    #[must_use]
    pub fn shards(&self) -> &[ShardRecord] {
        &self.shards
    }

    #[must_use]
    pub fn shard(&self, shard_id: &ShardId) -> Option<&ShardRecord> {
        self.shards.iter().find(|record| record.id == *shard_id)
    }

    #[must_use]
    pub fn authority(&self, destination_id: &DestinationId) -> &ShardRecord {
        let digest = Sha256::digest(destination_id.as_bytes());
        let mut prefix = [0_u8; 8];
        prefix.copy_from_slice(&digest[..8]);
        let value = u64::from_be_bytes(prefix);
        let index = (value % self.shards.len() as u64) as usize;
        &self.shards[index]
    }
}
