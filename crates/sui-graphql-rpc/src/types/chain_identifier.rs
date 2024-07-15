// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::{
    data::{Db, DbConnection, QueryExecutor},
    error::Error,
};
use async_graphql::*;
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl};
use sui_indexer::schema::checkpoints;
use sui_types::{
    digests::ChainIdentifier as NativeChainIdentifier, messages_checkpoint::CheckpointDigest,
};
use tokio::sync::RwLock;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ChainIdentifier(pub(crate) NativeChainIdentifier);

impl ChainIdentifier {
    /// Query the Chain Identifier from the DB.
    pub(crate) async fn query(db: &Db) -> Result<Option<NativeChainIdentifier>, Error> {
        use checkpoints::dsl;

        let Some(digest_bytes) = db
            .execute(move |conn| {
                conn.first(move || {
                    dsl::checkpoints
                        .select(dsl::checkpoint_digest)
                        .order_by(dsl::sequence_number.asc())
                })
                .optional()
            })
            .await
            .map_err(|e| Error::Internal(format!("Failed to fetch genesis digest: {e}")))?
        else {
            return Ok(None);
        };

        let native_identifier = Self::from_bytes(digest_bytes)?;

        Ok(Some(native_identifier))
    }

    /// Treat `bytes` as a checkpoint digest and extract a chain identifier from it.
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Result<NativeChainIdentifier, Error> {
        let genesis_digest = CheckpointDigest::try_from(bytes)
            .map_err(|e| Error::Internal(format!("Failed to deserialize genesis digest: {e}")))?;
        Ok(NativeChainIdentifier::from(genesis_digest))
    }

    pub(crate) async fn new(lock: ChainIdentifierLock) -> Self {
        let w = lock.0.read().await;

        Self(w.0)
    }
}

impl From<NativeChainIdentifier> for ChainIdentifier {
    fn from(chain_identifier: NativeChainIdentifier) -> Self {
        Self(chain_identifier)
    }
}
#[derive(Clone, Default)]
pub(crate) struct ChainIdentifierLock(pub(crate) Arc<RwLock<ChainIdentifier>>);
