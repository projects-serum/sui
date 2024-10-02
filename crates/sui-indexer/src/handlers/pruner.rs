// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use mysten_metrics::spawn_monitored_task;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use strum_macros;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::RetentionConfig;
use crate::errors::IndexerError;
use crate::models::watermarks::PrunableWatermark;
use crate::store::pg_partition_manager::PgPartitionManager;
use crate::store::PgIndexerStore;
use crate::{metrics::IndexerMetrics, store::IndexerStore, types::IndexerResult};

pub struct Pruner {
    pub store: PgIndexerStore,
    pub partition_manager: PgPartitionManager,
    pub retention_policies: HashMap<PrunableTable, u64>,
    pub metrics: IndexerMetrics,
}

/// Enum representing tables that the pruner is allowed to prune. This corresponds to table names in
/// the database, and should be used in lieu of string literals. This enum is also meant to
/// facilitate the process of determining which unit (epoch, cp, or tx) should be used for the
/// table's range. Pruner will ignore any table that is not listed here.
#[derive(
    Debug,
    Eq,
    PartialEq,
    strum_macros::Display,
    strum_macros::EnumString,
    strum_macros::EnumIter,
    strum_macros::AsRefStr,
    Hash,
    Serialize,
    Deserialize,
    Clone,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum PrunableTable {
    ObjectsHistory,
    Transactions,
    Events,

    EventEmitPackage,
    EventEmitModule,
    EventSenders,
    EventStructInstantiation,
    EventStructModule,
    EventStructName,
    EventStructPackage,

    TxAffectedAddresses,
    TxAffectedObjects,
    TxCallsPkg,
    TxCallsMod,
    TxCallsFun,
    TxChangedObjects,
    TxDigests,
    TxInputObjects,
    TxKinds,
    TxRecipients,
    TxSenders,

    Checkpoints,
    PrunerCpWatermark,
}

struct PruningWorkItem {
    table: PrunableTable,
    min: i64,
    max: i64,
}

struct PruningPool {
    work_tx: mpsc::Sender<PruningWorkItem>,
    cancel: CancellationToken,
}

impl PruningWorkItem {
    // async fn execute -> match on table and prune data between min and max
    // would look something like diesel::delete(table).filter(table::column.between(min, max)).execute()
    async fn execute(&self) -> IndexerResult<()> {
        Ok(())
    }
}

impl PrunableTable {
    pub fn select_lower_bound(&self, cp: u64, tx: u64) -> u64 {
        match self {
            PrunableTable::ObjectsHistory => cp,
            PrunableTable::Transactions => tx,
            PrunableTable::Events => tx,

            PrunableTable::EventEmitPackage => tx,
            PrunableTable::EventEmitModule => tx,
            PrunableTable::EventSenders => tx,
            PrunableTable::EventStructInstantiation => tx,
            PrunableTable::EventStructModule => tx,
            PrunableTable::EventStructName => tx,
            PrunableTable::EventStructPackage => tx,

            PrunableTable::TxAffectedAddresses => tx,
            PrunableTable::TxAffectedObjects => tx,
            PrunableTable::TxCallsPkg => tx,
            PrunableTable::TxCallsMod => tx,
            PrunableTable::TxCallsFun => tx,
            PrunableTable::TxChangedObjects => tx,
            PrunableTable::TxDigests => tx,
            PrunableTable::TxInputObjects => tx,
            PrunableTable::TxKinds => tx,
            PrunableTable::TxRecipients => tx,
            PrunableTable::TxSenders => tx,

            PrunableTable::Checkpoints => cp,
            PrunableTable::PrunerCpWatermark => cp,
        }
    }
}

impl Pruner {
    /// Instantiates a pruner with default retention and overrides. Pruner will finalize the
    /// retention policies so there is a value for every prunable table.
    pub fn new(
        store: PgIndexerStore,
        retention_config: RetentionConfig,
        metrics: IndexerMetrics,
    ) -> Result<Self, IndexerError> {
        let partition_manager = PgPartitionManager::new(store.pool())?;
        let retention_policies = retention_config.retention_policies();

        Ok(Self {
            store,
            partition_manager,
            retention_policies,
            metrics,
        })
    }

    pub async fn start(&self, cancel: CancellationToken) -> IndexerResult<()> {
        let store_clone = self.store.clone();
        let retention_policies = self.retention_policies.clone();
        let cancel_clone = cancel.clone();
        spawn_monitored_task!(update_watermarks_lower_bounds_task(
            store_clone,
            retention_policies,
            cancel_clone
        ));

        while !cancel.is_cancelled() {
            let (watermarks, latest_db_timestamp) = self.store.get_watermarks().await?;
            // Not all partitioned tables are epoch-partitioned, so we need to filter them out.
            let table_partitions: HashMap<_, _> = self
                .partition_manager
                .get_table_partitions()
                .await?
                .into_iter()
                .filter(|(table_name, _)| {
                    self.partition_manager
                        .get_strategy(table_name)
                        .is_epoch_partitioned()
                })
                .collect();

            for watermark in watermarks.iter() {
                let Some(watermark) =
                    PrunableWatermark::new(watermark.clone(), latest_db_timestamp)
                else {
                    continue;
                };

                tokio::time::sleep(Duration::from_millis(watermark.prune_delay(1000))).await;

                // Prune as an epoch-partitioned table
                if table_partitions.get(watermark.entity.as_ref()).is_some() {
                    let mut prune_start = watermark.pruner_lo();
                    while prune_start < watermark.epoch_lo {
                        if cancel.is_cancelled() {
                            info!("Pruner task cancelled.");
                            return Ok(());
                        }
                        self.partition_manager
                            .drop_table_partition(
                                watermark.entity.as_ref().to_string(),
                                prune_start,
                            )
                            .await?;
                        info!(
                            "Batch dropped table partition {} epoch {}",
                            watermark.entity, prune_start
                        );
                        prune_start += 1;

                        // Then need to update the `pruned_lo`
                        self.store
                            .update_watermark_latest_pruned(watermark.entity.clone(), prune_start)
                            .await?;
                    }
                } else {
                    // Dealing with an unpartitioned table
                    if watermark.is_prunable() {
                        match watermark.entity {
                            PrunableTable::ObjectsHistory
                            | PrunableTable::Transactions
                            | PrunableTable::Events => {}
                            PrunableTable::EventEmitPackage
                            | PrunableTable::EventEmitModule
                            | PrunableTable::EventSenders
                            | PrunableTable::EventStructInstantiation
                            | PrunableTable::EventStructModule
                            | PrunableTable::EventStructName
                            | PrunableTable::EventStructPackage => {
                                self.store
                                    .prune_event_indices_table(
                                        watermark.pruner_lo(),
                                        watermark.reader_lo - 1,
                                    )
                                    .await?;
                            }
                            PrunableTable::TxAffectedAddresses
                            | PrunableTable::TxAffectedObjects
                            | PrunableTable::TxCallsPkg
                            | PrunableTable::TxCallsMod
                            | PrunableTable::TxCallsFun
                            | PrunableTable::TxChangedObjects
                            | PrunableTable::TxDigests
                            | PrunableTable::TxInputObjects
                            | PrunableTable::TxKinds
                            | PrunableTable::TxRecipients
                            | PrunableTable::TxSenders => {
                                self.store
                                    .prune_tx_indices_table(
                                        watermark.pruner_lo(),
                                        watermark.reader_lo - 1,
                                    )
                                    .await?;
                            }
                            PrunableTable::Checkpoints => {
                                self.store
                                    .prune_cp_tx_table(
                                        watermark.pruner_lo(),
                                        watermark.reader_lo - 1,
                                    )
                                    .await?;
                            }
                            PrunableTable::PrunerCpWatermark => {
                                self.store
                                    .prune_cp_tx_table(
                                        watermark.pruner_lo(),
                                        watermark.reader_lo - 1,
                                    )
                                    .await?;
                            }
                        }

                        self.store
                            .update_watermark_latest_pruned(
                                watermark.entity.clone(),
                                watermark.reader_lo - 1,
                            )
                            .await?;
                    }
                }
            }
        }
        info!("Pruner task cancelled.");
        Ok(())
    }
}

/// Task to periodically query the `watermarks` table and update the lower bounds for all watermarks
/// if the entry exceeds epoch-level retention policy.
async fn update_watermarks_lower_bounds_task(
    store: PgIndexerStore,
    retention_policies: HashMap<PrunableTable, u64>,
    cancel: CancellationToken,
) -> IndexerResult<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Pruner watermark lower bound update task cancelled.");
                return Ok(());
            }
            _ = interval.tick() => {
                update_watermarks_lower_bounds(&store, &retention_policies, &cancel).await?;
            }
        }
    }
}

/// Fetches all entries from the `watermarks` table, and updates the `reader_lo` for each entry if
/// its epoch range exceeds the respective retention policy.
async fn update_watermarks_lower_bounds(
    store: &PgIndexerStore,
    retention_policies: &HashMap<PrunableTable, u64>,
    cancel: &CancellationToken,
) -> IndexerResult<()> {
    let (watermarks, latest_db_timestamp) = store.get_watermarks().await?;
    let mut lower_bound_updates = vec![];

    for watermark in watermarks.iter() {
        if cancel.is_cancelled() {
            info!("Pruner watermark lower bound update task cancelled.");
            return Ok(());
        }

        let Some(watermark) = PrunableWatermark::new(watermark.clone(), latest_db_timestamp) else {
            continue;
        };

        let Some(epochs_to_keep) = retention_policies.get(&watermark.entity) else {
            continue;
        };

        if watermark.epoch_lo + epochs_to_keep <= watermark.epoch_hi_inclusive {
            let new_epoch_lower_bound = watermark
                .epoch_hi_inclusive
                .saturating_sub(epochs_to_keep - 1);

            lower_bound_updates.push((watermark.entity, new_epoch_lower_bound));
        }
    }

    if !lower_bound_updates.is_empty() {
        store
            .update_watermarks_lower_bound(lower_bound_updates)
            .await?;
        info!("Finished updating lower bounds for watermarks");
    }

    Ok(())
}

async fn prune_table(store: &PgIndexerStore, watermark: &PrunableWatermark) -> IndexerResult<()> {
    // if pruner_lo is not set yet, we can't naively set it to reader_lo - 1; what if reader_lo is 0
    let pruner_hi_inclusive = match watermark.pruner_lo {
        Some(pruner_lo) if pruner_lo < watermark.reader_lo - 1 => pruner_lo,
        _ => {
            tokio::time::sleep(Duration::from_millis(watermark.prune_delay(1000))).await;
            watermark.reader_lo - 1
        }
    };

    // prune up to and including pruner_lo
    match watermark.entity {
        PrunableTable::ObjectsHistory | PrunableTable::Transactions | PrunableTable::Events => {}
        PrunableTable::EventEmitPackage
        | PrunableTable::EventEmitModule
        | PrunableTable::EventSenders
        | PrunableTable::EventStructInstantiation
        | PrunableTable::EventStructModule
        | PrunableTable::EventStructName
        | PrunableTable::EventStructPackage => {
            store
                .prune_event_indices_table(watermark.pruner_lo(), pruner_hi_inclusive)
                .await?;
        }
        PrunableTable::TxAffectedAddresses
        | PrunableTable::TxAffectedObjects
        | PrunableTable::TxCallsPkg
        | PrunableTable::TxCallsMod
        | PrunableTable::TxCallsFun
        | PrunableTable::TxChangedObjects
        | PrunableTable::TxDigests
        | PrunableTable::TxInputObjects
        | PrunableTable::TxKinds
        | PrunableTable::TxRecipients
        | PrunableTable::TxSenders => {
            store
                .prune_tx_indices_table(watermark.pruner_lo(), pruner_hi_inclusive)
                .await?;
        }
        PrunableTable::Checkpoints => {
            store
                .prune_cp_tx_table(watermark.pruner_lo(), pruner_hi_inclusive)
                .await?;
        }
        PrunableTable::PrunerCpWatermark => {
            store
                .prune_cp_tx_table(watermark.pruner_lo(), pruner_hi_inclusive)
                .await?;
        }
    }

    // and then update pruner_lo to reader_lo - 1
    store
        .update_watermark_latest_pruned(watermark.entity.clone(), pruner_hi_inclusive)
        .await?;

    // start pruning

    Ok(())
}

async fn supervisor(
    mut work_tx: mpsc::Sender<PruningWorkItem>,
    cancel: CancellationToken,
    store: PgIndexerStore,
) -> IndexerResult<()> {
    info!("Pruning supervisor started");
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Fetch watermarks
                // do the filtering here, and send any tables that need pruning to the worker pool
                // hmm ... maybe this can orchestrate (1) updating watermarks work and (2) pruning work
                // let conn = db_pool.get().expect("Failed to get DB connection");
                // let new_tasks = generate_pruning_tasks(&conn);

                // basically, fetch, check if update data range
                // then send the db timestamp, reader_lo, and pruner_lo to the task
                let (watermarks, latest_db_timestamp) = store.get_watermarks().await?;

                // maybe we shoudl think of pruner_lo as pruner_hi, exclusive
                if let Some(pruner_hi) = watermark.pruner_lo {

                }

                let new_tasks = vec![];
                for task in new_tasks {
                    if let Err(e) = work_tx.send(task).await {
                        error!("[Supervisor] Failed to send work: {}", e);
                    }
                }
            },
            _ = cancel.cancelled() => {
                info!("[Supervisor] Shutting down...");
                break;
            }
        }
    }

    Ok(())
}

async fn worker(
    id: usize,
    mut work_rx: mpsc::Receiver<PruningWorkItem>,
    cancel: CancellationToken,
    store: PgIndexerStore,
) {
    info!("[Worker {}] Starting...", id);
    loop {
        tokio::select! {
            Some(task) = work_rx.recv() => {
                // call prune_table() basically
            }
            _ = cancel.cancelled() => {
                info!("[Worker {}] Shutting down...", id);
                break;
            }
        }
    }
}

/*


impl PruningPool {
    async fn new(worker_count: usize, db_pool: Pool<ConnectionManager<PgConnection>>) -> Self {
        let (work_tx, work_rx) = mpsc::channel(100);
        let (result_tx, result_rx) = mpsc::channel(100);
        let cancellation_token = CancellationToken::new();

        let db_pool = Arc::new(db_pool);

        for id in 0..worker_count {
            let work_rx = work_rx.clone();
            let result_tx = result_tx.clone();
            let worker_cancellation_token = cancellation_token.clone();
            let worker_db_pool = db_pool.clone();
            tokio::spawn(async move {
                worker(id, work_rx, result_tx, worker_cancellation_token, worker_db_pool).await;
            });
        }

        let supervisor_cancellation_token = cancellation_token.clone();
        let supervisor_work_tx = work_tx.clone();
        let supervisor_db_pool = db_pool.clone();
        tokio::spawn(async move {
            supervisor(supervisor_work_tx, result_rx, supervisor_cancellation_token, supervisor_db_pool).await;
        });

        Self {
            work_tx,
            cancellation_token,
        }
    }

    async fn shutdown(&self) {
        self.cancellation_token.cancel();
    }
}

impl PruningTask {
    async fn execute(&self, conn: &mut PgConnection) -> Result<(), Box<dyn std::error::Error>> {
        match self.table {
            PrunableTable::Table1 => {
                use schema::table1::dsl::*;
                diesel::delete(table1)
                    .filter(id.between(self.min, self.max))
                    .execute(conn)?;
            },
            PrunableTable::Table2 => {
                // Similar implementation for Table2
            },
            // Handle other tables...
        }
        Ok(())
    }
}

fn generate_pruning_tasks(conn: &PgConnection) -> Vec<PruningTask> {
    // Implement logic to read from watermarks table and generate tasks
    // This is just a placeholder implementation
    vec![
        PruningTask { table: PrunableTable::Table1, min: 0, max: 1000 },
        PruningTask { table: PrunableTable::Table2, min: 0, max: 500 },
    ]
}


use tokio::sync::{mpsc, CancellationToken};
use std::sync::Arc;
use tokio::time::{Duration, interval};
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use log::{info, error};

// ... (previous code for PruningTask, PrunableTable, etc.)

async fn supervisor(
    work_tx: mpsc::Sender<PruningTask>,
    cancellation_token: CancellationToken,
    db_pool: Arc<Pool<ConnectionManager<PgConnection>>>,
) {
    info!("[Supervisor] Starting...");
    let mut interval = interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let conn = match db_pool.get() {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("[Supervisor] Failed to get DB connection: {}", e);
                        continue;
                    }
                };
                let new_tasks = generate_granular_pruning_tasks(&conn);
                for task in new_tasks {
                    if let Err(e) = work_tx.send(task).await {
                        error!("[Supervisor] Failed to send work: {}", e);
                    }
                }
            }
            _ = cancellation_token.cancelled() => {
                info!("[Supervisor] Shutting down...");
                break;
            }
        }
    }
}

fn generate_granular_pruning_tasks(conn: &PgConnection) -> Vec<PruningTask> {
    let mut tasks = Vec::new();
    let watermarks = get_watermarks(conn);  // Implement this function to fetch current watermarks

    for watermark in watermarks {
        let table_size = get_table_size(conn, &watermark.table);  // Implement this function
        let chunk_size = determine_chunk_size(table_size);  // Implement this function

        let mut current_min = watermark.min;
        while current_min < watermark.max {
            let current_max = std::cmp::min(current_min + chunk_size, watermark.max);
            tasks.push(PruningTask {
                table: watermark.table,
                min: current_min,
                max: current_max,
                priority: calculate_priority(&watermark, table_size),  // Implement this function
            });
            current_min = current_max + 1;
        }
    }

    // Sort tasks by priority
    tasks.sort_by(|a, b| b.priority.cmp(&a.priority));

    tasks
}

// Add these fields to PruningTask
struct PruningTask {
    table: PrunableTable,
    min: i64,
    max: i64,
    priority: u32,
}

// Implement this struct and related functions
struct Watermark {
    table: PrunableTable,
    min: i64,
    max: i64,
}

fn get_watermarks(conn: &PgConnection) -> Vec<Watermark> {
    // Implement fetching of current watermarks from the database
    unimplemented!()
}

fn get_table_size(conn: &PgConnection, table: &PrunableTable) -> i64 {
    // Implement fetching of table size
    unimplemented!()
}

fn determine_chunk_size(table_size: i64) -> i64 {
    // Implement logic to determine appropriate chunk size based on table size
    unimplemented!()
}

fn calculate_priority(watermark: &Watermark, table_size: i64) -> u32 {
    // Implement priority calculation based on various factors
    unimplemented!()
}
*/
