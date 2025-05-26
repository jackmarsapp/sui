// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::env;

use anyhow::Result;
use futures::future::try_join_all;
use mysten_metrics::spawn_monitored_task;
use prometheus::Registry;
use sui_data_ingestion_core::{
    DataIngestionMetrics, IndexerExecutor, ReaderOptions, ShimIndexerProgressStore, WorkerPool,
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::build_json_rpc_server;
use crate::config::{IngestionConfig, JsonRpcConfig, RetentionConfig, SnapshotLagConfig};
use crate::database::ConnectionPool;
use crate::errors::IndexerError;
use crate::handlers::checkpoint_handler::new_handlers;
use crate::handlers::objects_snapshot_handler::start_objects_snapshot_handler;
use crate::handlers::pruner::Pruner;
use crate::indexer_reader::IndexerReader;
use crate::metrics::IndexerMetrics;
use crate::store::{IndexerStore, PgIndexerStore};
use tokio::net::UnixListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::path::Path;
use std::sync::Arc;
use sui_json_rpc_types::{DynamicFieldPage, Page};
use sui_types::base_types::ObjectID;
use sui_json_rpc_api::{cap_page_limit, QUERY_MAX_RESULT_LIMIT};

pub struct Indexer;

impl Indexer {
    pub async fn start_writer(
        config: IngestionConfig,
        store: PgIndexerStore,
        metrics: IndexerMetrics,
        snapshot_config: SnapshotLagConfig,
        retention_config: Option<RetentionConfig>,
        cancel: CancellationToken,
    ) -> Result<(), IndexerError> {
        info!(
            "Sui Indexer Writer (version {:?}) started...",
            env!("CARGO_PKG_VERSION")
        );
        info!("Sui Indexer Writer config: {config:?}",);

        let extra_reader_options = ReaderOptions {
            batch_size: config.checkpoint_download_queue_size,
            timeout_secs: config.checkpoint_download_timeout,
            data_limit: config.checkpoint_download_queue_size_bytes,
            gc_checkpoint_files: config.gc_checkpoint_files,
            ..Default::default()
        };

        // Start objects snapshot processor, which is a separate pipeline with its ingestion pipeline.
        let (object_snapshot_worker, object_snapshot_watermark) = start_objects_snapshot_handler(
            store.clone(),
            metrics.clone(),
            snapshot_config,
            cancel.clone(),
            config.start_checkpoint,
            config.end_checkpoint,
        )
        .await?;

        if let Some(retention_config) = retention_config {
            let pruner = Pruner::new(store.clone(), retention_config, metrics.clone())?;
            let cancel_clone = cancel.clone();
            spawn_monitored_task!(pruner.start(cancel_clone));
        }

        // If we already have chain identifier indexed (i.e. the first checkpoint has been indexed),
        // then we persist protocol configs for protocol versions not yet in the db.
        // Otherwise, we would do the persisting in `commit_checkpoint` while the first cp is
        // being indexed.
        if let Some(chain_id) = IndexerStore::get_chain_identifier(&store).await? {
            store
                .persist_protocol_configs_and_feature_flags(chain_id)
                .await?;
        }

        let mut exit_senders = vec![];
        let mut executors = vec![];

        let (worker, primary_watermark) = new_handlers(
            store,
            metrics,
            cancel.clone(),
            config.start_checkpoint,
            config.end_checkpoint,
        )
        .await?;
        // Ingestion task watermarks are snapshotted once on indexer startup based on the
        // corresponding watermark table before being handed off to the ingestion task.
        let progress_store = ShimIndexerProgressStore::new(
            vec![
                ("primary".to_string(), primary_watermark),
                ("object_snapshot".to_string(), object_snapshot_watermark),
            ]
            .into_iter()
            .collect(),
        );
        let mut executor = IndexerExecutor::new(
            progress_store.clone(),
            2,
            DataIngestionMetrics::new(&Registry::new()),
        );

        let worker_pool = WorkerPool::new(
            worker,
            "primary".to_string(),
            config.checkpoint_download_queue_size,
        );
        executor.register(worker_pool).await?;
        let (exit_sender, exit_receiver) = oneshot::channel();
        executors.push((executor, exit_receiver));
        exit_senders.push(exit_sender);

        // in a non-colocated setup, start a separate indexer for processing object snapshots
        if config.sources.data_ingestion_path.is_none() {
            let executor = IndexerExecutor::new(
                progress_store,
                1,
                DataIngestionMetrics::new(&Registry::new()),
            );
            let (exit_sender, exit_receiver) = oneshot::channel();
            exit_senders.push(exit_sender);
            executors.push((executor, exit_receiver));
        }

        let worker_pool = WorkerPool::new(
            object_snapshot_worker,
            "object_snapshot".to_string(),
            config.checkpoint_download_queue_size,
        );
        let executor = executors.last_mut().expect("executors is not empty");
        executor.0.register(worker_pool).await?;

        // Spawn a task that links the cancellation token to the exit sender
        spawn_monitored_task!(async move {
            cancel.cancelled().await;
            for exit_sender in exit_senders {
                let _ = exit_sender.send(());
            }
        });

        info!("Starting data ingestion executor...");
        let futures = executors.into_iter().map(|(executor, exit_receiver)| {
            executor.run(
                config
                    .sources
                    .data_ingestion_path
                    .clone()
                    .unwrap_or(tempfile::tempdir().unwrap().into_path()),
                config
                    .sources
                    .remote_store_url
                    .as_ref()
                    .map(|url| url.as_str().to_owned()),
                vec![],
                extra_reader_options.clone(),
                exit_receiver,
            )
        });
        try_join_all(futures).await?;
        Ok(())
    }

    pub async fn start_reader(
        config: &JsonRpcConfig,
        registry: &Registry,
        pool: ConnectionPool,
        cancel: CancellationToken,
    ) -> Result<(), IndexerError> {
        info!(
            "Sui Indexer Reader (version {:?}) started...",
            env!("CARGO_PKG_VERSION")
        );
        let indexer_reader = IndexerReader::new(pool);
        let handle = build_json_rpc_server(registry, indexer_reader, config, cancel)
            .await
            .expect("Json rpc server should not run into errors upon start.");
        tokio::spawn(async move { handle.stopped().await })
            .await
            .expect("Rpc server task failed");

        Ok(())
    }

    pub async fn start_batch_test1(indexer_reader:IndexerReader) -> Result<(), std::io::Error> {
        //开启unix socket,等待执行
        let socket_path = "/tmp/listen_get_dy.sock";
        // 如果 socket 文件已存在，先移除
        if Path::new(socket_path).exists() {
            std::fs::remove_file(socket_path)?;
        }
        let arc_indexer_reader = Arc::new(indexer_reader);
        let listener = UnixListener::bind(socket_path)?;
        println!("Listening on {}", socket_path);

        loop {
            let reader = Arc::clone(&arc_indexer_reader);
            let (mut socket, _) = listener.accept().await?;
            tokio::spawn(async move {
                // 1. 读取4字节长度前缀
                let mut len_buf = [0u8; 4];
                if let Err(e) = socket.read_exact(&mut len_buf).await {
                    eprintln!("Failed to read length prefix: {:?}", e);
                    return;
                }
                let len = u32::from_le_bytes(len_buf) as usize;

                // 2. 读取指定长度的数据
                let mut data_buf = vec![0u8; len];
                if let Err(e) = socket.read_exact(&mut data_buf).await {
                    eprintln!("Failed to read data: {:?}", e);
                    return;
                }

                match bcs::from_bytes::<(ObjectID, Option<ObjectID>, Option<usize>)>(&data_buf) {
                    Ok((parent_object_id, cursor, limit)) => {
                        // 业务逻辑
                        let reply = Self::get_dynamic_fields_custom(parent_object_id,cursor,limit,reader).await.unwrap();
                        let reply_bytes = bcs::to_bytes(&reply).unwrap();
                        let reply_len = reply_bytes.len() as u32;
                        let reply_len_bytes = reply_len.to_le_bytes();

                        // 5. 先写长度，再写内容
                        if let Err(e) = socket.write_all(&reply_len_bytes).await {
                            eprintln!("Failed to write reply length: {:?}", e);
                        }
                        if let Err(e) = socket.write_all(&reply_bytes).await {
                            eprintln!("Failed to write reply struct: {:?}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to deserialize params: {:?}", e);
                        return;
                    }
                }

            });
        }
    }

    pub async fn get_dynamic_fields_custom(
        parent_object_id: ObjectID,
        cursor: Option<ObjectID>,
        limit: Option<usize>,
        indexer_reader:Arc<IndexerReader>
    ) -> RpcResult<DynamicFieldPage> {
        let limit = cap_page_limit(limit);
        if limit == 0 {
            return Ok(DynamicFieldPage::empty());
        }
        let mut results = indexer_reader
            .get_dynamic_fields(parent_object_id, cursor, limit + 1)
            .await?;

        let has_next_page = results.len() > limit;
        results.truncate(limit);
        let next_cursor = results.last().map(|o| o.object_id);
        Ok(Page {
            data: results.into_iter().map(Into::into).collect(),
            next_cursor,
            has_next_page,
        })
    }
}
