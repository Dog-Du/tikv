// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

// Independent Raft gRPC service implementation
use std::{
    mem,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use api_version::KvFormat;
use fail::fail_point;
use futures::{
    compat::Future01CompatExt,
    future::{self, Future, FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::{StreamExt, TryStreamExt},
};
use grpcio::{
    ClientStreamingSink, DuplexSink, Error as GrpcError, RequestStream, Result as GrpcResult,
    RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink, WriteFlags,
};
use health_controller::HealthController;
use kvproto::{coprocessor::*, kvrpcpb::*, mpp::*, raft_serverpb::*, tikvpb::*};
use protobuf::RepeatedField;
use raft::eraftpb::MessageType;
use raftstore::{
    Error as RaftStoreError, Result as RaftStoreResult,
    store::{
        CheckLeaderTask, get_memory_usage_entry_cache,
        memory::{MEMTRACE_APPLYS, MEMTRACE_RAFT_ENTRIES, MEMTRACE_RAFT_MESSAGES},
        metrics::MESSAGE_RECV_BY_STORE,
    },
};
use resource_control::ResourceGroupManager;
use tikv_alloc::trace::MemoryTraceGuard;
use tikv_kv::{RaftExtension, StageLatencyStats};
use tikv_util::{
    future::{paired_future_callback, poll_future_notify},
    mpsc::future::{BatchReceiver, Sender, WakePolicy, unbounded},
    sys::memory_usage_reaches_high_water,
    time::{Instant, nanos_to_secs},
    worker::Scheduler,
};
use tracker::{GLOBAL_TRACKERS, RequestInfo, RequestType, Tracker, set_tls_tracker_token};
use txn_types::{self, Key};

use super::batch::{BatcherBuilder, ReqBatcher};
use crate::{
    coprocessor::Endpoint,
    coprocessor_v2, forward_duplex, forward_unary, log_net_error,
    server::{
        Error, MetadataSourceStoreId, Proxy, Result as ServerResult, gc_worker::GcWorker,
        load_statistics::ThreadLoadPool, metrics::*, service::RaftGrpcMessageFilter,
        snap::Task as SnapTask,
    },
    storage::{
        self, SecondaryLocksStatus, Storage, TxnStatus,
        errors::{
            extract_committed, extract_key_error, extract_key_errors, extract_kv_pairs,
            extract_region_error, extract_region_error_from_error, map_kv_pairs,
        },
        kv::Engine,
        lock_manager::LockManager,
    },
};

#[derive(Clone)]
pub struct RaftService<E: Engine, L: LockManager, R: RaftExtension, F: KvFormat> {
    cluster_id: u64,
    store_id: u64,
    raft_router: R,
    // For handling KV requests.
    storage: Arc<Storage<E, L, F>>,
    // For handling snapshot.
    snap_scheduler: Scheduler<SnapTask>,
    proxy: Proxy,
    raft_message_filter: Arc<dyn RaftGrpcMessageFilter>,
}

impl<E: Engine, L: LockManager, R: RaftExtension, F: KvFormat> RaftService<E, L, R, F> {
    pub fn new(
        cluster_id: u64,
        store_id: u64,
        raft_router: R,
        snap_scheduler: Scheduler<SnapTask>,
        storage: Arc<Storage<E, L, F>>,
        proxy: Proxy,
        raft_message_filter: Arc<dyn RaftGrpcMessageFilter>,
    ) -> Self {
        RaftService {
            cluster_id,
            store_id,
            raft_router,
            storage,
            snap_scheduler,
            proxy,
            raft_message_filter,
        }
    }

    fn get_store_id_from_metadata(ctx: &RpcContext<'_>) -> Option<u64> {
        let metadata = ctx.request_headers();
        for i in 0..metadata.len() {
            let (key, value) = metadata.get(i).unwrap();
            if key == MetadataSourceStoreId::KEY {
                let store_id = MetadataSourceStoreId::parse(value);
                return Some(store_id);
            }
        }
        None
    }

    fn handle_raft_message(
        store_id: u64,
        ch: &E::RaftExtension,
        msg: RaftMessage,
        raft_msg_filter: &Arc<dyn RaftGrpcMessageFilter>,
    ) -> RaftStoreResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();
        if to_store_id != store_id {
            return Err(RaftStoreError::StoreNotMatch {
                to_store_id,
                my_store_id: store_id,
            });
        }

        if raft_msg_filter.should_reject_raft_message(&msg) {
            if msg.get_message().get_msg_type() == MessageType::MsgAppend {
                RAFT_APPEND_REJECTS.inc();
            }
            let id = msg.get_region_id();
            let peer_id = msg.get_message().get_from();
            ch.report_reject_message(id, peer_id);
            return Ok(());
        }

        fail_point!("receive_raft_message_from_outside");
        ch.feed(msg, false);
        Ok(())
    }
}

use kvproto::tikvpb::Tikv;

impl<E: Engine, L: LockManager, R: RaftExtension + Clone + Send + 'static, F: KvFormat> Tikv
    for RaftService<E, L, R, F>
{
    fn raft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<RaftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        println!("raft service raft");
        let source_store_id = Self::get_store_id_from_metadata(&ctx);
        let message_received =
            source_store_id.map(|x| MESSAGE_RECV_BY_STORE.with_label_values(&[&format!("{}", x)]));
        info!(
            "raft RPC is called, new gRPC stream established";
            "source_store_id" => ?source_store_id,
        );

        let store_id = self.store_id;
        let ch = self.storage.get_engine().raft_extension();
        let ob = self.raft_message_filter.clone();

        let res = async move {
            let mut stream = stream.map_err(Error::from);
            while let Some(msg) = stream.try_next().await? {
                RAFT_MESSAGE_RECV_COUNTER.inc();

                if let Err(err @ RaftStoreError::StoreNotMatch { .. }) =
                    Self::handle_raft_message(store_id, &ch, msg, &ob)
                {
                    // Return an error here will break the connection, only do that for
                    // `StoreNotMatch` to let tikv to resolve a correct address from PD
                    return Err(Error::from(err));
                }
                if let Some(ref counter) = message_received {
                    counter.inc();
                }
            }
            Ok::<(), Error>(())
        };

        ctx.spawn(async move {
            let status = match res.await {
                Err(e) => {
                    let msg = format!("{:?}", e);
                    error!("dispatch raft msg from gRPC to raftstore fail"; "err" => %msg);
                    RpcStatus::with_message(RpcStatusCode::UNKNOWN, msg)
                }
                Ok(_) => RpcStatus::new(RpcStatusCode::UNKNOWN),
            };
            let _ = sink
                .fail(status)
                .map_err(|e| error!("KvService::raft send response fail"; "err" => ?e))
                .await;
        });
    }

    fn batch_raft(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<BatchRaftMessage>,
        sink: ClientStreamingSink<Done>,
    ) {
        println!("raft service batch_raft");
        let source_store_id = Self::get_store_id_from_metadata(&ctx);
        let message_received =
            source_store_id.map(|x| MESSAGE_RECV_BY_STORE.with_label_values(&[&format!("{}", x)]));
        info!(
            "batch_raft RPC is called, new gRPC stream established";
            "source_store_id" => ?source_store_id,
        );

        let store_id = self.store_id;
        let ch = self.storage.get_engine().raft_extension();
        let ob = self.raft_message_filter.clone();

        let res = async move {
            let mut stream = stream.map_err(Error::from);
            while let Some(mut batch_msg) = stream.try_next().await? {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                let elapsed = nanos_to_secs(now.saturating_sub(batch_msg.last_observed_time));
                RAFT_MESSAGE_DURATION.receive_delay.observe(elapsed);

                let len = batch_msg.get_msgs().len();
                RAFT_MESSAGE_RECV_COUNTER.inc_by(len as u64);
                RAFT_MESSAGE_BATCH_SIZE.observe(len as f64);

                for msg in batch_msg.take_msgs().into_iter() {
                    if let Err(err @ RaftStoreError::StoreNotMatch { .. }) =
                        Self::handle_raft_message(store_id, &ch, msg, &ob)
                    {
                        // Return an error here will break the connection, only do that for
                        // `StoreNotMatch` to let tikv to resolve a correct address from PD
                        return Err(Error::from(err));
                    }
                }
                if let Some(ref counter) = message_received {
                    counter.inc_by(len as u64);
                }
            }
            Ok::<(), Error>(())
        };

        ctx.spawn(async move {
            let status = match res.await {
                Err(e) => {
                    fail_point!("on_batch_raft_stream_drop_by_err");
                    let msg = format!("{:?}", e);
                    error!("dispatch raft msg from gRPC to raftstore fail"; "err" => %msg);
                    RpcStatus::with_message(RpcStatusCode::UNKNOWN, msg)
                }
                Ok(_) => RpcStatus::new(RpcStatusCode::UNKNOWN),
            };
            let _ = sink
                .fail(status)
                .map_err(|e| error!("KvService::batch_raft send response fail"; "err" => ?e))
                .await;
        });
    }

    fn snapshot(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<SnapshotChunk>,
        sink: ClientStreamingSink<Done>,
    ) {
        println!("raft service snapshot");
        if self.raft_message_filter.should_reject_snapshot() {
            RAFT_SNAPSHOT_REJECTS.inc();
            let status =
                RpcStatus::with_message(RpcStatusCode::UNAVAILABLE, "rejected by peer".to_string());
            ctx.spawn(sink.fail(status).map(|_| ()));
            return;
        };
        let task = SnapTask::Recv { stream, sink };
        if let Err(e) = self.snap_scheduler.schedule(task) {
            let err_msg = format!("{}", e);
            let sink = match e.into_inner() {
                SnapTask::Recv { sink, .. } => sink,
                _ => unreachable!(),
            };
            let status = RpcStatus::with_message(RpcStatusCode::RESOURCE_EXHAUSTED, err_msg);
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    }

    fn tablet_snapshot(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<TabletSnapshotRequest>,
        sink: DuplexSink<TabletSnapshotResponse>,
    ) {
        println!("raft service tablet_snapshot");
        let task = SnapTask::RecvTablet { stream, sink };
        if let Err(e) = self.snap_scheduler.schedule(task) {
            let err_msg = format!("{}", e);
            let sink = match e.into_inner() {
                SnapTask::Recv { sink, .. } => sink,
                _ => unreachable!(),
            };
            let status = RpcStatus::with_message(RpcStatusCode::RESOURCE_EXHAUSTED, err_msg);
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    }

    #[allow(clippy::collapsible_else_if)]
    fn split_region(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: SplitRegionRequest,
        sink: UnarySink<SplitRegionResponse>,
    ) {
        forward_unary!(self.proxy, split_region, ctx, req, sink);
        let begin_instant = Instant::now();

        let region_id = req.get_context().get_region_id();
        let mut split_keys = if req.is_raw_kv {
            if !req.get_split_key().is_empty() {
                vec![F::encode_raw_key_owned(req.take_split_key(), None).into_encoded()]
            } else {
                req.take_split_keys()
                    .into_iter()
                    .map(|x| F::encode_raw_key_owned(x, None).into_encoded())
                    .collect()
            }
        } else {
            if !req.get_split_key().is_empty() {
                vec![Key::from_raw(req.get_split_key()).into_encoded()]
            } else {
                req.take_split_keys()
                    .into_iter()
                    .map(|x| Key::from_raw(&x).into_encoded())
                    .collect()
            }
        };
        split_keys.sort();
        let engine = self.storage.get_engine();
        let f = engine.raft_extension().split(
            region_id,
            req.take_context().take_region_epoch(),
            split_keys,
            ctx.peer(),
        );

        let task = async move {
            let res = f.await;
            let mut resp = SplitRegionResponse::default();
            match res {
                Ok(regions) => {
                    if regions.len() < 2 {
                        error!(
                            "invalid split response";
                            "region_id" => region_id,
                            "resp" => ?regions
                        );
                        resp.mut_region_error().set_message(format!(
                            "Internal Error: invalid response: {:?}",
                            regions
                        ));
                    } else {
                        if regions.len() == 2 {
                            resp.set_left(regions[0].clone());
                            resp.set_right(regions[1].clone());
                        }
                        resp.set_regions(regions.into());
                    }
                }
                Err(e) => {
                    let err: crate::storage::Result<()> = Err(e.into());
                    if let Some(err) = extract_region_error(&err) {
                        resp.set_region_error(err)
                    } else {
                        resp.mut_region_error()
                            .set_message(format!("failed to split: {:?}", err));
                    }
                }
            }
            GRPC_MSG_HISTOGRAM_STATIC
                .split_region
                .unknown
                .observe(begin_instant.saturating_elapsed().as_secs_f64());
            sink.success(resp).await?;
            ServerResult::Ok(())
        }
        .map_err(|e| {
            log_net_error!(e, "kv rpc failed";
                "request" => "split_region"
            );
            GRPC_MSG_FAIL_COUNTER.split_region.inc();
        })
        .map(|_| ());

        ctx.spawn(task);
    }
}

/// Create a gRPC service for Raft-related RPCs
pub fn create_raft_service<
    E: Engine,
    L: LockManager,
    R: RaftExtension + Clone + Send + 'static,
    F: KvFormat,
>(
    service: RaftService<E, L, R, F>,
) -> grpcio::Service {
    use kvproto::tikvpb_grpc::create_tikv;
    create_tikv(service)
}
