use std::{
    future::Future,
    time::{Duration, Instant},
};

use engine_traits::{KvEngine, RaftEngine};
use futures::executor::block_on;
use futures_util::{compat::Future01CompatExt, future::BoxFuture};
use kvproto::{
    kvrpcpb::{ReadIndexRequest, ReadIndexResponse},
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request as RaftRequest},
};
use raftstore::{
    router::RaftStoreRouter,
    store::{Callback, RaftCmdExtraOpts, RaftRouter, ReadResponse},
};
use tikv_util::{debug, error, future::paired_future_callback};

use super::utils::ArcNotifyWaker;
use raftstore::store::fsm::ApplyRouter;
use raftstore::store::fsm::apply::Msg;
use raftstore::store::fsm::ApplyTask;

pub trait ApplyRouterHelper: Sync + Send{
    fn schedule_compact_log_task(&self, region_id: u64, compact_index: u64, compact_term: u64, applied_index: u64);
}

pub struct ProxyApplyRouterHelper<EK: KvEngine> {
    pub apply_router: std::sync::Mutex<ApplyRouter<EK>>
}

impl<EK: KvEngine> ProxyApplyRouterHelper<EK> {
    pub fn new(apply_router: ApplyRouter<EK>) -> Self {
        Self { apply_router: std::sync::Mutex::new(apply_router.clone()) }
    }
}

impl<EK: KvEngine> ApplyRouterHelper for ProxyApplyRouterHelper<EK> {
    fn schedule_compact_log_task(&self, region_id: u64, compact_index: u64, compact_term: u64, applied_index: u64) {
        self.apply_router
        .lock()
        .unwrap()
        .schedule_task(region_id, ApplyTask::CheckCompact {
            region_id: region_id,
            voter_replicated_index: compact_index,
            voter_replicated_term: compact_term,
            applied_index: applied_index,
        })
    }
}

