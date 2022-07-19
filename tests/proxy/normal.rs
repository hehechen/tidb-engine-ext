// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    io::{self, Read, Write},
    ops::{Deref, DerefMut},
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Once, RwLock,
    },
};

use engine_traits::{
    Error, ExternalSstFileInfo, Iterable, Iterator, MiscExt, Mutable, Peekable, Result, SeekKey,
    SstExt, SstReader, SstWriter, SstWriterBuilder, WriteBatch, WriteBatchExt, CF_DEFAULT, CF_LOCK,
    CF_RAFT, CF_WRITE,
};
use kvproto::{
    raft_cmdpb::{AdminCmdType, AdminRequest},
    raft_serverpb::{RaftApplyState, RegionLocalState, StoreIdent},
};
use new_mock_engine_store::{
    mock_cluster::FFIHelperSet,
    node::NodeCluster,
    transport_simulate::{
        CloneFilterFactory, CollectSnapshotFilter, Direction, RegionPacketFilter,
    },
    Cluster, ProxyConfig, Simulator, TestPdClient,
};
use pd_client::PdClient;
use proxy_server::{
    config::{address_proxy_config, ensure_no_common_unrecognized_keys},
    run::run_tikv_proxy,
};
use raft::eraftpb::MessageType;
use raftstore::{
    coprocessor::{ConsistencyCheckMethod, Coprocessor},
    engine_store_ffi,
    engine_store_ffi::{KVGetStatus, RaftStoreProxyFFI},
    store::util::find_peer,
};
use server::setup::validate_and_persist_config;
use sst_importer::SstImporter;
use test_raftstore::new_tikv_config;
pub use test_raftstore::{must_get_equal, must_get_none, new_peer};
use tikv::config::TiKvConfig;
use tikv_util::{
    config::{LogFormat, ReadableDuration, ReadableSize},
    time::Duration,
    HandyRwLock,
};

use crate::proxy::*;

#[test]
fn test_config() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let text = "memory-usage-high-water=0.65\nsnap-handle-pool-size=4\n[nosense]\nfoo=2\n[rocksdb]\nmax-open-files = 111\nz=1";
    write!(file, "{}", text).unwrap();
    let path = file.path();

    let mut unrecognized_keys = Vec::new();
    let mut config = TiKvConfig::from_file(path, Some(&mut unrecognized_keys)).unwrap();
    assert_eq!(config.memory_usage_high_water, 0.65);
    assert_eq!(config.rocksdb.max_open_files, 111);
    assert_eq!(unrecognized_keys.len(), 3);

    let mut proxy_unrecognized_keys = Vec::new();
    let proxy_config = ProxyConfig::from_file(path, Some(&mut proxy_unrecognized_keys)).unwrap();
    assert_eq!(proxy_config.snap_handle_pool_size, 4);
    let v1 = vec!["a.b", "b"]
        .iter()
        .map(|e| String::from(*e))
        .collect::<Vec<String>>();
    let v2 = vec!["a.b", "b.b", "c"]
        .iter()
        .map(|e| String::from(*e))
        .collect::<Vec<String>>();
    let unknown = ensure_no_common_unrecognized_keys(&v1, &v2);
    assert_eq!(unknown.is_err(), true);
    assert_eq!(unknown.unwrap_err(), "a.b, b.b");
    let unknown = ensure_no_common_unrecognized_keys(&proxy_unrecognized_keys, &unrecognized_keys);
    assert_eq!(unknown.is_err(), true);
    assert_eq!(unknown.unwrap_err(), "nosense, rocksdb.z");

    // Need run this test with ENGINE_LABEL_VALUE=tiflash, otherwise will fatal exit.
    server::setup::validate_and_persist_config(&mut config, true);

    // Will not override ProxyConfig
    let proxy_config_new = ProxyConfig::from_file(path, None).unwrap();
    assert_eq!(proxy_config_new.snap_handle_pool_size, 4);
}

#[test]
fn test_store_setup() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

    // Add label to cluster
    address_proxy_config(&mut cluster.cfg.tikv);

    // Try to start this node, return after persisted some keys.
    let _ = cluster.start();
    let store_id = cluster.engines.keys().last().unwrap();
    let store = pd_client.get_store(*store_id).unwrap();
    println!("store {:?}", store);
    assert!(
        store
            .get_labels()
            .iter()
            .find(|&x| x.key == "engine" && x.value == "tiflash")
            .is_some()
    );

    cluster.shutdown();
}

#[test]
fn test_interaction() {
    // TODO Maybe we should pick this test to TiKV.
    // This test is to check if empty entries can affect pre_exec and post_exec.
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

    fail::cfg("try_flush_data", "return(0)").unwrap();
    let _ = cluster.run();

    cluster.must_put(b"k1", b"v1");
    let region = cluster.get_region(b"k1");
    let region_id = region.get_id();

    // Wait until all nodes have (k1, v1).
    check_key(&cluster, b"k1", b"v1", Some(true), None, None);

    let prev_states = collect_all_states(&cluster, region_id);
    let compact_log = test_raftstore::new_compact_log_request(100, 10);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
    let res = cluster
        .call_command_on_leader(req.clone(), Duration::from_secs(3))
        .unwrap();

    // Empty result can also be handled by post_exec
    let mut retry = 0;
    let new_states = loop {
        let new_states = collect_all_states(&cluster, region_id);
        let mut ok = true;
        for i in prev_states.keys() {
            let old = prev_states.get(i).unwrap();
            let new = new_states.get(i).unwrap();
            if old.in_memory_apply_state == new.in_memory_apply_state
                && old.in_memory_applied_term == new.in_memory_applied_term
            {
                ok = false;
                break;
            }
        }
        if ok {
            break new_states;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        retry += 1;
    };

    for i in prev_states.keys() {
        let old = prev_states.get(i).unwrap();
        let new = new_states.get(i).unwrap();
        assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
        assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
        // An empty cmd will not cause persistence.
        assert_eq!(old.in_disk_apply_state, new.in_disk_apply_state);
    }

    cluster.must_put(b"k2", b"v2");
    // Wait until all nodes have (k2, v2).
    check_key(&cluster, b"k2", b"v2", Some(true), None, None);

    fail::cfg("on_empty_cmd_normal", "return").unwrap();
    let prev_states = collect_all_states(&cluster, region_id);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(400));
    let new_states = collect_all_states(&cluster, region_id);
    for i in prev_states.keys() {
        let old = prev_states.get(i).unwrap();
        let new = new_states.get(i).unwrap();
        assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
        assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
    }

    fail::remove("try_flush_data");
    fail::remove("on_empty_cmd_normal");
    cluster.shutdown();
}

#[test]
fn test_leadership_change_filter() {
    test_leadership_change_impl(true);
}

#[test]
fn test_leadership_change_no_persist() {
    test_leadership_change_impl(false);
}

fn test_leadership_change_impl(filter: bool) {
    // Test if a empty command can be observed when leadership changes.
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

    // Disable compact log, otherwise is may advance and persist apply state after leadership change.
    // This will not totally disable, so we use some failpoints later.
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(1000);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10000);
    cluster.cfg.raft_store.snap_apply_batch_size = ReadableSize(50000);
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;

    if filter {
        // We don't handle CompactLog at all.
        fail::cfg("try_flush_data", "return(0)").unwrap();
    } else {
        // We don't return Persist after handling CompactLog.
        fail::cfg("no_persist_compact_log", "return").unwrap();
    }
    // Do not handle empty cmd.
    fail::cfg("on_empty_cmd_normal", "return").unwrap();
    let _ = cluster.run();

    cluster.must_put(b"k1", b"v1");
    let region = cluster.get_region(b"k1");
    let region_id = region.get_id();

    let eng_ids = cluster
        .engines
        .iter()
        .map(|e| e.0.to_owned())
        .collect::<Vec<_>>();
    let peer_1 = find_peer(&region, eng_ids[0]).cloned().unwrap();
    let peer_2 = find_peer(&region, eng_ids[1]).cloned().unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_1.clone());

    cluster.must_put(b"k2", b"v2");
    fail::cfg("on_empty_cmd_normal", "return").unwrap();

    // Wait until all nodes have (k2, v2), then transfer leader.
    check_key(&cluster, b"k2", b"v2", Some(true), None, None);
    if filter {
        // We should also filter normal kv, since a empty result can also be invoke pose_exec.
        fail::cfg("on_post_exec_normal", "return(false)").unwrap();
    }
    let prev_states = collect_all_states(&cluster, region_id);
    cluster.must_transfer_leader(region.get_id(), peer_2.clone());

    // The states remain the same, since we don't observe empty cmd.
    let new_states = collect_all_states(&cluster, region_id);
    for i in prev_states.keys() {
        let old = prev_states.get(i).unwrap();
        let new = new_states.get(i).unwrap();
        if filter {
            // CompactLog can still change in-memory state, when exec in memory.
            assert_eq!(old.in_memory_apply_state, new.in_memory_apply_state);
            assert_eq!(old.in_memory_applied_term, new.in_memory_applied_term);
        }
        assert_eq!(old.in_disk_apply_state, new.in_disk_apply_state);
    }

    fail::remove("on_empty_cmd_normal");
    // We need forward empty cmd generated by leadership changing to TiFlash.
    cluster.must_transfer_leader(region.get_id(), peer_1.clone());
    std::thread::sleep(std::time::Duration::from_secs(1));

    let new_states = collect_all_states(&cluster, region_id);
    for i in prev_states.keys() {
        let old = prev_states.get(i).unwrap();
        let new = new_states.get(i).unwrap();
        assert_ne!(old.in_memory_apply_state, new.in_memory_apply_state);
        assert_ne!(old.in_memory_applied_term, new.in_memory_applied_term);
    }

    if filter {
        fail::remove("try_flush_data");
        fail::remove("on_post_exec_normal");
    } else {
        fail::remove("no_persist_compact_log");
    }
    cluster.shutdown();
}

#[test]
fn test_kv_write_always_persist() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

    let _ = cluster.run();

    cluster.must_put(b"k0", b"v0");
    let region_id = cluster.get_region(b"k0").get_id();

    let mut prev_states = collect_all_states(&cluster, region_id);
    // Always persist on every command
    fail::cfg("on_post_exec_normal_end", "return(true)").unwrap();
    for i in 1..20 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());

        // We can't always get kv from disk, even we commit everytime,
        // since they are filtered by engint_tiflash
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);

        // This may happen after memory write data and before commit.
        // We must check if we already have in memory.
        check_apply_state(&cluster, region_id, &prev_states, Some(false), None);
        std::thread::sleep(std::time::Duration::from_millis(20));
        // However, advanced apply index will always persisted.
        let new_states = collect_all_states(&cluster, region_id);
        for id in cluster.engines.keys() {
            let p = &prev_states.get(id).unwrap().in_disk_apply_state;
            let n = &new_states.get(id).unwrap().in_disk_apply_state;
            assert_ne!(p, n);
        }
        prev_states = new_states;
    }

    cluster.shutdown();
}

#[test]
fn test_kv_write() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);

    fail::cfg("on_post_exec_normal", "return(false)").unwrap();
    fail::cfg("on_post_exec_admin", "return(false)").unwrap();
    // Abandon CompactLog and previous flush.
    fail::cfg("try_flush_data", "return(0)").unwrap();

    let _ = cluster.run();

    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    // Since we disable all observers, we can get nothing in either memory and disk.
    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(
            &cluster,
            k.as_bytes(),
            v.as_bytes(),
            Some(false),
            Some(false),
            None,
        );
    }

    // We can read initial raft state, since we don't persist meta either.
    let r1 = cluster.get_region(b"k1").get_id();
    let prev_states = collect_all_states(&cluster, r1);

    fail::remove("on_post_exec_normal");
    fail::remove("on_post_exec_admin");
    for i in 10..20 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    // Since we enable all observers, we can get in memory.
    // However, we get nothing in disk since we don't persist.
    for i in 10..20 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(
            &cluster,
            k.as_bytes(),
            v.as_bytes(),
            Some(true),
            Some(false),
            None,
        );
    }

    let new_states = collect_all_states(&cluster, r1);
    for id in cluster.engines.keys() {
        assert_ne!(
            &prev_states.get(id).unwrap().in_memory_apply_state,
            &new_states.get(id).unwrap().in_memory_apply_state
        );
        assert_eq!(
            &prev_states.get(id).unwrap().in_disk_apply_state,
            &new_states.get(id).unwrap().in_disk_apply_state
        );
    }

    std::thread::sleep(std::time::Duration::from_millis(20));
    fail::remove("try_flush_data");

    let prev_states = collect_all_states(&cluster, r1);
    // Write more after we force persist when CompactLog.
    for i in 20..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    // We can read from mock-store's memory, we are not sure if we can read from disk,
    // since there may be or may not be a CompactLog.
    for i in 11..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
    }

    // Force a compact log to persist.
    let region_r = cluster.get_region("k1".as_bytes());
    let region_id = region_r.get_id();
    let compact_log = test_raftstore::new_compact_log_request(1000, 100);
    let req =
        test_raftstore::new_admin_request(region_id, region_r.get_region_epoch(), compact_log);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(res.get_header().has_error(), "{:?}", res);
    // This CompactLog is executed with an error. It will not trigger a compaction.
    // However, it can trigger a persistence.
    for i in 11..30 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(
            &cluster,
            k.as_bytes(),
            v.as_bytes(),
            Some(true),
            Some(true),
            None,
        );
    }

    let new_states = collect_all_states(&cluster, r1);

    // apply_state is changed in memory, and persisted.
    for id in cluster.engines.keys() {
        assert_ne!(
            &prev_states.get(id).unwrap().in_memory_apply_state,
            &new_states.get(id).unwrap().in_memory_apply_state
        );
        assert_ne!(
            &prev_states.get(id).unwrap().in_disk_apply_state,
            &new_states.get(id).unwrap().in_disk_apply_state
        );
    }

    fail::remove("no_persist_compact_log");
    cluster.shutdown();
}

#[test]
fn test_consistency_check() {
    // ComputeHash and VerifyHash shall be filtered.
    let (mut cluster, pd_client) = new_mock_cluster(0, 2);

    cluster.run();

    cluster.must_put(b"k", b"v");
    let region = cluster.get_region("k".as_bytes());
    let region_id = region.get_id();

    let r = new_verify_hash_request(vec![1, 2, 3, 4, 5, 6], 1000);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), r);
    let _ = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    let r = new_verify_hash_request(vec![7, 8, 9, 0], 1000);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), r);
    let _ = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    cluster.must_put(b"k2", b"v2");
    cluster.shutdown();
}

#[test]
fn test_old_compact_log() {
    // If we just return None for CompactLog, the region state in ApplyFsm will change.
    // Because there is no rollback in new implementation.
    // This is a ERROR state.
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    cluster.run();

    // We don't return Persist after handling CompactLog.
    fail::cfg("no_persist_compact_log", "return").unwrap();
    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
    }

    let region = cluster.get_region(b"k1");
    let region_id = region.get_id();
    let prev_state = collect_all_states(&cluster, region_id);
    let (compact_index, compact_term) = get_valid_compact_index(&prev_state);
    let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();

    // Wait for state applys.
    std::thread::sleep(std::time::Duration::from_secs(2));

    let new_state = collect_all_states(&cluster, region_id);
    for i in prev_state.keys() {
        let old = prev_state.get(i).unwrap();
        let new = new_state.get(i).unwrap();
        assert_ne!(
            old.in_memory_apply_state.get_truncated_state(),
            new.in_memory_apply_state.get_truncated_state()
        );
        assert_eq!(
            old.in_disk_apply_state.get_truncated_state(),
            new.in_disk_apply_state.get_truncated_state()
        );
    }

    cluster.shutdown();
}

#[test]
fn test_compact_log() {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    cluster.run();

    cluster.must_put(b"k", b"v");
    let region = cluster.get_region("k".as_bytes());
    let region_id = region.get_id();

    fail::cfg("on_empty_cmd_normal", "return").unwrap();
    fail::cfg("try_flush_data", "return(0)").unwrap();
    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    let prev_state = collect_all_states(&cluster, region_id);

    let (compact_index, compact_term) = get_valid_compact_index(&prev_state);
    let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    // compact index should less than applied index
    assert!(!res.get_header().has_error(), "{:?}", res);

    // CompactLog is filtered, because we can't flush data.
    let new_state = collect_all_states(&cluster, region_id);
    for i in prev_state.keys() {
        let old = prev_state.get(i).unwrap();
        let new = new_state.get(i).unwrap();
        assert_eq!(
            old.in_memory_apply_state.get_truncated_state(),
            new.in_memory_apply_state.get_truncated_state()
        );
        assert_eq!(
            old.in_disk_apply_state.get_truncated_state(),
            new.in_disk_apply_state.get_truncated_state()
        );
    }

    fail::remove("on_empty_cmd_normal");
    fail::remove("try_flush_data");

    let (compact_index, compact_term) = get_valid_compact_index(&new_state);
    let compact_log = test_raftstore::new_compact_log_request(compact_index, compact_term);
    let req = test_raftstore::new_admin_request(region_id, region.get_region_epoch(), compact_log);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(!res.get_header().has_error(), "{:?}", res);

    cluster.must_put(b"kz", b"vz");
    check_key(&cluster, b"kz", b"vz", Some(true), None, None);

    // CompactLog is not filtered
    let new_state = collect_all_states(&cluster, region_id);
    for i in prev_state.keys() {
        let old = prev_state.get(i).unwrap();
        let new = new_state.get(i).unwrap();
        assert_ne!(
            old.in_memory_apply_state.get_truncated_state(),
            new.in_memory_apply_state.get_truncated_state()
        );
    }

    cluster.shutdown();
}
