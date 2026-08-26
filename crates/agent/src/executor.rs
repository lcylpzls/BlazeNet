//! 任务执行器：拉版本清单 → 对账 → 多源下载 → 入库/合并 → 上报。
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use anyhow::{Context, Result, anyhow, bail};
use blaze_common::manifest::GameIndex;
use blaze_common::update_plan;
use blaze_proto::control::{ChunkDone, PeerQuery, Task};
use origin::storage::{GameStore, NodeStore};
use tokio::sync::{Mutex, Semaphore};

use crate::cafe_store::{self, CafeStore};
use crate::config::{Config, NodeType};
use crate::control;
use crate::fetch::{self, PeerTarget};
use crate::update;

/// 任务执行器：按节点类型（IDC/网吧）执行完整同步链路。
#[derive(Clone)]
pub struct TaskExecutor {
    config: Config,
    node_id: u64,
    data_stores: Arc<Mutex<HashMap<u64, NodeStore>>>,
    pack_stores: Arc<Mutex<HashMap<u64, Arc<StdMutex<GameStore>>>>>,
    cafe_stores: Arc<Mutex<HashMap<u64, Arc<CafeStore>>>>,
    game_permits: Arc<Semaphore>,
    source_permits: Arc<Semaphore>,
}

impl TaskExecutor {
    pub fn new(
        config: Config,
        node_id: u64,
        data_stores: Arc<Mutex<HashMap<u64, NodeStore>>>,
        pack_stores: Arc<Mutex<HashMap<u64, Arc<StdMutex<GameStore>>>>>,
        cafe_stores: Arc<Mutex<HashMap<u64, Arc<CafeStore>>>>,
    ) -> Self {
        Self {
            game_permits: Arc::new(Semaphore::new(config.concurrent_games as usize)),
            source_permits: Arc::new(Semaphore::new(config.chunk_concurrency as usize)),
            config,
            node_id,
            data_stores,
            pack_stores,
            cafe_stores,
        }
    }

    /// 同时处理游戏数信号量（任务调度时先获取）。
    pub fn game_permits(&self) -> Arc<Semaphore> {
        self.game_permits.clone()
    }

    /// 执行单个任务；成功返回后由调用方标记完成。
    pub async fn run_task(&self, task: Task) -> Result<()> {
        let Some(addr) = &self.config.control_addr else {
            bail!("未配置控制面地址，无法执行任务");
        };
        let mut client = control::connect(addr).await?;
        control::report_task(&mut client, self.node_id, task.id, "running", "").await?;
        let manifest = control::get_version(&mut client, task.game_id, task.version)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "版本清单不存在: 游戏 {} 版本 {}",
                    task.game_id,
                    task.version
                )
            })?;
        let index = GameIndex::decode(&manifest).context("解析版本清单失败")?;
        let result = match self.config.node_type {
            NodeType::Idc => self.run_idc(&mut client, &task, &index).await,
            NodeType::Cafe => self.run_cafe(&mut client, &task, &index, &manifest).await,
        };
        match result {
            Ok(()) => {
                control::report_task(&mut client, self.node_id, task.id, "done", "").await?;
                Ok(())
            }
            Err(err) => {
                let msg = format!("{err:#}");
                let _ =
                    control::report_task(&mut client, self.node_id, task.id, "failed", &msg).await;
                Err(anyhow!(msg))
            }
        }
    }

    async fn run_idc(
        &self,
        client: &mut blaze_proto::control::control_client::ControlClient<tonic::transport::Channel>,
        task: &Task,
        index: &GameIndex,
    ) -> Result<()> {
        let game_id = task.game_id;
        let store = {
            let mut stores = self.pack_stores.lock().await;
            match stores.entry(game_id) {
                std::collections::hash_map::Entry::Occupied(e) => e.get().clone(),
                std::collections::hash_map::Entry::Vacant(e) => e
                    .insert(Arc::new(StdMutex::new(GameStore::open(
                        &self.config.data_dir,
                        game_id,
                    )?)))
                    .clone(),
            }
        };
        let mut missing = Vec::new();
        for hash in index.chunk_set() {
            let guard = store.lock().expect("块库锁不应被污染");
            if !guard.contains(&hash)? {
                missing.push(hash);
            }
        }
        missing.sort();
        self.download_missing(client, game_id, missing, |hash, data| {
            let mut guard = store.lock().expect("块库锁不应被污染");
            guard.append_chunk(&hash, &data)?;
            Ok(())
        })
        .await?;
        let mut held: Vec<[u8; 32]> = index.chunk_set().into_iter().collect();
        held.sort();
        self.report_chunks(client, game_id, &held).await?;
        Ok(())
    }

    async fn run_cafe(
        &self,
        client: &mut blaze_proto::control::control_client::ControlClient<tonic::transport::Channel>,
        task: &Task,
        index: &GameIndex,
        manifest: &[u8],
    ) -> Result<()> {
        let game_id = task.game_id;
        let game_dir = cafe_store::game_dir(&self.config.data_dir, game_id);
        let temp_dir = cafe_store::temp_dir(&self.config.data_dir, game_id);
        std::fs::create_dir_all(&temp_dir)
            .context(format!("创建临时块目录失败: {}", temp_dir.display()))?;
        let cafe = {
            let mut stores = self.cafe_stores.lock().await;
            match stores.entry(game_id) {
                std::collections::hash_map::Entry::Occupied(e) => e.get().clone(),
                std::collections::hash_map::Entry::Vacant(e) => e
                    .insert(Arc::new(CafeStore::open(&self.config.data_dir, game_id)?))
                    .clone(),
            }
        };
        {
            let mut stores = self.data_stores.lock().await;
            stores
                .entry(game_id)
                .or_insert_with(|| NodeStore::Cafe(cafe.clone()));
        }
        let old_bytes = cafe.current_manifest_bytes()?;
        let old = old_bytes
            .as_deref()
            .map(GameIndex::decode)
            .transpose()
            .context("解析旧版本清单失败")?;
        let temp_hashes = update::collect_temp_hashes(&temp_dir)?;
        let plan = update_plan::compute(index, old.as_ref(), &temp_hashes);
        self.download_missing(
            client,
            game_id,
            plan.chunks_to_download.clone(),
            |hash, data| {
                std::fs::write(temp_dir.join(format!("{}.blk", update::hex(&hash))), data)
                    .context("写入临时块失败")?;
                Ok(())
            },
        )
        .await?;
        update::merge_files(&game_dir, index, old.as_ref(), &temp_dir)?;
        cafe.save_manifest(task.version, manifest)?;
        let mut held: Vec<[u8; 32]> = index.chunk_set().into_iter().collect();
        held.sort();
        self.report_chunks(client, game_id, &held).await?;
        Ok(())
    }

    /// 多源下载：按候选 peer 分组批量拉取，无 peer 时回退原始节点。
    async fn download_missing<F>(
        &self,
        client: &mut blaze_proto::control::control_client::ControlClient<tonic::transport::Channel>,
        game_id: u64,
        missing: Vec<[u8; 32]>,
        mut sink: F,
    ) -> Result<Vec<[u8; 32]>>
    where
        F: FnMut([u8; 32], Vec<u8>) -> Result<()>,
    {
        if missing.is_empty() {
            return Ok(Vec::new());
        }
        let mut groups: Vec<(PeerTarget, Vec<[u8; 32]>)> = Vec::new();
        let mut group_index: HashMap<String, usize> = HashMap::new();
        let mut fallback: Vec<[u8; 32]> = Vec::new();
        for hash in &missing {
            let peers = client
                .query_peers(PeerQuery {
                    game_id,
                    chunk_hash: hash.to_vec(),
                    limit: 5,
                })
                .await?
                .into_inner()
                .peers;
            if let Some(peer) = peers.into_iter().find(|p| !p.addrs.is_empty()) {
                let addr = peer.addrs[0].addr.parse().ok();
                let target = PeerTarget {
                    endpoint_id: peer.endpoint_id.parse().context("候选端点 ID 非法")?,
                    addr,
                    relay_url: self.config.relay_url.clone(),
                    direct_only: peer.direct_only,
                };
                let key = format!(
                    "{}|{}|{}",
                    target.endpoint_id,
                    target.addr.map(|a| a.to_string()).unwrap_or_default(),
                    target.direct_only
                );
                let idx = match group_index.get(&key) {
                    Some(idx) => *idx,
                    None => {
                        let idx = groups.len();
                        groups.push((target, Vec::new()));
                        group_index.insert(key, idx);
                        idx
                    }
                };
                groups[idx].1.push(*hash);
            } else {
                fallback.push(*hash);
            }
        }
        if !fallback.is_empty() {
            match (&self.config.origin_endpoint, &self.config.origin_addr) {
                (Some(endpoint), Some(addr)) => groups.push((
                    PeerTarget {
                        endpoint_id: endpoint.parse().context("原始节点端点 ID 非法")?,
                        addr: Some(addr.parse().context("原始节点地址非法")?),
                        relay_url: self.config.relay_url.clone(),
                        direct_only: false,
                    },
                    fallback,
                )),
                _ => bail!(
                    "块无候选源且未配置原始节点: 游戏 {game_id} 缺 {} 块",
                    fallback.len()
                ),
            }
        }
        let ep = fetch::build_endpoint(self.config.relay_url.as_deref()).await?;
        let mut downloaded = Vec::new();
        let mut failed: Vec<[u8; 32]> = Vec::new();
        for (target, hashes) in groups {
            let _permit = self.source_permits.acquire().await;
            let stats = fetch::fetch_chunks(&ep, &target, game_id, &hashes, |hash, data| {
                sink(hash, data)?;
                downloaded.push(hash);
                Ok(())
            })
            .await?;
            failed.extend(stats.failed);
        }
        if !failed.is_empty() {
            bail!("仍有 {} 块下载失败", failed.len());
        }
        Ok(downloaded)
    }

    async fn report_chunks(
        &self,
        client: &mut blaze_proto::control::control_client::ControlClient<tonic::transport::Channel>,
        game_id: u64,
        hashes: &[[u8; 32]],
    ) -> Result<()> {
        for hash in hashes {
            client
                .report_chunk(ChunkDone {
                    node_id: self.node_id,
                    game_id,
                    chunk_hash: hash.to_vec(),
                    size: 0,
                })
                .await
                .context("上报块完成失败")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blaze_common::manifest::{ChunkMeta, FileEntry};
    use origin::datapath;
    use origin::storage::GameStore;
    use scheduler::db::{AddrRecord, NodeRecord, Store, TaskRecord};
    use scheduler::server::{ControlService, ServerHandle, serve};
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    fn hash_of(data: &[u8]) -> [u8; 32] {
        blake3::hash(data).into()
    }

    fn manifest(files: &[(&str, &[u8])]) -> Vec<u8> {
        let entries = files
            .iter()
            .map(|(name, data)| {
                let hash = hash_of(data);
                FileEntry {
                    name: name.to_string(),
                    file_hash: hash,
                    chunks: vec![ChunkMeta {
                        hash,
                        len: data.len() as u32,
                    }],
                }
            })
            .collect();
        GameIndex::build(entries).encode().unwrap()
    }

    async fn scheduler_setup(dir: &Path) -> (String, ControlService, ServerHandle, Arc<Store>) {
        let store = Arc::new(Store::open(dir).unwrap());
        let service = ControlService::new(store.clone());
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let svc = service.clone();
        let handle = serve(addr, svc).await.unwrap();
        (format!("http://{addr}"), service, handle, store)
    }

    async fn seed_server(
        data_dir: &Path,
        game_id: u64,
    ) -> (datapath::DataPathHandle, Vec<[u8; 32]>) {
        let mut store = GameStore::open(data_dir, game_id).unwrap();
        let d1 = b"hello".to_vec();
        let d2 = b"world".to_vec();
        let h1 = hash_of(&d1);
        let h2 = hash_of(&d2);
        store.append_chunk(&h1, &d1).unwrap();
        store.append_chunk(&h2, &d2).unwrap();
        let pack = Arc::new(StdMutex::new(store));
        let pack_stores: Arc<Mutex<HashMap<u64, Arc<StdMutex<GameStore>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        pack_stores.lock().await.insert(game_id, pack.clone());
        let data_stores: Arc<Mutex<HashMap<u64, NodeStore>>> = Arc::new(Mutex::new(HashMap::new()));
        data_stores
            .lock()
            .await
            .insert(game_id, NodeStore::Pack(pack));
        let handle = datapath::serve(
            data_stores,
            pack_stores,
            data_dir.to_path_buf(),
            0,
            None,
            None,
            true,
        )
        .await
        .unwrap();
        (handle, vec![h1, h2])
    }

    fn config(
        node_type: NodeType,
        data_dir: PathBuf,
        control_addr: String,
        origin_endpoint: Option<String>,
        origin_addr: Option<String>,
    ) -> Config {
        Config {
            node_type,
            data_dir,
            concurrent_games: 2,
            chunk_concurrency: 2,
            disk_free_threshold: 1024,
            compact_threshold: 0.3,
            listen_port: 0,
            temp_ttl_hours: 24,
            origin_endpoint,
            origin_addr,
            keepalive_port: None,
            relay_url: None,
            external_addr: None,
            stun_addr: None,
            control_addr: Some(control_addr),
        }
    }

    type DataStoreMap = Arc<Mutex<HashMap<u64, NodeStore>>>;
    type PackStoreMap = Arc<Mutex<HashMap<u64, Arc<StdMutex<GameStore>>>>>;
    type CafeStoreMap = Arc<Mutex<HashMap<u64, Arc<CafeStore>>>>;

    fn executor(
        cfg: Config,
        node_id: u64,
    ) -> (TaskExecutor, DataStoreMap, PackStoreMap, CafeStoreMap) {
        let data_stores: DataStoreMap = Arc::new(Mutex::new(HashMap::new()));
        let pack_stores: PackStoreMap = Arc::new(Mutex::new(HashMap::new()));
        let cafe_stores: CafeStoreMap = Arc::new(Mutex::new(HashMap::new()));
        let exec = TaskExecutor::new(
            cfg,
            node_id,
            data_stores.clone(),
            pack_stores.clone(),
            cafe_stores.clone(),
        );
        (exec, data_stores, pack_stores, cafe_stores)
    }

    fn task(id: u64, game_id: u64, version: u64) -> Task {
        Task {
            id,
            game_id,
            version,
            kind: 1,
            assigned_chunks: vec![],
        }
    }

    #[tokio::test]
    async fn test_idc_task_mixed_peer_and_origin() {
        let dir = std::env::temp_dir().join("blaze-exec-idc");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let seed_dir = dir.join("seed");
        let idc_dir = dir.join("idc");
        std::fs::create_dir_all(&seed_dir).unwrap();
        std::fs::create_dir_all(&idc_dir).unwrap();
        let (handle, hashes) = seed_server(&seed_dir, 1).await;
        let bytes = manifest(&[("dir/a.bin", b"hello"), ("dir/b.bin", b"world")]);
        let (url, service, _srv, store) = scheduler_setup(&dir.join("sched")).await;
        store.save_version(1, 1, &bytes).unwrap();
        let peer_id = 2u64;
        store
            .insert_node(&NodeRecord {
                id: peer_id,
                node_type: "idc".to_string(),
                endpoint_id: handle.endpoint_id().to_string(),
                token: "peer".to_string(),
                addrs: vec![AddrRecord {
                    addr: format!("127.0.0.1:{}", handle.port()),
                    kind: "config".to_string(),
                    link: String::new(),
                }],
                status: "online".to_string(),
                last_heartbeat_ms: 1,
            })
            .unwrap();
        store.record_chunk_holder(peer_id, 1, &hashes[0]).unwrap();
        service
            .push_task(TaskRecord {
                id: 1,
                node_id: 1,
                game_id: 1,
                version: 1,
                kind: "UPDATE".to_string(),
                assigned_chunks: vec![],
                status: "queued".to_string(),
                error: String::new(),
            })
            .await
            .unwrap();
        service
            .push_task(TaskRecord {
                id: 2,
                node_id: 1,
                game_id: 1,
                version: 1,
                kind: "UPDATE".to_string(),
                assigned_chunks: vec![],
                status: "queued".to_string(),
                error: String::new(),
            })
            .await
            .unwrap();
        let (exec, _data, pack_stores, _cafe) = executor(
            config(
                NodeType::Idc,
                idc_dir.clone(),
                url,
                Some(handle.endpoint_id().to_string()),
                Some(format!("127.0.0.1:{}", handle.port())),
            ),
            1,
        );
        exec.run_task(task(1, 1, 1)).await.unwrap();
        exec.run_task(task(2, 1, 1)).await.unwrap();

        let local = pack_stores.lock().await.get(&1).unwrap().clone();
        {
            let guard = local.lock().unwrap();
            assert!(guard.contains(&hashes[0]).unwrap());
            assert!(guard.contains(&hashes[1]).unwrap());
        }
        let tasks = store.tasks_for_node(1).unwrap();
        assert_eq!(tasks[0].status, "done");
        assert!(store.chunk_holders(1, &hashes[0]).unwrap().contains(&1));
        assert!(store.chunk_holders(1, &hashes[1]).unwrap().contains(&1));
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_cafe_task_full_flow() {
        let dir = std::env::temp_dir().join("blaze-exec-cafe");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let seed_dir = dir.join("seed");
        let cafe_dir = dir.join("cafe");
        std::fs::create_dir_all(&seed_dir).unwrap();
        std::fs::create_dir_all(&cafe_dir).unwrap();
        let (handle, hashes) = seed_server(&seed_dir, 1).await;
        let bytes = manifest(&[("dir/a.bin", b"hello"), ("dir/b.bin", b"world")]);
        let (url, service, _srv, store) = scheduler_setup(&dir.join("sched")).await;
        store.save_version(1, 1, &bytes).unwrap();
        store
            .insert_node(&NodeRecord {
                id: 2,
                node_type: "idc".to_string(),
                endpoint_id: handle.endpoint_id().to_string(),
                token: "peer".to_string(),
                addrs: vec![AddrRecord {
                    addr: format!("127.0.0.1:{}", handle.port()),
                    kind: "config".to_string(),
                    link: String::new(),
                }],
                status: "online".to_string(),
                last_heartbeat_ms: 1,
            })
            .unwrap();
        for hash in &hashes {
            store.record_chunk_holder(2, 1, hash).unwrap();
        }
        service
            .push_task(TaskRecord {
                id: 1,
                node_id: 1,
                game_id: 1,
                version: 1,
                kind: "UPDATE".to_string(),
                assigned_chunks: vec![],
                status: "queued".to_string(),
                error: String::new(),
            })
            .await
            .unwrap();
        service
            .push_task(TaskRecord {
                id: 2,
                node_id: 1,
                game_id: 1,
                version: 1,
                kind: "UPDATE".to_string(),
                assigned_chunks: vec![],
                status: "queued".to_string(),
                error: String::new(),
            })
            .await
            .unwrap();
        let (exec, _data, _pack, cafe_stores) =
            executor(config(NodeType::Cafe, cafe_dir.clone(), url, None, None), 1);
        exec.run_task(task(1, 1, 1)).await.unwrap();
        exec.run_task(task(2, 1, 1)).await.unwrap();

        assert_eq!(
            std::fs::read(cafe_dir.join("games/1/dir/a.bin")).unwrap(),
            b"hello"
        );
        assert_eq!(
            std::fs::read(cafe_dir.join("games/1/dir/b.bin")).unwrap(),
            b"world"
        );
        let temp = crate::cafe_store::temp_dir(&cafe_dir, 1);
        for hash in &hashes {
            assert!(
                temp.join(format!("{}.blk", crate::update::hex(hash)))
                    .exists()
            );
        }
        let cafe = cafe_stores.lock().await.get(&1).unwrap().clone();
        assert_eq!(cafe.current_version().unwrap(), Some(1));
        assert_eq!(store.tasks_for_node(1).unwrap()[0].status, "done");
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_task_no_source_and_missing_version() {
        let dir = std::env::temp_dir().join("blaze-exec-err");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cafe_dir = dir.join("cafe");
        std::fs::create_dir_all(&cafe_dir).unwrap();
        let bytes = manifest(&[("a.bin", b"hello")]);
        let (url, _service, _srv, store) = scheduler_setup(&dir.join("sched")).await;
        store.save_version(1, 1, &bytes).unwrap();
        let (exec, _data, _pack, _cafe) = executor(
            config(NodeType::Cafe, cafe_dir.clone(), url.clone(), None, None),
            1,
        );
        let _ = exec.game_permits();
        let err = exec.run_task(task(1, 1, 1)).await.unwrap_err();
        assert!(err.to_string().contains("无候选源"));

        let (exec2, _data2, _pack2, _cafe2) =
            executor(config(NodeType::Cafe, cafe_dir, url, None, None), 1);
        let err2 = exec2.run_task(task(2, 1, 99)).await.unwrap_err();
        assert!(err2.to_string().contains("版本清单不存在"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_run_task_without_control_addr() {
        let dir = std::env::temp_dir().join("blaze-exec-noctl");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut cfg = config(
            NodeType::Idc,
            dir.join("data"),
            "http://127.0.0.1:1".to_string(),
            None,
            None,
        );
        cfg.control_addr = None;
        let (exec, _data, _pack, _cafe) = executor(cfg, 1);
        let err = exec.run_task(task(1, 1, 1)).await.unwrap_err();
        assert!(err.to_string().contains("未配置控制面地址"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_idc_open_error() {
        let dir = std::env::temp_dir().join("blaze-exec-open");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let data_file = dir.join("data-file");
        std::fs::write(&data_file, b"x").unwrap();
        let bytes = manifest(&[("a.bin", b"hello")]);
        let (url, _service, _srv, store) = scheduler_setup(&dir.join("sched")).await;
        store.save_version(1, 1, &bytes).unwrap();
        let (exec, _data, _pack, _cafe) = executor(
            config(
                NodeType::Idc,
                data_file,
                url,
                Some("ep".to_string()),
                Some("127.0.0.1:42001".to_string()),
            ),
            1,
        );
        let err = exec.run_task(task(1, 1, 1)).await.unwrap_err();
        assert!(err.to_string().contains("创建游戏目录失败"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_failed_download_reported() {
        let dir = std::env::temp_dir().join("blaze-exec-fail");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let idc_dir = dir.join("idc");
        let seed_dir = dir.join("seed");
        std::fs::create_dir_all(&idc_dir).unwrap();
        std::fs::create_dir_all(&seed_dir).unwrap();
        // 故意把错误数据挂到目标哈希下：数据面返回内容与哈希不符。
        let wrong_hash = hash_of(b"hello");
        let mut store = GameStore::open(&seed_dir, 1).unwrap();
        store.append_chunk(&wrong_hash, b"WRONG").unwrap();
        let pack = Arc::new(StdMutex::new(store));
        let pack_stores: Arc<Mutex<HashMap<u64, Arc<StdMutex<GameStore>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        pack_stores.lock().await.insert(1, pack.clone());
        let data_stores: Arc<Mutex<HashMap<u64, NodeStore>>> = Arc::new(Mutex::new(HashMap::new()));
        data_stores.lock().await.insert(1, NodeStore::Pack(pack));
        let handle = datapath::serve(data_stores, pack_stores, seed_dir, 0, None, None, true)
            .await
            .unwrap();
        let bytes = manifest(&[("a.bin", b"hello")]);
        let (url, _service, _srv, store) = scheduler_setup(&dir.join("sched")).await;
        store.save_version(1, 1, &bytes).unwrap();
        store
            .insert_node(&NodeRecord {
                id: 2,
                node_type: "idc".to_string(),
                endpoint_id: handle.endpoint_id().to_string(),
                token: "bad".to_string(),
                addrs: vec![AddrRecord {
                    addr: format!("127.0.0.1:{}", handle.port()),
                    kind: "config".to_string(),
                    link: String::new(),
                }],
                status: "online".to_string(),
                last_heartbeat_ms: 1,
            })
            .unwrap();
        store.record_chunk_holder(2, 1, &wrong_hash).unwrap();
        let (exec, _data, _pack, _cafe) =
            executor(config(NodeType::Idc, idc_dir, url, None, None), 1);
        let err = exec.run_task(task(1, 1, 1)).await.unwrap_err();
        assert!(err.to_string().contains("下载失败"));
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
