//! 任务执行器：拉版本清单 → 对账 → 多源下载 → 入库/合并 → 上报。
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

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

/// IDC 下载批量落盘阈值：攒满该数量的块后一次写入 pack + 一次 redb 提交。
const IDC_BATCH_SIZE: usize = 64;
/// IDC 下载待落盘批次类型。
type PendingBatch = Vec<([u8; 32], Vec<u8>)>;

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
        let assigned: HashSet<[u8; 32]> = task
            .assigned_chunks
            .iter()
            .filter_map(|v| <[u8; 32]>::try_from(v.as_slice()).ok())
            .collect();
        let pending: Arc<StdMutex<PendingBatch>> = Arc::new(StdMutex::new(Vec::new()));
        let flush_pending = |pending: &Arc<StdMutex<PendingBatch>>| -> Result<()> {
            let batch = std::mem::take(&mut *pending.lock().expect("块库缓冲锁不应被污染"));
            if batch.is_empty() {
                return Ok(());
            }
            let mut guard = store.lock().expect("块库锁不应被污染");
            guard.append_chunks_batch(&batch)?;
            Ok(())
        };
        self.download_missing(client, game_id, missing, &assigned, |hash, data| {
            let mut batch = pending.lock().expect("块库缓冲锁不应被污染");
            batch.push((hash, data));
            if batch.len() >= IDC_BATCH_SIZE {
                drop(batch);
                flush_pending(&pending)?;
            }
            Ok(())
        })
        .await?;
        flush_pending(&pending)?;
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
        let mut plan = update_plan::compute(index, old.as_ref(), &temp_hashes);
        // 真实文件缺失时（如磁盘写满中断），补齐对应块以便重新合并。
        let mut repair_chunks = HashSet::new();
        for entry in &index.files {
            if !game_dir.join(&entry.name).is_file() {
                for chunk in &entry.chunks {
                    repair_chunks.insert(chunk.hash);
                }
            }
        }
        plan.chunks_to_download.extend(repair_chunks);
        plan.chunks_to_download.sort_unstable();
        plan.chunks_to_download.dedup();
        self.download_missing(
            client,
            game_id,
            plan.chunks_to_download.clone(),
            &HashSet::new(),
            |hash, data| {
                std::fs::write(temp_dir.join(format!("{}.blk", update::hex(&hash))), data)
                    .context("写入临时块失败")?;
                Ok(())
            },
        )
        .await?;
        let merge = update::merge_files(&game_dir, index, old.as_ref(), &temp_dir)?;
        if !merge.failed.is_empty() {
            bail!("合并失败: {}", merge.failed.join("; "));
        }
        cafe.save_manifest(task.version, manifest)?;
        let mut held: Vec<[u8; 32]> = index.chunk_set().into_iter().collect();
        held.sort();
        self.report_chunks(client, game_id, &held).await?;
        Ok(())
    }

    /// 多源下载：按候选 peer 分组批量拉取；只有本节点责任分片内的块才允许回退原始节点。
    async fn download_missing<F>(
        &self,
        client: &mut blaze_proto::control::control_client::ControlClient<tonic::transport::Channel>,
        game_id: u64,
        missing: Vec<[u8; 32]>,
        assigned: &HashSet<[u8; 32]>,
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
                // 多 IP 分流：按块哈希轮询地址，同一节点的多个公网地址可并行。
                let idx = hash[0] as usize % peer.addrs.len();
                let addr = peer.addrs[idx].addr.parse().ok();
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
        let origin_target = match (&self.config.origin_endpoint, &self.config.origin_addr) {
            (Some(endpoint), Some(addr)) => Some(PeerTarget {
                endpoint_id: endpoint.parse().context("原始节点端点 ID 非法")?,
                addr: Some(addr.parse().context("原始节点地址非法")?),
                relay_url: self.config.relay_url.clone(),
                direct_only: false,
            }),
            _ => None,
        };
        // 数据面只用直连端点：relay 只打洞不传数据，且 iroh 配置 relay 会阻塞公网直连。
        let direct_ep = fetch::build_endpoint(None).await?;
        let rate = self.config.download_mbps;
        let started = Instant::now();
        let mut total_bytes = 0u64;
        let mut downloaded = Vec::new();
        let mut retry = Vec::new();
        let mut blocked = Vec::new();
        for hash in fallback {
            if assigned.contains(&hash) {
                retry.push(hash);
            } else {
                blocked.push(hash);
            }
        }
        for (target, hashes) in groups {
            let _permit = self.source_permits.acquire().await;
            let ep = Self::choose_ep(&target, &direct_ep)?;
            match fetch::fetch_chunks(ep, &target, game_id, &hashes, |hash, data| {
                sink(hash, data)?;
                downloaded.push(hash);
                Ok(())
            })
            .await
            {
                Ok(stats) => {
                    Self::throttle(rate, &mut total_bytes, stats.bytes, started).await;
                    for hash in stats.failed {
                        if assigned.contains(&hash) {
                            retry.push(hash);
                        } else {
                            blocked.push(hash);
                        }
                    }
                }
                Err(err) => {
                    println!("候选源连接失败，转入原始节点重试: {err:#}");
                    for hash in hashes {
                        if assigned.contains(&hash) {
                            retry.push(hash);
                        } else {
                            blocked.push(hash);
                        }
                    }
                }
            }
        }
        if !blocked.is_empty() {
            if origin_target.is_none() {
                bail!(
                    "块无候选源且未配置原始节点: 游戏 {game_id} 缺 {} 块",
                    blocked.len()
                );
            }
            bail!(
                "等待其他节点完成责任分片后重试: 游戏 {game_id} 缺 {} 块",
                blocked.len()
            );
        }
        if !retry.is_empty() {
            let Some(target) = origin_target else {
                bail!(
                    "块无候选源且未配置原始节点: 游戏 {game_id} 缺 {} 块",
                    retry.len()
                );
            };
            retry.sort_unstable();
            retry.dedup();
            let stats = fetch::fetch_chunks(&direct_ep, &target, game_id, &retry, |hash, data| {
                sink(hash, data)?;
                downloaded.push(hash);
                Ok(())
            })
            .await?;
            Self::throttle(rate, &mut total_bytes, stats.bytes, started).await;
            if !stats.failed.is_empty() {
                bail!("仍有 {} 块下载失败", stats.failed.len());
            }
        }
        Ok(downloaded)
    }

    fn choose_ep<'a>(
        target: &PeerTarget,
        direct: &'a iroh::Endpoint,
    ) -> Result<&'a iroh::Endpoint> {
        if target.addr.is_some() {
            Ok(direct)
        } else {
            bail!("源无直连地址，无法传输数据（relay 只打洞不传数据）")
        }
    }

    /// 下载限速：按累计字节数维持平均 Mbps 上限。
    async fn throttle(rate: Option<u64>, total_bytes: &mut u64, bytes: u64, started: Instant) {
        if let Some(mbps) = rate {
            *total_bytes += bytes;
            let target_secs = *total_bytes as f64 / (mbps as f64 * 1024.0 * 1024.0 / 8.0);
            let elapsed = started.elapsed().as_secs_f64();
            if target_secs > elapsed {
                tokio::time::sleep(std::time::Duration::from_secs_f64(target_secs - elapsed)).await;
            }
        }
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

    fn manifest_hashes(files: &[(&str, [u8; 32], u32)]) -> Vec<u8> {
        let entries = files
            .iter()
            .map(|(name, hash, len)| FileEntry {
                name: name.to_string(),
                file_hash: *hash,
                chunks: vec![ChunkMeta {
                    hash: *hash,
                    len: *len,
                }],
            })
            .collect();
        GameIndex::build(entries).encode().unwrap()
    }

    async fn wrong_seed_server(
        data_dir: &Path,
        game_id: u64,
        hashes: &[[u8; 32]],
    ) -> datapath::DataPathHandle {
        let mut store = GameStore::open(data_dir, game_id).unwrap();
        for hash in hashes {
            store.append_chunk(hash, b"WRONG").unwrap();
        }
        let pack = Arc::new(StdMutex::new(store));
        let pack_stores: Arc<Mutex<HashMap<u64, Arc<StdMutex<GameStore>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        pack_stores.lock().await.insert(game_id, pack.clone());
        let data_stores: Arc<Mutex<HashMap<u64, NodeStore>>> = Arc::new(Mutex::new(HashMap::new()));
        data_stores
            .lock()
            .await
            .insert(game_id, NodeStore::Pack(pack));
        datapath::serve(
            data_stores,
            pack_stores,
            data_dir.to_path_buf(),
            0,
            None,
            None,
            true,
        )
        .await
        .unwrap()
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
            download_mbps: None,
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

    fn assigned_task(id: u64, game_id: u64, version: u64, hashes: &[[u8; 32]]) -> Task {
        let mut t = task(id, game_id, version);
        t.assigned_chunks = hashes.iter().map(|h| h.to_vec()).collect();
        t
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
        let mut cfg = config(
            NodeType::Idc,
            idc_dir.clone(),
            url,
            Some(handle.endpoint_id().to_string()),
            Some(format!("127.0.0.1:{}", handle.port())),
        );
        cfg.download_mbps = Some(1);
        let (exec, _data, pack_stores, _cafe) = executor(cfg, 1);
        exec.run_task(assigned_task(1, 1, 1, &hashes))
            .await
            .unwrap();
        exec.run_task(assigned_task(2, 1, 1, &hashes))
            .await
            .unwrap();

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
    async fn test_idc_batch_flush() {
        let dir = std::env::temp_dir().join("blaze-exec-batch");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let seed_dir = dir.join("seed");
        let idc_dir = dir.join("idc");
        std::fs::create_dir_all(&seed_dir).unwrap();
        std::fs::create_dir_all(&idc_dir).unwrap();
        let contents: Vec<Vec<u8>> = (0u8..65).map(|i| vec![i]).collect();
        let hashes: Vec<[u8; 32]> = contents.iter().map(|c| hash_of(c)).collect();
        // 种子库：65 个单字节块（超过批量阈值 64，触发中途落盘分支）。
        let mut seed = GameStore::open(&seed_dir, 1).unwrap();
        let chunks: Vec<([u8; 32], Vec<u8>)> = hashes
            .iter()
            .zip(&contents)
            .map(|(h, c)| (*h, c.clone()))
            .collect();
        seed.append_chunks_batch(&chunks).unwrap();
        let pack = Arc::new(StdMutex::new(seed));
        let pack_stores: Arc<Mutex<HashMap<u64, Arc<StdMutex<GameStore>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        pack_stores.lock().await.insert(1, pack.clone());
        let data_stores: Arc<Mutex<HashMap<u64, NodeStore>>> = Arc::new(Mutex::new(HashMap::new()));
        data_stores.lock().await.insert(1, NodeStore::Pack(pack));
        let handle = datapath::serve(data_stores, pack_stores, seed_dir, 0, None, None, true)
            .await
            .unwrap();
        let entries: Vec<FileEntry> = hashes
            .iter()
            .enumerate()
            .map(|(i, h)| FileEntry {
                name: format!("f{i:02}.bin"),
                file_hash: *h,
                chunks: vec![ChunkMeta { hash: *h, len: 1 }],
            })
            .collect();
        let bytes = GameIndex::build(entries).encode().unwrap();
        let (url, service, _srv, store) = scheduler_setup(&dir.join("sched")).await;
        store.save_version(1, 1, &bytes).unwrap();
        store
            .insert_node(&NodeRecord {
                id: 2,
                node_type: "idc".to_string(),
                endpoint_id: handle.endpoint_id().to_string(),
                token: "seed".to_string(),
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
        let cfg = config(
            NodeType::Idc,
            idc_dir.clone(),
            url,
            Some(handle.endpoint_id().to_string()),
            Some(format!("127.0.0.1:{}", handle.port())),
        );
        let (exec, _data, local_stores, _cafe) = executor(cfg, 1);
        exec.run_task(assigned_task(1, 1, 1, &hashes))
            .await
            .unwrap();
        let local = local_stores.lock().await.get(&1).unwrap().clone();
        {
            let guard = local.lock().unwrap();
            for hash in &hashes {
                assert!(guard.contains(hash).unwrap());
            }
            assert_eq!(guard.size(), 65);
        }
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

        // 真实文件被删除后重跑同版本任务：补齐块并修复文件。
        std::fs::remove_file(cafe_dir.join("games/1/dir/a.bin")).unwrap();
        std::fs::remove_file(
            crate::cafe_store::temp_dir(&cafe_dir, 1)
                .join(format!("{}.blk", crate::update::hex(&hashes[0]))),
        )
        .unwrap();
        service
            .push_task(TaskRecord {
                id: 3,
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
        exec.run_task(task(3, 1, 1)).await.unwrap();
        assert_eq!(
            std::fs::read(cafe_dir.join("games/1/dir/a.bin")).unwrap(),
            b"hello"
        );
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_cafe_datapath_serves_temp_then_real_file() {
        let dir = std::env::temp_dir().join("blaze-exec-cafe-dp");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let seed_dir = dir.join("seed");
        let cafe_dir = dir.join("cafe");
        std::fs::create_dir_all(&seed_dir).unwrap();
        std::fs::create_dir_all(&cafe_dir).unwrap();
        let (seed, hashes) = seed_server(&seed_dir, 1).await;
        let bytes = manifest(&[("dir/a.bin", b"hello")]);
        let (url, service, _srv, store) = scheduler_setup(&dir.join("sched")).await;
        store.save_version(1, 1, &bytes).unwrap();
        store
            .insert_node(&NodeRecord {
                id: 2,
                node_type: "idc".to_string(),
                endpoint_id: seed.endpoint_id().to_string(),
                token: "peer".to_string(),
                addrs: vec![AddrRecord {
                    addr: format!("127.0.0.1:{}", seed.port()),
                    kind: "config".to_string(),
                    link: String::new(),
                }],
                status: "online".to_string(),
                last_heartbeat_ms: 1,
            })
            .unwrap();
        store.record_chunk_holder(2, 1, &hashes[0]).unwrap();
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
        let (exec, data_stores, _pack, _cafe) =
            executor(config(NodeType::Cafe, cafe_dir.clone(), url, None, None), 1);
        exec.run_task(task(1, 1, 1)).await.unwrap();

        // 用网吧数据面（pack_default=false）直接提供块服务。
        let pack_stores: Arc<Mutex<HashMap<u64, Arc<StdMutex<GameStore>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let cafe_handle = datapath::serve(
            data_stores,
            pack_stores,
            cafe_dir.clone(),
            0,
            None,
            None,
            false,
        )
        .await
        .unwrap();
        let ep = fetch::build_endpoint(None).await.unwrap();
        let target = fetch::PeerTarget {
            endpoint_id: cafe_handle.endpoint_id(),
            addr: Some(format!("127.0.0.1:{}", cafe_handle.port()).parse().unwrap()),
            relay_url: None,
            direct_only: true,
        };

        // 临时块提供上传。
        let mut got = Vec::new();
        let stats = fetch::fetch_chunks(&ep, &target, 1, &hashes[..1], |hash, data| {
            got.push((hash, data));
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(stats.downloaded, 1);
        assert_eq!(got[0].1, b"hello");

        // 删除临时块后回退真实文件偏移读。
        let temp = crate::cafe_store::temp_dir(&cafe_dir, 1);
        std::fs::remove_file(temp.join(format!("{}.blk", crate::update::hex(&hashes[0])))).unwrap();
        let mut got2 = Vec::new();
        let stats2 = fetch::fetch_chunks(&ep, &target, 1, &hashes[..1], |hash, data| {
            got2.push((hash, data));
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(stats2.downloaded, 1);
        assert_eq!(got2[0].1, b"hello");

        tokio::time::sleep(Duration::from_millis(700)).await;
        cafe_handle.shutdown();
        seed.shutdown();
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
        let (exec, _data, _pack, _cafe) = executor(
            config(
                NodeType::Idc,
                idc_dir,
                url,
                Some(handle.endpoint_id().to_string()),
                Some(format!("127.0.0.1:{}", handle.port())),
            ),
            1,
        );
        let err = exec
            .run_task(assigned_task(1, 1, 1, &[wrong_hash]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("下载失败"));
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_idc_peer_unreachable_falls_back_to_origin() {
        let dir = std::env::temp_dir().join("blaze-exec-fallback");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let idc_dir = dir.join("idc");
        let seed_dir = dir.join("seed");
        std::fs::create_dir_all(&idc_dir).unwrap();
        std::fs::create_dir_all(&seed_dir).unwrap();
        let (handle, hashes) = seed_server(&seed_dir, 1).await;
        let bytes = manifest(&[("a.bin", b"hello")]);
        let (url, _service, _srv, store) = scheduler_setup(&dir.join("sched")).await;
        store.save_version(1, 1, &bytes).unwrap();
        // 候选 peer 地址不可达：连接失败后应回退原始节点。
        store
            .insert_node(&NodeRecord {
                id: 2,
                node_type: "idc".to_string(),
                endpoint_id: handle.endpoint_id().to_string(),
                token: "bad".to_string(),
                addrs: vec![AddrRecord {
                    addr: "127.0.0.1:1".to_string(),
                    kind: "config".to_string(),
                    link: String::new(),
                }],
                status: "online".to_string(),
                last_heartbeat_ms: 1,
            })
            .unwrap();
        store.record_chunk_holder(2, 1, &hashes[0]).unwrap();
        let mut cfg = config(
            NodeType::Idc,
            idc_dir,
            url,
            Some(handle.endpoint_id().to_string()),
            Some(format!("127.0.0.1:{}", handle.port())),
        );
        cfg.download_mbps = Some(1);
        let (exec, _data, pack_stores, _cafe) = executor(cfg, 1);
        exec.run_task(assigned_task(1, 1, 1, &hashes))
            .await
            .unwrap();
        let local = pack_stores.lock().await.get(&1).unwrap().clone();
        {
            let guard = local.lock().unwrap();
            assert!(guard.contains(&hashes[0]).unwrap());
        }
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_idc_unassigned_chunks_wait_for_peers() {
        let dir = std::env::temp_dir().join("blaze-exec-wait");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let idc_dir = dir.join("idc");
        let seed_dir = dir.join("seed");
        std::fs::create_dir_all(&idc_dir).unwrap();
        std::fs::create_dir_all(&seed_dir).unwrap();
        let (handle, _hashes) = seed_server(&seed_dir, 1).await;
        let bytes = manifest(&[("a.bin", b"hello")]);
        let (url, _service, _srv, store) = scheduler_setup(&dir.join("sched")).await;
        store.save_version(1, 1, &bytes).unwrap();
        // 未分配中心责任且无 peer：不允许回退原始节点，等待其他节点完成分片。
        let (exec, _data, _pack, _cafe) = executor(
            config(
                NodeType::Idc,
                idc_dir,
                url,
                Some(handle.endpoint_id().to_string()),
                Some(format!("127.0.0.1:{}", handle.port())),
            ),
            1,
        );
        let err = exec.run_task(task(1, 1, 1)).await.unwrap_err();
        assert!(err.to_string().contains("等待其他节点完成责任分片"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_peer_wrong_data_partitions_assigned_and_blocked() {
        let dir = std::env::temp_dir().join("blaze-exec-partition");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let idc_dir = dir.join("idc");
        let seed_dir = dir.join("seed");
        std::fs::create_dir_all(&idc_dir).unwrap();
        std::fs::create_dir_all(&seed_dir).unwrap();
        let h1 = [1u8; 32];
        let h2 = [2u8; 32];
        let handle = wrong_seed_server(&seed_dir, 1, &[h1, h2]).await;
        let bytes = manifest_hashes(&[("a.bin", h1, 5), ("b.bin", h2, 5)]);
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
        store.record_chunk_holder(2, 1, &h1).unwrap();
        store.record_chunk_holder(2, 1, &h2).unwrap();
        let (exec, _data, _pack, _cafe) =
            executor(config(NodeType::Idc, idc_dir, url, None, None), 1);
        let err = exec
            .run_task(assigned_task(1, 1, 1, &[h1]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("无候选源"));
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_assigned_peer_failed_without_origin() {
        let dir = std::env::temp_dir().join("blaze-exec-noorigin");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let idc_dir = dir.join("idc");
        let seed_dir = dir.join("seed");
        std::fs::create_dir_all(&idc_dir).unwrap();
        std::fs::create_dir_all(&seed_dir).unwrap();
        let h1 = [1u8; 32];
        let handle = wrong_seed_server(&seed_dir, 1, &[h1]).await;
        let bytes = manifest_hashes(&[("a.bin", h1, 5)]);
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
        store.record_chunk_holder(2, 1, &h1).unwrap();
        let (exec, _data, _pack, _cafe) =
            executor(config(NodeType::Idc, idc_dir, url, None, None), 1);
        let err = exec
            .run_task(assigned_task(1, 1, 1, &[h1]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("无候选源"));
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_peer_unreachable_unassigned_blocks() {
        let dir = std::env::temp_dir().join("blaze-exec-unreach");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let idc_dir = dir.join("idc");
        let seed_dir = dir.join("seed");
        std::fs::create_dir_all(&idc_dir).unwrap();
        std::fs::create_dir_all(&seed_dir).unwrap();
        let (handle, hashes) = seed_server(&seed_dir, 1).await;
        let bytes = manifest_hashes(&[("a.bin", hashes[0], 5), ("b.bin", hashes[1], 5)]);
        let (url, _service, _srv, store) = scheduler_setup(&dir.join("sched")).await;
        store.save_version(1, 1, &bytes).unwrap();
        store
            .insert_node(&NodeRecord {
                id: 2,
                node_type: "idc".to_string(),
                endpoint_id: handle.endpoint_id().to_string(),
                token: "bad".to_string(),
                addrs: vec![AddrRecord {
                    addr: "127.0.0.1:1".to_string(),
                    kind: "config".to_string(),
                    link: String::new(),
                }],
                status: "online".to_string(),
                last_heartbeat_ms: 1,
            })
            .unwrap();
        store.record_chunk_holder(2, 1, &hashes[1]).unwrap();
        let (exec, _data, _pack, _cafe) = executor(
            config(
                NodeType::Idc,
                idc_dir,
                url,
                Some(handle.endpoint_id().to_string()),
                Some(format!("127.0.0.1:{}", handle.port())),
            ),
            1,
        );
        let err = exec
            .run_task(assigned_task(1, 1, 1, &[hashes[0]]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("等待其他节点完成责任分片"));
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_cafe_rollback_restores_previous_version() {
        let dir = std::env::temp_dir().join("blaze-exec-rollback");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let seed_dir = dir.join("seed");
        let cafe_dir = dir.join("cafe");
        std::fs::create_dir_all(&seed_dir).unwrap();
        std::fs::create_dir_all(&cafe_dir).unwrap();
        let (handle, hashes) = seed_server(&seed_dir, 1).await;
        let v1 = manifest(&[("a.bin", b"hello")]);
        let v2 = manifest(&[("b.bin", b"world")]);
        let (url, service, _srv, store) = scheduler_setup(&dir.join("sched")).await;
        store.save_version(1, 1, &v1).unwrap();
        store.save_version(1, 2, &v2).unwrap();
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
        for id in 1..=3u64 {
            service
                .push_task(TaskRecord {
                    id,
                    node_id: 1,
                    game_id: 1,
                    version: if id == 2 { 2 } else { 1 },
                    kind: if id == 3 {
                        "ROLLBACK".to_string()
                    } else {
                        "UPDATE".to_string()
                    },
                    assigned_chunks: vec![],
                    status: "queued".to_string(),
                    error: String::new(),
                })
                .await
                .unwrap();
        }
        let (exec, _data, _pack, cafe_stores) =
            executor(config(NodeType::Cafe, cafe_dir.clone(), url, None, None), 1);
        exec.run_task(task(1, 1, 1)).await.unwrap();
        exec.run_task(task(2, 1, 2)).await.unwrap();
        exec.run_task(Task {
            id: 3,
            game_id: 1,
            version: 1,
            kind: 2,
            assigned_chunks: vec![],
        })
        .await
        .unwrap();

        assert_eq!(
            std::fs::read(cafe_dir.join("games/1/a.bin")).unwrap(),
            b"hello"
        );
        assert!(!cafe_dir.join("games/1/b.bin").exists());
        let cafe = cafe_stores.lock().await.get(&1).unwrap().clone();
        assert_eq!(cafe.current_version().unwrap(), Some(1));
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_cafe_merge_failure_reported() {
        let dir = std::env::temp_dir().join("blaze-exec-mergefail");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let seed_dir = dir.join("seed");
        let cafe_dir = dir.join("cafe");
        std::fs::create_dir_all(&seed_dir).unwrap();
        std::fs::create_dir_all(&cafe_dir).unwrap();
        let (handle, hashes) = seed_server(&seed_dir, 1).await;
        // 清单 file_hash 错误：块齐全但合并后哈希校验必然失败。
        let wrong_index = GameIndex::build(vec![blaze_common::manifest::FileEntry {
            name: "a.bin".to_string(),
            file_hash: [9u8; 32],
            chunks: vec![blaze_common::manifest::ChunkMeta {
                hash: hashes[0],
                len: 5,
            }],
        }]);
        let bytes = wrong_index.encode().unwrap();
        let (url, _service, _srv, store) = scheduler_setup(&dir.join("sched")).await;
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
        store.record_chunk_holder(2, 1, &hashes[0]).unwrap();
        let (exec, _data, _pack, _cafe) =
            executor(config(NodeType::Cafe, cafe_dir, url, None, None), 1);
        let err = exec.run_task(task(1, 1, 1)).await.unwrap_err();
        assert!(err.to_string().contains("合并失败"));
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_choose_ep_direct_relay_and_missing() {
        let direct = fetch::build_endpoint(None).await.unwrap();
        let with_addr = PeerTarget {
            endpoint_id: direct.id(),
            addr: Some("127.0.0.1:1".parse().unwrap()),
            relay_url: None,
            direct_only: true,
        };
        assert!(std::ptr::eq(
            TaskExecutor::choose_ep(&with_addr, &direct).unwrap(),
            &direct
        ));
        let no_addr = PeerTarget {
            endpoint_id: direct.id(),
            addr: None,
            relay_url: Some("https://127.0.0.1:8443".to_string()),
            direct_only: false,
        };
        let err = TaskExecutor::choose_ep(&no_addr, &direct).unwrap_err();
        assert!(err.to_string().contains("无直连地址"));
    }

    #[tokio::test]
    async fn test_throttle_none_and_limited() {
        let started = Instant::now();
        let mut total = 0u64;
        TaskExecutor::throttle(None, &mut total, 100, started).await;
        assert_eq!(total, 0);
        TaskExecutor::throttle(Some(1), &mut total, 5, started).await;
        assert_eq!(total, 5);
    }
}
