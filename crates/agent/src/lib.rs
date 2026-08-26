//! 节点 agent 库：IDC 节点（Linux）与网吧服务器（Windows）共用实现（M4/M5）。
pub mod cafe_store;
pub mod config;
pub mod control;
pub mod download;
pub mod executor;
pub mod fetch;
pub mod keepalive;
pub mod stun;
pub mod update;

use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::config::NodeType;
use crate::executor::TaskExecutor;

/// agent 运行句柄：数据面 + 控制面任务。
pub struct AgentHandle {
    datapath: origin::datapath::DataPathHandle,
    control_shutdown: Vec<oneshot::Sender<()>>,
    _control_tasks: Vec<JoinHandle<()>>,
}

impl AgentHandle {
    pub fn port(&self) -> u16 {
        self.datapath.port()
    }

    pub fn shutdown(self) {
        self.datapath.shutdown();
        for tx in self.control_shutdown {
            let _ = tx.send(());
        }
    }
}

/// 程序入口：Linux 走 `--config`，Windows 自动定位固定配置（见 config 模块）。
pub fn run_from_args(args: &[String]) -> Result<()> {
    let config = config::Config::resolve(args)?;
    println!(
        "agent 启动：节点类型 {}，数据目录 {}",
        config.node_type,
        config.data_dir.display()
    );
    Ok(())
}

/// 启动 agent 数据面服务（主程序入口）。
pub async fn start(config: config::Config) -> Result<AgentHandle> {
    let external_addr = match config.external_addr.as_deref() {
        Some(addr) => Some(addr.parse()?),
        None => match config.stun_addr.as_deref() {
            Some(server) => Some(stun::discover(server, config.listen_port).await?),
            None => None,
        },
    };
    println!(
        "外部地址：{}",
        external_addr
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "无".to_string())
    );
    let pack_stores: Arc<Mutex<HashMap<u64, Arc<StdMutex<origin::storage::GameStore>>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let data_stores: Arc<Mutex<HashMap<u64, origin::storage::NodeStore>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let cafe_stores: Arc<Mutex<HashMap<u64, Arc<cafe_store::CafeStore>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let handle = origin::datapath::serve(
        data_stores.clone(),
        pack_stores.clone(),
        config.data_dir.clone(),
        config.listen_port,
        config.relay_url.clone(),
        external_addr,
        config.node_type == NodeType::Idc,
    )
    .await?;
    println!(
        "agent 数据面启动：类型 {}，端点 {}，数据目录 {}，监听端口 {}，relay {}",
        config.node_type,
        handle.endpoint_id(),
        config.data_dir.display(),
        config.listen_port,
        config.relay_url.as_deref().unwrap_or("无")
    );
    let mut control_shutdown = Vec::new();
    let mut control_tasks = Vec::new();
    if let Some(port) = config.keepalive_port {
        let (tx, rx) = oneshot::channel();
        control_tasks.push(tokio::spawn(async move {
            let _ = keepalive::serve_pong(port, rx).await;
        }));
        control_shutdown.push(tx);
    }
    if config.node_type == NodeType::Cafe {
        let data_dir = config.data_dir.clone();
        let ttl = config.temp_ttl_hours;
        let (tx, rx) = oneshot::channel();
        control_tasks.push(tokio::spawn(cafe_store::run_cleaner(
            data_dir,
            ttl,
            std::time::Duration::from_secs(3600),
            rx,
        )));
        control_shutdown.push(tx);
    }
    if let Some(addr) = &config.control_addr {
        let mut client = control::connect(addr).await?;
        let addrs = external_addr
            .map(|addr| blaze_proto::control::Addr {
                addr: addr.to_string(),
                kind: if config.external_addr.is_some() {
                    "config"
                } else {
                    "stun"
                }
                .to_string(),
                link: String::new(),
            })
            .into_iter()
            .collect();
        let reply = control::register(
            &mut client,
            &config.node_type.to_string(),
            &handle.endpoint_id().to_string(),
            addrs,
        )
        .await?;
        println!("控制面注册成功：节点 ID {}", reply.node_id);
        let (tx_hb, rx_hb) = oneshot::channel();
        let (tx_watch, rx_watch) = oneshot::channel();
        let executor = TaskExecutor::new(
            config.clone(),
            reply.node_id,
            data_stores,
            pack_stores,
            cafe_stores,
        );
        let on_task = Arc::new(move |task: blaze_proto::control::Task| {
            let executor = executor.clone();
            tokio::spawn(async move {
                let _permit = executor.game_permits().acquire_owned().await;
                match executor.run_task(task).await {
                    Ok(()) => println!("任务执行完成"),
                    Err(err) => println!("任务执行失败: {err:#}"),
                }
            });
        });
        for task in reply.initial_tasks {
            println!(
                "补推历史任务: ID {}，游戏 {}，版本 {}",
                task.id, task.game_id, task.version
            );
            on_task(task);
        }
        control_tasks.push(tokio::spawn(control::heartbeat_loop(
            client.clone(),
            reply.node_id,
            std::time::Duration::from_secs(25),
            rx_hb,
        )));
        control_tasks.push(tokio::spawn(control::watch_loop(
            client,
            reply.node_id,
            on_task,
            rx_watch,
        )));
        control_shutdown.push(tx_hb);
        control_shutdown.push(tx_watch);
    } else {
        println!("未配置 control_addr，仅运行数据面");
    }
    Ok(AgentHandle {
        datapath: handle,
        control_shutdown,
        _control_tasks: control_tasks,
    })
}

/// 写入测试种子块（联调用）。
pub fn seed(data_dir: &Path, game_id: u64, chunk_count: usize) -> Result<()> {
    let mut store = origin::storage::GameStore::open(data_dir, game_id)?;
    for i in 0..chunk_count {
        let data = format!("blazenet-seed-{game_id}-{i:04}").repeat(64 * 1024);
        let hash: [u8; 32] = blake3::hash(data.as_bytes()).into();
        store.append_chunk(&hash, data.as_bytes())?;
    }
    println!("已写入 {chunk_count} 个种子块（游戏 {game_id}）");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_run_from_args_ok() {
        let dir = std::env::temp_dir().join("blaze-agent-run");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let cfg_path = dir.join("agent.toml");
        fs::write(
            &cfg_path,
            format!("node_type = \"idc\"\ndata_dir = \"{}\"\n", dir.display()),
        )
        .unwrap();
        run_from_args(&[
            "agent".to_string(),
            "--config".to_string(),
            cfg_path.to_string_lossy().to_string(),
        ])
        .unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_run_from_args_invalid() {
        let err = run_from_args(&["agent".to_string()]).unwrap_err();
        assert!(err.to_string().contains("用法"));
    }

    async fn scheduler_setup() -> (
        String,
        scheduler::server::ControlService,
        scheduler::server::ServerHandle,
        std::sync::Arc<scheduler::db::Store>,
    ) {
        let dir = std::env::temp_dir().join("blaze-agent-sched");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let store = scheduler::db::Store::open(&dir).unwrap();
        let store = std::sync::Arc::new(store);
        let service = scheduler::server::ControlService::new(store.clone());
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let svc = service.clone();
        let handle = scheduler::server::serve(addr, svc).await.unwrap();
        (format!("http://{addr}"), service, handle, store)
    }

    #[tokio::test]
    async fn test_start_serve_and_seed() {
        let dir = std::env::temp_dir().join("blaze-agent-start");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        seed(&dir, 1, 2).unwrap();
        let (url, _service, _handle, _store) = scheduler_setup().await;
        let handle = start(config::Config {
            node_type: config::NodeType::Idc,
            data_dir: dir.clone(),
            concurrent_games: 5,
            chunk_concurrency: 4,
            disk_free_threshold: 200 * 1024 * 1024 * 1024,
            compact_threshold: 0.3,
            listen_port: 0,
            temp_ttl_hours: 24,
            origin_endpoint: None,
            origin_addr: None,
            keepalive_port: None,
            relay_url: None,
            external_addr: Some("127.0.0.1:42001".to_string()),
            stun_addr: None,
            control_addr: Some(url),
        })
        .await
        .unwrap();
        assert!(handle.port() > 0);
        handle.shutdown();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_start_without_control() {
        let dir = std::env::temp_dir().join("blaze-agent-start2");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let handle = start(config::Config {
            node_type: config::NodeType::Cafe,
            data_dir: dir.clone(),
            concurrent_games: 5,
            chunk_concurrency: 4,
            disk_free_threshold: 200 * 1024 * 1024 * 1024,
            compact_threshold: 0.3,
            listen_port: 0,
            temp_ttl_hours: 24,
            origin_endpoint: None,
            origin_addr: None,
            keepalive_port: None,
            relay_url: None,
            external_addr: None,
            stun_addr: None,
            control_addr: None,
        })
        .await
        .unwrap();
        handle.shutdown();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_start_with_stun_discovery() {
        // 本地地址回显服务：收到 ECHO 即回复观测地址（一次性响应）。
        let echo =
            std::sync::Arc::new(tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap());
        let echo_addr = echo.local_addr().unwrap().to_string();
        let handle = echo.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 256];
            let (_, src) = handle.recv_from(&mut buf).await.unwrap();
            let reply = format!("ADDR blazenet-agent {src}");
            let _ = handle.send_to(reply.as_bytes(), src).await;
        });

        let dir = std::env::temp_dir().join("blaze-agent-start3");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let agent = start(config::Config {
            node_type: config::NodeType::Cafe,
            data_dir: dir.clone(),
            concurrent_games: 5,
            chunk_concurrency: 4,
            disk_free_threshold: 200 * 1024 * 1024 * 1024,
            compact_threshold: 0.3,
            listen_port: 0,
            temp_ttl_hours: 24,
            origin_endpoint: None,
            origin_addr: None,
            keepalive_port: Some(0),
            relay_url: None,
            external_addr: None,
            stun_addr: Some(echo_addr),
            control_addr: None,
        })
        .await
        .unwrap();
        agent.shutdown();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_start_with_stun_and_control() {
        let echo =
            std::sync::Arc::new(tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap());
        let echo_addr = echo.local_addr().unwrap().to_string();
        let handle = echo.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 256];
            let (_, src) = handle.recv_from(&mut buf).await.unwrap();
            let reply = format!("ADDR blazenet-agent {src}");
            let _ = handle.send_to(reply.as_bytes(), src).await;
        });

        let (url, service, _handle, _store) = scheduler_setup().await;
        service
            .push_task(scheduler::db::TaskRecord {
                id: 1,
                node_id: 1,
                game_id: 1,
                version: 99,
                kind: "UPDATE".to_string(),
                assigned_chunks: vec![],
                status: "queued".to_string(),
                error: String::new(),
            })
            .await
            .unwrap();
        let dir = std::env::temp_dir().join("blaze-agent-start4");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let agent = start(config::Config {
            node_type: config::NodeType::Cafe,
            data_dir: dir.clone(),
            concurrent_games: 5,
            chunk_concurrency: 4,
            disk_free_threshold: 200 * 1024 * 1024 * 1024,
            compact_threshold: 0.3,
            listen_port: 0,
            temp_ttl_hours: 24,
            origin_endpoint: None,
            origin_addr: None,
            keepalive_port: None,
            relay_url: None,
            external_addr: None,
            stun_addr: Some(echo_addr),
            control_addr: Some(url),
        })
        .await
        .unwrap();
        assert!(agent.port() > 0);
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        agent.shutdown();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_start_auto_executes_initial_task() {
        let dir = std::env::temp_dir().join("blaze-agent-auto");
        let _ = fs::remove_dir_all(&dir);
        let seed_dir = dir.join("seed");
        let idc_dir = dir.join("idc");
        fs::create_dir_all(&seed_dir).unwrap();
        fs::create_dir_all(&idc_dir).unwrap();

        let data = b"auto";
        let hash: [u8; 32] = blake3::hash(data).into();
        let mut store = origin::storage::GameStore::open(&seed_dir, 1).unwrap();
        store.append_chunk(&hash, data).unwrap();
        let pack = Arc::new(StdMutex::new(store));
        let pack_stores: Arc<
            tokio::sync::Mutex<HashMap<u64, Arc<StdMutex<origin::storage::GameStore>>>>,
        > = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        pack_stores.lock().await.insert(1, pack.clone());
        let data_stores: Arc<tokio::sync::Mutex<HashMap<u64, origin::storage::NodeStore>>> =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        data_stores
            .lock()
            .await
            .insert(1, origin::storage::NodeStore::Pack(pack));
        let seed_handle =
            origin::datapath::serve(data_stores, pack_stores, seed_dir, 0, None, None, true)
                .await
                .unwrap();

        let index =
            blaze_common::manifest::GameIndex::build(vec![blaze_common::manifest::FileEntry {
                name: "a.bin".to_string(),
                file_hash: hash,
                chunks: vec![blaze_common::manifest::ChunkMeta {
                    hash,
                    len: data.len() as u32,
                }],
            }]);
        let bytes = index.encode().unwrap();
        let (url, service, _handle, store) = scheduler_setup().await;
        store.save_version(1, 1, &bytes).unwrap();
        service
            .push_task(scheduler::db::TaskRecord {
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

        let agent = start(config::Config {
            node_type: config::NodeType::Idc,
            data_dir: idc_dir,
            concurrent_games: 2,
            chunk_concurrency: 2,
            disk_free_threshold: 1024,
            compact_threshold: 0.3,
            listen_port: 0,
            temp_ttl_hours: 24,
            origin_endpoint: Some(seed_handle.endpoint_id().to_string()),
            origin_addr: Some(format!("127.0.0.1:{}", seed_handle.port())),
            keepalive_port: None,
            relay_url: None,
            external_addr: None,
            stun_addr: None,
            control_addr: Some(url),
        })
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        agent.shutdown();
        seed_handle.shutdown();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let tasks = store.tasks_for_node(1).unwrap();
        assert_eq!(tasks[0].status, "done");
        let _ = fs::remove_dir_all(&dir);
    }
}
