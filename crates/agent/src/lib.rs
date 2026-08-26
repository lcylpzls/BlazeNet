//! 节点 agent 库：IDC 节点（Linux）与网吧服务器（Windows）共用实现（M4/M5）。
pub mod config;
pub mod control;
pub mod datapath;
pub mod download;
pub mod update;

use anyhow::Result;
use std::path::Path;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// agent 运行句柄：数据面 + 控制面任务。
pub struct AgentHandle {
    datapath: datapath::DataPathHandle,
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
    let handle = datapath::serve(
        config.data_dir.clone(),
        config.listen_port,
        config.relay_url.clone(),
        config
            .external_addr
            .as_deref()
            .map(str::parse)
            .transpose()?,
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
    if let Some(addr) = &config.control_addr {
        let mut client = control::connect(addr).await?;
        let addrs = config
            .external_addr
            .as_deref()
            .map(|addr| blaze_proto::control::Addr {
                addr: addr.to_string(),
                kind: "config".to_string(),
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
        control_tasks.push(tokio::spawn(control::heartbeat_loop(
            client.clone(),
            reply.node_id,
            std::time::Duration::from_secs(25),
            rx_hb,
        )));
        control_tasks.push(tokio::spawn(control::watch_loop(
            client,
            reply.node_id,
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
    ) {
        let dir = std::env::temp_dir().join("blaze-agent-sched");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let store = scheduler::db::Store::open(&dir).unwrap();
        let service = scheduler::server::ControlService::new(store);
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let svc = service.clone();
        let handle = scheduler::server::serve(addr, svc).await.unwrap();
        (format!("http://{addr}"), service, handle)
    }

    #[tokio::test]
    async fn test_start_serve_and_seed() {
        let dir = std::env::temp_dir().join("blaze-agent-start");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        seed(&dir, 1, 2).unwrap();
        let (url, _service, _handle) = scheduler_setup().await;
        let handle = start(config::Config {
            node_type: config::NodeType::Idc,
            data_dir: dir.clone(),
            concurrent_games: 5,
            chunk_concurrency: 4,
            disk_free_threshold: 200 * 1024 * 1024 * 1024,
            compact_threshold: 0.3,
            listen_port: 0,
            relay_url: None,
            external_addr: Some("127.0.0.1:42001".to_string()),
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
            relay_url: None,
            external_addr: None,
            control_addr: None,
        })
        .await
        .unwrap();
        handle.shutdown();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let _ = fs::remove_dir_all(&dir);
    }
}
