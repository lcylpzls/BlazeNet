//! 调度中心库：配置、数据层、控制面 gRPC、HTTP 后台与前端托管。
pub mod config;
pub mod db;
pub mod http;
pub mod keepalive;
pub mod server;

use std::future::Future;
use std::sync::Arc;

use anyhow::{Context, Result};
use config::Config;
use tokio::net::TcpListener;

/// 解析启动参数并加载配置（Linux 规范：唯一参数 `--config`）。
pub fn load_from_args(args: &[String]) -> Result<Config> {
    let path = Config::parse_args(args)?;
    let config = Config::load(&path)?;
    Ok(config)
}

/// 运行调度中心：控制面 gRPC + HTTP 后台/前端托管；`stop` 触发后优雅退出。
pub async fn run(config: Config, stop: impl Future<Output = ()>) -> Result<()> {
    let store = Arc::new(db::Store::open(&config.data_dir)?);
    println!(
        "调度中心启动：数据目录 {}，gRPC {}，HTTP {}，心跳 {}s，离线判定 {}s",
        config.data_dir.display(),
        config.bind_addr,
        config.http_bind_addr,
        config.heartbeat_interval_secs,
        config.offline_timeout_secs
    );
    println!("当前节点数: {}", store.list_nodes()?.len());

    let grpc_handle = server::serve(
        config.bind_socket_addr()?,
        server::ControlService::new(store.clone()),
    )
    .await
    .context("启动控制面 gRPC 服务失败")?;

    let listener = TcpListener::bind(config.http_bind_socket_addr()?)
        .await
        .context("绑定 HTTP 端口失败")?;
    let app = http::router(
        config.web_dir.clone().into(),
        config.admin_user.clone(),
        config.admin_password.clone(),
        store.clone(),
    );
    let (http_tx, http_rx) = tokio::sync::oneshot::channel();
    let http_task = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = http_rx.await;
            })
            .await;
    });

    println!("调度中心服务就绪，等待停止信号...");
    stop.await;
    let _ = http_tx.send(());
    let _ = http_task.await;
    drop(grpc_handle);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tokio::sync::oneshot;

    #[test]
    fn test_load_from_args_ok() {
        let dir = std::env::temp_dir().join("blaze-sched-load");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let cfg_path = dir.join("scheduler.toml");
        fs::write(
            &cfg_path,
            format!(
                "data_dir = \"{}\"\nhttp_bind_addr = \"127.0.0.1:0\"\n",
                dir.join("data").display()
            ),
        )
        .unwrap();
        let config = load_from_args(&[
            "scheduler".to_string(),
            "--config".to_string(),
            cfg_path.to_string_lossy().to_string(),
        ])
        .unwrap();
        assert_eq!(config.http_bind_addr, "127.0.0.1:0");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_from_args_invalid() {
        let err = load_from_args(&["scheduler".to_string()]).unwrap_err();
        assert!(err.to_string().contains("用法"));
    }

    #[tokio::test]
    async fn test_run_starts_and_stops() {
        let dir = std::env::temp_dir().join("blaze-sched-run");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let config = Config {
            data_dir: dir.join("data"),
            bind_addr: "127.0.0.1:0".to_string(),
            http_bind_addr: "127.0.0.1:0".to_string(),
            heartbeat_interval_secs: 25,
            offline_timeout_secs: 75,
            keepalive_interval_secs: 25,
            keepalive_fail_threshold: 3,
            web_dir: dir.join("web").display().to_string(),
            admin_user: "admin".to_string(),
            admin_password: "admin123".to_string(),
        };
        fs::create_dir_all(&config.web_dir.clone()).unwrap();
        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(run(config, async move {
            let _ = rx.await;
        }));
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        tx.send(()).unwrap();
        task.await.unwrap().unwrap();
        assert!(dir.join("data/scheduler.redb").exists());
        let _ = fs::remove_dir_all(&dir);
    }
}
