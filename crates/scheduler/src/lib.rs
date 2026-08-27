//! 调度中心库：配置、数据层、控制面 gRPC、HTTP 后台与前端托管。
pub mod config;
pub mod db;
pub mod http;
pub mod keepalive;
pub mod server;

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use config::Config;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

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

    let (backup_shutdown, backup_tasks) = start_backup(&config, store.clone()).await?;

    let (events_tx, _) = tokio::sync::broadcast::channel(256);
    let control = server::ControlService::with_events(store.clone(), events_tx.clone());
    let grpc_handle = server::serve(config.bind_socket_addr()?, control.clone())
        .await
        .context("启动控制面 gRPC 服务失败")?;

    let keepalive_socket = Arc::new(
        tokio::net::UdpSocket::bind("0.0.0.0:0")
            .await
            .context("绑定保活 UDP 端口失败")?,
    );
    let (keepalive_tx, keepalive_rx) = tokio::sync::oneshot::channel();
    let keepalive_task = tokio::spawn(keepalive::run(
        store.clone(),
        keepalive_socket,
        config.keepalive_interval_secs,
        config.keepalive_fail_threshold,
        keepalive_rx,
    ));

    let listener = TcpListener::bind(config.http_bind_socket_addr()?)
        .await
        .context("绑定 HTTP 端口失败")?;
    let app = http::router(
        config.web_dir.clone().into(),
        config.admin_user.clone(),
        config.admin_password.clone(),
        store.clone(),
        control,
        events_tx,
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
    let _ = keepalive_tx.send(());
    let _ = keepalive_task.await;
    for tx in backup_shutdown {
        let _ = tx.send(());
    }
    for task in backup_tasks {
        let _ = task.await;
    }
    let _ = http_tx.send(());
    let _ = http_task.await;
    drop(grpc_handle);
    Ok(())
}

type BackupShutdown = Vec<oneshot::Sender<()>>;
type BackupTasks = Vec<tokio::task::JoinHandle<()>>;

/// 启动每日备份：立即执行一次并创建周期任务；未配置备份目录时返回空。
async fn start_backup(
    config: &Config,
    store: Arc<db::Store>,
) -> Result<(BackupShutdown, BackupTasks)> {
    let Some(dir) = &config.backup_dir else {
        return Ok((Vec::new(), Vec::new()));
    };
    std::fs::create_dir_all(dir).context(format!("创建备份目录失败: {}", dir.display()))?;
    let path = store.backup_to(dir).context("启动备份失败")?;
    println!("启动备份完成: {}", path.display());
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(run_backup_loop(
        store,
        dir.clone(),
        Duration::from_secs(config.backup_interval_hours * 3600),
        rx,
    ));
    Ok((vec![tx], vec![task]))
}

/// 周期备份循环；收到关闭信号退出。
pub async fn run_backup_loop(
    store: Arc<db::Store>,
    dir: PathBuf,
    interval: Duration,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(interval) => {
                match store.backup_to(&dir) {
                    Ok(path) => println!("每日备份完成: {}", path.display()),
                    Err(err) => println!("每日备份失败: {err:#}"),
                }
            }
        }
    }
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
            backup_dir: Some(dir.join("backup")),
            backup_interval_hours: 24,
        };
        fs::create_dir_all(config.web_dir.clone()).unwrap();
        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(run(config, async move {
            let _ = rx.await;
        }));
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        tx.send(()).unwrap();
        task.await.unwrap().unwrap();
        assert!(dir.join("data/scheduler.redb").exists());
        assert!(!fs::read_dir(dir.join("backup")).unwrap().next().is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_run_backup_loop_success_and_error() {
        let dir = std::env::temp_dir().join("blaze-sched-backup");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let store = Arc::new(db::Store::open(&dir.join("data")).unwrap());
        let backup = dir.join("backup");
        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(run_backup_loop(
            store.clone(),
            backup.clone(),
            Duration::from_millis(50),
            rx,
        ));
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send(()).unwrap();
        task.await.unwrap();
        assert!(!fs::read_dir(&backup).unwrap().next().is_none());

        let bad = dir.join("bad-backup");
        fs::write(&bad, b"x").unwrap();
        let (tx2, rx2) = oneshot::channel();
        let task2 = tokio::spawn(run_backup_loop(store, bad, Duration::from_millis(50), rx2));
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx2.send(()).unwrap();
        task2.await.unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_start_backup_disabled() {
        let dir = std::env::temp_dir().join("blaze-sched-nobackup");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let store = Arc::new(db::Store::open(&dir.join("data")).unwrap());
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
            backup_dir: None,
            backup_interval_hours: 24,
        };
        let (shutdown, tasks) = start_backup(&config, store).await.unwrap();
        assert!(shutdown.is_empty());
        assert!(tasks.is_empty());
        let _ = fs::remove_dir_all(&dir);
    }
}
