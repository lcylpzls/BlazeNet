//! 原始节点库：配置、块库、上传服务与版本提交。
pub mod config;
pub mod datapath;
pub mod server;
pub mod storage;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use anyhow::{Context, Result};
use config::Config;
use tokio::sync::Mutex;

/// 解析启动参数并加载配置（Linux 规范：唯一参数 `--config`）。
pub fn load_from_args(args: &[String]) -> Result<Config> {
    let path = Config::parse_args(args)?;
    let config = Config::load(&path)?;
    Ok(config)
}

/// 运行原始节点：上传 gRPC + 数据面块服务；`stop` 触发后退出。
pub async fn run(config: Config, stop: impl Future<Output = ()>) -> Result<()> {
    std::fs::create_dir_all(&config.data_dir)
        .context(format!("创建数据目录失败: {}", config.data_dir.display()))?;
    println!(
        "原始节点启动：数据目录 {}，上传 {}，数据面端口 {}，压缩阈值 {:.0}%，磁盘下限 {:.1}GiB",
        config.data_dir.display(),
        config.bind_addr,
        config.listen_port,
        config.compact_threshold * 100.0,
        config.min_free_bytes as f64 / 1024.0 / 1024.0 / 1024.0
    );
    let stores = Arc::new(Mutex::new(HashMap::new()));
    let upload_handle = server::serve(
        config.bind_socket_addr()?,
        server::UploadService::with_stores(config.data_dir.clone(), stores.clone()),
    )
    .await
    .context("启动上传服务失败")?;
    let external_addr = config
        .external_addr
        .as_deref()
        .map(str::parse)
        .transpose()?;
    let data_handle = datapath::serve(
        stores,
        config.data_dir.clone(),
        config.listen_port,
        config.relay_url.clone(),
        external_addr,
    )
    .await
    .context("启动数据面服务失败")?;
    println!(
        "原始节点服务就绪：数据面端点 {}，等待停止信号...",
        data_handle.endpoint_id()
    );
    stop.await;
    data_handle.shutdown();
    drop(upload_handle);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tokio::sync::oneshot;

    #[test]
    fn test_load_from_args_ok() {
        let dir = std::env::temp_dir().join("blaze-origin-load");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let data_dir = dir.join("data");
        let cfg_path = dir.join("origin.toml");
        fs::write(
            &cfg_path,
            format!(
                "data_dir = \"{}\"\nbind_addr = \"127.0.0.1:0\"\n",
                data_dir.display()
            ),
        )
        .unwrap();
        let config = load_from_args(&[
            "origin".to_string(),
            "--config".to_string(),
            cfg_path.to_string_lossy().to_string(),
        ])
        .unwrap();
        assert_eq!(config.bind_addr, "127.0.0.1:0");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_from_args_invalid() {
        let err = load_from_args(&["origin".to_string()]).unwrap_err();
        assert!(err.to_string().contains("用法"));
    }

    #[tokio::test]
    async fn test_run_starts_and_stops() {
        let dir = std::env::temp_dir().join("blaze-origin-run");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let config = Config {
            data_dir: dir.join("data"),
            bind_addr: "127.0.0.1:0".to_string(),
            listen_port: 0,
            external_addr: None,
            relay_url: None,
            compact_threshold: 0.3,
            min_free_bytes: 1024,
        };
        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(run(config, async move {
            let _ = rx.await;
        }));
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        tx.send(()).unwrap();
        task.await.unwrap().unwrap();
        assert!(dir.join("data").is_dir());
        let _ = fs::remove_dir_all(&dir);
    }
}
