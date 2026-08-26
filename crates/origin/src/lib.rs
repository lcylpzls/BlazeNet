//! 原始节点库：配置、块库、上传服务与版本提交。
pub mod config;
pub mod server;
pub mod storage;

use std::future::Future;

use anyhow::{Context, Result};
use config::Config;

/// 解析启动参数并加载配置（Linux 规范：唯一参数 `--config`）。
pub fn load_from_args(args: &[String]) -> Result<Config> {
    let path = Config::parse_args(args)?;
    let config = Config::load(&path)?;
    Ok(config)
}

/// 运行原始节点：上传 gRPC 服务；`stop` 触发后退出。
pub async fn run(config: Config, stop: impl Future<Output = ()>) -> Result<()> {
    std::fs::create_dir_all(&config.data_dir)
        .context(format!("创建数据目录失败: {}", config.data_dir.display()))?;
    println!(
        "原始节点启动：数据目录 {}，监听 {}，压缩阈值 {:.0}%，磁盘下限 {:.1}GiB",
        config.data_dir.display(),
        config.bind_addr,
        config.compact_threshold * 100.0,
        config.min_free_bytes as f64 / 1024.0 / 1024.0 / 1024.0
    );
    let handle = server::serve(
        config.bind_socket_addr()?,
        server::UploadService::new(config.data_dir.clone()),
    )
    .await
    .context("启动上传服务失败")?;
    println!("原始节点上传服务就绪，等待停止信号...");
    stop.await;
    drop(handle);
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
