//! 节点 agent 库：IDC 节点（Linux）与网吧服务器（Windows）共用实现（M4/M5）。
pub mod config;
pub mod datapath;
pub mod download;
pub mod update;

use anyhow::Result;
use std::path::Path;

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
pub async fn start(config: config::Config) -> Result<datapath::DataPathHandle> {
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
    Ok(handle)
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

    #[tokio::test]
    async fn test_start_serve_and_seed() {
        let dir = std::env::temp_dir().join("blaze-agent-start");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        seed(&dir, 1, 2).unwrap();
        let handle = start(config::Config {
            node_type: config::NodeType::Idc,
            data_dir: dir.clone(),
            concurrent_games: 5,
            chunk_concurrency: 4,
            disk_free_threshold: 200 * 1024 * 1024 * 1024,
            compact_threshold: 0.3,
            listen_port: 0,
            relay_url: None,
            external_addr: None,
        })
        .await
        .unwrap();
        assert!(handle.port() > 0);
        handle.shutdown();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let _ = fs::remove_dir_all(&dir);
    }
}
