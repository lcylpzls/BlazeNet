//! 调度中心库：配置、数据层；控制面服务（M3 后续实现）。
pub mod config;
pub mod db;
pub mod server;

use anyhow::Result;
use config::Config;

/// 程序入口：Linux 规范，唯一参数 `--config <路径>`。
pub fn run_from_args(args: &[String]) -> Result<()> {
    let path = Config::parse_args(args)?;
    let config = Config::load(&path)?;
    let store = db::Store::open(&config.data_dir)?;
    println!(
        "调度中心启动：数据目录 {}，监听 {}，心跳 {}s，离线判定 {}s",
        config.data_dir.display(),
        config.bind_addr,
        config.heartbeat_interval_secs,
        config.offline_timeout_secs
    );
    println!("当前节点数: {}", store.list_nodes()?.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_run_from_args_ok() {
        let dir = std::env::temp_dir().join("blaze-sched-run");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let cfg_path = dir.join("scheduler.toml");
        fs::write(
            &cfg_path,
            format!("data_dir = \"{}\"\n", dir.join("data").display()),
        )
        .unwrap();
        run_from_args(&[
            "scheduler".to_string(),
            "--config".to_string(),
            cfg_path.to_string_lossy().to_string(),
        ])
        .unwrap();
        assert!(dir.join("data/scheduler.redb").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_run_from_args_invalid() {
        let err = run_from_args(&["scheduler".to_string()]).unwrap_err();
        assert!(err.to_string().contains("用法"));
    }
}
