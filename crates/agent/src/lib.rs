//! 节点 agent 库：IDC 节点（Linux）与网吧服务器（Windows）共用实现（M4/M5）。
pub mod config;
pub mod datapath;
pub mod download;

use anyhow::Result;

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
}
