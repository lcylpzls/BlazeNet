//! 原始节点库：配置、块库；上传服务与版本发布（M2 后续实现）。
pub mod config;
pub mod storage;

use anyhow::{Context, Result};
use config::Config;

/// 程序入口：Linux 规范，唯一参数 `--config <路径>`。
pub fn run_from_args(args: &[String]) -> Result<()> {
    let path = Config::parse_args(args)?;
    let config = Config::load(&path)?;
    std::fs::create_dir_all(&config.data_dir)
        .context(format!("创建数据目录失败: {}", config.data_dir.display()))?;
    println!(
        "原始节点启动：数据目录 {}，压缩阈值 {:.0}%，磁盘下限 {:.1}GiB",
        config.data_dir.display(),
        config.compact_threshold * 100.0,
        config.min_free_bytes as f64 / 1024.0 / 1024.0 / 1024.0
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_run_from_args_ok() {
        let dir = std::env::temp_dir().join("blaze-origin-run");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let data_dir = dir.join("data");
        let cfg_path = dir.join("origin.toml");
        fs::write(
            &cfg_path,
            format!("data_dir = \"{}\"\n", data_dir.display()),
        )
        .unwrap();
        run_from_args(&[
            "origin".to_string(),
            "--config".to_string(),
            cfg_path.to_string_lossy().to_string(),
        ])
        .unwrap();
        assert!(data_dir.is_dir());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_run_from_args_invalid() {
        let err = run_from_args(&["origin".to_string()]).unwrap_err();
        assert!(err.to_string().contains("用法"));
    }
}
