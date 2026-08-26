//! relay 独立服务入口：Linux 交付物，唯一参数 `--config <配置文件>`。
use anyhow::{Context, Result};
use std::path::PathBuf;

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let config_arg = args.next().context("缺少 --config 参数")?;
    let path = args
        .next()
        .context("缺少 --config 参数值")?
        .parse::<PathBuf>()
        .context("--config 参数值非法")?;
    if config_arg != "--config" || args.next().is_some() {
        anyhow::bail!("用法: relay --config <配置文件>");
    }
    let config = relay::Config::from_file(&path)?;
    tokio::runtime::Runtime::new()
        .context("创建 tokio 运行时失败")?
        .block_on(relay::run(config, async {
            let _ = tokio::signal::ctrl_c().await;
        }))
}
