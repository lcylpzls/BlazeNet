//! relay 独立服务库：iroh-relay 打洞协助服务（PoC 已验证，生产封装待 M0/M2）。

use anyhow::Result;

/// 程序入口逻辑，当前为占位实现。
pub fn run() -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_ok() {
        run().expect("占位入口应成功");
    }
}
