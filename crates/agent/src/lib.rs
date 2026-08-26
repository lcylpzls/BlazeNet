//! 节点 agent 库：IDC 节点（Linux）与网吧服务器（Windows）共用实现（M4/M5）。

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
