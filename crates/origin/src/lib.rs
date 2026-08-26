//! 原始节点库：上传服务、块库、版本发布与数据面块服务（M2 实现）。

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
