//! 三期 P3.2b/P3.4b：基于 compio（Linux io_uring / Windows IOCP）的顺序大块 IO。
//!
//! 设计说明：
//! - 块库常规单块读写保持同步路径（单块随机 IO 实测 compio 无收益，见 docs/14）；
//! - compio 用于**顺序大块**场景：pack 压缩重写、网吧合并拼装写盘，io_uring 收益明显；
//! - 每个调用线程缓存一个 compio 运行时，`block_on` 提交操作，对外保持同步接口，
//!   调用方无需迁移运行时（控制面与数据面仍为 tokio）。
use std::cell::OnceCell;
use std::path::Path;

use anyhow::{Context, Result};

const COPY_CHUNK: usize = 1024 * 1024;

thread_local! {
    static COMPIO_RUNTIME: OnceCell<compio::runtime::Runtime> = const { OnceCell::new() };
}

fn with_runtime<T>(f: impl FnOnce(&compio::runtime::Runtime) -> T) -> T {
    COMPIO_RUNTIME.with(|cell| {
        let rt =
            cell.get_or_init(|| compio::runtime::Runtime::new().expect("创建 compio 运行时失败"));
        f(rt)
    })
}

/// 按范围列表顺序复制：源文件按 `(偏移, 长度)` 读取，目标文件从 0 顺序写入。
/// 返回写入字节数；目标文件不存在时创建。
pub fn copy_ranges(src: &Path, dst: &Path, ranges: &[(u64, u32)]) -> Result<u64> {
    with_runtime(|rt| {
        rt.block_on(async move {
            use compio::buf::BufResult;
            use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
            let src_file = compio::fs::File::open(src)
                .await
                .with_context(|| format!("打开源文件失败: {}", src.display()))?;
            let mut dst_file = compio::fs::File::create(dst)
                .await
                .with_context(|| format!("创建目标文件失败: {}", dst.display()))?;
            let mut out_offset = 0u64;
            let mut total = 0u64;
            for (start, len) in ranges {
                let mut remaining = usize::try_from(*len).context("块长度超出 usize 范围")?;
                let mut pos = *start;
                while remaining > 0 {
                    let want = remaining.min(COPY_CHUNK);
                    let read_buf = vec![0u8; want];
                    let BufResult(res, data) = src_file.read_exact_at(read_buf, pos).await;
                    res.context("读取源块失败")?;
                    let BufResult(wres, _) = dst_file.write_all_at(data, out_offset).await;
                    wres.context("写入目标块失败")?;
                    remaining -= want;
                    pos += want as u64;
                    out_offset += want as u64;
                    total += want as u64;
                }
            }
            dst_file.sync_all().await.context("同步目标文件失败")?;
            Ok(total)
        })
    })
}

/// compio 顺序写句柄：网吧合并拼装真实文件时使用（io_uring/IOCP 提交写操作）。
/// 非 `Send`，必须在创建线程内使用（同步调用点运行于同一线程，满足约束）。
pub struct CompioWriter {
    file: compio::fs::File,
    offset: u64,
}

impl CompioWriter {
    /// 创建（或截断）目标文件。
    pub fn create(path: &Path) -> Result<Self> {
        with_runtime(|rt| {
            rt.block_on(async move {
                let file = compio::fs::File::create(path)
                    .await
                    .with_context(|| format!("创建目标文件失败: {}", path.display()))?;
                Ok(Self { file, offset: 0 })
            })
        })
    }

    /// 在当前位置顺序写入整块数据（`write_all_at` 保证写满或报错）。
    pub fn write_owned(&mut self, data: Vec<u8>) -> Result<()> {
        let len = data.len() as u64;
        with_runtime(|rt| {
            rt.block_on(async move {
                use compio::buf::BufResult;
                use compio::io::AsyncWriteAtExt;
                let BufResult(res, _) = self.file.write_all_at(data, self.offset).await;
                res.context("写入目标文件失败")?;
                self.offset += len;
                Ok(())
            })
        })
    }

    /// 同步数据到磁盘。
    pub fn sync_all(&self) -> Result<()> {
        with_runtime(|rt| {
            rt.block_on(async { self.file.sync_all().await.context("同步目标文件失败") })
        })
    }

    /// 显式关闭（Windows 上打开中的文件不可删除/重命名，合并前必须关闭）。
    pub fn close(self) -> Result<()> {
        with_runtime(|rt| {
            rt.block_on(async { self.file.close().await.context("关闭目标文件失败") })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_copy_ranges_ok() {
        let dir = std::env::temp_dir().join(format!("blaze-common-copy-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let src = dir.join("src.bin");
        let dst = dir.join("dst.bin");
        fs::write(&src, b"0123456789").unwrap();
        // 只复制偏移 2 长度 3 与偏移 7 长度 2 → "23478"
        let copied = copy_ranges(&src, &dst, &[(2, 3), (7, 2)]).unwrap();
        assert_eq!(copied, 5);
        assert_eq!(fs::read(&dst).unwrap(), b"23478");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_copy_ranges_eof() {
        let dir =
            std::env::temp_dir().join(format!("blaze-common-copy-eof-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let src = dir.join("src.bin");
        let dst = dir.join("dst.bin");
        fs::write(&src, b"0123456789").unwrap();
        let err = copy_ranges(&src, &dst, &[(2, 100)]).unwrap_err();
        assert!(err.to_string().contains("读取源块失败"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_copy_ranges_missing_src() {
        let dir =
            std::env::temp_dir().join(format!("blaze-common-copy-miss-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let src = dir.join("nope.bin");
        let dst = dir.join("dst.bin");
        let err = copy_ranges(&src, &dst, &[(0, 1)]).unwrap_err();
        assert!(err.to_string().contains("打开源文件失败"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compio_writer_roundtrip() {
        let dir = std::env::temp_dir().join(format!("blaze-common-writer-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let dst = dir.join("dst.bin");
        let mut writer = CompioWriter::create(&dst).unwrap();
        writer.write_owned(b"hello".to_vec()).unwrap();
        writer.write_owned(b"world".to_vec()).unwrap();
        writer.sync_all().unwrap();
        writer.close().unwrap();
        assert_eq!(fs::read(&dst).unwrap(), b"helloworld");
        let _ = fs::remove_dir_all(&dir);
    }
}
