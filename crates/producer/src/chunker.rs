//! 分块与哈希：FastCDC 变长分块 + BLAKE3 内容寻址。
use anyhow::{Context, Result};
use fastcdc::v2020::StreamCDC;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use crate::config::ChunkParams;

/// 单个块的元信息（数据不上屏，按需从源文件偏移读取或从暂存区读取）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkMeta {
    pub hash: [u8; 32],
    pub len: u32,
}

/// 计算文件内容的 BLAKE3 哈希（流式，避免整文件载入内存）。
pub fn file_hash(path: &Path) -> Result<[u8; 32]> {
    let file = File::open(path).context(format!("打开文件失败: {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = reader.read(&mut buf).context("读取文件失败")?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().into())
}

/// 对文件分块；`stage_dir` 提供时，把新出现的块写入暂存区（同哈希只写一次）。
pub fn chunk_file(
    path: &Path,
    params: &ChunkParams,
    stage_dir: Option<&Path>,
    seen: &mut HashSet<[u8; 32]>,
) -> Result<Vec<ChunkMeta>> {
    let file = File::open(path).context(format!("打开文件失败: {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut metas = Vec::new();
    for item in StreamCDC::new(reader, params.min_size, params.avg_size, params.max_size) {
        let chunk = item.context("FastCDC 分块失败")?;
        let hash = blake3::hash(&chunk.data).into();
        metas.push(ChunkMeta {
            hash,
            len: chunk.data.len() as u32,
        });
        if let Some(dir) = stage_dir
            && seen.insert(hash)
        {
            std::fs::write(dir.join(format!("{}.blk", hex(&hash))), &chunk.data)
                .context(format!("写入暂存块失败: {}", path.display()))?;
        }
    }
    Ok(metas)
}

/// 字节数组转小写十六进制。
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// 递归收集目录下所有普通文件，返回相对路径列表（排序稳定）。
pub fn list_files(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    collect_files(dir, dir, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_files(root: &Path, dir: &Path, files: &mut Vec<std::path::PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir).context(format!("读取目录失败: {}", dir.display()))? {
        let entry = entry.context("读取目录项失败")?;
        let path = entry.path();
        match (path.is_file(), path.is_dir()) {
            (true, _) => {
                let rel = path
                    .strip_prefix(root)
                    .context("路径前缀剥离失败")?
                    .to_path_buf();
                files.push(rel);
            }
            (false, true) => collect_files(root, &path, files)?,
            (false, false) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha20Rng;
    use std::fs;

    fn random_file(path: &Path, size: usize, seed: u64) {
        let mut rng = ChaCha20Rng::seed_from_u64(seed);
        let data: Vec<u8> = (0..size).map(|_| rng.random()).collect();
        fs::write(path, data).unwrap();
    }

    #[test]
    fn test_chunk_file_sum_lengths() {
        let dir = std::env::temp_dir().join("blaze-chunk-ok");
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("game.bin");
        random_file(&file, 3 * 1024 * 1024, 1);
        let mut seen = HashSet::new();
        let metas = chunk_file(&file, &ChunkParams::default(), None, &mut seen).unwrap();
        let total: u64 = metas.iter().map(|m| m.len as u64).sum();
        assert_eq!(total, 3 * 1024 * 1024);
        assert!(!metas.is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_chunk_file_stage_dedup() {
        let dir = std::env::temp_dir().join("blaze-chunk-stage");
        let stage = dir.join("chunks");
        fs::create_dir_all(&stage).unwrap();
        let file = dir.join("game.bin");
        random_file(&file, 1024 * 1024, 2);
        let mut seen = HashSet::new();
        let metas = chunk_file(&file, &ChunkParams::default(), Some(&stage), &mut seen).unwrap();
        let staged = fs::read_dir(&stage).unwrap().count();
        assert_eq!(staged, metas.len());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_chunk_file_missing_file() {
        let mut seen = HashSet::new();
        let err = chunk_file(
            Path::new("/tmp/不存在的文件.bin"),
            &ChunkParams::default(),
            None,
            &mut seen,
        )
        .unwrap_err();
        assert!(err.to_string().contains("打开文件失败"));
    }

    #[test]
    fn test_file_hash_matches_blake3() {
        let dir = std::env::temp_dir().join("blaze-hash");
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("data.bin");
        random_file(&file, 2 * 1024 * 1024, 3);
        let expected: [u8; 32] = blake3::hash(&fs::read(&file).unwrap()).into();
        assert_eq!(file_hash(&file).unwrap(), expected);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_file_hash_missing_file() {
        let err = file_hash(Path::new("/tmp/不存在的文件.bin")).unwrap_err();
        assert!(err.to_string().contains("打开文件失败"));
    }

    #[test]
    fn test_list_files_recursive() {
        let dir = std::env::temp_dir().join("blaze-list");
        fs::create_dir_all(dir.join("sub")).unwrap();
        fs::write(dir.join("b.bin"), b"b").unwrap();
        fs::write(dir.join("sub/a.bin"), b"a").unwrap();
        fs::write(dir.join("sub/c.bin"), b"c").unwrap();
        let files = list_files(&dir).unwrap();
        let names: Vec<String> = files
            .iter()
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .collect();
        assert_eq!(names, vec!["b.bin", "sub/a.bin", "sub/c.bin"]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_list_files_missing_dir() {
        let err = list_files(Path::new("/tmp/不存在的目录")).unwrap_err();
        assert!(err.to_string().contains("读取目录失败"));
    }

    #[cfg(unix)]
    #[test]
    fn test_list_files_ignores_broken_symlink() {
        let dir = std::env::temp_dir().join("blaze-list-symlink");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("a.bin"), b"a").unwrap();
        std::os::unix::fs::symlink(dir.join("不存在"), dir.join("broken")).unwrap();
        let files = list_files(&dir).unwrap();
        assert_eq!(files.len(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_hex() {
        assert_eq!(hex(&[0xab, 0x01]), "ab01");
    }
}
