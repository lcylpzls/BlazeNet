//! 更新计划：对比新旧版本清单，输出文件级与块级差异。
//! 设计见 docs/06-数据存储设计文档.md §3.2。
use std::collections::{HashMap, HashSet};

use crate::manifest::{FileEntry, GameIndex};

/// 新旧版本对比结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePlan {
    /// 新文件（全部块需下载）。
    pub files_to_download: Vec<String>,
    /// 内容变化文件（同名且 file_hash 不同）。
    pub files_to_update: Vec<String>,
    /// 需要删除的文件（旧有新无，按名称排序）。
    pub files_to_delete: Vec<String>,
    /// 需要下载的块（全局去重，按新清单顺序）。
    pub chunks_to_download: Vec<[u8; 32]>,
    /// 下载块总字节数。
    pub download_bytes: u64,
}

/// 对比新旧版本；`old` 为 `None` 时视为全新游戏。
/// `temp_chunks` 为网吧本地临时目录已有的块，可跳过下载。
pub fn compute(
    new: &GameIndex,
    old: Option<&GameIndex>,
    temp_chunks: &HashSet<[u8; 32]>,
) -> UpdatePlan {
    let old_files: HashMap<&str, &FileEntry> = old
        .map(|o| o.files.iter().map(|f| (f.name.as_str(), f)).collect())
        .unwrap_or_default();
    let old_names: HashSet<&str> = old_files.keys().copied().collect();

    let mut files_to_download = Vec::new();
    let mut files_to_update = Vec::new();
    for file in &new.files {
        match old_files.get(file.name.as_str()) {
            None => files_to_download.push(file.name.clone()),
            Some(prev) if prev.file_hash != file.file_hash => {
                files_to_update.push(file.name.clone())
            }
            Some(_) => {}
        }
    }

    let new_names: HashSet<&str> = new.files.iter().map(|f| f.name.as_str()).collect();
    let mut files_to_delete: Vec<String> = old_names
        .difference(&new_names)
        .map(|s| s.to_string())
        .collect();
    files_to_delete.sort();

    let old_chunks: HashSet<[u8; 32]> = old.map(GameIndex::chunk_set).unwrap_or_default();
    let mut chunks_to_download = Vec::new();
    let mut seen = HashSet::new();
    let mut download_bytes = 0u64;
    for file in &new.files {
        for chunk in &file.chunks {
            if old_chunks.contains(&chunk.hash) || temp_chunks.contains(&chunk.hash) {
                continue;
            }
            if seen.insert(chunk.hash) {
                chunks_to_download.push(chunk.hash);
                download_bytes += chunk.len as u64;
            }
        }
    }

    UpdatePlan {
        files_to_download,
        files_to_update,
        files_to_delete,
        chunks_to_download,
        download_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{ChunkMeta, FORMAT_VERSION, FileEntry};

    fn entry(name: &str, file_hash: u8, hashes: &[u8]) -> FileEntry {
        FileEntry {
            name: name.to_string(),
            file_hash: [file_hash; 32],
            chunks: hashes
                .iter()
                .map(|h| ChunkMeta {
                    hash: [*h; 32],
                    len: 100,
                })
                .collect(),
        }
    }

    fn index(files: Vec<FileEntry>) -> GameIndex {
        GameIndex {
            format_version: FORMAT_VERSION,
            files,
            manifest_hash: [0; 32],
        }
    }

    #[test]
    fn test_all_new_with_temp_skip() {
        let new = index(vec![entry("a.bin", 1, &[1, 2, 3])]);
        let temp: HashSet<[u8; 32]> = HashSet::from([[2; 32]]);
        let plan = compute(&new, None, &temp);
        assert_eq!(plan.files_to_download, vec!["a.bin"]);
        assert!(plan.files_to_update.is_empty());
        assert!(plan.files_to_delete.is_empty());
        assert_eq!(plan.chunks_to_download, vec![[1; 32], [3; 32]]);
        assert_eq!(plan.download_bytes, 200);
    }

    #[test]
    fn test_mixed_files() {
        let old = index(vec![
            entry("same.bin", 1, &[1]),
            entry("changed.bin", 2, &[2, 3]),
            entry("gone.bin", 3, &[4]),
        ]);
        let new = index(vec![
            entry("same.bin", 1, &[1]),
            entry("changed.bin", 9, &[2, 5]),
            entry("added.bin", 4, &[6]),
        ]);
        let plan = compute(&new, Some(&old), &HashSet::new());
        assert_eq!(plan.files_to_download, vec!["added.bin"]);
        assert_eq!(plan.files_to_update, vec!["changed.bin"]);
        assert_eq!(plan.files_to_delete, vec!["gone.bin"]);
        assert_eq!(plan.chunks_to_download, vec![[5; 32], [6; 32]]);
        assert_eq!(plan.download_bytes, 200);
    }

    #[test]
    fn test_shared_chunk_dedup_across_files() {
        let old = index(vec![entry("old.bin", 1, &[1])]);
        let new = index(vec![entry("a.bin", 2, &[1, 2]), entry("b.bin", 3, &[2, 3])]);
        let plan = compute(&new, Some(&old), &HashSet::new());
        assert_eq!(plan.chunks_to_download, vec![[2; 32], [3; 32]]);
        assert_eq!(plan.download_bytes, 200);
    }

    #[test]
    fn test_delete_sorted() {
        let old = index(vec![entry("b.bin", 1, &[1]), entry("a.bin", 2, &[2])]);
        let new = index(vec![entry("c.bin", 3, &[3])]);
        let plan = compute(&new, Some(&old), &HashSet::new());
        assert_eq!(plan.files_to_delete, vec!["a.bin", "b.bin"]);
    }
}
