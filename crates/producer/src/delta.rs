//! 新旧版本差异计算：以块哈希集合差确定需要上传的差异块。
use std::collections::HashSet;

use crate::manifest::GameIndex;

/// 差异结果：只包含新版本有、旧版本没有的块。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaPlan {
    /// 需要上传的块（按清单顺序去重）。
    pub new_chunks: Vec<[u8; 32]>,
    /// 新版本中可复用的块数（旧版本已有）。
    pub reused_chunks: usize,
    /// 差异块总字节数。
    pub new_bytes: u64,
}

/// 计算新旧版本差异；`old` 为 `None` 时视为全新游戏（全部块为新块）。
pub fn compute(new: &GameIndex, old: Option<&GameIndex>) -> DeltaPlan {
    let old_set: HashSet<[u8; 32]> = old.map(GameIndex::chunk_set).unwrap_or_default();
    let mut new_chunks = Vec::new();
    let mut seen = HashSet::new();
    let mut reused_chunks = 0usize;
    let mut new_bytes = 0u64;
    for file in &new.files {
        for chunk in &file.chunks {
            if old_set.contains(&chunk.hash) {
                if seen.insert(chunk.hash) {
                    reused_chunks += 1;
                }
            } else if seen.insert(chunk.hash) {
                new_chunks.push(chunk.hash);
                new_bytes += chunk.len as u64;
            }
        }
    }
    DeltaPlan {
        new_chunks,
        reused_chunks,
        new_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{FORMAT_VERSION, FileEntry};

    fn index(hashes: &[u8]) -> GameIndex {
        GameIndex {
            format_version: FORMAT_VERSION,
            files: vec![FileEntry {
                name: "a.bin".to_string(),
                file_hash: [0; 32],
                chunks: hashes
                    .iter()
                    .map(|h| crate::chunker::ChunkMeta {
                        hash: [*h; 32],
                        len: 100,
                    })
                    .collect(),
            }],
            manifest_hash: [0; 32],
        }
    }

    #[test]
    fn test_compute_all_new() {
        let plan = compute(&index(&[1, 2, 3]), None);
        assert_eq!(plan.new_chunks.len(), 3);
        assert_eq!(plan.reused_chunks, 0);
        assert_eq!(plan.new_bytes, 300);
    }

    #[test]
    fn test_compute_partial_reuse() {
        let plan = compute(&index(&[1, 2, 3]), Some(&index(&[1, 4])));
        assert_eq!(plan.new_chunks, vec![[2; 32], [3; 32]]);
        assert_eq!(plan.reused_chunks, 1);
        assert_eq!(plan.new_bytes, 200);
    }

    #[test]
    fn test_compute_no_change() {
        let plan = compute(&index(&[1, 2]), Some(&index(&[1, 2])));
        assert!(plan.new_chunks.is_empty());
        assert_eq!(plan.reused_chunks, 2);
        assert_eq!(plan.new_bytes, 0);
    }

    #[test]
    fn test_compute_dedup_duplicate_within_new() {
        let mut new = index(&[1, 2]);
        new.files.push(crate::manifest::FileEntry {
            name: "b.bin".to_string(),
            file_hash: [0; 32],
            chunks: vec![crate::chunker::ChunkMeta {
                hash: [1; 32],
                len: 100,
            }],
        });
        let plan = compute(&new, None);
        assert_eq!(plan.new_chunks.len(), 2);
        assert_eq!(plan.new_bytes, 200);
    }
}
