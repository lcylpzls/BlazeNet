//! 更新引擎：拉索引 → 对账 → 清理 → 补缺 → 合并（真实文件原子替换）。
//! 设计见 docs/06-数据存储设计文档.md §3.2。
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use blaze_common::manifest::{FileEntry, GameIndex};
use blaze_common::update_plan::{self, UpdatePlan};

#[derive(Debug, Default, PartialEq, Eq)]
pub struct MergeSummary {
    pub merged: usize,
    pub deleted: usize,
    pub failed: Vec<String>,
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn parse_hex(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// 扫描临时块目录，返回已有块哈希集合（文件名 `<hex>.blk`）。
pub fn collect_temp_hashes(temp_dir: &Path) -> Result<HashSet<[u8; 32]>> {
    let mut hashes = HashSet::new();
    for entry in fs::read_dir(temp_dir).context("读取临时目录失败")? {
        let entry = entry.context("读取临时目录项失败")?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(stem) = name.strip_suffix(".blk")
            && let Some(hash) = parse_hex(stem)
        {
            hashes.insert(hash);
        }
    }
    Ok(hashes)
}

/// 生成更新计划（新/旧清单 + 临时块）。
pub fn plan_update(
    new_manifest: &[u8],
    old_manifest: Option<&[u8]>,
    temp_dir: &Path,
) -> Result<UpdatePlan> {
    let new = GameIndex::decode(new_manifest).context("解析新版本清单失败")?;
    let old = old_manifest
        .map(GameIndex::decode)
        .transpose()
        .context("解析旧版本清单失败")?;
    let temp = collect_temp_hashes(temp_dir)?;
    Ok(update_plan::compute(&new, old.as_ref(), &temp))
}

fn read_chunk_at(file: &Path, offset: u64, len: u32) -> Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = fs::File::open(file).context("打开文件失败")?;
    f.seek(SeekFrom::Start(offset)).context("定位失败")?;
    let mut buf = vec![0u8; len as usize];
    f.read_exact(&mut buf).context("读取块失败")?;
    Ok(buf)
}

fn write_new_file(
    game_dir: &Path,
    entry: &FileEntry,
    old_chunks: Option<&HashMap<[u8; 32], (u64, u32)>>,
    old_file: Option<&Path>,
    temp_dir: &Path,
) -> Result<()> {
    let target = game_dir.join(&entry.name);
    let new_path = target.with_extension("new");
    let parent = new_path.parent().context("缺少父目录")?;
    fs::create_dir_all(parent).context("创建父目录失败")?;
    // 流式拼装：逐块写入临时文件并增量哈希，避免整文件载入内存。
    let mut out = fs::File::create(&new_path).context("创建临时文件失败")?;
    let mut hasher = blake3::Hasher::new();
    for chunk in &entry.chunks {
        let data = if let (Some(old_file), Some(map)) = (old_file, old_chunks) {
            if old_file.is_file()
                && let Some((offset, len)) = map.get(&chunk.hash)
            {
                read_chunk_at(old_file, *offset, *len)?
            } else {
                fs::read(temp_dir.join(format!("{}.blk", hex(&chunk.hash))))
                    .context("读取临时块失败")?
            }
        } else {
            fs::read(temp_dir.join(format!("{}.blk", hex(&chunk.hash))))
                .context("读取临时块失败")?
        };
        hasher.update(&data);
        out.write_all(&data).context("写入临时文件失败")?;
    }
    let actual: [u8; 32] = hasher.finalize().into();
    if actual != entry.file_hash {
        drop(out);
        let _ = fs::remove_file(&new_path);
        return Err(anyhow!("文件哈希校验失败"));
    }
    out.sync_all().context("同步临时文件失败")?;
    if target.exists() {
        fs::remove_file(&target).context("删除旧文件失败")?;
    }
    fs::rename(&new_path, &target).context("原子替换失败")?;
    Ok(())
}

/// 合并真实文件：新文件/变化文件拼装替换，删除文件清理。
pub fn merge_files(
    game_dir: &Path,
    new: &GameIndex,
    old: Option<&GameIndex>,
    temp_dir: &Path,
) -> Result<MergeSummary> {
    let old_files: HashMap<&str, &FileEntry> = old
        .map(|o| o.files.iter().map(|f| (f.name.as_str(), f)).collect())
        .unwrap_or_default();
    let old_chunk_maps: HashMap<&str, HashMap<[u8; 32], (u64, u32)>> = old
        .map(|o| {
            o.files
                .iter()
                .map(|f| {
                    let mut map = HashMap::new();
                    let mut offset = 0u64;
                    for chunk in &f.chunks {
                        map.entry(chunk.hash).or_insert((offset, chunk.len));
                        offset += u64::from(chunk.len);
                    }
                    (f.name.as_str(), map)
                })
                .collect()
        })
        .unwrap_or_default();
    let old_names: HashSet<&str> = old_files.keys().copied().collect();
    let new_names: HashSet<&str> = new.files.iter().map(|f| f.name.as_str()).collect();

    let mut summary = MergeSummary::default();
    for entry in &new.files {
        let target_exists = game_dir.join(&entry.name).is_file();
        let need_merge = match old_files.get(entry.name.as_str()) {
            None => true,
            Some(prev) => prev.file_hash != entry.file_hash || !target_exists,
        };
        if !need_merge {
            continue;
        }
        let old_chunks = old_chunk_maps.get(entry.name.as_str());
        let old_file = old_files
            .get(entry.name.as_str())
            .map(|_| game_dir.join(&entry.name));
        match write_new_file(game_dir, entry, old_chunks, old_file.as_deref(), temp_dir) {
            Ok(()) => summary.merged += 1,
            Err(err) => {
                let _ = fs::remove_file(game_dir.join(&entry.name).with_extension("new"));
                summary.failed.push(format!("{}: {err}", entry.name));
            }
        }
    }
    for name in old_names.difference(&new_names) {
        let path = game_dir.join(name);
        if path.exists() {
            fs::remove_file(&path).context("删除文件失败")?;
            summary.deleted += 1;
        }
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use blaze_common::manifest::ChunkMeta;

    fn hash_of(data: &[u8]) -> [u8; 32] {
        blake3::hash(data).into()
    }

    fn entry(name: &str, file_hash: [u8; 32], chunks: Vec<(u8, u32)>) -> FileEntry {
        FileEntry {
            name: name.to_string(),
            file_hash,
            chunks: chunks
                .into_iter()
                .map(|(seed, len)| ChunkMeta {
                    hash: [seed; 32],
                    len,
                })
                .collect(),
        }
    }

    fn index(files: Vec<FileEntry>) -> GameIndex {
        GameIndex::build(files)
    }

    #[test]
    fn test_collect_temp_hashes() {
        let dir = std::env::temp_dir().join("blaze-upd-temp");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let h = [7u8; 32];
        fs::write(dir.join(format!("{}.blk", hex(&h))), b"x").unwrap();
        fs::write(dir.join("bad.txt"), b"x").unwrap();
        fs::write(dir.join("abc.blk"), b"x").unwrap();
        let hashes = collect_temp_hashes(&dir).unwrap();
        assert!(hashes.contains(&h));
        assert_eq!(hashes.len(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_plan_update_delta() {
        let dir = std::env::temp_dir().join("blaze-upd-plan");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let old = index(vec![entry("a.bin", [1; 32], vec![(1, 2)])]);
        let new = index(vec![
            entry("a.bin", [2; 32], vec![(1, 2), (3, 3)]),
            entry("b.bin", [3; 32], vec![(4, 1)]),
        ]);
        let old_bytes = old.encode().unwrap();
        let new_bytes = new.encode().unwrap();
        let plan = plan_update(&new_bytes, Some(&old_bytes), &dir).unwrap();
        assert_eq!(plan.files_to_update, vec!["a.bin"]);
        assert_eq!(plan.files_to_download, vec!["b.bin"]);
        assert_eq!(plan.chunks_to_download, vec![[3; 32], [4; 32]]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_files_success_and_delete() {
        let dir = std::env::temp_dir().join("blaze-upd-merge");
        let _ = fs::remove_dir_all(&dir);
        let game_dir = dir.join("game");
        fs::create_dir_all(&game_dir).unwrap();
        let temp_dir = dir.join("temp");
        fs::create_dir_all(&temp_dir).unwrap();

        // 旧版：a.bin = "hello"，b.bin = "bye"
        let old_a = b"hello".to_vec();
        let old_b = b"bye".to_vec();
        fs::write(game_dir.join("a.bin"), &old_a).unwrap();
        fs::write(game_dir.join("b.bin"), &old_b).unwrap();
        fs::write(game_dir.join("d.bin"), b"same").unwrap();
        let old = index(vec![
            entry("a.bin", hash_of(&old_a), vec![(1, 2), (2, 3)]),
            entry("b.bin", hash_of(&old_b), vec![(5, 3)]),
            entry("d.bin", hash_of(b"same"), vec![(7, 4)]),
            entry("ghost.bin", [8; 32], vec![(8, 1)]),
        ]);

        // 新版：a.bin = "hello!"（复用旧块 "he" + 新块 "llo!"），b.bin 删除
        let new_a = b"hello!".to_vec();
        let llo_bang = b"llo!".to_vec();
        let new = index(vec![
            entry("a.bin", hash_of(&new_a), vec![(1, 2), (9, 4)]),
            entry("c.bin", hash_of(b"new"), vec![(8, 3)]),
            entry("d.bin", hash_of(b"same"), vec![(7, 4)]),
        ]);
        fs::write(temp_dir.join(format!("{}.blk", hex(&[9u8; 32]))), &llo_bang).unwrap();
        fs::write(temp_dir.join(format!("{}.blk", hex(&[8u8; 32]))), b"new").unwrap();

        let summary = merge_files(&game_dir, &new, Some(&old), &temp_dir).unwrap();
        assert_eq!(summary.merged, 2);
        assert_eq!(summary.deleted, 1);
        assert!(summary.failed.is_empty());
        assert_eq!(fs::read(game_dir.join("a.bin")).unwrap(), new_a);
        assert_eq!(fs::read(game_dir.join("c.bin")).unwrap(), b"new");
        assert_eq!(fs::read(game_dir.join("d.bin")).unwrap(), b"same");
        assert!(!game_dir.join("b.bin").exists());
        assert!(!game_dir.join("ghost.bin").exists());
        assert!(!game_dir.join("a.bin.new").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_hash_mismatch_keeps_old() {
        let dir = std::env::temp_dir().join("blaze-upd-hash");
        let _ = fs::remove_dir_all(&dir);
        let game_dir = dir.join("game");
        fs::create_dir_all(&game_dir).unwrap();
        let temp_dir = dir.join("temp");
        fs::create_dir_all(&temp_dir).unwrap();
        let old_a = b"hello".to_vec();
        fs::write(game_dir.join("a.bin"), &old_a).unwrap();
        let old = index(vec![entry("a.bin", hash_of(&old_a), vec![(1, 2), (2, 3)])]);
        // 块齐全但清单 file_hash 错误 → 校验失败
        let new = index(vec![entry("a.bin", [9; 32], vec![(1, 2), (2, 3)])]);
        let summary = merge_files(&game_dir, &new, Some(&old), &temp_dir).unwrap();
        assert_eq!(summary.failed.len(), 1);
        assert!(summary.failed[0].contains("哈希校验失败"));
        assert_eq!(fs::read(game_dir.join("a.bin")).unwrap(), old_a);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_missing_chunk_keeps_old() {
        let dir = std::env::temp_dir().join("blaze-upd-fail");
        let _ = fs::remove_dir_all(&dir);
        let game_dir = dir.join("game");
        fs::create_dir_all(&game_dir).unwrap();
        let temp_dir = dir.join("temp");
        fs::create_dir_all(&temp_dir).unwrap();
        let old_a = b"hello".to_vec();
        fs::write(game_dir.join("a.bin"), &old_a).unwrap();
        let old = index(vec![entry("a.bin", hash_of(&old_a), vec![(1, 2), (2, 3)])]);
        let new = index(vec![entry("a.bin", [9; 32], vec![(1, 2), (9, 4)])]);
        let summary = merge_files(&game_dir, &new, Some(&old), &temp_dir).unwrap();
        assert_eq!(summary.failed.len(), 1);
        assert_eq!(summary.merged, 0);
        assert_eq!(fs::read(game_dir.join("a.bin")).unwrap(), old_a);
        assert!(!game_dir.join("a.bin.new").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_repairs_missing_file() {
        let dir = std::env::temp_dir().join("blaze-upd-repair");
        let _ = fs::remove_dir_all(&dir);
        let game_dir = dir.join("game");
        fs::create_dir_all(&game_dir).unwrap();
        let temp_dir = dir.join("temp");
        fs::create_dir_all(&temp_dir).unwrap();
        let data = b"hello".to_vec();
        let hash = hash_of(&data);
        let manifest = index(vec![FileEntry {
            name: "a.bin".to_string(),
            file_hash: hash,
            chunks: vec![ChunkMeta {
                hash,
                len: data.len() as u32,
            }],
        }]);
        fs::write(game_dir.join("a.bin"), &data).unwrap();
        fs::remove_file(game_dir.join("a.bin")).unwrap();
        fs::write(temp_dir.join(format!("{}.blk", hex(&hash))), &data).unwrap();
        let summary = merge_files(&game_dir, &manifest, Some(&manifest), &temp_dir).unwrap();
        assert!(summary.failed.is_empty());
        assert_eq!(summary.merged, 1);
        assert_eq!(fs::read(game_dir.join("a.bin")).unwrap(), data);
        let _ = fs::remove_dir_all(&dir);
    }
}
