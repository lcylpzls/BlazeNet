//! 联调检查工具：遍历块库索引、逐块哈希校验、解析版本清单并统计版本间共享块。
use anyhow::{Context, Result, bail};
use blaze_common::manifest::GameIndex;
use origin::storage::GameStore;
use std::collections::HashSet;
use std::path::Path;

fn parse_hash(text: &str) -> Result<[u8; 32]> {
    if text.len() != 64 {
        bail!("哈希长度非法: {text}");
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)
            .with_context(|| format!("哈希含非十六进制字符: {text}"))?;
    }
    Ok(out)
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        bail!("用法: inspect <数据目录> <game_id>");
    }
    let data_dir = Path::new(&args[1]);
    let game_id: u64 = args[2].parse().context("game_id 必须是数字")?;
    let store = GameStore::open(data_dir, game_id)?;
    println!(
        "pack 大小: {} 字节 ({:.3} GiB)",
        store.size(),
        store.size() as f64 / 1024.0 / 1024.0 / 1024.0
    );
    let entries = store.entries()?;
    println!("索引项数（唯一块）: {}", entries.len());
    let mut ok = 0u64;
    let mut bad = 0u64;
    for (key, _offset, _len) in &entries {
        let Ok(hash) = parse_hash(&key[16..]) else {
            bad += 1;
            continue;
        };
        match store.read_chunk(&hash) {
            Ok(Some(data)) if blake3::hash(&data).as_bytes() == &hash => ok += 1,
            _ => bad += 1,
        }
    }
    println!("逐块哈希校验: 通过 {ok}，失败 {bad}");

    let published = data_dir.join(game_id.to_string()).join("published");
    let mut versions: Vec<(u64, HashSet<[u8; 32]>, usize)> = Vec::new();
    for entry in std::fs::read_dir(&published).context("读取 published 目录失败")? {
        let entry = entry.context("读取 published 目录项失败")?;
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(ver_text) = name.strip_suffix(".gameindex") else {
            continue;
        };
        let version: u64 = ver_text.parse()?;
        let bytes = std::fs::read(entry.path()).context("读取版本清单失败")?;
        let index = GameIndex::decode(&bytes).context("解析版本清单失败")?;
        let chunks = index.chunk_set();
        let missing: Vec<[u8; 32]> = chunks
            .iter()
            .filter(|hash| !store.contains(hash).unwrap_or(false))
            .copied()
            .collect();
        let hash_ok = index.manifest_hash == *blake3::hash(&bytes[..bytes.len() - 32]).as_bytes();
        println!(
            "版本 {version}: 文件 {}，引用块 {}，索引缺失 {}，manifest_hash 校验 {}",
            index.files.len(),
            chunks.len(),
            missing.len(),
            if hash_ok { "通过" } else { "失败" }
        );
        versions.push((version, chunks, index.files.len()));
    }
    if versions.len() >= 2 {
        let (v1, set1, _) = &versions[0];
        let (v2, set2, _) = &versions[1];
        let shared = set1.intersection(set2).count();
        let only_v1 = set1.difference(set2).count();
        let only_v2 = set2.difference(set1).count();
        println!("版本间共享块: {shared}；仅 v{v1}: {only_v1}；仅 v{v2}: {only_v2}");
    }
    Ok(())
}
