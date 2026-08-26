//! 制作机工具库：配置、分块、版本清单与差异计算。
pub mod chunker;
pub mod config;
pub mod delta;
pub mod manifest;

use anyhow::{Context, Result, bail};
use std::collections::HashSet;
use std::path::Path;

use crate::config::Config;
use crate::manifest::{FileEntry, GameIndex};

/// 程序入口：按 Windows 规范自动定位配置文件后执行。
pub fn run() -> Result<()> {
    let path = Config::default_path()?;
    run_with_config(&path)
}

/// 按指定配置文件执行完整制作流程。
pub fn run_with_config(path: &Path) -> Result<()> {
    let config = Config::load(path)?;
    let files = chunker::list_files(&config.source_dir)?;
    if files.is_empty() {
        bail!("source_dir 下没有文件: {}", config.source_dir.display());
    }

    let stage_dir = config.output_dir.join("chunks");
    let index_dir = config.output_dir.join("index");
    let delta_dir = config.output_dir.join("delta");
    std::fs::create_dir_all(&stage_dir)
        .context(format!("创建暂存目录失败: {}", stage_dir.display()))?;
    std::fs::create_dir_all(&index_dir)
        .context(format!("创建清单目录失败: {}", index_dir.display()))?;
    std::fs::create_dir_all(&delta_dir)
        .context(format!("创建差异目录失败: {}", delta_dir.display()))?;

    let mut seen = HashSet::new();
    let mut entries = Vec::new();
    for rel in &files {
        let full = config.source_dir.join(rel);
        let chunks = chunker::chunk_file(&full, &config.chunk, Some(&stage_dir), &mut seen)?;
        let file_hash = chunker::file_hash(&full)?;
        entries.push(FileEntry {
            name: rel.to_string_lossy().replace('\\', "/"),
            file_hash,
            chunks,
        });
    }
    let index = GameIndex::build(entries);
    let index_path = index_dir.join(format!("{}.gameindex", config.version));
    std::fs::write(&index_path, index.encode()?)
        .context(format!("写入版本清单失败: {}", index_path.display()))?;

    let old = match &config.previous_dir {
        Some(prev) if prev.is_dir() => {
            let prev_files = chunker::list_files(prev)?;
            let mut prev_entries = Vec::new();
            let mut prev_seen = HashSet::new();
            for rel in &prev_files {
                let full = prev.join(rel);
                let chunks = chunker::chunk_file(&full, &config.chunk, None, &mut prev_seen)?;
                let file_hash = chunker::file_hash(&full)?;
                prev_entries.push(FileEntry {
                    name: rel.to_string_lossy().replace('\\', "/"),
                    file_hash,
                    chunks,
                });
            }
            Some(GameIndex::build(prev_entries))
        }
        _ => None,
    };

    let plan = delta::compute(&index, old.as_ref());
    let delta_json = serde_json::json!({
        "game_id": config.game_id,
        "version": config.version,
        "new_chunk_count": plan.new_chunks.len(),
        "new_bytes": plan.new_bytes,
        "reused_chunk_count": plan.reused_chunks,
        "chunk_hashes": plan.new_chunks.iter().map(|h| chunker::hex(h)).collect::<Vec<_>>(),
    });
    let delta_path = delta_dir.join(format!("{}.json", config.version));
    std::fs::write(&delta_path, serde_json::to_vec_pretty(&delta_json)?)
        .context(format!("写入差异清单失败: {}", delta_path.display()))?;

    println!("制作完成: 游戏 {} 版本 {}", config.game_id, config.version);
    println!(
        "  文件数: {}，差异块数: {}（{:.1}MiB），复用块数: {}",
        files.len(),
        plan.new_chunks.len(),
        plan.new_bytes as f64 / 1024.0 / 1024.0,
        plan.reused_chunks
    );
    println!("  版本清单: {}", index_path.display());
    println!("  差异清单: {}", delta_path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha20Rng;
    use std::fs;

    fn write_random(path: &Path, size: usize, seed: u64) {
        let mut rng = ChaCha20Rng::seed_from_u64(seed);
        let data: Vec<u8> = (0..size).map(|_| rng.random()).collect();
        fs::write(path, data).unwrap();
    }

    fn write_config(
        dir: &Path,
        source: &Path,
        previous: &Path,
        output: &Path,
    ) -> std::path::PathBuf {
        let path = dir.join("producer.toml");
        let text = format!(
            "game_id = 7\nversion = 2\nsource_dir = \"{}\"\nprevious_dir = \"{}\"\noutput_dir = \"{}\"\n",
            source.display(),
            previous.display(),
            output.display()
        );
        fs::write(&path, text).unwrap();
        path
    }

    #[test]
    fn test_run_with_config_happy_path() {
        let dir = std::env::temp_dir().join("blaze-run-ok");
        let _ = fs::remove_dir_all(&dir);
        let previous = dir.join("previous");
        let source = dir.join("source");
        let output = dir.join("output");
        fs::create_dir_all(&previous).unwrap();
        fs::create_dir_all(&source).unwrap();
        write_random(&previous.join("game.bin"), 4 * 1024 * 1024, 1);
        // 新版 = 旧版 + 追加 2MiB
        let mut new_data = fs::read(previous.join("game.bin")).unwrap();
        let mut rng = ChaCha20Rng::seed_from_u64(2);
        new_data.extend((0..2 * 1024 * 1024).map(|_| rng.random::<u8>()));
        fs::write(source.join("game.bin"), new_data).unwrap();

        let cfg_path = write_config(&dir, &source, &previous, &output);
        run_with_config(&cfg_path).unwrap();

        let index_path = output.join("index/2.gameindex");
        let delta_path = output.join("delta/2.json");
        assert!(index_path.exists());
        assert!(delta_path.exists());
        let delta: serde_json::Value =
            serde_json::from_slice(&fs::read(&delta_path).unwrap()).unwrap();
        assert!(delta["new_chunk_count"].as_u64().unwrap() > 0);
        assert!(delta["reused_chunk_count"].as_u64().unwrap() > 0);
        assert!(
            !fs::read_dir(output.join("chunks"))
                .unwrap()
                .next()
                .is_none()
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_run_with_config_missing_file() {
        let err = run_with_config(Path::new("/tmp/不存在的配置.toml")).unwrap_err();
        assert!(err.to_string().contains("读取配置文件失败"));
    }

    #[test]
    fn test_run_with_config_empty_source() {
        let dir = std::env::temp_dir().join("blaze-run-empty");
        let _ = fs::remove_dir_all(&dir);
        let source = dir.join("source");
        let output = dir.join("output");
        fs::create_dir_all(&source).unwrap();
        let cfg_path = dir.join("producer.toml");
        fs::write(
            &cfg_path,
            format!(
                "game_id = 1\nversion = 1\nsource_dir = \"{}\"\noutput_dir = \"{}\"\n",
                source.display(),
                output.display()
            ),
        )
        .unwrap();
        let err = run_with_config(&cfg_path).unwrap_err();
        assert!(err.to_string().contains("没有文件"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_run_with_config_all_new_when_previous_missing() {
        let dir = std::env::temp_dir().join("blaze-run-new");
        let _ = fs::remove_dir_all(&dir);
        let source = dir.join("source");
        let output = dir.join("output");
        fs::create_dir_all(&source).unwrap();
        write_random(&source.join("a.bin"), 512 * 1024, 3);
        let cfg_path = dir.join("producer.toml");
        fs::write(
            &cfg_path,
            format!(
                "game_id = 2\nversion = 1\nsource_dir = \"{}\"\nprevious_dir = \"/tmp/不存在的旧版\"\noutput_dir = \"{}\"\n",
                source.display(),
                output.display()
            ),
        )
        .unwrap();
        run_with_config(&cfg_path).unwrap();
        let delta: serde_json::Value =
            serde_json::from_slice(&fs::read(output.join("delta/1.json")).unwrap()).unwrap();
        assert_eq!(delta["reused_chunk_count"].as_u64().unwrap(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_run_default_path_missing_config() {
        let path = Config::default_path().unwrap();
        assert_eq!(run().is_ok(), path.exists());
    }
}
