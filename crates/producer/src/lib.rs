//! 制作机工具库：配置、分块、版本清单与差异计算。
pub mod chunker;
pub mod config;
pub mod upload;

use anyhow::{Context, Result, bail};
use blaze_common::manifest::{FileEntry, GameIndex};
use blaze_common::update_plan;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::config::Config;

/// 输出目录单实例锁：存在期间阻止并发制作，Drop 时释放。
struct OutputLock(PathBuf);

impl Drop for OutputLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// 程序入口：按 Windows 规范自动定位配置文件后执行。
pub fn run() -> Result<()> {
    let path = Config::default_path()?;
    run_with_config(&path)
}

/// 按指定配置文件执行完整制作流程。
pub fn run_with_config(path: &Path) -> Result<()> {
    let config = Config::load(path)?;
    std::fs::create_dir_all(&config.output_dir)
        .context(format!("创建输出目录失败: {}", config.output_dir.display()))?;
    let lock_path = config.output_dir.join(".producer.lock");
    std::fs::File::create_new(&lock_path)
        .with_context(|| format!("已有制作实例占用输出目录: {}", lock_path.display()))?;
    let _lock = OutputLock(lock_path);
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
    for (idx, rel) in files.iter().enumerate() {
        let full = config.source_dir.join(rel);
        let chunks = chunker::chunk_file(&full, &config.chunk, Some(&stage_dir), &mut seen)?;
        let file_hash = chunker::file_hash(&full)?;
        println!(
            "分块进度 {}/{}: {}",
            idx + 1,
            files.len(),
            rel.to_string_lossy()
        );
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
            for (idx, rel) in prev_files.iter().enumerate() {
                let full = prev.join(rel);
                let chunks = chunker::chunk_file(&full, &config.chunk, None, &mut prev_seen)?;
                let file_hash = chunker::file_hash(&full)?;
                println!(
                    "比对上一版本进度 {}/{}: {}",
                    idx + 1,
                    prev_files.len(),
                    rel.to_string_lossy()
                );
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

    let plan = update_plan::compute(&index, old.as_ref(), &HashSet::new());
    let delta_json = serde_json::json!({
        "game_id": config.game_id,
        "version": config.version,
        "files_to_download_count": plan.files_to_download.len(),
        "files_to_update_count": plan.files_to_update.len(),
        "files_to_delete_count": plan.files_to_delete.len(),
        "new_chunk_count": plan.chunks_to_download.len(),
        "new_bytes": plan.download_bytes,
        "chunk_hashes": plan.chunks_to_download.iter().map(|h| chunker::hex(h)).collect::<Vec<_>>(),
    });
    let delta_path = delta_dir.join(format!("{}.json", config.version));
    std::fs::write(&delta_path, serde_json::to_vec_pretty(&delta_json)?)
        .context(format!("写入差异清单失败: {}", delta_path.display()))?;

    // 配置 origin_addr 时执行上传：秒传 → 流式上传 → 提交版本
    if let Some(addr) = &config.origin_addr {
        println!(
            "开始上传差异块: {} 块（{:.1}MiB）",
            plan.chunks_to_download.len(),
            plan.download_bytes as f64 / 1024.0 / 1024.0
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("创建异步运行时失败")?;
        let result: anyhow::Result<upload::UploadSummary> = runtime.block_on(async {
            let mut client =
                blaze_proto::upload::upload_client::UploadClient::connect(addr.clone())
                    .await
                    .context(format!("连接原始节点失败: {addr}"))?;
            // 流式上传：边读暂存块边发送，避免全部差异块载入内存。
            let summary = upload::upload_delta(
                &mut client,
                config.game_id,
                &stage_dir,
                &plan.chunks_to_download,
                &config.origin_token,
            )
            .await?;
            upload::commit_version(
                &mut client,
                config.game_id,
                config.version,
                index.encode()?,
                index.manifest_hash,
                &config.origin_token,
            )
            .await?;
            Ok(summary)
        });
        let summary = result?;
        println!(
            "  上传完成: 新传 {} 块，秒传跳过 {} 块，失败 {} 块",
            summary.uploaded,
            summary.skipped,
            summary.failed.len()
        );
    }

    println!("制作完成: 游戏 {} 版本 {}", config.game_id, config.version);
    println!(
        "  文件数: {}，新文件: {}，更新文件: {}，删除文件: {}，差异块数: {}（{:.1}MiB）",
        files.len(),
        plan.files_to_download.len(),
        plan.files_to_update.len(),
        plan.files_to_delete.len(),
        plan.chunks_to_download.len(),
        plan.download_bytes as f64 / 1024.0 / 1024.0,
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

    fn spawn_origin(data_dir: &Path) -> (String, tokio::sync::oneshot::Sender<()>) {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let url = format!("http://{addr}");
        let dir = data_dir.to_path_buf();
        let (tx, rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let _handle = origin::server::serve(addr, origin::server::UploadService::new(dir))
                    .await
                    .unwrap();
                let _ = rx.await;
            });
        });
        (url, tx)
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
        assert_eq!(delta["files_to_update_count"].as_u64().unwrap(), 1);
        assert_eq!(delta["files_to_download_count"].as_u64().unwrap(), 0);
        assert_eq!(delta["files_to_delete_count"].as_u64().unwrap(), 0);
        assert!(
            !fs::read_dir(output.join("chunks"))
                .unwrap()
                .next()
                .is_none()
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_run_locked_by_another_instance() {
        let dir = std::env::temp_dir().join("blaze-run-lock");
        let _ = fs::remove_dir_all(&dir);
        let previous = dir.join("previous");
        let source = dir.join("source");
        let output = dir.join("output");
        fs::create_dir_all(&previous).unwrap();
        fs::create_dir_all(&source).unwrap();
        fs::create_dir_all(&output).unwrap();
        write_random(&previous.join("a.bin"), 1024, 1);
        write_random(&source.join("a.bin"), 2048, 2);
        fs::write(output.join(".producer.lock"), b"locked").unwrap();
        let cfg_path = write_config(&dir, &source, &previous, &output);
        let err = run_with_config(&cfg_path).unwrap_err();
        assert!(err.to_string().contains("已有制作实例"));
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
        assert_eq!(delta["files_to_download_count"].as_u64().unwrap(), 1);
        assert_eq!(delta["files_to_update_count"].as_u64().unwrap(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_run_default_path_missing_config() {
        let path = Config::default_path().unwrap();
        assert_eq!(run().is_ok(), path.exists());
    }

    #[test]
    fn test_run_with_config_upload() {
        let dir = std::env::temp_dir().join("blaze-run-upload");
        let _ = fs::remove_dir_all(&dir);
        let server_dir = dir.join("origin-data");
        fs::create_dir_all(&server_dir).unwrap();
        let (url, shutdown) = spawn_origin(&server_dir);
        let previous = dir.join("previous");
        let source = dir.join("source");
        let output = dir.join("output");
        fs::create_dir_all(&previous).unwrap();
        fs::create_dir_all(&source).unwrap();
        write_random(&previous.join("game.bin"), 4 * 1024 * 1024, 1);
        let mut new_data = fs::read(previous.join("game.bin")).unwrap();
        let mut rng = ChaCha20Rng::seed_from_u64(2);
        new_data.extend((0..2 * 1024 * 1024).map(|_| rng.random::<u8>()));
        fs::write(source.join("game.bin"), new_data).unwrap();

        // 先发布 v1（全新游戏，全量上传）
        let cfg_v1 = dir.join("producer-v1.toml");
        fs::write(
            &cfg_v1,
            format!(
                "game_id = 1\nversion = 1\nsource_dir = \"{}\"\noutput_dir = \"{}\"\norigin_addr = \"{}\"\n",
                previous.display(),
                output.display(),
                url
            ),
        )
        .unwrap();
        run_with_config(&cfg_v1).unwrap();
        assert!(server_dir.join("1/published/1.gameindex").exists());

        // 再发布 v2（增量更新）
        let cfg_v2 = dir.join("producer-v2.toml");
        fs::write(
            &cfg_v2,
            format!(
                "game_id = 1\nversion = 2\nsource_dir = \"{}\"\nprevious_dir = \"{}\"\noutput_dir = \"{}\"\norigin_addr = \"{}\"\n",
                source.display(),
                previous.display(),
                output.display(),
                url
            ),
        )
        .unwrap();
        run_with_config(&cfg_v2).unwrap();
        assert!(server_dir.join("1/published/2.gameindex").exists());
        let _ = shutdown.send(());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_run_with_config_upload_connect_failure() {
        let dir = std::env::temp_dir().join("blaze-run-upload-fail");
        let _ = fs::remove_dir_all(&dir);
        let source = dir.join("source");
        let output = dir.join("output");
        fs::create_dir_all(&source).unwrap();
        write_random(&source.join("a.bin"), 512 * 1024, 3);
        let cfg_path = dir.join("producer.toml");
        fs::write(
            &cfg_path,
            format!(
                "game_id = 1\nversion = 1\nsource_dir = \"{}\"\noutput_dir = \"{}\"\norigin_addr = \"http://127.0.0.1:1\"\n",
                source.display(),
                output.display()
            ),
        )
        .unwrap();
        let err = run_with_config(&cfg_path).unwrap_err();
        assert!(err.to_string().contains("连接原始节点失败"));
        let _ = fs::remove_dir_all(&dir);
    }
}
