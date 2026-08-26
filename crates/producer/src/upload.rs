//! 制作机上传客户端：秒传查重、流式上传、版本提交。
use anyhow::{Context, Result, anyhow, bail};
use blaze_proto::upload::upload_client::UploadClient;
use blaze_proto::upload::{ChunkQuery, CommitRequest, UploadChunk};
use std::path::Path;
use std::str::FromStr;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

use crate::chunker::hex;

/// 上传结果汇总。
#[derive(Debug, Default, PartialEq, Eq)]
pub struct UploadSummary {
    pub uploaded: usize,
    pub skipped: usize,
    pub failed: Vec<[u8; 32]>,
}

fn attach_token<T>(request: &mut Request<T>, token: &Option<String>) -> Result<()> {
    if let Some(token) = token {
        let value =
            MetadataValue::from_str(&format!("Bearer {token}")).context("Token 格式非法")?;
        request.metadata_mut().insert("authorization", value);
    }
    Ok(())
}

/// 批量查询已存在的块（秒传）。
pub async fn query_existing(
    client: &mut UploadClient<Channel>,
    game_id: u64,
    hashes: &[[u8; 32]],
    token: &Option<String>,
) -> Result<Vec<[u8; 32]>> {
    let mut request = Request::new(ChunkQuery {
        game_id,
        chunk_hashes: hashes.iter().map(|h| h.to_vec()).collect(),
    });
    attach_token(&mut request, token)?;
    let reply = client.query_existing_chunks(request).await?.into_inner();
    Ok(reply
        .existing_hashes
        .into_iter()
        .map(|v| <[u8; 32]>::try_from(v.as_slice()).expect("服务端返回 32 字节哈希"))
        .collect())
}

/// 提交版本清单；未发布时返回错误。
pub async fn commit_version(
    client: &mut UploadClient<Channel>,
    game_id: u64,
    version: u64,
    manifest: Vec<u8>,
    manifest_hash: [u8; 32],
    token: &Option<String>,
) -> Result<()> {
    let mut request = Request::new(CommitRequest {
        game_id,
        version,
        manifest,
        manifest_hash: manifest_hash.to_vec(),
    });
    attach_token(&mut request, token)?;
    let reply = client.commit_version(request).await?.into_inner();
    if reply.published {
        Ok(())
    } else {
        Err(anyhow!("版本发布失败: {}", reply.error))
    }
}

/// 完整上传流程：查重 → 流式上传缺失块（边读暂存文件边发送，内存仅保留单个块）→ 汇总。
pub async fn upload_delta(
    client: &mut UploadClient<Channel>,
    game_id: u64,
    stage_dir: &Path,
    hashes: &[[u8; 32]],
    token: &Option<String>,
) -> Result<UploadSummary> {
    let existing = query_existing(client, game_id, hashes, token).await?;
    let missing: Vec<[u8; 32]> = hashes
        .iter()
        .filter(|h| !existing.contains(h))
        .copied()
        .collect();
    let mut summary = UploadSummary {
        skipped: existing.len(),
        ..Default::default()
    };
    if missing.is_empty() {
        return Ok(summary);
    }
    let stage = stage_dir.to_path_buf();
    for hash in &missing {
        let path = stage.join(format!("{}.blk", hex(hash)));
        if !path.exists() {
            bail!("暂存块缺失: {}", path.display());
        }
    }
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let _task = tokio::spawn(async move {
        for (seq, hash) in missing.iter().enumerate() {
            let path = stage.join(format!("{}.blk", hex(hash)));
            // 已预检存在；读取异常时该块缺失，提交阶段会因缺块失败兜底。
            if let Ok(data) = tokio::fs::read(&path).await {
                let _ = tx
                    .send(UploadChunk {
                        game_id,
                        chunk_hash: hash.to_vec(),
                        data,
                        seq: seq as u32,
                    })
                    .await;
            }
        }
    });
    let stream = ReceiverStream::new(rx);
    let mut request = Request::new(stream);
    attach_token(&mut request, token)?;
    let mut response = client.upload_chunks(request).await?.into_inner();
    while let Some(ack) = response.next().await {
        let ack = ack?;
        if ack.ok {
            summary.uploaded += 1;
        } else {
            let hash: [u8; 32] = ack
                .chunk_hash
                .as_slice()
                .try_into()
                .expect("服务端返回 32 字节哈希");
            summary.failed.push(hash);
        }
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use origin::server::{UploadService, serve};
    use std::fs;
    use std::net::SocketAddr;
    use std::thread;
    use std::time::Duration;

    fn spawn_origin(data_dir: &std::path::Path) -> (String, tokio::sync::oneshot::Sender<()>) {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);
        let url = format!("http://{addr}");
        let dir = data_dir.to_path_buf();
        let (tx, rx) = tokio::sync::oneshot::channel();
        thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let _handle = serve(addr, UploadService::new(dir)).await.unwrap();
                let _ = rx.await;
            });
        });
        (url, tx)
    }

    async fn connect(url: &str) -> Result<UploadClient<Channel>> {
        for _ in 0..50 {
            if let Ok(client) = UploadClient::connect(url.to_string()).await {
                return Ok(client);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::bail!("连接上传服务失败: {url}");
    }

    fn hash_of(data: &[u8]) -> [u8; 32] {
        blake3::hash(data).into()
    }

    #[tokio::test]
    async fn test_upload_delta_full_flow() {
        let dir = std::env::temp_dir().join("blaze-producer-upload");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (url, shutdown) = spawn_origin(&dir);
        let mut client = connect(&url).await.unwrap();

        let d1 = b"hello".to_vec();
        let d2 = b"world".to_vec();
        let h1 = hash_of(&d1);
        let h2 = hash_of(&d2);
        let token = Some("test-token".to_string());
        let stage = dir.join("stage");
        fs::create_dir_all(&stage).unwrap();
        fs::write(stage.join(format!("{}.blk", hex(&h1))), &d1).unwrap();
        fs::write(stage.join(format!("{}.blk", hex(&h2))), &d2).unwrap();

        // 第一次：两个块都上传
        let summary = upload_delta(&mut client, 1, &stage, &[h1, h2], &token)
            .await
            .unwrap();
        assert_eq!(summary.uploaded, 2);
        assert_eq!(summary.skipped, 0);
        assert!(summary.failed.is_empty());

        // 第二次：两个块都秒传跳过
        let summary = upload_delta(&mut client, 1, &stage, &[h1, h2], &token)
            .await
            .unwrap();
        assert_eq!(summary.uploaded, 0);
        assert_eq!(summary.skipped, 2);

        // 提交版本
        let index =
            blaze_common::manifest::GameIndex::build(vec![blaze_common::manifest::FileEntry {
                name: "a.bin".to_string(),
                file_hash: [1; 32],
                chunks: vec![blaze_common::manifest::ChunkMeta {
                    hash: h1,
                    len: d1.len() as u32,
                }],
            }]);
        let manifest = index.encode().unwrap();
        commit_version(&mut client, 1, 1, manifest, index.manifest_hash, &token)
            .await
            .unwrap();
        assert!(dir.join("1/published/1.gameindex").exists());
        let _ = shutdown.send(());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_upload_failed_ack_and_commit_rejected() {
        let dir = std::env::temp_dir().join("blaze-producer-fail");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (url, shutdown) = spawn_origin(&dir);
        let mut client = connect(&url).await.unwrap();
        let token: Option<String> = None;

        // 哈希与数据不匹配 → 服务端 ack ok=false
        let wrong = [7u8; 32];
        let stage = dir.join("stage");
        fs::create_dir_all(&stage).unwrap();
        fs::write(stage.join(format!("{}.blk", hex(&wrong))), b"data").unwrap();
        let summary = upload_delta(&mut client, 1, &stage, &[wrong], &token)
            .await
            .unwrap();
        assert_eq!(summary.failed, vec![wrong]);
        assert_eq!(summary.uploaded, 0);

        // 引用未上传块 → 提交被拒
        let index =
            blaze_common::manifest::GameIndex::build(vec![blaze_common::manifest::FileEntry {
                name: "a.bin".to_string(),
                file_hash: [2; 32],
                chunks: vec![blaze_common::manifest::ChunkMeta {
                    hash: wrong,
                    len: 4,
                }],
            }]);
        let manifest = index.encode().unwrap();
        let err = commit_version(&mut client, 1, 1, manifest, index.manifest_hash, &token)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("版本发布失败"));
        let _ = shutdown.send(());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_attach_token_invalid() {
        let dir = std::env::temp_dir().join("blaze-producer-token");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (url, shutdown) = spawn_origin(&dir);
        let mut client = connect(&url).await.unwrap();
        // 非法 Token 值（含换行）应报错
        let token = Some("bad\nvalue".to_string());
        let err = query_existing(&mut client, 1, &[], &token)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Token"));
        let _ = shutdown.send(());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_upload_missing_staged_chunk() {
        let dir = std::env::temp_dir().join("blaze-producer-missing");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (url, shutdown) = spawn_origin(&dir);
        let mut client = connect(&url).await.unwrap();
        let stage = dir.join("stage");
        fs::create_dir_all(&stage).unwrap();
        let missing = [9u8; 32];
        let err = upload_delta(&mut client, 1, &stage, &[missing], &None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("暂存块缺失"));
        let _ = shutdown.send(());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_upload_staged_chunk_read_error() {
        let dir = std::env::temp_dir().join("blaze-producer-readerr");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (url, shutdown) = spawn_origin(&dir);
        let mut client = connect(&url).await.unwrap();
        let stage = dir.join("stage");
        fs::create_dir_all(&stage).unwrap();
        // 预检通过但读取失败：同名目录会让 exists() 为真而 fs::read 报错。
        let bad = [10u8; 32];
        fs::create_dir(stage.join(format!("{}.blk", hex(&bad)))).unwrap();
        let summary = upload_delta(&mut client, 1, &stage, &[bad], &None)
            .await
            .unwrap();
        assert_eq!(summary.uploaded, 0);
        assert_eq!(summary.skipped, 0);
        let _ = shutdown.send(());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_connect_failure() {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let err = connect(&format!("http://{addr}")).await.unwrap_err();
        assert!(err.to_string().contains("连接上传服务失败"));
    }
}
