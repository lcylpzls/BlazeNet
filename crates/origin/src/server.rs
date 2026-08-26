//! 原始节点上传服务：秒传查重、块上传（双向流）、版本提交。
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use blaze_common::manifest::GameIndex;
use blaze_proto::upload::upload_server::Upload;
use blaze_proto::upload::{
    ChunkExists, ChunkQuery, CommitReply, CommitRequest, UploadAck, UploadChunk,
};
use tokio::sync::Mutex;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

use crate::storage::GameStore;

/// 上传服务：每个游戏一个块库句柄，按游戏串行写入。
pub struct UploadService {
    data_dir: PathBuf,
    stores: Arc<Mutex<HashMap<u64, GameStore>>>,
}

impl UploadService {
    pub fn new(data_dir: PathBuf) -> Self {
        Self::with_stores(data_dir, Arc::new(Mutex::new(HashMap::new())))
    }

    /// 与数据面共享块库句柄，避免同一 redb 被重复打开。
    pub fn with_stores(data_dir: PathBuf, stores: Arc<Mutex<HashMap<u64, GameStore>>>) -> Self {
        Self { data_dir, stores }
    }
}

fn open_store_error(err: anyhow::Error) -> Status {
    Status::internal(format!("打开块库失败: {err}"))
}

fn query_chunk_error(err: anyhow::Error) -> Status {
    Status::internal(format!("查询块失败: {err}"))
}

fn create_publish_dir_error(err: std::io::Error) -> Status {
    Status::internal(format!("创建发布目录失败: {err}"))
}

fn write_manifest_error(err: std::io::Error) -> Status {
    Status::internal(format!("写入版本清单失败: {err}"))
}

fn bad_hash_len(_: std::array::TryFromSliceError) -> Status {
    Status::invalid_argument("块哈希必须为 32 字节")
}

fn bad_manifest_hash_len(_: std::array::TryFromSliceError) -> Status {
    Status::invalid_argument("manifest_hash 必须为 32 字节")
}

fn decode_manifest_error(err: anyhow::Error) -> Status {
    Status::invalid_argument(format!("版本清单解析失败: {err}"))
}

fn append_ack(result: Result<(u64, u32), anyhow::Error>, hash: [u8; 32]) -> UploadAck {
    match result {
        Ok(_) => UploadAck {
            chunk_hash: hash.to_vec(),
            ok: true,
            error: String::new(),
        },
        Err(err) => UploadAck {
            chunk_hash: hash.to_vec(),
            ok: false,
            error: err.to_string(),
        },
    }
}

/// 处理一次块上传流：读取首包确定游戏，逐块校验入库并回 ack。
async fn process_upload<S>(
    mut stream: S,
    stores: Arc<Mutex<HashMap<u64, GameStore>>>,
    data_dir: PathBuf,
    tx: tokio::sync::mpsc::Sender<Result<UploadAck, Status>>,
) -> Result<(), Status>
where
    S: Stream<Item = Result<UploadChunk, Status>> + Unpin,
{
    let Some(first) = stream.next().await.transpose()? else {
        return Ok(());
    };
    if first.game_id == 0 {
        let _ = tx
            .send(Err(Status::invalid_argument("game_id 必须大于 0")))
            .await;
        return Ok(());
    }
    let mut stores = stores.lock().await;
    let store = match stores.entry(first.game_id) {
        std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
        std::collections::hash_map::Entry::Vacant(e) => {
            e.insert(GameStore::open(&data_dir, first.game_id).map_err(open_store_error)?)
        }
    };
    let mut chunks = tokio_stream::once(Ok::<UploadChunk, Status>(first)).chain(stream);
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        let hash: [u8; 32] = chunk
            .chunk_hash
            .as_slice()
            .try_into()
            .map_err(bad_hash_len)?;
        let actual: [u8; 32] = blake3::hash(&chunk.data).into();
        let ack = if actual != hash {
            UploadAck {
                chunk_hash: hash.to_vec(),
                ok: false,
                error: "块哈希校验失败".to_string(),
            }
        } else {
            append_ack(store.append_chunk(&hash, &chunk.data), hash)
        };
        let _ = tx.send(Ok(ack)).await;
    }
    Ok(())
}

#[tonic::async_trait]
impl Upload for UploadService {
    type UploadChunksStream = ReceiverStream<Result<UploadAck, Status>>;

    async fn query_existing_chunks(
        &self,
        request: Request<ChunkQuery>,
    ) -> Result<Response<ChunkExists>, Status> {
        let query = request.into_inner();
        if query.game_id == 0 {
            return Err(Status::invalid_argument("game_id 必须大于 0"));
        }
        let mut stores = self.stores.lock().await;
        let store = match stores.entry(query.game_id) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(GameStore::open(&self.data_dir, query.game_id).map_err(open_store_error)?)
            }
        };
        let mut existing = Vec::new();
        for hash in query.chunk_hashes {
            let hash: [u8; 32] = hash.as_slice().try_into().map_err(bad_hash_len)?;
            if store.contains(&hash).map_err(query_chunk_error)? {
                existing.push(hash.to_vec());
            }
        }
        Ok(Response::new(ChunkExists {
            existing_hashes: existing,
        }))
    }

    async fn upload_chunks(
        &self,
        request: Request<Streaming<UploadChunk>>,
    ) -> Result<Response<Self::UploadChunksStream>, Status> {
        let stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let stores = self.stores.clone();
        let data_dir = self.data_dir.clone();
        tokio::spawn(async move {
            let _ = process_upload(stream, stores, data_dir, tx).await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn commit_version(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitReply>, Status> {
        let commit = request.into_inner();
        if commit.game_id == 0 || commit.version == 0 {
            return Err(Status::invalid_argument("game_id 与 version 必须大于 0"));
        }
        if commit.manifest.len() < 32 {
            return Err(Status::invalid_argument("版本清单长度不足"));
        }
        let body = &commit.manifest[..commit.manifest.len() - 32];
        let actual_hash: [u8; 32] = blake3::hash(body).into();
        let expected_hash: [u8; 32] = commit
            .manifest_hash
            .as_slice()
            .try_into()
            .map_err(bad_manifest_hash_len)?;
        if actual_hash != expected_hash {
            return Err(Status::invalid_argument("manifest_hash 校验失败"));
        }
        let index = GameIndex::decode(&commit.manifest).map_err(decode_manifest_error)?;

        let mut stores = self.stores.lock().await;
        let store = match stores.entry(commit.game_id) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(GameStore::open(&self.data_dir, commit.game_id).map_err(open_store_error)?)
            }
        };
        for hash in index.chunk_set() {
            if !store.contains(&hash).map_err(query_chunk_error)? {
                return Ok(Response::new(CommitReply {
                    published: false,
                    error: "版本清单引用的块未齐全".to_string(),
                }));
            }
        }
        let published_dir = self
            .data_dir
            .join(commit.game_id.to_string())
            .join("published");
        std::fs::create_dir_all(&published_dir).map_err(create_publish_dir_error)?;
        std::fs::write(
            published_dir.join(format!("{}.gameindex", commit.version)),
            &commit.manifest,
        )
        .map_err(write_manifest_error)?;
        Ok(Response::new(CommitReply {
            published: true,
            error: String::new(),
        }))
    }
}

/// 上传服务句柄：drop 时触发关闭。
#[derive(Debug)]
pub struct ServerHandle {
    #[allow(dead_code)]
    // 字段本身不被读取：drop 时 channel 关闭即触发服务关闭
    shutdown: tokio::sync::oneshot::Sender<()>,
}

/// 启动上传服务；返回句柄，drop 句柄即停止服务。
pub async fn serve(addr: std::net::SocketAddr, service: UploadService) -> Result<ServerHandle> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let incoming = tonic::transport::server::TcpIncoming::bind(addr)?;
    tokio::spawn(async move {
        let result = tonic::transport::Server::builder()
            .add_service(blaze_proto::upload::upload_server::UploadServer::new(
                service,
            ))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = rx.await;
            })
            .await;
        // 服务退出时无需额外处理；错误由 tonic 记录
        let _ = result;
    });
    Ok(ServerHandle { shutdown: tx })
}

#[cfg(test)]
mod tests {
    use super::*;
    use blaze_proto::upload::upload_client::UploadClient;
    use blaze_proto::upload::{ChunkQuery, CommitRequest, UploadChunk};
    use std::fs;
    use tokio_stream::StreamExt;

    fn hash_of(data: &[u8]) -> [u8; 32] {
        blake3::hash(data).into()
    }

    async fn connect_retry(url: &str) -> anyhow::Result<UploadClient<tonic::transport::Channel>> {
        for _ in 0..50 {
            if let Ok(client) = UploadClient::connect(url.to_string()).await {
                return Ok(client);
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        anyhow::bail!("连接上传服务失败: {url}");
    }

    async fn setup(
        dir: &std::path::Path,
    ) -> (UploadClient<tonic::transport::Channel>, super::ServerHandle) {
        let service = UploadService::new(dir.to_path_buf());
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let handle = serve(addr, service).await.unwrap();
        (
            connect_retry(&format!("http://{addr}")).await.unwrap(),
            handle,
        )
    }

    #[test]
    fn test_append_ack_ok() {
        let ack = append_ack(Ok((0, 5)), [1; 32]);
        assert!(ack.ok);
        assert!(ack.error.is_empty());
    }

    #[test]
    fn test_append_ack_err() {
        let ack = append_ack(Err(anyhow::anyhow!("磁盘写入失败")), [2; 32]);
        assert!(!ack.ok);
        assert!(ack.error.contains("磁盘写入失败"));
    }

    #[test]
    fn test_error_helpers() {
        assert!(
            open_store_error(anyhow::anyhow!("x"))
                .message()
                .contains("打开块库失败")
        );
        assert!(
            query_chunk_error(anyhow::anyhow!("x"))
                .message()
                .contains("查询块失败")
        );
        assert!(
            create_publish_dir_error(std::io::Error::other("x"))
                .message()
                .contains("创建发布目录失败")
        );
        assert!(
            write_manifest_error(std::io::Error::other("x"))
                .message()
                .contains("写入版本清单失败")
        );
        let slice_err = <[u8; 32]>::try_from(&[1u8][..]).unwrap_err();
        assert!(bad_hash_len(slice_err).message().contains("32 字节"));
        let slice_err2 = <[u8; 32]>::try_from(&[1u8][..]).unwrap_err();
        assert!(
            bad_manifest_hash_len(slice_err2)
                .message()
                .contains("32 字节")
        );
        assert!(
            decode_manifest_error(anyhow::anyhow!("x"))
                .message()
                .contains("版本清单解析失败")
        );
    }

    #[tokio::test]
    async fn test_query_invalid_hash_len() {
        let dir = std::env::temp_dir().join("blaze-upload-hashlen");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle) = setup(&dir).await;
        let err = client
            .query_existing_chunks(ChunkQuery {
                game_id: 1,
                chunk_hashes: vec![vec![1, 2]],
            })
            .await
            .unwrap_err();
        assert!(err.message().contains("32 字节"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_broken_data_dir() {
        let dir = std::env::temp_dir().join("blaze-upload-broken");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let data_file = dir.join("data-file");
        fs::write(&data_file, b"x").unwrap();
        let service = UploadService::new(data_file);
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let _handle = serve(addr, service).await.unwrap();
        let mut client = connect_retry(&format!("http://{addr}")).await.unwrap();
        let err = client
            .query_existing_chunks(ChunkQuery {
                game_id: 1,
                chunk_hashes: vec![],
            })
            .await
            .unwrap_err();
        assert!(err.message().contains("打开块库失败"));
        let empty_index = GameIndex::build(vec![]);
        let manifest = empty_index.encode().unwrap();
        let err = client
            .commit_version(CommitRequest {
                game_id: 1,
                version: 1,
                manifest,
                manifest_hash: empty_index.manifest_hash.to_vec(),
            })
            .await
            .unwrap_err();
        assert!(err.message().contains("打开块库失败"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_upload_zero_game_id() {
        let dir = std::env::temp_dir().join("blaze-upload-zero");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle) = setup(&dir).await;
        let stream = tokio_stream::iter(vec![UploadChunk {
            game_id: 0,
            chunk_hash: vec![0u8; 32],
            data: vec![],
            seq: 1,
        }]);
        let resp = client
            .upload_chunks(Request::new(stream))
            .await
            .unwrap()
            .into_inner();
        let items: Vec<Result<UploadAck, tonic::Status>> = resp.collect().await;
        assert!(items[0].is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_upload_short_hash_empty_acks() {
        let dir = std::env::temp_dir().join("blaze-upload-shorthash");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle) = setup(&dir).await;
        let stream = tokio_stream::iter(vec![UploadChunk {
            game_id: 1,
            chunk_hash: vec![1, 2],
            data: vec![1],
            seq: 1,
        }]);
        let resp = client
            .upload_chunks(Request::new(stream))
            .await
            .unwrap()
            .into_inner();
        let acks: Vec<Result<UploadAck, tonic::Status>> = resp.collect().await;
        assert!(acks.is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_serve_bind_failure() {
        let dir = std::env::temp_dir().join("blaze-upload-bind");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let service = UploadService::new(dir.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let err = serve(addr, service).await.unwrap_err();
        assert!(err.to_string().contains("in use"));
        drop(listener);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_process_upload_first_message_error() {
        let stores = Arc::new(Mutex::new(HashMap::new()));
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let stream = tokio_stream::iter(vec![Err::<UploadChunk, tonic::Status>(
            tonic::Status::internal("传输错误"),
        )]);
        let err = process_upload(stream, stores, PathBuf::from("/tmp"), tx)
            .await
            .unwrap_err();
        assert!(err.message().contains("传输错误"));
    }

    #[tokio::test]
    async fn test_process_upload_mid_stream_error() {
        let dir = std::env::temp_dir().join("blaze-upload-midstream");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let stores = Arc::new(Mutex::new(HashMap::new()));
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let stream = tokio_stream::iter(vec![
            Ok(UploadChunk {
                game_id: 1,
                chunk_hash: vec![0u8; 32],
                data: vec![],
                seq: 2,
            }),
            Err::<UploadChunk, tonic::Status>(tonic::Status::internal("中断")),
        ]);
        let err = process_upload(stream, stores, dir.clone(), tx)
            .await
            .unwrap_err();
        assert!(err.message().contains("中断"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_process_upload_empty_stream() {
        let stores = Arc::new(Mutex::new(HashMap::new()));
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let stream = tokio_stream::iter(Vec::<Result<UploadChunk, tonic::Status>>::new());
        process_upload(stream, stores, PathBuf::from("/tmp"), tx)
            .await
            .unwrap();
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_upload_open_failure_empty_acks() {
        let dir = std::env::temp_dir().join("blaze-upload-openfail");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let data_file = dir.join("data-file");
        fs::write(&data_file, b"x").unwrap();
        let service = UploadService::new(data_file);
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let _handle = serve(addr, service).await.unwrap();
        let mut client = connect_retry(&format!("http://{addr}")).await.unwrap();
        let stream = tokio_stream::iter(vec![UploadChunk {
            game_id: 1,
            chunk_hash: vec![0u8; 32],
            data: vec![1],
            seq: 1,
        }]);
        let resp = client
            .upload_chunks(Request::new(stream))
            .await
            .unwrap()
            .into_inner();
        let acks: Vec<Result<UploadAck, tonic::Status>> = resp.collect().await;
        assert!(acks.is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_commit_bad_manifest_hash_len() {
        let dir = std::env::temp_dir().join("blaze-upload-mhlen");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle) = setup(&dir).await;
        let err = client
            .commit_version(CommitRequest {
                game_id: 1,
                version: 1,
                manifest: vec![0u8; 40],
                manifest_hash: vec![0u8; 16],
            })
            .await
            .unwrap_err();
        assert!(err.message().contains("32 字节"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_commit_decode_failure() {
        let dir = std::env::temp_dir().join("blaze-upload-decode");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle) = setup(&dir).await;
        let body = vec![0xffu8; 8];
        let mut manifest = body.clone();
        let hash: [u8; 32] = blake3::hash(&body).into();
        manifest.extend_from_slice(&hash);
        let err = client
            .commit_version(CommitRequest {
                game_id: 1,
                version: 1,
                manifest,
                manifest_hash: hash.to_vec(),
            })
            .await
            .unwrap_err();
        assert!(err.message().contains("版本清单解析失败"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_commit_publish_dir_conflict() {
        let dir = std::env::temp_dir().join("blaze-upload-pubdir");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("1")).unwrap();
        fs::write(dir.join("1/published"), b"x").unwrap();
        let (mut client, _handle) = setup(&dir).await;
        let empty_index = GameIndex::build(vec![]);
        let manifest = empty_index.encode().unwrap();
        let err = client
            .commit_version(CommitRequest {
                game_id: 1,
                version: 1,
                manifest,
                manifest_hash: empty_index.manifest_hash.to_vec(),
            })
            .await
            .unwrap_err();
        assert!(err.message().contains("创建发布目录失败"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_commit_write_conflict() {
        let dir = std::env::temp_dir().join("blaze-upload-write");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("1/published/1.gameindex")).unwrap();
        let (mut client, _handle) = setup(&dir).await;
        let empty_index = GameIndex::build(vec![]);
        let manifest = empty_index.encode().unwrap();
        let err = client
            .commit_version(CommitRequest {
                game_id: 1,
                version: 1,
                manifest,
                manifest_hash: empty_index.manifest_hash.to_vec(),
            })
            .await
            .unwrap_err();
        assert!(err.message().contains("写入版本清单失败"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_query_and_upload_and_commit() {
        let dir = std::env::temp_dir().join("blaze-upload-srv");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle) = setup(&dir).await;

        let d1 = b"hello".to_vec();
        let d2 = b"world".to_vec();
        let h1 = hash_of(&d1);
        let h2 = hash_of(&d2);

        let resp = client
            .query_existing_chunks(ChunkQuery {
                game_id: 1,
                chunk_hashes: vec![h1.to_vec(), h2.to_vec()],
            })
            .await
            .unwrap()
            .into_inner();
        assert!(resp.existing_hashes.is_empty());

        let stream = tokio_stream::iter(vec![
            UploadChunk {
                game_id: 1,
                chunk_hash: h1.to_vec(),
                data: d1.clone(),
                seq: 1,
            },
            UploadChunk {
                game_id: 1,
                chunk_hash: vec![0u8; 32],
                data: d2.clone(),
                seq: 2,
            },
        ]);
        let resp = client
            .upload_chunks(Request::new(stream))
            .await
            .unwrap()
            .into_inner();
        let acks: Vec<UploadAck> = resp.map(|a| a.unwrap()).collect().await;
        assert!(acks[0].ok);
        assert!(!acks[1].ok);
        assert!(acks[1].error.contains("哈希校验失败"));

        let resp = client
            .query_existing_chunks(ChunkQuery {
                game_id: 1,
                chunk_hashes: vec![h1.to_vec(), h2.to_vec()],
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.existing_hashes, vec![h1.to_vec()]);

        let missing_index = GameIndex::build(vec![blaze_common::manifest::FileEntry {
            name: "a.bin".to_string(),
            file_hash: [1; 32],
            chunks: vec![blaze_common::manifest::ChunkMeta {
                hash: h2,
                len: d2.len() as u32,
            }],
        }]);
        let missing_manifest = missing_index.encode().unwrap();
        let reply = client
            .commit_version(CommitRequest {
                game_id: 1,
                version: 1,
                manifest: missing_manifest.clone(),
                manifest_hash: missing_index.manifest_hash.to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(!reply.published);

        let ok_index = GameIndex::build(vec![blaze_common::manifest::FileEntry {
            name: "a.bin".to_string(),
            file_hash: [2; 32],
            chunks: vec![blaze_common::manifest::ChunkMeta {
                hash: h1,
                len: d1.len() as u32,
            }],
        }]);
        let ok_manifest = ok_index.encode().unwrap();
        let reply = client
            .commit_version(CommitRequest {
                game_id: 1,
                version: 1,
                manifest: ok_manifest.clone(),
                manifest_hash: ok_index.manifest_hash.to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(reply.published);
        assert!(dir.join("1/published/1.gameindex").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_commit_hash_mismatch() {
        let dir = std::env::temp_dir().join("blaze-upload-badhash");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle) = setup(&dir).await;
        let err = client
            .commit_version(CommitRequest {
                game_id: 1,
                version: 1,
                manifest: vec![0u8; 40],
                manifest_hash: vec![0u8; 32],
            })
            .await
            .unwrap_err();
        assert!(err.message().contains("manifest_hash"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_commit_short_manifest() {
        let dir = std::env::temp_dir().join("blaze-upload-short");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle) = setup(&dir).await;
        let err = client
            .commit_version(CommitRequest {
                game_id: 1,
                version: 1,
                manifest: vec![1, 2, 3],
                manifest_hash: vec![0u8; 32],
            })
            .await
            .unwrap_err();
        assert!(err.message().contains("长度不足"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_invalid_game_id() {
        let dir = std::env::temp_dir().join("blaze-upload-gid");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle) = setup(&dir).await;
        let err = client
            .query_existing_chunks(ChunkQuery {
                game_id: 0,
                chunk_hashes: vec![],
            })
            .await
            .unwrap_err();
        assert!(err.message().contains("game_id"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_empty_upload_stream() {
        let dir = std::env::temp_dir().join("blaze-upload-empty");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle) = setup(&dir).await;
        let resp = client
            .upload_chunks(Request::new(tokio_stream::iter(Vec::<UploadChunk>::new())))
            .await
            .unwrap()
            .into_inner();
        let acks: Vec<Result<UploadAck, tonic::Status>> = resp.collect().await;
        assert!(acks.is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_connect_retry_failure() {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let err = connect_retry(&format!("http://{addr}")).await.unwrap_err();
        assert!(err.to_string().contains("连接上传服务失败"));
    }

    #[tokio::test]
    async fn test_serve_shutdown() {
        let dir = std::env::temp_dir().join("blaze-upload-shutdown");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let service = UploadService::new(dir.clone());
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let handle = serve(addr, service).await.unwrap();
        let _client = connect_retry(&format!("http://{addr}")).await.unwrap();
        drop(handle);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = fs::remove_dir_all(&dir);
    }
}
