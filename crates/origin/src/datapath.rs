//! 数据面块服务：iroh 自定义 ALPN，批量流式传块；relay-only 路径拒绝传块。
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use blaze_common::manifest::HASH_LEN;
use iroh::endpoint::QuicTransportConfig;
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMap, RelayMode, endpoint::Connection};
use iroh_relay::tls::CaTlsConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::storage::GameStore;

pub const ALPN: &[u8] = b"blazenet/1";

/// 路径门控：只有直连路径允许传块（relay 只做打洞）。
pub fn allow_data(has_direct: bool, _has_relay: bool) -> bool {
    has_direct
}

/// relay-only 连接：拒绝块传输（可独立测试）。
pub fn reject_relay_only(has_direct: bool, has_relay: bool) -> Result<()> {
    if !allow_data(has_direct, has_relay) {
        println!("relay-only 路径，拒绝块传输");
        return Ok(());
    }
    Ok(())
}

/// 路径门控：直连路径未就绪时拒绝块传输（供连接处理与单元测试共用）。
pub fn gate_or_reject(ready: bool) -> Result<()> {
    if !ready {
        reject_relay_only(false, true)?;
    }
    Ok(())
}

/// 等待直连路径出现；`check` 返回（是否有直连，是否还有 relay 路径）。
/// 直连出现返回 true；连接失去 relay 路径或超时返回 false。
async fn wait_for_direct_path<F>(mut check: F, timeout: std::time::Duration) -> bool
where
    F: FnMut() -> (bool, bool),
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let (has_direct, has_relay) = check();
        if has_direct {
            return true;
        }
        if !has_relay || std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

async fn handle_conn(
    conn: Connection,
    stores: Arc<Mutex<HashMap<u64, GameStore>>>,
    data_dir: PathBuf,
) -> Result<()> {
    // NAT 打洞的直连路径可能延迟数秒建立，等待直连后再放行数据。
    let ready = wait_for_direct_path(
        || {
            let paths = conn.paths();
            (
                paths.iter().any(|p| p.is_ip()),
                paths.iter().any(|p| p.is_relay()),
            )
        },
        std::time::Duration::from_secs(30),
    )
    .await;
    gate_or_reject(ready)?;
    let (mut send, mut recv) = conn.accept_bi().await.context("accept_bi 失败")?;
    let game_id = recv.read_u64().await.context("读 game_id 失败")?;
    let count = recv.read_u32().await.context("读块数失败")?;
    // 与上传服务共享块库句柄，避免同一 redb 被重复打开。
    let mut stores = stores.lock().await;
    let store = match stores.entry(game_id) {
        std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
        std::collections::hash_map::Entry::Vacant(e) => {
            e.insert(GameStore::open(&data_dir, game_id)?)
        }
    };
    for _ in 0..count {
        let mut hash = [0u8; HASH_LEN];
        recv.read_exact(&mut hash).await.context("读块哈希失败")?;
        let Some(data) = store.read_chunk(&hash)? else {
            return Err(anyhow!("块不存在"));
        };
        send.write_u32(data.len() as u32).await?;
        send.write_all(&data).await?;
    }
    send.finish().context("finish 失败")?;
    // 长连接语义：发送完成后短暂保活，避免对端未读完即断开
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    Ok(())
}

async fn accept_loop(
    endpoint: Endpoint,
    stores: Arc<Mutex<HashMap<u64, GameStore>>>,
    data_dir: PathBuf,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            incoming = endpoint.accept() => {
                let _ = incoming.map(|incoming| {
                    let data_dir = data_dir.clone();
                    let stores = stores.clone();
                    tokio::spawn(async move {
                        if let Ok(conn) = incoming.await
                            && let Err(err) = handle_conn(conn, stores, data_dir).await
                        {
                            println!("连接处理失败: {err:#}");
                        }
                    })
                });
            }
        }
    }
}

/// 数据面服务句柄。
pub struct DataPathHandle {
    port: u16,
    endpoint_id: iroh::EndpointId,
    shutdown: oneshot::Sender<()>,
    _endpoint: Endpoint,
    _task: JoinHandle<()>,
}

impl DataPathHandle {
    /// 服务监听端口。
    pub fn port(&self) -> u16 {
        self.port
    }

    /// 服务端 EndpointId。
    pub fn endpoint_id(&self) -> iroh::EndpointId {
        self.endpoint_id
    }

    /// 触发停止。
    pub fn shutdown(self) {
        let _ = self.shutdown.send(());
    }
}

/// 启动数据面块服务。
pub async fn serve(
    stores: Arc<Mutex<HashMap<u64, GameStore>>>,
    data_dir: PathBuf,
    listen_port: u16,
    relay_url: Option<String>,
    external_addr: Option<std::net::SocketAddr>,
) -> Result<DataPathHandle> {
    let mut builder = Endpoint::builder(presets::Minimal)
        .alpns(vec![ALPN.to_vec()])
        .clear_address_lookup()
        .clear_ip_transports()
        .transport_config(
            QuicTransportConfig::builder()
                .send_observed_address_reports(true)
                .receive_observed_address_reports(true)
                .build(),
        )
        .bind_addr(format!("0.0.0.0:{listen_port}"))?;
    if let Some(addr) = external_addr {
        builder = builder.external_addr(addr);
    }
    if let Some(url) = relay_url {
        let relay_url: iroh::RelayUrl = url.parse()?;
        builder = builder
            .relay_mode(RelayMode::Custom(RelayMap::from_iter([relay_url])))
            .ca_tls_config(CaTlsConfig::insecure_skip_verify());
    } else {
        builder = builder.relay_mode(RelayMode::Disabled);
    }
    let endpoint = builder.bind().await.context("绑定 iroh 端点失败")?;
    let port = endpoint
        .bound_sockets()
        .first()
        .map(|s| s.port())
        .unwrap_or(listen_port);
    let (tx, rx) = oneshot::channel();
    let endpoint_id = endpoint.id();
    let task = tokio::spawn(accept_loop(endpoint.clone(), stores, data_dir, rx));
    // 等待 accept 循环就绪，避免测试/调用方立即连接失败
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    Ok(DataPathHandle {
        port,
        endpoint_id,
        shutdown: tx,
        _endpoint: endpoint,
        _task: task,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{Endpoint, EndpointAddr, TransportAddr};
    use std::fs;
    use std::time::Duration;

    async fn client() -> Endpoint {
        Endpoint::builder(presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .clear_address_lookup()
            .relay_mode(RelayMode::Disabled)
            .clear_ip_transports()
            .bind_addr("127.0.0.1:0")
            .unwrap()
            .bind()
            .await
            .unwrap()
    }

    fn hash_of(data: &[u8]) -> [u8; 32] {
        blake3::hash(data).into()
    }

    async fn fetch(
        handle: &DataPathHandle,
        game_id: u64,
        hashes: &[[u8; 32]],
    ) -> Result<Vec<Vec<u8>>> {
        let ep = client().await;
        let server_addr = EndpointAddr::from_parts(
            handle.endpoint_id(),
            [TransportAddr::Ip(
                format!("127.0.0.1:{}", handle.port()).parse()?,
            )],
        );
        let conn = ep.connect(server_addr, ALPN).await.context("连接失败")?;
        let (mut send, mut recv) = conn.open_bi().await.context("open_bi 失败")?;
        send.write_u64(game_id).await?;
        send.write_u32(hashes.len() as u32).await?;
        for hash in hashes {
            send.write_all(hash).await?;
        }
        send.finish().context("finish 失败")?;
        let mut out = Vec::new();
        for _ in 0..hashes.len() {
            let len = recv.read_u32().await.context("读块长度失败")?;
            let mut data = vec![0u8; len as usize];
            recv.read_exact(&mut data).await.context("读块数据失败")?;
            out.push(data);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn test_direct_batch_and_missing_chunk() {
        let dir = std::env::temp_dir().join("blaze-agent-dp");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let mut store = GameStore::open(&dir, 3).unwrap();
        let d1 = b"hello".to_vec();
        let d2 = b"world".to_vec();
        let h1 = hash_of(&d1);
        let h2 = hash_of(&d2);
        store.append_chunk(&h1, &d1).unwrap();
        store.append_chunk(&h2, &d2).unwrap();
        let stores = Arc::new(Mutex::new(HashMap::new()));
        stores.lock().await.insert(3, store);

        let handle = serve(stores, dir.clone(), 0, None, None).await.unwrap();
        let out = fetch(&handle, 3, &[h1, h2]).await.unwrap();
        assert_eq!(out, vec![d1, d2]);
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(fetch(&handle, 3, &[[9u8; 32]]).await.is_err());
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_serve_with_relay_url_and_broken_data_dir() {
        let dir = std::env::temp_dir().join("blaze-agent-dp-relay");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let stores = Arc::new(Mutex::new(HashMap::new()));
        let handle = serve(
            stores,
            dir.clone(),
            0,
            Some("https://127.0.0.1:1".to_string()),
            Some("127.0.0.1:42001".parse().unwrap()),
        )
        .await
        .unwrap();
        assert!(fetch(&handle, 1, &[]).await.is_ok());
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let data_file = dir.join("data-file");
        fs::write(&data_file, b"x").unwrap();
        let stores2 = Arc::new(Mutex::new(HashMap::new()));
        let handle2 = serve(stores2, data_file, 0, None, None).await.unwrap();
        assert!(fetch(&handle2, 1, &[[1u8; 32]]).await.is_err());
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle2.shutdown();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_allow_data() {
        assert!(allow_data(true, false));
        assert!(allow_data(true, true));
        assert!(!allow_data(false, true));
        assert!(!allow_data(false, false));
        assert!(reject_relay_only(false, true).is_ok());
        assert!(reject_relay_only(true, false).is_ok());
        assert!(gate_or_reject(true).is_ok());
        assert!(gate_or_reject(false).is_ok());
    }

    #[tokio::test]
    async fn test_wait_for_direct_path_immediate() {
        assert!(wait_for_direct_path(|| (true, false), Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn test_wait_for_direct_path_delayed() {
        let mut calls = 0;
        let ready = wait_for_direct_path(
            || {
                calls += 1;
                (calls >= 2, true)
            },
            Duration::from_secs(5),
        )
        .await;
        assert!(ready);
        assert_eq!(calls, 2);
    }

    #[tokio::test]
    async fn test_wait_for_direct_path_timeout() {
        assert!(!wait_for_direct_path(|| (false, true), Duration::from_millis(50)).await);
    }

    #[tokio::test]
    async fn test_wait_for_direct_path_relay_lost() {
        assert!(!wait_for_direct_path(|| (false, false), Duration::from_secs(1)).await);
    }
}
