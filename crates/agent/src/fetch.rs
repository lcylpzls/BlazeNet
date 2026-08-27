//! 数据面拉取客户端：批量请求缺失块，逐块校验后交给回调保存。
use anyhow::{Context, Result};
use blaze_common::manifest::HASH_LEN;
use iroh::endpoint::QuicTransportConfig;
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, RelayMap, RelayMode, TransportAddr};
use iroh_relay::tls::CaTlsConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const ALPN: &[u8] = b"blazenet/1";

/// 候选源：优先直连，必要时经 relay。
#[derive(Debug, Clone)]
pub struct PeerTarget {
    pub endpoint_id: iroh::EndpointId,
    pub addr: Option<std::net::SocketAddr>,
    pub relay_url: Option<String>,
    pub direct_only: bool,
}

/// 拉取结果汇总。
#[derive(Debug, Default, PartialEq, Eq)]
pub struct FetchStats {
    pub downloaded: usize,
    pub bytes: u64,
    pub failed: Vec<[u8; HASH_LEN]>,
}

/// 校验块哈希并交给保存回调；哈希不一致返回 `false`。
fn store_chunk<F>(hash: &[u8; HASH_LEN], data: Vec<u8>, sink: &mut F) -> Result<bool>
where
    F: FnMut([u8; HASH_LEN], Vec<u8>) -> Result<()>,
{
    let actual: [u8; HASH_LEN] = blake3::hash(&data).into();
    if actual != *hash {
        return Ok(false);
    }
    sink(*hash, data)?;
    Ok(true)
}

/// 构建本地拉取端点；`relay_url` 缺省时仅直连。
pub async fn build_endpoint(relay_url: Option<&str>) -> Result<Endpoint> {
    let mut builder = Endpoint::builder(presets::Minimal)
        .alpns(vec![ALPN.to_vec()])
        .clear_address_lookup()
        .clear_ip_transports()
        // NAT 内网直连需要地址观测报告，否则对端无法确认公网路径（联调工具同配置）。
        .transport_config(
            QuicTransportConfig::builder()
                .send_observed_address_reports(true)
                .receive_observed_address_reports(true)
                .build(),
        )
        // 直连与 relay 均信任自签证书：项目 PoC 阶段使用自签证书，
        // 上线前接入 ACME/受信任 CA 后移除（见 docs/08-部署运维文档）。
        .ca_tls_config(CaTlsConfig::insecure_skip_verify());
    if let Some(url) = relay_url {
        let relay: iroh::RelayUrl = url.parse().context("relay 地址非法")?;
        builder = builder.relay_mode(RelayMode::Custom(RelayMap::from_iter([relay])));
    } else {
        builder = builder.relay_mode(RelayMode::Disabled);
    }
    Ok(builder.bind_addr("0.0.0.0:0")?.bind().await?)
}

async fn connect_peer(ep: &Endpoint, target: &PeerTarget) -> Result<iroh::endpoint::Connection> {
    let mut last_err: Option<anyhow::Error> = None;
    if let Some(addr) = target.addr {
        let direct = EndpointAddr::from_parts(target.endpoint_id, [TransportAddr::Ip(addr)]);
        match ep.connect(direct, ALPN).await {
            Ok(conn) => return Ok(conn),
            Err(err) => last_err = Some(err.into()),
        }
    }
    if !target.direct_only
        && let Some(url) = &target.relay_url
    {
        let relay: iroh::RelayUrl = url.parse().context("relay 地址非法")?;
        let via_relay = EndpointAddr::new(target.endpoint_id).with_relay_url(relay);
        match ep.connect(via_relay, ALPN).await {
            Ok(conn) => return Ok(conn),
            Err(err) => last_err = Some(err.into()),
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("peer 无可用连接地址")))
}

/// 批量拉取并逐块校验；`sink` 负责落盘，返回失败块列表。
pub async fn fetch_chunks<F>(
    ep: &Endpoint,
    target: &PeerTarget,
    game_id: u64,
    hashes: &[[u8; HASH_LEN]],
    mut sink: F,
) -> Result<FetchStats>
where
    F: FnMut([u8; HASH_LEN], Vec<u8>) -> Result<()>,
{
    if hashes.is_empty() {
        return Ok(FetchStats::default());
    }
    let conn = connect_peer(ep, target).await?;
    let (mut send, mut recv) = conn.open_bi().await.context("open_bi 失败")?;
    send.write_u64(game_id).await?;
    send.write_u32(hashes.len() as u32).await?;
    for hash in hashes {
        send.write_all(hash).await?;
    }
    send.finish().context("finish 失败")?;
    let mut stats = FetchStats::default();
    for hash in hashes {
        let len = match recv.read_u32().await {
            Ok(len) => len,
            Err(_) => {
                stats.failed.push(*hash);
                continue;
            }
        };
        let mut data = vec![0u8; len as usize];
        recv.read_exact(&mut data).await.context("读取块数据失败")?;
        if !store_chunk(hash, data, &mut sink)? {
            stats.failed.push(*hash);
            continue;
        }
        stats.downloaded += 1;
        stats.bytes += len as u64;
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use origin::datapath;
    use origin::storage::{GameStore, NodeStore};
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;
    use tokio::sync::Mutex;

    fn hash_of(data: &[u8]) -> [u8; HASH_LEN] {
        blake3::hash(data).into()
    }

    async fn seed_server(
        data_dir: &Path,
        game_id: u64,
        relay_url: Option<String>,
    ) -> (datapath::DataPathHandle, Vec<[u8; HASH_LEN]>) {
        let mut store = GameStore::open(data_dir, game_id).unwrap();
        let d1 = b"hello".to_vec();
        let d2 = b"world".to_vec();
        let h1 = hash_of(&d1);
        let h2 = hash_of(&d2);
        store.append_chunk(&h1, &d1).unwrap();
        store.append_chunk(&h2, &d2).unwrap();
        let pack_stores: Arc<Mutex<HashMap<u64, Arc<StdMutex<GameStore>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pack = Arc::new(StdMutex::new(store));
        pack_stores.lock().await.insert(game_id, pack.clone());
        let data_stores: Arc<Mutex<HashMap<u64, NodeStore>>> = Arc::new(Mutex::new(HashMap::new()));
        data_stores
            .lock()
            .await
            .insert(game_id, NodeStore::Pack(pack));
        let handle = datapath::serve(
            data_stores,
            pack_stores,
            data_dir.to_path_buf(),
            0,
            relay_url,
            None,
            true,
        )
        .await
        .unwrap();
        (handle, vec![h1, h2])
    }

    async fn empty_server(data_dir: &Path, game_id: u64) -> datapath::DataPathHandle {
        let store = GameStore::open(data_dir, game_id).unwrap();
        let pack = Arc::new(StdMutex::new(store));
        let pack_stores: Arc<Mutex<HashMap<u64, Arc<StdMutex<GameStore>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        pack_stores.lock().await.insert(game_id, pack.clone());
        let data_stores: Arc<Mutex<HashMap<u64, NodeStore>>> = Arc::new(Mutex::new(HashMap::new()));
        data_stores
            .lock()
            .await
            .insert(game_id, NodeStore::Pack(pack));
        datapath::serve(
            data_stores,
            pack_stores,
            data_dir.to_path_buf(),
            0,
            None,
            None,
            true,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_fetch_direct_and_empty() {
        let dir = std::env::temp_dir().join("blaze-fetch-direct");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (handle, hashes) = seed_server(&dir, 1, None).await;
        let ep = build_endpoint(None).await.unwrap();
        let target = PeerTarget {
            endpoint_id: handle.endpoint_id(),
            addr: Some(format!("127.0.0.1:{}", handle.port()).parse().unwrap()),
            relay_url: None,
            direct_only: true,
        };
        let mut got = Vec::new();
        let stats = fetch_chunks(&ep, &target, 1, &hashes, |hash, data| {
            got.push((hash, data));
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(stats.downloaded, 2);
        assert!(stats.failed.is_empty());
        assert_eq!(got.len(), 2);
        let empty = fetch_chunks(&ep, &target, 1, &[], |_, _| Ok(()))
            .await
            .unwrap();
        assert_eq!(empty.downloaded, 0);
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_store_chunk_matches_and_rejects() {
        let hash = hash_of(b"hello");
        let stored = std::cell::RefCell::new(Vec::new());
        let mut sink = |h: [u8; HASH_LEN], d: Vec<u8>| {
            stored.borrow_mut().push((h, d));
            Ok(())
        };
        assert!(store_chunk(&hash, b"hello".to_vec(), &mut sink).unwrap());
        assert_eq!(stored.borrow().len(), 1);
        assert!(!store_chunk(&hash, b"WRONG".to_vec(), &mut sink).unwrap());
        assert_eq!(stored.borrow().len(), 1);
    }

    #[tokio::test]
    async fn test_fetch_connect_failures() {
        let dir = std::env::temp_dir().join("blaze-fetch-connfail");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (handle, hashes) = seed_server(&dir, 1, None).await;
        let ep = build_endpoint(None).await.unwrap();
        let target = PeerTarget {
            endpoint_id: handle.endpoint_id(),
            addr: Some("127.0.0.1:1".parse().unwrap()),
            relay_url: None,
            direct_only: true,
        };
        assert!(
            fetch_chunks(&ep, &target, 1, &hashes, |_, _| Ok(()))
                .await
                .is_err()
        );

        let ep2 = build_endpoint(Some("https://127.0.0.1:1")).await.unwrap();
        let target2 = PeerTarget {
            endpoint_id: handle.endpoint_id(),
            addr: Some("127.0.0.1:1".parse().unwrap()),
            relay_url: Some("https://127.0.0.1:1".to_string()),
            direct_only: false,
        };
        assert!(
            fetch_chunks(&ep2, &target2, 1, &hashes, |_, _| Ok(()))
                .await
                .is_err()
        );
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_fetch_via_relay() {
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();
        let _ = relay_map;
        let url = relay_url.to_string();
        let dir = std::env::temp_dir().join("blaze-fetch-relay");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (handle, hashes) = seed_server(&dir, 1, Some(url.clone())).await;
        let ep = build_endpoint(Some(&url)).await.unwrap();
        let target = PeerTarget {
            endpoint_id: handle.endpoint_id(),
            addr: None,
            relay_url: Some(url),
            direct_only: false,
        };
        let stats = fetch_chunks(&ep, &target, 1, &hashes, |_, _| Ok(()))
            .await
            .unwrap();
        assert_eq!(stats.downloaded, 2);
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_fetch_no_usable_addr() {
        let dir = std::env::temp_dir().join("blaze-fetch-noaddr");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (handle, hashes) = seed_server(&dir, 1, None).await;
        let ep = build_endpoint(None).await.unwrap();
        let target = PeerTarget {
            endpoint_id: handle.endpoint_id(),
            addr: None,
            relay_url: None,
            direct_only: true,
        };
        let err = fetch_chunks(&ep, &target, 1, &hashes, |_, _| Ok(()))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("无可用连接地址"));
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_fetch_missing_chunk_closes_stream() {
        let dir = std::env::temp_dir().join("blaze-fetch-missing");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let handle = empty_server(&dir, 1).await;
        let ep = build_endpoint(None).await.unwrap();
        let target = PeerTarget {
            endpoint_id: handle.endpoint_id(),
            addr: Some(format!("127.0.0.1:{}", handle.port()).parse().unwrap()),
            relay_url: None,
            direct_only: true,
        };
        let hash = hash_of(b"missing");
        let stats = fetch_chunks(&ep, &target, 1, &[hash], |_, _| Ok(()))
            .await
            .unwrap();
        assert_eq!(stats.downloaded, 0);
        assert_eq!(stats.failed, vec![hash]);
        tokio::time::sleep(Duration::from_millis(700)).await;
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
