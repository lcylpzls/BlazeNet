//! 联调工具：按版本清单从指定端点批量拉取缺失块并校验入库。
use anyhow::{Context, Result, bail};
use blaze_common::manifest::{GameIndex, HASH_LEN};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, RelayMode, TransportAddr};
use iroh_relay::tls::CaTlsConfig;
use origin::storage::GameStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ALPN: &[u8] = b"blazenet/1";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 6 && args.len() != 7 {
        anyhow::bail!(
            "用法: sync <数据目录> <game_id> <manifest路径> <端点ID> <ip:port|relay_url> [relay_url]"
        );
    }
    let data_dir = std::path::PathBuf::from(&args[1]);
    let game_id: u64 = args[2].parse()?;
    let manifest_bytes = std::fs::read(&args[3]).context("读取版本清单失败")?;
    let index = GameIndex::decode(&manifest_bytes).context("解析版本清单失败")?;
    let endpoint_id: iroh::EndpointId = args[4].parse()?;
    let relay_url = if args[5].starts_with("https://") {
        Some(args[5].clone())
    } else {
        args.get(6).cloned()
    };

    let mut builder = Endpoint::builder(presets::Minimal)
        .alpns(vec![ALPN.to_vec()])
        .clear_address_lookup()
        .clear_ip_transports();
    let target = if let Some(url) = relay_url {
        let relay: iroh::RelayUrl = url.parse()?;
        builder = builder
            .relay_mode(RelayMode::Custom(iroh::RelayMap::from_iter(
                [relay.clone()],
            )))
            .ca_tls_config(CaTlsConfig::insecure_skip_verify());
        EndpointAddr::new(endpoint_id).with_relay_url(relay)
    } else {
        builder = builder.relay_mode(RelayMode::Disabled);
        let addr: std::net::SocketAddr = args[5].parse()?;
        EndpointAddr::from_parts(endpoint_id, [TransportAddr::Ip(addr)])
    };
    let ep = builder.bind_addr("0.0.0.0:0")?.bind().await?;

    let mut store = GameStore::open(&data_dir, game_id)?;
    let mut missing: Vec<[u8; HASH_LEN]> = index
        .chunk_set()
        .into_iter()
        .filter(|hash| !store.contains(hash).unwrap_or(false))
        .collect();
    missing.sort();
    println!(
        "清单共引用 {} 块，本地已命中 {} 块，需下载 {} 块",
        index.chunk_set().len(),
        index.chunk_set().len() - missing.len(),
        missing.len()
    );
    if missing.is_empty() {
        println!("全部命中，无需下载");
        return Ok(());
    }

    let conn = ep.connect(target, ALPN).await.context("连接失败")?;
    let paths = conn.paths();
    println!(
        "连接路径：direct={} relay={}",
        paths.iter().any(|p| p.is_ip()),
        paths.iter().any(|p| p.is_relay())
    );
    let (mut send, mut recv) = conn.open_bi().await.context("open_bi 失败")?;
    send.write_u64(game_id).await?;
    send.write_u32(missing.len() as u32).await?;
    for hash in &missing {
        send.write_all(hash).await?;
    }
    send.finish().context("finish 失败")?;

    let mut downloaded = 0u64;
    let mut failed = 0u64;
    for hash in &missing {
        let len = match recv.read_u32().await {
            Ok(len) => len,
            Err(_) => {
                failed += 1;
                continue;
            }
        };
        let mut data = vec![0u8; len as usize];
        if recv.read_exact(&mut data).await.is_err() {
            failed += 1;
            continue;
        }
        let actual: [u8; HASH_LEN] = blake3::hash(&data).into();
        if actual != *hash {
            println!("块哈希校验失败: {}", hex(hash));
            failed += 1;
            continue;
        }
        store.append_chunk(hash, &data)?;
        downloaded += 1;
    }
    println!(
        "同步完成：下载 {downloaded} 块，失败 {failed} 块，pack 大小 {} 字节",
        store.size()
    );
    if failed > 0 {
        bail!("存在失败块");
    }
    Ok(())
}
