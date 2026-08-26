// 跨机 PoC 节点：serve 监听并接受连接；fetch 通过 relay 拨号对端。
// 应用策略：relay-only 连接不传输块数据（relay 只做打洞协助）。
use anyhow::{Context, Result, anyhow};
use iroh::endpoint::presets;
use iroh::{
    Endpoint, EndpointAddr, RelayMode, RelayMap,
    endpoint::Connection,
};
use iroh_relay::tls::CaTlsConfig;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ALPN: &[u8] = b"blaze-poc/1";

fn make_store() -> Arc<HashMap<[u8; 32], Vec<u8>>> {
    let mut store = HashMap::new();
    for i in 0..5u8 {
        let data = vec![i; (i as usize + 1) * 1024 * 1024];
        store.insert(*blake3::hash(&data).as_bytes(), data);
    }
    Arc::new(store)
}

async fn make_endpoint(relay_url: &str, bind_port: u16) -> Result<Endpoint> {
    if bind_port != 0 && bind_port < 10001 {
        return Err(anyhow!("打洞端口必须在 10001-65535 之间（NAT 网关限制），当前 {bind_port}"));
    }
    let url: iroh::RelayUrl = relay_url.parse()?;
    Ok(Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Custom(RelayMap::from_iter([url])))
        .clear_address_lookup()
        .ca_tls_config(CaTlsConfig::insecure_skip_verify())
        .alpns(vec![ALPN.to_vec()])
        .clear_ip_transports()
        .bind_addr(format!("0.0.0.0:{bind_port}"))?
        .bind()
        .await?)
}

fn print_paths(tag: &str, conn: &Connection) -> (bool, bool) {
    let mut has_direct = false;
    let mut has_relay = false;
    for p in conn.paths().iter() {
        has_direct |= p.is_ip();
        has_relay |= p.is_relay();
        println!("  {tag} 路径: relay={} ip={} 详情={:?}", p.is_relay(), p.is_ip(), p);
    }
    (has_direct, has_relay)
}

async fn serve(relay_url: &str, port: u16) -> Result<()> {
    let ep = make_endpoint(relay_url, port).await?;
    let store = make_store();
    println!("serve 端点 ID: {}", ep.id());
    println!("serve 通告地址: {:?}", ep.addr());
    loop {
        let Some(incoming) = ep.accept().await else { break };
        println!("收到入站连接: 本地地址={:?}", incoming.local_addr());
        let store = store.clone();
        tokio::spawn(async move {
            let result: anyhow::Result<()> = async {
                let conn = incoming.await.context("握手失败")?;
                let (direct, relay) = print_paths("服务端", &conn);
                if !direct && relay {
                    println!("→ RELAY_ONLY：按策略拒绝块传输");
                    return Ok::<(), anyhow::Error>(());
                }
                let (mut send, mut recv) = conn.accept_bi().await.context("accept_bi 失败")?;
                let count = recv.read_u32().await.context("读块数失败")?;
                println!("  批量请求 {count} 个块");
                for _ in 0..count {
                    let mut h = [0u8; 32];
                    recv.read_exact(&mut h).await.context("读块哈希失败")?;
                    let data = store.get(&h).context("块不存在")?;
                    send.write_u32(data.len() as u32).await?;
                    send.write_all(data).await?;
                }
                send.finish().context("finish 失败")?;
                // 长连接语义：发完后保持连接短暂存活，避免对端未读完即断
                tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
                Ok(())
            }
            .await;
            if let Err(e) = result {
                println!("连接处理失败: {e:#}");
            }
        });
    }
    Ok(())
}

async fn fetch(relay_url: &str, endpoint_id: &str) -> Result<()> {
    let ep = make_endpoint(relay_url, 0).await?;
    let id: iroh::EndpointId = endpoint_id.parse()?;
    let addr = EndpointAddr::new(id).with_relay_url(relay_url.parse()?);
    println!("fetch 端点 ID: {}，拨号 {endpoint_id}", ep.id());
    let conn = ep.connect(addr, ALPN).await.context("连接失败")?;
    let (direct, relay) = print_paths("客户端", &conn);
    if !direct && relay {
        println!("→ RELAY_ONLY：按策略不传输块数据（符合预期）");
        return Ok(());
    }
    if !direct && !relay {
        return Err(anyhow!("连接没有可用路径"));
    }

    let store = make_store();
    let hashes: Vec<[u8; 32]> = store.keys().copied().take(2).collect();
    let (mut send, mut recv) = conn.open_bi().await.context("open_bi 失败")?;
    send.write_u32(hashes.len() as u32).await?;
    for h in &hashes {
        send.write_all(h).await?;
    }
    send.finish().context("finish 失败")?;
    for h in &hashes {
        let len = recv.read_u32().await.context("读块长度失败")?;
        let mut buf = vec![0u8; len as usize];
        recv.read_exact(&mut buf).await.context("读块数据失败")?;
        let got = *blake3::hash(&buf).as_bytes();
        if &got != h {
            return Err(anyhow!("块哈希校验失败"));
        }
        println!("  收到并校验块 len={}", buf.len());
    }
    println!("→ DIRECT：批量传输成功");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).context("用法: node serve <relay-url> [端口] | node fetch <relay-url> <端点ID>")?;
    match mode {
        "serve" => {
            let relay_url = args.get(2).context("缺少 relay URL")?;
        let port: u16 = args.get(3).map(|s| s.parse().unwrap_or(42001)).unwrap_or(42001);
            serve(relay_url, port).await
        }
        "fetch" => {
            let relay_url = args.get(2).context("缺少 relay URL")?;
            let id = args.get(3).context("缺少端点 ID")?;
            fetch(relay_url, id).await
        }
        _ => Err(anyhow!("未知模式 {mode}")),
    }
}
