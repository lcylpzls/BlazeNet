// netpoc：验证 iroh 自定义 ALPN 批量流式传块、多 IP 入站分流、relay-only 路径拦截。
use anyhow::{Context, Result, anyhow};
use iroh::endpoint::presets;
use iroh::{
    Endpoint, EndpointAddr, RelayMode, RelayMap, TransportAddr,
    endpoint::{BindOpts, Connection},
};
use iroh_relay::server::{Server, testing};
use iroh_relay::tls::CaTlsConfig;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
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

async fn start_relay() -> Result<(RelayMap, iroh::RelayUrl)> {
    let server = Server::spawn(testing::server_config())
        .await
        .context("启动 relay 失败")?;
    let addr = server
        .https_addr()
        .context("relay 未提供 https 地址")?;
    let url: iroh::RelayUrl = format!("https://{addr}").parse()?;
    println!("relay 已启动: {url}");
    Ok((RelayMap::from_iter([url.clone()]), url))
}

async fn new_endpoint(
    relay_map: RelayMap,
    binds: &[SocketAddr],
) -> Result<Endpoint> {
    let mut b = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Custom(relay_map))
        .clear_address_lookup()
        .ca_tls_config(CaTlsConfig::insecure_skip_verify())
        .alpns(vec![ALPN.to_vec()]);
    b = b.clear_ip_transports();
    for addr in binds {
        // 单地址用默认路由（/0），多地址场景每个 IP 用 /32 精确绑定
        b = if binds.len() == 1 {
            b.bind_addr(*addr)?
        } else {
            b.bind_addr_with_opts(*addr, BindOpts::default().set_prefix_len(32))?
        };
    }
    Ok(b.bind().await?)
}

async fn handle_conn(
    conn: Connection,
    store: Arc<HashMap<[u8; 32], Vec<u8>>>,
) -> Result<()> {
    let paths = conn.paths();
    let has_direct = paths.iter().any(|p| p.is_ip());
    let has_relay = paths.iter().any(|p| p.is_relay());
    println!(
        "收到连接 remote={:?} direct_path={} relay_path={}",
        conn.remote_id(),
        has_direct,
        has_relay
    );
    // 关键验证：relay-only 连接不传块数据（relay 只做打洞协助）
    if !has_direct && has_relay {
        println!("→ relay-only 路径，按策略拒绝块传输，仅保留连接");
        return Ok(());
    }
    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .context("accept_bi 失败")?;
    let count = recv.read_u32().await.context("读块数失败")?;
    println!("  批量请求 {} 个块", count);
    for _ in 0..count {
        let mut h = [0u8; 32];
        recv.read_exact(&mut h).await.context("读块哈希失败")?;
        let data = store.get(&h).context("块不存在")?;
        eprintln!("  服务端发送块 len={}", data.len());
        send.write_u32(data.len() as u32).await?;
        send.write_all(data).await?;
    }
    eprintln!("  服务端发送完成");
    send.finish().context("finish 失败")?;
    // 生产中连接是长连接复用，不会在发完即断；PoC 用短暂保活避免对端未读完就被关闭
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    Ok(())
}

async fn fetch_batch(
    client: &Endpoint,
    server_addr: EndpointAddr,
    want: &[[u8; 32]],
) -> Result<()> {
    let conn = client.connect(server_addr.clone(), ALPN).await?;
    let paths = conn.paths();
    println!(
        "客户端连接 {}: direct_path={} relay_path={}",
        server_addr
            .ip_addrs()
            .next()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "relay-only".into()),
        paths.iter().any(|p| p.is_ip()),
        paths.iter().any(|p| p.is_relay())
    );
    let (mut send, mut recv) = conn.open_bi().await.context("open_bi 失败")?;
    send.write_u32(want.len() as u32).await?;
    for h in want {
        send.write_all(h).await?;
    }
    send.finish().context("finish 失败")?;
    let mut total = 0usize;
    for h in want {
        let len = recv.read_u32().await.context("读块长度失败")?;
        let mut buf = vec![0u8; len as usize];
        recv.read_exact(&mut buf).await.context("读块数据失败")?;
        eprintln!("  客户端收到块 len={}", buf.len());
        let got = *blake3::hash(&buf).as_bytes();
        if &got != h {
            return Err(anyhow!("块哈希校验失败"));
        }
        total += buf.len();
    }
    println!(
        "  收到并校验 {} 个块，共 {:.1}MiB",
        want.len(),
        total as f64 / 1024.0 / 1024.0
    );
    Ok(())
}

fn check_loopback_alias(addr: &SocketAddr) -> Result<()> {
    let ip = addr.ip();
    if let IpAddr::V4(v4) = ip {
        if !v4.is_loopback() || v4 == Ipv4Addr::LOCALHOST {
            return Ok(());
        }
        // 回环别名：127.0.0.2/127.0.0.3 需要在执行前添加
        let out = std::process::Command::new("ip")
            .args(["addr", "show", "dev", "lo"])
            .output()
            .context("无法执行 ip 命令（需要 root）")?;
        let text = String::from_utf8_lossy(&out.stdout);
        if !text.contains(&v4.to_string()) {
            return Err(anyhow!(
                "回环别名 {} 不存在，请先执行: ip addr add {}/8 dev lo",
                v4, v4
            ));
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let (relay_map, relay_url) = start_relay().await?;
    let store = make_store();
    let hashes: Vec<[u8; 32]> = store.keys().copied().collect();

    // 服务端：绑定两个回环别名 IP（多 IP 入站）
    let addr_a: SocketAddr = "127.0.0.2:0".parse()?;
    let addr_b: SocketAddr = "127.0.0.3:0".parse()?;
    check_loopback_alias(&addr_a)?;
    check_loopback_alias(&addr_b)?;
    let server = new_endpoint(relay_map.clone(), &[addr_a, addr_b]).await?;
    let bound = server.bound_sockets();
    println!("服务端绑定地址: {:?}", bound);
    let server_id = server.id();
    let handler = tokio::spawn({
        let server = server.clone();
        let store = store.clone();
        async move {
        loop {
            let Some(incoming) = server.accept().await else { break };
            println!("入站连接本地地址: {:?}", incoming.local_addr());
            let store = store.clone();
            tokio::spawn(async move {
                let result = async {
                    let conn = incoming.await.context("连接握手失败")?;
                    handle_conn(conn, store).await
                }
                .await;
                if let Err(e) = result {
                    println!("连接处理失败: {e:#}");
                }
            });
        }
        Ok::<(), anyhow::Error>(())
        }
    });

    let client = new_endpoint(relay_map.clone(), &["127.0.0.1:0".parse()?]).await?;

    println!("\n== 1. 多 IP 直连 + 批量流式 ==");
    let s1 = server
        .bound_sockets()
        .iter()
        .find(|s| s.ip().to_string() == "127.0.0.2")
        .copied()
        .context("服务端缺少 127.0.0.2 地址")?;
    let s2 = server
        .bound_sockets()
        .iter()
        .find(|s| s.ip().to_string() == "127.0.0.3")
        .copied()
        .context("服务端缺少 127.0.0.3 地址")?;
    let addr1 = EndpointAddr::from_parts(server_id, [TransportAddr::Ip(s1)]);
    let addr2 = EndpointAddr::from_parts(server_id, [TransportAddr::Ip(s2)]);
    if let Err(e) = fetch_batch(&client, addr1, &hashes[..3]).await {
        eprintln!("fetch 失败: {e:#}");
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        return Err(e);
    }
    fetch_batch(&client, addr2, &hashes[3..]).await?;

    println!("\n== 2. relay-only 路径拦截 ==");
    let relay_addr = EndpointAddr::new(server_id).with_relay_url(relay_url);
    let conn = client.connect(relay_addr, ALPN).await?;
    let paths = conn.paths();
    println!(
        "relay-only 连接建立成功: direct_path={} relay_path={}",
        paths.iter().any(|p| p.is_ip()),
        paths.iter().any(|p| p.is_relay())
    );
    println!("→ 按应用策略不打开块传输流（等待服务端侧确认拦截）");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    client.close().await;
    server.close().await;
    handler.abort();
    println!("\nnetpoc 验证完成");
    Ok(())
}
