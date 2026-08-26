//! 联调工具：从指定端点按哈希拉取块并校验。
use anyhow::{Context, Result};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, RelayMode, TransportAddr};
use iroh_relay::tls::CaTlsConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ALPN: &[u8] = b"blazenet/1";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 && args.len() != 6 && args.len() != 7 {
        anyhow::bail!(
            "用法: fetch <端点ID> <ip:port|relay_url> <game_id> <块哈希hex> [relay_url] [本端通告地址]"
        );
    }
    let endpoint_id: iroh::EndpointId = args[1].parse()?;
    let game_id: u64 = args[3].parse()?;
    let hash: [u8; 32] = hex_decode(&args[4])?;
    let relay_url = if args[2].starts_with("https://") {
        Some(args[2].clone())
    } else {
        args.get(5).cloned()
    };
    let external_addr = args
        .get(6)
        .map(|s| s.parse::<std::net::SocketAddr>())
        .transpose()?;

    let mut builder = Endpoint::builder(presets::Minimal)
        .alpns(vec![ALPN.to_vec()])
        .clear_address_lookup()
        .clear_ip_transports();
    builder = if let Some(addr) = external_addr {
        builder
            .external_addr(addr)
            .bind_addr(format!("0.0.0.0:{}", addr.port()))?
    } else {
        builder.bind_addr("0.0.0.0:0")?
    };
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
        let addr: std::net::SocketAddr = args[2].parse()?;
        EndpointAddr::from_parts(endpoint_id, [TransportAddr::Ip(addr)])
    };
    let ep = builder.bind().await?;
    let conn = ep.connect(target, ALPN).await.context("连接失败")?;
    // NAT 打洞的直连路径迁移需要时间，等待直连建立后再请求数据。
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let paths = conn.paths();
        let direct = paths.iter().any(|p| p.is_ip());
        let relay = paths.iter().any(|p| p.is_relay());
        println!(
            "路径状态：direct={direct} relay={relay} 路径数={}",
            paths.len()
        );
        if direct {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("等待直连路径超时（20 秒）");
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let (mut send, mut recv) = conn.open_bi().await.context("open_bi 失败")?;
    send.write_u64(game_id).await?;
    send.write_u32(1).await?;
    send.write_all(&hash).await?;
    send.finish().context("finish 失败")?;
    let len = recv.read_u32().await.context("读块长度失败")?;
    let mut data = vec![0u8; len as usize];
    recv.read_exact(&mut data).await.context("读块数据失败")?;
    let actual: [u8; 32] = blake3::hash(&data).into();
    if actual != hash {
        anyhow::bail!("块哈希校验失败");
    }
    println!("拉取成功：{} 字节，哈希校验通过", data.len());
    Ok(())
}

fn hex_decode(text: &str) -> Result<[u8; 32]> {
    if text.len() != 64 {
        anyhow::bail!("哈希必须是 64 位十六进制");
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)?;
    }
    Ok(out)
}
