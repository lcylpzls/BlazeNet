//! 联调工具：从指定端点按哈希拉取块并校验。
use anyhow::{Context, Result};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, RelayMode, TransportAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ALPN: &[u8] = b"blazenet/1";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        anyhow::bail!("用法: fetch <端点ID> <ip:port> <game_id> <块哈希hex>");
    }
    let endpoint_id: iroh::EndpointId = args[1].parse()?;
    let addr: std::net::SocketAddr = args[2].parse()?;
    let game_id: u64 = args[3].parse()?;
    let hash: [u8; 32] = hex_decode(&args[4])?;

    let ep = Endpoint::builder(presets::Minimal)
        .alpns(vec![ALPN.to_vec()])
        .clear_address_lookup()
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr("0.0.0.0:0")?
        .bind()
        .await?;
    let target = EndpointAddr::from_parts(endpoint_id, [TransportAddr::Ip(addr)]);
    let conn = ep.connect(target, ALPN).await.context("连接失败")?;
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
