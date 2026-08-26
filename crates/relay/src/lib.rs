//! relay 独立服务：UDP 地址回显信令 + 可选 iroh 数据中继。
//!
//! 每个 relay 主机可配置 `data_relay_enabled` 开关：
//! - false：仅提供信令（UDP 地址回显 + QUIC 地址观测），不提供数据中继；
//! - true：在信令基础上额外提供完整 iroh relay（含打洞信令与数据转发）。
//!
//! 说明：iroh-relay 的 WebSocket `/relay` 端点同时承载打洞信令与数据转发，
//! 协议层面无法只保留信令；因此"仅信令"模式不启动该端点，只保留地址观测。
use anyhow::{Context, Result};
use serde::Deserialize;
use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// relay 服务配置（TOML）。
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// UDP 地址回显信令服务监听地址（始终开启，agent `stun_addr` 指向）。
    pub udp_echo_addr: SocketAddr,
    /// 是否提供数据中继；false 时仅提供信令与 QUIC 地址观测。
    pub data_relay_enabled: bool,
    /// iroh relay 明文 HTTP 监听地址（仅数据中继模式使用，供 captive portal 探测）。
    pub http_bind_addr: SocketAddr,
    /// iroh relay HTTPS 监听地址（agent `relay_url` 指向）。
    pub https_bind_addr: SocketAddr,
    /// QUIC 地址观测监听地址。
    pub quic_bind_addr: SocketAddr,
}

impl Config {
    /// 从 TOML 文件加载配置。
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("读取配置文件失败: {}", path.display()))?;
        toml::from_str(&text).context("解析配置文件失败")
    }
}

/// UDP 地址回显主循环：收到任意数据包即回显观测到的来源地址；
/// 收到停止信号或 UDP 收发错误时退出。
async fn echo_loop(sock: UdpSocket, mut stop: oneshot::Receiver<()>) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];
    loop {
        tokio::select! {
            _ = &mut stop => break,
            result = sock.recv_from(&mut buf) => {
                let (_len, src) = result?;
                let reply = format!("ADDR blazenet-relay {src}");
                sock.send_to(reply.as_bytes(), src).await?;
            }
        }
    }
    Ok(())
}

/// UDP 地址回显服务：绑定监听地址并启动回显任务。
async fn spawn_echo(
    addr: SocketAddr,
) -> Result<(
    JoinHandle<std::io::Result<()>>,
    SocketAddr,
    oneshot::Sender<()>,
)> {
    let sock = UdpSocket::bind(addr)
        .await
        .with_context(|| format!("绑定地址回显服务失败: {addr}"))?;
    let local = sock.local_addr().context("获取地址回显监听地址失败")?;
    let (stop_tx, stop_rx) = oneshot::channel();
    let handle = tokio::spawn(echo_loop(sock, stop_rx));
    Ok((handle, local, stop_tx))
}

/// 启动 iroh relay（含打洞信令与数据转发）与 QUIC 地址观测。
async fn spawn_iroh(config: &Config) -> Result<iroh_relay::server::Server> {
    use iroh_relay::server::{
        AllowAll, CertConfig, QuicConfig, RelayConfig, ServerConfig, TlsConfig,
    };
    let (_, server_config) = iroh_relay::server::testing::self_signed_tls_certs_and_config();
    let mut relay = RelayConfig::new(config.http_bind_addr);
    relay.tls = Some(TlsConfig::new(
        config.https_bind_addr,
        CertConfig::Manual {
            server_config: server_config.clone(),
        },
    ));
    relay.key_cache_capacity = Some(1024);
    relay.access = Arc::new(AllowAll);
    let mut quic = QuicConfig::new(config.quic_bind_addr);
    quic.server_config = Some(server_config);
    let mut server = ServerConfig::default();
    server.relay = Some(relay);
    server.quic = Some(quic);
    iroh_relay::server::Server::spawn(server)
        .await
        .context("启动 iroh relay 失败")
}

/// 启动仅 QUIC 地址观测服务（信令模式，不提供数据中继）。
async fn spawn_quic_only(config: &Config) -> Result<iroh_relay::server::Server> {
    use iroh_relay::server::{QuicConfig, ServerConfig};
    let (_, server_config) = iroh_relay::server::testing::self_signed_tls_certs_and_config();
    let mut quic = QuicConfig::new(config.quic_bind_addr);
    quic.server_config = Some(server_config);
    let mut server = ServerConfig::default();
    server.quic = Some(quic);
    iroh_relay::server::Server::spawn(server)
        .await
        .context("启动 QUIC 地址观测失败")
}

/// 启动 relay 服务；`stop` 触发后优雅退出（测试可注入即时停止）。
pub async fn run(config: Config, stop: impl Future<Output = ()>) -> Result<()> {
    let (echo, echo_addr, echo_stop) = spawn_echo(config.udp_echo_addr).await?;
    println!("地址回显信令已启动: {echo_addr}");
    let server = if config.data_relay_enabled {
        let server = spawn_iroh(&config).await?;
        println!(
            "iroh relay（信令+中继）已启动: http={} https={} quic={}",
            config.http_bind_addr, config.https_bind_addr, config.quic_bind_addr
        );
        server
    } else {
        let server = spawn_quic_only(&config).await?;
        println!("仅信令模式：QUIC 地址观测已启动: {}", config.quic_bind_addr);
        server
    };
    println!("relay 服务运行中，按 Ctrl+C 退出");
    stop.await;
    server.shutdown().await.context("relay 服务关闭失败")?;
    let _ = echo_stop.send(());
    let _ = echo.await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::Duration;

    #[test]
    fn test_config_from_file() {
        let dir = std::env::temp_dir().join(format!("blazenet-relay-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("relay.toml");
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(file, "udp_echo_addr = \"127.0.0.1:42000\"").unwrap();
        writeln!(file, "data_relay_enabled = true").unwrap();
        writeln!(file, "http_bind_addr = \"127.0.0.1:8080\"").unwrap();
        writeln!(file, "https_bind_addr = \"127.0.0.1:8443\"").unwrap();
        writeln!(file, "quic_bind_addr = \"127.0.0.1:7842\"").unwrap();
        let config = Config::from_file(&path).unwrap();
        assert!(config.data_relay_enabled);
        assert_eq!(config.udp_echo_addr.port(), 42000);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_config_from_file_bad() {
        let dir = std::env::temp_dir().join(format!("blazenet-relay-bad-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("relay.toml");
        std::fs::write(&path, "udp_echo_addr = \"不是地址\"").unwrap();
        assert!(Config::from_file(&path).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn test_echo_replies_source_addr() {
        let (handle, addr, stop_tx) = spawn_echo("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sock.send_to(b"ECHO blazenet-agent", addr).await.unwrap();
        let mut buf = [0u8; 256];
        let (len, src) = tokio::time::timeout(Duration::from_secs(3), sock.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(src, addr);
        let reply = String::from_utf8_lossy(&buf[..len]);
        assert!(reply.starts_with("ADDR blazenet-relay "));
        assert!(
            reply
                .split_whitespace()
                .nth(2)
                .unwrap()
                .parse::<SocketAddr>()
                .is_ok()
        );
        let _ = stop_tx.send(());
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_spawn_full_relay() {
        let config = Config {
            udp_echo_addr: "127.0.0.1:0".parse().unwrap(),
            data_relay_enabled: true,
            http_bind_addr: "127.0.0.1:0".parse().unwrap(),
            https_bind_addr: "127.0.0.1:0".parse().unwrap(),
            quic_bind_addr: "127.0.0.1:0".parse().unwrap(),
        };
        let server = spawn_iroh(&config).await.unwrap();
        assert!(server.http_addr().is_some());
        assert!(server.https_addr().is_some());
        assert!(server.quic_addr().is_some());
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_spawn_signaling_only() {
        let config = Config {
            udp_echo_addr: "127.0.0.1:0".parse().unwrap(),
            data_relay_enabled: false,
            http_bind_addr: "127.0.0.1:0".parse().unwrap(),
            https_bind_addr: "127.0.0.1:0".parse().unwrap(),
            quic_bind_addr: "127.0.0.1:0".parse().unwrap(),
        };
        let server = spawn_quic_only(&config).await.unwrap();
        assert!(server.http_addr().is_none());
        assert!(server.https_addr().is_none());
        assert!(server.quic_addr().is_some());
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_run_full_relay() {
        let config = Config {
            udp_echo_addr: "127.0.0.1:0".parse().unwrap(),
            data_relay_enabled: true,
            http_bind_addr: "127.0.0.1:0".parse().unwrap(),
            https_bind_addr: "127.0.0.1:0".parse().unwrap(),
            quic_bind_addr: "127.0.0.1:0".parse().unwrap(),
        };
        run(config, async {}).await.unwrap();
    }

    #[tokio::test]
    async fn test_run_signaling_only() {
        let config = Config {
            udp_echo_addr: "127.0.0.1:0".parse().unwrap(),
            data_relay_enabled: false,
            http_bind_addr: "127.0.0.1:0".parse().unwrap(),
            https_bind_addr: "127.0.0.1:0".parse().unwrap(),
            quic_bind_addr: "127.0.0.1:0".parse().unwrap(),
        };
        run(config, async {}).await.unwrap();
    }
}
