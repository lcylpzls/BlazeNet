// relay 独立服务：默认监听 0.0.0.0:8443，自签证书（PoC 用，客户端需跳过证书校验）。
use iroh_relay::server::{
    AllowAll, CertConfig, RelayConfig as RelayServerConfig, Server, ServerConfig, TlsConfig,
    testing::self_signed_tls_certs_and_config,
};
use std::net::Ipv4Addr;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let port: u16 = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "8443".into())
        .parse()
        .expect("端口参数必须是数字");

    let (_certs, server_config) = self_signed_tls_certs_and_config();
    let tls = TlsConfig::new(
        (Ipv4Addr::UNSPECIFIED, port),
        CertConfig::Manual { server_config },
    );
    // 纯 HTTP 辅助服务（captive portal）用随机端口，避免与 HTTPS 主端口冲突
    let mut relay = RelayServerConfig::new((Ipv4Addr::UNSPECIFIED, 0));
    relay.tls = Some(tls);
    relay.key_cache_capacity = Some(1024);
    relay.access = Arc::new(AllowAll);

    let mut config = ServerConfig::default();
    config.relay = Some(relay);
    let server = Server::spawn(config).await?;
    let addr = server.https_addr().expect("relay 未提供 https 地址");
    println!("relay 已启动: https://{addr}");
    std::future::pending::<()>().await;
    Ok(())
}
