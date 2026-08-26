//! 启动期地址探测：向 relay 主机的 UDP 地址回显服务查询本端 NAT 公网地址。
//!
//! 必须与数据面绑定同一本地端口探测，才能拿到一致的 NAT 映射；
//! 仅适用于端口保留型 NAT（网吧全锥/受限锥形），对称 NAT 不保证。
use anyhow::{Context, Result, bail};
use std::net::SocketAddr;
use std::time::Duration;

/// 默认探测超时。
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);

/// 向地址回显服务发送探测，返回服务观测到的本端公网地址。
pub async fn discover(server: &str, local_port: u16) -> Result<SocketAddr> {
    discover_with_timeout(server, local_port, DEFAULT_TIMEOUT).await
}

/// 带超时的探测实现（测试可注入短超时）。
pub async fn discover_with_timeout(
    server: &str,
    local_port: u16,
    timeout: Duration,
) -> Result<SocketAddr> {
    let server_addr: SocketAddr = server
        .parse()
        .with_context(|| format!("地址回显服务地址非法: {server}"))?;
    let sock = tokio::net::UdpSocket::bind(("0.0.0.0", local_port))
        .await
        .context("绑定探测 socket 失败")?;
    sock.send_to(b"ECHO blazenet-agent", server_addr)
        .await
        .context("发送地址探测失败")?;
    let mut buf = [0u8; 256];
    let (len, src) = tokio::time::timeout(timeout, sock.recv_from(&mut buf))
        .await
        .context("等待地址回显超时")?
        .context("接收地址回显失败")?;
    if src != server_addr {
        bail!("地址回显来源不匹配: {src}");
    }
    let text = String::from_utf8_lossy(&buf[..len]);
    let mut parts = text.split_whitespace();
    let _tag = parts.next();
    let _name = parts.next();
    let addr_text = parts.next().context("地址回显格式错误")?;
    addr_text
        .parse()
        .with_context(|| format!("地址回显内容非法: {addr_text}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_discover_ok() {
        let server = Arc::new(tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap());
        let addr = server.local_addr().unwrap();
        let handle = server.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 256];
            let (len, src) = handle.recv_from(&mut buf).await.unwrap();
            assert!(buf[..len].starts_with(b"ECHO "));
            let reply = format!("ADDR blazenet-agent {src}");
            let _ = handle.send_to(reply.as_bytes(), src).await;
        });
        let found = discover_with_timeout(&addr.to_string(), 0, Duration::from_secs(2))
            .await
            .unwrap();
        assert!(found.ip().is_loopback());
        assert!(found.port() > 0);
    }

    #[tokio::test]
    async fn test_discover_bad_reply() {
        let server = Arc::new(tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap());
        let addr = server.local_addr().unwrap();
        let handle = server.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 256];
            let (_, src) = handle.recv_from(&mut buf).await.unwrap();
            let _ = handle.send_to(b"garbage", src).await;
        });
        let err = discover_with_timeout(&addr.to_string(), 0, Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("格式错误") || err.to_string().contains("非法"));
    }

    #[tokio::test]
    async fn test_discover_wrong_source() {
        let server = Arc::new(tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap());
        let addr = server.local_addr().unwrap();
        let handle = server.clone();
        let other = Arc::new(tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap());
        let other_handle = other.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 256];
            let (_, src) = handle.recv_from(&mut buf).await.unwrap();
            let reply = format!("ADDR blazenet-agent {src}");
            let _ = other_handle.send_to(reply.as_bytes(), src).await;
        });
        let err = discover_with_timeout(&addr.to_string(), 0, Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("来源不匹配"));
    }

    #[tokio::test]
    async fn test_discover_timeout() {
        let server = tokio::net::UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = server.local_addr().unwrap();
        let err = discover_with_timeout(&addr.to_string(), 0, Duration::from_millis(100))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("超时"));
    }

    #[tokio::test]
    async fn test_discover_invalid_server() {
        let err = discover("not-an-addr", 0).await.unwrap_err();
        assert!(err.to_string().contains("非法"));
    }
}
