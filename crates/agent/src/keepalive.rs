//! 保活 pong 应答：接收调度中心 UDP ping，回复 BLZPONG。
use anyhow::{Context, Result};
use blaze_common::keepalive::{PING_LEN, build_pong, parse_ping};
use tokio::sync::oneshot;

/// 在指定 UDP 端口应答调度中心的保活 ping，收到停止信号退出。
pub async fn serve_pong(port: u16, mut shutdown: oneshot::Receiver<()>) -> Result<()> {
    let sock = tokio::net::UdpSocket::bind(("0.0.0.0", port))
        .await
        .context("绑定保活 UDP 端口失败")?;
    let mut buf = [0u8; PING_LEN];
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            recv = sock.recv_from(&mut buf) => {
                let (len, src) = recv?;
                if let Some((seq, _, _)) = parse_ping(&buf[..len]) {
                    let pong = build_pong(seq);
                    let _ = sock.send_to(&pong, src).await;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blaze_common::keepalive::{PONG_LEN, PONG_MAGIC, build_ping, parse_pong};

    #[tokio::test]
    async fn test_serve_pong_roundtrip() {
        let probe = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(serve_pong(port, rx));
        let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        // 无效包应被忽略。
        client.send_to(b"garbage", target).await.unwrap();
        let ping = build_ping(7, 1, 0);
        client.send_to(&ping, target).await.unwrap();
        let mut buf = [0u8; PONG_LEN];
        let (len, _) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.recv_from(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&buf[..7], PONG_MAGIC);
        assert_eq!(parse_pong(&buf[..len]), Some(7));
        tx.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_serve_pong_bind_failure() {
        let holder = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = holder.local_addr().unwrap().port();
        let (tx, rx) = oneshot::channel();
        let err = serve_pong(port, rx).await.unwrap_err();
        assert!(err.to_string().contains("绑定保活 UDP 端口失败"));
        let _ = tx;
    }
}
