//! 多地址保活：轻量 UDP ping/pong，窗口均摊探测，连续失败标记不可达。
//! 设计见 docs/07-网络协议与接口设计文档.md §5。
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use blaze_common::keepalive::{build_ping, parse_pong};
use tokio::sync::oneshot;

pub use blaze_common::keepalive::{
    PING_LEN, PING_MAGIC, PONG_LEN, PONG_MAGIC, build_pong, parse_ping,
};

use crate::db::Store;

/// 发送 UDP ping 包。
pub fn send_ping(socket: &UdpSocket, target: SocketAddr, packet: &[u8]) -> std::io::Result<()> {
    socket.send_to(packet, target)?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddrState {
    pub failures: u32,
    pub available: bool,
}

impl Default for AddrState {
    fn default() -> Self {
        Self {
            failures: 0,
            available: true,
        }
    }
}

/// 保活状态：地址 → 连续失败次数/可用性。
#[derive(Debug, Default)]
pub struct Keepalive {
    addrs: HashMap<SocketAddr, AddrState>,
}

impl Keepalive {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册地址（视为可用）。
    pub fn register(&mut self, addr: SocketAddr) {
        self.addrs.entry(addr).or_default();
    }

    /// 当前时刻应探测的地址（按地址哈希均摊到窗口）。
    pub fn probe_due(&self, now_secs: u64, interval_secs: u64) -> Vec<SocketAddr> {
        let slot = now_secs % interval_secs;
        self.addrs
            .keys()
            .filter(|addr| addr_slot(**addr, interval_secs) == slot)
            .copied()
            .collect()
    }

    /// 记录探测结果；连续失败达阈值标记不可达，成功即恢复。
    pub fn record(&mut self, addr: SocketAddr, ok: bool, fail_threshold: u32) {
        let state = self.addrs.entry(addr).or_default();
        if ok {
            state.failures = 0;
            state.available = true;
        } else {
            state.failures += 1;
            if state.failures >= fail_threshold {
                state.available = false;
            }
        }
    }

    /// 当前不可达地址列表。
    pub fn unavailable(&self) -> Vec<SocketAddr> {
        self.addrs
            .iter()
            .filter(|(_, s)| !s.available)
            .map(|(addr, _)| *addr)
            .collect()
    }
}

fn addr_slot(addr: SocketAddr, interval_secs: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    addr.hash(&mut hasher);
    hasher.finish() % interval_secs
}

/// 保活服务主循环：按窗口均摊探测节点声明可入站地址（kind=config），
/// pong 恢复可用、连续失败达阈值标记离线；`stop` 触发后退出。
pub async fn run(
    store: Arc<Store>,
    socket: Arc<tokio::net::UdpSocket>,
    interval_secs: u64,
    fail_threshold: u32,
    mut stop: oneshot::Receiver<()>,
) {
    let mut keep = Keepalive::new();
    let mut seq = 0u32;
    let mut outstanding: HashMap<u32, (SocketAddr, Instant)> = HashMap::new();
    let mut meta: HashMap<SocketAddr, (u64, u32)> = HashMap::new();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = &mut stop => break,
            _ = ticker.tick() => {
                let now_secs = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                // 同步节点可入站地址（仅 kind=config 的映射地址，打洞地址不探测）。
                let nodes = store.list_nodes().unwrap_or_default();
                meta.clear();
                for node in nodes {
                    for (index, addr) in node.addrs.iter().enumerate() {
                        if addr.kind == "config"
                            && let Ok(parsed) = addr.addr.parse()
                        {
                            meta.insert(parsed, (node.id, index as u32));
                            keep.register(parsed);
                        }
                    }
                }
                // 发送本轮应探测地址的 ping（按地址哈希均摊窗口）。
                for (addr, (node_id, addr_index)) in &meta {
                    if addr_slot(*addr, interval_secs) == now_secs % interval_secs {
                        seq = seq.wrapping_add(1);
                        let ping = build_ping(seq, *node_id, *addr_index);
                        let _ = socket.send_to(&ping, *addr).await;
                        outstanding.insert(seq, (*addr, Instant::now()));
                    }
                }
                // 收取 pong。
                let mut buf = [0u8; PONG_LEN];
                loop {
                    let recv = tokio::time::timeout(
                        Duration::from_millis(10),
                        socket.recv_from(&mut buf),
                    )
                    .await;
                    let Ok(Ok((len, _src))) = recv else {
                        break;
                    };
                    if let Some(recv_seq) = parse_pong(&buf[..len])
                        && let Some((addr, _)) = outstanding.remove(&recv_seq)
                    {
                        keep.record(addr, true, fail_threshold);
                    }
                }
                // 超时未回执的探测记为失败。
                let expired: Vec<(u32, SocketAddr)> = outstanding
                    .iter()
                    .filter(|(_, (_, sent))| {
                        sent.elapsed() >= Duration::from_secs(interval_secs)
                    })
                    .map(|(s, (addr, _))| (*s, *addr))
                    .collect();
                for (s, addr) in expired {
                    outstanding.remove(&s);
                    keep.record(addr, false, fail_threshold);
                }
                // 按可用性同步节点状态。
                for (addr, (node_id, _)) in &meta {
                    let available = keep.addrs.get(addr).map(|s| s.available).unwrap_or(true);
                    let want = if available { "online" } else { "offline" };
                    if let Ok(Some(mut node)) = store.get_node(*node_id)
                        && node.status != want
                    {
                        node.status = want.to_string();
                        let _ = store.insert_node(&node);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{AddrRecord, NodeRecord};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::SystemTime;

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn insert_node(store: &Store, id: u64, addr: AddrRecord) {
        store
            .insert_node(&NodeRecord {
                id,
                node_type: "idc".to_string(),
                endpoint_id: format!("endpoint-{id}"),
                token: "token".to_string(),
                addrs: vec![addr],
                status: "online".to_string(),
                last_heartbeat_ms: now_ms(),
            })
            .unwrap();
    }

    async fn wait_status(store: &Store, want: &'static str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            let status = store.get_node(1).unwrap().unwrap().status;
            if status == want {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline, "状态未变为 {want}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[test]
    fn test_packet_roundtrip() {
        let ping = build_ping(42, 7, 3);
        assert_eq!(ping.len(), PING_LEN);
        assert_eq!(parse_ping(&ping), Some((42, 7, 3)));
        assert_eq!(parse_ping(&ping[..10]), None);
        let pong = build_pong(42);
        assert_eq!(pong.len(), PONG_LEN);
        assert_eq!(parse_pong(&pong), Some(42));
        assert_eq!(parse_pong(&pong[..5]), None);
    }

    #[test]
    fn test_udp_send_and_pong() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let target = receiver.local_addr().unwrap();
        let ping = build_ping(9, 1, 0);
        send_ping(&sender, target, &ping).unwrap();
        let mut buf = [0u8; PING_LEN];
        let (n, from) = receiver.recv_from(&mut buf).unwrap();
        assert_eq!(&buf[..n], &ping);
        let pong = build_pong(9);
        send_ping(&receiver, from, &pong).unwrap();
        let mut buf2 = [0u8; PONG_LEN];
        let (n2, _) = sender.recv_from(&mut buf2).unwrap();
        assert_eq!(&buf2[..n2], &pong);
    }

    #[test]
    fn test_probe_due_spread() {
        let mut keep = Keepalive::new();
        let addrs: Vec<SocketAddr> = ["127.0.0.1:10001", "127.0.0.1:10002", "127.0.0.1:10003"]
            .iter()
            .map(|s| s.parse().unwrap())
            .collect();
        for addr in &addrs {
            keep.register(*addr);
        }
        let mut seen = Vec::new();
        for now in 0..10 {
            seen.extend(keep.probe_due(now, 10));
        }
        for addr in &addrs {
            assert!(seen.contains(addr));
        }
        assert!(seen.len() >= 3);
    }

    #[test]
    fn test_record_transitions() {
        let mut keep = Keepalive::new();
        let addr: SocketAddr = "127.0.0.1:10004".parse().unwrap();
        keep.register(addr);
        keep.record(addr, false, 3);
        keep.record(addr, false, 3);
        assert!(keep.unavailable().is_empty());
        keep.record(addr, false, 3);
        assert_eq!(keep.unavailable(), vec![addr]);
        keep.record(addr, true, 3);
        assert!(keep.unavailable().is_empty());
        assert_eq!(keep.addrs[&addr].failures, 0);
    }

    #[tokio::test]
    async fn test_run_marks_offline_without_pong() {
        let dir = std::env::temp_dir().join("blaze-keep-offline");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Arc::new(Store::open(&dir).unwrap());
        // 无响应端 + 一个 kind=stun 地址（不应探测）+ 一个非法 config 地址（应跳过）。
        let silent = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = silent.local_addr().unwrap();
        insert_node(
            &store,
            1,
            AddrRecord {
                addr: target.to_string(),
                kind: "config".to_string(),
                link: String::new(),
            },
        );
        insert_node(
            &store,
            2,
            AddrRecord {
                addr: "127.0.0.1:9".to_string(),
                kind: "stun".to_string(),
                link: String::new(),
            },
        );
        insert_node(
            &store,
            3,
            AddrRecord {
                addr: "不合法".to_string(),
                kind: "config".to_string(),
                link: String::new(),
            },
        );
        let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(run(store.clone(), socket, 2, 2, rx));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            let status = store.get_node(1).unwrap().unwrap().status;
            if status == "offline" {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "节点未按时离线");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(store.get_node(2).unwrap().unwrap().status, "online");
        tx.send(()).unwrap();
        task.await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_run_pong_keeps_online_and_restores() {
        let dir = std::env::temp_dir().join("blaze-keep-pong");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Arc::new(Store::open(&dir).unwrap());
        // 应答端：默认回 pong，可切换为不回。
        let responder = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let target = responder.local_addr().unwrap();
        let enabled = Arc::new(AtomicBool::new(true));
        let stop = Arc::new(AtomicBool::new(false));
        let r_enabled = enabled.clone();
        let r_stop = stop.clone();
        let r_sock = responder.clone();
        let responder_task = tokio::spawn(async move {
            let mut buf = [0u8; PING_LEN];
            loop {
                if r_stop.load(Ordering::Relaxed) {
                    break;
                }
                let recv =
                    tokio::time::timeout(Duration::from_millis(200), r_sock.recv_from(&mut buf))
                        .await;
                match recv {
                    Ok(Ok((len, src))) => {
                        if let Some((seq, _, _)) = parse_ping(&buf[..len])
                            && r_enabled.load(Ordering::Relaxed)
                        {
                            let _ = r_sock.send_to(&build_pong(seq), src).await;
                        }
                    }
                    _ => continue,
                }
            }
        });
        insert_node(
            &store,
            1,
            AddrRecord {
                addr: target.to_string(),
                kind: "config".to_string(),
                link: String::new(),
            },
        );
        // 先发一个垃圾包给调度 socket，覆盖无效 pong 分支。
        let probe = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        probe.send_to(b"garbage", target).await.unwrap();
        let sched = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        probe
            .send_to(b"garbage", sched.local_addr().unwrap())
            .await
            .unwrap();
        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(run(store.clone(), sched, 2, 2, rx));
        wait_status(&store, "online").await;
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(store.get_node(1).unwrap().unwrap().status, "online");
        enabled.store(false, Ordering::Relaxed);
        wait_status(&store, "offline").await;
        enabled.store(true, Ordering::Relaxed);
        wait_status(&store, "online").await;
        stop.store(true, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(300)).await;
        responder_task.await.unwrap();
        tx.send(()).unwrap();
        task.await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
