//! 多地址保活：轻量 UDP ping/pong，窗口均摊探测，连续失败标记不可达。
//! 设计见 docs/07-网络协议与接口设计文档.md §5。
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::{SocketAddr, UdpSocket};

pub const PING_MAGIC: &[u8; 6] = b"BLZPNG";
pub const PONG_MAGIC: &[u8; 7] = b"BLZPONG";
pub const PING_LEN: usize = 22;
pub const PONG_LEN: usize = 11;

/// 构造 ping 包：magic(6) + seq u32 + node_id u64 + addr_index u32。
pub fn build_ping(seq: u32, node_id: u64, addr_index: u32) -> [u8; PING_LEN] {
    let mut packet = [0u8; PING_LEN];
    packet[..6].copy_from_slice(PING_MAGIC);
    packet[6..10].copy_from_slice(&seq.to_le_bytes());
    packet[10..18].copy_from_slice(&node_id.to_le_bytes());
    packet[18..22].copy_from_slice(&addr_index.to_le_bytes());
    packet
}

/// 构造 pong 包：magic(7) + seq u32。
pub fn build_pong(seq: u32) -> [u8; PONG_LEN] {
    let mut packet = [0u8; PONG_LEN];
    packet[..7].copy_from_slice(PONG_MAGIC);
    packet[7..11].copy_from_slice(&seq.to_le_bytes());
    packet
}

/// 解析 ping 包，返回 (seq, node_id, addr_index)。
pub fn parse_ping(packet: &[u8]) -> Option<(u32, u64, u32)> {
    if packet.len() != PING_LEN || &packet[..6] != PING_MAGIC {
        return None;
    }
    Some((
        u32::from_le_bytes(packet[6..10].try_into().ok()?),
        u64::from_le_bytes(packet[10..18].try_into().ok()?),
        u32::from_le_bytes(packet[18..22].try_into().ok()?),
    ))
}

/// 解析 pong 包，返回 seq。
pub fn parse_pong(packet: &[u8]) -> Option<u32> {
    if packet.len() != PONG_LEN || &packet[..7] != PONG_MAGIC {
        return None;
    }
    Some(u32::from_le_bytes(packet[7..11].try_into().ok()?))
}

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
