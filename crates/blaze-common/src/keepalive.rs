//! 保活 UDP ping/pong 报文编解码（调度中心与节点共用）。

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packet_roundtrip() {
        let ping = build_ping(42, 7, 3);
        assert_eq!(ping.len(), PING_LEN);
        assert_eq!(parse_ping(&ping), Some((42, 7, 3)));
        assert_eq!(parse_ping(&ping[..10]), None);
        assert_eq!(parse_ping(b"garbage"), None);
        let pong = build_pong(42);
        assert_eq!(pong.len(), PONG_LEN);
        assert_eq!(parse_pong(&pong), Some(42));
        assert_eq!(parse_pong(&pong[..5]), None);
        assert_eq!(parse_pong(b"garbage"), None);
    }
}
