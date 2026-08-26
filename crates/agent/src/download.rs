//! 多源下载调度：全局去重、唯一分配、失败回填。
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// 按 RTT 升序排序候选源。
pub fn sort_by_rtt<T>(peers: &mut [(T, Duration)]) {
    peers.sort_by_key(|(_, rtt)| *rtt);
}

/// 下载管理器：维护待下载块与各 peer 的分配。
#[derive(Debug, Default)]
pub struct DownloadManager {
    pending: HashSet<[u8; 32]>,
    assigned: HashMap<usize, Vec<[u8; 32]>>,
}

impl DownloadManager {
    pub fn new(chunks: Vec<[u8; 32]>) -> Self {
        Self {
            pending: chunks.into_iter().collect(),
            assigned: HashMap::new(),
        }
    }

    /// 待下载块集合。
    pub fn pending(&self) -> &HashSet<[u8; 32]> {
        &self.pending
    }

    /// 把待下载块按轮询唯一分配给 peer（编号 0..peer_count）。
    pub fn assign(&mut self, peer_count: usize) -> HashMap<usize, Vec<[u8; 32]>> {
        let mut sorted: Vec<[u8; 32]> = self.pending.iter().copied().collect();
        sorted.sort();
        let mut plan: HashMap<usize, Vec<[u8; 32]>> = HashMap::new();
        for (index, hash) in sorted.into_iter().enumerate() {
            let peer = index % peer_count;
            plan.entry(peer).or_default().push(hash);
        }
        self.pending.clear();
        self.assigned = plan.clone();
        plan
    }

    /// peer 失败：其未完成块回填到待下载集合。
    pub fn peer_failed(&mut self, peer: usize) {
        if let Some(chunks) = self.assigned.remove(&peer) {
            self.pending.extend(chunks);
        }
    }

    /// 块完成：从分配中移除。
    pub fn chunk_done(&mut self, hash: &[u8; 32]) {
        for chunks in self.assigned.values_mut() {
            chunks.retain(|h| h != hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hashes(count: u8) -> Vec<[u8; 32]> {
        (0..count).map(|i| [i; 32]).collect()
    }

    #[test]
    fn test_sort_by_rtt() {
        let mut peers = vec![
            ("b", Duration::from_millis(50)),
            ("a", Duration::from_millis(10)),
        ];
        sort_by_rtt(&mut peers);
        assert_eq!(peers[0].0, "a");
        assert_eq!(peers[1].0, "b");
    }

    #[test]
    fn test_assign_unique_and_distributed() {
        let mut manager = DownloadManager::new(hashes(5));
        let plan = manager.assign(2);
        let total: usize = plan.values().map(Vec::len).sum();
        assert_eq!(total, 5);
        assert!(manager.pending().is_empty());
        let mut all = Vec::new();
        for chunks in plan.values() {
            all.extend(chunks.iter().copied());
        }
        all.sort();
        assert_eq!(all, hashes(5));
    }

    #[test]
    fn test_peer_failed_backfill() {
        let mut manager = DownloadManager::new(hashes(4));
        let plan = manager.assign(2);
        let failed = plan.get(&0).unwrap().clone();
        manager.peer_failed(0);
        assert!(manager.pending().len() >= failed.len());
        for hash in failed {
            assert!(manager.pending().contains(&hash));
        }
    }

    #[test]
    fn test_chunk_done() {
        let mut manager = DownloadManager::new(hashes(3));
        let plan = manager.assign(1);
        let hash = plan.get(&0).unwrap()[0];
        manager.chunk_done(&hash);
        assert!(!manager.assigned.get(&0).unwrap().contains(&hash));
    }

    #[test]
    fn test_assign_empty() {
        let mut manager = DownloadManager::new(vec![]);
        assert!(manager.assign(2).is_empty());
    }
}
