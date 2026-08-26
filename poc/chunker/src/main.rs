// PoC：验证 FastCDC 1MiB 分块在真实风格文件上的差异率，以及块大文件追加/压缩模型。
use blake3::Hash;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Seek, SeekFrom, Write};

const MIN: usize = 64 * 1024;
const AVG: usize = 1024 * 1024;
const MAX: usize = 4 * 1024 * 1024;

fn chunk(data: &[u8], avg: usize) -> Vec<(Hash, Vec<u8>)> {
    // fastcdc 5：v2020 StreamCDC 按 min/avg/max 切块
    fastcdc::v2020::StreamCDC::new(data, MIN, avg, MAX)
        .map(|c| {
            let c = c.unwrap();
            (blake3::hash(&c.data), c.data)
        })
        .collect()
}

fn rand_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    (0..len).map(|_| rng.random()).collect()
}

fn chunk_demo() {
    // 构造 64MiB 旧版 + 1MiB 区域覆盖 + 2MiB 追加 的新版
    let base = rand_bytes(1, 64 * 1024 * 1024);
    let mut new = base.clone();
    let patch = rand_bytes(2, 1024 * 1024);
    new[30 * 1024 * 1024..31 * 1024 * 1024].copy_from_slice(&patch);
    let tail = rand_bytes(3, 2 * 1024 * 1024);
    new.extend_from_slice(&tail);

    for avg in [AVG, 4 * 1024 * 1024] {
        let old_chunks = chunk(&base, avg);
        let new_chunks = chunk(&new, avg);
        let old_hashes: HashSet<Hash> = old_chunks.iter().map(|(h, _)| *h).collect();
        let delta_bytes: usize = new_chunks
            .iter()
            .filter(|(h, _)| !old_hashes.contains(h))
            .map(|(_, d)| d.len())
            .sum();
        println!(
            "avg={}MiB 旧块数={} 新块数={} 差异块字节={:.1}MiB 差异率={:.2}%",
            avg / 1024 / 1024,
            old_chunks.len(),
            new_chunks.len(),
            delta_bytes as f64 / 1024.0 / 1024.0,
            delta_bytes as f64 / base.len() as f64 * 100.0
        );
    }
}

struct Pack {
    path: String,
    file: File,
    /// 块哈希 -> (偏移, 长度)
    index: HashMap<Hash, (u64, u32)>,
    size: u64,
}

impl Pack {
    fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            file: File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .unwrap(),
            index: HashMap::new(),
            size: 0,
        }
    }

    fn append(&mut self, chunks: &[(Hash, Vec<u8>)]) {
        for (h, d) in chunks {
            if self.index.contains_key(h) {
                continue; // 已有块，复用
            }
            self.file.seek(SeekFrom::End(0)).unwrap();
            self.file.write_all(d).unwrap();
            self.index.insert(*h, (self.size, d.len() as u32));
            self.size += d.len() as u64;
        }
    }

    /// 压缩：保留 live 块，垃圾占比超过阈值且空间足够才执行，否则推迟。
    fn compact_if_needed(&mut self, live: &HashSet<Hash>, threshold: f64, free_space: u64) -> bool {
        let live_bytes: u64 = live.iter().map(|h| self.index[h].1 as u64).sum();
        let garbage = self.size.saturating_sub(live_bytes);
        let ratio = garbage as f64 / self.size.max(1) as f64;
        println!(
            "pack 大小={:.1}MiB 存活={:.1}MiB 垃圾占比={:.1}% 阈值={:.0}% 可用空间={:.0}MiB",
            self.size as f64 / 1024.0 / 1024.0,
            live_bytes as f64 / 1024.0 / 1024.0,
            ratio * 100.0,
            threshold * 100.0,
            free_space as f64 / 1024.0 / 1024.0
        );
        if ratio < threshold {
            println!("→ 未达阈值，不压缩");
            return false;
        }
        if free_space < live_bytes {
            println!("→ 达到阈值但空间不足，推迟压缩");
            return false;
        }
        let tmp = format!("{}.compact", self.path);
        let mut out = File::create(&tmp).unwrap();
        let mut new_index = HashMap::new();
        let mut off = 0u64;
        for h in live {
            let (o, len) = self.index[h];
            self.file.seek(SeekFrom::Start(o)).unwrap();
            let mut buf = vec![0u8; len as usize];
            use std::io::Read;
            self.file.read_exact(&mut buf).unwrap();
            out.write_all(&buf).unwrap();
            new_index.insert(*h, (off, len));
            off += len as u64;
        }
        drop(out);
        fs::rename(&tmp, &self.path).unwrap();
        self.file = File::options().read(true).write(true).open(&self.path).unwrap();
        self.index = new_index;
        self.size = off;
        println!("→ 压缩完成，pack 大小={:.1}MiB", self.size as f64 / 1024.0 / 1024.0);
        true
    }
}

fn pack_demo() {
    let dir = "/tmp/blaze-poc";
    let _ = fs::remove_dir_all(dir);
    fs::create_dir_all(dir).unwrap();
    let path = format!("{}/game.pack", dir);
    let mut pack = Pack::new(&path);

    // v1：20 个随机块（模拟约 20MiB）
    let v1: Vec<(Hash, Vec<u8>)> = (0..20)
        .map(|i| (blake3::hash(&[i as u8]), rand_bytes(i as u64 + 10, AVG)))
        .collect();
    pack.append(&v1);
    let v1_hashes: HashSet<Hash> = v1.iter().map(|(h, _)| *h).collect();

    // v2：改动 12 个位置，新块 12 个，其中 5 个是新内容，7 个复用旧块
    let v2: Vec<(Hash, Vec<u8>)> = (0..12)
        .map(|i| {
            let h = if i % 2 == 0 { v1[i].0 } else { blake3::hash(&[(100 + i) as u8]) };
            (h, rand_bytes(200 + i as u64, AVG))
        })
        .collect();
    pack.append(&v2);
    let v2_hashes: HashSet<Hash> = v2.iter().map(|(h, _)| *h).collect();
    println!("v2 后唯一块数={}", pack.index.len());

    // 保留当前 + 上一版引用，其余为垃圾
    let live: HashSet<Hash> = v1_hashes.union(&v2_hashes).copied().collect();
    assert!(!pack.compact_if_needed(&live, 0.3, 1024 * 1024 * 1024), "垃圾占比不足不应压缩");

    // v3：全部换新，v1 引用变垃圾
    let v3: Vec<(Hash, Vec<u8>)> = (0..12)
        .map(|i| (blake3::hash(&[(300 + i) as u8]), rand_bytes(300 + i as u64, AVG)))
        .collect();
    pack.append(&v3);
    let v3_hashes: HashSet<Hash> = v3.iter().map(|(h, _)| *h).collect();
    let live: HashSet<Hash> = v2_hashes.union(&v3_hashes).copied().collect();
    assert!(pack.compact_if_needed(&live, 0.3, 1024 * 1024 * 1024), "垃圾占比达标且空间足够应压缩");
    assert_eq!(pack.index.len(), live.len(), "压缩后索引应只含存活块");

    // 空间不足场景：应推迟
    let v4: Vec<(Hash, Vec<u8>)> = (0..12)
        .map(|i| (blake3::hash(&[(400 + i) as u8]), rand_bytes(400 + i as u64, AVG)))
        .collect();
    pack.append(&v4);
    let v4_hashes: HashSet<Hash> = v4.iter().map(|(h, _)| *h).collect();
    let live: HashSet<Hash> = v3_hashes.union(&v4_hashes).copied().collect();
    assert!(!pack.compact_if_needed(&live, 0.3, 1024), "空间不足应推迟压缩");
    let _ = fs::remove_dir_all(dir);
}

fn main() {
    println!("== FastCDC 差异率 ==");
    chunk_demo();
    println!("\n== 块大文件追加/压缩 ==");
    pack_demo();
    println!("\nPoC 自检通过");
}
