//! 原始节点/IDC 块库：每游戏一个 `blocks.pack` + redb 本地块索引 + 延迟压缩。
//! 设计见 docs/06-数据存储设计文档.md §3.1/§4。
use anyhow::{Context, Result};
use blaze_common::manifest::HASH_LEN;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};

const INDEX_DB_NAME: &str = "index.redb";
const PACK_FILE_NAME: &str = "blocks.pack";
const TABLE: TableDefinition<String, u64> = TableDefinition::new("chunks");
/// 索引值：高 32 位为 pack 偏移，低 32 位为块长度。
const LEN_MASK: u64 = 0xffff_ffff;

fn key(game_id: u64, hash: &[u8; HASH_LEN]) -> String {
    format!("{:016x}{}", game_id, hex(hash))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unpack(value: u64) -> (u64, u32) {
    ((value >> 32), (value & LEN_MASK) as u32)
}

fn pack(offset: u64, len: u32) -> u64 {
    (offset << 32) | u64::from(len)
}

/// 按偏移读取（Linux 用 `pread` 单次系统调用；其余平台回退 seek+read）。
#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset)
}

#[cfg(not(unix))]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::io::{Read, Seek, SeekFrom};
    let mut cloned = file.try_clone()?;
    cloned.seek(SeekFrom::Start(offset))?;
    cloned.read(buf)
}

/// 按偏移写入（Linux 用 `pwrite` 单次系统调用；其余平台回退 seek+write）。
#[cfg(unix)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.write_at(buf, offset)
}

#[cfg(not(unix))]
fn write_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    use std::io::{Seek, SeekFrom, Write};
    let mut cloned = file.try_clone()?;
    cloned.seek(SeekFrom::Start(offset))?;
    cloned.write(buf)
}

/// 按偏移精确读满缓冲区（处理短读与 EOF）。
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    while !buf.is_empty() {
        let n = read_at(file, buf, offset)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "读取块数据遇到文件末尾",
            ));
        }
        buf = &mut buf[n..];
        offset += n as u64;
    }
    Ok(())
}

/// 按偏移写满缓冲区（处理短写）；写入函数可注入，便于测试 0 长度分支。
fn write_exact_at_impl<F>(mut write: F, mut buf: &[u8], mut offset: u64) -> std::io::Result<()>
where
    F: FnMut(&[u8], u64) -> std::io::Result<usize>,
{
    while !buf.is_empty() {
        let n = write(buf, offset)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "写入失败",
            ));
        }
        buf = &buf[n..];
        offset += n as u64;
    }
    Ok(())
}

/// 按偏移写满缓冲区（Linux 用 `pwrite` 单次系统调用；其余平台回退 seek+write）。
fn write_exact_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    write_exact_at_impl(|b, o| write_at(file, b, o), buf, offset)
}

/// 块数据源抽象：IDC/原始节点用 pack 块库，网吧用临时块 + 真实文件偏移读。
pub trait ChunkSource: Send + Sync {
    /// 按哈希读取块数据；不存在返回 `None`。
    fn read_chunk(&self, hash: &[u8; HASH_LEN]) -> Result<Option<Vec<u8>>>;

    /// 判断块是否存在。
    fn contains(&self, hash: &[u8; HASH_LEN]) -> Result<bool>;
}

impl ChunkSource for GameStore {
    fn read_chunk(&self, hash: &[u8; HASH_LEN]) -> Result<Option<Vec<u8>>> {
        self.read_chunk(hash)
    }

    fn contains(&self, hash: &[u8; HASH_LEN]) -> Result<bool> {
        self.contains(hash)
    }
}

impl ChunkSource for StdMutex<GameStore> {
    fn read_chunk(&self, hash: &[u8; HASH_LEN]) -> Result<Option<Vec<u8>>> {
        let store = self.lock().expect("块库锁不应被污染");
        store.read_chunk(hash)
    }

    fn contains(&self, hash: &[u8; HASH_LEN]) -> Result<bool> {
        let store = self.lock().expect("块库锁不应被污染");
        store.contains(hash)
    }
}

impl<T: ChunkSource + ?Sized> ChunkSource for Arc<T> {
    fn read_chunk(&self, hash: &[u8; HASH_LEN]) -> Result<Option<Vec<u8>>> {
        (**self).read_chunk(hash)
    }

    fn contains(&self, hash: &[u8; HASH_LEN]) -> Result<bool> {
        (**self).contains(hash)
    }
}

/// 数据面统一的节点块源：IDC/原始节点用 pack 块库，网吧用真实文件/临时块。
pub enum NodeStore {
    Pack(Arc<StdMutex<GameStore>>),
    Cafe(Arc<dyn ChunkSource>),
}

impl NodeStore {
    /// 按哈希读取块数据；不存在返回 `None`。
    pub fn read_chunk(&self, hash: &[u8; HASH_LEN]) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Pack(store) => {
                let store = store.lock().expect("块库锁不应被污染");
                store.read_chunk(hash)
            }
            Self::Cafe(store) => store.read_chunk(hash),
        }
    }

    /// 判断块是否存在。
    pub fn contains(&self, hash: &[u8; HASH_LEN]) -> Result<bool> {
        match self {
            Self::Pack(store) => {
                let store = store.lock().expect("块库锁不应被污染");
                store.contains(hash)
            }
            Self::Cafe(store) => store.contains(hash),
        }
    }
}

/// 单个游戏的块库。
pub struct GameStore {
    game_id: u64,
    db: Database,
    pack_path: PathBuf,
    pack_file: File,
    size: u64,
}

impl GameStore {
    /// 打开（或创建）指定游戏的块库。
    pub fn open(data_dir: &Path, game_id: u64) -> Result<Self> {
        let game_dir = data_dir.join(game_id.to_string());
        std::fs::create_dir_all(&game_dir)
            .context(format!("创建游戏目录失败: {}", game_dir.display()))?;
        let db_path = game_dir.join(INDEX_DB_NAME);
        let db = Database::create(&db_path)
            .context(format!("创建本地索引失败: {}", db_path.display()))?;
        // redb 4 读事务要求表已存在：先以写事务打开（不存在则自动创建）并提交。
        {
            let write_txn = db.begin_write().context("开始建表事务失败")?;
            write_txn.open_table(TABLE).context("创建索引表失败")?;
            write_txn.commit().context("提交建表事务失败")?;
        }
        let pack_path = game_dir.join(PACK_FILE_NAME);
        let pack_file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&pack_path)
            .context(format!("打开块文件失败: {}", pack_path.display()))?;
        let size = pack_file.metadata().context("读取块文件大小失败")?.len();
        Ok(Self {
            game_id,
            db,
            pack_path,
            pack_file,
            size,
        })
    }

    /// 追加块；同哈希已存在时直接返回已有位置（去重）。
    pub fn append_chunk(&mut self, hash: &[u8; HASH_LEN], data: &[u8]) -> Result<(u64, u32)> {
        let key = key(self.game_id, hash);
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let table = read_txn.open_table(TABLE).context("打开索引表失败")?;
        if let Some(value) = table.get(&key).context("查询索引失败")? {
            let (offset, len) = unpack(value.value());
            return Ok((offset, len));
        }
        drop(read_txn);

        let offset = self.size;
        let len = data.len() as u32;
        write_exact_at(&self.pack_file, data, offset).context("追加写入块失败")?;
        self.size += data.len() as u64;

        let write_txn = self.db.begin_write().context("开始写事务失败")?;
        {
            let mut table = write_txn.open_table(TABLE).context("打开索引表失败")?;
            table
                .insert(&key, pack(offset, len))
                .context("写入索引失败")?;
        }
        write_txn.commit().context("提交索引事务失败")?;
        Ok((offset, len))
    }

    /// 批量追加块：一次只读事务查重 + 一次连续写入 + 一次索引提交（减少事务与 fsync 放大）。
    /// 返回与输入一一对应的 (偏移, 长度)；已存在的块直接复用旧位置。
    pub fn append_chunks_batch(
        &mut self,
        chunks: &[([u8; HASH_LEN], Vec<u8>)],
    ) -> Result<Vec<(u64, u32)>> {
        let mut out = Vec::with_capacity(chunks.len());
        let mut new_data: Vec<(&[u8; HASH_LEN], &[u8])> = Vec::new();
        let mut new_pos: Vec<usize> = Vec::new();
        let mut seen_new: HashMap<[u8; HASH_LEN], usize> = HashMap::new();
        let mut dup_of_new: Vec<(usize, usize)> = Vec::new();
        {
            let read_txn = self.db.begin_read().context("开始只读事务失败")?;
            let table = read_txn.open_table(TABLE).context("打开索引表失败")?;
            for (idx, (hash, data)) in chunks.iter().enumerate() {
                if let Some(first) = seen_new.get(hash) {
                    dup_of_new.push((idx, *first));
                    out.push((0, 0));
                    continue;
                }
                if let Some(value) = table
                    .get(&key(self.game_id, hash))
                    .context("查询索引失败")?
                {
                    out.push(unpack(value.value()));
                } else {
                    seen_new.insert(*hash, idx);
                    out.push((0, 0));
                    new_data.push((hash, data));
                    new_pos.push(idx);
                }
            }
        }
        let mut offset = self.size;
        for (idx, (_, data)) in new_data.iter().enumerate() {
            write_exact_at(&self.pack_file, data, offset).context("批量追加写入块失败")?;
            let len = data.len() as u32;
            out[new_pos[idx]] = (offset, len);
            offset += u64::from(len);
        }
        for (idx, first) in dup_of_new {
            out[idx] = out[first];
        }
        self.size = offset;
        if !new_data.is_empty() {
            let write_txn = self.db.begin_write().context("开始写事务失败")?;
            {
                let mut table = write_txn.open_table(TABLE).context("打开索引表失败")?;
                for ((hash, _), idx) in new_data.iter().zip(&new_pos) {
                    let (offset, len) = out[*idx];
                    table
                        .insert(&key(self.game_id, hash), pack(offset, len))
                        .context("写入索引失败")?;
                }
            }
            write_txn.commit().context("提交索引事务失败")?;
        }
        Ok(out)
    }

    /// 按哈希读取块数据；不存在返回 `None`。
    pub fn read_chunk(&self, hash: &[u8; HASH_LEN]) -> Result<Option<Vec<u8>>> {
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let table = read_txn.open_table(TABLE).context("打开索引表失败")?;
        let Some(value) = table
            .get(&key(self.game_id, hash))
            .context("查询索引失败")?
        else {
            return Ok(None);
        };
        let (offset, len) = unpack(value.value());
        drop(read_txn);
        let mut buf = vec![0u8; len as usize];
        read_exact_at(&self.pack_file, &mut buf, offset).context("读取块数据失败")?;
        Ok(Some(buf))
    }

    /// 判断块是否存在（仅查索引，不读数据）。
    pub fn contains(&self, hash: &[u8; HASH_LEN]) -> Result<bool> {
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let table = read_txn.open_table(TABLE).context("打开索引表失败")?;
        let found = table
            .get(&key(self.game_id, hash))
            .context("查询索引失败")?
            .is_some();
        Ok(found)
    }

    /// 返回全部索引项：(索引键, 偏移, 长度)。
    pub fn entries(&self) -> Result<Vec<(String, u64, u32)>> {
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let table = read_txn.open_table(TABLE).context("打开索引表失败")?;
        let mut out = Vec::new();
        for item in table.iter().context("遍历索引失败")? {
            let (key, value) = item.context("读取索引项失败")?;
            let (offset, len) = unpack(value.value());
            out.push((key.value(), offset, len));
        }
        Ok(out)
    }

    /// 延迟压缩：垃圾占比达阈值且可用空间足够时整文件重写。
    /// 返回是否执行了压缩；空间不足或未达阈值时返回 `false`。
    pub fn compact(
        &mut self,
        live: &HashSet<[u8; HASH_LEN]>,
        threshold: f64,
        available_space: u64,
    ) -> Result<bool> {
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let table = read_txn.open_table(TABLE).context("打开索引表失败")?;
        let prefix = format!("{:016x}", self.game_id);
        let mut entries = Vec::new();
        for item in table.iter().context("遍历索引失败")? {
            let (key, value) = item.context("读取索引项失败")?;
            if key.value().starts_with(&prefix) {
                entries.push((key.value(), value.value()));
            }
        }
        drop(read_txn);

        let mut live_bytes = 0u64;
        let mut live_locations = Vec::new();
        for hash in live {
            let key = key(self.game_id, hash);
            if let Some((_, value)) = entries.iter().find(|(k, _)| k.as_str() == key.as_str()) {
                let (offset, len) = unpack(*value);
                live_bytes += u64::from(len);
                live_locations.push((*hash, offset, len));
            }
        }
        if self.size == 0 {
            return Ok(false);
        }
        let garbage_ratio = 1.0 - live_bytes as f64 / self.size as f64;
        if garbage_ratio < threshold {
            return Ok(false);
        }
        if available_space < live_bytes {
            return Ok(false);
        }

        live_locations.sort();
        let tmp_path = self.pack_path.with_extension("pack.compact");
        let mut new_locations = Vec::new();
        let mut new_offset = 0u64;
        for (hash, _, len) in &live_locations {
            new_locations.push((*hash, new_offset, *len));
            new_offset += u64::from(*len);
        }
        // 三期 P3.2b：压缩重写走 compio（io_uring）顺序大块 IO。
        let ranges: Vec<(u64, u32)> = live_locations
            .iter()
            .map(|(_, offset, len)| (*offset, *len))
            .collect();
        let copied = blaze_common::compio_io::copy_ranges(&self.pack_path, &tmp_path, &ranges)
            .context("compio 重写块文件失败")?;
        Self::ensure_copy_size(copied, live_bytes)?;

        std::fs::rename(&tmp_path, &self.pack_path).context("替换块文件失败")?;
        self.pack_file = File::options()
            .read(true)
            .write(true)
            .open(&self.pack_path)
            .context("重新打开块文件失败")?;
        self.size = new_offset;

        let write_txn = self.db.begin_write().context("开始写事务失败")?;
        {
            let mut table = write_txn.open_table(TABLE).context("打开索引表失败")?;
            for (key, _) in &entries {
                // 每个游戏一个独立索引库，entries 均为本游戏键，直接清理
                table.remove(key).context("删除索引项失败")?;
            }
            for (hash, offset, len) in &new_locations {
                table
                    .insert(&key(self.game_id, hash), pack(*offset, *len))
                    .context("更新索引失败")?;
            }
        }
        write_txn.commit().context("提交压缩事务失败")?;
        Ok(true)
    }

    /// 校验压缩重写字节数与存活字节数一致。
    fn ensure_copy_size(copied: u64, expected: u64) -> Result<()> {
        if copied != expected {
            anyhow::bail!("压缩重写字节数不一致: 预期 {expected}，实际 {copied}");
        }
        Ok(())
    }

    /// 当前 pack 文件大小（字节）。
    pub fn size(&self) -> u64 {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn hash(seed: u8) -> [u8; HASH_LEN] {
        [seed; HASH_LEN]
    }

    fn store(dir: &Path, game_id: u64) -> GameStore {
        GameStore::open(dir, game_id).unwrap()
    }

    #[test]
    fn test_append_dedup_and_read() {
        let dir = std::env::temp_dir().join("blaze-store-1");
        let _ = fs::remove_dir_all(&dir);
        let mut s = store(&dir, 1);
        let h = hash(7);
        let (o1, l1) = s.append_chunk(&h, b"hello").unwrap();
        let (o2, l2) = s.append_chunk(&h, b"hello").unwrap();
        assert_eq!((o1, l1), (o2, l2));
        assert_eq!(s.read_chunk(&h).unwrap(), Some(b"hello".to_vec()));
        assert_eq!(s.read_chunk(&hash(8)).unwrap(), None);
        assert_eq!(s.size(), 5);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_batch_append_dedupe_and_read() {
        let dir = std::env::temp_dir().join("blaze-store-batch");
        let _ = fs::remove_dir_all(&dir);
        let mut s = store(&dir, 1);
        let h1 = hash(1);
        let h2 = hash(2);
        let chunks = vec![
            (h1, b"aaaa".to_vec()),
            (h2, b"bb".to_vec()),
            (h1, b"aaaa".to_vec()),
        ];
        let locs = s.append_chunks_batch(&chunks).unwrap();
        assert_eq!(locs[0], locs[2]);
        assert_eq!(locs[0].1, 4);
        assert_eq!(locs[1].1, 2);
        assert_eq!(s.read_chunk(&h1).unwrap(), Some(b"aaaa".to_vec()));
        assert_eq!(s.read_chunk(&h2).unwrap(), Some(b"bb".to_vec()));
        assert_eq!(s.size(), 6);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_batch_append_empty() {
        let dir = std::env::temp_dir().join("blaze-store-batch-empty");
        let _ = fs::remove_dir_all(&dir);
        let mut s = store(&dir, 1);
        assert!(s.append_chunks_batch(&[]).unwrap().is_empty());
        assert_eq!(s.size(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_batch_append_existing_from_db() {
        let dir = std::env::temp_dir().join("blaze-store-batch-existing");
        let _ = fs::remove_dir_all(&dir);
        let mut s = store(&dir, 1);
        let h1 = hash(1);
        let h2 = hash(2);
        s.append_chunk(&h1, b"aaaa").unwrap();
        let locs = s
            .append_chunks_batch(&[(h1, b"aaaa".to_vec()), (h2, b"bb".to_vec())])
            .unwrap();
        assert_eq!(locs[0], (0, 4));
        assert_eq!(locs[1].1, 2);
        assert_eq!(s.size(), 6);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_read_chunk_eof_after_truncate() {
        let dir = std::env::temp_dir().join("blaze-store-eof");
        let _ = fs::remove_dir_all(&dir);
        let mut s = store(&dir, 1);
        let h = hash(3);
        s.append_chunk(&h, b"hello").unwrap();
        // 截断 pack，使索引偏移越过文件末尾 → 读取返回 EOF 错误。
        let pack = dir.join("1/blocks.pack");
        let file = fs::OpenOptions::new().write(true).open(&pack).unwrap();
        file.set_len(2).unwrap();
        let err = s.read_chunk(&h).unwrap_err();
        assert!(err.to_string().contains("读取块数据失败"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_write_exact_at_zero_write() {
        let err = write_exact_at_impl(|_, _| Ok(0), b"x", 0).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WriteZero);
    }

    #[test]
    fn test_ensure_copy_size_mismatch() {
        let err = GameStore::ensure_copy_size(1, 2).unwrap_err();
        assert!(err.to_string().contains("字节数不一致"));
        GameStore::ensure_copy_size(2, 2).unwrap();
    }

    #[test]
    fn test_contains() {
        let dir = std::env::temp_dir().join("blaze-store-contains");
        let _ = fs::remove_dir_all(&dir);
        let mut s = store(&dir, 1);
        let h = hash(5);
        assert!(!s.contains(&h).unwrap());
        s.append_chunk(&h, b"data").unwrap();
        assert!(s.contains(&h).unwrap());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_entries() {
        let dir = std::env::temp_dir().join("blaze-store-entries");
        let _ = fs::remove_dir_all(&dir);
        let mut s = store(&dir, 1);
        let h1 = hash(1);
        let h2 = hash(2);
        s.append_chunk(&h1, b"aaaa").unwrap();
        s.append_chunk(&h2, b"bbbb").unwrap();
        let entries = s.entries().unwrap();
        assert_eq!(entries.len(), 2);
        for (key, offset, len) in &entries {
            assert_eq!(key.len(), 80);
            assert_eq!(*len, 4);
            assert_eq!(*offset, if key.ends_with(&hex(&h1)) { 0 } else { 4 });
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_skipped_below_threshold() {
        let dir = std::env::temp_dir().join("blaze-store-2");
        let _ = fs::remove_dir_all(&dir);
        let mut s = store(&dir, 1);
        let h1 = hash(1);
        let h2 = hash(2);
        s.append_chunk(&h1, b"aaaa").unwrap();
        s.append_chunk(&h2, b"bbbb").unwrap();
        let live = HashSet::from([h1, h2]);
        assert!(!s.compact(&live, 0.3, 1024).unwrap());
        assert_eq!(s.size(), 8);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_deferred_when_space_insufficient() {
        let dir = std::env::temp_dir().join("blaze-store-3");
        let _ = fs::remove_dir_all(&dir);
        let mut s = store(&dir, 1);
        s.append_chunk(&hash(1), b"aaaa").unwrap();
        s.append_chunk(&hash(2), b"bbbb").unwrap();
        s.append_chunk(&hash(3), b"cccc").unwrap();
        let live = HashSet::from([hash(1)]);
        assert!(!s.compact(&live, 0.3, 1).unwrap());
        assert_eq!(s.size(), 12);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_rewrites_and_keeps_only_live() {
        let dir = std::env::temp_dir().join("blaze-store-4");
        let _ = fs::remove_dir_all(&dir);
        let mut s = store(&dir, 1);
        let h1 = hash(1);
        let h2 = hash(2);
        let h3 = hash(3);
        s.append_chunk(&h1, b"aaaa").unwrap();
        s.append_chunk(&h2, b"bbbb").unwrap();
        s.append_chunk(&h3, b"cccc").unwrap();
        let live = HashSet::from([h1, h3]);
        assert!(s.compact(&live, 0.3, 1024).unwrap());
        assert_eq!(s.size(), 8);
        assert_eq!(s.read_chunk(&h1).unwrap(), Some(b"aaaa".to_vec()));
        assert_eq!(s.read_chunk(&h3).unwrap(), Some(b"cccc".to_vec()));
        assert_eq!(s.read_chunk(&h2).unwrap(), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_keeps_other_games() {
        let dir = std::env::temp_dir().join("blaze-store-5");
        let _ = fs::remove_dir_all(&dir);
        let mut g1 = store(&dir, 1);
        let mut g2 = store(&dir, 2);
        g1.append_chunk(&hash(1), b"aaaa").unwrap();
        g2.append_chunk(&hash(9), b"zzzz").unwrap();
        let live = HashSet::from([hash(1)]);
        assert!(g1.compact(&live, 0.0, 1024).unwrap());
        assert_eq!(g2.read_chunk(&hash(9)).unwrap(), Some(b"zzzz".to_vec()));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_empty_store() {
        let dir = std::env::temp_dir().join("blaze-store-empty");
        let _ = fs::remove_dir_all(&dir);
        let mut s = store(&dir, 1);
        assert!(!s.compact(&HashSet::new(), 0.0, 1024).unwrap());
        let _ = fs::remove_dir_all(&dir);
    }

    struct MockSource {
        hash: [u8; HASH_LEN],
        data: Vec<u8>,
    }

    impl ChunkSource for MockSource {
        fn read_chunk(&self, hash: &[u8; HASH_LEN]) -> Result<Option<Vec<u8>>> {
            Ok((hash == &self.hash).then(|| self.data.clone()))
        }

        fn contains(&self, hash: &[u8; HASH_LEN]) -> Result<bool> {
            Ok(hash == &self.hash)
        }
    }

    #[test]
    fn test_chunk_source_impls() {
        let dir = std::env::temp_dir().join("blaze-store-source");
        let _ = fs::remove_dir_all(&dir);
        let mut s = store(&dir, 1);
        let h = hash(4);
        s.append_chunk(&h, b"data").unwrap();

        let source = &s as &dyn ChunkSource;
        assert_eq!(source.read_chunk(&h).unwrap(), Some(b"data".to_vec()));
        assert!(source.contains(&h).unwrap());

        let mutex = StdMutex::new(s);
        let source = &mutex as &dyn ChunkSource;
        assert_eq!(source.read_chunk(&h).unwrap(), Some(b"data".to_vec()));
        assert!(source.contains(&h).unwrap());

        let arc: Arc<StdMutex<GameStore>> = Arc::new(mutex);
        let source = &arc as &dyn ChunkSource;
        assert_eq!(source.read_chunk(&h).unwrap(), Some(b"data".to_vec()));
        assert!(source.contains(&h).unwrap());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_node_store_pack_and_cafe() {
        let dir = std::env::temp_dir().join("blaze-store-node");
        let _ = fs::remove_dir_all(&dir);
        let mut s = store(&dir, 1);
        let h = hash(6);
        s.append_chunk(&h, b"pack").unwrap();
        let pack = NodeStore::Pack(Arc::new(StdMutex::new(s)));
        assert_eq!(pack.read_chunk(&h).unwrap(), Some(b"pack".to_vec()));
        assert!(pack.contains(&h).unwrap());
        assert_eq!(pack.read_chunk(&hash(7)).unwrap(), None);

        let cafe = NodeStore::Cafe(Arc::new(MockSource {
            hash: h,
            data: b"cafe".to_vec(),
        }));
        assert_eq!(cafe.read_chunk(&h).unwrap(), Some(b"cafe".to_vec()));
        assert!(cafe.contains(&h).unwrap());
        assert!(!cafe.contains(&hash(7)).unwrap());
        let _ = fs::remove_dir_all(&dir);
    }
}
