//! 网吧块源：优先读临时块，删除后回退到真实文件偏移读。
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use blaze_common::manifest::{GameIndex, HASH_LEN};
use origin::storage::ChunkSource;

use crate::update::hex;

/// 网吧真实文件根目录（按游戏分目录）。
pub fn game_dir(data_dir: &Path, game_id: u64) -> PathBuf {
    data_dir.join("games").join(game_id.to_string())
}

/// 网吧临时块目录（24 小时后清理）。
pub fn temp_dir(data_dir: &Path, game_id: u64) -> PathBuf {
    data_dir
        .join(".blazenet")
        .join("temp")
        .join(game_id.to_string())
}

fn manifest_dir(data_dir: &Path, game_id: u64) -> PathBuf {
    data_dir
        .join(".blazenet")
        .join("manifests")
        .join(game_id.to_string())
}

fn current_version_path(data_dir: &Path, game_id: u64) -> PathBuf {
    manifest_dir(data_dir, game_id).join("current.version")
}

/// 哈希 →（相对路径, 偏移, 长度）映射。
type ChunkMap = HashMap<[u8; HASH_LEN], (PathBuf, u64, u32)>;

/// 按版本清单建立 哈希 →（相对路径, 偏移, 长度）映射。
fn build_map(index: &GameIndex) -> ChunkMap {
    let mut map = HashMap::new();
    for file in &index.files {
        let mut offset = 0u64;
        for chunk in &file.chunks {
            map.entry(chunk.hash)
                .or_insert((PathBuf::from(&file.name), offset, chunk.len));
            offset += u64::from(chunk.len);
        }
    }
    map
}

/// 网吧块库：真实文件 + 临时块 + 版本清单。
pub struct CafeStore {
    game_id: u64,
    data_dir: PathBuf,
    game_dir: PathBuf,
    temp_dir: PathBuf,
    manifest_dir: PathBuf,
    current: RwLock<Option<Arc<ChunkMap>>>,
}

impl CafeStore {
    /// 打开（或创建）网吧块库；已有当前版本时加载映射。
    pub fn open(data_dir: &Path, game_id: u64) -> Result<Self> {
        let store = Self {
            game_id,
            data_dir: data_dir.to_path_buf(),
            game_dir: game_dir(data_dir, game_id),
            temp_dir: temp_dir(data_dir, game_id),
            manifest_dir: manifest_dir(data_dir, game_id),
            current: RwLock::new(None),
        };
        std::fs::create_dir_all(&store.game_dir).context(format!(
            "创建真实文件目录失败: {}",
            store.game_dir.display()
        ))?;
        std::fs::create_dir_all(&store.temp_dir)
            .context(format!("创建临时块目录失败: {}", store.temp_dir.display()))?;
        if let Some(bytes) = store.current_manifest_bytes()? {
            store.set_current(&bytes)?;
        }
        Ok(store)
    }

    /// 当前版本号；尚无版本返回 `None`。
    pub fn current_version(&self) -> Result<Option<u64>> {
        let path = current_version_path(&self.data_dir, self.game_id);
        if !path.is_file() {
            return Ok(None);
        }
        let text = std::fs::read_to_string(&path).context("读取当前版本号失败")?;
        Ok(Some(text.trim().parse().context("当前版本号非法")?))
    }

    /// 当前版本清单字节；尚无版本返回 `None`。
    pub fn current_manifest_bytes(&self) -> Result<Option<Vec<u8>>> {
        let Some(version) = self.current_version()? else {
            return Ok(None);
        };
        let path = self.manifest_dir.join(format!("{version}.gameindex"));
        Ok(Some(std::fs::read(&path).context("读取当前版本清单失败")?))
    }

    /// 保存新版本清单并切换当前版本。
    pub fn save_manifest(&self, version: u64, manifest: &[u8]) -> Result<()> {
        std::fs::create_dir_all(&self.manifest_dir)
            .context(format!("创建清单目录失败: {}", self.manifest_dir.display()))?;
        std::fs::write(
            self.manifest_dir.join(format!("{version}.gameindex")),
            manifest,
        )
        .context("写入版本清单失败")?;
        std::fs::write(
            current_version_path(&self.data_dir, self.game_id),
            version.to_string(),
        )
        .context("写入当前版本号失败")?;
        self.set_current(manifest)
    }

    fn set_current(&self, manifest: &[u8]) -> Result<()> {
        let index = GameIndex::decode(manifest).context("解析当前版本清单失败")?;
        let map = build_map(&index);
        *self.current.write().expect("网吧块源锁不应被污染") = Some(Arc::new(map));
        Ok(())
    }
}

impl ChunkSource for CafeStore {
    fn read_chunk(&self, hash: &[u8; HASH_LEN]) -> Result<Option<Vec<u8>>> {
        let temp = self.temp_dir.join(format!("{}.blk", hex(hash)));
        if temp.is_file() {
            return Ok(Some(std::fs::read(&temp).context("读取临时块失败")?));
        }
        let current = self.current.read().expect("网吧块源锁不应被污染");
        let Some((rel, offset, len)) = current.as_ref().and_then(|map| map.get(hash)) else {
            return Ok(None);
        };
        let path = self.game_dir.join(rel);
        let Ok(mut file) = File::open(&path) else {
            return Ok(None);
        };
        file.seek(SeekFrom::Start(*offset))
            .context("定位真实文件偏移失败")?;
        let mut buf = vec![0u8; *len as usize];
        file.read_exact(&mut buf).context("读取真实文件块失败")?;
        Ok(Some(buf))
    }

    fn contains(&self, hash: &[u8; HASH_LEN]) -> Result<bool> {
        let temp = self.temp_dir.join(format!("{}.blk", hex(hash)));
        if temp.is_file() {
            return Ok(true);
        }
        let current = self.current.read().expect("网吧块源锁不应被污染");
        let Some((rel, _, _)) = current.as_ref().and_then(|map| map.get(hash)) else {
            return Ok(false);
        };
        Ok(self.game_dir.join(rel).is_file())
    }
}

/// 清理超过 `ttl` 的临时块，返回删除数量。
pub fn cleanup_expired(data_dir: &Path, ttl: Duration, now: SystemTime) -> Result<usize> {
    let root = data_dir.join(".blazenet").join("temp");
    if !root.exists() {
        return Ok(0);
    }
    let mut removed = 0;
    for game in std::fs::read_dir(&root).context("读取临时根目录失败")? {
        let game = game.context("读取临时游戏目录失败")?;
        if !game.path().is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(game.path()).context("读取游戏临时目录失败")? {
            let entry = entry.context("读取临时块项失败")?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("blk") {
                continue;
            }
            let modified = entry
                .metadata()
                .context("读取临时块元数据失败")?
                .modified()?;
            if now.duration_since(modified).unwrap_or_default() > ttl {
                std::fs::remove_file(&path).context("删除过期临时块失败")?;
                removed += 1;
            }
        }
    }
    Ok(removed)
}

/// 周期清理过期临时块；每小时检查一次，收到关闭信号退出。
pub async fn run_cleaner(
    data_dir: PathBuf,
    ttl_hours: u64,
    interval: Duration,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    let ttl = Duration::from_secs(ttl_hours * 3600);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(interval) => {
                match cleanup_expired(&data_dir, ttl, SystemTime::now()) {
                    Ok(removed) => println!("清理过期临时块: {removed} 个"),
                    Err(err) => println!("清理临时块失败: {err:#}"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blaze_common::manifest::{ChunkMeta, FileEntry, GameIndex};
    use std::fs;

    fn hash_of(data: &[u8]) -> [u8; 32] {
        blake3::hash(data).into()
    }

    fn manifest(data: &[u8]) -> Vec<u8> {
        let hash = hash_of(data);
        let index = GameIndex::build(vec![FileEntry {
            name: "dir/a.bin".to_string(),
            file_hash: hash,
            chunks: vec![ChunkMeta {
                hash,
                len: data.len() as u32,
            }],
        }]);
        index.encode().unwrap()
    }

    #[test]
    fn test_open_without_version() {
        let dir = std::env::temp_dir().join("blaze-cafe-open");
        let _ = fs::remove_dir_all(&dir);
        let store = CafeStore::open(&dir, 1).unwrap();
        assert_eq!(store.current_version().unwrap(), None);
        assert_eq!(store.current_manifest_bytes().unwrap(), None);
        assert!(!store.contains(&[1u8; 32]).unwrap());
        assert_eq!(store.read_chunk(&[1u8; 32]).unwrap(), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_open_fails_when_game_dir_is_file() {
        let dir = std::env::temp_dir().join("blaze-cafe-openfile");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("games")).unwrap();
        fs::write(dir.join("games/1"), b"x").unwrap();
        let err = CafeStore::open(&dir, 1).err().unwrap();
        assert!(err.to_string().contains("创建真实文件目录失败"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_open_loads_existing_manifest() {
        let dir = std::env::temp_dir().join("blaze-cafe-reopen");
        let _ = fs::remove_dir_all(&dir);
        let data = b"reopen";
        let hash = hash_of(data);
        let bytes = manifest(data);
        {
            let store = CafeStore::open(&dir, 1).unwrap();
            store.save_manifest(1, &bytes).unwrap();
        }
        let store = CafeStore::open(&dir, 1).unwrap();
        assert_eq!(store.current_version().unwrap(), Some(1));
        fs::create_dir_all(game_dir(&dir, 1).join("dir")).unwrap();
        fs::write(game_dir(&dir, 1).join("dir/a.bin"), data).unwrap();
        assert!(store.contains(&hash).unwrap());
        assert_eq!(store.read_chunk(&hash).unwrap(), Some(data.to_vec()));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_save_and_read_temp_then_real_file() {
        let dir = std::env::temp_dir().join("blaze-cafe-rw");
        let _ = fs::remove_dir_all(&dir);
        let store = CafeStore::open(&dir, 1).unwrap();
        let data = b"real-file-data";
        let hash = hash_of(data);
        let bytes = manifest(data);
        store.save_manifest(1, &bytes).unwrap();
        assert_eq!(store.current_version().unwrap(), Some(1));
        assert_eq!(store.current_manifest_bytes().unwrap(), Some(bytes.clone()));

        // 真实文件尚未存在：读不到且不视为已持有。
        assert!(!store.contains(&hash).unwrap());
        assert_eq!(store.read_chunk(&hash).unwrap(), None);

        // 写入真实文件后：偏移读可命中。
        let game_dir = game_dir(&dir, 1);
        fs::create_dir_all(game_dir.join("dir")).unwrap();
        fs::write(game_dir.join("dir/a.bin"), data).unwrap();
        assert!(store.contains(&hash).unwrap());
        assert_eq!(store.read_chunk(&hash).unwrap(), Some(data.to_vec()));

        // 临时块优先于真实文件。
        let temp = temp_dir(&dir, 1);
        let temp_data = b"temp-first";
        fs::write(temp.join(format!("{}.blk", hex(&hash))), temp_data).unwrap();
        assert_eq!(store.read_chunk(&hash).unwrap(), Some(temp_data.to_vec()));
        assert!(store.contains(&hash).unwrap());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_cleanup_expired() {
        let dir = std::env::temp_dir().join("blaze-cafe-clean");
        let _ = fs::remove_dir_all(&dir);
        let temp = temp_dir(&dir, 1);
        fs::create_dir_all(&temp).unwrap();
        let old = temp.join(format!("{}.blk", hex(&[1u8; 32])));
        let fresh = temp.join(format!("{}.blk", hex(&[2u8; 32])));
        let other = temp.join("keep.txt");
        fs::write(&old, b"old").unwrap();
        fs::write(&fresh, b"fresh").unwrap();
        fs::write(&other, b"other").unwrap();
        let now = SystemTime::now();
        let old_time = now.checked_sub(Duration::from_secs(25 * 3600)).unwrap();
        let file = fs::File::open(&old).unwrap();
        file.set_times(
            std::fs::FileTimes::new()
                .set_modified(old_time)
                .set_accessed(old_time),
        )
        .unwrap();
        drop(file);
        assert_eq!(
            cleanup_expired(&dir, Duration::from_secs(24 * 3600), now).unwrap(),
            1
        );
        assert!(!old.exists());
        assert!(fresh.exists());
        assert!(other.exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_cleanup_missing_root_and_non_dir_entry() {
        let dir = std::env::temp_dir().join("blaze-cafe-clean2");
        let _ = fs::remove_dir_all(&dir);
        assert_eq!(
            cleanup_expired(&dir, Duration::from_secs(24 * 3600), SystemTime::now()).unwrap(),
            0
        );
        let root = dir.join(".blazenet/temp");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("note.txt"), b"x").unwrap();
        assert_eq!(
            cleanup_expired(&dir, Duration::from_secs(24 * 3600), SystemTime::now()).unwrap(),
            0
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_run_cleaner_error_branch() {
        let dir = std::env::temp_dir().join("blaze-cafe-cleanerr");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join(".blazenet")).unwrap();
        fs::write(dir.join(".blazenet/temp"), b"x").unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(run_cleaner(dir.clone(), 24, Duration::from_millis(50), rx));
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send(()).unwrap();
        task.await.unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_run_cleaner_removes_expired() {
        let dir = std::env::temp_dir().join("blaze-cafe-cleaner");
        let _ = fs::remove_dir_all(&dir);
        let temp = temp_dir(&dir, 1);
        fs::create_dir_all(&temp).unwrap();
        let old = temp.join(format!("{}.blk", hex(&[3u8; 32])));
        fs::write(&old, b"old").unwrap();
        let file = fs::File::open(&old).unwrap();
        let old_time = SystemTime::now()
            .checked_sub(Duration::from_secs(25 * 3600))
            .unwrap();
        file.set_times(
            std::fs::FileTimes::new()
                .set_modified(old_time)
                .set_accessed(old_time),
        )
        .unwrap();
        drop(file);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(run_cleaner(dir.clone(), 24, Duration::from_millis(50), rx));
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send(()).unwrap();
        task.await.unwrap();
        assert!(!old.exists());
        let _ = fs::remove_dir_all(&dir);
    }
}
