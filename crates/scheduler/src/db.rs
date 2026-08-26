//! 调度中心 redb 数据层：节点、任务与 ID 分配。
use anyhow::{Context, Result};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::path::Path;

const NODES: TableDefinition<u64, String> = TableDefinition::new("nodes");
const TASKS: TableDefinition<u64, String> = TableDefinition::new("tasks");
const COUNTERS: TableDefinition<String, u64> = TableDefinition::new("counters");
const CHUNKS: TableDefinition<String, String> = TableDefinition::new("chunks");
const HEAT: TableDefinition<String, u64> = TableDefinition::new("heat");

const NEXT_NODE_ID: &str = "next_node_id";
const NEXT_TASK_ID: &str = "next_task_id";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddrRecord {
    pub addr: String,
    pub kind: String,
    pub link: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRecord {
    pub id: u64,
    pub node_type: String,
    pub endpoint_id: String,
    pub token: String,
    pub addrs: Vec<AddrRecord>,
    pub status: String,
    pub last_heartbeat_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskRecord {
    pub id: u64,
    pub node_id: u64,
    pub game_id: u64,
    pub version: u64,
    pub kind: String,
    pub assigned_chunks: Vec<Vec<u8>>,
    pub status: String,
    pub error: String,
}

fn encode<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).context("序列化记录失败")
}

fn decode<T: for<'de> Deserialize<'de>>(text: &str) -> Result<T> {
    serde_json::from_str(text).context("反序列化记录失败")
}

fn ensure_tables(db: &Database) -> Result<()> {
    let write_txn = db.begin_write().context("开始建表事务失败")?;
    write_txn.open_table(NODES).context("创建 nodes 表失败")?;
    write_txn.open_table(TASKS).context("创建 tasks 表失败")?;
    write_txn
        .open_table(COUNTERS)
        .context("创建 counters 表失败")?;
    write_txn.open_table(CHUNKS).context("创建 chunks 表失败")?;
    write_txn.open_table(HEAT).context("创建 heat 表失败")?;
    write_txn.commit().context("提交建表事务失败")?;
    Ok(())
}

fn chunk_key(game_id: u64, hash: &[u8]) -> String {
    format!(
        "{game_id:016x}{}",
        hash.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}

/// 调度中心数据存储。
pub struct Store {
    db: Database,
}

impl Store {
    pub fn open(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)
            .context(format!("创建数据目录失败: {}", data_dir.display()))?;
        let db_path = data_dir.join("scheduler.redb");
        let db =
            Database::create(&db_path).context(format!("创建数据库失败: {}", db_path.display()))?;
        ensure_tables(&db)?;
        Ok(Self { db })
    }

    fn next_id(&self, counter: &str) -> Result<u64> {
        let write_txn = self.db.begin_write().context("开始写事务失败")?;
        let result = {
            let mut table = write_txn.open_table(COUNTERS).context("打开计数表失败")?;
            let next = table
                .get(counter.to_string())
                .context("读取计数失败")?
                .map(|v| v.value() + 1)
                .unwrap_or(1);
            table
                .insert(counter.to_string(), next)
                .context("写入计数失败")?;
            next
        };
        write_txn.commit().context("提交计数事务失败")?;
        Ok(result)
    }

    pub fn next_node_id(&self) -> Result<u64> {
        self.next_id(NEXT_NODE_ID)
    }

    pub fn next_task_id(&self) -> Result<u64> {
        self.next_id(NEXT_TASK_ID)
    }

    pub fn insert_node(&self, node: &NodeRecord) -> Result<()> {
        let value = encode(node)?;
        let write_txn = self.db.begin_write().context("开始写事务失败")?;
        {
            let mut table = write_txn.open_table(NODES).context("打开 nodes 表失败")?;
            table.insert(node.id, value).context("写入节点失败")?;
        }
        write_txn.commit().context("提交节点事务失败")?;
        Ok(())
    }

    pub fn get_node(&self, id: u64) -> Result<Option<NodeRecord>> {
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let table = read_txn.open_table(NODES).context("打开 nodes 表失败")?;
        let Some(value) = table.get(id).context("查询节点失败")? else {
            return Ok(None);
        };
        Ok(Some(decode(&value.value())?))
    }

    pub fn list_nodes(&self) -> Result<Vec<NodeRecord>> {
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let table = read_txn.open_table(NODES).context("打开 nodes 表失败")?;
        let mut nodes = Vec::new();
        for item in table.iter().context("遍历节点失败")? {
            let (_, value) = item.context("读取节点项失败")?;
            nodes.push(decode(&value.value())?);
        }
        Ok(nodes)
    }

    pub fn insert_task(&self, task: &TaskRecord) -> Result<()> {
        let value = encode(task)?;
        let write_txn = self.db.begin_write().context("开始写事务失败")?;
        {
            let mut table = write_txn.open_table(TASKS).context("打开 tasks 表失败")?;
            table.insert(task.id, value).context("写入任务失败")?;
        }
        write_txn.commit().context("提交任务事务失败")?;
        Ok(())
    }

    /// 更新任务状态；任务不存在时返回 `false`。
    pub fn update_task_status(&self, id: u64, status: &str, error: &str) -> Result<bool> {
        let write_txn = self.db.begin_write().context("开始写事务失败")?;
        let result = {
            let mut table = write_txn.open_table(TASKS).context("打开 tasks 表失败")?;
            let Some(value) = table.get(id).context("查询任务失败")? else {
                return Ok(false);
            };
            let mut task: TaskRecord = decode(&value.value())?;
            drop(value);
            task.status = status.to_string();
            task.error = error.to_string();
            table.insert(id, encode(&task)?).context("更新任务失败")?;
            true
        };
        write_txn.commit().context("提交任务事务失败")?;
        Ok(result)
    }

    pub fn tasks_for_node(&self, node_id: u64) -> Result<Vec<TaskRecord>> {
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let table = read_txn.open_table(TASKS).context("打开 tasks 表失败")?;
        let mut tasks = Vec::new();
        for item in table.iter().context("遍历任务失败")? {
            let (_, value) = item.context("读取任务项失败")?;
            let task: TaskRecord = decode(&value.value())?;
            if task.node_id == node_id {
                tasks.push(task);
            }
        }
        Ok(tasks)
    }

    /// 离线节点：最近心跳早于超时阈值。
    pub fn offline_nodes(&self, now_ms: u64, timeout_ms: u64) -> Result<Vec<u64>> {
        let nodes = self.list_nodes()?;
        Ok(nodes
            .into_iter()
            .filter(|n| now_ms.saturating_sub(n.last_heartbeat_ms) > timeout_ms)
            .map(|n| n.id)
            .collect())
    }

    /// 记录节点持有某块（幂等）。
    pub fn record_chunk_holder(&self, node_id: u64, game_id: u64, hash: &[u8]) -> Result<()> {
        let key = chunk_key(game_id, hash);
        let write_txn = self.db.begin_write().context("开始写事务失败")?;
        {
            let mut table = write_txn.open_table(CHUNKS).context("打开 chunks 表失败")?;
            let mut holders: Vec<u64> = table
                .get(key.clone())
                .context("查询块账本失败")?
                .map(|v| {
                    let text = v.value();
                    serde_json::from_str(&text).unwrap_or_default()
                })
                .unwrap_or_default();
            if !holders.contains(&node_id) {
                holders.push(node_id);
                table
                    .insert(key, encode(&holders)?)
                    .context("写入块账本失败")?;
            }
        }
        write_txn.commit().context("提交块账本事务失败")?;
        Ok(())
    }

    /// 查询某块的持有节点。
    pub fn chunk_holders(&self, game_id: u64, hash: &[u8]) -> Result<Vec<u64>> {
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let table = read_txn.open_table(CHUNKS).context("打开 chunks 表失败")?;
        let Some(value) = table
            .get(chunk_key(game_id, hash))
            .context("查询块账本失败")?
        else {
            return Ok(Vec::new());
        };
        let holders: Vec<u64> = serde_json::from_str(&value.value()).context("解析块账本失败")?;
        Ok(holders)
    }

    /// 增加游戏启动次数（热度 mock 输入）。
    pub fn add_launch(&self, game_id: u64, count: u64) -> Result<u64> {
        let key = game_id.to_string();
        let write_txn = self.db.begin_write().context("开始写事务失败")?;
        let total = {
            let mut table = write_txn.open_table(HEAT).context("打开 heat 表失败")?;
            let next = table
                .get(key.clone())
                .context("读取热度失败")?
                .map(|v| v.value() + count)
                .unwrap_or(count);
            table.insert(key, next).context("写入热度失败")?;
            next
        };
        write_txn.commit().context("提交热度事务失败")?;
        Ok(total)
    }

    /// 按热度降序返回游戏 ID 列表。
    pub fn top_games(&self, limit: usize) -> Result<Vec<u64>> {
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let table = read_txn.open_table(HEAT).context("打开 heat 表失败")?;
        let mut games: Vec<(u64, u64)> = Vec::new();
        for item in table.iter().context("遍历热度失败")? {
            let (key, value) = item.context("读取热度项失败")?;
            let game_id = key.value().parse().context("解析游戏 ID 失败")?;
            games.push((game_id, value.value()));
        }
        games.sort_by_key(|a| std::cmp::Reverse(a.1));
        Ok(games.into_iter().take(limit).map(|(id, _)| id).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn store(dir: &std::path::Path) -> Store {
        Store::open(dir).unwrap()
    }

    fn node(id: u64, heartbeat: u64) -> NodeRecord {
        NodeRecord {
            id,
            node_type: "idc".to_string(),
            endpoint_id: format!("ep-{id}"),
            token: format!("tok-{id}"),
            addrs: vec![AddrRecord {
                addr: "127.0.0.1:42001".to_string(),
                kind: "config".to_string(),
                link: "".to_string(),
            }],
            status: "online".to_string(),
            last_heartbeat_ms: heartbeat,
        }
    }

    #[test]
    fn test_node_roundtrip_and_ids() {
        let dir = std::env::temp_dir().join("blaze-sched-db");
        let _ = fs::remove_dir_all(&dir);
        let s = store(&dir);
        assert_eq!(s.next_node_id().unwrap(), 1);
        assert_eq!(s.next_node_id().unwrap(), 2);
        let n = node(1, 1000);
        s.insert_node(&n).unwrap();
        assert_eq!(s.get_node(1).unwrap(), Some(n));
        assert_eq!(s.get_node(99).unwrap(), None);
        assert_eq!(s.list_nodes().unwrap().len(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_task_roundtrip() {
        let dir = std::env::temp_dir().join("blaze-sched-task");
        let _ = fs::remove_dir_all(&dir);
        let s = store(&dir);
        assert_eq!(s.next_task_id().unwrap(), 1);
        let task = TaskRecord {
            id: 1,
            node_id: 7,
            game_id: 3,
            version: 2,
            kind: "UPDATE".to_string(),
            assigned_chunks: vec![vec![1u8; 32]],
            status: "queued".to_string(),
            error: String::new(),
        };
        s.insert_task(&task).unwrap();
        assert_eq!(s.tasks_for_node(7).unwrap(), vec![task.clone()]);
        assert!(s.tasks_for_node(8).unwrap().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_update_task_status() {
        let dir = std::env::temp_dir().join("blaze-sched-task-upd");
        let _ = fs::remove_dir_all(&dir);
        let s = store(&dir);
        assert_eq!(s.next_task_id().unwrap(), 1);
        let task = TaskRecord {
            id: 1,
            node_id: 7,
            game_id: 3,
            version: 2,
            kind: "UPDATE".to_string(),
            assigned_chunks: vec![],
            status: "queued".to_string(),
            error: String::new(),
        };
        s.insert_task(&task).unwrap();
        assert!(s.update_task_status(1, "ready", "").unwrap());
        assert!(!s.update_task_status(99, "ready", "").unwrap());
        let tasks = s.tasks_for_node(7).unwrap();
        assert_eq!(tasks[0].status, "ready");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_offline_nodes() {
        let dir = std::env::temp_dir().join("blaze-sched-offline");
        let _ = fs::remove_dir_all(&dir);
        let s = store(&dir);
        s.insert_node(&node(1, 1000)).unwrap();
        s.insert_node(&node(2, 10_000)).unwrap();
        let offline = s.offline_nodes(11_000, 5_000).unwrap();
        assert_eq!(offline, vec![1]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_chunk_ledger() {
        let dir = std::env::temp_dir().join("blaze-sched-ledger");
        let _ = fs::remove_dir_all(&dir);
        let s = store(&dir);
        let hash = [7u8; 32];
        s.record_chunk_holder(1, 3, &hash).unwrap();
        s.record_chunk_holder(2, 3, &hash).unwrap();
        s.record_chunk_holder(1, 3, &hash).unwrap();
        assert_eq!(s.chunk_holders(3, &hash).unwrap(), vec![1, 2]);
        assert!(s.chunk_holders(3, &[8u8; 32]).unwrap().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_heat_top_games() {
        let dir = std::env::temp_dir().join("blaze-sched-heat");
        let _ = fs::remove_dir_all(&dir);
        let s = store(&dir);
        s.add_launch(1, 5).unwrap();
        s.add_launch(2, 9).unwrap();
        s.add_launch(1, 3).unwrap();
        assert_eq!(s.top_games(10).unwrap(), vec![2, 1]);
        assert_eq!(s.top_games(1).unwrap(), vec![2]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_open_creates_dir() {
        let dir = std::env::temp_dir().join("blaze-sched-newdir");
        let _ = fs::remove_dir_all(&dir);
        let s = store(&dir);
        assert!(dir.is_dir());
        assert!(s.next_node_id().is_ok());
        let _ = fs::remove_dir_all(&dir);
    }
}
