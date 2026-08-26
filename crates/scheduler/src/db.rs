//! 调度中心 redb 数据层：节点、任务与 ID 分配。
use anyhow::{Context, Result};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;

const NODES: TableDefinition<u64, String> = TableDefinition::new("nodes");
const TASKS: TableDefinition<u64, String> = TableDefinition::new("tasks");
const COUNTERS: TableDefinition<String, u64> = TableDefinition::new("counters");
const CHUNKS: TableDefinition<String, String> = TableDefinition::new("chunks");
const HEAT: TableDefinition<String, u64> = TableDefinition::new("heat");
const USERS: TableDefinition<u64, String> = TableDefinition::new("users");
const PLACES: TableDefinition<u64, String> = TableDefinition::new("places");
const GROUPS: TableDefinition<u64, String> = TableDefinition::new("groups");
const BINDINGS: TableDefinition<String, u64> = TableDefinition::new("bindings");
const GAMES: TableDefinition<u64, String> = TableDefinition::new("games");
const VERSIONS: TableDefinition<String, String> = TableDefinition::new("versions");

const NEXT_NODE_ID: &str = "next_node_id";
const NEXT_TASK_ID: &str = "next_task_id";
const NEXT_USER_ID: &str = "next_user_id";
const NEXT_PLACE_ID: &str = "next_place_id";
const NEXT_GROUP_ID: &str = "next_group_id";
const NEXT_GAME_ID: &str = "next_game_id";

const BIND_UP: &str = "user_place";
const BIND_UG: &str = "user_group";
const BIND_GP: &str = "group_place";

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserRecord {
    pub id: u64,
    pub username: String,
    pub password_hash: String,
    pub salt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlaceRecord {
    pub id: u64,
    pub name: String,
    pub region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupRecord {
    pub id: u64,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GameRecord {
    pub id: u64,
    pub name: String,
    pub status: String,
    pub current_version: u64,
    pub latest_version: u64,
}

/// 计算密码哈希（BLAKE3(password + salt)）。
pub fn hash_password(password: &str, salt: &str) -> String {
    blake3::hash(format!("{salt}:{password}").as_bytes()).to_string()
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
    write_txn.open_table(USERS).context("创建 users 表失败")?;
    write_txn.open_table(PLACES).context("创建 places 表失败")?;
    write_txn.open_table(GROUPS).context("创建 groups 表失败")?;
    write_txn
        .open_table(BINDINGS)
        .context("创建 bindings 表失败")?;
    write_txn.open_table(GAMES).context("创建 games 表失败")?;
    write_txn
        .open_table(VERSIONS)
        .context("创建 versions 表失败")?;
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

    /// 全部任务列表。
    pub fn list_tasks(&self) -> Result<Vec<TaskRecord>> {
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let table = read_txn.open_table(TASKS).context("打开 tasks 表失败")?;
        let mut tasks = Vec::new();
        for item in table.iter().context("遍历任务失败")? {
            let (_, value) = item.context("读取任务项失败")?;
            tasks.push(decode(&value.value())?);
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

    fn insert_json(&self, table: TableDefinition<u64, String>, id: u64, value: &str) -> Result<()> {
        let write_txn = self.db.begin_write().context("开始写事务失败")?;
        {
            let mut handle = write_txn.open_table(table).context("打开表失败")?;
            handle
                .insert(id, value.to_string())
                .context("写入记录失败")?;
        }
        write_txn.commit().context("提交事务失败")?;
        Ok(())
    }

    fn get_json(&self, table: TableDefinition<u64, String>, id: u64) -> Result<Option<String>> {
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let handle = read_txn.open_table(table).context("打开表失败")?;
        Ok(handle.get(id).context("查询记录失败")?.map(|v| v.value()))
    }

    fn list_json(&self, table: TableDefinition<u64, String>) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let handle = read_txn.open_table(table).context("打开表失败")?;
        let mut values = Vec::new();
        for item in handle.iter().context("遍历记录失败")? {
            let (_, value) = item.context("读取记录失败")?;
            values.push(value.value());
        }
        Ok(values)
    }

    fn delete_json(&self, table: TableDefinition<u64, String>, id: u64) -> Result<bool> {
        let write_txn = self.db.begin_write().context("开始写事务失败")?;
        let found = {
            let mut handle = write_txn.open_table(table).context("打开表失败")?;
            handle.remove(id).context("删除记录失败")?.is_some()
        };
        write_txn.commit().context("提交事务失败")?;
        Ok(found)
    }

    pub fn next_user_id(&self) -> Result<u64> {
        self.next_id(NEXT_USER_ID)
    }

    pub fn next_place_id(&self) -> Result<u64> {
        self.next_id(NEXT_PLACE_ID)
    }

    pub fn next_group_id(&self) -> Result<u64> {
        self.next_id(NEXT_GROUP_ID)
    }

    pub fn next_game_id(&self) -> Result<u64> {
        self.next_id(NEXT_GAME_ID)
    }

    pub fn insert_user(&self, user: &UserRecord) -> Result<()> {
        self.insert_json(USERS, user.id, &encode(user)?)
    }

    pub fn get_user(&self, id: u64) -> Result<Option<UserRecord>> {
        let Some(text) = self.get_json(USERS, id)? else {
            return Ok(None);
        };
        Ok(Some(decode(&text)?))
    }

    pub fn list_users(&self) -> Result<Vec<UserRecord>> {
        self.list_json(USERS)?
            .iter()
            .map(|text| decode(text))
            .collect()
    }

    pub fn delete_user(&self, id: u64) -> Result<bool> {
        self.delete_json(USERS, id)
    }

    pub fn insert_place(&self, place: &PlaceRecord) -> Result<()> {
        self.insert_json(PLACES, place.id, &encode(place)?)
    }

    pub fn get_place(&self, id: u64) -> Result<Option<PlaceRecord>> {
        let Some(text) = self.get_json(PLACES, id)? else {
            return Ok(None);
        };
        Ok(Some(decode(&text)?))
    }

    pub fn list_places(&self) -> Result<Vec<PlaceRecord>> {
        self.list_json(PLACES)?
            .iter()
            .map(|text| decode(text))
            .collect()
    }

    pub fn delete_place(&self, id: u64) -> Result<bool> {
        self.delete_json(PLACES, id)
    }

    pub fn insert_group(&self, group: &GroupRecord) -> Result<()> {
        self.insert_json(GROUPS, group.id, &encode(group)?)
    }

    pub fn get_group(&self, id: u64) -> Result<Option<GroupRecord>> {
        let Some(text) = self.get_json(GROUPS, id)? else {
            return Ok(None);
        };
        Ok(Some(decode(&text)?))
    }

    pub fn list_groups(&self) -> Result<Vec<GroupRecord>> {
        self.list_json(GROUPS)?
            .iter()
            .map(|text| decode(text))
            .collect()
    }

    pub fn delete_group(&self, id: u64) -> Result<bool> {
        self.delete_json(GROUPS, id)
    }

    pub fn insert_game(&self, game: &GameRecord) -> Result<()> {
        self.insert_json(GAMES, game.id, &encode(game)?)
    }

    pub fn get_game(&self, id: u64) -> Result<Option<GameRecord>> {
        let Some(text) = self.get_json(GAMES, id)? else {
            return Ok(None);
        };
        Ok(Some(decode(&text)?))
    }

    pub fn list_games(&self) -> Result<Vec<GameRecord>> {
        self.list_json(GAMES)?
            .iter()
            .map(|text| decode(text))
            .collect()
    }

    pub fn delete_game(&self, id: u64) -> Result<bool> {
        self.delete_json(GAMES, id)
    }

    /// 保存版本清单（幂等覆盖）。
    pub fn save_version(&self, game_id: u64, version: u64, manifest: &[u8]) -> Result<()> {
        let key = format!("{game_id}:{version}");
        let value = encode(&manifest.to_vec())?;
        let write_txn = self.db.begin_write().context("开始写事务失败")?;
        {
            let mut table = write_txn
                .open_table(VERSIONS)
                .context("打开 versions 表失败")?;
            table.insert(key, value).context("写入版本清单失败")?;
        }
        write_txn.commit().context("提交版本事务失败")?;
        Ok(())
    }

    /// 读取版本清单，不存在返回 `None`。
    pub fn get_version(&self, game_id: u64, version: u64) -> Result<Option<Vec<u8>>> {
        let key = format!("{game_id}:{version}");
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let table = read_txn
            .open_table(VERSIONS)
            .context("打开 versions 表失败")?;
        let Some(value) = table.get(key).context("查询版本清单失败")? else {
            return Ok(None);
        };
        Ok(Some(decode(&value.value())?))
    }

    /// 建立绑定（幂等）。
    pub fn bind(&self, kind: &str, a: u64, b: u64) -> Result<()> {
        let key = format!("{kind}/{a}/{b}");
        let write_txn = self.db.begin_write().context("开始写事务失败")?;
        {
            let mut handle = write_txn.open_table(BINDINGS).context("打开绑定表失败")?;
            handle.insert(key, 1).context("写入绑定失败")?;
        }
        write_txn.commit().context("提交绑定事务失败")?;
        Ok(())
    }

    fn binding_ids(&self, kind: &str, a: u64) -> Result<Vec<u64>> {
        let prefix = format!("{kind}/{a}/");
        let read_txn = self.db.begin_read().context("开始只读事务失败")?;
        let handle = read_txn.open_table(BINDINGS).context("打开绑定表失败")?;
        let mut ids = Vec::new();
        for item in handle.iter().context("遍历绑定失败")? {
            let (key, _) = item.context("读取绑定失败")?;
            let key = key.value();
            if let Some(rest) = key.strip_prefix(&prefix)
                && let Ok(id) = rest.parse()
            {
                ids.push(id);
            }
        }
        Ok(ids)
    }

    /// 用户可见场所 = 直绑 + 所属分组绑定的场所（去重排序）。
    pub fn places_of_user(&self, user_id: u64) -> Result<Vec<u64>> {
        let mut places: HashSet<u64> = self.binding_ids(BIND_UP, user_id)?.into_iter().collect();
        for group in self.binding_ids(BIND_UG, user_id)? {
            for place in self.binding_ids(BIND_GP, group)? {
                places.insert(place);
            }
        }
        let mut result: Vec<u64> = places.into_iter().collect();
        result.sort();
        Ok(result)
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
    fn test_password_hash() {
        assert_eq!(hash_password("abc", "s1"), hash_password("abc", "s1"));
        assert_ne!(hash_password("abc", "s1"), hash_password("abc", "s2"));
    }

    #[test]
    fn test_admin_entities_roundtrip() {
        let dir = std::env::temp_dir().join("blaze-sched-admin");
        let _ = fs::remove_dir_all(&dir);
        let s = store(&dir);
        assert_eq!(s.next_user_id().unwrap(), 1);
        assert_eq!(s.next_place_id().unwrap(), 1);
        assert_eq!(s.next_group_id().unwrap(), 1);
        let user = UserRecord {
            id: 1,
            username: "u1".to_string(),
            password_hash: hash_password("p", "s"),
            salt: "s".to_string(),
        };
        let place = PlaceRecord {
            id: 1,
            name: "网吧A".to_string(),
            region: "上海".to_string(),
        };
        let group = GroupRecord {
            id: 1,
            name: "华东组".to_string(),
        };
        s.insert_user(&user).unwrap();
        s.insert_place(&place).unwrap();
        s.insert_group(&group).unwrap();
        assert_eq!(s.get_user(1).unwrap(), Some(user));
        assert_eq!(s.get_place(1).unwrap(), Some(place));
        assert_eq!(s.get_group(1).unwrap(), Some(group));
        assert_eq!(s.get_place(99).unwrap(), None);
        assert_eq!(s.get_group(99).unwrap(), None);
        assert_eq!(s.list_users().unwrap().len(), 1);
        assert_eq!(s.list_places().unwrap().len(), 1);
        assert_eq!(s.list_groups().unwrap().len(), 1);
        assert_eq!(s.get_user(99).unwrap(), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_bindings_places_of_user() {
        let dir = std::env::temp_dir().join("blaze-sched-bind");
        let _ = fs::remove_dir_all(&dir);
        let s = store(&dir);
        s.bind(BIND_UP, 1, 10).unwrap();
        s.bind(BIND_UP, 1, 11).unwrap();
        s.bind(BIND_UG, 1, 7).unwrap();
        s.bind(BIND_GP, 7, 20).unwrap();
        s.bind(BIND_GP, 7, 10).unwrap();
        assert_eq!(s.places_of_user(1).unwrap(), vec![10, 11, 20]);
        assert!(s.places_of_user(2).unwrap().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_delete_entities() {
        let dir = std::env::temp_dir().join("blaze-sched-del");
        let _ = fs::remove_dir_all(&dir);
        let s = store(&dir);
        let user = UserRecord {
            id: 1,
            username: "u".to_string(),
            password_hash: "h".to_string(),
            salt: "s".to_string(),
        };
        let place = PlaceRecord {
            id: 1,
            name: "p".to_string(),
            region: "r".to_string(),
        };
        let group = GroupRecord {
            id: 1,
            name: "g".to_string(),
        };
        s.insert_user(&user).unwrap();
        s.insert_place(&place).unwrap();
        s.insert_group(&group).unwrap();
        assert!(s.delete_user(1).unwrap());
        assert!(!s.delete_user(1).unwrap());
        assert!(s.delete_place(1).unwrap());
        assert!(!s.delete_place(1).unwrap());
        assert!(s.delete_group(1).unwrap());
        assert!(!s.delete_group(1).unwrap());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_games_roundtrip() {
        let dir = std::env::temp_dir().join("blaze-sched-game");
        let _ = fs::remove_dir_all(&dir);
        let s = store(&dir);
        assert_eq!(s.next_game_id().unwrap(), 1);
        let game = GameRecord {
            id: 1,
            name: "GameX".to_string(),
            status: "published".to_string(),
            current_version: 1,
            latest_version: 2,
        };
        s.insert_game(&game).unwrap();
        assert_eq!(s.get_game(1).unwrap(), Some(game));
        assert_eq!(s.get_game(2).unwrap(), None);
        assert_eq!(s.list_games().unwrap().len(), 1);
        assert!(s.delete_game(1).unwrap());
        assert!(!s.delete_game(1).unwrap());
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

    #[test]
    fn test_version_roundtrip() {
        let dir = std::env::temp_dir().join("blaze-sched-version");
        let _ = fs::remove_dir_all(&dir);
        let s = store(&dir);
        assert_eq!(s.get_version(1, 1).unwrap(), None);
        s.save_version(1, 1, b"manifest-v1").unwrap();
        s.save_version(1, 2, b"manifest-v2").unwrap();
        assert_eq!(s.get_version(1, 1).unwrap(), Some(b"manifest-v1".to_vec()));
        assert_eq!(s.get_version(1, 2).unwrap(), Some(b"manifest-v2".to_vec()));
        s.save_version(1, 1, b"overwrite").unwrap();
        assert_eq!(s.get_version(1, 1).unwrap(), Some(b"overwrite".to_vec()));
        let _ = fs::remove_dir_all(&dir);
    }
}
