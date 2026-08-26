//! 调度中心控制面服务：注册、心跳、任务流推送、任务上报、peer 查询、版本查询。
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use blaze_proto::control::control_server::Control;
use blaze_proto::control::{
    Addr, ChunkDone, Empty, HeartbeatReply, HeartbeatRequest, Peer, PeerList, PeerQuery,
    RegisterReply, RegisterRequest, Task, TaskEvent, TaskFilter, TaskReport, VersionInfo,
    VersionQuery, task_event,
};
use tokio::sync::{Mutex, Notify};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::db::{AddrRecord, NodeRecord, Store, TaskRecord};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn valid_node_type(node_type: &str) -> bool {
    matches!(node_type, "origin" | "idc" | "cafe")
}

fn task_kind(kind: &str) -> i32 {
    match kind {
        "DOWNLOAD" => 0,
        "UPDATE" => 1,
        "ROLLBACK" => 2,
        _ => 0,
    }
}

fn to_proto_task(task: &TaskRecord) -> Task {
    Task {
        id: task.id,
        game_id: task.game_id,
        version: task.version,
        kind: task_kind(&task.kind),
        assigned_chunks: task.assigned_chunks.clone(),
    }
}

fn bad_node_type() -> Status {
    Status::invalid_argument("node_type 必须为 origin/idc/cafe")
}

fn node_not_found() -> Status {
    Status::not_found("节点未注册")
}

fn alloc_node_id_error(err: anyhow::Error) -> Status {
    Status::internal(format!("分配节点 ID 失败: {err}"))
}

fn write_node_error(err: anyhow::Error) -> Status {
    Status::internal(format!("写入节点失败: {err}"))
}

fn query_tasks_error(err: anyhow::Error) -> Status {
    Status::internal(format!("查询任务失败: {err}"))
}

fn query_node_error(err: anyhow::Error) -> Status {
    Status::internal(format!("查询节点失败: {err}"))
}

fn update_node_error(err: anyhow::Error) -> Status {
    Status::internal(format!("更新节点失败: {err}"))
}

fn update_task_error(err: anyhow::Error) -> Status {
    Status::internal(format!("更新任务失败: {err}"))
}

fn chunk_ledger_error(err: anyhow::Error) -> Status {
    Status::internal(format!("块账本操作失败: {err}"))
}

/// 任务流推送循环：发送待推送事件，空闲时等待通知或超时。
async fn watch_loop(
    node_id: u64,
    pending: Arc<Mutex<HashMap<u64, VecDeque<TaskEvent>>>>,
    notify: Arc<Notify>,
    tx: tokio::sync::mpsc::Sender<Result<TaskEvent, Status>>,
) {
    loop {
        let events: Vec<TaskEvent> = {
            let mut map = pending.lock().await;
            let queue = map.entry(node_id).or_default();
            queue.drain(..).collect()
        };
        for event in events {
            if tx.send(Ok(event)).await.is_err() {
                return;
            }
        }
        if tokio::time::timeout(std::time::Duration::from_millis(1000), notify.notified())
            .await
            .is_err()
        {
            continue;
        }
    }
}

/// 调度中心控制面服务。
#[derive(Clone)]
pub struct ControlService {
    store: Arc<Store>,
    pending: Arc<Mutex<HashMap<u64, VecDeque<TaskEvent>>>>,
    notify: Arc<Notify>,
}

impl ControlService {
    pub fn new(store: Store) -> Self {
        Self {
            store: Arc::new(store),
            pending: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
        }
    }

    /// 给节点推送任务（内部/测试用）。
    pub async fn push_task(&self, task: TaskRecord) -> Result<(), Status> {
        self.store
            .insert_task(&task)
            .map_err(|err| Status::internal(format!("写入任务失败: {err}")))?;
        let event = TaskEvent {
            ev: Some(task_event::Ev::Task(to_proto_task(&task))),
        };
        self.pending
            .lock()
            .await
            .entry(task.node_id)
            .or_default()
            .push_back(event);
        self.notify.notify_waiters();
        Ok(())
    }
}

#[tonic::async_trait]
impl Control for ControlService {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        let req = request.into_inner();
        if !valid_node_type(&req.node_type) {
            return Err(bad_node_type());
        }
        let id = self.store.next_node_id().map_err(alloc_node_id_error)?;
        let token = format!("{}-{}", now_ms(), id);
        let node = NodeRecord {
            id,
            node_type: req.node_type,
            endpoint_id: req.endpoint_id,
            token: token.clone(),
            addrs: req
                .addrs
                .into_iter()
                .map(|a| AddrRecord {
                    addr: a.addr,
                    kind: a.kind,
                    link: a.link,
                })
                .collect(),
            status: "online".to_string(),
            last_heartbeat_ms: now_ms(),
        };
        self.store.insert_node(&node).map_err(write_node_error)?;
        let initial_tasks = self
            .store
            .tasks_for_node(id)
            .map_err(query_tasks_error)?
            .into_iter()
            .filter(|t| t.status == "queued")
            .map(|t| to_proto_task(&t))
            .collect();
        Ok(Response::new(RegisterReply {
            node_id: id,
            token,
            initial_tasks,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let req = request.into_inner();
        let mut node = self
            .store
            .get_node(req.node_id)
            .map_err(query_node_error)?
            .ok_or_else(node_not_found)?;
        node.last_heartbeat_ms = now_ms();
        node.status = "online".to_string();
        self.store.insert_node(&node).map_err(update_node_error)?;
        let task_ids = self
            .store
            .tasks_for_node(req.node_id)
            .map_err(query_tasks_error)?
            .into_iter()
            .filter(|t| t.status == "queued")
            .map(|t| t.id)
            .collect();
        Ok(Response::new(HeartbeatReply {
            server_time: now_ms(),
            task_ids,
        }))
    }

    type WatchTasksStream = ReceiverStream<Result<TaskEvent, Status>>;

    async fn watch_tasks(
        &self,
        request: Request<TaskFilter>,
    ) -> Result<Response<Self::WatchTasksStream>, Status> {
        let node_id = request.into_inner().node_id;
        let pending = self.pending.clone();
        let notify = self.notify.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(watch_loop(node_id, pending, notify, tx));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn report_task(&self, request: Request<TaskReport>) -> Result<Response<Empty>, Status> {
        let report = request.into_inner();
        self.store
            .update_task_status(report.task_id, &report.status, &report.error)
            .map_err(update_task_error)?;
        Ok(Response::new(Empty {}))
    }

    async fn report_chunk(&self, request: Request<ChunkDone>) -> Result<Response<Empty>, Status> {
        let done = request.into_inner();
        self.store
            .record_chunk_holder(done.node_id, done.game_id, &done.chunk_hash)
            .map_err(chunk_ledger_error)?;
        Ok(Response::new(Empty {}))
    }

    async fn query_peers(&self, request: Request<PeerQuery>) -> Result<Response<PeerList>, Status> {
        let query = request.into_inner();
        let holders = self
            .store
            .chunk_holders(query.game_id, &query.chunk_hash)
            .map_err(chunk_ledger_error)?;
        let limit = if query.limit == 0 {
            usize::MAX
        } else {
            query.limit as usize
        };
        let mut peers = Vec::new();
        for id in holders.into_iter().take(limit) {
            let Some(node) = self.store.get_node(id).map_err(query_node_error)? else {
                continue;
            };
            peers.push(Peer {
                node_id: node.id,
                endpoint_id: node.endpoint_id,
                addrs: node
                    .addrs
                    .into_iter()
                    .map(|a| Addr {
                        addr: a.addr,
                        kind: a.kind,
                        link: a.link,
                    })
                    .collect(),
                direct_only: node.node_type == "cafe",
            });
        }
        Ok(Response::new(PeerList { peers }))
    }

    async fn get_version(
        &self,
        _request: Request<VersionQuery>,
    ) -> Result<Response<VersionInfo>, Status> {
        Ok(Response::new(VersionInfo {
            found: false,
            version: 0,
            manifest: vec![],
        }))
    }
}

/// 控制面服务句柄：drop 时触发关闭。
#[derive(Debug)]
pub struct ServerHandle {
    #[allow(dead_code)]
    shutdown: tokio::sync::oneshot::Sender<()>,
}

/// 启动控制面服务；返回句柄，drop 句柄即停止服务。
pub async fn serve(addr: std::net::SocketAddr, service: ControlService) -> Result<ServerHandle> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let incoming = tonic::transport::server::TcpIncoming::bind(addr)?;
    tokio::spawn(async move {
        let result = tonic::transport::Server::builder()
            .add_service(blaze_proto::control::control_server::ControlServer::new(
                service,
            ))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = rx.await;
            })
            .await;
        // 服务退出时无需额外处理；错误由 tonic 记录
        let _ = result;
    });
    Ok(ServerHandle { shutdown: tx })
}

#[cfg(test)]
mod tests {
    use super::*;
    use blaze_proto::control::control_client::ControlClient;
    use blaze_proto::control::{
        Addr, ChunkDone, HeartbeatRequest, RegisterRequest, TaskFilter, TaskReport,
    };
    use std::fs;
    use std::time::Duration;
    use tokio_stream::StreamExt;

    async fn connect_retry(url: &str) -> anyhow::Result<ControlClient<tonic::transport::Channel>> {
        for _ in 0..50 {
            if let Ok(client) = ControlClient::connect(url.to_string()).await {
                return Ok(client);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::bail!("连接控制面失败: {url}");
    }

    async fn setup(
        dir: &std::path::Path,
    ) -> (
        ControlClient<tonic::transport::Channel>,
        ServerHandle,
        ControlService,
    ) {
        let store = Store::open(dir).unwrap();
        let service = ControlService::new(store);
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let handle = serve(addr, service.clone()).await.unwrap();
        (
            connect_retry(&format!("http://{addr}")).await.unwrap(),
            handle,
            service,
        )
    }

    fn task_record(id: u64, node_id: u64) -> TaskRecord {
        TaskRecord {
            id,
            node_id,
            game_id: 3,
            version: 2,
            kind: "UPDATE".to_string(),
            assigned_chunks: vec![vec![1u8; 32]],
            status: "queued".to_string(),
            error: String::new(),
        }
    }

    #[tokio::test]
    async fn test_register_heartbeat_and_watch() {
        let dir = std::env::temp_dir().join("blaze-sched-srv");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle, service) = setup(&dir).await;

        let reply = client
            .register(RegisterRequest {
                node_type: "idc".to_string(),
                endpoint_id: "ep-1".to_string(),
                addrs: vec![],
                token: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_id, 1);
        assert!(!reply.token.is_empty());

        let hb = client
            .heartbeat(HeartbeatRequest {
                node_id: 1,
                summary: "ok".to_string(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(hb.server_time > 0);

        service.push_task(task_record(1, 1)).await.unwrap();
        let mut stream = client
            .watch_tasks(TaskFilter { node_id: 1 })
            .await
            .unwrap()
            .into_inner();
        let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap();
        let event = first.unwrap().unwrap();
        let expected = task_event::Ev::Task(to_proto_task(&task_record(1, 1)));
        assert_eq!(event.ev, Some(expected));

        let err = client
            .heartbeat(HeartbeatRequest {
                node_id: 99,
                summary: "x".to_string(),
            })
            .await
            .unwrap_err();
        assert!(err.message().contains("节点未注册"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_register_invalid_type() {
        let dir = std::env::temp_dir().join("blaze-sched-type");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle, _service) = setup(&dir).await;
        let err = client
            .register(RegisterRequest {
                node_type: "other".to_string(),
                endpoint_id: "ep".to_string(),
                addrs: vec![],
                token: String::new(),
            })
            .await
            .unwrap_err();
        assert!(err.message().contains("node_type"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_report_query_and_version() {
        let dir = std::env::temp_dir().join("blaze-sched-rest");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle, service) = setup(&dir).await;
        service.push_task(task_record(1, 7)).await.unwrap();
        client
            .register(RegisterRequest {
                node_type: "idc".to_string(),
                endpoint_id: "ep-1".to_string(),
                addrs: vec![Addr {
                    addr: "127.0.0.1:42001".to_string(),
                    kind: "config".to_string(),
                    link: "".to_string(),
                }],
                token: String::new(),
            })
            .await
            .unwrap();
        client
            .register(RegisterRequest {
                node_type: "cafe".to_string(),
                endpoint_id: "ep-2".to_string(),
                addrs: vec![],
                token: String::new(),
            })
            .await
            .unwrap();
        let hash = vec![7u8; 32];
        for node_id in [1u64, 2, 99] {
            client
                .report_chunk(ChunkDone {
                    node_id,
                    game_id: 3,
                    chunk_hash: hash.clone(),
                    size: 4,
                })
                .await
                .unwrap();
        }
        client
            .report_task(TaskReport {
                node_id: 7,
                task_id: 1,
                status: "ready".to_string(),
                error: String::new(),
            })
            .await
            .unwrap();
        let peers = client
            .query_peers(blaze_proto::control::PeerQuery {
                game_id: 1,
                chunk_hash: vec![8u8; 32],
                limit: 0,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(peers.peers.is_empty());
        let peers = client
            .query_peers(blaze_proto::control::PeerQuery {
                game_id: 3,
                chunk_hash: hash.clone(),
                limit: 0,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(peers.peers.len(), 2);
        assert_eq!(peers.peers[0].addrs.len(), 1);
        assert!(peers.peers.iter().any(|p| p.direct_only));
        let peers = client
            .query_peers(blaze_proto::control::PeerQuery {
                game_id: 3,
                chunk_hash: hash,
                limit: 1,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(peers.peers.len(), 1);
        let version = client
            .get_version(blaze_proto::control::VersionQuery {
                game_id: 1,
                version: 1,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(!version.found);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_watch_empty_then_drop() {
        let dir = std::env::temp_dir().join("blaze-sched-watch");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle, _service) = setup(&dir).await;
        let stream = client
            .watch_tasks(TaskFilter { node_id: 5 })
            .await
            .unwrap()
            .into_inner();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        drop(stream);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_register_with_addrs_and_initial_tasks() {
        let dir = std::env::temp_dir().join("blaze-sched-initial");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle, service) = setup(&dir).await;
        service.push_task(task_record(1, 1)).await.unwrap();
        let reply = client
            .register(RegisterRequest {
                node_type: "cafe".to_string(),
                endpoint_id: "ep-cafe".to_string(),
                addrs: vec![Addr {
                    addr: "127.0.0.1:42001".to_string(),
                    kind: "config".to_string(),
                    link: "".to_string(),
                }],
                token: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_id, 1);
        assert_eq!(reply.initial_tasks.len(), 1);
        let node = service.store.get_node(1).unwrap().unwrap();
        assert_eq!(node.addrs.len(), 1);
        let hb = client
            .heartbeat(HeartbeatRequest {
                node_id: 1,
                summary: "ok".to_string(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(hb.task_ids, vec![1]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_watch_send_failure() {
        let dir = std::env::temp_dir().join("blaze-sched-sendfail");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (mut client, _handle, service) = setup(&dir).await;
        let stream = client
            .watch_tasks(TaskFilter { node_id: 7 })
            .await
            .unwrap()
            .into_inner();
        drop(stream);
        service.push_task(task_record(1, 7)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_watch_loop_send_failure() {
        let pending: Arc<Mutex<HashMap<u64, VecDeque<TaskEvent>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let notify = Arc::new(Notify::new());
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        drop(rx);
        pending
            .lock()
            .await
            .entry(1)
            .or_default()
            .push_back(TaskEvent { ev: None });
        let handle = tokio::spawn(watch_loop(1, pending, notify, tx));
        let done = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .is_ok();
        assert!(done);
    }

    #[tokio::test]
    async fn test_serve_shutdown() {
        let dir = std::env::temp_dir().join("blaze-sched-shutdown");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let store = Store::open(&dir).unwrap();
        let service = ControlService::new(store);
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let handle = serve(addr, service.clone()).await.unwrap();
        let _client = connect_retry(&format!("http://{addr}")).await.unwrap();
        drop(handle);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_connect_failure() {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let err = connect_retry(&format!("http://{addr}")).await.unwrap_err();
        assert!(err.to_string().contains("连接控制面失败"));
    }

    #[test]
    fn test_helpers() {
        assert!(valid_node_type("origin"));
        assert!(!valid_node_type("other"));
        assert_eq!(task_kind("UPDATE"), 1);
        assert_eq!(task_kind("ROLLBACK"), 2);
        assert_eq!(task_kind("未知"), 0);
        assert!(bad_node_type().message().contains("node_type"));
        assert!(node_not_found().message().contains("节点未注册"));
        assert!(
            alloc_node_id_error(anyhow::anyhow!("x"))
                .message()
                .contains("分配节点 ID 失败")
        );
        assert!(
            write_node_error(anyhow::anyhow!("x"))
                .message()
                .contains("写入节点失败")
        );
        assert!(
            query_tasks_error(anyhow::anyhow!("x"))
                .message()
                .contains("查询任务失败")
        );
        assert!(
            query_node_error(anyhow::anyhow!("x"))
                .message()
                .contains("查询节点失败")
        );
        assert!(
            update_node_error(anyhow::anyhow!("x"))
                .message()
                .contains("更新节点失败")
        );
        assert!(
            update_task_error(anyhow::anyhow!("x"))
                .message()
                .contains("更新任务失败")
        );
        assert!(
            chunk_ledger_error(anyhow::anyhow!("x"))
                .message()
                .contains("块账本操作失败")
        );
    }
}
