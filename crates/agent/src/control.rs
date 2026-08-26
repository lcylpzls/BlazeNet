//! 控制面客户端：注册、心跳、任务流订阅。
use std::time::Duration;

use anyhow::{Context, Result};
use blaze_proto::control::control_client::ControlClient;
use blaze_proto::control::{Addr, HeartbeatRequest, RegisterRequest, Task, TaskFilter, task_event};
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tonic::transport::Channel;

pub async fn connect(addr: &str) -> Result<ControlClient<Channel>> {
    for _ in 0..30 {
        if let Ok(client) = ControlClient::connect(addr.to_string()).await {
            return Ok(client);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("连接调度中心失败: {addr}")
}

pub async fn register(
    client: &mut ControlClient<Channel>,
    node_type: &str,
    endpoint_id: &str,
    addrs: Vec<Addr>,
) -> Result<blaze_proto::control::RegisterReply> {
    client
        .register(RegisterRequest {
            node_type: node_type.to_string(),
            endpoint_id: endpoint_id.to_string(),
            addrs,
            token: String::new(),
        })
        .await
        .map(|reply| reply.into_inner())
        .context("注册失败")
}

/// 从事件中提取任务（忽略取消等其他事件）。
pub fn task_from_event(ev: Option<task_event::Ev>) -> Option<Task> {
    match ev {
        Some(task_event::Ev::Task(task)) => Some(task),
        Some(task_event::Ev::Cancel(_)) => None,
        None => None,
    }
}

/// 心跳循环：每 25 秒上报一次，收到关闭信号退出。
pub async fn heartbeat_loop(
    client: ControlClient<Channel>,
    node_id: u64,
    interval: Duration,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut client = client;
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = tokio::time::sleep(interval) => {
                let _ = client
                    .heartbeat(HeartbeatRequest {
                        node_id,
                        summary: "ok".to_string(),
                    })
                    .await;
            }
        }
    }
}

/// 任务流订阅循环：断线自动重连。
pub async fn watch_loop(
    client: ControlClient<Channel>,
    node_id: u64,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut client = client;
    loop {
        let mut stream = {
            tokio::select! {
                _ = &mut shutdown => return,
                result = client.watch_tasks(TaskFilter { node_id }) => {
                    match result {
                        Ok(reply) => reply.into_inner(),
                        Err(_) => {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                    }
                }
            }
        };
        loop {
            tokio::select! {
                _ = &mut shutdown => return,
                event = stream.next() => {
                    let Some(Ok(event)) = event else { break };
                    if let Some(task) = task_from_event(event.ev) {
                        println!(
                            "收到任务: ID {}，游戏 {}，版本 {}",
                            task.id, task.game_id, task.version
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scheduler::db::Store;
    use scheduler::server::{ControlService, serve};
    use std::fs;

    async fn setup(
        dir: &std::path::Path,
    ) -> (String, ControlService, scheduler::server::ServerHandle) {
        let store = Store::open(dir).unwrap();
        let service = ControlService::new(std::sync::Arc::new(store));
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let svc = service.clone();
        let handle = serve(addr, svc).await.unwrap();
        (format!("http://{addr}"), service, handle)
    }

    #[tokio::test]
    async fn test_register_heartbeat_watch() {
        let dir = std::env::temp_dir().join("blaze-ctl");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (url, service, _handle) = setup(&dir).await;
        let mut client = connect(&url).await.unwrap();
        let reply = register(&mut client, "idc", "ep-1", vec![]).await.unwrap();
        assert_eq!(reply.node_id, 1);
        let (tx, rx) = oneshot::channel();
        let hb = tokio::spawn(heartbeat_loop(
            client.clone(),
            1,
            Duration::from_millis(100),
            rx,
        ));
        let (tx2, rx2) = oneshot::channel();
        let watch = tokio::spawn(watch_loop(client, 1, rx2));
        tokio::time::sleep(Duration::from_millis(200)).await;
        service
            .push_task(scheduler::db::TaskRecord {
                id: 1,
                node_id: 1,
                game_id: 9,
                version: 2,
                kind: "UPDATE".to_string(),
                assigned_chunks: vec![],
                status: "queued".to_string(),
                error: String::new(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(700)).await;
        let _ = tx.send(());
        let _ = tx2.send(());
        let _ = hb.await;
        let _ = watch.await;
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_connect_failure() {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let err = connect(&format!("http://{addr}")).await.unwrap_err();
        assert!(err.to_string().contains("连接调度中心失败"));
    }

    #[tokio::test]
    async fn test_watch_reconnect() {
        let dir = std::env::temp_dir().join("blaze-ctl-reconnect");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (url, _service, handle) = setup(&dir).await;
        let mut client = connect(&url).await.unwrap();
        let reply = register(&mut client, "idc", "ep", vec![]).await.unwrap();
        drop(handle);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (tx, rx) = oneshot::channel();
        let watch = tokio::spawn(watch_loop(client, reply.node_id, rx));
        tokio::time::sleep(Duration::from_millis(2500)).await;
        let _ = tx.send(());
        let _ = watch.await;
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_watch_unimplemented_retries() {
        let dir = std::env::temp_dir().join("blaze-ctl-unimpl");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let origin_dir = dir.join("origin");
        fs::create_dir_all(&origin_dir).unwrap();
        let service = origin::server::UploadService::new(origin_dir);
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let handle = origin::server::serve(addr, service).await.unwrap();
        let client = connect(&format!("http://{addr}")).await.unwrap();
        let (tx, rx) = oneshot::channel();
        let watch = tokio::spawn(watch_loop(client, 1, rx));
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = tx.send(());
        let _ = watch.await;
        drop(handle);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_task_from_event() {
        assert!(task_from_event(None).is_none());
        assert!(
            task_from_event(Some(task_event::Ev::Cancel(
                blaze_proto::control::TaskCancel {
                    task_id: 1,
                    reason: "x".to_string(),
                }
            )))
            .is_none()
        );
        let task = task_from_event(Some(task_event::Ev::Task(Task {
            id: 1,
            game_id: 2,
            version: 3,
            kind: 0,
            assigned_chunks: vec![],
        })));
        assert_eq!(task.unwrap().id, 1);
    }
}
