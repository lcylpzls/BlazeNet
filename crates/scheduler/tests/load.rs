//! 三期 P3.6 控制面压测：节点注册与心跳的直调/真实 gRPC 吞吐与耗时。
//! 运行：cargo test -p scheduler --test load -- --nocapture
use std::sync::Arc;
use std::time::{Duration, Instant};

use blaze_proto::control::control_client::ControlClient;
use blaze_proto::control::control_server::Control;
use blaze_proto::control::{Addr, HeartbeatRequest, RegisterRequest};
use scheduler::db::Store;
use scheduler::server::{ControlService, serve};
use tonic::Request;

/// 直调压测节点数（覆盖千级节点目标）。
const DIRECT_NODES: usize = 2000;
/// gRPC 传输压测节点数（避免 CI 超时，覆盖真实链路）。
const GRPC_NODES: usize = 500;

fn register_req(i: usize) -> RegisterRequest {
    RegisterRequest {
        node_type: "idc".to_string(),
        endpoint_id: format!("endpoint-{i}"),
        token: format!("token-{i}"),
        addrs: vec![Addr {
            addr: format!("10.0.0.{}:42001", i % 250),
            kind: "config".to_string(),
            link: String::new(),
        }],
    }
}

fn qps(count: usize, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64()
}

#[tokio::test]
async fn test_load_register_and_heartbeat_direct() {
    let dir = std::env::temp_dir().join(format!("blaze-sched-load-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(Store::open(&dir).unwrap());
    let service = ControlService::new(store.clone());

    let start = Instant::now();
    let mut ids = Vec::with_capacity(DIRECT_NODES);
    for i in 0..DIRECT_NODES {
        let reply = service
            .register(Request::new(register_req(i)))
            .await
            .unwrap()
            .into_inner();
        ids.push(reply.node_id);
    }
    let register_elapsed = start.elapsed();
    println!(
        "直调注册 {} 节点: {:.2?}（{:.0} 节点/秒）",
        DIRECT_NODES,
        register_elapsed,
        qps(DIRECT_NODES, register_elapsed)
    );

    let start = Instant::now();
    for id in &ids {
        service
            .heartbeat(Request::new(HeartbeatRequest {
                node_id: *id,
                summary: "ok".to_string(),
            }))
            .await
            .unwrap();
    }
    let heartbeat_elapsed = start.elapsed();
    println!(
        "直调心跳 {} 次: {:.2?}（{:.0} 次/秒）",
        DIRECT_NODES,
        heartbeat_elapsed,
        qps(DIRECT_NODES, heartbeat_elapsed)
    );

    assert_eq!(store.list_nodes().unwrap().len(), DIRECT_NODES);
    assert!(
        register_elapsed.as_secs_f64() < 30.0,
        "注册耗时超预期: {register_elapsed:?}"
    );
    assert!(
        heartbeat_elapsed.as_secs_f64() < 30.0,
        "心跳耗时超预期: {heartbeat_elapsed:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_load_register_and_heartbeat_grpc() {
    let dir = std::env::temp_dir().join(format!("blaze-sched-load-grpc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(Store::open(&dir).unwrap());
    let service = ControlService::new(store.clone());
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    let _handle = serve(addr, service).await.unwrap();
    let mut client = None;
    for _ in 0..50 {
        if let Ok(c) = ControlClient::connect(format!("http://{addr}")).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut client = client.expect("连接控制面失败");

    let start = Instant::now();
    let mut ids = Vec::with_capacity(GRPC_NODES);
    for i in 0..GRPC_NODES {
        let reply = client
            .register(Request::new(register_req(i)))
            .await
            .unwrap()
            .into_inner();
        ids.push(reply.node_id);
    }
    let register_elapsed = start.elapsed();
    println!(
        "gRPC 注册 {} 节点: {:.2?}（{:.0} 节点/秒）",
        GRPC_NODES,
        register_elapsed,
        qps(GRPC_NODES, register_elapsed)
    );

    let start = Instant::now();
    for id in &ids {
        client
            .heartbeat(Request::new(HeartbeatRequest {
                node_id: *id,
                summary: "ok".to_string(),
            }))
            .await
            .unwrap();
    }
    let heartbeat_elapsed = start.elapsed();
    println!(
        "gRPC 心跳 {} 次: {:.2?}（{:.0} 次/秒）",
        GRPC_NODES,
        heartbeat_elapsed,
        qps(GRPC_NODES, heartbeat_elapsed)
    );

    assert_eq!(store.list_nodes().unwrap().len(), GRPC_NODES);
    assert!(
        register_elapsed.as_secs_f64() < 60.0,
        "gRPC 注册耗时超预期: {register_elapsed:?}"
    );
    assert!(
        heartbeat_elapsed.as_secs_f64() < 60.0,
        "gRPC 心跳耗时超预期: {heartbeat_elapsed:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
