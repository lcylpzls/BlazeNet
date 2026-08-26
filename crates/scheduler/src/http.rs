//! 调度中心 HTTP 服务：健康检查、后台登录、账号/场所/分组管理、前端静态资源托管。
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tower_http::services::ServeDir;

use blaze_common::manifest::GameIndex;

use crate::db::{
    AddrRecord, AuditRecord, GameRecord, GroupRecord, NodeRecord, PlaceRecord, Store, TaskRecord,
    UserRecord, hash_password,
};
use crate::server::ControlService;

#[derive(Clone)]
pub struct HttpState {
    admin_user: String,
    admin_password: String,
    store: Arc<Store>,
    control: ControlService,
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Debug, Serialize)]
pub struct LoginReply {
    token: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
pub struct CreatePlaceRequest {
    name: String,
    region: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateGroupRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateGameRequest {
    name: String,
    /// 指定游戏 ID（内部/迁移用）；缺省自动分配。
    id: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct CreateTaskRequest {
    node_id: u64,
    game_id: u64,
    version: u64,
    kind: String,
}

#[derive(Debug, Deserialize)]
pub struct RollbackRequest {
    node_id: u64,
    version: u64,
}

#[derive(Debug, Deserialize)]
pub struct BindRequest {
    kind: String,
    a: u64,
    b: u64,
}

#[derive(Debug, Serialize)]
pub struct NodeView {
    id: u64,
    node_type: String,
    endpoint_id: String,
    addrs: Vec<AddrRecord>,
    status: String,
    last_heartbeat_ms: u64,
}

impl From<NodeRecord> for NodeView {
    fn from(node: NodeRecord) -> Self {
        Self {
            id: node.id,
            node_type: node.node_type,
            endpoint_id: node.endpoint_id,
            addrs: node.addrs,
            status: node.status,
            last_heartbeat_ms: node.last_heartbeat_ms,
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn admin_error() -> StatusCode {
    StatusCode::UNAUTHORIZED
}

fn store_error(_: anyhow::Error) -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}

fn bad_request() -> StatusCode {
    StatusCode::BAD_REQUEST
}

fn not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}

/// IDC 任务责任分片：清单中尚无持有者的块分配给本节点从中心拉取，
/// 已有持有者的块不分配（由该节点从 peer 拉取），保证中心出口每块一次。
fn assign_idc_chunks(
    store: &Store,
    game_id: u64,
    version: u64,
    node_type: &str,
) -> Result<Vec<Vec<u8>>, anyhow::Error> {
    if node_type != "idc" {
        return Ok(Vec::new());
    }
    let Some(manifest) = store.get_version(game_id, version)? else {
        return Ok(Vec::new());
    };
    let index = GameIndex::decode(&manifest)?;
    let mut assigned = Vec::new();
    for hash in index.chunk_set() {
        if store.chunk_holders(game_id, &hash)?.is_empty() {
            assigned.push(hash.to_vec());
        }
    }
    assigned.sort();
    Ok(assigned)
}

/// 写入审计日志（一期固定管理员身份，二期接 RBAC 会话）。
async fn audit(state: &HttpState, action: &str, detail: &str) {
    let _ = state.store.add_audit("admin", action, detail);
}

async fn healthz() -> &'static str {
    "ok"
}

/// Prometheus 文本格式基础指标（二期可观测，三期接入大盘）。
async fn metrics(State(state): State<HttpState>) -> String {
    let nodes = state.store.list_nodes().unwrap_or_default();
    let online = nodes.iter().filter(|n| n.status == "online").count();
    let tasks = state.store.list_tasks().unwrap_or_default();
    let done = tasks.iter().filter(|t| t.status == "done").count();
    let failed = tasks.iter().filter(|t| t.status == "failed").count();
    let running = tasks.iter().filter(|t| t.status == "running").count();
    let games = state.store.list_games().unwrap_or_default().len();
    let audits = state.store.list_audits().unwrap_or_default().len();
    format!(
        "# HELP blazenet_nodes_total 节点总数\n# TYPE blazenet_nodes_total gauge\nblazenet_nodes_total {}\n# HELP blazenet_nodes_online 在线节点数\n# TYPE blazenet_nodes_online gauge\nblazenet_nodes_online {}\n# HELP blazenet_tasks_total 任务总数\n# TYPE blazenet_tasks_total gauge\nblazenet_tasks_total {}\n# HELP blazenet_tasks_running 运行中任务\n# TYPE blazenet_tasks_running gauge\nblazenet_tasks_running {}\n# HELP blazenet_tasks_done 完成任务\n# TYPE blazenet_tasks_done gauge\nblazenet_tasks_done {}\n# HELP blazenet_tasks_failed 失败任务\n# TYPE blazenet_tasks_failed gauge\nblazenet_tasks_failed {}\n# HELP blazenet_games_total 游戏总数\n# TYPE blazenet_games_total gauge\nblazenet_games_total {}\n# HELP blazenet_audits_total 审计日志数\n# TYPE blazenet_audits_total gauge\nblazenet_audits_total {}\n",
        nodes.len(),
        online,
        tasks.len(),
        running,
        done,
        failed,
        games,
        audits
    )
}

async fn login(
    State(state): State<HttpState>,
    Json(request): Json<LoginRequest>,
) -> Result<Json<LoginReply>, StatusCode> {
    if request.username != state.admin_user || request.password != state.admin_password {
        return Err(admin_error());
    }
    // 一期固定开发 token；二期接入 RBAC 会话
    Ok(Json(LoginReply {
        token: "dev-token".to_string(),
    }))
}

async fn list_users(State(state): State<HttpState>) -> Result<Json<Vec<UserRecord>>, StatusCode> {
    state.store.list_users().map_err(store_error).map(Json)
}

async fn create_user(
    State(state): State<HttpState>,
    Json(request): Json<CreateUserRequest>,
) -> Result<Json<UserRecord>, StatusCode> {
    if request.username.is_empty() || request.password.is_empty() {
        return Err(bad_request());
    }
    let id = state.store.next_user_id().map_err(store_error)?;
    let salt = now_ms().to_string();
    let user = UserRecord {
        id,
        username: request.username,
        password_hash: hash_password(&request.password, &salt),
        salt,
    };
    state.store.insert_user(&user).map_err(store_error)?;
    audit(&state, "创建账号", &format!("账号 ID {}", user.id)).await;
    Ok(Json(user))
}

async fn delete_user(
    State(state): State<HttpState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, StatusCode> {
    if !state.store.delete_user(id).map_err(store_error)? {
        return Err(not_found());
    }
    audit(&state, "删除账号", &format!("账号 ID {id}")).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_places(State(state): State<HttpState>) -> Result<Json<Vec<PlaceRecord>>, StatusCode> {
    state.store.list_places().map_err(store_error).map(Json)
}

async fn create_place(
    State(state): State<HttpState>,
    Json(request): Json<CreatePlaceRequest>,
) -> Result<Json<PlaceRecord>, StatusCode> {
    if request.name.is_empty() {
        return Err(bad_request());
    }
    let place = PlaceRecord {
        id: state.store.next_place_id().map_err(store_error)?,
        name: request.name,
        region: request.region,
    };
    state.store.insert_place(&place).map_err(store_error)?;
    audit(&state, "创建场所", &format!("场所 ID {}", place.id)).await;
    Ok(Json(place))
}

async fn delete_place(
    State(state): State<HttpState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, StatusCode> {
    if !state.store.delete_place(id).map_err(store_error)? {
        return Err(not_found());
    }
    audit(&state, "删除场所", &format!("场所 ID {id}")).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_groups(State(state): State<HttpState>) -> Result<Json<Vec<GroupRecord>>, StatusCode> {
    state.store.list_groups().map_err(store_error).map(Json)
}

async fn create_group(
    State(state): State<HttpState>,
    Json(request): Json<CreateGroupRequest>,
) -> Result<Json<GroupRecord>, StatusCode> {
    if request.name.is_empty() {
        return Err(bad_request());
    }
    let group = GroupRecord {
        id: state.store.next_group_id().map_err(store_error)?,
        name: request.name,
    };
    state.store.insert_group(&group).map_err(store_error)?;
    audit(&state, "创建分组", &format!("分组 ID {}", group.id)).await;
    Ok(Json(group))
}

async fn delete_group(
    State(state): State<HttpState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, StatusCode> {
    if !state.store.delete_group(id).map_err(store_error)? {
        return Err(not_found());
    }
    audit(&state, "删除分组", &format!("分组 ID {id}")).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn bind(
    State(state): State<HttpState>,
    Json(request): Json<BindRequest>,
) -> Result<StatusCode, StatusCode> {
    if !matches!(
        request.kind.as_str(),
        "user_place" | "user_group" | "group_place"
    ) {
        return Err(bad_request());
    }
    state
        .store
        .bind(&request.kind, request.a, request.b)
        .map_err(store_error)?;
    audit(
        &state,
        "建立绑定",
        &format!("{} {}/{}", request.kind, request.a, request.b),
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

async fn user_places(
    State(state): State<HttpState>,
    Path(id): Path<u64>,
) -> Result<Json<Vec<u64>>, StatusCode> {
    state
        .store
        .places_of_user(id)
        .map_err(store_error)
        .map(Json)
}

async fn list_nodes(State(state): State<HttpState>) -> Result<Json<Vec<NodeView>>, StatusCode> {
    state
        .store
        .list_nodes()
        .map_err(store_error)
        .map(|nodes| Json(nodes.into_iter().map(NodeView::from).collect()))
}

async fn list_tasks(State(state): State<HttpState>) -> Result<Json<Vec<TaskRecord>>, StatusCode> {
    state.store.list_tasks().map_err(store_error).map(Json)
}

async fn list_games(State(state): State<HttpState>) -> Result<Json<Vec<GameRecord>>, StatusCode> {
    state.store.list_games().map_err(store_error).map(Json)
}

async fn list_audits(State(state): State<HttpState>) -> Result<Json<Vec<AuditRecord>>, StatusCode> {
    state.store.list_audits().map_err(store_error).map(Json)
}

async fn create_game(
    State(state): State<HttpState>,
    Json(request): Json<CreateGameRequest>,
) -> Result<Json<GameRecord>, StatusCode> {
    if request.name.is_empty() {
        return Err(bad_request());
    }
    let id = match request.id {
        Some(0) => return Err(bad_request()),
        Some(id) if state.store.get_game(id).map_err(store_error)?.is_some() => {
            return Err(bad_request());
        }
        Some(id) => id,
        None => state.store.next_game_id().map_err(store_error)?,
    };
    let game = GameRecord {
        id,
        name: request.name,
        status: "uploading".to_string(),
        current_version: 0,
        latest_version: 0,
    };
    state.store.insert_game(&game).map_err(store_error)?;
    audit(&state, "创建游戏", &format!("游戏 ID {}", game.id)).await;
    Ok(Json(game))
}

/// 创建并推送任务给指定节点（端到端联调用）。
async fn create_task(
    State(state): State<HttpState>,
    Json(request): Json<CreateTaskRequest>,
) -> Result<Json<TaskRecord>, StatusCode> {
    if request.kind.is_empty() {
        return Err(bad_request());
    }
    let Some(node) = state.store.get_node(request.node_id).map_err(store_error)? else {
        return Err(not_found());
    };
    let assigned_chunks = assign_idc_chunks(
        &state.store,
        request.game_id,
        request.version,
        &node.node_type,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let task = TaskRecord {
        id: state.store.next_task_id().map_err(store_error)?,
        node_id: request.node_id,
        game_id: request.game_id,
        version: request.version,
        kind: request.kind,
        assigned_chunks,
        status: "queued".to_string(),
        error: String::new(),
    };
    state
        .control
        .push_task(task.clone())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    audit(
        &state,
        "创建任务",
        &format!(
            "任务 ID {} 游戏 {} 版本 {}",
            task.id, task.game_id, task.version
        ),
    )
    .await;
    Ok(Json(task))
}

/// 回滚指定节点到历史版本：校验版本存在后推送 ROLLBACK 任务并更新游戏当前版本。
async fn rollback_game(
    State(state): State<HttpState>,
    Path(game_id): Path<u64>,
    Json(request): Json<RollbackRequest>,
) -> Result<Json<TaskRecord>, StatusCode> {
    if request.version == 0 {
        return Err(bad_request());
    }
    let Some(mut game) = state.store.get_game(game_id).map_err(store_error)? else {
        return Err(not_found());
    };
    if state
        .store
        .get_version(game_id, request.version)
        .map_err(store_error)?
        .is_none()
    {
        return Err(not_found());
    }
    if state
        .store
        .get_node(request.node_id)
        .map_err(store_error)?
        .is_none()
    {
        return Err(not_found());
    }
    let task = TaskRecord {
        id: state.store.next_task_id().map_err(store_error)?,
        node_id: request.node_id,
        game_id,
        version: request.version,
        kind: "ROLLBACK".to_string(),
        assigned_chunks: Vec::new(),
        status: "queued".to_string(),
        error: String::new(),
    };
    state
        .control
        .push_task(task.clone())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    game.current_version = request.version;
    state.store.insert_game(&game).map_err(store_error)?;
    audit(
        &state,
        "回滚游戏",
        &format!("游戏 {game_id} 回滚到版本 {}", request.version),
    )
    .await;
    Ok(Json(task))
}

/// 取消排队中的任务；运行中/已完成任务不允许取消。
async fn cancel_task(
    State(state): State<HttpState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, StatusCode> {
    let Some(task) = state
        .store
        .list_tasks()
        .map_err(store_error)?
        .into_iter()
        .find(|t| t.id == id)
    else {
        return Err(not_found());
    };
    if task.status != "queued" {
        return Err(bad_request());
    }
    state
        .store
        .update_task_status(id, "cancelled", "人工取消")
        .map_err(store_error)?;
    audit(
        &state,
        "取消任务",
        &format!("任务 ID {id} 已取消（排队中）"),
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_game(
    State(state): State<HttpState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, StatusCode> {
    if !state.store.delete_game(id).map_err(store_error)? {
        return Err(not_found());
    }
    audit(&state, "删除游戏", &format!("游戏 ID {id}")).await;
    Ok(StatusCode::NO_CONTENT)
}

/// 构建 HTTP 路由：静态资源 + 健康检查 + 登录。
pub fn router(
    web_dir: PathBuf,
    admin_user: String,
    admin_password: String,
    store: Arc<Store>,
    control: ControlService,
) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/api/login", post(login))
        .route("/api/users", get(list_users).post(create_user))
        .route("/api/users/{id}", delete(delete_user))
        .route("/api/users/{id}/places", get(user_places))
        .route("/api/places", get(list_places).post(create_place))
        .route("/api/places/{id}", delete(delete_place))
        .route("/api/groups", get(list_groups).post(create_group))
        .route("/api/groups/{id}", delete(delete_group))
        .route("/api/nodes", get(list_nodes))
        .route("/api/tasks", get(list_tasks).post(create_task))
        .route("/api/tasks/{id}/cancel", post(cancel_task))
        .route("/api/games", get(list_games).post(create_game))
        .route("/api/audit", get(list_audits))
        .route("/api/games/{id}", delete(delete_game))
        .route("/api/games/{id}/rollback", post(rollback_game))
        .route("/api/bindings", post(bind))
        .fallback_service(ServeDir::new(web_dir))
        .with_state(HttpState {
            admin_user,
            admin_password,
            store,
            control,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::fs;
    use tower::ServiceExt;

    fn app_with_store(dir: &std::path::Path) -> (Router, Arc<Store>) {
        let store = Arc::new(Store::open(&dir.join("data")).unwrap());
        let app = router(
            dir.to_path_buf(),
            "admin".to_string(),
            "secret".to_string(),
            store.clone(),
            ControlService::new(store.clone()),
        );
        (app, store)
    }

    fn app(dir: &std::path::Path) -> Router {
        app_with_store(dir).0
    }

    #[tokio::test]
    async fn test_healthz() {
        let dir = std::env::temp_dir().join("blaze-http-health");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let response = app(&dir)
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let dir = std::env::temp_dir().join("blaze-http-metrics");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (app, store) = app_with_store(&dir);
        store
            .insert_node(&NodeRecord {
                id: 1,
                node_type: "idc".to_string(),
                endpoint_id: "ep".to_string(),
                token: "tok".to_string(),
                addrs: vec![],
                status: "online".to_string(),
                last_heartbeat_ms: 1,
            })
            .unwrap();
        store
            .insert_task(&TaskRecord {
                id: 1,
                node_id: 1,
                game_id: 1,
                version: 1,
                kind: "UPDATE".to_string(),
                assigned_chunks: vec![],
                status: "done".to_string(),
                error: String::new(),
            })
            .unwrap();
        store
            .insert_game(&GameRecord {
                id: 1,
                name: "G".to_string(),
                status: "ready".to_string(),
                current_version: 1,
                latest_version: 1,
            })
            .unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        for key in [
            "blazenet_nodes_total 1",
            "blazenet_nodes_online 1",
            "blazenet_tasks_total 1",
            "blazenet_tasks_done 1",
            "blazenet_games_total 1",
        ] {
            assert!(text.contains(key), "缺少指标: {key}");
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_cancel_queued_task() {
        let dir = std::env::temp_dir().join("blaze-http-cancel");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (app, store) = app_with_store(&dir);
        store
            .insert_task(&TaskRecord {
                id: 1,
                node_id: 1,
                game_id: 1,
                version: 1,
                kind: "UPDATE".to_string(),
                assigned_chunks: vec![],
                status: "queued".to_string(),
                error: String::new(),
            })
            .unwrap();
        let ok = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks/1/cancel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::NO_CONTENT);
        let tasks = store.list_tasks().unwrap();
        assert_eq!(tasks[0].status, "cancelled");
        let again = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks/1/cancel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(again.status(), StatusCode::BAD_REQUEST);
        let missing = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks/99/cancel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_login_ok_and_wrong() {
        let dir = std::env::temp_dir().join("blaze-http-login");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let app = app(&dir);
        let ok = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/login")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"username":"admin","password":"secret"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
        let wrong = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/login")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"username":"admin","password":"bad"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_static_index() {
        let dir = std::env::temp_dir().join("blaze-http-static");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("index.html"), "<html>后台</html>").unwrap();
        let response = app(&dir)
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_users_crud() {
        let dir = std::env::temp_dir().join("blaze-http-users");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let app = app(&dir);
        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/users")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"username":"alice","password":"pw"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::OK);
        let listed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/users")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);
        let deleted = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/users/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
        let missing = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/users/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let bad = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/users")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"username":"","password":""}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_places_groups_and_bindings() {
        let dir = std::env::temp_dir().join("blaze-http-bind");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let app = app(&dir);
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/places")
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"name":"网吧A","region":"上海"}"#))
                        .unwrap(),
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/places")
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"name":"","region":""}"#))
                        .unwrap(),
                )
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/groups")
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"name":"华东组"}"#))
                        .unwrap(),
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/groups")
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"name":""}"#))
                        .unwrap(),
                )
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/places")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/groups")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        for (kind, a, b) in [
            ("user_place", 1u64, 1u64),
            ("user_group", 1, 1),
            ("group_place", 1, 2),
            ("bad", 1, 1),
        ] {
            let status = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/bindings")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(
                            r#"{{"kind":"{kind}","a":{a},"b":{b}}}"#
                        )))
                        .unwrap(),
                )
                .await
                .unwrap()
                .status();
            let expected = if kind == "bad" {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::NO_CONTENT
            };
            assert_eq!(status, expected);
        }
        let places = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/users/1/places")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(places.status(), StatusCode::OK);
        for (uri, first_status) in [
            ("/api/places/1", StatusCode::NO_CONTENT),
            ("/api/groups/1", StatusCode::NO_CONTENT),
        ] {
            let status = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("DELETE")
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status();
            assert_eq!(status, first_status);
            let missing = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("DELETE")
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status();
            assert_eq!(missing, StatusCode::NOT_FOUND);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_status_helpers() {
        assert_eq!(
            store_error(anyhow::anyhow!("x")),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(bad_request(), StatusCode::BAD_REQUEST);
        assert_eq!(not_found(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_nodes_tasks_games_api() {
        let dir = std::env::temp_dir().join("blaze-http-ngt");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let store = Arc::new(Store::open(&dir.join("data")).unwrap());
        store
            .insert_node(&crate::db::NodeRecord {
                id: 1,
                node_type: "idc".to_string(),
                endpoint_id: "ep".to_string(),
                token: "secret".to_string(),
                addrs: vec![crate::db::AddrRecord {
                    addr: "127.0.0.1:42001".to_string(),
                    kind: "config".to_string(),
                    link: "".to_string(),
                }],
                status: "online".to_string(),
                last_heartbeat_ms: 1,
            })
            .unwrap();
        store
            .insert_task(&crate::db::TaskRecord {
                id: 1,
                node_id: 1,
                game_id: 1,
                version: 1,
                kind: "UPDATE".to_string(),
                assigned_chunks: vec![],
                status: "queued".to_string(),
                error: String::new(),
            })
            .unwrap();
        let app = router(
            dir.clone(),
            "admin".to_string(),
            "secret".to_string(),
            store.clone(),
            ControlService::new(store),
        );
        for uri in ["/api/nodes", "/api/tasks", "/api/games"] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/games")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"GameX"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::OK);
        let bad = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/games")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":""}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
        // 创建并推送任务：节点存在时成功，节点不存在时 404，空类型 400。
        let created_task = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"node_id":1,"game_id":3,"version":2,"kind":"UPDATE"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created_task.status(), StatusCode::OK);
        let missing_node = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"node_id":99,"game_id":3,"version":2,"kind":"UPDATE"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_node.status(), StatusCode::NOT_FOUND);
        let empty_kind = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"node_id":1,"game_id":3,"version":2,"kind":""}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(empty_kind.status(), StatusCode::BAD_REQUEST);
        let deleted = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/games/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
        let missing = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/games/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_rollback_api() {
        let dir = std::env::temp_dir().join("blaze-http-rollback");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (app, store) = app_with_store(&dir);
        store
            .insert_game(&GameRecord {
                id: 1,
                name: "GameX".to_string(),
                status: "ready".to_string(),
                current_version: 2,
                latest_version: 2,
            })
            .unwrap();
        store.save_version(1, 1, b"v1").unwrap();
        store.save_version(1, 2, b"v2").unwrap();
        store
            .insert_node(&NodeRecord {
                id: 1,
                node_type: "cafe".to_string(),
                endpoint_id: "ep".to_string(),
                token: "secret".to_string(),
                addrs: vec![],
                status: "online".to_string(),
                last_heartbeat_ms: 1,
            })
            .unwrap();

        let ok = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/games/1/rollback")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"node_id":1,"version":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
        assert_eq!(store.get_game(1).unwrap().unwrap().current_version, 1);
        let tasks = store.list_tasks().unwrap();
        assert_eq!(tasks[0].kind, "ROLLBACK");
        assert_eq!(tasks[0].version, 1);

        let missing_version = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/games/1/rollback")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"node_id":1,"version":99}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_version.status(), StatusCode::NOT_FOUND);

        let zero_version = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/games/1/rollback")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"node_id":1,"version":0}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(zero_version.status(), StatusCode::BAD_REQUEST);

        let missing_node = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/games/1/rollback")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"node_id":99,"version":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_node.status(), StatusCode::NOT_FOUND);

        let missing_game = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/games/99/rollback")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"node_id":1,"version":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_game.status(), StatusCode::NOT_FOUND);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_audit_logged_and_listed() {
        let dir = std::env::temp_dir().join("blaze-http-audit");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let app = app(&dir);
        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/users")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"username":"alice","password":"pw"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::OK);
        let listed = app
            .oneshot(
                Request::builder()
                    .uri("/api/audit")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(listed.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("创建账号"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_create_game_with_explicit_id() {
        let dir = std::env::temp_dir().join("blaze-http-gameid");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (app, store) = app_with_store(&dir);
        let ok = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/games")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"X","id":11}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
        assert!(store.get_game(11).unwrap().is_some());
        let dup = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/games")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"Y","id":11}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(dup.status(), StatusCode::BAD_REQUEST);
        let zero = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/games")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"Z","id":0}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(zero.status(), StatusCode::BAD_REQUEST);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_create_task_assigns_idc_chunks() {
        let dir = std::env::temp_dir().join("blaze-http-assign");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let (app, store) = app_with_store(&dir);
        let h1 = [1u8; 32];
        let h2 = [2u8; 32];
        let index =
            blaze_common::manifest::GameIndex::build(vec![blaze_common::manifest::FileEntry {
                name: "a.bin".to_string(),
                file_hash: [9u8; 32],
                chunks: vec![
                    blaze_common::manifest::ChunkMeta { hash: h1, len: 4 },
                    blaze_common::manifest::ChunkMeta { hash: h2, len: 4 },
                ],
            }]);
        store.save_version(9, 1, &index.encode().unwrap()).unwrap();
        for (id, node_type) in [(1u64, "idc"), (2, "idc"), (3, "cafe")] {
            store
                .insert_node(&NodeRecord {
                    id,
                    node_type: node_type.to_string(),
                    endpoint_id: format!("ep-{id}"),
                    token: "tok".to_string(),
                    addrs: vec![],
                    status: "online".to_string(),
                    last_heartbeat_ms: 1,
                })
                .unwrap();
        }
        for node_id in [1u64, 2, 3] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/tasks")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(
                            r#"{{"node_id":{node_id},"game_id":9,"version":1,"kind":"UPDATE"}}"#
                        )))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        let tasks = store.list_tasks().unwrap();
        assert_eq!(tasks[0].assigned_chunks.len(), 2);
        // 节点 1 完成后：节点 2 只分配尚无持有者的块。
        store.record_chunk_holder(1, 9, &h1).unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"node_id":2,"game_id":9,"version":1,"kind":"UPDATE"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let tasks = store.list_tasks().unwrap();
        assert_eq!(tasks[3].assigned_chunks.len(), 1);
        assert_eq!(tasks[3].assigned_chunks[0], h2.to_vec());
        assert!(tasks[2].assigned_chunks.is_empty());
        let _ = fs::remove_dir_all(&dir);
    }
}
