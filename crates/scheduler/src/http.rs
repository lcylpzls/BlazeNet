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

use crate::db::{GroupRecord, PlaceRecord, Store, UserRecord, hash_password};

#[derive(Clone)]
pub struct HttpState {
    admin_user: String,
    admin_password: String,
    store: Arc<Store>,
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
pub struct BindRequest {
    kind: String,
    a: u64,
    b: u64,
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

async fn healthz() -> &'static str {
    "ok"
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
    Ok(Json(user))
}

async fn delete_user(
    State(state): State<HttpState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, StatusCode> {
    if !state.store.delete_user(id).map_err(store_error)? {
        return Err(not_found());
    }
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
    Ok(Json(place))
}

async fn delete_place(
    State(state): State<HttpState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, StatusCode> {
    if !state.store.delete_place(id).map_err(store_error)? {
        return Err(not_found());
    }
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
    Ok(Json(group))
}

async fn delete_group(
    State(state): State<HttpState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, StatusCode> {
    if !state.store.delete_group(id).map_err(store_error)? {
        return Err(not_found());
    }
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

/// 构建 HTTP 路由：静态资源 + 健康检查 + 登录。
pub fn router(
    web_dir: PathBuf,
    admin_user: String,
    admin_password: String,
    store: Arc<Store>,
) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/api/login", post(login))
        .route("/api/users", get(list_users).post(create_user))
        .route("/api/users/{id}", delete(delete_user))
        .route("/api/users/{id}/places", get(user_places))
        .route("/api/places", get(list_places).post(create_place))
        .route("/api/places/{id}", delete(delete_place))
        .route("/api/groups", get(list_groups).post(create_group))
        .route("/api/groups/{id}", delete(delete_group))
        .route("/api/bindings", post(bind))
        .fallback_service(ServeDir::new(web_dir))
        .with_state(HttpState {
            admin_user,
            admin_password,
            store,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::fs;
    use tower::ServiceExt;

    fn app(dir: &std::path::Path) -> Router {
        let store = Arc::new(Store::open(&dir.join("data")).unwrap());
        router(
            dir.to_path_buf(),
            "admin".to_string(),
            "secret".to_string(),
            store,
        )
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
}
