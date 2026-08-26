//! 调度中心 HTTP 服务：健康检查、后台登录、前端静态资源托管。
use std::path::PathBuf;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tower_http::services::ServeDir;

#[derive(Debug, Clone)]
pub struct HttpState {
    admin_user: String,
    admin_password: String,
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

fn admin_error() -> StatusCode {
    StatusCode::UNAUTHORIZED
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

/// 构建 HTTP 路由：静态资源 + 健康检查 + 登录。
pub fn router(web_dir: PathBuf, admin_user: String, admin_password: String) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/api/login", post(login))
        .fallback_service(ServeDir::new(web_dir))
        .with_state(HttpState {
            admin_user,
            admin_password,
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
        router(dir.to_path_buf(), "admin".to_string(), "secret".to_string())
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
}
