//! 调度中心配置：Linux 规范，唯一参数 `--config <路径>`。
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

pub const DEFAULT_BIND_ADDR: &str = "0.0.0.0:50051";
/// 心跳间隔默认 25 秒。
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 25;
/// 连续 3 次心跳判定离线（75 秒）。
pub const DEFAULT_OFFLINE_TIMEOUT_SECS: u64 = 75;
/// 保活探测周期默认 25 秒（窗口均摊）。
pub const DEFAULT_KEEPALIVE_INTERVAL_SECS: u64 = 25;
/// 连续失败默认 3 次标记不可达。
pub const DEFAULT_KEEPALIVE_FAIL_THRESHOLD: u32 = 3;
/// 默认前端静态资源目录。
pub const DEFAULT_WEB_DIR: &str = "web";
/// 默认管理员账号（生产必须修改）。
pub const DEFAULT_ADMIN_USER: &str = "admin";
pub const DEFAULT_ADMIN_PASSWORD: &str = "admin123";

fn default_bind() -> String {
    DEFAULT_BIND_ADDR.to_string()
}

fn default_interval() -> u64 {
    DEFAULT_HEARTBEAT_INTERVAL_SECS
}

fn default_timeout() -> u64 {
    DEFAULT_OFFLINE_TIMEOUT_SECS
}

fn default_keepalive_interval() -> u64 {
    DEFAULT_KEEPALIVE_INTERVAL_SECS
}

fn default_keepalive_threshold() -> u32 {
    DEFAULT_KEEPALIVE_FAIL_THRESHOLD
}

fn default_web_dir() -> String {
    DEFAULT_WEB_DIR.to_string()
}

fn default_admin_user() -> String {
    DEFAULT_ADMIN_USER.to_string()
}

fn default_admin_password() -> String {
    DEFAULT_ADMIN_PASSWORD.to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub data_dir: PathBuf,
    #[serde(default = "default_bind")]
    pub bind_addr: String,
    #[serde(default = "default_interval")]
    pub heartbeat_interval_secs: u64,
    #[serde(default = "default_timeout")]
    pub offline_timeout_secs: u64,
    #[serde(default = "default_keepalive_interval")]
    pub keepalive_interval_secs: u64,
    #[serde(default = "default_keepalive_threshold")]
    pub keepalive_fail_threshold: u32,
    #[serde(default = "default_web_dir")]
    pub web_dir: String,
    #[serde(default = "default_admin_user")]
    pub admin_user: String,
    #[serde(default = "default_admin_password")]
    pub admin_password: String,
}

impl Config {
    /// 从启动参数解析配置文件路径（有且仅有一个参数 `--config`）。
    pub fn parse_args(args: &[String]) -> Result<PathBuf> {
        if args.len() != 3 || args[1] != "--config" {
            bail!("用法: scheduler --config <配置文件路径>");
        }
        Ok(PathBuf::from(&args[2]))
    }

    /// 加载并校验配置文件。
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .context(format!("读取配置文件失败: {}", path.display()))?;
        let config: Config =
            toml::from_str(&text).context(format!("解析配置文件失败: {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    /// 解析后的监听地址。
    pub fn bind_socket_addr(&self) -> Result<SocketAddr> {
        self.bind_addr
            .parse()
            .context(format!("bind_addr 非法: {}", self.bind_addr))
    }

    fn validate(&self) -> Result<()> {
        if self.data_dir.as_os_str().is_empty() {
            bail!("data_dir 不能为空");
        }
        if self.bind_socket_addr().is_err() {
            bail!("bind_addr 非法: {}", self.bind_addr);
        }
        if self.heartbeat_interval_secs == 0 || self.offline_timeout_secs == 0 {
            bail!("心跳间隔与离线超时必须大于 0");
        }
        if self.offline_timeout_secs < self.heartbeat_interval_secs * 2 {
            bail!("离线超时至少应为心跳间隔的 2 倍");
        }
        if self.keepalive_interval_secs == 0 || self.keepalive_fail_threshold == 0 {
            bail!("保活周期与失败阈值必须大于 0");
        }
        if self.admin_user.is_empty() || self.admin_password.is_empty() {
            bail!("admin_user 与 admin_password 不能为空");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_parse_args_ok() {
        let path = Config::parse_args(&[
            "scheduler".into(),
            "--config".into(),
            "/tmp/scheduler.toml".into(),
        ])
        .unwrap();
        assert_eq!(path, PathBuf::from("/tmp/scheduler.toml"));
    }

    #[test]
    fn test_parse_args_invalid() {
        let err = Config::parse_args(&["scheduler".into()]).unwrap_err();
        assert!(err.to_string().contains("用法"));
    }

    #[test]
    fn test_load_defaults() {
        let dir = std::env::temp_dir().join("blaze-sched-cfg");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("scheduler.toml");
        fs::write(&path, format!("data_dir = \"{}\"\n", dir.display())).unwrap();
        let config = Config::load(&path).unwrap();
        assert_eq!(config.bind_addr, DEFAULT_BIND_ADDR);
        assert_eq!(
            config.heartbeat_interval_secs,
            DEFAULT_HEARTBEAT_INTERVAL_SECS
        );
        assert_eq!(config.offline_timeout_secs, DEFAULT_OFFLINE_TIMEOUT_SECS);
        assert_eq!(
            config.keepalive_interval_secs,
            DEFAULT_KEEPALIVE_INTERVAL_SECS
        );
        assert_eq!(
            config.keepalive_fail_threshold,
            DEFAULT_KEEPALIVE_FAIL_THRESHOLD
        );
        assert_eq!(config.web_dir, DEFAULT_WEB_DIR);
        assert_eq!(config.admin_user, DEFAULT_ADMIN_USER);
        assert_eq!(config.admin_password, DEFAULT_ADMIN_PASSWORD);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_timeout() {
        let dir = std::env::temp_dir().join("blaze-sched-cfg-bad");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("scheduler.toml");
        fs::write(
            &path,
            format!(
                "data_dir = \"{}\"\nheartbeat_interval_secs = 25\noffline_timeout_secs = 30\n",
                dir.display()
            ),
        )
        .unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("2 倍"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_bind() {
        let dir = std::env::temp_dir().join("blaze-sched-cfg-bind");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("scheduler.toml");
        fs::write(
            &path,
            format!("data_dir = \"{}\"\nbind_addr = \"不合法\"\n", dir.display()),
        )
        .unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("bind_addr"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_missing_file() {
        let err = Config::load(Path::new("/tmp/不存在的配置.toml")).unwrap_err();
        assert!(err.to_string().contains("读取配置文件失败"));
    }

    #[test]
    fn test_load_zero_interval() {
        let dir = std::env::temp_dir().join("blaze-sched-cfg-zero");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("scheduler.toml");
        fs::write(
            &path,
            format!(
                "data_dir = \"{}\"\nheartbeat_interval_secs = 0\n",
                dir.display()
            ),
        )
        .unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("必须大于 0"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_empty_data_dir() {
        let cfg_path = std::env::temp_dir().join("blaze-sched-cfg-empty.toml");
        fs::write(&cfg_path, "data_dir = \"\"\n").unwrap();
        let err = Config::load(&cfg_path).unwrap_err();
        assert!(err.to_string().contains("data_dir"));
        let _ = fs::remove_file(&cfg_path);
    }

    #[test]
    fn test_load_zero_keepalive() {
        let dir = std::env::temp_dir().join("blaze-sched-cfg-keep");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("scheduler.toml");
        fs::write(
            &path,
            format!(
                "data_dir = \"{}\"\nkeepalive_interval_secs = 0\n",
                dir.display()
            ),
        )
        .unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("保活"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_empty_admin() {
        let dir = std::env::temp_dir().join("blaze-sched-cfg-admin");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("scheduler.toml");
        fs::write(
            &path,
            format!("data_dir = \"{}\"\nadmin_password = \"\"\n", dir.display()),
        )
        .unwrap();
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("admin"));
        let _ = fs::remove_dir_all(&dir);
    }
}
