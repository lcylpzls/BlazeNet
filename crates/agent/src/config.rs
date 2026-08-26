//! agent 配置：Linux 唯一参数 `--config`；Windows 自动读取程序目录上一级 `config/` 下固定文件名。
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Windows 固定配置文件名（写死在代码中）。
pub const CONFIG_FILE_NAME: &str = "cafe-agent.toml";
/// 默认同时处理游戏数。
pub const DEFAULT_CONCURRENT_GAMES: u32 = 5;
/// 默认块并发数。
pub const DEFAULT_CHUNK_CONCURRENCY: u32 = 4;
/// 默认磁盘空闲阈值（200GB）。
pub const DEFAULT_DISK_FREE_THRESHOLD: u64 = 200 * 1024 * 1024 * 1024;
/// 默认压缩触发阈值。
pub const DEFAULT_COMPACT_THRESHOLD: f64 = 0.3;
/// 默认数据面监听端口（>10000，NAT 网关打洞约束）。
pub const DEFAULT_LISTEN_PORT: u16 = 42001;
/// 默认临时块保留时长（24 小时）。
pub const DEFAULT_TEMP_TTL_HOURS: u64 = 24;

fn default_concurrent_games() -> u32 {
    DEFAULT_CONCURRENT_GAMES
}

fn default_chunk_concurrency() -> u32 {
    DEFAULT_CHUNK_CONCURRENCY
}

fn default_disk_free_threshold() -> u64 {
    DEFAULT_DISK_FREE_THRESHOLD
}

fn default_compact_threshold() -> f64 {
    DEFAULT_COMPACT_THRESHOLD
}

fn default_listen_port() -> u16 {
    DEFAULT_LISTEN_PORT
}

fn default_temp_ttl_hours() -> u64 {
    DEFAULT_TEMP_TTL_HOURS
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeType {
    Idc,
    Cafe,
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idc => write!(f, "idc"),
            Self::Cafe => write!(f, "cafe"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub node_type: NodeType,
    /// IDC：块库根目录；网吧：真实文件根目录。
    pub data_dir: PathBuf,
    #[serde(default = "default_concurrent_games")]
    pub concurrent_games: u32,
    #[serde(default = "default_chunk_concurrency")]
    pub chunk_concurrency: u32,
    #[serde(default = "default_disk_free_threshold")]
    pub disk_free_threshold: u64,
    #[serde(default = "default_compact_threshold")]
    pub compact_threshold: f64,
    #[serde(default = "default_listen_port")]
    pub listen_port: u16,
    /// 网吧临时块保留小时数（到期清理后改读真实文件偏移）。
    #[serde(default = "default_temp_ttl_hours")]
    pub temp_ttl_hours: u64,
    /// IDC 回退源：原始节点数据面端点 ID。
    pub origin_endpoint: Option<String>,
    /// IDC 回退源：原始节点数据面地址（ip:port）。
    pub origin_addr: Option<String>,
    /// 保活 pong 应答端口（独立 UDP，须可入站），缺省不启用。
    pub keepalive_port: Option<u16>,
    /// relay 地址；缺省仅直连。
    pub relay_url: Option<String>,
    /// 对外通告的公网映射地址（如网吧映射/IDC 公网地址），缺省不通告。
    pub external_addr: Option<String>,
    /// 启动期地址探测服务地址（relay 主机的 UDP 地址回显），缺省不探测。
    pub stun_addr: Option<String>,
    /// 调度中心 gRPC 地址；缺省仅数据面。
    pub control_addr: Option<String>,
}

impl Config {
    /// 解析启动参数与平台规则，返回配置。
    /// Linux：唯一参数 `--config <路径>`；Windows：固定路径自动定位。
    pub fn resolve(args: &[String]) -> Result<Self> {
        #[cfg(windows)]
        let path = Self::default_path()?;
        #[cfg(not(windows))]
        let path = Self::parse_args(args)?;
        Self::load(&path)
    }

    /// Linux 参数解析：有且仅有一个参数 `--config`。
    pub fn parse_args(args: &[String]) -> Result<PathBuf> {
        if args.len() != 3 || args[1] != "--config" {
            bail!("用法: agent --config <配置文件路径>");
        }
        Ok(PathBuf::from(&args[2]))
    }

    /// Windows 规则：程序目录上一级目录下的 `config/` 目录中的固定文件名配置。
    pub fn default_path() -> Result<PathBuf> {
        let exe = std::env::current_exe().context("无法获取可执行文件路径")?;
        let exe_dir = exe.parent().context("可执行文件缺少父目录")?;
        let parent = exe_dir.parent().context("可执行文件缺少上一级目录")?;
        Ok(parent.join("config").join(CONFIG_FILE_NAME))
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

    fn validate(&self) -> Result<()> {
        if self.data_dir.as_os_str().is_empty() {
            bail!("data_dir 不能为空");
        }
        if self.concurrent_games == 0 || self.chunk_concurrency == 0 {
            bail!("concurrent_games 与 chunk_concurrency 必须大于 0");
        }
        if self.disk_free_threshold == 0 {
            bail!("disk_free_threshold 必须大于 0");
        }
        if !(0.0 < self.compact_threshold && self.compact_threshold <= 1.0) {
            bail!("compact_threshold 必须在 (0, 1] 之间");
        }
        if self.listen_port < 10001 {
            bail!("listen_port 必须大于 10000（NAT 网关打洞限制）");
        }
        if self.temp_ttl_hours == 0 {
            bail!("temp_ttl_hours 必须大于 0");
        }
        if (self.origin_endpoint.is_some()) != (self.origin_addr.is_some()) {
            bail!("origin_endpoint 与 origin_addr 必须同时配置或同时缺省");
        }
        if self.node_type == NodeType::Cafe
            && (self.origin_endpoint.is_some() || self.origin_addr.is_some())
        {
            bail!("网吧节点不得配置 origin_endpoint/origin_addr（网吧只从 IDC 拉取）");
        }
        if let Some(addr) = &self.origin_addr {
            addr.parse::<SocketAddr>()
                .map_err(|_| anyhow::anyhow!("origin_addr 必须是 ip:port"))?;
        }
        if let Some(port) = self.keepalive_port
            && port < 10001
        {
            bail!("keepalive_port 必须大于 10000（NAT 网关打洞限制）");
        }
        if let Some(addr) = &self.external_addr {
            addr.parse::<SocketAddr>()
                .map_err(|_| anyhow::anyhow!("external_addr 必须是 ip:port"))?;
        }
        if let Some(addr) = &self.stun_addr {
            addr.parse::<SocketAddr>()
                .map_err(|_| anyhow::anyhow!("stun_addr 必须是 ip:port"))?;
        }
        if let Some(addr) = &self.control_addr
            && !addr.starts_with("http://")
            && !addr.starts_with("https://")
        {
            bail!("control_addr 必须是 http(s):// 地址");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write_config(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("agent.toml");
        fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn test_parse_args_ok() {
        let path =
            Config::parse_args(&["agent".into(), "--config".into(), "/tmp/agent.toml".into()])
                .unwrap();
        assert_eq!(path, PathBuf::from("/tmp/agent.toml"));
    }

    #[test]
    fn test_parse_args_invalid() {
        let err = Config::parse_args(&["agent".into(), "/tmp/agent.toml".into()]).unwrap_err();
        assert!(err.to_string().contains("用法"));
    }

    #[test]
    fn test_default_path() {
        let path = Config::default_path().unwrap();
        let text = path.to_string_lossy();
        assert!(text.ends_with(&format!(
            "config{}cafe-agent.toml",
            std::path::MAIN_SEPARATOR
        )));
    }

    #[test]
    fn test_load_defaults() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!("node_type = \"cafe\"\ndata_dir = \"{}\"\n", dir.display()),
        );
        let config = Config::load(&path).unwrap();
        assert_eq!(config.concurrent_games, DEFAULT_CONCURRENT_GAMES);
        assert_eq!(config.chunk_concurrency, DEFAULT_CHUNK_CONCURRENCY);
        assert_eq!(config.disk_free_threshold, DEFAULT_DISK_FREE_THRESHOLD);
        assert_eq!(config.compact_threshold, DEFAULT_COMPACT_THRESHOLD);
        assert_eq!(config.listen_port, DEFAULT_LISTEN_PORT);
        assert_eq!(config.temp_ttl_hours, DEFAULT_TEMP_TTL_HOURS);
        assert_eq!(config.origin_endpoint, None);
        assert_eq!(config.origin_addr, None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_temp_ttl() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-ttl");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"cafe\"\ndata_dir = \"{}\"\ntemp_ttl_hours = 0\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("temp_ttl_hours"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_origin_mismatch() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-origin");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"idc\"\ndata_dir = \"{}\"\norigin_endpoint = \"abc\"\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("origin_endpoint"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_cafe_forbids_origin() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-cafeorigin");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"cafe\"\ndata_dir = \"{}\"\norigin_endpoint = \"abc\"\norigin_addr = \"127.0.0.1:42001\"\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("网吧节点不得配置 origin"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_origin_addr() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-originaddr");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"idc\"\ndata_dir = \"{}\"\norigin_endpoint = \"abc\"\norigin_addr = \"bad\"\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("origin_addr"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_node_type() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-type");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!("node_type = \"other\"\ndata_dir = \"{}\"\n", dir.display()),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("解析配置文件失败"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_concurrency() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-conc");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"idc\"\ndata_dir = \"{}\"\nconcurrent_games = 0\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("concurrent_games"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_display_node_type() {
        assert_eq!(NodeType::Idc.to_string(), "idc");
        assert_eq!(NodeType::Cafe.to_string(), "cafe");
    }

    #[test]
    fn test_load_empty_data_dir() {
        let cfg_path = std::env::temp_dir().join("blaze-agent-cfg-empty.toml");
        fs::write(&cfg_path, "node_type = \"idc\"\ndata_dir = \"\"\n").unwrap();
        let err = Config::load(&cfg_path).unwrap_err();
        assert!(err.to_string().contains("data_dir"));
        let _ = fs::remove_file(&cfg_path);
    }

    #[test]
    fn test_load_zero_disk_threshold() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-free");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"idc\"\ndata_dir = \"{}\"\ndisk_free_threshold = 0\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("disk_free_threshold"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_compact_threshold() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-thr");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"idc\"\ndata_dir = \"{}\"\ncompact_threshold = 1.5\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("compact_threshold"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_listen_port() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-port");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"idc\"\ndata_dir = \"{}\"\nlisten_port = 443\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("10000"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_external_addr() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-ext");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"cafe\"\ndata_dir = \"{}\"\nexternal_addr = \"111.161.74.28:42001\"\n",
                dir.display()
            ),
        );
        let config = Config::load(&path).unwrap();
        assert_eq!(config.external_addr.as_deref(), Some("111.161.74.28:42001"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_external_addr() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-extbad");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"cafe\"\ndata_dir = \"{}\"\nexternal_addr = \"不合法\"\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("external_addr"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_stun_addr() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-stunbad");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"cafe\"\ndata_dir = \"{}\"\nstun_addr = \"不合法\"\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("stun_addr"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_keepalive_port() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-kap");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"cafe\"\ndata_dir = \"{}\"\nkeepalive_port = 443\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("keepalive_port"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_control_addr() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-ctl");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"idc\"\ndata_dir = \"{}\"\ncontrol_addr = \"http://127.0.0.1:50051\"\n",
                dir.display()
            ),
        );
        let config = Config::load(&path).unwrap();
        assert_eq!(
            config.control_addr.as_deref(),
            Some("http://127.0.0.1:50051")
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_control_addr() {
        let dir = std::env::temp_dir().join("blaze-agent-cfg-ctlbad");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "node_type = \"idc\"\ndata_dir = \"{}\"\ncontrol_addr = \"127.0.0.1:50051\"\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("control_addr"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_missing_file() {
        let err = Config::load(Path::new("/tmp/不存在的配置.toml")).unwrap_err();
        assert!(err.to_string().contains("读取配置文件失败"));
    }

    #[cfg(not(windows))]
    #[test]
    fn test_resolve_uses_args_on_linux() {
        let dir = std::env::temp_dir().join("blaze-agent-resolve");
        fs::create_dir_all(&dir).unwrap();
        let cfg_path = write_config(
            &dir,
            &format!("node_type = \"idc\"\ndata_dir = \"{}\"\n", dir.display()),
        );
        let config = Config::resolve(&[
            "agent".to_string(),
            "--config".to_string(),
            cfg_path.to_string_lossy().to_string(),
        ])
        .unwrap();
        assert_eq!(config.node_type, NodeType::Idc);
        let _ = fs::remove_dir_all(&dir);
    }
}
