//! 原始节点配置：Linux 规范——唯一参数 `--config <路径>` 指定 TOML 配置文件。
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// 默认压缩触发阈值：垃圾占比 30%。
pub const DEFAULT_COMPACT_THRESHOLD: f64 = 0.3;
/// 默认磁盘空闲下限（50GB），低于此值拒绝新上传。
pub const DEFAULT_MIN_FREE_BYTES: u64 = 50 * 1024 * 1024 * 1024;
/// 默认上传 gRPC 监听地址。
pub const DEFAULT_BIND_ADDR: &str = "0.0.0.0:50052";
/// 默认数据面监听端口（>10000，NAT 网关打洞约束）。
pub const DEFAULT_LISTEN_PORT: u16 = 42001;

fn default_threshold() -> f64 {
    DEFAULT_COMPACT_THRESHOLD
}

fn default_min_free() -> u64 {
    DEFAULT_MIN_FREE_BYTES
}

fn default_bind() -> String {
    DEFAULT_BIND_ADDR.to_string()
}

fn default_listen_port() -> u16 {
    DEFAULT_LISTEN_PORT
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// 块库根目录，每个游戏一个子目录。
    pub data_dir: PathBuf,
    /// 上传 gRPC 监听地址。
    #[serde(default = "default_bind")]
    pub bind_addr: String,
    /// 数据面 iroh 监听端口。
    #[serde(default = "default_listen_port")]
    pub listen_port: u16,
    /// 对外通告的公网映射地址（原始机多 IP 时逐个登记，一期单地址）。
    pub external_addr: Option<String>,
    /// relay 地址；缺省仅直连。
    pub relay_url: Option<String>,
    /// 压缩触发阈值（垃圾占比 0~1）。
    #[serde(default = "default_threshold")]
    pub compact_threshold: f64,
    /// 磁盘空闲下限，低于此值拒绝新上传。
    #[serde(default = "default_min_free")]
    pub min_free_bytes: u64,
}

impl Config {
    /// 从启动参数解析配置文件路径（有且仅有一个参数 `--config`）。
    pub fn parse_args(args: &[String]) -> Result<PathBuf> {
        if args.len() != 3 || args[1] != "--config" {
            bail!("用法: origin --config <配置文件路径>");
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
    pub fn bind_socket_addr(&self) -> Result<std::net::SocketAddr> {
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
        if self.listen_port < 10001 {
            bail!("listen_port 必须大于 10000（NAT 网关打洞限制）");
        }
        if let Some(addr) = &self.external_addr {
            addr.parse::<std::net::SocketAddr>()
                .map_err(|_| anyhow::anyhow!("external_addr 必须是 ip:port"))?;
        }
        if !(0.0 < self.compact_threshold && self.compact_threshold <= 1.0) {
            bail!("compact_threshold 必须在 (0, 1] 之间");
        }
        if self.min_free_bytes == 0 {
            bail!("min_free_bytes 必须大于 0");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write_config(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("origin.toml");
        fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn test_parse_args_ok() {
        let path = Config::parse_args(&[
            "origin".into(),
            "--config".into(),
            "/tmp/origin.toml".into(),
        ])
        .unwrap();
        assert_eq!(path, PathBuf::from("/tmp/origin.toml"));
    }

    #[test]
    fn test_parse_args_missing_flag() {
        let err = Config::parse_args(&["origin".into(), "/tmp/origin.toml".into()]).unwrap_err();
        assert!(err.to_string().contains("用法"));
    }

    #[test]
    fn test_parse_args_extra() {
        let err = Config::parse_args(&[
            "origin".into(),
            "--config".into(),
            "/tmp/origin.toml".into(),
            "extra".into(),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("用法"));
    }

    #[test]
    fn test_load_defaults() {
        let dir = std::env::temp_dir().join("blaze-origin-cfg");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(&dir, &format!("data_dir = \"{}\"\n", dir.display()));
        let config = Config::load(&path).unwrap();
        assert_eq!(config.compact_threshold, DEFAULT_COMPACT_THRESHOLD);
        assert_eq!(config.min_free_bytes, DEFAULT_MIN_FREE_BYTES);
        assert_eq!(config.listen_port, DEFAULT_LISTEN_PORT);
        assert_eq!(config.external_addr, None);
        assert_eq!(config.relay_url, None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_threshold() {
        let dir = std::env::temp_dir().join("blaze-origin-cfg-bad");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "data_dir = \"{}\"\ncompact_threshold = 1.5\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("compact_threshold"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_missing_file() {
        let err = Config::load(Path::new("/tmp/不存在的配置.toml")).unwrap_err();
        assert!(err.to_string().contains("读取配置文件失败"));
    }

    #[test]
    fn test_load_empty_data_dir() {
        let cfg_path = std::env::temp_dir().join("blaze-origin-cfg-empty.toml");
        fs::write(&cfg_path, "data_dir = \"\"\n").unwrap();
        let err = Config::load(&cfg_path).unwrap_err();
        assert!(err.to_string().contains("data_dir"));
        let _ = fs::remove_file(&cfg_path);
    }

    #[test]
    fn test_load_zero_min_free() {
        let dir = std::env::temp_dir().join("blaze-origin-cfg-free");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!("data_dir = \"{}\"\nmin_free_bytes = 0\n", dir.display()),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("min_free_bytes"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_bind() {
        let dir = std::env::temp_dir().join("blaze-origin-cfg-bind");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!("data_dir = \"{}\"\nbind_addr = \"不合法\"\n", dir.display()),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("bind_addr"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_listen_port() {
        let dir = std::env::temp_dir().join("blaze-origin-cfg-port");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!("data_dir = \"{}\"\nlisten_port = 443\n", dir.display()),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("listen_port"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_external_addr() {
        let dir = std::env::temp_dir().join("blaze-origin-cfg-ext");
        fs::create_dir_all(&dir).unwrap();
        let path = write_config(
            &dir,
            &format!(
                "data_dir = \"{}\"\nexternal_addr = \"不合法\"\n",
                dir.display()
            ),
        );
        let err = Config::load(&path).unwrap_err();
        assert!(err.to_string().contains("external_addr"));
        let _ = fs::remove_dir_all(&dir);
    }
}
