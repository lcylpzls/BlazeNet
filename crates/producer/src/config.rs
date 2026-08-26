//! 制作机配置：TOML 加载与校验。
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// 默认分块参数：min 64KiB / avg 1MiB / max 4MiB。
pub const DEFAULT_CHUNK_MIN: usize = 64 * 1024;
pub const DEFAULT_CHUNK_AVG: usize = 1024 * 1024;
pub const DEFAULT_CHUNK_MAX: usize = 4 * 1024 * 1024;
/// Windows 固定配置文件名（写死在代码中）。
pub const CONFIG_FILE_NAME: &str = "producer.toml";

fn default_min() -> usize {
    DEFAULT_CHUNK_MIN
}

fn default_avg() -> usize {
    DEFAULT_CHUNK_AVG
}

fn default_max() -> usize {
    DEFAULT_CHUNK_MAX
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkParams {
    #[serde(default = "default_min")]
    pub min_size: usize,
    #[serde(default = "default_avg")]
    pub avg_size: usize,
    #[serde(default = "default_max")]
    pub max_size: usize,
}

impl Default for ChunkParams {
    fn default() -> Self {
        Self {
            min_size: DEFAULT_CHUNK_MIN,
            avg_size: DEFAULT_CHUNK_AVG,
            max_size: DEFAULT_CHUNK_MAX,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub game_id: u64,
    pub version: u64,
    pub source_dir: PathBuf,
    /// 上一版本完整文件目录；缺省表示全新游戏。
    pub previous_dir: Option<PathBuf>,
    pub output_dir: PathBuf,
    #[serde(default)]
    pub chunk: ChunkParams,
}

impl Config {
    /// 加载并校验配置文件。
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .context(format!("读取配置文件失败: {}", path.display()))?;
        let config: Config =
            toml::from_str(&text).context(format!("解析配置文件失败: {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    /// Windows 规范：自动读取"程序目录上一级目录下的 config/ 目录"中的固定文件名配置。
    pub fn default_path() -> Result<PathBuf> {
        let exe = std::env::current_exe().context("无法获取可执行文件路径")?;
        let exe_dir = exe.parent().context("可执行文件缺少父目录")?;
        let parent = exe_dir.parent().context("可执行文件缺少上一级目录")?;
        Ok(parent.join("config").join(CONFIG_FILE_NAME))
    }

    fn validate(&self) -> Result<()> {
        if self.game_id == 0 {
            bail!("game_id 必须大于 0");
        }
        if self.version == 0 {
            bail!("version 必须大于 0");
        }
        let p = &self.chunk;
        if p.min_size < DEFAULT_CHUNK_MIN {
            bail!("分块最小尺寸不得小于 64KiB");
        }
        if !(p.min_size < p.avg_size && p.avg_size <= p.max_size) {
            bail!("分块参数必须满足 min < avg <= max");
        }
        if !self.source_dir.is_dir() {
            bail!("source_dir 不是有效目录: {}", self.source_dir.display());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn sample_config(source: &Path) -> String {
        format!(
            "game_id = 1\nversion = 2\nsource_dir = \"{}\"\noutput_dir = \"/tmp/out\"\n",
            source.display()
        )
    }

    #[test]
    fn test_load_with_defaults() {
        let dir = std::env::temp_dir().join("blaze-cfg-ok");
        fs::create_dir_all(&dir).unwrap();
        let cfg_path = dir.join("producer.toml");
        fs::write(&cfg_path, sample_config(&dir)).unwrap();
        let config = Config::load(&cfg_path).unwrap();
        assert_eq!(config.game_id, 1);
        assert_eq!(config.version, 2);
        assert_eq!(config.chunk.min_size, DEFAULT_CHUNK_MIN);
        assert_eq!(config.chunk.avg_size, DEFAULT_CHUNK_AVG);
        assert_eq!(config.chunk.max_size, DEFAULT_CHUNK_MAX);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_missing_file() {
        let err = Config::load(Path::new("/tmp/不存在的配置.toml")).unwrap_err();
        assert!(err.to_string().contains("读取配置文件失败"));
    }

    #[test]
    fn test_load_invalid_toml() {
        let dir = std::env::temp_dir().join("blaze-cfg-bad");
        fs::create_dir_all(&dir).unwrap();
        let cfg_path = dir.join("producer.toml");
        fs::write(&cfg_path, "game_id = ").unwrap();
        let err = Config::load(&cfg_path).unwrap_err();
        assert!(err.to_string().contains("解析配置文件失败"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_invalid_params() {
        let dir = std::env::temp_dir().join("blaze-cfg-param");
        fs::create_dir_all(&dir).unwrap();
        let cfg_path = dir.join("producer.toml");
        fs::write(
            &cfg_path,
            format!(
                "game_id = 1\nversion = 1\nsource_dir = \"{}\"\noutput_dir = \"/tmp/out\"\n[chunk]\nmin_size = 1024\n",
                dir.display()
            ),
        )
        .unwrap();
        let err = Config::load(&cfg_path).unwrap_err();
        assert!(err.to_string().contains("64KiB"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_game_id_zero() {
        let cfg_path = std::env::temp_dir().join("blaze-cfg-gid.toml");
        fs::write(
            &cfg_path,
            "game_id = 0\nversion = 1\nsource_dir = \"/tmp\"\noutput_dir = \"/tmp/out\"\n",
        )
        .unwrap();
        let err = Config::load(&cfg_path).unwrap_err();
        assert!(err.to_string().contains("game_id"));
        let _ = fs::remove_file(&cfg_path);
    }

    #[test]
    fn test_load_version_zero() {
        let cfg_path = std::env::temp_dir().join("blaze-cfg-ver.toml");
        fs::write(
            &cfg_path,
            "game_id = 1\nversion = 0\nsource_dir = \"/tmp\"\noutput_dir = \"/tmp/out\"\n",
        )
        .unwrap();
        let err = Config::load(&cfg_path).unwrap_err();
        assert!(err.to_string().contains("version"));
        let _ = fs::remove_file(&cfg_path);
    }

    #[test]
    fn test_load_params_min_not_less_than_avg() {
        let dir = std::env::temp_dir().join("blaze-cfg-order");
        fs::create_dir_all(&dir).unwrap();
        let cfg_path = dir.join("producer.toml");
        fs::write(
            &cfg_path,
            format!(
                "game_id = 1\nversion = 1\nsource_dir = \"{}\"\noutput_dir = \"/tmp/out\"\n[chunk]\nmin_size = 2097152\n",
                dir.display()
            ),
        )
        .unwrap();
        let err = Config::load(&cfg_path).unwrap_err();
        assert!(err.to_string().contains("min < avg"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_partial_chunk_uses_field_defaults() {
        let dir = std::env::temp_dir().join("blaze-cfg-partial");
        fs::create_dir_all(&dir).unwrap();
        let cfg_path = dir.join("producer.toml");
        fs::write(
            &cfg_path,
            format!(
                "game_id = 1\nversion = 1\nsource_dir = \"{}\"\noutput_dir = \"/tmp/out\"\n[chunk]\navg_size = 2097152\n",
                dir.display()
            ),
        )
        .unwrap();
        let config = Config::load(&cfg_path).unwrap();
        assert_eq!(config.chunk.min_size, DEFAULT_CHUNK_MIN);
        assert_eq!(config.chunk.avg_size, 2 * 1024 * 1024);
        assert_eq!(config.chunk.max_size, DEFAULT_CHUNK_MAX);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_load_missing_source_dir() {
        let cfg_path = std::env::temp_dir().join("blaze-cfg-nosrc.toml");
        fs::write(
            &cfg_path,
            "game_id = 1\nversion = 1\nsource_dir = \"/tmp/不存在的目录\"\noutput_dir = \"/tmp/out\"\n",
        )
        .unwrap();
        let err = Config::load(&cfg_path).unwrap_err();
        assert!(err.to_string().contains("source_dir"));
        let _ = fs::remove_file(&cfg_path);
    }

    #[test]
    fn test_default_path() {
        let path = Config::default_path().unwrap();
        let text = path.to_string_lossy();
        assert!(text.ends_with(&format!("config{}producer.toml", std::path::MAIN_SEPARATOR)));
    }
}
