use anyhow::{bail, Context};
use figment::providers::{Env, Serialized};
use figment::Figment;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub bind_addr: String,
    pub library_path: String,
    pub staging_path: String,
    pub cache_dir: String,
    pub cache_max_bytes: u64,
    pub metrics: bool,
    pub metrics_addr: String,
    pub log_json: bool,
    pub log_filter: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:50051".to_owned(),
            library_path: "/library".to_owned(),
            staging_path: "/staging".to_owned(),
            cache_dir: "/cache".to_owned(),
            cache_max_bytes: 10 * 1024 * 1024 * 1024,
            metrics: false,
            metrics_addr: "0.0.0.0:8082".to_owned(),
            log_json: true,
            log_filter: "info".to_owned(),
        }
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let config: Self = Figment::from(Serialized::defaults(Self::default()))
            .merge(Env::prefixed("MC_"))
            .extract()
            .context("read configuration from environment")?;

        if config.library_path.trim().is_empty() {
            bail!("MC_LIBRARY_PATH is required");
        }

        if config.cache_dir.trim().is_empty() {
            bail!("MC_CACHE_DIR is required");
        }

        Ok(config)
    }
}
