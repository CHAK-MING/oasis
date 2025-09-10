//! 针对不同运行环境的具体配置策略实现。

use crate::config::OasisConfig;
use crate::config_strategy::{ConfigContext, ConfigStrategy, RuntimeEnvironment};
use anyhow::Result;
use figment::providers::Format;
use std::path::PathBuf;

/// Server 配置策略
pub struct ServerConfigStrategy {
    config_path: PathBuf,
    context: ConfigContext,
}

impl ServerConfigStrategy {
    pub fn new(config_path: impl Into<PathBuf>) -> Result<Self> {
        let config_path = config_path.into();

        let context = ConfigContext {
            base_dir: config_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .to_path_buf(),
            runtime_env: RuntimeEnvironment::Server,
        };

        Ok(Self {
            config_path,
            context,
        })
    }
}

#[async_trait::async_trait]
impl ConfigStrategy for ServerConfigStrategy {
    async fn load_initial_config(&self) -> Result<OasisConfig> {
        let config = self.load_config_from_file().await?;
        self.validate_config(&config).await?;
        Ok(config)
    }

    async fn validate_config(&self, config: &OasisConfig) -> Result<()> {
        config
            .validate_with_context(&self.context)
            .map_err(|e| anyhow::anyhow!("Server 配置校验失败: {:?}", e))
    }

    fn strategy_name(&self) -> &'static str {
        "server"
    }
}

impl ServerConfigStrategy {
    async fn load_config_from_file(&self) -> Result<OasisConfig> {
        let path_str = self.config_path.to_string_lossy().to_string();
        let config = OasisConfig::load_config(Some(&path_str))?;
        Ok(config)
    }
}

/// Agent 配置策略 —— 由环境变量驱动，变更通过重启生效
pub struct AgentConfigStrategy {
    context: ConfigContext,
}

impl AgentConfigStrategy {
    pub fn new() -> Self {
        Self {
            context: ConfigContext {
                base_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
                runtime_env: RuntimeEnvironment::Agent,
            },
        }
    }
}

#[async_trait::async_trait]
impl ConfigStrategy for AgentConfigStrategy {
    async fn load_initial_config(&self) -> Result<OasisConfig> {
        // 加载基础配置
        let mut config = self.load_base_config().await?;

        // 应用 Agent 侧的环境变量覆盖
        self.apply_agent_env_overrides(&mut config)?;

        // 校验配置
        self.validate_config(&config).await?;

        Ok(config)
    }


    async fn validate_config(&self, config: &OasisConfig) -> Result<()> {
        config
            .validate_with_context(&self.context)
            .map_err(|e| anyhow::anyhow!("Agent 配置校验失败: {:?}", e))?;

        // Agent 侧特定校验
        self.validate_agent_specific(config).await
    }

    fn strategy_name(&self) -> &'static str {
        "agent"
    }
}

impl AgentConfigStrategy {
    async fn load_base_config(&self) -> Result<OasisConfig> {
        use figment::{
            Figment,
            providers::{Env, Serialized, Toml},
        };

        let mut figment = Figment::from(Serialized::defaults(OasisConfig::default()));

        // 尝试加载配置文件（Agent 可能没有配置文件）
        let config_file =
            std::env::var("OASIS_CONFIG_FILE").unwrap_or_else(|_| "oasis.toml".to_string());

        if std::path::Path::new(&config_file).exists() {
            figment = figment.merge(Toml::file(config_file));
        }

        // 追加环境变量层 —— 只解析已知字段
        // 未知字段在 extract 阶段优雅处理
        figment = figment.merge(Env::prefixed("OASIS__").split("__"));

        let config: OasisConfig = figment
            .extract()
            .map_err(|e| anyhow::anyhow!("加载基础配置失败: {}", e))?;

        Ok(config)
    }

    fn apply_agent_env_overrides(&self, config: &mut OasisConfig) -> Result<()> {
        // NATS 连接配置 —— 直接从环境变量读取
        if let Ok(nats_url) = std::env::var("OASIS__NATS__URL") {
            config.nats.url = nats_url;
        }

        // 日志配置
        if let Ok(log_level) = std::env::var("OASIS__TELEMETRY__LOG_LEVEL") {
            config.telemetry.log_level = log_level;
        }

        if let Ok(log_format) = std::env::var("OASIS__TELEMETRY__LOG_FORMAT") {
            config.telemetry.log_format = log_format;
        }

        if let Ok(log_no_ansi) = std::env::var("OASIS__TELEMETRY__LOG_NO_ANSI") {
            config.telemetry.log_no_ansi = log_no_ansi.parse().unwrap_or(false);
        }

        // 证书路径
        if let Ok(certs_dir) = std::env::var("OASIS__TLS__CERTS_DIR") {
            config.tls.certs_dir = PathBuf::from(certs_dir);
        }

        Ok(())
    }

    async fn validate_agent_specific(&self, config: &OasisConfig) -> Result<()> {
        // 校验证书文件是否存在（Agent 运行环境必须具备相关证书）
        if !config.tls.nats_ca_path().exists() {
            return Err(anyhow::anyhow!(
                "未找到 NATS CA 证书: {}",
                config.tls.nats_ca_path().display()
            ));
        }

        if !config.tls.nats_client_cert_path().exists() {
            return Err(anyhow::anyhow!(
                "未找到 NATS 客户端证书: {}",
                config.tls.nats_client_cert_path().display()
            ));
        }

        if !config.tls.nats_client_key_path().exists() {
            return Err(anyhow::anyhow!(
                "未找到 NATS 客户端私钥: {}",
                config.tls.nats_client_key_path().display()
            ));
        }

        Ok(())
    }
}

/// CLI 配置策略 —— 轻量化，每次运行加载最新配置
pub struct CliConfigStrategy {
    context: ConfigContext,
    config_path: Option<PathBuf>,
}

impl CliConfigStrategy {
    pub fn new(config_path: Option<PathBuf>) -> Self {
        let base_dir = config_path
            .as_ref()
            .and_then(|p| p.parent())
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf();

        Self {
            context: ConfigContext {
                base_dir,
                runtime_env: RuntimeEnvironment::Cli,
            },
            config_path,
        }
    }
}

#[async_trait::async_trait]
impl ConfigStrategy for CliConfigStrategy {
    async fn load_initial_config(&self) -> Result<OasisConfig> {
        // CLI 与 Server 一致：优先使用传入的 config_path，否则默认当前目录 oasis.toml
        let path_str = self
            .config_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string());
        let config = OasisConfig::load_config(path_str.as_deref())?;
        Ok(config)
    }


    fn strategy_name(&self) -> &'static str {
        "cli"
    }

    async fn validate_config(&self, config: &OasisConfig) -> Result<()> {
        config
            .validate_with_context(&self.context)
            .map_err(|e| anyhow::anyhow!("CLI 配置校验失败: {:?}", e))
    }
}
