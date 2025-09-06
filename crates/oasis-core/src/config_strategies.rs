//! Concrete configuration strategy implementations for different runtime environments.

use crate::config::OasisConfig;
use crate::config_strategy::{
    ConfigChangeEvent, ConfigContext, ConfigStrategy, RuntimeEnvironment,
};
use anyhow::Result;
use figment::providers::Format;
use futures::StreamExt;
use std::path::PathBuf;
use tokio::sync::broadcast;

/// Server configuration strategy - supports hot reload via SIGHUP
pub struct ServerConfigStrategy {
    config_path: PathBuf,
    current_config: std::sync::Arc<tokio::sync::RwLock<OasisConfig>>,
    change_tx: broadcast::Sender<ConfigChangeEvent>,
    context: ConfigContext,
}

impl ServerConfigStrategy {
    pub fn new(config_path: impl Into<PathBuf>) -> Result<Self> {
        let config_path = config_path.into();
        let (change_tx, _) = broadcast::channel(100);

        let context = ConfigContext {
            base_dir: config_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .to_path_buf(),
            runtime_env: RuntimeEnvironment::Server,
        };

        Ok(Self {
            config_path,
            current_config: std::sync::Arc::new(tokio::sync::RwLock::new(OasisConfig::default())),
            change_tx,
            context,
        })
    }
}

#[async_trait::async_trait]
impl ConfigStrategy for ServerConfigStrategy {
    async fn load_initial_config(&self) -> Result<OasisConfig> {
        let config = self.load_config_from_file().await?;
        self.validate_config(&config).await?;

        *self.current_config.write().await = config.clone();
        Ok(config)
    }

    fn supports_hot_reload(&self) -> bool {
        true
    }

    async fn start_hot_reload(&self) -> Result<broadcast::Receiver<ConfigChangeEvent>> {
        let rx = self.change_tx.subscribe();

        // Start SIGHUP-based hot reload
        self.start_signal_reload().await?;

        Ok(rx)
    }

    async fn validate_config(&self, config: &OasisConfig) -> Result<()> {
        config
            .validate_with_context(&self.context)
            .map_err(|e| anyhow::anyhow!("Server config validation failed: {:?}", e))
    }

    fn strategy_name(&self) -> &'static str {
        "server"
    }
}

impl ServerConfigStrategy {
    async fn load_config_from_file(&self) -> Result<OasisConfig> {
        use figment::{
            Figment,
            providers::{Env, Serialized, Toml},
        };

        let figment = Figment::from(Serialized::defaults(OasisConfig::default()))
            .merge(Toml::file(&self.config_path))
            .merge(Env::prefixed("OASIS__").split("__"));

        let mut config: OasisConfig = figment
            .extract()
            .map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?;

        // Resolve relative paths
        config.resolve_relative_paths(&self.context.base_dir);

        Ok(config)
    }

    async fn start_signal_reload(&self) -> Result<()> {
        let mut signals = signal_hook_tokio::Signals::new(&[signal_hook::consts::SIGHUP])?;
        let strategy = self.clone();

        tokio::spawn(async move {
            tracing::info!("配置热重载已启用 (信号: SIGHUP)");

            while let Some(signal) = signals.next().await {
                match signal {
                    signal_hook::consts::SIGHUP => {
                        tracing::info!("收到 SIGHUP 信号，开始重载配置");

                        match strategy.reload_config().await {
                            Ok(change_event) => {
                                tracing::info!("配置重载成功: {:?}", change_event.changes);

                                if let Err(e) = strategy.change_tx.send(change_event) {
                                    tracing::warn!("配置变更通知发送失败: {}", e);
                                }
                            }
                            Err(e) => {
                                tracing::error!("配置重载失败: {}", e);
                            }
                        }
                    }
                    _ => {}
                }
            }
        });

        Ok(())
    }

    async fn reload_config(&self) -> Result<ConfigChangeEvent> {
        let old_config = self.current_config.read().await.clone();
        let new_config = self.load_config_from_file().await?;

        self.validate_config(&new_config).await?;

        *self.current_config.write().await = new_config.clone();

        Ok(ConfigChangeEvent::new(old_config, new_config))
    }
}

// Implement Clone for ServerConfigStrategy
impl Clone for ServerConfigStrategy {
    fn clone(&self) -> Self {
        Self {
            config_path: self.config_path.clone(),
            current_config: std::sync::Arc::clone(&self.current_config),
            change_tx: self.change_tx.clone(),
            context: self.context.clone(),
        }
    }
}

/// Agent configuration strategy - environment variable driven, restart-based reload
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
        // Load base configuration
        let mut config = self.load_base_config().await?;

        // Apply Agent-specific environment variable overrides
        self.apply_agent_env_overrides(&mut config)?;

        // Validate configuration
        self.validate_config(&config).await?;

        Ok(config)
    }

    fn supports_hot_reload(&self) -> bool {
        false
    } // Agent reloads via restart

    async fn validate_config(&self, config: &OasisConfig) -> Result<()> {
        config
            .validate_with_context(&self.context)
            .map_err(|e| anyhow::anyhow!("Agent config validation failed: {:?}", e))?;

        // Agent-specific validation
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

        // Try to load config file (Agent might not have one)
        let config_file =
            std::env::var("OASIS_CONFIG_FILE").unwrap_or_else(|_| "oasis.toml".to_string());

        if std::path::Path::new(&config_file).exists() {
            figment = figment.merge(Toml::file(config_file));
        }

        // Add environment variable layer - only parse known fields
        // We'll handle unknown fields gracefully in the extract step
        figment = figment.merge(Env::prefixed("OASIS__").split("__"));

        let config: OasisConfig = figment
            .extract()
            .map_err(|e| anyhow::anyhow!("Failed to load base config: {}", e))?;

        Ok(config)
    }

    fn apply_agent_env_overrides(&self, config: &mut OasisConfig) -> Result<()> {
        // Agent ID - most important configuration (store for later use)
        if let Ok(agent_id) = std::env::var("OASIS_AGENT_ID") {
            if !agent_id.trim().is_empty() {
                // Store agent_id in a way that doesn't conflict with config structure
                unsafe {
                    std::env::set_var("OASIS_AGENT_ID_INTERNAL", &agent_id);
                }
                tracing::info!("Agent ID from environment: {}", agent_id);
            }
        }

        // Set a default listen_addr for Agent (not used but required by config validation)
        if config.listen_addr.is_empty() {
            config.listen_addr = "127.0.0.1:0".to_string();
        }

        // NATS connection config - direct environment variables (keep backward compatibility)
        if let Ok(nats_url) = std::env::var("OASIS_NATS_URL") {
            config.nats.url = nats_url;
        }

        // gRPC server address override
        if let Ok(grpc_url) = std::env::var("OASIS__GRPC__URL") {
            config.grpc.url = grpc_url.clone();
            tracing::info!("gRPC URL from environment: {}", grpc_url);
        } else if let Ok(server_url) = std::env::var("OASIS_SERVER_URL") {
            // Store server_url in a way that doesn't conflict with config structure
            unsafe {
                std::env::set_var("OASIS_SERVER_URL_INTERNAL", &server_url);
            }
            tracing::info!("Server URL from environment: {}", server_url);
        }

        // Log configuration
        if let Ok(log_level) = std::env::var("OASIS_LOG_LEVEL") {
            config.telemetry.log_level = log_level;
        }

        // Agent behavior configuration
        if let Ok(heartbeat) = std::env::var("OASIS_HEARTBEAT_INTERVAL_SEC") {
            if let Ok(interval) = heartbeat.parse::<u64>() {
                config.agent.heartbeat_interval_sec = interval;
            }
        }

        if let Ok(fact_interval) = std::env::var("OASIS_FACT_COLLECTION_INTERVAL_SEC") {
            if let Ok(interval) = fact_interval.parse::<u64>() {
                config.agent.fact_collection_interval_sec = interval;
            }
        }

        Ok(())
    }

    async fn validate_agent_specific(&self, config: &OasisConfig) -> Result<()> {
        // Validate certificate files exist (Agent runtime must have certificates)
        if !config.tls.nats_ca_path().exists() {
            return Err(anyhow::anyhow!(
                "NATS CA certificate not found: {}",
                config.tls.nats_ca_path().display()
            ));
        }

        if !config.tls.nats_client_cert_path().exists() {
            return Err(anyhow::anyhow!(
                "NATS client certificate not found: {}",
                config.tls.nats_client_cert_path().display()
            ));
        }

        if !config.tls.nats_client_key_path().exists() {
            return Err(anyhow::anyhow!(
                "NATS client key not found: {}",
                config.tls.nats_client_key_path().display()
            ));
        }

        Ok(())
    }
}

/// CLI configuration strategy - lightweight, fresh config each time
pub struct CliConfigStrategy {
    context: ConfigContext,
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
        }
    }
}

#[async_trait::async_trait]
impl ConfigStrategy for CliConfigStrategy {
    async fn load_initial_config(&self) -> Result<OasisConfig> {
        // CLI uses simple direct configuration loading
        let config = OasisConfig::load_config(None)?;
        self.validate_config(&config).await?;
        Ok(config)
    }

    fn supports_hot_reload(&self) -> bool {
        false
    } // CLI loads fresh each time

    async fn validate_config(&self, config: &OasisConfig) -> Result<()> {
        // CLI only needs to validate connection-related configuration
        self.validate_connection_config(config).await
    }

    fn strategy_name(&self) -> &'static str {
        "cli"
    }
}

impl CliConfigStrategy {
    async fn validate_connection_config(&self, config: &OasisConfig) -> Result<()> {
        // Validate client certificates (CLI needs to connect to Server)
        // Use context.base_dir to resolve relative paths
        let cert_path = self
            .context
            .base_dir
            .join(&config.tls.grpc_client_cert_path());
        if !cert_path.exists() {
            return Err(anyhow::anyhow!(
                "gRPC client certificate not found: {}",
                cert_path.display()
            ));
        }

        let key_path = self
            .context
            .base_dir
            .join(&config.tls.grpc_client_key_path());
        if !key_path.exists() {
            return Err(anyhow::anyhow!(
                "gRPC client key not found: {}",
                key_path.display()
            ));
        }

        let ca_path = self.context.base_dir.join(&config.tls.grpc_ca_path());
        if !ca_path.exists() {
            return Err(anyhow::anyhow!(
                "gRPC CA certificate not found: {}",
                ca_path.display()
            ));
        }

        Ok(())
    }
}
