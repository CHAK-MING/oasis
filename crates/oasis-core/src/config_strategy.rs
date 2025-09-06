//! Configuration strategy module for different runtime environments.
//!
//! This module provides a unified interface for loading and managing configuration
//! across different components (Server, Agent, CLI) with their specific requirements.

use crate::config::OasisConfig;
use anyhow::Result;
use std::path::PathBuf;
use tokio::sync::broadcast;

/// Configuration strategy trait for different runtime environments.
///
/// Each component (Server, Agent, CLI) can implement this trait to define
/// how it loads and manages configuration according to its specific needs.
#[async_trait::async_trait]
pub trait ConfigStrategy: Send + Sync {
    /// Load initial configuration for the component.
    ///
    /// This method should handle the specific configuration loading logic
    /// for the component (e.g., file-based for Server, env-var-based for Agent).
    async fn load_initial_config(&self) -> Result<OasisConfig>;

    /// Whether this strategy supports hot reloading of configuration.
    ///
    /// - Server: true (supports SIGHUP and file watching)
    /// - Agent: false (configuration changes require restart)
    /// - CLI: false (configuration is loaded fresh each time)
    fn supports_hot_reload(&self) -> bool {
        false
    }

    /// Start hot reload monitoring if supported.
    ///
    /// Returns a receiver for configuration change events.
    /// If hot reload is not supported, returns an error.
    async fn start_hot_reload(&self) -> Result<broadcast::Receiver<ConfigChangeEvent>> {
        Err(anyhow::anyhow!("Hot reload not supported by this strategy"))
    }

    /// Validate configuration for this specific runtime environment.
    ///
    /// This allows each component to perform environment-specific validation
    /// (e.g., Agent must have agent_id, Server must have valid TLS certs).
    async fn validate_config(&self, config: &OasisConfig) -> Result<()>;

    /// Get the name of this configuration strategy.
    fn strategy_name(&self) -> &'static str;
}

/// Configuration change event for hot reloading.
#[derive(Debug, Clone)]
pub struct ConfigChangeEvent {
    /// The previous configuration
    pub old_config: OasisConfig,
    /// The new configuration
    pub new_config: OasisConfig,
    /// What parts of the configuration changed
    pub changes: ConfigChanges,
    /// When the change occurred
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl ConfigChangeEvent {
    /// Create a new configuration change event.
    pub fn new(old_config: OasisConfig, new_config: OasisConfig) -> Self {
        let changes = old_config.diff(&new_config);
        Self {
            old_config,
            new_config,
            changes,
            timestamp: chrono::Utc::now(),
        }
    }
}

/// Tracks which parts of the configuration have changed.
#[derive(Debug, Clone, Default)]
pub struct ConfigChanges {
    pub tls_changed: bool,
    pub nats_changed: bool,
    pub telemetry_changed: bool,
    pub grpc_changed: bool,
    pub server_changed: bool,
    pub agent_changed: bool,
}

impl ConfigChanges {
    /// Create a new empty ConfigChanges.
    pub fn new() -> Self {
        Default::default()
    }

    /// Check if any configuration has changed.
    pub fn has_changes(&self) -> bool {
        self.tls_changed
            || self.nats_changed
            || self.telemetry_changed
            || self.grpc_changed
            || self.server_changed
            || self.agent_changed
    }

    /// Check if the changes require a full restart.
    ///
    /// Some configuration changes (like NATS URL or gRPC settings)
    /// require a full restart rather than hot reload.
    pub fn requires_restart(&self) -> bool {
        self.nats_changed || self.grpc_changed
    }
}

/// Configuration context for validation and path resolution.
#[derive(Debug, Clone)]
pub struct ConfigContext {
    /// Base directory for resolving relative paths
    pub base_dir: PathBuf,
    /// Runtime environment type
    pub runtime_env: RuntimeEnvironment,
}

/// Runtime environment types.
#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeEnvironment {
    /// Server runtime - supports hot reload, uses config files
    Server,
    /// Agent runtime - environment variable driven, restart-based reload
    Agent,
    /// CLI runtime - lightweight, fresh config each time
    Cli,
}

impl OasisConfig {
    /// Compare two configurations and return what changed.
    ///
    /// This is used for hot reload to determine which parts of the
    /// configuration have changed and need to be updated.
    pub fn diff(&self, other: &Self) -> ConfigChanges {
        let mut changes = ConfigChanges::new();

        if self.tls != other.tls {
            changes.tls_changed = true;
        }

        if self.nats != other.nats {
            changes.nats_changed = true;
        }

        if self.telemetry != other.telemetry {
            changes.telemetry_changed = true;
        }

        if self.grpc != other.grpc {
            changes.grpc_changed = true;
        }

        if self.server != other.server {
            changes.server_changed = true;
        }

        if self.agent != other.agent {
            changes.agent_changed = true;
        }

        changes
    }

    /// Validate configuration with runtime context.
    ///
    /// This extends the basic validation with runtime-specific checks.
    pub fn validate_with_context(&self, context: &ConfigContext) -> Result<()> {
        // Basic validation
        self.validate()?;

        // Runtime-specific validation
        match context.runtime_env {
            RuntimeEnvironment::Server => {
                self.validate_server_context()?;
            }
            RuntimeEnvironment::Agent => {
                self.validate_agent_context()?;
            }
            RuntimeEnvironment::Cli => {
                self.validate_cli_context()?;
            }
        }

        Ok(())
    }

    /// Server-specific validation.
    fn validate_server_context(&self) -> Result<()> {
        // Server needs valid TLS certificates
        if !self.tls.grpc_server_cert_path().exists() {
            return Err(anyhow::anyhow!(
                "Server certificate not found: {}",
                self.tls.grpc_server_cert_path().display()
            ));
        }

        if !self.tls.grpc_server_key_path().exists() {
            return Err(anyhow::anyhow!(
                "Server key not found: {}",
                self.tls.grpc_server_key_path().display()
            ));
        }

        if !self.tls.grpc_ca_path().exists() {
            return Err(anyhow::anyhow!(
                "CA certificate not found: {}",
                self.tls.grpc_ca_path().display()
            ));
        }

        Ok(())
    }

    /// Agent-specific validation.
    fn validate_agent_context(&self) -> Result<()> {
        // Agent needs client certificates for NATS and gRPC
        if !self.tls.nats_ca_path().exists() {
            return Err(anyhow::anyhow!(
                "NATS CA certificate not found: {}",
                self.tls.nats_ca_path().display()
            ));
        }

        if !self.tls.nats_client_cert_path().exists() {
            return Err(anyhow::anyhow!(
                "NATS client certificate not found: {}",
                self.tls.nats_ca_path().display()
            ));
        }

        if !self.tls.nats_client_key_path().exists() {
            return Err(anyhow::anyhow!(
                "NATS client key not found: {}",
                self.tls.nats_client_key_path().display()
            ));
        }

        Ok(())
    }

    /// CLI-specific validation.
    fn validate_cli_context(&self) -> Result<()> {
        // CLI needs client certificates to connect to server
        if !self.tls.grpc_client_cert_path().exists() {
            return Err(anyhow::anyhow!(
                "gRPC client certificate not found: {}",
                self.tls.grpc_client_cert_path().display()
            ));
        }

        if !self.tls.grpc_client_key_path().exists() {
            return Err(anyhow::anyhow!(
                "gRPC client key not found: {}",
                self.tls.grpc_client_key_path().display()
            ));
        }

        if !self.tls.grpc_ca_path().exists() {
            return Err(anyhow::anyhow!(
                "gRPC CA certificate not found: {}",
                self.tls.grpc_ca_path().display()
            ));
        }

        Ok(())
    }
}
