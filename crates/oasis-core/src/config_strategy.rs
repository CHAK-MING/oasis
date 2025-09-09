//! 不同运行环境的配置策略模块。
//!
//! 本模块为不同组件（Server、Agent、CLI）提供统一的配置加载与管理接口，
//! 以满足各自的运行需求。

use crate::config::OasisConfig;
use anyhow::Result;
use std::path::PathBuf;
use tokio::sync::broadcast;

/// 不同运行环境的配置策略接口。
///
/// 每个组件（Server、Agent、CLI）都可以实现该接口，
/// 以定义如何根据其特定需求加载与管理配置。
#[async_trait::async_trait]
pub trait ConfigStrategy: Send + Sync {
    /// 加载组件的初始配置。
    ///
    /// 该方法应处理组件的具体配置加载逻辑
    /// （例如：Server 基于文件，Agent 基于环境变量）。
    async fn load_initial_config(&self) -> Result<OasisConfig>;

    /// 是否支持配置热重载。
    ///
    /// - Server：true（支持 SIGHUP 与文件监听）
    /// - Agent：false（配置变更需要重启）
    /// - CLI：false（每次运行重新加载配置）
    fn supports_hot_reload(&self) -> bool {
        false
    }

    /// 若支持，启动配置热重载监听。
    ///
    /// 返回一个用于接收配置变更事件的接收端。
    /// 若不支持热重载，则返回错误。
    async fn start_hot_reload(&self) -> Result<broadcast::Receiver<ConfigChangeEvent>> {
        Err(anyhow::anyhow!("Hot reload not supported by this strategy"))
    }

    /// 针对特定运行环境校验配置。
    ///
    /// 允许每个组件执行环境相关的校验
    /// （例如：Agent 必须具备 agent_id，Server 必须持有有效 TLS 证书）。
    async fn validate_config(&self, config: &OasisConfig) -> Result<()>;

    /// 获取该配置策略的名称。
    fn strategy_name(&self) -> &'static str;
}

/// 热重载的配置变更事件。
#[derive(Debug, Clone)]
pub struct ConfigChangeEvent {
    /// 旧配置
    pub old_config: OasisConfig,
    /// 新配置
    pub new_config: OasisConfig,
    /// 配置项发生了变化
    pub changes: ConfigChanges,
    /// 变更发生的时间
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl ConfigChangeEvent {
    /// 创建一个新的配置变更事件。
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

/// 跟踪哪些配置项发生了变化。
#[derive(Debug, Clone, Default)]
pub struct ConfigChanges {
    pub tls_changed: bool,
    pub nats_changed: bool,
    pub telemetry_changed: bool,
    pub grpc_changed: bool,
    pub server_changed: bool,
}

impl ConfigChanges {
    /// 创建一个空的 ConfigChanges。
    pub fn new() -> Self {
        Default::default()
    }

    /// 检查是否有任意配置发生了变化。
    pub fn has_changes(&self) -> bool {
        self.tls_changed
            || self.nats_changed
            || self.telemetry_changed
            || self.grpc_changed
            || self.server_changed
    }

    /// 检查这些变更是否需要完全重启。
    ///
    /// 某些配置变更（如 NATS URL 或 gRPC 设置）需要完全重启，而不是热重载。
    pub fn requires_restart(&self) -> bool {
        self.nats_changed || self.grpc_changed
    }
}

/// 用于校验与路径解析的配置上下文。
#[derive(Debug, Clone)]
pub struct ConfigContext {
    /// 用于解析相对路径的基目录
    pub base_dir: PathBuf,
    /// 运行环境类型
    pub runtime_env: RuntimeEnvironment,
}

/// 运行环境类型。
#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeEnvironment {
    /// Server 运行时 - 支持热重载，使用配置文件
    Server,
    /// Agent 运行时 - 基于环境变量，需重启生效
    Agent,
    /// CLI 运行时 - 轻量级，每次运行加载新配置
    Cli,
}

impl OasisConfig {
    /// 比较两个配置并返回变更项。
    ///
    /// 用于热重载，以确定哪些配置项发生变化并需要更新。
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

        changes
    }

    pub fn validate_with_context(&self, context: &ConfigContext) -> Result<()> {
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

    /// Server 侧校验。
    fn validate_server_context(&self) -> Result<()> {
        // Server 需要有效的 TLS 证书
        if !self.tls.grpc_server_cert_path().exists() {
            return Err(anyhow::anyhow!(
                "未找到服务端证书: {}",
                self.tls.grpc_server_cert_path().display()
            ));
        }

        if !self.tls.grpc_server_key_path().exists() {
            return Err(anyhow::anyhow!(
                "未找到服务端私钥: {}",
                self.tls.grpc_server_key_path().display()
            ));
        }

        if !self.tls.grpc_ca_path().exists() {
            return Err(anyhow::anyhow!(
                "未找到 CA 证书: {}",
                self.tls.grpc_ca_path().display()
            ));
        }

        Ok(())
    }

    /// Agent 侧校验。
    fn validate_agent_context(&self) -> Result<()> {
        // 什么都不做
        Ok(())
    }

    /// CLI 侧校验。
    fn validate_cli_context(&self) -> Result<()> {
        // 什么都不用做
        Ok(())
    }
}
