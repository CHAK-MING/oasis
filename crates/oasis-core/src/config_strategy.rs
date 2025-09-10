//! 不同运行环境的配置策略模块。
//!
//! 本模块为不同组件（Server、Agent、CLI）提供统一的配置加载与管理接口，
//! 以满足各自的运行需求。

use crate::config::OasisConfig;
use anyhow::Result;
use std::path::PathBuf;

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

    /// 针对特定运行环境校验配置。
    ///
    /// 允许每个组件执行环境相关的校验
    /// （例如：Agent 必须具备 agent_id，Server 必须持有有效 TLS 证书）。
    async fn validate_config(&self, config: &OasisConfig) -> Result<()>;

    /// 获取该配置策略的名称。
    fn strategy_name(&self) -> &'static str;
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
    /// Server 运行时 - 使用配置文件
    Server,
    /// Agent 运行时 - 基于环境变量
    Agent,
    /// CLI 运行时 - 轻量级，每次运行加载新配置
    Cli,
}

impl OasisConfig {
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
