use async_trait::async_trait;
use oasis_core::config::{CommonConfig, ComponentConfig, ConfigLoadOptions, ConfigSource};
use oasis_core::error::Result;
use serde::{Deserialize, Serialize};

/// 统一的 CLI 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliConfig {
    #[serde(flatten)]
    pub common: CommonConfig,

    /// CLI 特定配置
    pub cli: CliSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliSection {
    /// 服务器连接地址
    pub server_url: String,
    /// 连接超时时间（秒）
    pub connection_timeout_sec: u64,
    /// 请求超时时间（秒）
    pub request_timeout_sec: u64,
    /// 是否启用交互式模式
    pub interactive: bool,
    /// 输出格式
    pub output_format: OutputFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputFormat {
    #[serde(rename = "json")]
    Json,
    #[serde(rename = "yaml")]
    Yaml,
    #[serde(rename = "table")]
    Table,
    #[serde(rename = "text")]
    Text,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            common: CommonConfig::default(),
            cli: CliSection::default(),
        }
    }
}

impl Default for CliSection {
    fn default() -> Self {
        Self {
            server_url: "http://127.0.0.1:50051".into(),
            connection_timeout_sec: 30,
            request_timeout_sec: 60,
            interactive: false,
            output_format: OutputFormat::Table,
        }
    }
}

impl Default for OutputFormat {
    fn default() -> Self {
        Self::Table
    }
}

impl CliConfig {
    /// 智能配置加载
    pub async fn load_smart() -> Result<Self> {
        // CLI 仅使用默认值
        Self::load_from_sources(&[ConfigSource::Defaults], ConfigLoadOptions::default()).await
    }

    /// 从指定路径加载配置
    pub async fn load_from_path(config_path: &str) -> Result<Self> {
        let sources = vec![
            ConfigSource::Defaults,
            ConfigSource::File(config_path.into()),
        ];

        Self::load_from_sources(&sources, ConfigLoadOptions::default()).await
    }
}

#[async_trait]
impl ComponentConfig for CliConfig {
    type Common = CommonConfig;

    async fn load_from_sources(
        sources: &[ConfigSource],
        options: ConfigLoadOptions,
    ) -> Result<Self> {
        oasis_core::config::load_config(sources, options).await
    }

    fn merge_with_common(&mut self, common: Self::Common) {
        self.common = common;
    }

    fn common(&self) -> &Self::Common {
        &self.common
    }

    fn common_mut(&mut self) -> &mut Self::Common {
        &mut self.common
    }

    fn validate_business_rules(&self) -> Result<()> {
        // CLI 特定的业务规则验证
        if self.cli.server_url.is_empty() {
            return Err(oasis_core::error::CoreError::Config {
                message: "server_url cannot be empty".to_string(),
            });
        }

        if self.cli.connection_timeout_sec == 0 {
            return Err(oasis_core::error::CoreError::Config {
                message: "connection_timeout_sec must be greater than 0".to_string(),
            });
        }

        if self.cli.request_timeout_sec == 0 {
            return Err(oasis_core::error::CoreError::Config {
                message: "request_timeout_sec must be greater than 0".to_string(),
            });
        }

        // 验证 URL 格式
        if !self.cli.server_url.starts_with("http://")
            && !self.cli.server_url.starts_with("https://")
        {
            return Err(oasis_core::error::CoreError::Config {
                message: "server_url must start with http:// or https://".to_string(),
            });
        }

        Ok(())
    }

    /// 检查配置是否可以热重载
    fn can_hot_reload(&self, other: &Self) -> bool {
        // CLI 配置大部分都可以热重载
        // 只有 NATS 连接相关的配置需要重启
        if self.common.nats.url != other.common.nats.url {
            return false; // NATS URL 变更需要重启
        }

        if self.common.nats.tls_required != other.common.nats.tls_required {
            return false; // TLS 要求变更需要重启
        }

        // 其他配置可以热重载
        true
    }

    /// 获取配置摘要
    fn summary(&self) -> std::collections::HashMap<String, String> {
        let mut summary = std::collections::HashMap::new();

        // 基本信息
        summary.insert("server_url".to_string(), self.cli.server_url.clone());
        summary.insert("nats_url".to_string(), self.common.nats.url.clone());
        summary.insert(
            "log_level".to_string(),
            self.common.telemetry.log_level.clone(),
        );
        summary.insert(
            "log_format".to_string(),
            self.common.telemetry.log_format.clone(),
        );

        // CLI 特定配置
        summary.insert(
            "connection_timeout_sec".to_string(),
            self.cli.connection_timeout_sec.to_string(),
        );
        summary.insert(
            "request_timeout_sec".to_string(),
            self.cli.request_timeout_sec.to_string(),
        );
        summary.insert("interactive".to_string(), self.cli.interactive.to_string());
        summary.insert(
            "output_format".to_string(),
            format!("{:?}", self.cli.output_format).to_lowercase(),
        );

        // 遥测配置
        summary.insert(
            "log_no_ansi".to_string(),
            self.common.telemetry.log_no_ansi.to_string(),
        );
        if let Some(ref log_file) = self.common.telemetry.log_file {
            summary.insert("log_file".to_string(), log_file.display().to_string());
        }
        summary.insert(
            "metrics_enabled".to_string(),
            self.common.telemetry.metrics_enabled.to_string(),
        );
        summary.insert(
            "tracing_enabled".to_string(),
            self.common.telemetry.tracing_enabled.to_string(),
        );

        // TLS 配置
        summary.insert(
            "tls_enabled".to_string(),
            self.common.tls.enabled.to_string(),
        );
        summary.insert(
            "nats_tls_required".to_string(),
            self.common.nats.tls_required.to_string(),
        );

        summary
    }
}
