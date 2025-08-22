use crate::config::{ComponentConfig, ConfigLoadOptions, ConfigSource};
use crate::error::{CoreError, Result};
use figment::{
    Figment,
    providers::{Format, Serialized, Toml},
};
use std::collections::HashMap;
use tokio_stream::StreamExt;

/// 统一配置加载器
pub struct ConfigLoader {
    sources: Vec<ConfigSource>,
    options: ConfigLoadOptions,
}

impl ConfigLoader {
    /// 创建新的配置加载器
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            options: ConfigLoadOptions::default(),
        }
    }

    /// 添加配置源
    pub fn add_source(mut self, source: ConfigSource) -> Self {
        self.sources.push(source);
        self
    }

    /// 添加多个配置源
    pub fn add_sources(mut self, sources: &[ConfigSource]) -> Self {
        self.sources.extend(sources.iter().cloned());
        self
    }

    /// 设置加载选项
    pub fn with_options(mut self, options: ConfigLoadOptions) -> Self {
        self.options = options;
        self
    }

    /// 加载配置
    pub async fn load<T: ComponentConfig + Default>(&self) -> Result<T> {
        let mut figment = Figment::new();
        let mut dynamic_sources = Vec::new();

        // 按优先级处理配置源
        for source in &self.sources {
            match source {
                ConfigSource::Defaults => {
                    figment = figment.merge(Serialized::defaults(T::default()));
                }
                ConfigSource::File(path) => {
                    if path.exists() {
                        figment = figment.merge(Toml::file(path));
                    } else if self.options.fail_on_missing_file {
                        return Err(CoreError::Config {
                            message: format!("Config file not found: {}", path.display()),
                        });
                    }
                }
                ConfigSource::NatsKv { .. } => {
                    // 动态配置源在运行时处理
                    dynamic_sources.push(source.clone());
                }
            }
        }

        // 提取基础配置
        let mut config: T = figment.extract().map_err(|e| CoreError::Config {
            message: format!("Failed to extract config: {}", e),
        })?;

        // 处理动态配置源
        for source in &dynamic_sources {
            if let ConfigSource::NatsKv {
                client,
                bucket,
                prefix,
                ..
            } = source
            {
                let kv_updates = self.load_nats_kv_config(client, bucket, prefix).await?;

                // 将 KV 更新应用到配置（基于 JSON 路径写入）
                let mut json_config =
                    serde_json::to_value(&config).map_err(|e| CoreError::Config {
                        message: format!("Failed to serialize config to JSON: {}", e),
                    })?;

                for (path, value) in kv_updates.iter() {
                    Self::set_json_value(&mut json_config, path, value)?;
                }

                config = serde_json::from_value(json_config).map_err(|e| CoreError::Config {
                    message: format!("Failed to deserialize updated config: {}", e),
                })?;
            }
        }

        // 验证配置
        if self.options.validate {
            config.validate()?;
        }

        Ok(config)
    }

    /// 从 NATS KV 加载配置
    async fn load_nats_kv_config(
        &self,
        client: &async_nats::Client,
        bucket: &str,
        prefix: &str,
    ) -> Result<HashMap<String, serde_json::Value>> {
        let js = async_nats::jetstream::new(client.clone());
        let kv = js
            .get_key_value(bucket)
            .await
            .map_err(|e| CoreError::Config {
                message: format!("Failed to get KV store {}: {}", bucket, e),
            })?;

        let mut config_updates = HashMap::new();

        // 获取所有匹配前缀的配置项
        let mut keys_stream = kv.keys().await.map_err(|e| CoreError::Config {
            message: format!("Failed to list KV keys: {}", e),
        })?;

        while let Some(key_result) = keys_stream.next().await {
            if let Ok(key) = key_result {
                if key.starts_with(prefix) {
                    if let Ok(entry) = kv.get(&key).await {
                        if let Some(entry) = entry {
                            let config_key = key.strip_prefix(prefix).unwrap_or(&key);
                            let value = String::from_utf8(entry.to_vec()).map_err(|e| {
                                CoreError::Config {
                                    message: format!("Invalid UTF-8 in config value: {}", e),
                                }
                            })?;

                            // 解析为 JSON 值
                            let json_value = self.parse_config_value(&value)?;
                            config_updates.insert(config_key.to_string(), json_value);
                        }
                    }
                }
            }
        }

        Ok(config_updates)
    }

    /// 解析配置值
    fn parse_config_value(&self, value: &str) -> Result<serde_json::Value> {
        // 尝试解析为 JSON
        if let Ok(json_value) = serde_json::from_str(value) {
            return Ok(json_value);
        }

        // 尝试解析为布尔值
        if let Ok(bool_value) = value.parse::<bool>() {
            return Ok(serde_json::Value::Bool(bool_value));
        }

        // 尝试解析为数字
        if let Ok(num_value) = value.parse::<i64>() {
            return Ok(serde_json::Value::Number(num_value.into()));
        }

        if let Ok(num_value) = value.parse::<f64>() {
            if let Some(number) = serde_json::Number::from_f64(num_value) {
                return Ok(serde_json::Value::Number(number));
            }
        }

        // 默认为字符串
        Ok(serde_json::Value::String(value.to_string()))
    }

    /// 在 JSON 对象中根据点分路径写入值
    fn set_json_value(
        json: &mut serde_json::Value,
        path: &str,
        value: &serde_json::Value,
    ) -> Result<()> {
        let parts: Vec<&str> = path
            .trim_matches('.')
            .split('.')
            .filter(|p| !p.is_empty())
            .collect();
        if parts.is_empty() {
            return Ok(());
        }

        let mut current = json;
        for (i, part) in parts.iter().enumerate() {
            let is_last = i == parts.len() - 1;
            if is_last {
                match current {
                    serde_json::Value::Object(obj) => {
                        obj.insert((*part).to_string(), value.clone());
                    }
                    _ => {
                        return Err(CoreError::Config {
                            message: format!("Cannot set value at path: {}", path),
                        });
                    }
                }
            } else {
                match current {
                    serde_json::Value::Object(obj) => {
                        if !obj.contains_key(*part) {
                            obj.insert(
                                (*part).to_string(),
                                serde_json::Value::Object(serde_json::Map::new()),
                            );
                        }
                        current = obj.get_mut(*part).ok_or_else(|| CoreError::Config {
                            message: format!("Path not found: {}", path),
                        })?;
                    }
                    _ => {
                        return Err(CoreError::Config {
                            message: format!("Cannot navigate path on non-object: {}", path),
                        });
                    }
                }
            }
        }

        Ok(())
    }
}

impl Default for ConfigLoader {
    fn default() -> Self {
        Self::new()
    }
}

/// 便捷的配置加载函数
pub async fn load_config_from_file<T: ComponentConfig>(path: &str) -> Result<T> {
    ConfigLoader::new()
        .add_source(ConfigSource::File(path.into()))
        .load()
        .await
}

/// 从多个源加载配置
pub async fn load_config_from_sources<T: ComponentConfig>(
    sources: &[ConfigSource],
    options: ConfigLoadOptions,
) -> Result<T> {
    ConfigLoader::new()
        .with_options(options)
        .add_sources(sources)
        .load()
        .await
}

/// 智能配置加载（尝试多个位置）
pub async fn load_config_smart<T: ComponentConfig>() -> Result<T> {
    let mut sources = vec![ConfigSource::Defaults];

    let config_paths = ["config/config.toml"];

    for path in &config_paths {
        if std::path::Path::new(path).exists() {
            sources.push(ConfigSource::File(path.into()));
            break;
        }
    }

    // 不再从环境变量加载覆盖；仅保留文件 + KV

    load_config_from_sources(&sources, ConfigLoadOptions::default()).await
}
