/// 统一的配置格式处理器
///
/// 消除重复代码，提供一致的格式处理逻辑
use oasis_core::error::CoreError;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigFormat {
    Toml,
}

impl ConfigFormat {
    /// 从字符串解析格式，拒绝无效格式
    pub fn from_str(format: &str) -> Result<Self, CoreError> {
        match format.to_lowercase().as_str() {
            "toml" => Ok(ConfigFormat::Toml),
            _ => Err(CoreError::Config {
                message: format!(
                    "Unsupported config format: {}. Only 'toml' is supported",
                    format
                ),
            }),
        }
    }

    // 获取格式名称功能已不再需要
}

/// 统一的配置序列化器
pub struct ConfigSerializer;

impl ConfigSerializer {
    /// 将配置HashMap序列化为指定格式的字节数组
    pub fn serialize(
        config: &HashMap<String, String>,
        format: &ConfigFormat,
    ) -> Result<(Vec<u8>, String), CoreError> {
        match format {
            ConfigFormat::Toml => {
                let mut toml_table = toml::Table::new();
                for (key, value) in config {
                    Self::insert_nested_toml(&mut toml_table, key, value)?;
                }

                let toml_str =
                    toml::to_string_pretty(&toml_table).map_err(|e| CoreError::Config {
                        message: format!("Failed to serialize TOML: {}", e),
                    })?;
                Ok((toml_str.into_bytes(), "toml".to_string()))
            }
        }
    }

    /// 将字节数组反序列化为配置HashMap
    pub fn deserialize(
        data: &[u8],
        format: &ConfigFormat,
    ) -> Result<HashMap<String, String>, CoreError> {
        // 首先验证是否为有效UTF-8
        let text = std::str::from_utf8(data).map_err(|e| CoreError::Config {
            message: format!("Invalid UTF-8 data: {}", e),
        })?;

        match format {
            ConfigFormat::Toml => {
                let toml_value: toml::Value =
                    toml::from_str(text).map_err(|e| CoreError::Config {
                        message: format!("Invalid TOML format: {}", e),
                    })?;

                let mut config = HashMap::new();
                Self::flatten_toml_value("".to_string(), &toml_value, &mut config);
                Ok(config)
            }
        }
    }

    /// 验证配置数据格式是否正确
    pub fn validate(
        data: &[u8],
        format: &ConfigFormat,
    ) -> Result<Vec<String>, (bool, Vec<String>, Vec<String>)> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // 验证UTF-8
        if std::str::from_utf8(data).is_err() {
            errors.push("Data contains invalid UTF-8 characters".to_string());
            return Err((false, errors, warnings));
        }

        // 尝试解析
        match Self::deserialize(data, format) {
            Ok(config) => {
                // 基本验证通过，添加一些警告
                if config.is_empty() {
                    warnings.push("Configuration is empty".to_string());
                }

                // 检查可能的问题
                for (key, value) in &config {
                    if key.is_empty() {
                        warnings.push("Found empty configuration key".to_string());
                    }
                    if value.is_empty() {
                        warnings.push(format!("Configuration key '{}' has empty value", key));
                    }
                }

                Ok(warnings)
            }
            Err(e) => {
                errors.push(e.to_string());
                Err((false, errors, warnings))
            }
        }
    }

    /// 辅助函数：将嵌套的key插入TOML表
    fn insert_nested_toml(
        table: &mut toml::Table,
        key: &str,
        value: &str,
    ) -> Result<(), CoreError> {
        let parts: Vec<&str> = key.split('.').collect();
        let mut current = table;

        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                // 最后一部分，插入值
                current.insert(part.to_string(), toml::Value::String(value.to_string()));
            } else {
                // 中间部分，创建或获取嵌套表
                let entry = current
                    .entry(part.to_string())
                    .or_insert_with(|| toml::Value::Table(toml::Table::new()));

                match entry {
                    toml::Value::Table(nested_table) => {
                        current = nested_table;
                    }
                    _ => {
                        return Err(CoreError::Config {
                            message: format!("Key conflict: '{}' is not a table", part),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// 辅助函数：展平TOML值为键值对
    pub fn flatten_toml_value(
        prefix: String,
        value: &toml::Value,
        result: &mut HashMap<String, String>,
    ) {
        match value {
            toml::Value::Table(table) => {
                for (key, val) in table {
                    let new_prefix = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", prefix, key)
                    };
                    Self::flatten_toml_value(new_prefix, val, result);
                }
            }
            _ => {
                result.insert(prefix, value.to_string());
            }
        }
    }
}
