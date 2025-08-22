use crate::config::CommonConfig;
use crate::error::{Result, CoreError};
use std::collections::HashMap;

/// 配置验证器
pub struct ConfigValidator;

impl ConfigValidator {
    /// 验证通用配置
    pub fn validate_common_config(config: &CommonConfig) -> Result<()> {
        // 验证 NATS 配置
        Self::validate_nats_config(&config.nats)?;
        
        // 验证 TLS 配置
        Self::validate_tls_config(&config.tls)?;
        
        // 验证遥测配置
        Self::validate_telemetry_config(&config.telemetry)?;
        
        Ok(())
    }
    
    /// 验证 NATS 配置
    fn validate_nats_config(nats: &crate::config::NatsConfig) -> Result<()> {
        // 验证 URL 格式
        if !nats.url.starts_with("nats://") && !nats.url.starts_with("nats://") {
            return Err(CoreError::Config {
                message: "Invalid NATS URL format".to_string(),
            });
        }
        
        // 验证超时设置
        if nats.connection_timeout_sec == 0 {
            return Err(CoreError::Config {
                message: "NATS connection timeout must be greater than 0".to_string(),
            });
        }
        
        // 验证重连延迟
        if nats.reconnect_delay_sec == 0 {
            return Err(CoreError::Config {
                message: "NATS reconnect delay must be greater than 0".to_string(),
            });
        }
        
        Ok(())
    }
    
    /// 验证 TLS 配置
    fn validate_tls_config(tls: &crate::config::TlsConfig) -> Result<()> {
        if tls.enabled {
            // 验证证书和私钥的配对
            if tls.client_cert.is_some() && tls.client_key.is_none() {
                return Err(CoreError::Config {
                    message: "TLS client certificate requires private key".to_string(),
                });
            }
            
            if tls.client_key.is_some() && tls.client_cert.is_none() {
                return Err(CoreError::Config {
                    message: "TLS private key requires client certificate".to_string(),
                });
            }
            
            // 验证文件存在性
            if let Some(cert_path) = &tls.client_cert {
                if !cert_path.exists() {
                    return Err(CoreError::Config {
                        message: format!("TLS client certificate file not found: {}", cert_path.display()),
                    });
                }
            }
            
            if let Some(key_path) = &tls.client_key {
                if !key_path.exists() {
                    return Err(CoreError::Config {
                        message: format!("TLS private key file not found: {}", key_path.display()),
                    });
                }
            }
            
            if let Some(ca_path) = &tls.ca_cert {
                if !ca_path.exists() {
                    return Err(CoreError::Config {
                        message: format!("TLS CA certificate file not found: {}", ca_path.display()),
                    });
                }
            }
            
            // 验证 TLS 版本
            let valid_versions = ["1.0", "1.1", "1.2", "1.3"];
            if !valid_versions.contains(&tls.min_version.as_str()) {
                return Err(CoreError::Config {
                    message: format!("Invalid TLS minimum version: {}", tls.min_version),
                });
            }
        }
        
        Ok(())
    }
    
    /// 验证遥测配置
    fn validate_telemetry_config(telemetry: &crate::config::TelemetryConfig) -> Result<()> {
        // 验证日志级别
        let valid_levels = ["trace", "debug", "info", "warn", "error"];
        if !valid_levels.contains(&telemetry.log_level.as_str()) {
            return Err(CoreError::Config {
                message: format!("Invalid log level: {}", telemetry.log_level),
            });
        }
        
        // 验证日志格式
        let valid_formats = ["text", "json"];
        if !valid_formats.contains(&telemetry.log_format.as_str()) {
            return Err(CoreError::Config {
                message: format!("Invalid log format: {}", telemetry.log_format),
            });
        }
        
        // 验证端口范围
        if telemetry.metrics_port == 0 || telemetry.metrics_port > 65535 {
            return Err(CoreError::Config {
                message: format!("Invalid metrics port: {}", telemetry.metrics_port),
            });
        }
        
        // 验证日志文件路径
        if let Some(log_file) = &telemetry.log_file {
            if let Some(parent) = log_file.parent() {
                if !parent.exists() {
                    return Err(CoreError::Config {
                        message: format!("Log file directory does not exist: {}", parent.display()),
                    });
                }
            }
        }
        
        // 验证追踪端点
        if let Some(endpoint) = &telemetry.tracing_endpoint {
            if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                return Err(CoreError::Config {
                    message: format!("Invalid tracing endpoint format: {}", endpoint),
                });
            }
        }
        
        Ok(())
    }
    
    /// 验证配置值类型
    pub fn validate_config_value(key: &str, value: &str) -> Result<()> {
        match key {
            "nats.connection_timeout_sec" | "nats.reconnect_delay_sec" => {
                value.parse::<u64>().map_err(|_| CoreError::Config {
                    message: format!("Invalid number for {}: {}", key, value),
                })?;
            }
            "nats.max_reconnects" => {
                if !value.is_empty() {
                    value.parse::<usize>().map_err(|_| CoreError::Config {
                        message: format!("Invalid number for {}: {}", key, value),
                    })?;
                }
            }
            "nats.tls_required" | "tls.enabled" | "tls.verify_hostname" | 
            "telemetry.log_no_ansi" | "telemetry.metrics_enabled" | "telemetry.tracing_enabled" => {
                value.parse::<bool>().map_err(|_| CoreError::Config {
                    message: format!("Invalid boolean for {}: {}", key, value),
                })?;
            }
            "telemetry.metrics_port" => {
                let port = value.parse::<u16>().map_err(|_| CoreError::Config {
                    message: format!("Invalid port number for {}: {}", key, value),
                })?;
                if port == 0 {
                    return Err(CoreError::Config {
                        message: format!("Port cannot be 0 for {}: {}", key, value),
                    });
                }
            }
            "telemetry.log_level" => {
                let valid_levels = ["trace", "debug", "info", "warn", "error"];
                if !valid_levels.contains(&value) {
                    return Err(CoreError::Config {
                        message: format!("Invalid log level for {}: {}", key, value),
                    });
                }
            }
            "telemetry.log_format" => {
                let valid_formats = ["text", "json"];
                if !valid_formats.contains(&value) {
                    return Err(CoreError::Config {
                        message: format!("Invalid log format for {}: {}", key, value),
                    });
                }
            }
            "tls.min_version" => {
                let valid_versions = ["1.0", "1.1", "1.2", "1.3"];
                if !valid_versions.contains(&value) {
                    return Err(CoreError::Config {
                        message: format!("Invalid TLS version for {}: {}", key, value),
                    });
                }
            }
            _ => {
                // 对于其他配置项，只验证非空
                if value.is_empty() {
                    return Err(CoreError::Config {
                        message: format!("Empty value not allowed for {}: {}", key, value),
                    });
                }
            }
        }
        
        Ok(())
    }
    
    /// 验证配置映射
    pub fn validate_config_map(config_map: &HashMap<String, String>) -> Result<()> {
        for (key, value) in config_map {
            Self::validate_config_value(key, value)?;
        }
        Ok(())
    }
    
    /// 获取配置验证错误
    pub fn get_validation_errors(config: &CommonConfig) -> Vec<String> {
        let mut errors = Vec::new();
        
        // 验证 NATS 配置
        if let Err(e) = Self::validate_nats_config(&config.nats) {
            errors.push(format!("NATS config error: {}", e));
        }
        
        // 验证 TLS 配置
        if let Err(e) = Self::validate_tls_config(&config.tls) {
            errors.push(format!("TLS config error: {}", e));
        }
        
        // 验证遥测配置
        if let Err(e) = Self::validate_telemetry_config(&config.telemetry) {
            errors.push(format!("Telemetry config error: {}", e));
        }
        
        errors
    }
}
