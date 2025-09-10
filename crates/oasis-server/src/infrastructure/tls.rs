use anyhow::Result;
use oasis_core::config::TlsConfig;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tracing::{info};

/// TLS 服务
pub struct TlsService {
    config: TlsConfig,
    tls_config: Arc<RwLock<Option<ServerTlsConfig>>>,
    reload_notifier: Arc<broadcast::Sender<()>>,
}

impl TlsService {
    /// 创建一个新的 TLS 服务
    pub fn new(config: TlsConfig, reload_notifier: broadcast::Sender<()>) -> Self {
        Self {
            config,
            tls_config: Arc::new(RwLock::new(None)),
            reload_notifier: Arc::new(reload_notifier),
        }
    }

    /// 创建一个新的 TLS 服务
    pub async fn new_with_paths(
        cert_path: std::path::PathBuf,
    ) -> Result<Self> {
        let config = TlsConfig {
            certs_dir: cert_path
                .to_path_buf(),
        };

        let (reload_tx, _) = broadcast::channel(1);
        let service = Self::new(config, reload_tx);

        // 加载证书
        service.load_certificates().await?;

        Ok(service)
    }

    /// 从文件加载证书
    pub async fn load_certificates(&self) -> Result<(), anyhow::Error> {
        let server_cert_path = self.config.grpc_server_cert_path();
        let server_key_path = self.config.grpc_server_key_path();
        let ca_cert_path = self.config.grpc_ca_path();

        info!(
            server_cert = %server_cert_path.display(),
            server_key = %server_key_path.display(),
            ca_cert = %ca_cert_path.display(),
            "Loading TLS certificates..."
        );

        // 加载服务器证书和私钥
        let server_cert = self
            .load_certificate(&server_cert_path.to_string_lossy())
            .await?;
        let server_key = self
            .load_private_key(&server_key_path.to_string_lossy())
            .await?;

        // 创建服务器身份
        let identity = Identity::from_pem(server_cert, server_key);

        // 创建 TLS 配置
        let mut tls_config = ServerTlsConfig::new().identity(identity);

        // 加载 CA 证书用于客户端验证
        let ca_cert_data = self
            .load_certificate(&ca_cert_path.to_string_lossy())
            .await?;
        let ca_cert = Certificate::from_pem(ca_cert_data);
        tls_config = tls_config.client_ca_root(ca_cert);
        info!("mTLS enabled - client certificates required");

        // 存储 TLS 配置
        {
            let mut tls_config_guard = self.tls_config.write().await;
            *tls_config_guard = Some(tls_config);
        }

        info!("TLS certificates loaded successfully");
        Ok(())
    }


    /// 获取当前 ServerTlsConfig 克隆如果可用
    pub async fn get_server_tls_config(&self) -> Option<ServerTlsConfig> {
        let guard = self.tls_config.read().await;
        guard.clone()
    }

    /// 从文件加载证书
    async fn load_certificate(&self, path: &str) -> Result<Vec<u8>, anyhow::Error> {
        let cert_path = PathBuf::from(path);
        if !cert_path.exists() {
            return Err(anyhow::Error::msg(format!(
                "Certificate file not found: {}",
                path
            )));
        }

        fs::read(cert_path).await.map_err(|e| {
            anyhow::Error::msg(format!("Failed to read certificate file {}: {}", path, e))
        })
    }

    /// 从文件加载私钥
    async fn load_private_key(&self, path: &str) -> Result<Vec<u8>, anyhow::Error> {
        let key_path = PathBuf::from(path);
        if !key_path.exists() {
            return Err(anyhow::Error::msg(format!(
                "Private key file not found: {}",
                path
            )));
        }

        fs::read(key_path).await.map_err(|e| {
            anyhow::Error::msg(format!("Failed to read private key file {}: {}", path, e))
        })
    }
}

impl Clone for TlsService {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            tls_config: Arc::clone(&self.tls_config),
            reload_notifier: Arc::clone(&self.reload_notifier),
        }
    }
}
