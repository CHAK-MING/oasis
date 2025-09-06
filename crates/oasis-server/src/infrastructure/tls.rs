use anyhow::Result;
use oasis_core::config::TlsConfig;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tracing::{info};

/// TLS service for managing certificates and TLS configuration
pub struct TlsService {
    config: TlsConfig,
    tls_config: Arc<RwLock<Option<ServerTlsConfig>>>,
    reload_notifier: Arc<broadcast::Sender<()>>,
}

impl TlsService {
    /// Create a new TLS service
    pub fn new(config: TlsConfig, reload_notifier: broadcast::Sender<()>) -> Self {
        Self {
            config,
            tls_config: Arc::new(RwLock::new(None)),
            reload_notifier: Arc::new(reload_notifier),
        }
    }

    /// Create a new TLS service with file paths
    pub async fn new_with_paths(
        cert_path: std::path::PathBuf,
        _key_path: std::path::PathBuf,
        _ca_path: std::path::PathBuf,
    ) -> Result<Self> {
        let config = TlsConfig {
            certs_dir: cert_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .to_path_buf(),
        };

        let (reload_tx, _) = broadcast::channel(1);
        let service = Self::new(config, reload_tx);

        // Load certificates
        service.load_certificates().await?;

        Ok(service)
    }

    /// Load certificates from files
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

        // Load server certificate and private key
        let server_cert = self
            .load_certificate(&server_cert_path.to_string_lossy())
            .await?;
        let server_key = self
            .load_private_key(&server_key_path.to_string_lossy())
            .await?;

        // Create server identity
        let identity = Identity::from_pem(server_cert, server_key);

        // Create TLS configuration
        let mut tls_config = ServerTlsConfig::new().identity(identity);

        // Load CA certificate for client verification
        let ca_cert_data = self
            .load_certificate(&ca_cert_path.to_string_lossy())
            .await?;
        let ca_cert = Certificate::from_pem(ca_cert_data);
        tls_config = tls_config.client_ca_root(ca_cert);
        info!("mTLS enabled - client certificates required");

        // Store the TLS configuration
        {
            let mut tls_config_guard = self.tls_config.write().await;
            *tls_config_guard = Some(tls_config);
        }

        info!("TLS certificates loaded successfully");
        Ok(())
    }

    /// Get TLS reload receiver for monitoring certificate changes
    pub fn get_reload_receiver(&self) -> broadcast::Receiver<()> {
        self.reload_notifier.subscribe()
    }

    /// Reload certificates (for hot reload)
    // pub async fn reload_certificates(&self) -> Result<(), anyhow::Error> {
    //     info!("Reloading TLS certificates...");
    //     self.load_certificates().await?;

    //     // Notify listeners that certificates have been reloaded
    //     if self.reload_notifier.send(()).is_err() {
    //         warn!("No active listeners for TLS reload notifications.");
    //     }

    //     Ok(())
    // }

    /// Get current ServerTlsConfig clone if available
    pub async fn get_server_tls_config(&self) -> Option<ServerTlsConfig> {
        let guard = self.tls_config.read().await;
        guard.clone()
    }

    /// Load certificate from file
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

    /// Load private key from file
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

    // /// Start certificate monitoring for hot reload
    // pub async fn start_certificate_monitoring(&self) -> Result<(), anyhow::Error> {
    //     // Hardcoded check interval: 300 seconds (5 minutes)
    //     let check_interval = Duration::from_secs(300);

    //     let service_clone = self.clone();

    //     tokio::spawn(async move {
    //         let mut interval = interval(check_interval);
    //         loop {
    //             interval.tick().await;

    //             if let Err(e) = service_clone.reload_certificates().await {
    //                 error!("Failed to reload certificates: {}", e);
    //             }
    //         }
    //     });

    //     info!("Certificate monitoring started with interval 300s");
    //     Ok(())
    // }
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
