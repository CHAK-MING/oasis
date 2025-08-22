use crate::config::GrpcTlsConfig;
use oasis_core::error::CoreError;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tonic::transport::{Identity, ServerTlsConfig};

/// TLS service for managing certificates and TLS configuration
pub struct TlsService {
    config: GrpcTlsConfig,
    tls_config: Arc<RwLock<Option<ServerTlsConfig>>>,
    reload_notifier: Arc<broadcast::Sender<()>>,
}

impl TlsService {
    /// Create a new TLS service
    pub fn new(config: GrpcTlsConfig, reload_notifier: broadcast::Sender<()>) -> Self {
        Self {
            config,
            tls_config: Arc::new(RwLock::new(None)),
            reload_notifier: Arc::new(reload_notifier),
        }
    }

    /// Load certificates from files
    pub async fn load_certificates(&self) -> Result<(), CoreError> {
        if !self.config.enabled {
            tracing::info!("TLS is disabled");
            return Ok(());
        }

        tracing::info!("Loading TLS certificates...");

        // Load server certificate and private key
        let server_cert = self.load_certificate(&self.config.server_cert)?;
        let server_key = self.load_private_key(&self.config.server_key)?;

        // Create server identity
        let identity = Identity::from_pem(server_cert, server_key);

        // Create TLS configuration
        let mut tls_config = ServerTlsConfig::new().identity(identity);

        // Load CA certificate for client verification if required
        if self.config.require_client_cert {
            let ca_cert_data = self.load_certificate(&self.config.ca_cert)?;
            let ca_cert = tonic::transport::Certificate::from_pem(ca_cert_data);
            tls_config = tls_config.client_ca_root(ca_cert);
            tracing::info!("mTLS enabled - client certificates required");
        } else {
            tracing::info!("TLS enabled - client certificates optional");
        }

        // Store the TLS configuration
        {
            let mut tls_config_guard = self.tls_config.write().await;
            *tls_config_guard = Some(tls_config);
        }

        tracing::info!("TLS certificates loaded successfully");
        Ok(())
    }

    /// Get the current TLS configuration
    pub async fn get_tls_config(&self) -> Option<ServerTlsConfig> {
        let tls_config_guard = self.tls_config.read().await;
        tls_config_guard.clone()
    }

    /// Reload certificates (for hot reload)
    pub async fn reload_certificates(&self) -> Result<(), CoreError> {
        tracing::info!("Reloading TLS certificates...");
        self.load_certificates().await?;

        // Notify listeners that certificates have been reloaded
        if self.reload_notifier.send(()).is_err() {
            tracing::warn!("No active listeners for TLS reload notifications.");
        }

        Ok(())
    }

    /// Load certificate from file
    fn load_certificate(&self, path: &str) -> Result<Vec<u8>, CoreError> {
        let cert_path = Path::new(path);
        if !cert_path.exists() {
            return Err(CoreError::Config {
                message: format!("Certificate file not found: {}", path),
            });
        }

        fs::read(cert_path).map_err(|e| CoreError::Config {
            message: format!("Failed to read certificate file {}: {}", path, e),
        })
    }

    /// Load private key from file
    fn load_private_key(&self, path: &str) -> Result<Vec<u8>, CoreError> {
        let key_path = Path::new(path);
        if !key_path.exists() {
            return Err(CoreError::Config {
                message: format!("Private key file not found: {}", path),
            });
        }

        fs::read(key_path).map_err(|e| CoreError::Config {
            message: format!("Failed to read private key file {}: {}", path, e),
        })
    }

    /// Start certificate monitoring for hot reload
    pub async fn start_certificate_monitoring(&self) -> Result<(), CoreError> {
        if !self.config.enabled || self.config.cert_check_interval_sec == 0 {
            return Ok(());
        }

        let service_clone = self.clone();
        let check_interval = std::time::Duration::from_secs(self.config.cert_check_interval_sec);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(check_interval);
            loop {
                interval.tick().await;

                if let Err(e) = service_clone.reload_certificates().await {
                    tracing::error!("Failed to reload certificates: {}", e);
                }
            }
        });

        tracing::info!(
            "Certificate monitoring started with interval {}s",
            self.config.cert_check_interval_sec
        );
        Ok(())
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
