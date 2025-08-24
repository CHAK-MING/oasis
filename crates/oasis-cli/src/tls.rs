use crate::config::unified::GrpcTlsConfig;
use anyhow::{Context, Result};
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;
use tonic::transport::{Certificate, ClientTlsConfig, Identity};

/// TLS client service for gRPC connections
pub struct TlsClientService {
    config: GrpcTlsConfig,
}

impl TlsClientService {
    /// Create a new TLS client service
    pub fn new(config: GrpcTlsConfig) -> Self {
        Self { config }
    }

    /// Create a ClientTlsConfig for gRPC connection
    pub fn create_client_tls_config(&self) -> Result<ClientTlsConfig> {
        // Load CA certificate PEM (and validate via rustls-pemfile)
        let ca_pem = fs::read(&self.config.ca_cert)
            .with_context(|| format!("Failed to read CA cert: {}", &self.config.ca_cert))?;
        // Validate parse
        let mut ca_reader = BufReader::new(File::open(&self.config.ca_cert)?);
        let _ = rustls_pemfile::certs(&mut ca_reader)
            .map_err(|_| anyhow::anyhow!("Failed to parse CA cert: {}", &self.config.ca_cert))?;
        let ca_cert = Certificate::from_pem(ca_pem);

        // Start with basic TLS config
        let mut tls_config = ClientTlsConfig::new()
            .ca_certificate(ca_cert)
            .domain_name("localhost"); // Use localhost as domain name for local development

        // Load client certificate and key for mutual TLS if files exist
        if Path::new(&self.config.client_cert).exists()
            && Path::new(&self.config.client_key).exists()
        {
            // Read PEM as-is for Identity
            let client_cert_pem = fs::read(&self.config.client_cert).with_context(|| {
                format!("Failed to read client cert: {}", &self.config.client_cert)
            })?;
            let client_key_pem = fs::read(&self.config.client_key).with_context(|| {
                format!("Failed to read client key: {}", &self.config.client_key)
            })?;

            // Validate parse using rustls-pemfile (PKCS8 or RSA)
            let mut cert_reader = BufReader::new(File::open(&self.config.client_cert)?);
            let _ = rustls_pemfile::certs(&mut cert_reader).map_err(|_| {
                anyhow::anyhow!("Failed to parse client cert: {}", &self.config.client_cert)
            })?;
            let mut key_reader = BufReader::new(File::open(&self.config.client_key)?);
            let pkcs8_ok = rustls_pemfile::pkcs8_private_keys(&mut key_reader)
                .map(|v| !v.is_empty())
                .unwrap_or(false);
            let rsa_ok = if !pkcs8_ok {
                let mut key_reader = BufReader::new(File::open(&self.config.client_key)?);
                rustls_pemfile::rsa_private_keys(&mut key_reader)
                    .map(|v| !v.is_empty())
                    .unwrap_or(false)
            } else {
                true
            };
            if !pkcs8_ok && !rsa_ok {
                return Err(anyhow::anyhow!(
                    "Failed to parse private key: {}",
                    &self.config.client_key
                ));
            }

            let identity = Identity::from_pem(client_cert_pem, client_key_pem);
            tls_config = tls_config.identity(identity);
            tracing::info!("mTLS enabled - using client certificate for authentication");
        } else {
            tracing::info!("Using server-only TLS - no client certificate provided");
        }

        Ok(tls_config)
    }

    // helper loaders removed; we validate using rustls-pemfile but keep PEM bytes intact for tonic
}
