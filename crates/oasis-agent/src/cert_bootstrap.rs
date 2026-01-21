
//! Agent 证书引导流程

use oasis_core::core_types::AgentId;
use oasis_core::csr_types::{cert_needs_renewal, CsrGenerator, CsrRequest, CsrResponse, save_credentials, load_private_key};
use oasis_core::error::{CoreError, ErrorSeverity, Result};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

pub struct CertBootstrap {
    agent_id: AgentId,
    certs_dir: std::path::PathBuf,
    nats_url: String,
    bootstrap_token: Option<String>,
    renew_before_days: u32,
}

impl CertBootstrap {
    pub fn new(
        agent_id: AgentId,
        certs_dir: impl AsRef<Path>,
        nats_url: String,
        bootstrap_token: Option<String>,
    ) -> Self {
        Self {
            agent_id,
            certs_dir: certs_dir.as_ref().to_path_buf(),
            nats_url,
            bootstrap_token,
            renew_before_days: 30,
        }
    }

    pub fn with_renew_before_days(mut self, days: u32) -> Self {
        self.renew_before_days = days;
        self
    }

    pub fn certs_exist(&self) -> bool {
        let cert_path = self.certs_dir.join("nats-client.pem");
        let key_path = self.certs_dir.join("nats-client-key.pem");
        let ca_path = self.certs_dir.join("nats-ca.pem");
        cert_path.exists() && key_path.exists() && ca_path.exists()
    }

    pub async fn bootstrap_if_needed(&self) -> Result<bool> {
        if self.certs_exist() {
            info!("Certificates already exist, skipping bootstrap");
            return Ok(false);
        }

        let token = self.bootstrap_token.as_ref().ok_or_else(|| CoreError::Config {
            message: "No certificates found and no bootstrap token provided. Set OASIS_BOOTSTRAP_TOKEN environment variable.".to_string(),
            severity: ErrorSeverity::Critical,
        })?;

        info!("Certificates not found, starting bootstrap process");
        self.request_certificate_with_token(token).await?;
        Ok(true)
    }

    pub async fn renew_if_needed(&self) -> Result<bool> {
        if !self.certs_exist() {
            return Ok(false);
        }

        let cert_path = self.certs_dir.join("nats-client.pem");
        let cert_pem = tokio::fs::read_to_string(&cert_path).await.map_err(|e| {
            CoreError::Io {
                message: format!("Failed to read certificate: {e}"),
                severity: ErrorSeverity::Error,
            }
        })?;

        if !cert_needs_renewal(&cert_pem, self.renew_before_days)? {
            return Ok(false);
        }

        info!("Certificate expires within {} days, initiating renewal", self.renew_before_days);
        self.renew_certificate().await?;
        Ok(true)
    }

    async fn request_certificate_with_token(&self, bootstrap_token: &str) -> Result<()> {
        let generator = CsrGenerator::new()?;
        let csr_pem = generator.generate_csr(&self.agent_id)?;
        let private_key_pem = generator.private_key_pem();

        let request = self.build_csr_request(csr_pem, Some(bootstrap_token.to_string()), None);
        let response = self.send_csr_request(request).await?;
        self.save_response(&private_key_pem, response).await
    }

    async fn renew_certificate(&self) -> Result<()> {
        let existing_key = load_private_key(&self.certs_dir).await?.ok_or_else(|| {
            CoreError::Internal {
                message: "Cannot renew: private key not found".to_string(),
                severity: ErrorSeverity::Error,
            }
        })?;

        let generator = CsrGenerator::from_existing_key(&existing_key)?;
        let csr_pem = generator.generate_csr(&self.agent_id)?;

        let cert_path = self.certs_dir.join("nats-client.pem");
        let current_cert_pem = tokio::fs::read_to_string(&cert_path).await.map_err(|e| {
            CoreError::Io {
                message: format!("Failed to read certificate: {e}"),
                severity: ErrorSeverity::Error,
            }
        })?;

        let request = self.build_csr_request(csr_pem, None, Some(current_cert_pem));
        let response = self.send_csr_request(request).await?;
        self.save_response(&existing_key, response).await
    }

    fn build_csr_request(
        &self,
        csr_pem: String,
        bootstrap_token: Option<String>,
        current_cert_pem: Option<String>,
    ) -> CsrRequest {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        CsrRequest {
            agent_id: self.agent_id.clone(),
            csr_pem,
            bootstrap_token,
            current_cert_pem,
            timestamp: now,
        }
    }

    async fn send_csr_request(&self, request: CsrRequest) -> Result<CsrResponse> {
        if !self.nats_url.starts_with("tls://") {
            return Err(CoreError::Config {
                message: "Certificate bootstrap requires TLS (tls://)".to_string(),
                severity: ErrorSeverity::Critical,
            });
        }

        let ca_path = self.certs_dir.join("nats-ca.pem");
        if !ca_path.exists() {
            return Err(CoreError::Config {
                message: format!("Missing CA certificate: {}", ca_path.display()),
                severity: ErrorSeverity::Critical,
            });
        }

        let connect_opts = async_nats::ConnectOptions::new()
            .require_tls(true)
            .add_root_certificates(ca_path);

        info!("Connecting to NATS for certificate request: {}", self.nats_url);
        
        let client = async_nats::connect_with_options(&self.nats_url, connect_opts)
            .await
            .map_err(|e| {
                CoreError::Internal {
                    message: format!("Failed to connect to NATS for bootstrap: {e}"),
                    severity: ErrorSeverity::Critical,
                }
            })?;

        let request_json = serde_json::to_string(&request).map_err(|e| CoreError::Internal {
            message: format!("Failed to serialize CSR request: {e}"),
            severity: ErrorSeverity::Error,
        })?;

        let subject = "oasis.ca.csr";
        let response = client
            .request(subject.to_string(), request_json.into())
            .await
            .map_err(|e| CoreError::Internal {
                message: format!("Failed to send CSR request: {e}"),
                severity: ErrorSeverity::Error,
            })?;

        let response: CsrResponse = serde_json::from_slice(&response.payload).map_err(|e| {
            CoreError::Internal {
                message: format!("Failed to parse CSR response: {e}"),
                severity: ErrorSeverity::Error,
            }
        })?;

        if !response.success {
            return Err(CoreError::Internal {
                message: format!(
                    "Certificate request failed: {}",
                    response.error_message.unwrap_or_else(|| "Unknown error".to_string())
                ),
                severity: ErrorSeverity::Error,
            });
        }

        Ok(response)
    }

    async fn save_response(&self, private_key_pem: &str, response: CsrResponse) -> Result<()> {
        let cert_pem = response.certificate_pem.ok_or_else(|| CoreError::Internal {
            message: "Response missing certificate".to_string(),
            severity: ErrorSeverity::Error,
        })?;

        let ca_pem = response.ca_certificate_pem.ok_or_else(|| CoreError::Internal {
            message: "Response missing CA certificate".to_string(),
            severity: ErrorSeverity::Error,
        })?;

        save_credentials(&self.certs_dir, private_key_pem, &cert_pem, &ca_pem).await?;

        info!("Certificate saved successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_certs_exist() {
        let dir = tempdir().unwrap();
        let agent_id = AgentId::new("test-agent");
        let bootstrap = CertBootstrap::new(
            agent_id,
            dir.path(),
            "nats://localhost:4222".to_string(),
            None,
        );

        assert!(!bootstrap.certs_exist());

        fs::write(dir.path().join("nats-client.pem"), "cert").unwrap();
        fs::write(dir.path().join("nats-client-key.pem"), "key").unwrap();
        fs::write(dir.path().join("nats-ca.pem"), "ca").unwrap();

        assert!(bootstrap.certs_exist());
    }

    #[tokio::test]
    async fn test_certs_exist_partial_files() {
        let dir = tempdir().unwrap();
        let agent_id = AgentId::new("test-agent");
        let bootstrap = CertBootstrap::new(
            agent_id,
            dir.path(),
            "nats://localhost:4222".to_string(),
            None,
        );

        fs::write(dir.path().join("nats-client.pem"), "cert").unwrap();
        fs::write(dir.path().join("nats-client-key.pem"), "key").unwrap();

        assert!(!bootstrap.certs_exist());

        fs::write(dir.path().join("nats-ca.pem"), "ca").unwrap();
        assert!(bootstrap.certs_exist());
    }

    #[tokio::test]
    async fn test_renew_if_needed_no_certs() {
        let dir = tempdir().unwrap();
        let agent_id = AgentId::new("test-agent");
        let bootstrap = CertBootstrap::new(
            agent_id,
            dir.path(),
            "nats://localhost:4222".to_string(),
            None,
        );

        let result = bootstrap.renew_if_needed().await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_bootstrap_if_needed_no_token() {
        let dir = tempdir().unwrap();
        let agent_id = AgentId::new("test-agent");
        let bootstrap = CertBootstrap::new(
            agent_id,
            dir.path(),
            "nats://localhost:4222".to_string(),
            None,
        );

        let result = bootstrap.bootstrap_if_needed().await;
        assert!(result.is_err());
        
        let error = result.unwrap_err();
        assert!(matches!(error, CoreError::Config { .. }));
    }

    #[tokio::test]
    async fn test_bootstrap_if_needed_certs_already_exist() {
        let dir = tempdir().unwrap();
        let agent_id = AgentId::new("test-agent");
        
        fs::write(dir.path().join("nats-client.pem"), "cert").unwrap();
        fs::write(dir.path().join("nats-client-key.pem"), "key").unwrap();
        fs::write(dir.path().join("nats-ca.pem"), "ca").unwrap();

        let bootstrap = CertBootstrap::new(
            agent_id,
            dir.path(),
            "nats://localhost:4222".to_string(),
            Some("token".to_string()),
        );

        let result = bootstrap.bootstrap_if_needed().await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_build_csr_request() {
        let dir = tempdir().unwrap();
        let agent_id = AgentId::new("test-agent");
        let bootstrap = CertBootstrap::new(
            agent_id.clone(),
            dir.path(),
            "nats://localhost:4222".to_string(),
            None,
        );

        let request = bootstrap.build_csr_request(
            "csr_content".to_string(),
            Some("token123".to_string()),
            None,
        );
        
        assert_eq!(request.agent_id, agent_id);
        assert_eq!(request.csr_pem, "csr_content");
        assert_eq!(request.bootstrap_token, Some("token123".to_string()));
        assert!(request.timestamp > 0);
    }

    #[test]
    fn test_build_csr_request_with_current_cert() {
        let dir = tempdir().unwrap();
        let agent_id = AgentId::new("test-agent");
        let bootstrap = CertBootstrap::new(
            agent_id.clone(),
            dir.path(),
            "nats://localhost:4222".to_string(),
            None,
        );

        let current_cert = "current_cert_pem".to_string();
        let request = bootstrap.build_csr_request(
            "new_csr_pem".to_string(),
            None,
            Some(current_cert.clone()),
        );
        
        assert_eq!(request.agent_id, agent_id);
        assert_eq!(request.csr_pem, "new_csr_pem");
        assert!(request.bootstrap_token.is_none());
        assert_eq!(request.current_cert_pem, Some(current_cert));
        assert!(request.timestamp > 0);
    }

    #[test]
    fn test_with_renew_before_days() {
        let dir = tempdir().unwrap();
        let agent_id = AgentId::new("test-agent");
        let bootstrap = CertBootstrap::new(
            agent_id,
            dir.path(),
            "nats://localhost:4222".to_string(),
            None,
        ).with_renew_before_days(7);

        assert_eq!(bootstrap.renew_before_days, 7);
    }

    #[test]
    fn test_with_renew_before_days_chaining() {
        let dir = tempdir().unwrap();
        let agent_id = AgentId::new("test-agent");
        let bootstrap = CertBootstrap::new(
            agent_id,
            dir.path(),
            "nats://localhost:4222".to_string(),
            Some("token".to_string()),
        )
        .with_renew_before_days(14);

        assert_eq!(bootstrap.renew_before_days, 14);
        assert_eq!(bootstrap.bootstrap_token, Some("token".to_string()));
    }

    #[tokio::test]
    async fn test_bootstrap_new_construction() {
        let dir = tempdir().unwrap();
        let agent_id = AgentId::new("test-agent-construct");
        let nats_url = "tls://localhost:4222".to_string();
        let token = Some("bootstrap-token".to_string());

        let bootstrap = CertBootstrap::new(
            agent_id.clone(),
            dir.path(),
            nats_url.clone(),
            token.clone(),
        );

        assert_eq!(bootstrap.agent_id, agent_id);
        assert_eq!(bootstrap.certs_dir, dir.path().to_path_buf());
        assert_eq!(bootstrap.nats_url, nats_url);
        assert_eq!(bootstrap.bootstrap_token, token);
        assert_eq!(bootstrap.renew_before_days, 30);
    }

    #[tokio::test]
    async fn test_multiple_bootstrap_instances() {
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();
        
        let agent_id1 = AgentId::new("agent-1");
        let agent_id2 = AgentId::new("agent-2");

        let bootstrap1 = CertBootstrap::new(
            agent_id1,
            dir1.path(),
            "nats://localhost:4222".to_string(),
            None,
        );

        let bootstrap2 = CertBootstrap::new(
            agent_id2,
            dir2.path(),
            "nats://localhost:4222".to_string(),
            None,
        );

        assert_ne!(bootstrap1.certs_dir, bootstrap2.certs_dir);
        assert_ne!(bootstrap1.agent_id, bootstrap2.agent_id);
    }

    #[test]
    fn test_csr_request_timestamp_accuracy() {
        let dir = tempdir().unwrap();
        let agent_id = AgentId::new("test-agent");
        let bootstrap = CertBootstrap::new(
            agent_id,
            dir.path(),
            "nats://localhost:4222".to_string(),
            None,
        );

        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let request = bootstrap.build_csr_request(
            "csr".to_string(),
            None,
            None,
        );

        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        assert!(request.timestamp >= before);
        assert!(request.timestamp <= after);
    }
}
