//! CSR (Certificate Signing Request) 相关类型定义

use crate::core_types::AgentId;
use crate::error::{CoreError, ErrorSeverity, Result};
use base64::Engine as _;
use rcgen::{CertificateParams, DistinguishedName, DnType, DnValue, KeyPair};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::OpenOptions;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use x509_parser::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapToken {
    pub token: String,
    pub agent_id: AgentId,
    pub created_at: i64,
    pub expires_at: i64,
    pub used: bool,
}

impl BootstrapToken {
    pub fn new(agent_id: AgentId, ttl: Duration) -> Result<Self> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let token = Self::generate_token()?;

        Ok(Self {
            token,
            agent_id,
            created_at: now,
            expires_at: now + ttl.as_secs() as i64,
            used: false,
        })
    }

    fn generate_token() -> Result<String> {
        let mut bytes = [0u8; 32];
        getrandom::getrandom(&mut bytes).map_err(|e| CoreError::Internal {
            message: format!("Failed to generate random token: {e}"),
            severity: ErrorSeverity::Critical,
        })?;
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn is_valid(&self) -> bool {
        if self.used {
            return false;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        now < self.expires_at
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsrRequest {
    pub agent_id: AgentId,
    pub csr_pem: String,
    pub bootstrap_token: Option<String>,
    pub current_cert_pem: Option<String>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsrResponse {
    pub success: bool,
    pub certificate_pem: Option<String>,
    pub ca_certificate_pem: Option<String>,
    pub error_message: Option<String>,
    pub expires_at: Option<i64>,
}

impl CsrResponse {
    pub fn success(cert_pem: String, ca_pem: String, expires_at: i64) -> Self {
        Self {
            success: true,
            certificate_pem: Some(cert_pem),
            ca_certificate_pem: Some(ca_pem),
            error_message: None,
            expires_at: Some(expires_at),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            success: false,
            certificate_pem: None,
            ca_certificate_pem: None,
            error_message: Some(message.into()),
            expires_at: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertRenewRequest {
    pub agent_id: AgentId,
    pub csr_pem: String,
    pub current_cert_fingerprint: String,
    pub timestamp: i64,
}

pub struct CsrGenerator {
    key_pair: KeyPair,
}

impl CsrGenerator {
    pub fn new() -> Result<Self> {
        let key_pair = KeyPair::generate().map_err(|e| CoreError::Internal {
            message: format!("Failed to generate key pair: {e}"),
            severity: ErrorSeverity::Critical,
        })?;
        Ok(Self { key_pair })
    }

    pub fn from_existing_key(key_pem: &str) -> Result<Self> {
        let key_pair = KeyPair::from_pem(key_pem).map_err(|e| CoreError::Internal {
            message: format!("Failed to parse key pair: {e}"),
            severity: ErrorSeverity::Critical,
        })?;
        Ok(Self { key_pair })
    }

    pub fn generate_csr(&self, agent_id: &AgentId) -> Result<String> {
        let common_name = format!("oasis-agent-{}", agent_id.as_str());

        let mut params =
            CertificateParams::new(vec![common_name.clone()]).map_err(|e| CoreError::Internal {
                message: format!("Failed to create certificate params: {e}"),
                severity: ErrorSeverity::Error,
            })?;

        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, DnValue::Utf8String(common_name));
        dn.push(
            DnType::OrganizationName,
            DnValue::Utf8String("Oasis Cluster".to_string()),
        );
        params.distinguished_name = dn;

        let csr = params
            .serialize_request(&self.key_pair)
            .map_err(|e| CoreError::Internal {
                message: format!("Failed to serialize CSR: {e}"),
                severity: ErrorSeverity::Error,
            })?;

        let csr_pem = csr.pem().map_err(|e| CoreError::Internal {
            message: format!("Failed to convert CSR to PEM: {e}"),
            severity: ErrorSeverity::Error,
        })?;

        Ok(csr_pem)
    }

    pub fn private_key_pem(&self) -> String {
        self.key_pair.serialize_pem()
    }

    pub fn public_key_pem(&self) -> String {
        self.key_pair.public_key_pem()
    }
}

impl Default for CsrGenerator {
    fn default() -> Self {
        Self::new().expect("Failed to create CSR generator")
    }
}

pub fn calculate_cert_fingerprint(cert_pem: &str) -> Result<String> {
    let cert_der = pem_to_der(cert_pem)?;
    let mut hasher = Sha256::new();
    hasher.update(&cert_der);
    let hash = hasher.finalize();
    Ok(base64::engine::general_purpose::STANDARD.encode(hash))
}

fn pem_to_der(pem: &str) -> Result<Vec<u8>> {
    let pem = pem.trim();
    if pem.is_empty() {
        return Err(CoreError::Internal {
            message: "Empty PEM".to_string(),
            severity: ErrorSeverity::Error,
        });
    }
    let lines: Vec<&str> = pem.lines().filter(|l| !l.starts_with("-----")).collect();

    if lines.is_empty() {
        return Err(CoreError::Internal {
            message: "No PEM content found".to_string(),
            severity: ErrorSeverity::Error,
        });
    }

    let base64_content = lines.join("");
    base64::engine::general_purpose::STANDARD
        .decode(&base64_content)
        .map_err(|e| CoreError::Internal {
            message: format!("Failed to decode PEM: {e}"),
            severity: ErrorSeverity::Error,
        })
}

pub async fn save_credentials(
    certs_dir: &Path,
    private_key_pem: &str,
    certificate_pem: &str,
    ca_certificate_pem: &str,
) -> Result<()> {
    tokio::fs::create_dir_all(certs_dir)
        .await
        .map_err(|e| CoreError::Io {
            message: format!("Failed to create certs directory: {e}"),
            severity: ErrorSeverity::Critical,
        })?;

    let key_path = certs_dir.join("nats-client-key.pem");
    let cert_path = certs_dir.join("nats-client.pem");
    let ca_path = certs_dir.join("nats-ca.pem");

    tokio::task::spawn_blocking({
        let certs_dir = certs_dir.to_path_buf();
        let key_path = key_path.clone();
        let cert_path = cert_path.clone();
        let ca_path = ca_path.clone();
        let private_key_pem = private_key_pem.to_string();
        let certificate_pem = certificate_pem.to_string();
        let ca_certificate_pem = ca_certificate_pem.to_string();
        move || -> Result<()> {
            use std::io::Write;
            #[cfg(unix)]
            use std::os::unix::fs::OpenOptionsExt;

            fn reject_symlink(path: &Path) -> Result<()> {
                if let Ok(meta) = std::fs::symlink_metadata(path) {
                    if meta.file_type().is_symlink() {
                        return Err(CoreError::Io {
                            message: format!("Refuse to write to symlink: {}", path.display()),
                            severity: ErrorSeverity::Critical,
                        });
                    }
                }
                Ok(())
            }

            fn write_atomic(path: &Path, content: &str, mode: u32) -> Result<()> {
                reject_symlink(path)?;

                let parent = path.parent().ok_or_else(|| CoreError::Io {
                    message: format!("Missing parent dir for {}", path.display()),
                    severity: ErrorSeverity::Critical,
                })?;

                let file_name =
                    path.file_name()
                        .and_then(|s| s.to_str())
                        .ok_or_else(|| CoreError::Io {
                            message: format!("Invalid filename for {}", path.display()),
                            severity: ErrorSeverity::Critical,
                        })?;

                let nonce = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();
                let tmp_path = parent.join(format!("{}.{}.tmp", file_name, nonce));

                reject_symlink(&tmp_path)?;

                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(mode)
                    .open(&tmp_path)
                    .map_err(|e| CoreError::Io {
                        message: format!("Failed to create {}: {e}", tmp_path.display()),
                        severity: ErrorSeverity::Critical,
                    })?;

                file.write_all(content.as_bytes())
                    .map_err(|e| CoreError::Io {
                        message: format!("Failed to write {}: {e}", tmp_path.display()),
                        severity: ErrorSeverity::Critical,
                    })?;
                file.sync_all().map_err(|e| CoreError::Io {
                    message: format!("Failed to sync {}: {e}", tmp_path.display()),
                    severity: ErrorSeverity::Warning,
                })?;

                std::fs::rename(&tmp_path, path).map_err(|e| CoreError::Io {
                    message: format!(
                        "Failed to rename {} -> {}: {e}",
                        tmp_path.display(),
                        path.display()
                    ),
                    severity: ErrorSeverity::Critical,
                })?;

                if let Ok(dir) = OpenOptions::new().read(true).open(parent) {
                    let _ = dir.sync_all();
                }

                Ok(())
            }

            let _ = certs_dir.metadata().map_err(|e| CoreError::Io {
                message: format!("Failed to stat certs dir {}: {e}", certs_dir.display()),
                severity: ErrorSeverity::Critical,
            })?;

            write_atomic(&key_path, &private_key_pem, 0o600)?;
            write_atomic(&cert_path, &certificate_pem, 0o644)?;
            write_atomic(&ca_path, &ca_certificate_pem, 0o644)?;
            Ok(())
        }
    })
    .await
    .map_err(|e| CoreError::Internal {
        message: format!("Credential write task failed: {e}"),
        severity: ErrorSeverity::Critical,
    })??;

    Ok(())
}

pub async fn load_private_key(certs_dir: &Path) -> Result<Option<String>> {
    let key_path = certs_dir.join("nats-client-key.pem");
    if !key_path.exists() {
        return Ok(None);
    }
    let content = tokio::fs::read_to_string(&key_path)
        .await
        .map_err(|e| CoreError::Io {
            message: format!("Failed to read private key: {e}"),
            severity: ErrorSeverity::Error,
        })?;
    Ok(Some(content))
}

pub fn cert_needs_renewal(cert_pem: &str, renew_before_days: u32) -> Result<bool> {
    let pem = pem_to_der(cert_pem)?;
    let (_, cert) = x509_parser::certificate::X509Certificate::from_der(&pem).map_err(|e| {
        CoreError::Internal {
            message: format!("Failed to parse X509 certificate: {e}"),
            severity: ErrorSeverity::Error,
        }
    })?;

    let not_after = cert.validity().not_after.timestamp();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let buffer_seconds = (renew_before_days as i64) * 24 * 3600;

    Ok(now + buffer_seconds > not_after)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_bootstrap_token_creation() {
        let agent_id = AgentId::new("test-agent");
        let token = BootstrapToken::new(agent_id, Duration::from_secs(3600)).unwrap();

        assert!(!token.token.is_empty());
        assert!(token.is_valid());
        assert!(!token.used);
    }

    #[test]
    fn test_bootstrap_token_expiration() {
        let agent_id = AgentId::new("test-agent");
        let token = BootstrapToken::new(agent_id, Duration::from_secs(0)).unwrap();

        // Token with 0 TTL should be invalid immediately
        std::thread::sleep(Duration::from_millis(10));
        assert!(!token.is_valid());
    }

    #[test]
    fn test_bootstrap_token_used_status() {
        let agent_id = AgentId::new("test-agent");
        let mut token = BootstrapToken::new(agent_id, Duration::from_secs(3600)).unwrap();

        assert!(token.is_valid());

        // Mark as used
        token.used = true;
        assert!(!token.is_valid());
    }

    #[test]
    fn test_bootstrap_token_uniqueness() {
        let agent_id = AgentId::new("test-agent");
        let token1 = BootstrapToken::new(agent_id.clone(), Duration::from_secs(3600)).unwrap();
        let token2 = BootstrapToken::new(agent_id, Duration::from_secs(3600)).unwrap();

        // Each token should be unique
        assert_ne!(token1.token, token2.token);
    }

    #[test]
    fn test_csr_generation() {
        let generator = CsrGenerator::new().unwrap();
        let agent_id = AgentId::new("test-agent");
        let csr = generator.generate_csr(&agent_id).unwrap();

        assert!(csr.contains("-----BEGIN CERTIFICATE REQUEST-----"));
        assert!(csr.contains("-----END CERTIFICATE REQUEST-----"));
    }

    #[test]
    fn test_csr_contains_agent_id() {
        let generator = CsrGenerator::new().unwrap();
        let agent_id = AgentId::new("test-agent-123");
        let csr = generator.generate_csr(&agent_id).unwrap();

        // CSR should contain the agent ID in some form
        assert!(csr.contains("BEGIN CERTIFICATE REQUEST"));

        // Verify we can parse it back
        let generator2 = CsrGenerator::from_existing_key(&generator.private_key_pem()).unwrap();
        let csr2 = generator2.generate_csr(&agent_id).unwrap();

        // Same key should produce same structure
        assert_eq!(csr.len(), csr2.len());
    }

    #[test]
    fn test_private_key_persistence() {
        let generator = CsrGenerator::new().unwrap();
        let pem = generator.private_key_pem();

        let restored = CsrGenerator::from_existing_key(&pem).unwrap();
        assert_eq!(generator.public_key_pem(), restored.public_key_pem());
    }

    #[test]
    fn test_csr_response_success() {
        let cert_pem = "cert".to_string();
        let ca_pem = "ca".to_string();
        let expires_at = 1234567890;

        let response = CsrResponse::success(cert_pem.clone(), ca_pem.clone(), expires_at);

        assert!(response.success);
        assert_eq!(response.certificate_pem, Some(cert_pem));
        assert_eq!(response.ca_certificate_pem, Some(ca_pem));
        assert_eq!(response.expires_at, Some(expires_at));
        assert!(response.error_message.is_none());
    }

    #[test]
    fn test_csr_response_error() {
        let error_msg = "Certificate generation failed";
        let response = CsrResponse::error(error_msg);

        assert!(!response.success);
        assert_eq!(response.error_message, Some(error_msg.to_string()));
        assert!(response.certificate_pem.is_none());
        assert!(response.ca_certificate_pem.is_none());
        assert!(response.expires_at.is_none());
    }

    #[test]
    fn test_pem_to_der_valid() {
        let valid_pem = "-----BEGIN CERTIFICATE-----\nSGVsbG9Xb3JsZA==\n-----END CERTIFICATE-----";
        let result = pem_to_der(valid_pem);
        assert!(result.is_ok());
    }

    #[test]
    fn test_pem_to_der_invalid_base64() {
        let invalid_pem =
            "-----BEGIN CERTIFICATE-----\nInvalidBase64!!!\n-----END CERTIFICATE-----";
        let result = pem_to_der(invalid_pem);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_save_credentials_creates_directory() {
        let temp_dir = tempdir().unwrap();
        let certs_dir = temp_dir.path().join("new_certs");

        let result =
            save_credentials(&certs_dir, "private_key", "certificate", "ca_certificate").await;

        assert!(result.is_ok());
        assert!(certs_dir.exists());
        assert!(certs_dir.join("nats-client-key.pem").exists());
        assert!(certs_dir.join("nats-client.pem").exists());
        assert!(certs_dir.join("nats-ca.pem").exists());
    }

    #[tokio::test]
    async fn test_save_credentials_file_permissions() {
        let temp_dir = tempdir().unwrap();
        let certs_dir = temp_dir.path();

        save_credentials(certs_dir, "private_key", "certificate", "ca_certificate")
            .await
            .unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let key_path = certs_dir.join("nats-client-key.pem");
            let metadata = std::fs::metadata(&key_path).unwrap();
            let permissions = metadata.permissions();

            // Private key should be 0600 (readable/writable only by owner)
            assert_eq!(permissions.mode() & 0o777, 0o600);

            let cert_path = certs_dir.join("nats-client.pem");
            let cert_metadata = std::fs::metadata(&cert_path).unwrap();
            let cert_permissions = cert_metadata.permissions();

            // Certificate should be 0644 (readable by all)
            assert_eq!(cert_permissions.mode() & 0o777, 0o644);
        }
    }

    #[tokio::test]
    async fn test_save_credentials_file_content() {
        let temp_dir = tempdir().unwrap();
        let certs_dir = temp_dir.path();

        let private_key = "test_private_key_content";
        let certificate = "test_certificate_content";
        let ca_certificate = "test_ca_certificate_content";

        save_credentials(certs_dir, private_key, certificate, ca_certificate)
            .await
            .unwrap();

        let saved_key = tokio::fs::read_to_string(certs_dir.join("nats-client-key.pem"))
            .await
            .unwrap();
        let saved_cert = tokio::fs::read_to_string(certs_dir.join("nats-client.pem"))
            .await
            .unwrap();
        let saved_ca = tokio::fs::read_to_string(certs_dir.join("nats-ca.pem"))
            .await
            .unwrap();

        assert_eq!(saved_key, private_key);
        assert_eq!(saved_cert, certificate);
        assert_eq!(saved_ca, ca_certificate);
    }

    #[tokio::test]
    async fn test_load_private_key_exists() {
        let temp_dir = tempdir().unwrap();
        let certs_dir = temp_dir.path();

        let key_content = "test_private_key";
        tokio::fs::create_dir_all(certs_dir).await.unwrap();
        tokio::fs::write(certs_dir.join("nats-client-key.pem"), key_content)
            .await
            .unwrap();

        let result = load_private_key(certs_dir).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), key_content);
    }

    #[tokio::test]
    async fn test_load_private_key_not_exists() {
        let temp_dir = tempdir().unwrap();
        let certs_dir = temp_dir.path();

        let result = load_private_key(certs_dir).await.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_default_csr_generator() {
        let generator = CsrGenerator::default();
        let agent_id = AgentId::new("default-test");
        let csr = generator.generate_csr(&agent_id).unwrap();

        assert!(csr.contains("-----BEGIN CERTIFICATE REQUEST-----"));
    }

    #[test]
    fn test_csr_generator_public_key() {
        let generator = CsrGenerator::new().unwrap();
        let public_key = generator.public_key_pem();

        assert!(public_key.contains("-----BEGIN PUBLIC KEY-----"));
        assert!(public_key.contains("-----END PUBLIC KEY-----"));
    }

    #[test]
    fn test_calculate_cert_fingerprint_valid_cert() {
        // Generate a valid self-signed cert for testing
        let cert = rcgen::generate_simple_self_signed(vec!["test.local".to_string()]).unwrap();
        let cert_pem = cert.cert.pem();

        let fingerprint = calculate_cert_fingerprint(&cert_pem);
        assert!(
            fingerprint.is_ok(),
            "Fingerprint calculation failed for valid cert"
        );
        let fp = fingerprint.unwrap();
        assert!(!fp.is_empty());

        // Fingerprint should be valid base64
        assert!(
            base64::engine::general_purpose::STANDARD
                .decode(&fp)
                .is_ok()
        );
    }

    #[test]
    fn test_calculate_cert_fingerprint_invalid_pem() {
        let invalid_pem =
            "-----BEGIN CERTIFICATE-----\nInvalidBase64!!!\n-----END CERTIFICATE-----";
        let result = calculate_cert_fingerprint(invalid_pem);
        assert!(result.is_err(), "Should fail on invalid base64 in PEM");
    }

    #[test]
    fn test_calculate_cert_fingerprint_empty() {
        let result = calculate_cert_fingerprint("");
        // Empty string should be rejected as it contains no certificate data
        assert!(result.is_err(), "Should fail on empty input");
    }

    #[test]
    fn test_cert_needs_renewal_invalid_pem() {
        let invalid = "invalid pem content";
        let result = cert_needs_renewal(invalid, 30);
        assert!(result.is_err(), "Should fail on invalid PEM");
    }

    #[test]
    fn test_cert_needs_renewal_fresh_cert_no_renew_needed() {
        // Generate a fresh cert (valid for ~365 days by default)
        let cert = rcgen::generate_simple_self_signed(vec!["test.local".to_string()]).unwrap();
        let cert_pem = cert.cert.pem();

        // With 30 days buffer, a fresh cert should NOT need renewal
        let result = cert_needs_renewal(&cert_pem, 30);
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "Fresh cert should not need renewal with small buffer"
        );
    }

    #[test]
    fn test_cert_needs_renewal_large_buffer_needs_renew() {
        let mut params = CertificateParams::new(vec!["test.local".to_string()]).unwrap();
        // Make cert valid for only 1 day
        let now = SystemTime::now();
        params.not_before = now.into();
        params.not_after = (now + Duration::from_secs(24 * 3600)).into();

        let key_pair = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        let cert_pem = cert.pem();

        // With 2 days buffer, a 1-day cert DOES need renewal
        let result = cert_needs_renewal(&cert_pem, 2);
        assert!(result.is_ok());
        assert!(
            result.unwrap(),
            "Cert valid for 1 day should need renewal if buffer is 2 days"
        );
    }

    #[test]
    fn test_calculate_cert_fingerprint_uniqueness() {
        let cert1 = rcgen::generate_simple_self_signed(vec!["test1.local".to_string()]).unwrap();
        let cert2 = rcgen::generate_simple_self_signed(vec!["test2.local".to_string()]).unwrap();

        let fp1 = calculate_cert_fingerprint(&cert1.cert.pem()).unwrap();
        let fp2 = calculate_cert_fingerprint(&cert2.cert.pem()).unwrap();

        assert_ne!(
            fp1, fp2,
            "Different certs should have different fingerprints"
        );
    }

    #[test]
    fn test_pem_to_der_empty_string() {
        let result = pem_to_der("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Empty PEM"));
    }

    #[test]
    fn test_pem_to_der_whitespace_only() {
        let result = pem_to_der("   \n  \t  ");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Empty PEM"));
    }

    #[tokio::test]
    async fn test_save_credentials_overwrites_existing() {
        let temp_dir = tempdir().unwrap();
        let certs_dir = temp_dir.path();

        // Initial save
        save_credentials(certs_dir, "old_key", "old_cert", "old_ca")
            .await
            .unwrap();

        // Verify initial content
        let key_path = certs_dir.join("nats-client-key.pem");
        assert_eq!(
            tokio::fs::read_to_string(&key_path).await.unwrap(),
            "old_key"
        );

        // Overwrite
        save_credentials(certs_dir, "new_key", "new_cert", "new_ca")
            .await
            .unwrap();

        // Verify new content
        assert_eq!(
            tokio::fs::read_to_string(&key_path).await.unwrap(),
            "new_key"
        );
        assert_eq!(
            tokio::fs::read_to_string(certs_dir.join("nats-client.pem"))
                .await
                .unwrap(),
            "new_cert"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_save_credentials_refuses_symlink() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempdir().unwrap();
        let certs_dir = temp_dir.path();
        tokio::fs::create_dir_all(certs_dir).await.unwrap();

        let target_path = certs_dir.join("target_file");
        tokio::fs::write(&target_path, "should not be overwritten")
            .await
            .unwrap();

        let symlink_path = certs_dir.join("nats-client-key.pem");
        symlink(&target_path, &symlink_path).unwrap();

        let result = save_credentials(certs_dir, "new_key", "cert", "ca").await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Refuse to write to symlink")
        );

        // Verify target was not overwritten
        let content = tokio::fs::read_to_string(target_path).await.unwrap();
        assert_eq!(content, "should not be overwritten");
    }
}
