//! CA Service - 处理证书签名请求

use oasis_core::core_types::AgentId;
use oasis_core::csr_types::{BootstrapToken, CsrRequest, CsrResponse};
use oasis_core::error::{CoreError, ErrorSeverity, Result};
use rcgen::{
    CertificateParams, CertificateSigningRequestParams, DistinguishedName, DnType, DnValue,
    ExtendedKeyUsagePurpose, Issuer, KeyPair, KeyUsagePurpose, SanType,
};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use x509_parser::prelude::*;

pub struct CaService {
    ca_issuer: Issuer<'static, KeyPair>,
    ca_cert_pem: String,
    ca_cert_der: Vec<u8>,
    token_store: Arc<RwLock<TokenStore>>,
    cert_validity_hours: u64,
}

struct TokenStore {
    tokens: std::collections::HashMap<String, BootstrapToken>,
}

impl TokenStore {
    fn new() -> Self {
        Self {
            tokens: std::collections::HashMap::new(),
        }
    }

    fn add(&mut self, token: BootstrapToken) {
        self.tokens.insert(token.token.clone(), token);
    }

    fn validate_and_consume(&mut self, token_str: &str) -> Option<AgentId> {
        if let Some(token) = self.tokens.get_mut(token_str) {
            if token.is_valid() {
                token.used = true;
                return Some(token.agent_id.clone());
            }
        }
        None
    }

    fn cleanup_expired(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        self.tokens.retain(|_, t| t.expires_at > now);
    }
}

impl CaService {
    pub async fn new(
        ca_cert_path: impl AsRef<Path>,
        ca_key_path: impl AsRef<Path>,
        cert_validity_hours: u64,
    ) -> Result<Self> {
        let ca_key_pem = tokio::fs::read_to_string(ca_key_path.as_ref())
            .await
            .map_err(|e| CoreError::Io {
                message: format!("Failed to read CA key: {e}"),
                severity: ErrorSeverity::Critical,
            })?;

        let ca_cert_pem = tokio::fs::read_to_string(ca_cert_path.as_ref())
            .await
            .map_err(|e| CoreError::Io {
                message: format!("Failed to read CA cert: {e}"),
                severity: ErrorSeverity::Critical,
            })?;

        let ca_key_pair = KeyPair::from_pem(&ca_key_pem).map_err(|e| CoreError::Internal {
            message: format!("Failed to parse CA key: {e}"),
            severity: ErrorSeverity::Critical,
        })?;

        let ca_pem = ::pem::parse(ca_cert_pem.as_bytes()).map_err(|e| CoreError::Internal {
            message: format!("Failed to parse CA cert PEM: {e}"),
            severity: ErrorSeverity::Critical,
        })?;
        let ca_cert_der = ca_pem.contents().to_vec();

        let (_, ca_x509) = X509Certificate::from_der(&ca_cert_der).map_err(|e| CoreError::Internal {
            message: format!("Failed to parse CA cert DER: {e}"),
            severity: ErrorSeverity::Critical,
        })?;

        let (dn, _) = build_dn_from_x509_subject(&ca_x509);

        let mut ca_params = CertificateParams::new(vec![]).map_err(|e| CoreError::Internal {
            message: format!("Failed to create CA params: {e}"),
            severity: ErrorSeverity::Critical,
        })?;
        ca_params.distinguished_name = dn;
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);

        let ca_issuer = Issuer::new(ca_params, ca_key_pair);

        info!("CA Service initialized");

        Ok(Self {
            ca_issuer,
            ca_cert_pem,
            ca_cert_der,
            token_store: Arc::new(RwLock::new(TokenStore::new())),
            cert_validity_hours,
        })
    }

    pub async fn create_bootstrap_token(
        &self,
        agent_id: AgentId,
        ttl: StdDuration,
    ) -> Result<BootstrapToken> {
        let token = BootstrapToken::new(agent_id.clone(), ttl)?;
        self.token_store.write().await.add(token.clone());
        info!(agent_id = %agent_id.as_str(), "Created bootstrap token");
        Ok(token)
    }

    pub async fn handle_csr_request(&self, request: CsrRequest) -> CsrResponse {
        if let Some(token_str) = &request.bootstrap_token {
            let maybe_agent_id = self
                .token_store
                .write()
                .await
                .validate_and_consume(token_str);

            match maybe_agent_id {
                Some(agent_id) => {
                    if agent_id != request.agent_id {
                        warn!(
                            expected = %agent_id.as_str(),
                            got = %request.agent_id.as_str(),
                            "Agent ID mismatch in CSR request"
                        );
                        return CsrResponse::error("Agent ID mismatch with bootstrap token");
                    }
                }
                None => {
                    warn!("Invalid or expired bootstrap token");
                    return CsrResponse::error("Invalid or expired bootstrap token");
                }
            }
        } else {
            let current_cert_pem = match &request.current_cert_pem {
                Some(pem) => pem,
                None => {
                    warn!("Renewal request missing current certificate");
                    return CsrResponse::error("Renewal requires current certificate");
                }
            };

            if let Err(e) = self.validate_renewal(&request.agent_id, &request.csr_pem, current_cert_pem) {
                error!(error = %e, "Invalid renewal request");
                return CsrResponse::error(format!("Invalid renewal request: {e}"));
            }
        }

        match self.issue_certificate_for_agent(&request.agent_id, &request.csr_pem).await {
            Ok((cert_pem, expires_at)) => {
                info!(agent_id = %request.agent_id.as_str(), "Issued certificate");
                CsrResponse::success(cert_pem, self.ca_cert_pem.clone(), expires_at)
            }
            Err(e) => {
                error!(error = %e, "Failed to issue certificate");
                CsrResponse::error(format!("Failed to issue certificate: {e}"))
            }
        }
    }

    async fn issue_certificate_for_agent(&self, agent_id: &AgentId, csr_pem: &str) -> Result<(String, i64)> {
        let mut csr_params = CertificateSigningRequestParams::from_pem(csr_pem).map_err(|e| {
            CoreError::Internal {
                message: format!("Failed to parse CSR: {e}"),
                severity: ErrorSeverity::Error,
            }
        })?;

        validate_csr_sig_alg(csr_pem)?;

        let common_name = format!("oasis-agent-{}", agent_id.as_str());
        let uri_san = format!("oasis-agent:{}", agent_id.as_str());

        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, DnValue::Utf8String(common_name));
        dn.push(
            DnType::OrganizationName,
            DnValue::Utf8String("Oasis Cluster".to_string()),
        );
        csr_params.params.distinguished_name = dn;

        csr_params.params.subject_alt_names = vec![SanType::URI(uri_san.try_into().map_err(|_| {
            CoreError::Internal {
                message: "Invalid URI SAN".to_string(),
                severity: ErrorSeverity::Error,
            }
        })?)];

        csr_params.params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        csr_params.params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        csr_params.params.is_ca = rcgen::IsCa::NoCa;

        let now = ::time::OffsetDateTime::now_utc();
        let not_before = now - ::time::Duration::minutes(5);
        let not_after = now + ::time::Duration::hours(self.cert_validity_hours as i64);
        csr_params.params.not_before = not_before;
        csr_params.params.not_after = not_after;
        csr_params.params.use_authority_key_identifier_extension = true;

        let cert = csr_params.signed_by(&self.ca_issuer).map_err(|e| CoreError::Internal {
            message: format!("Failed to sign certificate: {e}"),
            severity: ErrorSeverity::Error,
        })?;

        let cert_pem = cert.pem();
        Ok((cert_pem, not_after.unix_timestamp()))
    }

    fn validate_renewal(&self, claimed_agent_id: &AgentId, csr_pem: &str, current_cert_pem: &str) -> Result<()> {
        let current_pem = ::pem::parse(current_cert_pem.as_bytes()).map_err(|e| CoreError::Internal {
            message: format!("Failed to parse current cert PEM: {e}"),
            severity: ErrorSeverity::Error,
        })?;
        let current_cert_der = current_pem.contents().to_vec();

        verify_with_webpki(&current_cert_der, &self.ca_cert_der)?;

        let (_, current_cert) = X509Certificate::from_der(&current_cert_der).map_err(|e| CoreError::Internal {
            message: format!("Failed to parse current cert DER: {e}"),
            severity: ErrorSeverity::Error,
        })?;

        let agent_id_from_cert = extract_agent_id_from_cert(&current_cert)?;
        if &agent_id_from_cert != claimed_agent_id {
            return Err(CoreError::Internal {
                message: "Identity mismatch".to_string(),
                severity: ErrorSeverity::Error,
            });
        }

        let csr_pem = ::pem::parse(csr_pem.as_bytes()).map_err(|e| CoreError::Internal {
            message: format!("Failed to parse CSR PEM: {e}"),
            severity: ErrorSeverity::Error,
        })?;
        let csr_der = csr_pem.contents().to_vec();
        let (_, csr) = X509CertificationRequest::from_der(&csr_der).map_err(|e| CoreError::Internal {
            message: format!("Failed to parse CSR DER: {e}"),
            severity: ErrorSeverity::Error,
        })?;

        let csr_pubkey = csr.certification_request_info.subject_pki.raw;
        let cert_pubkey = current_cert.public_key().raw;
        if csr_pubkey != cert_pubkey {
            return Err(CoreError::Internal {
                message: "CSR public key mismatch".to_string(),
                severity: ErrorSeverity::Error,
            });
        }

        Ok(())
    }

    pub async fn cleanup_expired_tokens(&self) {
        self.token_store.write().await.cleanup_expired();
    }
}

fn build_dn_from_x509_subject(cert: &X509Certificate<'_>) -> (DistinguishedName, bool) {
    let mut dn = DistinguishedName::new();
    let mut has_any = false;

    for rdn in cert.subject().iter() {
        for attr in rdn.iter() {
            let oid = attr.attr_type();
            let value = match attr.attr_value().as_str() {
                Ok(v) => v,
                Err(_) => continue,
            };

            if *oid == oid_registry::OID_X509_COMMON_NAME {
                dn.push(DnType::CommonName, DnValue::Utf8String(value.to_string()));
                has_any = true;
            } else if *oid == oid_registry::OID_X509_ORGANIZATION_NAME {
                dn.push(DnType::OrganizationName, DnValue::Utf8String(value.to_string()));
                has_any = true;
            }
        }
    }

    if !has_any {
        dn.push(DnType::CommonName, DnValue::Utf8String("Oasis CA".to_string()));
    }

    (dn, has_any)
}

fn extract_agent_id_from_cert(cert: &X509Certificate<'_>) -> Result<AgentId> {
    if let Ok(Some(sans)) = cert.subject_alternative_name() {
        for name in &sans.value.general_names {
            if let GeneralName::URI(uri) = name {
                let uri = *uri;
                if let Some(agent_id_str) = uri.strip_prefix("oasis-agent:") {
                    return Ok(AgentId::new(agent_id_str));
                }
            }
        }
    }

    Err(CoreError::Internal {
        message: "Missing oasis-agent URI SAN".to_string(),
        severity: ErrorSeverity::Error,
    })
}

fn validate_csr_sig_alg(csr_pem: &str) -> Result<()> {
    let pem = ::pem::parse(csr_pem.as_bytes()).map_err(|e| CoreError::Internal {
        message: format!("Failed to parse CSR PEM: {e}"),
        severity: ErrorSeverity::Error,
    })?;

    let (_, csr) = X509CertificationRequest::from_der(pem.contents()).map_err(|e| CoreError::Internal {
        message: format!("Failed to parse CSR DER: {e}"),
        severity: ErrorSeverity::Error,
    })?;

    let algo = csr.signature_algorithm.algorithm;
    let allowed = [
        oid_registry::OID_SIG_ECDSA_WITH_SHA256,
        oid_registry::OID_SIG_ECDSA_WITH_SHA384,
        oid_registry::OID_PKCS1_SHA256WITHRSA,
        oid_registry::OID_PKCS1_SHA384WITHRSA,
        oid_registry::OID_PKCS1_SHA512WITHRSA,
        oid_registry::OID_SIG_ED25519,
    ];
    if !allowed.iter().any(|a| a == &algo) {
        return Err(CoreError::Internal {
            message: "Disallowed CSR signature algorithm".to_string(),
            severity: ErrorSeverity::Error,
        });
    }

    Ok(())
}

fn verify_with_webpki(end_entity_der: &[u8], ca_der: &[u8]) -> Result<()> {
    use webpki::{EndEntityCert, Time, TrustAnchor};

    let trust_anchor = TrustAnchor::try_from_cert_der(ca_der).map_err(|e| CoreError::Internal {
        message: format!("Invalid CA trust anchor: {e:?}"),
        severity: ErrorSeverity::Error,
    })?;

    let cert = EndEntityCert::try_from(end_entity_der).map_err(|e| CoreError::Internal {
        message: format!("Invalid end-entity cert: {e:?}"),
        severity: ErrorSeverity::Error,
    })?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let now = Time::from_seconds_since_unix_epoch(now.as_secs());

    let supported_sig_algs: &[&webpki::SignatureAlgorithm] = &[
        &webpki::ECDSA_P256_SHA256,
        &webpki::ECDSA_P256_SHA384,
        &webpki::ECDSA_P384_SHA256,
        &webpki::ECDSA_P384_SHA384,
        &webpki::ED25519,
        &webpki::RSA_PKCS1_2048_8192_SHA256,
        &webpki::RSA_PKCS1_2048_8192_SHA384,
        &webpki::RSA_PKCS1_2048_8192_SHA512,
        &webpki::RSA_PKCS1_3072_8192_SHA384,
    ];

    let anchors = [trust_anchor];
    let anchors = webpki::TlsClientTrustAnchors(&anchors);

    cert.verify_is_valid_tls_client_cert(supported_sig_algs, &anchors, &[], now)
    .map_err(|e| CoreError::Internal {
        message: format!("Certificate verification failed: {e:?}"),
        severity: ErrorSeverity::Error,
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use oasis_core::csr_types::CsrGenerator;
    use tempfile::tempdir;
    use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256, IsCa, BasicConstraints, KeyUsagePurpose};

    async fn create_test_ca() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let temp_dir = tempdir().unwrap();
        
        let mut ca_params = CertificateParams::default();
        ca_params.distinguished_name.push(DnType::CommonName, "Test CA");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        
        let ca_key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let ca_cert = ca_params.self_signed(&ca_key_pair).unwrap();
        
        let ca_cert_path = temp_dir.path().join("ca.pem");
        let ca_key_path = temp_dir.path().join("ca-key.pem");
        
        tokio::fs::write(&ca_cert_path, ca_cert.pem()).await.unwrap();
        tokio::fs::write(&ca_key_path, ca_key_pair.serialize_pem()).await.unwrap();
        
        (temp_dir, ca_cert_path, ca_key_path)
    }

    #[tokio::test]
    async fn test_ca_service_new() {
        let (_temp_dir, ca_cert_path, ca_key_path) = create_test_ca().await;
        
        let result = CaService::new(&ca_cert_path, &ca_key_path, 24 * 365).await;
        assert!(result.is_ok(), "Failed to create CA Service: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_create_bootstrap_token() {
        let (_temp_dir, ca_cert_path, ca_key_path) = create_test_ca().await;
        let ca_service = CaService::new(&ca_cert_path, &ca_key_path, 24 * 365).await.unwrap();
        
        let agent_id = AgentId::new("test-agent");
        let token = ca_service.create_bootstrap_token(agent_id.clone(), StdDuration::from_secs(3600)).await.unwrap();
        
        assert!(!token.token.is_empty());
        assert_eq!(token.agent_id, agent_id);
        assert!(token.is_valid());
        assert!(!token.used);
    }

    #[tokio::test]
    async fn test_handle_csr_request_with_valid_token() {
        let (_temp_dir, ca_cert_path, ca_key_path) = create_test_ca().await;
        let ca_service = CaService::new(&ca_cert_path, &ca_key_path, 24 * 365).await.unwrap();
        
        let agent_id = AgentId::new("test-agent-001");
        let token = ca_service.create_bootstrap_token(agent_id.clone(), StdDuration::from_secs(3600)).await.unwrap();
        
        let generator = CsrGenerator::new().unwrap();
        let csr_pem = generator.generate_csr(&agent_id).unwrap();
        
        let request = CsrRequest {
            agent_id: agent_id.clone(),
            csr_pem,
            bootstrap_token: Some(token.token.clone()),
            current_cert_pem: None,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        };
        
        let response = ca_service.handle_csr_request(request).await;
        
        assert!(response.success, "Response should be successful");
        assert!(response.certificate_pem.is_some(), "Certificate should be present");
        assert!(response.ca_certificate_pem.is_some(), "CA certificate should be present");
        assert!(response.expires_at.is_some(), "Expiration time should be present");
        assert!(response.error_message.is_none(), "No error message should be present");
    }

    #[tokio::test]
    async fn test_handle_csr_request_with_invalid_token() {
        let (_temp_dir, ca_cert_path, ca_key_path) = create_test_ca().await;
        let ca_service = CaService::new(&ca_cert_path, &ca_key_path, 24 * 365).await.unwrap();
        
        let agent_id = AgentId::new("test-agent-002");
        let generator = CsrGenerator::new().unwrap();
        let csr_pem = generator.generate_csr(&agent_id).unwrap();
        
        let request = CsrRequest {
            agent_id,
            csr_pem,
            bootstrap_token: Some("invalid-token-12345".to_string()),
            current_cert_pem: None,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        };
        
        let response = ca_service.handle_csr_request(request).await;
        
        assert!(!response.success, "Response should fail");
        assert!(response.error_message.is_some(), "Error message should be present");
        assert!(response.certificate_pem.is_none(), "No certificate should be issued");
    }

    #[tokio::test]
    async fn test_handle_csr_request_with_agent_id_mismatch() {
        let (_temp_dir, ca_cert_path, ca_key_path) = create_test_ca().await;
        let ca_service = CaService::new(&ca_cert_path, &ca_key_path, 24 * 365).await.unwrap();
        
        let agent_id_1 = AgentId::new("agent-1");
        let agent_id_2 = AgentId::new("agent-2");
        
        let token = ca_service.create_bootstrap_token(agent_id_1, StdDuration::from_secs(3600)).await.unwrap();
        
        let generator = CsrGenerator::new().unwrap();
        let csr_pem = generator.generate_csr(&agent_id_2).unwrap();
        
        let request = CsrRequest {
            agent_id: agent_id_2,
            csr_pem,
            bootstrap_token: Some(token.token.clone()),
            current_cert_pem: None,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        };
        
        let response = ca_service.handle_csr_request(request).await;
        
        assert!(!response.success, "Response should fail due to agent ID mismatch");
        assert!(response.error_message.unwrap().contains("mismatch"));
    }

    #[tokio::test]
    async fn test_token_consumed_after_use() {
        let (_temp_dir, ca_cert_path, ca_key_path) = create_test_ca().await;
        let ca_service = CaService::new(&ca_cert_path, &ca_key_path, 24 * 365).await.unwrap();
        
        let agent_id = AgentId::new("test-agent-003");
        let token = ca_service.create_bootstrap_token(agent_id.clone(), StdDuration::from_secs(3600)).await.unwrap();
        
        let generator = CsrGenerator::new().unwrap();
        let csr_pem = generator.generate_csr(&agent_id).unwrap();
        
        let request1 = CsrRequest {
            agent_id: agent_id.clone(),
            csr_pem: csr_pem.clone(),
            bootstrap_token: Some(token.token.clone()),
            current_cert_pem: None,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        };
        
        let response1 = ca_service.handle_csr_request(request1).await;
        assert!(response1.success, "First request should succeed");
        
        let request2 = CsrRequest {
            agent_id,
            csr_pem,
            bootstrap_token: Some(token.token.clone()),
            current_cert_pem: None,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        };
        
        let response2 = ca_service.handle_csr_request(request2).await;
        assert!(!response2.success, "Second request with same token should fail");
    }

    #[tokio::test]
    async fn test_cleanup_expired_tokens() {
        let (_temp_dir, ca_cert_path, ca_key_path) = create_test_ca().await;
        let ca_service = CaService::new(&ca_cert_path, &ca_key_path, 24 * 365).await.unwrap();
        
        let agent_id = AgentId::new("test-agent-004");
        let _expired_token = ca_service.create_bootstrap_token(agent_id.clone(), StdDuration::from_secs(0)).await.unwrap();
        let valid_token = ca_service.create_bootstrap_token(agent_id, StdDuration::from_secs(3600)).await.unwrap();
        
        tokio::time::sleep(StdDuration::from_millis(100)).await;
        
        ca_service.cleanup_expired_tokens().await;
        
        let token_store = ca_service.token_store.read().await;
        assert_eq!(token_store.tokens.len(), 1, "Should have only 1 valid token");
        assert!(token_store.tokens.contains_key(&valid_token.token), "Valid token should still exist");
    }

    #[tokio::test]
    async fn test_issued_certificate_has_correct_properties() {
        let (_temp_dir, ca_cert_path, ca_key_path) = create_test_ca().await;
        let ca_service = CaService::new(&ca_cert_path, &ca_key_path, 24 * 365).await.unwrap();
        
        let agent_id = AgentId::new("test-agent-005");
        let token = ca_service.create_bootstrap_token(agent_id.clone(), StdDuration::from_secs(3600)).await.unwrap();
        
        let generator = CsrGenerator::new().unwrap();
        let csr_pem = generator.generate_csr(&agent_id).unwrap();
        
        let request = CsrRequest {
            agent_id: agent_id.clone(),
            csr_pem,
            bootstrap_token: Some(token.token),
            current_cert_pem: None,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        };
        
        let response = ca_service.handle_csr_request(request).await;
        assert!(response.success);
        
        let cert_pem = response.certificate_pem.unwrap();
        assert!(cert_pem.contains("-----BEGIN CERTIFICATE-----"));
        assert!(cert_pem.contains("-----END CERTIFICATE-----"));
        
        let pem = ::pem::parse(cert_pem.as_bytes()).unwrap();
        let (_, cert) = X509Certificate::from_der(pem.contents()).unwrap();
        
        assert!(!cert.is_ca());
        
        let agent_id_from_cert = extract_agent_id_from_cert(&cert).unwrap();
        assert_eq!(agent_id_from_cert, agent_id);
    }

    #[tokio::test]
    async fn test_handle_csr_request_without_token_requires_current_cert() {
        let (_temp_dir, ca_cert_path, ca_key_path) = create_test_ca().await;
        let ca_service = CaService::new(&ca_cert_path, &ca_key_path, 24 * 365).await.unwrap();
        
        let agent_id = AgentId::new("test-agent-006");
        let generator = CsrGenerator::new().unwrap();
        let csr_pem = generator.generate_csr(&agent_id).unwrap();
        
        let request = CsrRequest {
            agent_id,
            csr_pem,
            bootstrap_token: None,
            current_cert_pem: None,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        };
        
        let response = ca_service.handle_csr_request(request).await;
        
        assert!(!response.success, "Request without token or current cert should fail");
        assert!(response.error_message.unwrap().contains("current certificate"));
    }
}
