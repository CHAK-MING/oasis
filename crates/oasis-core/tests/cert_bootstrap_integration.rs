//! Certificate Bootstrap Integration Test
//! 
//! This test requires a real NATS server running with TLS.
//! 
//! Setup steps:
//!   1. cargo build --release
//!   2. ./target/release/oasis-cli system init
//!   3. docker compose -f docker-compose.test.yml up -d
//! 
//! Run tests:
//!   TEST_NATS_URL=tls://localhost:14222 cargo test --package oasis-core --test cert_bootstrap_integration -- --ignored --nocapture
//! 
//! Cleanup:
//!   docker compose -f docker-compose.test.yml down -v

use oasis_core::core_types::AgentId;
use oasis_core::csr_types::CsrRequest;
use std::env;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::tempdir;

fn get_test_nats_url() -> String {
    env::var("TEST_NATS_URL").unwrap_or_else(|_| {
        panic!(
            "TEST_NATS_URL environment variable not set.\n\
             Start NATS first: docker compose -f docker-compose.test.yml up -d\n\
             Then run: TEST_NATS_URL=tls://localhost:14222 cargo test"
        )
    })
}

fn get_ca_cert_path() -> PathBuf {
    let workspace_root = env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    
    let ca_path = workspace_root.join("certs/ca.pem");
    if !ca_path.exists() {
        panic!(
            "CA certificate not found at {:?}\n\
             Run 'cargo build --release && ./target/release/oasis-cli system init' first",
            ca_path
        );
    }
    ca_path
}

fn get_ca_key_path() -> PathBuf {
    let workspace_root = env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    
    let key_path = workspace_root.join("certs/ca-key.pem");
    if !key_path.exists() {
        panic!(
            "CA private key not found at {:?}\n\
             Run 'cargo build --release && ./target/release/oasis-cli system init' first",
            key_path
        );
    }
    key_path
}


fn get_nats_client_cert_path() -> PathBuf {
    let workspace_root = env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    workspace_root.join("certs/nats-client.pem")
}

fn get_nats_client_key_path() -> PathBuf {
    let workspace_root = env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    workspace_root.join("certs/nats-client-key.pem")
}

async fn setup_test_ca() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    use rcgen::{CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256, IsCa, BasicConstraints, KeyUsagePurpose, DnType};
    
    let temp_dir = tempdir().unwrap();
    
    let mut ca_params = CertificateParams::default();
    ca_params.distinguished_name.push(DnType::CommonName, "Integration Test CA");
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
#[ignore]
async fn test_e2e_agent_bootstrap_with_real_nats() {
    let nats_url = get_test_nats_url();
    println!("🔗 Connecting to NATS at: {}", nats_url);
    
    let ca_cert_path = get_ca_cert_path();
    let ca_key_path = get_ca_key_path();
    
    let ca_service = oasis_server::infrastructure::services::ca_service::CaService::new(
        &ca_cert_path,
        &ca_key_path,
        24 * 365,
    )
    .await
    .expect("Failed to create CA Service");
    
    let agent_id = AgentId::new("integration-test-agent-001");
    let token = ca_service
        .create_bootstrap_token(agent_id.clone(), Duration::from_secs(3600))
        .await
        .expect("Failed to create bootstrap token");
    
    println!("✅ Bootstrap token created: {}", &token.token[..16]);
    
    let nats_client = async_nats::ConnectOptions::new()
        .require_tls(true)
        .add_root_certificates(ca_cert_path.clone())
        .add_client_certificate(get_nats_client_cert_path(), get_nats_client_key_path())
        .connect(&nats_url)
        .await
        .expect("Failed to connect to NATS - is the test NATS server running?");
    
    println!("✅ Connected to NATS");
    
    let ca_service = std::sync::Arc::new(ca_service);
    let ca_service_clone = ca_service.clone();
    let nats_client_clone = nats_client.clone();
    
    let ca_listener = tokio::spawn(async move {
        use futures::StreamExt;
        
        let mut subscriber = nats_client_clone
            .subscribe("oasis.ca.csr")
            .await
            .expect("Failed to subscribe to CSR subject");
        
        println!("✅ CA Service listening on NATS subject: oasis.ca.csr");
        
        while let Some(msg) = subscriber.next().await {
            let request: CsrRequest = match serde_json::from_slice(&msg.payload) {
                Ok(req) => req,
                Err(e) => {
                    eprintln!("❌ Failed to parse CSR request: {}", e);
                    continue;
                }
            };
            
            println!("📨 Received CSR request from agent: {}", request.agent_id.as_str());
            
            let response = ca_service_clone.handle_csr_request(request).await;
            
            if let Some(reply_subject) = msg.reply {
                let response_bytes = serde_json::to_vec(&response).expect("Failed to serialize response");
                let _ = nats_client_clone.publish(reply_subject, response_bytes.into()).await;
                
                if response.success {
                    println!("✅ Certificate issued successfully");
                } else {
                    eprintln!("❌ Certificate issuance failed: {:?}", response.error_message);
                }
            }
        }
    });
    
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    let agent_certs_dir = tempdir().unwrap();
    tokio::fs::copy(&ca_cert_path, agent_certs_dir.path().join("nats-ca.pem"))
        .await
        .expect("Failed to copy CA cert for agent");
    
    let cert_bootstrap = oasis_agent::cert_bootstrap::CertBootstrap::new(
        agent_id.clone(),
        agent_certs_dir.path(),
        nats_url.clone(),
        Some(token.token.clone()),
    );
    
    println!("🚀 Agent requesting certificate via NATS...");
    
    let bootstrap_result = cert_bootstrap.bootstrap_if_needed().await;
    
    ca_listener.abort();
    
    assert!(
        bootstrap_result.is_ok(),
        "Bootstrap failed: {:?}",
        bootstrap_result.err()
    );
    assert!(
        bootstrap_result.unwrap(),
        "Bootstrap should have been performed"
    );
    
    println!("✅ Agent bootstrap completed");
    
    let cert_path = agent_certs_dir.path().join("nats-client.pem");
    let key_path = agent_certs_dir.path().join("nats-client-key.pem");
    let ca_path = agent_certs_dir.path().join("nats-ca.pem");
    
    assert!(cert_path.exists(), "Certificate file should exist");
    assert!(key_path.exists(), "Private key file should exist");
    assert!(ca_path.exists(), "CA certificate file should exist");
    
    println!("✅ Certificate files verified");
    
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        
        let key_metadata = std::fs::metadata(&key_path).unwrap();
        let key_perms = key_metadata.permissions().mode() & 0o777;
        assert_eq!(
            key_perms, 0o600,
            "Private key should have 0600 permissions, got {:o}",
            key_perms
        );
        
        let cert_metadata = std::fs::metadata(&cert_path).unwrap();
        let cert_perms = cert_metadata.permissions().mode() & 0o777;
        assert_eq!(
            cert_perms, 0o644,
            "Certificate should have 0644 permissions, got {:o}",
            cert_perms
        );
        
        println!("✅ File permissions verified (Unix)");
    }
    
    let cert_content = tokio::fs::read_to_string(&cert_path).await.unwrap();
    assert!(cert_content.contains("-----BEGIN CERTIFICATE-----"));
    assert!(cert_content.contains("-----END CERTIFICATE-----"));
    
    println!("✅ Certificate format verified");
    println!("🎉 End-to-end certificate bootstrap integration test PASSED!");
}

#[tokio::test]
#[ignore]
async fn test_bootstrap_with_invalid_token_fails() {
    let nats_url = get_test_nats_url();
    
    let ca_cert_path = get_ca_cert_path();
    let ca_key_path = get_ca_key_path();
    let ca_service = oasis_server::infrastructure::services::ca_service::CaService::new(
        &ca_cert_path,
        &ca_key_path,
        24 * 365,
    )
    .await
    .unwrap();
    
    
    let nats_client = async_nats::ConnectOptions::new()
        .require_tls(true)
        .add_root_certificates(ca_cert_path.clone())
        .add_client_certificate(get_nats_client_cert_path(), get_nats_client_key_path())
        .connect(&nats_url)
        .await
        .expect("Failed to connect to NATS");
    
    let ca_service = std::sync::Arc::new(ca_service);
    let ca_service_clone = ca_service.clone();
    let nats_client_clone = nats_client.clone();
    
    let ca_listener = tokio::spawn(async move {
        use futures::StreamExt;
        
        let mut subscriber = nats_client_clone
            .subscribe("oasis.ca.csr")
            .await
            .unwrap();
        
        while let Some(msg) = subscriber.next().await {
            let request: CsrRequest = match serde_json::from_slice(&msg.payload) {
                Ok(req) => req,
                Err(_) => continue,
            };
            
            let response = ca_service_clone.handle_csr_request(request).await;
            
            if let Some(reply_subject) = msg.reply {
                let response_bytes = serde_json::to_vec(&response).unwrap();
                let _ = nats_client_clone.publish(reply_subject, response_bytes.into()).await;
            }
        }
    });
    
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    let agent_id = AgentId::new("invalid-token-test-agent");
    let agent_certs_dir = tempdir().unwrap();
    tokio::fs::copy(&ca_cert_path, agent_certs_dir.path().join("nats-ca.pem"))
        .await
        .unwrap();
    
    let cert_bootstrap = oasis_agent::cert_bootstrap::CertBootstrap::new(
        agent_id,
        agent_certs_dir.path(),
        nats_url,
        Some("invalid-token-12345".to_string()),
    );
    
    let bootstrap_result = cert_bootstrap.bootstrap_if_needed().await;
    
    ca_listener.abort();
    
    assert!(
        bootstrap_result.is_err(),
        "Bootstrap with invalid token should fail"
    );
    
    println!("✅ Invalid token correctly rejected");
}

#[tokio::test]
#[ignore]
async fn test_multiple_agents_concurrent_bootstrap() {
    let nats_url = get_test_nats_url();
    
    let ca_cert_path = get_ca_cert_path();
    let ca_key_path = get_ca_key_path();
    let ca_service = oasis_server::infrastructure::services::ca_service::CaService::new(
        &ca_cert_path,
        &ca_key_path,
        24 * 365,
    )
    .await
    .unwrap();
    
    
    let nats_client = async_nats::ConnectOptions::new()
        .require_tls(true)
        .add_root_certificates(ca_cert_path.clone())
        .add_client_certificate(get_nats_client_cert_path(), get_nats_client_key_path())
        .connect(&nats_url)
        .await
        .unwrap();
    
    let ca_service = std::sync::Arc::new(ca_service);
    let ca_service_clone = ca_service.clone();
    let nats_client_clone = nats_client.clone();
    
    let ca_listener = tokio::spawn(async move {
        use futures::StreamExt;
        
        let mut subscriber = nats_client_clone
            .subscribe("oasis.ca.csr")
            .await
            .unwrap();
        
        while let Some(msg) = subscriber.next().await {
            let request: CsrRequest = match serde_json::from_slice(&msg.payload) {
                Ok(req) => req,
                Err(_) => continue,
            };
            
            let response = ca_service_clone.handle_csr_request(request).await;
            
            if let Some(reply_subject) = msg.reply {
                let response_bytes = serde_json::to_vec(&response).unwrap();
                let _ = nats_client_clone.publish(reply_subject, response_bytes.into()).await;
            }
        }
    });
    
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    let mut handles = vec![];
    
    for i in 0..5 {
        let agent_id = AgentId::new(&format!("concurrent-agent-{}", i));
        let token = ca_service
            .create_bootstrap_token(agent_id.clone(), Duration::from_secs(3600))
            .await
            .unwrap();
        
        let nats_url = nats_url.clone();
        let ca_cert_path = ca_cert_path.clone();
        
        let handle = tokio::spawn(async move {
            let agent_certs_dir = tempdir().unwrap();
            tokio::fs::copy(&ca_cert_path, agent_certs_dir.path().join("nats-ca.pem"))
                .await
                .unwrap();
            
            let cert_bootstrap = oasis_agent::cert_bootstrap::CertBootstrap::new(
                agent_id.clone(),
                agent_certs_dir.path(),
                nats_url,
                Some(token.token),
            );
            
            let result = cert_bootstrap.bootstrap_if_needed().await;
            
            (agent_id, result)
        });
        
        handles.push(handle);
    }
    
    let results = futures::future::join_all(handles).await;
    
    ca_listener.abort();
    
    for (i, result) in results.into_iter().enumerate() {
        let (agent_id, bootstrap_result) = result.unwrap();
        assert!(
            bootstrap_result.is_ok(),
            "Agent {} bootstrap failed: {:?}",
            i,
            bootstrap_result.err()
        );
        println!("✅ Agent {} ({}) bootstrap succeeded", i, agent_id.as_str());
    }
    
    println!("🎉 Concurrent bootstrap test PASSED! All 5 agents succeeded.");
}
