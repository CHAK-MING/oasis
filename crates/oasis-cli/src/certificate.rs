use anyhow::Result;
use console::style;
use rcgen::string::Ia5String;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedKey, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256,
    SanType,
};
use std::net::IpAddr;

pub struct CertificateGenerator;

impl CertificateGenerator {
    /// 生成基础证书
    pub async fn generate_base_certificates(certs_dir: &std::path::Path) -> Result<()> {
        println!("► {}", style("正在生成 mTLS 证书...").bold());

        std::fs::create_dir_all(certs_dir)?;

        // 生成 CA 证书
        let ca = Self::generate_ca("Oasis Root CA")?;
        let ca_pem = ca.cert.pem();
        let ca_key_pem = ca.signing_key.serialize_pem();

        // 保存 CA 证书
        std::fs::write(certs_dir.join("ca.pem"), &ca_pem)?;
        std::fs::write(certs_dir.join("ca-key.pem"), &ca_key_pem)?;

        // 兼容性文件
        std::fs::write(certs_dir.join("nats-ca.pem"), &ca_pem)?;
        std::fs::write(certs_dir.join("grpc-ca.pem"), &ca_pem)?;

        // 生成服务器和客户端证书
        Self::generate_server_certificates(certs_dir, &ca)?;
        Self::generate_client_certificates(certs_dir, &ca)?;

        println!(
            "  {} 基础证书已成功生成到 {}",
            style("✔").green(),
            style(certs_dir.display()).cyan()
        );

        Ok(())
    }

    /// 获取 CA 参数
    fn get_ca_params() -> CertificateParams {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "Oasis Root CA");

        let mut params = CertificateParams::default();
        params.distinguished_name = dn;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        params
    }

    /// 生成 CA 证书
    fn generate_ca(common_name: &str) -> Result<CertifiedKey<KeyPair>> {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, common_name);

        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;

        let params = Self::get_ca_params();
        let cert = params.self_signed(&key_pair)?;

        Ok(CertifiedKey {
            cert,
            signing_key: key_pair,
        })
    }

    /// 生成服务器证书
    fn generate_server_certificates(
        certs_dir: &std::path::Path,
        ca: &CertifiedKey<KeyPair>,
    ) -> Result<()> {
        let ca_params = Self::get_ca_params();

        // NATS 服务器证书
        let nats_cert = Self::generate_server_cert(
            "nats-server",
            &["localhost", "nats", "oasis-nats", "*.nats.local"],
            &Self::parse_ip_addresses(&["127.0.0.1", "::1", "0.0.0.0"])?,
            ca,
            &ca_params,
        )?;

        std::fs::write(certs_dir.join("nats-server.pem"), nats_cert.cert.pem())?;
        std::fs::write(
            certs_dir.join("nats-server-key.pem"),
            nats_cert.signing_key.serialize_pem(),
        )?;

        // gRPC 服务器证书
        let grpc_cert = Self::generate_server_cert(
            "oasis-grpc-server",
            &["localhost", "oasis-server", "*.oasis.local"],
            &Self::parse_ip_addresses(&["127.0.0.1", "::1", "0.0.0.0"])?,
            ca,
            &ca_params,
        )?;

        std::fs::write(certs_dir.join("grpc-server.pem"), grpc_cert.cert.pem())?;
        std::fs::write(
            certs_dir.join("grpc-server-key.pem"),
            grpc_cert.signing_key.serialize_pem(),
        )?;

        Ok(())
    }

    /// 生成客户端证书
    fn generate_client_certificates(
        certs_dir: &std::path::Path,
        ca: &CertifiedKey<KeyPair>,
    ) -> Result<()> {
        let ca_params = Self::get_ca_params();

        let client_cert = Self::generate_client_cert(
            "oasis-cli",
            &[ExtendedKeyUsagePurpose::ClientAuth],
            None,
            ca,
            &ca_params,
        )?;

        // 保存 NATS 客户端证书
        std::fs::write(certs_dir.join("nats-client.pem"), client_cert.cert.pem())?;
        std::fs::write(
            certs_dir.join("nats-client-key.pem"),
            client_cert.signing_key.serialize_pem(),
        )?;

        // 保存 gRPC 客户端证书
        std::fs::write(certs_dir.join("grpc-client.pem"), client_cert.cert.pem())?;
        std::fs::write(
            certs_dir.join("grpc-client-key.pem"),
            client_cert.signing_key.serialize_pem(),
        )?;

        Ok(())
    }

    /// 生成客户端证书
    fn generate_client_cert(
        common_name: &str,
        extended_key_usage: &[ExtendedKeyUsagePurpose],
        agent_id: Option<&str>,
        ca: &CertifiedKey<KeyPair>,
        ca_params: &CertificateParams,
    ) -> Result<CertifiedKey<KeyPair>> {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, common_name);

        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;

        let mut params = CertificateParams::default();
        params.distinguished_name = dn;
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.extended_key_usages = extended_key_usage.to_vec();

        // 如果提供了 agent_id，添加到 SAN
        if let Some(id) = agent_id {
            params
                .subject_alt_names
                .push(SanType::DnsName(Ia5String::try_from(id.to_string())?));
        }

        let issuer = Issuer::from_params(ca_params, &ca.signing_key);
        let cert = params.signed_by(&key_pair, &issuer)?;

        Ok(CertifiedKey {
            cert,
            signing_key: key_pair,
        })
    }

    /// 生成服务器证书
    fn generate_server_cert(
        common_name: &str,
        sans_dns: &[&str],
        sans_ip: &[IpAddr],
        ca: &CertifiedKey<KeyPair>,
        ca_params: &CertificateParams,
    ) -> Result<CertifiedKey<KeyPair>> {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, common_name);

        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;

        let mut params = CertificateParams::default();
        params.distinguished_name = dn;
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
            KeyUsagePurpose::KeyAgreement,
        ];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];

        // 添加 DNS SAN - 确保包含所有必要的名称
        for dns in sans_dns {
            params
                .subject_alt_names
                .push(SanType::DnsName(Ia5String::try_from((*dns).to_string())?));
        }

        // 添加 IP SAN - 确保包含所有必要的 IP
        for ip in sans_ip {
            params.subject_alt_names.push(SanType::IpAddress(*ip));
        }

        let issuer = Issuer::from_params(ca_params, &ca.signing_key);
        let cert = params.signed_by(&key_pair, &issuer)?;

        Ok(CertifiedKey {
            cert,
            signing_key: key_pair,
        })
    }

    /// 安全地解析 IP 地址列表
    fn parse_ip_addresses(ip_strings: &[&str]) -> Result<Vec<IpAddr>> {
        let mut addresses = Vec::new();

        for ip_str in ip_strings {
            match ip_str.parse::<IpAddr>() {
                Ok(addr) => addresses.push(addr),
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Failed to parse IP address '{}': {}",
                        ip_str,
                        e
                    ));
                }
            }
        }

        Ok(addresses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use x509_parser::prelude::*;

    #[tokio::test]
    async fn test_generate_base_certificates() {
        let temp_dir = tempdir().unwrap();
        let certs_dir = temp_dir.path();

        let result = CertificateGenerator::generate_base_certificates(certs_dir).await;
        assert!(result.is_ok());

        assert!(certs_dir.join("ca.pem").exists());
        assert!(certs_dir.join("ca-key.pem").exists());
        assert!(certs_dir.join("nats-ca.pem").exists());
        assert!(certs_dir.join("grpc-ca.pem").exists());
        assert!(certs_dir.join("nats-server.pem").exists());
        assert!(certs_dir.join("nats-server-key.pem").exists());
        assert!(certs_dir.join("grpc-server.pem").exists());
        assert!(certs_dir.join("grpc-server-key.pem").exists());
        assert!(certs_dir.join("nats-client.pem").exists());
        assert!(certs_dir.join("nats-client-key.pem").exists());
        assert!(certs_dir.join("grpc-client.pem").exists());
        assert!(certs_dir.join("grpc-client-key.pem").exists());
    }

    #[tokio::test]
    async fn test_ca_certificate_is_valid() {
        let temp_dir = tempdir().unwrap();
        let certs_dir = temp_dir.path();

        CertificateGenerator::generate_base_certificates(certs_dir)
            .await
            .unwrap();

        let ca_pem = std::fs::read_to_string(certs_dir.join("ca.pem")).unwrap();

        assert!(ca_pem.contains("-----BEGIN CERTIFICATE-----"));
        assert!(ca_pem.contains("-----END CERTIFICATE-----"));

        let pem = Pem::iter_from_buffer(ca_pem.as_bytes())
            .next()
            .unwrap()
            .unwrap();
        let (_, cert) = X509Certificate::from_der(&pem.contents).unwrap();

        assert!(cert.is_ca());
    }

    #[tokio::test]
    async fn test_server_certificate_has_san() {
        let temp_dir = tempdir().unwrap();
        let certs_dir = temp_dir.path();

        CertificateGenerator::generate_base_certificates(certs_dir)
            .await
            .unwrap();

        let nats_cert_pem = std::fs::read_to_string(certs_dir.join("nats-server.pem")).unwrap();
        let pem = Pem::iter_from_buffer(nats_cert_pem.as_bytes())
            .next()
            .unwrap()
            .unwrap();
        let (_, cert) = X509Certificate::from_der(&pem.contents).unwrap();

        let san_ext = cert.get_extension_unique(&oid_registry::OID_X509_EXT_SUBJECT_ALT_NAME);
        assert!(san_ext.is_ok());
    }

    #[test]
    fn test_parse_ip_addresses_valid() {
        let ips = &["127.0.0.1", "::1", "0.0.0.0"];
        let result = CertificateGenerator::parse_ip_addresses(ips);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.len(), 3);
    }

    #[test]
    fn test_parse_ip_addresses_invalid() {
        let ips = &["127.0.0.1", "invalid-ip", "::1"];
        let result = CertificateGenerator::parse_ip_addresses(ips);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_ip_addresses_empty() {
        let ips: &[&str] = &[];
        let result = CertificateGenerator::parse_ip_addresses(ips);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_get_ca_params() {
        let params = CertificateGenerator::get_ca_params();

        assert!(matches!(params.is_ca, IsCa::Ca(_)));
        assert!(params.key_usages.contains(&KeyUsagePurpose::KeyCertSign));
        assert!(params.key_usages.contains(&KeyUsagePurpose::CrlSign));
        assert!(
            params
                .key_usages
                .contains(&KeyUsagePurpose::DigitalSignature)
        );
    }

    #[test]
    fn test_generate_ca() {
        let result = CertificateGenerator::generate_ca("Test CA");
        assert!(result.is_ok());

        let ca = result.unwrap();
        let cert_pem = ca.cert.pem();

        assert!(cert_pem.contains("-----BEGIN CERTIFICATE-----"));
        assert!(cert_pem.contains("-----END CERTIFICATE-----"));
    }

    #[tokio::test]
    async fn test_nats_ca_equals_grpc_ca() {
        let temp_dir = tempdir().unwrap();
        let certs_dir = temp_dir.path();

        CertificateGenerator::generate_base_certificates(certs_dir)
            .await
            .unwrap();

        let nats_ca = std::fs::read_to_string(certs_dir.join("nats-ca.pem")).unwrap();
        let grpc_ca = std::fs::read_to_string(certs_dir.join("grpc-ca.pem")).unwrap();
        let main_ca = std::fs::read_to_string(certs_dir.join("ca.pem")).unwrap();

        assert_eq!(nats_ca, main_ca);
        assert_eq!(grpc_ca, main_ca);
    }

    #[tokio::test]
    async fn test_client_cert_reusable() {
        let temp_dir = tempdir().unwrap();
        let certs_dir = temp_dir.path();

        CertificateGenerator::generate_base_certificates(certs_dir)
            .await
            .unwrap();

        let nats_client = std::fs::read_to_string(certs_dir.join("nats-client.pem")).unwrap();
        let grpc_client = std::fs::read_to_string(certs_dir.join("grpc-client.pem")).unwrap();

        assert_eq!(nats_client, grpc_client);
    }

    #[tokio::test]
    async fn test_certificates_directory_creation() {
        let temp_dir = tempdir().unwrap();
        let nested_dir = temp_dir.path().join("deeply").join("nested").join("certs");

        let result = CertificateGenerator::generate_base_certificates(&nested_dir).await;
        assert!(result.is_ok());
        assert!(nested_dir.exists());
    }
}
