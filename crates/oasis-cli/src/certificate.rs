use anyhow::{Context, Result};
use console::style;
use rcgen::string::Ia5String;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedKey, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256,
    SanType,
};
use std::net::IpAddr;

/// 证书生成器
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

    /// 生成 Agent 证书
    pub async fn generate_agent_certificate(
        agent_id: &str,
        ca: &CertifiedKey<KeyPair>,
        output_dir: &std::path::Path,
    ) -> Result<()> {
        std::fs::create_dir_all(output_dir)?;

        let ca_params = Self::get_ca_params();

        // 生成客户端证书
        let client_cert = Self::generate_client_cert(
            &format!("oasis-agent-{}", agent_id),
            &[ExtendedKeyUsagePurpose::ClientAuth],
            Some(agent_id),
            ca,
            &ca_params,
        )?;

        // 保存 NATS 客户端证书
        std::fs::write(output_dir.join("nats-client.pem"), client_cert.cert.pem())?;
        std::fs::write(
            output_dir.join("nats-client-key.pem"),
            client_cert.signing_key.serialize_pem(),
        )?;

        // 复制 CA 证书
        let ca_pem = ca.cert.pem();
        std::fs::write(output_dir.join("nats-ca.pem"), &ca_pem)?;

        Ok(())
    }

    /// 加载 CA 证书
    pub fn load_ca(certs_dir: &std::path::Path) -> Result<CertifiedKey<KeyPair>> {
        let ca_key = std::fs::read_to_string(certs_dir.join("ca-key.pem"))
            .context("Failed to read CA private key")?;

        let key_pair =
            rcgen::KeyPair::from_pem(&ca_key).context("Failed to parse CA private key")?;

        // 重新生成 CA 参数
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
        let cert = params.self_signed(&key_pair)?;

        Ok(CertifiedKey {
            cert,
            signing_key: key_pair,
        })
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
