#!/bin/bash
set -euo pipefail

# -----------------------------------------------------------------------------
# 脚本名称: generate-certs.sh
# 功能概述:
#   - 生成 NATS 服务所需 TLS 证书（自签 CA/服务端/客户端）
#   - 通过 nsc 创建/配置 Operator、System Account、业务 Account 与用户
#   - 导出 Operator/Account JWT，生成应用侧 .creds
#   - 生成内存解析器 (resolver: MEMORY) 的预载文件 accounts.json
#   - 拼装最终 NATS 配置 nats-server.conf（TLS/JetStream/认证片段）
# 前置条件:
#   - 已安装 openssl、jq、nsc，且具有写权限
# 输出物:
#   - certs/: 证书、nats.creds、nats-jwt.conf、nats-server.conf
#   - jwt/:   op.jwt、SYS.jwt、OasisAccount.jwt、accounts.json
#   - .env:   含 SYS_ACCOUNT_ID（供 docker compose 使用）
# 使用提示:
#   - 该脚本面向本地开发/测试。生产环境需替换证书与密钥管理方式。
#   - 仅新增注释，不改变任何逻辑与命令顺序。
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# 步骤 1 - 初始化输出目录
# 说明: 准备证书输出目录 certs/
# -----------------------------------------------------------------------------
CERT_DIR="certs"
mkdir -p "$CERT_DIR"

# -----------------------------------------------------------------------------
# 步骤 2 - 生成 TLS 证书（CA/Server/Client）
# 说明:
#   - 使用自签 CA 为服务端与客户端证书签发
#   - 服务端证书包含 SAN（localhost/IP），避免握手时主机名不匹配
#   - 如启用 mTLS，客户端需提供 client 证书
# -----------------------------------------------------------------------------
echo "=== Generate proper CA, server, and client certificates (with correct extensions) ==="

# --- 2.1 生成自签 Root CA（CA:TRUE） ---
cat > ca.conf <<'EOF'
[ req ]
default_bits       = 4096
distinguished_name = dn
x509_extensions    = v3_ca
prompt             = no

[ dn ]
CN = Oasis NATS CA

[ v3_ca ]
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always,issuer
basicConstraints = critical, CA:true
keyUsage = critical, cRLSign, keyCertSign
EOF

openssl req -x509 -new -nodes -days 3650 -config ca.conf -keyout "$CERT_DIR/nats-ca-key.pem" -out "$CERT_DIR/nats-ca.pem"

# --- 2.2 生成服务端证书（含 SAN 与 serverAuth） ---
cat > server.conf.ext <<'EOF'
subjectAltName = @alt_names
basicConstraints = CA:false
keyUsage = critical, digitalSignature, keyEncipherment, keyAgreement
extendedKeyUsage = serverAuth, clientAuth

[alt_names]
DNS.1 = localhost
IP.1  = 127.0.0.1
EOF

openssl genrsa -out "$CERT_DIR/nats-server-key.pem" 2048
openssl req -new -key "$CERT_DIR/nats-server-key.pem" -subj "/CN=localhost" -out server.csr
openssl x509 -req -in server.csr -CA "$CERT_DIR/nats-ca.pem" -CAkey "$CERT_DIR/nats-ca-key.pem" -CAcreateserial \
  -out "$CERT_DIR/nats-server.pem" -days 365 -extfile server.conf.ext

# --- 2.3 生成客户端证书（clientAuth，用于 mTLS 客户端） ---
cat > client.conf.ext <<'EOF'
basicConstraints = CA:false
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

openssl genrsa -out "$CERT_DIR/nats-client-key.pem" 2048
openssl req -new -key "$CERT_DIR/nats-client-key.pem" -subj "/CN=oasis-client" -out client.csr
openssl x509 -req -in client.csr -CA "$CERT_DIR/nats-ca.pem" -CAkey "$CERT_DIR/nats-ca-key.pem" -CAcreateserial \
  -out "$CERT_DIR/nats-client.pem" -days 365 -extfile client.conf.ext

# -----------------------------------------------------------------------------
# 步骤 3 - 生成 gRPC 专用 CA（与 NATS 证书链分离）
# -----------------------------------------------------------------------------
cat > grpc-ca.conf <<'EOF'
[ req ]
default_bits       = 4096
distinguished_name = dn
x509_extensions    = v3_ca
prompt             = no

[ dn ]
CN = Oasis gRPC CA

[ v3_ca ]
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always,issuer
basicConstraints = critical, CA:true
keyUsage = critical, cRLSign, keyCertSign
EOF

openssl req -x509 -new -nodes -days 3650 -config grpc-ca.conf -keyout "$CERT_DIR/grpc-ca-key.pem" -out "$CERT_DIR/grpc-ca.pem"

# --- 3.1 生成 gRPC 服务器证书 ---
openssl genrsa -out "$CERT_DIR/grpc-server-key.pem" 2048
openssl req -new -key "$CERT_DIR/grpc-server-key.pem" -subj "/CN=oasis-grpc-server" -out grpc-server.csr
openssl x509 -req -in grpc-server.csr -CA "$CERT_DIR/grpc-ca.pem" -CAkey "$CERT_DIR/grpc-ca-key.pem" -CAcreateserial \
  -out "$CERT_DIR/grpc-server.pem" -days 365 -extfile server.conf.ext

# --- 3.2 生成 gRPC 客户端证书（用于 CLI/客户端） ---
openssl genrsa -out "$CERT_DIR/grpc-client-key.pem" 2048
openssl req -new -key "$CERT_DIR/grpc-client-key.pem" -subj "/CN=oasis-grpc-client" -out grpc-client.csr
openssl x509 -req -in grpc-client.csr -CA "$CERT_DIR/grpc-ca.pem" -CAkey "$CERT_DIR/grpc-ca-key.pem" -CAcreateserial \
  -out "$CERT_DIR/grpc-client.pem" -days 365 -extfile client.conf.ext

# -----------------------------------------------------------------------------
# 步骤 4 - 使用 NSC 生成 NATS 所需 JWT 与账户实体
# 说明:
#   - 创建 Operator(OasisOp)、系统账户 SYS（JetStream/系统任务）、业务账户 OasisAccount
#   - 为 OasisAccount 创建用户 oasis-user，并生成应用使用的 .creds
# -----------------------------------------------------------------------------
echo "=== Generate NATS JWT credentials ==="

# 检查 nsc 是否已安装，未安装则临时下载放置 /usr/local/bin/nsc
if ! command -v nsc &> /dev/null; then
    echo "NSC (NATS Security CLI) is not installed. Installing..."
    # Install nsc
    curl -L https://github.com/nats-io/nsc/releases/latest/download/nsc-linux-amd64.zip -o /tmp/nsc.zip
    unzip -o /tmp/nsc.zip -d /tmp/
    sudo mv /tmp/nsc /usr/local/bin/
    chmod +x /usr/local/bin/nsc
    rm -f /tmp/nsc.zip
fi

# 清理本机全局 NSC stores（避免历史状态影响）
rm -rf ~/.local/share/nats/nsc/stores/* 2>/dev/null || true

# 初始化项目本地 NSC 工作目录（NSC_HOME）与 JWT 输出目录（jwt/）
export NSC_HOME="$PWD/nsc"
rm -rf "$NSC_HOME" # Clean up previous state to ensure a fresh start
mkdir -p "$NSC_HOME"

JWT_DIR="$PWD/jwt"
mkdir -p "$JWT_DIR"

# 创建 Operator（OasisOp），并生成签名密钥（signing key）
nsc add operator --name OasisOp --generate-signing-key

# 创建系统账户 SYS 及其用户 sys（用于系统连接/JetStream）
nsc add account SYS
nsc add user --name sys --account SYS

# 在 Operator 级别设置系统账户 = SYS
nsc edit operator --system-account SYS

# 创建业务账户 OasisAccount 及其用户 oasis-user，并设置 JetStream 限额
nsc add account OasisAccount
nsc edit account --name OasisAccount --js-mem-storage 1GB --js-disk-storage 20GB
nsc add user --name oasis-user --account OasisAccount

# 导出/解析实体所需的 ID（Operator/Account），为后续导出与预载做准备

OPERATOR_ID=$(nsc describe operator -J | jq -r .jwt.sub)
SYS_ACCOUNT_ID=$(nsc describe account SYS -J | jq -r .jwt.sub)
OASIS_ACCOUNT_ID=$(nsc describe account OasisAccount -J | jq -r .jwt.sub)

# 导出所需 JWT 到稳定位置 jwt/（op.jwt、OasisAccount.jwt、SYS.jwt）
nsc export operator > "$JWT_DIR/op.jwt"
nsc export account OasisAccount > "$JWT_DIR/OasisAccount.jwt"
nsc export account SYS > "$JWT_DIR/SYS.jwt"

# 生成应用使用的凭据文件 certs/nats.creds（供客户端连接用）
nsc generate creds --account OasisAccount --name oasis-user --output-file "$CERT_DIR/nats.creds"

# 基于已导出的 JWT 生成内存解析器预载文件 jwt/accounts.json（resolver_preload）
cat <<EOF > ${JWT_DIR}/accounts.json
{
  "resolver_preload": {
    "${SYS_ACCOUNT_ID}": "$(cat ${JWT_DIR}/SYS.jwt)",
    "${OASIS_ACCOUNT_ID}": "$(cat ${JWT_DIR}/OasisAccount.jwt)"
  }
}
EOF

# -----------------------------------------------------------------------------
# 步骤 5 - 生成 JWT 认证配置片段（由 nsc 输出）
# 说明:
#   - 使用 --mem-resolver 生成启用内存解析器的认证片段到 certs/nats-jwt.conf
# -----------------------------------------------------------------------------
rm -f "$CERT_DIR/nats-jwt.conf" # Avoid error if file exists
nsc generate config --mem-resolver --config-file "$CERT_DIR/nats-jwt.conf"

# 生成 .env（供 docker compose 通过 --system_account 注入 System Account ID）
echo "SYS_ACCOUNT_ID=${SYS_ACCOUNT_ID}" > .env

# -----------------------------------------------------------------------------
# 步骤 6 - 生成最终 NATS 主配置（nats-server.conf 的基础部分）
# 说明:
#   - 包含监听端口、监控端口、TLS 证书路径、JetStream 存储等基础配置
#   - 稍后将把 JWT 认证片段追加到该文件末尾
# -----------------------------------------------------------------------------
cat > "$CERT_DIR/nats-server.conf" << 'EOF'
# NATS Server Configuration
port: 4443
monitor_port: 8222

# TLS 配置（使用上面生成的 certs 目录中的证书与私钥）
tls {
  cert_file: "/etc/nats/certs/nats-server.pem"
  key_file:  "/etc/nats/certs/nats-server-key.pem"
  ca_file:   "/etc/nats/certs/nats-ca.pem"
  verify:    true
}

# JetStream 配置（数据目录挂载自 docker 卷）
jetstream {
  store_dir: "/data/nats"
  max_memory_store: 1GB
  max_file_store: 20GB
}

# 日志配置
debug: true
trace: false

EOF

# -----------------------------------------------------------------------------
# 步骤 7 - 追加 JWT 认证片段到 nats-server.conf
# 说明:
#   - 将 certs/nats-jwt.conf（包含 operator、resolver: MEMORY、resolver_preload）
#     直接拼接到 nats-server.conf 末尾，供 NATS 启动时一次性加载
# -----------------------------------------------------------------------------
echo "
# JWT Authentication Configuration"
cat "$CERT_DIR/nats-jwt.conf" >> "$CERT_DIR/nats-server.conf"

# -----------------------------------------------------------------------------
# 步骤 8 - 清理临时文件
# 说明: 删除 OpenSSL 生成的中间文件，保持仓库整洁
# -----------------------------------------------------------------------------
rm -f ca.conf grpc-ca.conf server.conf.ext client.conf.ext server.csr client.csr grpc-server.csr grpc-client.csr *.srl || true

# -----------------------------------------------------------------------------
# 步骤 9 - 设置文件权限
# 说明: 为私钥/证书/凭据文件设置合理权限，降低泄露风险
# -----------------------------------------------------------------------------
chmod 600 "$CERT_DIR"/*-key.pem
chmod 644 "$CERT_DIR"/*.pem
chmod 600 "$CERT_DIR"/*.creds

# -----------------------------------------------------------------------------
# 完成: 所有证书、JWT、配置已生成；可通过 docker compose 启动 NATS
# 客户端连接建议:
#   - TLS: 使用 certs/nats-ca.pem 校验
#   - JWT: 使用 certs/nats.creds（如 async-nats 的 with_credentials_file ）
#   - mTLS: 需要加载 certs/nats-client.pem 与 certs/nats-client-key.pem
# -----------------------------------------------------------------------------
echo "Certificates and NATS credentials generated under $CERT_DIR"
