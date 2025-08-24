# Oasis

Oasis 是一个为大规模集群设计的分布式命令执行与文件分发系统，核心通信基于 NATS JetStream，管控接口基于 gRPC。

## 核心功能

- **远程命令执行**: 在符合条件的节点上安全地执行 Shell 命令
- **文件分发**: 将文件或配置原子化地应用到目标节点
- **动态配置管理**: 实时更新 Agent 的运行时配置，支持热重载
- **灰度发布**: 支持滚动、金丝雀等策略，对变更进行分批、可控的部署
- **目标选择**: 使用 CEL 表达式或直接指定 Agent ID 列表，基于节点标签（Labels）进行精确的目标选择

---

## 快速开始

### 1. 启动基础设施

#### 1.1 生成证书

```bash
./scripts/generate-certs.sh
```

将生成的证书放在 `certs/` 目录下。Server 与 CLI 的 gRPC 必须启用 TLS（mTLS），NATS 也需要 TLS + JWT。

#### 1.2 启动 NATS 服务

```bash
docker compose up -d
```

### 2. 构建项目

```bash
cargo build --release
```

### 3. 启动 Server

```bash
./target/release/oasis-server --config config/oasis-server.toml
```

`config/oasis-server.toml` 关键段：

```toml
[server]
grpc_addr = "127.0.0.1:50051"

[server.grpc_tls]
ca_cert = "certs/grpc-ca.pem"
server_cert = "certs/grpc-server.pem"
server_key = "certs/grpc-server-key.pem"
cert_check_interval_sec = 300
```

### 4. 启动 Agent

- 支持环境变量（带 `OASIS_AGENT_` 前缀，`__` 作为分层分隔符）+ 远端 NATS KV 覆盖
- 或通过 CLI 下发配置（TOML）

```bash
# 方式1: 环境变量（示例）
OASIS_AGENT_AGENT_ID=agent-001 \
OASIS_NATS_URL=tls://<NATS_SERVER_IP>:4443 \
./target/release/oasis-agent

# 方式2: 通过 CLI 下发配置（TOML）
./target/release/oasis-cli --config config/oasis-cli.toml config apply --src config/oasis-agent.toml -t 'agent_id == "agent-001"'
```

### 5. 使用 CLI（统一 --target/-t）

```bash
# 列出节点
./target/release/oasis-cli --config config/oasis-cli.toml node ls

# 过滤节点
./target/release/oasis-cli --config config/oasis-cli.toml node ls -t 'labels["environment"] == "production"'
```

---

## CLI 用法示例

### 目标选择方式

- 批量命令（exec、file apply、rollout、node、config apply）统一使用 `--target`/`-t`：
  - 支持 CEL 表达式（如 `labels["role"] == "web"`）
  - 支持逗号分隔的 Agent ID 列表（如 `agent-001,agent-002`）
  - 支持 `true` 表示全部节点
- 配置管理（单点操作）使用 `--agent-id`

### 节点管理 (`node`)

```bash
# 列出所有在线节点
oasis-cli --config config/oasis-cli.toml node ls

# 列出生产环境的节点
oasis-cli --config config/oasis-cli.toml node ls -t 'labels["environment"] == "production"'

# 查看指定节点的详细信息（Facts）
oasis-cli --config config/oasis-cli.toml node facts -t 'agent-001'

# 查看所有 Web 服务器的系统信息（JSON）
oasis-cli --config config/oasis-cli.toml node facts -t 'labels["role"] == "web"' --output json
```

### 远程执行 (`exec`)

```bash
# 在所有 `role == 'web'` 的节点上执行 `uptime`
oasis-cli --config config/oasis-cli.toml exec -t 'labels["role"] == "web"' -- /usr/bin/uptime

# 在生产环境的前端服务器上执行命令
oasis-cli --config config/oasis-cli.toml exec -t 'labels["environment"] == "prod" && "frontend" in groups' -- /usr/bin/ps aux

# 在所有节点上执行（谨慎使用）
oasis-cli --config config/oasis-cli.toml exec -t 'true' -- /usr/bin/uptime

# 在指定的 Agent ID 列表上执行命令
oasis-cli --config config/oasis-cli.toml exec -t 'agent-001,agent-002,agent-003' -- /usr/bin/uptime
```

### 文件分发 (`file`)

```bash
# 将本地的 nginx.conf 分发到目标节点并原子化更新
oasis-cli --config config/oasis-cli.toml file apply \
  --src ./nginx.conf \
  --dest /etc/nginx/nginx.conf \
  -t 'labels["role"] == "web"' \
  --atomic

# 验证文件完整性
oasis-cli --config config/oasis-cli.toml file apply \
  --src ./secure.conf \
  --dest /etc/secure.conf \
  -t 'labels["role"] == "db"' \
  --sha256 a1b2c3d4e5f6...
```

### 配置管理 (`config`)

```bash
# 应用配置文件到匹配的 agents
oasis-cli --config config/oasis-cli.toml config apply --src ./agent.toml -t 'labels["role"] == "web"'

# 设置单个配置项（单点）
oasis-cli --config config/oasis-cli.toml config set --component agent --agent-id agent-1 telemetry.log_level debug

# 获取配置值
oasis-cli --config config/oasis-cli.toml config get --component agent --agent-id agent-1 telemetry.log_level

# 列出所有配置键
oasis-cli --config config/oasis-cli.toml config list --component agent --agent-id agent-1

# 显示配置摘要
oasis-cli --config config/oasis-cli.toml config show --component agent --agent-id agent-1

# 备份配置
oasis-cli --config config/oasis-cli.toml config backup --component agent --agent-id agent-1 --output backup.toml

# 验证配置文件
oasis-cli --config config/oasis-cli.toml config validate --component agent --file agent.toml

# 比较配置差异
oasis-cli --config config/oasis-cli.toml config diff --component agent config/old.toml config/new.toml

# 删除配置项
oasis-cli --config config/oasis-cli.toml config delete --component agent --agent-id agent-1 telemetry.log_level
```

### 灰度发布 (`rollout`)

```bash
# 创建一个滚动更新任务，每次更新 10% 的节点
oasis-cli --config config/oasis-cli.toml rollout start \
  --name 'security-updates' \
  --strategy rolling \
  -t 'labels["os"] == "opencloudos"' \
  --task-file ./update-script.sh \
  --batch-size 10% \
  --auto-advance

# 监控发布状态
oasis-cli --config config/oasis-cli.toml rollout status security-updates --watch
```

## TODO

当前还只是一个半成品项目，还有许多复杂测试没有做。代码也存在诸多问题，后面会慢慢调整。大多数功能均已经实现。当前的实现与初版的设计文档之间存在一些差异。后面，会重新上传一份最新的文档。
