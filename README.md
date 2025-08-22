# Oasis

Oasis 是一个为大规模集群设计的分布式命令执行与文件分发系统，核心通信基于 NATS JetStream，管控接口基于 gRPC。

## 核心功能

- **远程命令执行**: 在符合条件的节点上安全地执行 Shell 命令
- **文件分发**: 将文件或配置原子化地应用到目标节点
- **动态配置管理**: 实时更新 Agent 的运行时配置，支持热重载
- **灰度发布**: 支持滚动、金丝雀等策略，对变更进行分批、可控的部署
- **目标选择**: 使用 CEL 表达式，基于节点标签（Labels）和事实（Facts）进行精确的目标选择

---

## 快速开始

### 1. 启动基础设施

项目提供了一个 `docker-compose.yml` 文件来快速启动 NATS 服务。

```bash
docker compose up -d
```

### 2. 构建项目

```bash
cargo build --release
```

构建产物位于 `target/release/` 目录下。

### 3. 启动 Server

Server 通过 `--config` 参数指定配置文件路径。

```bash
./target/release/oasis-server --config config/oasis-server.toml
```

### 4. 启动 Agent

Agent 以单一二进制文件分发，支持两种配置方式：

```bash
# 方式1: 使用环境变量（推荐用于容器化部署）
# 环境变量格式：OASIS_AGENT__<SECTION>__<KEY>
OASIS_AGENT__AGENT__AGENT_ID=agent-001 \
OASIS_AGENT__NATS__URL=nats://<NATS_SERVER_IP>:4222 \
OASIS_AGENT__SECURITY__ENFORCE_WHITELIST=true \
OASIS_AGENT__SECURITY__WHITELIST_PATHS='["/tmp", "/home"]' \
./target/release/oasis-agent

# 方式2: 通过 CLI 动态下发配置
./target/release/oasis-cli --config config/oasis-cli.toml config apply --src config/oasis-agent.toml --selector 'agent_id == "agent-001"'
```

### 5. 使用 CLI

CLI 通过 `--config` 参数指定配置文件路径。

```bash
# 使用配置文件
./target/release/oasis-cli --config config/oasis-cli.toml node ls
```

---

## CLI 用法示例

### 节点管理 (`node`)

```bash
# 列出所有在线节点
oasis-cli --config config/oasis-cli.toml node ls

# 列出生产环境的节点（environment 融合进 labels）
oasis-cli --config config/oasis-cli.toml node ls --selector 'labels["environment"] == "production"'

# 查看指定节点的详细信息（Facts）
oasis-cli --config config/oasis-cli.toml node facts --selector 'agent_id == "agent-001"'

# 查看所有 Web 服务器的系统信息
oasis-cli --config config/oasis-cli.toml node facts --selector 'labels["role"] == "web"' --output json
```

### 远程执行 (`exec`)

```bash
# 在所有 `role == 'web'` 的节点上执行 `uptime`
oasis-cli --config config/oasis-cli.toml exec --selector 'labels["role"] == "web"' -- /usr/bin/uptime

# 在生产环境的前端服务器上执行命令（groups 为独立字段）
oasis-cli --config config/oasis-cli.toml exec --selector 'labels["environment"] == "prod" && "frontend" in groups' -- /usr/bin/ps aux

# 执行命令并传递环境变量
oasis-cli --config config/oasis-cli.toml exec --selector 'agent_id == "agent-1"' --env DEBUG=true --env LOG_LEVEL=info -- /usr/bin/echo $DEBUG

# 在所有节点上执行（谨慎使用）
oasis-cli --config config/oasis-cli.toml exec --selector 'true' -- /usr/bin/uptime
```

### 文件分发 (`file`)

```bash
# 将本地的 nginx.conf 分发到目标节点的 /etc/nginx/ 并原子化更新
oasis-cli --config config/oasis-cli.toml file apply \
  --src ./nginx.conf \
  --dest /etc/nginx/nginx.conf \
  --selector 'labels["role"] == "web"' \
  --atomic

# 分发配置文件并设置权限
oasis-cli --config config/oasis-cli.toml file apply \
  --src ./app.conf \
  --dest /etc/myapp/config.conf \
  --selector 'labels["environment"] == "prod"' \
  --atomic \
  --owner root:root \
  --mode 0644

# 验证文件完整性
oasis-cli --config config/oasis-cli.toml file apply \
  --src ./secure.conf \
  --dest /etc/secure.conf \
  --selector 'labels["role"] == "db"' \
  --sha256 a1b2c3d4e5f6...
```

### 配置管理 (`config`)

```bash
# 应用配置文件到匹配的 agents
oasis-cli --config config/oasis-cli.toml config apply --src ./agent.toml --selector 'labels["role"] == "web"'

# 设置单个配置项
oasis-cli --config config/oasis-cli.toml config set --component agent --agent-id agent-1 security.enforce_whitelist true

# 获取配置值
oasis-cli --config config/oasis-cli.toml config get --component agent --agent-id agent-1 security.enforce_whitelist

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
oasis-cli --config config/oasis-cli.toml config delete --component agent --agent-id agent-1 security.enforce_whitelist
```

### 灰度发布 (`rollout`)

```bash
# 创建一个滚动更新任务，每次更新 10% 的节点
oasis-cli --config config/oasis-cli.toml rollout start \
  --name 'security-updates' \
  --strategy rolling \
  --selector 'labels["os"] == "opencloudos"' \
  --task-exec 'yum update -y --security' \
  --batch-size 10% \
  --auto-advance

# 监控发布状态
oasis-cli --config config/oasis-cli.toml rollout status security-updates --watch
```

## TODO

当前还只是一个半成品项目，还有许多复杂测试没有做。代码也存在诸多问题，后面会慢慢调整。大多数功能均已经实现。当前的实现与初版的设计文档之间存在一些差异。后面，会重新上传一份最新的文档。
