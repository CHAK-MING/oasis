# Oasis

在 OpenCloudOS Stream 23/OpenCloudOS 9 上的大规模集群节点管理工具

## 快速开始（本机）

1. 初始化（生成证书、配置与 docker-compose.yml）

```bash
oasis-cli system init --output-dir . --force
```

2. 启动 NATS

```bash
docker-compose up -d
```

3. 启动服务器

```bash
oasis-cli system start --daemon
```

4. 启动本机 Agent（使用 `./certs` 证书）

```bash
OASIS_NATS_URL=tls://127.0.0.1:4222 \
OASIS_NATS_CA=certs/nats-ca.pem \
OASIS_NATS_CLIENT_CERT=certs/nats-client.pem \
OASIS_NATS_CLIENT_KEY=certs/nats-client-key.pem \
OASIS_SERVER_URL=https://127.0.0.1:50051 \
OASIS_GRPC_CA=certs/grpc-ca.pem \
OASIS_GRPC_CLIENT_CERT=certs/grpc-client.pem \
OASIS_GRPC_CLIENT_KEY=certs/grpc-client-key.pem \
./target/debug/oasis-agent
```

5. 执行命令并获取结果（exec 已内置结果获取）

```bash
# 下发任务（不等待）
oasis-cli exec --target true -- /bin/echo hello

# 下发并等待 5 秒结果
oasis-cli exec --target true --wait-ms 5000 -- /bin/echo hi

# 下发并流式查看结果（直到 Ctrl+C）
oasis-cli exec --target true --stream -- /bin/echo hi
```

说明：历史上的 `task get` 将逐步移除，请优先使用 `exec --wait-ms` 或 `--stream`。

6. 查看状态与停止

```bash
oasis-cli system status
oasis-cli system stop
```

## 远端部署 Agent（示例）

```bash
oasis-cli agent deploy \
  --target user@host \
  --server-url https://127.0.0.1:50051 \
  --nats-url tls://127.0.0.1:4222 \
  --output-dir ./deploy

# 将 oasis-agent 放入 ./deploy 目录后执行
cd deploy && ./install.sh
```

`agent.env`（自动生成）中的关键变量：

- OASIS_SERVER_URL：Server 的 gRPC 地址（必须为 https）
- OASIS_NATS_URL：NATS 地址（必须为 tls）
- 证书路径默认在 `/opt/oasis/certs/`，安装脚本会从 `deploy/certs/` 拷贝过去

## CLI 用法

### system：系统管理

```bash
# 初始化系统
oasis-cli system init --force

# 启动服务器（守护模式，日志重定向到文件）
oasis-cli system start --daemon --log-file ./oasis-server.log

# 查看状态
oasis-cli system status

# 停止服务器
oasis-cli system stop
```

### exec：执行命令

```bash
# 基本执行
oasis-cli exec --target 'labels["role"] == "web"' -- /usr/bin/uptime

# 等待结果（5秒超时）
oasis-cli exec --target true --wait-ms 5000 -- /bin/echo hi

# 流式查看结果
oasis-cli exec --target true --stream -- /bin/echo hi
```

### file：分发文件

```bash
# 分发文件
oasis-cli file apply --src ./nginx.conf --dest /etc/nginx/nginx.conf --target 'labels["role"] == "web"'

# 清空对象存储
oasis-cli file clear
```

### agent：Agent 管理

```bash
# 部署 Agent
oasis-cli agent deploy --target user@host --output-dir ./deploy

# 列出 Agent（简洁模式）
oasis-cli agent list

# 列出 Agent（详细模式，包含系统信息）
oasis-cli agent list --verbose

# 移除 Agent
oasis-cli agent remove --target user@host
```

### rollout：灰度发布

```bash
# 启动灰度
oasis-cli rollout start --name my-rollout --strategy rolling -t 'labels["role"] == "web"' --task-file ./task.sh

# 查看状态
oasis-cli rollout status my-rollout

# 控制灰度
oasis-cli rollout pause my-rollout
oasis-cli rollout resume my-rollout
oasis-cli rollout abort my-rollout
oasis-cli rollout rollback my-rollout
```

### storage：存储管理

```bash
# 查看存储信息
oasis-cli storage info

# 设置存储容量
oasis-cli storage set-capacity --size-gb 100
```

## 输出说明
- CLI 输出使用中文，支持彩色显示
- 默认启用 ANSI 颜色（可通过 `CLICOLOR_FORCE=0` 关闭）
- 服务器日志重定向到文件（守护模式）
