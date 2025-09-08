# Oasis

在 OpenCloudOS Stream 23/OpenCloudOS 9 上的大规模集群节点管理工具

## 快速开始

1. 初始化（生成证书、配置与 docker-compose.yml）

```bash
./oasis-cli system init --output-dir .
```

2. 启动 NATS

```bash
docker compose up -d
```

3. 启动服务器

```bash
./oasis-cli system start -d
```

4. 部署 Agent

```bash
./oasis-cli agent deploy \
  --agent-id agent123456
  --ssh-target user@remote-host \
  --nats-url tls://YOUR_SERVER_IP:4222 \
  --output-dir ~/agent-deploy \
  --labels "env=test" \
  --labels "role=worker" \
  --groups "test-group" \
  --agent-binary ./oasis-agent

# 安装
cd ~/agent-deploy/*/
sudo ./install.sh

# 查看状态
sudo systemctl status oasis-agent
```

5. 执行命令并获取结果

```bash
# 下发任务
./oasis-cli exec run -t 'true' -- /bin/echo hello

# 获取任务结果
./oasis-cli exec get <batch_id>

# 列出最近任务
./oasis-cli exec list --limit 20

# 取消任务
./oasis-cli exec cancel <batch_id>
```

6. 查看系统状态与停止

```bash
oasis-cli system status
oasis-cli system stop
```

7. 移除 Agent

```bash
./oasis-cli agent remove \
  --ssh-target root@localhost \
  --agent-id agent123456
```

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
# 下发任务
oasis-cli exec run -t 'labels["role"] == "worker"' -- /usr/bin/uptime

# 下发任务（支持系统参数）
oasis-cli exec run -t 'system["hostname"] == "server-name"' -- /usr/bin/uptime

# 下发任务（支持下发全部在线的 Agent）
oasis-cli exec run -t 'true' -- /usr/bin/uptime

# 获取某任务结果
oasis-cli exec get <batch_id>

# 列出最近任务
oasis-cli exec list --limit 20
```

### file：分发文件

```bash
# 向 Web 服务器分发 nginx 配置
oasis-cli file apply --src ./nginx.conf --dest /etc/nginx/nginx.conf --target 'labels["role"] == "web"'

# 设置权限/属主
oasis-cli file apply --src ./app.conf --dest /etc/myapp/config.conf --target 'labels["environment"] == "prod"' --owner root:root --mode 0644

# 指定多个 agent ID
oasis-cli file apply --src ./config.conf --dest /etc/config.conf --target 'agent-1,agent-2,agent-3'

# 查看文件的历史版本
oasis-cli file history --source-path ./nginx.conf

# 回滚文件到指定版本
oasis-cli file rollback --source-path ./nginx.conf --revision 1 --dest /etc/nginx/nginx.conf --target 'labels["role"] == "web"'

# 清空文件仓库（对象存储）——危险操作，会提示确认
oasis-cli file clear"
```

### agent：Agent 管理

```bash
# 部署 Agent
oasis-cli agent deploy --agent-id <agent_id> --ssh-target user@host --output-dir ./deploy

# 列出 Agent（简洁模式）
oasis-cli agent list

# 列出 Agent（详细模式，包含系统信息）
oasis-cli agent list --verbose

# 移除 Agent
oasis-cli agent remove --agent-id <agent_id> --ssh-target user@host
```

### rollout：灰度发布

```bash
# 创建命令灰度发布
oasis-cli rollout create \
  --name "系统更新" \
  --target 'labels["role"] == "web"' \
  --strategy percentage:10,30,60,100 \
  --command "dnf update && dnf upgrade -y" \
  --timeout 300

# 创建文件灰度发布
oasis-cli rollout create \
  --name "配置更新" \
  --target 'labels["environment"] == "prod"' \
  --strategy count:2,5,10,0 \
  --file-src ./nginx.conf \
  --file-dest /etc/nginx/nginx.conf \
  --file-mode 0644

# 查看发布状态
oasis-cli rollout status rollout-12345678

# 推进到下一阶段
oasis-cli rollout advance rollout-12345678

# 回滚发布
oasis-cli rollout rollback rollout-12345678 --rollback-cmd "systemctl restart nginx"
```

## 语法器用法说明

### 基础语法
```bash
# all 或 true表示所有Agent
all | true
# 指定AgentId
agent-1,agent-2,agent-3
# 指定标签
labels["version"] == "1.0"
# 指定系统参数
system["hostname"] == "server01"
# 指定分组
"production" in groups
```


### 基础逻辑运算
```bash
# 与运算
labels["env"] == "prod" and system["os_name"] == "linux"

# 或运算  
labels["team"] == "backend" or labels["team"] == "frontend"

# 非运算
not labels["maintenance"] == "true"

# 运算符优先级是not > and > or 符合常见的运算规则
```

### 复杂表达式
```bash
# 括号分组
(labels["env"] == "prod" or labels["env"] == "staging") and system["cpu_cores"] == "8"

# 多重嵌套
not (labels["deprecated"] == "true" or system["os_name"] == "windows")

# 混合条件
"web-servers" in groups and labels["env"] == "prod" and not system["memory_total_gb"] == "1"
```