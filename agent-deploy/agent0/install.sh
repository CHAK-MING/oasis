#!/bin/bash
# Oasis Agent 自动安装脚本
# 目标: chakming@127.0.0.1
# Agent ID: agent0

set -e

echo "==========================================="
echo "  Oasis Agent 自动安装"
echo "  Agent ID: agent0"
echo "==========================================="

# 检查是否以 root 权限运行
if [[ $EUID -ne 0 ]]; then
   echo "此脚本需要 root 权限运行"
   echo "请使用: sudo $0"
   exit 1
fi

echo "► 创建必要目录..."
mkdir -p /opt/oasis/agent
mkdir -p /opt/oasis/certs
mkdir -p /var/log/oasis

echo "► 安装 Agent 文件..."
if [ -f "oasis-agent" ]; then
    cp oasis-agent /opt/oasis/agent/
    chmod +x /opt/oasis/agent/oasis-agent
    echo "  ✔ Agent 二进制文件已安装"
else
    echo "  ⚠ Agent 二进制文件未找到，请手动复制"
fi

echo "► 安装配置文件..."
cp agent.env /opt/oasis/agent/
echo "  ✔ 环境变量文件已安装"

echo "► 安装证书..."
if [ -d "certs" ]; then
    cp -r certs/* /opt/oasis/certs/
    chmod 600 /opt/oasis/certs/*.pem 2>/dev/null || true
    echo "  ✔ 证书已安装"
else
    echo "  ⚠ 证书目录未找到"
fi

echo "► 安装 systemd 服务..."
cp oasis-agent.service /etc/systemd/system/
systemctl daemon-reload
echo "  ✔ systemd 服务已安装"

echo "► 启动 Agent 服务..."
systemctl enable oasis-agent
systemctl restart oasis-agent

# 等待服务启动
sleep 2

# 检查服务状态
if systemctl is-active --quiet oasis-agent; then
    echo "  ✔ Agent 服务已成功启动"
    echo ""
    echo "==========================================="
    echo "  安装完成！"
    echo "  Agent ID: agent0"
    echo "  状态: 运行中"
    echo "==========================================="
    echo ""
    echo "有用的命令:"
    echo "  查看状态: systemctl status oasis-agent"
    echo "  查看日志: journalctl -u oasis-agent -f"
    echo "  重启服务: systemctl restart oasis-agent"
else
    echo "  ✗ Agent 服务启动失败"
    echo "  请检查日志: journalctl -u oasis-agent -n 50"
    exit 1
fi
