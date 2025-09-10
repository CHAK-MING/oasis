use anyhow::Result;
use std::time::Duration;
use tonic::transport::Server;
use tracing::{error, info, warn};

/// gRPC 服务器管理器
///
/// 负责管理 gRPC 服务器的生命周期，包括：
/// - 优雅关闭处理
pub struct GrpcServerManager;

impl GrpcServerManager {
    /// 运行循环：启动 gRPC 服务器
    pub async fn run_loop<F>(
        addr: std::net::SocketAddr,
        mut make_svc: F,
        tls_service: Option<crate::infrastructure::tls::TlsService>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Result<()>
    where
        F: FnMut() -> oasis_core::proto::oasis_service_server::OasisServiceServer<
            crate::interface::grpc::server::OasisServer,
        >,
    {
        info!("Starting gRPC server manager on {}", addr);
        info!("Creating new gRPC server instance");

        // 构建服务
        let svc = make_svc();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let token = shutdown_token.clone();

        // 监听关闭信号
        tokio::spawn(async move {
            token.cancelled().await;
            let _ = shutdown_tx.send(());
        });

        // 构建 Server，并按需应用 TLS 配置
        let mut builder = if let Some(ref svc_tls) = tls_service {
            if let Some(tls_cfg) = svc_tls.get_server_tls_config().await {
                info!("Applying TLS configuration to gRPC server");
                Server::builder().tls_config(tls_cfg).map_err(|e| {
                    tracing::error!("Failed to apply TLS config: {}", e);
                    e
                })?
            } else {
                warn!("TLS service available but no TLS config, starting without TLS");
                Server::builder()
            }
        } else {
            info!("Starting gRPC server without TLS");
            Server::builder()
        };

        // 服务器端保持连接健康的配置 - 与客户端参数匹配
        builder = builder
            .timeout(Duration::from_secs(60)) // 增加请求超时到 60s
            .http2_keepalive_interval(Some(Duration::from_secs(30)))
            .http2_keepalive_timeout(Some(Duration::from_secs(60))) // 与客户端 keep_alive_timeout 匹配
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .max_concurrent_streams(Some(256)); // 增加并发流数量

        // 启动服务器，直到收到关闭信号
        let result = builder
            .add_service(svc)
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
                info!("Gracefully shutting down gRPC server");
            })
            .await;

        if let Err(e) = result {
            error!("gRPC server error: {} | debug: {:#?}", e, e);
            let mut source = std::error::Error::source(&e);
            while let Some(s) = source {
                error!("  caused by: {}", s);
                source = s.source();
            }
            Err(e.into())
        } else {
            info!("gRPC server shut down successfully");
            Ok(())
        }
    }
}
