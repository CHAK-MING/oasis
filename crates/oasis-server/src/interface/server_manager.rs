use oasis_core::error::Result;
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

        let mut backoff_ms: u64 = 500;
        loop {
            if shutdown_token.is_cancelled() {
                info!("Shutdown requested before (re)starting gRPC server");
                return Ok(());
            }

            info!("Creating new gRPC service instance");
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
                .timeout(Duration::from_secs(60))
                .http2_keepalive_interval(Some(Duration::from_secs(30)))
                .http2_keepalive_timeout(Some(Duration::from_secs(60)))
                .tcp_keepalive(Some(Duration::from_secs(60)))
                .max_concurrent_streams(Some(256));

            info!("gRPC server is about to listen on {}", addr);
            // 启动服务器，直到收到关闭信号
            let result = builder
                .add_service(svc)
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                    info!("Gracefully shutting down gRPC server");
                })
                .await;

            match result {
                Ok(()) => {
                    // 正常完成通常意味着收到关停信号
                    if shutdown_token.is_cancelled() {
                        info!("gRPC server shut down successfully (by shutdown signal)");
                        return Ok(());
                    }
                    // 未收到关停却提前结束，视为异常，尝试自愈重启
                    warn!("gRPC server exited without shutdown signal; restarting after backoff");
                }
                Err(e) => {
                    error!("gRPC server error: {} | debug: {:#?}", e, e);
                    let mut source = std::error::Error::source(&e);
                    while let Some(s) = source {
                        error!("  caused by: {}", s);
                        source = s.source();
                    }
                }
            }

            // 退避后重试
            tokio::select! {
                _ = shutdown_token.cancelled() => { info!("Shutdown requested; stop restarting gRPC server"); return Ok(()); }
                _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
            }
            backoff_ms = (backoff_ms.saturating_mul(2)).min(30_000);
            info!("Restarting gRPC server after backoff: {} ms", backoff_ms);
        }
    }
}
