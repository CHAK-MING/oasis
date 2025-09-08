use anyhow::Result;
use std::time::Duration;
use tonic::transport::Server;
use tracing::{error, info, warn};

/// gRPC 服务器管理器
///
/// 负责管理 gRPC 服务器的生命周期，包括：
/// - TLS 热重载支持
/// - 优雅关闭处理
pub struct GrpcServerManager;

impl GrpcServerManager {
    /// 运行循环：根据 TLS 变更信号优雅重启 gRPC 服务器
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

        // 如果提供了 TLS 服务，则订阅热重载通知
        let mut tls_reload_rx = tls_service.as_ref().map(|svc| svc.get_reload_receiver());

        loop {
            info!("Creating new gRPC server instance");

            // 启动一次服务器
            let svc = make_svc();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let token = shutdown_token.clone();

            // 监听关闭信号
            tokio::spawn(async move {
                token.cancelled().await;
                let _ = shutdown_tx.send(());
            });

            // 构建 Server，并在可用时应用 TLS 配置
            let tls_for_spawn = tls_service.clone();
            let addr_for_spawn = addr;
            let server_handle = tokio::spawn(async move {
                // 构建 Server 并按需应用 TLS
                let mut builder = if let Some(ref svc) = tls_for_spawn {
                    if let Some(tls_cfg) = svc.get_server_tls_config().await {
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

                // 启动服务器
                // Server-side keepalive to keep connections healthy across idle periods
                builder = builder
                    .http2_keepalive_interval(Some(Duration::from_secs(30)))
                    .http2_keepalive_timeout(Some(Duration::from_secs(5)))
                    .tcp_keepalive(Some(Duration::from_secs(60)));

                let result = builder
                    .add_service(svc)
                    .serve_with_shutdown(addr_for_spawn, async {
                        let _ = shutdown_rx.await;
                        info!("Gracefully shutting down gRPC server");
                    })
                    .await;

                if let Err(e) = result {
                    error!("gRPC server error: {}", e);
                    Err(e)
                } else {
                    info!("gRPC server shut down successfully");
                    Ok(())
                }
            });

            // 等待关闭信号或 TLS 重载信号
            if let Some(ref mut rx) = tls_reload_rx {
                tokio::select! {
                    // 全局关闭信号
                    _ = shutdown_token.cancelled() => {
                        info!("Global shutdown signal received");
                        break;
                    }
                    // TLS 重载信号
                    res = rx.recv() => {
                        if res.is_ok() {
                            info!("TLS reload signal received, restarting gRPC server...");
                            continue; // 重启服务器
                        } else {
                            // TLS 通道关闭，继续运行但不再监听 TLS 重载
                            warn!("TLS reload channel closed, disabling TLS reload");
                            tls_reload_rx = None;
                            // 如果没有全局关闭信号，继续运行
                            if !shutdown_token.is_cancelled() {
                                continue;
                            } else {
                                break;
                            }
                        }
                    }
                    // 服务器异常退出
                    server_result = server_handle => {
                        match server_result {
                            Ok(Ok(())) => {
                                info!("Server completed successfully");
                            }
                            Ok(Err(e)) => {
                                error!("Server error: {}", e);
                                return Err(e.into());
                            }
                            Err(e) => {
                                error!("Server task panicked: {}", e);
                                return Err(e.into());
                            }
                        }

                        // 如果没有关闭信号，则重启
                        if !shutdown_token.is_cancelled() {
                            warn!("Server exited unexpectedly, restarting...");
                            continue;
                        } else {
                            break;
                        }
                    }
                }
            } else {
                // 不监听 TLS reload，仅等待关闭信号或服务器完成
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        info!("Global shutdown signal received");
                        break;
                    }
                    server_result = server_handle => {
                        match server_result {
                            Ok(Ok(())) => {
                                info!("Server completed successfully");
                            }
                            Ok(Err(e)) => {
                                error!("Server error: {}", e);
                                return Err(e.into());
                            }
                            Err(e) => {
                                error!("Server task panicked: {}", e);
                                return Err(e.into());
                            }
                        }

                        // 如果没有关闭信号，则重启
                        if !shutdown_token.is_cancelled() {
                            warn!("Server exited unexpectedly, restarting...");
                            continue;
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        info!("gRPC server manager shut down successfully");
        Ok(())
    }
}
