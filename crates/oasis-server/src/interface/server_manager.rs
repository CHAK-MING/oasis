use anyhow::Result;
use tonic::transport::Server;
use tracing::info;

/// gRPC 服务器管理器
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
        F: FnMut() -> (
            oasis_core::proto::oasis_service_server::OasisServiceServer<
                crate::interface::grpc::server::OasisServer,
            >,
            Option<std::sync::Arc<crate::interface::health::HealthService>>,
        ),
    {
        // 如果提供了 TLS 服务，则订阅热重载通知
        let mut tls_reload_rx = tls_service.as_ref().map(|svc| svc.get_reload_receiver());

        loop {
            // 启动一次服务器
            let (svc, health_service) = make_svc();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let token = shutdown_token.clone();
            tokio::spawn(async move {
                token.cancelled().await;
                let _ = shutdown_tx.send(());
            });

            // 构建 Server，并在可用时应用 TLS 配置
            let tls_for_spawn = tls_service.clone();
            let server = tokio::spawn(async move {
                // 创建标准健康服务，并通过 HealthService 标记为 Serving
                let (mut reporter, health_svc) = tonic_health::server::health_reporter();
                if let Some(h) = health_service.clone() {
                    h.register_reporter(reporter.clone()).await;
                    h.set_serving_oasis().await;
                } else {
                    let _ = reporter
                        .set_serving::<oasis_core::proto::oasis_service_server::OasisServiceServer<
                            crate::interface::grpc::server::OasisServer,
                        >>()
                        .await;
                }
                // 构建 Server 并按需应用 TLS
                let mut builder = if let Some(ref svc) = tls_for_spawn {
                    if let Some(tls_cfg) = svc.get_server_tls_config().await {
                        Server::builder()
                            .tls_config(tls_cfg)
                            .expect("failed to apply TLS config")
                    } else {
                        Server::builder()
                    }
                } else {
                    Server::builder()
                };

                builder
                    .add_service(health_svc)
                    .add_service(svc)
                    .serve_with_shutdown(addr, async {
                        let _ = shutdown_rx.await;
                        // 关闭前将健康置为 NotServing，便于探针快速摘流
                        if let Some(h) = health_service.clone() {
                            h.set_not_serving_oasis().await;
                        } else {
                            let _ = reporter
                                .set_not_serving::<oasis_core::proto::oasis_service_server::OasisServiceServer<
                                    crate::interface::grpc::server::OasisServer,
                                >>()
                                .await;
                        }
                    })
                    .await
            });

            if let Some(ref mut rx) = tls_reload_rx {
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        let _ = server.await;
                        break;
                    }
                    res = rx.recv() => {
                        if res.is_ok() {
                            info!("TLS reload signal received, restarting gRPC server...");
                            let _ = server.await;
                            continue; // 重启
                        } else {
                            // 通道关闭时不再监听 TLS 事件，但保持服务运行直到关闭信号
                            info!("TLS reload channel closed; disabling TLS reload and continuing run_loop.");
                            let _ = server.await;
                            // 退出循环由外层控制；此处 break 会停止一次服务器重启循环
                            break;
                        }
                    }
                }
            } else {
                // 不监听 TLS reload，仅等待关闭信号
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        let _ = server.await;
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
