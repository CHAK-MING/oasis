use anyhow::Result;
use tonic::transport::{Server, ServerTlsConfig};
use tonic_health::server::health_reporter;
use tracing::info;

/// gRPC 服务器管理器
pub struct GrpcServerManager;

impl GrpcServerManager {
    /// 运行循环：根据 TLS 变更信号优雅重启 gRPC 服务器
    pub async fn run_loop<F>(
        addr: std::net::SocketAddr,
        mut make_svc: F,
        mut tls_reload_rx: tokio::sync::broadcast::Receiver<()>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Result<()>
    where
        F: FnMut() -> oasis_core::proto::oasis_service_server::OasisServiceServer<
            crate::interface::grpc::server::OasisServer,
        >,
    {
        loop {
            // 启动一次服务器
            let svc = make_svc();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let token = shutdown_token.clone();
            tokio::spawn(async move {
                token.cancelled().await;
                let _ = shutdown_tx.send(());
            });

            let server = tokio::spawn(async move {
                Server::builder()
                    .add_service(tonic_health::server::health_reporter().1)
                    .add_service(svc)
                    .serve_with_shutdown(addr, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
            });

            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    let _ = server.await;
                    break;
                }
                res = tls_reload_rx.recv() => {
                    if res.is_ok() {
                        info!("TLS reload signal received, restarting gRPC server...");
                        let _ = server.await;
                        continue; // 重启
                    } else {
                        info!("TLS reload channel closed; shutting down run_loop.");
                        let _ = server.await;
                        break;
                    }
                }
            }
        }
        Ok(())
    }
    /// 启动 gRPC 服务器
    pub async fn start(
        addr: std::net::SocketAddr,
        svc: oasis_core::proto::oasis_service_server::OasisServiceServer<
            crate::interface::grpc::server::OasisServer,
        >,
        shutdown_signal: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<()> {
        info!(%addr, "Starting gRPC server");

        // gRPC健康服务
        let (mut health_reporter, health_service) = health_reporter();
        health_reporter
            .set_serving::<oasis_core::proto::oasis_service_server::OasisServiceServer<
                crate::interface::grpc::server::OasisServer,
            >>()
            .await;

        // 启动gRPC服务器
        let server_result = Server::builder()
            .add_service(health_service)
            .add_service(svc)
            .serve_with_shutdown(addr, async {
                let _ = shutdown_signal.await;
            })
            .await;

        info!("gRPC server stopped");
        server_result?;
        Ok(())
    }

    /// 启动带 TLS 的 gRPC 服务器
    pub async fn start_with_tls(
        addr: std::net::SocketAddr,
        svc: oasis_core::proto::oasis_service_server::OasisServiceServer<
            crate::interface::grpc::server::OasisServer,
        >,
        tls_config: ServerTlsConfig,
        shutdown_signal: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<()> {
        info!(%addr, "Starting gRPC server with TLS");

        // gRPC健康服务
        let (mut health_reporter, health_service) = health_reporter();
        health_reporter
            .set_serving::<oasis_core::proto::oasis_service_server::OasisServiceServer<
                crate::interface::grpc::server::OasisServer,
            >>()
            .await;

        // 启动带 TLS 的 gRPC 服务器
        let server_result = Server::builder()
            .tls_config(tls_config)?
            .add_service(health_service)
            .add_service(svc)
            .serve_with_shutdown(addr, async {
                let _ = shutdown_signal.await;
            })
            .await;

        info!("gRPC server with TLS stopped");
        server_result?;
        Ok(())
    }
}
