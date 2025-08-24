use tonic::{Request, Response, Status};

use oasis_core::proto::{HealthCheckRequest, HealthCheckResponse};

use crate::interface::grpc::server::OasisServer;

pub struct HealthHandlers;

impl HealthHandlers {
    pub async fn health_check(
        srv: &OasisServer,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let is_healthy = srv.health_service().is_healthy().await;
        let nats_healthy = srv.health_service().is_nats_healthy().await;
        let agents_healthy = srv.health_service().is_agents_healthy().await;
        let online_count = srv.health_service().get_online_agents_count().await;

        let message = if is_healthy {
            format!(
                "healthy - NATS: {}, Agents: {} ({} online)",
                if nats_healthy { "OK" } else { "FAIL" },
                if agents_healthy { "OK" } else { "FAIL" },
                online_count
            )
        } else {
            format!(
                "unhealthy - NATS: {}, Agents: {} ({} online)",
                if nats_healthy { "OK" } else { "FAIL" },
                if agents_healthy { "OK" } else { "FAIL" },
                online_count
            )
        };

        Ok(Response::new(HealthCheckResponse {
            ok: is_healthy,
            message,
        }))
    }
}
