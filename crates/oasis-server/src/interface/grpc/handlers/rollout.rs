use tonic::{Request, Response, Status};

use crate::interface::grpc::converters::{from_proto_rollout_config, to_proto_rollout};
use crate::interface::grpc::errors::map_core_error;
use crate::interface::grpc::server::OasisServer;

pub struct RolloutHandlers;

impl RolloutHandlers {
    pub async fn create_rollout(
        srv: &OasisServer,
        request: Request<oasis_core::proto::CreateRolloutRequest>,
    ) -> Result<Response<oasis_core::proto::CreateRolloutResponse>, Status> {
        let req = request.into_inner();
        let task = req
            .task
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Task specification is required"))
            .and_then(|t| {
                crate::interface::grpc::converters::from_proto_task_spec(t).map_err(map_core_error)
            })?;

        let config = req
            .config
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Rollout configuration is required"))
            .and_then(|c| from_proto_rollout_config(c).map_err(map_core_error))?;

        let target_selector = match req.target.as_ref().and_then(|t| t.target.as_ref()) {
            Some(oasis_core::proto::task_target_msg::Target::Selector(s)) => s.clone(),
            None => return Err(Status::invalid_argument("Target is required")),
        };

        let payload =
            crate::application::use_cases::commands::rollout_deploy::CreateRolloutPayload {
                name: req.name.clone(),
                task,
                target_selector,
                config,
                labels: req.labels.clone(),
            };

        let rollout_id = srv
            .rollout_deploy_use_case()
            .create_rollout_from_payload(payload)
            .await
            .map_err(map_core_error)?;

        tracing::Span::current().record("rollout_id", &rollout_id);

        Ok(Response::new(oasis_core::proto::CreateRolloutResponse {
            rollout_id: Some(oasis_core::proto::RolloutId { value: rollout_id }),
        }))
    }

    pub async fn start_rollout(
        srv: &OasisServer,
        request: Request<oasis_core::proto::StartRolloutRequest>,
    ) -> Result<Response<oasis_core::proto::StartRolloutResponse>, Status> {
        let req = request.into_inner();
        if let Some(rollout_id) = &req.rollout_id {
            tracing::Span::current().record("rollout_id", &rollout_id.value);
        }
        let rollout_id = req
            .rollout_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("rollout_id is required"))?
            .value
            .as_str();
        srv.rollout_deploy_use_case()
            .start_rollout(rollout_id)
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(oasis_core::proto::StartRolloutResponse {}))
    }

    pub async fn pause_rollout(
        srv: &OasisServer,
        request: Request<oasis_core::proto::PauseRolloutRequest>,
    ) -> Result<Response<oasis_core::proto::PauseRolloutResponse>, Status> {
        let req = request.into_inner();
        if let Some(rollout_id) = &req.rollout_id {
            tracing::Span::current().record("rollout_id", &rollout_id.value);
        }
        let rollout_id = req
            .rollout_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("rollout_id is required"))?
            .value
            .as_str();
        srv.rollout_deploy_use_case()
            .pause_rollout(rollout_id, &req.reason)
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(oasis_core::proto::PauseRolloutResponse {}))
    }

    pub async fn resume_rollout(
        srv: &OasisServer,
        request: Request<oasis_core::proto::ResumeRolloutRequest>,
    ) -> Result<Response<oasis_core::proto::ResumeRolloutResponse>, Status> {
        let req = request.into_inner();
        if let Some(rollout_id) = &req.rollout_id {
            tracing::Span::current().record("rollout_id", &rollout_id.value);
        }
        let rollout_id = req
            .rollout_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("rollout_id is required"))?
            .value
            .as_str();
        srv.rollout_deploy_use_case()
            .resume_rollout(rollout_id)
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(oasis_core::proto::ResumeRolloutResponse {}))
    }

    pub async fn abort_rollout(
        srv: &OasisServer,
        request: Request<oasis_core::proto::AbortRolloutRequest>,
    ) -> Result<Response<oasis_core::proto::AbortRolloutResponse>, Status> {
        let req = request.into_inner();
        if let Some(rollout_id) = &req.rollout_id {
            tracing::Span::current().record("rollout_id", &rollout_id.value);
        }
        let rollout_id = req
            .rollout_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("rollout_id is required"))?
            .value
            .as_str();
        srv.rollout_deploy_use_case()
            .abort_rollout(rollout_id, &req.reason)
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(oasis_core::proto::AbortRolloutResponse {}))
    }

    pub async fn rollback_rollout(
        srv: &OasisServer,
        request: Request<oasis_core::proto::RollbackRolloutRequest>,
    ) -> Result<Response<oasis_core::proto::RollbackRolloutResponse>, Status> {
        let req = request.into_inner();
        if let Some(rollout_id) = &req.rollout_id {
            tracing::Span::current().record("rollout_id", &rollout_id.value);
        }
        let rollout_id = req
            .rollout_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("rollout_id is required"))?
            .value
            .as_str();
        srv.rollout_deploy_use_case()
            .rollback_rollout(rollout_id, &req.reason)
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(oasis_core::proto::RollbackRolloutResponse {}))
    }

    pub async fn get_rollout(
        srv: &OasisServer,
        request: Request<oasis_core::proto::GetRolloutRequest>,
    ) -> Result<Response<oasis_core::proto::GetRolloutResponse>, Status> {
        let req = request.into_inner();
        let rollout_id = req
            .rollout_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("rollout_id is required"))?
            .value
            .as_str();
        let rollout = srv
            .rollout_deploy_use_case()
            .get_rollout(rollout_id)
            .await
            .map_err(map_core_error)?;
        match rollout {
            Some(rollout) => Ok(Response::new(oasis_core::proto::GetRolloutResponse {
                rollout: Some(to_proto_rollout(&rollout)),
            })),
            None => Err(Status::not_found("Rollout not found")),
        }
    }

    pub async fn list_rollouts(
        srv: &OasisServer,
        request: Request<oasis_core::proto::ListRolloutsRequest>,
    ) -> Result<Response<oasis_core::proto::ListRolloutsResponse>, Status> {
        let _req = request.into_inner();
        let rollouts = srv
            .rollout_deploy_use_case()
            .list_rollouts()
            .await
            .map_err(map_core_error)?;
        let rollouts_proto: Vec<oasis_core::proto::RolloutMsg> =
            rollouts.iter().map(to_proto_rollout).collect();
        Ok(Response::new(oasis_core::proto::ListRolloutsResponse {
            rollouts: rollouts_proto,
        }))
    }
}
