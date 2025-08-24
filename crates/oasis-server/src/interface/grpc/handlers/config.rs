use tonic::{Request, Response, Status};

use crate::interface::grpc::errors::map_core_error;
use crate::interface::grpc::server::OasisServer;

pub struct ConfigHandlers;

impl ConfigHandlers {
    pub async fn apply_agent_config(
        srv: &OasisServer,
        request: Request<oasis_core::proto::ApplyAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::ApplyAgentConfigResponse>, Status> {
        let req = request.into_inner();
        let applied = srv
            .manage_agent_config_use_case()
            .apply_toml_config(&req.selector, &req.config_data)
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(oasis_core::proto::ApplyAgentConfigResponse {
            applied_agents: applied,
        }))
    }

    pub async fn get_agent_config(
        srv: &OasisServer,
        request: Request<oasis_core::proto::GetAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::GetAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.is_none()
            || req.agent_id.as_ref().unwrap().value.trim().is_empty()
            || req.key.trim().is_empty()
        {
            return Err(Status::invalid_argument("Agent ID and key cannot be empty"));
        }
        let agent_id = req.agent_id.as_ref().unwrap().value.as_str();
        let value = srv
            .manage_agent_config_use_case()
            .get(agent_id, &req.key)
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(oasis_core::proto::GetAgentConfigResponse {
            found: value.is_some(),
            value: value.unwrap_or_default(),
        }))
    }

    pub async fn set_agent_config(
        srv: &OasisServer,
        request: Request<oasis_core::proto::SetAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::SetAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.is_none()
            || req.agent_id.as_ref().unwrap().value.trim().is_empty()
            || req.key.trim().is_empty()
        {
            return Err(Status::invalid_argument("Agent ID and key cannot be empty"));
        }
        let agent_id = req.agent_id.as_ref().unwrap().value.as_str();
        srv.manage_agent_config_use_case()
            .set(agent_id, &req.key, &req.value)
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(oasis_core::proto::SetAgentConfigResponse {}))
    }

    pub async fn del_agent_config(
        srv: &OasisServer,
        request: Request<oasis_core::proto::DelAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::DelAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.is_none()
            || req.agent_id.as_ref().unwrap().value.trim().is_empty()
            || req.key.trim().is_empty()
        {
            return Err(Status::invalid_argument("Agent ID and key cannot be empty"));
        }
        let agent_id = req.agent_id.as_ref().unwrap().value.as_str();
        srv.manage_agent_config_use_case()
            .del(agent_id, &req.key)
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(oasis_core::proto::DelAgentConfigResponse {}))
    }

    pub async fn list_agent_config(
        srv: &OasisServer,
        request: Request<oasis_core::proto::ListAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::ListAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.is_none() || req.agent_id.as_ref().unwrap().value.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }
        let agent_id = req.agent_id.as_ref().unwrap().value.as_str();
        let keys = srv
            .manage_agent_config_use_case()
            .list_keys(
                agent_id,
                if req.prefix.is_empty() {
                    None
                } else {
                    Some(&req.prefix)
                },
            )
            .await
            .map_err(map_core_error)?;
        let total_count = keys.len() as u64;
        Ok(Response::new(oasis_core::proto::ListAgentConfigResponse {
            keys,
            total_count,
        }))
    }

    pub async fn show_agent_config(
        srv: &OasisServer,
        request: Request<oasis_core::proto::ShowAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::ShowAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.is_none() || req.agent_id.as_ref().unwrap().value.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }
        let agent_id = req.agent_id.as_ref().unwrap().value.as_str();
        let summary = srv
            .manage_agent_config_use_case()
            .get_summary(agent_id)
            .await
            .map_err(map_core_error)?;
        let total_keys = summary
            .get("total_keys")
            .unwrap_or(&"0".to_string())
            .parse()
            .unwrap_or(0);
        Ok(Response::new(oasis_core::proto::ShowAgentConfigResponse {
            summary,
            total_keys,
        }))
    }

    pub async fn backup_agent_config(
        srv: &OasisServer,
        request: Request<oasis_core::proto::BackupAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::BackupAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.is_none() || req.agent_id.as_ref().unwrap().value.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }
        let agent_id = req.agent_id.as_ref().unwrap().value.as_str();
        let (data, format) = srv
            .manage_agent_config_use_case()
            .backup_config(agent_id, &req.output_format)
            .await
            .map_err(map_core_error)?;
        // compute key_count similar to server
        let key_count = match format.as_str() {
            "toml" => {
                let toml_str = std::str::from_utf8(&data).unwrap_or("");
                let toml_value: toml::Value =
                    toml::from_str(toml_str).unwrap_or(toml::Value::Table(toml::Table::new()));
                let mut flat = std::collections::HashMap::new();
                crate::application::use_cases::queries::config_format_handler::ConfigSerializer::flatten_toml_value(
                    "".to_string(),
                    &toml_value,
                    &mut flat,
                );
                flat.len() as u64
            }
            "json" => {
                let json_str = std::str::from_utf8(&data).unwrap_or("{}");
                let config: std::collections::HashMap<String, String> =
                    serde_json::from_str(json_str).unwrap_or_default();
                config.len() as u64
            }
            "yaml" => {
                let yaml_str = std::str::from_utf8(&data).unwrap_or("");
                let config: std::collections::HashMap<String, String> =
                    serde_yaml::from_str(yaml_str).unwrap_or_default();
                config.len() as u64
            }
            _ => 0,
        };
        Ok(Response::new(
            oasis_core::proto::BackupAgentConfigResponse {
                config_data: data,
                format,
                key_count,
            },
        ))
    }

    pub async fn restore_agent_config(
        srv: &OasisServer,
        request: Request<oasis_core::proto::RestoreAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::RestoreAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.is_none() || req.agent_id.as_ref().unwrap().value.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }
        let agent_id = req.agent_id.as_ref().unwrap().value.as_str();
        let (restored, skipped) = srv
            .manage_agent_config_use_case()
            .restore_config(agent_id, &req.config_data, &req.format, req.overwrite)
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(
            oasis_core::proto::RestoreAgentConfigResponse {
                restored_keys: restored,
                skipped_keys: skipped,
            },
        ))
    }

    pub async fn validate_agent_config(
        srv: &OasisServer,
        request: Request<oasis_core::proto::ValidateAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::ValidateAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.is_none() || req.agent_id.as_ref().unwrap().value.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }
        let agent_id = req.agent_id.as_ref().unwrap().value.as_str();
        let (valid, errors, warnings) = srv
            .manage_agent_config_use_case()
            .validate_config(agent_id, &req.config_data, &req.format)
            .await
            .map_err(map_core_error)?;
        Ok(Response::new(
            oasis_core::proto::ValidateAgentConfigResponse {
                valid,
                errors,
                warnings,
            },
        ))
    }

    pub async fn diff_agent_config(
        srv: &OasisServer,
        request: Request<oasis_core::proto::DiffAgentConfigRequest>,
    ) -> Result<Response<oasis_core::proto::DiffAgentConfigResponse>, Status> {
        let req = request.into_inner();
        if req.agent_id.is_none() || req.agent_id.as_ref().unwrap().value.trim().is_empty() {
            return Err(Status::invalid_argument("Agent ID cannot be empty"));
        }
        let agent_id = req.agent_id.as_ref().unwrap().value.as_str();
        let _ = agent_id; // diff 不需要具体 agent_id
        let changes = srv
            .manage_agent_config_use_case()
            .diff_config(&req.from_config, &req.to_config, &req.format)
            .await
            .map_err(map_core_error)?;
        let mut added_count = 0;
        let mut modified_count = 0;
        let mut deleted_count = 0;
        let proto_changes: Vec<oasis_core::proto::ConfigDiffItem> = changes
            .into_iter()
            .map(|change| {
                match change.operation.as_str() {
                    "added" => added_count += 1,
                    "modified" => modified_count += 1,
                    "deleted" => deleted_count += 1,
                    _ => {}
                }
                oasis_core::proto::ConfigDiffItem {
                    key: change.key,
                    operation: change.operation,
                    old_value: change.old_value.unwrap_or_default(),
                    new_value: change.new_value.unwrap_or_default(),
                }
            })
            .collect();
        Ok(Response::new(oasis_core::proto::DiffAgentConfigResponse {
            changes: proto_changes,
            added_count,
            modified_count,
            deleted_count,
        }))
    }
}
