use crate::common::target::TargetSelector;
use anyhow::Result;
use clap::Subcommand;
use oasis_core::proto::AgentId;
use tracing::info;

/// Configuration management commands for distributed agent configuration
#[derive(Subcommand, Debug)]
pub enum ConfigCommands {
    /// Apply configuration file to target agents
    ///
    /// This command distributes configuration files to agents that match the specified CEL selector.
    /// The configuration is validated before application and can be applied atomically across multiple agents.
    Apply {
        /// Path to the configuration file (TOML format)
        #[arg(long, value_name = "PATH")]
        src: String,
        /// Target specification (CEL selector or comma-separated agent IDs)
        #[arg(long = "target", short = 't', value_name = "<TARGET>")]
        target: String,
        /// Skip configuration validation before applying
        #[arg(long)]
        no_validate: bool,
    },
    /// Display configuration summary and statistics for agents
    ///
    /// Shows a high-level overview of configuration keys, categories, and values
    /// for the specified component or agent.
    Show {
        /// Target component type (agent, server, cli)
        #[arg(short, long, value_name = "COMPONENT")]
        component: Option<String>,
        /// Specific agent ID (required for agent config)
        #[arg(short, long, value_name = "AGENT_ID")]
        agent_id: Option<String>,
    },
    /// Set a specific configuration value for target agents
    ///
    /// Updates individual configuration keys using dot notation (e.g., "security.enforce_whitelist").
    /// Changes are applied immediately and can trigger hot-reload if supported.
    Set {
        /// Configuration key path using dot notation
        ///
        /// Examples:
        ///   security.enforce_whitelist
        ///   telemetry.log_level
        ///   executor.command_timeout_sec
        #[arg(value_name = "KEY_PATH")]
        key: String,
        /// Configuration value to set
        #[arg(value_name = "VALUE")]
        value: String,
        /// Target component type (agent, server, cli)
        #[arg(short, long, default_value = "agent", value_name = "COMPONENT")]
        component: String,
        /// Specific agent ID (required for agent config)
        #[arg(short, long, value_name = "AGENT_ID")]
        agent_id: Option<String>,
        /// Skip validation before setting the value
        #[arg(long)]
        no_validate: bool,
    },
    /// Retrieve a specific configuration value from agents
    ///
    /// Fetches and displays the current value of a configuration key
    /// for the specified component or agent.
    Get {
        /// Configuration key path using dot notation
        #[arg(value_name = "KEY_PATH")]
        key: String,
        /// Target component type (agent, server, cli)
        #[arg(short, long, default_value = "agent", value_name = "COMPONENT")]
        component: String,
        /// Specific agent ID (required for agent config)
        #[arg(short, long, value_name = "AGENT_ID")]
        agent_id: Option<String>,
    },
    /// Remove a configuration key from agents
    ///
    /// Deletes a specific configuration key, reverting it to its default value
    /// or removing it entirely if no default exists.
    Delete {
        /// Configuration key path to delete
        #[arg(value_name = "KEY_PATH")]
        key: String,
        /// Target component type (agent, server, cli)
        #[arg(short, long, default_value = "agent", value_name = "COMPONENT")]
        component: String,
        /// Specific agent ID (required for agent config)
        #[arg(short, long, value_name = "AGENT_ID")]
        agent_id: Option<String>,
    },
    /// List all configuration keys for a component or agent
    ///
    /// Displays all available configuration keys, optionally filtered by prefix.
    /// Useful for discovering available configuration options.
    List {
        /// Target component type (agent, server, cli)
        #[arg(short, long, default_value = "agent", value_name = "COMPONENT")]
        component: String,
        /// Specific agent ID (required for agent config)
        #[arg(short, long, value_name = "AGENT_ID")]
        agent_id: Option<String>,
        /// Filter keys by prefix (e.g., "security." for all security-related keys)
        #[arg(short, long, value_name = "PREFIX")]
        prefix: Option<String>,
    },
    /// Validate configuration file syntax and business rules
    ///
    /// Performs comprehensive validation of configuration files including:
    /// - TOML syntax validation
    /// - Required field validation
    /// - Business rule validation
    /// - Cross-field dependency checks
    Validate {
        /// Path to configuration file to validate
        #[arg(short, long, value_name = "FILE_PATH")]
        file: Option<String>,
        /// Target component type for validation rules
        #[arg(short, long, default_value = "agent", value_name = "COMPONENT")]
        component: String,
    },
    /// Compare configuration differences between two sources
    ///
    /// Shows detailed differences between configuration files or current agent configs.
    /// Useful for understanding what changes will be applied.
    Diff {
        /// First configuration source (file path or agent ID)
        #[arg(value_name = "FROM")]
        from: String,
        /// Second configuration source (file path or agent ID)
        #[arg(value_name = "TO")]
        to: String,
        /// Target component type for comparison
        #[arg(short, long, default_value = "agent", value_name = "COMPONENT")]
        component: String,
    },
    /// Create a backup of current configuration
    ///
    /// Exports the current configuration to a file for backup or migration purposes.
    /// Supports multiple output formats (TOML, JSON, YAML).
    Backup {
        /// Output file path for the backup
        #[arg(short, long, value_name = "OUTPUT_PATH")]
        output: String,
        /// Target component type to backup
        #[arg(short, long, default_value = "agent", value_name = "COMPONENT")]
        component: String,
        /// Specific agent ID (required for agent config)
        #[arg(short, long, value_name = "AGENT_ID")]
        agent_id: Option<String>,
    },
    /// Restore configuration from a backup file
    ///
    /// Imports configuration from a backup file, replacing or merging with existing config.
    /// Supports validation before restoration to ensure data integrity.
    Restore {
        /// Input backup file path
        #[arg(short, long, value_name = "INPUT_PATH")]
        input: String,
        /// Target component type to restore
        #[arg(short, long, default_value = "agent", value_name = "COMPONENT")]
        component: String,
        /// Specific agent ID (required for agent config)
        #[arg(short, long, value_name = "AGENT_ID")]
        agent_id: Option<String>,
        /// Skip validation before restoration
        #[arg(long)]
        no_validate: bool,
    },
}

/// Configuration command arguments (compatibility with existing CLI structure)
pub type ConfigArgs = ConfigCommands;

/// Run configuration command through gRPC
pub async fn run_config(
    client: oasis_core::proto::oasis_service_client::OasisServiceClient<tonic::transport::Channel>,
    args: ConfigArgs,
) -> Result<()> {
    handle_config_command_via_grpc(client, args).await
}

async fn handle_config_command_via_grpc(
    mut client: oasis_core::proto::oasis_service_client::OasisServiceClient<
        tonic::transport::Channel,
    >,
    cmd: ConfigCommands,
) -> Result<()> {
    match cmd {
        ConfigCommands::Apply {
            src,
            target,
            no_validate,
        } => handle_apply_command(&mut client, src, target, no_validate).await,

        ConfigCommands::Set {
            key,
            value,
            component,
            agent_id,
            ..
        } => handle_set_command(&mut client, key, value, component, agent_id).await,

        ConfigCommands::Get {
            key,
            component,
            agent_id,
        } => handle_get_command(&mut client, key, component, agent_id).await,

        ConfigCommands::Delete {
            key,
            component,
            agent_id,
        } => handle_delete_command(&mut client, key, component, agent_id).await,

        ConfigCommands::List {
            component,
            agent_id,
            prefix,
        } => handle_list_command(&mut client, component, agent_id, prefix).await,

        ConfigCommands::Show {
            component,
            agent_id,
        } => handle_show_command(&mut client, component, agent_id).await,

        ConfigCommands::Backup {
            output,
            component,
            agent_id,
        } => handle_backup_command(&mut client, output, component, agent_id).await,

        ConfigCommands::Restore {
            input,
            component,
            agent_id,
            ..
        } => handle_restore_command(&mut client, input, component, agent_id).await,

        ConfigCommands::Validate { file, component } => {
            handle_validate_command(&mut client, file, component).await
        }

        ConfigCommands::Diff {
            from,
            to,
            component,
        } => handle_diff_command(&mut client, from, to, component).await,
    }
}

async fn handle_apply_command(
    client: &mut oasis_core::proto::oasis_service_client::OasisServiceClient<
        tonic::transport::Channel,
    >,
    src: String,
    target: String,
    no_validate: bool,
) -> Result<()> {
    // 读取配置文件
    let config_content = tokio::fs::read_to_string(&src)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read config file {}: {}", src, e))?;

    if !no_validate {
        // 验证 TOML 格式
        let _: toml::Value = toml::from_str(&config_content)
            .map_err(|e| anyhow::anyhow!("Invalid TOML format: {}", e))?;
        info!("Configuration file validation passed");
    }

    let request = oasis_core::proto::ApplyAgentConfigRequest {
        selector: TargetSelector::parse(&target).expression().to_string(),
        config_data: config_content.into_bytes(),
    };

    let response = client
        .apply_agent_config(request)
        .await
        .map_err(|e| anyhow::anyhow!("gRPC call failed: {}", e))?;

    println!(
        "Configuration applied to {} agents",
        response.into_inner().applied_agents
    );
    Ok(())
}

async fn handle_set_command(
    client: &mut oasis_core::proto::oasis_service_client::OasisServiceClient<
        tonic::transport::Channel,
    >,
    key: String,
    value: String,
    component: String,
    agent_id: Option<String>,
) -> Result<()> {
    if component != "agent" {
        return Err(anyhow::anyhow!("Only agent config is supported via gRPC"));
    }

    let agent_id = agent_id.ok_or_else(|| anyhow::anyhow!("Agent ID required for agent config"))?;

    let request = oasis_core::proto::SetAgentConfigRequest {
        agent_id: Some(AgentId { value: agent_id }),
        key,
        value,
    };

    client
        .set_agent_config(request)
        .await
        .map_err(|e| anyhow::anyhow!("gRPC call failed: {}", e))?;

    println!("Successfully set config for agent");
    Ok(())
}

async fn handle_get_command(
    client: &mut oasis_core::proto::oasis_service_client::OasisServiceClient<
        tonic::transport::Channel,
    >,
    key: String,
    component: String,
    agent_id: Option<String>,
) -> Result<()> {
    if component != "agent" {
        return Err(anyhow::anyhow!("Only agent config is supported via gRPC"));
    }

    let agent_id = agent_id.ok_or_else(|| anyhow::anyhow!("Agent ID required for agent config"))?;

    let request = oasis_core::proto::GetAgentConfigRequest {
        agent_id: Some(AgentId { value: agent_id }),
        key,
    };

    let response = client
        .get_agent_config(request)
        .await
        .map_err(|e| anyhow::anyhow!("gRPC call failed: {}", e))?;

    let response = response.into_inner();
    if response.found {
        println!("{}", response.value);
    } else {
        println!("<not set>");
    }
    Ok(())
}

async fn handle_delete_command(
    client: &mut oasis_core::proto::oasis_service_client::OasisServiceClient<
        tonic::transport::Channel,
    >,
    key: String,
    component: String,
    agent_id: Option<String>,
) -> Result<()> {
    if component != "agent" {
        return Err(anyhow::anyhow!("Only agent config is supported via gRPC"));
    }

    let agent_id = agent_id.ok_or_else(|| anyhow::anyhow!("Agent ID required for agent config"))?;

    let request = oasis_core::proto::DelAgentConfigRequest {
        agent_id: Some(AgentId { value: agent_id }),
        key,
    };

    client
        .del_agent_config(request)
        .await
        .map_err(|e| anyhow::anyhow!("gRPC call failed: {}", e))?;

    println!("Successfully deleted config for agent");
    Ok(())
}

async fn handle_list_command(
    client: &mut oasis_core::proto::oasis_service_client::OasisServiceClient<
        tonic::transport::Channel,
    >,
    component: String,
    agent_id: Option<String>,
    prefix: Option<String>,
) -> Result<()> {
    if component != "agent" {
        return Err(anyhow::anyhow!("Only agent config is supported via gRPC"));
    }

    let agent_id = agent_id.ok_or_else(|| anyhow::anyhow!("Agent ID required for agent config"))?;

    let request = oasis_core::proto::ListAgentConfigRequest {
        agent_id: Some(AgentId { value: agent_id }),
        prefix: prefix.unwrap_or_default(),
    };

    let response = client
        .list_agent_config(request)
        .await
        .map_err(|e| anyhow::anyhow!("gRPC call failed: {}", e))?;

    let response = response.into_inner();
    println!("Found {} configuration keys:", response.total_count);
    for key in response.keys {
        println!("  {}", key);
    }
    Ok(())
}

async fn handle_show_command(
    client: &mut oasis_core::proto::oasis_service_client::OasisServiceClient<
        tonic::transport::Channel,
    >,
    component: Option<String>,
    agent_id: Option<String>,
) -> Result<()> {
    if component.as_deref() != Some("agent") {
        return Err(anyhow::anyhow!("Only agent config is supported via gRPC"));
    }

    let agent_id = agent_id.ok_or_else(|| anyhow::anyhow!("Agent ID required for agent config"))?;

    let request = oasis_core::proto::ShowAgentConfigRequest {
        agent_id: Some(AgentId { value: agent_id }),
    };

    let response = client
        .show_agent_config(request)
        .await
        .map_err(|e| anyhow::anyhow!("gRPC call failed: {}", e))?;

    let response = response.into_inner();
    println!(
        "Configuration summary for agent (total keys: {}):",
        response.total_keys
    );
    for (key, value) in response.summary {
        println!("  {}: {}", key, value);
    }
    Ok(())
}

async fn handle_backup_command(
    client: &mut oasis_core::proto::oasis_service_client::OasisServiceClient<
        tonic::transport::Channel,
    >,
    output: String,
    component: String,
    agent_id: Option<String>,
) -> Result<()> {
    if component != "agent" {
        return Err(anyhow::anyhow!("Only agent config is supported via gRPC"));
    }

    let agent_id = agent_id.ok_or_else(|| anyhow::anyhow!("Agent ID required for agent config"))?;

    let request = oasis_core::proto::BackupAgentConfigRequest {
        agent_id: Some(AgentId { value: agent_id }),
        output_format: "toml".to_string(), // 默认使用 TOML 格式
    };

    let response = client
        .backup_agent_config(request)
        .await
        .map_err(|e| anyhow::anyhow!("gRPC call failed: {}", e))?;

    let response = response.into_inner();

    // 写入文件
    tokio::fs::write(&output, response.config_data)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to write backup file: {}", e))?;

    println!(
        "Backup completed: {} keys saved to {} ({})",
        response.key_count, output, response.format
    );
    Ok(())
}

async fn handle_restore_command(
    client: &mut oasis_core::proto::oasis_service_client::OasisServiceClient<
        tonic::transport::Channel,
    >,
    input: String,
    component: String,
    agent_id: Option<String>,
) -> Result<()> {
    if component != "agent" {
        return Err(anyhow::anyhow!("Only agent config is supported via gRPC"));
    }

    let agent_id = agent_id.ok_or_else(|| anyhow::anyhow!("Agent ID required for agent config"))?;

    // 读取备份文件
    let config_data = tokio::fs::read(&input)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read backup file: {}", e))?;

    let request = oasis_core::proto::RestoreAgentConfigRequest {
        agent_id: Some(AgentId { value: agent_id }),
        config_data,
        format: "toml".to_string(), // 默认使用 TOML 格式
        overwrite: true,            // 默认覆盖现有配置
    };

    let response = client
        .restore_agent_config(request)
        .await
        .map_err(|e| anyhow::anyhow!("gRPC call failed: {}", e))?;

    let response = response.into_inner();
    println!(
        "Restore completed: {} keys restored, {} keys skipped",
        response.restored_keys, response.skipped_keys
    );
    Ok(())
}

async fn handle_validate_command(
    client: &mut oasis_core::proto::oasis_service_client::OasisServiceClient<
        tonic::transport::Channel,
    >,
    file: Option<String>,
    component: String,
) -> Result<()> {
    if component != "agent" {
        return Err(anyhow::anyhow!("Only agent config is supported via gRPC"));
    }

    // 读取配置文件
    let config_data = if let Some(file_path) = file {
        tokio::fs::read(&file_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read config file: {}", e))?
    } else {
        return Err(anyhow::anyhow!("Config file path is required"));
    };

    let request = oasis_core::proto::ValidateAgentConfigRequest {
        agent_id: Some(AgentId {
            value: "dummy".to_string(),
        }), // 验证时不关心具体的 agent_id
        config_data,
        format: "toml".to_string(), // 默认使用 TOML 格式
    };

    let response = client
        .validate_agent_config(request)
        .await
        .map_err(|e| anyhow::anyhow!("gRPC call failed: {}", e))?;

    let response = response.into_inner();
    if response.valid {
        println!("Configuration is valid");
    } else {
        println!("Configuration validation failed:");
        for error in response.errors {
            println!("  Error: {}", error);
        }
    }

    if !response.warnings.is_empty() {
        println!("Warnings:");
        for warning in response.warnings {
            println!("  Warning: {}", warning);
        }
    }
    Ok(())
}

async fn handle_diff_command(
    client: &mut oasis_core::proto::oasis_service_client::OasisServiceClient<
        tonic::transport::Channel,
    >,
    from: String,
    to: String,
    component: String,
) -> Result<()> {
    if component != "agent" {
        return Err(anyhow::anyhow!("Only agent config is supported via gRPC"));
    }

    // 读取两个配置文件
    let from_data = tokio::fs::read(&from)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read from file: {}", e))?;
    let to_data = tokio::fs::read(&to)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read to file: {}", e))?;

    let request = oasis_core::proto::DiffAgentConfigRequest {
        agent_id: Some(AgentId {
            value: "dummy".to_string(),
        }), // diff 时不关心具体的 agent_id
        from_config: from_data,
        to_config: to_data,
        format: "toml".to_string(), // 默认使用 TOML 格式
    };

    let response = client
        .diff_agent_config(request)
        .await
        .map_err(|e| anyhow::anyhow!("gRPC call failed: {}", e))?;

    let response = response.into_inner();
    println!(
        "Configuration diff: {} added, {} modified, {} deleted",
        response.added_count, response.modified_count, response.deleted_count
    );

    for change in response.changes {
        match change.operation.as_str() {
            "added" => println!("  + {} = {}", change.key, change.new_value),
            "modified" => println!(
                "  ~ {}: {} -> {}",
                change.key, change.old_value, change.new_value
            ),
            "deleted" => println!("  - {} = {}", change.key, change.old_value),
            _ => println!("  ? {}: {}", change.key, change.operation),
        }
    }
    Ok(())
}
