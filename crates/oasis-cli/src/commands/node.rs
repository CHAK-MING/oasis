use anyhow::Result;
use oasis_core::proto::oasis_service_client::OasisServiceClient;
use oasis_core::selector::CelSelector;

use serde_json::Value;
use std::collections::HashMap;

/// Node management commands for cluster administration
///
/// These commands allow you to view and manage cluster nodes (agents).
/// You can list nodes, view their facts, and filter them using CEL selectors.
#[derive(clap::Subcommand, Debug)]
pub enum NodeCommands {
    /// List cluster nodes with optional filtering
    ///
    /// Displays a list of all nodes in the cluster, including their status,
    /// basic facts, and labels. Can be filtered using CEL selectors.
    Ls {
        /// CEL selector expression to filter nodes
        ///
        /// Examples:
        ///   'labels["environment"] == "production"'
        ///   'labels["role"] == "web"'
        ///   'facts.os_name == "Ubuntu"'
        #[arg(
            long,
            value_name = "CEL_EXPRESSION",
            help = "CEL selector expression to filter nodes"
        )]
        selector: Option<String>,
        /// Output format for displaying results
        ///
        /// Available formats:
        ///   table - Human-readable table format (default)
        ///   json  - JSON format for scripting
        ///   yaml  - YAML format for configuration
        #[arg(
            long,
            value_name = "FORMAT",
            default_value = "table",
            help = "Output format: table|json|yaml"
        )]
        output: String,
        /// Show additional detailed columns in table output
        ///
        /// When enabled, displays extra information including:
        /// - CPU cores and memory
        /// - Operating system details
        /// - Network interfaces
        #[arg(long, help = "Show verbose columns (only for table format)")]
        verbose: bool,
    },
    /// Display detailed system facts for selected nodes
    ///
    /// Shows comprehensive system information for nodes that match the CEL selector.
    /// Facts include hardware details, OS information, network configuration, and more.
    Facts {
        /// CEL selector expression to target specific nodes
        ///
        /// Examples:
        ///   'agent_id == "agent-1"'
        ///   'labels["environment"] == "production"'
        ///   'facts.cpu_count >= 8'
        #[arg(
            long,
            value_name = "CEL_EXPRESSION",
            help = "CEL selector expression to target specific nodes"
        )]
        selector: String,
        /// Output format for displaying facts
        ///
        /// Available formats:
        ///   table - Human-readable table format (default)
        ///   json  - JSON format for scripting
        ///   yaml  - YAML format for configuration
        #[arg(
            long,
            value_name = "FORMAT",
            default_value = "table",
            help = "Output format: table|json|yaml"
        )]
        output: String,
    },
}

#[derive(Debug, serde::Serialize)]
pub struct NodeInfoRow {
    pub agent_id: String,
    pub facts: Value,
    pub labels: HashMap<String, String>,
    pub is_online: bool,
}

pub async fn run_node(
    mut client: OasisServiceClient<tonic::transport::Channel>,
    cmd: NodeCommands,
) -> Result<()> {
    match cmd {
        NodeCommands::Ls {
            selector,
            output,
            verbose,
        } => run_node_list(&mut client, selector, output, verbose).await,
        NodeCommands::Facts { selector, output } => {
            run_node_facts(&mut client, selector, output).await
        }
    }
}

async fn run_node_list(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    selector: Option<String>,
    output: String,
    verbose: bool,
) -> Result<()> {
    let mut rows: Vec<NodeInfoRow> = Vec::new();

    if let Some(selector_expr) = selector {
        // 本地校验选择器语法
        let _ = CelSelector::new(selector_expr.clone())
            .map_err(|e| anyhow::anyhow!("Invalid selector: {}", e))?;

        let request = tonic::Request::new(oasis_core::proto::ResolveSelectorRequest {
            selector_expression: selector_expr.clone(),
        });
        let result = client.resolve_selector(request).await?.into_inner();
        if !result.error_message.is_empty() {
            anyhow::bail!("Selector resolution failed: {}", result.error_message);
        }
        for agent_id in result.agent_ids {
            rows.push(fetch_node_row(client, &agent_id).await?);
        }
    } else {
        let request = tonic::Request::new(oasis_core::proto::ListNodesRequest {
            verbose,
            selector: String::new(),
        });
        let nodes = client.list_nodes(request).await?.into_inner();
        for node in nodes.nodes {
            let facts: Value = serde_json::from_str(&node.facts_json)?;
            let mut row = NodeInfoRow {
                agent_id: node.agent_id.clone(),
                facts,
                labels: node.labels,
                is_online: node.is_online,
            };
            // 回退策略：若 facts 缺失/不完整，直接再次拉取，避免展示不一致
            let hostname_missing = row
                .facts
                .get("hostname")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                == "unknown";
            let ip_missing = row
                .facts
                .get("primary_ip")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                == "unknown";
            if hostname_missing || ip_missing {
                row = fetch_node_row(client, &node.agent_id).await?;
            }
            rows.push(row);
        }
    }

    rows.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));

    match output.as_str() {
        "json" => println!("{}", serde_json::to_string_pretty(&rows)?),
        "yaml" => println!("{}", serde_yaml::to_string(&rows)?),
        _ => {
            print_nodes_table(&rows, verbose);
            print_nodes_summary(&rows);
        }
    }
    Ok(())
}

async fn run_node_facts(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    selector: String,
    output: String,
) -> Result<()> {
    let mut rows: Vec<NodeInfoRow> = Vec::new();

    // Validate selector syntax locally
    let _ = CelSelector::new(selector.clone())
        .map_err(|e| anyhow::anyhow!("Invalid selector: {}", e))?;

    let req = tonic::Request::new(oasis_core::proto::ResolveSelectorRequest {
        selector_expression: selector,
    });
    let result = client.resolve_selector(req).await?.into_inner();
    if !result.error_message.is_empty() {
        anyhow::bail!("Selector resolution failed: {}", result.error_message);
    }
    for id in result.agent_ids {
        rows.push(fetch_node_row(client, &id).await?);
    }

    match output.as_str() {
        "json" => println!("{}", serde_json::to_string_pretty(&rows)?),
        "yaml" => println!("{}", serde_yaml::to_string(&rows)?),
        _ => print_nodes_table(&rows, true),
    }
    Ok(())
}

async fn fetch_node_row(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
    agent_id: &str,
) -> Result<NodeInfoRow> {
    let facts_req = tonic::Request::new(oasis_core::proto::GetNodeFactsRequest {
        agent_id: agent_id.to_string(),
    });
    let facts_json = client
        .get_node_facts(facts_req)
        .await?
        .into_inner()
        .facts_json;
    let facts: Value = serde_json::from_str(&facts_json)?;

    let labels_req = tonic::Request::new(oasis_core::proto::GetNodeLabelsRequest {
        agent_id: agent_id.to_string(),
    });
    let labels = client
        .get_node_labels(labels_req)
        .await?
        .into_inner()
        .labels;

    let status_req = tonic::Request::new(oasis_core::proto::CheckAgentsRequest {
        agent_ids: vec![agent_id.to_string()],
    });
    let is_online = client
        .check_agents(status_req)
        .await?
        .into_inner()
        .statuses
        .first()
        .map(|s| s.online)
        .unwrap_or(false);

    Ok(NodeInfoRow {
        agent_id: agent_id.to_string(),
        facts,
        labels,
        is_online,
    })
}

fn print_nodes_table(nodes: &[NodeInfoRow], verbose: bool) {
    if nodes.is_empty() {
        println!("No nodes found");
        return;
    }

    // 动态计算 agent_id 列宽，以兼容较长 ID
    let max_id_len = nodes.iter().map(|n| n.agent_id.len()).max().unwrap_or(20);
    let id_width = max_id_len.clamp(24, 64);

    if verbose {
        println!(
            "{:<idw$} {:<15} {:<10} {:<15} {:<10} {:<20} {}",
            "AGENT_ID",
            "HOSTNAME",
            "STATUS",
            "PRIMARY_IP",
            "CPU_CORES",
            "OS",
            "LABELS",
            idw = id_width
        );
        println!("{}", "-".repeat(id_width + 85));
        for n in nodes {
            let hostname = n.facts["hostname"].as_str().unwrap_or("unknown");
            let status = if n.is_online { "online" } else { "offline" };
            let primary_ip = n.facts["primary_ip"].as_str().unwrap_or("unknown");
            let cpu = n.facts["cpu_cores"].as_u64().unwrap_or(0);
            let os = n.facts["os_name"].as_str().unwrap_or("unknown");
            let labels = if n.labels.is_empty() {
                "-".to_string()
            } else {
                n.labels
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(",")
            };
            println!(
                "{:<idw$} {:<15} {:<10} {:<15} {:<10} {:<20} {}",
                n.agent_id,
                hostname,
                status,
                primary_ip,
                cpu,
                os,
                labels,
                idw = id_width
            );
        }
    } else {
        // 非 verbose 视图也包含 CPU/OS 以提升可读性
        println!(
            "{:<idw$} {:<15} {:<10} {:<15} {:<10} {:<20}",
            "AGENT_ID",
            "HOSTNAME",
            "STATUS",
            "PRIMARY_IP",
            "CPU",
            "OS",
            idw = id_width
        );
        println!("{}", "-".repeat(id_width + 15 + 10 + 15 + 10 + 20 + 1));
        for n in nodes {
            let hostname = n.facts["hostname"].as_str().unwrap_or("unknown");
            let status = if n.is_online { "online" } else { "offline" };
            let primary_ip = n.facts["primary_ip"].as_str().unwrap_or("unknown");
            let cpu = n.facts["cpu_cores"].as_u64().unwrap_or(0);
            let os = n.facts["os_name"].as_str().unwrap_or("unknown");
            println!(
                "{:<idw$} {:<15} {:<10} {:<15} {:<10} {:<20}",
                n.agent_id,
                hostname,
                status,
                primary_ip,
                cpu,
                os,
                idw = id_width
            );
        }
    }
}

fn print_nodes_summary(nodes: &[NodeInfoRow]) {
    let total = nodes.len();
    let online = nodes.iter().filter(|n| n.is_online).count();
    let offline = total - online;
    println!();
    println!(
        "Summary: {} total, {} online, {} offline",
        total, online, offline
    );
}
