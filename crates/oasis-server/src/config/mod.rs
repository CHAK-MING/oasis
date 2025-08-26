//! Server configuration module

use oasis_core::OasisConfig;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Server-specific configuration that extends the base OasisConfig
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(flatten)]
    pub base: OasisConfig,

    /// Server-specific settings
    #[serde(default)]
    pub server: ServerSettings,
}

/// Server-specific settings
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServerSettings {
    /// Heartbeat TTL in seconds (should be 2x agent heartbeat interval)
    #[serde(default = "default_heartbeat_ttl")]
    pub heartbeat_ttl_sec: u64,

    /// Leader election configuration
    #[serde(default)]
    pub leader_election: Option<LeaderElectionConfig>,
}

/// Leader election configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderElectionConfig {
    /// Lease TTL in seconds
    #[serde(default = "default_lease_ttl")]
    pub lease_ttl_sec: u64,
    /// Renewal interval in seconds
    #[serde(default = "default_renewal_interval")]
    pub renewal_interval_sec: u64,
}

// Default values
fn default_heartbeat_ttl() -> u64 {
    90
}

fn default_lease_ttl() -> u64 {
    30
}

fn default_renewal_interval() -> u64 {
    10
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            base: OasisConfig::default(),
            server: ServerSettings::default(),
        }
    }
}

impl Default for LeaderElectionConfig {
    fn default() -> Self {
        Self {
            lease_ttl_sec: default_lease_ttl(),
            renewal_interval_sec: default_renewal_interval(),
        }
    }
}
