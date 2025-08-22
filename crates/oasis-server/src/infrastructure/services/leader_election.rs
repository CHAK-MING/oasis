use crate::config::{LeaderElectionConfig, ServerConfig};
use async_nats::jetstream::Context;
use async_nats::jetstream::kv::Store;
use oasis_core::error::CoreError;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Leader election service using a NATS KV lease model.
pub struct LeaderElectionService {
    kv_store: Arc<Store>,
    instance_id: String,
    lease_ttl: Duration,
    renewal_interval: Duration,
    is_leader: Arc<RwLock<bool>>,
    leader_info: Arc<RwLock<Option<LeaderInfo>>>,
    /// Background task handles
    background_handles: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
    /// Cancellation token for graceful shutdown
    shutdown_token: CancellationToken,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderInfo {
    pub instance_id: String,
    pub elected_at: i64,   // Use i64 for serialization
    pub last_renewal: i64, // Use i64 for serialization
}

impl LeaderElectionService {
    /// Create a new leader election service
    pub async fn new(
        jetstream: Context,
        config: &ServerConfig,
        shutdown_token: CancellationToken,
    ) -> Result<Self, CoreError> {
        let instance_id = Uuid::new_v4().to_string();

        // Read election parameters from config
        let default_config = LeaderElectionConfig::default();
        let leader_config = config
            .server
            .leader_election
            .as_ref()
            .unwrap_or(&default_config);
        let lease_ttl = Duration::from_secs(leader_config.lease_ttl_sec);
        let renewal_interval = Duration::from_secs(leader_config.renewal_interval_sec);

        // Create or get the leader election KV store (作为备选方案)
        let kv_store = jetstream
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "leader-election".to_string(),
                max_age: lease_ttl,
                max_bytes: 1024, // Small bucket for leader info
                storage: async_nats::jetstream::stream::StorageType::File,
                num_replicas: 1, // Use 1 replica for single-node NATS
                ..Default::default()
            })
            .await
            .map_err(|e| CoreError::Nats {
                message: format!("Failed to create leader election KV store: {}", e),
            })?;

        let service = Self {
            kv_store: Arc::new(kv_store),
            instance_id: instance_id.clone(),
            lease_ttl,
            renewal_interval,
            is_leader: Arc::new(RwLock::new(false)),
            leader_info: Arc::new(RwLock::new(None)),
            background_handles: Arc::new(RwLock::new(Vec::new())),
            shutdown_token,
        };

        tracing::info!(
            instance_id = %instance_id,
            lease_ttl_secs = %lease_ttl.as_secs(),
            renewal_interval_secs = %renewal_interval.as_secs(),
            strategy = "kv_lease",
            "Leader election service initialized"
        );

        Ok(service)
    }

    /// Start the leader election process.
    pub async fn start_election(&self) -> Result<(), CoreError> {
        tracing::info!(
            instance_id = %self.instance_id,
            "Starting leader election using KV lease strategy"
        );

        // Try to become leader
        if self.try_become_leader().await? {
            tracing::info!(instance_id = %self.instance_id, "Became leader");

            // Start lease renewal
            self.start_lease_renewal().await?;
        } else {
            tracing::info!(instance_id = %self.instance_id, "Not elected as leader, monitoring for changes");

            // Monitor for leader changes
            self.start_leader_monitoring().await?;
        }

        Ok(())
    }

    /// Try to become the leader using atomic operations to prevent race conditions
    async fn try_become_leader(&self) -> Result<bool, CoreError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let new_leader_data = serde_json::to_string(&LeaderInfo {
            instance_id: self.instance_id.clone(),
            elected_at: now,
            last_renewal: now,
        })
        .map_err(|e| CoreError::Serialization {
            message: format!("Failed to serialize leader info: {}", e),
        })?;

        // Use atomic election strategy with retry logic
        let max_retries = 5;
        let mut retry_count = 0;

        while retry_count < max_retries {
            // Attempt atomic create first
            if self
                .kv_store
                .create("leader", new_leader_data.clone().into())
                .await
                .is_ok()
            {
                // Successfully became initial leader
                {
                    let mut is_leader_guard = self.is_leader.write().await;
                    *is_leader_guard = true;
                }
                {
                    let mut leader_info_guard = self.leader_info.write().await;
                    *leader_info_guard = Some(LeaderInfo {
                        instance_id: self.instance_id.clone(),
                        elected_at: now,
                        last_renewal: now,
                    });
                }
                tracing::info!(instance_id = %self.instance_id, "Became leader using KV create()");
                return Ok(true);
            }

            // If create failed, read current entry and consider CAS takeover
            match self.kv_store.entry("leader").await {
                Ok(Some(entry)) => {
                    let current_leader_info =
                        match serde_json::from_slice::<LeaderInfo>(&entry.value) {
                            Ok(info) => info,
                            Err(_) => {
                                retry_count += 1;
                                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                                continue;
                            }
                        };

                    let age = now - current_leader_info.last_renewal;
                    if age > self.lease_ttl.as_secs() as i64 {
                        if self
                            .kv_store
                            .update("leader", new_leader_data.clone().into(), entry.revision)
                            .await
                            .is_ok()
                        {
                            {
                                let mut is_leader_guard = self.is_leader.write().await;
                                *is_leader_guard = true;
                            }
                            {
                                let mut leader_info_guard = self.leader_info.write().await;
                                *leader_info_guard = Some(LeaderInfo {
                                    instance_id: self.instance_id.clone(),
                                    elected_at: now,
                                    last_renewal: now,
                                });
                            }
                            tracing::info!(instance_id = %self.instance_id, "Took over leadership using KV update() CAS");
                            return Ok(true);
                        }
                    }
                    return Ok(false);
                }
                _ => {
                    retry_count += 1;
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    continue;
                }
            }
        }

        Ok(false)
    }

    /// Start lease renewal for the current leader
    async fn start_lease_renewal(&self) -> Result<(), CoreError> {
        let kv_store = Arc::clone(&self.kv_store);
        let instance_id = self.instance_id.clone();
        let _lease_ttl = self.lease_ttl;
        let renewal_interval = self.renewal_interval;
        let is_leader = Arc::clone(&self.is_leader);
        let leader_info = Arc::clone(&self.leader_info);
        let shutdown_token = self.shutdown_token.child_token();

        let handle = tokio::spawn(async move {
            let mut interval = interval(renewal_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Check if we're still the leader
                        let is_still_leader = {
                            let is_leader_guard = is_leader.read().await;
                            *is_leader_guard
                        };

                        if !is_still_leader {
                            tracing::info!(instance_id = %instance_id, "No longer leader, stopping renewal");
                            break;
                        }

                        // Renew the lease with validation
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs() as i64;

                        // First, verify we're still the current leader
                        if let Ok(Some(current_entry)) = kv_store.get("leader").await {
                            if let Ok(current_leader_info) = serde_json::from_slice::<LeaderInfo>(&current_entry) {
                                if current_leader_info.instance_id != instance_id {
                                    tracing::warn!(
                                        instance_id = %instance_id,
                                        current_leader = %current_leader_info.instance_id,
                                        "Another node became leader, stopping renewal"
                                    );
                                    // Mark ourselves as not leader
                                    {
                                        let mut is_leader_guard = is_leader.write().await;
                                        *is_leader_guard = false;
                                    }
                                    break;
                                }
                            } else {
                                tracing::error!(instance_id = %instance_id, "Failed to deserialize current leader info during renewal");
                                break;
                            }
                        } else {
                            tracing::error!(instance_id = %instance_id, "Failed to get current leader info during renewal");
                            break;
                        }

                        // CAS renew: read current revision and ensure we are still the leader, then update
                        match kv_store.entry("leader").await {
                            Ok(Some(entry)) => {
                                if let Ok(current_leader_info) = serde_json::from_slice::<LeaderInfo>(&entry.value) {
                                    if current_leader_info.instance_id != instance_id {
                                        tracing::warn!(instance_id = %instance_id, "Lost leadership before renewal");
                                        let mut is_leader_guard = is_leader.write().await;
                                        *is_leader_guard = false;
                                        break;
                                    }

                                    let renewal_data = serde_json::to_string(&LeaderInfo {
                                        instance_id: instance_id.clone(),
                                        elected_at: current_leader_info.elected_at,
                                        last_renewal: now,
                                    })
                                    .unwrap();

                                    if kv_store
                                        .update("leader", renewal_data.into(), entry.revision)
                                        .await
                                        .is_ok()
                                    {
                                        let mut leader_info_guard = leader_info.write().await;
                                        if let Some(ref mut info) = *leader_info_guard {
                                            info.last_renewal = now;
                                        }
                                        tracing::debug!(instance_id = %instance_id, "Lease renewed via CAS");
                                    } else {
                                        tracing::warn!(instance_id = %instance_id, "CAS renew failed; stepping down");
                                        let mut is_leader_guard = is_leader.write().await;
                                        *is_leader_guard = false;
                                        break;
                                    }
                                } else {
                                    tracing::error!(instance_id = %instance_id, "Leader info decode failed during renewal");
                                    break;
                                }
                            }
                            _ => {
                                tracing::error!(instance_id = %instance_id, "Failed to load leader entry during renewal");
                                break;
                            }
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        tracing::info!(instance_id = %instance_id, "Shutdown signal received, stopping lease renewal");
                        break;
                    }
                }
            }
        });

        // 保存任务句柄
        {
            let mut handles = self.background_handles.write().await;
            handles.push(handle);
        }

        Ok(())
    }

    /// Start monitoring for leader changes
    async fn start_leader_monitoring(&self) -> Result<(), CoreError> {
        let kv_store = Arc::clone(&self.kv_store);
        let instance_id = self.instance_id.clone();
        let is_leader = Arc::clone(&self.is_leader);
        let leader_info = Arc::clone(&self.leader_info);
        let lease_ttl = self.lease_ttl;
        let shutdown_token = self.shutdown_token.child_token();

        let handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(5)); // Check every 5 seconds

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Check if we're already the leader
                        let is_currently_leader = {
                            let is_leader_guard = is_leader.read().await;
                            *is_leader_guard
                        };

                        if is_currently_leader {
                            continue;
                        }

                        // Check current leader status
                        if let Ok(Some(entry)) = kv_store.get("leader").await {
                            if let Ok(current_leader) = serde_json::from_slice::<LeaderInfo>(&entry) {
                                let now = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs() as i64;
                                let age = now - current_leader.last_renewal;

                                // If leader lease has expired, try to become leader
                                if age > lease_ttl.as_secs() as i64 {
                                    tracing::info!(
                                        instance_id = %instance_id,
                                        current_leader = %current_leader.instance_id,
                                        lease_age_secs = %age,
                                        "Leader lease expired, attempting to become leader"
                                    );

                                    // Try to become the new leader
                                    let now = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs() as i64;
                                    let new_leader_data = serde_json::to_string(&LeaderInfo {
                                        instance_id: instance_id.clone(),
                                        elected_at: now,
                                        last_renewal: now,
                                    });

                                    if let Ok(data) = new_leader_data {
                                        // Use create() first; if exists, try CAS with update() using current revision
                                        match kv_store.create("leader", data.clone().into()).await {
                                            Ok(_) => {
                                                // Successfully became leader
                                                {
                                                    let mut is_leader_guard = is_leader.write().await;
                                                    *is_leader_guard = true;
                                                }
                                                {
                                                    let mut leader_info_guard = leader_info.write().await;
                                                    *leader_info_guard = Some(LeaderInfo {
                                                        instance_id: instance_id.clone(),
                                                        elected_at: now,
                                                        last_renewal: now,
                                                    });
                                                }
                                                tracing::info!(
                                                    instance_id = %instance_id,
                                                    "Successfully became leader after detecting expired lease"
                                                );
                                            }
                                            Err(_) => {
                                                // Attempt CAS takeover
                                                if let Ok(Some(curr)) = kv_store.entry("leader").await {
                                                    let _ = kv_store.update("leader", data.into(), curr.revision).await;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        tracing::info!(instance_id = %instance_id, "Shutdown signal received, stopping leader monitoring");
                        break;
                    }
                }
            }
        });

        // 保存任务句柄
        {
            let mut handles = self.background_handles.write().await;
            handles.push(handle);
        }

        Ok(())
    }

    /// Check if this instance is the current leader
    pub async fn is_leader(&self) -> bool {
        let is_leader_guard = self.is_leader.read().await;
        *is_leader_guard
    }

    /// Stop being the leader (for graceful shutdown)
    pub async fn stop_leadership(&self) -> Result<(), CoreError> {
        tracing::info!(instance_id = %self.instance_id, "Stopping leadership");

        // Mark ourselves as not leader
        {
            let mut is_leader_guard = self.is_leader.write().await;
            *is_leader_guard = false;
        }

        // Clear leader info
        {
            let mut leader_info_guard = self.leader_info.write().await;
            *leader_info_guard = None;
        }

        // Cancel all background tasks
        self.shutdown_token.cancel();

        // Wait for all background tasks to complete
        {
            let mut handles = self.background_handles.write().await;
            while let Some(handle) = handles.pop() {
                if let Err(e) = handle.await {
                    tracing::warn!(instance_id = %self.instance_id, "Background task failed: {}", e);
                }
            }
        }

        // Try to remove the leader key from KV store
        if let Err(e) = self.kv_store.delete("leader").await {
            tracing::warn!(instance_id = %self.instance_id, "Failed to remove leader key: {}", e);
        }

        tracing::info!(instance_id = %self.instance_id, "Leadership stopped");
        Ok(())
    }
}
