use async_nats::jetstream::Context;
use async_nats::jetstream::kv::Store;
use oasis_core::error::CoreError;
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
    election_key: String,
    renewal_interval: Duration,
    is_leader: Arc<RwLock<bool>>,
    /// Cancellation token for graceful shutdown
    shutdown_token: CancellationToken,
}

#[derive(Clone)]
pub struct LeaderElectionConfig {
    pub election_key: String,
    pub ttl_sec: u64,
    pub check_interval_sec: u64,
}

impl LeaderElectionService {
    /// Create a new leader election service
    pub async fn new(
        jetstream: Context,
        config: &LeaderElectionConfig,
        shutdown_token: CancellationToken,
    ) -> Result<Self, CoreError> {
        let instance_id = Uuid::new_v4().to_string();

        let lease_ttl = Duration::from_secs(config.ttl_sec);
        let renewal_interval = Duration::from_secs(config.check_interval_sec);

        // Get-or-create the leader election KV store（幂等初始化）
        let kv_store = match jetstream.get_key_value("leader-election").await {
            Ok(store) => store,
            Err(_) => {
                jetstream
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
                    })?
            }
        };

        let service = Self {
            kv_store: Arc::new(kv_store),
            instance_id: instance_id.clone(),
            renewal_interval,
            is_leader: Arc::new(RwLock::new(false)),
            election_key: config.election_key.clone(),
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

    /// Start a single-task KV-based leader election loop (create/CAS update with TTL).
    pub async fn start_election(&self) -> Result<(), CoreError> {
        tracing::info!(instance_id = %self.instance_id, "Starting leader election loop");

        let kv_store = Arc::clone(&self.kv_store);
        let instance_id = self.instance_id.clone();
        let election_key = self.election_key.clone();
        let renewal_interval = self.renewal_interval;
        let is_leader = Arc::clone(&self.is_leader);
        let shutdown = self.shutdown_token.child_token();

        tokio::spawn(async move {
            let mut ticker = interval(renewal_interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Check current role
                        let am_leader = { *is_leader.read().await };
                        if !am_leader {
                            // Try create
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
                            let msg = oasis_core::proto::LeaderInfoMsg { instance_id: instance_id.clone(), elected_at: now, last_renewal: now };
                            let data = oasis_core::proto_impls::encoding::to_vec(&msg);
                            match kv_store.create(&election_key, data.clone().into()).await {
                                Ok(_) => {
                                    {
                                        let mut guard = is_leader.write().await;
                                        *guard = true;
                                    }
                                    // Seed CAS by reading current entry (best-effort)
                                    let _ = kv_store.entry(&election_key).await;
                                    tracing::info!(instance_id = %instance_id, "Became leader via create()");
                                }
                                Err(_) => {
                                    // Not leader; do nothing until next tick
                                }
                            }
                        } else {
                            // Renew leadership via CAS update
                            // Get current revision
                            let curr_rev = if let Ok(Some(entry)) = kv_store.entry(&election_key).await { Some(entry.revision) } else { None };
                            if let Some(rev) = curr_rev {
                                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
                                // Keep elected_at from existing value if available
                                let elected_at = now; // best-effort; correctness not critical
                                let msg = oasis_core::proto::LeaderInfoMsg { instance_id: instance_id.clone(), elected_at, last_renewal: now };
                                let data = oasis_core::proto_impls::encoding::to_vec(&msg);
                                match kv_store.update(&election_key, data.into(), rev).await {
                                    Ok(_) => {
                                        tracing::debug!(instance_id = %instance_id, "Lease renewed (CAS)");
                                    }
                                    Err(e) => {
                                        tracing::warn!(instance_id = %instance_id, error = %e, "CAS renew failed; stepping down");
                                        let mut guard = is_leader.write().await; *guard = false;
                                    }
                                }
                            } else {
                                // Lost entry; step down
                                let mut guard = is_leader.write().await; *guard = false;
                            }
                        }
                    }
                    _ = shutdown.cancelled() => {
                        tracing::info!(instance_id = %instance_id, "Leader election loop shutting down");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    // Removed complex renewal/monitoring; single loop handles both roles

    /// Stop being the leader (for graceful shutdown)
    pub async fn stop_leadership(&self) -> Result<(), CoreError> {
        tracing::info!(instance_id = %self.instance_id, "Stopping leadership");

        // Mark ourselves as not leader
        {
            let mut is_leader_guard = self.is_leader.write().await;
            *is_leader_guard = false;
        }

        // Cancel background loop
        self.shutdown_token.cancel();

        // Try to remove the leader key from KV store
        if let Err(e) = self.kv_store.delete(&self.election_key).await {
            tracing::warn!(instance_id = %self.instance_id, "Failed to remove leader key: {}", e);
        }

        tracing::info!(instance_id = %self.instance_id, "Leadership stopped");
        Ok(())
    }
}
