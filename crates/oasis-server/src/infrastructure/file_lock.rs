use async_nats::jetstream::Context;
use oasis_core::error::{CoreError, ErrorSeverity, Result};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

const FILE_LOCK_BUCKET: &str = "file-locks";
// NATS KV keys must be valid subject tokens; ':' is not allowed.
// We also hash the source path to keep keys short and safe.
const UPLOAD_LOCK_PREFIX: &str = "upload/";
const UPLOAD_LOCK_STALE_SECS: i64 = 10 * 60;

pub struct FileLockManager {
    jetstream: Arc<Context>,
}

impl FileLockManager {
    pub async fn new(jetstream: Arc<Context>) -> Result<Self> {
        Ok(Self { jetstream })
    }

    async fn kv_store(&self) -> Result<async_nats::jetstream::kv::Store> {
        match self.jetstream.get_key_value(FILE_LOCK_BUCKET).await {
            Ok(kv) => Ok(kv),
            Err(_) => self
                .jetstream
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: FILE_LOCK_BUCKET.to_string(),
                    history: 1,
                    max_age: Duration::from_secs(UPLOAD_LOCK_STALE_SECS as u64),
                    ..Default::default()
                })
                .await
                .map_err(|e| CoreError::Internal {
                    message: format!("Failed to create file-locks KV: {}", e),
                    severity: ErrorSeverity::Error,
                }),
        }
    }

    pub async fn acquire_lock(&self, source_path: &str) -> Result<FileLock> {
        let kv = self.kv_store().await?;
        let lock_key = Self::lock_key(source_path);

        // Store a timestamp for debugging. Stale recovery relies on the KV bucket max_age.
        let now = chrono::Utc::now().timestamp();
        let lock_value = now.to_string();

        match kv.create(&lock_key, lock_value.into()).await {
            Ok(_) => {
                debug!("Acquired upload lock for: {}", source_path);
                Ok(FileLock {
                    kv,
                    lock_key,
                    source_path: source_path.to_string(),
                })
            }
            Err(create_err) => {
                // Only treat as conflict if the lock key truly exists.
                // This prevents masking other problems (e.g., invalid key, KV errors) as conflicts.
                match kv.get(&lock_key).await {
                    Ok(Some(_)) => Err(CoreError::Conflict {
                        message: format!(
                            "File {} is being uploaded by another client. Please retry later.",
                            source_path
                        ),
                        severity: ErrorSeverity::Warning,
                    }),
                    Ok(None) => Err(CoreError::Internal {
                        message: format!(
                            "Failed to create upload lock for {} (lock_key={}): {}",
                            source_path, lock_key, create_err
                        ),
                        severity: ErrorSeverity::Error,
                    }),
                    Err(get_err) => Err(CoreError::Internal {
                        message: format!(
                            "Failed to check existing upload lock for {} (lock_key={}): {} (create error: {})",
                            source_path, lock_key, get_err, create_err
                        ),
                        severity: ErrorSeverity::Error,
                    }),
                }
            }
        }
    }

    fn lock_key(source_path: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(source_path.as_bytes());
        let digest = hasher.finalize();
        let mut hex = String::with_capacity(64);
        for b in digest.iter() {
            let _ = write!(&mut hex, "{:02x}", b);
        }

        // 32 hex chars (128 bits) is enough to avoid collisions for our purposes.
        format!("{}{}", UPLOAD_LOCK_PREFIX, &hex[..32])
    }
}

pub struct FileLock {
    kv: async_nats::jetstream::kv::Store,
    lock_key: String,
    source_path: String,
}

impl FileLock {
    pub async fn release(self) -> Result<()> {
        match self.kv.delete(&self.lock_key).await {
            Ok(_) => {
                debug!("Released upload lock for: {}", self.source_path);
                Ok(())
            }
            Err(e) => {
                warn!(
                    "Failed to release upload lock for {}: {}",
                    self.source_path, e
                );
                Err(CoreError::Internal {
                    message: format!("Failed to release lock: {}", e),
                    severity: ErrorSeverity::Warning,
                })
            }
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let kv = self.kv.clone();
        let lock_key = self.lock_key.clone();
        let source_path = self.source_path.clone();

        tokio::spawn(async move {
            if let Err(e) = kv.delete(&lock_key).await {
                warn!(
                    "Failed to auto-release lock for {} in Drop: {}",
                    source_path, e
                );
            } else {
                debug!("Auto-released lock for {} in Drop", source_path);
            }
        });
    }
}
