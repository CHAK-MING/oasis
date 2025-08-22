use crate::application::ports::ConfigRepository;
use crate::config::AgentConfig;
use anyhow::{Context, Result};
use async_nats::jetstream::kv::{Entry, WatcherError as WatchError};
use async_nats::{Client, jetstream};
use async_trait::async_trait;
use futures::Stream;
use futures::TryStreamExt;
use oasis_core::{
    constants::{self, JS_KV_CONFIG},
    types::AgentId,
};

pub struct NatsConfigRepository {
    js: jetstream::Context,
}

impl NatsConfigRepository {
    pub fn new(client: &Client) -> Self {
        Self {
            js: jetstream::new(client.clone()),
        }
    }

    pub async fn watch_all(&self) -> Result<impl Stream<Item = Result<Entry, WatchError>>> {
        let kv = self
            .js
            .get_key_value(JS_KV_CONFIG)
            .await
            .context("Failed to bind to config KV bucket")?;
        let watcher = kv.watch_all().await.context("Failed to watch config")?;
        Ok(watcher)
    }
}

#[async_trait]
impl ConfigRepository for NatsConfigRepository {
    async fn fetch_config(&self, agent_id: &AgentId) -> Result<AgentConfig> {
        let kv = self
            .js
            .get_key_value(JS_KV_CONFIG)
            .await
            .context("Failed to bind to config KV bucket")?;

        let prefix = constants::kv_config_scan_prefix(agent_id.as_str());

        // 列出所有键，然后获取匹配我们 agent 前缀的值
        let keys: Vec<String> = kv
            .keys()
            .await
            .context("Failed to list KV keys")?
            .try_collect()
            .await
            .context("Failed to collect KV keys")?;

        // 收集 KV 更新
        let mut kv_updates = std::collections::HashMap::new();
        for key in keys {
            if let Some(cfg_key) = key.strip_prefix(&prefix) {
                if let Ok(Some(bytes)) = kv.get(&key).await {
                    let val = String::from_utf8(bytes.to_vec()).unwrap_or_default();
                    // 转换键名格式：下划线 -> 点号
                    let config_key = cfg_key.replace('_', ".");
                    kv_updates.insert(config_key, val);
                }
            }
        }

        // 从默认配置开始，应用 KV 更新
        let mut config = AgentConfig::default();
        if !kv_updates.is_empty() {
            config.apply_kv_updates(&kv_updates)?;
        }

        Ok(config)
    }
}
