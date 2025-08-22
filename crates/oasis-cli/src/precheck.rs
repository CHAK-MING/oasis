use anyhow::Result;
use oasis_core::proto::{HealthCheckRequest, oasis_service_client::OasisServiceClient};
use tokio::time::{Duration, sleep, timeout};

/// 检查服务器健康状态
pub async fn precheck_server_health(
    client: &mut OasisServiceClient<tonic::transport::Channel>,
) -> Result<()> {
    let max_retries = 3;
    let request_timeout = Duration::from_secs(10); // 长超时
    let retry_delay = Duration::from_millis(500);

    for attempt in 1..=max_retries {
        match timeout(request_timeout, client.health_check(HealthCheckRequest {})).await {
            Ok(Ok(resp)) => {
                let r = resp.get_ref();
                if r.ok {
                    if attempt > 1 {
                        tracing::info!("Server health check succeeded on attempt {}", attempt);
                    }
                    return Ok(());
                } else {
                    return Err(anyhow::anyhow!("Server unhealthy: {}", r.message));
                }
            }
            Ok(Err(e)) => {
                if attempt < max_retries {
                    tracing::debug!(
                        "Health check attempt {} failed: {}, retrying...",
                        attempt,
                        e
                    );
                    sleep(retry_delay).await;
                } else {
                    return Err(anyhow::anyhow!(
                        "Server connection failed after {} attempts: {}",
                        max_retries,
                        e
                    ));
                }
            }
            Err(_) => {
                if attempt < max_retries {
                    tracing::debug!("Health check attempt {} timed out, retrying...", attempt);
                    sleep(retry_delay).await;
                } else {
                    return Err(anyhow::anyhow!(
                        "Server health check timed out after {} attempts",
                        max_retries
                    ));
                }
            }
        }
    }

    unreachable!()
}
