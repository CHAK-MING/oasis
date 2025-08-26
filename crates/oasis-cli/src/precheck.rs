use anyhow::{anyhow, Result};
use console::style;
use oasis_core::proto::{oasis_service_client::OasisServiceClient, HealthCheckRequest};
use tokio::time::{sleep, timeout, Duration};

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
                        println!(
                            "  {} 服务器健康检查成功 (第 {} 次尝试)",
                            style("✔").green(),
                            attempt
                        );
                    }
                    return Ok(());
                } else {
                    return Err(anyhow!("服务器返回不健康状态: {}", r.message));
                }
            }
            Ok(Err(e)) => {
                if attempt < max_retries {
                    eprintln!(
                        "  {} 健康检查失败 (尝试 {}/{}): {}。正在重试...",
                        style("!").yellow(),
                        attempt,
                        max_retries,
                        e
                    );
                    sleep(retry_delay).await;
                } else {
                    return Err(anyhow!("健康检查在 {} 次尝试后失败: {}", max_retries, e));
                }
            }
            Err(_) => {
                if attempt < max_retries {
                    eprintln!(
                        "  {} 健康检查超时 (尝试 {}/{})。正在重试...",
                        style("!").yellow(),
                        attempt,
                        max_retries,
                    );
                    sleep(retry_delay).await;
                } else {
                    return Err(anyhow!("健康检查在 {} 次尝试后超时", max_retries));
                }
            }
        }
    }

    unreachable!()
}
