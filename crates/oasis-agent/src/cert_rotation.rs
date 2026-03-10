use crate::{cert_bootstrap::CertBootstrap, nats_client::ManagedNatsClient};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub struct CertRotationManager {
    cert_bootstrap: CertBootstrap,
    nats_client: ManagedNatsClient,
    check_interval: Duration,
    shutdown_token: CancellationToken,
}

impl CertRotationManager {
    pub fn new(
        cert_bootstrap: CertBootstrap,
        nats_client: ManagedNatsClient,
        check_interval: Duration,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            cert_bootstrap,
            nats_client,
            check_interval,
            shutdown_token,
        }
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.check_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = self.shutdown_token.cancelled() => break,
                    _ = interval.tick() => {
                        match self.cert_bootstrap.renew_if_needed().await {
                            Ok(true) => {
                                match self.nats_client.reconnect().await {
                                    Ok(()) => {
                                        info!("Certificate rotated successfully, NATS connection reloaded in-process");
                                    }
                                    Err(err) => {
                                        error!("Certificate rotated but NATS reconnect failed: {}", err);
                                        self.shutdown_token.cancel();
                                        break;
                                    }
                                }
                            }
                            Ok(false) => {}
                            Err(err) => error!("Certificate rotation check failed: {}", err),
                        }
                    }
                }
            }
        })
    }
}
