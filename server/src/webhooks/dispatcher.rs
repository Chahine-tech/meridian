use std::sync::Arc;
use std::time::Duration;

use hmac::{Hmac, Mac};
use reqwest::Client;
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::{WebhookConfig, WebhookEvent};

const CHANNEL_CAPACITY: usize = 1_024;
const MAX_RETRIES: u32 = 3;
const RETRY_BASE_MS: u64 = 200;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Sends webhook events to a configured HTTP endpoint.
///
/// Events are delivered asynchronously via an internal channel so handlers
/// never block on delivery. Failed deliveries are retried up to `MAX_RETRIES`
/// times with exponential backoff.
#[derive(Clone)]
pub struct WebhookDispatcher {
    sender: mpsc::Sender<WebhookEvent>,
}

impl WebhookDispatcher {
    /// Creates a dispatcher and spawns its background delivery task.
    /// Returns `None` if no webhook is configured.
    pub fn new(config: WebhookConfig, cancel: CancellationToken) -> Self {
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        let config = Arc::new(config);

        tokio::spawn(run_dispatcher(receiver, config, cancel));

        Self { sender }
    }

    /// Enqueues an event for async delivery. Non-blocking — drops the event
    /// if the channel is full (back-pressure safety valve).
    pub fn send(&self, event: WebhookEvent) {
        if self.sender.try_send(event).is_err() {
            warn!("webhook channel full — event dropped");
        }
    }
}

async fn run_dispatcher(
    mut receiver: mpsc::Receiver<WebhookEvent>,
    config: Arc<WebhookConfig>,
    cancel: CancellationToken,
) {
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .expect("failed to build reqwest client");

    loop {
        tokio::select! {
            maybe_event = receiver.recv() => {
                match maybe_event {
                    Some(event) => deliver(&client, &config, event).await,
                    None => break,
                }
            }
            () = cancel.cancelled() => break,
        }
    }

    debug!("webhook dispatcher stopped");
}

async fn deliver(client: &Client, config: &WebhookConfig, event: WebhookEvent) {
    for attempt in 0..MAX_RETRIES {
        match try_deliver(client, config, &event).await {
            Ok(()) => return,
            Err(error) => {
                let backoff_ms = RETRY_BASE_MS * (1 << attempt);
                warn!(
                    ns = event.ns,
                    crdt_id = event.crdt_id,
                    attempt,
                    %error,
                    backoff_ms,
                    "webhook delivery failed — retrying"
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }

    warn!(
        ns = event.ns,
        crdt_id = event.crdt_id,
        "webhook delivery failed after {} attempts — giving up",
        MAX_RETRIES
    );
}

async fn try_deliver(
    client: &Client,
    config: &WebhookConfig,
    event: &WebhookEvent,
) -> Result<(), reqwest::Error> {
    let body = match serde_json::to_string(event) {
        Ok(b) => b,
        Err(error) => {
            warn!(%error, "failed to serialize webhook event — skipping");
            return Ok(());
        }
    };
    let signature = hmac_signature(&config.secret, &body);

    client
        .post(&config.url)
        .header("Content-Type", "application/json")
        .header("X-Meridian-Signature", signature)
        .body(body)
        .send()
        .await?
        .error_for_status()?;

    debug!(ns = event.ns, crdt_id = event.crdt_id, "webhook delivered");
    Ok(())
}

/// Computes `HMAC-SHA256(secret, body)` as a lowercase hex string.
/// Compatible with standard webhook verification in Node, Python, Go, etc.
pub(crate) fn hmac_signature(secret: &str, body: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts keys of any length");
    mac.update(body.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::hmac_signature;

    /// Known-good vector: verified against Node.js
    /// `crypto.createHmac('sha256', 'secret').update('hello').digest('hex')`
    #[test]
    fn hmac_signature_matches_known_vector() {
        let result = hmac_signature("secret", "hello");
        assert_eq!(
            result,
            "88aab3ede8d3adf94d26ab90d3bafd4a2083070c3bcce9c014ee04a443847c0b"
        );
    }

    #[test]
    fn hmac_signature_different_secrets_produce_different_output() {
        let signature_a = hmac_signature("secret-a", "body");
        let signature_b = hmac_signature("secret-b", "body");
        assert_ne!(signature_a, signature_b);
    }

    #[test]
    fn hmac_signature_different_bodies_produce_different_output() {
        let signature_a = hmac_signature("secret", "body-a");
        let signature_b = hmac_signature("secret", "body-b");
        assert_ne!(signature_a, signature_b);
    }
}
