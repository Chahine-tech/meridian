/// Webhook delivery configuration, loaded from environment variables.
///
/// | Variable                  | Description                                      |
/// |---------------------------|--------------------------------------------------|
/// | `MERIDIAN_WEBHOOK_URL`    | HTTP(S) endpoint to POST events to               |
/// | `MERIDIAN_WEBHOOK_SECRET` | HMAC-SHA256 signing secret (hex or plain string) |
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    /// Target URL for webhook delivery.
    pub url: String,
    /// Raw secret used to compute `X-Meridian-Signature`.
    pub secret: String,
}

impl WebhookConfig {
    /// Returns `None` if `MERIDIAN_WEBHOOK_URL` is not set (webhooks disabled).
    pub fn from_env() -> Option<Self> {
        let url = std::env::var("MERIDIAN_WEBHOOK_URL").ok()?;
        let secret = std::env::var("MERIDIAN_WEBHOOK_SECRET")
            .unwrap_or_else(|_| String::new());
        Some(Self { url, secret })
    }
}
