use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ClientError;

/// Configuration for the HTTP client.
pub struct HttpClientConfig {
    /// Base URL, e.g. `"https://api.example.com"`.
    pub base_url: String,
    /// Meridian auth token.
    pub token: String,
    /// Request timeout (default: 10 s).
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CrdtGetResponse {
    pub crdt_id: String,
    pub crdt_type: Option<String>,
    pub value: Value,
    pub vector_clock: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HistoryEntry {
    pub seq: u64,
    pub timestamp_ms: u64,
    pub op: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HistoryResponse {
    pub crdt_id: String,
    pub entries: Vec<HistoryEntry>,
    pub next_seq: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenIssueResponse {
    pub token: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenIssueRequest {
    pub client_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<Value>,
}

/// HTTP client for Meridian — covers GET/POST/history/token endpoints.
///
/// Use this alongside `MeridianClient` for one-shot reads, admin ops, and
/// history access that don't need a persistent WebSocket connection.
pub struct HttpClient {
    base_url: String,
    token: String,
    inner: Client,
}

impl HttpClient {
    pub fn new(config: HttpClientConfig) -> Self {
        let timeout = Duration::from_millis(config.timeout_ms.unwrap_or(10_000));
        let inner = Client::builder()
            .timeout(timeout)
            .build()
            .expect("reqwest client build");
        Self {
            base_url: config.base_url.trim_end_matches('/').to_owned(),
            token: config.token,
            inner,
        }
    }

    /// `GET /v1/namespaces/{ns}/crdts/{id}` — fetch current CRDT state.
    pub async fn get_crdt(
        &self,
        namespace: &str,
        crdt_id: &str,
    ) -> Result<CrdtGetResponse, ClientError> {
        let url = format!("{}/v1/namespaces/{namespace}/crdts/{crdt_id}", self.base_url);
        self.get_json(&url).await
    }

    /// `GET /v1/namespaces/{ns}/crdts/{id}/sync?since=…` — delta sync.
    ///
    /// `since_vc` is a vector clock map `{client_id: seq}`.
    pub async fn sync_crdt(
        &self,
        namespace: &str,
        crdt_id: &str,
        since_vc: Option<&std::collections::HashMap<u64, u64>>,
    ) -> Result<CrdtGetResponse, ClientError> {
        let mut url = format!(
            "{}/v1/namespaces/{namespace}/crdts/{crdt_id}/sync",
            self.base_url
        );
        if let Some(vc) = since_vc {
            if !vc.is_empty() {
                let vc_val: Value = serde_json::to_value(vc).map_err(ClientError::Json)?;
                let bytes = crate::codec::encode_vc_map(vc_val)?;
                use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
                url.push_str("?since=");
                url.push_str(&URL_SAFE_NO_PAD.encode(&bytes));
            }
        }
        self.get_json(&url).await
    }

    /// `GET /v1/namespaces/{ns}/crdts/{id}/history` — WAL history.
    pub async fn get_history(
        &self,
        namespace: &str,
        crdt_id: &str,
        since_seq: u64,
        limit: usize,
    ) -> Result<HistoryResponse, ClientError> {
        let url = format!(
            "{}/v1/namespaces/{namespace}/crdts/{crdt_id}/history?since_seq={since_seq}&limit={limit}",
            self.base_url
        );
        self.get_json(&url).await
    }

    /// `POST /v1/namespaces/{ns}/tokens` — issue a new auth token (admin).
    pub async fn issue_token(
        &self,
        namespace: &str,
        req: TokenIssueRequest,
    ) -> Result<TokenIssueResponse, ClientError> {
        let url = format!("{}/v1/namespaces/{namespace}/tokens", self.base_url);
        self.post_json(&url, &req).await
    }

    /// `GET /v1/namespaces/{ns}/tokens/me` — decode the caller's own token.
    pub async fn token_me(
        &self,
        namespace: &str,
    ) -> Result<meridian_core::auth::TokenClaims, ClientError> {
        let url = format!("{}/v1/namespaces/{namespace}/tokens/me", self.base_url);
        self.get_json(&url).await
    }


    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
    ) -> Result<T, ClientError> {
        let resp = self
            .inner
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| ClientError::Http { status: 0, body: e.to_string() })?;
        self.parse_response(resp).await
    }

    async fn post_json<B: Serialize, T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T, ClientError> {
        let resp = self
            .inner
            .post(url)
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|e| ClientError::Http { status: 0, body: e.to_string() })?;
        self.parse_response(resp).await
    }

    async fn parse_response<T: serde::de::DeserializeOwned>(
        &self,
        resp: reqwest::Response,
    ) -> Result<T, ClientError> {
        let status = resp.status();
        if status.is_success() {
            resp.json::<T>()
                .await
                .map_err(|e| ClientError::Http { status: status.as_u16(), body: e.to_string() })
        } else {
            let body = resp.text().await.unwrap_or_else(|_| status.to_string());
            Err(ClientError::Http { status: status.as_u16(), body })
        }
    }
}
