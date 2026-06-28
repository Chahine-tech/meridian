use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Deserialize;

use crate::auth::{ClaimsExt, PermEntry, Permissions, PermissionsV1, PermissionsV2, TokenClaims};

use super::AppStateExt;

// POST /v1/namespaces/:ns/tokens

#[derive(Deserialize)]
pub struct IssueTokenRequest {
    pub client_id: u64,
    /// TTL in milliseconds. Default: 1 hour.
    pub ttl_ms: Option<u64>,
    /// V1 permissions (glob lists). Mutually exclusive with `rules`.
    pub permissions: Option<PermissionsV1Dto>,
    /// V2 fine-grained rules. Takes precedence over `permissions` if both present.
    pub rules: Option<PermissionsV2Dto>,
    /// Optional base64url-encoded 32-byte Ed25519 public key for BFT op signing.
    /// When provided, the issued token embeds the key and the server will require
    /// a valid Ed25519 signature on every op from this client.
    pub client_pubkey: Option<String>,
}

/// V1 permission payload — glob-list style.
#[derive(Deserialize)]
pub struct PermissionsV1Dto {
    /// Glob patterns for readable keys. `["*"]` = all keys. Omit = no read access.
    pub read: Option<Vec<String>>,
    /// Glob patterns for writable keys. `["*"]` = all keys. Omit = no write access.
    pub write: Option<Vec<String>>,
    pub admin: Option<bool>,
}

/// V2 fine-grained permission payload.
#[derive(Deserialize)]
pub struct PermissionsV2Dto {
    /// Read rules — first match wins.
    pub r: Vec<PermEntryDto>,
    /// Write rules — first match wins.
    pub w: Vec<PermEntryDto>,
    pub admin: Option<bool>,
    /// Per-token rate limit override (requests/sec).
    pub rl: Option<u32>,
}

/// A single permission rule (V2).
#[derive(Deserialize)]
pub struct PermEntryDto {
    /// Glob pattern matched against `crdt_id`.
    pub p: String,
    /// Op mask — which ops are allowed. Absent = all ops.
    pub o: Option<u16>,
    /// Per-rule expiry — Unix timestamp in milliseconds.
    pub e: Option<u64>,
}

impl From<PermEntryDto> for PermEntry {
    fn from(dto: PermEntryDto) -> Self {
        Self {
            p: dto.p,
            o: dto.o.unwrap_or(crate::auth::op_masks::ALL),
            e: dto.e,
        }
    }
}

// GET /v1/namespaces/:ns/tokens/me

/// Return the decoded claims of the caller's token as JSON.
///
/// Useful for debugging — lets a client or operator verify exactly what
/// permissions and TTL the current token carries without decoding msgpack
/// manually.
pub async fn token_me<S: AppStateExt>(
    ClaimsExt(claims): ClaimsExt,
    axum::extract::Path(ns): axum::extract::Path<String>,
) -> Response {
    if claims.namespace != ns {
        return (
            axum::http::StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "token namespace mismatch" })),
        )
            .into_response();
    }
    let pubkey_b64 = claims.client_pubkey.as_ref().map(|b| {
        let alphabet = base64::alphabet::URL_SAFE;
        let config = base64::engine::GeneralPurposeConfig::new();
        let engine = base64::engine::GeneralPurpose::new(&alphabet, config);
        base64::Engine::encode(&engine, b.as_ref())
    });
    Json(serde_json::json!({
        "namespace":    claims.namespace,
        "client_id":    claims.client_id,
        "expires_at":   claims.expires_at,
        "permissions":  claims.permissions,
        "client_pubkey": pubkey_b64,
    }))
    .into_response()
}

/// Issue a new token for `client_id` in the caller's namespace.
/// Requires `admin` permission on the issuing token.
pub async fn issue_token<S: AppStateExt>(
    ClaimsExt(claims): ClaimsExt,
    State(state): State<S>,
    Json(req): Json<IssueTokenRequest>,
) -> Response {
    if !claims.is_admin() {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "admin permission required" })),
        )
            .into_response();
    }

    let perms: Permissions = if let Some(v2) = req.rules {
        // V2 fine-grained rules
        Permissions::V2(PermissionsV2 {
            v: 2,
            r: v2.r.into_iter().map(PermEntry::from).collect(),
            w: v2.w.into_iter().map(PermEntry::from).collect(),
            admin: v2.admin.unwrap_or(false),
            rl: v2.rl,
        })
    } else if let Some(v1) = req.permissions {
        // V1 glob lists
        Permissions::V1(PermissionsV1 {
            read: v1.read.unwrap_or_else(|| vec!["*".into()]),
            write: v1.write.unwrap_or_else(|| vec!["*".into()]),
            admin: v1.admin.unwrap_or(false),
        })
    } else {
        // Default: read-write V1
        Permissions::read_write()
    };

    // Decode the optional BFT client public key (base64url → 32 bytes).
    let client_pubkey: Option<serde_bytes::ByteBuf> = if let Some(b64) = req.client_pubkey {
        match base64_url_decode(&b64) {
            Ok(bytes) if bytes.len() == 32 => Some(serde_bytes::ByteBuf::from(bytes)),
            Ok(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(
                        serde_json::json!({ "error": "client_pubkey must be 32 bytes (Ed25519)" }),
                    ),
                )
                    .into_response();
            }
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "client_pubkey: invalid base64url" })),
                )
                    .into_response();
            }
        }
    } else {
        None
    };

    let new_claims = TokenClaims::new(
        &claims.namespace,
        req.client_id,
        req.ttl_ms.unwrap_or(3_600_000),
        perms,
    )
    .with_pubkey(client_pubkey);

    match state.signer().sign(&new_claims) {
        Ok(token) => Json(serde_json::json!({ "token": token })).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "token signing failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn base64_url_decode(s: &str) -> Result<Vec<u8>, ()> {
    // Use base64 with URL-safe alphabet, no padding.
    let alphabet = base64::alphabet::URL_SAFE;
    let config = base64::engine::GeneralPurposeConfig::new()
        .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent);
    let engine = base64::engine::GeneralPurpose::new(&alphabet, config);
    base64::Engine::decode(&engine, s).map_err(|_| ())
}
