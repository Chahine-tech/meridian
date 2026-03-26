use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde::Deserialize;

use crate::auth::{ClaimsExt, Permissions, PermissionsV1, PermissionsV2, PermEntry, TokenClaims};

use super::AppStateExt;

// ---------------------------------------------------------------------------
// POST /v1/namespaces/:ns/tokens
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct IssueTokenRequest {
    pub client_id: u64,
    /// TTL in milliseconds. Default: 1 hour.
    pub ttl_ms: Option<u64>,
    /// V1 permissions (glob lists). Mutually exclusive with `rules`.
    pub permissions: Option<PermissionsV1Dto>,
    /// V2 fine-grained rules. Takes precedence over `permissions` if both present.
    pub rules: Option<PermissionsV2Dto>,
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

    let new_claims = TokenClaims::new(
        &claims.namespace,
        req.client_id,
        req.ttl_ms.unwrap_or(3_600_000),
        perms,
    );

    match state.signer().sign(&new_claims) {
        Ok(token) => Json(serde_json::json!({ "token": token })).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "token signing failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
