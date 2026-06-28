// Re-export pure auth types from meridian-core.
pub use meridian_core::auth::{
    AuthError, PermEntry, Permissions, PermissionsV1, PermissionsV2, TokenClaims, TokenSigner,
    glob_match, op_masks,
};

// Server-only: axum middleware (depends on axum, stays in server)
pub mod middleware;
pub use middleware::{AuthState, ClaimsExt, auth_middleware};
