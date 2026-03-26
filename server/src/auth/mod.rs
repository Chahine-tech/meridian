// Re-export pure auth types from meridian-core.
pub use meridian_core::auth::{
    AuthError, Permissions, PermissionsV1, PermissionsV2, PermEntry, TokenClaims, TokenSigner,
    op_masks,
};

// Server-only: axum middleware (depends on axum, stays in server)
pub mod middleware;
pub use middleware::{auth_middleware, AuthState, ClaimsExt};
