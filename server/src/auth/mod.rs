// Re-export pure auth types from meridian-core.
pub use meridian_core::auth::{AuthError, Permissions, TokenClaims, TokenSigner};

// Server-only: axum middleware (depends on axum, stays in server)
pub mod middleware;
pub use middleware::{auth_middleware, AuthState, ClaimsExt};
