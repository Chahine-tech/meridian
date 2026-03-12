use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing or malformed Authorization header")]
    MissingToken,

    #[error("token format invalid: expected '<claims_b64>.<sig_b64>'")]
    InvalidFormat,

    #[error("token base64 decode failed: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error("token claims deserialization failed: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),

    #[error("signature invalid")]
    SignatureInvalid,

    #[error("token expired at {expired_at_ms}ms (now: {now_ms}ms)")]
    Expired { expired_at_ms: u64, now_ms: u64 },

    #[error("insufficient permissions: need {required}, have {have}")]
    InsufficientPermissions { required: String, have: String },

    #[error("signing error: {0}")]
    Signing(String),
}
