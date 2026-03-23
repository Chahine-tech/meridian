use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use rand::rngs::OsRng;

use crate::crdt::clock::now_ms;

use super::{
    claims::TokenClaims,
    error::AuthError,
};

/// Signs and verifies Meridian tokens using ed25519.
///
/// ## Token wire format
/// ```text
/// base64url_no_pad(msgpack(TokenClaims)) + "." + base64url_no_pad(ed25519_signature)
/// ```
///
/// Rationale for custom format over JWT:
/// - msgpack payload is 2-3× smaller than JSON
/// - No header field needed — algorithm is always ed25519
/// - `ed25519-dalek::VerifyingKey::verify_strict` is constant-time (timing-safe)
pub struct TokenSigner {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
}

impl TokenSigner {
    /// Construct from a 32-byte secret seed.
    pub fn from_bytes(seed: &[u8; 32]) -> Self {
        let signing_key = SigningKey::from_bytes(seed);
        let verifying_key = signing_key.verifying_key();
        Self { signing_key, verifying_key }
    }

    /// Generate a fresh random keypair (for dev/testing).
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();
        Self { signing_key, verifying_key }
    }

    /// Construct from a hex-encoded 32-byte secret seed.
    pub fn from_hex(hex: &str) -> Result<Self, AuthError> {
        let bytes = hex::decode(hex)
            .map_err(|e| AuthError::Signing(format!("invalid hex: {e}")))?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| AuthError::Signing("seed must be exactly 32 bytes".into()))?;
        Ok(Self::from_bytes(&arr))
    }

    /// Sign `claims` and return the token string.
    pub fn sign(&self, claims: &TokenClaims) -> Result<String, AuthError> {
        let payload = rmp_serde::encode::to_vec_named(claims)
            .map_err(|e| AuthError::Signing(e.to_string()))?;
        let payload_b64 = URL_SAFE_NO_PAD.encode(&payload);

        let signature: Signature = self.signing_key.sign(payload_b64.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        Ok(format!("{payload_b64}.{sig_b64}"))
    }

    /// Verify a token and return the decoded claims.
    ///
    /// Checks (in order):
    /// 1. Format (`claims_b64.sig_b64`)
    /// 2. ed25519 signature (constant-time)
    /// 3. Expiry (`claims.expires_at > now_ms()`)
    pub fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        let (payload_b64, sig_b64) = token
            .split_once('.')
            .ok_or(AuthError::InvalidFormat)?;

        // Decode signature
        let sig_bytes = URL_SAFE_NO_PAD.decode(sig_b64)?;
        let sig_arr: [u8; 64] = sig_bytes
            .try_into()
            .map_err(|_| AuthError::InvalidFormat)?;
        let signature = Signature::from_bytes(&sig_arr);

        // Verify (constant-time)
        self.verifying_key
            .verify_strict(payload_b64.as_bytes(), &signature)
            .map_err(|_| AuthError::SignatureInvalid)?;

        // Decode payload
        let payload = URL_SAFE_NO_PAD.decode(payload_b64)?;
        let claims: TokenClaims = rmp_serde::decode::from_slice(&payload)?;

        // Check expiry
        let now = now_ms();
        if now >= claims.expires_at {
            return Err(AuthError::Expired {
                expired_at_ms: claims.expires_at,
                now_ms: now,
            });
        }

        Ok(claims)
    }

    /// The verifying (public) key as a 32-byte array.
    pub fn verifying_key_bytes(&self) -> [u8; 32] {
        self.verifying_key.to_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::claims::Permissions;

    fn signer() -> TokenSigner {
        TokenSigner::generate()
    }

    fn valid_claims() -> TokenClaims {
        TokenClaims::new("my-room", 42, 3_600_000, Permissions::read_write())
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let s = signer();
        let claims = valid_claims();
        let token = s.sign(&claims).unwrap();
        let decoded = s.verify(&token).unwrap();
        assert_eq!(claims, decoded);
    }

    #[test]
    fn wrong_signature_rejected() {
        let s = signer();
        let claims = valid_claims();
        let token = s.sign(&claims).unwrap();

        // Tamper with the payload
        let (payload_b64, sig_b64) = token.split_once('.').unwrap();
        let mut payload = URL_SAFE_NO_PAD.decode(payload_b64).unwrap();
        payload[0] ^= 0xff; // flip first byte
        let tampered = format!("{}.{sig_b64}", URL_SAFE_NO_PAD.encode(&payload));

        assert!(matches!(s.verify(&tampered), Err(AuthError::SignatureInvalid)));
    }

    #[test]
    fn expired_token_rejected() {
        let s = signer();
        let claims = TokenClaims {
            namespace: "ns".into(),
            client_id: 1,
            expires_at: 1, // epoch + 1ms
            permissions: Permissions::read_write(),
        };
        let token = s.sign(&claims).unwrap();
        assert!(matches!(s.verify(&token), Err(AuthError::Expired { .. })));
    }

    #[test]
    fn missing_dot_rejected() {
        let s = signer();
        assert!(matches!(s.verify("nodothere"), Err(AuthError::InvalidFormat)));
    }

    #[test]
    fn wrong_signer_rejected() {
        let s1 = signer();
        let s2 = signer();
        let token = s1.sign(&valid_claims()).unwrap();
        assert!(matches!(s2.verify(&token), Err(AuthError::SignatureInvalid)));
    }

    #[test]
    fn from_hex_roundtrip() {
        let seed = [0x42u8; 32];
        let s1 = TokenSigner::from_bytes(&seed);
        let hex = hex::encode(seed);
        let s2 = TokenSigner::from_hex(&hex).unwrap();

        let token = s1.sign(&valid_claims()).unwrap();
        let decoded = s2.verify(&token).unwrap();
        assert_eq!(decoded.namespace, "my-room");
    }

    #[test]
    fn from_hex_wrong_length_rejected() {
        assert!(TokenSigner::from_hex("deadbeef").is_err());
    }
}
