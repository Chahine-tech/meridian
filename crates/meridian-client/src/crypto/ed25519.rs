use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signer, SigningKey};
use rand::rngs::OsRng;

use crate::error::ClientError;

/// Ed25519 client keypair for BFT op signing.
///
/// Embed `public_key_bytes` (base64url) in the token issuance request as
/// `client_pubkey` so the server can verify op signatures.
#[derive(Clone)]
pub struct ClientKeypair {
    signing_key: SigningKey,
    /// Raw 32-byte Ed25519 public key.
    pub public_key_bytes: [u8; 32],
}

impl ClientKeypair {
    /// Generate a fresh random keypair.
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        let public_key_bytes = signing_key.verifying_key().to_bytes();
        Self { signing_key, public_key_bytes }
    }

    /// Import from a 32-byte seed.
    pub fn from_seed(seed: &[u8; 32]) -> Self {
        let signing_key = SigningKey::from_bytes(seed);
        let public_key_bytes = signing_key.verifying_key().to_bytes();
        Self { signing_key, public_key_bytes }
    }

    /// Import from a base64url-encoded 32-byte seed.
    pub fn from_base64(s: &str) -> Result<Self, ClientError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|e| ClientError::Crypto(e.to_string()))?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| ClientError::Crypto("Ed25519 seed must be exactly 32 bytes".into()))?;
        Ok(Self::from_seed(&arr))
    }

    /// Public key as base64url (for embedding in token issuance requests).
    pub fn public_key_base64(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.public_key_bytes)
    }

    /// Sign `op_bytes` (raw msgpack of a `CrdtOp`). Returns a 64-byte signature.
    pub fn sign(&self, op_bytes: &[u8]) -> [u8; 64] {
        self.signing_key.sign(op_bytes).to_bytes()
    }

    /// Load a keypair from a JSON file, or generate and persist one if absent.
    ///
    /// Default path: `~/.meridian/keypair.json`.
    /// The file stores the seed as base64url — keep it secret.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_or_generate(path: Option<&std::path::Path>) -> Result<Self, ClientError> {
        use std::{fs, path::PathBuf};

        let resolved: PathBuf = match path {
            Some(p) => p.to_path_buf(),
            None => {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
                PathBuf::from(home).join(".meridian").join("keypair.json")
            }
        };

        if resolved.exists() {
            let raw = fs::read_to_string(&resolved)
                .map_err(|e| ClientError::Crypto(e.to_string()))?;
            let v: serde_json::Value =
                serde_json::from_str(&raw).map_err(ClientError::Json)?;
            let seed_b64 = v["seed"]
                .as_str()
                .ok_or_else(|| ClientError::Crypto("missing 'seed' field in keypair file".into()))?;
            return Self::from_base64(seed_b64);
        }

        let kp = Self::generate();
        if let Some(parent) = resolved.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| ClientError::Crypto(e.to_string()))?;
        }
        let seed_b64 = URL_SAFE_NO_PAD.encode(kp.signing_key.to_bytes());
        let json = serde_json::json!({
            "seed": seed_b64,
            "public_key": kp.public_key_base64(),
        });
        fs::write(
            &resolved,
            serde_json::to_string_pretty(&json).expect("json serialization"),
        )
        .map_err(|e| ClientError::Crypto(e.to_string()))?;
        Ok(kp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_deterministic() {
        let kp = ClientKeypair::generate();
        let msg = b"hello crdt";
        let sig1 = kp.sign(msg);
        let sig2 = kp.sign(msg);
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn base64_roundtrip() {
        let kp = ClientKeypair::generate();
        let seed_b64 = URL_SAFE_NO_PAD.encode(kp.signing_key.to_bytes());
        let kp2 = ClientKeypair::from_base64(&seed_b64).unwrap();
        assert_eq!(kp.public_key_bytes, kp2.public_key_bytes);
        assert_eq!(kp.sign(b"test"), kp2.sign(b"test"));
    }

    #[test]
    fn different_keypairs_produce_different_sigs() {
        let kp1 = ClientKeypair::generate();
        let kp2 = ClientKeypair::generate();
        assert_ne!(kp1.sign(b"msg"), kp2.sign(b"msg"));
    }
}
