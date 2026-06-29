use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, AeadCore, KeyInit, OsRng},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ClientError;

/// Wire envelope for an AES-GCM-256 encrypted JSON value.
/// Compatible with the TypeScript and Python SDKs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedValue {
    #[serde(rename = "$e")]
    pub version: u8,
    /// Base64url-encoded 12-byte nonce.
    pub n: String,
    /// Base64url-encoded ciphertext + 16-byte GCM tag.
    pub d: String,
}

pub fn is_encrypted_value(v: &Value) -> bool {
    v.get("$e").and_then(Value::as_u64) == Some(1)
}

/// AES-GCM-256 symmetric key for CRDT value encryption.
#[derive(Clone)]
pub struct AesGcmKey(aes_gcm::Key<Aes256Gcm>);

impl AesGcmKey {
    /// Generate a fresh random 256-bit key.
    pub fn generate() -> Self {
        Self(Aes256Gcm::generate_key(OsRng))
    }

    /// Import from a base64url-encoded 32-byte key.
    pub fn from_base64(s: &str) -> Result<Self, ClientError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|e| ClientError::Crypto(e.to_string()))?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| ClientError::Crypto("AES key must be exactly 32 bytes".into()))?;
        Ok(Self(arr.into()))
    }

    /// Export as base64url (for key distribution).
    pub fn to_base64(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0.as_slice())
    }

    /// Encrypt any JSON-serializable value. Returns an `EncryptedValue` envelope.
    pub fn encrypt(&self, value: &Value) -> Result<EncryptedValue, ClientError> {
        let plaintext = serde_json::to_vec(value).map_err(ClientError::Json)?;
        let cipher = Aes256Gcm::new(&self.0);
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = cipher
            .encrypt(&nonce, plaintext.as_ref())
            .map_err(|e| ClientError::Crypto(e.to_string()))?;
        Ok(EncryptedValue {
            version: 1,
            n: URL_SAFE_NO_PAD.encode(nonce.as_slice()),
            d: URL_SAFE_NO_PAD.encode(&ciphertext),
        })
    }

    /// Decrypt an `EncryptedValue` and return the original JSON value.
    pub fn decrypt(&self, enc: &EncryptedValue) -> Result<Value, ClientError> {
        let nonce_bytes = URL_SAFE_NO_PAD
            .decode(&enc.n)
            .map_err(|e| ClientError::Crypto(e.to_string()))?;
        let ct_bytes = URL_SAFE_NO_PAD
            .decode(&enc.d)
            .map_err(|e| ClientError::Crypto(e.to_string()))?;
        let nonce = Nonce::from_slice(&nonce_bytes);
        let cipher = Aes256Gcm::new(&self.0);
        let plaintext = cipher
            .decrypt(nonce, ct_bytes.as_ref())
            .map_err(|e| ClientError::Crypto(e.to_string()))?;
        serde_json::from_slice(&plaintext).map_err(ClientError::Json)
    }

    /// Decrypt a raw `Value` that may be an encrypted envelope.
    /// Returns the value unchanged if it is not an encrypted envelope.
    pub fn decrypt_value(&self, v: &Value) -> Result<Value, ClientError> {
        if !is_encrypted_value(v) {
            return Ok(v.clone());
        }
        let enc: EncryptedValue = serde_json::from_value(v.clone()).map_err(ClientError::Json)?;
        self.decrypt(&enc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip() {
        let key = AesGcmKey::generate();
        let val = json!({"hello": "world", "n": 42});
        let enc = key.encrypt(&val).unwrap();
        assert_eq!(enc.version, 1);
        let dec = key.decrypt(&enc).unwrap();
        assert_eq!(dec, val);
    }

    #[test]
    fn wrong_key_fails() {
        let k1 = AesGcmKey::generate();
        let k2 = AesGcmKey::generate();
        let enc = k1.encrypt(&json!("secret")).unwrap();
        assert!(k2.decrypt(&enc).is_err());
    }

    #[test]
    fn base64_import_export() {
        let key = AesGcmKey::generate();
        let b64 = key.to_base64();
        let key2 = AesGcmKey::from_base64(&b64).unwrap();
        let val = json!(42);
        let enc = key.encrypt(&val).unwrap();
        assert_eq!(key2.decrypt(&enc).unwrap(), val);
    }

    #[test]
    fn decrypt_value_passthrough() {
        let key = AesGcmKey::generate();
        let plain = json!("not encrypted");
        assert_eq!(key.decrypt_value(&plain).unwrap(), plain);
    }
}
