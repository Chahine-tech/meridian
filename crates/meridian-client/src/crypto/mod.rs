#[cfg(feature = "crypto")]
pub mod aes_gcm;
#[cfg(feature = "crypto")]
pub mod ed25519;

#[cfg(feature = "crypto")]
pub use aes_gcm::{AesGcmKey, EncryptedValue};
#[cfg(feature = "crypto")]
pub use ed25519::ClientKeypair;
