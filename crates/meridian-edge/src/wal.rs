/// WAL entry — mirrors `meridian_storage::WalEntry` but redefined here
/// because `meridian-storage` has native deps (sled, sqlx, tokio) that are
/// incompatible with the `wasm32-unknown-unknown` target.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WalEntry {
    pub seq: u64,
    pub crdt_id: String,
    /// msgpack-encoded `CrdtOp` — opaque to the WAL layer.
    #[serde(with = "serde_bytes")]
    pub op_bytes: Vec<u8>,
    pub timestamp_ms: u64,
}
