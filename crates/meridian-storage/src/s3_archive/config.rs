/// Configuration for S3-compatible WAL archiving.
///
/// Returns `None` from `from_env()` when `S3_BUCKET` is not set, making the
/// feature purely opt-in with zero behavioral change when absent.
#[derive(Debug, Clone)]
pub struct S3ArchiveConfig {
    pub bucket: String,
    /// Object key prefix (default: `"wal/"`).
    pub key_prefix: String,
    /// AWS region (default: `"us-east-1"`).
    pub region: String,
    /// Custom endpoint URL for S3-compatible stores (Cloudflare R2, MinIO, etc.).
    pub endpoint_url: Option<String>,
    /// Number of WAL entries per segment before triggering an upload (default: 500).
    pub segment_size: usize,
}

impl S3ArchiveConfig {
    /// Build config from environment variables. Returns `None` when `S3_BUCKET` is unset.
    ///
    /// Variables:
    /// - `S3_BUCKET` — bucket name (required to enable archiving)
    /// - `S3_ENDPOINT` — endpoint URL override (for R2/MinIO)
    /// - `S3_REGION` — AWS region (default: `us-east-1`)
    /// - `S3_KEY_PREFIX` — object key prefix (default: `wal/`)
    /// - `WAL_SEGMENT_SIZE` — entries per segment (default: 500)
    pub fn from_env() -> Option<Self> {
        let bucket = std::env::var("S3_BUCKET").ok()?;
        Some(Self {
            bucket,
            key_prefix: std::env::var("S3_KEY_PREFIX").unwrap_or_else(|_| "wal/".into()),
            region: std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
            endpoint_url: std::env::var("S3_ENDPOINT").ok(),
            segment_size: std::env::var("WAL_SEGMENT_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(500),
        })
    }
}
