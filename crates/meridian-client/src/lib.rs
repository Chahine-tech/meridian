//! Meridian Rust client SDK.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use meridian_client::MeridianClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = MeridianClient::connect("ws://localhost:3000", "game-room", "my-token").await?;
//!
//!     let score = client.gcounter("gc:score");
//!     score.increment(10).await?;
//!     println!("score: {}", score.value());
//!
//!     client.close().await;
//!     Ok(())
//! }
//! ```

pub mod codec;
pub mod error;
pub mod op_queue;
pub mod transport;

mod client;
mod handles;

pub use client::{LiveQueryResult, MeridianClient};
pub use error::ClientError;
pub use handles::{
    AwarenessHandle, GCounterHandle, LwwRegisterHandle, ORSetHandle, PNCounterHandle,
    PresenceHandle, RgaHandle, TreeHandle,
};
pub use transport::{ConnectionState, Transport};
