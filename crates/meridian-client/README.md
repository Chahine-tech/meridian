# meridian-client

Rust async client SDK for [Meridian](../../README.md) — real-time CRDT sync over WebSocket.

## Add to your project

```toml
[dependencies]
meridian-client = "0.1.2"

# Optional features
meridian-client = { version = "0.1.2", features = ["crypto", "http"] }
```

| Feature | Enables |
|---------|---------|
| `ws` *(default)* | WebSocket transport via `tokio-tungstenite` |
| `crypto` | AES-GCM-256 encryption + Ed25519 BFT signing |
| `http` | HTTP client for REST operations (`reqwest`) |

## Quick start

```rust
use meridian_client::MeridianClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = MeridianClient::connect("ws://localhost:3000", "my-room", "your-token").await?;

    let views = client.gcounter("gc:views");
    views.increment(1).await?;
    println!("views: {}", views.value());

    Ok(())
}
```

## CRDT handles

### GCounter

```rust
let views = client.gcounter("gc:views");
views.increment(5).await?;
println!("{}", views.value()); // u64

// Reactive — tokio::watch channel
let mut rx = views.watch();
tokio::spawn(async move {
    while rx.changed().await.is_ok() {
        println!("views: {}", *rx.borrow());
    }
});
```

### PNCounter

```rust
let balance = client.pncounter("pn:balance");
balance.increment(100).await?;
balance.decrement(20).await?;
println!("{}", balance.value()); // i64
```

### LwwRegister

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Profile { name: String, role: String }

let profile = client.lwwregister::<Profile>("lw:profile");
profile.set(Profile { name: "Alice".into(), role: "admin".into() }).await?;
println!("{:?}", profile.value()?); // Option<Profile>
```

### ORSet

```rust
let tags = client.orset::<String>("or:tags");
tags.add("rust".to_string()).await?;
tags.remove("rust".to_string()).await?;
println!("{:?}", tags.elements()); // Vec<String>
```

### Presence

```rust
use serde_json::json;

let room = client.presence::<serde_json::Value>("pr:room-1");
room.heartbeat(json!({"cursor": {"x": 100, "y": 200}}), Some(30_000)).await?;
println!("{:?}", room.online()); // Vec<PresenceEntry<Value>>
```

### RGA (collaborative text)

```rust
let doc = client.rga("rg:doc");
doc.insert(0, 'H').await?;
doc.insert(1, 'i').await?;
println!("{}", doc.text()); // "Hi"
doc.delete(0).await?;
```

### Tree

```rust
let outline = client.tree("tr:outline");
let root_id = outline.add_node(None, "a0", "Introduction").await?;
let child_id = outline.add_node(Some(&root_id), "a0", "Section 1").await?;
outline.update_node(&child_id, "Section 1 — Overview").await?;
outline.move_node(&child_id, Some(&root_id), "b0").await?;
outline.delete_node(&child_id).await?;
```

### Awareness

```rust
let cursors = client.awareness::<serde_json::Value>("cursors");
cursors.update(json!({"x": 42, "y": 100})).await?;
```

## Encryption (`--features crypto`)

```rust
use meridian_client::crypto::AesGcmKey;

let key = AesGcmKey::generate();

// Encrypted LwwRegister — value encrypted before sending, decrypted on receive
let profile = client.lwwregister_encrypted::<Profile>("lw:private", Some(key.clone()), Some(key));
profile.set(Profile { name: "Alice".into(), role: "admin".into() }).await?;
```

Same pattern for `presence_encrypted`.

## BFT signing (`--features crypto`)

Sign all ops with an Ed25519 keypair — the server verifies the signature against the client's registered public key.

```rust
use meridian_client::MeridianClient;
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;

let keypair = SigningKey::generate(&mut OsRng);
let client = MeridianClient::connect_signed(
    "ws://localhost:3000", "my-room", "your-token", keypair
).await?;
```

## HTTP client (`--features http`)

```rust
let http = client.http(); // meridian_client::http::HttpClient

let snapshot = http.get_crdt("my-room", "gc:views").await?;
let claims   = http.token_me("my-room").await?;
let history  = http.get_history("my-room", "gc:views", 0, 50).await?;
```

## Testing with `FakeTransport`

```rust
use meridian_client::{MeridianClient, transport::FakeTransport};

let (transport, _sink) = FakeTransport::new();
let client = MeridianClient::from_transport(transport, "test-ns", 1);

let views = client.gcounter("gc:views");
views.increment(1).await?;
assert_eq!(views.value(), 1);
```

## Development

```bash
cargo test -p meridian-client
cargo clippy -p meridian-client --all-features -- -D warnings
cargo test -p meridian-client --features crypto,http
```
