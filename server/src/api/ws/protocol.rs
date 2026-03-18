use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

// ---------------------------------------------------------------------------
// Client → Server messages
// ---------------------------------------------------------------------------

/// Messages sent by the client over the WebSocket connection.
/// Encoded as msgpack binary frames.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMsg {
    /// Subscribe to a CRDT's future deltas.
    Subscribe { crdt_id: String },

    /// Apply an operation to a CRDT.
    /// `op_bytes` is msgpack-encoded `CrdtOp`.
    /// `ttl_ms` — optional TTL in milliseconds.  When set, the CRDT entry is
    /// scheduled for deletion by the GC task after `ttl_ms` ms.
    Op {
        crdt_id: String,
        op_bytes: ByteBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },

    /// Request a delta since a vector clock checkpoint.
    /// `since_vc` is msgpack-encoded `VectorClock`.
    Sync { crdt_id: String, since_vc: ByteBuf },

    /// Broadcast an ephemeral awareness update to all other clients in the
    /// same namespace.  The server does **not** persist this — it is a
    /// stateless fan-out only.
    ///
    /// `key`  — logical channel name (e.g. `"cursors"`, `"selection:doc-1"`).
    /// `data` — msgpack-encoded payload; the server forwards it verbatim.
    AwarenessUpdate { key: String, data: ByteBuf },
}

// ---------------------------------------------------------------------------
// Server → Client messages
// ---------------------------------------------------------------------------

/// Messages sent by the server over the WebSocket connection.
/// Encoded as msgpack binary frames.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMsg {
    /// A CRDT delta for a subscribed CRDT.
    /// `delta_bytes` is msgpack-encoded delta type for the CRDT kind.
    Delta { crdt_id: String, delta_bytes: ByteBuf },

    /// Acknowledgement of a successfully applied `Op`.
    Ack { seq: u64 },

    /// Error response. `code` mirrors HTTP status codes where applicable.
    Error { code: u16, message: String },

    /// Fan-out of an [`ClientMsg::AwarenessUpdate`] to all other subscribers
    /// in the same namespace.  Not persisted; fire-and-forget.
    ///
    /// `client_id` — the sender's client ID (from their token).
    /// `key`       — the awareness channel name forwarded verbatim.
    /// `data`      — the raw payload forwarded verbatim.
    AwarenessBroadcast { client_id: u64, key: String, data: ByteBuf },
}

impl ServerMsg {
    /// Encode to msgpack bytes for wire transmission.
    pub fn to_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::encode::to_vec_named(self)
    }

    /// Decode from msgpack bytes.
    pub fn from_msgpack(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::decode::from_slice(bytes)
    }
}

impl ClientMsg {
    pub fn to_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::encode::to_vec_named(self)
    }

    pub fn from_msgpack(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::decode::from_slice(bytes)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_msg_subscribe_roundtrip() {
        let msg = ClientMsg::Subscribe { crdt_id: "counter".into() };
        let bytes = msg.to_msgpack().unwrap();
        let decoded = ClientMsg::from_msgpack(&bytes).unwrap();
        assert!(matches!(decoded, ClientMsg::Subscribe { crdt_id } if crdt_id == "counter"));
    }

    #[test]
    fn client_msg_op_roundtrip() {
        let msg = ClientMsg::Op { crdt_id: "set".into(), op_bytes: ByteBuf::from(vec![1, 2, 3]) };
        let bytes = msg.to_msgpack().unwrap();
        let decoded = ClientMsg::from_msgpack(&bytes).unwrap();
        assert!(matches!(decoded, ClientMsg::Op { op_bytes, .. } if op_bytes.as_ref() == [1, 2, 3]));
    }

    #[test]
    fn server_msg_ack_roundtrip() {
        let msg = ServerMsg::Ack { seq: 99 };
        let bytes = msg.to_msgpack().unwrap();
        let decoded = ServerMsg::from_msgpack(&bytes).unwrap();
        assert!(matches!(decoded, ServerMsg::Ack { seq: 99 }));
    }

    #[test]
    fn server_msg_delta_roundtrip() {
        let msg = ServerMsg::Delta { crdt_id: "x".into(), delta_bytes: ByteBuf::from(vec![0xde, 0xad]) };
        let bytes = msg.to_msgpack().unwrap();
        let decoded = ServerMsg::from_msgpack(&bytes).unwrap();
        assert!(
            matches!(decoded, ServerMsg::Delta { crdt_id, delta_bytes }
                if crdt_id == "x" && delta_bytes.as_ref() == [0xde, 0xad])
        );
    }

    #[test]
    fn server_msg_error_roundtrip() {
        let msg = ServerMsg::Error { code: 403, message: "forbidden".into() };
        let bytes = msg.to_msgpack().unwrap();
        let decoded = ServerMsg::from_msgpack(&bytes).unwrap();
        assert!(matches!(decoded, ServerMsg::Error { code: 403, .. }));
    }
}
