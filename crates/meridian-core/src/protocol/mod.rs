use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

/// Content filter for a live query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveQueryFilter {
    /// For ORSet: only include CRDTs whose element set contains this JSON value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contains: Option<serde_json::Value>,
    /// For LwwRegister: only include registers updated after this timestamp (ms).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_after: Option<u64>,
}

/// A live query specification carried in `ClientMsg::SubscribeQuery`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveQueryPayload {
    /// Glob pattern matched against CRDT IDs in the namespace.
    pub from: String,
    /// Optional explicit CRDT type filter.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub crdt_type: Option<String>,
    /// Aggregation function name (e.g. `"sum"`, `"union"`, `"latest"`).
    pub aggregate: String,
    /// Optional content filter applied before aggregation.
    #[serde(rename = "where", default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<LiveQueryFilter>,
}

/// A single operation within a `BatchOp`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchItem {
    pub crdt_id: String,
    /// msgpack-encoded `CrdtOp`.
    pub op_bytes: ByteBuf,
    /// Optional TTL in milliseconds for this specific CRDT entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    /// 64-byte Ed25519 signature over `op_bytes`. Present when BFT signing is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sig: Option<ByteBuf>,
}

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
    /// `client_seq` — optional monotonic counter set by the client so the
    /// server can echo it back in the `Ack`, enabling round-trip latency
    /// measurement without clock synchronisation.
    Op {
        crdt_id: String,
        op_bytes: ByteBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_seq: Option<u64>,
        /// 64-byte Ed25519 signature over `op_bytes`. Present when BFT signing is enabled.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sig: Option<ByteBuf>,
    },

    /// Apply multiple operations atomically.
    ///
    /// All ops are validated and applied in order, fanned out together, and
    /// written as a single WAL entry. If any op fails validation (clock drift,
    /// type mismatch, etc.) the entire batch is rejected — no partial state
    /// is persisted or broadcast.
    ///
    /// `client_seq` — echoed back in `BatchAck` for latency tracking.
    BatchOp {
        ops: Vec<BatchItem>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_seq: Option<u64>,
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

    /// Subscribe to a live cross-CRDT query. The server will execute the query
    /// immediately and re-execute it whenever a matching CRDT changes, pushing
    /// `ServerMsg::QueryResult` frames to this connection.
    ///
    /// `query_id` — client-assigned identifier; echoed back in each `QueryResult`.
    SubscribeQuery {
        query_id: String,
        query: LiveQueryPayload,
    },

    /// Cancel a previously registered live query subscription.
    UnsubscribeQuery { query_id: String },

    /// Undo a specific `LwwRegister::Set` op, validated server-side.
    ///
    /// The server checks whether `target_hlc` is still the current winning entry
    /// before applying the restore. If another client has since overwritten the
    /// register, the undo is skipped and `ServerMsg::UndoSkipped` is returned.
    ///
    /// Other CRDT types (ORSet, RGA, Tree, PNCounter) are undone by the client
    /// sending the corresponding inverse op as a normal `Op` message — no special
    /// server support is needed because their inverse ops are always CRDT-safe.
    ///
    /// `target_hlc`    — msgpack-encoded `HybridLogicalClock` of the Set op to undo.
    /// `restore_entry` — msgpack-encoded `Option<LwwEntry>`: the entry to restore
    ///                   (`None` = clear the register to empty).
    UndoLww {
        crdt_id: String,
        target_hlc: ByteBuf,
        restore_entry: ByteBuf,
    },
}

/// Describes why a client op was superseded or structurally rejected.
/// Carried in [`ServerMsg::Conflict`] and sent only to the originating connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConflictKind {
    /// A `LwwRegister::Set` was ignored because a concurrent write from another
    /// client with a higher timestamp was already applied.
    LwwOverwritten {
        /// `client_id` of the write that won.
        winning_client_id: u64,
        /// Wall-clock ms of the winning write.
        winning_ts_ms: u64,
    },
    /// A `Tree::MoveNode` was discarded to prevent a cycle.
    TreeMoveCycle {
        /// HLC string (`wall_ms:logical:node_id`) of the node that could not move.
        node_id: String,
        /// The parent it tried to move under (`None` = root level).
        attempted_parent_id: Option<String>,
    },
}

/// Messages sent by the server over the WebSocket connection.
/// Encoded as msgpack binary frames.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMsg {
    /// A CRDT delta for a subscribed CRDT.
    /// `delta_bytes` is msgpack-encoded delta type for the CRDT kind.
    Delta {
        crdt_id: String,
        delta_bytes: ByteBuf,
    },

    /// Acknowledgement of a successfully applied `Op`.
    /// `seq` is the WAL sequence number assigned by the server.
    /// `client_seq` is echoed back from the client's `Op.client_seq` if present,
    /// allowing the client to correlate acks with sent ops for latency tracking.
    Ack {
        seq: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_seq: Option<u64>,
    },

    /// Acknowledgement of a successfully applied `BatchOp`.
    /// `seq` — WAL sequence number of the batch entry.
    /// `count` — number of ops in the batch that produced a delta.
    /// `client_seq` — echoed back from `BatchOp.client_seq`.
    BatchAck {
        seq: u64,
        count: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_seq: Option<u64>,
    },

    /// Error response. `code` mirrors HTTP status codes where applicable.
    Error { code: u16, message: String },

    /// Fan-out of an [`ClientMsg::AwarenessUpdate`] to all other subscribers
    /// in the same namespace.  Not persisted; fire-and-forget.
    ///
    /// `client_id` — the sender's client ID (from their token).
    /// `key`       — the awareness channel name forwarded verbatim.
    /// `data`      — the raw payload forwarded verbatim.
    AwarenessBroadcast {
        client_id: u64,
        key: String,
        data: ByteBuf,
    },

    /// Result of a live query execution, pushed whenever the underlying data changes.
    ///
    /// `query_id` — echoed from `ClientMsg::SubscribeQuery`.
    /// `value`    — aggregated JSON result (same shape as the HTTP query endpoint).
    /// `matched`  — number of CRDTs that contributed to this result.
    QueryResult {
        query_id: String,
        value: serde_json::Value,
        matched: usize,
    },

    /// Sent privately to the connection whose op was superseded by a concurrent
    /// op or structurally rejected (e.g. cycle prevention in Tree).
    /// Never broadcast to other clients in the namespace.
    Conflict {
        crdt_id: String,
        kind: ConflictKind,
    },

    /// Confirms that a `ClientMsg::UndoLww` was successfully applied and the
    /// restore delta has been broadcast to all subscribers.
    UndoAck { crdt_id: String },

    /// Reports that a `ClientMsg::UndoLww` was skipped because the target entry
    /// has already been overwritten by a concurrent write from another client.
    /// This is not an error — it is the intended CRDT-safe behaviour.
    UndoSkipped { crdt_id: String, reason: String },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_msg_subscribe_roundtrip() {
        let msg = ClientMsg::Subscribe {
            crdt_id: "counter".into(),
        };
        let bytes = msg.to_msgpack().unwrap();
        let decoded = ClientMsg::from_msgpack(&bytes).unwrap();
        assert!(matches!(decoded, ClientMsg::Subscribe { crdt_id } if crdt_id == "counter"));
    }

    #[test]
    fn client_msg_op_roundtrip() {
        let msg = ClientMsg::Op {
            crdt_id: "set".into(),
            op_bytes: ByteBuf::from(vec![1, 2, 3]),
            ttl_ms: None,
            client_seq: None,
            sig: None,
        };
        let bytes = msg.to_msgpack().unwrap();
        let decoded = ClientMsg::from_msgpack(&bytes).unwrap();
        assert!(
            matches!(decoded, ClientMsg::Op { op_bytes, .. } if op_bytes.as_ref() == [1, 2, 3])
        );
    }

    #[test]
    fn server_msg_ack_roundtrip() {
        let msg = ServerMsg::Ack {
            seq: 99,
            client_seq: Some(7),
        };
        let bytes = msg.to_msgpack().unwrap();
        let decoded = ServerMsg::from_msgpack(&bytes).unwrap();
        assert!(matches!(
            decoded,
            ServerMsg::Ack {
                seq: 99,
                client_seq: Some(7)
            }
        ));
    }

    #[test]
    fn server_msg_ack_no_client_seq_roundtrip() {
        let msg = ServerMsg::Ack {
            seq: 42,
            client_seq: None,
        };
        let bytes = msg.to_msgpack().unwrap();
        let decoded = ServerMsg::from_msgpack(&bytes).unwrap();
        assert!(matches!(
            decoded,
            ServerMsg::Ack {
                seq: 42,
                client_seq: None
            }
        ));
    }

    #[test]
    fn client_msg_op_with_client_seq_roundtrip() {
        let msg = ClientMsg::Op {
            crdt_id: "counter".into(),
            op_bytes: ByteBuf::from(vec![1, 2, 3]),
            ttl_ms: None,
            client_seq: Some(5),
            sig: None,
        };
        let bytes = msg.to_msgpack().unwrap();
        let decoded = ClientMsg::from_msgpack(&bytes).unwrap();
        assert!(matches!(
            decoded,
            ClientMsg::Op {
                client_seq: Some(5),
                ..
            }
        ));
    }

    #[test]
    fn server_msg_delta_roundtrip() {
        let msg = ServerMsg::Delta {
            crdt_id: "x".into(),
            delta_bytes: ByteBuf::from(vec![0xde, 0xad]),
        };
        let bytes = msg.to_msgpack().unwrap();
        let decoded = ServerMsg::from_msgpack(&bytes).unwrap();
        assert!(matches!(decoded, ServerMsg::Delta { crdt_id, delta_bytes }
                if crdt_id == "x" && delta_bytes.as_ref() == [0xde, 0xad]));
    }

    #[test]
    fn server_msg_error_roundtrip() {
        let msg = ServerMsg::Error {
            code: 403,
            message: "forbidden".into(),
        };
        let bytes = msg.to_msgpack().unwrap();
        let decoded = ServerMsg::from_msgpack(&bytes).unwrap();
        assert!(matches!(decoded, ServerMsg::Error { code: 403, .. }));
    }
}
