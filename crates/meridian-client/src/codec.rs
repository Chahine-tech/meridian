use meridian_core::protocol::{ClientMsg, ServerMsg};

use crate::error::ClientError;

/// Encode a `ClientMsg` to msgpack bytes for wire transmission.
pub fn encode(msg: &ClientMsg) -> Result<Vec<u8>, ClientError> {
    msg.to_msgpack().map_err(ClientError::Encode)
}

/// Decode a `ServerMsg` from msgpack bytes received from the wire.
pub fn decode(bytes: &[u8]) -> Result<ServerMsg, ClientError> {
    ServerMsg::from_msgpack(bytes).map_err(ClientError::Decode)
}

/// Encode a CRDT op payload to a `serde_bytes::ByteBuf` for embedding in
/// `ClientMsg::Op { op_bytes }`.
pub fn encode_op<O: serde::Serialize>(op: &O) -> Result<serde_bytes::ByteBuf, ClientError> {
    rmp_serde::encode::to_vec_named(op)
        .map(serde_bytes::ByteBuf::from)
        .map_err(ClientError::Encode)
}

/// Decode a CRDT delta payload from a `delta_bytes` field in `ServerMsg::Delta`.
pub fn decode_delta<D: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<D, ClientError> {
    rmp_serde::decode::from_slice(bytes).map_err(ClientError::Decode)
}

#[cfg(test)]
mod tests {
    use super::*;
    use meridian_core::protocol::ClientMsg;

    #[test]
    fn subscribe_roundtrip() {
        let msg = ClientMsg::Subscribe { crdt_id: "gc:score".into() };
        let bytes = encode(&msg).unwrap();
        let decoded = decode(&bytes);
        // ClientMsg and ServerMsg are different enums — encode/decode are directional.
        // Just verify the bytes are non-empty and decode as ServerMsg fails gracefully.
        assert!(!bytes.is_empty());
        // Round-trip through ClientMsg directly
        let back = ClientMsg::from_msgpack(&bytes).unwrap();
        assert!(matches!(back, ClientMsg::Subscribe { crdt_id } if crdt_id == "gc:score"));
        let _ = decoded; // ServerMsg decode will error — that's expected
    }

    #[test]
    fn op_roundtrip() {
        let op_bytes = serde_bytes::ByteBuf::from(vec![0xc3]); // msgpack true
        let msg = ClientMsg::Op {
            crdt_id: "gc:views".into(),
            op_bytes,
            ttl_ms: None,
            client_seq: Some(42),
        };
        let bytes = encode(&msg).unwrap();
        let back = ClientMsg::from_msgpack(&bytes).unwrap();
        assert!(matches!(back, ClientMsg::Op { client_seq: Some(42), .. }));
    }

    #[test]
    fn server_delta_roundtrip() {
        let msg = meridian_core::protocol::ServerMsg::Delta {
            crdt_id: "gc:score".into(),
            delta_bytes: serde_bytes::ByteBuf::from(vec![1, 2, 3]),
        };
        let bytes = msg.to_msgpack().unwrap();
        let back = decode(&bytes).unwrap();
        assert!(matches!(back, ServerMsg::Delta { crdt_id, .. } if crdt_id == "gc:score"));
    }

    #[test]
    fn server_ack_roundtrip() {
        let msg = meridian_core::protocol::ServerMsg::Ack { seq: 99, client_seq: Some(1) };
        let bytes = msg.to_msgpack().unwrap();
        let back = decode(&bytes).unwrap();
        assert!(matches!(back, ServerMsg::Ack { seq: 99, .. }));
    }

    #[test]
    fn server_error_roundtrip() {
        let msg = meridian_core::protocol::ServerMsg::Error {
            code: 401,
            message: "unauthorized".into(),
        };
        let bytes = msg.to_msgpack().unwrap();
        let back = decode(&bytes).unwrap();
        assert!(matches!(back, ServerMsg::Error { code: 401, .. }));
    }
}
