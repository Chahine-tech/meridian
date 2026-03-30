use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    auth::ClaimsExt,
    api::ws::protocol::ServerMsg,
};

use super::AppStateExt;

// GET /v1/namespaces/:ns/crdts/:id/events

/// Server-Sent Events stream — delivers base64-encoded msgpack deltas for a
/// single CRDT to stateless clients (serverless agents, Lambda, CF Workers).
///
/// Each event is:
///   data: <base64-encoded msgpack delta>\n\n
///
/// The client decodes the base64, then decodes the msgpack delta as usual.
/// Keep-alive pings are sent every 15s to prevent proxy timeouts.
pub async fn get_sse<S: AppStateExt>(
    Path((ns, id)): Path<(String, String)>,
    ClaimsExt(claims): ClaimsExt,
    State(state): State<S>,
) -> Response {
    if claims.namespace != ns || !claims.can_read_key(&id) {
        return StatusCode::FORBIDDEN.into_response();
    }

    let rx = state.subscriptions().subscribe(&ns);
    let crdt_id = Arc::new(id);

    let stream = BroadcastStream::new(rx).filter_map(move |msg| {
        let crdt_id = Arc::clone(&crdt_id);
        match msg {
            Ok(server_msg) => {
                if let ServerMsg::Delta { crdt_id: ref cid, ref delta_bytes } = *server_msg
                    && cid.as_str() == crdt_id.as_str()
                {
                    let encoded = STANDARD.encode(delta_bytes.as_ref());
                    return Some(Ok::<_, std::convert::Infallible>(
                        Event::default().data(encoded),
                    ));
                }
                None
            }
            // Lagged — receiver fell too far behind, skip silently
            Err(_) => None,
        }
    });

    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(15)))
        .into_response()
}
