use meridian_core::namespace::NamespaceId;
use worker::{Request, Response, RouteContext};

use crate::auth;

/// WebSocket upgrade handler.
///
/// Route: `GET /v1/namespaces/:ns/connect`
///
/// The upgrade happens here in the main worker. The server-side WebSocket is
/// forwarded to the Durable Object via a fetch with `Upgrade: websocket` so
/// that the DO Hibernation API can manage it.
pub async fn ws_handler(req: Request, ctx: RouteContext<()>) -> worker::Result<Response> {
    let ns = ctx.param("ns").cloned().unwrap_or_default();

    // Validate token
    let claims = match auth::validate(&req, &ctx.env) {
        Ok(c) => c,
        Err(e) => return Ok(auth::auth_error_response(&e)),
    };

    if claims.namespace != ns {
        return Response::error("forbidden: namespace mismatch", 403);
    }

    if NamespaceId::new(&ns).is_err() {
        return Response::error("invalid namespace", 400);
    }

    if !claims.can_read() {
        return Response::error("forbidden: read permission required", 403);
    }

    // Forward the original request (which carries `Upgrade: websocket`) to the
    // Durable Object so its Hibernation API can accept the WebSocket.
    let do_stub = ctx
        .env
        .durable_object("NS_OBJECT")?
        .id_from_name(&ns)?
        .get_stub()?;

    do_stub.fetch_with_request(req).await
}
