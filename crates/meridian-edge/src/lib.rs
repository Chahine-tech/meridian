mod auth;
mod durable_object;
mod http;
mod ws;

use worker::{event, Context, Env, Request, Response, Router};

pub use durable_object::NsObject;

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> worker::Result<Response> {
    Router::new()
        // WebSocket connect
        .get_async("/v1/namespaces/:ns/connect", ws::ws_handler)
        // CRDT REST API
        .get_async("/v1/namespaces/:ns/crdts/:id", http::get_crdt)
        .post_async("/v1/namespaces/:ns/crdts/:id/ops", http::post_op)
        // Token issuance (admin only)
        .post_async("/v1/namespaces/:ns/tokens", http::issue_token)
        // Health check
        .get_async("/health", |_, _| async move { Response::ok("ok") })
        .run(req, env)
        .await
}
