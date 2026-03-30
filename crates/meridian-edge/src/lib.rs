mod auth;
mod durable_object;
mod http;
mod wal;
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
        .get_async("/v1/namespaces/:ns/crdts/:id/sync", http::get_sync)
        .get_async("/v1/namespaces/:ns/crdts/:id/history", http::get_history)
        .post_async("/v1/namespaces/:ns/crdts/:id/ops", http::post_op)
        // Query engine — cross-CRDT scan + aggregation
        .post_async("/v1/namespaces/:ns/query", http::post_query)
        // Token issuance + introspection (admin only for issue)
        .post_async("/v1/namespaces/:ns/tokens", http::issue_token)
        .get_async("/v1/namespaces/:ns/tokens/me", http::token_me)
        // WAL replay / audit log
        .get_async("/v1/namespaces/:ns/wal", http::get_wal)
        // Webhooks (admin only)
        .get_async("/v1/namespaces/:ns/webhooks", http::list_webhooks)
        .post_async("/v1/namespaces/:ns/webhooks", http::register_webhook)
        .delete_async("/v1/namespaces/:ns/webhooks/:id", http::delete_webhook)
        // Health check
        .get_async("/health", |_, _| async move { Response::ok("ok") })
        .run(req, env)
        .await
}
