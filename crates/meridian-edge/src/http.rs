use meridian_core::{
    auth::{Permissions, TokenClaims, TokenSigner},
    namespace::NamespaceId,
};
use serde::Deserialize;
use worker::{Request, Response, RouteContext};

use crate::auth;

// ---------------------------------------------------------------------------
// GET /v1/namespaces/:ns/crdts/:id
// ---------------------------------------------------------------------------

pub async fn get_crdt(req: Request, ctx: RouteContext<()>) -> worker::Result<Response> {
    let ns = ctx.param("ns").cloned().unwrap_or_default();
    let id = ctx.param("id").cloned().unwrap_or_default();

    let claims = match auth::validate(&req, &ctx.env) {
        Ok(c) => c,
        Err(e) => return Ok(auth::auth_error_response(&e)),
    };

    if claims.namespace != ns || !claims.can_read_key(&id) {
        return Response::error("forbidden", 403);
    }

    let do_stub = ctx
        .env
        .durable_object("NS_OBJECT")?
        .id_from_name(&ns)?
        .get_stub()?;

    let do_url = format!("http://do/{ns}/get/{id}");
    let do_req = Request::new(&do_url, worker::Method::Get)?;
    do_stub.fetch_with_request(do_req).await
}

// ---------------------------------------------------------------------------
// POST /v1/namespaces/:ns/crdts/:id/ops
// ---------------------------------------------------------------------------

pub async fn post_op(mut req: Request, ctx: RouteContext<()>) -> worker::Result<Response> {
    let ns = ctx.param("ns").cloned().unwrap_or_default();
    let id = ctx.param("id").cloned().unwrap_or_default();

    let claims = match auth::validate(&req, &ctx.env) {
        Ok(c) => c,
        Err(e) => return Ok(auth::auth_error_response(&e)),
    };

    if claims.namespace != ns || !claims.can_write_key(&id) {
        return Response::error("forbidden", 403);
    }

    // Parse optional ttl_ms from query string
    let ttl_ms: Option<u64> = req
        .url()?
        .query_pairs()
        .find(|(k, _)| k == "ttl_ms")
        .and_then(|(_, v)| v.parse().ok());

    let body = req.bytes().await?;

    let mut do_url = format!("http://do/{ns}/op?crdt_id={id}");
    if let Some(ms) = ttl_ms {
        do_url.push_str(&format!("&ttl_ms={ms}"));
    }

    let do_req = Request::new_with_init(
        &do_url,
        worker::RequestInit::new()
            .with_method(worker::Method::Post)
            .with_body(Some(body.into())),
    )?;

    let do_stub = ctx
        .env
        .durable_object("NS_OBJECT")?
        .id_from_name(&ns)?
        .get_stub()?;

    do_stub.fetch_with_request(do_req).await
}

// ---------------------------------------------------------------------------
// POST /v1/namespaces/:ns/tokens  (admin only)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct IssueTokenBody {
    client_id: u64,
    ttl_ms: u64,
    permissions: Permissions,
}

// ---------------------------------------------------------------------------
// GET /v1/namespaces/:ns/wal?from_seq=0[&until_ms=...]
// ---------------------------------------------------------------------------

pub async fn get_wal(req: Request, ctx: RouteContext<()>) -> worker::Result<Response> {
    let ns = ctx.param("ns").cloned().unwrap_or_default();

    let claims = match auth::validate(&req, &ctx.env) {
        Ok(c) => c,
        Err(e) => return Ok(auth::auth_error_response(&e)),
    };

    if claims.namespace != ns || !claims.is_admin() {
        return Response::error("forbidden: admin permission required", 403);
    }

    let url = req.url()?;
    let query = url.query().unwrap_or_default().to_owned();

    let do_url = format!("http://do/{ns}/wal?{query}");
    let do_req = Request::new(&do_url, worker::Method::Get)?;

    let do_stub = ctx
        .env
        .durable_object("NS_OBJECT")?
        .id_from_name(&ns)?
        .get_stub()?;

    do_stub.fetch_with_request(do_req).await
}

// ---------------------------------------------------------------------------
// POST /v1/namespaces/:ns/tokens  (admin only)
// ---------------------------------------------------------------------------

pub async fn issue_token(mut req: Request, ctx: RouteContext<()>) -> worker::Result<Response> {
    let ns = ctx.param("ns").cloned().unwrap_or_default();

    let claims = match auth::validate(&req, &ctx.env) {
        Ok(c) => c,
        Err(e) => return Ok(auth::auth_error_response(&e)),
    };

    if claims.namespace != ns || !claims.is_admin() {
        return Response::error("forbidden: admin permission required", 403);
    }

    if NamespaceId::new(&ns).is_err() {
        return Response::error("invalid namespace", 400);
    }

    let body: IssueTokenBody = match req.json().await {
        Ok(b) => b,
        Err(_) => return Response::error("invalid request body", 400),
    };

    let signing_key = ctx
        .env
        .secret("MERIDIAN_SIGNING_KEY")
        .map_err(|_| worker::Error::RustError("MERIDIAN_SIGNING_KEY not set".into()))?
        .to_string();

    let signer = TokenSigner::from_hex(&signing_key)
        .map_err(|e| worker::Error::RustError(e.to_string()))?;

    let new_claims = TokenClaims::new(&ns, body.client_id, body.ttl_ms, body.permissions);
    let token = signer
        .sign(&new_claims)
        .map_err(|e| worker::Error::RustError(e.to_string()))?;

    Response::from_json(&serde_json::json!({ "token": token }))
}
