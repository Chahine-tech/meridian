use meridian_core::{
    auth::{Permissions, PermissionsV1, PermissionsV2, PermEntry, op_masks, TokenClaims, TokenSigner},
    crdt::registry::CrdtOp,
    namespace::NamespaceId,
};
use serde::Deserialize;
use worker::{Request, Response, RouteContext};

use crate::auth;

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

    // Op-level permission check (V2 tokens).
    if let Ok(op) = rmp_serde::decode::from_slice::<CrdtOp>(&body)
        && !claims.can_write_key_op(&id, op.op_mask())
    {
        return Response::error("op not permitted by token", 403);
    }

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

#[derive(Deserialize)]
struct PermEntryDto {
    p: String,
    o: Option<u16>,
    e: Option<u64>,
}

impl From<PermEntryDto> for PermEntry {
    fn from(dto: PermEntryDto) -> Self {
        Self { p: dto.p, o: dto.o.unwrap_or(op_masks::ALL), e: dto.e }
    }
}

#[derive(Deserialize)]
struct PermissionsV2Dto {
    r: Vec<PermEntryDto>,
    w: Vec<PermEntryDto>,
    #[serde(default)]
    admin: bool,
    rl: Option<u32>,
}

#[derive(Deserialize)]
struct PermissionsV1Dto {
    read: Option<Vec<String>>,
    write: Option<Vec<String>>,
    #[serde(default)]
    admin: bool,
}

#[derive(Deserialize)]
struct IssueTokenBody {
    client_id: u64,
    ttl_ms: Option<u64>,
    /// V1 glob-list permissions.
    permissions: Option<PermissionsV1Dto>,
    /// V2 fine-grained rules — takes precedence over `permissions`.
    rules: Option<PermissionsV2Dto>,
}

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

pub async fn get_sync(req: Request, ctx: RouteContext<()>) -> worker::Result<Response> {
    let ns = ctx.param("ns").cloned().unwrap_or_default();
    let id = ctx.param("id").cloned().unwrap_or_default();

    let claims = match auth::validate(&req, &ctx.env) {
        Ok(c) => c,
        Err(e) => return Ok(auth::auth_error_response(&e)),
    };

    if claims.namespace != ns || !claims.can_read_key(&id) {
        return Response::error("forbidden", 403);
    }

    let url = req.url()?;
    let since = url.query_pairs()
        .find(|(k, _)| k == "since")
        .map(|(_, v)| v.into_owned());

    let mut do_url = format!("http://do/{ns}/sync?crdt_id={id}");
    if let Some(vc) = since {
        do_url.push_str(&format!("&since={vc}"));
    }

    let do_req = Request::new(&do_url, worker::Method::Get)?;
    let do_stub = ctx.env.durable_object("NS_OBJECT")?.id_from_name(&ns)?.get_stub()?;
    do_stub.fetch_with_request(do_req).await
}

// Webhooks (admin only)
// GET  /v1/namespaces/:ns/webhooks
// POST /v1/namespaces/:ns/webhooks
// DELETE /v1/namespaces/:ns/webhooks/:id
async fn webhook_do_stub(
    req: &Request,
    ctx: &RouteContext<()>,
    ns: &str,
) -> worker::Result<Option<Response>> {
    let claims = match crate::auth::validate(req, &ctx.env) {
        Ok(c) => c,
        Err(e) => return Ok(Some(crate::auth::auth_error_response(&e))),
    };
    if claims.namespace != ns || !claims.is_admin() {
        return Ok(Some(Response::error("forbidden: admin permission required", 403)?));
    }
    Ok(None)
}

pub async fn list_webhooks(req: Request, ctx: RouteContext<()>) -> worker::Result<Response> {
    let ns = ctx.param("ns").cloned().unwrap_or_default();
    if let Some(r) = webhook_do_stub(&req, &ctx, &ns).await? { return Ok(r); }

    let do_stub = ctx.env.durable_object("NS_OBJECT")?.id_from_name(&ns)?.get_stub()?;
    let do_req = Request::new(&format!("http://do/{ns}/webhooks"), worker::Method::Get)?;
    do_stub.fetch_with_request(do_req).await
}

pub async fn register_webhook(mut req: Request, ctx: RouteContext<()>) -> worker::Result<Response> {
    let ns = ctx.param("ns").cloned().unwrap_or_default();
    if let Some(r) = webhook_do_stub(&req, &ctx, &ns).await? { return Ok(r); }

    let body = req.bytes().await?;
    let do_stub = ctx.env.durable_object("NS_OBJECT")?.id_from_name(&ns)?.get_stub()?;
    let do_req = Request::new_with_init(
        &format!("http://do/{ns}/webhooks"),
        worker::RequestInit::new()
            .with_method(worker::Method::Post)
            .with_body(Some(body.into())),
    )?;
    do_stub.fetch_with_request(do_req).await
}

pub async fn delete_webhook(req: Request, ctx: RouteContext<()>) -> worker::Result<Response> {
    let ns = ctx.param("ns").cloned().unwrap_or_default();
    let id = ctx.param("id").cloned().unwrap_or_default();
    if let Some(r) = webhook_do_stub(&req, &ctx, &ns).await? { return Ok(r); }

    let do_stub = ctx.env.durable_object("NS_OBJECT")?.id_from_name(&ns)?.get_stub()?;
    let do_req = Request::new(
        &format!("http://do/{ns}/webhooks/{id}"),
        worker::Method::Delete,
    )?;
    do_stub.fetch_with_request(do_req).await
}

pub async fn get_history(req: Request, ctx: RouteContext<()>) -> worker::Result<Response> {
    let ns = ctx.param("ns").cloned().unwrap_or_default();
    let id = ctx.param("id").cloned().unwrap_or_default();

    let claims = match auth::validate(&req, &ctx.env) {
        Ok(c) => c,
        Err(e) => return Ok(auth::auth_error_response(&e)),
    };

    if claims.namespace != ns || !claims.can_read_key(&id) {
        return Response::error("forbidden", 403);
    }

    let url = req.url()?;
    let query = url.query().unwrap_or_default().to_owned();
    // Percent-encode the crdt_id for use in the internal DO URL query param.
    let encoded_id = id.replace('%', "%25").replace('&', "%26").replace('=', "%3D").replace('+', "%2B");
    let do_url = format!("http://do/{ns}/history?crdt_id={encoded_id}&{query}");
    let do_req = Request::new(&do_url, worker::Method::Get)?;

    let do_stub = ctx
        .env
        .durable_object("NS_OBJECT")?
        .id_from_name(&ns)?
        .get_stub()?;

    do_stub.fetch_with_request(do_req).await
}

pub async fn token_me(req: Request, ctx: RouteContext<()>) -> worker::Result<Response> {
    let ns = ctx.param("ns").cloned().unwrap_or_default();

    let claims = match auth::validate(&req, &ctx.env) {
        Ok(c) => c,
        Err(e) => return Ok(auth::auth_error_response(&e)),
    };

    if claims.namespace != ns {
        return Response::error("forbidden: namespace mismatch", 403);
    }

    Response::from_json(&serde_json::json!({
        "namespace":  claims.namespace,
        "client_id":  claims.client_id,
        "expires_at": claims.expires_at,
        "permissions": claims.permissions,
    }))
}

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

    let perms: Permissions = if let Some(v2) = body.rules {
        Permissions::V2(PermissionsV2 {
            v: 2,
            r: v2.r.into_iter().map(PermEntry::from).collect(),
            w: v2.w.into_iter().map(PermEntry::from).collect(),
            admin: v2.admin,
            rl: v2.rl,
        })
    } else if let Some(v1) = body.permissions {
        Permissions::V1(PermissionsV1 {
            read: v1.read.unwrap_or_else(|| vec!["*".into()]),
            write: v1.write.unwrap_or_else(|| vec!["*".into()]),
            admin: v1.admin,
        })
    } else {
        Permissions::read_write()
    };

    let new_claims = TokenClaims::new(&ns, body.client_id, body.ttl_ms.unwrap_or(3_600_000), perms);
    let token = signer
        .sign(&new_claims)
        .map_err(|e| worker::Error::RustError(e.to_string()))?;

    Response::from_json(&serde_json::json!({ "token": token }))
}
