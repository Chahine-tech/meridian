use std::time::Instant;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use meridian_core::query::{
    execute_query_on_values, AggregateOp, QueryError, WhereClause,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use tracing::instrument;

use crate::{
    api::handlers::AppStateExt,
    auth::ClaimsExt,
    storage::CrdtStore,
};

// Request / response types (HTTP layer only)

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub from: String,
    #[serde(rename = "type", default)]
    pub crdt_type: Option<String>,
    pub aggregate: AggregateOp,
    #[serde(rename = "where", default)]
    pub filter: Option<WhereClause>,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub value: JsonValue,
    pub matched: usize,
    pub scanned: usize,
    pub execution_ms: u64,
}

// Storage-aware execute_query (wraps the pure core fn)

pub use meridian_core::query::QueryOutcome;

pub enum ExecuteQueryError {
    UnknownCrdtType(String),
    IncompatibleAggregate { aggregate: String, crdt_type: String },
    Storage(String),
}

pub async fn execute_query<S: CrdtStore>(
    store: &S,
    ns: &str,
    from: &str,
    crdt_type_str: Option<&str>,
    aggregate_op: AggregateOp,
    filter: Option<&WhereClause>,
) -> Result<QueryOutcome, ExecuteQueryError> {
    let pairs = store
        .scan_prefix(&format!("{ns}/"))
        .await
        .map_err(|e| ExecuteQueryError::Storage(e.to_string()))?;

    // Strip the namespace prefix from keys so the core fn receives bare crdt_ids.
    let bare_pairs: Vec<(String, _)> = pairs
        .into_iter()
        .map(|(key, val)| (key.trim_start_matches(&format!("{ns}/")).to_owned(), val))
        .collect();

    execute_query_on_values(bare_pairs, from, crdt_type_str, aggregate_op, filter)
        .map_err(|e| match e {
            QueryError::UnknownCrdtType(s) => ExecuteQueryError::UnknownCrdtType(s),
            QueryError::IncompatibleAggregate { aggregate, crdt_type } => {
                ExecuteQueryError::IncompatibleAggregate { aggregate, crdt_type }
            }
        })
}

// HTTP handler

#[instrument(skip(state, claims, body))]
pub async fn post_query<S: AppStateExt>(
    Path((ns,)): Path<(String,)>,
    ClaimsExt(claims): ClaimsExt,
    State(state): State<S>,
    Json(body): Json<QueryRequest>,
) -> Response {
    if claims.namespace != ns {
        return StatusCode::FORBIDDEN.into_response();
    }

    let start = Instant::now();

    match execute_query(
        state.store(),
        &ns,
        &body.from,
        body.crdt_type.as_deref(),
        body.aggregate,
        body.filter.as_ref(),
    )
    .await
    {
        Ok(QueryOutcome { value, matched, scanned }) => {
            let execution_ms = start.elapsed().as_millis() as u64;
            Json(QueryResponse { value, matched, scanned, execution_ms }).into_response()
        }
        Err(ExecuteQueryError::UnknownCrdtType(s)) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "unknown_crdt_type", "detail": format!("unknown crdt type: {s}") })),
        )
            .into_response(),
        Err(ExecuteQueryError::IncompatibleAggregate { aggregate, crdt_type }) => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "incompatible_aggregate",
                "detail": format!("aggregate '{aggregate}' is not supported for crdt type '{crdt_type}'")
            })),
        )
            .into_response(),
        Err(ExecuteQueryError::Storage(e)) => {
            tracing::error!(error = %e, "store.scan_prefix failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
