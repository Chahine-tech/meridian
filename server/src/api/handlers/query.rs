use std::{collections::HashSet, time::Instant};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use tracing::instrument;

use crate::{
    api::handlers::AppStateExt,
    auth::{glob_match, ClaimsExt},
    crdt::{registry::CrdtType, Crdt, CrdtValue},
    storage::Store,
};

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// Glob pattern matched against the CRDT ID (the part after `{ns}/`).
    /// Examples: `"gc:views-*"`, `"or:cart-*"`, `"*"`.
    pub from: String,

    /// Optional CRDT type filter. When absent, inferred from `from` prefix
    /// (e.g. `"gc:"` → `gcounter`). When inference fails, all types are scanned.
    #[serde(rename = "type", default)]
    pub crdt_type: Option<String>,

    /// Aggregation function. Must be compatible with the matched CRDT types.
    pub aggregate: AggregateOp,

    /// Optional filter applied after deserialization, before aggregation.
    #[serde(rename = "where", default)]
    pub filter: Option<WhereClause>,
}

#[derive(Debug, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub enum AggregateOp {
    Sum,
    Max,
    Min,
    Count,
    Union,
    Intersection,
    Latest,
    Collect,
    Merge,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WhereClause {
    /// For ORSet: only include CRDTs whose element set contains this JSON value.
    pub contains: Option<JsonValue>,
    /// For LwwRegister: only include registers updated after this timestamp (ms).
    pub updated_after: Option<u64>,
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    /// Aggregated result value. Shape depends on `aggregate`.
    pub value: JsonValue,
    /// Number of CRDTs that passed glob + type + where filters.
    pub matched: usize,
    /// Total number of CRDTs scanned in the namespace.
    pub scanned: usize,
    /// Wall-clock execution time in milliseconds.
    pub execution_ms: u64,
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

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

    // Resolve explicit type filter, falling back to inference from the glob prefix.
    let type_filter: Option<CrdtType> = match body.crdt_type.as_deref() {
        Some(s) => match s.parse::<CrdtType>() {
            Ok(t) => Some(t),
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "unknown_crdt_type", "detail": format!("unknown crdt type: {s}") })),
                )
                    .into_response();
            }
        },
        None => infer_crdt_type(&body.from),
    };

    let start = Instant::now();

    // Scan the entire namespace.
    let pairs = match state.store().scan_prefix(&format!("{ns}/")).await {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, "store.scan_prefix failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let scanned = pairs.len();

    // Phase 2 short-circuit: for aggregate=count with no content filter we only
    // need to count matching keys — no deserialization needed.
    if body.aggregate == AggregateOp::Count && body.filter.is_none() {
        let matched = pairs
            .iter()
            .filter(|(key, crdt)| {
                let crdt_id = key.trim_start_matches(&format!("{ns}/"));
                glob_match(&body.from, crdt_id)
                    && type_filter.is_none_or(|t| crdt.crdt_type() == t)
            })
            .count();

        let execution_ms = start.elapsed().as_millis() as u64;
        return Json(QueryResponse {
            value: json!(matched),
            matched,
            scanned,
            execution_ms,
        })
        .into_response();
    }

    // Full path: filter then aggregate.
    let filtered: Vec<CrdtValue> = pairs
        .into_iter()
        .filter_map(|(key, crdt)| {
            let crdt_id = key.trim_start_matches(&format!("{ns}/"));
            if !glob_match(&body.from, crdt_id) {
                return None;
            }
            if type_filter.is_some_and(|t| crdt.crdt_type() != t) {
                return None;
            }
            if body.filter.as_ref().is_some_and(|clause| !apply_where(&crdt, clause)) {
                return None;
            }
            Some(crdt)
        })
        .collect();

    let matched = filtered.len();

    let value = if matched == 0 {
        JsonValue::Null
    } else {
        match aggregate(filtered, body.aggregate) {
            Ok(v) => v,
            Err(QueryError::IncompatibleAggregate { aggregate, crdt_type }) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": "incompatible_aggregate",
                        "detail": format!("aggregate '{aggregate}' is not supported for crdt type '{crdt_type}'")
                    })),
                )
                    .into_response();
            }
        }
    };

    let execution_ms = start.elapsed().as_millis() as u64;
    Json(QueryResponse { value, matched, scanned, execution_ms }).into_response()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Infer the CRDT type from the glob pattern prefix, e.g. `"gc:*"` → `GCounter`.
fn infer_crdt_type(from: &str) -> Option<CrdtType> {
    const PREFIXES: &[(&str, CrdtType)] = &[
        ("gc:", CrdtType::GCounter),
        ("pn:", CrdtType::PNCounter),
        ("or:", CrdtType::ORSet),
        ("lw:", CrdtType::LwwRegister),
        ("pr:", CrdtType::Presence),
        ("cm:", CrdtType::CRDTMap),
        ("rga:", CrdtType::Rga),
        ("tree:", CrdtType::Tree),
    ];
    for (prefix, crdt_type) in PREFIXES {
        if from.starts_with(prefix) {
            return Some(*crdt_type);
        }
    }
    None
}

/// Apply the `where` clause to a single `CrdtValue`.
#[allow(clippy::collapsible_if)]
fn apply_where(value: &CrdtValue, clause: &WhereClause) -> bool {
    if let Some(needle) = &clause.contains {
        if let CrdtValue::ORSet(set) = value {
            let needle_key = serde_json::to_string(needle).unwrap_or_default();
            if !set.entries.contains_key(&needle_key) {
                return false;
            }
        }
    }
    if let Some(after_ms) = clause.updated_after {
        if let CrdtValue::LwwRegister(reg) = value {
            let updated_ms = reg.entry.as_ref().map(|e| e.hlc.wall_ms).unwrap_or(0);
            if updated_ms <= after_ms {
                return false;
            }
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

enum QueryError {
    IncompatibleAggregate { aggregate: String, crdt_type: String },
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

fn aggregate(values: Vec<CrdtValue>, op: AggregateOp) -> Result<JsonValue, QueryError> {
    // Determine the first CRDT type for error reporting (all should be the same
    // after type-filter, but may be mixed when no filter is applied).
    let first_type = values.first().map(|v| v.crdt_type());

    match op {
        // --- Numeric ---
        AggregateOp::Sum => aggregate_numeric(&values, op, first_type),
        AggregateOp::Max => aggregate_numeric(&values, op, first_type),
        AggregateOp::Min => aggregate_numeric(&values, op, first_type),

        // --- Count (content-based, with filter) ---
        AggregateOp::Count => {
            // When we arrive here the count short-circuit was not taken (filter present).
            // Count ORSet elements across all matched sets.
            if values.iter().all(|v| matches!(v, CrdtValue::ORSet(_))) {
                let total: usize = values
                    .iter()
                    .map(|v| if let CrdtValue::ORSet(s) = v { s.value().elements.len() } else { 0 })
                    .sum();
                return Ok(json!(total));
            }
            if values.iter().all(|v| matches!(v, CrdtValue::Presence(_))) {
                let total: usize = values
                    .iter()
                    .map(|v| {
                        if let CrdtValue::Presence(p) = v {
                            p.value().entries.len()
                        } else {
                            0
                        }
                    })
                    .sum();
                return Ok(json!(total));
            }
            // For GCounter/PNCounter count is just the number of matched CRDTs.
            Ok(json!(values.len()))
        }

        // --- Set ops ---
        AggregateOp::Union => {
            // Works for ORSet and Presence.
            if values.iter().all(|v| matches!(v, CrdtValue::ORSet(_))) {
                let mut seen: HashSet<String> = HashSet::new();
                let mut result: Vec<JsonValue> = Vec::new();
                for v in &values {
                    if let CrdtValue::ORSet(s) = v {
                        for elem in &s.value().elements {
                            let key = serde_json::to_string(elem).unwrap_or_default();
                            if seen.insert(key) {
                                result.push(elem.clone());
                            }
                        }
                    }
                }
                return Ok(json!(result));
            }
            if values.iter().all(|v| matches!(v, CrdtValue::Presence(_))) {
                let mut merged = serde_json::Map::new();
                for v in &values {
                    if let JsonValue::Object(obj) = v.to_json_value()
                        && let Some(JsonValue::Object(e)) = obj.get("entries")
                    {
                        for (k, val) in e {
                            merged.insert(k.clone(), val.clone());
                        }
                    }
                }
                return Ok(json!({ "entries": merged }));
            }
            Err(incompatible("union", first_type))
        }

        AggregateOp::Intersection => {
            if values.is_empty() {
                return Ok(json!([]));
            }
            if values.iter().all(|v| matches!(v, CrdtValue::ORSet(_))) {
                // Start with elements of first set, retain those present in all others.
                let sets: Vec<HashSet<String>> = values
                    .iter()
                    .map(|v| {
                        if let CrdtValue::ORSet(s) = v {
                            s.value()
                                .elements
                                .iter()
                                .map(|e| serde_json::to_string(e).unwrap_or_default())
                                .collect()
                        } else {
                            HashSet::new()
                        }
                    })
                    .collect();

                let first_set = &sets[0];
                let intersection: HashSet<&String> = first_set
                    .iter()
                    .filter(|e| sets[1..].iter().all(|s| s.contains(*e)))
                    .collect();

                // Reconstruct JsonValue elements from the first CRDT for the intersection keys.
                let result: Vec<JsonValue> = if let CrdtValue::ORSet(s) = &values[0] {
                    s.value()
                        .elements
                        .iter()
                        .filter(|e| {
                            let key = serde_json::to_string(e).unwrap_or_default();
                            intersection.contains(&key)
                        })
                        .cloned()
                        .collect()
                } else {
                    vec![]
                };
                return Ok(json!(result));
            }
            Err(incompatible("intersection", first_type))
        }

        // --- LwwRegister ---
        AggregateOp::Latest => {
            if values.iter().all(|v| matches!(v, CrdtValue::LwwRegister(_))) {
                let best = values.iter().max_by_key(|v| {
                    if let CrdtValue::LwwRegister(r) = v {
                        r.value().updated_at_ms.unwrap_or(0)
                    } else {
                        0
                    }
                });
                return Ok(best
                    .and_then(|v| {
                        if let CrdtValue::LwwRegister(r) = v {
                            r.value().value.clone()
                        } else {
                            None
                        }
                    })
                    .unwrap_or(JsonValue::Null));
            }
            Err(incompatible("latest", first_type))
        }

        // --- Collect ---
        AggregateOp::Collect => {
            let result: Vec<JsonValue> = values.iter().map(|v| v.to_json_value()).collect();
            Ok(json!(result))
        }

        // --- CRDTMap merge ---
        AggregateOp::Merge => {
            if values.iter().all(|v| matches!(v, CrdtValue::CRDTMap(_))) {
                let result: Vec<JsonValue> = values.iter().map(|v| v.to_json_value()).collect();
                return Ok(json!(result));
            }
            Err(incompatible("merge", first_type))
        }
    }
}

fn aggregate_numeric(
    values: &[CrdtValue],
    op: AggregateOp,
    first_type: Option<CrdtType>,
) -> Result<JsonValue, QueryError> {
    // GCounter — u64
    if values.iter().all(|v| matches!(v, CrdtValue::GCounter(_))) {
        let nums: Vec<u64> = values
            .iter()
            .map(|v| if let CrdtValue::GCounter(g) = v { g.value().total } else { 0 })
            .collect();
        return Ok(match op {
            AggregateOp::Sum => json!(nums.iter().sum::<u64>()),
            AggregateOp::Max => json!(nums.iter().max().copied().unwrap_or(0)),
            AggregateOp::Min => json!(nums.iter().min().copied().unwrap_or(0)),
            _ => unreachable!(),
        });
    }

    // PNCounter — i64
    if values.iter().all(|v| matches!(v, CrdtValue::PNCounter(_))) {
        let nums: Vec<i64> = values
            .iter()
            .map(|v| if let CrdtValue::PNCounter(p) = v { p.value().value } else { 0 })
            .collect();
        return Ok(match op {
            AggregateOp::Sum => {
                let sum = nums.iter().fold(0i64, |acc, &x| acc.saturating_add(x));
                json!(sum)
            }
            AggregateOp::Max => json!(nums.iter().max().copied().unwrap_or(0)),
            AggregateOp::Min => json!(nums.iter().min().copied().unwrap_or(0)),
            _ => unreachable!(),
        });
    }

    Err(incompatible(
        match op {
            AggregateOp::Sum => "sum",
            AggregateOp::Max => "max",
            AggregateOp::Min => "min",
            _ => "numeric",
        },
        first_type,
    ))
}

fn incompatible(op: &str, crdt_type: Option<CrdtType>) -> QueryError {
    QueryError::IncompatibleAggregate {
        aggregate: op.to_string(),
        crdt_type: crdt_type
            .map(|t: CrdtType| t.as_str().to_string())
            .unwrap_or_else(|| "unknown".to_string()),
    }
}
