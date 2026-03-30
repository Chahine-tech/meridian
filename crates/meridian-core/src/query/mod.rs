//! Pure query execution logic — no storage, no HTTP, no async.
//!
//! Used by both the native server and the edge worker so the aggregation
//! behaviour is identical on both runtimes.

use std::collections::HashSet;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::auth::glob_match;
use crate::crdt::{Crdt, registry::{CrdtType, CrdtValue}};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Supported aggregation operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

impl FromStr for AggregateOp {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sum"          => Ok(Self::Sum),
            "max"          => Ok(Self::Max),
            "min"          => Ok(Self::Min),
            "count"        => Ok(Self::Count),
            "union"        => Ok(Self::Union),
            "intersection" => Ok(Self::Intersection),
            "latest"       => Ok(Self::Latest),
            "collect"      => Ok(Self::Collect),
            "merge"        => Ok(Self::Merge),
            _              => Err(()),
        }
    }
}

/// Optional content filter applied after glob + type matching.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WhereClause {
    /// For ORSet: only include CRDTs whose element set contains this value.
    pub contains: Option<JsonValue>,
    /// For LwwRegister: only include registers updated after this timestamp (ms).
    pub updated_after: Option<u64>,
}

/// Result of a successful query execution.
pub struct QueryOutcome {
    pub value: JsonValue,
    /// Number of CRDTs that passed all filters.
    pub matched: usize,
    /// Total number of candidate values provided.
    pub scanned: usize,
}

/// Errors returned by [`execute_query_on_values`].
pub enum QueryError {
    UnknownCrdtType(String),
    IncompatibleAggregate { aggregate: String, crdt_type: String },
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Infer the CRDT type from an ID or glob prefix.
///
/// Examples: `"gc:views-1"` → `GCounter`, `"or:cart-*"` → `ORSet`.
/// Returns `None` when the prefix is unknown or the pattern uses a leading `*`.
pub fn infer_crdt_type(from: &str) -> Option<CrdtType> {
    const PREFIXES: &[(&str, CrdtType)] = &[
        ("gc:",   CrdtType::GCounter),
        ("pn:",   CrdtType::PNCounter),
        ("or:",   CrdtType::ORSet),
        ("lw:",   CrdtType::LwwRegister),
        ("pr:",   CrdtType::Presence),
        ("cm:",   CrdtType::CRDTMap),
        ("rga:",  CrdtType::Rga),
        ("tree:", CrdtType::Tree),
    ];
    for (prefix, crdt_type) in PREFIXES {
        if from.starts_with(prefix) {
            return Some(*crdt_type);
        }
    }
    None
}

/// Apply the `where` clause to a single `CrdtValue`. Returns `false` when the
/// value should be excluded.
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
// Core execution — pure, synchronous, no I/O
// ---------------------------------------------------------------------------

/// Execute a query against a pre-fetched list of `(crdt_id, CrdtValue)` pairs.
///
/// `crdt_type_str` — explicit type filter. When `None`, inferred from `from`
/// prefix (e.g. `"gc:"` → `GCounter`). When inference also yields `None`, all
/// types are scanned.
pub fn execute_query_on_values(
    pairs: Vec<(String, CrdtValue)>,
    from: &str,
    crdt_type_str: Option<&str>,
    aggregate_op: AggregateOp,
    filter: Option<&WhereClause>,
) -> Result<QueryOutcome, QueryError> {
    let type_filter: Option<CrdtType> = match crdt_type_str {
        Some(s) => match s.parse::<CrdtType>() {
            Ok(t) => Some(t),
            Err(_) => return Err(QueryError::UnknownCrdtType(s.to_owned())),
        },
        None => infer_crdt_type(from),
    };

    let scanned = pairs.len();

    // Short-circuit for `count` without a content filter — no need to deserialise values.
    if aggregate_op == AggregateOp::Count && filter.is_none() {
        let matched = pairs
            .iter()
            .filter(|(crdt_id, crdt)| {
                glob_match(from, crdt_id)
                    && type_filter.is_none_or(|t| crdt.crdt_type() == t)
            })
            .count();
        return Ok(QueryOutcome { value: json!(matched), matched, scanned });
    }

    let filtered: Vec<CrdtValue> = pairs
        .into_iter()
        .filter_map(|(crdt_id, crdt)| {
            if !glob_match(from, &crdt_id) {
                return None;
            }
            if type_filter.is_some_and(|t| crdt.crdt_type() != t) {
                return None;
            }
            if filter.is_some_and(|clause| !apply_where(&crdt, clause)) {
                return None;
            }
            Some(crdt)
        })
        .collect();

    let matched = filtered.len();
    let value = if matched == 0 {
        JsonValue::Null
    } else {
        aggregate(filtered, aggregate_op)?
    };

    Ok(QueryOutcome { value, matched, scanned })
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

fn aggregate(values: Vec<CrdtValue>, op: AggregateOp) -> Result<JsonValue, QueryError> {
    let first_type = values.first().map(|v| v.crdt_type());

    match op {
        AggregateOp::Sum | AggregateOp::Max | AggregateOp::Min => {
            aggregate_numeric(&values, op, first_type)
        }

        AggregateOp::Count => {
            if values.iter().all(|v| matches!(v, CrdtValue::ORSet(_))) {
                let total: usize = values
                    .iter()
                    .map(|v| if let CrdtValue::ORSet(s) = v { s.entries.len() } else { 0 })
                    .sum();
                return Ok(json!(total));
            }
            if values.iter().all(|v| matches!(v, CrdtValue::Presence(_))) {
                let total: usize = values
                    .iter()
                    .map(|v| if let CrdtValue::Presence(p) = v { p.value().entries.len() } else { 0 })
                    .sum();
                return Ok(json!(total));
            }
            Ok(json!(values.len()))
        }

        AggregateOp::Union => {
            if values.iter().all(|v| matches!(v, CrdtValue::ORSet(_))) {
                let mut seen: HashSet<String> = HashSet::new();
                let mut result: Vec<JsonValue> = Vec::new();
                for v in &values {
                    if let CrdtValue::ORSet(s) = v {
                        for elem in s.value().elements {
                            let key = serde_json::to_string(&elem).unwrap_or_default();
                            if seen.insert(key) {
                                result.push(elem);
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
            Err(mk_incompatible("union", first_type))
        }

        AggregateOp::Intersection => {
            if values.is_empty() {
                return Ok(json!([]));
            }
            if values.iter().all(|v| matches!(v, CrdtValue::ORSet(_))) {
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
            Err(mk_incompatible("intersection", first_type))
        }

        AggregateOp::Latest => {
            if values.iter().all(|v| matches!(v, CrdtValue::LwwRegister(_))) {
                let best = values.iter().max_by_key(|v| {
                    if let CrdtValue::LwwRegister(r) = v {
                        r.entry.as_ref().map(|e| e.hlc.wall_ms).unwrap_or(0)
                    } else {
                        0
                    }
                });
                return Ok(best
                    .and_then(|v| {
                        if let CrdtValue::LwwRegister(r) = v {
                            r.entry.as_ref().map(|e| e.value.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or(JsonValue::Null));
            }
            Err(mk_incompatible("latest", first_type))
        }

        AggregateOp::Collect => {
            Ok(json!(values.iter().map(|v| v.to_json_value()).collect::<Vec<_>>()))
        }

        AggregateOp::Merge => {
            if values.iter().all(|v| matches!(v, CrdtValue::CRDTMap(_))) {
                return Ok(json!(values.iter().map(|v| v.to_json_value()).collect::<Vec<_>>()));
            }
            Err(mk_incompatible("merge", first_type))
        }
    }
}

fn aggregate_numeric(
    values: &[CrdtValue],
    op: AggregateOp,
    first_type: Option<CrdtType>,
) -> Result<JsonValue, QueryError> {
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

    if values.iter().all(|v| matches!(v, CrdtValue::PNCounter(_))) {
        let nums: Vec<i64> = values
            .iter()
            .map(|v| if let CrdtValue::PNCounter(p) = v { p.value().value } else { 0 })
            .collect();
        return Ok(match op {
            AggregateOp::Sum => json!(nums.iter().fold(0i64, |acc, &x| acc.saturating_add(x))),
            AggregateOp::Max => json!(nums.iter().max().copied().unwrap_or(0)),
            AggregateOp::Min => json!(nums.iter().min().copied().unwrap_or(0)),
            _ => unreachable!(),
        });
    }

    Err(mk_incompatible(
        match op {
            AggregateOp::Sum => "sum",
            AggregateOp::Max => "max",
            AggregateOp::Min => "min",
            _ => "numeric",
        },
        first_type,
    ))
}

fn mk_incompatible(op: &str, crdt_type: Option<CrdtType>) -> QueryError {
    QueryError::IncompatibleAggregate {
        aggregate: op.to_string(),
        crdt_type: crdt_type
            .map(|t| t.as_str().to_string())
            .unwrap_or_else(|| "unknown".to_string()),
    }
}
