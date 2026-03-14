use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use metrics_exporter_prometheus::PrometheusHandle;

/// `GET /metrics` — exposes Prometheus-format metrics.
/// This route is intentionally unauthenticated (standard scraping convention).
pub async fn get_metrics(
    axum::extract::Extension(handle): axum::extract::Extension<PrometheusHandle>,
) -> Response {
    let output = handle.render();
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        output,
    )
        .into_response()
}
