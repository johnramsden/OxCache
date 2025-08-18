use axum::{routing::get, Router};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{event, info, Level};
use metrics::{counter, gauge, histogram};
use once_cell::sync::Lazy;

use tracing_appender::rolling;
use tracing_appender::non_blocking;
use tracing_subscriber::fmt;

pub static METRICS: Lazy<MetricsRecorder> = Lazy::new(|| {
    MetricsRecorder::new()
});

pub struct HitRatio {
    hits: usize,
    misses: usize,
}

pub struct MetricState {
    hit_ratio: HitRatio,
}

pub fn init_metrics_exporter(addr: SocketAddr) {
    let builder = PrometheusBuilder::new();
    let recorder_handle = builder.install_recorder().expect("failed to install recorder");

    // Spawn HTTP server in a tokio task (green thread)
    tokio::spawn(async move {
        let app = Router::new().route("/metrics", get(move || async move {
            recorder_handle.render()
        }));

        tracing::info!("Serving metrics at http://{}/metrics", addr);

        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .expect("failed to start metrics server");
    });
}

pub struct MetricsRecorder {
    run_id: String,
    prefix: String,
    metric_state: MetricState
}

pub enum MetricType {
    MsLatency
}

const MILLISECONDS: f64 = 1_000.0;

impl MetricsRecorder {
    pub fn new() -> Self {
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();

        Self {
            run_id: since_epoch.as_secs().to_string(),
            prefix: String::from("oxcache"),
            metric_state: MetricState {
                hit_ratio: HitRatio { hits: 0, misses: 0 },
            }
        }
    }
    pub fn update_metric_histogram_latency(&self, name: &str, value: Duration, metric_type: MetricType) {
        let id = self.run_id.clone();
        let h = histogram!(format!("{}_{}", self.prefix, name.to_string()), "run_id" => id.clone());
        let v = match metric_type {
            MetricType::MsLatency => {
                value.as_secs_f64() * MILLISECONDS
            },
            _ => {
                tracing::warn!("Unknown metric type");
                return;
            },
        };
        h.record(v);
        event!(target: "metrics", Level::INFO, name = name, value=v);
    }

    pub fn update_metric_counter(&self, name: &str, value: u64) {
        let id = self.run_id.clone();
        let h = counter!(format!("{}_{}", self.prefix, name.to_string()), "run_id" => id.clone());
        h.increment(value);
    }
}

