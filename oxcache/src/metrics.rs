use axum::{routing::get, Router};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use metrics::{counter, histogram};
use once_cell::sync::Lazy;

pub static METRICS: Lazy<MetricsRecorder> = Lazy::new(|| {
    MetricsRecorder::new()
});

pub fn init_metrics_exporter(addr: SocketAddr) {
    let builder = PrometheusBuilder::new();
    let recorder_handle = builder.install_recorder().expect("failed to install recorder");

    // Spawn HTTP server in a tokio task (green thread)
    tokio::spawn(async move {
        let app = Router::new().route("/metrics", get(move || async move {
            recorder_handle.render()
        }));

        log::info!("Serving metrics at http://{}/metrics", addr);

        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .expect("failed to start metrics server");
    });
}

pub struct MetricsRecorder {
    run_id: String,
    prefix: String,
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
                log::warn!("Unknown metric type");
                return;
            },
        };
        h.record(v);
    }

    pub fn update_metric_counter(&self, name: &str, value: u64) {
        let id = self.run_id.clone();
        let h = counter!(format!("{}_{}", self.prefix, name.to_string()), "run_id" => id.clone());
        h.increment(value);
    }
}

