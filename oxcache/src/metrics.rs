use axum::{routing::get, Router};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{event, info, Level};
use metrics::{counter, gauge, histogram};
use once_cell::sync::Lazy;

use tracing_appender::rolling;
use tracing_appender::non_blocking;
use tracing_subscriber::fmt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub static METRICS: Lazy<MetricsRecorder> = Lazy::new(|| {
    MetricsRecorder::new()
});

pub struct HitRatio {
    hits: u64,
    misses: u64,
}
impl HitRatio {
    pub fn new() -> Self {
        Self {
            hits: 0, misses: 0
        }
    }
    pub fn update_hitratio(&mut self, hit_type: HitType) {
        match hit_type {
            HitType::Hit => {
                self.hits+=1
            },
            HitType::Miss => {
                self.misses+=1
            }
        }
    }
}

pub enum HitType {
    Hit, Miss
}

pub struct MetricState {
    hit_ratio: Arc<Mutex<HitRatio>>,
}

impl MetricState {
    pub fn new() -> Self {
        Self {
            hit_ratio: Arc::new(Mutex::new(HitRatio::new()))
        }
    }
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
    metric_state: MetricState,
    counters: Arc<Mutex<HashMap<String, u64>>>,
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
            metric_state: MetricState::new(),
            counters: Arc::new(Mutex::new(HashMap::new())),
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
        
        // Update thread-safe counter map
        {
            let mut counters = self.counters.lock().unwrap();
            let counter = counters.entry(name.to_string()).or_insert(0);
            *counter += value;
            event!(target: "metrics", Level::INFO, name = name, value=*counter);
        }
    }

    pub fn update_metric_gauge(&self, name: &str, value: f64) {
        let id = self.run_id.clone();
        let g = gauge!(format!("{}_{}", self.prefix, name.to_string()), "run_id" => id.clone());
        g.set(value);
        event!(target: "metrics", Level::INFO, name = name, value=value);
    }

    pub fn update_hitratio(&self, hit_type: HitType) {
        let mut hr = self.metric_state.hit_ratio.lock().unwrap();
        hr.update_hitratio(hit_type);

        let total = hr.hits + hr.misses;
        let ratio = if total == 0 {
            0.0
        } else {
            hr.hits as f64 / total as f64
        };

        self.update_metric_gauge("hitratio", ratio)
    }
}

