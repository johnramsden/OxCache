use axum::{routing::get, Router};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::{IpAddr, SocketAddr};

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
