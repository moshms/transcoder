mod config;
mod transcoder;

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use config::Config;
use mc_core::StorageId;
use mc_proto::transcoder::transcoder_server::TranscoderServer;
use mc_storage::LocalStorage;
use mc_telemetry::TelemetryConfig;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::info;
use transcoder::TranscoderService;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return healthcheck().await;
    }

    let config = Config::from_env()?;

    mc_telemetry::init(&TelemetryConfig::new(
        config.log_json,
        config.log_filter.clone(),
    ));

    let metrics_handle = if config.metrics {
        Some(mc_telemetry::install_metrics().map_err(|error| anyhow::anyhow!(error))?)
    } else {
        None
    };

    let storage =
        LocalStorage::new(StorageId::new(), &config.library_path).context("open local library")?;
    let service = TranscoderService::new(
        storage,
        PathBuf::from(&config.cache_dir),
        config.cache_max_bytes,
    );
    service.prepare().await.context("prepare transcode cache")?;

    if let Some(handle) = metrics_handle {
        let metrics_address: SocketAddr = config
            .metrics_addr
            .parse()
            .context("parse metrics bind address")?;
        let metrics_listener = TcpListener::bind(metrics_address)
            .await
            .with_context(|| format!("bind metrics {metrics_address}"))?;
        tokio::spawn(async move {
            if let Err(error) =
                axum::serve(metrics_listener, mc_telemetry::metrics_router(handle)).await
            {
                tracing::error!(%error, "metrics server failed");
            }
        });
    }

    let address: SocketAddr = config.bind_addr.parse().context("parse bind address")?;
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("bind {address}"))?;
    let local_address = listener.local_addr()?;

    let (reporter, health_service) = tonic_health::server::health_reporter();
    reporter
        .set_serving::<TranscoderServer<TranscoderService>>()
        .await;

    info!(%local_address, "mc-transcoder listening");

    Server::builder()
        .add_service(TranscoderServer::new(service))
        .add_service(health_service)
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            shutdown_signal(),
        )
        .await
        .context("grpc server")?;

    Ok(())
}

async fn healthcheck() -> anyhow::Result<()> {
    let url =
        std::env::var("MC_TRANSCODER_URL").unwrap_or_else(|_| "http://127.0.0.1:50051".to_owned());
    let channel = tonic::transport::Channel::from_shared(url)
        .context("parse transcoder URL")?
        .connect()
        .await
        .context("connect to transcoder")?;
    let mut client = tonic_health::pb::health_client::HealthClient::new(channel);
    let response = client
        .check(tonic_health::pb::HealthCheckRequest {
            service: String::new(),
        })
        .await
        .context("health check")?
        .into_inner();

    if response.status == tonic_health::pb::health_check_response::ServingStatus::Serving as i32 {
        Ok(())
    } else {
        anyhow::bail!("transcoder is not serving")
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }

    info!("shutdown signal received");
}
