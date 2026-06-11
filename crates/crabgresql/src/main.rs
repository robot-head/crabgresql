use std::sync::Arc;

use clap::Parser;
use pgwire::session::SessionConfig;
use pgwire::stub::StubEngine;
use tokio::net::TcpListener;

/// crabgresql node binary. SP1: serves the stub engine.
#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:5433")]
    listen: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let listener = TcpListener::bind(&args.listen).await?;
    tracing::info!("crabgresql listening on {}", args.listen);
    pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
    )
    .await
}
