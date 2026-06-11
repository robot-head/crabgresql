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

    /// Path to the server certificate chain (PEM). Enables TLS with --tls-key.
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<std::path::PathBuf>,

    /// Path to the server private key (PEM).
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<std::path::PathBuf>,

    /// Authentication mode: "trust" or "scram".
    #[arg(long, default_value = "trust")]
    auth: String,

    /// User credentials for --auth scram, as user=password (repeatable).
    #[arg(long = "user-cred", value_name = "USER=PASSWORD")]
    user_creds: Vec<String>,
}

fn tls_acceptor(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> std::io::Result<tokio_rustls::TlsAcceptor> {
    use std::io::{BufReader, Error, ErrorKind};
    let certs = rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(cert_path)?))
        .collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(std::fs::File::open(key_path)?))?
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "no private key in file"))?;
    let provider = Arc::new(rustls_rustcrypto::provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
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
    let tls = match (&args.tls_cert, &args.tls_key) {
        (Some(c), Some(k)) => Some(tls_acceptor(c, k)?),
        _ => None,
    };
    let session_config = build_session_config(&args)?;
    pgwire::server::serve_tls(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(session_config),
        tls,
    )
    .await
}

fn build_session_config(args: &Args) -> std::io::Result<SessionConfig> {
    use std::io::{Error, ErrorKind};
    match args.auth.as_str() {
        "trust" => Ok(SessionConfig::trust()),
        "scram" => {
            use pgwire::scram::ScramVerifier;
            use rand::Rng;
            if args.user_creds.is_empty() {
                return Err(Error::new(ErrorKind::InvalidInput, "--auth scram requires --user-cred"));
            }
            let mut verifiers = std::collections::HashMap::new();
            for cred in &args.user_creds {
                let (user, pass) = cred
                    .split_once('=')
                    .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "--user-cred must be USER=PASSWORD"))?;
                if user.is_empty() {
                    return Err(Error::new(ErrorKind::InvalidInput, "--user-cred user name is empty"));
                }
                let salt: [u8; 16] = rand::rng().random();
                verifiers.insert(user.to_string(), ScramVerifier::from_password(pass, salt.to_vec(), 4096));
            }
            let mock_secret: [u8; 32] = rand::rng().random();
            Ok(SessionConfig { auth: pgwire::session::AuthMode::ScramSha256 { verifiers, mock_secret }, ..SessionConfig::trust() })
        }
        other => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("unknown --auth {other:?}: expected \"trust\" or \"scram\""),
        )),
    }
}
