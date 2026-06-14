use std::sync::Arc;

use clap::Parser;
use executor::SqlEngine;
use pgwire::session::SessionConfig;
use tokio::net::TcpListener;

/// crabgresql — a replicated PostgreSQL-wire-compatible database.
#[derive(Parser, Debug)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    serve: ServeArgs,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Run a replicated durable Raft node.
    Node(NodeArgs),
}

/// Arguments for the default serve mode (no subcommand).
#[derive(clap::Args, Debug)]
struct ServeArgs {
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

    /// Directory for durable storage. Absent → ephemeral in-memory engine.
    #[arg(long)]
    data_dir: Option<std::path::PathBuf>,
}

/// Arguments for the `node` subcommand.
#[derive(clap::Args, Debug)]
struct NodeArgs {
    /// This node's Raft id.
    #[arg(long)]
    id: u64,

    /// host:port the node-protocol listener binds (Raft RPCs + control).
    #[arg(long)]
    node_addr: String,

    /// host:port the pgwire SQL listener binds.
    #[arg(long)]
    sql_addr: String,

    /// Directory for this node's durable store.
    #[arg(long)]
    data_dir: std::path::PathBuf,

    /// Repeatable: id@node_addr|sql_addr for every member (including self).
    #[arg(long = "peer", value_name = "ID@NODE_ADDR|SQL_ADDR")]
    peers: Vec<String>,

    /// Repeatable range split point (a table id): every boundary starts a new
    /// range, so N boundaries ⇒ N+1 ranges. An empty list ⇒ `RangeMap::single()`
    /// (one range covering every table — the single-range default). Boundaries
    /// must be strictly increasing and nonzero (range 0 always starts at table 0).
    #[arg(long = "range-boundaries", value_name = "TABLE_ID")]
    range_boundaries: Vec<u32>,

    /// When set, this node initializes the voting group once every peer is up.
    #[arg(long)]
    bootstrap: bool,
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
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Node(args)) => run_node(args).await,
        None => run_serve(cli.serve).await,
    }
}

async fn run_serve(args: ServeArgs) -> std::io::Result<()> {
    let listener = TcpListener::bind(&args.listen).await?;
    tracing::info!("crabgresql listening on {}", args.listen);
    let tls = match (&args.tls_cert, &args.tls_key) {
        (Some(c), Some(k)) => Some(tls_acceptor(c, k)?),
        _ => None,
    };
    let engine = match &args.data_dir {
        Some(dir) => Arc::new(
            SqlEngine::open(dir)
                .map_err(|e| std::io::Error::other(format!("opening data dir: {e:?}")))?,
        ),
        None => Arc::new(SqlEngine::new()),
    };
    let session_config = build_session_config(&args)?;
    pgwire::server::serve_tls(listener, engine, Arc::new(session_config), tls).await
}

async fn run_node(a: NodeArgs) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};

    let peers: Vec<(u64, String)> = a
        .peers
        .iter()
        .map(|s| {
            s.split_once('@')
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        format!("--peer {s:?}: expected ID@ADDR (e.g. 1@127.0.0.1:7001)"),
                    )
                })
                .and_then(|(id_str, addr)| {
                    id_str
                        .parse::<u64>()
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::InvalidInput,
                                format!("--peer {s:?}: id is not a u64: {e}"),
                            )
                        })
                        .map(|id| (id, addr.to_string()))
                })
        })
        .collect::<std::io::Result<_>>()?;

    tracing::info!(
        id = a.id,
        node_addr = %a.node_addr,
        sql_addr = %a.sql_addr,
        "crabgresql node starting"
    );

    // An empty `--range-boundaries` list is the single-range fast path; any
    // boundaries build a multi-range map where every node hosts every range.
    let range_map = if a.range_boundaries.is_empty() {
        cluster::range::RangeMap::single()
    } else {
        cluster::range::RangeMap::with_boundaries(a.range_boundaries.clone())
    };

    let cfg = cluster::server_node::NodeConfig {
        id: a.id,
        node_addr: a.node_addr,
        sql_addr: a.sql_addr,
        data_dir: a.data_dir,
        peers,
        bootstrap: a.bootstrap,
        layout: cluster::server_node::RangeLayout::Static(range_map),
    };

    let node = cluster::server_node::ServerNode::start(cfg).await?;
    node.shutdown.wait().await;
    Ok(())
}

fn build_session_config(args: &ServeArgs) -> std::io::Result<SessionConfig> {
    use std::io::{Error, ErrorKind};
    match args.auth.as_str() {
        "trust" => Ok(SessionConfig::trust()),
        "scram" => {
            use pgwire::scram::ScramVerifier;
            use rand::RngExt;
            if args.user_creds.is_empty() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "--auth scram requires --user-cred",
                ));
            }
            let mut verifiers = std::collections::HashMap::new();
            for cred in &args.user_creds {
                let (user, pass) = cred.split_once('=').ok_or_else(|| {
                    Error::new(ErrorKind::InvalidInput, "--user-cred must be USER=PASSWORD")
                })?;
                if user.is_empty() {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "--user-cred user name is empty",
                    ));
                }
                let salt: [u8; pgwire::scram::SALT_LEN] = rand::rng().random();
                verifiers.insert(
                    user.to_string(),
                    ScramVerifier::from_password(pass, salt.to_vec(), 4096),
                );
            }
            let mock_secret: [u8; 32] = rand::rng().random();
            Ok(SessionConfig {
                auth: pgwire::session::AuthMode::ScramSha256 {
                    verifiers,
                    mock_secret,
                },
                ..SessionConfig::trust()
            })
        }
        other => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("unknown --auth {other:?}: expected \"trust\" or \"scram\""),
        )),
    }
}
