use foreman_memory_v6::{AppState, AuthConfig, HttpEmbedder, PgMemoryStore, TokenGrant, router};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::{fs::OpenOptions, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tracing_subscriber::EnvFilter;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthFile {
    tokens: Vec<TokenGrant>,
}

struct Config {
    database_url: String,
    auth_file: PathBuf,
    listen: SocketAddr,
    trusted_proxy: bool,
    embedder_url: Option<String>,
    embedder_allowed_host: Option<String>,
    embedding_model: String,
}

impl Config {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let database_url = std::env::var("FOREMAN_V6_DATABASE_URL")?;
        let auth_file = PathBuf::from(std::env::var("FOREMAN_V6_AUTH_FILE")?);
        let listen: SocketAddr = std::env::var("FOREMAN_V6_LISTEN")
            .unwrap_or_else(|_| "127.0.0.1:8851".into())
            .parse()?;
        let trusted_proxy =
            std::env::var("FOREMAN_V6_TRUSTED_PROXY").is_ok_and(|value| value == "true");
        let embedder_url = std::env::var("FOREMAN_V6_EMBEDDER_URL").ok();
        let embedder_allowed_host = std::env::var("FOREMAN_V6_EMBEDDER_ALLOWED_HOST").ok();
        let embedding_model = std::env::var("FOREMAN_V6_EMBEDDING_MODEL")
            .unwrap_or_else(|_| "BAAI/bge-large-en-v1.5".into());
        if embedding_model.is_empty() || embedding_model.len() > 128 {
            return Err("FOREMAN_V6_EMBEDDING_MODEL must be 1..128 bytes".into());
        }
        if !listen.ip().is_loopback() && !trusted_proxy {
            return Err("non-loopback listen requires FOREMAN_V6_TRUSTED_PROXY=true".into());
        }
        Ok(Self {
            database_url,
            auth_file,
            listen,
            trusted_proxy,
            embedder_url,
            embedder_allowed_host,
            embedding_model,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let config = Config::from_env()?;
    let auth_handle = {
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(nix::libc::O_NOFOLLOW);
        }
        options.open(&config.auth_file)?
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = auth_handle.metadata()?;
        if !metadata.file_type().is_file() {
            return Err("FOREMAN_V6_AUTH_FILE must be a regular file".into());
        }
        if metadata.uid() != nix::unistd::Uid::current().as_raw() {
            return Err("FOREMAN_V6_AUTH_FILE must be owned by the service user".into());
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("FOREMAN_V6_AUTH_FILE must not be group/world accessible".into());
        }
    }
    let auth_file: AuthFile = serde_json::from_reader(auth_handle)?;
    let auth = AuthConfig::new(auth_file.tokens)?;
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(5))
        .after_connect(|connection, _| {
            Box::pin(async move {
                for statement in [
                    "SET statement_timeout='5s'",
                    "SET lock_timeout='2s'",
                    "SET idle_in_transaction_session_timeout='10s'",
                ] {
                    sqlx::query(statement).execute(&mut *connection).await?;
                }
                Ok(())
            })
        })
        .connect(&config.database_url)
        .await?;
    let store = PgMemoryStore::new(pool);
    let mut state = AppState::new(Arc::new(store.clone()), auth)?;
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let mut embedding_worker = None;
    if let Some(url) = &config.embedder_url {
        let embedder = Arc::new(HttpEmbedder::new_with_allowed_host(
            url,
            config.embedding_model.clone(),
            config.embedder_allowed_host.as_deref(),
        )?);
        let worker_model = config.embedding_model.clone();
        state = state.with_embedder(embedder.clone());
        let worker_store = store.clone();
        let worker_id = uuid::Uuid::new_v4();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() { break; }
                    }
                    _ = interval.tick() => {
                        match worker_store.embed_pending(embedder.as_ref(), &worker_model, 8, worker_id).await {
                            Ok(count) if count > 0 => tracing::info!(count, "embedded pending memory rows"),
                            Ok(_) => {}
                            Err(error) => tracing::warn!(%error, "embedding worker will retry"),
                        }
                    }
                }
            }
        });
        embedding_worker = Some((worker_id, handle));
    }
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(address = %config.listen, trusted_proxy = config.trusted_proxy, "Foreman Memory v6 listening");
    let shutdown_notifier = shutdown_tx.clone();
    let server_result = axum::serve(listener, router(state))
        .with_graceful_shutdown(async move {
            shutdown().await;
            let _ = shutdown_notifier.send(true);
        })
        .await;
    let _ = shutdown_tx.send(true);
    if let Some((worker_id, mut handle)) = embedding_worker {
        if tokio::time::timeout(Duration::from_secs(10), &mut handle)
            .await
            .is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
        store.release_embedding_leases(worker_id).await?;
    }
    server_result?;
    Ok(())
}

async fn shutdown() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
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
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}
