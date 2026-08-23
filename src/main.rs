use interlock::{
    AppState, AuthConfig, HttpEmbedder, PgArchiveStore, PgContinuityStore, PgMemoryStore,
    TokenGrant, router,
};
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
    archive_database_url: Option<String>,
    auth_file: PathBuf,
    listen: SocketAddr,
    trusted_proxy: bool,
    embedder_url: Option<String>,
    embedding_model: String,
}

impl Config {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let database_url = std::env::var("INTERLOCK_DATABASE_URL")?;
        let archive_database_url = std::env::var("INTERLOCK_ARCHIVE_DATABASE_URL").ok();
        let auth_file = PathBuf::from(std::env::var("INTERLOCK_AUTH_FILE")?);
        let listen: SocketAddr = std::env::var("INTERLOCK_LISTEN")
            .unwrap_or_else(|_| "127.0.0.1:8851".into())
            .parse()?;
        let trusted_proxy =
            std::env::var("INTERLOCK_TRUSTED_PROXY").is_ok_and(|value| value == "true");
        let embedder_url = std::env::var("INTERLOCK_EMBEDDER_URL").ok();
        let embedding_model = std::env::var("INTERLOCK_EMBEDDING_MODEL")
            .unwrap_or_else(|_| "BAAI/bge-large-en-v1.5".into());
        if embedding_model.is_empty() || embedding_model.len() > 128 {
            return Err("INTERLOCK_EMBEDDING_MODEL must be 1..128 bytes".into());
        }
        if !listen.ip().is_loopback() && !trusted_proxy {
            return Err("non-loopback listen requires INTERLOCK_TRUSTED_PROXY=true".into());
        }
        Ok(Self {
            database_url,
            archive_database_url,
            auth_file,
            listen,
            trusted_proxy,
            embedder_url,
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
            return Err("INTERLOCK_AUTH_FILE must be a regular file".into());
        }
        if metadata.uid() != nix::unistd::Uid::current().as_raw() {
            return Err("INTERLOCK_AUTH_FILE must be owned by the service user".into());
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("INTERLOCK_AUTH_FILE must not be group/world accessible".into());
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
    let store = PgMemoryStore::new(pool.clone());
    let mut state = AppState::new(Arc::new(store.clone()), auth)?;
    // Continuity handoffs live in a dedicated schema of the canonical database,
    // so they share its pool and are always available beside v6.
    state = state.with_continuity(Arc::new(PgContinuityStore::new(pool.clone())));
    // The conversation archive is a SEPARATE database with its own credentials,
    // volume, and backup policy. It is attached only when configured.
    let mut archive_store = None;
    if let Some(archive_url) = &config.archive_database_url {
        let archive_pool = PgPoolOptions::new()
            .max_connections(10)
            .acquire_timeout(Duration::from_secs(5))
            .after_connect(|connection, _| {
                Box::pin(async move {
                    for statement in [
                        "SET statement_timeout='10s'",
                        "SET lock_timeout='2s'",
                        "SET idle_in_transaction_session_timeout='10s'",
                    ] {
                        sqlx::query(statement).execute(&mut *connection).await?;
                    }
                    Ok(())
                })
            })
            .connect(archive_url)
            .await?;
        let attached = PgArchiveStore::new(archive_pool);
        state = state.with_archive(Arc::new(attached.clone()));
        archive_store = Some(attached);
        tracing::info!("conversation archive database attached");
    }
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let mut embedding_worker = None;
    if let Some(url) = &config.embedder_url {
        let embedder = Arc::new(HttpEmbedder::new(url, config.embedding_model.clone())?);
        let worker_model = config.embedding_model.clone();
        state = state.with_embedder(embedder.clone());
        let worker_store = store.clone();
        let worker_archive = archive_store.clone();
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
                        if let Some(archive) = &worker_archive {
                            match archive.embed_pending(
                                embedder.as_ref(),
                                &worker_model,
                                interlock::archive::ARCHIVE_EMBEDDING_GENERATION,
                                64,
                                worker_id,
                            ).await {
                                Ok(count) if count > 0 => tracing::info!(count, "embedded pending archive events"),
                                Ok(_) => {}
                                Err(error) => tracing::warn!(%error, "archive embedding worker will retry"),
                            }
                        }
                    }
                }
            }
        });
        embedding_worker = Some((worker_id, handle));
    }
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(address = %config.listen, trusted_proxy = config.trusted_proxy, "Interlock listening");
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
        if let Some(archive) = &archive_store {
            archive.release_embedding_leases(worker_id).await?;
        }
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
