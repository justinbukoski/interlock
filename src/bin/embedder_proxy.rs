use std::{sync::Arc, time::Duration};
use tokio::{io, sync::Semaphore, time::timeout};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let listen = std::env::var("FOREMAN_PROXY_LISTEN").unwrap_or_else(|_| "0.0.0.0:8840".into());
    let target = std::env::var("FOREMAN_PROXY_TARGET")?;
    if !["172.31.60.1:8840", "172.31.61.10:8851", "172.31.62.10:8852"].contains(&target.as_str()) {
        return Err("proxy target is not allowlisted".into());
    }
    let listener = tokio::net::TcpListener::bind(listen).await?;
    let permits = Arc::new(Semaphore::new(64));
    loop {
        let (mut inbound, peer) = listener.accept().await?;
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            tracing::warn!(%peer, "embedder proxy rejected connection at concurrency limit");
            continue;
        };
        let target = target.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let outbound = timeout(
                Duration::from_secs(5),
                tokio::net::TcpStream::connect(&target),
            )
            .await;
            let mut outbound = match outbound {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => {
                    tracing::warn!(%peer, %error, "embedder proxy connect failed");
                    return;
                }
                Err(_) => {
                    tracing::warn!(%peer, "embedder proxy connect timed out");
                    return;
                }
            };
            match timeout(
                Duration::from_secs(120),
                io::copy_bidirectional(&mut inbound, &mut outbound),
            )
            .await
            {
                Ok(Ok((to_embedder, to_client))) => tracing::debug!(
                    %peer, to_embedder, to_client, "embedder proxy connection completed"
                ),
                Ok(Err(error)) => tracing::warn!(%peer, %error, "embedder proxy relay failed"),
                Err(_) => tracing::warn!(%peer, "embedder proxy relay timed out"),
            }
        });
    }
}
