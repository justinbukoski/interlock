use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let first = args.next();
    let tcp_only = first.as_deref() == Some("--tcp");
    let http_mode = first.as_deref() == Some("--http");
    let address = (if tcp_only || http_mode {
        args.next()
    } else {
        first
    })
    .unwrap_or_else(|| "127.0.0.1:8851".into());
    let mut stream = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::net::TcpStream::connect(address),
    )
    .await??;
    if tcp_only {
        return Ok(());
    }
    let path = if http_mode {
        args.next().unwrap_or_else(|| "/health".into())
    } else {
        "/v6/health".into()
    };
    if !path.starts_with('/') || path.bytes().any(|byte| byte.is_ascii_control()) {
        return Err("invalid healthcheck path".into());
    }
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let status = response.split(|b| *b == b'\n').next().unwrap_or_default();
    if !status.windows(5).any(|part| part == b" 200 ") {
        return Err("unhealthy response".into());
    }
    Ok(())
}
