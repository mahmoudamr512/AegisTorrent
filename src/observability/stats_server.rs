use std::sync::{Arc, Mutex};

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

use crate::network::coordinator::DownloadProgress;

pub async fn run_stats_server(addr: &str, progress: Arc<Mutex<DownloadProgress>>) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Stats server failed to bind to {addr}: {e}");
            return;
        }
    };

    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(_) => continue,
        };

        let snapshot = {
            let p = progress.lock().unwrap();
            p.clone()
        };

        let body = match serde_json::to_string_pretty(&snapshot) {
            Ok(json) => json,
            Err(_) => continue,
        };

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );

        let _ = stream.write_all(response.as_bytes()).await;
    }
}
