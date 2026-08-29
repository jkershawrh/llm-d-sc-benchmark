//! Minimal Prometheus endpoint for the instrumented benchmark candidate.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

use crate::metrics::Metrics;

/// Bind a dedicated metrics listener and serve it on a named OS thread.
pub fn start(addr: &str, metrics: Metrics) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let bound = listener.local_addr()?;
    std::thread::Builder::new()
        .name("prometheus-metrics".to_string())
        .spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(mut stream) => serve_one(&mut stream, &metrics),
                    Err(error) => eprintln!("llm-d-sc metrics accept error: {error}"),
                }
            }
        })?;
    Ok(bound)
}

fn serve_one(stream: &mut TcpStream, metrics: &Metrics) {
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(2)));
    let mut request = [0u8; 1024];
    let read = stream.read(&mut request).unwrap_or(0);
    let metrics_path = request[..read].starts_with(b"GET /metrics ");
    let (status, content_type, body) = if metrics_path {
        (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            metrics.prometheus_text(),
        )
    } else {
        (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n".to_string(),
        )
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u135_exporter_serves_metrics_and_rejects_other_paths() {
        let addr = start("127.0.0.1:0", Metrics::new()).unwrap();
        let request = |path: &str| {
            let mut stream = TcpStream::connect(addr).unwrap();
            write!(stream, "GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            response
        };
        assert!(request("/metrics").starts_with("HTTP/1.1 200 OK"));
        assert!(request("/healthz").starts_with("HTTP/1.1 404 Not Found"));
    }
}
