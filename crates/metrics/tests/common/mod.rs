//! Shared test utilities for metrics integration tests.

use bytes::Bytes;
use frogdb_protocol::Response;
use frogdb_server::{Config, Server};
use futures::{SinkExt, StreamExt};
use redis_protocol::codec::Resp2;
use redis_protocol::resp2::types::BytesFrame;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::Framed;

/// Helper struct for managing a test server.
pub struct TestServer {
    pub addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
    handle: JoinHandle<()>,
    #[allow(dead_code)]
    temp_dir: TempDir,
    client: reqwest::Client,
}

impl TestServer {
    /// Start a new test server on an available port.
    pub async fn start() -> Self {
        let temp_dir = TempDir::new().unwrap();

        // Find available ports
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let metrics_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let metrics_addr = metrics_listener.local_addr().unwrap();
        drop(metrics_listener);

        let mut config = Config::default();
        config.server.bind = "127.0.0.1".to_string();
        config.server.port = addr.port();
        config.server.num_shards = 4; // Multiple shards for shard metrics tests
        config.logging.level = "warn".to_string();
        config.persistence.data_dir = temp_dir.path().to_path_buf();
        config.metrics.bind = "127.0.0.1".to_string();
        config.metrics.port = metrics_addr.port();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            let server = Server::new(config).await.unwrap();

            tokio::select! {
                result = server.run() => {
                    if let Err(e) = result {
                        eprintln!("Server error: {}", e);
                    }
                }
                _ = shutdown_rx => {
                    // Shutdown requested
                }
            }
        });

        // Wait for server to be ready
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        TestServer {
            addr,
            metrics_addr,
            shutdown_tx,
            handle,
            temp_dir,
            client,
        }
    }

    /// Connect to the test server.
    pub async fn connect(&self) -> TestClient {
        let stream = TcpStream::connect(self.addr).await.unwrap();
        let framed = Framed::new(stream, Resp2);
        TestClient { framed }
    }

    /// Get the metrics URL.
    pub fn metrics_url(&self, path: &str) -> String {
        format!("http://{}{}", self.metrics_addr, path)
    }

    /// Get the shared HTTP client (configured with no_proxy to avoid macOS sandbox panics).
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Fetch metrics as raw Prometheus text format.
    pub async fn fetch_metrics(&self) -> String {
        self.client
            .get(self.metrics_url("/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    }

    /// Shutdown the test server.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.handle.await;
    }
}

/// Helper struct for sending commands and receiving responses.
pub struct TestClient {
    framed: Framed<TcpStream, Resp2>,
}

impl TestClient {
    /// Send a command and receive a response.
    pub async fn command(&mut self, args: &[&str]) -> Response {
        let frame = BytesFrame::Array(
            args.iter()
                .map(|s| BytesFrame::BulkString(Bytes::from(s.to_string())))
                .collect(),
        );

        self.framed.send(frame).await.unwrap();

        let response_frame = timeout(Duration::from_secs(5), self.framed.next())
            .await
            .expect("timeout")
            .expect("connection closed")
            .expect("frame error");

        frame_to_response(response_frame)
    }
}

/// Convert a BytesFrame to our Response type.
pub fn frame_to_response(frame: BytesFrame) -> Response {
    match frame {
        BytesFrame::SimpleString(s) => Response::Simple(s),
        BytesFrame::Error(e) => Response::Error(e.into_inner()),
        BytesFrame::Integer(n) => Response::Integer(n),
        BytesFrame::BulkString(b) => Response::Bulk(Some(b)),
        BytesFrame::Null => Response::Bulk(None),
        BytesFrame::Array(items) => {
            Response::Array(items.into_iter().map(frame_to_response).collect())
        }
    }
}
