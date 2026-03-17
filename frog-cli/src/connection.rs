use anyhow::{Context, Result};

use crate::cli::GlobalOpts;

/// Holds lazy-initialized RESP and HTTP clients.
pub struct ConnectionContext {
    global: GlobalOpts,
    resp: Option<redis::aio::MultiplexedConnection>,
    http: reqwest::Client,
}

impl ConnectionContext {
    pub fn new(global: GlobalOpts) -> Self {
        Self {
            global,
            resp: None,
            http: reqwest::Client::new(),
        }
    }

    /// Build a Redis connection URL for a given host:port.
    fn build_url(&self, host: &str, port: u16) -> String {
        let scheme = if self.global.tls { "rediss" } else { "redis" };
        match (&self.global.user, &self.global.auth) {
            (Some(user), Some(auth)) => format!("{scheme}://{user}:{auth}@{host}:{port}"),
            (None, Some(auth)) => format!("{scheme}://:{auth}@{host}:{port}"),
            _ => format!("{scheme}://{host}:{port}"),
        }
    }

    /// Get or lazily connect the default RESP connection.
    pub async fn resp(&mut self) -> Result<&mut redis::aio::MultiplexedConnection> {
        if self.resp.is_none() {
            let url = self.build_url(&self.global.host, self.global.port);
            let client = redis::Client::open(url.as_str())
                .with_context(|| format!("invalid connection URL: {url}"))?;
            let conn = client
                .get_multiplexed_async_connection()
                .await
                .with_context(|| {
                    format!(
                        "failed to connect to {}:{}",
                        self.global.host, self.global.port
                    )
                })?;
            self.resp = Some(conn);
        }
        Ok(self.resp.as_mut().unwrap())
    }

    /// Connect to a specific address (for fan-out).
    pub async fn resp_to(&self, addr: &str) -> Result<redis::aio::MultiplexedConnection> {
        let (host, port) = parse_addr(addr)?;
        let url = self.build_url(host, port);
        let client = redis::Client::open(url.as_str())
            .with_context(|| format!("invalid connection URL: {url}"))?;
        client
            .get_multiplexed_async_connection()
            .await
            .with_context(|| format!("failed to connect to {addr}"))
    }

    /// Send a raw Redis command and get a string response.
    #[allow(dead_code)]
    pub async fn cmd(&mut self, cmd: &str, args: &[&str]) -> Result<String> {
        let conn = self.resp().await?;
        let mut redis_cmd = redis::cmd(cmd);
        for arg in args {
            redis_cmd.arg(*arg);
        }
        let result: String = redis_cmd
            .query_async(conn)
            .await
            .with_context(|| format!("command failed: {cmd}"))?;
        Ok(result)
    }

    /// Send PING and return the response.
    pub async fn ping(&mut self) -> Result<String> {
        let conn = self.resp().await?;
        let pong: String = redis::cmd("PING").query_async(conn).await?;
        Ok(pong)
    }

    /// Send INFO with optional sections and return the raw bulk string.
    pub async fn info(&mut self, sections: &[&str]) -> Result<String> {
        let conn = self.resp().await?;
        let mut cmd = redis::cmd("INFO");
        for s in sections {
            cmd.arg(*s);
        }
        let result: String = cmd.query_async(conn).await?;
        Ok(result)
    }

    /// Send INFO on a specific connection.
    #[allow(dead_code)]
    pub async fn info_on(
        conn: &mut redis::aio::MultiplexedConnection,
        sections: &[&str],
    ) -> Result<String> {
        let mut cmd = redis::cmd("INFO");
        for s in sections {
            cmd.arg(*s);
        }
        let result: String = cmd.query_async(conn).await?;
        Ok(result)
    }

    /// Resolve the admin URL.
    pub fn admin_url(&self) -> String {
        self.global
            .admin_url
            .clone()
            .unwrap_or_else(|| format!("http://{}:6380", self.global.host))
    }

    /// Resolve the metrics URL.
    pub fn metrics_url(&self) -> String {
        self.global
            .metrics_url
            .clone()
            .unwrap_or_else(|| format!("http://{}:9090", self.global.host))
    }

    /// HTTP GET against the admin API.
    pub async fn admin_get(&self, path: &str) -> Result<reqwest::Response> {
        let url = format!("{}{}", self.admin_url(), path);
        self.http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("admin request failed: {url}"))
    }

    /// HTTP GET against the metrics API.
    pub async fn metrics_get(&self, path: &str) -> Result<reqwest::Response> {
        let url = format!("{}{}", self.metrics_url(), path);
        self.http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("metrics request failed: {url}"))
    }

    pub fn global(&self) -> &GlobalOpts {
        &self.global
    }
}

fn parse_addr(addr: &str) -> Result<(&str, u16)> {
    let (host, port_str) = addr
        .rsplit_once(':')
        .with_context(|| format!("invalid address (expected host:port): {addr}"))?;
    let port: u16 = port_str
        .parse()
        .with_context(|| format!("invalid port in address: {addr}"))?;
    Ok((host, port))
}
