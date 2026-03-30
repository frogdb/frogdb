use frog_cli::cli::{GlobalOpts, OutputMode};
use frog_cli::connection::ConnectionContext;
use frogdb_test_harness::server::TestServer;

fn default_global(server: &TestServer) -> GlobalOpts {
    GlobalOpts {
        host: "127.0.0.1".to_string(),
        port: server.port(),
        auth: None,
        user: None,
        tls: false,
        tls_cert: None,
        tls_key: None,
        tls_ca: None,
        admin_url: None,
        metrics_url: None,
        output: OutputMode::Table,
        no_color: true,
    }
}

/// Create a ConnectionContext pointing at a running TestServer.
pub fn ctx_for_server(server: &TestServer) -> ConnectionContext {
    ConnectionContext::new(default_global(server))
}

/// Create a ConnectionContext with JSON output mode.
pub fn ctx_for_server_json(server: &TestServer) -> ConnectionContext {
    ConnectionContext::new(GlobalOpts {
        output: OutputMode::Json,
        ..default_global(server)
    })
}

/// Create a ConnectionContext with metrics URL configured (for liveness/readiness probes).
pub fn ctx_with_metrics(server: &TestServer) -> ConnectionContext {
    ConnectionContext::new(GlobalOpts {
        metrics_url: Some(format!("http://127.0.0.1:{}", server.metrics_port())),
        ..default_global(server)
    })
}

/// Create a ConnectionContext with admin and metrics URLs configured.
/// Only use with servers started with `admin_enabled: true`.
pub fn ctx_with_admin(server: &TestServer) -> ConnectionContext {
    ConnectionContext::new(GlobalOpts {
        admin_url: Some(format!("http://127.0.0.1:{}", server.admin_http_port())),
        metrics_url: Some(format!("http://127.0.0.1:{}", server.metrics_port())),
        ..default_global(server)
    })
}

/// Create a ConnectionContext pointing at an arbitrary port (e.g. for unreachable tests).
pub fn ctx_for_port(port: u16) -> ConnectionContext {
    ConnectionContext::new(GlobalOpts {
        host: "127.0.0.1".to_string(),
        port,
        auth: None,
        user: None,
        tls: false,
        tls_cert: None,
        tls_key: None,
        tls_ca: None,
        admin_url: None,
        metrics_url: None,
        output: OutputMode::Table,
        no_color: true,
    })
}
