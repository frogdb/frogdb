/// New connection to be handled by a shard.
pub struct NewConnection {
    /// The TCP socket.
    pub socket: tokio::net::TcpStream,
    /// Client address.
    pub addr: std::net::SocketAddr,
    /// Connection ID.
    pub conn_id: u64,
}

impl std::fmt::Debug for NewConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NewConnection")
            .field("addr", &self.addr)
            .field("conn_id", &self.conn_id)
            .finish()
    }
}
