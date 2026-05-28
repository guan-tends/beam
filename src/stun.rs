//! STUN/TURN helpers for WebRTC ICE candidate discovery.
//!
//! Provides synchronous (blocking) STUN Binding Request over a bound UDP socket.
//! Used during `WebRtcPeer::setup_rtc()` before the async runtime takes over the socket.

#[cfg(feature = "webrtc")]
pub mod webrtc_stun {
    use std::net::{SocketAddr, UdpSocket};
    use std::time::Duration;

    /// Parse an ICE server URI into a `(scheme, SocketAddr)` tuple.
    ///
    /// Supported schemes: `stun:`, `stuns:`, `turn:`, `turns:`.
    /// Falls back to treating the whole string as `host:port` if no scheme.
    /// DNS names are NOT resolved here — users should pre-resolve to IP literals
    /// or we rely on the OS resolver at runtime.
    pub fn parse_ice_server(uri: &str) -> Option<(String, SocketAddr)> {
        let uri = uri.trim();

        // Detect scheme
        let (scheme, rest) = if let Some(stripped) = uri.strip_prefix("stun:") {
            ("stun", stripped)
        } else if let Some(stripped) = uri.strip_prefix("stuns:") {
            ("stuns", stripped)
        } else if let Some(stripped) = uri.strip_prefix("turn:") {
            ("turn", stripped)
        } else if let Some(stripped) = uri.strip_prefix("turns:") {
            ("turns", stripped)
        } else {
            // No scheme — treat whole thing as host:port with stun default
            ("stun", uri)
        };

        let rest = rest.trim_start_matches('/');

        // Split host:port
        let (host, port) = if let Some(pos) = rest.rfind(':') {
            let h = &rest[..pos];
            let p = &rest[pos + 1..];
            (h, p.parse::<u16>().ok()?)
        } else {
            // Default STUN/TURN port per RFC 5766 / RFC 5389
            (rest, 3478u16)
        };

        // Try to parse as SocketAddr (handles IP literals)
        let addr = format!("{}:{}", host, port).parse::<SocketAddr>().ok()?;
        Some((scheme.to_string(), addr))
    }

    /// Perform a STUN Binding Request to discover our server-reflexive address.
    ///
    /// Blocks with `UdpSocket::set_read_timeout` — call before the socket is
    /// moved into the async runtime. Returns the reflexive (public) address
    /// or `None` on any failure.
    pub fn stun_binding_request(
        local_socket: &UdpSocket,
        stun_server: SocketAddr,
        timeout: Duration,
    ) -> Option<SocketAddr> {
        use stun::message::{Message, BINDING_REQUEST};
        use stun::xoraddr::XorMappedAddress;
        use stun::attributes::ATTR_XORMAPPED_ADDRESS;
        use std::io::Cursor;

        // Build Binding Request
        let mut request = Message::new();
        request.new_transaction_id().ok()?;
        request.typ = BINDING_REQUEST;

        let mut buf = vec![0u8; 1024];
        let n = request.write_to(&mut &mut buf[..]).ok()?;
        let req_bytes = &buf[..n];

        // Send
        local_socket.send_to(req_bytes, stun_server).ok()?;

        // Receive with timeout
        local_socket.set_read_timeout(Some(timeout)).ok()?;
        let mut resp_buf = [0u8; 1024];
        let (n, from) = local_socket.recv_from(&mut resp_buf).ok()?;

        // Basic sanity: response should come from the STUN server we queried
        if from != stun_server {
            return None;
        }

        // Parse response
        let mut response = Message::new();
        let mut cursor = Cursor::new(&resp_buf[..n]);
        response.read_from(&mut cursor).ok()?;

        // Extract XOR-MAPPED-ADDRESS using the public inherent method
        let mut xma = XorMappedAddress::default();
        xma.get_from_as(&response, ATTR_XORMAPPED_ADDRESS).ok()?;

        Some(SocketAddr::new(xma.ip, xma.port))
    }
}

#[cfg(not(feature = "webrtc"))]
pub mod webrtc_stun {
    use std::net::SocketAddr;

    pub fn parse_ice_server(_uri: &str) -> Option<(String, SocketAddr)> {
        None
    }

    pub fn stun_binding_request(
        _local_socket: &std::net::UdpSocket,
        _stun_server: SocketAddr,
        _timeout: std::time::Duration,
    ) -> Option<SocketAddr> {
        None
    }
}
