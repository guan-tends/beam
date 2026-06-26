//! STUN/TURN helpers for WebRTC ICE candidate discovery.
//!
//! Provides synchronous (blocking) STUN Binding Request and TURN Allocate
//! Request over a bound UDP socket. Used during `WebRtcPeer::setup_rtc()`
//! before the async runtime takes over the socket.

#[cfg(feature = "webrtc")]
pub mod webrtc_stun {
    use std::net::{SocketAddr, UdpSocket};
    use std::time::Duration;

    /// Parse an ICE server URI into a `(scheme, SocketAddr)` tuple.
    ///
    /// Supported schemes: `stun:`, `stuns:`, `turn:`, `turns:`.
    /// Falls back to treating the whole string as `host:port` if no scheme.
    /// DNS names are NOT resolved here — users should pre-resolve to IP literals.
    pub fn parse_ice_server(uri: &str) -> Option<(String, SocketAddr)> {
        let uri = uri.trim();

        let (scheme, rest) = if let Some(stripped) = uri.strip_prefix("stun:") {
            ("stun", stripped)
        } else if let Some(stripped) = uri.strip_prefix("stuns:") {
            ("stuns", stripped)
        } else if let Some(stripped) = uri.strip_prefix("turn:") {
            ("turn", stripped)
        } else if let Some(stripped) = uri.strip_prefix("turns:") {
            ("turns", stripped)
        } else {
            ("stun", uri)
        };

        let rest = rest.trim_start_matches('/');

        let (host, port) = if let Some(pos) = rest.rfind(':') {
            let h = &rest[..pos];
            let p = &rest[pos + 1..];
            (h, p.parse::<u16>().ok()?)
        } else {
            (rest, 3478u16)
        };

        let addr = format!("{}:{}", host, port).parse::<SocketAddr>().ok()?;
        Some((scheme.to_string(), addr))
    }

    /// Perform a STUN Binding Request to discover our server-reflexive address.
    ///
    /// Blocks with `UdpSocket::set_read_timeout`.
    /// Returns the reflexive (public) address or `None` on failure.
    pub fn stun_binding_request(
        local_socket: &UdpSocket,
        stun_server: SocketAddr,
        timeout: Duration,
    ) -> Option<SocketAddr> {
        use std::io::Cursor;
        use stun::attributes::ATTR_XORMAPPED_ADDRESS;
        use stun::message::{BINDING_REQUEST, Message};
        use stun::xoraddr::XorMappedAddress;

        let mut request = Message::new();
        request.new_transaction_id().ok()?;
        request.typ = BINDING_REQUEST;

        let mut buf = vec![0u8; 1024];
        let n = request.write_to(&mut &mut buf[..]).ok()?;
        let req_bytes = &buf[..n];

        local_socket.send_to(req_bytes, stun_server).ok()?;

        local_socket.set_read_timeout(Some(timeout)).ok()?;
        let mut resp_buf = [0u8; 1024];
        let (n, from) = local_socket.recv_from(&mut resp_buf).ok()?;
        if from != stun_server {
            return None;
        }

        let mut response = Message::new();
        let mut cursor = Cursor::new(&resp_buf[..n]);
        response.read_from(&mut cursor).ok()?;

        let mut xma = XorMappedAddress::default();
        xma.get_from_as(&response, ATTR_XORMAPPED_ADDRESS).ok()?;
        Some(SocketAddr::new(xma.ip, xma.port))
    }

    /// Perform a TURN Allocate Request to obtain a relayed address.
    ///
    /// Sends an Allocate request with REQUESTED-TRANSPORT = UDP.
    /// Parses the XOR-RELAYED-ADDRESS from the success response.
    /// Returns the relayed SocketAddr or None on failure.
    ///
    /// NOTE: This is a minimal synchronous allocation. It does NOT handle:
    /// - Authentication (long-term credential mechanism)
    /// - Create-Permission for peer addresses
    /// - Allocation refresh / lifetime management
    /// These are deferred to future work or managed by the caller.
    pub fn turn_allocate_request(
        local_socket: &UdpSocket,
        turn_server: SocketAddr,
        timeout: Duration,
    ) -> Option<SocketAddr> {
        use std::io::Cursor;
        use stun::attributes::{ATTR_REQUESTED_TRANSPORT, ATTR_XOR_RELAYED_ADDRESS};
        use stun::message::{CLASS_REQUEST, METHOD_ALLOCATE, Message, MessageType};
        use stun::xoraddr::XorMappedAddress;

        let mut request = Message::new();
        request.new_transaction_id().ok()?;
        request.typ = MessageType::new(METHOD_ALLOCATE, CLASS_REQUEST);

        // REQUESTED-TRANSPORT = UDP (protocol number 17 per RFC 5766)
        request.add(ATTR_REQUESTED_TRANSPORT, &[17u8, 0, 0, 0]);

        let mut buf = vec![0u8; 1024];
        let n = request.write_to(&mut &mut buf[..]).ok()?;
        let req_bytes = &buf[..n];

        local_socket.send_to(req_bytes, turn_server).ok()?;

        local_socket.set_read_timeout(Some(timeout)).ok()?;
        let mut resp_buf = [0u8; 1024];
        let (n, from) = local_socket.recv_from(&mut resp_buf).ok()?;
        if from != turn_server {
            return None;
        }

        let mut response = Message::new();
        let mut cursor = Cursor::new(&resp_buf[..n]);
        response.read_from(&mut cursor).ok()?;

        let mut xra = XorMappedAddress::default();
        xra.get_from_as(&response, ATTR_XOR_RELAYED_ADDRESS).ok()?;
        Some(SocketAddr::new(xra.ip, xra.port))
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

    pub fn turn_allocate_request(
        _local_socket: &std::net::UdpSocket,
        _turn_server: SocketAddr,
        _timeout: std::time::Duration,
    ) -> Option<SocketAddr> {
        None
    }
}
