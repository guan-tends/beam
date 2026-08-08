//! WebRTC peer adapter — P2P data channels over str0m (sans-io WebRTC).
//!
//! [`WebRtcPeer`] establishes direct P2P connections to remote peers using
//! WebRTC data channels. Signaling (SDP offer/answer and ICE candidates)
//! flows over the existing WebSocket mesh via [`Message::RtcSignal`].
//!
//! # Architecture
//!
//! ```text
//!   Peer A (Offerer)                    Peer B (Answerer)
//!   ┌─────────────┐                     ┌─────────────┐
//!   │ WebRtcPeer  │ ── RtcSignal ──→    │ WebRtcPeer  │
//!   │             │ ←── (via WS) ───    │             │
//!   │ str0m Rtc   │                     │ str0m Rtc   │
//!   │ UDP socket  │ ←── P2P data ──→    │ UDP socket  │
//!   └─────────────┘                     └─────────────┘
//! ```
//!
//! # ICE Negotiation
//!
//! 1. Each peer binds a UDP socket and queries STUN/TURN servers for candidates
//! 2. The offerer creates an SDP offer and sends it via `RtcSignal`
//! 3. The answerer accepts the offer and sends back an SDP answer
//! 4. ICE candidates are exchanged via `RtcSignal` messages
//! 5. Once connected, Gun protocol messages flow over the data channel
//!
//! # Channel
//!
//! A single data channel labeled `"gun-mesh"` is created. All Gun protocol
//! messages (Put, Get, etc.) are serialized as text and sent over this channel.
//!
//! Requires the `webrtc` feature.

use std::net::SocketAddr;
use std::sync::Arc;
use web_time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use str0m::change::SdpAnswer;
use str0m::channel::ChannelId;
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, IceConnectionState, Input, Output, Rtc};

use crate::actor::{Actor, ActorContext};
use crate::message::{Message, RtcSignal};
use crate::utils::random_string;
use async_trait::async_trait;
use log::{debug, error, info, warn};

/// Role in the WebRTC connection — offerer initiates, answerer responds.
#[derive(Clone, Debug)]
pub enum WebRtcRole {
    Offerer,
    Answerer,
}

#[derive(Clone, Debug)]
enum WrtcCommand {
    Write(Vec<u8>),
    Signal(RtcSignal),
    Stop,
}

pub struct WebRtcPeer {
    peer_id: String,
    target_peer_id: String,
    role: WebRtcRole,
    /// Mirrors Node Config.allow_public_space. Passed through to Message::try_from
    /// for ChannelData inbound parsing.
    allow_public_space: bool,
    /// ICE server URIs for STUN discovery and TURN relay.
    ice_servers: Vec<String>,
    tx: Option<mpsc::UnboundedSender<WrtcCommand>>,
    /// Local UDP socket address, shared with remote peer as an ICE candidate.
    local_addr: Option<SocketAddr>,
}

impl WebRtcPeer {
    pub fn new(
        peer_id: String,
        target_peer_id: String,
        role: WebRtcRole,
        allow_public_space: bool,
        ice_servers: Vec<String>,
    ) -> Self {
        Self {
            peer_id,
            target_peer_id,
            role,
            allow_public_space,
            ice_servers,
            tx: None,
            local_addr: None,
        }
    }

    /// Create a str0m `Rtc`, bind a UDP socket, and discover ICE candidates.
    ///
    /// 1. Binds a `std::net::UdpSocket` (blocking) to query STUN / TURN servers.
    /// 2. Adds a `host` candidate from the local socket address.
    /// 3. For each `stun:` URI, sends a Binding Request and adds a
    ///    `server_reflexive` candidate on success.
    /// 4. For each `turn:` URI, sends an Allocate Request and adds a
    ///    `relayed` candidate on success.
    /// 5. Hands the socket to `tokio::net::UdpSocket` for async I/O.
    async fn setup_rtc(ice_servers: &[String]) -> Option<(Rtc, Arc<UdpSocket>, SocketAddr)> {
        let mut rtc = Rtc::new(Instant::now());

        // Bind a std socket first so we can do blocking STUN/TURN queries
        // before handing the socket to the async runtime.
        let std_socket = match std::net::UdpSocket::bind("127.0.0.1:0") {
            Ok(s) => s,
            Err(e) => {
                error!("UDP bind failed: {}", e);
                return None;
            }
        };
        let local_addr = match std_socket.local_addr() {
            Ok(a) => a,
            Err(e) => {
                error!("UDP local_addr failed: {}", e);
                return None;
            }
        };

        // Host candidate (always present)
        let candidate = match Candidate::host(local_addr, "udp") {
            Ok(c) => c,
            Err(e) => {
                error!("ICE host candidate failed: {}", e);
                return None;
            }
        };
        rtc.add_local_candidate(candidate);

        // STUN discovery for server-reflexive candidates.
        // TURN allocation for relayed candidates when direct/reflexive paths
        // are blocked by symmetric NAT or restrictive firewalls.
        use crate::stun::webrtc_stun::{
            parse_ice_server, stun_binding_request, turn_allocate_request,
        };
        for uri in ice_servers {
            if let Some((scheme, srv_addr)) = parse_ice_server(uri) {
                match scheme.as_str() {
                    "stun" | "stuns" => {
                        match stun_binding_request(&std_socket, srv_addr, Duration::from_secs(2)) {
                            Some(reflexive_addr) => {
                                info!("STUN discovered reflexive {} via {}", reflexive_addr, uri);
                                match Candidate::server_reflexive(reflexive_addr, local_addr, "udp")
                                {
                                    Ok(c) => {
                                        rtc.add_local_candidate(c);
                                    }
                                    Err(e) => warn!("server-reflexive candidate failed: {}", e),
                                }
                            }
                            None => warn!("STUN query failed for {}", uri),
                        }
                    }
                    "turn" | "turns" => {
                        match turn_allocate_request(&std_socket, srv_addr, Duration::from_secs(3)) {
                            Some(relay_addr) => {
                                info!("TURN allocated relay {} via {}", relay_addr, uri);
                                match Candidate::relayed(relay_addr, local_addr, "udp") {
                                    Ok(c) => {
                                        rtc.add_local_candidate(c);
                                    }
                                    Err(e) => warn!("relayed candidate failed: {}", e),
                                }
                            }
                            None => warn!("TURN allocation failed for {}", uri),
                        }
                    }
                    _ => {}
                }
            }
        }

        // Hand the socket over to tokio
        let _ = std_socket.set_nonblocking(true);
        let socket = match UdpSocket::from_std(std_socket) {
            Ok(s) => Arc::new(s),
            Err(e) => {
                error!("tokio UdpSocket from_std failed: {}", e);
                return None;
            }
        };

        Some((rtc, socket, local_addr))
    }
}

#[async_trait]
impl Actor for WebRtcPeer {
    async fn pre_start(&mut self, ctx: &ActorContext) {
        info!("WebRtcPeer {:?} for {}", self.role, self.peer_id);

        // Register with Router BEFORE setup_rtc so offers can be forwarded.
        let hi = Message::Hi {
            from: ctx.addr.clone(),
            peer_id: self.peer_id.clone(),
        };
        let _ = ctx.router.send(hi);

        let (mut rtc, socket, local_addr) = match Self::setup_rtc(&self.ice_servers).await {
            Some(v) => v,
            None => return,
        };

        self.local_addr = Some(local_addr);
        let (tx, mut rx) = mpsc::unbounded_channel::<WrtcCommand>();
        self.tx = Some(tx);
        let local_addr_for_signal = local_addr;
        let pending_offer = match self.role {
            WebRtcRole::Offerer => self.start_as_offerer(ctx, &mut rtc, local_addr_for_signal),
            WebRtcRole::Answerer => {
                debug!(
                    "[WRTC] answerer waiting peer_id={} target={}",
                    self.peer_id, self.target_peer_id
                );
                None
            }
        };

        let router = ctx.router.clone();
        let own_addr = ctx.addr.clone();
        let peer_id = self.peer_id.clone();
        let router_target = self.target_peer_id.clone();
        let allow_public_space = self.allow_public_space;

        ctx.child_task(async move {
            let mut buf = vec![0u8; 2000];
            let mut channel_id: Option<ChannelId> = None;
            let mut pending: Option<str0m::change::SdpPendingOffer> = pending_offer;

            loop {
                // === 1. Process pending commands (non-blocking) ===
                while let Ok(cmd) = rx.try_recv() {
                    match cmd {
                        WrtcCommand::Write(data) => {
                            if let Some(cid) = channel_id {
                                if let Some(mut chan) = rtc.channel(cid) {
                                    let _ = chan.write(false, &data);
                                }
                            }
                        }
                        WrtcCommand::Signal(signal) => {
                            // Add remote candidate from peer's local_addr BEFORE processing offer/answer
                            if let Some(addr_str) = &signal.local_addr {
                                if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                                    if let Ok(candidate) = Candidate::host(addr, "udp") {
                                        rtc.add_remote_candidate(candidate);
                                    }
                                }
                            }
                            if let Some(offer_str) = &signal.offer {
                                if let Ok(offer) =
                                    serde_json::from_str::<str0m::change::SdpOffer>(offer_str)
                                {
                                    if let Ok(answer) = rtc.sdp_api().accept_offer(offer) {
                                        let answer_str =
                                            serde_json::to_string(&answer).unwrap_or_default();
                                        let reply = Message::RtcSignal(RtcSignal {
                                            id: format!("wrtca{}", random_string(24)),
                                            from: own_addr.clone(),
                                            to: Some(router_target.clone()),
                                            offer: None,
                                            answer: Some(answer_str),
                                            candidate: None,
                                            local_addr: Some(local_addr.to_string()),
                                            json_str: None,
                                        });
                                        let _ = router.send(reply);
                                    }
                                }
                            }
                            if let Some(answer_str) = &signal.answer {
                                if let Some(pending_offer) = pending.take() {
                                    if let Ok(answer) =
                                        serde_json::from_str::<SdpAnswer>(answer_str)
                                    {
                                        let _ = rtc.sdp_api().accept_answer(pending_offer, answer);
                                    }
                                }
                            }
                            if let Some(candidate_str) = &signal.candidate {
                                if let Ok(candidate) = Candidate::from_sdp_string(candidate_str) {
                                    rtc.add_remote_candidate(candidate);
                                }
                            }
                        }
                        WrtcCommand::Stop => break,
                    }
                }

                // === 2. Advance time (drives ICE/DTLS/SCTP state machines) ===
                if let Err(e) = rtc.handle_input(Input::Timeout(Instant::now())) {
                    error!("handle_input Timeout failed: {:?}", e);
                }

                // === 3. Drain ALL outputs until Timeout ===
                let mut timeout = Instant::now() + Duration::from_millis(100);
                loop {
                    match rtc.poll_output() {
                        Ok(Output::Transmit(t)) => {
                            let _ = socket.send_to(&t.contents, t.destination).await;
                        }
                        Ok(Output::Event(Event::IceConnectionStateChange(
                            IceConnectionState::Connected,
                        ))) => {
                            debug!("webrtc: ICE connected for peer {}", peer_id);
                        }
                        Ok(Output::Event(Event::IceConnectionStateChange(
                            IceConnectionState::Disconnected,
                        ))) => {
                            break;
                        }
                        Ok(Output::Event(Event::ChannelOpen(cid, _label))) => {
                            channel_id = Some(cid);
                            let hi = Message::Hi {
                                from: own_addr.clone(),
                                peer_id: peer_id.clone(),
                            };
                            let _ = router.send(hi);
                        }
                        Ok(Output::Event(Event::ChannelData(data))) => {
                            if let Ok(s) = std::str::from_utf8(&data.data) {
                                match Message::try_from(s, own_addr.clone(), allow_public_space) {
                                    Ok(msgs) => {
                                        for m in msgs {
                                            let _ = router.send(m);
                                        }
                                    }
                                    Err(e) => debug!("webrtc: failed to parse channel data: {}", e),
                                }
                            }
                        }
                        Ok(Output::Event(Event::ChannelClose(cid))) => {
                            if channel_id == Some(cid) {
                                channel_id = None;
                            }
                        }
                        Ok(Output::Timeout(t)) => {
                            timeout = t;
                            break;
                        }
                        Err(e) => {
                            error!("poll_output: {:?}", e);
                            break;
                        }
                        _ => {}
                    }
                }

                // === 4. Calculate sleep duration ===
                let now = Instant::now();
                let sleep_dur = if timeout > now {
                    timeout - now
                } else {
                    Duration::from_millis(1)
                };

                // === 5. Wait for next event (commands, UDP, or timeout) ===
                enum LoopResult {
                    Cmd(Option<WrtcCommand>),
                    Recv(Result<(usize, SocketAddr), std::io::Error>),
                    Timeout,
                }

                let result = tokio::select! {
                    cmd = rx.recv() => LoopResult::Cmd(cmd),
                    res = socket.recv_from(&mut buf) => LoopResult::Recv(res),
                    _ = crate::tokio_time::sleep(sleep_dur) => LoopResult::Timeout,
                };

                match result {
                    LoopResult::Cmd(Some(WrtcCommand::Write(data))) => {
                        if let Some(cid) = channel_id {
                            if let Some(mut chan) = rtc.channel(cid) {
                                let _ = chan.write(false, &data);
                            }
                        }
                    }
                    LoopResult::Cmd(Some(WrtcCommand::Signal(signal))) => {
                        if let Some(addr_str) = &signal.local_addr {
                            if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                                if let Ok(candidate) = Candidate::host(addr, "udp") {
                                    rtc.add_remote_candidate(candidate);
                                }
                            }
                        }
                        if let Some(offer_str) = &signal.offer {
                            if let Ok(offer) =
                                serde_json::from_str::<str0m::change::SdpOffer>(offer_str)
                            {
                                if let Ok(answer) = rtc.sdp_api().accept_offer(offer) {
                                    let answer_str =
                                        serde_json::to_string(&answer).unwrap_or_default();
                                    let reply = Message::RtcSignal(RtcSignal {
                                        id: format!("wrtca{}", random_string(24)),
                                        from: own_addr.clone(),
                                        to: Some(router_target.clone()),
                                        offer: None,
                                        answer: Some(answer_str),
                                        candidate: None,
                                        local_addr: Some(local_addr.to_string()),
                                        json_str: None,
                                    });
                                    let _ = router.send(reply);
                                }
                            }
                        }
                        if let Some(answer_str) = &signal.answer {
                            if let Some(pending_offer) = pending.take() {
                                if let Ok(answer) = serde_json::from_str::<SdpAnswer>(answer_str) {
                                    let _ = rtc.sdp_api().accept_answer(pending_offer, answer);
                                }
                            }
                        }
                        if let Some(candidate_str) = &signal.candidate {
                            if let Ok(candidate) = Candidate::from_sdp_string(candidate_str) {
                                rtc.add_remote_candidate(candidate);
                            }
                        }
                    }
                    LoopResult::Cmd(Some(WrtcCommand::Stop)) | LoopResult::Cmd(None) => break,
                    LoopResult::Recv(Ok((n, source))) => {
                        let input = Input::Receive(
                            Instant::now(),
                            Receive {
                                proto: Protocol::Udp,
                                source,
                                destination: local_addr,
                                contents: buf[..n]
                                    .as_ref()
                                    .try_into()
                                    .expect("UDP packet fits in buffer"),
                            },
                        );
                        if let Err(e) = rtc.handle_input(input) {
                            error!("handle_input: {:?}", e);
                        }
                    }
                    LoopResult::Recv(Err(e)) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    LoopResult::Recv(Err(e)) => {
                        error!("UDP recv: {:?}", e);
                    }
                    LoopResult::Timeout => {
                        // Time already advanced above; loop back to poll_output
                    }
                }
            }

            info!("WebRtcPeer task exited for {}", peer_id);
        });
    }

    async fn handle(&mut self, msg: Message, _ctx: &ActorContext) {
        if let Some(tx) = &self.tx {
            match msg {
                Message::RtcSignal(signal) => {
                    if signal.to.as_ref() != Some(&self.peer_id) {
                        return;
                    }
                    let _ = tx.send(WrtcCommand::Signal(signal));
                }
                other => {
                    let text = other.to_string();
                    let _ = tx.send(WrtcCommand::Write(text.into_bytes()));
                }
            }
        }
    }

    async fn stopping(&mut self, _ctx: &ActorContext) {
        info!("WebRtcPeer stopping for {}", self.peer_id);
        // Dual shutdown path: (1) WrtcCommand::Stop signals the child task
        // to end the ICE session gracefully, and (2) ActorContext::stop()
        // aborts the child task handle as a fallback. Either path triggers
        // clean shutdown — Stop is preferred, abort is the safety net.
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(WrtcCommand::Stop);
        }
    }
}

impl WebRtcPeer {
    fn start_as_offerer(
        &self,
        ctx: &ActorContext,
        rtc: &mut Rtc,
        local_addr: SocketAddr,
    ) -> Option<str0m::change::SdpPendingOffer> {
        let mut changes = rtc.sdp_api();
        let _cid = changes.add_channel("gun-mesh".to_string());
        let (offer, pending) = changes.apply()?;
        let offer_str = serde_json::to_string(&offer).unwrap_or_default();
        let signal = Message::RtcSignal(RtcSignal {
            id: format!("wrtco{}", random_string(24)),
            from: ctx.addr.clone(),
            to: Some(self.target_peer_id.clone()),
            offer: Some(offer_str),
            answer: None,
            candidate: None,
            local_addr: Some(local_addr.to_string()),
            json_str: None,
        });
        let _ = ctx.router.send(signal);
        Some(pending)
    }
}
