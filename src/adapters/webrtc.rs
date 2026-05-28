use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use str0m::net::{Protocol, Receive};
use str0m::{
    Candidate, Event, IceConnectionState, Input, Output, Rtc,
};
use str0m::change::SdpAnswer;
use str0m::channel::ChannelId;

use crate::message::{Message, RtcSignal};
use crate::utils::random_string;
use crate::actor::{Actor, ActorContext};
use async_trait::async_trait;
use log::{debug, error, info, warn};

/// WebRTC peer adapter: P2P data channel over str0m (sans-io WebRTC).
/// Mirrors the WsConn pattern. Signaling flows over the existing mesh.
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
}

impl WebRtcPeer {
    pub fn new(peer_id: String, target_peer_id: String, role: WebRtcRole, allow_public_space: bool, ice_servers: Vec<String>) -> Self {
        Self {
            peer_id,
            target_peer_id,
            role,
            allow_public_space,
            ice_servers,
            tx: None,
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
            Err(e) => { error!("UDP bind failed: {}", e); return None; }
        };
        let local_addr = match std_socket.local_addr() {
            Ok(a) => a,
            Err(e) => { error!("UDP local_addr failed: {}", e); return None; }
        };

        // Host candidate (always present)
        let candidate = match Candidate::host(local_addr, "udp") {
            Ok(c) => c,
            Err(e) => { error!("ICE host candidate failed: {}", e); return None; }
        };
        rtc.add_local_candidate(candidate);

        // STUN discovery for server-reflexive candidates.
        // TURN allocation for relayed candidates when direct/reflexive paths
        // are blocked by symmetric NAT or restrictive firewalls.
        use crate::stun::webrtc_stun::{parse_ice_server, stun_binding_request, turn_allocate_request};
        for uri in ice_servers {
            if let Some((scheme, srv_addr)) = parse_ice_server(uri) {
                match scheme.as_str() {
                    "stun" | "stuns" => {
                        match stun_binding_request(&std_socket, srv_addr, Duration::from_secs(2)) {
                            Some(reflexive_addr) => {
                                info!("STUN discovered reflexive {} via {}", reflexive_addr, uri);
                                match Candidate::server_reflexive(reflexive_addr, local_addr, "udp") {
                                    Ok(c) => { rtc.add_local_candidate(c); }
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
                                    Ok(c) => { rtc.add_local_candidate(c); }
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
            Err(e) => { error!("tokio UdpSocket from_std failed: {}", e); return None; }
        };

        Some((rtc, socket, local_addr))
    }
}

#[async_trait]
impl Actor for WebRtcPeer {
    async fn pre_start(&mut self, ctx: &ActorContext) {
        info!("WebRtcPeer {:?} for {}", self.role, self.peer_id);

        // Register with Router BEFORE setup_rtc so offers can be forwarded.
        let hi = Message::Hi { from: ctx.addr.clone(), peer_id: self.peer_id.clone() };
        let _ = ctx.router.send(hi);

        let (mut rtc, socket, local_addr) = match Self::setup_rtc(&self.ice_servers).await {
            Some(v) => v,
            None => return,
        };

        let (tx, mut rx) = mpsc::unbounded_channel::<WrtcCommand>();
        self.tx = Some(tx);
        let pending_offer = match self.role {
            WebRtcRole::Offerer => self.start_as_offerer(ctx, &mut rtc),
            WebRtcRole::Answerer => { eprintln!("[WRTC] answerer waiting peer_id={} target={}", self.peer_id, self.target_peer_id); None },
        };

        let router = ctx.router.clone();
        let own_addr = ctx.addr.clone();
        let peer_id = self.peer_id.clone();
        let router_target = self.target_peer_id.clone();
        let allow_public_space = self.allow_public_space;

        ctx.child_task(async move {
            let mut buf = vec![0u8; 2000];
            let mut channel_id: Option<ChannelId> = None;
            let mut connected = false;
            let mut pending: Option<str0m::change::SdpPendingOffer> = pending_offer;

            loop {

                // Drain pending commands before str0m I/O to prevent starvation.
                // ICE keepalive Transmits can dominate poll_output() and starve rx.recv()
                // in tokio::select! by returning Transmit before select is ever reached.
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
                            eprintln!("[WRTC] WHILE Signal id={} to={:?} offer={} answer={}", signal.id, signal.to, signal.offer.is_some(), signal.answer.is_some());
                            if let Some(offer_str) = &signal.offer {
                                eprintln!("[WRTC] WHILE offer_str len={}", offer_str.len());
                                match serde_json::from_str::<str0m::change::SdpOffer>(offer_str) {
                                    Ok(offer) => {
                                        eprintln!("[WRTC] WHILE deserialize offer OK");
                                        match rtc.sdp_api().accept_offer(offer) {
                                            Ok(answer) => {
                                                eprintln!("[WRTC] WHILE accept_offer OK, sending answer to {}", router_target);
                                                let answer_str = serde_json::to_string(&answer).unwrap_or_default();
                                                let reply = Message::RtcSignal(RtcSignal {
                                                    id: format!("wrtca{}", random_string(24)),
                                                    from: own_addr.clone(),
                                                    to: Some(router_target.clone()),
                                                    offer: None,
                                                    answer: Some(answer_str),
                                                    candidate: None,
                                                    json_str: None,
                                                });
                                                eprintln!("[WRTC] WHILE answer reply id={} to={:?}", reply.get_id(), router_target);
                                                let r = router.send(reply);
                                                eprintln!("[WRTC] WHILE router.send result: is_ok={}", r.is_ok());
                                            }
                                            Err(e) => eprintln!("[WRTC] WHILE accept_offer FAILED: {:?}", e),
                                        }
                                    }
                                    Err(e) => eprintln!("[WRTC] WHILE deserialize offer FAILED: {:?}", e),
                                }
                            }
                            if let Some(answer_str) = &signal.answer {
                                eprintln!("[WRTC] WHILE answer_str len={}", answer_str.len());
                                if let Some(pending_offer) = pending.take() {
                                    match serde_json::from_str::<SdpAnswer>(answer_str) {
                                        Ok(answer) => {
                                            eprintln!("[WRTC] WHILE deserialize answer OK for {}", peer_id);
                                            match rtc.sdp_api().accept_answer(pending_offer, answer) {
                                                Ok(()) => eprintln!("[WRTC] WHILE accept_answer OK for {}", peer_id),
                                                Err(e) => eprintln!("[WRTC] WHILE accept_answer FAILED for {}: {:?}", peer_id, e),
                                            }
                                        }
                                        Err(e) => eprintln!("[WRTC] WHILE deserialize answer FAILED for {}: {:?}", peer_id, e),
                                    }
                                } else {
                                    eprintln!("[WRTC] WHILE no pending offer for answer {}", peer_id);
                                }
                            }
                            if let Some(candidate_str) = &signal.candidate {
                                eprintln!("[WRTC] WHILE candidate_str for {}", peer_id);
                                if let Ok(candidate) = Candidate::from_sdp_string(candidate_str) {
                                    rtc.add_remote_candidate(candidate);
                                }
                            }
                        }
                        WrtcCommand::Stop => break,
                    }
                }
                let timeout = match rtc.poll_output() {
                    Ok(Output::Timeout(t)) => {
                        if t <= Instant::now() { Duration::ZERO }
                        else { t - Instant::now() }
                    }
                    Ok(Output::Transmit(t)) => {
                        eprintln!("[WRTC] Transmit {} bytes to {} peer={}", t.contents.len(), t.destination, peer_id);
                        let _ = socket.send_to(&t.contents, t.destination).await;
                        continue;
                    }
                    Ok(Output::Event(Event::IceConnectionStateChange(IceConnectionState::Connected))) => {
                        eprintln!("[WRTC] ICE connected peer={}", peer_id);
                        connected = true;
                        continue;
                    }
                    Ok(Output::Event(Event::IceConnectionStateChange(IceConnectionState::Disconnected))) => {
                        warn!("ICE disconnected for {}", peer_id);
                        break;
                    }
                    Ok(Output::Event(Event::ChannelOpen(cid, label))) => {
                        eprintln!("[WRTC] ChannelOpen cid={:?} label={} peer={}", cid, label, peer_id);
                        channel_id = Some(cid);
                        let hi = Message::Hi { from: own_addr.clone(), peer_id: peer_id.clone() };
                        let _ = router.send(hi);
                        continue;
                    }
                    Ok(Output::Event(Event::ChannelData(data))) => {
                        if let Ok(s) = std::str::from_utf8(&data.data) {
                            match Message::try_from(s, own_addr.clone(), allow_public_space) {
                                Ok(msgs) => for m in msgs { let _ = router.send(m); },
                                Err(_) => debug!("bad json from DataChannel"),
                            }
                        }
                        continue;
                    }
                    Ok(Output::Event(Event::ChannelClose(cid))) => {
                        if channel_id == Some(cid) { channel_id = None; }
                        continue;
                    }
                    Err(e) => { error!("poll_output: {:?}", e); break; }
                    _ => { continue; }
                };

                enum LoopResult {
                    Cmd(Option<WrtcCommand>),
                    Recv(Result<(usize, SocketAddr), std::io::Error>),
                    Timeout,
                }

                if timeout.is_zero() {
                    eprintln!("[WRTC] zero-timeout firing peer={}", peer_id);
                    if let Err(e) = rtc.handle_input(Input::Timeout(Instant::now())) {
                        eprintln!("[WRTC] timeout input ERROR peer={}: {:?}", peer_id, e);
                    }
                    continue;
                }

                let result = tokio::select! {
                    cmd = rx.recv() => LoopResult::Cmd(cmd),
                    res = socket.recv_from(&mut buf) => LoopResult::Recv(res),
                    _ = tokio::time::sleep(timeout) => LoopResult::Timeout,
                };

                match result {
                    LoopResult::Cmd(Some(WrtcCommand::Write(data))) => {
                        if let Some(cid) = channel_id {
                            if let Some(mut chan) = rtc.channel(cid) {
                                if chan.write(false, &data).is_err() {
                                    debug!("DataChannel write failed");
                                }
                            }
                        } else if connected {
                            debug!("DataChannel not open, dropping {} bytes", data.len());
                        }
                    }
                    LoopResult::Cmd(Some(WrtcCommand::Signal(signal))) => {
                        eprintln!("[WRTC] MATCH Signal id={} to={:?} offer={} answer={} peer={}", signal.id, signal.to, signal.offer.is_some(), signal.answer.is_some(), peer_id);
                        if let Some(offer_str) = &signal.offer {
                            eprintln!("[WRTC] MATCH offer_str len={} peer={}", offer_str.len(), peer_id);
                            match serde_json::from_str::<str0m::change::SdpOffer>(offer_str) {
                                Ok(offer) => {
                                    eprintln!("[WRTC] MATCH deserialize offer OK peer={}", peer_id);
                                    match rtc.sdp_api().accept_offer(offer) {
                                        Ok(answer) => {
                                            eprintln!("[WRTC] MATCH accept_offer OK peer={}", peer_id);
                                            let answer_str = serde_json::to_string(&answer).unwrap_or_default();
                                            let reply = Message::RtcSignal(RtcSignal {
                                                id: format!("wrtca{}", random_string(24)),
                                                from: own_addr.clone(),
                                                to: Some(router_target.clone()),
                                                offer: None,
                                                answer: Some(answer_str),
                                                candidate: None,
                                                json_str: None,
                                            });
                                            eprintln!("[WRTC] MATCH answer reply id={} to={:?} peer={}", reply.get_id(), router_target, peer_id);
                                            let r = router.send(reply);
                                            eprintln!("[WRTC] MATCH router.send result: is_ok={} peer={}", r.is_ok(), peer_id);
                                        }
                                        Err(e) => eprintln!("[WRTC] MATCH accept_offer FAILED peer={}: {:?}", peer_id, e),
                                    }
                                }
                                Err(e) => eprintln!("[WRTC] MATCH deserialize offer FAILED peer={}: {:?}", peer_id, e),
                            }
                        }
                        if let Some(answer_str) = &signal.answer {
                            eprintln!("[WRTC] MATCH answer_str len={} peer={}", answer_str.len(), peer_id);
                            if let Some(pending_offer) = pending.take() {
                                match serde_json::from_str::<SdpAnswer>(answer_str) {
                                    Ok(answer) => {
                                        eprintln!("[WRTC] MATCH deserialize answer OK peer={}", peer_id);
                                        match rtc.sdp_api().accept_answer(pending_offer, answer) {
                                            Ok(()) => eprintln!("[WRTC] MATCH accept_answer OK peer={}", peer_id),
                                            Err(e) => eprintln!("[WRTC] MATCH accept_answer FAILED peer={}: {:?}", peer_id, e),
                                        }
                                    }
                                    Err(e) => eprintln!("[WRTC] MATCH deserialize answer FAILED peer={}: {:?}", peer_id, e),
                                }
                            } else {
                                eprintln!("[WRTC] MATCH no pending offer for answer peer={}", peer_id);
                            }
                        }
                        if let Some(candidate_str) = &signal.candidate {
                            eprintln!("[WRTC] MATCH candidate_str peer={}", peer_id);
                            if let Ok(candidate) = Candidate::from_sdp_string(candidate_str) {
                                rtc.add_remote_candidate(candidate);
                            }
                        }
                    }
                    LoopResult::Cmd(Some(WrtcCommand::Stop)) |
                    LoopResult::Cmd(None) => break,
                    LoopResult::Recv(Ok((n, source))) => {
                        eprintln!("[WRTC] Recv {} bytes from {} peer={}", n, source, peer_id);
                        let input = Input::Receive(
                            Instant::now(),
                            Receive {
                                proto: Protocol::Udp,
                                source,
                                destination: local_addr,
                                contents: buf[..n].as_ref().try_into()
                                    .expect("UDP packet fits in buffer"),
                            }
                        );
                        if let Err(e) = rtc.handle_input(input) {
                            eprintln!("[WRTC] handle_input ERROR peer={}: {:?}", peer_id, e);
                        }
                    }
                    LoopResult::Recv(Err(e)) => {
                        error!("UDP recv: {:?}", e);
                    }
                    LoopResult::Timeout => {
                        if let Err(e) = rtc.handle_input(Input::Timeout(Instant::now())) {
                            eprintln!("[WRTC] timeout input ERROR peer={}: {:?}", peer_id, e);
                        }
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
                    if signal.to.as_ref() != Some(&self.peer_id) { return; }
                    debug!("signal from {}", signal.from);
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
        // Signal the child task to shut down gracefully.
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(WrtcCommand::Stop);
        }
    }
}

impl WebRtcPeer {
    fn start_as_offerer(&self, ctx: &ActorContext, rtc: &mut Rtc) -> Option<str0m::change::SdpPendingOffer> {
        eprintln!("[WRTC] start_as_offerer peer_id={} target={}", self.peer_id, self.target_peer_id);
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
            json_str: None,
        });
        let _ = ctx.router.send(signal);
        Some(pending)
    }
}
