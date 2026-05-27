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
use crate::actor::{Actor, ActorContext, Addr};
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
    role: WebRtcRole,
    /// Mirrors Node Config.allow_public_space. Passed through to Message::try_from
    /// for ChannelData inbound parsing.
    allow_public_space: bool,
    tx: Option<mpsc::UnboundedSender<WrtcCommand>>,
}

impl WebRtcPeer {
    pub fn new(peer_id: String, role: WebRtcRole, allow_public_space: bool) -> Self {
        Self {
            peer_id,
            role,
            allow_public_space,
            tx: None,
        }
    }

    async fn setup_rtc() -> Option<(Rtc, Arc<UdpSocket>, SocketAddr)> {
        let mut rtc = Rtc::new(Instant::now());
        let socket = match UdpSocket::bind("0.0.0.0:0").await {
            Ok(s) => s,
            Err(e) => { error!("UDP bind failed: {}", e); return None; }
        };
        let local_addr = match socket.local_addr() {
            Ok(a) => a,
            Err(e) => { error!("UDP local_addr failed: {}", e); return None; }
        };
        let socket = Arc::new(socket);
        let candidate = match Candidate::host(local_addr, "udp") {
            Ok(c) => c,
            Err(e) => { error!("ICE candidate failed: {}", e); return None; }
        };
        rtc.add_local_candidate(candidate);
        Some((rtc, socket, local_addr))
    }
}

#[async_trait]
impl Actor for WebRtcPeer {
    async fn pre_start(&mut self, ctx: &ActorContext) {
        info!("WebRtcPeer {:?} for {}", self.role, self.peer_id);

        let (mut rtc, socket, local_addr) = match Self::setup_rtc().await {
            Some(v) => v,
            None => return,
        };

        let (tx, mut rx) = mpsc::unbounded_channel::<WrtcCommand>();
        self.tx = Some(tx);
        let pending_offer = match self.role {
            WebRtcRole::Offerer => self.start_as_offerer(ctx, &mut rtc),
            WebRtcRole::Answerer => None,
        };

        let router = ctx.router.clone();
        let own_addr = ctx.addr.clone();
        let peer_id = self.peer_id.clone();
        let allow_public_space = self.allow_public_space;

        ctx.child_task(async move {
            let mut buf = vec![0u8; 2000];
            let mut channel_id: Option<ChannelId> = None;
            let mut connected = false;
            let mut pending: Option<str0m::change::SdpPendingOffer> = pending_offer;

            loop {
                let timeout = match rtc.poll_output() {
                    Ok(Output::Timeout(t)) => {
                        if t <= Instant::now() { Duration::ZERO }
                        else { t - Instant::now() }
                    }
                    Ok(Output::Transmit(t)) => {
                        let _ = socket.send_to(&t.contents, t.destination).await;
                        continue;
                    }
                    Ok(Output::Event(Event::IceConnectionStateChange(IceConnectionState::Connected))) => {
                        info!("ICE connected for {}", peer_id);
                        connected = true;
                        continue;
                    }
                    Ok(Output::Event(Event::IceConnectionStateChange(IceConnectionState::Disconnected))) => {
                        warn!("ICE disconnected for {}", peer_id);
                        break;
                    }
                    Ok(Output::Event(Event::ChannelOpen(cid, label))) => {
                        info!("channel {} open for {}", label, peer_id);
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

                let result = if timeout.is_zero() {
                    tokio::select! {
                        cmd = rx.recv() => LoopResult::Cmd(cmd),
                        res = socket.recv_from(&mut buf) => LoopResult::Recv(res),
                    }
                } else {
                    tokio::select! {
                        cmd = rx.recv() => LoopResult::Cmd(cmd),
                        res = socket.recv_from(&mut buf) => LoopResult::Recv(res),
                        _ = tokio::time::sleep(timeout) => LoopResult::Timeout,
                    }
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
                        if let Some(offer_str) = &signal.offer {
                            if let Ok(offer) = serde_json::from_str::<str0m::change::SdpOffer>(offer_str) {
                                match rtc.sdp_api().accept_offer(offer) {
                                    Ok(answer) => {
                                        let answer_str = serde_json::to_string(&answer).unwrap_or_default();
                                        let reply = Message::RtcSignal(RtcSignal {
                                            id: format!("wrtc-answer-{}", peer_id),
                                            from: own_addr.clone(),
                                            to: Some(peer_id.clone()),
                                            offer: None,
                                            answer: Some(answer_str),
                                            candidate: None,
                                            json_str: None,
                                        });
                                        let _ = router.send(reply);
                                    }
                                    Err(e) => error!("accept_offer: {:?}", e),
                                }
                            }
                        }
                        if let Some(answer_str) = &signal.answer {
                            if let Some(pending) = pending.take() {
                                if let Ok(answer) = serde_json::from_str::<SdpAnswer>(answer_str) {
                                    if let Err(e) = rtc.sdp_api().accept_answer(pending, answer) {
                                        error!("accept_answer: {:?}", e);
                                    }
                                }
                            }
                        }
                        if let Some(candidate_str) = &signal.candidate {
                            if let Ok(candidate) = Candidate::from_sdp_string(candidate_str) {
                                rtc.add_remote_candidate(candidate);
                            }
                        }
                    }
                    LoopResult::Cmd(Some(WrtcCommand::Stop)) |
                    LoopResult::Cmd(None) => break,
                    LoopResult::Recv(Ok((n, source))) => {
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
                            error!("handle_input: {:?}", e);
                        }
                    }
                    LoopResult::Recv(Err(e)) => {
                        error!("UDP recv: {:?}", e);
                    }
                    LoopResult::Timeout => {
                        if let Err(e) = rtc.handle_input(Input::Timeout(Instant::now())) {
                            error!("timeout input: {:?}", e);
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
        let mut changes = rtc.sdp_api();
        let _cid = changes.add_channel("gun-mesh".to_string());
        let (offer, pending) = changes.apply()?;
        let offer_str = offer.to_string();
        let signal = Message::RtcSignal(RtcSignal {
            id: format!("wrtc-offer-{}", self.peer_id),
            from: ctx.addr.clone(),
            to: Some(self.peer_id.clone()),
            offer: Some(offer_str),
            answer: None,
            candidate: None,
            json_str: None,
        });
        let _ = ctx.router.send(signal);
        Some(pending)
    }
}
