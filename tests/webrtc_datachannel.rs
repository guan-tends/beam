#![cfg(feature = "webrtc")]
use std::net::UdpSocket;
use std::thread;
use std::time::{Duration, Instant};

use str0m::{
    Candidate, Event, IceConnectionState, Input, Output, Rtc,
    channel::ChannelConfig,
    net::{Protocol, Receive},
};

/// Drive a single str0m `Rtc` instance: feed timeouts, poll outputs,
/// route UDP transmits to the peer, and feed inbound UDP back into the RTC.
fn drive_rtc(
    rtc: &mut Rtc,
    local_sock: &UdpSocket,
    remote_addr: std::net::SocketAddr,
    buf: &mut [u8; 1500],
    events: &mut Vec<Event>,
) {
    // Advance time — drives ICE/DTLS/SCTP state machines.
    let _ = rtc.handle_input(Input::Timeout(Instant::now()));

    // Drain all outputs until Timeout gives us the next wake time.
    while let Ok(out) = rtc.poll_output() {
        match out {
            Output::Transmit(t) => drop(local_sock.send_to(&t.contents, remote_addr)),
            Output::Event(ev) => events.push(ev),
            Output::Timeout(_) => break,
        }
    }

    // Drain inbound UDP from the socket (non-blocking).
    while let Ok((n, src)) = local_sock.recv_from(buf) {
        let input = Input::Receive(
            Instant::now(),
            Receive {
                proto: Protocol::Udp,
                source: src,
                destination: local_sock.local_addr().unwrap(),
                contents: (&buf[..n]).try_into().expect("valid packet"),
            },
        );
        let _ = rtc.handle_input(input);
    }
}

#[test]
#[cfg(feature = "webrtc")]
fn webrtc_datachannel_puts_reach_peer() {
    // — 1. Bind two real UDP sockets on loopback —
    let l_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let r_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let l_addr = l_sock.local_addr().unwrap();
    let r_addr = r_sock.local_addr().unwrap();
    l_sock.set_nonblocking(true).unwrap();
    r_sock.set_nonblocking(true).unwrap();

    // — 2. Crypto provider (required for DTLS) —
    str0m::crypto::from_feature_flags().install_process_default();

    // — 3. Create str0m Rtc instances —
    let now = Instant::now();
    let mut l_rtc = Rtc::new(now);
    let mut r_rtc = Rtc::new(now);

    // — 4. Add local + remote candidates —
    l_rtc
        .add_local_candidate(Candidate::host(l_addr, "udp").unwrap())
        .unwrap();
    l_rtc.add_remote_candidate(Candidate::host(r_addr, "udp").unwrap());
    r_rtc
        .add_local_candidate(Candidate::host(r_addr, "udp").unwrap())
        .unwrap();
    r_rtc.add_remote_candidate(Candidate::host(l_addr, "udp").unwrap());

    // — 5. Exchange DTLS fingerprints —
    let l_finger = l_rtc.direct_api().local_dtls_fingerprint().clone();
    let r_finger = r_rtc.direct_api().local_dtls_fingerprint().clone();
    l_rtc.direct_api().set_remote_fingerprint(r_finger);
    r_rtc.direct_api().set_remote_fingerprint(l_finger);

    // — 6. Exchange ICE credentials —
    let l_creds = l_rtc.direct_api().local_ice_credentials();
    let r_creds = r_rtc.direct_api().local_ice_credentials();
    l_rtc.direct_api().set_remote_ice_credentials(r_creds);
    r_rtc.direct_api().set_remote_ice_credentials(l_creds);

    // — 7. Set controlling / controlled roles —
    l_rtc.direct_api().set_ice_controlling(true);
    r_rtc.direct_api().set_ice_controlling(false);

    // — 8. Start DTLS + SCTP —
    l_rtc.direct_api().start_dtls(true).unwrap();
    r_rtc.direct_api().start_dtls(false).unwrap();
    l_rtc.direct_api().start_sctp(true);
    r_rtc.direct_api().start_sctp(false);

    // — 9. Create out-of-band negotiated data channel on both sides —
    let config = ChannelConfig {
        negotiated: Some(1),
        label: "beam-test-chan".into(),
        ..Default::default()
    };
    let l_cid = l_rtc.direct_api().create_data_channel(config.clone());
    let _r_cid = r_rtc.direct_api().create_data_channel(config);

    // — 10. Drive the handshake until ICE and channels are up —
    let start = Instant::now();
    let mut buf = [0u8; 1500];
    let mut l_events: Vec<Event> = Vec::new();
    let mut r_events: Vec<Event> = Vec::new();
    let mut l_connected = false;
    let mut r_connected = false;
    let mut l_chan_open = false;
    let mut r_chan_open = false;

    while start.elapsed() < Duration::from_secs(10) {
        l_events.clear();
        r_events.clear();

        drive_rtc(&mut l_rtc, &l_sock, r_addr, &mut buf, &mut l_events);
        drive_rtc(&mut r_rtc, &r_sock, l_addr, &mut buf, &mut r_events);

        for ev in &l_events {
            match ev {
                Event::IceConnectionStateChange(IceConnectionState::Completed) => {
                    l_connected = true
                }
                Event::ChannelOpen(_, _) => l_chan_open = true,
                _ => {}
            }
        }
        for ev in &r_events {
            match ev {
                Event::IceConnectionStateChange(IceConnectionState::Completed) => {
                    r_connected = true
                }
                Event::ChannelOpen(_, _) => r_chan_open = true,
                _ => {}
            }
        }

        if l_connected && r_connected && l_chan_open && r_chan_open {
            break;
        }

        thread::sleep(Duration::from_millis(5));
    }
    let all_open = l_chan_open && r_chan_open;

    assert!(l_connected, "offerer ICE+DTLS should reach Completed");
    assert!(r_connected, "answerer ICE+DTLS should reach Completed");
    assert!(all_open, "data channel should open on both sides");

    // — 11. Write payload from offerer —
    let expected = b"Hello from BEAM WebRTC offerer";
    if let Some(mut chan) = l_rtc.channel(l_cid) {
        chan.write(false, expected).expect("write to data channel");
    }

    // — 12. Drive until the data reaches the answerer —
    let mut received: Option<Vec<u8>> = None;
    for _ in 0..500 {
        l_events.clear();
        r_events.clear();

        drive_rtc(&mut l_rtc, &l_sock, r_addr, &mut buf, &mut l_events);
        drive_rtc(&mut r_rtc, &r_sock, l_addr, &mut buf, &mut r_events);

        for ev in &r_events {
            if let Event::ChannelData(data) = ev {
                received = Some(data.data.to_vec());
                break;
            }
        }
        if received.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }

    assert!(
        received.is_some(),
        "answerer should receive data channel message"
    );
    assert_eq!(
        String::from_utf8_lossy(&received.unwrap()),
        String::from_utf8_lossy(expected),
        "payload should match"
    );
}
