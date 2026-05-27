use crate::actor::{Actor, ActorContext, Addr};
use crate::message::{BatchPut, Flush, Get, Message, Put};
use crate::utils::BoundedHashMap;
use crate::Dup;
use crate::Config;
use async_trait::async_trait;
use log::{debug, error, info};
use rand::{seq::IteratorRandom, thread_rng};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

static SEEN_MSGS_MAX_SIZE: usize = 10000;

struct SeenGetMessage {
    from: Addr,
    last_reply_checksum: Option<i32>,
}

pub struct Router {
    config: Config,
    known_peers: HashSet<Addr>, // ping them periodically to remove closed addrs? and sort by timestamp & prefer long-lasting conns
    storage_adapters: HashSet<Addr>,
    network_adapters: HashSet<Addr>,
    storage_adapter_actors: Vec<Box<dyn Actor>>,
    network_adapter_actors: Vec<Box<dyn Actor>>,
    server_peers: HashSet<Addr>, // temporary, so we can forward stuff to outgoing websocket peers (servers)
    dup: Dup,
    seen_get_messages: BoundedHashMap<String, SeenGetMessage>,
    subscribers_by_topic: HashMap<String, HashSet<Addr>>,
    msg_counter: AtomicUsize,
}

#[async_trait]
impl Actor for Router {
    /// Listen to incoming messages and start [Actor]s
    async fn pre_start(&mut self, ctx: &ActorContext) {
        while let Some(adapter) = self.storage_adapter_actors.pop() {
            let addr = ctx.start_actor(adapter);
            self.storage_adapters.insert(addr);
        }
        while let Some(adapter) = self.network_adapter_actors.pop() {
            let subscribe_to_everything = adapter.subscribe_to_everything();
            let addr = ctx.start_actor(adapter);
            self.network_adapters.insert(addr.clone());
            if subscribe_to_everything {
                self.server_peers.insert(addr);
            }
        }

        if self.config.stats {
            self.update_stats();
        }
    }

    async fn stopping(&mut self, _ctx: &ActorContext) {
        info!("Router stopping");
    }

    async fn handle(&mut self, msg: Message, _ctx: &ActorContext) {
        debug!("incoming message {}", msg.clone().to_string());
        match msg {
            Message::Put(put) => self.handle_put(put),
            Message::BatchPut(batch) => {
                self.handle_batch_put(batch);
            }
            Message::Get(get) => self.handle_get(get),
            Message::Flush(flush) => self.handle_flush(flush),
            Message::Hi { from, peer_id: _ } => {
                self.known_peers.insert(from);
            }
            Message::RtcSignal(_rtc) => {
                // Handled by WebRtcPeer adapter, not Router
                // WebRTC signals flow peer-to-peer over data channels
            }
        };
    }
}

impl Router {
    pub fn new(
        config: Config,
        storage_adapter_actors: Vec<Box<dyn Actor>>,
        network_adapter_actors: Vec<Box<dyn Actor>>,
    ) -> Self {
        Self {
            config,
            known_peers: HashSet::new(),
            storage_adapters: HashSet::new(),
            network_adapters: HashSet::new(),
            storage_adapter_actors,
            network_adapter_actors,
            server_peers: HashSet::new(),
            dup: Dup::default_gun(),
            seen_get_messages: BoundedHashMap::new(SEEN_MSGS_MAX_SIZE),
            subscribers_by_topic: HashMap::new(),
            msg_counter: AtomicUsize::new(0),
        }
    }

    fn update_stats(&self) {
        /*
        let mut stats = node.get("node_stats").get(&peer_id);
        let start_time = Instant::now();
        let msg_counter = self.msg_counter;
        ctx.child_task(async move {
            let mut sys = System::new_all();
            loop { // TODO break
                sys.refresh_all();
                stats.get("msgs_per_second").put(msg_counter.load(Ordering::Relaxed).into());
                msg_counter.store(0, Ordering::Relaxed);
                stats.get("total_memory").put(format!("{} MB", sys.total_memory() / 1000).into());
                stats.get("used_memory").put(format!("{} MB", sys.used_memory() / 1000).into());
                stats.get("cpu_usage").put(format!("{} %", sys.global_processor_info().cpu_usage() as u64).into());
                let uptime_secs = start_time.elapsed().as_secs();
                let uptime;
                if uptime_secs <= 60 {
                    uptime = format!("{} seconds", uptime_secs);
                } else if uptime_secs <= 2 * 60 * 60 {
                    uptime = format!("{} minutes", uptime_secs / 60);
                } else {
                    uptime = format!("{} hours", uptime_secs / 60 / 60);
                }
                stats.get("process_uptime").put(uptime.into());
                sleep(Duration::from_millis(1000)).await;
            }
        });
         */
    }

    // record subscription & relay
    fn handle_get(&mut self, get: Get) {
        if !get.id.chars().all(char::is_alphanumeric) {
            error!("id {}", get.id);
        }
        if self.is_message_seen(&get.id) {
            return;
        }
        let seen_get_message = SeenGetMessage {
            from: get.from.clone(),
            last_reply_checksum: get.checksum.clone(),
        };
        self.seen_get_messages
            .insert(get.id.clone(), seen_get_message);

        // Record subscriber
        let topic = get.node_id.split("/").next().unwrap_or("");
        debug!("{} subscribed to {}", get.from, topic);
        self.subscribers_by_topic
            .entry(topic.to_string())
            .or_insert_with(HashSet::new)
            .insert(get.from.clone());

        // Ask storage
        for addr in self.storage_adapters.iter() {
            let _ = addr.send(Message::Get(get.clone()));
        }

        let mut already_sent_to = HashSet::new();

        // Send to server peers
        for addr in self.server_peers.iter() {
            debug!("send to server peer");
            let _ = addr.send(Message::Get(get.clone()));
            already_sent_to.insert(addr.clone());
        }

        // Ask network
        let mut errored = HashSet::new();
        let mut sent_to = 0;
        let mut rng = thread_rng();
        if let Some(topic_subscribers) = self.subscribers_by_topic.get(topic) {
            // should have a list of all peers and send to those who are the likeliest to respond (MANET)
            let sample = topic_subscribers.iter().choose_multiple(&mut rng, 4);
            for addr in sample {
                if get.from == *addr {
                    continue;
                }
                if already_sent_to.contains(addr) {
                    continue;
                }
                already_sent_to.insert(addr.clone());
                match addr.send(Message::Get(get.clone())) {
                    Ok(_) => {
                        sent_to += 1;
                    }
                    _ => {
                        errored.insert(addr.clone());
                    }
                }
            }
        }
        debug!(
            "sent get to a random sample of subscribers of size {}",
            sent_to
        );
        if errored.len() > 0 {
            if let Some(topic_subscribers) = self.subscribers_by_topic.get_mut(topic) {
                for addr in errored {
                    topic_subscribers.remove(&addr);
                    self.known_peers.remove(&addr);
                }
            }
        }
        if sent_to < 4 {
            let mut errored = HashSet::new();
            while let Some(addr) = self.known_peers.iter().choose(&mut rng) {
                sent_to += 1;
                if sent_to >= 4 {
                    break;
                }
                if get.from == *addr {
                    continue;
                }
                if already_sent_to.contains(addr) {
                    continue;
                }
                already_sent_to.insert(addr.clone());
                match addr.send(Message::Get(get.clone())) {
                    Ok(_) => {}
                    _ => {
                        errored.insert(addr.clone());
                    }
                }
            }
            for addr in errored {
                self.known_peers.remove(&addr);
            }
        }
    }

    // relay to original requester or all subscribers
    fn handle_put(&mut self, put: Put) {
        if self.is_message_seen(&put.id) {
            return;
        }

        // Gun.js DAM: ack + "##" + hash dedup for identical responses
        if let (Some(ack), Some(hash)) = (&put.in_response_to, put.checksum) {
            let checksum_key = format!("{}##{}", ack, hash);
            if self.dup.check(&checksum_key) {
                debug!("duplicate response checksum: {}", checksum_key);
                return;
            }
            self.dup.track(&checksum_key);
        }

        match &put.in_response_to {
            Some(in_response_to) => {
                if let Some(seen_get_message) = self.seen_get_messages.get_mut(in_response_to) {
                    if put.checksum != None && put.checksum == seen_get_message.last_reply_checksum {
                        debug!("same reply already sent");
                        return;
                    }
                    seen_get_message.last_reply_checksum = put.checksum.clone();
                    let _ = seen_get_message.from.send(Message::Put(put));
                }
            }
            _ => {
                // Forward to storage adapter(s)
                for addr in self.storage_adapters.iter() {
                    if put.from == *addr {
                        continue;
                    }
                    let _ = addr.send(Message::Put(put.clone()));
                    debug!("sent to adapter {}", addr);
                }
                // Network relay is handled by handle_put_relay for batching
                self.handle_put_relay(&put);
            }
        };
    }

    /// Relay a Put to server peers and subscribers.
    /// Storage is NOT touched — this is pure network fan-out.
    fn handle_put_relay(&mut self, put: &Put) {
        // NOTE: NO is_message_seen here. Router::handle_put already dedup'd.
        // The relay's only job is to fan out with anti-loop via peer_hop_list.
        let mut hops = put.peer_hop_list.clone().unwrap_or_default();
        hops.insert(put.from.to_string());
        let mut already_sent_to = HashSet::new();

        // Send to server peers
        for addr in self.server_peers.iter() {
            if put.from == *addr || hops.contains(&addr.to_string()) {
                continue;
            }
            let mut put = put.clone();
            put.peer_hop_list = Some(hops.clone());
            let _ = addr.send(Message::Put(put));
            already_sent_to.insert(addr.clone());
        }

        // Relay to subscribers
        let mut sent_to = 0;
        for node_id in put.clone().updated_nodes.keys() {
            let topic = node_id.split("/").next().unwrap_or("");
            if let Some(topic_subscribers) = self.subscribers_by_topic.get_mut(topic) {
                topic_subscribers.retain(|addr| {
                    // send & remove closed addresses
                    if put.from == *addr || hops.contains(&addr.to_string()) {
                        return true;
                    }
                    if already_sent_to.contains(addr) {
                        return true;
                    }
                    already_sent_to.insert(addr.clone());
                    let mut put = put.clone();
                    put.peer_hop_list = Some(hops.clone());
                    match addr.send(Message::Put(put)) {
                        Ok(_) => {
                            sent_to += 1;
                            true
                        }
                        _ => false,
                    }
                })
            }
        }
        debug!("sent put to {} subscribers", already_sent_to.len());
        if already_sent_to.len() < 4 {
            let mut rng = thread_rng();
            let mut errored = HashSet::new();
            while let Some(addr) = self.known_peers.iter().choose(&mut rng) {
                sent_to += 1;
                if sent_to >= 4 {
                    break;
                }
                // TODO: seems like the following is necessary, but it causes a test to fail
                if already_sent_to.contains(addr) {
                    continue;
                }
                already_sent_to.insert(addr.clone());
                if put.from == *addr || hops.contains(&addr.to_string()) {
                    continue;
                }
                let mut put = put.clone();
                put.peer_hop_list = Some(hops.clone());
                match addr.send(Message::Put(put)) {
                    Ok(_) => {
                        debug!("sent put to random dude");
                    }
                    _ => {
                        errored.insert(addr.clone());
                    }
                }
            }
            for addr in errored {
                self.known_peers.remove(&addr);
            }
        }
    }

    /// Forward a BatchPut to storage adapters (single transaction),
    /// then relay each constituent Put individually.
    fn handle_batch_put(&mut self, batch: BatchPut) {
        // Forward BatchPut to storage — preserves single-transaction semantics
        for addr in self.storage_adapters.iter() {
            if batch.from == *addr {
                continue;
            }
            let _ = addr.send(Message::BatchPut(batch.clone()));
        }

        // Relay each constituent put individually (with deduplication)
        for put in batch.puts {
            if self.is_message_seen(&put.id) {
                continue;
            }
            // Gun.js DAM: ack + "##" + hash dedup for identical responses
            if let (Some(ack), Some(hash)) = (&put.in_response_to, put.checksum) {
                let checksum_key = format!("{}##{}", ack, hash);
                if self.dup.check(&checksum_key) {
                    debug!("batch: duplicate response checksum: {}", checksum_key);
                    continue;
                }
                self.dup.track(&checksum_key);
            }
            // ACK responses within a batch are unusual but handled defensively
            if let Some(in_response_to) = &put.in_response_to {
                if let Some(seen_get_message) = self.seen_get_messages.get_mut(in_response_to) {
                    if put.checksum == seen_get_message.last_reply_checksum {
                        continue;
                    }
                    seen_get_message.last_reply_checksum = put.checksum.clone();
                    let _ = seen_get_message.from.send(Message::Put(put));
                }
                continue;
            }
            self.handle_put_relay(&put);
        }
    }

    fn handle_flush(&mut self, flush: Flush) {
        // Forward flush to all storage adapters (they can fsync)
        let mut sent = HashSet::new();
        for addr in self.storage_adapters.iter() {
            if flush.from == *addr {
                continue;
            }
            if sent.contains(addr) {
                continue;
            }
            sent.insert(addr.clone());
            let _ = addr.send(Message::Flush(flush.clone()));
        }
        debug!("forwarded flush to {} storage adapters", sent.len());
    }

    fn is_message_seen(&mut self, id: &String) -> bool {
        self.msg_counter.fetch_add(1, Ordering::Relaxed);
        if self.dup.check(id) {
            debug!("already seen message {}", id);
            return true;
        }
        self.dup.track(id);
        false
    }
}
