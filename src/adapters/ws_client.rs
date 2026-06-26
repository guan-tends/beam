use futures_util::StreamExt;
use std::collections::HashMap;
use tokio_tungstenite::connect_async;
use url::Url;

use crate::Config;
use crate::actor::{Actor, ActorContext, Addr};
use crate::adapters::ws_conn::WsConn;
use crate::message::Message;
use async_trait::async_trait;
use log::{debug, info};
use tokio::time::{Duration, sleep};

pub struct OutgoingWebsocketManager {
    config: Config,
    clients: HashMap<String, Addr>,
    urls: Vec<String>,
}

impl OutgoingWebsocketManager {
    pub fn new(config: Config, urls: Vec<String>) -> Self {
        OutgoingWebsocketManager {
            urls,
            clients: HashMap::new(),
            config,
        }
    }
}

#[async_trait]
impl Actor for OutgoingWebsocketManager {
    // TODO: support multiple outbound websockets
    async fn pre_start(&mut self, ctx: &ActorContext) {
        info!("OutgoingWebsocketManager starting");
        for url in self.urls.iter() {
            // Retry connection until the websocket is established.
            // TODO: break on actor shutdown signal instead of polling.
            loop {
                sleep(Duration::from_millis(1000)).await;
                if self.clients.contains_key(url) {
                    break; // Already connected — move to next URL
                }
                let result = connect_async(Url::parse(url).expect("Can't connect to URL")).await;
                if let Ok(tuple) = result {
                    let (socket, _) = tuple;
                    debug!("outgoing websocket opened to {}", url);
                    let (sender, receiver) = socket.split();
                    let client = WsConn::new(sender, receiver, self.config.allow_public_space);
                    let addr = ctx.start_actor(Box::new(client));
                    self.clients.insert(url.clone(), addr);
                    break; // Connected — move to next URL
                }
            }
        }
    }

    fn subscribe_to_everything(&self) -> bool {
        true
    }

    async fn handle(&mut self, message: Message, _ctx: &ActorContext) {
        self.clients
            .retain(|_url, client| client.send(message.clone()).is_ok());
    }
}
