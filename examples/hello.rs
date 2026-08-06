//! Hello World example — connect to a running BEAM node and send a greeting.
//!
//! Requires a BEAM node running with a WebSocket server on port 4944:
//! ```bash
//! cargo run --bin beam -- start --port 4944
//! ```
//! Then run this example:
//! ```bash
//! cargo run --example hello
//! ```

use beam::adapters::*;
use beam::{Config, Node, Value};

#[tokio::main]
async fn main() {
    let config = Config::default();
    let ws_client =
        OutgoingWebsocketManager::new(config.clone(), vec!["ws://localhost:4944/ws".to_string()]);
    let mut db = Node::new_with_config(config.clone(), vec![], vec![Box::new(ws_client)]);

    let mut sub = db.get("greeting").on();
    let _ = db.get("greeting").put("Hello World!".into());
    if let Value::Text(str) = sub.recv().await.unwrap() {
        assert_eq!(&str, "Hello World!");
        println!("{}", &str);
    }
    db.stop();
}
