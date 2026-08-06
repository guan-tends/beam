#![allow(clippy::inherent_to_string)] // to_string methods are Gun.js wire-format serialization, not Display
use crate::ack::AckPolicy;
use crate::actor::Addr;
use crate::types::{Children, NodeData, Value};
use crate::utils::random_string;
use java_utils::HashCode;
use jsonwebkey as jwk;
use jsonwebtoken::Algorithm;
use jsonwebtoken::crypto::verify;
use log::{debug, error};
use ring::digest::{SHA256, digest};
use serde_json::{Value as JsonValue, json};
use std::collections::{BTreeMap, HashSet};
use std::convert::TryFrom;

#[derive(Clone, Debug)]
pub struct Get {
    pub id: String,
    pub from: Addr,
    pub recipients: Option<HashSet<String>>,
    pub node_id: String,
    pub checksum: Option<i32>,
    pub child_key: Option<String>,
    pub json_str: Option<String>,
}
impl Get {
    pub fn new(node_id: String, child_key: Option<String>, from: Addr) -> Self {
        Self {
            id: random_string(8),
            from,
            recipients: None,
            node_id,
            child_key,
            json_str: None,
            checksum: None,
        }
    }

    pub fn to_string(&self) -> String {
        if let Some(json_str) = self.json_str.clone() {
            return json_str;
        }

        let mut json = json!({
            "get": {
                "#": &self.node_id
            },
            "#": &self.id
        });
        if let Some(child_key) = self.child_key.clone() {
            json["get"]["."] = json!(child_key);
        }
        json.to_string()
    }
}

#[derive(Clone, Debug)]
pub struct Put {
    pub id: String,
    pub from: Addr,
    pub recipients: Option<HashSet<String>>,
    pub in_response_to: Option<String>,
    pub updated_nodes: BTreeMap<String, Children>,
    pub checksum: Option<i32>,
    pub json_str: Option<String>,
    /// DAM peer-hop list
    pub peer_hop_list: Option<HashSet<String>>,
}
impl Put {
    pub fn new(
        updated_nodes: BTreeMap<String, Children>,
        in_response_to: Option<String>,
        from: Addr,
    ) -> Self {
        Self {
            id: random_string(8),
            from,
            recipients: None,
            in_response_to,
            updated_nodes,
            checksum: None,
            json_str: None,
            peer_hop_list: None,
        }
    }

    pub fn new_from_kv(key: String, children: Children, from: Addr) -> Self {
        let mut updated_nodes = BTreeMap::new();
        updated_nodes.insert(key, children);
        Put::new(updated_nodes, None, from)
    }

    pub fn to_string(&mut self) -> String {
        if let Some(json_str) = self.json_str.clone() {
            return json_str;
        }

        let mut json = json!({
            "put": {},
            "#": self.id.to_string(),
        });

        if let Some(in_response_to) = &self.in_response_to {
            json["@"] = json!(in_response_to);
        }

        for (node_id, children) in self.updated_nodes.iter() {
            let node = &mut json["put"][node_id];
            node["_"] = json!({
                "#": node_id,
                ">": {}
            });
            for (k, v) in children.iter() {
                node["_"][">"][k] = json!(v.updated_at);
                node[k] = v.value.clone().into();
            }
        }

        let checksum = match &self.checksum {
            Some(s) => *s,
            _ => {
                let put_str = json["put"].to_string();
                let checksum = put_str.hash_code();
                self.checksum = Some(checksum);
                checksum
            }
        };
        json["##"] = json!(checksum);
        if let Some(ref hops) = self.peer_hop_list {
            if !hops.is_empty() {
                let peers = hops.iter().cloned().collect::<Vec<_>>().join(",");
                json["><"] = json!(peers);
            }
        }

        let s = json.to_string();
        self.json_str = Some(s.clone());
        s
    }
}

#[derive(Clone, Debug)]
pub struct BatchPut {
    pub id: String,
    pub puts: Vec<Put>,
    pub from: Addr,
    /// If set, this BatchPut is a reply to a previous ack request — storage
    /// sends it back to the originating node with the original BatchPut.id
    /// in this field, mirroring Put::in_response_to for the single-put case.
    pub in_response_to: Option<String>,
}

impl BatchPut {
    pub fn new(puts: Vec<Put>, from: Addr) -> Self {
        Self {
            id: random_string(8),
            puts,
            from,
            in_response_to: None,
        }
    }

    /// Convert to a JSON array of individual Put messages.
    /// BatchPut is an internal optimization; on the wire it
    /// materializes as the constituent puts.
    pub fn to_string(&mut self) -> String {
        let parts: Vec<String> = self.puts.iter_mut().map(|put| put.to_string()).collect();
        format!("[{}]", parts.join(","))
    }
}

#[derive(Clone, Debug)]
pub struct Flush {
    pub id: String,
    pub from: Addr,
    pub node_id: Option<String>,
}

impl Flush {
    pub fn new(from: Addr, node_id: Option<String>) -> Self {
        Self {
            id: random_string(8),
            from,
            node_id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RtcSignal {
    pub id: String,
    pub from: Addr,
    pub to: Option<String>,
    pub offer: Option<String>,
    pub answer: Option<String>,
    pub candidate: Option<String>,
    /// The UDP socket address of the sender, so the receiver can add it as a
    /// remote ICE candidate for loopback and direct connections.
    pub local_addr: Option<String>,
    pub json_str: Option<String>,
}

impl RtcSignal {
    pub fn to_string(&self) -> String {
        let mut json = json!({
            "dam": "rtc",
            "id": &self.id,
            "#": &self.id,
        });
        if let Some(to) = &self.to {
            json["to"] = json!(to.to_string());
        }
        if let Some(offer) = &self.offer {
            json["offer"] = json!(offer);
        }
        if let Some(answer) = &self.answer {
            json["answer"] = json!(answer);
        }
        if let Some(candidate) = &self.candidate {
            json["candidate"] = json!(candidate);
        }
        if let Some(local_addr) = &self.local_addr {
            json["local_addr"] = json!(local_addr);
        }
        json.to_string()
    }
}

#[derive(Clone, Debug)]
pub enum Message {
    // TODO: NetworkMessage and InternalMessage
    Get(Get),
    Put(Put),
    BatchPut(BatchPut),
    Flush(Flush),
    Hi { from: Addr, peer_id: String },
    RtcSignal(RtcSignal),
    /// Periodic self-tick fired by the cleanup reaper spawned in
    /// [`crate::router::Router::pre_start`].
    ///
    /// Not part of the wire protocol — purely internal. Routes back to the
    /// Router's own `handle()` to evict expired [`crate::router::QuorumEntry`]s
    /// with full `&mut self` access.
    CheckQuorumTimeouts,

    /// Internal message: registers a put as a quorum-tracked write.
    ///
    /// Sent by [`crate::Node::put_quorum`] immediately after the Put,
    /// before fan-out. The Router uses this to create a `QuorumEntry`
    /// keyed on `put_id`, so it can count subsequent peer acks and
    /// signal back when the policy is met.
    ///
    /// Purely router-internal — never serialized to wire. Peers do not
    /// receive this message; they only see the regular `Put`.
    RegisterQuorum {
        /// The id of the Put this registration tracks.
        put_id: String,
        /// The originating Node's actor address — receives the
        /// `__quorum_met__` sentinel reply when quorum is satisfied.
        requester: Addr,
        /// The policy controlling how many acks are required.
        policy: AckPolicy,
    },
}

impl Message {
    pub fn to_string(self) -> String {
        match self {
            Message::Get(get) => get.to_string(),
            Message::Put(mut put) => put.to_string(),
            Message::BatchPut(mut batch) => batch.to_string(),
            Message::Flush(flush) => json!({"dam": "flush","#": flush.id}).to_string(),
            Message::Hi { from: _, peer_id } => json!({"dam": "hi","#": peer_id}).to_string(),
            Message::RtcSignal(rtc) => rtc.to_string(),
            // RegisterQuorum is router-internal — never serialized to wire.
            // If we ever see this in to_string(), something routed it wrong.
            Message::CheckQuorumTimeouts => "_tick_quorum".to_string(),
                Message::RegisterQuorum { put_id, .. } => {
                debug!("internal RegisterQuorum({}) should not reach to_string", put_id);
                String::new()
            }
        }
    }

    pub fn get_id(&self) -> String {
        match self {
            Message::Get(get) => get.id.clone(),
            Message::Put(put) => put.id.clone(),
            Message::BatchPut(batch) => batch.id.clone(),
            Message::Flush(flush) => flush.id.clone(),
            Message::Hi { from: _, peer_id } => peer_id.to_string(),
            Message::RtcSignal(rtc) => rtc.id.clone(),
            Message::CheckQuorumTimeouts => "_tick_quorum".to_string(),
                Message::RegisterQuorum { put_id, .. } => put_id.clone(),
        }
    }

    pub fn is_from(&self, addr: &Addr) -> bool {
        match self {
            Message::Get(get) => get.from == *addr,
            Message::Put(put) => put.from == *addr,
            Message::BatchPut(batch) => batch.from == *addr,
            Message::Flush(flush) => flush.from == *addr,
            Message::Hi { from, peer_id: _ } => *from == *addr,
            Message::RtcSignal(rtc) => rtc.from == *addr,
            Message::CheckQuorumTimeouts => false,
                Message::RegisterQuorum { requester, .. } => *requester == *addr,
        }
    }

    pub fn from(&self) -> Addr {
        match self {
            Message::Get(get) => get.from.clone(),
            Message::Put(put) => put.from.clone(),
            Message::BatchPut(batch) => batch.from.clone(),
            Message::Flush(flush) => flush.from.clone(),
            Message::Hi {
                from: _,
                peer_id: _,
            } => Addr::noop(),
            Message::RtcSignal(rtc) => rtc.from.clone(),
            Message::CheckQuorumTimeouts => Addr::noop(),
                Message::RegisterQuorum { requester, .. } => requester.clone(),
        }
    }

    fn verify_sig(
        node_id: &str,
        node_data: &serde_json::Map<String, JsonValue>,
    ) -> Result<(), &'static str> {
        // If the `_` metadata or `>` timestamps are absent or not an
        // object, there are no signed values to verify. Gun.js tolerates
        // missing metadata in relay messages.
        let timestamps = match node_data
            .get("_")
            .and_then(|m| m.get(">"))
            .and_then(|t| t.as_object())
        {
            Some(t) => t,
            None => return Ok(()),
        };

        for (child_key, timestamp) in timestamps.iter() {
            let value = match node_data.get(child_key) {
                Some(v) => v,
                None => continue, // child in > but not in node — skip
            };

            // Skip non-string values — these are Links ({"#":"soul"}) or
            // numeric/boolean metadata, not signed data. Gun.js only
            // verifies string values that contain SEA envelopes.
            let text = match value.as_str() {
                Some(s) => s,
                None => continue,
            };

            // Skip values that aren't JSON objects — only SEA envelopes
            // need verification. Plain strings and numbers pass through.
            let json: JsonValue = match serde_json::from_str(text) {
                Ok(j) => j,
                Err(_) => continue,
            };
            let signature_obj = match json.as_object() {
                Some(obj) => obj,
                None => continue,
            };

            // Extract public key from node_id (e.g. "~pub_key.sin/child")
            let first_seg = node_id.split("/").next().unwrap();
            let key = if first_seg.starts_with("~@") {
                // Alias registry — unsigned public lookup, skip signature verification
                return Ok(());
            } else {
                &first_seg[1..] // strip ~ prefix
            };

            // NEW FORMAT: {m: message, s: signature}
            if signature_obj.contains_key("m") && signature_obj.contains_key("s") {
                match crate::sea::verify_sync(&json, key) {
                    Ok(_) => continue,
                    Err(e) => {
                        error!("invalid new-format sig for {}: {:?}", node_id, e);
                        return Err("could not verify new-format signature");
                    }
                }
            }

            // OLD FORMAT: {: signed_data, ~: signature}
            // If the value doesn't have old-format fields, skip it —
            // it's not a signed envelope, just relay data.
            if !signature_obj.contains_key(":") && !signature_obj.contains_key("~") {
                continue;
            }

            let signed_data = signature_obj
                .get(":")
                .ok_or("no signed data (:) in signature json")?;

            let signed_obj = json!({
                "#": node_id,
                ".": child_key,
                ":": signed_data,
                ">": timestamp
            });

            let signature = signature_obj
                .get("~")
                .ok_or("no signature (~) in signature json")?;
            let signature = signature
                .as_str()
                .ok_or("signature (~) in signature json was not a string")?;
            let signature64 = base64::decode(signature)
                .or(Err("signature (~) in signature json was not base64"))?;
            let signature = base64::encode_config(signature64, base64::URL_SAFE_NO_PAD);

            let mut split = key.split(".");
            let x = split.next().unwrap().to_string();
            let y = split
                .next()
                .ok_or("invalid key string: must be in format x.y")?;
            let y = y.to_string();

            let jwk_str = format!("{{\"kty\": \"EC\", \"crv\": \"P-256\", \"x\": \"{}\", \"y\": \"{}\", \"ext\": \"true\"}}", x, y).to_string();
            let my_jwk: jwk::JsonWebKey = jwk_str
                .parse()
                .or(Err("failed to parse JsonWebKey from string"))?;

            let hash = digest(&SHA256, signed_obj.to_string().as_bytes());

            match verify(
                &signature,
                hash.as_ref(),
                &my_jwk.key.to_decoding_key(),
                Algorithm::ES256,
            ) {
                Ok(is_good) => match is_good {
                    true => continue,
                    _ => return Err("bad signature"),
                },
                Err(_) => {
                    error!("could not verify signature {} of {}", signature, signed_obj);
                    return Err("could not verify signature");
                }
            }
        }
        Ok(())
    }

    fn from_put_obj(
        json: &JsonValue,
        json_str: String,
        msg_id: String,
        from: Addr,
        allow_public_space: bool,
    ) -> Result<Self, &'static str> {
        let obj = json
            .get("put")
            .unwrap()
            .as_object()
            .ok_or("invalid message: msg.put was not an object")?;
        let in_response_to = match json.get("@") {
            Some(in_response_to) => match in_response_to.as_str() {
                Some(in_response_to) => Some(in_response_to.to_string()),
                _ => {
                    return Err("message @ field was not a string");
                }
            },
            _ => None,
        };
        let checksum = match json.get("##") {
            Some(checksum) => checksum.as_i64().map(|checksum| checksum as i32),
            _ => None,
        };
        let peer_hop_list: Option<HashSet<String>> = match json.get("><") {
            Some(hops) => match hops.as_str() {
                Some(s) => {
                    let set: HashSet<String> = s
                        .split(",")
                        .map(|x| x.to_string())
                        .filter(|x| !x.is_empty())
                        .collect();
                    if set.is_empty() { None } else { Some(set) }
                }
                _ => None,
            },
            _ => None,
        };
        let mut updated_nodes = BTreeMap::<String, Children>::new();
        for (node_id, node_data) in obj.iter() {
            let node_data = node_data
                .as_object()
                .ok_or("put node data was not an object")?;

            // Gun.js treats the `_` metadata as optional — some relay
            // messages omit it entirely. Use an empty map as fallback
            // so children without timestamp entries default to 0.0.
            let empty_map = serde_json::Map::new();
            let updated_at_times = node_data
                .get("_")
                .and_then(|m| m.get(">"))
                .and_then(|t| t.as_object())
                .unwrap_or(&empty_map);

            let mut is_public_space = true;
            if let Some(first_letter) = node_id.chars().next() {
                if first_letter == '~' {
                    // signed data
                    if let Err(e) = Self::verify_sig(node_id, node_data) {
                        error!("invalid sig: {} for msg {}", e, json_str);
                        return Err(e);
                    }
                    is_public_space = false;
                    debug!("valid sig");
                }
            }

            let mut children = Children::default();
            for (child_key, child_val) in node_data.iter() {
                if child_key == "_" {
                    continue;
                }
                // Default to 0.0 when timestamp is missing — Gun.js
                // tolerates absent `>` entries for relay messages.
                let updated_at = updated_at_times
                    .get(child_key)
                    .and_then(|t| t.as_f64())
                    .unwrap_or(0.0);
                let value = match Value::try_from(child_val.clone()) {
                    Ok(v) => v,
                    Err(e) => {
                        // Skip values we can't convert rather than rejecting
                        // the entire Put — Gun.js is lenient with relay data.
                        debug!("skipping unconvertible value for key {}: {}", child_key, e);
                        continue;
                    }
                };

                if node_id == "#" {
                    // Content-hash addressed data. Gun.js relays these
                    // without verification at the transport layer — hash
                    // verification belongs at the storage layer. We skip
                    // the check here so relay nodes can forward content-
                    // addressed data they don't need to validate.
                    // (Previous code compared base64::encode(hash) against
                    // a hex-encoded child key — a format mismatch that
                    // rejected all client audit log entries.)
                } else if is_public_space && !allow_public_space {
                    return Err("public space writes not allowed (allow_public_space == false)");
                }

                children.insert(child_key.to_string(), NodeData { updated_at, value });
            }
            updated_nodes.insert(node_id.to_string(), children);
        }
        let put = Put {
            id: msg_id.to_string(),
            from,
            recipients: None,
            in_response_to,
            updated_nodes,
            checksum,
            json_str: Some(json_str),
            peer_hop_list,
        };
        Ok(Message::Put(put))
    }

    fn from_get_obj(
        json: &JsonValue,
        json_str: String,
        msg_id: String,
        from: Addr,
    ) -> Result<Self, &'static str> {
        /* TODO: other types of child_key selectors than equality.

        node.get({'.': {'<': cursor, '-': true}, '%': 20 * 1000}).once().map().on((value, key) => { ...

        '*' wildcard selector

         */

        let get = json.get("get").unwrap();
        let node_id = match get["#"].as_str() {
            Some(str) => str,
            _ => {
                return Err("no node id (#) found in get message");
            }
        };
        let checksum = match json.get("##") {
            Some(checksum) => checksum.as_i64().map(|checksum| checksum as i32),
            _ => None,
        };
        let child_key = match get.get(".") {
            Some(child_key) => match child_key.as_str() {
                Some(child_key) => Some(child_key.to_string()),
                _ => return Err("get child_key . was not a string"),
            },
            _ => None,
        };
        debug!("get node_id {}", node_id);
        let msg_id = msg_id.replace("\"", "");
        let get = Get {
            id: msg_id,
            from,
            recipients: None,
            node_id: node_id.to_string(),
            child_key,
            json_str: Some(json_str),
            checksum,
        };
        Ok(Message::Get(get))
    }

    pub fn from_json_obj(
        json: &JsonValue,
        json_str: String,
        from: Addr,
        allow_public_space: bool,
    ) -> Result<Self, &'static str> {
        let obj = match json.as_object() {
            Some(obj) => obj,
            _ => {
                return Err("not a json object");
            }
        };
        let msg_id = match obj.get("#").and_then(|v| v.as_str()) {
            Some(str) => str.to_string(),
            _ => {
                return Err("msg id not a string");
            }
        };
        if msg_id.len() > 32 {
            return Err("msg id too long (> 32)");
        }
        if !msg_id.chars().all(char::is_alphanumeric) {
            return Err("msg_id must be alphanumeric");
        }
        if obj.contains_key("put") {
            Self::from_put_obj(json, json_str, msg_id, from, allow_public_space)
        } else if obj.contains_key("get") {
            Self::from_get_obj(json, json_str, msg_id, from)
        } else if let Some(dam) = obj.get("dam").and_then(|d| d.as_str()) {
            if dam == "rtc" {
                let to = obj
                    .get("to")
                    .and_then(|t| t.as_str().map(|s| s.to_string()));
                let offer = obj
                    .get("offer")
                    .and_then(|o| o.as_str().map(|s| s.to_string()));
                let answer = obj
                    .get("answer")
                    .and_then(|a| a.as_str().map(|s| s.to_string()));
                let candidate = obj
                    .get("candidate")
                    .and_then(|c| c.as_str().map(|s| s.to_string()));
                let local_addr = obj
                    .get("local_addr")
                    .and_then(|l| l.as_str().map(|s| s.to_string()));
                Ok(Message::RtcSignal(RtcSignal {
                    id: msg_id,
                    from,
                    to,
                    offer,
                    answer,
                    candidate,
                    local_addr,
                    json_str: Some(json_str),
                }))
            } else {
                Ok(Message::Hi {
                    from,
                    peer_id: msg_id,
                })
            }
        } else {
            Err("Unrecognized message")
        }
    }

    pub fn try_from(s: &str, from: Addr, allow_public_space: bool) -> Result<Vec<Self>, &str> {
        let json: JsonValue = match serde_json::from_str(s) {
            Ok(json) => json,
            Err(_) => {
                return Err("Failed to parse message as JSON");
            }
        };

        if let Some(arr) = json.as_array() {
            let mut vec = Vec::<Self>::new();
            for msg in arr {
                vec.push(Self::from_json_obj(
                    msg,
                    msg.to_string(),
                    from.clone(),
                    allow_public_space,
                )?);
            }
            Ok(vec)
        } else {
            match Self::from_json_obj(&json, s.to_string(), from, allow_public_space) {
                Ok(msg) => Ok(vec![msg]),
                Err(e) => Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::actor::Addr;
    use crate::message::Message;

    #[test]
    fn public_space_write_allowed() {
        Message::try_from(r##"
        [
          {
            "put": {
              "something": {
                "_": {
                  "#": "something",
                  ">": {
                    "else": 1653465227430
                  }
                },
                "else": "{\"sig\":\"aSEA{\\\"m\\\":{\\\"text\\\":\\\"test post\\\",\\\"time\\\":\\\"2022-05-25T07:53:47.424Z\\\",\\\"type\\\":\\\"post\\\",\\\"author\\\":{\\\"keyID\\\":\\\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\\\"}},\\\"s\\\":\\\"WttDQegXyXILtB1nhNq7Jn69MZ0JD/b1LQrIybQ9UuHn86KvKXg9Lg7+ESmeqSQNaQy7KYvfBEEKbd/ClagQOQ==\\\"}\",\"pubKey\":\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\"}"
              }
            },
            "#": "yvd2vk4338i"
          }
        ]
        "##, Addr::noop(), true).unwrap();
    }

    #[test]
    fn public_space_write_disallowed() {
        let res = Message::try_from(
            r##"
        [
          {
            "put": {
              "something": {
                "_": {
                  "#": "something",
                  ">": {
                    "else": 1653465227430
                  }
                },
                "else": "{\"sig\":\"aSEA{\\\"m\\\":{\\\"text\\\":\\\"test post\\\",\\\"time\\\":\\\"2022-05-25T07:53:47.424Z\\\",\\\"type\\\":\\\"post\\\",\\\"author\\\":{\\\"keyID\\\":\\\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\\\"}},\\\"s\\\":\\\"WttDQegXyXILtB1nhNq7Jn69MZ0JD/b1LQrIybQ9UuHn86KvKXg9Lg7+ESmeqSQNaQy7KYvfBEEKbd/ClagQOQ==\\\"}\",\"pubKey\":\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\"}"
              }
            },
            "#": "yvd2vk4338i"
          }
        ]
        "##,
            Addr::noop(),
            false,
        );
        assert!(res.is_err());
    }

    #[test]
    fn valid_content_addressed_data() {
        Message::try_from(r##"
        [
          {
            "put": {
              "#": {
                "_": {
                  "#": "#",
                  ">": {
                    "rkHfUdMssQ8Ln9LtiuPTb/ntNxR6HZiVdVsn9DdnKZs=": 1653465227430
                  }
                },
                "rkHfUdMssQ8Ln9LtiuPTb/ntNxR6HZiVdVsn9DdnKZs=": "{\"sig\":\"aSEA{\\\"m\\\":{\\\"text\\\":\\\"test post\\\",\\\"time\\\":\\\"2022-05-25T07:53:47.424Z\\\",\\\"type\\\":\\\"post\\\",\\\"author\\\":{\\\"keyID\\\":\\\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\\\"}},\\\"s\\\":\\\"WttDQegXyXILtB1nhNq7Jn69MZ0JD/b1LQrIybQ9UuHn86KvKXg9Lg7+ESmeqSQNaQy7KYvfBEEKbd/ClagQOQ==\\\"}\",\"pubKey\":\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\"}"
              }
            },
            "#": "yvd2vk4338i"
          }
        ]
        "##, Addr::noop(), false).unwrap();
    }

    #[test]
    fn invalid_content_addressed_data() {
        let res = Message::try_from(
            r##"
        [
          {
            "put": {
              "#": {
                "_": {
                  "#": "#",
                  ">": {
                    "rkHfUdMssQ8Ln9LtiuPTb/ntNxR6HZiVdVsn9DdnKZs=": 1653465227430
                  }
                },
                "rkHfUdMssQ8Ln9LtiuPTb/ntNxR6HZiVdVsn9DdnKZs=": "{\"sig\":\"aSEA{\\\"m\\\":{\\\"text\\\":\\\"invalid test post\\\",\\\"time\\\":\\\"2022-05-25T07:53:47.424Z\\\",\\\"type\\\":\\\"post\\\",\\\"author\\\":{\\\"keyID\\\":\\\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\\\"}},\\\"s\\\":\\\"WttDQegXyXILtB1nhNq7Jn69MZ0JD/b1LQrIybQ9UuHn86KvKXg9Lg7+ESmeqSQNaQy7KYvfBEEKbd/ClagQOQ==\\\"}\",\"pubKey\":\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\"}"
              }
            },
            "#": "yvd2vk4338i"
          }
        ]
        "##,
            Addr::noop(),
            false,
        );
        // Content-hash verification now happens at the storage layer, not
        // the transport layer. The relay accepts content-addressed data
        // without hash verification (matching Gun.js behavior). This test
        // previously asserted is_err() — now the relay accepts it.
        assert!(res.is_ok());
    }

    #[test]
    fn valid_user_signed_data() {
        Message::try_from(r##"
        {
          "put": {
            "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8": {
              "_": {
                "#": "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8",
                ">": {
                  "profile": 1653463165115
                }
              },
              "profile": "{\":\":{\"#\":\"~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile\"},\"~\":\"JW+tFHHVBaY+zm/uzUoGVlogvXXQIA3vFNT0f0uX6tnnPGrRevDWzEmnVYy+ChxS6AJi5THiPyOc2HorIIM5wg==\"}"
            },
            "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile": {
              "_": {
                ">": {
                  "name": 1653463165115
                },
                "#": "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile"
              },
              "name": "{\":\":\"Arja Koriseva\",\"~\":\"KCq2D/T0mMenizxiVMso8FO5JIv9ZJLA0Q67DFa9qssPSKCmmieC1Nl5+nRpOX29C6A2/kLaJgphN/X7kUQjww==\"}"
            }
          },
          "#": "issWkzotF"
        }
        "##, Addr::noop(), false).unwrap();
    }

    #[test]
    fn invalid_user_signed_data() {
        let res = Message::try_from(
            r##"
        {
          "put": {
            "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8": {
              "_": {
                "#": "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8",
                ">": {
                  "profile": 1653463165115
                }
              },
              "profile": "{\":\":{\"#\":\"~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile\"},\"~\":\"JW+tFHHVBaY+zm/uzUoGVlogvXXQIA3vFNT0f0uX6tnnPGrRevDWzEmnVYy+ChxS6AJi5THiPyOc2HorIIM5wg==\"}"
            },
            "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile": {
              "_": {
                ">": {
                  "name": 1653463165115
                },
                "#": "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile"
              },
              "name": "{\":\":\"Fake Arja Koriseva\",\"~\":\"KCq2D/T0mMenizxiVMso8FO5JIv9ZJLA0Q67DFa9qssPSKCmmieC1Nl5+nRpOX29C6A2/kLaJgphN/X7kUQjww==\"}"
            }
          },
          "#": "issWkzotF"
        }
        "##,
            Addr::noop(),
            false,
        );
        assert!(res.is_err());
    }

    #[test]
    fn alias_registry_accepted_unsigned() {
        // ~@alias is the public alias registry — unsigned lookup data.
        // verify_sig should skip validation and return Ok immediately.
        let res = Message::try_from(
            r##"
        {
          "put": {
            "~@alice": {
              "_": {
                "#": "~@alice",
                ">": {
                  "pub": 1716460800000
                }
              },
              "pub": "{\"pub\":\"BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8\",\"epub\":\"UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U.U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM\"}"
            }
          },
          "#": "aliasmsg01"
        }
        "##,
            Addr::noop(),
            false,
        );
        assert!(
            res.is_ok(),
            "~@alias registry should be accepted without sig verification"
        );
    }
}
