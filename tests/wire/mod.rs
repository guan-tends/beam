//! Golden wire-fixture test harness for Gun.js protocol compatibility.
//!
//! This module loads JSON fixture files from `tests/wire/fixtures/`, feeds
//! each fixture's raw wire message into [`Message::try_from`], and asserts
//! that BEAM's parser produces the expected result. The fixtures double as
//! a human-readable protocol specification — each file documents one wire
//! message shape and the behaviour BEAM must exhibit when receiving it.
//!
//! # Design
//!
//! The harness is intentionally minimal: no external test framework crate,
//! no custom assertion library. Fixtures are plain JSON consumed by both
//! Rust (this harness) and the Node.js mirror tests (Layer 2), so the
//! protocol contract is defined exactly once.
//!
//! # Fixture Schema
//!
//! ```json
//! {
//!   "name": "unique_identifier",
//!   "description": "human-readable explanation",
//!   "category": "put|get|handshake|dam|batch|edge",
//!   "input": "<raw Gun.js wire message as JSON string>",
//!   "expected": {
//!     "parses": true,
//!     "kind": "Put|Get|Hi|RtcSignal",
//!     "souls": ["soul1"],
//!     "fields": { "soul1": ["name", "age"] },
//!     "values": { "soul1": { "name": "Alice" } },
//!     "timestamps": { "soul1": { "name": 1653463165115.0 } },
//!     "error": null
//!   }
//! }
//! ```
//!
//! - `parses: false` expects [`Message::try_from`] to return `Err`.
//!   Only `error` is checked in this case (substring match).
//! - `parses: true` with `kind` checks the [`Message`] variant.
//! - `souls`, `fields`, `values`, `timestamps` are checked when present.
//!
//! # Adding Fixtures
//!
//! Drop a new `.json` file into the appropriate category subdirectory under
//! `tests/wire/fixtures/`. No code changes needed — the harness discovers
//! fixtures by walking the directory tree at test time.

use beam::actor::Addr;
use beam::message::Message;

use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

/// A single wire-protocol test fixture loaded from JSON.
///
/// See the [module docs](self) for the full schema.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct WireFixture {
    /// Unique identifier — used in test failure messages.
    pub name: String,
    /// Human-readable description of what the fixture tests.
    #[serde(default)]
    pub description: String,
    /// Category folder: `put`, `get`, `handshake`, `dam`, `batch`, `edge`.
    #[serde(default)]
    pub category: String,
    /// Raw Gun.js wire message as a JSON string.
    pub input: String,
    /// Expected BEAM parser behaviour.
    pub expected: Expected,
    /// When `true`, pass `allow_public_space=true` to the parser.
    /// Default `false` — only signed (`~`-prefixed) souls are accepted.
    #[serde(default)]
    pub allow_public_space: bool,
}

/// Expected result of feeding `input` into [`Message::try_from`].
#[derive(Debug, Deserialize)]
pub struct Expected {
    /// `false` → expect `Err`. `true` → expect `Ok` and check remaining fields.
    pub parses: bool,
    /// Expected [`Message`] variant: `"Put"`, `"Get"`, `"Hi"`, `"RtcSignal"`.
    #[serde(default)]
    pub kind: Option<String>,
    /// Expected soul (node ID) list extracted from the message.
    #[serde(default)]
    pub souls: Vec<String>,
    /// Expected child keys per soul.
    #[serde(default)]
    pub fields: BTreeMap<String, Vec<String>>,
    /// Expected wire values per soul per key.
    #[serde(default)]
    pub values: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Expected timestamps per soul per key.
    #[serde(default)]
    pub timestamps: BTreeMap<String, BTreeMap<String, f64>>,
    /// When `parses: false`, the expected error substring.
    #[serde(default)]
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Loader
// ---------------------------------------------------------------------------

/// Recursively loads all `.json` fixture files under `fixtures/`.
///
/// Returns fixtures sorted by name for deterministic test execution.
pub fn load_fixtures(base: &Path) -> Vec<WireFixture> {
    let mut fixtures = Vec::new();
    collect_fixtures(base, &mut fixtures);
    fixtures.sort_by(|a, b| a.name.cmp(&b.name));
    fixtures
}

fn collect_fixtures(dir: &Path, out: &mut Vec<WireFixture>) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => panic!("failed to read fixture dir {}: {e}", dir.display()),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_fixtures(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "json") {
            let content = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
            let fixture: WireFixture = serde_json::from_str(&content)
                .unwrap_or_else(|e| panic!("failed to parse fixture {}: {e}", path.display()));
            out.push(fixture);
        }
    }
}

// ---------------------------------------------------------------------------
// Assertion Engine
// ---------------------------------------------------------------------------

/// Feeds a fixture's input into [`Message::try_from`] and asserts the result.
///
/// On failure, panics with the fixture name and a description of the
/// mismatch so CI output is immediately actionable.
pub fn run_fixture(fixture: &WireFixture, from: Addr, allow_public_space: bool) {
    let result = Message::try_from(&fixture.input, from, allow_public_space);

    if !fixture.expected.parses {
        assert!(
            result.is_err(),
            "[{}] expected error but parsed successfully",
            fixture.name
        );
        if let Some(expected_err) = &fixture.expected.error {
            let actual = result.unwrap_err();
            assert!(
                actual.contains(expected_err.as_str()),
                "[{}] expected error containing {:?}, got {:?}",
                fixture.name,
                expected_err,
                actual
            );
        }
        return;
    }

    let messages = result.unwrap_or_else(|e| {
        panic!("[{}] expected parse success but got error: {e}", fixture.name)
    });

    // An empty array yields an empty vec — valid if the fixture has no
    // further assertions. Only borrow the first message when there is one
    // and the fixture actually specifies checks beyond `parses: true`.
    let has_assertions = fixture.expected.kind.is_some()
        || !fixture.expected.souls.is_empty()
        || !fixture.expected.fields.is_empty()
        || !fixture.expected.values.is_empty()
        || !fixture.expected.timestamps.is_empty();

    if messages.is_empty() {
        assert!(
            !has_assertions,
            "[{}] expected at least one message but got empty vec",
            fixture.name
        );
        return;
    }

    // try_from may return multiple messages (array input). We assert
    // against the first message for kind/souls/fields/values/timestamps.
    // Batch fixtures should set kind to the first message's type.
    let msg = &messages[0];

    // --- Kind ---
    if let Some(expected_kind) = &fixture.expected.kind {
        let actual_kind = match msg {
            Message::Put(_) => "Put",
            Message::Get(_) => "Get",
            Message::Hi { .. } => "Hi",
            Message::RtcSignal(_) => "RtcSignal",
            Message::BatchPut(_) => "BatchPut",
            Message::Flush(_) => "Flush",
            _ => "Other",
        };
        assert_eq!(
            actual_kind, expected_kind,
            "[{}] message kind mismatch",
            fixture.name
        );
    }

    // --- Souls + Fields + Values + Timestamps (Put only) ---
    if let Message::Put(put) = msg {
        let actual_souls: Vec<&String> = put.updated_nodes.keys().collect();

        if !fixture.expected.souls.is_empty() {
            assert_eq!(
                actual_souls.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                fixture.expected.souls.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                "[{}] souls mismatch",
                fixture.name
            );
        }

        for (soul, expected_fields) in &fixture.expected.fields {
            let children = put
                .updated_nodes
                .get(soul)
                .unwrap_or_else(|| panic!("[{}] soul {soul} not in parsed put", fixture.name));
            let actual_fields: Vec<&String> = children.keys().collect();
            assert_eq!(
                actual_fields.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                expected_fields.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                "[{}] fields mismatch for soul {soul}",
                fixture.name
            );
        }

        for (soul, expected_values) in &fixture.expected.values {
            let children = put
                .updated_nodes
                .get(soul)
                .unwrap_or_else(|| panic!("[{}] soul {soul} not in parsed put", fixture.name));
            for (key, expected_val) in expected_values {
                let node_data = children
                    .get(key)
                    .unwrap_or_else(|| panic!("[{}] key {key} not in soul {soul}", fixture.name));
                let actual_json: serde_json::Value = node_data.value.clone().into();
                assert_eq!(
                    actual_json, *expected_val,
                    "[{}] value mismatch for {soul}.{key}",
                    fixture.name
                );
            }
        }

        for (soul, expected_ts) in &fixture.expected.timestamps {
            let children = put
                .updated_nodes
                .get(soul)
                .unwrap_or_else(|| panic!("[{}] soul {soul} not in parsed put", fixture.name));
            for (key, expected_ts_val) in expected_ts {
                let node_data = children
                    .get(key)
                    .unwrap_or_else(|| panic!("[{}] key {key} not in soul {soul}", fixture.name));
                assert_eq!(
                    node_data.updated_at, *expected_ts_val,
                    "[{}] timestamp mismatch for {soul}.{key}",
                    fixture.name
                );
            }
        }
    }

    // --- Souls for Get ---
    if let Message::Get(get) = msg {
        if !fixture.expected.souls.is_empty() {
            assert_eq!(
                get.node_id, fixture.expected.souls[0],
                "[{}] get node_id mismatch",
                fixture.name
            );
        }
    }

    // --- Hi peer_id ---
    if let Message::Hi { peer_id, .. } = msg {
        if !fixture.expected.souls.is_empty() {
            assert_eq!(
                peer_id, &fixture.expected.souls[0],
                "[{}] Hi peer_id mismatch",
                fixture.name
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Single entry point: loads all fixtures and asserts each one.
    ///
    /// This test discovers every `.json` file under `tests/wire/fixtures/`
    /// at compile time (via `env!("CARGO_MANIFEST_DIR")`) and runs each
    /// through the assertion engine. Add fixtures without touching code.
    #[test]
    fn all_wire_fixtures() {
        let base = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("wire")
            .join("fixtures");

        let fixtures = load_fixtures(&base);
        assert!(
            !fixtures.is_empty(),
            "no wire fixtures found in {} — did you create fixture files?",
            base.display()
        );

        for fixture in &fixtures {
            // Most fixtures use allow_public_space=false (signed data only).
            // Fixtures that need public space set it via the input itself.
            run_fixture(fixture, Addr::noop(), fixture.allow_public_space);
        }
    }
}
