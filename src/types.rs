#![allow(clippy::inherent_to_string)] // to_string is wire-format serialization, not Display

//! Core type system for Rod — the Rust port of Gun.js.
//!
//! This module defines the fundamental data types that flow through the
//! distributed graph database:
//!
//! - [`Value`] — the five wire-compatible leaf types (null, bool, number, text, link)
//! - [`NodeData`] — a leaf node's payload (value + timestamp)
//! - [`Children`] — a branch node's sorted child map
//!
//! ## Gun.js Wire Compatibility
//!
//! All types implement [`serde::Serialize`] / [`serde::Deserialize`] so they
//! can be serialized to the JSON wire format that Gun.js uses. The
//! [`Value::to_string`] method produces the Gun.js wire representation
//! (not the [`std::fmt::Display`] representation).
//!
//! ## Value Validation
//!
//! Valid values are a subset of JSON: null, boolean, number (finite, not NaN
//! or Infinity), text, or a soul relation link. Arrays need special
//! algorithms to handle concurrency and are not supported directly. Objects
//! are valid *only* as node references: `{"#": soul}`.
//!
//! ## Example
//!
//! ```
//! use rod::types::{Value, NodeData};
//!
//! let v = Value::Text("hello".into());
//! assert_eq!(v.to_string(), "hello");
//! assert_eq!(v.size(), 5);
//!
//! let node = NodeData::default();
//! assert!(node.value.is_null());
//! ```

use serde::{Deserialize, Serialize};
use serde_json::{Value as SerdeJsonValue, json};
use std::collections::BTreeMap;
use std::convert::TryFrom;

/// Branch node — a sorted map of child key to child data.
///
/// Each entry represents one child of a graph node. The key is the child's
/// name within the parent; the value is the child's [`NodeData`] (value +
/// timestamp). `BTreeMap` is used to ensure deterministic iteration order,
/// which is important for consistent checksums across distributed peers.
pub type Children = BTreeMap<String, NodeData>;

/// Data stored in a leaf node of the graph.
///
/// Combines the actual [`Value`] with an `updated_at` timestamp (Unix epoch
/// seconds, as used by Gun.js). The timestamp determines conflict resolution:
/// when two peers send conflicting puts for the same key, the newer timestamp
/// wins.
///
/// # Conflict Resolution
///
/// Gun.js uses "last write wins" semantics based on `updated_at`. If two
/// peers write to the same key with the same timestamp, the implementation
/// does not guarantee ordering — this is by design in Gun.js.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct NodeData {
    /// The value stored at this node.
    pub value: Value,
    /// Unix timestamp (seconds) of the last update. Newer wins conflicts.
    pub updated_at: f64,
}

impl Default for NodeData {
    fn default() -> Self {
        Self {
            value: Value::Null,
            updated_at: 0.0,
        }
    }
}

/// Value types supported by Rod and Gun.js.
///
/// These are the five valid leaf types in the distributed graph. Each variant
/// maps to a JSON type, with the exception of [`Value::Link`] which represents
/// a Gun.js soul relation (`{"#": "node_id"}`).
///
/// # Wire Format
///
/// | Variant | JSON representation | Example |
/// |---------|---------------------|----------|
/// | `Null` | `null` | `Value::Null` |
/// | `Bit` | `true` / `false` | `Value::Bit(true)` |
/// | `Number` | number | `Value::Number(42.0)` |
/// | `Text` | string | `Value::Text("hello")` |
/// | `Link` | `{"#": "soul"}` | `Value::Link("node/abc")` |
///
/// # NaN / Infinity
///
/// `Value::Number(f64)` can technically hold NaN or Infinity, but these are
/// not valid in the Gun.js wire format. When converting to JSON, NaN/Infinity
/// will produce `null` (serde_json behavior). Callers should validate before
/// constructing `Value::Number` from untrusted input.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum Value {
    /// Absence of a value (not the same as "deleted").
    Null,
    /// Boolean flag.
    Bit(bool),
    /// Floating-point number. Should be finite (not NaN/Infinity).
    Number(f64),
    /// Unicode text string.
    Text(String),
    /// A soul relation — a reference to another node by ID.
    Link(String),
}

impl Value {
    /// Returns the approximate byte size of the value's payload.
    ///
    /// For [`Value::Text`], this is the string's byte length. For all other
    /// variants, it is the `size_of_val` of the enum discriminant + data.
    ///
    /// This is used for stats reporting and memory budgeting.
    pub fn size(&self) -> usize {
        match self {
            Value::Text(s) => s.len(),
            _ => std::mem::size_of_val(self),
        }
    }

    /// Serializes the value to its Gun.js wire-format string.
    ///
    /// **Not** a [`Display`] implementation — this produces the exact string
    /// representation used on the wire:
    ///
    /// - `Null` → `"null"`
    /// - `Bit(true)` → `"true"`
    /// - `Bit(false)` → `"false"`
    /// - `Number(42.0)` → `"42"`
    /// - `Text("hello")` → `"hello"`
    /// - `Link("node/abc")` → `"node/abc"` (the soul string, not JSON)
    ///
    /// For JSON serialization with proper typing, use
    /// `serde_json::to_string(&value)` instead.
    pub fn to_string(&self) -> String {
        match self {
            Value::Null => "null".to_string(),
            Value::Bit(bool) => {
                if *bool {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            Value::Number(n) => n.to_string(),
            Value::Text(t) => t.clone(),
            Value::Link(l) => l.clone(),
        }
    }

    /// Returns `true` if this value is [`Value::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns `true` if this value is [`Value::Bit`].
    pub fn is_bit(&self) -> bool {
        matches!(self, Value::Bit(_))
    }

    /// Returns `true` if this value is [`Value::Number`].
    pub fn is_number(&self) -> bool {
        matches!(self, Value::Number(_))
    }

    /// Returns `true` if this value is [`Value::Text`].
    pub fn is_text(&self) -> bool {
        matches!(self, Value::Text(_))
    }

    /// Returns `true` if this value is [`Value::Link`].
    pub fn is_link(&self) -> bool {
        matches!(self, Value::Link(_))
    }

    /// Returns `Some(bool)` if this value is [`Value::Bit`], else `None`.
    pub fn as_bit(&self) -> Option<bool> {
        match self {
            Value::Bit(b) => Some(*b),
            _ => None,
        }
    }

    /// Returns `Some(f64)` if this value is [`Value::Number`], else `None`.
    pub fn as_number(&self) -> Option<f64> {
        match self {
            Value::Number(n) => Some(*n),
            _ => None,
        }
    }

    /// Returns `Some(&str)` if this value is [`Value::Text`], else `None`.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(t) => Some(t.as_str()),
            _ => None,
        }
    }

    /// Returns `Some(&str)` if this value is [`Value::Link`], else `None`.
    pub fn as_link(&self) -> Option<&str> {
        match self {
            Value::Link(l) => Some(l.as_str()),
            _ => None,
        }
    }
}

/// Converts a [`serde_json::Value`] into a Rod [`Value`].
///
/// This conversion validates that the JSON value is one of the five
/// supported types. Objects are accepted *only* if they are a Gun.js
/// soul relation (`{"#": "soul"}`); all other objects are rejected.
///
/// # Errors
///
/// Returns `&'static str` if the JSON value cannot be represented as a
/// Rod [`Value`]:
/// - Arrays → `"cannot convert array into Value"`
/// - Non-soul objects → `"cannot convert json object into Value"`
/// - Numbers not convertible to f64 → `"not convertible to f64"`
///
/// # Security Note
///
/// For production use, consider wrapping this in a custom error type rather
/// than `&'static str`. The string error type limits programmatic error
/// handling. This is a known limitation to address in a future API revision.
impl TryFrom<SerdeJsonValue> for Value {
    type Error = &'static str;

    fn try_from(v: SerdeJsonValue) -> Result<Value, Self::Error> {
        match v {
            SerdeJsonValue::Null => Ok(Value::Null),
            SerdeJsonValue::Bool(b) => Ok(Value::Bit(b)),
            SerdeJsonValue::String(s) => Ok(Value::Text(s)),
            SerdeJsonValue::Number(n) => match n.as_f64() {
                Some(n) => Ok(Value::Number(n)),
                _ => Err("not convertible to f64"),
            },
            SerdeJsonValue::Object(obj) => {
                // Node reference: {"#": "soul"} → Value::Link
                if let Some(soul) = obj.get("#").and_then(|v| v.as_str()) {
                    Ok(Value::Link(soul.to_string()))
                } else {
                    Err("cannot convert json object into Value")
                }
            }
            SerdeJsonValue::Array(_) => Err("cannot convert array into Value"),
        }
    }
}

/// Converts a Rod [`Value`] into a [`serde_json::Value`].
///
/// This is the inverse of [`TryFrom<SerdeJsonValue> for Value`]. The
/// [`Value::Link`] variant serializes to a Gun.js soul relation object
/// (`{"#": "soul"}`) so it round-trips correctly through JSON.
impl From<Value> for SerdeJsonValue {
    fn from(v: Value) -> SerdeJsonValue {
        match v {
            Value::Null => SerdeJsonValue::Null,
            Value::Text(t) => SerdeJsonValue::String(t),
            Value::Bit(b) => SerdeJsonValue::Bool(b),
            Value::Number(n) => json!(n),
            Value::Link(l) => json!({ "#": l }),
        }
    }
}

impl From<usize> for Value {
    fn from(n: usize) -> Value {
        Value::Number(n as f64)
    }
}

impl From<f32> for Value {
    fn from(n: f32) -> Value {
        Value::Number(n as f64)
    }
}

impl From<u64> for Value {
    fn from(n: u64) -> Value {
        Value::Number(n as f64)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Value {
        Value::Text(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Value {
        Value::Text(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── NodeData ──

    #[test]
    fn test_nodedata_default() {
        let nd = NodeData::default();
        assert!(nd.value.is_null());
        assert_eq!(nd.updated_at, 0.0);
    }

    #[test]
    fn test_nodedata_partial_eq() {
        let a = NodeData { value: Value::Text("x".into()), updated_at: 1.0 };
        let b = NodeData { value: Value::Text("x".into()), updated_at: 1.0 };
        let c = NodeData { value: Value::Text("y".into()), updated_at: 1.0 };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // ── Value::size ──

    #[test]
    fn test_value_size_text() {
        assert_eq!(Value::Text("hello".into()).size(), 5);
        assert_eq!(Value::Text("".into()).size(), 0);
        assert_eq!(Value::Text("héllo".into()).size(), 6); // é is 2 bytes in UTF-8
    }

    #[test]
    fn test_value_size_non_text() {
        // Non-text variants use size_of_val
        assert!(Value::Null.size() > 0);
        assert!(Value::Bit(true).size() > 0);
        assert!(Value::Number(42.0).size() > 0);
        assert!(Value::Link("soul".into()).size() > 0);
    }

    // ── Value::to_string (wire format) ──

    #[test]
    fn test_value_to_string_null() {
        assert_eq!(Value::Null.to_string(), "null");
    }

    #[test]
    fn test_value_to_string_bit() {
        assert_eq!(Value::Bit(true).to_string(), "true");
        assert_eq!(Value::Bit(false).to_string(), "false");
    }

    #[test]
    fn test_value_to_string_number() {
        assert_eq!(Value::Number(42.0).to_string(), "42");
        assert_eq!(Value::Number(3.14).to_string(), "3.14");
    }

    #[test]
    fn test_value_to_string_text() {
        assert_eq!(Value::Text("hello".into()).to_string(), "hello");
    }

    #[test]
    fn test_value_to_string_link() {
        assert_eq!(Value::Link("node/abc".into()).to_string(), "node/abc");
    }

    // ── Value type predicates ──

    #[test]
    fn test_value_is_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Bit(false).is_null());
    }

    #[test]
    fn test_value_is_bit() {
        assert!(Value::Bit(true).is_bit());
        assert!(!Value::Null.is_bit());
    }

    #[test]
    fn test_value_is_number() {
        assert!(Value::Number(1.0).is_number());
        assert!(!Value::Null.is_number());
    }

    #[test]
    fn test_value_is_text() {
        assert!(Value::Text("x".into()).is_text());
        assert!(!Value::Null.is_text());
    }

    #[test]
    fn test_value_is_link() {
        assert!(Value::Link("soul".into()).is_link());
        assert!(!Value::Null.is_link());
    }

    // ── Value accessors ──

    #[test]
    fn test_value_as_bit() {
        assert_eq!(Value::Bit(true).as_bit(), Some(true));
        assert_eq!(Value::Bit(false).as_bit(), Some(false));
        assert_eq!(Value::Null.as_bit(), None);
    }

    #[test]
    fn test_value_as_number() {
        assert_eq!(Value::Number(42.0).as_number(), Some(42.0));
        assert_eq!(Value::Null.as_number(), None);
    }

    #[test]
    fn test_value_as_text() {
        assert_eq!(Value::Text("hello".into()).as_text(), Some("hello"));
        assert_eq!(Value::Null.as_text(), None);
    }

    #[test]
    fn test_value_as_link() {
        assert_eq!(Value::Link("soul".into()).as_link(), Some("soul"));
        assert_eq!(Value::Null.as_link(), None);
    }

    // ── TryFrom<JsonValue> ──

    #[test]
    fn test_try_from_json_null() {
        let v = Value::try_from(SerdeJsonValue::Null).unwrap();
        assert!(v.is_null());
    }

    #[test]
    fn test_try_from_json_bool() {
        let v = Value::try_from(SerdeJsonValue::Bool(true)).unwrap();
        assert_eq!(v.as_bit(), Some(true));
    }

    #[test]
    fn test_try_from_json_string() {
        let v = Value::try_from(SerdeJsonValue::String("hello".into())).unwrap();
        assert_eq!(v.as_text(), Some("hello"));
    }

    #[test]
    fn test_try_from_json_number() {
        let v = Value::try_from(serde_json::json!(42.0)).unwrap();
        assert_eq!(v.as_number(), Some(42.0));
    }

    #[test]
    fn test_try_from_json_link() {
        let json = serde_json::json!({ "#": "node/abc" });
        let v = Value::try_from(json).unwrap();
        assert_eq!(v.as_link(), Some("node/abc"));
    }

    #[test]
    fn test_try_from_json_array_fails() {
        let json = serde_json::json!([1, 2, 3]);
        assert!(Value::try_from(json).is_err());
    }

    #[test]
    fn test_try_from_json_object_without_soul_fails() {
        let json = serde_json::json!({ "foo": "bar" });
        assert!(Value::try_from(json).is_err());
    }

    // ── From<Value> for JsonValue (round-trip) ──

    #[test]
    fn test_roundtrip_null() {
        let v = Value::Null;
        let json: SerdeJsonValue = v.clone().into();
        let v2 = Value::try_from(json).unwrap();
        assert_eq!(v, v2);
    }

    #[test]
    fn test_roundtrip_bit() {
        let v = Value::Bit(true);
        let json: SerdeJsonValue = v.clone().into();
        let v2 = Value::try_from(json).unwrap();
        assert_eq!(v, v2);
    }

    #[test]
    fn test_roundtrip_text() {
        let v = Value::Text("hello world".into());
        let json: SerdeJsonValue = v.clone().into();
        let v2 = Value::try_from(json).unwrap();
        assert_eq!(v, v2);
    }

    #[test]
    fn test_roundtrip_number() {
        let v = Value::Number(42.0);
        let json: SerdeJsonValue = v.clone().into();
        let v2 = Value::try_from(json).unwrap();
        assert_eq!(v, v2);
    }

    #[test]
    fn test_roundtrip_link() {
        let v = Value::Link("node/abc".into());
        let json: SerdeJsonValue = v.clone().into();
        let v2 = Value::try_from(json).unwrap();
        assert_eq!(v, v2);
    }

    // ── From<&str> / From<String> ──

    #[test]
    fn test_from_str() {
        let v: Value = "hello".into();
        assert_eq!(v.as_text(), Some("hello"));
    }

    #[test]
    fn test_from_string() {
        let v: Value = String::from("world").into();
        assert_eq!(v.as_text(), Some("world"));
    }

    // ── From<usize> / From<u64> / From<f32> ──

    #[test]
    fn test_from_usize() {
        let v: Value = 42usize.into();
        assert_eq!(v.as_number(), Some(42.0));
    }

    #[test]
    fn test_from_u64() {
        let v: Value = 99u64.into();
        assert_eq!(v.as_number(), Some(99.0));
    }

    #[test]
    fn test_from_f32() {
        let v: Value = 3.14f32.into();
        assert!((v.as_number().unwrap() - 3.14).abs() < 0.001);
    }

    // ── Children type alias ──

    #[test]
    fn test_children_btreemap() {
        let mut children: Children = BTreeMap::new();
        children.insert(
            "key1".to_string(),
            NodeData { value: Value::Text("v1".into()), updated_at: 1.0 },
        );
        children.insert(
            "key2".to_string(),
            NodeData { value: Value::Text("v2".into()), updated_at: 2.0 },
        );
        // BTreeMap is sorted
        let keys: Vec<&String> = children.keys().collect();
        assert_eq!(keys, vec!["key1", "key2"]);
    }
}
