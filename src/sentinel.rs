//! BEAM protocol sentinel strings — the single source of truth.
//!
//! BEAM uses sentinel values embedded in Put payloads to signal internal
//! routing events that ordinary key/value traffic must not trigger. These
//! strings are wire-protocol-adjacent: they appear as node IDs / child keys
//! inside Put messages, so **renaming any of them is a breaking change** to
//! any deployed mesh (peers match on the literal). This module exists so
//! that (a) BEAM's internal call sites can never drift from one another —
//! the historical failure mode where doc comments said
//! `__beam_replay_complete__` while the source still carried the Rod-era
//! string — and (b) consumers can `use beam::sentinel::*` to recognize
//! these markers in received Puts instead of hard-coding the strings.
//!
//! # What belongs here (and what does not)
//!
//! **In scope:** sentinels that flow through the Put pipeline as payload
//! markers — ack/error replies, replay-completion markers, and quorum
//! notifications.
//!
//! **Deliberately NOT here:** storage table identifiers (`rod_nodes_v1`,
//! `rod_meta_v1`, `ROD_NODES`, `ROD_META`, `rod_persy_test_`). Those are
//! persistence-format identifiers, not protocol sentinels — renaming them
//! would orphan existing databases rather than break a live mesh. They
//! stay as literal table names in the storage adapters.

/// Child key carried by a successful storage-commit reply.
///
/// Produced by storage adapters when a Put (or batch) commits; consumed by
/// `Node`'s ack decoder to resolve the write's `Future`. Any child key
/// matching this sentinel inside a Put aimed at a pending write is the
/// "your data is durable" signal.
pub const ACK: &str = "_ack";

/// Child key carried by a failed storage-commit reply.
///
/// Same pipeline as [`ACK`], but the payload's children carry the error
/// detail. A pending write matching this sentinel resolves to `Err` —
/// the storage layer refused or failed the write.
pub const ERR: &str = "_err";

/// Replay-completion marker Put, emitted after a storage adapter finishes
/// replaying persisted children to a fresh `map()` subscriber.
///
/// Storage adapters stream a subscriber's existing children as replay Puts
/// (each tagged `in_response_to` the original Get), then send this sentinel
/// so the subscriber knows the replay is *complete* — any value arriving
/// after it is live traffic, not history. Renaming this string would leave
/// every `map()` consumer hanging, waiting for a replay that already ended.
pub const REPLAY_COMPLETE: &str = "__beam_replay_complete__";

/// Quorum-met notification Put, emitted by the Router when a
/// replication-ack threshold registered via `RegisterQuorum` is satisfied.
///
/// The Router routes this sentinel Put back to the originating Node actor,
/// which resolves the awaiting replication `Future` with "quorum reached."
/// Carries `Value::Bool(true)`; the quorum channel doubles as the timeout
/// notification path.
pub const QUORUM_MET: &str = "__quorum_met__";

#[cfg(test)]
mod tests {
    use super::*;

    /// Rename-guards: each assertion pins the exact wire string. If a
    /// sentinel must ever change, that is a *protocol migration* — update
    /// the constant, every internal call site (they all reference these
    /// consts), and then these tests — never the strings piecemeal.
    #[test]
    fn ack_sentinel_exact_value() {
        assert_eq!(ACK, "_ack");
    }

    #[test]
    fn err_sentinel_exact_value() {
        assert_eq!(ERR, "_err");
    }

    #[test]
    fn replay_complete_sentinel_exact_value() {
        assert_eq!(REPLAY_COMPLETE, "__beam_replay_complete__");
    }

    #[test]
    fn quorum_met_sentinel_exact_value() {
        assert_eq!(QUORUM_MET, "__quorum_met__");
    }

    /// Sentinels must stay distinct from one another — a collision would
    /// make one pipeline's marker trigger another's.
    #[test]
    fn sentinels_are_mutually_distinct() {
        let all = [ACK, ERR, REPLAY_COMPLETE, QUORUM_MET];
        for (i, a) in all.iter().enumerate() {
            for b in all.iter().skip(i + 1) {
                assert_ne!(a, b, "sentinels must be distinct: {a} vs {b}");
            }
        }
    }
}
