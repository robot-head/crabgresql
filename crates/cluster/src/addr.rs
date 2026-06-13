//! Helpers for the packed node address `"<node_addr>|<sql_addr>"` carried in
//! `openraft::BasicNode.addr`. The node half is the Raft RPC / control listener;
//! the sql half is the pgwire listener (used by leader-routing to proxy SQL).

/// The Raft RPC / control address — the part before `|`. Returns the whole
/// string when there is no `|` (an un-packed addr, e.g. the in-process
/// `testcluster`), so existing callers are unaffected.
pub fn node_dial_addr(addr: &str) -> &str {
    addr.split('|').next().unwrap_or(addr)
}

/// The pgwire SQL address — the part after `|`, or `None` if the addr is not
/// packed with a sql half.
pub fn sql_addr_part(addr: &str) -> Option<&str> {
    addr.split('|').nth(1)
}

/// Pack a node + sql address into the `BasicNode.addr` form.
pub fn pack(node_addr: &str, sql_addr: &str) -> String {
    format!("{node_addr}|{sql_addr}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packs_and_splits() {
        let a = pack("127.0.0.1:5001", "127.0.0.1:6001");
        assert_eq!(a, "127.0.0.1:5001|127.0.0.1:6001");
        assert_eq!(node_dial_addr(&a), "127.0.0.1:5001");
        assert_eq!(sql_addr_part(&a), Some("127.0.0.1:6001"));
    }

    #[test]
    fn unpacked_addr_is_node_addr() {
        assert_eq!(node_dial_addr("127.0.0.1:5001"), "127.0.0.1:5001");
        assert_eq!(sql_addr_part("127.0.0.1:5001"), None);
    }
}
