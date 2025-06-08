use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerStatus {
    Alive,
    Suspect,
    Dead,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerState {
    pub address: SocketAddr,
    pub incarnation: u64,
    pub status: PeerStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerHistory {
    pub current: PeerState,
    pub previous: Option<PeerState>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerTable {
    peers: HashMap<String, PeerHistory>,
    dirty_nodes: HashSet<String>,
    #[serde(skip)]
    last_seen: HashMap<String, Instant>,
    #[serde(skip)]
    dead_at: HashMap<String, Instant>,
}

impl PeerTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    pub fn active_count(&self) -> usize {
        let active = self
            .peers
            .values()
            .filter(|h| h.current.status == PeerStatus::Alive)
            .count();

        #[cfg(debug_assertions)]
        {
            let all_peers: Vec<_> = self
                .peers
                .iter()
                .map(|(id, h)| (id, h.current.status))
                .collect();
            println!(
                "[DEBUG] active_count: {}/{} alive. Peers: {:?}",
                active,
                self.peers.len(),
                all_peers
            );
        }

        active
    }

    pub fn insert(&mut self, node_id: String, state: PeerState) {
        let changed = if let Some(history) = self.peers.get_mut(&node_id) {
            if state.incarnation > history.current.incarnation {
                if history.current.status == PeerStatus::Dead && state.status == PeerStatus::Alive {
                    println!(
                        "[CRDT] Node {} resurrected! Incarnation bumped to {}",
                        node_id, state.incarnation
                    );
                }
                history.previous = Some(history.current.clone());
                history.current = state.clone();
                true
            } else {
                false
            }
        } else {
            self.peers.insert(
                node_id.clone(),
                PeerHistory {
                    current: state.clone(),
                    previous: None,
                },
            );
            true
        };

        #[cfg(debug_assertions)]
        {
            println!(
                "[DEBUG] PeerTable::insert: node={} status={:?} inc={} changed={}",
                node_id, state.status, state.incarnation, changed
            );
        }

        if changed {
            self.dirty_nodes.insert(node_id);
        }
    }

    pub fn update_last_seen(&mut self, node_id: &str) {
        self.last_seen.insert(node_id.to_string(), Instant::now());
        self.dead_at.remove(node_id);
    }

    pub fn get(&self, node_id: &str) -> Option<&PeerState> {
        self.peers.get(node_id).map(|h| &h.current)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &PeerState)> {
        self.peers.iter().map(|(k, v)| (k, &v.current))
    }

    pub fn last_seen(&self, node_id: &str) -> Option<Instant> {
        self.last_seen.get(node_id).copied()
    }

    pub fn merge(&mut self, other: &PeerTable) {
        for (node_id, other_history) in &other.peers {
            self.insert(node_id.clone(), other_history.current.clone());
        }
    }

    pub fn validate_packet(&self, node_id: &str, incarnation: u64) -> bool {
        if let Some(history) = self.peers.get(node_id)
            && let Some(prev) = &history.previous
                && incarnation < prev.incarnation {
            return false;
        }
        true
    }

    pub fn get_deltas(&mut self) -> Vec<(String, PeerState)> {
        let mut deltas = Vec::new();
        for node_id in self.dirty_nodes.drain() {
            if let Some(history) = self.peers.get(&node_id) {
                deltas.push((node_id, history.current.clone()));
            }
        }
        deltas
    }

    pub fn check_timeouts(&mut self, suspect_timeout: Duration, dead_timeout: Duration) {
        let now = Instant::now();
        let mut to_suspect = Vec::new();
        let mut to_dead = Vec::new();

        for (node_id, last_seen) in &self.last_seen {
            let elapsed = now.duration_since(*last_seen);
            if let Some(state) = self.peers.get(node_id) {
                if state.current.status == PeerStatus::Alive && elapsed > suspect_timeout {
                    to_suspect.push(node_id.clone());
                } else if state.current.status == PeerStatus::Suspect && elapsed > dead_timeout {
                    to_dead.push(node_id.clone());
                }
            }
        }

        for node_id in to_suspect {
            if let Some(history) = self.peers.get_mut(&node_id) {
                println!(
                    "[GOSSIP] Peer {} status transition: Alive -> Suspect",
                    node_id
                );
                let _ = std::io::stdout().flush();
                history.current.status = PeerStatus::Suspect;
                self.dirty_nodes.insert(node_id);
            }
        }

        for node_id in to_dead {
            if let Some(history) = self.peers.get_mut(&node_id) {
                println!(
                    "[GOSSIP] Peer {} status transition: Suspect -> Dead",
                    node_id
                );
                let _ = std::io::stdout().flush();
                history.current.status = PeerStatus::Dead;
                self.dirty_nodes.insert(node_id.clone());
                self.dead_at.insert(node_id, now);
            }
        }
    }

    pub fn cleanup_tombstones(&mut self, tombstone_timeout: Duration) {
        let now = Instant::now();
        let mut to_remove = Vec::new();

        for (node_id, dead_at) in &self.dead_at {
            if now.duration_since(*dead_at) > tombstone_timeout {
                to_remove.push(node_id.clone());
            }
        }

        for node_id in to_remove {
            println!("[GC] Purging dead tombstone for node {}", node_id);
            self.peers.remove(&node_id);
            self.last_seen.remove(&node_id);
            self.dead_at.remove(&node_id);
            self.dirty_nodes.remove(&node_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lww_merge_higher_incarnation_wins() {
        let mut table1 = PeerTable::new();
        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        table1.insert(
            "node-1".to_string(),
            PeerState {
                address: addr1,
                incarnation: 10,
                status: PeerStatus::Alive,
            },
        );

        let mut table2 = PeerTable::new();
        table2.insert(
            "node-1".to_string(),
            PeerState {
                address: addr2,
                incarnation: 11, // Higher incarnation
                status: PeerStatus::Suspect,
            },
        );

        table1.merge(&table2);

        let state = table1.get("node-1").expect("Node-1 should exist");
        assert_eq!(state.incarnation, 11);
        assert_eq!(state.address, addr2);
        assert_eq!(state.status, PeerStatus::Suspect);
    }

    #[test]
    fn test_lww_merge_lower_incarnation_ignored() {
        let mut table1 = PeerTable::new();
        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        table1.insert(
            "node-1".to_string(),
            PeerState {
                address: addr1,
                incarnation: 15,
                status: PeerStatus::Alive,
            },
        );

        let mut table2 = PeerTable::new();
        table2.insert(
            "node-1".to_string(),
            PeerState {
                address: addr2,
                incarnation: 10, // Lower incarnation
                status: PeerStatus::Suspect,
            },
        );

        table1.merge(&table2);

        let state = table1.get("node-1").expect("Node-1 should exist");
        assert_eq!(state.incarnation, 15);
        assert_eq!(state.address, addr1);
        assert_eq!(state.status, PeerStatus::Alive);
    }

    #[test]
    fn test_validate_packet_causality() {
        let mut table = PeerTable::new();
        let node_id = "node-1".to_string();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Initial state: current=10, previous=None
        table.insert(
            node_id.clone(),
            PeerState {
                address: addr,
                incarnation: 10,
                status: PeerStatus::Alive,
            },
        );

        // Update to 11: current=11, previous=10
        table.insert(
            node_id.clone(),
            PeerState {
                address: addr,
                incarnation: 11,
                status: PeerStatus::Alive,
            },
        );

        assert!(
            table.validate_packet(&node_id, 11),
            "Current incarnation should be accepted"
        );
        assert!(
            table.validate_packet(&node_id, 12),
            "Higher incarnation should be accepted"
        );
        assert!(
            !table.validate_packet(&node_id, 9),
            "Incarnation lower than previous (10) should be rejected"
        );
    }
}
