use crate::crdt::{PeerState, PeerStatus, PeerTable};
use crate::net::NetManager;
use crate::node::NodeState;
use crate::protocol::{Opcode, Packet};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
pub enum GossipPacket {
    Delta(Vec<(String, PeerState)>),
    Full(PeerTable),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AnnouncePayload {
    pub node_id: String,
    pub incarnation: u64,
    pub address: SocketAddr,
}

pub struct GossipManager {
    pub net_manager: NetManager,
    pub peer_table: PeerTable,
    pub node_state: NodeState,
}

impl GossipManager {
    pub fn new(net_manager: NetManager, node_state: NodeState) -> Self {
        Self {
            net_manager,
            peer_table: PeerTable::new(),
            node_state,
        }
    }

    pub fn calculate_announce_jitter(priority: u8) -> Duration {
        let mut rng = rand::thread_rng();
        let base_ms = priority as u64 * 100;
        let jitter_ms = rng.gen_range(0..500);
        Duration::from_millis(base_ms + jitter_ms)
    }

    pub async fn send_announce(
        &self,
        multi_addr: SocketAddr,
        payload: Vec<u8>,
    ) -> std::io::Result<()> {
        let packet = Packet::new(
            Opcode::AnnounceMulticast,
            self.node_state.cluster_token,
            payload,
        );
        self.net_manager
            .send_to(&packet.encode(), &multi_addr)
            .await?;
        Ok(())
    }

    pub async fn send_ping(&self, peer_addr: &SocketAddr) -> std::io::Result<()> {
        let packet = Packet::new(
            Opcode::PingUnicast,
            self.node_state.cluster_token,
            b"PING".to_vec(),
        );
        self.net_manager
            .send_to(&packet.encode(), peer_addr)
            .await?;
        Ok(())
    }

    pub async fn send_delta_gossip(&mut self, peer_addr: &SocketAddr) -> std::io::Result<usize> {
        let deltas = self.peer_table.get_deltas();
        if deltas.is_empty() {
            return Ok(0);
        }

        let packet_data = GossipPacket::Delta(deltas);
        let payload = serde_json::to_vec(&packet_data).map_err(std::io::Error::other)?;

        let packet = Packet::new(Opcode::GossipDelta, self.node_state.cluster_token, payload);
        self.net_manager.send_to(&packet.encode(), peer_addr).await
    }

    pub fn handle_packet(&mut self, payload: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::decode(payload, self.node_state.cluster_token)?;
        let debug_msg = format!(
            "[DEBUG] Packet decoded: opcode={:?}, len={}",
            packet.header.opcode,
            packet.payload.len()
        );
        println!("{}", debug_msg);
        self.node_state.log_debug(&debug_msg);

        match packet.header.opcode {
            Opcode::GossipFull | Opcode::GossipDelta => self.handle_gossip(&packet.payload),
            Opcode::PingUnicast => {
                // Send pong back
                // Note: This would normally be async, but we are in a sync function.
                // For now, we'll just log it or handle it via a channel.
                Ok(())
            }
            Opcode::AnnounceMulticast => self.handle_announce(&packet.payload),
            _ => {
                let debug_msg = format!("[DEBUG] Unhandled opcode: {:?}", packet.header.opcode);
                println!("{}", debug_msg);
                self.node_state.log_debug(&debug_msg);
                Ok(())
            }
        }
    }

    pub fn handle_announce(&mut self, payload: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let announce: AnnouncePayload = serde_json::from_slice(payload)?;
        let debug_msg = format!(
            "[DEBUG] Announce parsed: node_id={}, incarnation={}",
            announce.node_id, announce.incarnation
        );
        println!("{}", debug_msg);
        self.node_state.log_debug(&debug_msg);

        // Ignore self
        if announce.node_id == self.node_state.node_id() {
            let debug_msg = format!("[DEBUG] Ignoring self-announce: {}", announce.node_id);
            println!("{}", debug_msg);
            self.node_state.log_debug(&debug_msg);
            return Ok(());
        }

        let state = PeerState {
            address: announce.address,
            incarnation: announce.incarnation,
            status: PeerStatus::Alive,
        };

        let debug_msg = format!(
            "[DEBUG] Inserting peer: {} at {}",
            announce.node_id, announce.address
        );
        println!("{}", debug_msg);
        self.node_state.log_debug(&debug_msg);
        self.peer_table.update_last_seen(&announce.node_id);
        self.peer_table.insert(announce.node_id, state);
        Ok(())
    }

    pub fn handle_gossip(&mut self, payload: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let packet: GossipPacket = serde_json::from_slice(payload)?;
        match packet {
            GossipPacket::Delta(deltas) => {
                for (node_id, state) in deltas {
                    // Ignore self updates unless it's for self-refutation
                    if node_id == self.node_state.node_id() {
                        if state.status == PeerStatus::Suspect {
                            self.refute_suspect();
                        }
                        continue;
                    }

                    self.peer_table.update_last_seen(&node_id);
                    self.peer_table.insert(node_id, state);
                }
            }
            GossipPacket::Full(table) => {
                // We can't easily filter the whole table in one go without a custom merge,
                // but PeerTable::merge should handle the LWW logic.
                // To be safe, we can filter the table before merging if needed.
                self.peer_table.merge(&table);
            }
        }
        Ok(())
    }

    fn refute_suspect(&mut self) {
        self.node_state.bump_incarnation();

        let my_state = PeerState {
            address: self.net_manager.local_addr(),
            incarnation: self.node_state.incarnation(),
            status: PeerStatus::Alive,
        };
        self.peer_table
            .insert(self.node_state.node_id().to_string(), my_state);
    }

    pub fn handle_pong(&mut self, peer_addr: &SocketAddr, payload: &[u8]) {
        // Now wrapped in binary packet
        if let Ok(packet) = Packet::decode(payload, self.node_state.cluster_token)
            && packet.header.opcode == Opcode::PongUnicast
            && packet.payload == b"PONG"
        {
            self.peer_table.update_last_seen(&peer_addr.to_string());
            self.peer_table.insert(
                peer_addr.to_string(),
                PeerState {
                    address: *peer_addr,
                    incarnation: 0,
                    status: PeerStatus::Alive,
                },
            );
        }
    }

    pub fn tick(&mut self) {
        self.peer_table.check_timeouts(
            Duration::from_secs(5),  // suspect_timeout
            Duration::from_secs(15), // dead_timeout
        );
        self.peer_table.cleanup_tombstones(Duration::from_secs(30));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jitter_variation() {
        let p = 1;
        let d1 = GossipManager::calculate_announce_jitter(p);
        let d2 = GossipManager::calculate_announce_jitter(p);

        assert_ne!(d1, d2, "Jitter should be random");
        assert!(d1 >= Duration::from_millis(100));
        assert!(d1 <= Duration::from_millis(600));
    }

    #[tokio::test]
    async fn test_send_announce() {
        let net_manager = NetManager::new(0).expect("Should bind to port 0");
        let node_state = NodeState::init(tempfile::tempdir().unwrap().keep().to_str().unwrap());
        let gossip_manager = GossipManager::new(net_manager, node_state);

        let multi_addr: SocketAddr = "239.0.0.1:9999".parse().unwrap();
        let payload = b"test announce";

        let result = gossip_manager
            .send_announce(multi_addr, payload.to_vec())
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_heartbeat_ping_pong() {
        let net_manager = NetManager::new(0).expect("Should bind to port 0");
        let node_state = NodeState::init(tempfile::tempdir().unwrap().keep().to_str().unwrap());
        let mut gossip_manager = GossipManager::new(net_manager, node_state);
        let token = gossip_manager.node_state.cluster_token;

        let peer_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        let _ = gossip_manager.send_ping(&peer_addr).await;

        let pong_packet = Packet::new(Opcode::PongUnicast, token, b"PONG".to_vec());
        let pong_payload = pong_packet.encode();
        gossip_manager.handle_pong(&peer_addr, &pong_payload);

        let state = gossip_manager
            .peer_table
            .get(&peer_addr.to_string())
            .expect("Peer should be alive");
        assert_eq!(state.status, PeerStatus::Alive);
    }

    #[tokio::test]
    async fn test_delta_gossip_efficiency() {
        let net_manager = NetManager::new(0).expect("Should bind to port 0");
        let node_state = NodeState::init(tempfile::tempdir().unwrap().keep().to_str().unwrap());
        let mut gossip_manager = GossipManager::new(net_manager, node_state);
        let peer_addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        // 1. Initial state: no changes
        let bytes_sent = gossip_manager.send_delta_gossip(&peer_addr).await.unwrap();
        assert_eq!(
            bytes_sent, 0,
            "Should send 0 bytes when no changes occurred"
        );

        // 2. Change one node
        gossip_manager.peer_table.insert(
            "node-1".to_string(),
            PeerState {
                address: "127.0.0.1:9001".parse().unwrap(),
                incarnation: 1,
                status: PeerStatus::Alive,
            },
        );

        let bytes_sent = gossip_manager.send_delta_gossip(&peer_addr).await.unwrap();
        assert!(bytes_sent > 0);

        // 3. Second send should be empty (dirty set cleared)
        let bytes_sent = gossip_manager.send_delta_gossip(&peer_addr).await.unwrap();
        assert_eq!(
            bytes_sent, 0,
            "Should send 0 bytes after dirty set is cleared"
        );

        // 4. Change multiple nodes, verify delta size
        gossip_manager.peer_table.insert(
            "node-1".to_string(),
            PeerState {
                address: "127.0.0.1:9001".parse().unwrap(),
                incarnation: 2,
                status: PeerStatus::Alive,
            },
        );
        gossip_manager.peer_table.insert(
            "node-2".to_string(),
            PeerState {
                address: "127.0.0.1:9002".parse().unwrap(),
                incarnation: 1,
                status: PeerStatus::Alive,
            },
        );

        let deltas = gossip_manager.peer_table.get_deltas();
        assert_eq!(deltas.len(), 2, "Delta should contain exactly 2 updates");
    }
}
