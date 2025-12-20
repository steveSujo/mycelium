#![allow(unused_mut)]

use mycelium_core::{
    AnnouncePayload, DISCOVERY_PORT, GossipManager, MULTICAST_ADDR, NetManager, NodeState,
};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::Mutex;
use tokio::time::interval;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // Create temp directory for node state persistence
    let dir = tempdir()?;
    let storage_path = dir.path().to_str().unwrap().to_string();

    let mut node_state = NodeState::init(&storage_path);
    node_state.cluster_token = 12345;

    let net_manager = NetManager::new_with_multicast(
        DISCOVERY_PORT,
        Some((MULTICAST_ADDR.parse()?, Ipv4Addr::UNSPECIFIED)),
    )?;

    let local_addr = net_manager.local_addr();
    let node_id = node_state.node_id().to_string();
    let incarnation = node_state.incarnation();

    println!("Starting Mycelium Node:");
    println!("  Node ID: {}", node_id);
    println!("  Address: {}", local_addr);
    println!("  Cluster Token: {}", node_state.cluster_token);

    let gossip_manager = Arc::new(Mutex::new(GossipManager::new(net_manager, node_state)));

    // Clone socket and gossip manager for background receiver loop
    let recv_gm = Arc::clone(&gossip_manager);
    let socket = gossip_manager.lock().await.net_manager.socket.clone();

    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        loop {
            if let Ok((n, addr)) = socket.recv_from(&mut buf).await {
                let payload = buf[..n].to_vec();
                let mut gm = recv_gm.lock().await;
                if let Err(e) = gm.handle_packet(&payload) {
                    eprintln!("Error handling packet from {}: {}", addr, e);
                }
            }
        }
    });

    let mut tick_interval = interval(Duration::from_secs(1));
    let mut announce_interval = interval(Duration::from_secs(2));
    let mut heartbeat_interval = interval(Duration::from_secs(5));

    // Send initial announce
    {
        let mut gm = gossip_manager.lock().await;
        let announce = AnnouncePayload {
            node_id: node_id.clone(),
            incarnation,
            address: local_addr,
        };
        let payload = serde_json::to_vec(&announce).unwrap();
        let multi_addr: SocketAddr = format!("{}:{}", MULTICAST_ADDR, DISCOVERY_PORT)
            .parse()
            .unwrap();
        gm.send_announce(multi_addr, payload).await.unwrap();
        println!("Sent initial ANNOUNCE_MULTICAST");
    }

    loop {
        tokio::select! {
            _ = tick_interval.tick() => {
                let mut gm = gossip_manager.lock().await;
                gm.tick();
            }
            _ = announce_interval.tick() => {
                let mut gm = gossip_manager.lock().await;
                let announce = AnnouncePayload {
                    node_id: node_id.clone(),
                    incarnation,
                    address: local_addr,
                };
                let payload = serde_json::to_vec(&announce).unwrap();
                let multi_addr: SocketAddr = format!("{}:{}", MULTICAST_ADDR, DISCOVERY_PORT).parse().unwrap();
                if let Err(e) = gm.send_announce(multi_addr, payload).await {
                    eprintln!("Failed to send announce: {}", e);
                }
            }
            _ = heartbeat_interval.tick() => {
                let gm = gossip_manager.lock().await;
                println!(
                    "[Heartbeat] Active peers in table: {}",
                    gm.peer_table.iter().count()
                );
            }
        }
    }
}
