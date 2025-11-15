use mycelium_core::crdt::PeerStatus;
use mycelium_core::{
    AnnouncePayload, GossipManager, MULTICAST_ADDR, NetManager, NodeState,
};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::Mutex;

struct OverseerNode {
    id: String,
    manager: Arc<Mutex<GossipManager>>,
    addr: SocketAddr,
    recv_handle: tokio::task::JoinHandle<()>,
    _temp_dir: tempfile::TempDir,
}

impl Drop for OverseerNode {
    fn drop(&mut self) {
        self.recv_handle.abort();
    }
}

struct ClusterOverseer {
    nodes: Vec<OverseerNode>,
}

impl ClusterOverseer {
    async fn spawn_cluster(size: usize, token: u32) -> Self {
        println!("[Overseer] Spawning cluster of size {}...", size);
        let mut nodes = Vec::with_capacity(size);

        for i in 0..size {
            let temp_dir = tempdir().unwrap();
            let storage_path = temp_dir.path().to_str().unwrap().to_string();

            let mut node_state = NodeState::init(&storage_path);
            node_state.cluster_token = token;
            let id = node_state.node_id().to_string();

            let net_manager = NetManager::new_with_multicast(
                0,
                Some((MULTICAST_ADDR.parse().unwrap(), Ipv4Addr::UNSPECIFIED)),
            )
            .expect("Failed to bind socket");

            let addr = net_manager.local_addr();
            let gossip_manager = Arc::new(Mutex::new(GossipManager::new(net_manager, node_state)));

            let recv_gm = Arc::clone(&gossip_manager);
            let socket = gossip_manager.lock().await.net_manager.socket.clone();

            let recv_handle = tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                loop {
                    if let Ok((n, _addr)) = socket.recv_from(&mut buf).await {
                        let payload = buf[..n].to_vec();
                        let mut gm = recv_gm.lock().await;
                        let _ = gm.handle_packet(&payload);
                    }
                }
            });

            println!("[Overseer] Spawned node {} with ID {} at {}", i, id, addr);
            nodes.push(OverseerNode {
                id,
                manager: gossip_manager,
                addr,
                recv_handle,
                _temp_dir: temp_dir,
            });
        }

        Self { nodes }
    }

    async fn connect_all(&self) {
        println!("[Overseer] Connecting all nodes in cluster...");
        let mut announces = Vec::new();
        for n in &self.nodes {
            let gm = n.manager.lock().await;
            let ann = AnnouncePayload {
                node_id: n.id.clone(),
                incarnation: gm.node_state.incarnation(),
                address: n.addr,
            };
            announces.push(serde_json::to_vec(&ann).unwrap());
        }

        for (i, node) in self.nodes.iter().enumerate() {
            let mut gm = node.manager.lock().await;
            for (j, ann) in announces.iter().enumerate() {
                if i != j {
                    let _ = gm.handle_announce(ann);
                }
            }
        }
        println!("[Overseer] All nodes connected.");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster_direct_convergence() {
    println!("Step 1: Spawning 3-node cluster for direct convergence...");
    let overseer = ClusterOverseer::spawn_cluster(3, 1337).await;

    println!("Step 2: Connecting all nodes...");
    overseer.connect_all().await;

    println!("Step 3: Verifying peer table counts and Alive status...");
    for node in &overseer.nodes {
        let gm = node.manager.lock().await;
        assert_eq!(gm.peer_table.iter().count(), 2);
        for (_, state) in gm.peer_table.iter() {
            assert_eq!(state.status, PeerStatus::Alive);
        }
    }
    println!("Test passed successfully.");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster_crdt_delta_gossip_propagation() {
    println!("Step 1: Spawning cluster for delta gossip propagation...");
    let overseer = ClusterOverseer::spawn_cluster(3, 1338).await;
    overseer.connect_all().await;

    println!("Step 2: Mutating Node A state (bump incarnation)...");
    let node_a_id = overseer.nodes[0].id.clone();
    {
        let mut gm_a = overseer.nodes[0].manager.lock().await;
        gm_a.node_state.bump_incarnation();
    }

    let ann_a = {
        let gm_a = overseer.nodes[0].manager.lock().await;
        serde_json::to_vec(&AnnouncePayload {
            node_id: node_a_id.clone(),
            incarnation: gm_a.node_state.incarnation(),
            address: overseer.nodes[0].addr,
        }).unwrap()
    };

    println!("Step 3: Propagating Node A update to peer nodes...");
    for node in overseer.nodes.iter().skip(1) {
        let mut gm = node.manager.lock().await;
        let _ = gm.handle_announce(&ann_a);
    }

    println!("Step 4: Verifying Node B and C reflect bumped incarnation...");
    for node in overseer.nodes.iter().skip(1) {
        let gm = node.manager.lock().await;
        if let Some(state) = gm.peer_table.get(&node_a_id) {
            assert!(state.incarnation >= 2);
        } else {
            panic!("Node A not found in peer table");
        }
    }
    println!("Test passed successfully.");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster_failure_detection_and_tombstone() {
    println!("Step 1: Spawning cluster for failure detection and tombstone...");
    let mut overseer = ClusterOverseer::spawn_cluster(3, 1339).await;
    overseer.connect_all().await;

    let dead_node_id = overseer.nodes[2].id.clone();

    println!("Step 2: Terminating Node C...");
    let dead_node = overseer.nodes.remove(2);
    drop(dead_node);

    println!("Step 3: Forcing failure detection timeout checks...");
    for node in &overseer.nodes {
        let mut gm = node.manager.lock().await;
        gm.peer_table.check_timeouts(std::time::Duration::from_millis(0), std::time::Duration::from_millis(0));
    }

    println!("Step 4: Verifying remaining nodes detect Node C failure...");
    for node in &overseer.nodes {
        let gm = node.manager.lock().await;
        if let Some(state) = gm.peer_table.get(&dead_node_id) {
            assert!(
                state.status == PeerStatus::Suspect
                    || state.status == PeerStatus::Dead
            );
        }
    }
    println!("Test passed successfully.");
}
