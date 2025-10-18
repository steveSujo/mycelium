use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use uuid::Uuid;

pub fn generate_id() -> String {
    Uuid::new_v4().to_string()
}

#[derive(Serialize, Deserialize, Debug)]
pub struct NodeState {
    pub node_id: String,
    pub incarnation: u64,
    pub cluster_token: u32,
    #[serde(skip)]
    storage_path: String,
}

impl NodeState {
    pub fn init(storage_path: &str) -> Self {
        let path = Path::new(storage_path).join("node_state.json");
        let storage_path = storage_path.to_string();

        // Ensure storage directory exists
        if let Err(e) = fs::create_dir_all(&storage_path) {
            eprintln!(
                "Warning: Failed to create storage directory {}: {}",
                storage_path, e
            );
        }

        if let Ok(content) = fs::read_to_string(&path)
            && let Ok(mut state) = serde_json::from_str::<NodeState>(&content)
        {
            state.storage_path = storage_path.clone();
            state.incarnation += 1; // Cold boot bump
            let _ = state.save(&path);
            return state;
        }

        let state = NodeState {
            node_id: generate_id(),
            incarnation: 1,
            cluster_token: 12345, // Default token
            storage_path,
        };

        let _ = state.save(&path);
        state
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }

    pub fn bump_incarnation(&mut self) {
        self.incarnation += 1;
        let path = Path::new(&self.storage_path).join("node_state.json");
        let _ = self.save(&path);
    }

    pub fn log_debug(&self, _msg: &str) {
        #[cfg(debug_assertions)]
        {
            use std::io::Write;
            let path = Path::new(&self.storage_path).join("debug.log");
            if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(&path) {
                let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
                let _ = writeln!(file, "[{}] {}", timestamp, _msg);
            }
        }
    }

    fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        let content = serde_json::to_string(self).unwrap();
        fs::write(path, content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_generate_id_is_unique() {
        let id1 = generate_id();
        let id2 = generate_id();
        assert_ne!(id1, id2, "Generated Node IDs should be unique");
    }

    #[test]
    fn test_node_id_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        // First init should generate a new ID
        let state1 = NodeState::init(path);
        let id1 = state1.node_id().to_string();
        assert!(!id1.is_empty());

        // Second init from same path should load the same ID
        let state2 = NodeState::init(path);
        assert_eq!(state2.node_id(), id1);
    }

    #[test]
    fn test_incarnation_persistence_and_bump() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        // First init creates state with incarnation (likely 1 or 0)
        let state1 = NodeState::init(path);
        let inc1 = state1.incarnation();

        // Second init should bump the incarnation
        let state2 = NodeState::init(path);
        assert_eq!(
            state2.incarnation(),
            inc1 + 1,
            "Incarnation should be bumped on cold boot"
        );
    }
}
