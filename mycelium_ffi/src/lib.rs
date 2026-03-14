use mycelium_core::{GossipManager, NetManager, NodeState};
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::panic::catch_unwind;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

pub struct MyceliumInstance {
    pub runtime: Runtime,
    pub gossip_manager: Arc<Mutex<GossipManager>>,
}

/// Initialize a new Mycelium instance.
///
/// # Safety
/// - `storage_path` must be a valid null-terminated C string pointer, or null.
/// - The caller must ensure the pointer remains valid for the duration of the call.
/// - Returns a raw pointer that must be freed with `mycelium_destroy`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mycelium_init(
    storage_path: *const c_char,
    cluster_token: u32,
) -> *mut MyceliumInstance {
    let result = catch_unwind(|| {
        if storage_path.is_null() {
            return std::ptr::null_mut();
        }

        let c_str = unsafe { CStr::from_ptr(storage_path) };
        let path = c_str.to_string_lossy().into_owned();

        let rt = Runtime::new().expect("Failed to create Tokio runtime");
        let net_manager = NetManager::new(0).expect("Failed to create NetManager");

        // We need to handle the cluster_token here.
        // Note: NodeState::init currently has a hardcoded token.
        // In a real impl, we'd pass it to init.
        let mut node_state = NodeState::init(&path);
        node_state.cluster_token = cluster_token;

        let gossip_manager = GossipManager::new(net_manager, node_state);

        Box::into_raw(Box::new(MyceliumInstance {
            runtime: rt,
            gossip_manager: Arc::new(Mutex::new(gossip_manager)),
        }))
    });

    result.unwrap_or(std::ptr::null_mut())
}

/// Get the node ID of a Mycelium instance.
///
/// # Safety
/// - `instance` must be a valid pointer returned by `mycelium_init`, or null.
/// - The returned string must be freed with `mycelium_free_string`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mycelium_get_node_id(instance: *mut MyceliumInstance) -> *const c_char {
    let result = catch_unwind(|| {
        if instance.is_null() {
            return std::ptr::null();
        }

        let inst = unsafe { &*instance };
        let manager = inst.gossip_manager.lock().unwrap();

        let node_id = manager.node_state.node_id().to_string();
        CString::new(node_id).unwrap().into_raw()
    });

    result.unwrap_or(std::ptr::null())
}

/// Advance the Mycelium instance by one tick.
///
/// # Safety
/// - `instance` must be a valid pointer returned by `mycelium_init`, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mycelium_tick(instance: *mut MyceliumInstance) -> c_int {
    let result = catch_unwind(|| {
        if instance.is_null() {
            return -1;
        }

        let inst = unsafe { &*instance };
        let mut manager = inst.gossip_manager.lock().unwrap();
        manager.tick();
        0
    });

    result.unwrap_or(-1)
}

/// Free a string returned by `mycelium_get_node_id`.
///
/// # Safety
/// - `s` must be a valid pointer returned by `mycelium_get_node_id`, or null.
/// - The pointer must not be used after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mycelium_free_string(s: *mut c_char) {
    if !s.is_null() {
        let _ = unsafe { CString::from_raw(s) };
    }
}

/// Destroy a Mycelium instance and free its resources.
///
/// # Safety
/// - `instance` must be a valid pointer returned by `mycelium_init`, or null.
/// - The pointer must not be used after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mycelium_destroy(instance: *mut MyceliumInstance) {
    if !instance.is_null() {
        let _ = unsafe { Box::from_raw(instance) };
    }
}