//! Shared test helpers for oxidized-mcp-core.

#[cfg(test)]
pub mod test_env {
    use std::sync::{Mutex, MutexGuard};

    static LOCK: Mutex<()> = Mutex::new(());

    pub fn lock() -> MutexGuard<'static, ()> {
        LOCK.lock().unwrap()
    }
}
