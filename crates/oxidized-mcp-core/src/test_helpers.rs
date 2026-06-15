//! Shared test helpers for oxidized-mcp-core.

#[cfg(test)]
pub mod test_env {
    use std::sync::{Mutex, MutexGuard};

    static LOCK: Mutex<()> = Mutex::new(());

    pub fn lock() -> MutexGuard<'static, ()> {
        LOCK.lock().unwrap()
    }
}

#[cfg(test)]
pub mod fake_executable {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// Write an executable shell script to a unique temp path.
    ///
    /// Uses write-to-`.tmp` + `rename` so the script is never executed while
    /// still open for writing (avoids `ETXTBSY` / "Text file busy" on Linux
    /// under parallel `cargo test`).
    pub fn write(script: &str, tag: &str) -> PathBuf {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "oxidized-mcp-exec-{tag}-{}-{seq}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("fake-podman");
        let tmp = dir.join("fake-podman.tmp");

        {
            let mut file = std::fs::File::create(&tmp).unwrap();
            file.write_all(script.as_bytes()).unwrap();
            file.sync_all().unwrap();
        }
        std::fs::rename(&tmp, &path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }
}
