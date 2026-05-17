use bepository_lock::{EpochFile, Lock};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

#[tokio::main]
async fn main() {
    let tmp = tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let prefix = Path::from("bench-lock");

    let num_locks = 2500;

    // Instead of sequentially acquiring the lock, bypass logic and directly write dummy files
    for i in 0..num_locks {
        let epoch = bepository_lock::Epoch::new(i).unwrap();
        let path = prefix.child(epoch.json_filename().as_str());
        let file = EpochFile {
            holder: "my_holder".to_string(),
            priority: 10,
            duration: 30,
        };
        let bytes = serde_json::to_vec(&file).unwrap();
        store.put(&path, bytes.into()).await.unwrap();
    }

    let lock_to_release = Lock::new(&*store, prefix.clone(), "my_holder".to_string(), 10, 30);

    // Start measuring
    let start = Instant::now();
    lock_to_release.release().await.unwrap();
    let duration = start.elapsed();

    println!("Release of {} locks took: {:?}", num_locks, duration);
}
