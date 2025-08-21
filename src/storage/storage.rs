use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use tokio::{
    sync::{Mutex, Notify},
    time::Instant,
};

pub struct DB {
    storage: Arc<Storage>,
}

pub struct Storage {
    inner: Mutex<HashMap<String, String>>,
    expiry: Mutex<BTreeMap<String, Instant>>,
    background_task: Notify,
}

impl DB {
    pub fn new() -> Self {
        let storage = Arc::new(Storage::new());

        Self { storage }
    }

    pub async fn get(&self, k: &str) -> Option<String> {
        let now = Instant::now();
        {
            let expired = {
                let expiry = self.storage.expiry.lock().await;
                expiry.get(k).map(|i| &now > i).unwrap_or(false)
            };

            if expired {
                let mut storage = self.storage.inner.lock().await;
                let mut expiry = self.storage.expiry.lock().await;
                storage.remove(k);
                expiry.remove(k);

                return None;
            }

            let storage = self.storage.inner.lock().await;
            storage.get(k).cloned()
        }
    }

    pub async fn set(&mut self, k: &str, v: &str, expiry: Option<i64>) {
        {
            if let Some(time) = expiry {
                let when = Instant::now() + Duration::new(0, (time * 1000000) as u32);
                self.storage.expiry.lock().await.insert(k.into(), when);
            }

            self.storage.inner.lock().await.insert(k.into(), v.into());

            self.storage.background_task.notify_one();
        }
    }
}

impl Storage {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            expiry: Mutex::new(BTreeMap::new()),
            background_task: Notify::new(),
        }
    }
}
