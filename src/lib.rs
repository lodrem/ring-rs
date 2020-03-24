use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Cursor;
use std::rc::Rc;

use blake2::{Blake2b, Digest};
use byteorder::{LittleEndian, ReadBytesExt};

#[derive(Debug)]
pub struct Host {
    name: String,
    load: u64,
}

#[derive(Debug)]
pub struct Config {
    pub replication_factor: u64,
    pub load: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            replication_factor: 10,
            load: 1.25,
        }
    }
}

pub struct Ring {
    config: Config,

    hashes: Vec<u64>,                                 // hashes sorted ascendingly
    host_by_hash: HashMap<u64, Rc<RefCell<Host>>>,    // index host by hash
    host_by_name: HashMap<String, Rc<RefCell<Host>>>, // index host by name
    load: u64,                                        // the total load of ring
}

unsafe impl Send for Ring {}
unsafe impl Sync for Ring {}

impl Ring {
    pub fn new(config: Config) -> Self {
        Self {
            config: config,
            hashes: Default::default(),
            host_by_hash: Default::default(),
            host_by_name: Default::default(),
            load: 0,
        }
    }

    pub fn replication_factor(&self) -> u64 {
        self.config.replication_factor
    }

    /// Adds a new host to the ring.
    /// If the host already added, ignore.
    pub fn add(&mut self, hostname: &str) {
        if self.host_by_name.contains_key(hostname) {
            return;
        }

        let host = Rc::new(RefCell::new(Host {
            name: hostname.to_owned(),
            load: 0,
        }));

        self.host_by_name.insert(hostname.to_owned(), host.clone());

        for i in 0..self.replication_factor() {
            let hash = Self::hash(&format!("{}{}", hostname, i));
            self.host_by_hash.insert(hash, host.clone());
            self.hashes.push(hash);
        }

        self.hashes.sort();
    }

    /// Removes host from the ring.
    pub fn remove(&mut self, hostname: &str) {
        for i in 0..self.replication_factor() {
            let hash = Self::hash(&format!("{}{}", hostname, i));
            self.host_by_hash.remove(&hash);
            let idx = self.hashes.iter().position(|x| *x == hash).unwrap();
            self.hashes.remove(idx);
        }

        self.host_by_name.remove(hostname);
    }

    /// Locates a host for the key.
    pub fn get(&mut self, key: &str) -> Option<String> {
        if self.host_by_hash.is_empty() {
            return None;
        }

        let hash = Self::hash(key);
        let idx = self.search(hash);
        if let Some(host) = self.host_by_hash.get(&self.hashes[idx]) {
            Some(host.borrow().name.clone())
        } else {
            None
        }
    }

    /// Picks the least load host for the key.
    pub fn get_least(&mut self, key: &str) -> Option<String> {
        if self.host_by_hash.is_empty() {
            return None;
        }

        let hash = Self::hash(key);
        let avg_load = self.avg_load();

        let mut idx = self.search(hash);
        loop {
            let host = self.host_by_hash.get(&self.hashes[idx]).unwrap();
            if (host.borrow().load + 1) as f64 <= avg_load {
                return Some(host.borrow().name.clone());
            }
            idx += 1;
            if idx >= self.host_by_hash.len() {
                idx = 0;
            }
        }
    }

    /// Lists all hosts in the ring.
    pub fn hosts(&mut self) -> Vec<String> {
        self.host_by_name.keys().cloned().into_iter().collect()
    }

    /// Sets the load of host to the given value.
    pub fn set_load(&mut self, hostname: &str, load: u64) {
        if let Some(host) = self.host_by_name.get(hostname) {
            let mut host = host.borrow_mut();
            self.load -= host.load;
            host.load = load;
            self.load += load;
        }
    }

    /// Increments the load of host by 1.
    pub fn inc_load(&mut self, hostname: &str) {
        if let Some(host) = self.host_by_name.get(hostname) {
            self.load += 1;
            host.borrow_mut().load += 1;
        }
    }

    /// Decrements the load of host by 1.
    pub fn decr_load(&mut self, hostname: &str) {
        if let Some(host) = self.host_by_name.get(hostname) {
            self.load -= 1;
            host.borrow_mut().load -= 1;
        }
    }

    /// Gets the average load of ring.
    pub fn avg_load(&self) -> f64 {
        let mut load = (self.load + 1) as f64 / self.host_by_name.len() as f64;
        if load == 0.0 {
            load = 1.0;
        }
        (load * self.config.load).ceil()
    }

    fn search(&self, key: u64) -> usize {
        for i in 0..self.hashes.len() {
            let idx = self.hashes[i];
            if idx >= key {
                return i as usize;
            }
        }

        0
    }

    /// Hashes key.
    /// TODO(luncj): supports custom hasher.
    fn hash(key: &str) -> u64 {
        let hash = Blake2b::new().chain(key.as_bytes()).result();

        let mut rdr = Cursor::new(hash);

        rdr.read_u64::<LittleEndian>().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::Ring;

    #[test]
    fn ring_add() {
        let mut r = Ring::new(Default::default());
        r.add("1.1.1.1");

        assert_eq!(r.replication_factor(), r.hashes.len() as u64);
    }

    #[test]
    fn ring_get() {
        let mut r = Ring::new(Default::default());
        r.add("1.1.1.1");
        let host = r.get("1.1.1.1");

        assert!(host.is_some());
        assert_eq!("1.1.1.1", host.unwrap());
    }

    #[test]
    fn ring_remove() {
        let mut r = Ring::new(Default::default());
        r.add("1.1.1.1");
        r.remove("1.1.1.1");

        assert!(r.hashes.is_empty());
        assert!(r.hosts().is_empty());
    }
}
