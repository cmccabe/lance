use anyhow::bail;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use siphasher::sip::SipHasher13;
use std::fmt;
use std::hash::Hasher;

use super::cluster::AvalonCluster;

#[derive(Debug)]
pub struct AvalonClusterAndHasher {
    pub inner: AvalonCluster,
    pub hasher: AvalonHasher,
}

pub struct AvalonHasher {
    num_slots: u16,
    servers: Vec<bool>,
    slot_to_server: Vec<u16>,
}

impl AvalonHasher {
    pub fn new(servers: Vec<bool>) -> Result<Self> {
        if servers.len() >= std::u16::MAX as usize {
            bail!(
                "The number of servers cannot be more than {}.",
                std::u16::MAX - 1
            );
        }
        if !servers.contains(&true) {
            return Ok(Self {
                num_slots: 0 as u16,
                servers: servers,
                slot_to_server: vec![],
            });
        }
        let num_slots = 31991; // Must be a prime number.
        let mut slot_to_server = vec![std::u16::MAX; num_slots];
        let mut table = Vec::<Vec<u16>>::with_capacity(servers.len());
        for server in 0..servers.len() {
            if servers[server] {
                table.push(Self::permutations_for_server(server as u32, num_slots));
            } else {
                table.push(vec![]);
            }
        }
        let mut num_assigned = 0;
        for row in 0..num_slots {
            for server in 0..servers.len() {
                if num_assigned >= num_slots {
                    return Ok(Self {
                        num_slots: num_slots as u16,
                        servers: servers,
                        slot_to_server,
                    });
                }
                if table[server].len() > 0 {
                    let index = table[server][row] as usize;
                    if slot_to_server[index] == std::u16::MAX {
                        slot_to_server[index] = server as u16;
                        num_assigned = num_assigned + 1;
                    }
                }
            }
        }
        if num_assigned < num_slots {
            panic!("Logic error: unable to assign a server to each slot. num_assigned = {}, num_slots = {}", num_assigned, num_slots);
        }
        Ok(Self {
            num_slots: num_slots as u16,
            servers: servers,
            slot_to_server,
        })
    }

    fn permutations_for_server(server: u32, num_slots: usize) -> Vec<u16> {
        let mut results = Vec::<u16>::with_capacity(num_slots);
        let mut k_hash = SipHasher13::new();
        k_hash.write(&server.to_le_bytes());
        let n = num_slots as u64;
        let k = k_hash.finish() % n;
        let mut l_hash = SipHasher13::new_with_keys(123, 456);
        l_hash.write(&server.to_le_bytes());
        let l = l_hash.finish() % n;
        for j in 0..n {
            results.push(((k + (j * l)) % n) as u16);
        }
        results
    }

    pub fn lookup(&self, indicator: &Indicator) -> Option<u16> {
        if self.num_slots == 0 {
            return None;
        }
        let mut hasher = SipHasher13::new();
        hasher.write(indicator.prefix.as_bytes());
        hasher.write(indicator.path.as_bytes());
        hasher.write(&indicator.offset.to_le_bytes());
        let slot = hasher.finish() % (self.num_slots as u64);
        Some(*self.slot_to_server.get(slot as usize).unwrap())
    }
}

impl fmt::Debug for AvalonHasher {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AvalonHasher{{num_slots: {}, servers: {:?}}}",
            self.num_slots, self.servers
        )
    }
}

impl AvalonClusterAndHasher {
    pub fn new(cluster: AvalonCluster) -> Result<Self> {
        let servers = match cluster.nodes.last_key_value() {
            None => vec![],
            Some((k, _)) => {
                let mut servers = Vec::<bool>::with_capacity((k + 1) as usize);
                for i in 0..=*k {
                    servers.push(cluster.up.contains(&i));
                }
                servers
            }
        };
        let hasher = AvalonHasher::new(servers)?;
        Ok(Self {
            inner: cluster.clone(),
            hasher,
        })
    }

    pub fn lookup(&self, indicator: &Indicator) -> Option<u32> {
        self.hasher.lookup(indicator).map(|node| node as u32)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub struct Indicator<'a> {
    pub prefix: &'a str,
    pub path: &'a str,
    pub offset: u64,
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use rand::distributions::{Alphanumeric, DistString};
    use rand::thread_rng;

    use crate::cluster::test::make_fake_cluster_object;
    use crate::hash::*;

    fn random_string() -> String {
        Alphanumeric.sample_string(&mut thread_rng(), 16)
    }

    #[tokio::test]
    async fn test_hash_distribution() {
        let up = vec![0, 1, 2];
        let cluster = make_fake_cluster_object(&up, &vec![]);
        let cluster_and_hasher = AvalonClusterAndHasher::new(cluster).unwrap();

        let mut results = BTreeMap::<u32, usize>::new();
        for _i in 1..1000 {
            let indicator = Indicator {
                prefix: &random_string(),
                path: &random_string(),
                offset: 0,
            };
            let node = cluster_and_hasher.lookup(&indicator).unwrap();
            results.insert(node, results.get(&node).unwrap_or(&0) + 1);
        }

        for &result in results.keys() {
            assert!(up.contains(&result));
        }
        let mut lowest = std::usize::MAX;
        let mut highest: usize = 0;
        for &result in results.values() {
            lowest = std::cmp::min(lowest, result);
            highest = std::cmp::max(highest, result);
        }
        if highest - lowest > 250 {
            panic!(
                "Expected a more even distribution of values than {:?}",
                results.values()
            );
        }
    }

    #[tokio::test]
    async fn test_single_node() {
        let up = vec![2];
        let cluster = make_fake_cluster_object(&up, &vec![]);
        let cluster_and_hasher = AvalonClusterAndHasher::new(cluster).unwrap();
        for _i in 1..1000 {
            let indicator = Indicator {
                prefix: &random_string(),
                path: &random_string(),
                offset: 0,
            };
            assert_eq!(2, cluster_and_hasher.lookup(&indicator).unwrap());
        }
    }
}
