use anyhow::{bail, Result};
use bytes::Bytes;
use tonic::transport::Channel;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::transport::Endpoint;
use tonic::Code;
use tonic::Status;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::object_store::providers::avalon::avalon_client;
use crate::object_store::providers::avalon::avalon_client::AvalonClient;

use super::FetchChunkRequest;
use super::GetClusterRequest;
use super::RemoveNodeRequest;

use super::cluster::unparse_endpoints;
use super::cluster::AvalonCluster;
use super::cluster::AvalonEndpoint;
use super::hash::AvalonClusterAndHasher;
use super::hash::Indicator;

#[derive(Debug)]
pub struct FetchChunkResult {
    pub data: Bytes,

    // Total object size.
    pub object_size: u64,

    // Object mtime in seconds since the epoch.
    pub object_mtime: u32,

    // The start offset of the data we fetched.
    pub offset: u64,
}

#[tonic::async_trait]
pub trait AvalonClients: std::fmt::Debug + Send + Sync {
    async fn get_cluster(&self) -> Result<Arc<AvalonClusterAndHasher>, Status>;
    async fn fetch_chunk(
        &self,
        prefix: &str,
        path: &str,
        range: Range<u64>,
    ) -> Result<FetchChunkResult, Status>;
    async fn remove_node(&self, node_id: u32) -> Result<(), Status>;
}

#[derive(Debug)]
pub struct AvalonClientsImpl {
    cluster_data: Mutex<ClusterData>,
}

#[derive(Debug)]
struct ClusterData {
    bootstrap_endpoints: Vec<AvalonEndpoint>,
    next_bootstrap_endpoint: usize,
    cluster: Option<Arc<AvalonClusterAndHasher>>,
    node_clients: HashMap<u32, Arc<Mutex<AvalonNodeClient>>>,
}

impl ClusterData {
    fn new(bootstrap_endpoints: Vec<AvalonEndpoint>) -> Self {
        Self {
            bootstrap_endpoints,
            next_bootstrap_endpoint: 0,
            cluster: None,
            node_clients: HashMap::<u32, Arc<Mutex<AvalonNodeClient>>>::new(),
        }
    }

    fn next_bootstrap_endpoint(&mut self) -> usize {
        let result = self.next_bootstrap_endpoint;
        self.next_bootstrap_endpoint =
            (self.next_bootstrap_endpoint + 1) % self.bootstrap_endpoints.len();
        result
    }

    async fn reload_cluster(&mut self) -> Result<Arc<AvalonClusterAndHasher>, Status> {
        let mut tries = 0;
        while tries < self.bootstrap_endpoints.len() {
            tries += 1;
            let node_index = self.next_bootstrap_endpoint();
            let addr = self.bootstrap_endpoints[node_index].addr();
            let mut node_client = match AvalonNodeClient::connect(&addr).await {
                Ok(node_client) => node_client,
                Err(err) => {
                    info!("Unable to connect to {}: {}", addr, err);
                    continue;
                }
            };
            let response = match node_client.inner.get_cluster(GetClusterRequest {}).await {
                Ok(response) => response,
                Err(err) => {
                    error!(
                        "Server {} returned error for GetClusterRequest: {}",
                        addr, err
                    );
                    continue;
                }
            };
            match AvalonCluster::from_response(response.get_ref()) {
                Ok(cluster) => {
                    debug!("Server {} returned cluster {}", addr, cluster);
                    let cluster_and_hasher = match AvalonClusterAndHasher::new(cluster) {
                        Ok(result) => result,
                        Err(err) => {
                            return Err(Status::unknown(format!(
                                "AvalonClusterAndHasher::new failed: {}",
                                err
                            )));
                        }
                    };
                    let new_cluster = Arc::new(cluster_and_hasher);
                    self.cluster = Some(new_cluster.clone());
                    self.node_clients.insert(
                        new_cluster.inner.source_node_id,
                        Arc::new(Mutex::new(node_client)),
                    );
                    return Ok(new_cluster);
                }
                Err(err) => {
                    return Err(Status::unknown(format!(
                        "Server {} returned a GetClusterRequest that could not be parsed: {}",
                        addr, err
                    )));
                }
            }
        }
        return Err(Status::unavailable(format!(
            "Unable to connect to any of the bootstrap endpoints in {}",
            unparse_endpoints(&self.bootstrap_endpoints)
        )));
    }

    async fn reload_node_client(&mut self, node_id: &u32) -> Result<(), Status> {
        let node = match self.cluster.as_ref().unwrap().inner.nodes.get(node_id) {
            None => {
                return Err(Status::unknown(format!(
                    "No node in the cluster was found with id {}.",
                    node_id
                )));
            }
            Some(node) => node,
        };
        let node_client = match AvalonNodeClient::connect(&node.addr()).await {
            Ok(node_client) => node_client,
            Err(err) => {
                return Err(Status::unavailable(format!(
                    "Unable to connect to {}: {}",
                    node, err
                )));
            }
        };
        self.node_clients
            .insert(node_id.clone(), Arc::new(Mutex::new(node_client)));
        Ok(())
    }
}

impl AvalonClientsImpl {
    pub fn new(bootstrap_endpoints: Vec<AvalonEndpoint>) -> Self {
        debug!(
            "Creating new AvalonClientsImpl with bootstrap_endpoints {:?}",
            bootstrap_endpoints
        );
        Self {
            cluster_data: Mutex::new(ClusterData::new(bootstrap_endpoints)),
        }
    }
}

#[tonic::async_trait]
impl AvalonClients for AvalonClientsImpl {
    async fn get_cluster(&self) -> Result<Arc<AvalonClusterAndHasher>, Status> {
        let mut data = self.cluster_data.lock().await;
        match &data.cluster {
            None => data.reload_cluster().await,
            Some(cluster) => Ok(cluster.clone()),
        }
    }

    async fn fetch_chunk(
        &self,
        prefix: &str,
        path: &str,
        range: Range<u64>,
    ) -> Result<FetchChunkResult, Status> {
        let (node_client, length) = {
            // Take the cluster lock and find what server we need to fetch the data from.
            let mut data = self.cluster_data.lock().await;
            let cluster = match &data.cluster {
                None => data.reload_cluster().await?,
                Some(cluster) => cluster.clone(),
            };
            let chunk_start = cluster.inner.align_offset_to_chunk_boundary(range.start);
            let indicator = Indicator {
                prefix,
                path,
                offset: chunk_start,
            };
            let node_client = match cluster.lookup(&indicator) {
                None => {
                    return Err(Status::new(
                        Code::Unavailable,
                        format!(
                            "No available servers for chunk starting at {}",
                            indicator.offset
                        ),
                    ));
                }
                Some(node_id) => {
                    if !data.node_clients.contains_key(&node_id) {
                        data.reload_node_client(&node_id).await?
                    }
                    data.node_clients.get(&node_id).unwrap().clone()
                }
            };
            let end = std::cmp::min(
                range.end,
                chunk_start + cluster.inner.client_chunk_size as u64,
            );
            let length = (end - range.start) as u32;
            (node_client, length)
        };
        // Release the cluster lock and take the lock for the specific node client.
        // Then perform the fetch. TODO: support multiple clients for a specific node.
        let mut client = node_client.lock().await;
        let response_or_err = client
            .inner
            .fetch_chunk(FetchChunkRequest {
                prefix: prefix.to_string(),
                path: path.to_string(),
                offset: range.start,
                length: length,
            })
            .await;
        match response_or_err {
            Ok(response) => {
                let object_mtime = response.get_ref().object_mtime;
                let object_size = response.get_ref().object_size;
                Ok(FetchChunkResult {
                    data: response.into_inner().payload.into(),
                    object_mtime,
                    object_size,
                    offset: range.start,
                })
            }
            Err(err) => Err(Status::new(
                err.code(),
                format!(
                    "Server {} returned error for FetchChunkRequest at offset {}: {}",
                    client.addr,
                    range.start,
                    err.message()
                ),
            )),
        }
    }

    async fn remove_node(&self, id_to_remove: u32) -> Result<(), Status> {
        let node_client = {
            let mut data = self.cluster_data.lock().await;
            let cluster = match &data.cluster {
                None => data.reload_cluster().await?,
                Some(cluster) => cluster.clone(),
            };
            match data.node_clients.values().next() {
                Some(node_client) => node_client.clone(),
                None => match cluster.as_ref().inner.up.first().clone() {
                    Some(node_id) => {
                        if !data.node_clients.contains_key(&node_id) {
                            data.reload_node_client(&node_id).await?
                        }
                        data.node_clients.get(&node_id).unwrap().clone()
                    }
                    None => {
                        return Err(Status::unavailable(
                            "No servers were up to receive this request.",
                        ));
                    }
                },
            }
        };
        let mut client = node_client.lock().await;
        match client
            .inner
            .remove_node(RemoveNodeRequest { id: id_to_remove })
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                return Err(Status::new(
                    err.code(),
                    format!("error removing node {}: {}", id_to_remove, err.message()),
                ));
            }
        }
    }
}

#[derive(Debug)]
pub struct AvalonNodeClient {
    addr: String,
    inner: avalon_client::AvalonClient<Channel>,
}

impl AvalonNodeClient {
    async fn connect(addr: &str) -> Result<Self> {
        let timeout = Duration::from_secs(10);
        let tonic_addr = if addr.contains("://") {
            addr.to_string()
        } else {
            format!("http://{}", addr)
        };
        let channel = match Endpoint::try_from(tonic_addr)?
            .connect_timeout(timeout)
            .connect()
            .await {
                Ok(channel) => channel,
                Err(err) => bail!("failed to connect to {} within timeout of {:#?}: {:?}",
                    addr, timeout, err),
            };
        let inner = AvalonClient::new(channel)
            .max_decoding_message_size(1024 * 1024 * 1024 /* 1GiB */);
        Ok(Self {
            addr: addr.to_string(),
            inner,
        })
    }
}
