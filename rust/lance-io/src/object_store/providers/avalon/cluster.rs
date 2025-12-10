use anyhow::bail;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use sophon_protos::avalon::GetClusterResponse;
use sophon_protos::avalon::GetClusterResponseNode;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AvalonCluster {
    pub cluster_id: Uuid,
    pub client_chunk_size: u32,
    pub source_node_id: u32,
    pub nodes: BTreeMap<u32, AvalonEndpoint>,
    pub up: BTreeSet<u32>,
}

pub const EMPTY_CLUSTER: AvalonCluster = AvalonCluster {
    cluster_id: Uuid::from_u64_pair(0, 0),
    client_chunk_size: 0,
    source_node_id: 0,
    nodes: BTreeMap::new(),
    up: BTreeSet::new(),
};

impl AvalonCluster {
    /// Convert the AvalonCluster object to a GetClusterResponse.
    pub fn to_response(&self) -> GetClusterResponse {
        let (cluster_id_hi, cluster_id_lo) = self.cluster_id.as_u64_pair();
        let mut response_nodes = vec![];
        for (id, node) in &self.nodes {
            let response_node = GetClusterResponseNode {
                id: *id,
                host: node.host.clone(),
                port: node.port,
                up: self.up.contains(id),
            };
            response_nodes.push(response_node);
        }
        GetClusterResponse {
            cluster_id_hi,
            cluster_id_lo,
            client_chunk_size: self.client_chunk_size,
            source_node_id: self.source_node_id,
            nodes: response_nodes,
        }
    }

    /// Load the AvalonCluster object from a GetClusterResponse.
    pub fn from_response(response: &GetClusterResponse) -> Result<Self> {
        let mut nodes = BTreeMap::<u32, AvalonEndpoint>::new();
        let mut up = BTreeSet::<u32>::new();
        for response_node in &response.nodes {
            nodes.insert(
                response_node.id,
                AvalonEndpoint::new(&response_node.host, response_node.port)?,
            );
            if response_node.up {
                up.insert(response_node.id);
            }
        }
        if !nodes.contains_key(&response.source_node_id) {
            bail!(
                "Source node id was {}, but that node was not found in the response nodes.",
                response.source_node_id
            )
        }
        Ok(Self {
            cluster_id: Uuid::from_u64_pair(response.cluster_id_hi, response.cluster_id_lo),
            client_chunk_size: response.client_chunk_size,
            source_node_id: response.source_node_id,
            nodes,
            up,
        })
    }

    pub fn from_json(input: &str) -> Result<Self> {
        let cluster = serde_json::from_str(input)?;
        Ok(cluster)
    }

    pub fn align_offset_to_chunk_boundary(&self, offset: u64) -> u64 {
        let client_chunk_size = self.client_chunk_size as u64;
        let chunk = offset / client_chunk_size;
        chunk * client_chunk_size
    }
}

impl Display for AvalonCluster {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match serde_json::to_string(self) {
            Ok(s) => f.write_str(&s),
            Err(_) => Err(fmt::Error),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AvalonEndpoint {
    pub host: String,
    pub port: u32,
}

impl AvalonEndpoint {
    pub fn new(host: &str, port: u32) -> Result<Self> {
        if host.is_empty() {
            bail!("Invalid empty hostname.");
        }
        if port > 65535 {
            bail!("Invalid overly large port number {}.", port);
        }
        Ok(Self {
            host: host.to_string(),
            port,
        })
    }

    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn parse(input: &str) -> Result<Self> {
        let components = input.split("://").collect::<Vec<&str>>();
        let (prefix, remainder) = if components.len() == 1 {
            ("".to_string(), components[0])
        } else if components.len() == 2 {
            (format!("{}://", components[0]), components[1])
        } else {
            bail!(
                "Unable to parse {}: found more than one occurrence of ://",
                input
            );
        };
        let parts = remainder.split(":").collect::<Vec<&str>>();
        if parts.len() != 2 {
            bail!("Unable to parse {} as a host:port pair.", remainder);
        }
        let port = match parts[1].parse::<u32>() {
            Ok(port) => port,
            Err(err) => bail!(
                "Unable to parse the second half of {} as a port: {}",
                remainder,
                err
            ),
        };
        let node = Self::new(&format!("{}{}", &prefix, &parts[0]), port)?;
        Ok(node)
    }
}

impl Display for AvalonEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)?;
        Ok(())
    }
}

pub fn parse_endpoints(input: &str) -> Result<Vec<AvalonEndpoint>> {
    let mut result = vec![];
    let parts = input.split(",");
    for part in parts {
        result.push(AvalonEndpoint::parse(part)?);
    }
    Ok(result)
}

pub fn unparse_endpoints(input: &Vec<AvalonEndpoint>) -> String {
    let mut result = String::new();
    let mut prefix = "";
    for endpoint in input {
        result.push_str(prefix);
        result.push_str(&endpoint.to_string());
        prefix = ",";
    }
    result
}

#[cfg(test)]
pub mod test {
    use crate::cluster::*;

    pub fn make_fake_cluster_object(up_nodes: &[u32], down_nodes: &[u32]) -> AvalonCluster {
        let cluster_id = Uuid::from_u64_pair(0xa1a2a3a4b1b2c1c2u64, 0xd1d2d3d4d5d6d7d8u64);
        let source_node_id = if !up_nodes.is_empty() {
            *up_nodes.first().unwrap()
        } else if !down_nodes.is_empty() {
            *down_nodes.first().unwrap()
        } else {
            0
        };
        let mut nodes = BTreeMap::<u32, AvalonEndpoint>::new();
        let mut up = BTreeSet::<u32>::new();
        for node in up_nodes {
            nodes.insert(
                *node,
                AvalonEndpoint::new("localhost", 8080 + (node % 10) as u32).unwrap(),
            );
            up.insert(*node);
        }
        for node in down_nodes {
            nodes.insert(
                *node,
                AvalonEndpoint::new("localhost", 8080 + (node % 10) as u32).unwrap(),
            );
        }
        AvalonCluster {
            cluster_id,
            client_chunk_size: 16_384,
            source_node_id,
            nodes,
            up,
        }
    }

    #[tokio::test]
    async fn test_new_node_with_empty_hostname_fails() {
        assert_eq!(
            "Invalid empty hostname.",
            AvalonEndpoint::new("", 9090).unwrap_err().to_string()
        );
    }

    #[tokio::test]
    async fn test_new_node_with_overly_large_port_fails() {
        assert_eq!(
            "Invalid overly large port number 9000000.",
            AvalonEndpoint::new("example.com", 9000000)
                .unwrap_err()
                .to_string()
        );
    }

    #[tokio::test]
    async fn test_node_addr() {
        assert_eq!(
            "example.com:9090",
            AvalonEndpoint::new("example.com", 9090).unwrap().addr()
        );
    }

    #[tokio::test]
    async fn test_addr_parse() {
        assert_eq!(
            AvalonEndpoint::new("example.com", 9090).unwrap(),
            AvalonEndpoint::parse("example.com:9090").unwrap()
        );
    }

    #[tokio::test]
    async fn test_addr_parse_failure() {
        assert_eq!(
            "Unable to parse example.com as a host:port pair.",
            AvalonEndpoint::parse("example.com")
                .unwrap_err()
                .to_string()
        );
    }

    #[tokio::test]
    async fn test_single_element_node_vec_parse() {
        test_node_vec_round_trip(
            vec![AvalonEndpoint::new("local", 9090).unwrap()],
            "local:9090",
        )
        .unwrap();
    }

    #[tokio::test]
    async fn test_three_element_node_vec_parse() {
        test_node_vec_round_trip(
            vec![
                AvalonEndpoint::new("example.com", 9090).unwrap(),
                AvalonEndpoint::new("example2.com", 9091).unwrap(),
                AvalonEndpoint::new("local", 9090).unwrap(),
            ],
            "example.com:9090,example2.com:9091,local:9090",
        )
        .unwrap();
    }

    fn test_node_vec_round_trip(vec: Vec<AvalonEndpoint>, input: &str) -> Result<()> {
        assert_eq!(vec, parse_endpoints(input)?);
        assert_eq!(input, unparse_endpoints(&vec));
        Ok(())
    }

    #[tokio::test]
    async fn test_big_cluster_to_response() {
        let mut nodes = BTreeMap::<u32, AvalonEndpoint>::new();
        nodes.insert(1, AvalonEndpoint::new("localhost", 9090).unwrap());
        nodes.insert(2, AvalonEndpoint::new("localhost", 9091).unwrap());
        nodes.insert(3, AvalonEndpoint::new("localhost", 9092).unwrap());
        let mut up = BTreeSet::<u32>::new();
        up.insert(1);
        up.insert(2);
        let cluster_id = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let cluster = AvalonCluster {
            cluster_id,
            client_chunk_size: 4084,
            source_node_id: 2,
            nodes,
            up,
        };
        let response_nodes = vec![
            GetClusterResponseNode {
                id: 1 as u32,
                host: "localhost".to_string(),
                port: 9090,
                up: true,
            },
            GetClusterResponseNode {
                id: 2 as u32,
                host: "localhost".to_string(),
                port: 9091,
                up: true,
            },
            GetClusterResponseNode {
                id: 3 as u32,
                host: "localhost".to_string(),
                port: 9092,
                up: false,
            },
        ];
        let response = GetClusterResponse {
            cluster_id_hi: cluster_id.as_u64_pair().0,
            cluster_id_lo: cluster_id.as_u64_pair().1,
            client_chunk_size: 4084,
            source_node_id: 2,
            nodes: response_nodes,
        };
        test_cluster_round_trip(&cluster, response);
    }

    fn test_cluster_round_trip(cluster: &AvalonCluster, response: GetClusterResponse) {
        assert_eq!(response, cluster.to_response());
        assert_eq!(*cluster, AvalonCluster::from_response(&response).unwrap());
    }

    #[tokio::test]
    async fn test_parse_ip_addr_endpoints() {
        assert_eq!(
            vec![
                AvalonEndpoint::new("123.456.789.123", 9090).unwrap(),
                AvalonEndpoint::new("123.456.789.321", 9090).unwrap(),
                AvalonEndpoint::new("123.456.789.456", 9090).unwrap(),
            ],
            parse_endpoints("123.456.789.123:9090,123.456.789.321:9090,123.456.789.456:9090").unwrap()
        );
    }
}
