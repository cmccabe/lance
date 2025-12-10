use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use lance_core::error::{Error, Result};
use snafu::location;
use log::info;
use url::Url;

use crate::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreProvider, ObjectStoreRegistry};

use super::clients::{AvalonClients, AvalonClientsImpl};
use super::cluster::parse_endpoints;
use super::object_store::AvalonObjectStore;

#[derive(Debug, Default)]
pub struct AvalonObjectStoreProvider;

#[async_trait::async_trait]
impl ObjectStoreProvider for AvalonObjectStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        self.new_avalon_store(base_path, params).await?.wrap()
    }
}

impl AvalonObjectStoreProvider {
    pub async fn new_avalon_store(&self, outer_url: Url, params: &ObjectStoreParams) -> Result<AvalonObjectStore> {
        let endpoints = match Self::get_avalon_endpoints_string(&params.storage_options) {
            Err(err) => return Err(err),
            Ok(endpoints_string) => match parse_endpoints(&endpoints_string) {
                Ok(node_vec) => node_vec,
                Err(err) => {
                    return Err(Error::invalid_input(
                        format!("Unable to parse avalon endpoints string: {}", err),
                        location!(),
                    ))
                }
            },
        };
        let clients = Arc::new(AvalonClientsImpl::new(endpoints));
        let _ = clients.get_cluster().await.map_err(|err| {
            return Error::IO {
                source: Box::new(err),
                location: location!(),
            };
        })?;
        let inner_url = Self::parse_avalon_url(outer_url)?;
        let registry = ObjectStoreRegistry::default();
        let inner_provider = match registry.get_provider(inner_url.scheme()) {
            None => {
                return Err(Error::invalid_input(
                    format!(
                        "No provider found for inner object store scheme: {}",
                        inner_url.scheme()
                    ),
                    location!(),
                ))
            }
            Some(provider) => provider,
        };
        let inner_store = match inner_provider.new_store(inner_url.clone(), params).await {
            Err(err) => return Err(err),
            Ok(store) => store,
        };
        let inner_prefix = match inner_provider.calculate_object_store_prefix(
            inner_url.scheme(),
            inner_url.authority(),
            params.storage_options.as_ref(),
        ) {
            Ok(inner_prefix) => inner_prefix,
            Err(err) => return Err(err),
        };
        info!(
            "Creating new AvalonObjectStore with inner_prefix={}",
            inner_prefix
        );
        Ok(AvalonObjectStore::new(inner_store, inner_prefix, clients))
    }

    fn get_avalon_endpoints_string(
        storage_options: &Option<HashMap<String, String>>,
    ) -> Result<String> {
        if let Some(options) = storage_options {
            if let Some(endpoints_string) = options.get("avalon_endpoints") {
                return Ok(endpoints_string.clone());
            }
        };
        if let Ok(endpoints_string) = env::var("AVALON_ENDPOINTS") {
            return Ok(endpoints_string);
        };
        Err(Error::invalid_input(
            "No avalon_endpoints storage option or AVALON_ENDPOINTS environment variable found.",
            location!(),
        ))
    }

    /// Parse an avalon URL into the inner URL.
    /// For example, avalon://az_container@account/my/path becomes az://container@account/my/path.
    fn parse_avalon_url(outer_url: Url) -> Result<Url> {
        let full_url = &outer_url.to_string();
        if !full_url.starts_with("avalon://") {
            return Err(Error::invalid_input(
                "The URL scheme must be avalon.",
                location!(),
            ));
        }
        let url = &full_url["avalon://".len()..];
        let index = match url.find("_") {
            None => match url.find("/") {
                None => url.len(),
                Some(index) => index,
            },
            Some(index) => index,
        };
        let inner_scheme = &url[..index];
        let inner_remaining = if index >= url.len() {
            format!("{}://", inner_scheme)
        } else {
            format!("{}://{}", inner_scheme, &url[index + 1..])
        };
        let inner_url = match Url::parse(&inner_remaining) {
            Err(err) => {
                return Err(Error::invalid_input(
                    format!("Unable to reconstruct inner URL: {}", err),
                    location!(),
                ))
            }
            Ok(inner_url) => inner_url,
        };
        Ok(inner_url)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use url::Url;

    use crate::{mini::test::{ConfigContext, MiniAvalon}, provider::*};

    #[tokio::test]
    async fn test_parse_non_avalon_url_as_avalon() {
        let expected = "Invalid user input: The URL scheme must be avalon.";
        assert_eq!(
            expected,
            AvalonObjectStoreProvider::parse_avalon_url(
                Url::parse("s3://mybucket/mypath").unwrap()
            )
            .unwrap_err()
            .to_string()[..expected.len()]
                .to_string()
        );
    }

    #[tokio::test]
    async fn test_parse_no_underscore_avalon_url() {
        assert_eq!(
            "file-object-store://tmp/foo".to_string(),
            AvalonObjectStoreProvider::parse_avalon_url(
                Url::parse("avalon://file-object-store/tmp/foo").unwrap()
            )
            .unwrap()
            .to_string()
        );
    }

    #[tokio::test]
    async fn test_parse_avalon_url() {
        assert_eq!(
            Url::parse("s3://mybucket/mypath?blah=meh").unwrap(),
            AvalonObjectStoreProvider::parse_avalon_url(
                Url::parse("avalon://s3_mybucket/mypath?blah=meh").unwrap()
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn test_get_avalon_endpoints_string_with_no_endpoints() {
        let storage_options = HashMap::<String, String>::new();
        let expected = "Invalid user input: No avalon_endpoints storage option or AVALON_ENDPOINTS environment variable found.".to_string();
        assert_eq!(
            expected,
            AvalonObjectStoreProvider::get_avalon_endpoints_string(&Some(storage_options))
                .unwrap_err()
                .to_string()[0..expected.len()]
        );
    }

    #[tokio::test]
    async fn test_get_avalon_endpoints_string_with_endpoints_in_map() {
        let mut storage_options = HashMap::<String, String>::new();
        storage_options.insert(
            "avalon_endpoints".to_string(),
            "localhost:9090,localhost:9091,localhost:9092".to_string(),
        );
        assert_eq!(
            "localhost:9090,localhost:9091,localhost:9092".to_string(),
            AvalonObjectStoreProvider::get_avalon_endpoints_string(&Some(storage_options)).unwrap()
        );
    }

    async fn do_test_new_store(url: &str, expected_inner_prefix: &str) {
        let mut avalon = MiniAvalon::new();
        avalon.add_etcd().await.unwrap();

        for i in 1..=3 {
            let mut config = ConfigContext::new(i, &avalon.backing_dir());
            config.add_etcd(&avalon).add_dir();
            avalon.add_server(config).await.unwrap();
        }
        let provider = AvalonObjectStoreProvider;
        let url = Url::parse(url).unwrap();
        let object_store = provider.new_avalon_store(url, &avalon.storage_parameters()).await.unwrap();
        assert_eq!(expected_inner_prefix.to_string(), object_store.inner_prefix);
        let wrapped_object_store = object_store.wrap().unwrap();
        assert_eq!("avalon".to_string(), wrapped_object_store.scheme());
        avalon.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_new_store_with_s3() {
        do_test_new_store("avalon://s3_mybucket/my/path", "s3$mybucket").await;
    }

    #[tokio::test]
    async fn test_new_store_with_file_object_store_no_path() {
        do_test_new_store("avalon://file-object-store", "file-object-store").await;
    }

    #[tokio::test]
    async fn test_new_store_with_file_object_store_with_path() {
        do_test_new_store("avalon://file-object-store/my/path", "file-object-store").await;
    }
}
