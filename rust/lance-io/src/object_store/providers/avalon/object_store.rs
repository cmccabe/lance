use bytes::Bytes;
use chrono::DateTime;
use futures::stream::BoxStream;
use futures::{stream, StreamExt};
use lance_core::error::Result;
use object_store::{
    Attributes, GetRange, GetResultPayload,
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tonic::Status;
use log::error;
use log::info;
use url::Url;

use crate::object_store::{DEFAULT_DOWNLOAD_RETRY_COUNT, ObjectStore};

use super::clients::{AvalonClients, FetchChunkResult};

#[derive(Debug)]
pub struct AvalonObjectStore {
    pub inner: ObjectStore,
    pub inner_prefix: String,
    pub clients: Arc<dyn AvalonClients>,
    pub fallback_reads: AtomicU64,
}

impl std::fmt::Display for AvalonObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Avalon({})", &self.inner)?;
        Ok(())
    }
}

impl AvalonObjectStore {
    // Wrap this AvalonObjectStore in a lance_io::object_store::ObjectStore structure.
    pub fn wrap(
        self
    ) -> Result<ObjectStore> {
        let block_size = self.inner.block_size();
        let use_constant_size_upload_parts = self.inner.use_constant_size_upload_parts;
        let list_is_lexically_ordered = self.inner.list_is_lexically_ordered;
        let io_parallelism = self.inner.io_parallelism();
        Ok(ObjectStore::new(
            Arc::new(self),
            Url::parse("avalon://").unwrap(),
            Some(block_size),
            None,
            use_constant_size_upload_parts,
            list_is_lexically_ordered,
            io_parallelism,
            DEFAULT_DOWNLOAD_RETRY_COUNT,
            None,
        ))
    }

    // Create a new AvalonObjectStore.
    pub fn new(
        inner: ObjectStore,
        inner_prefix: String,
        clients: Arc<dyn AvalonClients>,
    ) -> Self {
        Self {
            inner,
            inner_prefix,
            clients,
            fallback_reads: AtomicU64::new(0),
        }
    }

    async fn fallback_get_opts(
        &self,
        location: &Path,
        options: GetOptions,
        reason: &str,
    ) -> object_store::Result<GetResult> {
        if self.fallback_reads.fetch_add(1, Ordering::SeqCst) % 1000 == 0 {
            error!(
                "Unable to read from Avalon. Falling back to direct get_opts. Reason: {}",
                reason
            );
        }
        self.inner.inner.get_opts(location, options).await
    }

    pub fn fallback_reads(&self) -> u64 {
        self.fallback_reads.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for AvalonObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> object_store::Result<PutResult> {
        self.inner.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        info!("DEBUG2: put_opts: location: {}, opts: {:?}", location, opts);
        self.inner.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        info!("DEBUG2: put_multipart: location: {}", location);
        self.inner.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        info!("DEBUG2: put_multipart_opts: location: {}, opts: {:?}", location, opts);
        self.inner.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        info!("DEBUG2: get: location: {}", location);
        self.get_opts(location, GetOptions::default()).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        info!("DEBUG2: get_opts: location: {}, options: {:?}", location, options);
        if options.version.is_some() {
            return object_store::Result::Err(object_store::Error::NotSupported {
                source: Box::new(std::io::Error::other("object versioning is not supported.")),
            });
        }
        let (start, end) = if options.head {
            (0u64, 0u64)
        } else {
            match &options.range {
                None => (0, u64::MAX),
                Some(GetRange::Bounded(range)) => {
                    if range.start >= range.end {
                        return object_store::Result::Err(object_store::Error::Generic {
                            store: "avalon",
                            source: Box::new(std::io::Error::other("invalid bounded range.")),
                        });
                    }
                    (range.start, range.end)
                }
                Some(GetRange::Offset(start)) => {
                    if *start == u64::MAX {
                        return object_store::Result::Err(object_store::Error::Generic {
                            store: "avalon",
                            source: Box::new(std::io::Error::other("invalid offset range.")),
                        });
                    }
                    (*start, u64::MAX)
                }
                Some(GetRange::Suffix(_)) => {
                    return self
                        .fallback_get_opts(location, options, "suffix ranges are not supported.")
                        .await
                }
            }
        };
        let fetch_result = match self
            .clients
            .fetch_chunk(&self.inner_prefix, &location.to_string(), start..end)
            .await
        {
            Err(err) => {
                return self
                    .fallback_get_opts(location, options, &format!("avalon fetch error: {}", err))
                    .await
            }
            Ok(result) => result,
        };
        let last_modified =
            DateTime::from_timestamp(fetch_result.object_mtime as i64, 0).unwrap_or_default();
        let object_size = fetch_result.object_size;
        type State = Option<anyhow::Result<FetchChunkResult, Status>>;
        let path = location.to_string();
        let clients = self.clients.clone();
        let inner_prefix = self.inner_prefix.clone();
        let stream = stream::unfold::<State, _, _, object_store::Result<Bytes>>(
            Some(Ok(fetch_result)),
            move |prev_result| {
                let path = path.clone();
                let clients = clients.clone();
                let inner_prefix = inner_prefix.clone();
                async move {
                    match prev_result {
                        None => None,
                        Some(Err(err)) => Some((
                            object_store::Result::Err(object_store::Error::Generic {
                                store: "avalon",
                                source: Box::new(std::io::Error::other(format!(
                                    "fetch_chunk error: {}.",
                                    err.message()
                                ))),
                            }),
                            None,
                        )),
                        Some(Ok(result)) => {
                            let next_start = result.offset + result.data.len() as u64;
                            if next_start >= result.object_size || next_start >= end {
                                Some((Ok(result.data), None))
                            } else {
                                let next_result = clients
                                    .fetch_chunk(&inner_prefix, &path, next_start..end)
                                    .await;
                                Some((Ok(result.data), Some(next_result)))
                            }
                        }
                    }
                }
            },
        );
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream.boxed()),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified,
                size: object_size,
                e_tag: None,
                version: None,
            },
            range: (start..std::cmp::min(end, object_size)),
            attributes: Attributes::default(),
        })
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> object_store::Result<Bytes> {
        // Default falls back on get_opts.
        info!("DEBUG2: get: get_range location: {}, range: {:?}", location, range);
        self.inner.inner.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        // Default falls back on get_range and ultimately get_opts.
        info!("DEBUG2: get_ranges location: {}, ranges: {:?}", location, ranges);
        self.inner.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        // Default falls back on get_range.
        info!("DEBUG2: head location: {}", location);
        self.inner.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        info!("DEBUG2: delete: location: {}", location);
        self.inner.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        info!("DEBUG2: delete_stream");
        self.inner.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        info!("DEBUG2: list: prefix: {:?}", prefix);
        self.inner.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        info!("DEBUG2: list_with_offset: prefix: {:?}, offset: {}", prefix, offset);
        self.inner.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        info!("DEBUG2: list_with_delimiter: prefix: {:?}", prefix);
        self.inner.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        info!("DEBUG2: copy: from: {}, to: {}", from, to);
        self.inner.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        info!("DEBUG2: rename: from: {}, to: {}", from, to);
        self.inner.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        info!("DEBUG2: copy_if_not_exists: from: {}, to: {}", from, to);
        self.inner.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        info!("DEBUG2: rename_if_not_exists: from: {}, to: {}", from, to);
        self.inner.inner.rename_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod test {
    use crate::object_store::*;
    use crate::mini::test::*;
    use crate::{metadata_plane::test::{AvalonClientClusterProvider, assert_cluster_topology_eq}, provider::AvalonObjectStoreProvider};
    use object_store::ObjectStore;

    #[tokio::test]
    async fn test_read_file() {
        let mut avalon = MiniAvalon::new();
        avalon.add_etcd().await.unwrap();
        let mut config = ConfigContext::new(1, &avalon.backing_dir());
        config.add_etcd(&avalon).add_dir();
        avalon.add_server(config).await.unwrap();
        let path = avalon
            .write_file(&avalon.to_abspath("abc/def"), 8193)
            .unwrap();
        let cluster_provider = AvalonClientClusterProvider::new(avalon.endpoints());
        assert_cluster_topology_eq(&cluster_provider, vec![1], Vec::<u32>::new())
            .await
            .unwrap();
        let provider = AvalonObjectStoreProvider;
        let url = Url::parse("avalon://file-object-store").unwrap();
        let object_store = provider.new_avalon_store(url, &avalon.storage_parameters()).await.unwrap();
        let get_options = GetOptions::default();
        let get_result = object_store.get_opts(&Path::parse(&path).unwrap(), get_options).await.unwrap();
        assert_eq!(path[1..], get_result.meta.location.to_string()); // object_store::Path always omits the initial slash.
        assert_eq!(8193, get_result.meta.size);
        assert_eq!(None, get_result.meta.e_tag);
        assert_eq!(None, get_result.meta.version);
        assert_eq!(avalon.read_file(&path).unwrap(), get_result.bytes().await.unwrap());
        avalon.shutdown().await.unwrap();
    }
}
