// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::Result;
use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use lance_arrow::json::{
    arrow_json_to_lance_json, convert_json_columns, convert_lance_json_to_arrow,
    is_arrow_json_field, is_json_field,
};
use lance_core::ROW_ID;
use lance_core::Result as LanceCoreResult;
use lance_io::object_store::{ObjectStoreParams, StorageOptionsAccessor, StorageOptionsProvider};
use lance_table::format::BasePath;
use lance_table::rowids::{RowIdIndex, RowIdSequence};
use roaring::RoaringTreemap;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::mpsc::Receiver;

fn extract_row_ids(
    row_ids: &mut CapturedRowIds,
    batch: RecordBatch,
    row_id_idx: usize,
    non_row_id_projection: &[usize],
) -> DFResult<RecordBatch> {
    let row_ids_arr = batch.column(row_id_idx);
    let row_ids_itr = row_ids_arr
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap_or_else(|| {
            panic!(
                "Row ids had an unexpected type: {}",
                row_ids_arr.data_type()
            )
        })
        .values();
    row_ids.capture(row_ids_itr)?;
    Ok(batch.project(non_row_id_projection)?)
}

/// Given a stream that includes a row id column, return a stream that will
/// capture the row id. At completion of the stream, the captured row ids can
/// be received from the returned receiver.
pub fn make_rowid_capture_stream(
    mut target: SendableRecordBatchStream,
    stable_row_ids: bool,
) -> Result<(SendableRecordBatchStream, Receiver<CapturedRowIds>)> {
    let mut row_ids = CapturedRowIds::new(stable_row_ids);

    let (tx, rx) = std::sync::mpsc::channel();

    let schema = target.schema();
    let (row_id_idx, _) = schema
        .column_with_name(ROW_ID)
        .expect("Received a batch without row ids");
    let non_row_ids_cols = (0..schema.fields.len())
        .filter(|col| *col != row_id_idx)
        .collect::<Vec<_>>();
    let output_schema = Arc::new(schema.project(&non_row_ids_cols)?);

    let stream = futures::stream::poll_fn(move |cx| match target.poll_next_unpin(cx) {
        std::task::Poll::Ready(Some(Ok(batch))) => {
            let res = extract_row_ids(&mut row_ids, batch, row_id_idx, &non_row_ids_cols);
            std::task::Poll::Ready(Some(res))
        }
        std::task::Poll::Ready(Some(Err(err))) => std::task::Poll::Ready(Some(Err(err))),
        std::task::Poll::Ready(None) => {
            let row_ids_out = std::mem::take(&mut row_ids);
            tx.send(row_ids_out).unwrap();
            std::task::Poll::Ready(None)
        }
        std::task::Poll::Pending => std::task::Poll::Pending,
    });

    let stream = RecordBatchStreamAdapter::new(output_schema, stream);

    Ok((Box::pin(stream), rx))
}

#[derive(Debug)]
pub enum CapturedRowIds {
    AddressStyle(RoaringTreemap),
    SequenceStyle(RowIdSequence),
}

impl CapturedRowIds {
    pub fn new(stable_row_ids: bool) -> Self {
        if stable_row_ids {
            Self::SequenceStyle(RowIdSequence::new())
        } else {
            Self::AddressStyle(RoaringTreemap::new())
        }
    }

    pub fn capture(&mut self, row_ids: &[u64]) -> DFResult<()> {
        match self {
            Self::AddressStyle(ids) => {
                // Assume they are sorted
                ids.append(row_ids.iter().cloned())
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
            }
            Self::SequenceStyle(sequence) => {
                sequence.extend(row_ids.into());
            }
        }
        Ok(())
    }

    pub fn row_id_sequence(&self) -> Option<&RowIdSequence> {
        match self {
            Self::SequenceStyle(sequence) => Some(sequence),
            _ => None,
        }
    }

    pub fn row_addrs(&self, index: Option<&RowIdIndex>) -> Cow<'_, RoaringTreemap> {
        match self {
            Self::AddressStyle(addrs) => Cow::Borrowed(addrs),
            Self::SequenceStyle(sequence) => {
                let mut treemap = RoaringTreemap::new();
                let Some(index) = index else {
                    panic!("RowIdIndex required for sequence style row ids")
                };
                for row_id in sequence.iter() {
                    treemap.insert(index.get(row_id).expect("row id missing from index").into());
                }
                Cow::Owned(treemap)
            }
        }
    }
}

impl Default for CapturedRowIds {
    fn default() -> Self {
        Self::AddressStyle(RoaringTreemap::new())
    }
}

#[derive(Debug)]
struct BasePathStorageOptionsProvider {
    initial_options: HashMap<String, String>,
    base_storage_options: HashMap<String, String>,
    provider: Arc<dyn StorageOptionsProvider>,
}

impl BasePathStorageOptionsProvider {
    fn new(
        initial_options: HashMap<String, String>,
        base_storage_options: HashMap<String, String>,
        provider: Arc<dyn StorageOptionsProvider>,
    ) -> Self {
        Self {
            initial_options,
            base_storage_options,
            provider,
        }
    }
}

#[async_trait]
impl StorageOptionsProvider for BasePathStorageOptionsProvider {
    async fn fetch_storage_options(&self) -> LanceCoreResult<Option<HashMap<String, String>>> {
        let fetched_options = self.provider.fetch_storage_options().await?;
        let mut merged = self.initial_options.clone();
        if let Some(fetched_options) = fetched_options {
            merged.extend(fetched_options);
        }
        merged.extend(self.base_storage_options.clone());
        Ok(Some(merged))
    }

    fn provider_id(&self) -> String {
        format!(
            "base-path-storage-options[initial={},base={},provider={}]",
            stable_options_hash(&self.initial_options),
            stable_options_hash(&self.base_storage_options),
            self.provider.provider_id()
        )
    }
}

fn stable_options_hash(options: &HashMap<String, String>) -> String {
    let mut hasher = DefaultHasher::new();
    let mut keys = options.keys().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        key.hash(&mut hasher);
        options.get(key).hash(&mut hasher);
    }
    format!("{:x}", hasher.finish())
}

pub(crate) fn object_store_params_for_base_path(
    base_path: &BasePath,
    store_params: Option<&ObjectStoreParams>,
) -> ObjectStoreParams {
    let mut merged_params = store_params.cloned().unwrap_or_default();
    if base_path.storage_options.is_empty() {
        return merged_params;
    }

    let initial_options = merged_params.storage_options().cloned().unwrap_or_default();
    let mut merged_initial_options = initial_options.clone();
    merged_initial_options.extend(base_path.storage_options.clone());

    let merged_accessor =
        if let Some(existing_accessor) = merged_params.storage_options_accessor.take() {
            if let Some(provider) = existing_accessor.provider().cloned() {
                let provider = Arc::new(BasePathStorageOptionsProvider::new(
                    initial_options,
                    base_path.storage_options.clone(),
                    provider,
                ));
                Arc::new(StorageOptionsAccessor::with_initial_and_provider(
                    merged_initial_options,
                    provider,
                ))
            } else {
                Arc::new(StorageOptionsAccessor::with_static_options(
                    merged_initial_options,
                ))
            }
        } else {
            Arc::new(StorageOptionsAccessor::with_static_options(
                base_path.storage_options.clone(),
            ))
        };

    merged_params.storage_options_accessor = Some(merged_accessor);
    merged_params
}

/// Adapter around the existing JSON conversion utilities.
#[derive(Debug, Clone)]
pub struct SchemaAdapter {
    logical_schema: ArrowSchemaRef,
}

impl SchemaAdapter {
    /// Create a new adapter given the logical Arrow schema.
    pub fn new(logical_schema: ArrowSchemaRef) -> Self {
        Self { logical_schema }
    }

    /// Determine if the logical schema includes Arrow JSON fields that require conversion.
    pub fn requires_physical_conversion(&self) -> bool {
        self.logical_schema
            .fields()
            .iter()
            .any(|field| is_arrow_json_field(field))
    }

    /// Determine if the physical schema includes Lance JSON fields that must be converted back.
    pub fn requires_logical_conversion(schema: &ArrowSchemaRef) -> bool {
        schema.fields().iter().any(|field| is_json_field(field))
    }

    pub fn to_physical_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if self.requires_physical_conversion() {
            Ok(convert_json_columns(&batch)?)
        } else {
            Ok(batch)
        }
    }

    /// Convert a logical stream into a physical stream.
    pub fn to_physical_stream(
        &self,
        stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        // Check if any fields need conversion
        if !self.requires_physical_conversion() {
            return stream;
        }

        let arrow_schema = stream.schema();
        let mut new_fields = Vec::with_capacity(arrow_schema.fields().len());
        for field in arrow_schema.fields() {
            if is_arrow_json_field(field) {
                new_fields.push(Arc::new(arrow_json_to_lance_json(field)));
            } else {
                new_fields.push(Arc::clone(field));
            }
        }
        let converted_schema = Arc::new(ArrowSchema::new_with_metadata(
            new_fields,
            arrow_schema.metadata().clone(),
        ));

        let converted_stream = stream.map(move |batch_result| {
            batch_result.and_then(|batch| {
                convert_json_columns(&batch)
                    .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
            })
        });

        Box::pin(RecordBatchStreamAdapter::new(
            converted_schema,
            converted_stream,
        ))
    }

    /// Convert a physical stream into a logical stream.
    pub fn to_logical_stream(
        &self,
        stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        use lance_arrow::ARROW_EXT_NAME_KEY;
        use lance_arrow::json::ARROW_JSON_EXT_NAME;

        if !Self::requires_logical_conversion(&stream.schema()) {
            return stream;
        }

        let arrow_schema = stream.schema();
        let mut new_fields = Vec::with_capacity(arrow_schema.fields().len());
        for field in arrow_schema.fields() {
            if is_json_field(field) {
                let mut new_field = arrow_schema::Field::new(
                    field.name(),
                    arrow_schema::DataType::Utf8,
                    field.is_nullable(),
                );
                let mut metadata = field.metadata().clone();
                metadata.insert(
                    ARROW_EXT_NAME_KEY.to_string(),
                    ARROW_JSON_EXT_NAME.to_string(),
                );
                new_field.set_metadata(metadata);
                new_fields.push(new_field);
            } else {
                new_fields.push(field.as_ref().clone());
            }
        }
        let converted_schema = Arc::new(ArrowSchema::new_with_metadata(
            new_fields,
            arrow_schema.metadata().clone(),
        ));

        let converted_stream = stream.map(move |batch_result| {
            batch_result.and_then(|batch| {
                convert_lance_json_to_arrow(&batch).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(
                        Box::new(arrow_schema::ArrowError::InvalidArgumentError(
                            e.to_string(),
                        )),
                        None,
                    )
                })
            })
        });

        Box::pin(RecordBatchStreamAdapter::new(
            converted_schema,
            converted_stream,
        ))
    }
}
